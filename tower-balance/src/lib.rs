#![doc(html_root_url = "https://docs.rs/tower-balance/0.1.0")]
#![deny(rust_2018_idioms)]
#![allow(elided_lifetimes_in_paths)]
#[cfg(test)]
extern crate quickcheck;

use futures::{Async, Poll};
use indexmap::{IndexMap, IndexSet};
use log::{debug, trace};
use rand::{rngs::SmallRng, SeedableRng};
use std::{cmp, fmt, hash};
use tower_discover::Discover;
use tower_service::Service;

pub mod choose;
pub mod error;
pub mod future;
pub mod load;
pub mod pool;
mod weight;

#[cfg(test)]
mod test;

pub use self::{
    choose::Choose,
    load::Load,
    pool::Pool,
    weight::{HasWeight, Weight, Weighted, WithWeighted},
};

use self::{error::Error, future::ResponseFuture};

/// Balances requests across a set of inner services.
#[derive(Debug)]
pub struct P2CBalance<D, K, S> {
    /// Provides endpoints from service discovery.
    discover: D,

    /// Holds an index into `ready`, indicating the service that has been chosen to
    /// dispatch the next request.
    chosen_ready_index: Option<(Weight, usize)>,

    /// Holds an index into `ready`, indicating the service that dispatched the last
    /// request.
    dispatched_ready_index: Option<(Weight, usize)>,

    endpoints_by_weight: IndexMap<Weight, IndexMap<K, S>>,
}

// ===== impl Balance =====

impl<D, K> P2CBalance<D, K, D::Service>
where
    D: Discover<Key = Weighted<K>>,
    K: hash::Hash + cmp::Eq,
    D::Service: Load,
    <D::Service as Load>::Metric: PartialOrd + fmt::Debug,
{
    /// Chooses services using the [Power of Two Choices][p2c].
    ///
    /// This configuration is prefered when a load metric is known.
    ///
    /// As described in the [Finagle Guide][finagle]:
    ///
    /// > The algorithm randomly picks two services from the set of ready endpoints and
    /// > selects the least loaded of the two. By repeatedly using this strategy, we can
    /// > expect a manageable upper bound on the maximum load of any server.
    /// >
    /// > The maximum load variance between any two servers is bound by `ln(ln(n))` where
    /// > `n` is the number of servers in the cluster.
    ///
    /// [finagle]: https://twitter.github.io/finagle/guide/Clients.html#power-of-two-choices-p2c-least-loaded
    /// [p2c]: http://www.eecs.harvard.edu/~michaelm/postscripts/handbook2001.pdf
    pub fn new(discover: D) -> Self {
        Self {
            discover,
            chosen_ready_index: None,
            dispatched_ready_index: None,
            ready: IndexMap::default(),
            not_ready: IndexMap::default(),
        }
    }

    /// Initializes a P2C load balancer from the provided randomization source.
    ///
    /// This may be preferable when an application instantiates many balancers.
    pub fn new_with_rng<R: rand::Rng>(discover: D, rng: &mut R) -> Result<Self, rand::Error> {
        let rng = SmallRng::from_rng(rng)?;
        Ok(Self::new(discover, choose::PowerOfTwoChoices::new(rng)))
    }

    // Finds the coordinates of `key` within the current `endpoints_by_weight` structure.
    //
    // THese coordinates are invalid once endpoints_by_weight is altered.
    fn find_indices(&self, key: &K) -> Option<(usize, usize)> {
        for (widx, _, eps) in self.endpoints_by_weight.iter().enumerate()
            if let Some((eidx, _, _)) = eps.get_full(key) {
                return Some((widx, eidx));
            }
        }
        None
    }

    fn remove_endpoint(&mut self, key: &K) -> Option<D::Service> {
        let (widx, eidx) = self.find_indices(key)?;

        let (ep, empty) = {
            let ref mut eps = self.endpoints_by_weight.get_index_mut(widx)?;
            let ep = eps.swap_remove_index(eidx)?;
            (ep, eps.is_empty())
        };
        if empty {
            self.endpoints_by_weight.swap_remove_index(widx);
        }

        Some(ep)
    }

    /// Polls `discover` for updates, adding new items to `not_ready`.
    ///
    /// Removals may alter the order of either `ready` or `not_ready`.
    fn update_from_discover(&mut self) -> Result<(), error::Balance> {
        debug!("updating from discover");
        use tower_discover::Change::*;

        loop {
            match try_ready(self.discover.poll().map_err(|e| error::Balance(e.into()))) {
                Insert(weighted, svc) => {
                    let (key, weight) = weighted.into_parts();
                    drop(self.remove_endpoint(&key));

                    self.endpoints
                        .entry(weight)
                        .or_insert_with(|| IndexMap::default())
                        .insert(key, svc);
                }

                Remove(weighted) => {
                    let (key, _) = weighted.into_parts();
                    drop(self.remove_endpoint(&key));
                }
            }
        }
    }

    /// Chooses the next service to which a request will be dispatched.
    ///
    /// Ensures that .
    fn choose_and_poll_ready<Request>(
        &mut self,
    ) -> Poll<(), <D::Service as Service<Request>>::Error>
    where
        D::Service: Service<Request>,
    {
        loop {
            let n = self.ready.len();
            debug!("choosing from {} replicas", n);
            let idx = match n {
                0 => return Ok(Async::NotReady),
                1 => 0,
                _ => {
                    let replicas = choose::replicas(&self.ready).expect("too few replicas");
                    self.choose.choose(replicas)
                }
            };

            // XXX Should we handle per-endpoint errors?
            if self
                .poll_ready_index(idx)
                .expect("invalid ready index")?
                .is_ready()
            {
                self.chosen_ready_index = Some(idx);
                return Ok(Async::Ready(()));
            }
        }
    }
}

impl<D, K, Svc, Request> Service<Request> for P2CBalance<D, K, Svc>
where
    D: Discover<Key = Weighted<K>, Service = Svc>,
    D::Error: Into<Error>,
    Svc: Service<Request>,
    Svc::Error: Into<Error>,
{
    type Response = Svc::Response;
    type Error = Error;
    type Future = ResponseFuture<Svc::Future>;

    /// Prepares the balancer to process a request.
    ///
    /// When `Async::Ready` is returned, `chosen_ready_index` is set with a valid index
    /// into `ready` referring to a `Service` that is ready to disptach a request.
    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        // Clear before `ready` is altered.
        self.chosen_ready_index = None;

        // Update `not_ready` and `ready`.
        self.update_from_discover()?;

        // Choose the next service to be used by `call`.
        self.choose_and_poll_ready().map_err(Into::into)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let idx = self.chosen_ready_index.take().expect("not ready");
        let (_, svc) = self
            .ready
            .get_index_mut(idx)
            .expect("invalid chosen ready index");
        self.dispatched_ready_index = Some(idx);

        let rsp = svc.call(request);
        ResponseFuture::new(rsp)
    }
}
