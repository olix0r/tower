#![doc(html_root_url = "https://docs.rs/tower-balance/0.1.0")]
#![deny(rust_2018_idioms)]
#![allow(elided_lifetimes_in_paths)]
#[cfg(test)]
extern crate quickcheck;

use futures::{Async, Poll};
use indexmap::IndexMap;
use log::debug;
use rand::{rngs::SmallRng, FromEntropy, Rng, SeedableRng};
use std::{cmp, fmt, hash};
use tower_discover::{Change, Discover};
use tower_service::Service;

pub mod error;
pub mod future;
pub mod load;
mod weight;

#[cfg(test)]
mod test;

pub use self::{
    load::Load,
    weight::{HasWeight, Weight, Weighted, WithWeighted},
};

use self::{error::Error, future::ResponseFuture};

/// Balances requests across a set of inner services.
#[derive(Debug)]
pub struct P2CBalance<D, K, S>
where
    K: cmp::Eq + hash::Hash,
{
    /// Provides endpoints from service discovery.
    discover: D,

    /// Holds an index into `ready`, indicating the service that has been chosen to
    /// dispatch the next request.
    chosen_ready: Option<Coordinate>,

    /// Holds an index into `ready`, indicating the service that dispatched the last
    /// request.
    dispatched_ready: Option<Coordinate>,

    endpoints_by_weight: IndexMap<Weight, IndexMap<K, S>>,
    total_weight: usize,

    rng: SmallRng,
}

#[derive(Clone, Copy, Debug)]
struct Coordinate {
    /// Index into the `endpoints_by_weight` array.
    widx: usize,
    /// Index into the per-weight endpoints array.
    eidx: usize,
}

// ===== impl Balance =====

impl<D, K> P2CBalance<D, K, D::Service>
where
    K: hash::Hash + cmp::Eq,
    D: Discover<Key = Weighted<K>>,
    D::Error: Into<Error>,
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
            rng: SmallRng::from_entropy(),
            discover,
            chosen_ready: None,
            dispatched_ready: None,
            endpoints_by_weight: IndexMap::default(),
            total_weight: 0,
        }
    }

    /// Initializes a P2C load balancer from the provided randomization source.
    ///
    /// This may be preferable when an application instantiates many balancers.
    pub fn new_with_rng<R: Rng>(discover: D, rng: &mut R) -> Result<Self, rand::Error> {
        let rng = SmallRng::from_rng(rng)?;
        Ok(Self {
            rng,
            discover,
            chosen_ready: None,
            dispatched_ready: None,
            endpoints_by_weight: IndexMap::default(),
            total_weight: 0,
        })
    }

    // Finds the coordinates of `key` within the current `endpoints_by_weight` structure.
    //
    // These coordinates are invalid once endpoints_by_weight is altered.
    fn find(&self, key: &K) -> Option<Coordinate> {
        for (widx, (_, eps)) in self.endpoints_by_weight.iter().enumerate() {
            if let Some((eidx, _, _)) = eps.get_full(key) {
                return Some(Coordinate { widx, eidx });
            }
        }
        None
    }

    fn locate_mut(&mut self, coord: &Coordinate) -> Option<&mut D::Service> {
        self.endpoints_by_weight
            .get_index_mut(coord.widx)
            .and_then(move |(_, eps)| eps.get_index_mut(coord.eidx).map(|(_, svc)| svc))
    }

    fn remove_endpoint(&mut self, key: &K) -> Option<D::Service> {
        let Coordinate { widx, eidx } = self.find(key)?;

        let (ep, empty) = {
            let (_, ref mut eps) = self.endpoints_by_weight.get_index_mut(widx)?;
            let (_, ep) = eps.swap_remove_index(eidx)?;
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

        loop {
            match self.discover.poll().map_err(|e| error::Balance(e.into()))? {
                Async::NotReady => return Ok(()),

                Async::Ready(Change::Insert(weighted, svc)) => {
                    let (key, weight) = weighted.into_parts();
                    drop(self.remove_endpoint(&key));

                    self.endpoints_by_weight
                        .entry(weight)
                        .or_insert_with(|| IndexMap::default())
                        .insert(key, svc);
                }

                Async::Ready(Change::Remove(weighted)) => {
                    let (key, _) = weighted.into_parts();
                    drop(self.remove_endpoint(&key));
                }
            }
        }
    }
}

impl<D, K, Svc, Request> Service<Request> for P2CBalance<D, K, Svc>
where
    K: cmp::Eq + hash::Hash,
    D: Discover<Key = Weighted<K>, Service = Svc>,
    D::Error: Into<Error>,
    Svc: Service<Request> + Load,
    Svc::Error: Into<Error>,
    Svc::Metric: PartialOrd + fmt::Debug,
{
    type Response = <D::Service as Service<Request>>::Response;
    type Error = Error;
    type Future = ResponseFuture<<D::Service as Service<Request>>::Future>;

    /// Prepares the balancer to process a request.
    ///
    /// When `Async::Ready` is returned, `chosen_ready` is set with a valid index
    /// into `ready` referring to a `Service` that is ready to disptach a request.
    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        // Clear before `ready` is altered.
        self.chosen_ready = None;

        self.update_from_discover()?;

        let total_weight: usize = self.total_weight.into();
        for _ in 0..3 {
            let iw = self.rng.gen::<usize>() % total_weight;
            let jw = self.rng.gen::<usize>() % total_weight;


        }

        Ok(Async::NotReady)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let coord = self.chosen_ready.take().expect("not ready");
        let rsp = {
            let ref mut svc = self.locate_mut(&coord).expect("invalid chosen ready index");
            svc.call(request)
        };
        self.dispatched_ready = Some(coord);
        ResponseFuture::new(rsp)
    }
}
