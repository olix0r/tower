#![doc(html_root_url = "https://docs.rs/tower-balance/0.1.0")]
#![deny(rust_2018_idioms)]
#![allow(elided_lifetimes_in_paths)]

use futures::{Async, Poll};
use indexmap::IndexMap;
use log::{debug, trace};
use rand::{distributions::WeightedIndex, rngs::SmallRng, FromEntropy, Rng, SeedableRng};
use std::{cmp, fmt, hash};
use tower_discover::{Change, Discover};
use tower_service::Service;

pub mod error;
pub mod future;
pub mod load;
mod weight;

pub use self::{
    load::Load,
    weight::{Weight, Weighted},
};

use self::{error::Error, future::ResponseFuture};

/// Balances requests across a set of inner services.
#[derive(Debug)]
pub struct P2CBalance<D: Discover<Key = Weighted<K>>, K>
where
    K: cmp::Eq + hash::Hash,
{
    /// Provides endpoints from service discovery.
    discover: D,

    /// Holds an index into `ready`, indicating the service that has been chosen to
    /// dispatch the next request.
    chosen: Option<Coordinate>,

    endpoints_by_weight: IndexMap<Weight, IndexMap<K, D::Service>>,
    weighted_index: Option<WeightedIndex<usize>>,

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

impl<D, K> P2CBalance<D, K>
where
    K: hash::Hash + cmp::Eq,
    D: Discover<Key = Weighted<K>>,
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
            chosen: None,
            endpoints_by_weight: IndexMap::default(),
            weighted_index: None,
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
            chosen: None,
            endpoints_by_weight: IndexMap::default(),
            weighted_index: None,
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

    fn locate_mut(&mut self, Coordinate { widx, eidx }: Coordinate) -> Option<&mut D::Service> {
        self.endpoints_by_weight
            .get_index_mut(widx)
            .and_then(move |(_, eps)| eps.get_index_mut(eidx).map(|(_, svc)| svc))
    }

    fn remove_endpoint(&mut self, Coordinate { widx, eidx }: Coordinate) -> Option<D::Service> {
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
    fn update_from_discover(&mut self) -> Result<(), error::Balance>
    where
        D::Error: Into<Error>,
    {
        debug!("updating from discover");

        loop {
            match self.discover.poll().map_err(|e| error::Balance(e.into()))? {
                Async::NotReady => {
                    let weights = self.endpoints_by_weight.keys().map(|Weight(w)| w);
                    self.weighted_index = WeightedIndex::new(weights).ok();
                    return Ok(());
                }

                Async::Ready(Change::Insert(weighted, svc)) => {
                    let (key, weight) = weighted.into_parts();

                    if let Some(coord) = self.find(&key) {
                        drop(self.remove_endpoint(coord));
                    }

                    self.endpoints_by_weight
                        .entry(weight)
                        .or_insert_with(|| IndexMap::default())
                        .insert(key, svc);
                }

                Async::Ready(Change::Remove(weighted)) => {
                    let (key, _) = weighted.into_parts();

                    if let Some(coord) = self.find(&key) {
                        drop(self.remove_endpoint(coord));
                    }
                }
            }
        }
    }

    fn select_endpoint<Svc, Req>(
        &mut self,
        widx: usize,
    ) -> Result<(Coordinate, Option<<D::Service as Load>::Metric>), Error>
    where
        D: Discover<Service = Svc>,
        Svc: Service<Req> + Load,
        Svc::Error: Into<Error>,
    {
        let (_, ref mut eps) = self
            .endpoints_by_weight
            .get_index_mut(widx)
            .expect("index must be valid");

        let eidx = self.rng.gen::<usize>() % eps.len();
        let (_, svc) = eps.get_index_mut(eidx).expect("index must be valid");
        let load = if svc.poll_ready().map_err(Into::into)?.is_ready() {
            Some(svc.load())
        } else {
            None
        };

        Ok((Coordinate { widx, eidx }, load))
    }
}

impl<D, K, Svc, Request> Service<Request> for P2CBalance<D, K>
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
    /// When `Async::Ready` is returned, `chosen` is set with a valid index
    /// into `ready` referring to a `Service` that is ready to disptach a request.
    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        use rand::distributions::Distribution;

        // Clear before `ready` is altered.
        self.chosen = None;

        self.update_from_discover()?;

        let widx = match self.weighted_index.as_ref() {
            Some(d) => d.sample(&mut self.rng),
            None => {
                // There are no weights to consider. We've already polled the
                // discover, so it's safe to return not ready until new
                // endpoints are available.
                debug!("No weighted index");
                debug_assert!(self.endpoints_by_weight.is_empty());
                return Ok(Async::NotReady);
            }
        };

        let (a, aload) = self.select_endpoint(widx)?;
        let (b, bload) = self.select_endpoint(widx)?;
        trace!("a={:?} load={:?}; b={:?} load={:?}", a, aload, b, bload);
        self.chosen = match (aload, bload) {
            (Some(aload), Some(bload)) => {
                if aload <= bload {
                    Some(a)
                } else {
                    Some(b)
                }
            }
            (Some(_), None) => Some(a),
            (None, Some(_)) => Some(b),
            (None, None) => None,
        };

        trace!("chosen: {:?}", self.chosen);
        if self.chosen.is_some() {
            return Ok(Async::Ready(()));
        }

        Ok(Async::NotReady)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let coord = self.chosen.take().expect("not ready");
        let rsp = {
            let ref mut svc = self.locate_mut(coord).expect("invalid chosen ready index");
            svc.call(request)
        };
        ResponseFuture::new(rsp)
    }
}
