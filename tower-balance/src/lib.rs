#![doc(html_root_url = "https://docs.rs/tower-balance/0.1.0")]
#![deny(rust_2018_idioms)]
#![allow(elided_lifetimes_in_paths)]

pub mod error;
pub mod future;

#[cfg(test)]
mod test;

use self::future::ResponseFuture;
use crate::error;
use futures::{try_ready, Async, Poll};
use indexmap::IndexMap;
use log::{debug, info, trace};
use rand::{rngs::SmallRng, FromEntropy, Rng, SeedableRng};
use std::cmp;
use tower_discover::{Change, Discover};
use tower_load::Load;
use tower_service::Service;

/// Distributes requests across inner services using the [Power of Two Choices][p2c].
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
#[derive(Debug)]
pub struct P2CBalance<D: Discover> {
    // XXX Pool requires direct access to this... Not ideal.
    pub(crate) discover: D,

    endpoints: IndexMap<D::Key, D::Service>,

    /// Holds an index into `endpoints`, indicating the service that has been
    /// chosen to dispatch the next request.
    ready_index: Option<usize>,

    rng: SmallRng,
}

// ===== impl P2CBalance =====

impl<D: Discover> P2CBalance<D> {
    pub fn new(discover: D) -> Self {
        Self {
            rng: SmallRng::from_entropy(),
            discover,
            ready_index: None,
            endpoints: IndexMap::default(),
        }
    }

    /// Initializes a P2C load balancer from the provided randomization source.
    ///
    /// This may be preferable when an application instantiates many balancers.
    pub fn with_rng<R: Rng>(discover: D, rng: &mut R) -> Result<Self, rand::Error> {
        let rng = SmallRng::from_rng(rng)?;
        Ok(Self {
            rng,
            discover,
            ready_index: None,
            endpoints: IndexMap::default(),
        })
    }

    /// Polls `discover` for updates, adding new items to `not_ready`.
    ///
    /// Removals may alter the order of either `ready` or `not_ready`.
    fn poll_discover(&mut self) -> Poll<(), error::Balance>
    where
        D::Error: Into<error::Error>,
    {
        debug!("updating from discover");

        loop {
            match try_ready!(self.discover.poll().map_err(|e| error::Balance(e.into()))) {
                Change::Insert(key, svc) => drop(self.endpoints.insert(key, svc)),
                Change::Remove(rm_key) => {
                    // Update the ready index to account for reordering of endpoints.
                    let orig_sz = self.endpoints.len();
                    println!("removing (ready={:?})", self.ready_index);
                    if let Some((rm_idx, _, _)) = self.endpoints.swap_remove_full(&rm_key) {
                        self.ready_index = match self.ready_index {
                            Some(i) => Self::repair_index(i, rm_idx, orig_sz),
                            None => None,
                        };
                    }
                }
            }
        }
    }

    fn repair_index(orig_idx: usize, rm_idx: usize, orig_sz: usize) -> Option<usize> {
        let repaired = match orig_idx {
            i if i == rm_idx => None,              // removed
            i if i == orig_sz - 1 => Some(rm_idx), // swapped
            i => Some(i),                          // uneffected
        };
        trace!(
            "repair_index: orig={}; rm={}; sz={}; => {:?}",
            orig_idx,
            rm_idx,
            orig_sz,
            repaired,
        );
        repaired
    }

    fn poll_ready_index<Svc, Request>(&mut self) -> Poll<usize, Svc::Error>
    where
        D: Discover<Service = Svc>,
        Svc: Service<Request> + Load,
        Svc::Error: Into<error::Error>,
    {
        match self.endpoints.len() {
            0 => Ok(Async::NotReady),
            1 => {
                // If there's only one endpoint, ignore its but require that it
                // is ready.
                match self.poll_endpoint_index_load(0) {
                    Ok(Async::NotReady) => Ok(Async::NotReady),
                    Ok(Async::Ready(_)) => {
                        self.ready_index = Some(0);
                        Ok(Async::Ready(0))
                    }
                    Err(e) => {
                        info!("evicting failed endpoint: {}", e.into());
                        let _ = self.endpoints.swap_remove_index(0);
                        Ok(Async::NotReady)
                    }
                }
            }
            len => {
                // Get two distinct random indexes (in a random order). Poll each
                let idxs = rand::seq::index::sample(&mut self.rng, len, 2);

                let aidx = idxs.index(0);
                let bidx = idxs.index(1);
                println!("indexes a={} b={} / {}", aidx, bidx, len);

                let (aload, bidx) = match self.poll_endpoint_index_load(aidx) {
                    Ok(ready) => (ready, bidx),
                    Err(e) => {
                        info!("evicting failed endpoint: {}", e.into());
                        let _ = self.endpoints.swap_remove_index(aidx);
                        let new_bidx = Self::repair_index(bidx, aidx, len)
                            .expect("random indices must be distinct");
                        (Async::NotReady, new_bidx)
                    }
                };

                let (bload, aidx) = match self.poll_endpoint_index_load(bidx) {
                    Ok(ready) => (ready, aidx),
                    Err(e) => {
                        info!("evicting failed endpoint: {}", e.into());
                        let _ = self.endpoints.swap_remove_index(bidx);
                        let new_aidx = Self::repair_index(aidx, bidx, len)
                            .expect("random indices must be distinct");
                        (Async::NotReady, new_aidx)
                    }
                };

                trace!("load[{}]={:?}; load[{}]={:?}", aidx, aload, bidx, bload);

                let ready = match (aload, bload) {
                    (Async::Ready(aload), Async::Ready(bload)) => {
                        if aload <= bload {
                            Async::Ready(aidx)
                        } else {
                            Async::Ready(bidx)
                        }
                    }
                    (Async::Ready(_), Async::NotReady) => Async::Ready(aidx),
                    (Async::NotReady, Async::Ready(_)) => Async::Ready(bidx),
                    (Async::NotReady, Async::NotReady) => Async::NotReady,
                };
                trace!(" -> ready={:?}", ready);
                Ok(ready)
            }
        }
    }

    fn poll_endpoint_index_load<Svc, Request>(
        &mut self,
        index: usize,
    ) -> Poll<Svc::Metric, Svc::Error>
    where
        D: Discover<Service = Svc>,
        Svc: Service<Request> + Load,
        Svc::Error: Into<error::Error>,
    {
        println!(
            "poll_endpoint_index_load: index={}, len={}",
            index,
            self.endpoints.len()
        );
        let (_, svc) = self.endpoints.get_index_mut(index).expect("invalid index");
        try_ready!(svc.poll_ready());
        Ok(Async::Ready(svc.load()))
    }
}

impl<D, Svc, Request> Service<Request> for P2CBalance<D>
where
    D: Discover<Service = Svc>,
    D::Error: Into<error::Error>,
    Svc: Service<Request> + Load,
    Svc::Error: Into<error::Error>,
{
    type Response = <D::Service as Service<Request>>::Response;
    type Error = error::Error;
    type Future = ResponseFuture<<D::Service as Service<Request>>::Future>;

    /// Prepares the balancer to process a request.
    ///
    /// When `Async::Ready` is returned, `chosen` is set with a valid index
    /// into `ready` referring to a `Service` that is ready to disptach a request.
    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        // First and foremost, process discovery updates. This removes or updates a
        // previously-selected `ready_index` if appropriate.
        self.poll_discover()?;

        if let Some(index) = self.ready_index {
            debug_assert!(!self.endpoints.is_empty());
            // Ensure the selected endpoint is still ready.
            match self.poll_endpoint_index_load(index) {
                Ok(Async::Ready(_)) => return Ok(Async::Ready(())),
                Ok(Async::NotReady) => {}
                Err(e) => {
                    drop(self.endpoints.swap_remove_index(index));
                    info!("evicting failed endpoint: {}", e.into());
                }
            }

            self.ready_index = None;
        }

        let tries = match self.endpoints.len() {
            0 => return Ok(Async::NotReady),
            n => cmp::max(1, n / 2),
        };
        for _ in 0..tries {
            if let Async::Ready(idx) = self.poll_ready_index().map_err(Into::into)? {
                trace!("ready: {:?}", idx);
                self.ready_index = Some(idx);
                return Ok(Async::Ready(()));
            }
        }

        trace!("exhausted {} attempts", tries);
        Ok(Async::NotReady)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let index = self.ready_index.take().expect("not ready");
        let (_, svc) = self
            .endpoints
            .get_index_mut(index)
            .expect("invalid ready index");

        let fut = svc.call(request);
        ResponseFuture::new(fut)
    }
}
