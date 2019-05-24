use crate::{error, future::ResponseFuture, load::Load};
use futures::{try_ready, Async, Poll};
use indexmap::IndexMap;
use log::{debug, trace};
use rand::{rngs::SmallRng, FromEntropy, Rng, SeedableRng};
use std::{cmp, fmt};
use tower_discover::{Change, Discover};
use tower_service::Service;

/// Balances requests across a set of inner services.
#[derive(Debug)]
pub struct P2CBalance<D: Discover> {
    discover: D,

    endpoints: IndexMap<D::Key, D::Service>,

    /// Holds an index into `endpoints`, indicating the service that has been
    /// chosen to dispatch the next request.
    ready_index: Option<usize>,

    rng: SmallRng,
}

// ===== impl P2CBalance =====

impl<D: Discover> P2CBalance<D> {
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
                    if let Some((rm_idx, _, _)) = self.endpoints.swap_remove_full(&rm_key) {
                        self.ready_index = match self.ready_index {
                            Some(i) if i == rm_idx => None,              // removed
                            Some(i) if i == orig_sz - 1 => Some(rm_idx), // swapped
                            ready_index => ready_index,                  // uneffected
                        };
                    }
                }
            }
        }
    }

    fn poll_ready_index<Svc, Request>(&mut self) -> Poll<usize, Svc::Error>
    where
        D: Discover<Service = Svc>,
        Svc: Service<Request> + Load,
        Svc::Error: Into<error::Error>,
        Svc::Metric: fmt::Debug,
    {
        match self.endpoints.len() {
            0 => Ok(Async::NotReady),
            1 => {
                try_ready!(self.poll_endpoint_index_load(0));
                self.ready_index = Some(0);
                Ok(Async::Ready(0))
            }
            len => {
                let idxs = rand::seq::index::sample(&mut self.rng, len, 2);

                let aidx = idxs.index(0);
                let aload = self.poll_endpoint_index_load(aidx)?;
                let bidx = idxs.index(1);
                let bload = self.poll_endpoint_index_load(bidx)?;
                trace!("load[{:?}]={:?}; load[{:?}]={:?}", aidx, aload, bidx, bload);

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
        let (_, svc) = self.endpoints.get_index_mut(index).unwrap();
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
    Svc::Metric: PartialOrd + fmt::Debug,
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

        if self.ready_index.is_some() {
            debug_assert!(!self.endpoints.is_empty());
            return Ok(Async::Ready(()));
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
