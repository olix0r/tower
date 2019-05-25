use super::Level;
use futures::{try_ready, Async, Future, Poll};
use tower_discover::{Change, Discover};
use tower_service::Service;
use tower_util::MakeService;

/// A wrapper around `MakeService` that discovers a new service when load is high, and removes a
/// service when load is low. See [`Pool`].
pub struct PoolDiscover<MS, Target, Request>
where
    MS: MakeService<Target, Request>,
{
    maker: MS,
    making: Option<MS::Future>,
    target: Target,
    level: Level,
    services: usize,
}

// === impl PoolDiscover ===

impl<MS, Target, Request> Discover for PoolDiscover<MS, Target, Request>
where
    MS: MakeService<Target, Request>,
    // NOTE: these bounds should go away once MakeService adopts Box<dyn Error>
    MS::MakeError: ::std::error::Error + Send + Sync + 'static,
    MS::Error: ::std::error::Error + Send + Sync + 'static,
    Target: Clone,
{
    type Key = usize;
    type Service = MS::Service;
    type Error = MS::MakeError;

    fn poll(&mut self) -> Poll<Change<Self::Key, Self::Service>, Self::Error> {
        if self.services == 0 && self.making.is_none() {
            self.making = Some(self.maker.make_service(self.target.clone()));
        }

        if let Level::High = self.level {
            if self.making.is_none() {
                try_ready!(self.maker.poll_ready());
                self.making = Some(self.maker.make_service(self.target.clone()));
            }
        }

        if let Some(mut fut) = self.making.take() {
            if let Async::Ready(s) = fut.poll()? {
                self.services += 1;
                self.level = Level::Normal;
                return Ok(Async::Ready(Change::Insert(self.services, s)));
            } else {
                self.making = Some(fut);
                return Ok(Async::NotReady);
            }
        }

        match self.level {
            Level::High => {
                unreachable!("found high load but no Service being made");
            }
            Level::Normal => Ok(Async::NotReady),
            Level::Low if self.services == 1 => Ok(Async::NotReady),
            Level::Low => {
                self.level = Level::Normal;
                let rm = self.services;
                self.services -= 1;
                Ok(Async::Ready(Change::Remove(rm)))
            }
        }
    }
}
