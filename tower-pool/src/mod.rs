//! This module defines a load-balanced pool of services that adds new services when load is high.
//!
//! The pool uses `poll_ready` as a signal indicating whether additional services should be spawned
//! to handle the current level of load. Specifically, every time `poll_ready` on the inner service
//! returns `Ready`, [`Pool`] consider that a 0, and every time it returns `NotReady`, [`Pool`]
//! considers it a 1. [`Pool`] then maintains an [exponential moving
//! average](https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average) over those
//! samples, which gives an estimate of how often the underlying service has been ready when it was
//! needed "recently" (see [`Builder::urgency`]). If the service is loaded (see
//! [`Builder::loaded_above`]), a new service is created and added to the underlying [`Balance`].
//! If the service is underutilized (see [`Builder::underutilized_below`]) and there are two or
//! more services, then the latest added service is removed. In either case, the load estimate is
//! reset to its initial value (see [`Builder::initial`] to prevent services from being rapidly
//! added or removed.
#![deny(missing_docs)]

use crate::{Load, P2CBalance};
use futures::{try_ready, Async, Future, Poll};
use tower_discover::{Change, Discover};
use tower_service::Service;
use tower_util::MakeService;

mod builder;
mod discover;

pub use self::builder::Builder;
pub use self::discover::PoolDiscover;

/// A dynamically sized, load-balanced pool of `Service` instances.
pub struct Pool<MS, Target, Request>
where
    MS: MakeService<Target, Request>,
    MS::MakeError: ::std::error::Error + Send + Sync + 'static,
    MS::Error: ::std::error::Error + Send + Sync + 'static,
    Target: Clone,
{
    balance: P2CBalance<PoolDiscover<MS, Target, Request>>,
    options: Builder,
    ewma: f64,
}

impl<MS, Target, Request> Pool<MS, Target, Request>
where
    MS: MakeService<Target, Request>,
    MS::MakeError: ::std::error::Error + Send + Sync + 'static,
    MS::Error: ::std::error::Error + Send + Sync + 'static,
    MS::Service: Load,
    Target: Clone,
{
    /// Construct a new dynamically sized `Pool`.
    ///
    /// If many calls to `poll_ready` return `NotReady`, `new_service` is used to construct another
    /// `Service` that is then added to the load-balanced pool. If multiple services are available,
    /// `choose` is used to determine which one to use (just as in `Balance`). If many calls to
    /// `poll_ready` succeed, the most recently added `Service` is dropped from the pool.
    pub fn new(make_service: MS, target: Target) -> Self {
        Builder::new().build(make_service, target)
    }
}

// === impl Pool ===

impl<MS, Target, Request> Service<Request> for Pool<MS, Target, Request>
where
    MS: MakeService<Target, Request>,
    MS::MakeError: ::std::error::Error + Send + Sync + 'static,
    MS::Error: ::std::error::Error + Send + Sync + 'static,
    MS::Service: Load,
    Target: Clone,
{
    type Response = <P2CBalance<PoolDiscover<MS, Target, Request>> as Service<Request>>::Response;
    type Error = <P2CBalance<PoolDiscover<MS, Target, Request>> as Service<Request>>::Error;
    type Future = <P2CBalance<PoolDiscover<MS, Target, Request>> as Service<Request>>::Future;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        if let Async::Ready(()) = self.balance.poll_ready()? {
            // services was ready -- there are enough services
            // update ewma with a 0 sample
            self.ewma = (1.0 - self.options.alpha) * self.ewma;

            if self.ewma < self.options.low {
                self.balance.discover.load = Level::Low;

                if self.balance.discover.services > 1 {
                    // reset EWMA so we don't immediately try to remove another service
                    self.ewma = self.options.init;
                }
            } else {
                self.balance.discover.load = Level::Normal;
            }

            Ok(Async::Ready(()))
        } else if self.balance.discover.making.is_none() {
            // no services are ready -- we're overloaded
            // update ewma with a 1 sample
            self.ewma = self.options.alpha + (1.0 - self.options.alpha) * self.ewma;

            if self.ewma > self.options.high {
                self.balance.discover.load = Level::High;

            // don't reset the EWMA -- in theory, poll_ready should now start returning
            // `Ready`, so we won't try to launch another service immediately.
            } else {
                self.balance.discover.load = Level::Normal;
            }

            Ok(Async::NotReady)
        } else {
            // no services are ready, but we're already making another service!
            Ok(Async::NotReady)
        }
    }

    fn call(&mut self, req: Request) -> Self::Future {
        Service::call(&mut self.balance, req)
    }
}
