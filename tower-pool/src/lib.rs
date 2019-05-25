#![doc(html_root_url = "https://docs.rs/tower-pool/0.1.0")]
#![deny(rust_2018_idioms)]
#![allow(elided_lifetimes_in_paths)]

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

use futures::{try_ready, Async, Future, Poll};
use tower_discover::{Change, Discover};
use tower_service::Service;
use tower_util::MakeService;

mod builder;
mod discover;

pub use self::builder::Builder;
pub use self::discover::PoolDiscover;

type Error = Box<dyn std::error::Error + Send + Sync>;

/// A dynamically sized, load-balanced pool of `Service` instances.
pub struct Pool<S> {
    inner: S,
    options: Options,
    ewma: f64,
}

#[derive(Copy, Clone, Debug)]
struct Options {
    low: f64,
    high: f64,
    init: f64,
    alpha: f64,
}

// === impl Options ===

impl Default for Options {
    fn default() -> Self {
        Self {
            init: 0.1,
            low: 0.00001,
            high: 0.2,
            alpha: 0.03,
        }
    }
}

// === impl Pool ===

// impl<S, Request> Pool<S, Request>
// where
//     S: Service<Request>,
//     S::Error: Into<Error>,
// {
//     /// Construct a new dynamically sized `Pool`.
//     ///
//     /// If many calls to `poll_ready` return `NotReady`, `new_service` is used to construct another
//     /// `Service` that is then added to the load-balanced pool. If multiple services are available,
//     /// `choose` is used to determine which one to use (just as in `Balance`). If many calls to
//     /// `poll_ready` succeed, the most recently added `Service` is dropped from the pool.
//     pub fn new(inner: S, target: Target) -> Self {
//         Builder::new().build(make_service, target)
//     }
// }

impl<S, Request> Service<Request> for Pool<S>
where
    S: Service<Request>,
    S::Error: Into<Error>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        if let Async::Ready(()) = self.inner.poll_ready()? {
            // services was ready -- there are enough services
            // update ewma with a 0 sample
            self.ewma = (1.0 - self.options.alpha) * self.ewma;

            if self.ewma < self.options.low {
                self.inner.discover.load = Level::Low;

                if self.inner.discover.services > 1 {
                    // reset EWMA so we don't immediately try to remove another service
                    self.ewma = self.options.init;
                }
            } else {
                self.inner.discover.load = Level::Normal;
            }

            Ok(Async::Ready(()))
        } else if self.inner.discover.making.is_none() {
            // no services are ready -- we're overloaded
            // update ewma with a 1 sample
            self.ewma = self.options.alpha + (1.0 - self.options.alpha) * self.ewma;

            if self.ewma > self.options.high {
                self.inner.discover.load = Level::High;

            // don't reset the EWMA -- in theory, poll_ready should now start returning
            // `Ready`, so we won't try to launch another service immediately.
            } else {
                self.inner.discover.load = Level::Normal;
            }

            Ok(Async::NotReady)
        } else {
            // no services are ready, but we're already making another service!
            Ok(Async::NotReady)
        }
    }

    fn call(&mut self, req: Request) -> Self::Future {
        self.inner.call(req)
    }
}
