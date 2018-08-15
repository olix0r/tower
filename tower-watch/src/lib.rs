#[macro_use]
extern crate futures;
extern crate futures_watch;
extern crate tower_service;

use std::{error, fmt};

use futures::{Async, Future, Poll, Stream};
use futures_watch::{Watch, WatchError};
use tower_service::Service;

/// Binds new instances of a Service with a borrowed reference to the watched value.
pub trait Bind<T> {
    type Service: Service;

    fn bind(&mut self, t: &T) -> Self::Service;
}

/// A Service that re-binds an inner Service each time a Watch is notified.
///
// This can be used to reconfigure Services from a shared or otherwise
// externally-controlled configuration source (for instance, a file system).
#[derive(Debug)]
pub struct WatchService<T, B: Bind<T>> {
    watch: Watch<T>,
    bind: B,
    inner: B::Service,
}

#[derive(Debug)]
pub struct Error<E> {
    kind: ErrorKind<E>,
}

// We can't generate this using `kind_error!`, since one of the variants
// is a concrete type rather than a type parameter.
#[derive(Debug)]
enum ErrorKind<E> {
    Inner(E),
    Watch(WatchError),
}

#[derive(Debug)]
pub struct ResponseFuture<F>(F);

// ==== impl WatchService ====

impl<T, B: Bind<T>> WatchService<T, B> {
    /// Creates a new WatchService, bound from the initial value of `watch`.
    pub fn new(watch: Watch<T>, mut bind: B) -> WatchService<T, B> {
        let inner = bind.bind(&*watch.borrow());
        WatchService { watch, bind, inner }
    }

    /// Checks to see if the watch has been updated and, if so, bind the service.
    fn poll_rebind(&mut self) -> Poll<(), WatchError> {
        if try_ready!(self.watch.poll()).is_some() {
            let t = self.watch.borrow();
            self.inner = self.bind.bind(&*t);
            Ok(().into())
        } else {
            // Will never be notified.
            Ok(Async::NotReady)
        }
    }
}

impl<T, B: Bind<T>> Service for WatchService<T, B> {
    type Request = <B::Service as Service>::Request;
    type Response = <B::Service as Service>::Response;
    type Error = Error<<B::Service as Service>::Error>;
    type Future = ResponseFuture<<B::Service as Service>::Future>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        let _ = self.poll_rebind().map_err(ErrorKind::Watch)?;
        self.inner.poll_ready().map_err(|e| ErrorKind::Inner(e).into())
    }

    fn call(&mut self, req: Self::Request) -> Self::Future {
        ResponseFuture(self.inner.call(req))
    }
}

// ==== impl Bind<T> ====

impl<T, S, F> Bind<T> for F
where
    S: Service,
    for<'t> F: FnMut(&'t T) -> S,
{
    type Service = S;

    fn bind(&mut self, t: &T) -> S {
        (self)(t)
    }
}

// ==== impl ResponseFuture ====

impl<F: Future> Future for ResponseFuture<F> {
    type Item = F::Item;
    type Error = Error<F::Error>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll().map_err(|e| ErrorKind::Inner(e).into())
    }
}

// ==== impl Error ====

impl<E> fmt::Display for Error<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.kind {
            ErrorKind::Inner(ref e) => fmt::Display::fmt(e, f),
            ErrorKind::Watch(ref e) => write!(f, "watch error: {:?}", e),
        }
    }
}

impl<E> error::Error for Error<E>
where
    E: fmt::Display,
    E: error::Error,
{
    fn cause(&self) -> Option<&error::Error> {
        match self.kind {
            ErrorKind::Inner(ref e) => e.cause().or(Some(e)),
            ErrorKind::Watch(_) => None,
        }
    }
}

impl<E> From<ErrorKind<E>> for Error<E> {
    fn from(kind: ErrorKind<E>) -> Self {
        Self { kind }
    }
}

// ==== mod tests ====

#[cfg(test)]
mod tests {
    extern crate tokio;

    use futures::future;
    use super::*;

    #[test]
    fn rebind() {
        struct Svc(usize);
        impl Service for Svc {
            type Request = ();
            type Response = usize;
            type Error = ();
            type Future = future::FutureResult<usize, ()>;
            fn poll_ready(&mut self) -> Poll<(), Self::Error> {
                Ok(().into())
            }
            fn call(&mut self, _: ()) -> Self::Future {
                future::ok(self.0)
            }
        }

        let mut rt = tokio::runtime::current_thread::Runtime::new().unwrap();
        macro_rules! assert_ready {
            ($svc:expr) => {{
                let f = future::lazy(|| future::result($svc.poll_ready()));
                assert!(rt.block_on(f).expect("ready").is_ready(), "ready")
            }};
        }
        macro_rules! assert_call {
            ($svc:expr, $expect:expr) => {{
                let f = rt.block_on($svc.call(()));
                assert_eq!(f.expect("call"), $expect, "call")
            }};
        }

        let (watch, mut store) = Watch::new(1);
        let mut svc = WatchService::new(watch, |n: &usize| Svc(*n));

        assert_ready!(svc);
        assert_call!(svc, 1);

        assert_ready!(svc);
        assert_call!(svc, 1);

        store.store(2).expect("store");
        assert_ready!(svc);
        assert_call!(svc, 2);

        store.store(3).expect("store");
        store.store(4).expect("store");
        assert_ready!(svc);
        assert_call!(svc, 4);

        drop(store);
        assert_ready!(svc);
        assert_call!(svc, 4);
    }
}
