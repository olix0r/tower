use crate::{
    error::{Error, SpawnError},
    future::{BackgroundReady, BackgroundReadyExecutor},
};

use futures::{future, sync::oneshot, try_ready, Async, Future, Poll};
use std::marker::PhantomData;
use tokio_executor::DefaultExecutor;
use tower_service::Service;

/// Adds a buffer in front of an inner service.
///
/// See crate level documentation for more details.
pub struct SpawnReady<T, Request, E>
where
    T: Service<Request>,
    T::Error: Into<Error>,
    E: BackgroundReadyExecutor<T, Request>,
{
    executor: E,
    inner: Inner<T>,
    _marker: PhantomData<fn(Request)>,
}

enum Inner<T> {
    Service(Option<T>),
    Future(oneshot::Receiver<Result<T, Error>>),
}

impl<T, Request> SpawnReady<T, Request, DefaultExecutor>
where
    T: Service<Request> + Send + 'static,
    T::Error: Into<Error>,
    Request: 'static,
{
    /// Creates a new `SpawnReady` wrapping `service`.
    ///
    /// The default Tokio executor is used to drive service readiness, which
    /// means that this method must be called while on the Tokio runtime.
    pub fn new(service: T) -> Self {
        Self::with_executor(service, DefaultExecutor::current())
    }
}

impl<T, Request, E> SpawnReady<T, Request, E>
where
    T: Service<Request> + Send,
    T::Error: Into<Error>,
    E: BackgroundReadyExecutor<T, Request>,
{
    /// Creates a new `SpawnReady` wrapping `service`.
    ///
    /// `executor` is used to spawn a new `BackgroundReady` task that is
    /// dedicated to draining the buffer and dispatching the requests to the
    /// internal service.
    ///
    /// `bound` gives the maximal number of requests that can be queued for the service before
    /// backpressure is applied to callers.
    pub fn with_executor(service: T, executor: E) -> Self {
        Self {
            executor,
            inner: Inner::Service(Some(service)),
            _marker: PhantomData,
        }
    }
}

impl<T, Request, E> Service<Request> for SpawnReady<T, Request, E>
where
    T: Service<Request> + Send,
    T::Error: Into<Error>,
    E: BackgroundReadyExecutor<T, Request>,
{
    type Response = T::Response;
    type Error = Error;
    type Future = future::MapErr<T::Future, fn(T::Error) -> Error>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        loop {
            self.inner = match self.inner {
                Inner::Service(ref mut svc) => {
                    if svc
                        .as_mut()
                        .expect("illegal state")
                        .poll_ready()
                        .map_err(Into::into)?
                        .is_ready()
                    {
                        return Ok(Async::Ready(()));
                    }

                    let (bg, rx) = BackgroundReady::new(svc.take().expect("illegal state"));
                    self.executor.spawn(bg).map_err(|_| SpawnError::new())?;

                    Inner::Future(rx)
                }
                Inner::Future(ref mut fut) => match try_ready!(fut.poll()) {
                    Ok(svc) => Inner::Service(Some(svc)),
                    Err(e) => return Err(e),
                },
            }
        }
    }

    fn call(&mut self, request: Request) -> Self::Future {
        match self.inner {
            Inner::Service(Some(ref mut svc)) => svc.call(request).map_err(Into::into),
            _ => unreachable!("poll_ready must be called"),
        }
    }
}