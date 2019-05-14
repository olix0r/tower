use futures::{try_ready, Async, Poll};
use log::trace;
use std::marker::PhantomData;
use std::{fmt, hash::Hash, ops};
use tower_discover::{Change, Discover};
use tower_service::Service;

use crate::Load;

/// A weight on [0.0, ∞].
///
/// Lesser-weighted nodes receive less traffic than heavier-weighted nodes.
#[derive(Copy, Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct Weight(usize);

/// A Service, that implements Load, that
#[derive(Copy, Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct Weighted<T> {
    inner: T,
    weight: Weight,
}

#[derive(Debug)]
pub struct WithWeighted<T, K> {
    inner: T,
    _marker: PhantomData<K>,
}

pub trait HasWeight {
    fn weight(&self) -> Weight;
}

// === impl Weighted ===

impl<T: HasWeight> From<T> for Weighted<T> {
    fn from(inner: T) -> Self {
        let weight = inner.weight();
        Self { inner, weight }
    }
}

impl<T> HasWeight for Weighted<T> {
    fn weight(&self) -> Weight {
        self.weight
    }
}

impl<T> Weighted<T> {
    pub fn new<W: Into<Weight>>(inner: T, w: W) -> Self {
        let weight = w.into();
        Self { inner, weight }
    }

    pub fn into_parts(self) -> (T, Weight) {
        let Self { inner, weight } = self;
        (inner, weight)
    }
}

impl<L> Load for Weighted<L>
where
    L: Load,
    L::Metric: ops::Div<Weight> + fmt::Debug + Copy,
    <L::Metric as ops::Div<Weight>>::Output: PartialOrd + fmt::Debug,
{
    type Metric = <L::Metric as ops::Div<Weight>>::Output;

    fn load(&self) -> Self::Metric {
        let load = self.inner.load();
        let v = load / self.weight;
        trace!("load={:?}; weight={:?} => {:?}", load, self.weight, v);
        v
    }
}

impl<R, S: Service<R>> Service<R> for Weighted<S> {
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        self.inner.poll_ready()
    }

    fn call(&mut self, req: R) -> Self::Future {
        self.inner.call(req)
    }
}

// === impl WithWeighted ===

impl<D, K> From<D> for WithWeighted<D, K>
where
    D: Discover<Key = Weighted<K>>,
    K: Hash + Eq,
{
    fn from(inner: D) -> Self {
        WithWeighted {
            inner,
            _marker: PhantomData,
        }
    }
}

impl<D, K> Discover for WithWeighted<D, K>
where
    D: Discover<Key = Weighted<K>>,
    K: Hash + Eq,
{
    type Key = K;
    type Error = D::Error;
    type Service = Weighted<D::Service>;

    fn poll(&mut self) -> Poll<Change<K, Self::Service>, Self::Error> {
        let c = match try_ready!(self.inner.poll()) {
            Change::Remove(k) => Change::Remove(k.inner),
            Change::Insert(k, svc) => {
                let (inner, weight) = k.into_parts();
                Change::Insert(inner, Weighted::new(svc, weight))
            }
        };

        Ok(Async::Ready(c))
    }
}

// === impl Weight ===

impl Weight {
    pub const ZERO: Weight = Weight(0);
    pub const MIN: Weight = Weight(1);
    pub const UNIT: Weight = Weight(10_000);
    pub const MAX: Weight = Weight(std::usize::MAX);
}

impl Default for Weight {
    fn default() -> Self {
        Weight::UNIT
    }
}

impl Into<usize> for Weight {
    fn into(self) -> usize {
        self.0
    }
}

impl ops::Add<Weight> for Weight {
    type Output = Weight;

    fn add(self, w: Weight) -> Weight {
        Weight(self.0.saturating_add(w.0))
    }
}

#[test]
fn into() {
    assert_eq!(Weight::from(std::f64::INFINITY), Weight::MAX);
    assert_eq!(Weight::from(std::f64::NAN), Weight::ZERO);
    assert_eq!(Weight::from(0.0), Weight::ZERO);
    assert_eq!(Weight::from(1.0), Weight::UNIT);
    assert_eq!(Weight::from(0.1), Weight(1_000));
    assert_eq!(Weight::from(0.01), Weight(100));
    assert_eq!(Weight::from(0.001), Weight(10));
    assert_eq!(Weight::from(0.0001), Weight::MIN);
    assert_eq!(Weight::from(0.00001), Weight::MIN);
}

#[test]
fn div_min() {
    assert_eq!(10.0 / Weight::ZERO, ::std::f64::INFINITY);
    assert_eq!(10 / Weight::ZERO, ::std::f64::INFINITY);
    assert_eq!(0 / Weight::ZERO, ::std::f64::INFINITY);
}
