use futures::{try_ready, Async, Poll};
use tower_discover::{Change, Discover};

/// A weight on [0.0, ∞].
///
/// Lesser-weighted nodes receive less traffic than heavier-weighted nodes.
#[derive(Copy, Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct Weight(pub usize);

/// A Service, that implements Load, that
#[derive(Copy, Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct Weighted<T> {
    inner: T,
    weight: Weight,
}

// === impl Weighted ===

impl<T> Weighted<T> {
    pub fn new<W: Into<Weight>>(inner: T, w: W) -> Self {
        let weight = w.into();
        Self { inner, weight }
    }

    pub fn weight(&self) -> Weight {
        self.weight
    }

    pub fn into_parts(self) -> (T, Weight) {
        let Self { inner, weight } = self;
        (inner, weight)
    }
}

impl<T> std::convert::AsRef<T> for Weighted<T> {
    fn as_ref(&self) -> &T {
        &self.inner
    }
}

impl<T> std::convert::AsMut<T> for Weighted<T> {
    fn as_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<D: Discover> Discover for Weighted<D> {
    type Key = Weighted<D::Key>;
    type Error = D::Error;
    type Service = D::Service;

    fn poll(&mut self) -> Poll<Change<Self::Key, Self::Service>, Self::Error> {
        let c = match try_ready!(self.inner.poll()) {
            Change::Remove(k) => Change::Remove(Weighted::new(k, self.weight)),
            Change::Insert(k, svc) => Change::Insert(Weighted::new(k, self.weight), svc),
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
