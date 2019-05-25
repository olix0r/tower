use super::{Pool, PoolDiscover};
use crate::{Load, P2CBalance};
use tower_util::MakeService;

/// A [builder] that lets you configure how a [`Pool`] determines whether the underlying service is
/// loaded or not. See the [module-level documentation](index.html) and the builder's methods for
/// details.
///
///  [builder]: https://rust-lang-nursery.github.io/api-guidelines/type-safety.html#builders-enable-construction-of-complex-values-c-builder
#[derive(Copy, Clone, Debug, Default)]
pub struct Builder(super::Options);

impl Builder {
    /// Create a new builder with default values for all load settings.
    ///
    /// If you just want to use the defaults, you can just use [`Pool::new`].
    pub fn new() -> Self {
        Self::default()
    }

    /// When the estimated load (see the [module-level docs](index.html)) drops below this
    /// threshold, and there are at least two services active, a service is removed.
    ///
    /// The default value is 0.01. That is, when one in every 100 `poll_ready` calls return
    /// `NotReady`, then the underlying service is considered underutilized.
    pub fn underutilized_below(&mut self, low: f64) -> &mut Self {
        self.0.low = low;
        self
    }

    /// When the estimated load (see the [module-level docs](index.html)) exceeds this
    /// threshold, and no service is currently in the process of being added, a new service is
    /// scheduled to be added to the underlying [`P2CBalance`].
    ///
    /// The default value is 0.5. That is, when every other call to `poll_ready` returns
    /// `NotReady`, then the underlying service is considered highly loaded.
    pub fn loaded_above(&mut self, high: f64) -> &mut Self {
        self.0.high = high;
        self
    }

    /// The initial estimated load average.
    ///
    /// This is also the value that the estimated load will be reset to whenever a service is added
    /// or removed.
    ///
    /// The default value is 0.1.
    pub fn initial(&mut self, init: f64) -> &mut Self {
        self.0.init = init;
        self
    }

    /// How aggressively the estimated load average is updated.
    ///
    /// This is the α parameter of the formula for the [exponential moving
    /// average](https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average), and
    /// dictates how quickly new samples of the current load affect the estimated load. If the
    /// value is closer to 1, newer samples affect the load average a lot (when α is 1, the load
    /// average is immediately set to the current load). If the value is closer to 0, newer samples
    /// affect the load average very little at a time.
    ///
    /// The given value is clamped to `[0,1]`.
    ///
    /// The default value is 0.05, meaning, in very approximate terms, that each new load sample
    /// affects the estimated load by 5%.
    pub fn urgency(&mut self, alpha: f64) -> &mut Self {
        self.0.alpha = alpha.max(0.0).min(1.0);
        self
    }

    pub fn build(self) -> () {}
}
