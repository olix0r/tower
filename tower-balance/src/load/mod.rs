mod constant;
mod instrument;
pub mod peak_ewma;
pub mod pending_requests;

pub use self::{
    constant::Constant,
    instrument::{Instrument, InstrumentFuture, NoInstrument},
    peak_ewma::{PeakEwma, WithPeakEwma},
    pending_requests::{PendingRequests, WithPendingRequests},
};

/// Exposes a load metric.
///
/// Implementors should choose load values so that lesser-loaded instances return lesser
/// values than higher-load instances.
pub trait Load {
    type Metric: PartialOrd + std::fmt::Debug;

    fn load(&self) -> Self::Metric;
}
