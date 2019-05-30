use crate::MakeP2CBalance;
use rand::{rngs::SmallRng, FromEntropy, Rng, SeedableRng};
use std::{fmt, marker::PhantomData};
use tower_layer::Layer;

/// Efficiently distributes requests across an arbitrary number of services
#[derive(Clone)]
pub struct P2CBalanceLayer<D> {
    rng: SmallRng,
    _marker: PhantomData<fn(D)>,
}

impl<D> P2CBalanceLayer<D> {
    /// Builds a balancer using the system entropy.
    pub fn new() -> Self {
        Self {
            rng: SmallRng::from_entropy(),
            _marker: PhantomData,
        }
    }

    /// Builds a balancer from the provided RNG.
    ///
    /// This may be preferrable when many balancers are initialized.
    pub fn from_rng<R: Rng>(rng: &mut R) -> Result<Self, rand::Error> {
        let rng = SmallRng::from_rng(rng)?;
        Ok(Self {
            rng,
            _marker: PhantomData,
        })
    }
}

impl<S> Layer<S> for P2CBalanceLayer<S> {
    type Service = MakeP2CBalance<S>;

    fn layer(&self, make_discover: S) -> Self::Service {
        MakeP2CBalance::new(make_discover, self.rng.clone())
    }
}

impl<D> fmt::Debug for P2CBalanceLayer<D> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("P2CBalanceLayer")
            .field("rng", &self.rng)
            .finish()
    }
}
