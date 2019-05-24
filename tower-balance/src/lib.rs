#![doc(html_root_url = "https://docs.rs/tower-balance/0.1.0")]
#![deny(rust_2018_idioms)]
#![allow(elided_lifetimes_in_paths)]

pub mod error;
pub mod future;
pub mod load;
mod p2c;
//pub mod pool;

#[cfg(test)]
mod test;

pub use self::{
    load::Load,
    p2c::P2CBalance,
    //pool::Pool,
};
