#![cfg_attr(not(feature = "dpdk"), allow(unused))]

#[cfg(feature = "dpdk")]
pub mod eal;
#[cfg(feature = "dpdk")]
pub mod mempool;
#[cfg(feature = "dpdk")]
pub mod port;
#[cfg(feature = "dpdk")]
pub mod rx;

#[cfg(feature = "dpdk")]
pub use eal::Eal;
#[cfg(feature = "dpdk")]
pub use mempool::MbufPool;
#[cfg(feature = "dpdk")]
pub use port::{init_port, PortConfig};
#[cfg(feature = "dpdk")]
pub use rx::RxWorker;
