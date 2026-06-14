//! TCP gateway for the matching/risk engine.
//!
//! Responsibilities:
//! - Accept client TCP connections (`server.rs`).
//! - Per-connection session state and auth (`session.rs`).
//! - Binary wire protocol encode/decode (`codec.rs`).
//! - Fan out market data (book updates, trades) to subscribed
//!   sessions (`market_data.rs`).
//!
//! The gateway is intentionally "dumb": it does not validate business
//! rules (that's `risk-engine`/`matching-engine`). Its job is framing,
//! auth, backpressure, and pushing well-formed `Command`s onto the
//! SPSC ring buffer that feeds the sequencer.

pub mod codec;
pub mod market_data;
pub mod server;
pub mod session;

pub use codec::{Codec, CodecError, Frame};
pub use market_data::{MarketDataEvent, MarketDataHub};
pub use server::{GatewayServer, GatewayConfig};
pub use session::{Session, SessionId};