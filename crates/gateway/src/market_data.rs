// Market data feed publisher
//! Market data fanout.
//!
//! The matching engine emits `Event`s on a single output stream.
//! `MarketDataHub` consumes a feed of these events (or a derived
//! `MarketDataEvent` stream from `order-book` snapshots/diffs) and
//! fans them out to subscribed sessions via per-session `tokio::sync`
//! channels.
//!
//! This module is deliberately decoupled from `session.rs`'s framing:
//! it produces `MarketDataEvent`s, which `server.rs` then encodes
//! using `Codec` before writing to each subscriber's socket.

use std::collections::HashMap;
use std::sync::Arc;

use core_types::{InstrumentId, Price, Qty, Side};
use tokio::sync::{broadcast, RwLock};

use crate::session::SessionId;

/// A market data update for a single instrument.
///
/// This is intentionally coarse-grained — full top-of-book + last
/// trade — rather than incremental diffs, to keep the gateway's
/// fanout logic simple. `order-book` is the source of truth for full
/// depth; richer diff-based feeds can be layered on later by adding
/// variants here.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MarketDataEvent {
    /// Best bid/ask changed.
    TopOfBook {
        instrument_id: InstrumentId,
        best_bid: Option<(Price, Qty)>,
        best_ask: Option<(Price, Qty)>,
    },
    /// A trade printed.
    Trade {
        instrument_id: InstrumentId,
        price: Price,
        qty: Qty,
        aggressor_side: Side,
    },
    /// Instrument halt state changed.
    HaltStatus { instrument_id: InstrumentId, halted: bool },
}

impl MarketDataEvent {
    pub fn instrument_id(&self) -> InstrumentId {
        match self {
            MarketDataEvent::TopOfBook { instrument_id, .. }
            | MarketDataEvent::Trade { instrument_id, .. }
            | MarketDataEvent::HaltStatus { instrument_id, .. } => *instrument_id,
        }
    }
}

/// Per-instrument broadcast capacity. If a subscriber falls behind by
/// more than this many updates, it will receive
/// `broadcast::error::RecvError::Lagged` and should resync via a
/// fresh snapshot (not implemented here — see `order-book` for
/// snapshot retrieval).
const CHANNEL_CAPACITY: usize = 1024;

/// Central hub for market data distribution.
///
/// One `broadcast` channel per instrument, created lazily on first
/// subscription. `server.rs` calls `subscribe` when a session sends
/// `SUBSCRIBE_MD`, and spawns a task per (session, instrument) pair
/// that forwards received `MarketDataEvent`s to the session's write
/// half.
pub struct MarketDataHub {
    channels: RwLock<HashMap<InstrumentId, broadcast::Sender<MarketDataEvent>>>,
}

impl MarketDataHub {
    pub fn new() -> Arc<Self> {
        Arc::new(MarketDataHub { channels: RwLock::new(HashMap::new()) })
    }

    /// Publishes a market data event to all subscribers of its
    /// instrument. If there are no subscribers (no channel exists or
    /// it has zero receivers), this is a cheap no-op.
    pub async fn publish(&self, event: MarketDataEvent) {
        let instrument_id = event.instrument_id();

        // Fast path: read lock to check for an existing channel.
        {
            let channels = self.channels.read().await;
            if let Some(tx) = channels.get(&instrument_id) {
                // Ignore send errors (no receivers) — that's fine.
                let _ = tx.send(event);
                return;
            }
        }

        // Slow path: no channel yet. Only create one if we expect
        // subscribers; since `subscribe` always creates the channel
        // first, reaching here means there are currently no
        // subscribers at all, so we can simply drop the event.
        let _ = instrument_id; // event dropped intentionally
    }

    /// Subscribes to market data for `instrument_id`, creating the
    /// underlying broadcast channel if it doesn't exist yet. Returns
    /// a `broadcast::Receiver` that `server.rs` should poll in a task
    /// dedicated to this `(session, instrument)` pair.
    pub async fn subscribe(&self, instrument_id: InstrumentId) -> broadcast::Receiver<MarketDataEvent> {
        // Fast path.
        {
            let channels = self.channels.read().await;
            if let Some(tx) = channels.get(&instrument_id) {
                return tx.subscribe();
            }
        }

        // Slow path: create the channel.
        let mut channels = self.channels.write().await;
        let tx = channels
            .entry(instrument_id)
            .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0);
        tx.subscribe()
    }

    /// Returns the number of instruments with at least one active
    /// broadcast channel (used for metrics/diagnostics).
    pub async fn active_instrument_count(&self) -> usize {
        self.channels.read().await.len()
    }
}

/// Tracks which sessions are subscribed to which instruments, purely
/// for diagnostics/admin purposes (e.g. reporting subscriber counts).
/// The actual data flow uses `broadcast` channels above, which don't
/// require knowing individual subscriber identities.
#[derive(Default)]
pub struct SubscriptionRegistry {
    by_instrument: HashMap<InstrumentId, Vec<SessionId>>,
}

impl SubscriptionRegistry {
    pub fn add(&mut self, instrument_id: InstrumentId, session_id: SessionId) {
        let entry = self.by_instrument.entry(instrument_id).or_default();
        if !entry.contains(&session_id) {
            entry.push(session_id);
        }
    }

    pub fn remove(&mut self, instrument_id: InstrumentId, session_id: SessionId) {
        if let Some(entry) = self.by_instrument.get_mut(&instrument_id) {
            entry.retain(|s| *s != session_id);
            if entry.is_empty() {
                self.by_instrument.remove(&instrument_id);
            }
        }
    }

    pub fn subscriber_count(&self, instrument_id: InstrumentId) -> usize {
        self.by_instrument.get(&instrument_id).map(|v| v.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::{InstrumentId, Price, Qty, Side};

    #[tokio::test]
    async fn publish_with_no_subscribers_is_noop() {
        let hub = MarketDataHub::new();
        hub.publish(MarketDataEvent::Trade {
            instrument_id: InstrumentId::new(1),
            price: Price::new(100),
            qty: Qty::new(1),
            aggressor_side: Side::Buy,
        }).await;
        assert_eq!(hub.active_instrument_count().await, 0);
    }

    #[tokio::test]
    async fn subscribe_then_publish_delivers_event() {
        let hub = MarketDataHub::new();
        let instrument_id = InstrumentId::new(1);
        let mut rx = hub.subscribe(instrument_id).await;

        hub.publish(MarketDataEvent::TopOfBook {
            instrument_id,
            best_bid: Some((Price::new(99), Qty::new(10))),
            best_ask: Some((Price::new(101), Qty::new(5))),
        }).await;

        let ev = rx.recv().await.unwrap();
        match ev {
            MarketDataEvent::TopOfBook { best_bid, best_ask, .. } => {
                assert_eq!(best_bid, Some((Price::new(99), Qty::new(10))));
                assert_eq!(best_ask, Some((Price::new(101), Qty::new(5))));
            }
            _ => panic!("expected TopOfBook"),
        }
    }

    #[test]
    fn subscription_registry_basic() {
        let mut reg = SubscriptionRegistry::default();
        let instr = InstrumentId::new(1);
        reg.add(instr, SessionId(1));
        reg.add(instr, SessionId(2));
        assert_eq!(reg.subscriber_count(instr), 2);

        reg.remove(instr, SessionId(1));
        assert_eq!(reg.subscriber_count(instr), 1);

        reg.remove(instr, SessionId(2));
        assert_eq!(reg.subscriber_count(instr), 0);
    }
}