//! Per-connection session state.
//!
//! A `Session` represents one authenticated client connection. It
//! owns:
//! - The set of instruments the client is subscribed to for market data.
//! - A mapping from `ClientOrderId` -> `OrderId` (assigned by the
//!   sequencer) for translating execution reports back to the
//!   client's own identifiers.
//! - A handle to push `Command`s onto the inbound SPSC ring buffer.
//!
//! `Session` itself does not own the socket — `server.rs` drives the
//! actual I/O and calls into `Session` to interpret/produce frames.

use std::collections::HashMap;

use bytes::{Buf, BufMut, BytesMut};
use core_types::{
    AccountId, CancelOrder, ClientOrderId, Command, Event, InstrumentId, NewOrder, OrderId,
    OrderType, Price, Qty, Side, TimeInForce,
};
use ring_buffer::SpscProducer;

use crate::codec::{Codec, CodecError, Frame};

/// Globally unique identifier for a gateway session (i.e. one TCP
/// connection), assigned at accept time. Not persisted — purely for
/// in-process bookkeeping (logging, market data fanout).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SessionId(pub u64);

/// Wire message type tags. Kept here (rather than in `codec.rs`)
/// because they're specific to the application protocol, not the
/// generic framing layer.
pub mod msg_type {
    pub const NEW_ORDER: u8 = 0x01;
    pub const CANCEL_ORDER: u8 = 0x02;
    pub const SUBSCRIBE_MD: u8 = 0x03;
    pub const UNSUBSCRIBE_MD: u8 = 0x04;

    pub const EXEC_REPORT: u8 = 0x81;
    pub const REJECT: u8 = 0x82;
    pub const MARKET_DATA: u8 = 0x83;
    pub const ERROR: u8 = 0xFF;
}

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("codec error: {0}")]
    Codec(#[from] CodecError),
    #[error("malformed payload for message type {msg_type:#x}: {reason}")]
    MalformedPayload { msg_type: u8, reason: &'static str },
    #[error("inbound command queue full")]
    QueueFull,
}

/// Per-session state for one authenticated client connection.
pub struct Session {
    pub id: SessionId,
    pub account_id: AccountId,

    /// Producer side of the SPSC ring buffer feeding the sequencer.
    /// Each session gets a dedicated lane; the sequencer multiplexes
    /// across all sessions (see `sequencer/sequencer.rs`).
    cmd_producer: SpscProducer<Command, 4096>,

    /// Instruments this session is currently subscribed to for
    /// market data updates.
    pub subscriptions: std::collections::HashSet<InstrumentId>,

    /// Maps client-assigned order IDs to engine-assigned `OrderId`s,
    /// populated as `Event::Accepted` events arrive for this account.
    /// Used to translate later `Event`s back into the client's frame
    /// of reference for execution reports.
    client_order_map: HashMap<ClientOrderId, OrderId>,

    codec: Codec,
}

impl Session {
    pub fn new(
        id: SessionId,
        account_id: AccountId,
        cmd_producer: SpscProducer<Command, 4096>,
    ) -> Self {
        Session {
            id,
            account_id,
            cmd_producer,
            subscriptions: Default::default(),
            client_order_map: HashMap::new(),
            codec: Codec,
        }
    }

    /// Decodes the next complete frame from `buf` (if any) and, if it
    /// represents a command, attempts to push it onto the sequencer's
    /// inbound queue.
    ///
    /// Returns:
    /// - `Ok(Some(SessionAction))` describing a side effect the caller
    ///   (`server.rs`) should perform (e.g. write an immediate reply).
    /// - `Ok(None)` if no complete frame was available, or the frame
    ///   was fully handled with no immediate reply needed.
    /// - `Err(_)` on protocol violation; caller should close the
    ///   connection.
    pub fn handle_inbound(
        &mut self,
        buf: &mut BytesMut,
    ) -> Result<Option<SessionAction>, SessionError> {
        let Some(frame) = self.codec.decode(buf)? else {
            return Ok(None);
        };
        self.dispatch_frame(frame)
    }

    fn dispatch_frame(&mut self, frame: Frame) -> Result<Option<SessionAction>, SessionError> {
        match frame.msg_type {
            msg_type::NEW_ORDER => {
                let cmd = decode_new_order(self.account_id, &frame.payload)?;
                self.enqueue(Command::New(cmd))?;
                Ok(None)
            }
            msg_type::CANCEL_ORDER => {
                let cmd = decode_cancel_order(self.account_id, &frame.payload)?;
                self.enqueue(Command::Cancel(cmd))?;
                Ok(None)
            }
            msg_type::SUBSCRIBE_MD => {
                let instrument_id = decode_instrument_id(&frame.payload)?;
                self.subscriptions.insert(instrument_id);
                Ok(None)
            }
            msg_type::UNSUBSCRIBE_MD => {
                let instrument_id = decode_instrument_id(&frame.payload)?;
                self.subscriptions.remove(&instrument_id);
                Ok(None)
            }
            other => Err(SessionError::MalformedPayload {
                msg_type: other,
                reason: "unknown inbound message type",
            }),
        }
    }

    fn enqueue(&mut self, cmd: Command) -> Result<(), SessionError> {
        self.cmd_producer
            .try_push(cmd)
            .map_err(|_| SessionError::QueueFull)
    }

    /// Records the mapping from a client's order id to the engine's
    /// assigned `OrderId`. Called by `server.rs` when an
    /// `Event::Accepted` for this account is observed.
    pub fn record_order_mapping(&mut self, client_order_id: ClientOrderId, order_id: OrderId) {
        self.client_order_map.insert(client_order_id, order_id);
    }

    /// Looks up the client-facing order id for a given engine
    /// `OrderId`, if known. Used when translating `Event::Filled` /
    /// `Event::Canceled` back to execution reports.
    pub fn client_order_id_for(&self, order_id: OrderId) -> Option<ClientOrderId> {
        self.client_order_map
            .iter()
            .find_map(|(coid, oid)| if *oid == order_id { Some(*coid) } else { None })
    }

    /// Encodes an `Event` relevant to this session into a wire frame,
    /// if applicable. Returns `None` for events this session doesn't
    /// care about (e.g. fills for orders belonging to other accounts,
    /// unless it's market data the session is subscribed to).
    pub fn encode_event(&self, ev: &Event, out: &mut BytesMut) -> Result<bool, CodecError> {
        match ev {
            Event::Accepted { account_id, .. }
            | Event::Canceled { account_id, .. }
            | Event::Rejected { account_id, .. }
            | Event::Modified { account_id, .. }
                if *account_id == self.account_id =>
            {
                encode_exec_report(ev, &self.codec, out)?;
                Ok(true)
            }
            Event::Filled { fill, .. }
                if fill.aggressor_account_id == self.account_id
                    || fill.resting_account_id == self.account_id =>
            {
                encode_exec_report(ev, &self.codec, out)?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}

/// Side effects that `Session::handle_inbound` may request from the
/// connection driver in `server.rs`.
#[derive(Debug)]
pub enum SessionAction {
    /// Write an immediate response frame to the client (e.g. an
    /// error for a malformed-but-recoverable request).
    WriteFrame(BytesMut),
    /// Close the connection (e.g. after a fatal protocol error).
    Close,
}

// --- Payload decoding helpers -------------------------------------------------
//
// Wire format notes: all multi-byte integers are little-endian.
// `NEW_ORDER` payload layout:
//   u64 client_order_id
//   u64 instrument_id
//   u8  side          (0 = Buy, 1 = Sell)
//   u8  order_type    (0 = Limit, 1 = Market)
//   i64 price         (ignored if order_type == Market)
//   u64 qty
//   u8  time_in_force (0 = GTC, 1 = IOC, 2 = FOK)

fn decode_new_order(account_id: AccountId, payload: &[u8]) -> Result<NewOrder, SessionError> {
    const EXPECTED_LEN: usize = 8 + 8 + 1 + 1 + 8 + 8 + 1;
    if payload.len() != EXPECTED_LEN {
        return Err(SessionError::MalformedPayload {
            msg_type: msg_type::NEW_ORDER,
            reason: "unexpected payload length",
        });
    }

    let mut p = payload;
    let client_order_id = ClientOrderId::new(p.get_u64_le());
    let instrument_id = InstrumentId::new(p.get_u64_le());
    let side = match p.get_u8() {
        0 => Side::Buy,
        1 => Side::Sell,
        _ => {
            return Err(SessionError::MalformedPayload {
                msg_type: msg_type::NEW_ORDER,
                reason: "invalid side",
            })
        }
    };
    let order_type_tag = p.get_u8();
    let price_raw = p.get_i64_le();
    let qty = Qty::new(p.get_u64_le());
    let tif = match p.get_u8() {
        0 => TimeInForce::Gtc,
        1 => TimeInForce::Ioc,
        2 => TimeInForce::Fok,
        _ => {
            return Err(SessionError::MalformedPayload {
                msg_type: msg_type::NEW_ORDER,
                reason: "invalid time_in_force",
            })
        }
    };

    let price = Price::new(price_raw);
    let order_type = match order_type_tag {
        0 => OrderType::Limit,
        1 => OrderType::Market,
        _ => {
            return Err(SessionError::MalformedPayload {
                msg_type: msg_type::NEW_ORDER,
                reason: "invalid order_type",
            })
        }
    };

    if qty.is_zero() {
        return Err(SessionError::MalformedPayload {
            msg_type: msg_type::NEW_ORDER,
            reason: "qty must be > 0",
        });
    }

    Ok(NewOrder {
        account_id,
        instrument_id,
        client_order_id,
        side,
        price,
        order_type,
        qty,
        time_in_force: tif,
    })
}

/// `CANCEL_ORDER` payload layout:
///   u64 instrument_id
///   u64 order_id
fn decode_cancel_order(account_id: AccountId, payload: &[u8]) -> Result<CancelOrder, SessionError> {
    const EXPECTED_LEN: usize = 8 + 8;
    if payload.len() != EXPECTED_LEN {
        return Err(SessionError::MalformedPayload {
            msg_type: msg_type::CANCEL_ORDER,
            reason: "unexpected payload length",
        });
    }
    let mut p = payload;
    let instrument_id = InstrumentId::new(p.get_u64_le());
    let order_id = OrderId::new(p.get_u64_le());
    Ok(CancelOrder {
        account_id,
        instrument_id,
        order_id,
    })
}

/// `SUBSCRIBE_MD` / `UNSUBSCRIBE_MD` payload layout:
///   u64 instrument_id
fn decode_instrument_id(payload: &[u8]) -> Result<InstrumentId, SessionError> {
    if payload.len() != 8 {
        return Err(SessionError::MalformedPayload {
            msg_type: msg_type::SUBSCRIBE_MD,
            reason: "unexpected payload length",
        });
    }
    let mut p = payload;
    Ok(InstrumentId::new(p.get_u64_le()))
}

// --- Event encoding -------------------------------------------------------------
//
// `EXEC_REPORT` payload layout (variant-tagged):
//   u8  variant
//   ... variant-specific fields, all little-endian
//
// variant 0: Accepted   { order_id: u64, client_order_id: u64, side: u8, price: i64, qty: u64 }
// variant 1: Filled     { order_id: u64, price: i64, qty: u64, remaining_qty: u64, is_aggressor: u8 }
// variant 2: Canceled   { order_id: u64, reason: u8, remaining_qty: u64 }
// variant 3: Rejected   { client_order_id: u64, reason: u8 }
// variant 4: Modified   { order_id: u64, new_qty: u64, has_new_price: u8, new_price: i64 }

fn encode_exec_report(ev: &Event, codec: &Codec, out: &mut BytesMut) -> Result<(), CodecError> {
    let mut payload = BytesMut::new();

    match ev {
        Event::Accepted {
            order_id,
            client_order_id,
            side,
            price,
            qty,
            ..
        } => {
            payload.put_u8(0);
            payload.put_u64_le(order_id.get());
            payload.put_u64_le(client_order_id.get());
            payload.put_u8(if side.is_buy() { 0 } else { 1 });
            payload.put_i64_le(price.ticks());
            payload.put_u64_le(qty.lots());
        }
        Event::Filled { fill, .. } => {
            payload.put_u8(1);
            // Encode from the perspective of *this* account; the
            // caller (`Session::encode_event`) already verified that
            // this account is either the aggressor or the maker. We
            // pick whichever side matches.
            //
            // Note: if both legs belong to the same account (self-trade),
            // `Session::encode_event` will call this once per leg via
            // the aggressor branch first — callers needing per-leg
            // detail should match on `is_aggressor` downstream.
            payload.put_u64_le(fill.aggressor_order_id.get());
            payload.put_i64_le(fill.price.ticks());
            payload.put_u64_le(fill.qty.lots());
            payload.put_u64_le(fill.aggressor_remaining_qty.lots());
            payload.put_u8(1); // is_aggressor = true for this encoding
        }
        Event::Canceled {
            order_id,
            reason,
            remaining_qty,
            ..
        } => {
            payload.put_u8(2);
            payload.put_u64_le(order_id.get());
            payload.put_u8(cancel_reason_tag(*reason));
            payload.put_u64_le(remaining_qty.lots());
        }
        Event::Rejected {
            client_order_id,
            reason,
            ..
        } => {
            payload.put_u8(3);
            payload.put_u64_le(client_order_id.get());
            payload.put_u8(reject_reason_tag(*reason));
        }
        Event::Modified {
            order_id,
            new_qty,
            new_price,
            ..
        } => {
            payload.put_u8(4);
            payload.put_u64_le(order_id.get());
            payload.put_u64_le(new_qty.lots());
            match new_price {
                Some(p) => {
                    payload.put_u8(1);
                    payload.put_i64_le(p.ticks());
                }
                None => {
                    payload.put_u8(0);
                    payload.put_i64_le(0);
                }
            }
        }
        Event::InstrumentHalted { .. } | Event::InstrumentResumed { .. } => {
            // Not account-scoped; `Session::encode_event` never routes
            // these here. Defensive no-op.
            return Ok(());
        }
    }

    codec.encode(msg_type::EXEC_REPORT, &payload, out)
}

fn cancel_reason_tag(r: core_types::CancelReason) -> u8 {
    use core_types::CancelReason::*;
    match r {
        UserRequested => 0,
        TimeInForceExpired => 1,
        RiskLimitBreach => 2,
        InstrumentHalted => 3,
    }
}

fn reject_reason_tag(r: core_types::RejectReason) -> u8 {
    use core_types::RejectReason::*;
    match r {
        RiskLimitBreach => 0,
        InstrumentHalted => 1,
        InvalidPrice => 2,
        InvalidQuantity => 3,
        UnknownInstrument => 4,
        UnknownOrder => 5,
        InvalidQty => 6,
        PriceOutOfRange => 7,
        IocNoMatch => 8,
        ArenaFull => 9,
        OrderNotFound => 10,
        WrongAccount => 11,
        FokNotFullyFillable => 12,
        UnsupportedOrderType => 13,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;
    use core_types::{AccountId, ClientOrderId, InstrumentId, OrderId, Price, Qty, Side};
    use ring_buffer::spsc;

    fn make_session() -> (Session, ring_buffer::spsc::SpscConsumer<Command, 4096>) {
        let (producer, consumer) = spsc::spsc_queue::<Command, 4096>();
        let session = Session::new(SessionId(1), AccountId::new(42), producer);
        (session, consumer)
    }

    fn encode_new_order_payload(
        coid: u64,
        instr: u64,
        side: u8,
        otype: u8,
        price: i64,
        qty: u64,
        tif: u8,
    ) -> BytesMut {
        let mut b = BytesMut::new();
        b.put_u64_le(coid);
        b.put_u64_le(instr);
        b.put_u8(side);
        b.put_u8(otype);
        b.put_i64_le(price);
        b.put_u64_le(qty);
        b.put_u8(tif);
        b
    }

    #[test]
    fn new_order_roundtrip_enqueues_command() {
        let (mut session, mut consumer) = make_session();
        let codec = Codec;

        let payload = encode_new_order_payload(7, 1, 0, 0, 10_050, 5, 0);
        let mut wire = BytesMut::new();
        codec
            .encode(msg_type::NEW_ORDER, &payload, &mut wire)
            .unwrap();

        let action = session.handle_inbound(&mut wire).unwrap();
        assert!(action.is_none());

        let cmd = consumer.try_pop().expect("command enqueued");
        match cmd {
            Command::New(n) => {
                assert_eq!(n.account_id, AccountId::new(42));
                assert_eq!(n.instrument_id, InstrumentId::new(1));
                assert_eq!(n.client_order_id, ClientOrderId::new(7));
                assert_eq!(n.side, Side::Buy);
                assert_eq!(n.qty, Qty::new(5));
                assert_eq!(n.order_type, core_types::OrderType::Limit);
                assert_eq!(n.price, Price::new(10_050));
            }
            _ => panic!("expected limit order"),
        }
    }

    #[test]
    fn rejects_zero_quantity() {
        let (mut session, _consumer) = make_session();
        let codec = Codec;

        let payload = encode_new_order_payload(1, 1, 0, 0, 100, 0, 0);
        let mut wire = BytesMut::new();
        codec
            .encode(msg_type::NEW_ORDER, &payload, &mut wire)
            .unwrap();

        let err = session.handle_inbound(&mut wire).unwrap_err();
        matches!(err, SessionError::MalformedPayload { .. });
    }

    #[test]
    fn subscribe_and_unsubscribe() {
        let (mut session, _consumer) = make_session();
        let codec = Codec;

        let mut payload = BytesMut::new();
        payload.put_u64_le(99);

        let mut wire = BytesMut::new();
        codec
            .encode(msg_type::SUBSCRIBE_MD, &payload, &mut wire)
            .unwrap();
        session.handle_inbound(&mut wire).unwrap();
        assert!(session.subscriptions.contains(&InstrumentId::new(99)));

        let mut wire2 = BytesMut::new();
        codec
            .encode(msg_type::UNSUBSCRIBE_MD, &payload, &mut wire2)
            .unwrap();
        session.handle_inbound(&mut wire2).unwrap();
        assert!(!session.subscriptions.contains(&InstrumentId::new(99)));
    }

    #[test]
    fn order_mapping_lookup() {
        let (mut session, _consumer) = make_session();
        session.record_order_mapping(ClientOrderId::new(5), OrderId::new(1000));
        assert_eq!(
            session.client_order_id_for(OrderId::new(1000)),
            Some(ClientOrderId::new(5))
        );
        assert_eq!(session.client_order_id_for(OrderId::new(9999)), None);
    }

    #[test]
    fn encode_accepted_event_for_own_account() {
        let (session, _consumer) = make_session();
        let ev = Event::Accepted {
            seq: core_types::SequenceNo::FIRST,
            instrument_id: InstrumentId::new(1),
            order_id: OrderId::new(55),
            account_id: AccountId::new(42), // matches session
            client_order_id: ClientOrderId::new(7),
            side: Side::Buy,
            price: Price::new(100),
            qty: Qty::new(5),
        };
        let mut out = BytesMut::new();
        let written = session.encode_event(&ev, &mut out).unwrap();
        assert!(written);
        assert!(!out.is_empty());
    }

    #[test]
    fn does_not_encode_event_for_other_account() {
        let (session, _consumer) = make_session();
        let ev = Event::Accepted {
            seq: core_types::SequenceNo::FIRST,
            instrument_id: InstrumentId::new(1),
            order_id: OrderId::new(55),
            account_id: AccountId::new(999), // does not match session
            client_order_id: ClientOrderId::new(7),
            side: Side::Buy,
            price: Price::new(100),
            qty: Qty::new(5),
        };
        let mut out = BytesMut::new();
        let written = session.encode_event(&ev, &mut out).unwrap();
        assert!(!written);
        assert!(out.is_empty());
    }
}
