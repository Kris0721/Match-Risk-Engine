use core_types::events::{EngineEvent, Event, Fill};
use core_types::ids::{InstrumentId, SequenceNo};

pub fn map_engine_event(ev: EngineEvent) -> Option<Event> {
    match ev {
        EngineEvent::Trade { seq, symbol, price, qty, maker_order, taker_order, maker_acct, taker_acct, maker_side, maker_remaining_qty, taker_remaining_qty, .. } => {
            let seq_no = SequenceNo::new(seq).unwrap_or(SequenceNo::FIRST);
            let fill = Fill {
            instrument_id: InstrumentId(symbol.0.into()),
                aggressor_order_id: taker_order,
                aggressor_account_id: taker_acct,
                aggressor_side: maker_side.opposite(),
                resting_order_id: maker_order,
                resting_account_id: maker_acct,
                price,
                qty,
                resting_remaining_qty: maker_remaining_qty,
                aggressor_remaining_qty: taker_remaining_qty,
            };
            Some(Event::Filled { seq: seq_no, fill })
        }
        EngineEvent::Accepted { seq, order_id, symbol, account_id, client_order_id, side, price, qty, .. } => {
            let seq_no = SequenceNo::new(seq).unwrap_or(SequenceNo::FIRST);
            Some(Event::Accepted { seq: seq_no, instrument_id: InstrumentId(symbol.0.into()), order_id, account_id, client_order_id, side, price: price, qty })
        }
        EngineEvent::Rejected { seq, account_id, client_order_id, reason, .. } => {
            let seq_no = SequenceNo::new(seq).unwrap_or(SequenceNo::FIRST);
            Some(Event::Rejected { seq: seq_no, account_id, client_order_id, reason })
        }
        EngineEvent::OrderCancelled { order_id, account_id, seq, symbol } => {
            let seq_no = SequenceNo::new(seq).unwrap_or(SequenceNo::FIRST);
            Some(Event::Canceled { seq: seq_no, instrument_id: InstrumentId(symbol.0.into()), order_id, account_id, reason: core_types::events::CancelReason::UserRequested, remaining_qty: core_types::Qty::from_raw(0) })
        }
        _ => None
    }
}