// TCP / WebSocket connection listener server
//! TCP server: accepts connections, drives per-connection I/O loops,
//! and wires `Session`s to the inbound command queue and outbound
//! event/market-data streams.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashSet;

use bytes::BytesMut;
use core_types::{AccountId, Command, Event};
use ring_buffer::SpscProducer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};


use crate::codec::Codec;
use crate::market_data::MarketDataHub;
use crate::session::{msg_type, Session, SessionAction, SessionId};

/// Gateway configuration.
#[derive(Debug, Clone)]
pub struct GatewayConfig {
    pub bind_addr: SocketAddr,
    /// Capacity of each session's inbound SPSC command queue.
    pub inbound_queue_capacity: usize,
    /// Initial size of the per-connection read buffer.
    pub read_buf_capacity: usize,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        GatewayConfig {
            bind_addr: "127.0.0.1:7000".parse().unwrap(),
            inbound_queue_capacity: 4096,
            read_buf_capacity: 8 * 1024,
        }
    }
}

/// The gateway server. Owns the listener and shared market-data hub;
/// spawns one task per accepted connection.
///
/// `cmd_producer_factory` is a closure that, given a `SessionId`,
/// returns a fresh `Producer<Command>` for that session's lane into
/// the sequencer. In production this would be backed by per-session
/// SPSC ring buffers registered with `sequencer::Sequencer`; tests can
/// supply an in-memory factory.
pub struct GatewayServer<F>
where
    F: Fn(SessionId) -> SpscProducer<Command, 4096> + Send + Sync + 'static,
{
    config: GatewayConfig,
    market_data: Arc<MarketDataHub>,
    cmd_producer_factory: Arc<F>,
    next_session_id: AtomicU64,
}

impl<F> GatewayServer<F>
where
    F: Fn(SessionId) -> SpscProducer<Command, 4096> + Send + Sync + 'static,
{
    pub fn new(config: GatewayConfig, market_data: Arc<MarketDataHub>, cmd_producer_factory: F) -> Self {
        GatewayServer {
            config,
            market_data,
            cmd_producer_factory: Arc::new(cmd_producer_factory),
            next_session_id: AtomicU64::new(1),
        }
    }

    /// Binds the listener and runs the accept loop forever (or until
    /// an I/O error occurs on the listener itself, which is treated
    /// as fatal).
    pub async fn run(&self) -> std::io::Result<()> {
        let listener = TcpListener::bind(self.config.bind_addr).await?;
        logger::info(&format!("gateway listening on {}", self.config.bind_addr));

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            let session_id = SessionId(self.next_session_id.fetch_add(1, Ordering::Relaxed));
            // Relaxed: session_id is a local identifier only; no happens-before
            // relationship with the spawned task's data is required.
            let market_data = Arc::clone(&self.market_data);
            let cmd_producer = (self.cmd_producer_factory)(session_id);
            let read_buf_capacity = self.config.read_buf_capacity;

            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, session_id, peer_addr, cmd_producer, market_data, read_buf_capacity).await {
                    logger::warn(&format!("session {session_id:?} ({peer_addr}) closed with error: {e}"));
                }
            });
        }
    }
}

/// Per-connection I/O loop.
///
/// This is a simplified single-task implementation that:
/// 1. Reads and authenticates the first frame (assumed to carry the
///    `AccountId` — real auth would involve a handshake/token, elided
///    here for brevity).
/// 2. Loops reading frames and dispatching them via `Session`.
/// 3. Concurrently, drains any market-data subscriptions established
///    by the session and writes them out.
///
/// Note: a production implementation would split read/write halves
/// into separate tasks (as sketched in the `select!` below) so that
/// market data can be pushed even while waiting on the next inbound
/// frame. This sketch shows the structure; full bidirectional
/// concurrency requires `tokio::io::split` and a write-side mpsc
/// channel multiplexing exec reports + market data, which is left as
/// an integration detail for `sim`/production wiring.
async fn handle_connection(
    stream: TcpStream,
    session_id: SessionId,
    peer_addr: SocketAddr,
    cmd_producer: SpscProducer<Command, 4096>,
    market_data: Arc<MarketDataHub>,
    read_buf_capacity: usize,
) -> std::io::Result<()> {
    logger::info(&format!("session {session_id:?} connected from {peer_addr}"));

    let (mut read_half, mut write_half) = tokio::io::split(stream);

    // --- Auth handshake -----------------------------------------------------
    // First frame must be a NEW_ORDER-protocol-agnostic auth frame:
    // payload = u64 account_id. In production this would validate a
    // signed token; here we trust the client-supplied id.
    let account_id = read_auth_frame(&mut read_half, read_buf_capacity).await?;

    let mut session = Session::new(session_id, account_id, cmd_producer);
    // TODO: load ArcSwap<RiskConfig> for this account and pass to Session
    // so that handle_inbound can call tier0::check(config, cmd) before
    // pushing to cmd_producer. Orders failing Tier-0 must be rejected here
    // — before the SPSC push — and never reach the sequencer or WAL.
    let codec = Codec;

    // Channel used by the market-data forwarding tasks to push encoded
    // frames to the writer task.
    let (md_tx, mut md_rx) = tokio::sync::mpsc::channel::<BytesMut>(256);

    // Writer task: serializes all outbound bytes (exec reports +
    // market data) onto the socket.
    let writer_task = tokio::spawn(async move {
        while let Some(frame) = md_rx.recv().await {
            if write_half.write_all(&frame).await.is_err() {
                break;
            }
        }
    });

    let mut read_buf = BytesMut::with_capacity(read_buf_capacity);
    let mut forwarded: HashSet<InstrumentId> = HashSet::new();

    loop {
        // Read more bytes from the socket.
        let n = read_half.read_buf(&mut read_buf).await?;
        if n == 0 {
            // Connection closed by peer.
            break;
        }

        // Process as many complete frames as are buffered.
        loop {
            match session.handle_inbound(&mut read_buf) {
                Ok(Some(SessionAction::WriteFrame(frame))) => {
                    if md_tx.send(frame).await.is_err() {
                        break;
                    }
                }
                Ok(Some(SessionAction::Close)) => {
                    drop(md_tx);
                    let _ = writer_task.await;
                    return Ok(());
                }
                Ok(None) => break, // no more complete frames buffered
                Err(e) => {
                    logger::warn(&format!("session {session_id:?} protocol error: {e}"));
                    drop(md_tx);
                    let _ = writer_task.await;
                    return Ok(());
                }
            }
        }

        // After processing inbound frames, spawn forwarders for any
        // newly-added subscriptions. (Idempotent: in a real impl we'd
        // track which subscriptions already have an active forwarder
        // task; elided here for brevity — see note above re: full
        // bidirectional design.)
        for &instrument_id in session.subscriptions.iter() {
            if forwarded.insert(instrument_id) {
                let mut rx = market_data.subscribe(instrument_id).await;
                let tx = md_tx.clone();
                let codec = codec;
                tokio::spawn(async move {
                    while let Ok(ev) = rx.recv().await {
                        let mut buf = BytesMut::new();
                        if encode_market_data(&codec, &ev, &mut buf).is_ok() {
                            if tx.send(buf).await.is_err() {
                                break;
                            }
                        }
                    }
                });
            }
        }
    }

    drop(md_tx);
    let _ = writer_task.await;
    logger::info(&format!("session {session_id:?} disconnected"));
    Ok(())
}

/// Reads a single length-prefixed auth frame and extracts the
/// `AccountId` from its payload (`u64`, little-endian).
async fn read_auth_frame<R: AsyncReadExt + Unpin>(reader: &mut R, capacity: usize) -> std::io::Result<AccountId> {
    let codec = Codec;
    let mut buf = BytesMut::with_capacity(capacity);

    loop {
        if let Some(frame) = codec.decode(&mut buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))? {
            if frame.payload.len() != 8 {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "auth payload must be 8 bytes"));
            }
            let mut p = &frame.payload[..];
            use bytes::Buf;
            let _raw = p.get_u64_le();   
        }

        let n = reader.read_buf(&mut buf).await?;
        if n == 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "connection closed during auth"));
        }
    }
}

/// Encodes a `MarketDataEvent` into the `MARKET_DATA` wire frame.
///
/// Payload layout (variant-tagged, little-endian):
///   u8 variant
/// variant 0 (TopOfBook): u64 instrument_id, u8 has_bid, i64 bid_price, u64 bid_qty,
///                          u8 has_ask, i64 ask_price, u64 ask_qty
/// variant 1 (Trade):     u64 instrument_id, i64 price, u64 qty, u8 aggressor_side
/// variant 2 (HaltStatus): u64 instrument_id, u8 halted
fn encode_market_data(codec: &Codec, ev: &crate::market_data::MarketDataEvent, out: &mut BytesMut) -> Result<(), crate::codec::CodecError> {
    use crate::market_data::MarketDataEvent::*;
    use bytes::BufMut;

    let mut payload = BytesMut::new();
    match ev {
        TopOfBook { instrument_id, best_bid, best_ask } => {
            payload.put_u8(0);
            payload.put_u64_le(instrument_id.get());
            match best_bid {
                Some((p, q)) => {
                    payload.put_u8(1);
                    payload.put_i64_le(p.ticks());
                    payload.put_u64_le(q.lots());
                }
                None => {
                    payload.put_u8(0);
                    payload.put_i64_le(0);
                    payload.put_u64_le(0);
                }
            }
            match best_ask {
                Some((p, q)) => {
                    payload.put_u8(1);
                    payload.put_i64_le(p.ticks());
                    payload.put_u64_le(q.lots());
                }
                None => {
                    payload.put_u8(0);
                    payload.put_i64_le(0);
                    payload.put_u64_le(0);
                }
            }
        }
        Trade { instrument_id, price, qty, aggressor_side } => {
            payload.put_u8(1);
            payload.put_u64_le(instrument_id.get());
            payload.put_i64_le(price.ticks());
            payload.put_u64_le(qty.lots());
            payload.put_u8(if aggressor_side.is_buy() { 0 } else { 1 });
        }
        HaltStatus { instrument_id, halted } => {
            payload.put_u8(2);
            payload.put_u64_le(instrument_id.get());
            payload.put_u8(if *halted { 1 } else { 0 });
        }
    }

    codec.encode(msg_type::MARKET_DATA, &payload, out)
}

/// Dispatches an `Event` from the matching engine's output stream to
/// the appropriate session(s)' write channels.
///
/// In the full system, this would be driven by a task that consumes
/// `Event`s from the engine's SPMC output ring buffer (see
/// `ring-buffer/spmc.rs`) and looks up the relevant session(s) by
/// `account_id` via a shared session registry. The registry/lookup
/// mechanism is intentionally left abstract here (`sessions` is a
/// caller-provided lookup function) since its concrete form depends
/// on how `server.rs` is integrated with `sim`/production wiring.
pub async fn dispatch_event_to_sessions<L>(ev: &Event, sessions: &L)
where
    L: Fn(AccountId) -> Option<tokio::sync::mpsc::Sender<BytesMut>>,
{
    let _codec = Codec;
    let accounts: Vec<AccountId> = match ev {
        Event::Accepted { account_id, .. }
        | Event::Canceled { account_id, .. }
        | Event::Rejected { account_id, .. }
        | Event::Modified { account_id, .. } => vec![*account_id],
        Event::Filled { fill, .. } => vec![fill.aggressor_account_id, fill.resting_account_id],
        Event::InstrumentHalted { .. } | Event::InstrumentResumed { .. } => vec![],
    };

    for account_id in accounts {
        if let Some(_tx) = sessions(account_id) {
            // TODO: wire to real session registry.
            // let mut buf = BytesMut::new();
            // session.encode_event(ev, &mut buf);
            // let _ = tx.send(buf).await;
            todo!("dispatch_event_to_sessions: wire session registry before sim integration");  
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;
    use core_types::{AccountId, ClientOrderId, Command, InstrumentId, NewOrder, OrderType, Price, Qty, Side, TimeInForce};
    use std::sync::Mutex;
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn auth_then_new_order_enqueues_command() {
        // Set up a listener and a single producer/consumer pair that
        // the connection handler will use directly (bypassing the
        // factory abstraction for this focused test).
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let (producer, mut consumer) = spsc::channel::<Command>(16);

        let server_task = tokio::spawn(async move {
            let (stream, peer) = listener.accept().await.unwrap();
            handle_connection(stream, SessionId(1), peer, producer, MarketDataHub::new(), 4096)
                .await
                .unwrap();
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let codec = Codec;

        // Send auth frame: account_id = 42
        let mut auth_payload = BytesMut::new();
        auth_payload.put_u64_le(42);
        let mut auth_frame = BytesMut::new();
        codec.encode(0x00, &auth_payload, &mut auth_frame).unwrap();
        client.write_all(&auth_frame).await.unwrap();

        // Send a NEW_ORDER frame.
        let mut order_payload = BytesMut::new();
        order_payload.put_u64_le(1); // client_order_id
        order_payload.put_u64_le(5); // instrument_id
        order_payload.put_u8(0);     // side = Buy
        order_payload.put_u8(0);     // order_type = Limit
        order_payload.put_i64_le(10_000); // price
        order_payload.put_u64_le(3); // qty
        order_payload.put_u8(0);     // tif = GTC

        let mut order_frame = BytesMut::new();
        codec.encode(msg_type::NEW_ORDER, &order_payload, &mut order_frame).unwrap();
        client.write_all(&order_frame).await.unwrap();

        // Give the server a moment to process.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let cmd = consumer.try_pop().expect("expected enqueued command");
        match cmd {
            Command::New(NewOrder { account_id, instrument_id, client_order_id, side, qty, order_type, time_in_force }) => {
                assert_eq!(account_id, AccountId::new(42));
                assert_eq!(instrument_id, InstrumentId::new(5));
                assert_eq!(client_order_id, ClientOrderId::new(1));
                assert_eq!(side, Side::Buy);
                assert_eq!(qty, Qty::new(3));
                assert_eq!(time_in_force, TimeInForce::Gtc);
                match order_type {
                    OrderType::Limit { price } => assert_eq!(price, Price::new(10_000)),
                    _ => panic!("expected limit"),
                }
            }
            _ => panic!("expected New command"),
        }

        drop(client);
        let _ = server_task.await;
    }
}