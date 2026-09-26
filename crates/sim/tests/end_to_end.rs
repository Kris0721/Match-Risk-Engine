//! End-to-end integration test: boots a real `GatewayServer`, `Sequencer`,
//! `MatchingEngine`, and `RiskShard`, wired together with real ring
//! buffers over real OS threads (unlike `SimHarness`, which intentionally
//! bypasses all of this with `VecDeque`s) — and proves a client can send
//! a NEW_ORDER over a real TCP socket and get a real ack back through the
//! whole stack. Nothing before this test ever did that.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use bytes::{BufMut, BytesMut};

use core_types::clock::MonotonicClock;
use core_types::{AccountId, EngineEvent, Event, InboundCommand, Price, SequencedCommand, Symbol};
use gateway::server::{GatewayConfig, GatewayServer};
use gateway::session::{msg_type, SessionId};
use gateway::{Codec, MarketDataHub};
use matching_engine::{EngineConfig, MatchingEngine, Tier0Limits};
use order_book::book::BookConfig;
use order_book::OrderBook;
use ring_buffer::{spmc_queue, spsc_queue, SpscProducer};
use risk_engine::{RiskShard, ShardConfig};
use seqlock::AccountRiskTable;
use sequencer::sequencer::ME_INBOUND_CAP;
use sequencer::{GlobalHalt, RoleHandle, Sequencer, SequencerConfig};
use wal::log::NullWal;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const TEST_ACCOUNT: u64 = 7;
const TEST_SYMBOL: u16 = 0;

#[tokio::test]
async fn order_flows_gateway_to_engine_and_ack_returns() {
    // gateway -> sequencer lane (this test opens one session)
    let (gw_producer, gw_consumer) = spsc_queue::<InboundCommand, ME_INBOUND_CAP>();

    // sequencer -> matching engine lane (one per symbol; one symbol here)
    let (me_producer, me_consumer) = spsc_queue::<SequencedCommand, ME_INBOUND_CAP>();

    // sequencer -> WAL writer lane (must match sequencer's private WAL_CAP)
    let (wal_producer, mut wal_consumer) = spsc_queue::<SequencedCommand, { 1 << 14 }>();

    // matching engine -> gateway exec-report lane
    let (me_out_producer, mut me_out_consumer) = spsc_queue::<Event, 1024>();

    // matching engine -> risk shard lane (must match risk-engine's private FANOUT_CAP)
    let (risk_out_producer, mut risk_out_consumers) = spmc_queue::<EngineEvent, 16384>(1);
    let risk_out_consumer = risk_out_consumers.remove(0);

    let account_states = Arc::new(AccountRiskTable::new(16));
    let mut shard = RiskShard::new(0..16, ShardConfig::new(16), account_states.clone());
    shard.seed_deposit(AccountId::new(TEST_ACCOUNT), 100_000_00000000);

    // --- Sequencer thread ---
    let sequencer = Sequencer::new(
        vec![gw_consumer],
        vec![me_producer],
        wal_producer,
        vec![],
        GlobalHalt::new(),
        RoleHandle::for_test_leader(),
        SequencerConfig::new(1),
        MonotonicClock::new(),
        0,
    );
    thread::spawn(move || sequencer.run());

    // --- WAL writer thread (NullWal — proving the pipeline moves data
    // doesn't require real file durability) ---
    thread::spawn(move || loop {
        if wal_consumer.try_pop().is_none() {
            std::hint::spin_loop();
        }
    });

    // --- Risk shard thread ---
    thread::spawn(move || {
        let mark_prices: &'static risk_engine::MarkPrices =
            Box::leak(Box::new(risk_engine::MarkPrices::new()));
        // Liquidate commands aren't looped back into the sequencer here —
        // proving the forward path (order -> ack) doesn't need that.
        // Wiring liquidation feedback belongs to report item (e).
        let (discard_tx, _discard_rx) = spsc_queue::<InboundCommand, 1024>();
        risk_engine::shard::run_risk_shard(shard, risk_out_consumer, discard_tx, mark_prices)
    });

    // --- Matching engine thread ---
    let book_cfg = BookConfig {
        symbol: Symbol(TEST_SYMBOL),
        tick_floor: Price::ZERO,
        num_ticks: 1024,
        arena_capacity: 4096,
    };
    let mut engine = MatchingEngine::new(
        EngineConfig {
            limits: Tier0Limits::default(),
            pin_core: None,
        },
        OrderBook::new(book_cfg),
        me_consumer,
        me_out_producer,
        risk_out_producer,
        account_states,
        NullWal::default(),
    );
    thread::spawn(move || engine.run());

    // --- Bridge matching-engine's Event stream into the gateway's
    // exec-report broadcast channel ---
    let (exec_tx, _keep_alive_rx) = tokio::sync::broadcast::channel::<Event>(1024);
    let exec_tx_bridge = exec_tx.clone();
    thread::spawn(move || loop {
        match me_out_consumer.try_pop() {
            Some(ev) => {
                let _ = exec_tx_bridge.send(ev);
            }
            None => std::hint::spin_loop(),
        }
    });

    // --- Gateway server ---
    let producer_slot: Arc<Mutex<Option<SpscProducer<InboundCommand, 4096>>>> =
        Arc::new(Mutex::new(Some(gw_producer)));
    let server = GatewayServer::new(
        GatewayConfig::default(),
        MarketDataHub::new(),
        move |_session_id: SessionId| {
            producer_slot
                .lock()
                .unwrap()
                .take()
                .expect("this test only opens one gateway session")
        },
        exec_tx,
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });

    // --- Real client over TCP ---
    let mut client = TcpStream::connect(addr).await.unwrap();
    let codec = Codec;

    let mut auth_payload = BytesMut::new();
    auth_payload.put_u64_le(TEST_ACCOUNT);
    let mut auth_frame = BytesMut::new();
    codec.encode(0x00, &auth_payload, &mut auth_frame).unwrap();
    client.write_all(&auth_frame).await.unwrap();

    let mut order_payload = BytesMut::new();
    order_payload.put_u64_le(1); // client_order_id
    order_payload.put_u64_le(TEST_SYMBOL as u64); // instrument_id
    order_payload.put_u8(0); // side = Buy
    order_payload.put_u8(0); // order_type = Limit
    order_payload.put_i64_le(10_000); // price
    order_payload.put_u64_le(3); // qty
    order_payload.put_u8(0); // tif = GTC

    let mut order_frame = BytesMut::new();
    codec
        .encode(msg_type::NEW_ORDER, &order_payload, &mut order_frame)
        .unwrap();
    client.write_all(&order_frame).await.unwrap();

    // --- Assert an EXEC_REPORT frame comes back ---
    let mut resp = BytesMut::with_capacity(256);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut got_exec_report = false;
    while tokio::time::Instant::now() < deadline {
        let mut buf = [0u8; 256];
        if let Ok(Ok(n)) =
            tokio::time::timeout(Duration::from_millis(200), client.read(&mut buf)).await
        {
            if n > 0 {
                resp.extend_from_slice(&buf[..n]);
                if let Ok(Some(frame)) = codec.decode(&mut resp) {
                    if frame.msg_type == msg_type::EXEC_REPORT {
                        got_exec_report = true;
                        break;
                    }
                }
            }
        }
    }

    assert!(
        got_exec_report,
        "expected an EXEC_REPORT after sending NEW_ORDER through the full \
         gateway -> sequencer -> matching-engine pipeline"
    );
}
