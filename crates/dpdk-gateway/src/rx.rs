//! RX poll loop. Runs pinned to its own core, spins on `rte_eth_rx_burst`,
//! parses Ethernet/IP/UDP framing off each mbuf, decodes the payload with
//! the same `Codec` the tokio gateway path uses, and pushes the resulting
//! `InboundCommand` into the SPSC ring the matching engine already reads.
//!
//! No syscalls, no allocation, no tokio runtime in this loop.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use core_types::InboundCommand;
use dpdk_sys::{rte_eth_rx_burst, rte_mbuf, rte_pktmbuf_free};
use ring_buffer::SpscProducer;

use gateway::codec::Codec; // reuse the existing wire format, not a new one

const BURST_SIZE: usize = 32;
/// Ethernet(14) + IP(20, no options) + UDP(8) header bytes to skip
/// before the order payload starts.
const HEADER_SKIP: usize = 14 + 20 + 8;

pub struct RxWorker {
    port_id: u16,
    queue_id: u16,
    producer: SpscProducer<InboundCommand>,
    codec: Codec,
    running: Arc<AtomicBool>,
}

impl RxWorker {
    pub fn new(
        port_id: u16,
        queue_id: u16,
        producer: SpscProducer<InboundCommand>,
        codec: Codec,
        running: Arc<AtomicBool>,
    ) -> Self {
        Self {
            port_id,
            queue_id,
            producer,
            codec,
            running,
        }
    }

    /// Call this on a thread pinned to a dedicated core via `affinity`
    /// (the same crate `matching-engine` uses) — this loop is meant to
    /// spin at 100% CPU, never yield, never sleep.
    pub fn run(mut self) {
        let mut burst: [*mut rte_mbuf; BURST_SIZE] = [std::ptr::null_mut(); BURST_SIZE];

        while self.running.load(Ordering::Relaxed) {
            let n = unsafe {
                rte_eth_rx_burst(
                    self.port_id,
                    self.queue_id,
                    burst.as_mut_ptr(),
                    BURST_SIZE as u16,
                )
            };

            for i in 0..n as usize {
                let mbuf = burst[i];
                self.handle_packet(mbuf);
                unsafe { rte_pktmbuf_free(mbuf) };
            }
            // No sleep/yield here by design — burst==0 just spins again.
            // Add a backoff (e.g. after N consecutive empty polls) if
            // this core needs to share time with anything else.
        }
    }

    fn handle_packet(&mut self, mbuf: *mut rte_mbuf) {
        // SAFETY: mbuf is valid and owned by us until rte_pktmbuf_free.
        let (data_ptr, data_len) = unsafe {
            let m = &*mbuf;
            (
                dpdk_sys::rte_pktmbuf_mtod(mbuf) as *const u8,
                m.data_len as usize,
            )
        };

        if data_len <= HEADER_SKIP {
            return; // truncated/garbage frame — drop silently, don't panic the RX core
        }

        let payload = unsafe {
            std::slice::from_raw_parts(data_ptr.add(HEADER_SKIP), data_len - HEADER_SKIP)
        };

        // TODO: verify EtherType/IP protocol/dest UDP port match the
        // configured order-entry port before trusting `payload` —
        // omitted here since exact offsets depend on whether you're
        // also carrying VLAN tags.

        match self.codec.decode_command(payload) {
            Ok(cmd) => {
                if self.producer.try_push(cmd).is_err() {
                    // Ring full — matching engine can't keep up. Count
                    // this, don't block: blocking here defeats the
                    // point of kernel bypass.
                    // metrics.rx_ring_full.fetch_add(1, Relaxed);
                }
            }
            Err(_e) => {
                // metrics.decode_errors.fetch_add(1, Relaxed);
            }
        }
    }
}
