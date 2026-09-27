use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use core_types::commands::InboundCommand;
use core_types::log_entry::LogEntry;
use core_types::SequencedCommand;

use wal::log::{FileWalWriter, WalError, WalWriterConfig};

use crate::pending_ring::PendingRing;

/// Errors from the dual log write path.
#[derive(Debug)]
pub enum DualLogError {
    /// Log A (primary) write failed.
    LogAFailed(WalError),
    /// Log B (secondary) write failed.
    LogBFailed(WalError),
    /// Both log writes failed.
    BothFailed { a: WalError, b: WalError },
}

impl std::fmt::Display for DualLogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DualLogError::LogAFailed(e) => write!(f, "Log A write failed: {e}"),
            DualLogError::LogBFailed(e) => write!(f, "Log B write failed: {e}"),
            DualLogError::BothFailed { a, b } => {
                write!(f, "Both logs failed: A={a}, B={b}")
            }
        }
    }
}

impl std::error::Error for DualLogError {}

/// Configuration for the dual log system.
#[derive(Clone, Debug)]
pub struct DualLogConfig {
    /// Configuration for Log A (Primary Engine's log).
    pub log_a_config: WalWriterConfig,
    /// Configuration for Log B (Secondary Engine's log).
    pub log_b_config: WalWriterConfig,
}

impl Default for DualLogConfig {
    fn default() -> Self {
        Self {
            log_a_config: WalWriterConfig::default(),
            log_b_config: WalWriterConfig::default(),
        }
    }
}

pub struct DualLog {
    log_a: FileWalWriter,
    log_b: FileWalWriter,
    last_assigned_seq: u64,
    pending_ring: Arc<PendingRing>,
    clock_origin: Instant,
}

impl DualLog {
    pub fn open(
        log_a_path: impl AsRef<Path>,
        log_b_path: impl AsRef<Path>,
        config: DualLogConfig,
        pending_ring: Arc<PendingRing>,
    ) -> Result<Self, DualLogError> {
        let log_a = FileWalWriter::open(log_a_path, config.log_a_config)
            .map_err(DualLogError::LogAFailed)?;
        let log_b = FileWalWriter::open(log_b_path, config.log_b_config)
            .map_err(DualLogError::LogBFailed)?;
        let last_assigned_seq = log_a.last_seq().max(log_b.last_seq());

        Ok(Self {
            log_a,
            log_b,
            last_assigned_seq,
            pending_ring,
            clock_origin: Instant::now(),
        })
    }

    /// `term`/`seq` are assigned upstream by the `Sequencer` — this is the
    /// downstream escalation/backup layer, not a competing sequencer.
    pub fn write(
        &mut self,
        term: u64,
        seq: u64,
        cmd: InboundCommand,
    ) -> Result<Arc<LogEntry>, DualLogError> {
        debug_assert!(
            seq > self.last_assigned_seq,
            "DualLog: received non-monotonic seq {seq} after {}",
            self.last_assigned_seq
        );
        self.last_assigned_seq = seq;
        let ts_ns = self.clock_origin.elapsed().as_nanos() as u64;

        let entry = Arc::new(LogEntry::new(seq, term, ts_ns, cmd.clone()));
        let sc = SequencedCommand {
            term,
            seq,
            ts_ns,
            cmd,
        };

        let res_a = self.log_a.append(&sc);
        let res_b = self.log_b.append(&sc);

        match (res_a, res_b) {
            (Ok(_), Ok(_)) => {
                self.pending_ring.push(Arc::clone(&entry));
                Ok(entry)
            }
            (Err(a), Err(b)) => Err(DualLogError::BothFailed { a, b }),
            (Err(a), Ok(_)) => Err(DualLogError::LogAFailed(a)),
            (Ok(_), Err(b)) => Err(DualLogError::LogBFailed(b)),
        }
    }

    /// Get a reference to the shared pending ring.
    pub fn pending_ring(&self) -> &Arc<PendingRing> {
        &self.pending_ring
    }

    /// Current sequence number (last assigned).
    pub fn current_seq(&self) -> u64 {
        self.last_assigned_seq
    }

    /// Flush both logs to disk.
    pub fn flush(&mut self) -> Result<(), DualLogError> {
        self.log_a.flush().map_err(DualLogError::LogAFailed)?;
        self.log_b.flush().map_err(DualLogError::LogBFailed)?;
        Ok(())
    }

    /// Cursor positions for monitoring (log_a_cursor, log_b_cursor).
    pub fn cursors(&self) -> (usize, usize) {
        (self.log_a.cursor(), self.log_b.cursor())
    }

    /// Last sequence numbers written to each log.
    pub fn last_seqs(&self) -> (u64, u64) {
        (self.log_a.last_seq(), self.log_b.last_seq())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::order_status::OrderStatus;
    use core_types::{
        AccountId, ClientOrderId, InboundCommand, OrderType, Price, Qty, Side, Symbol, TimeInForce,
    };
    use tempfile::TempDir;

    fn sample_cmd() -> InboundCommand {
        InboundCommand::NewOrder {
            account: AccountId(1),
            client_order_id: ClientOrderId::new(42),
            symbol: Symbol(0),
            side: Side::Buy,
            price: Price(100_00000000),
            qty: Qty(10_00000000),
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
        }
    }

    fn mk_dual_log(dir: &TempDir) -> DualLog {
        let ring = Arc::new(PendingRing::new());
        let config = DualLogConfig {
            log_a_config: WalWriterConfig {
                file_capacity: 1024 * 1024,
                sync_on_write: false,
            },
            log_b_config: WalWriterConfig {
                file_capacity: 1024 * 1024,
                sync_on_write: false,
            },
        };
        DualLog::open(
            dir.path().join("log_a.wal"),
            dir.path().join("log_b.wal"),
            config,
            ring,
        )
        .expect("DualLog::open failed")
    }

    #[test]
    fn write_populates_both_logs_and_pending_ring() {
        let dir = TempDir::new().unwrap();
        let mut dl = mk_dual_log(&dir);

        let entry = dl.write(1, 1, sample_cmd()).expect("dual write failed");

        // Entry should be Pending
        assert_eq!(entry.load_status(), OrderStatus::Pending);
        assert_eq!(entry.seq, 1);

        // Pending ring should have it
        assert_eq!(dl.pending_ring().len(), 1);

        // Both logs should have the same sequence
        let (seq_a, seq_b) = dl.last_seqs();
        assert_eq!(seq_a, 1);
        assert_eq!(seq_b, 1);
    }

    #[test]
    fn sequence_numbers_are_monotonic() {
        let dir = TempDir::new().unwrap();
        let mut dl = mk_dual_log(&dir);

        let e1 = dl.write(1, 1, sample_cmd()).unwrap();
        let e2 = dl.write(1, 2, sample_cmd()).unwrap();
        let e3 = dl.write(1, 3, sample_cmd()).unwrap();

        assert_eq!(e1.seq, 1);
        assert_eq!(e2.seq, 2);
        assert_eq!(e3.seq, 3);
        assert_eq!(dl.pending_ring().len(), 3);
    }

    #[test]
    fn multiple_writes_produce_correct_cursors() {
        let dir = TempDir::new().unwrap();
        let mut dl = mk_dual_log(&dir);

        for i in 1..10 {
            dl.write(1, i, sample_cmd()).unwrap();
        }

        let (ca, cb) = dl.cursors();
        // Both cursors should be past the file header (32 bytes)
        assert!(ca > 32);
        assert!(cb > 32);
        // Both should be equal (identical writes)
        assert_eq!(ca, cb);
    }
}
