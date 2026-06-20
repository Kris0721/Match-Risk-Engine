// State recovery from log and snapshot files
//! WAL + snapshot recovery.
//!
//! # Recovery protocol
//!
//! 1. **Load snapshot**: find the latest valid snapshot for each shard in the
//!    snapshot directory. This gives us a base state at `snapshot.seq`.
//! 2. **Open WAL**: scan the WAL file to find all valid records with
//!    `seq > snapshot.seq`. Stop at the first corrupt or truncated record.
//! 3. **Replay**: feed the recovered commands (in seq order) back through the
//!    engine. Because the system is `(snapshot, ordered_log) -> state`,
//!    this deterministically reconstructs the exact pre-crash state.
//!
//! # Partial writes
//! A crash mid-write may leave a partially written record at the tail of the
//! WAL. The scanner detects this via CRC32 mismatch or truncated length and
//! stops before the corrupt record. Commands after the last valid record are
//! lost — this is the standard WAL durability trade-off (`sync_on_write=true`
//! minimises this window to a single record).
//!
//! # Gaps
//! If `seq` numbers are not contiguous in the WAL (a gap exists), recovery
//! logs a warning and continues — a gap means the Sequencer itself crashed
//! after writing to the WAL but before routing to the ME. The WAL is the
//! source of truth; the matching engines re-process everything from the WAL.

use std::path::Path;

use thiserror::Error;

use core_types::{InboundCommand, SequencedCommand};
use crate::snapshot::{latest_snapshot, Snapshot, SnapshotError};

#[derive(Debug, Error)]
pub enum RecoveryError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("snapshot error: {0}")]
    Snapshot(#[from] SnapshotError),
    #[error("WAL file not found: {0}")]
    WalNotFound(String),
    #[error("WAL header invalid")]
    InvalidWalHeader,
    #[error("deserialisation error: {0}")]
    Deserialise(String),
}

/// The output of a recovery run.
pub struct RecoveryOutput {
    /// The snapshot that was loaded (if any).
    pub snapshot:          Option<Snapshot>,
    /// Sequence number of the last record in the snapshot (0 if no snapshot).
    pub snapshot_seq:      u64,
    /// Commands recovered from the WAL after the snapshot point.
    pub commands:          Vec<SequencedCommand>,
    /// Sequence number of the last valid WAL record recovered.
    pub last_recovered_seq: u64,
    /// Number of records skipped due to CRC errors.
    pub corrupt_records:   usize,
    /// Number of sequence gaps detected.
    pub seq_gaps:          usize,
}

/// Recover state for `shard_id` from a WAL file and snapshot directory.
///
/// # Arguments
/// * `wal_path`      — path to the WAL mmap file.
/// * `snapshot_dir`  — directory containing `.snap` files.
/// * `shard_id`      — which shard to recover (used to select the right snapshot).
///
/// Returns `RecoveryOutput` with all commands that must be replayed.
pub fn recover(
    wal_path:     impl AsRef<Path>,
    snapshot_dir: impl AsRef<Path>,
    shard_id:     u32,
) -> Result<RecoveryOutput, RecoveryError> {
    let wal_path = wal_path.as_ref();

    if !wal_path.exists() {
        return Err(RecoveryError::WalNotFound(
            wal_path.display().to_string()
        ));
    }

    // --- Step 1: Load snapshot ---
    let snapshot = latest_snapshot(&snapshot_dir, shard_id)?;
    let snapshot_seq = snapshot.as_ref().map_or(0, |s| s.meta.seq);

    // --- Step 2: Read and scan the WAL file ---
    let wal_bytes = std::fs::read(wal_path)?;
    let records = scan_wal(&wal_bytes, snapshot_seq);

    let last_recovered_seq = records.commands.last().map_or(0, |c| c.seq);

    Ok(RecoveryOutput {
        snapshot,
        snapshot_seq,
        commands:           records.commands,
        last_recovered_seq,
        corrupt_records:    records.corrupt_records,
        seq_gaps:           records.seq_gaps,
    })
}

// ── WAL scanner ──────────────────────────────────────────────────────────────

const FILE_HEADER_SIZE: usize = 32;
const RECORD_HEADER_SIZE: usize = 24;
const MAGIC: &[u8; 8] = b"MREWAL01";

struct ScanResult {
    commands:        Vec<SequencedCommand>,
    corrupt_records: usize,
    seq_gaps:        usize,
}

/// Scan the raw WAL bytes and deserialise all valid records with `seq > after_seq`.
fn scan_wal(data: &[u8], after_seq: u64) -> ScanResult {
    let mut commands        = Vec::new();
    let mut corrupt_records = 0usize;
    let mut seq_gaps        = 0usize;
    let mut last_seq        = after_seq;

    if data.len() < FILE_HEADER_SIZE {
        return ScanResult { commands, corrupt_records, seq_gaps };
    }
    if &data[0..8] != MAGIC {
        return ScanResult { commands, corrupt_records, seq_gaps };
    }

    let mut offset = FILE_HEADER_SIZE;

    loop {
        if offset + RECORD_HEADER_SIZE > data.len() {
            break;
        }

        let seq = u64::from_le_bytes(data[offset..offset+8].try_into().unwrap());
        if seq == 0 {
            break; // unwritten region
        }

        let ts_ns = u64::from_le_bytes(data[offset+8..offset+16].try_into().unwrap());
        let len   = u32::from_le_bytes(data[offset+16..offset+20].try_into().unwrap()) as usize;
        let crc_stored = u32::from_le_bytes(data[offset+20..offset+24].try_into().unwrap());

        let payload_start = offset + RECORD_HEADER_SIZE;
        let payload_end   = payload_start + len;

        if payload_end > data.len() {
            // Truncated record at tail — stop.
            corrupt_records += 1;
            break;
        }

        let payload = &data[payload_start..payload_end];
        let crc_computed = {
            let mut h = crc32fast::Hasher::new();
            h.update(payload);
            h.finalize()
        };

        if crc_computed != crc_stored {
            // Corrupt record — stop here; don't skip, as downstream records
            // may depend on this one being applied.
            corrupt_records += 1;
            break;
        }

        // Detect sequence gap.
        if seq != last_seq + 1 && last_seq != 0 && last_seq != after_seq {
            eprintln!(
                "[wal/recovery] sequence gap: expected {} got {}",
                last_seq + 1, seq
            );
            seq_gaps += 1;
        }
        last_seq = seq;

        // Only replay records after the snapshot point.
        if seq > after_seq {
            match deserialise_command(payload) {
                Ok(cmd) => commands.push(SequencedCommand { seq, ts_ns, cmd }),
                Err(e)  => {
                    eprintln!("[wal/recovery] deserialisation failed at seq={seq}: {e}");
                    corrupt_records += 1;
                    break;
                }
            }
        }

        let record_size = align8(RECORD_HEADER_SIZE + len);
        offset += record_size;
    }

    ScanResult { commands, corrupt_records, seq_gaps }
}

/// Deserialise an `InboundCommand` from rkyv bytes.
fn deserialise_command(bytes: &[u8]) -> Result<InboundCommand, RecoveryError> {
    bincode::deserialize(bytes).map_err(|e| RecoveryError::Deserialise(e.to_string()))
}

#[inline]
fn align8(n: usize) -> usize {
    (n + 7) & !7
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use core_types::{AccountId, InboundCommand, OrderType, Price, Qty, Side, Symbol};
    use crate::log::{FileWalWriter, WalWriterConfig};
    use crate::snapshot::SnapshotWriter;

    fn dummy_sc(seq: u64) -> SequencedCommand {
        SequencedCommand {
            seq,
            ts_ns: seq * 1000,
            cmd: InboundCommand::NewOrder {
                account:    AccountId(1),
                client_order_id: core_types::ClientOrderId::new(0),
                symbol:     Symbol(0),
                side:       Side::Buy,
                price:      Price(100_00000000),
                qty:        Qty(1_00000000),
                order_type: OrderType::Limit,
                time_in_force: core_types::TimeInForce::Gtc,
            },
        }
    }

    #[test]
    fn recover_all_from_wal_no_snapshot() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");

        let mut writer = FileWalWriter::open(&wal_path, WalWriterConfig {
            file_capacity: 1024 * 1024,
            sync_on_write: false,
        }).unwrap();

        for i in 1..=5 {
            writer.append(&dummy_sc(i)).unwrap();
        }
        drop(writer);

        let snap_dir = dir.path().join("snaps");
        let out = recover(&wal_path, &snap_dir, 0).unwrap();

        assert_eq!(out.snapshot_seq,       0);
        assert_eq!(out.commands.len(),     5);
        assert_eq!(out.last_recovered_seq, 5);
        assert_eq!(out.corrupt_records,    0);
        assert_eq!(out.seq_gaps,           0);
    }

    #[test]
    fn recover_skips_commands_before_snapshot() {
        let dir = TempDir::new().unwrap();
        let wal_path  = dir.path().join("test.wal");
        let snap_dir  = dir.path().join("snaps");

        // Write 10 WAL records.
        let mut writer = FileWalWriter::open(&wal_path, WalWriterConfig {
            file_capacity: 1024 * 1024,
            sync_on_write: false,
        }).unwrap();
        for i in 1..=10 {
            writer.append(&dummy_sc(i)).unwrap();
        }
        drop(writer);

        // Write a snapshot at seq=7.
        let snap_writer = SnapshotWriter::new(&snap_dir, 1024 * 1024);
        snap_writer.write(0, 7, 0, b"fake shard state").unwrap();

        let out = recover(&wal_path, &snap_dir, 0).unwrap();

        assert_eq!(out.snapshot_seq,       7);
        // Only records 8, 9, 10 should be replayed.
        assert_eq!(out.commands.len(),     3);
        assert_eq!(out.commands[0].seq,    8);
        assert_eq!(out.last_recovered_seq, 10);
    }

    #[test]
    fn corrupt_tail_stops_recovery() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");

        let mut writer = FileWalWriter::open(&wal_path, WalWriterConfig {
            file_capacity: 1024 * 1024,
            sync_on_write: false,
        }).unwrap();
        for i in 1..=4 {
            writer.append(&dummy_sc(i)).unwrap();
        }
        let cursor = writer.cursor();
        drop(writer);

        // Corrupt bytes in the 4th record's payload.
        let mut data = std::fs::read(&wal_path).unwrap();
        if cursor > 10 {
            data[cursor - 10] ^= 0xFF;
        }
        std::fs::write(&wal_path, &data).unwrap();

        let snap_dir = dir.path().join("snaps");
        let out = recover(&wal_path, &snap_dir, 0).unwrap();

        // Should recover up to 3 valid records, then stop.
        assert!(out.commands.len() <= 3);
        assert!(out.corrupt_records >= 1);
    }

    #[test]
    fn wal_not_found_error() {
        let dir = TempDir::new().unwrap();
        let result = recover(
            dir.path().join("nonexistent.wal"),
            dir.path().join("snaps"),
            0,
        );
        assert!(matches!(result, Err(RecoveryError::WalNotFound(_))));
    }
}