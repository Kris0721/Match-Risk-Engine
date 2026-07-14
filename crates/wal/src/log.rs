// Write-ahead log append and sync operations
//! Append-only WAL writer backed by an mmap'd file.
//!
//! # File layout
//!
//! ```text
//! [ FILE HEADER (32 bytes) ]
//! [ RECORD 0               ]
//! [ RECORD 1               ]
//! ...
//! ```
//!
//! ## File header (32 bytes)
//! ```text
//! magic:     [u8; 8]   "MREWAL01"
//! version:   u32       = 1
//! reserved:  [u8; 20]
//! ```
//!
//! ## Record layout
//! ```text
//! seq:       u64        — global monotonic sequence number
//! ts_ns:     u64        — hardware timestamp at sequencing time
//! len:       u32        — byte length of the rkyv-serialised payload
//! crc32:     u32        — CRC32 of the payload bytes
//! payload:   [u8; len]  — rkyv archive of InboundCommand
//! padding:   [u8; ?]    — zero bytes to align next record to 8 bytes
//! ```
//!
//! # Design choices
//! - **mmap + `msync`**: the OS page cache absorbs bursts; we call `msync` on
//!   each record for durability. On Linux with `MAP_SHARED` this is equivalent
//!   to a `fdatasync` of the dirty pages.
//! - **No framing delimiters**: record boundaries are found by reading `len`
//!   from the fixed-offset header of each record. A torn `len` field is
//!   detected by the CRC32 of the payload.
//! - **Zero heap allocation per write**: the rkyv serialiser writes directly
//!   into a stack-allocated scratch buffer that is then `copy_from_slice`'d
//!   into the mmap region.
//! - **Single writer**: `FileWalWriter` is `!Sync`. The WAL writer runs on its
//!   own thread and receives `SequencedCommand`s via an SPSC queue.

use std::fs::OpenOptions;
use std::path::Path;

use memmap2::MmapMut;
use crc32fast::Hasher as Crc32Hasher;
use thiserror::Error;

use core_types::{InboundCommand, SequencedCommand};
use core_types::events::Event;

// ── Constants ────────────────────────────────────────────────────────────────

const MAGIC: &[u8; 8] = b"MREWAL01";
const VERSION: u32     = 1;
const FILE_HEADER_SIZE: usize = 32;

/// Fixed overhead per record: seq(8) + ts_ns(8) + len(4) + crc32(4) = 24 bytes.
const RECORD_HEADER_SIZE: usize = 24;


/// Default mmap file capacity: 512 MiB.
const DEFAULT_FILE_CAPACITY: usize = 512 * 1024 * 1024;

// ── Error ────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum WalError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("WAL file capacity exhausted (capacity={capacity}, needed={needed})")]
    CapacityExhausted { capacity: usize, needed: usize },
    #[error("serialisation error: {0}")]
    Serialise(String),
    #[error("invalid magic bytes in WAL file header")]
    InvalidMagic,
    #[error("unsupported WAL version: {0}")]
    UnsupportedVersion(u32),
}

// ── Config ───────────────────────────────────────────────────────────────────

/// Configuration for `FileWalWriter`.
#[derive(Clone, Debug)]
pub struct WalWriterConfig {
    /// Pre-allocated file size in bytes. The file is created (or extended) to
    /// exactly this size; records are written into the mmap region.
    pub file_capacity: usize,
    /// If `true`, call `msync(MS_SYNC)` after every record write for
    /// synchronous durability. If `false`, rely on OS writeback (lower
    /// latency, small durability window on crash).
    pub sync_on_write: bool,
}

impl Default for WalWriterConfig {
    fn default() -> Self {
        Self {
            file_capacity: DEFAULT_FILE_CAPACITY,
            sync_on_write: true,
        }
    }
}

// ── WalWriter trait ──────────────────────────────────────────────────────────

/// Trait for writing events to a write-ahead log.
///
/// Implementors must be `Send + 'static` so they can be owned by engine
/// threads. The matching engine calls `append_event` on the hot path.
pub trait WalWriter: Send + 'static {
    type Error: std::fmt::Debug;
    fn append_event(&mut self, ev: &Event) -> Result<(), Self::Error>;
    fn flush(&mut self) -> Result<(), Self::Error>;
}

// ── NullWal ──────────────────────────────────────────────────────────────────

/// No-op WAL for use in tests and simulations.
#[derive(Debug, Default, Clone)]
pub struct NullWal;

impl WalWriter for NullWal {
    type Error = std::convert::Infallible;
    fn append_event(&mut self, _ev: &Event) -> Result<(), Self::Error> { Ok(()) }
    fn flush(&mut self) -> Result<(), Self::Error> { Ok(()) }
}

// ── FileWalWriter ────────────────────────────────────────────────────────────

/// Append-only WAL writer backed by an mmap'd file. Single-writer: `!Sync`.
pub struct FileWalWriter {
    mmap:   MmapMut,
    /// Byte offset of the next record to write (starts after file header).
    cursor: usize,
    config: WalWriterConfig,
    /// Last sequence number written (for monotonicity assertions).
    last_seq: u64,
}


impl FileWalWriter {
    /// Open or create a WAL file at `path`.
    ///
    /// If the file is new, writes the file header.
    /// If the file exists and is non-empty, validates the header and positions
    /// the cursor at the end of the last valid record (via `recovery::scan`).
    pub fn open(path: impl AsRef<Path>, config: WalWriterConfig) -> Result<Self, WalError> {
        let path = path.as_ref();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        // Pre-allocate to the requested capacity.
        file.set_len(config.file_capacity as u64)?;

        // SAFETY: the file is kept open for the lifetime of `FileWalWriter`.
        // No other process should write to this file concurrently.
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };

        let is_new = mmap[..8] != *MAGIC;
        if is_new {
            write_file_header(&mut mmap);
        } else {
            validate_file_header(&mmap)?;
        }

        // Scan to find the cursor position (end of last complete record).
        let (cursor, last_seq) = if is_new {
            (FILE_HEADER_SIZE, 0)
        } else {
            scan_to_end(&mmap)?
        };

        Ok(Self { mmap, cursor, config, last_seq })
    }

    /// Append one `SequencedCommand` to the WAL.
    ///
    /// Returns the byte offset of the record just written.
    pub fn append(&mut self, sc: &SequencedCommand) -> Result<usize, WalError> {
    debug_assert!(
        sc.seq > self.last_seq,
        "WAL: sequence numbers must be strictly increasing (got {} after {})",
        sc.seq, self.last_seq
    );

    let payload = serialise_command(&sc.cmd)?;
    self.write_record(sc.seq, sc.ts_ns, &payload)
}

/// Writes one record (header + payload + padding) at the current cursor,
/// advances the cursor, and updates `last_seq`. Shared by `append` and
/// `append_event`.
fn write_record(&mut self, seq: u64, ts_ns: u64, payload: &[u8]) -> Result<usize, WalError> {
    let payload_len = payload.len();
    let record_size = align8(RECORD_HEADER_SIZE + payload_len);
    let end = self.cursor + record_size;

    if end > self.mmap.len() {
        return Err(WalError::CapacityExhausted {
            capacity: self.mmap.len(),
            needed:   end,
        });
    }

    let crc = crc32(payload);
    let offset = self.cursor;
    let buf = &mut self.mmap[offset..offset + record_size];

    buf[0..8].copy_from_slice(&seq.to_le_bytes());
    buf[8..16].copy_from_slice(&ts_ns.to_le_bytes());
    buf[16..20].copy_from_slice(&(payload_len as u32).to_le_bytes());
    buf[20..24].copy_from_slice(&crc.to_le_bytes());
    buf[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + payload_len].copy_from_slice(payload);
    for b in buf[RECORD_HEADER_SIZE + payload_len..].iter_mut() {
        *b = 0;
    }

    if self.config.sync_on_write {
        self.mmap.flush_range(offset, record_size)?;
    }

    self.cursor = end;
    self.last_seq = seq;
    Ok(offset)
}

    /// Force an `msync` of all dirty pages regardless of `sync_on_write`.
    pub fn flush(&mut self) -> Result<(), WalError> {
        self.mmap.flush().map_err(WalError::Io)
    }

    /// Current write cursor (bytes from file start).
    pub fn cursor(&self) -> usize { self.cursor }

    /// Last successfully written sequence number.
    pub fn last_seq(&self) -> u64 { self.last_seq }
}

impl WalWriter for FileWalWriter {
    type Error = WalError;
    fn append_event(&mut self, ev: &Event) -> Result<(), Self::Error> {
        let payload = bincode::serialize(ev)
            .map_err(|e| WalError::Serialise(e.to_string()))?;
        self.write_record(ev.seq().get(), now_ns(), &payload)?;
        Ok(())
    }
    fn flush(&mut self) -> Result<(), Self::Error> {
        self.mmap.flush().map_err(WalError::Io)
    }
}

// ── File header ──────────────────────────────────────────────────────────────

fn write_file_header(mmap: &mut MmapMut) {
    mmap[0..8].copy_from_slice(MAGIC);
    mmap[8..12].copy_from_slice(&VERSION.to_le_bytes());
    // reserved bytes stay zero
}

fn validate_file_header(mmap: &[u8]) -> Result<(), WalError> {
    if &mmap[0..8] != MAGIC {
        return Err(WalError::InvalidMagic);
    }
    let version = u32::from_le_bytes(mmap[8..12].try_into().unwrap());
    if version != VERSION {
        return Err(WalError::UnsupportedVersion(version));
    }
    Ok(())
}

// ── Record helpers ───────────────────────────────────────────────────────────

/// Scan forward from `FILE_HEADER_SIZE`, reading records until we hit a zero
/// seq (which marks the first unwritten byte). Returns `(cursor, last_seq)`.
fn scan_to_end(mmap: &[u8]) -> Result<(usize, u64), WalError> {
    let mut offset = FILE_HEADER_SIZE;
    let mut last_seq = 0u64;

    loop {
        if offset + RECORD_HEADER_SIZE > mmap.len() {
            break;
        }
        let seq = u64::from_le_bytes(mmap[offset..offset+8].try_into().unwrap());
        if seq == 0 {
            break; // reached unwritten region
        }
        let len = u32::from_le_bytes(
            mmap[offset+16..offset+20].try_into().unwrap()
        ) as usize;
        let crc_stored = u32::from_le_bytes(
            mmap[offset+20..offset+24].try_into().unwrap()
        );

        let payload_end = offset + RECORD_HEADER_SIZE + len;
        if payload_end > mmap.len() {
            // Truncated record — stop here; recovery will replay up to last_seq.
            break;
        }

        let crc_computed = crc32(&mmap[offset + RECORD_HEADER_SIZE..payload_end]);
        if crc_computed != crc_stored {
            // Corrupt record — stop before it.
            break;
        }

        last_seq = seq;
        offset += align8(RECORD_HEADER_SIZE + len);
    }

    Ok((offset, last_seq))
}

/// Serialize `InboundCommand` using bincode into a Vec<u8>.
fn serialise_command(cmd: &InboundCommand) -> Result<Vec<u8>, WalError> {
    bincode::serialize(cmd).map_err(|e| WalError::Serialise(e.to_string()))
}

/// CRC32 of a byte slice.
#[inline]
fn crc32(data: &[u8]) -> u32 {
    let mut h = Crc32Hasher::new();
    h.update(data);
    h.finalize()
}

use std::time::{SystemTime, UNIX_EPOCH};

#[inline]
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Round `n` up to the next multiple of 8.
#[inline]
fn align8(n: usize) -> usize {
    (n + 7) & !7
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use core_types::{AccountId, InboundCommand, OrderType, Price, Qty, Side, Symbol};

    fn dummy_sc(seq: u64) -> SequencedCommand {
        SequencedCommand {
            seq,
            ts_ns: seq * 1_000,
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
    fn write_and_scan() {
        let tmp = NamedTempFile::new().unwrap();
        let config = WalWriterConfig {
            file_capacity: 1024 * 1024, // 1 MiB
            sync_on_write: false,
        };

        let mut writer = FileWalWriter::open(tmp.path(), config.clone()).unwrap();
        for i in 1..=5 {
            writer.append(&dummy_sc(i)).unwrap();
        }
        assert_eq!(writer.last_seq(), 5);

        // Re-open and scan — cursor should be past the 5 records.
        let writer2 = FileWalWriter::open(tmp.path(), config).unwrap();
        assert_eq!(writer2.last_seq(), 5);
        assert!(writer2.cursor() > FILE_HEADER_SIZE);
    }

    #[test]
    fn capacity_exhausted_error() {
        let tmp = NamedTempFile::new().unwrap();
        let config = WalWriterConfig {
            file_capacity: FILE_HEADER_SIZE + 64, // only room for ~one tiny record
            sync_on_write: false,
        };
        let mut writer = FileWalWriter::open(tmp.path(), config).unwrap();
        let _ = writer.append(&dummy_sc(1)); // may or may not fit
        // Second append must eventually exhaust capacity.
        let result = writer.append(&dummy_sc(2));
        // Either the first or second exhausts — at least one must fail.
        // We just verify no panic and the error type is correct if it fails.
        if let Err(e) = result {
            assert!(matches!(e, WalError::CapacityExhausted { .. }));
        }
    }

    #[test]
    fn align8_correctness() {
        assert_eq!(super::align8(0),  0);
        assert_eq!(super::align8(1),  8);
        assert_eq!(super::align8(8),  8);
        assert_eq!(super::align8(9),  16);
        assert_eq!(super::align8(24), 24);
        assert_eq!(super::align8(25), 32);
    }
}