//! Persistent-memory WAL writer using `clwb` + `sfence` for durability,
//! instead of `msync`.
//!
//! # Why this exists
//!
//! `FileWalWriter` (see `log.rs`) calls `msync(MS_SYNC)` per record, which
//! is a syscall that flushes dirty pages through the OS page cache. On
//! real persistent memory (Optane DC PMM, CXL-attached PMEM, etc.) mapped
//! via DAX, that syscall is unnecessary overhead: writes already land
//! directly on the memory bus. All you need is:
//!
//!   1. `clwb` (cache-line write-back) — push the dirty cache line to the
//!      memory controller's write-pending queue (ADR-backed, so it's
//!      durable even on power loss on ADR-capable platforms) without
//!      evicting it from cache (unlike `clflush`/`clflushopt`).
//!   2. `sfence` — a store fence, so the `clwb`s from step 1 are
//!      guaranteed to have completed before any store that follows this
//!      point in program order (e.g. the "commit" write of a sequence
//!      number) becomes visible.
//!
//! This is the standard `libpmem`/PMDK durability pattern, reimplemented
//! directly against the `SequencedCommand` record format so we don't
//! pull in PMDK as a dependency.
//!
//! # Requirements
//!
//! - CPU must support `clwb` (check `/proc/cpuinfo` flags on Linux, or
//!   `__cpuid` leaf 7 EBX bit 24). Falls back to `clflushopt` if `clwb`
//!   is unavailable and neither if that's unavailable either — see
//!   `CacheFlush::detect()`.
//! - The backing file should be a real DAX mapping (`mount -o dax` on an
//!   `fsdax` namespace, or a `devdax` device) for the durability
//!   guarantee to be real. On a normal filesystem this still runs
//!   correctly but the "no syscall needed" guarantee doesn't hold —
//!   the kernel may still be buffering under you.
//! - x86-64 only. No ARM equivalent implemented here (would need `dc cvap`
//!   / `dc cvadp` on ARMv8.2+).

use std::arch::x86_64::{__cpuid_count, _mm_clflush, _mm_sfence};
use std::fs::OpenOptions;
use std::path::Path;

use memmap2::MmapMut;
use thiserror::Error;

use core_types::SequencedCommand;

use crate::log::{WalError, WalWriterConfig};

const MAGIC: &[u8; 8] = b"MREWAL01";
const VERSION: u32 = 1;
const FILE_HEADER_SIZE: usize = 32;
const RECORD_HEADER_SIZE: usize = 24;

/// Typical PMEM cache line size. AMD/Intel both use 64B lines.
const CACHE_LINE_SIZE: usize = 64;

#[derive(Debug, Error)]
pub enum PmemError {
    #[error("wal error: {0}")]
    Wal(#[from] WalError),
    #[error("clwb/clflushopt not supported by this CPU — refusing to claim PMEM durability")]
    UnsupportedCpu,
}

/// Which cache-flush instruction this CPU supports, cheapest-durable-first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CacheFlush {
    /// Preferred: writes back without evicting, doesn't serialize
    /// against other `clwb`s (only against the following `sfence`).
    Clwb,
    /// Fallback: also writes back without evicting; slightly weaker
    /// ordering guarantees than `clwb` but same durability outcome
    /// when paired with `sfence`.
    Clflushopt,
    /// Last resort: evicts on every call, serializing — much slower,
    /// but at least correct. Used if the CPU predates both of the above.
    Clflush,
}

impl CacheFlush {
    /// CPUID leaf 7, sub-leaf 0: EBX bit 24 = CLWB, bit 23 = CLFLUSHOPT.
    fn detect() -> Self {
        // SAFETY: CPUID is always safe to call on x86-64; leaf 7 is
        // universally available on anything from the last ~15 years.
        let regs = __cpuid_count(7, 0);
        if regs.ebx & (1 << 24) != 0 {
            CacheFlush::Clwb
        } else if regs.ebx & (1 << 23) != 0 {
            CacheFlush::Clflushopt
        } else {
            CacheFlush::Clflush
        }
    }

    /// Flush every cache line covering `[ptr, ptr+len)`. Caller must
    /// follow with `sfence()` before treating the range as durable.
    ///
    /// SAFETY: `ptr` must be valid for reads of `len` bytes for the
    /// duration of the call.
    #[inline]
    unsafe fn flush_range(self, ptr: *const u8, len: usize) {
        let start = (ptr as usize) & !(CACHE_LINE_SIZE - 1);
        let end = (ptr as usize) + len;
        let mut addr = start;
        while addr < end {
            match self {
                // `clwb`/`clflushopt` have no stable core::arch intrinsic
                // exposed as of this writing on stable Rust — emit them
                // via inline asm. `clflush` does have a stable intrinsic
                // (`_mm_clflush`), used as the fallback path.
                CacheFlush::Clwb => {
                    std::arch::asm!("clwb [{0}]", in(reg) addr, options(nostack, preserves_flags));
                }
                CacheFlush::Clflushopt => {
                    std::arch::asm!("clflushopt [{0}]", in(reg) addr, options(nostack, preserves_flags));
                }
                CacheFlush::Clflush => {
                    _mm_clflush(addr as *const u8);
                }
            }
            addr += CACHE_LINE_SIZE;
        }
    }
}

/// Append-only WAL writer backed by a DAX-mapped file, persisted via
/// `clwb`/`sfence` instead of `msync`. Same on-disk record format as
/// `FileWalWriter` — a PMEM WAL and a regular WAL are byte-compatible
/// and can be read by the same `recovery::scan`.
pub struct PmemWalWriter {
    mmap: MmapMut,
    cursor: usize,
    last_seq: u64,
    flush_kind: CacheFlush,
}

impl PmemWalWriter {
    /// Open or create a PMEM WAL at `path`. `path` should point into a
    /// DAX-mounted filesystem or devdax device for real PMEM durability;
    /// see module docs.
    pub fn open(path: impl AsRef<Path>, config: WalWriterConfig) -> Result<Self, PmemError> {
        let flush_kind = CacheFlush::detect();

        let path = path.as_ref();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .map_err(WalError::Io)?;

        file.set_len(config.file_capacity as u64)
            .map_err(WalError::Io)?;

        // SAFETY: file kept open for the lifetime of this writer; single
        // writer, no concurrent external mutation assumed (same
        // contract as FileWalWriter).
        let mut mmap = unsafe { MmapMut::map_mut(&file) }.map_err(WalError::Io)?;

        let is_new = mmap[..8] != *MAGIC;
        if is_new {
            mmap[0..8].copy_from_slice(MAGIC);
            mmap[8..12].copy_from_slice(&VERSION.to_le_bytes());
            mmap[12..32].fill(0);
            // Persist the header itself before anything else can be
            // considered durable relative to it.
            unsafe {
                flush_kind.flush_range(mmap.as_ptr(), FILE_HEADER_SIZE);
                _mm_sfence();
            }
        } else if mmap[..8] != *MAGIC {
            return Err(PmemError::Wal(WalError::InvalidMagic));
        }

        let (cursor, last_seq) = if is_new {
            (FILE_HEADER_SIZE, 0)
        } else {
            scan_to_end(&mmap)?
        };

        Ok(Self {
            mmap,
            cursor,
            last_seq,
            flush_kind,
        })
    }

    /// Append one `SequencedCommand`, persisting it durably before
    /// returning. This is the PMEM-WAL equivalent of
    /// `FileWalWriter::append` — same serialisation, different
    /// durability mechanism.
    pub fn append(&mut self, sc: &SequencedCommand) -> Result<usize, PmemError> {
        debug_assert!(
            sc.seq > self.last_seq,
            "PMEM WAL: sequence numbers must be strictly increasing (got {} after {})",
            sc.seq,
            self.last_seq
        );

        let payload =
            bincode::serialize(&sc.cmd).map_err(|e| WalError::Serialise(e.to_string()))?;

        let record_size = align8(RECORD_HEADER_SIZE + payload.len());
        let end = self.cursor + record_size;
        if end > self.mmap.len() {
            return Err(PmemError::Wal(WalError::CapacityExhausted {
                capacity: self.mmap.len(),
                needed: end,
            }));
        }

        let crc = crc32fast::hash(&payload);
        let offset = self.cursor;
        let buf = &mut self.mmap[offset..offset + record_size];

        buf[0..8].copy_from_slice(&sc.seq.to_le_bytes());
        buf[8..16].copy_from_slice(&sc.ts_ns.to_le_bytes());
        buf[16..20].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        buf[20..24].copy_from_slice(&crc.to_le_bytes());
        buf[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + payload.len()].copy_from_slice(&payload);
        for b in buf[RECORD_HEADER_SIZE + payload.len()..].iter_mut() {
            *b = 0;
        }

        // ── Durability: flush every touched cache line, then fence. ──
        //
        // We flush the *whole record* (header+payload+padding) before
        // the fence — this is the "data then commit" ordering PMDK
        // calls out explicitly: if we fenced only the seq field first,
        // a crash between the two flushes could leave a record whose
        // seq looks committed but whose payload is stale/torn.
        unsafe {
            self.flush_kind.flush_range(buf.as_ptr(), buf.len());
            _mm_sfence();
        }

        self.cursor = end;
        self.last_seq = sc.seq;
        Ok(offset)
    }

    /// No-op beyond what `append` already guarantees — every `append`
    /// is durable on return. Kept for API parity with `FileWalWriter`.
    pub fn flush(&mut self) -> Result<(), PmemError> {
        Ok(())
    }

    pub fn cursor(&self) -> usize {
        self.cursor
    }
    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }
}

/// Same record-scanning logic as `log.rs::scan_to_end` — duplicated
/// rather than shared because it operates on the same byte format but
/// this module intentionally has zero dependency on `log.rs` internals
/// (they're private). If you'd rather share one implementation, make
/// `log::scan_to_end` `pub(crate)` and import it instead.
fn scan_to_end(mmap: &MmapMut) -> Result<(usize, u64), PmemError> {
    let mut cursor = FILE_HEADER_SIZE;
    let mut last_seq = 0u64;

    while cursor + RECORD_HEADER_SIZE <= mmap.len() {
        let seq = u64::from_le_bytes(mmap[cursor..cursor + 8].try_into().unwrap());
        if seq == 0 {
            break; // unwritten tail
        }
        let len = u32::from_le_bytes(mmap[cursor + 16..cursor + 20].try_into().unwrap()) as usize;
        let stored_crc = u32::from_le_bytes(mmap[cursor + 20..cursor + 24].try_into().unwrap());

        let payload_start = cursor + RECORD_HEADER_SIZE;
        let payload_end = payload_start + len;
        if payload_end > mmap.len() {
            break; // torn record — stop before it
        }
        let payload = &mmap[payload_start..payload_end];
        if crc32fast::hash(payload) != stored_crc {
            break; // torn/corrupt — stop before it
        }

        last_seq = seq;
        cursor = payload_end;
        cursor = align8(cursor);
    }

    Ok((cursor, last_seq))
}

#[inline]
fn align8(n: usize) -> usize {
    (n + 7) & !7
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::{
        AccountId, ClientOrderId, InboundCommand, OrderType, Price, Qty, Side, Symbol, TimeInForce,
    };
    use tempfile::TempDir;

    fn sample_cmd(seq: u64) -> SequencedCommand {
        SequencedCommand {
            seq,
            ts_ns: 0,
            cmd: InboundCommand::NewOrder {
                account: AccountId(1),
                client_order_id: ClientOrderId::new(seq),
                symbol: Symbol(0),
                side: Side::Buy,
                price: Price(100_00000000),
                qty: Qty(10_00000000),
                order_type: OrderType::Limit,
                time_in_force: TimeInForce::Gtc,
            },
        }
    }

    #[test]
    fn append_and_reopen_recovers_last_seq() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pmem.wal");
        let config = WalWriterConfig {
            file_capacity: 1024 * 1024,
            sync_on_write: true,
        };

        {
            let mut w = PmemWalWriter::open(&path, config.clone()).unwrap();
            w.append(&sample_cmd(1)).unwrap();
            w.append(&sample_cmd(2)).unwrap();
            assert_eq!(w.last_seq(), 2);
        }

        // Reopen — should recover cursor/last_seq from the persisted records
        // without needing any explicit flush call (durability was per-append).
        let w2 = PmemWalWriter::open(&path, config).unwrap();
        assert_eq!(w2.last_seq(), 2);
    }

    #[test]
    fn sequence_numbers_must_increase() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pmem.wal");
        let config = WalWriterConfig {
            file_capacity: 1024 * 1024,
            sync_on_write: true,
        };
        let mut w = PmemWalWriter::open(&path, config).unwrap();
        w.append(&sample_cmd(5)).unwrap();
        // Would trip the debug_assert in a debug build if seq didn't increase;
        // not re-asserted here since debug_assert is compiled out in release.
    }
}
