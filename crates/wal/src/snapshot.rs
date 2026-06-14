// Snapshot serialization and writing utilities
//! Periodic per-shard snapshots using rkyv zero-copy serialisation.
//!
//! # Design
//! Every thread (matching engine, risk shard) independently serialises its
//! own state when it processes a `SnapshotMarker(seq)` event. Because every
//! thread snapshots at the **same logical sequence number**, recovery can load
//! each shard's snapshot in parallel and then replay the WAL from
//! `snapshot.seq + 1` — no cross-thread coordination is required.
//!
//! # File layout
//! ```text
//! [ SNAPSHOT HEADER (48 bytes) ]
//! [ rkyv payload               ]
//! ```
//!
//! ## Snapshot header
//! ```text
//! magic:       [u8; 8]   "MRESN001"
//! version:     u32       = 1
//! shard_id:    u32       — which shard/engine produced this snapshot
//! seq:         u64       — WAL sequence number at snapshot time
//! ts_ns:       u64       — wall-clock timestamp
//! payload_len: u32       — byte length of rkyv payload
//! crc32:       u32       — CRC32 of payload bytes
//! ```

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crc32fast::Hasher as Crc32Hasher;
use thiserror::Error;

const SNAP_MAGIC: &[u8; 8] = b"MRESN001";
const SNAP_VERSION: u32     = 1;
const SNAP_HEADER_SIZE: usize = 48;

#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid snapshot magic")]
    InvalidMagic,
    #[error("unsupported snapshot version: {0}")]
    UnsupportedVersion(u32),
    #[error("CRC32 mismatch: stored={stored} computed={computed}")]
    Crc32Mismatch { stored: u32, computed: u32 },
    #[error("serialisation error: {0}")]
    Serialise(String),
    #[error("payload too large: {0} bytes")]
    PayloadTooLarge(usize),
}

/// Metadata stored in the snapshot header.
#[derive(Clone, Debug)]
pub struct SnapshotMeta {
    pub shard_id:    u32,
    pub seq:         u64,
    pub ts_ns:       u64,
    pub payload_len: u32,
    pub crc32:       u32,
}

/// A fully loaded snapshot: header metadata + raw rkyv payload bytes.
#[derive(Clone, Debug)]
pub struct Snapshot {
    pub meta:    SnapshotMeta,
    pub payload: Vec<u8>,
}

/// Writes snapshots to disk.
pub struct SnapshotWriter {
    /// Directory where snapshot files are written.
    dir: PathBuf,
    /// Maximum payload size (bytes). Payloads larger than this are rejected.
    max_payload: usize,
}

impl SnapshotWriter {
    pub fn new(dir: impl Into<PathBuf>, max_payload: usize) -> Self {
        Self { dir: dir.into(), max_payload }
    }

    /// Write `payload` (rkyv-serialised shard state) as a snapshot for
    /// `shard_id` at WAL sequence `seq`.
    ///
    /// The file is written atomically: we write to a `.tmp` file then rename.
    /// This ensures readers never see a partially written snapshot.
    pub fn write(
        &self,
        shard_id: u32,
        seq:      u64,
        ts_ns:    u64,
        payload:  &[u8],
    ) -> Result<PathBuf, SnapshotError> {
        if payload.len() > self.max_payload {
            return Err(SnapshotError::PayloadTooLarge(payload.len()));
        }

        fs::create_dir_all(&self.dir)?;

        let final_path = self.snapshot_path(shard_id, seq);
        let tmp_path   = final_path.with_extension("tmp");

        let crc = {
            let mut h = Crc32Hasher::new();
            h.update(payload);
            h.finalize()
        };

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;

        // Write header.
        let mut header = [0u8; SNAP_HEADER_SIZE];
        header[0..8].copy_from_slice(SNAP_MAGIC);
        header[8..12].copy_from_slice(&SNAP_VERSION.to_le_bytes());
        header[12..16].copy_from_slice(&shard_id.to_le_bytes());
        header[16..24].copy_from_slice(&seq.to_le_bytes());
        header[24..32].copy_from_slice(&ts_ns.to_le_bytes());
        header[32..36].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        header[36..40].copy_from_slice(&crc.to_le_bytes());
        // bytes 40..48 reserved / zero

        file.write_all(&header)?;
        file.write_all(payload)?;
        file.sync_data()?;
        drop(file);

        fs::rename(&tmp_path, &final_path)?;
        Ok(final_path)
    }

    /// Canonical path for a snapshot file.
    fn snapshot_path(&self, shard_id: u32, seq: u64) -> PathBuf {
        self.dir.join(format!("shard_{shard_id:04}_{seq:020}.snap"))
    }
}

/// Read and validate a snapshot file from `path`.
pub fn read_snapshot(path: impl AsRef<Path>) -> Result<Snapshot, SnapshotError> {
    let mut file = File::open(path)?;
    let mut header = [0u8; SNAP_HEADER_SIZE];
    file.read_exact(&mut header)?;

    if &header[0..8] != SNAP_MAGIC {
        return Err(SnapshotError::InvalidMagic);
    }
    let version = u32::from_le_bytes(header[8..12].try_into().unwrap());
    if version != SNAP_VERSION {
        return Err(SnapshotError::UnsupportedVersion(version));
    }

    let shard_id    = u32::from_le_bytes(header[12..16].try_into().unwrap());
    let seq         = u64::from_le_bytes(header[16..24].try_into().unwrap());
    let ts_ns       = u64::from_le_bytes(header[24..32].try_into().unwrap());
    let payload_len = u32::from_le_bytes(header[32..36].try_into().unwrap());
    let crc_stored  = u32::from_le_bytes(header[36..40].try_into().unwrap());

    let mut payload = vec![0u8; payload_len as usize];
    file.read_exact(&mut payload)?;

    let crc_computed = {
        let mut h = Crc32Hasher::new();
        h.update(&payload);
        h.finalize()
    };

    if crc_stored != crc_computed {
        return Err(SnapshotError::Crc32Mismatch {
            stored:   crc_stored,
            computed: crc_computed,
        });
    }

    Ok(Snapshot {
        meta: SnapshotMeta { shard_id, seq, ts_ns, payload_len, crc32: crc_stored },
        payload,
    })
}

/// Find the most recent valid snapshot for `shard_id` in `dir`.
///
/// Snapshots are named `shard_NNNN_SSSSSSSSSSSSSSSSSSSS.snap` where the
/// second component is the sequence number zero-padded to 20 digits, so a
/// lexicographic sort gives us chronological order.
pub fn latest_snapshot(
    dir: impl AsRef<Path>,
    shard_id: u32,
) -> Result<Option<Snapshot>, SnapshotError> {
    let dir = dir.as_ref();
    if !dir.exists() {
        return Ok(None);
    }

    let prefix = format!("shard_{shard_id:04}_");
    let mut candidates: Vec<PathBuf> = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.extension().map_or(false, |ext| ext == "snap")
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map_or(false, |n| n.starts_with(&prefix))
        })
        .collect();

    // Lexicographic sort = chronological order (zero-padded seq in filename).
    candidates.sort();

    // Walk from newest to oldest, return first valid snapshot.
    for path in candidates.into_iter().rev() {
        match read_snapshot(&path) {
            Ok(snap) => return Ok(Some(snap)),
            Err(_)   => continue, // corrupt file — try the previous one
        }
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn write_and_read_snapshot() {
        let dir = TempDir::new().unwrap();
        let writer = SnapshotWriter::new(dir.path(), 1024 * 1024);

        let payload = b"hello snapshot world";
        let path = writer.write(0, 42, 999, payload).unwrap();

        let snap = read_snapshot(&path).unwrap();
        assert_eq!(snap.meta.shard_id,    0);
        assert_eq!(snap.meta.seq,         42);
        assert_eq!(snap.meta.ts_ns,       999);
        assert_eq!(snap.payload,          payload);
    }

    #[test]
    fn latest_snapshot_finds_newest() {
        let dir = TempDir::new().unwrap();
        let writer = SnapshotWriter::new(dir.path(), 1024 * 1024);

        writer.write(0,  10, 0, b"snap at seq 10").unwrap();
        writer.write(0, 100, 0, b"snap at seq 100").unwrap();
        writer.write(0,  50, 0, b"snap at seq 50").unwrap();

        let snap = latest_snapshot(dir.path(), 0).unwrap().unwrap();
        assert_eq!(snap.meta.seq, 100, "should return the highest-seq snapshot");
    }

    #[test]
    fn crc_mismatch_detected() {
        let dir = TempDir::new().unwrap();
        let writer = SnapshotWriter::new(dir.path(), 1024 * 1024);
        let path = writer.write(1, 1, 0, b"good payload").unwrap();

        // Corrupt the payload byte in the file.
        let mut data = fs::read(&path).unwrap();
        let payload_start = SNAP_HEADER_SIZE;
        data[payload_start] ^= 0xFF;
        fs::write(&path, &data).unwrap();

        let err = read_snapshot(&path).unwrap_err();
        assert!(matches!(err, SnapshotError::Crc32Mismatch { .. }));
    }

    #[test]
    fn no_snapshot_returns_none() {
        let dir = TempDir::new().unwrap();
        let result = latest_snapshot(dir.path(), 99).unwrap();
        assert!(result.is_none());
    }
}