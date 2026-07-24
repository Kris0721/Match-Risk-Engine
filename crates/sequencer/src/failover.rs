//! Leader election and fencing for the Sequencer.
//!
//! # Design
//! Exactly one `Sequencer` process may dispatch commands at a time. Leadership
//! is arbitrated via an OS-level advisory lock (`flock`) on a shared lease
//! file. This is deliberately simple rather than a full consensus protocol:
//!
//! - `flock` locks are held by the OS per-file-descriptor and are released
//!   automatically when the holding process dies or closes the fd — even on
//!   SIGKILL. That gives us crash-detection for free: a standby's blocking
//!   attempt to acquire the lock unblocks the instant the leader dies, with
//!   no heartbeat-timeout window to tune and no split-brain during the
//!   detection window.
//! - The lease file also stores a monotonically increasing `term` (fencing
//!   token). Every time leadership is acquired, `term` is incremented and
//!   persisted before the caller is told it is leader. Downstream components
//!   that accept writes from the Sequencer (WAL writer, matching engines)
//!   should tag state with `term` and reject anything from a lower term —
//!   this guards against a "zombie" leader whose flock-holding process is
//!   merely stalled (e.g. STW GC pause, kernel freeze) and not actually dead,
//!   which is the one case `flock` alone cannot rule out.
//!
//! # Production note
//! `FileLeaseBackend` assumes a single host (or a shared filesystem that
//! supports real `flock` semantics, e.g. NFSv4 — NOT NFSv3, and NOT most
//! object stores). For a multi-host deployment without such a filesystem,
//! implement `LeaseBackend` against an external coordination service
//! (etcd/Consul/ZooKeeper) instead — the trait boundary here is exactly
//! where that swap happens; nothing else in the Sequencer needs to change.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use fs2::FileExt;

/// This node's role in the leader/standby pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Role {
    Standby = 0,
    Leader = 1,
}

/// Pluggable lease acquisition backend. `FileLeaseBackend` below is the
/// single-host implementation; swap in a distributed one for multi-host.
pub trait LeaseBackend: Send + 'static {
    /// Block until the lease is acquired (returns the new fencing term),
    /// or the shutdown flag is set (returns `None`).
    fn acquire(&mut self, shutdown: &std::sync::atomic::AtomicBool) -> Option<u64>;

    /// Best-effort release, called on graceful step-down. Crash-release is
    /// handled automatically by the backend (e.g. OS `flock` semantics) and
    /// does not depend on this being called.
    fn release(&mut self);
}

/// `flock`-based lease on a local (or NFSv4-shared) file.
pub struct FileLeaseBackend {
    path: PathBuf,
    file: Option<File>,
}

impl FileLeaseBackend {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            file: None,
        }
    }

    fn open_or_create(&self) -> std::io::Result<File> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&self.path)
    }

    fn read_term(file: &mut File) -> u64 {
        let mut buf = [0u8; 8];
        file.seek(SeekFrom::Start(0)).ok();
        if file.read_exact(&mut buf).is_ok() {
            u64::from_le_bytes(buf)
        } else {
            0
        }
    }

    fn write_term(file: &mut File, term: u64) -> std::io::Result<()> {
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&term.to_le_bytes())?;
        file.sync_data()?;
        Ok(())
    }
}

impl LeaseBackend for FileLeaseBackend {
    fn acquire(&mut self, shutdown: &std::sync::atomic::AtomicBool) -> Option<u64> {
        loop {
            if shutdown.load(Ordering::Acquire) {
                return None;
            }

            let mut file = self
                .open_or_create()
                .expect("failover: cannot open lease file");

            // Poll try_lock instead of a blocking lock_exclusive() so we can
            // still observe the shutdown flag while waiting.
            match file.try_lock_exclusive() {
                Ok(()) => {
                    let term = Self::read_term(&mut file) + 1;
                    Self::write_term(&mut file, term)
                        .expect("failover: cannot persist fencing term");
                    self.file = Some(file);
                    return Some(term);
                }
                Err(_) => {
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        }
    }

    fn release(&mut self) {
        if let Some(file) = self.file.take() {
            let _ = fs2::FileExt::unlock(&file);
        }
    }
}

/// Shared, cheaply-cloneable handle the Sequencer's hot loop polls to decide
/// whether it's allowed to dispatch. The actual (blocking) lease acquisition
/// runs on a dedicated background thread — the hot loop only ever does a
/// non-blocking atomic load.
#[derive(Clone)]
pub struct RoleHandle {
    role: Arc<AtomicU8>,
    term: Arc<AtomicU64>,
}

impl RoleHandle {
    #[inline]
    pub fn role(&self) -> Role {
        match self.role.load(Ordering::Acquire) {
            1 => Role::Leader,
            _ => Role::Standby,
        }
    }

    #[inline]
    pub fn is_leader(&self) -> bool {
        self.role() == Role::Leader
    }

    /// Current fencing term. Attach this to WAL writes / privileged commands
    /// so downstream components can reject stale-leader traffic.
    #[inline]
    pub fn term(&self) -> u64 {
        self.term.load(Ordering::Acquire)
    }
}

impl RoleHandle {
    /// A handle permanently pinned to `Leader`, for unit tests that don't
    /// exercise the election machinery itself. Not for production use —
    /// this bypasses lease acquisition entirely.
    #[cfg(test)]
    pub fn for_test_leader() -> Self {
        Self {
            role: Arc::new(AtomicU8::new(Role::Leader as u8)),
            term: Arc::new(AtomicU64::new(1)),
        }
    }
}

/// Owns the background election thread. Drop this to step down and stop
/// contending for leadership (e.g. on graceful shutdown).
pub struct FailoverController {
    handle: RoleHandle,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl FailoverController {
    /// Spawn the election thread. `backend` is consumed by that thread.
    pub fn spawn(mut backend: impl LeaseBackend) -> Self {
        let role = Arc::new(AtomicU8::new(Role::Standby as u8));
        let term = Arc::new(AtomicU64::new(0));
        let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let handle = RoleHandle {
            role: role.clone(),
            term: term.clone(),
        };

        let thread_shutdown = shutdown.clone();
        let thread = std::thread::Builder::new()
            .name("sequencer-election".into())
            .spawn(move || {
                // This blocks (via the backend's internal poll loop) until the
                // lease is acquired or shutdown is requested. There is
                // deliberately no automatic re-election loop after step-down:
                // once this process loses leadership it stays Standby and
                // process supervision (systemd/k8s) is responsible for
                // deciding whether to restart it as a fresh contender.
                if let Some(acquired_term) = backend.acquire(&thread_shutdown) {
                    term.store(acquired_term, Ordering::Release);
                    role.store(Role::Leader as u8, Ordering::Release);
                    eprintln!("[failover] acquired leadership, term={acquired_term}");
                }
                // Park until shutdown; release happens in Drop of the caller
                // via `backend.release()` below, or automatically on crash.
                while !thread_shutdown.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(100));
                }
                backend.release();
            })
            .expect("failed to spawn election thread");

        Self {
            handle,
            shutdown,
            thread: Some(thread),
        }
    }

    pub fn handle(&self) -> RoleHandle {
        self.handle.clone()
    }
}

impl Drop for FailoverController {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}
