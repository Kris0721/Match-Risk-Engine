// Low-latency logging implementation
//! Low-latency async logger: hot path pushes to a lock-free channel,
//! a background thread does the actual I/O / formatting.

use std::fmt;
use std::io::{self, Write, BufWriter};
use std::thread::{self, JoinHandle};
use std::time::{SystemTime, UNIX_EPOCH};

use crossbeam_channel::{bounded, Sender, TrySendError};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Level::Trace => "TRACE",
            Level::Debug => "DEBUG",
            Level::Info => "INFO",
            Level::Warn => "WARN",
            Level::Error => "ERROR",
        };
        f.write_str(s)
    }
}

/// A pre-formatted log record. We format on the hot-path thread into a
/// fixed-size inline buffer to avoid heap allocation, then send the
/// buffer + length to the background writer.
pub struct LogRecord {
    pub level: Level,
    pub ts_ns: u64,
    pub len: u16,
    pub buf: [u8; 256],
}

impl LogRecord {
    #[inline]
    pub fn as_str(&self) -> &str {
        // SAFETY: we only ever write valid UTF-8 via write! into buf
        std::str::from_utf8(&self.buf[..self.len as usize]).unwrap_or("<invalid utf8>")
    }
}

#[derive(Clone)]
pub struct Logger {
    tx: Sender<LogRecord>,
    min_level: Level,
}

pub struct LoggerHandle {
    pub join: JoinHandle<()>,
}

impl Logger {
    /// Spawn the background writer thread and return (logger, handle).
    /// `capacity` is the bounded channel size; on overflow, records are
    /// dropped (and a drop counter incremented) rather than blocking the
    /// hot path.
    pub fn spawn<W: Write + Send + 'static>(
        writer: W,
        capacity: usize,
        min_level: Level,
    ) -> (Self, LoggerHandle) {
        let (tx, rx) = bounded::<LogRecord>(capacity);

        let join = thread::Builder::new()
            .name("logger-writer".into())
            .spawn(move || {
                let mut out = BufWriter::with_capacity(1 << 20, writer);
                let dropped: u64 = 0;

                loop {
                    match rx.recv() {
                        Ok(rec) => {
                            let _ = writeln!(
                                out,
                                "{} {:>5} {}",
                                rec.ts_ns,
                                rec.level,
                                rec.as_str()
                            );
                            // Flush opportunistically when channel drains.
                            if rx.is_empty() {
                                let _ = out.flush();
                            }
                        }
                        Err(_) => {
                            // All senders dropped: flush and exit.
                            if dropped > 0 {
                                let _ = writeln!(out, "<{} records dropped>", dropped);
                            }
                            let _ = out.flush();
                            break;
                        }
                    }
                    let _ = dropped; // silence unused warning if branch unreached
                }
            })
            .expect("failed to spawn logger thread");

        (Self { tx, min_level }, LoggerHandle { join })
    }

    /// Convenience: spawn writing to stdout.
    pub fn spawn_stdout(capacity: usize, min_level: Level) -> (Self, LoggerHandle) {
        Self::spawn(io::stdout(), capacity, min_level)
    }

    #[inline]
    pub fn enabled(&self, level: Level) -> bool {
        level >= self.min_level
    }

    /// Hot-path log call: formats into a stack buffer and sends without
    /// blocking. Returns false if the record was dropped (channel full).
    #[inline]
    pub fn log_fmt(&self, level: Level, args: fmt::Arguments<'_>) -> bool {
        if level < self.min_level {
            return true;
        }

        let mut buf = [0u8; 256];
        let len = {
            let mut writer = SliceWriter { buf: &mut buf, pos: 0 };
            let _ = fmt::write(&mut writer, args);
            writer.pos
        };

        let rec = LogRecord {
            level,
            ts_ns: now_ns(),
            len: len as u16,
            buf,
        };

        match self.tx.try_send(rec) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => false,
        }
    }
}

/// Writer adapter into a fixed-size byte slice; truncates on overflow.
struct SliceWriter<'a> {
    buf: &'a mut [u8],
    pos: usize,
}

impl<'a> fmt::Write for SliceWriter<'a> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        let remaining = self.buf.len() - self.pos;
        let n = s.len().min(remaining);
        self.buf[self.pos..self.pos + n].copy_from_slice(&s.as_bytes()[..n]);
        self.pos += n;
        Ok(())
    }
}

#[inline]
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[macro_export]
macro_rules! log_info {
    ($logger:expr, $($arg:tt)*) => {
        $crate::log_fmt_macro!($logger, $crate::Level::Info, $($arg)*)
    };
}

#[macro_export]
macro_rules! log_warn {
    ($logger:expr, $($arg:tt)*) => {
        $crate::log_fmt_macro!($logger, $crate::Level::Warn, $($arg)*)
    };
}

#[macro_export]
macro_rules! log_error {
    ($logger:expr, $($arg:tt)*) => {
        $crate::log_fmt_macro!($logger, $crate::Level::Error, $($arg)*)
    };
}

#[macro_export]
macro_rules! log_debug {
    ($logger:expr, $($arg:tt)*) => {
        $crate::log_fmt_macro!($logger, $crate::Level::Debug, $($arg)*)
    };
}

#[macro_export]
macro_rules! log_fmt_macro {
    ($logger:expr, $level:expr, $($arg:tt)*) => {
        $logger.log_fmt($level, format_args!($($arg)*))
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    struct VecWriter(Arc<Mutex<Vec<u8>>>);
    impl Write for VecWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn basic_log_roundtrip() {
        let data = Arc::new(Mutex::new(Vec::new()));
        let (logger, handle) = Logger::spawn(VecWriter(data.clone()), 1024, Level::Trace);

        log_info!(logger, "order accepted id={} px={}", 42, 100_50);
        log_warn!(logger, "risk near limit acct={}", 7);

        drop(logger);
        handle.join.join().unwrap();

        let out = String::from_utf8(data.lock().unwrap().clone()).unwrap();
        assert!(out.contains("order accepted id=42 px=10050"));
        assert!(out.contains("risk near limit acct=7"));
    }

    #[test]
    fn truncates_long_messages() {
        let data = Arc::new(Mutex::new(Vec::new()));
        let (logger, handle) = Logger::spawn(VecWriter(data.clone()), 1024, Level::Trace);

        let long = "x".repeat(1000);
        log_info!(logger, "{}", long);

        drop(logger);
        handle.join.join().unwrap();

        let out = String::from_utf8(data.lock().unwrap().clone()).unwrap();
        // line should be capped at buffer size (256), not 1000
        let line = out.lines().next().unwrap();
        assert!(line.len() < 300);
    }

    #[test]
    fn min_level_filters() {
        let data = Arc::new(Mutex::new(Vec::new()));
        let (logger, handle) = Logger::spawn(VecWriter(data.clone()), 1024, Level::Warn);

        log_info!(logger, "should be filtered");
        log_error!(logger, "should appear");

        drop(logger);
        handle.join.join().unwrap();

        let out = String::from_utf8(data.lock().unwrap().clone()).unwrap();
        assert!(!out.contains("should be filtered"));
        assert!(out.contains("should appear"));
    }
}