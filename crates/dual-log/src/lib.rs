//! Dual Write-Ahead Log infrastructure for the dual-engine matching system.
//!
//! This crate provides:
//! - `DualLog`: writes every order to two independent WALs simultaneously
//! - `PendingRing`: fixed-size ring buffer holding only pending orders for O(1) scanning
//!
//! Together these components implement the "dual log write" and "ring buffer
//! pending queue" described in the architecture document.

pub mod dual_log;
pub mod pending_ring;

pub use dual_log::{DualLog, DualLogConfig, DualLogError};
pub use pending_ring::PendingRing;
