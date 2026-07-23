//! DPDK Environment Abstraction Layer bootstrap.
//!
//! `rte_eal_init` parses its own argv (hugepage config, core mask,
//! PCI allowlist, etc.) — these normally come from the process's
//! deployment config, not user input, since a bad EAL arg can crash
//! the whole NIC binding.

use std::ffi::CString;
use std::os::raw::c_char;

use dpdk_sys::rte_eal_init;

#[derive(Debug, thiserror::Error)]
pub enum EalError {
    #[error("rte_eal_init failed, rc={0}")]
    InitFailed(i32),
}

/// Owns the fact that EAL has been initialized. Only ever construct
/// one of these per process — DPDK EAL is not designed to be
/// re-initialized.
pub struct Eal {
    _private: (),
}

impl Eal {
    /// `args` mirrors what you'd pass on argv, e.g.:
    /// `["gateway", "-l", "0-3", "-n", "4", "--proc-type=primary"]`
    pub fn init(args: &[&str]) -> Result<Self, EalError> {
        let c_args: Vec<CString> = args
            .iter()
            .map(|a| CString::new(*a).expect("EAL arg contains NUL"))
            .collect();

        let mut argv: Vec<*mut c_char> = c_args.iter().map(|s| s.as_ptr() as *mut c_char).collect();

        let rc = unsafe { rte_eal_init(argv.len() as i32, argv.as_mut_ptr()) };
        if rc < 0 {
            return Err(EalError::InitFailed(rc));
        }

        Ok(Self { _private: () })
    }
}
