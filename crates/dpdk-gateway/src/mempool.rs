use std::ffi::CString;
use std::ptr::NonNull;

use dpdk_sys::{rte_mempool, rte_pktmbuf_pool_create, rte_socket_id};

#[derive(Debug, thiserror::Error)]
pub enum MempoolError {
    #[error("rte_pktmbuf_pool_create returned null (check hugepage config)")]
    CreateFailed,
}

/// Pool of pre-allocated packet buffers (`rte_mbuf`s). RX/TX both draw
/// from this — sized for worst-case burst depth across all queues.
pub struct MbufPool {
    ptr: NonNull<rte_mempool>,
}

unsafe impl Send for MbufPool {}
unsafe impl Sync for MbufPool {}

impl MbufPool {
    pub fn new(
        name: &str,
        n_mbufs: u32,
        cache_size: u32,
        mbuf_data_room: u16,
    ) -> Result<Self, MempoolError> {
        let c_name = CString::new(name).unwrap();
        let socket_id = unsafe { rte_socket_id() };

        let ptr = unsafe {
            rte_pktmbuf_pool_create(
                c_name.as_ptr(),
                n_mbufs,
                cache_size,
                0, // no per-mbuf private data
                mbuf_data_room,
                socket_id,
            )
        };

        NonNull::new(ptr)
            .map(|ptr| Self { ptr })
            .ok_or(MempoolError::CreateFailed)
    }

    pub fn as_ptr(&self) -> *mut rte_mempool {
        self.ptr.as_ptr()
    }
}
