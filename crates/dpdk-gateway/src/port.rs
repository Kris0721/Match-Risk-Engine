use dpdk_sys::{
    rte_eth_conf, rte_eth_dev_configure, rte_eth_dev_start, rte_eth_rx_queue_setup,
    rte_eth_tx_queue_setup, rte_mempool,
};

#[derive(Debug, thiserror::Error)]
pub enum PortError {
    #[error("rte_eth_dev_configure failed, rc={0}")]
    ConfigureFailed(i32),
    #[error("rte_eth_rx_queue_setup failed, rc={0}")]
    RxQueueFailed(i32),
    #[error("rte_eth_tx_queue_setup failed, rc={0}")]
    TxQueueFailed(i32),
    #[error("rte_eth_dev_start failed, rc={0}")]
    StartFailed(i32),
}

pub struct PortConfig {
    pub port_id: u16,
    pub n_rx_queues: u16,
    pub n_tx_queues: u16,
    pub rx_ring_size: u16,
    pub tx_ring_size: u16,
}

/// Bring a NIC port up in poll-mode: no RX interrupts, matching engine
/// core spins on `rte_eth_rx_burst` instead of blocking on epoll —
/// this is the actual latency win over the tokio TCP path.
pub fn init_port(cfg: &PortConfig, pool: &crate::mempool::MbufPool) -> Result<(), PortError> {
    let mut eth_conf: rte_eth_conf = unsafe { std::mem::zeroed() };
    // Leave RSS/offload fields zeroed for a minimal single-queue-per-core
    // setup; extend here if you need RSS hashing across multiple RX
    // queues for multi-core fan-out.

    let rc =
        unsafe { rte_eth_dev_configure(cfg.port_id, cfg.n_rx_queues, cfg.n_tx_queues, &eth_conf) };
    if rc != 0 {
        return Err(PortError::ConfigureFailed(rc));
    }

    for q in 0..cfg.n_rx_queues {
        let rc = unsafe {
            rte_eth_rx_queue_setup(
                cfg.port_id,
                q,
                cfg.rx_ring_size,
                dpdk_sys::rte_eth_dev_socket_id(cfg.port_id) as u32,
                std::ptr::null(),
                pool.as_ptr(),
            )
        };
        if rc != 0 {
            return Err(PortError::RxQueueFailed(rc));
        }
    }

    for q in 0..cfg.n_tx_queues {
        let rc = unsafe {
            rte_eth_tx_queue_setup(
                cfg.port_id,
                q,
                cfg.tx_ring_size,
                dpdk_sys::rte_eth_dev_socket_id(cfg.port_id) as u32,
                std::ptr::null(),
            )
        };
        if rc != 0 {
            return Err(PortError::TxQueueFailed(rc));
        }
    }

    let rc = unsafe { rte_eth_dev_start(cfg.port_id) };
    if rc != 0 {
        return Err(PortError::StartFailed(rc));
    }

    Ok(())
}
