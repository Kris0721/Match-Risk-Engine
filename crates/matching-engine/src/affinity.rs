// Thread affinity and core pinning configuration
//! CPU pinning helpers for the matching engine's hot thread.

#[cfg(target_os = "linux")]
pub fn pin_to_core(core_id: usize) -> std::io::Result<()> {
    use std::mem;

    unsafe {
        let mut set: libc_cpu_set_t = mem::zeroed();
        cpu_set(core_id, &mut set);
        let pid = 0; // current thread
        let rc = sched_setaffinity(pid, mem::size_of::<libc_cpu_set_t>(), &set);
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn pin_to_core(_core_id: usize) -> std::io::Result<()> {
    // No-op on non-Linux targets.
    Ok(())
}

// Minimal cpu_set_t / sched_setaffinity bindings to avoid pulling in libc
// as a dependency for a single syscall.
#[cfg(target_os = "linux")]
#[repr(C)]
struct libc_cpu_set_t {
    bits: [u64; 16], // supports up to 1024 CPUs
}

#[cfg(target_os = "linux")]
unsafe fn cpu_set(cpu: usize, set: &mut libc_cpu_set_t) {
    let idx = cpu / 64;
    let bit = cpu % 64;
    if idx < set.bits.len() {
        set.bits[idx] |= 1u64 << bit;
    }
}

#[cfg(target_os = "linux")]
extern "C" {
    fn sched_setaffinity(pid: i32, cpusetsize: usize, mask: *const libc_cpu_set_t) -> i32;
}

/// Park the thread on the given core and set its name (for `top -H` / debugging).
pub fn setup_hot_thread(core_id: Option<usize>, name: &str) {
    if let Some(id) = core_id {
        if let Err(e) = pin_to_core(id) {
            eprintln!("warning: failed to pin {} to core {}: {}", name, id, e);
        }
    }
}