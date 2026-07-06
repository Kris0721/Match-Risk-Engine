#[cfg(not(feature = "loom"))]
use std::sync::atomic::{AtomicU64, Ordering, fence};
#[cfg(feature = "loom")]
use loom::sync::atomic::{AtomicU64, AtomicI64, AtomicBool, AtomicU32, Ordering, fence};

use std::cell::UnsafeCell;

#[derive(Clone, Copy, Debug, Default)]
pub struct AccountRiskSnapshot {
    pub balance: i64,
    pub used_margin: i64,
    pub frozen: bool,
    pub halted: bool,
    pub position: i64,
    pub open_order_count: u32,
}

pub struct AccountRiskState {
    seq:               AtomicU64,
    balance:           UnsafeCell<i64>,
    used_margin:       UnsafeCell<i64>,
    frozen:            UnsafeCell<bool>,
    halted:            UnsafeCell<bool>,
    position:          UnsafeCell<i64>,
    open_order_count:  UnsafeCell<u32>,
    _pad: [u8; Self::PAD_BYTES],
}

impl AccountRiskState {
    const PAYLOAD_SIZE: usize =
        std::mem::size_of::<u64>()   // seq
        + std::mem::size_of::<i64>() // balance
        + std::mem::size_of::<i64>() // used_margin
        + std::mem::size_of::<bool>()// frozen
        + std::mem::size_of::<bool>()// halted
        + std::mem::size_of::<i64>() // position
        + std::mem::size_of::<u32>();// open_order_count

    const PAD_BYTES: usize = {
        let r = Self::PAYLOAD_SIZE % 64;
        if r == 0 { 0 } else { 64 - r }
    };

    pub fn new() -> Self {
        Self {
            seq:              AtomicU64::new(0),
            balance:          UnsafeCell::new(0),
            used_margin:      UnsafeCell::new(0),
            frozen:           UnsafeCell::new(false),
            halted:           UnsafeCell::new(false),
            position:         UnsafeCell::new(0),
            open_order_count: UnsafeCell::new(0),
            _pad:             [0u8; Self::PAD_BYTES],
        }
    }

    #[inline]
    pub fn update(&self, balance: i64, used_margin: i64, frozen: bool,
                  halted: bool, position: i64, open_order_count: u32) {
        let seq = self.seq.load(Ordering::Relaxed);
        debug_assert!(seq % 2 == 0, "seqlock: concurrent update");
        self.seq.store(seq.wrapping_add(1), Ordering::Relaxed);
        unsafe {
            *self.balance.get()          = balance;
            *self.used_margin.get()      = used_margin;
            *self.frozen.get()           = frozen;
            *self.halted.get()           = halted;
            *self.position.get()         = position;
            *self.open_order_count.get() = open_order_count;
        }
        fence(Ordering::Release);
        self.seq.store(seq.wrapping_add(2), Ordering::Release);
    }

    #[inline]
    pub fn read(&self) -> AccountRiskSnapshot {
        loop {
            let seq1 = self.seq.load(Ordering::Acquire);
            if seq1 % 2 != 0 { std::hint::spin_loop(); continue; }
            let balance          = unsafe { *self.balance.get() };
            let used_margin      = unsafe { *self.used_margin.get() };
            let frozen           = unsafe { *self.frozen.get() };
            let halted           = unsafe { *self.halted.get() };
            let position         = unsafe { *self.position.get() };
            let open_order_count = unsafe { *self.open_order_count.get() };
            fence(Ordering::Acquire);
            let seq2 = self.seq.load(Ordering::Acquire);
            if seq1 == seq2 {
                return AccountRiskSnapshot {
                    balance, used_margin, frozen,
                    halted, position, open_order_count,
                };
            }
            std::hint::spin_loop();
        }
    }

    #[inline] pub fn is_frozen(&self)         -> bool { self.read().frozen }
    #[inline] pub fn is_halted(&self)         -> bool { self.read().halted }
    #[inline] pub fn position(&self)          -> i64  { self.read().position }
    #[inline] pub fn open_order_count(&self)  -> u32  { self.read().open_order_count }

    /// Convenience setter used in tests — writes only the halted flag,
    /// preserving all other fields.
    pub fn set_halted(&self, halted: bool) {
        let s = self.read();
        self.update(s.balance, s.used_margin, s.frozen,
                    halted, s.position, s.open_order_count);
    }

    /// Convenience setter used in tests — writes only the position field.
    pub fn set_position(&self, position: i64) {
        let s = self.read();
        self.update(s.balance, s.used_margin, s.frozen,
                    s.halted, position, s.open_order_count);
    }
}

impl Default for AccountRiskState {
    fn default() -> Self { Self::new() }
}

unsafe impl Send for AccountRiskState {}
unsafe impl Sync for AccountRiskState {}

// ── Writer ────────────────────────────────────────────────────────────────────

pub struct AccountRiskStateWriter {
    inner: AccountRiskState,
}

impl AccountRiskStateWriter {
    pub fn new(inner: AccountRiskState) -> Self { Self { inner } }

    pub fn update(&mut self, balance: i64, used_margin: i64, frozen: bool,
                  halted: bool, position: i64, open_order_count: u32) {
        self.inner.update(balance, used_margin, frozen,
                          halted, position, open_order_count);
    }

    pub fn read(&self) -> AccountRiskSnapshot { self.inner.read() }
    pub fn inner(&self) -> &AccountRiskState  { &self.inner }
}