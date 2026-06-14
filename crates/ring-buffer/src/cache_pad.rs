/// Wraps a value in a cache-line-sized allocation to prevent false sharing.
/// Aligns to 64 bytes (x86-64 cache line size).
#[derive(Debug, Default)]
#[repr(align(64))]
pub struct CachePadded<T>(pub T);

impl<T> CachePadded<T> {
    pub const fn new(val: T) -> Self {
        Self(val)
    }
}

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> std::ops::DerefMut for CachePadded<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}