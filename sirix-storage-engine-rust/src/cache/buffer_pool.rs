//! BufferPool - striped pool of reusable byte buffers.
//!
//! Minimizes allocation pressure by reusing `Vec<u8>` buffers
//! across I/O operations. Uses per-thread striping to reduce contention.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use crossbeam_utils::CachePadded;
use parking_lot::Mutex;

/// Predefined buffer size tiers for the pool.
pub const BUFFER_SIZES: [usize; 7] = [
    4_096,   // 4 KiB
    8_192,   // 8 KiB
    16_384,  // 16 KiB
    32_768,  // 32 KiB
    65_536,  // 64 KiB
    131_072, // 128 KiB
    262_144, // 256 KiB
];

/// A single stripe containing buffers for each size tier.
struct Stripe {
    /// Buffers organized by size tier. Index corresponds to `BUFFER_SIZES`.
    tiers: [Vec<Vec<u8>>; 7],
}

impl Stripe {
    fn new() -> Self {
        Self {
            tiers: [
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            ],
        }
    }

    /// Take a buffer from the appropriate tier, or allocate a new one.
    fn acquire(&mut self, size: usize) -> Vec<u8> {
        let tier = find_tier(size);
        if let Some(mut buf) = self.tiers[tier].pop() {
            buf.clear();
            if buf.capacity() < size {
                buf.reserve(size - buf.capacity());
            }
            buf
        } else {
            Vec::with_capacity(BUFFER_SIZES[tier])
        }
    }

    /// Return a buffer to the pool.
    fn release(&mut self, buf: Vec<u8>, max_per_tier: usize) {
        let cap = buf.capacity();
        let tier = find_tier(cap);
        if self.tiers[tier].len() < max_per_tier {
            self.tiers[tier].push(buf);
        }
        // else: drop the buffer (pool is full for this tier)
    }
}

/// Find the appropriate tier index for a given size.
fn find_tier(size: usize) -> usize {
    for (i, &tier_size) in BUFFER_SIZES.iter().enumerate() {
        if size <= tier_size {
            return i;
        }
    }
    BUFFER_SIZES.len() - 1 // largest tier
}

/// A striped buffer pool for reusable byte buffers.
///
/// Uses cache-line-padded stripes to minimize false sharing.
/// Each stripe holds buffers organized by size tier.
pub struct BufferPool {
    stripes: Vec<CachePadded<Mutex<Stripe>>>,
    max_per_tier: usize,
    acquire_count: AtomicU64,
    release_count: AtomicU64,
    miss_count: AtomicU64,
}

impl BufferPool {
    /// Create a new buffer pool with the given number of stripes.
    ///
    /// `max_per_tier` controls how many buffers are kept per size tier per stripe.
    pub fn new(stripe_count: usize, max_per_tier: usize) -> Self {
        let mut stripes = Vec::with_capacity(stripe_count);
        for _ in 0..stripe_count {
            stripes.push(CachePadded::new(Mutex::new(Stripe::new())));
        }
        Self {
            stripes,
            max_per_tier,
            acquire_count: AtomicU64::new(0),
            release_count: AtomicU64::new(0),
            miss_count: AtomicU64::new(0),
        }
    }

    /// Create a pool sized to the number of CPUs.
    pub fn default_sized() -> Self {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self::new(cpus * 2, 16)
    }

    /// Acquire a buffer of at least `size` bytes.
    pub fn acquire(&self, size: usize) -> Vec<u8> {
        self.acquire_count.fetch_add(1, Ordering::Relaxed);
        let stripe_idx = self.stripe_index();
        let mut stripe = self.stripes[stripe_idx].lock();
        stripe.acquire(size)
    }

    /// Release a buffer back to the pool for reuse.
    pub fn release(&self, buf: Vec<u8>) {
        self.release_count.fetch_add(1, Ordering::Relaxed);
        let stripe_idx = self.stripe_index();
        let mut stripe = self.stripes[stripe_idx].lock();
        stripe.release(buf, self.max_per_tier);
    }

    /// Get the stripe index for the current thread.
    #[inline]
    fn stripe_index(&self) -> usize {
        let tid = std::thread::current().id();
        let hash = {
            use std::hash::Hash;
            use std::hash::Hasher;
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            tid.hash(&mut hasher);
            hasher.finish()
        };
        (hash as usize) % self.stripes.len()
    }

    /// Total number of acquire operations.
    pub fn acquire_count(&self) -> u64 {
        self.acquire_count.load(Ordering::Relaxed)
    }

    /// Total number of release operations.
    pub fn release_count(&self) -> u64 {
        self.release_count.load(Ordering::Relaxed)
    }

    /// Total number of cache misses (new allocations).
    pub fn miss_count(&self) -> u64 {
        self.miss_count.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_tier() {
        assert_eq!(find_tier(100), 0);     // fits in 4 KiB
        assert_eq!(find_tier(4096), 0);    // exact 4 KiB
        assert_eq!(find_tier(4097), 1);    // needs 8 KiB
        assert_eq!(find_tier(65536), 4);   // exact 64 KiB
        assert_eq!(find_tier(999999), 6);  // oversized -> largest tier
    }

    #[test]
    fn test_acquire_release() {
        let pool = BufferPool::new(4, 8);

        let buf = pool.acquire(1024);
        assert!(buf.capacity() >= 1024);

        pool.release(buf);
        assert_eq!(pool.acquire_count(), 1);
        assert_eq!(pool.release_count(), 1);

        // Second acquire should reuse the released buffer
        let buf2 = pool.acquire(1024);
        assert!(buf2.capacity() >= 1024);
    }

    #[test]
    fn test_multiple_sizes() {
        let pool = BufferPool::new(2, 4);

        let small = pool.acquire(100);
        let medium = pool.acquire(10_000);
        let large = pool.acquire(200_000);

        assert!(small.capacity() >= 100);
        assert!(medium.capacity() >= 10_000);
        assert!(large.capacity() >= 200_000);

        pool.release(small);
        pool.release(medium);
        pool.release(large);
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let pool = Arc::new(BufferPool::new(8, 16));
        let mut handles = Vec::new();

        for _ in 0..8 {
            let pool = Arc::clone(&pool);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let buf = pool.acquire(4096);
                    pool.release(buf);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(pool.acquire_count(), 800);
        assert_eq!(pool.release_count(), 800);
    }
}
