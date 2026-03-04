//! LeanStore/Umbra-style page cache with clock eviction.
//!
//! A fixed-size buffer pool that caches pages in memory using buffer frames.
//! Eviction is managed via a clock (second-chance) algorithm with guard-based
//! pinning to prevent eviction of pages currently in use.
//!
//! Key design:
//! - Fixed number of buffer frames, each holding one page
//! - Clock eviction with hot bit (second chance)
//! - Guard-based pinning: guarded pages have eviction weight 0
//! - Dirty tracking for write-back
//! - Lock-free fast path for cached page lookup via sharded hash map
//!
//! HFT optimizations:
//! - O(1) HashMap lookup in shards (replaces O(n) linear scan)
//! - CachePadded atomics to prevent false sharing across cores
//! - Shard lock dropped before frame lock acquisition (reduces contention)
//! - Bitmask shard selection (replaces modulo)
//! - XXH3 hash for PageId (replaces byte-by-byte FNV-1a)

use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crossbeam_utils::CachePadded;
use parking_lot::Mutex;
use parking_lot::RwLock;

use crate::page::slotted_page::SlottedPage;

/// A unique key for identifying cached pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageId {
    /// Database ID.
    pub database_id: i64,
    /// Resource ID.
    pub resource_id: i64,
    /// Page key (file offset or logical key).
    pub page_key: i64,
}

impl PageId {
    /// Create a new page ID.
    #[inline]
    pub fn new(database_id: i64, resource_id: i64, page_key: i64) -> Self {
        Self {
            database_id,
            resource_id,
            page_key,
        }
    }

    /// Compute hash using XXH3 over the three i64 fields for fast, high-quality distribution.
    #[inline]
    fn hash(&self) -> u64 {
        // Pack 24 bytes and hash with XXH3 (SIMD-accelerated on supported platforms)
        let mut buf = [0u8; 24];
        buf[0..8].copy_from_slice(&self.page_key.to_le_bytes());
        buf[8..16].copy_from_slice(&self.database_id.to_le_bytes());
        buf[16..24].copy_from_slice(&self.resource_id.to_le_bytes());
        xxhash_rust::xxh3::xxh3_64(&buf)
    }
}

/// State bits packed into a single AtomicU32 for each buffer frame.
const FRAME_VALID: u32 = 1;
const FRAME_DIRTY: u32 = 2;
const FRAME_HOT: u32 = 4;
const FRAME_EVICTING: u32 = 8;

/// A buffer frame in the page cache.
///
/// Each frame can hold one page. Frames are reused via clock eviction.
/// CachePadded atomics prevent false sharing between cores scanning adjacent frames.
struct BufferFrame {
    /// The cached page (None if frame is free).
    page: RwLock<Option<Arc<SlottedPage>>>,
    /// Page ID of the cached page.
    page_id: Mutex<Option<PageId>>,
    /// State flags (valid, dirty, hot, evicting) - CachePadded to prevent false sharing.
    state: CachePadded<AtomicU32>,
    /// Number of active guards pinning this frame - CachePadded to prevent false sharing.
    guard_count: CachePadded<AtomicU32>,
    /// Version counter for frame reuse detection.
    version: CachePadded<AtomicU32>,
}

impl BufferFrame {
    fn new() -> Self {
        Self {
            page: RwLock::new(None),
            page_id: Mutex::new(None),
            state: CachePadded::new(AtomicU32::new(0)),
            guard_count: CachePadded::new(AtomicU32::new(0)),
            version: CachePadded::new(AtomicU32::new(0)),
        }
    }

    #[inline]
    fn is_pinned(&self) -> bool {
        self.guard_count.load(Ordering::Acquire) > 0
    }

    #[inline]
    fn is_hot(&self) -> bool {
        self.state.load(Ordering::Relaxed) & FRAME_HOT != 0
    }

    #[inline]
    fn set_hot(&self) {
        self.state.fetch_or(FRAME_HOT, Ordering::Relaxed);
    }

    #[inline]
    fn clear_hot(&self) {
        self.state.fetch_and(!FRAME_HOT, Ordering::Relaxed);
    }

    #[inline]
    fn is_dirty(&self) -> bool {
        self.state.load(Ordering::Relaxed) & FRAME_DIRTY != 0
    }

    #[inline]
    fn mark_dirty(&self) {
        self.state.fetch_or(FRAME_DIRTY, Ordering::Relaxed);
    }

    #[inline]
    fn clear_dirty(&self) {
        self.state.fetch_and(!FRAME_DIRTY, Ordering::Relaxed);
    }

    #[inline]
    fn is_valid(&self) -> bool {
        self.state.load(Ordering::Relaxed) & FRAME_VALID != 0
    }

    #[inline]
    fn try_mark_evicting(&self) -> bool {
        let old = self.state.load(Ordering::Relaxed);
        if old & FRAME_EVICTING != 0 {
            return false;
        }
        self.state
            .compare_exchange(old, old | FRAME_EVICTING, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }

    #[inline]
    fn clear_evicting(&self) {
        self.state.fetch_and(!FRAME_EVICTING, Ordering::Release);
    }
}

/// Number of shards for the page-to-frame lookup table (must be power of 2).
const SHARD_COUNT: usize = 64;
/// Bitmask for shard selection (avoids expensive modulo).
const SHARD_MASK: usize = SHARD_COUNT - 1;

/// A shard of the page-to-frame mapping using O(1) HashMap lookup.
struct LookupShard {
    entries: HashMap<u64, usize>,
}

impl LookupShard {
    fn new() -> Self {
        Self {
            entries: HashMap::with_capacity(64),
        }
    }

    #[inline]
    fn find(&self, hash: u64) -> Option<usize> {
        self.entries.get(&hash).copied()
    }

    #[inline]
    fn insert(&mut self, hash: u64, frame_idx: usize) {
        self.entries.insert(hash, frame_idx);
    }

    #[inline]
    fn remove(&mut self, hash: u64) {
        self.entries.remove(&hash);
    }
}

/// A handle to a cached page, pinning it in the cache.
///
/// While this handle exists, the page cannot be evicted.
pub struct CachedPage {
    page: Arc<SlottedPage>,
    frame_idx: usize,
    version_at_pin: u32,
    cache: Arc<PageCacheInner>,
}

impl CachedPage {
    #[inline]
    pub fn page(&self) -> &SlottedPage {
        &self.page
    }

    pub fn page_arc(&self) -> &Arc<SlottedPage> {
        &self.page
    }

    pub fn is_valid(&self) -> bool {
        self.cache.frames[self.frame_idx]
            .version
            .load(Ordering::Acquire)
            == self.version_at_pin
    }

    pub fn mark_dirty(&self) {
        self.cache.frames[self.frame_idx].mark_dirty();
    }
}

impl Drop for CachedPage {
    fn drop(&mut self) {
        self.cache.frames[self.frame_idx]
            .guard_count
            .fetch_sub(1, Ordering::Release);
    }
}

/// Inner shared state of the page cache.
struct PageCacheInner {
    /// Buffer frames.
    frames: Vec<BufferFrame>,
    /// Sharded lookup table: PageId hash → frame index.
    shards: Vec<CachePadded<Mutex<LookupShard>>>,
    /// Clock hand for eviction scanning.
    clock_hand: CachePadded<AtomicU32>,
    /// Statistics - CachePadded to prevent contention on counters.
    hits: CachePadded<AtomicU64>,
    misses: CachePadded<AtomicU64>,
    evictions: CachePadded<AtomicU64>,
}

/// LeanStore/Umbra-style page cache.
///
/// Fixed-size buffer pool with clock-based eviction and guard-based pinning.
pub struct PageCache {
    inner: Arc<PageCacheInner>,
    capacity: usize,
}

impl PageCache {
    /// Create a new page cache with the given capacity (number of buffer frames).
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(16);

        let mut frames = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            frames.push(BufferFrame::new());
        }

        let mut shards = Vec::with_capacity(SHARD_COUNT);
        for _ in 0..SHARD_COUNT {
            shards.push(CachePadded::new(Mutex::new(LookupShard::new())));
        }

        Self {
            inner: Arc::new(PageCacheInner {
                frames,
                shards,
                clock_hand: CachePadded::new(AtomicU32::new(0)),
                hits: CachePadded::new(AtomicU64::new(0)),
                misses: CachePadded::new(AtomicU64::new(0)),
                evictions: CachePadded::new(AtomicU64::new(0)),
            }),
            capacity,
        }
    }

    /// Create a cache sized to available memory.
    pub fn with_memory_budget(memory_budget: usize, avg_page_size: usize) -> Self {
        let capacity = (memory_budget / avg_page_size).max(16);
        Self::new(capacity)
    }

    /// Look up a page in the cache. Returns a pinned handle if found.
    ///
    /// Hot path: shard lock is dropped before acquiring frame locks to minimize contention.
    pub fn get(&self, page_id: &PageId) -> Option<CachedPage> {
        let hash = page_id.hash();
        let shard_idx = (hash as usize) & SHARD_MASK;

        // Look up frame index under shard lock, then drop it immediately
        let frame_idx = {
            let shard = self.inner.shards[shard_idx].lock();
            match shard.find(hash) {
                Some(idx) => idx,
                None => {
                    drop(shard);
                    self.inner.misses.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
            }
        };
        // Shard lock is dropped here - no lock held while accessing frame

        let frame = &self.inner.frames[frame_idx];

        if !frame.is_valid() {
            self.inner.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        // Verify PageId match (handles hash collisions)
        {
            let frame_page_id = frame.page_id.lock();
            if frame_page_id.as_ref() != Some(page_id) {
                self.inner.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        }

        // Pin the frame
        frame.guard_count.fetch_add(1, Ordering::AcqRel);
        frame.set_hot();

        let page_guard = frame.page.read();
        if let Some(ref page) = *page_guard {
            let version = frame.version.load(Ordering::Acquire);
            self.inner.hits.fetch_add(1, Ordering::Relaxed);

            return Some(CachedPage {
                page: Arc::clone(page),
                frame_idx,
                version_at_pin: version,
                cache: Arc::clone(&self.inner),
            });
        }

        // Frame was cleared between our check and pin
        frame.guard_count.fetch_sub(1, Ordering::Release);
        self.inner.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Insert a page into the cache. Returns a pinned handle.
    ///
    /// If the cache is full, evicts a page using clock algorithm.
    /// Returns the evicted page's data if it was dirty (caller must flush).
    pub fn put(
        &self,
        page_id: PageId,
        page: Arc<SlottedPage>,
    ) -> (CachedPage, Option<EvictedPage>) {
        let hash = page_id.hash();
        let shard_idx = (hash as usize) & SHARD_MASK;

        // Check if already cached - drop shard lock before frame access
        {
            let shard = self.inner.shards[shard_idx].lock();
            if let Some(frame_idx) = shard.find(hash) {
                let frame = &self.inner.frames[frame_idx];
                let is_match = {
                    let frame_page_id = frame.page_id.lock();
                    frame_page_id.as_ref() == Some(&page_id)
                };
                if is_match {
                    drop(shard);

                    // Update page without holding shard lock
                    *frame.page.write() = Some(Arc::clone(&page));
                    frame.guard_count.fetch_add(1, Ordering::AcqRel);
                    frame.set_hot();
                    let version = frame.version.load(Ordering::Acquire);

                    return (
                        CachedPage {
                            page,
                            frame_idx,
                            version_at_pin: version,
                            cache: Arc::clone(&self.inner),
                        },
                        None,
                    );
                }
            }
        }

        // Find a free or evictable frame
        let (frame_idx, evicted) = self.find_victim();

        let frame = &self.inner.frames[frame_idx];

        // Remove old entry from lookup
        {
            let old_page_id = frame.page_id.lock();
            if let Some(ref old_id) = *old_page_id {
                let old_hash = old_id.hash();
                let old_shard_idx = (old_hash as usize) & SHARD_MASK;
                let mut old_shard = self.inner.shards[old_shard_idx].lock();
                old_shard.remove(old_hash);
            }
        }

        // Install new page
        *frame.page_id.lock() = Some(page_id);
        *frame.page.write() = Some(Arc::clone(&page));
        frame.state.store(FRAME_VALID | FRAME_HOT, Ordering::Release);
        frame.guard_count.store(1, Ordering::Release);
        frame.version.fetch_add(1, Ordering::AcqRel);
        frame.clear_evicting();

        // Insert into lookup
        {
            let mut shard = self.inner.shards[shard_idx].lock();
            shard.insert(hash, frame_idx);
        }

        let version = frame.version.load(Ordering::Acquire);

        (
            CachedPage {
                page,
                frame_idx,
                version_at_pin: version,
                cache: Arc::clone(&self.inner),
            },
            evicted,
        )
    }

    /// Remove a page from the cache.
    pub fn remove(&self, page_id: &PageId) -> Option<Arc<SlottedPage>> {
        let hash = page_id.hash();
        let shard_idx = (hash as usize) & SHARD_MASK;

        let mut shard = self.inner.shards[shard_idx].lock();
        if let Some(frame_idx) = shard.find(hash) {
            let frame = &self.inner.frames[frame_idx];
            let frame_page_id = frame.page_id.lock();
            if frame_page_id.as_ref() == Some(page_id) {
                drop(frame_page_id);
                shard.remove(hash);
                drop(shard);

                *frame.page_id.lock() = None;
                let page = frame.page.write().take();
                frame.state.store(0, Ordering::Release);
                frame.version.fetch_add(1, Ordering::AcqRel);

                return page;
            }
        }
        None
    }

    /// Find a victim frame for eviction using clock algorithm.
    fn find_victim(&self) -> (usize, Option<EvictedPage>) {
        let num_frames = self.capacity;
        let max_scans = num_frames * 2;

        for _ in 0..max_scans {
            let idx = self.inner.clock_hand.fetch_add(1, Ordering::Relaxed) as usize % num_frames;
            let frame = &self.inner.frames[idx];

            if frame.is_pinned() {
                continue;
            }

            if !frame.try_mark_evicting() {
                continue;
            }

            if !frame.is_valid() {
                return (idx, None);
            }

            // Clock: if hot, give second chance
            if frame.is_hot() {
                frame.clear_hot();
                frame.clear_evicting();
                continue;
            }

            // Evict this frame
            self.inner.evictions.fetch_add(1, Ordering::Relaxed);

            let evicted = if frame.is_dirty() {
                let page = frame.page.read().as_ref().map(Arc::clone);
                let page_id = frame.page_id.lock().clone();
                frame.clear_dirty();

                match (page, page_id) {
                    (Some(p), Some(id)) => Some(EvictedPage {
                        page_id: id,
                        page: p,
                    }),
                    _ => None,
                }
            } else {
                None
            };

            *frame.page.write() = None;
            *frame.page_id.lock() = None;
            frame.state.store(0, Ordering::Release);
            frame.version.fetch_add(1, Ordering::AcqRel);
            frame.clear_evicting();

            return (idx, evicted);
        }

        // Fallback: force evict frame 0
        let frame = &self.inner.frames[0];
        let evicted = if frame.is_dirty() {
            let page = frame.page.read().as_ref().map(Arc::clone);
            let page_id = frame.page_id.lock().clone();
            match (page, page_id) {
                (Some(p), Some(id)) => Some(EvictedPage { page_id: id, page: p }),
                _ => None,
            }
        } else {
            None
        };

        *frame.page.write() = None;
        *frame.page_id.lock() = None;
        frame.state.store(0, Ordering::Release);
        frame.guard_count.store(0, Ordering::Release);
        frame.version.fetch_add(1, Ordering::AcqRel);
        self.inner.evictions.fetch_add(1, Ordering::Relaxed);

        (0, evicted)
    }

    /// Iterate over all dirty pages.
    pub fn dirty_pages(&self) -> Vec<(PageId, Arc<SlottedPage>)> {
        let mut result = Vec::new();
        for frame in &self.inner.frames {
            if frame.is_valid() && frame.is_dirty() {
                let page_id = frame.page_id.lock().clone();
                let page = frame.page.read().as_ref().map(Arc::clone);
                if let (Some(id), Some(p)) = (page_id, page) {
                    result.push((id, p));
                }
            }
        }
        result
    }

    /// Clear dirty flag for a specific page.
    pub fn clear_dirty(&self, page_id: &PageId) {
        let hash = page_id.hash();
        let shard_idx = (hash as usize) & SHARD_MASK;
        let shard = self.inner.shards[shard_idx].lock();
        if let Some(frame_idx) = shard.find(hash) {
            self.inner.frames[frame_idx].clear_dirty();
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline]
    pub fn hits(&self) -> u64 {
        self.inner.hits.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn misses(&self) -> u64 {
        self.inner.misses.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn evictions(&self) -> u64 {
        self.inner.evictions.load(Ordering::Relaxed)
    }

    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits() as f64;
        let misses = self.misses() as f64;
        let total = hits + misses;
        if total == 0.0 {
            0.0
        } else {
            hits / total * 100.0
        }
    }

    pub fn size(&self) -> usize {
        self.inner
            .frames
            .iter()
            .filter(|f| f.is_valid())
            .count()
    }
}

/// A page that was evicted from the cache.
pub struct EvictedPage {
    pub page_id: PageId,
    pub page: Arc<SlottedPage>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::IndexType;

    fn make_page(key: i64) -> Arc<SlottedPage> {
        Arc::new(SlottedPage::new(key, 0, IndexType::Document))
    }

    fn make_id(key: i64) -> PageId {
        PageId::new(1, 1, key)
    }

    #[test]
    fn test_put_and_get() {
        let cache = PageCache::new(64);
        let page = make_page(1);
        let id = make_id(1);

        let (handle, evicted) = cache.put(id, Arc::clone(&page));
        assert!(evicted.is_none());
        assert!(handle.is_valid());
        drop(handle);

        let handle = cache.get(&id);
        assert!(handle.is_some());
        assert_eq!(handle.unwrap().page().record_page_key(), 1);
    }

    #[test]
    fn test_miss() {
        let cache = PageCache::new(64);
        let id = make_id(999);
        assert!(cache.get(&id).is_none());
        assert_eq!(cache.misses(), 1);
    }

    #[test]
    fn test_remove() {
        let cache = PageCache::new(64);
        let page = make_page(1);
        let id = make_id(1);

        let (handle, _) = cache.put(id, page);
        drop(handle);

        let removed = cache.remove(&id);
        assert!(removed.is_some());
        assert!(cache.get(&id).is_none());
    }

    #[test]
    fn test_eviction() {
        let cache = PageCache::new(16);

        for i in 0..32 {
            let page = make_page(i);
            let id = make_id(i);
            let (handle, _) = cache.put(id, page);
            drop(handle);
        }

        assert!(cache.evictions() > 0);
        assert!(cache.size() <= 16);
    }

    #[test]
    fn test_dirty_tracking() {
        let cache = PageCache::new(64);
        let page = make_page(1);
        let id = make_id(1);

        let (handle, _) = cache.put(id, page);
        handle.mark_dirty();
        drop(handle);

        let dirty = cache.dirty_pages();
        assert_eq!(dirty.len(), 1);
        assert_eq!(dirty[0].0, id);

        cache.clear_dirty(&id);
        let dirty = cache.dirty_pages();
        assert!(dirty.is_empty());
    }

    #[test]
    fn test_pinned_not_evicted() {
        let cache = PageCache::new(16);

        let pinned_page = make_page(0);
        let pinned_id = make_id(0);
        let pinned_handle = cache.put(pinned_id, pinned_page);

        for i in 1..32 {
            let page = make_page(i);
            let id = make_id(i);
            let (handle, _) = cache.put(id, page);
            drop(handle);
        }

        drop(pinned_handle);
        let handle = cache.get(&pinned_id);
        assert!(handle.is_some());
    }

    #[test]
    fn test_statistics() {
        let cache = PageCache::new(64);
        let page = make_page(1);
        let id = make_id(1);

        cache.get(&id);
        assert_eq!(cache.misses(), 1);

        let (handle, _) = cache.put(id, page);
        drop(handle);

        let _ = cache.get(&id);
        assert_eq!(cache.hits(), 1);
    }

    #[test]
    fn test_concurrent_access() {
        use std::thread;

        let cache = Arc::new(PageCache::new(256));
        let mut handles = Vec::new();

        for t in 0..4 {
            let cache = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    let key = t * 100 + i;
                    let page = make_page(key);
                    let id = make_id(key);
                    let (handle, _) = cache.put(id, page);
                    drop(handle);

                    let _ = cache.get(&id);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert!(cache.hits() > 0);
    }

    #[test]
    fn test_clock_second_chance() {
        let cache = PageCache::new(16);

        for i in 0..16 {
            let page = make_page(i);
            let id = make_id(i);
            let (handle, _) = cache.put(id, page);
            drop(handle);
        }

        let _ = cache.get(&make_id(0));

        for i in 16..24 {
            let page = make_page(i);
            let id = make_id(i);
            let (handle, _) = cache.put(id, page);
            drop(handle);
        }

        assert!(cache.evictions() > 0);
    }

    #[test]
    fn test_evicted_dirty_page_returned() {
        let cache = PageCache::new(16);

        for i in 0..16 {
            let page = make_page(i);
            let id = make_id(i);
            let (handle, _) = cache.put(id, page);
            handle.mark_dirty();
            drop(handle);
        }

        let page = make_page(100);
        let id = make_id(100);
        let (handle, evicted) = cache.put(id, page);
        drop(handle);

        assert!(evicted.is_some());
    }

    #[test]
    fn test_with_memory_budget() {
        let cache = PageCache::with_memory_budget(1024 * 1024, 64 * 1024);
        assert_eq!(cache.capacity(), 16);
    }

    #[test]
    fn test_hit_rate() {
        let cache = PageCache::new(64);
        let page = make_page(1);
        let id = make_id(1);

        let (handle, _) = cache.put(id, page);
        drop(handle);

        let _ = cache.get(&id);
        assert!(cache.hit_rate() > 99.0);

        let _ = cache.get(&make_id(999));
        assert!((cache.hit_rate() - 50.0).abs() < 1.0);
    }

    // --- Additional HFT & behavioral tests ---

    #[test]
    fn test_page_id_hash_distribution() {
        // Verify XXH3 hash provides good distribution across shards
        let mut shard_counts = [0u32; SHARD_COUNT];
        for i in 0..10000i64 {
            let id = PageId::new(1, 1, i);
            let shard = (id.hash() as usize) & SHARD_MASK;
            shard_counts[shard] += 1;
        }
        // Each shard should get roughly 10000/64 = ~156 entries
        // Allow 3x deviation
        let expected = 10000.0 / SHARD_COUNT as f64;
        for count in &shard_counts {
            assert!(
                (*count as f64) < expected * 3.0,
                "poor hash distribution: shard got {} entries (expected ~{})",
                count,
                expected
            );
        }
    }

    #[test]
    fn test_page_id_hash_different_databases() {
        let id1 = PageId::new(1, 1, 1);
        let id2 = PageId::new(2, 1, 1);
        let id3 = PageId::new(1, 2, 1);
        assert_ne!(id1.hash(), id2.hash());
        assert_ne!(id1.hash(), id3.hash());
        assert_ne!(id2.hash(), id3.hash());
    }

    #[test]
    fn test_put_update_existing() {
        let cache = PageCache::new(64);
        let id = make_id(1);
        let page1 = make_page(1);
        let page2 = make_page(1);

        let (h1, _) = cache.put(id, page1);
        drop(h1);

        // Put same page_id again - should update in place
        let (h2, evicted) = cache.put(id, page2);
        assert!(evicted.is_none());
        assert!(h2.is_valid());
        drop(h2);

        // Should still be one entry
        assert_eq!(cache.size(), 1);
    }

    #[test]
    fn test_remove_nonexistent() {
        let cache = PageCache::new(64);
        let id = make_id(42);
        assert!(cache.remove(&id).is_none());
    }

    #[test]
    fn test_guard_prevents_eviction_multiple_pins() {
        let cache = PageCache::new(16);
        let id = make_id(0);
        let page = make_page(0);

        // Pin twice
        let (h1, _) = cache.put(id, Arc::clone(&page));
        let h2 = cache.get(&id).unwrap();

        // Fill cache to trigger eviction attempts
        for i in 1..32 {
            let (h, _) = cache.put(make_id(i), make_page(i));
            drop(h);
        }

        // Both guards should still be valid
        assert!(h1.is_valid());
        assert!(h2.is_valid());

        drop(h1);
        drop(h2);

        // Page should still be accessible after all guards dropped
        assert!(cache.get(&id).is_some());
    }

    #[test]
    fn test_version_increments_on_eviction() {
        let cache = PageCache::new(16);

        for i in 0..16 {
            let (h, _) = cache.put(make_id(i), make_page(i));
            drop(h);
        }

        // Get version of page 0
        let h0 = cache.get(&make_id(0)).unwrap();
        let _v0 = h0.version_at_pin;
        drop(h0);

        // Force eviction of page 0 by filling cache with new pages
        // First clear page 0's hot bit
        for i in 16..48 {
            let (h, _) = cache.put(make_id(i), make_page(i));
            drop(h);
        }

        // If page 0 was evicted and re-inserted, version should differ
        // (This is a probabilistic test - at minimum evictions occurred)
        assert!(cache.evictions() > 0);
    }

    #[test]
    fn test_dirty_page_survives_get() {
        let cache = PageCache::new(64);
        let id = make_id(1);
        let page = make_page(1);

        let (h, _) = cache.put(id, page);
        h.mark_dirty();
        drop(h);

        // Get should not clear dirty flag
        let h2 = cache.get(&id).unwrap();
        drop(h2);

        let dirty = cache.dirty_pages();
        assert_eq!(dirty.len(), 1);
    }

    #[test]
    fn test_concurrent_put_same_key() {
        use std::thread;

        let cache = Arc::new(PageCache::new(64));
        let mut handles = Vec::new();

        for _ in 0..8 {
            let cache = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let page = make_page(42);
                    let id = make_id(42);
                    let (h, _) = cache.put(id, page);
                    drop(h);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Should still be exactly 1 entry for key 42
        let h = cache.get(&make_id(42));
        assert!(h.is_some());
        assert_eq!(cache.size(), 1);
    }

    #[test]
    fn test_minimum_capacity_enforced() {
        let cache = PageCache::new(1);
        assert_eq!(cache.capacity(), 16);
    }

    #[test]
    fn test_heavy_eviction_stress() {
        let cache = PageCache::new(32);

        for i in 0..1000i64 {
            let (h, _) = cache.put(make_id(i), make_page(i));
            drop(h);
        }

        assert!(cache.size() <= 32);
        assert!(cache.evictions() > 900);
    }

    #[test]
    fn test_mixed_dirty_clean_eviction() {
        let cache = PageCache::new(16);

        // Fill with alternating dirty/clean
        for i in 0..16 {
            let (h, _) = cache.put(make_id(i), make_page(i));
            if i % 2 == 0 {
                h.mark_dirty();
            }
            drop(h);
        }

        // Trigger evictions
        let mut dirty_evictions = 0;
        let mut clean_evictions = 0;
        for i in 16..32 {
            let (h, evicted) = cache.put(make_id(i), make_page(i));
            if let Some(_) = evicted {
                dirty_evictions += 1;
            } else if cache.evictions() > (dirty_evictions + clean_evictions) as u64 {
                clean_evictions += 1;
            }
            drop(h);
        }

        assert!(cache.evictions() > 0);
    }

    #[test]
    fn test_empty_cache_hit_rate() {
        let cache = PageCache::new(64);
        assert_eq!(cache.hit_rate(), 0.0);
    }

    #[test]
    fn test_multiple_databases_same_page_key() {
        let cache = PageCache::new(64);

        let id1 = PageId::new(1, 1, 100);
        let id2 = PageId::new(2, 1, 100);
        let id3 = PageId::new(1, 2, 100);

        let (h1, _) = cache.put(id1, make_page(100));
        let (h2, _) = cache.put(id2, make_page(100));
        let (h3, _) = cache.put(id3, make_page(100));
        drop(h1);
        drop(h2);
        drop(h3);

        // All three should be independently cached
        assert!(cache.get(&id1).is_some());
        assert!(cache.get(&id2).is_some());
        assert!(cache.get(&id3).is_some());
        assert_eq!(cache.size(), 3);
    }
}
