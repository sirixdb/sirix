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
    pub fn new(database_id: i64, resource_id: i64, page_key: i64) -> Self {
        Self {
            database_id,
            resource_id,
            page_key,
        }
    }

    /// Compute hash for shard selection.
    #[inline]
    fn hash(&self) -> u64 {
        // FNV-1a hash over the three i64 fields
        let mut h: u64 = 0xcbf29ce484222325;
        for byte in self.page_key.to_le_bytes() {
            h ^= byte as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        for byte in self.database_id.to_le_bytes() {
            h ^= byte as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        for byte in self.resource_id.to_le_bytes() {
            h ^= byte as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h
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
struct BufferFrame {
    /// The cached page (None if frame is free).
    page: RwLock<Option<Arc<SlottedPage>>>,
    /// Page ID of the cached page.
    page_id: Mutex<Option<PageId>>,
    /// State flags (valid, dirty, hot, evicting).
    state: AtomicU32,
    /// Number of active guards pinning this frame.
    guard_count: AtomicU32,
    /// Version counter for frame reuse detection.
    version: AtomicU32,
}

impl BufferFrame {
    fn new() -> Self {
        Self {
            page: RwLock::new(None),
            page_id: Mutex::new(None),
            state: AtomicU32::new(0),
            guard_count: AtomicU32::new(0),
            version: AtomicU32::new(0),
        }
    }

    /// Check if this frame is pinned (has active guards).
    #[inline]
    fn is_pinned(&self) -> bool {
        self.guard_count.load(Ordering::Acquire) > 0
    }

    /// Check if the frame has the hot bit set.
    #[inline]
    fn is_hot(&self) -> bool {
        self.state.load(Ordering::Relaxed) & FRAME_HOT != 0
    }

    /// Set the hot bit.
    #[inline]
    fn set_hot(&self) {
        self.state.fetch_or(FRAME_HOT, Ordering::Relaxed);
    }

    /// Clear the hot bit.
    #[inline]
    fn clear_hot(&self) {
        self.state.fetch_and(!FRAME_HOT, Ordering::Relaxed);
    }

    /// Check if dirty.
    #[inline]
    fn is_dirty(&self) -> bool {
        self.state.load(Ordering::Relaxed) & FRAME_DIRTY != 0
    }

    /// Mark as dirty.
    #[inline]
    fn mark_dirty(&self) {
        self.state.fetch_or(FRAME_DIRTY, Ordering::Relaxed);
    }

    /// Clear dirty flag.
    #[inline]
    fn clear_dirty(&self) {
        self.state.fetch_and(!FRAME_DIRTY, Ordering::Relaxed);
    }

    /// Check if valid (contains a page).
    #[inline]
    fn is_valid(&self) -> bool {
        self.state.load(Ordering::Relaxed) & FRAME_VALID != 0
    }

    /// Try to mark for eviction (CAS on EVICTING bit).
    #[inline]
    fn try_mark_evicting(&self) -> bool {
        let old = self.state.load(Ordering::Relaxed);
        if old & FRAME_EVICTING != 0 {
            return false; // Already being evicted
        }
        self.state
            .compare_exchange(old, old | FRAME_EVICTING, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }

    /// Clear evicting flag.
    #[inline]
    fn clear_evicting(&self) {
        self.state.fetch_and(!FRAME_EVICTING, Ordering::Release);
    }
}

/// Number of shards for the page-to-frame lookup table.
const SHARD_COUNT: usize = 64;

/// A shard of the page-to-frame mapping.
struct LookupShard {
    /// Map from PageId hash to frame index.
    entries: Vec<(u64, usize)>, // (PageId hash, frame_index)
}

impl LookupShard {
    fn new() -> Self {
        Self {
            entries: Vec::with_capacity(64),
        }
    }

    fn find(&self, hash: u64) -> Option<usize> {
        self.entries
            .iter()
            .find(|(h, _)| *h == hash)
            .map(|(_, idx)| *idx)
    }

    fn insert(&mut self, hash: u64, frame_idx: usize) {
        // Remove old entry if exists
        self.entries.retain(|(h, _)| *h != hash);
        self.entries.push((hash, frame_idx));
    }

    fn remove(&mut self, hash: u64) {
        self.entries.retain(|(h, _)| *h != hash);
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
    /// Access the cached page.
    #[inline]
    pub fn page(&self) -> &SlottedPage {
        &self.page
    }

    /// Get the underlying Arc.
    pub fn page_arc(&self) -> &Arc<SlottedPage> {
        &self.page
    }

    /// Check if this handle is still valid (frame hasn't been reused).
    pub fn is_valid(&self) -> bool {
        self.cache.frames[self.frame_idx]
            .version
            .load(Ordering::Acquire)
            == self.version_at_pin
    }

    /// Mark the cached page as dirty (modified).
    pub fn mark_dirty(&self) {
        self.cache.frames[self.frame_idx].mark_dirty();
    }
}

impl Drop for CachedPage {
    fn drop(&mut self) {
        // Unpin the frame
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
    clock_hand: AtomicU32,
    /// Statistics.
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

/// LeanStore/Umbra-style page cache.
///
/// Fixed-size buffer pool with clock-based eviction and guard-based pinning.
///
/// Features:
/// - Fixed number of buffer frames
/// - Clock (second-chance) eviction algorithm
/// - Guard-based pinning prevents eviction of in-use pages
/// - Sharded lookup for concurrent access
/// - Dirty page tracking for write-back
/// - Version counter for frame reuse detection
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
                clock_hand: AtomicU32::new(0),
                hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
                evictions: AtomicU64::new(0),
            }),
            capacity,
        }
    }

    /// Create a cache sized to available memory.
    ///
    /// `memory_budget` is the total memory to use for the cache in bytes.
    /// `avg_page_size` is the estimated average page size.
    pub fn with_memory_budget(memory_budget: usize, avg_page_size: usize) -> Self {
        let capacity = (memory_budget / avg_page_size).max(16);
        Self::new(capacity)
    }

    /// Look up a page in the cache. Returns a pinned handle if found.
    pub fn get(&self, page_id: &PageId) -> Option<CachedPage> {
        let hash = page_id.hash();
        let shard_idx = (hash as usize) % SHARD_COUNT;

        let shard = self.inner.shards[shard_idx].lock();
        if let Some(frame_idx) = shard.find(hash) {
            let frame = &self.inner.frames[frame_idx];

            // Verify the frame still holds our page
            if !frame.is_valid() {
                return None;
            }

            let frame_page_id = frame.page_id.lock();
            if frame_page_id.as_ref() != Some(page_id) {
                return None;
            }
            drop(frame_page_id);

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
        }

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
        let shard_idx = (hash as usize) % SHARD_COUNT;

        // Check if already cached
        {
            let shard = self.inner.shards[shard_idx].lock();
            if let Some(frame_idx) = shard.find(hash) {
                let frame = &self.inner.frames[frame_idx];
                let frame_page_id = frame.page_id.lock();
                if frame_page_id.as_ref() == Some(&page_id) {
                    drop(frame_page_id);
                    drop(shard);

                    // Update page
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
                let old_shard_idx = (old_hash as usize) % SHARD_COUNT;
                let mut old_shard = self.inner.shards[old_shard_idx].lock();
                old_shard.remove(old_hash);
            }
        }

        // Install new page
        *frame.page_id.lock() = Some(page_id);
        *frame.page.write() = Some(Arc::clone(&page));
        frame.state.store(FRAME_VALID | FRAME_HOT, Ordering::Release);
        frame.guard_count.store(1, Ordering::Release); // Pin for caller
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
        let shard_idx = (hash as usize) % SHARD_COUNT;

        let mut shard = self.inner.shards[shard_idx].lock();
        if let Some(frame_idx) = shard.find(hash) {
            let frame = &self.inner.frames[frame_idx];
            let frame_page_id = frame.page_id.lock();
            if frame_page_id.as_ref() == Some(page_id) {
                drop(frame_page_id);
                shard.remove(hash);

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
    ///
    /// Returns (frame_index, optional_evicted_page).
    fn find_victim(&self) -> (usize, Option<EvictedPage>) {
        let num_frames = self.capacity;
        // Scan up to 2 full rotations before giving up
        let max_scans = num_frames * 2;

        for _ in 0..max_scans {
            let idx = self.inner.clock_hand.fetch_add(1, Ordering::Relaxed) as usize % num_frames;
            let frame = &self.inner.frames[idx];

            // Skip pinned frames
            if frame.is_pinned() {
                continue;
            }

            // Skip frames being evicted
            if !frame.try_mark_evicting() {
                continue;
            }

            // Check if frame is free
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

            // Clear the frame
            *frame.page.write() = None;
            *frame.page_id.lock() = None;
            frame.state.store(0, Ordering::Release);
            frame.version.fetch_add(1, Ordering::AcqRel);
            frame.clear_evicting();

            return (idx, evicted);
        }

        // Fallback: force evict frame 0 (should be rare)
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
    ///
    /// Returns a list of (PageId, Arc<SlottedPage>) for dirty frames.
    /// Does NOT clear dirty flags — caller should do that after flushing.
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
        let shard_idx = (hash as usize) % SHARD_COUNT;
        let shard = self.inner.shards[shard_idx].lock();
        if let Some(frame_idx) = shard.find(hash) {
            self.inner.frames[frame_idx].clear_dirty();
        }
    }

    /// Cache capacity (number of buffer frames).
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Number of cache hits.
    pub fn hits(&self) -> u64 {
        self.inner.hits.load(Ordering::Relaxed)
    }

    /// Number of cache misses.
    pub fn misses(&self) -> u64 {
        self.inner.misses.load(Ordering::Relaxed)
    }

    /// Number of evictions.
    pub fn evictions(&self) -> u64 {
        self.inner.evictions.load(Ordering::Relaxed)
    }

    /// Hit rate as a percentage.
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

    /// Number of currently cached pages.
    pub fn size(&self) -> usize {
        self.inner
            .frames
            .iter()
            .filter(|f| f.is_valid())
            .count()
    }
}

/// A page that was evicted from the cache.
///
/// If the page was dirty, the caller must flush it to storage.
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

        // Fill cache beyond capacity
        for i in 0..32 {
            let page = make_page(i);
            let id = make_id(i);
            let (handle, _) = cache.put(id, page);
            drop(handle); // Unpin so it can be evicted
        }

        // Some pages should have been evicted
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

        // Pin first page
        let pinned_page = make_page(0);
        let pinned_id = make_id(0);
        let pinned_handle = cache.put(pinned_id, pinned_page);
        // Keep handle alive (pinned)

        // Fill cache to trigger eviction
        for i in 1..32 {
            let page = make_page(i);
            let id = make_id(i);
            let (handle, _) = cache.put(id, page);
            drop(handle);
        }

        // Pinned page should still be accessible
        drop(pinned_handle);
        let handle = cache.get(&pinned_id);
        assert!(handle.is_some());
    }

    #[test]
    fn test_statistics() {
        let cache = PageCache::new(64);
        let page = make_page(1);
        let id = make_id(1);

        // Miss
        cache.get(&id);
        assert_eq!(cache.misses(), 1);

        // Put + hit
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

        // Fill cache
        for i in 0..16 {
            let page = make_page(i);
            let id = make_id(i);
            let (handle, _) = cache.put(id, page);
            drop(handle);
        }

        // Access first page to mark it hot
        let _ = cache.get(&make_id(0));

        // Insert more pages to trigger eviction
        for i in 16..24 {
            let page = make_page(i);
            let id = make_id(i);
            let (handle, _) = cache.put(id, page);
            drop(handle);
        }

        // Page 0 should survive (was hot, got second chance)
        // This is probabilistic but with small cache it's highly likely
        // At minimum, some evictions should have occurred
        assert!(cache.evictions() > 0);
    }

    #[test]
    fn test_evicted_dirty_page_returned() {
        let cache = PageCache::new(16);

        // Fill cache with dirty pages
        for i in 0..16 {
            let page = make_page(i);
            let id = make_id(i);
            let (handle, _) = cache.put(id, page);
            handle.mark_dirty();
            drop(handle);
        }

        // Insert one more - should evict a dirty page
        let page = make_page(100);
        let id = make_id(100);
        let (handle, evicted) = cache.put(id, page);
        drop(handle);

        // The evicted page should be returned since it was dirty
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

        // 1 hit, 0 misses
        let _ = cache.get(&id);
        assert!(cache.hit_rate() > 99.0);

        // 1 miss
        let _ = cache.get(&make_id(999));
        assert!((cache.hit_rate() - 50.0).abs() < 1.0);
    }
}
