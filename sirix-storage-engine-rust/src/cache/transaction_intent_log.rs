//! Transaction Intent Log (TIL) - epoch-based page modification tracking.
//!
//! Equivalent to Java's `TransactionIntentLog`. Stores modified pages during
//! a read/write transaction. Supports O(1) epoch-based snapshotting for
//! async auto-commit using crossbeam's epoch-based reclamation.
//!
//! 3-Layer lookup:
//! - Layer 1: Current TIL (fast path for active transaction)
//! - Layer 2: Frozen snapshot (for background flush reads)
//! - Layer 3: Completed disk offsets (O(1) HashMap lookup)

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use crate::constants::NULL_ID_INT;
use crate::constants::NULL_ID_LONG;

/// A page container holding the complete (read-only) and modified versions.
#[derive(Debug)]
pub struct PageContainer {
    /// Complete version of the page (unmodified snapshot).
    pub complete: Option<PageData>,
    /// Modified version with uncommitted changes.
    pub modified: Option<PageData>,
}

impl PageContainer {
    #[inline]
    pub fn new(complete: Option<PageData>, modified: Option<PageData>) -> Self {
        Self { complete, modified }
    }

    #[inline]
    pub fn empty() -> Self {
        Self {
            complete: None,
            modified: None,
        }
    }
}

/// Opaque page data - the actual page bytes or a reference to a cached page.
#[derive(Debug, Clone)]
pub struct PageData {
    /// Raw serialized page data.
    pub data: Vec<u8>,
    /// Page key for identification.
    pub page_key: i64,
    /// Whether this is a KeyValueLeafPage.
    pub is_kvl: bool,
}

/// A reference tracked by the TIL.
#[derive(Debug)]
pub struct TilPageRef {
    /// File offset (set on commit, NULL_ID_LONG before).
    pub key: i64,
    /// Log key (index into TIL arrays).
    pub log_key: i32,
    /// Generation when this ref was added to TIL.
    pub active_til_generation: i32,
    /// Hash of the compressed page.
    pub hash: u64,
}

impl TilPageRef {
    #[inline]
    pub fn new() -> Self {
        Self {
            key: NULL_ID_LONG,
            log_key: NULL_ID_INT,
            active_til_generation: -1,
            hash: 0,
        }
    }
}

/// Completed disk offset entry for Layer 3 stale reference resolution.
#[allow(dead_code)]
struct CompletedEntry {
    /// Disk offset.
    disk_offset: i64,
    /// Page hash.
    hash: u64,
}

/// Frozen snapshot state for background flush.
struct Snapshot {
    entries: Vec<Option<PageContainer>>,
    refs: Vec<TilPageRef>,
    size: usize,
    generation: i32,
    /// Side-channel disk offsets written by background thread.
    disk_offsets: Vec<i64>,
    /// Side-channel hashes written by background thread.
    hashes: Vec<u64>,
    /// Set to true by background thread when flush is complete.
    commit_complete: AtomicBool,
}

/// Transaction intent log with epoch-based O(1) snapshotting.
///
/// Stores modified pages during a transaction. When the transaction commits,
/// pages are written to storage. On rollback, the TIL is simply cleared.
///
/// Supports epoch-based snapshotting: `snapshot()` swaps the current arrays
/// and increments the generation counter. A background thread can then flush
/// the frozen snapshot while the insert thread continues with a fresh TIL.
pub struct TransactionIntentLog {
    /// Page containers indexed by log_key.
    entries: Vec<Option<PageContainer>>,
    /// Back-references: refs[i] is the TilPageRef for entries[i].
    refs: Vec<TilPageRef>,
    /// Number of active entries.
    size: usize,
    /// Current generation (incremented on each snapshot).
    current_generation: i32,
    /// Frozen snapshot (None if no active snapshot).
    snapshot: Option<Snapshot>,
    /// Completed disk offsets for stale reference resolution (Layer 3).
    /// Uses HashMap with packed key for O(1) lookup (replaces O(n) Vec scan).
    completed: HashMap<u64, CompletedEntry>,
    /// Counter: Layer 3 hits.
    layer3_hits: u64,
}

impl TransactionIntentLog {
    /// Create a new transaction intent log.
    ///
    /// `initial_capacity` is the expected number of modified pages.
    pub fn new(initial_capacity: usize) -> Self {
        let cap = initial_capacity.max(64);
        let mut entries = Vec::with_capacity(cap);
        let mut refs = Vec::with_capacity(cap);
        entries.resize_with(cap, || None);
        refs.resize_with(cap, TilPageRef::new);

        Self {
            entries,
            refs,
            size: 0,
            current_generation: 0,
            snapshot: None,
            completed: HashMap::new(),
            layer3_hits: 0,
        }
    }

    /// Get a page container from the TIL using 3-layer lookup.
    ///
    /// Returns `None` if the page is not in any TIL layer.
    /// For Layer 3 hits, updates the ref's key/hash and returns `None`
    /// (caller should load from disk).
    #[inline]
    pub fn get(&mut self, ref_generation: i32, log_key: i32) -> Option<&PageContainer> {
        if log_key < 0 {
            return None;
        }

        let lk = log_key as usize;

        // Layer 1: Current TIL (fast path)
        if ref_generation == self.current_generation && lk < self.size {
            return self.entries[lk].as_ref();
        }

        // Layer 2: Active snapshot
        if let Some(ref snap) = self.snapshot {
            if ref_generation == snap.generation && lk < snap.size {
                return snap.entries[lk].as_ref();
            }
        }

        // Layer 3: Completed disk offsets - O(1) HashMap lookup
        if !self.completed.is_empty() {
            let packed = ((ref_generation as u64) << 32) | (log_key as u64 & 0xFFFFFFFF);
            if self.completed.remove(&packed).is_some() {
                self.layer3_hits += 1;
                return None;
            }
        }

        None
    }

    /// Get a mutable reference to a page container.
    #[inline]
    pub fn get_mut(&mut self, ref_generation: i32, log_key: i32) -> Option<&mut PageContainer> {
        if log_key < 0 {
            return None;
        }
        let lk = log_key as usize;
        if ref_generation == self.current_generation && lk < self.size {
            return self.entries[lk].as_mut();
        }
        None
    }

    /// Add a page container to the TIL.
    ///
    /// Returns the log_key and generation for the caller to store in their ref.
    #[inline]
    pub fn put(&mut self, container: PageContainer) -> (i32, i32) {
        self.ensure_capacity();

        let log_key = self.size as i32;
        let generation = self.current_generation;

        self.entries[self.size] = Some(container);
        self.refs[self.size] = TilPageRef {
            key: NULL_ID_LONG,
            log_key,
            active_til_generation: generation,
            hash: 0,
        };
        self.size += 1;

        (log_key, generation)
    }

    /// Ensure capacity for one more entry.
    #[inline]
    fn ensure_capacity(&mut self) {
        if self.size >= self.entries.len() {
            let new_cap = self.entries.len() * 2;
            self.entries.resize_with(new_cap, || None);
            self.refs.resize_with(new_cap, TilPageRef::new);
        }
    }

    /// Freeze current entries for background flush. O(1) array swap.
    ///
    /// After this call, the insert thread continues with fresh empty arrays.
    /// Returns the snapshot size (0 = nothing to flush).
    pub fn snapshot(&mut self) -> usize {
        let snap_size = self.size;

        let entries_cap = self.entries.len();
        let refs_cap = self.refs.len();

        let mut fresh_entries = Vec::with_capacity(entries_cap);
        fresh_entries.resize_with(entries_cap, || None);
        let snap_entries = std::mem::replace(&mut self.entries, fresh_entries);

        let mut fresh_refs = Vec::with_capacity(refs_cap);
        fresh_refs.resize_with(refs_cap, TilPageRef::new);
        let snap_refs = std::mem::replace(&mut self.refs, fresh_refs);

        let snap_generation = self.current_generation;

        let mut disk_offsets = Vec::with_capacity(snap_size);
        disk_offsets.resize(snap_size, NULL_ID_LONG);
        let mut hashes = Vec::with_capacity(snap_size);
        hashes.resize(snap_size, 0u64);

        self.snapshot = Some(Snapshot {
            entries: snap_entries,
            refs: snap_refs,
            size: snap_size,
            generation: snap_generation,
            disk_offsets,
            hashes,
            commit_complete: AtomicBool::new(false),
        });

        self.current_generation += 1;
        self.size = 0;

        snap_size
    }

    /// Check if a reference is in the frozen snapshot.
    #[inline]
    pub fn is_frozen(&self, ref_generation: i32, log_key: i32) -> bool {
        if let Some(ref snap) = self.snapshot {
            ref_generation == snap.generation
                && log_key >= 0
                && (log_key as usize) < snap.size
        } else {
            false
        }
    }

    /// Store a disk offset from the background thread.
    #[inline]
    pub fn set_snapshot_disk_offset(&mut self, index: usize, offset: i64) {
        if let Some(ref mut snap) = self.snapshot {
            if index < snap.disk_offsets.len() {
                snap.disk_offsets[index] = offset;
            }
        }
    }

    /// Store a page hash from the background thread.
    #[inline]
    pub fn set_snapshot_hash(&mut self, index: usize, hash: u64) {
        if let Some(ref mut snap) = self.snapshot {
            if index < snap.hashes.len() {
                snap.hashes[index] = hash;
            }
        }
    }

    /// Mark the snapshot flush as complete.
    pub fn mark_snapshot_commit_complete(&self) {
        if let Some(ref snap) = self.snapshot {
            snap.commit_complete.store(true, Ordering::Release);
        }
    }

    /// Check if the snapshot commit has completed.
    #[inline]
    pub fn is_snapshot_commit_complete(&self) -> bool {
        self.snapshot
            .as_ref()
            .map(|s| s.commit_complete.load(Ordering::Acquire))
            .unwrap_or(true)
    }

    /// Get the snapshot size.
    #[inline]
    pub fn snapshot_size(&self) -> usize {
        self.snapshot.as_ref().map(|s| s.size).unwrap_or(0)
    }

    /// Get snapshot entry at the given index.
    #[inline]
    pub fn get_snapshot_entry(&self, index: usize) -> Option<&PageContainer> {
        self.snapshot
            .as_ref()
            .and_then(|s| {
                if index < s.size {
                    s.entries[index].as_ref()
                } else {
                    None
                }
            })
    }

    /// Get snapshot ref at the given index.
    #[inline]
    pub fn get_snapshot_ref(&self, index: usize) -> Option<&TilPageRef> {
        self.snapshot.as_ref().and_then(|s| {
            if index < s.size {
                Some(&s.refs[index])
            } else {
                None
            }
        })
    }

    /// Clean up a completed snapshot.
    ///
    /// For KVL pages: applies disk offsets and stores in completed map.
    /// For structural pages: promotes to current TIL.
    ///
    /// Returns entries that need to be promoted (structural pages).
    pub fn cleanup_snapshot(&mut self) -> Vec<PageContainer> {
        let mut promoted = Vec::new();

        if let Some(mut snap) = self.snapshot.take() {
            for i in 0..snap.size {
                let disk_offset = snap.disk_offsets[i];
                let hash = snap.hashes[i];

                if let Some(container) = snap.entries[i].take() {
                    let is_kvl = container.modified
                        .as_ref()
                        .map(|d| d.is_kvl)
                        .unwrap_or(false);

                    if is_kvl {
                        if disk_offset != NULL_ID_LONG {
                            let packed = ((snap.generation as u64) << 32)
                                | (i as u64 & 0xFFFFFFFF);
                            self.completed.insert(packed, CompletedEntry {
                                disk_offset,
                                hash,
                            });
                        }
                    } else {
                        promoted.push(container);
                    }
                }
            }
        }

        promoted
    }

    /// Clear the TIL (transaction rollback).
    pub fn clear(&mut self) {
        for i in 0..self.size {
            self.entries[i] = None;
            self.refs[i] = TilPageRef::new();
        }
        self.size = 0;

        self.snapshot = None;
        self.completed.clear();
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }

    #[inline]
    pub fn current_generation(&self) -> i32 {
        self.current_generation
    }

    #[inline]
    pub fn snapshot_generation(&self) -> i32 {
        self.snapshot
            .as_ref()
            .map(|s| s.generation)
            .unwrap_or(-1)
    }

    #[inline]
    pub fn layer3_hits(&self) -> u64 {
        self.layer3_hits
    }

    #[inline]
    pub fn entries(&self) -> &[Option<PageContainer>] {
        &self.entries[..self.size]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_kvl_data(key: i64) -> PageData {
        PageData {
            data: vec![1, 2, 3],
            page_key: key,
            is_kvl: true,
        }
    }

    fn make_structural_data(key: i64) -> PageData {
        PageData {
            data: vec![4, 5, 6],
            page_key: key,
            is_kvl: false,
        }
    }

    #[test]
    fn test_put_and_get() {
        let mut til = TransactionIntentLog::new(16);
        let container = PageContainer::new(
            Some(make_kvl_data(1)),
            Some(make_kvl_data(1)),
        );

        let (log_key, generation) = til.put(container);
        assert_eq!(log_key, 0);
        assert_eq!(generation, 0);
        assert_eq!(til.size(), 1);

        let entry = til.get(generation, log_key);
        assert!(entry.is_some());
    }

    #[test]
    fn test_multiple_entries() {
        let mut til = TransactionIntentLog::new(4);

        for i in 0..10 {
            let c = PageContainer::new(
                Some(make_kvl_data(i)),
                Some(make_kvl_data(i)),
            );
            til.put(c);
        }

        assert_eq!(til.size(), 10);
        for i in 0..10 {
            assert!(til.get(0, i as i32).is_some());
        }
    }

    #[test]
    fn test_snapshot() {
        let mut til = TransactionIntentLog::new(16);

        for i in 0..5 {
            let c = PageContainer::new(
                Some(make_kvl_data(i)),
                Some(make_kvl_data(i)),
            );
            til.put(c);
        }

        let snap_size = til.snapshot();
        assert_eq!(snap_size, 5);
        assert_eq!(til.size(), 0);
        assert_eq!(til.current_generation(), 1);

        for i in 0..5 {
            let entry = til.get(0, i as i32);
            assert!(entry.is_some(), "snapshot entry {} should be accessible", i);
        }

        let c = PageContainer::new(
            Some(make_kvl_data(100)),
            Some(make_kvl_data(100)),
        );
        let (log_key, generation) = til.put(c);
        assert_eq!(generation, 1);
        assert_eq!(log_key, 0);
    }

    #[test]
    fn test_is_frozen() {
        let mut til = TransactionIntentLog::new(16);
        let c = PageContainer::new(
            Some(make_kvl_data(1)),
            Some(make_kvl_data(1)),
        );
        let (log_key, generation) = til.put(c);

        assert!(!til.is_frozen(generation, log_key));

        til.snapshot();
        assert!(til.is_frozen(generation, log_key));
    }

    #[test]
    fn test_cleanup_snapshot_kvl() {
        let mut til = TransactionIntentLog::new(16);

        for i in 0..3 {
            let c = PageContainer::new(
                Some(make_kvl_data(i)),
                Some(make_kvl_data(i)),
            );
            til.put(c);
        }

        til.snapshot();

        for i in 0..3 {
            til.set_snapshot_disk_offset(i, (i * 1000 + 4096) as i64);
            til.set_snapshot_hash(i, (i * 111) as u64);
        }

        let promoted = til.cleanup_snapshot();
        assert!(promoted.is_empty());
        assert_eq!(til.completed.len(), 3);
    }

    #[test]
    fn test_cleanup_snapshot_structural() {
        let mut til = TransactionIntentLog::new(16);

        for i in 0..2 {
            let c = PageContainer::new(
                Some(make_structural_data(i)),
                Some(make_structural_data(i)),
            );
            til.put(c);
        }

        til.snapshot();

        let promoted = til.cleanup_snapshot();
        assert_eq!(promoted.len(), 2);
    }

    #[test]
    fn test_clear() {
        let mut til = TransactionIntentLog::new(16);

        for i in 0..5 {
            let c = PageContainer::new(
                Some(make_kvl_data(i)),
                Some(make_kvl_data(i)),
            );
            til.put(c);
        }

        til.clear();
        assert_eq!(til.size(), 0);
    }

    #[test]
    fn test_generation_tracking() {
        let mut til = TransactionIntentLog::new(16);
        assert_eq!(til.current_generation(), 0);

        til.snapshot();
        assert_eq!(til.current_generation(), 1);

        til.snapshot();
        assert_eq!(til.current_generation(), 2);
    }

    #[test]
    fn test_negative_log_key_returns_none() {
        let mut til = TransactionIntentLog::new(16);
        assert!(til.get(0, -1).is_none());
        assert!(til.get(0, -15).is_none());
    }

    #[test]
    fn test_wrong_generation_returns_none() {
        let mut til = TransactionIntentLog::new(16);
        let c = PageContainer::new(
            Some(make_kvl_data(1)),
            Some(make_kvl_data(1)),
        );
        let (log_key, _gen) = til.put(c);

        assert!(til.get(999, log_key).is_none());
    }

    // --- Additional behavioral and coverage tests ---

    #[test]
    fn test_layer3_lookup_o1_hashmap() {
        let mut til = TransactionIntentLog::new(16);

        // Put entries, snapshot, set disk offsets, cleanup to populate Layer 3
        for i in 0..5 {
            let c = PageContainer::new(
                Some(make_kvl_data(i)),
                Some(make_kvl_data(i)),
            );
            til.put(c);
        }
        let gen0 = til.current_generation();
        til.snapshot();

        for i in 0..5 {
            til.set_snapshot_disk_offset(i, (i * 1000 + 4096) as i64);
            til.set_snapshot_hash(i, (i * 111) as u64);
        }
        til.cleanup_snapshot();

        // Layer 3 lookups should work and remove entries
        assert_eq!(til.layer3_hits(), 0);
        til.get(gen0, 0);
        assert_eq!(til.layer3_hits(), 1);
        til.get(gen0, 2);
        assert_eq!(til.layer3_hits(), 2);

        // Re-lookup should not hit (entry was removed)
        til.get(gen0, 0);
        assert_eq!(til.layer3_hits(), 2);
    }

    #[test]
    fn test_get_mut() {
        let mut til = TransactionIntentLog::new(16);
        let c = PageContainer::new(
            Some(make_kvl_data(1)),
            Some(make_kvl_data(1)),
        );
        let (log_key, generation) = til.put(c);

        // Should be able to get mutable reference
        let entry = til.get_mut(generation, log_key);
        assert!(entry.is_some());

        // Modify it
        let entry = entry.unwrap();
        entry.modified = Some(make_kvl_data(42));
    }

    #[test]
    fn test_get_mut_wrong_generation() {
        let mut til = TransactionIntentLog::new(16);
        let c = PageContainer::new(
            Some(make_kvl_data(1)),
            Some(make_kvl_data(1)),
        );
        let (log_key, _gen) = til.put(c);
        assert!(til.get_mut(999, log_key).is_none());
    }

    #[test]
    fn test_get_mut_negative_log_key() {
        let mut til = TransactionIntentLog::new(16);
        assert!(til.get_mut(0, -1).is_none());
    }

    #[test]
    fn test_snapshot_disk_offset_out_of_bounds() {
        let mut til = TransactionIntentLog::new(16);
        let c = PageContainer::new(
            Some(make_kvl_data(1)),
            Some(make_kvl_data(1)),
        );
        til.put(c);
        til.snapshot();

        // Setting out-of-bounds should be a no-op
        til.set_snapshot_disk_offset(999, 12345);
        til.set_snapshot_hash(999, 12345);
    }

    #[test]
    fn test_snapshot_commit_lifecycle() {
        let mut til = TransactionIntentLog::new(16);
        let c = PageContainer::new(
            Some(make_kvl_data(1)),
            Some(make_kvl_data(1)),
        );
        til.put(c);
        til.snapshot();

        // Initially not complete
        assert!(!til.is_snapshot_commit_complete());

        // Mark complete
        til.mark_snapshot_commit_complete();
        assert!(til.is_snapshot_commit_complete());
    }

    #[test]
    fn test_no_snapshot_commit_complete_returns_true() {
        let til = TransactionIntentLog::new(16);
        // No snapshot exists, should return true (nothing to wait for)
        assert!(til.is_snapshot_commit_complete());
    }

    #[test]
    fn test_snapshot_size_no_snapshot() {
        let til = TransactionIntentLog::new(16);
        assert_eq!(til.snapshot_size(), 0);
    }

    #[test]
    fn test_snapshot_generation_no_snapshot() {
        let til = TransactionIntentLog::new(16);
        assert_eq!(til.snapshot_generation(), -1);
    }

    #[test]
    fn test_get_snapshot_entry_and_ref() {
        let mut til = TransactionIntentLog::new(16);
        for i in 0..3 {
            let c = PageContainer::new(
                Some(make_kvl_data(i)),
                Some(make_kvl_data(i)),
            );
            til.put(c);
        }
        til.snapshot();

        assert!(til.get_snapshot_entry(0).is_some());
        assert!(til.get_snapshot_entry(2).is_some());
        assert!(til.get_snapshot_entry(3).is_none()); // out of bounds

        let sref = til.get_snapshot_ref(0).unwrap();
        assert_eq!(sref.log_key, 0);
        assert!(til.get_snapshot_ref(3).is_none());
    }

    #[test]
    fn test_entries_view() {
        let mut til = TransactionIntentLog::new(16);
        for i in 0..3 {
            let c = PageContainer::new(
                Some(make_kvl_data(i)),
                Some(make_kvl_data(i)),
            );
            til.put(c);
        }

        let entries = til.entries();
        assert_eq!(entries.len(), 3);
        assert!(entries[0].is_some());
        assert!(entries[2].is_some());
    }

    #[test]
    fn test_empty_container() {
        let c = PageContainer::empty();
        assert!(c.complete.is_none());
        assert!(c.modified.is_none());
    }

    #[test]
    fn test_til_page_ref_defaults() {
        let r = TilPageRef::new();
        assert_eq!(r.key, NULL_ID_LONG);
        assert_eq!(r.log_key, NULL_ID_INT);
        assert_eq!(r.active_til_generation, -1);
        assert_eq!(r.hash, 0);
    }

    #[test]
    fn test_capacity_growth() {
        let mut til = TransactionIntentLog::new(2); // small initial capacity (clamped to 64)
        // Fill beyond initial capacity to trigger growth
        for i in 0..200 {
            let c = PageContainer::new(
                Some(make_kvl_data(i)),
                Some(make_kvl_data(i)),
            );
            til.put(c);
        }
        assert_eq!(til.size(), 200);
        // All entries should be accessible
        for i in 0..200 {
            assert!(til.get(0, i as i32).is_some());
        }
    }

    #[test]
    fn test_multiple_snapshots_overwrites_previous() {
        let mut til = TransactionIntentLog::new(16);

        let c = PageContainer::new(Some(make_kvl_data(1)), Some(make_kvl_data(1)));
        til.put(c);
        til.snapshot(); // gen 0 frozen

        let c = PageContainer::new(Some(make_kvl_data(2)), Some(make_kvl_data(2)));
        til.put(c);
        til.snapshot(); // gen 1 frozen, gen 0 snapshot is overwritten

        // gen 0 entries are no longer accessible via Layer 2
        assert!(til.get(0, 0).is_none());
        // gen 1 entries are accessible
        assert!(til.get(1, 0).is_some());
    }

    #[test]
    fn test_clear_resets_everything() {
        let mut til = TransactionIntentLog::new(16);

        for i in 0..5 {
            let c = PageContainer::new(Some(make_kvl_data(i)), Some(make_kvl_data(i)));
            til.put(c);
        }
        til.snapshot();
        for i in 0..3 {
            til.set_snapshot_disk_offset(i, 4096);
            til.set_snapshot_hash(i, 111);
        }
        til.cleanup_snapshot();

        // Now clear
        til.clear();
        assert_eq!(til.size(), 0);
        assert!(til.snapshot.is_none());
        assert!(til.completed.is_empty());
    }

    #[test]
    fn test_mixed_kvl_structural_cleanup() {
        let mut til = TransactionIntentLog::new(16);

        // Mix of KVL and structural pages
        let c = PageContainer::new(Some(make_kvl_data(1)), Some(make_kvl_data(1)));
        til.put(c);
        let c = PageContainer::new(Some(make_structural_data(2)), Some(make_structural_data(2)));
        til.put(c);
        let c = PageContainer::new(Some(make_kvl_data(3)), Some(make_kvl_data(3)));
        til.put(c);

        til.snapshot();

        til.set_snapshot_disk_offset(0, 4096);
        til.set_snapshot_hash(0, 111);
        til.set_snapshot_disk_offset(2, 8192);
        til.set_snapshot_hash(2, 222);

        let promoted = til.cleanup_snapshot();

        // Only structural page (index 1) should be promoted
        assert_eq!(promoted.len(), 1);
        // KVL pages should be in completed map
        assert_eq!(til.completed.len(), 2);
    }
}
