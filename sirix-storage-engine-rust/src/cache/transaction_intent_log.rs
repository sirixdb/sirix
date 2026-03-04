//! Transaction Intent Log (TIL) - epoch-based page modification tracking.
//!
//! Equivalent to Java's `TransactionIntentLog`. Stores modified pages during
//! a read/write transaction. Supports O(1) epoch-based snapshotting for
//! async auto-commit using crossbeam's epoch-based reclamation.
//!
//! 3-Layer lookup:
//! - Layer 1: Current TIL (fast path for active transaction)
//! - Layer 2: Frozen snapshot (for background flush reads)
//! - Layer 3: Completed disk offsets (stale reference resolution)

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
    /// Create a new container with both versions.
    pub fn new(complete: Option<PageData>, modified: Option<PageData>) -> Self {
        Self { complete, modified }
    }

    /// Create an empty container.
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
struct CompletedEntry {
    /// Packed key: (generation << 32) | (log_key & 0xFFFFFFFF).
    packed_key: u64,
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
    completed: Vec<CompletedEntry>,
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
            completed: Vec::new(),
            layer3_hits: 0,
        }
    }

    /// Get a page container from the TIL using 3-layer lookup.
    ///
    /// Returns `None` if the page is not in any TIL layer.
    /// For Layer 3 hits, updates the ref's key/hash and returns `None`
    /// (caller should load from disk).
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

        // Layer 3: Completed disk offsets
        if !self.completed.is_empty() {
            let packed = ((ref_generation as u64) << 32) | (log_key as u64 & 0xFFFFFFFF);
            if let Some(pos) = self.completed.iter().position(|e| e.packed_key == packed) {
                let _entry = self.completed.remove(pos);
                self.layer3_hits += 1;
                // Caller needs to resolve from disk
                return None;
            }
        }

        None
    }

    /// Get a mutable reference to a page container.
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

        // Capture current state (swap with fresh arrays)
        let entries_cap = self.entries.len();
        let refs_cap = self.refs.len();

        let mut fresh_entries = Vec::with_capacity(entries_cap);
        fresh_entries.resize_with(entries_cap, || None);
        let snap_entries = std::mem::replace(&mut self.entries, fresh_entries);

        let mut fresh_refs = Vec::with_capacity(refs_cap);
        fresh_refs.resize_with(refs_cap, TilPageRef::new);
        let snap_refs = std::mem::replace(&mut self.refs, fresh_refs);

        let snap_generation = self.current_generation;

        // Initialize side-channel arrays
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

        // Increment generation AFTER capturing snapshot
        self.current_generation += 1;
        self.size = 0;

        snap_size
    }

    /// Check if a reference is in the frozen snapshot.
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
    pub fn set_snapshot_disk_offset(&mut self, index: usize, offset: i64) {
        if let Some(ref mut snap) = self.snapshot {
            if index < snap.disk_offsets.len() {
                snap.disk_offsets[index] = offset;
            }
        }
    }

    /// Store a page hash from the background thread.
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
    pub fn is_snapshot_commit_complete(&self) -> bool {
        self.snapshot
            .as_ref()
            .map(|s| s.commit_complete.load(Ordering::Acquire))
            .unwrap_or(true)
    }

    /// Get the snapshot size.
    pub fn snapshot_size(&self) -> usize {
        self.snapshot.as_ref().map(|s| s.size).unwrap_or(0)
    }

    /// Get snapshot entry at the given index.
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
                        // KVL: already written to disk by background thread
                        if disk_offset != NULL_ID_LONG {
                            let packed = ((snap.generation as u64) << 32)
                                | (i as u64 & 0xFFFFFFFF);
                            self.completed.push(CompletedEntry {
                                packed_key: packed,
                                disk_offset,
                                hash,
                            });
                        }
                        // Container is dropped (pages released)
                    } else {
                        // Structural page: promote to current TIL
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

        // Clear snapshot
        self.snapshot = None;
        self.completed.clear();
    }

    /// Number of entries in the current TIL.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Current generation.
    pub fn current_generation(&self) -> i32 {
        self.current_generation
    }

    /// Snapshot generation.
    pub fn snapshot_generation(&self) -> i32 {
        self.snapshot
            .as_ref()
            .map(|s| s.generation)
            .unwrap_or(-1)
    }

    /// Layer 3 hit count.
    pub fn layer3_hits(&self) -> u64 {
        self.layer3_hits
    }

    /// Get a list view of current entries.
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

        // Add entries to gen 0
        for i in 0..5 {
            let c = PageContainer::new(
                Some(make_kvl_data(i)),
                Some(make_kvl_data(i)),
            );
            til.put(c);
        }

        // Snapshot
        let snap_size = til.snapshot();
        assert_eq!(snap_size, 5);
        assert_eq!(til.size(), 0); // Fresh TIL
        assert_eq!(til.current_generation(), 1);

        // Old entries accessible via Layer 2
        for i in 0..5 {
            let entry = til.get(0, i as i32);
            assert!(entry.is_some(), "snapshot entry {} should be accessible", i);
        }

        // New entries go to gen 1
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

        assert!(!til.is_frozen(generation, log_key)); // Not frozen yet

        til.snapshot();
        assert!(til.is_frozen(generation, log_key)); // Now frozen
    }

    #[test]
    fn test_cleanup_snapshot_kvl() {
        let mut til = TransactionIntentLog::new(16);

        // Add KVL pages
        for i in 0..3 {
            let c = PageContainer::new(
                Some(make_kvl_data(i)),
                Some(make_kvl_data(i)),
            );
            til.put(c);
        }

        til.snapshot();

        // Simulate background thread writing disk offsets
        for i in 0..3 {
            til.set_snapshot_disk_offset(i, (i * 1000 + 4096) as i64);
            til.set_snapshot_hash(i, (i * 111) as u64);
        }

        let promoted = til.cleanup_snapshot();
        assert!(promoted.is_empty()); // KVL pages are not promoted

        // Completed map should have 3 entries
        assert_eq!(til.completed.len(), 3);
    }

    #[test]
    fn test_cleanup_snapshot_structural() {
        let mut til = TransactionIntentLog::new(16);

        // Add structural (non-KVL) pages
        for i in 0..2 {
            let c = PageContainer::new(
                Some(make_structural_data(i)),
                Some(make_structural_data(i)),
            );
            til.put(c);
        }

        til.snapshot();

        let promoted = til.cleanup_snapshot();
        assert_eq!(promoted.len(), 2); // Structural pages are promoted
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

        // Wrong generation
        assert!(til.get(999, log_key).is_none());
    }
}
