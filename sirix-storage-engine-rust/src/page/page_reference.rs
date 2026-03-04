//! Page reference - a pointer to a page in the storage file.
//!
//! Equivalent to Java's `PageReference` class.

use crate::constants::NULL_ID_INT;
use crate::constants::NULL_ID_LONG;
use crate::types::PageFragmentKey;

/// A reference to a page stored on disk or in the transaction intent log.
///
/// Contains the file offset, optional hash for integrity checks,
/// and page fragment keys for versioned pages.
#[derive(Debug, Clone)]
pub struct PageReference {
    /// Byte offset in the data file where the page starts.
    /// `NULL_ID_LONG` if the page is not yet persisted.
    key: i64,

    /// In-memory log key for the transaction intent log (TIL).
    /// `NULL_ID_INT` if not in the TIL.
    log_key: i32,

    /// TIL generation counter for epoch-based snapshotting.
    active_til_generation: i32,

    /// Database ID for global cache disambiguation.
    database_id: i64,

    /// Resource ID for resource-level cache disambiguation.
    resource_id: i64,

    /// SHA-256 hash of the serialized page (for integrity verification).
    hash: Option<Vec<u8>>,

    /// Page fragment keys for versioned/incremental pages.
    page_fragments: Vec<PageFragmentKey>,

    /// Cached hash code for HashMap use.
    cached_hash: u64,
}

impl PageReference {
    /// Create a new empty page reference (not yet persisted).
    pub fn new() -> Self {
        Self {
            key: NULL_ID_LONG,
            log_key: NULL_ID_INT,
            active_til_generation: -1,
            database_id: NULL_ID_LONG,
            resource_id: NULL_ID_LONG,
            hash: None,
            page_fragments: Vec::new(),
            cached_hash: 0,
        }
    }

    /// Create a page reference pointing to a specific file offset.
    pub fn with_key(key: i64) -> Self {
        let mut r = Self::new();
        r.key = key;
        r.update_hash();
        r
    }

    /// File offset of the referenced page.
    #[inline]
    pub fn key(&self) -> i64 {
        self.key
    }

    /// Set the file offset.
    #[inline]
    pub fn set_key(&mut self, key: i64) {
        self.key = key;
        self.update_hash();
    }

    /// Whether this reference points to a persisted page.
    #[inline]
    pub fn is_persisted(&self) -> bool {
        self.key != NULL_ID_LONG
    }

    /// TIL log key.
    #[inline]
    pub fn log_key(&self) -> i32 {
        self.log_key
    }

    /// Set the TIL log key.
    #[inline]
    pub fn set_log_key(&mut self, log_key: i32) {
        self.log_key = log_key;
    }

    /// Active TIL generation.
    #[inline]
    pub fn active_til_generation(&self) -> i32 {
        self.active_til_generation
    }

    /// Set the active TIL generation.
    #[inline]
    pub fn set_active_til_generation(&mut self, generation: i32) {
        self.active_til_generation = generation;
    }

    /// Database ID.
    #[inline]
    pub fn database_id(&self) -> i64 {
        self.database_id
    }

    /// Set database ID.
    #[inline]
    pub fn set_database_id(&mut self, id: i64) {
        self.database_id = id;
    }

    /// Resource ID.
    #[inline]
    pub fn resource_id(&self) -> i64 {
        self.resource_id
    }

    /// Set resource ID.
    #[inline]
    pub fn set_resource_id(&mut self, id: i64) {
        self.resource_id = id;
    }

    /// SHA-256 hash of the page contents.
    #[inline]
    pub fn hash_bytes(&self) -> Option<&[u8]> {
        self.hash.as_deref()
    }

    /// Set the integrity hash from raw bytes.
    #[inline]
    pub fn set_hash(&mut self, hash: Vec<u8>) {
        self.hash = Some(hash);
    }

    /// Set the integrity hash from a u64 value (XXH3).
    #[inline]
    pub fn set_hash_value(&mut self, hash: u64) {
        self.hash = Some(hash.to_le_bytes().to_vec());
    }

    /// Page fragment keys for versioned pages.
    #[inline]
    pub fn page_fragments(&self) -> &[PageFragmentKey] {
        &self.page_fragments
    }

    /// Add a page fragment.
    #[inline]
    pub fn add_page_fragment(&mut self, fragment: PageFragmentKey) {
        self.page_fragments.push(fragment);
    }

    /// Clear all page fragments.
    #[inline]
    pub fn clear_page_fragments(&mut self) {
        self.page_fragments.clear();
    }

    /// Fix up database and resource IDs (after deserialization).
    #[inline]
    pub fn fixup_ids(&mut self, database_id: i64, resource_id: i64) {
        self.database_id = database_id;
        self.resource_id = resource_id;
    }

    /// Recompute cached hash.
    fn update_hash(&mut self) {
        // FNV-1a hash for fast HashMap lookups
        let mut h: u64 = 0xcbf29ce484222325;
        for b in self.key.to_le_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        for b in self.database_id.to_le_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        for b in self.resource_id.to_le_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        self.cached_hash = h;
    }
}

impl Default for PageReference {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for PageReference {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
            && self.database_id == other.database_id
            && self.resource_id == other.resource_id
    }
}

impl Eq for PageReference {}

impl std::hash::Hash for PageReference {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_u64(self.cached_hash);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_reference_not_persisted() {
        let r = PageReference::new();
        assert!(!r.is_persisted());
        assert_eq!(r.key(), NULL_ID_LONG);
        assert_eq!(r.log_key(), NULL_ID_INT);
    }

    #[test]
    fn test_with_key() {
        let r = PageReference::with_key(1024);
        assert!(r.is_persisted());
        assert_eq!(r.key(), 1024);
    }

    #[test]
    fn test_fixup_ids() {
        let mut r = PageReference::with_key(100);
        r.fixup_ids(1, 2);
        assert_eq!(r.database_id(), 1);
        assert_eq!(r.resource_id(), 2);
    }

    #[test]
    fn test_page_fragments() {
        let mut r = PageReference::new();
        r.add_page_fragment(PageFragmentKey {
            revision: 1,
            offset: 100,
        });
        r.add_page_fragment(PageFragmentKey {
            revision: 2,
            offset: 200,
        });
        assert_eq!(r.page_fragments().len(), 2);

        r.clear_page_fragments();
        assert!(r.page_fragments().is_empty());
    }

    #[test]
    fn test_equality_and_hash() {
        use std::collections::HashSet;

        let mut r1 = PageReference::with_key(100);
        r1.fixup_ids(1, 2);

        let mut r2 = PageReference::with_key(100);
        r2.fixup_ids(1, 2);

        assert_eq!(r1, r2);

        let mut set = HashSet::new();
        set.insert(r1.cached_hash);
        assert!(set.contains(&r2.cached_hash));
    }
}
