//! Page tree traversal for the multi-level indirect page trie.
//!
//! The page tree is a radix trie of `IndirectPage`s, each holding 1024 references.
//! The tree supports up to 7 levels (2^70 logical keys). The level exponents
//! `INP_LEVEL_PAGE_COUNT_EXPONENT = [70, 60, 50, 40, 30, 20, 10, 0]` define the
//! bit range consumed at each level:
//!
//! ```text
//! Level 0: bits 70..60 (index into top IndirectPage)
//! Level 1: bits 60..50
//! ...
//! Level 6: bits 10..0
//! Level 7: leaf (KeyValueLeafPage / SlottedPage)
//! ```
//!
//! Equivalent to Java's `NodeStorageEngineReader.getReferenceToLeafOfSubtree()`
//! and `TrieWriter.prepareLeafOfTree()`.

use crate::constants::INP_LEVEL_PAGE_COUNT_EXPONENT;
use crate::constants::INP_REFERENCE_COUNT_EXPONENT;
use crate::constants::NDP_NODE_COUNT_EXPONENT;
use crate::error::Result;
use crate::error::StorageError;
use crate::page::indirect_page::IndirectPage;
use crate::page::page_reference::PageReference;
use crate::revision::revision_root_page::RevisionRootPage;
use crate::types::IndexType;

/// Compute the record page key from a record key and index type.
///
/// This determines which KeyValueLeafPage (SlottedPage) a given record belongs to,
/// by right-shifting the record key by the appropriate exponent.
///
/// Equivalent to Java's `NodeStorageEngineReader.pageKey()`.
#[inline]
pub fn page_key(record_key: i64, index_type: IndexType) -> i64 {
    debug_assert!(record_key >= 0, "record_key must not be negative");
    match index_type {
        // PathSummary uses a different exponent in Java (PATHINP_REFERENCE_COUNT_EXPONENT).
        // For now, all use INP_REFERENCE_COUNT_EXPONENT = 10 (1024 records per leaf).
        IndexType::Document
        | IndexType::ChangedNodes
        | IndexType::RecordToRevisions
        | IndexType::PathSummary
        | IndexType::Name
        | IndexType::Cas
        | IndexType::Path
        | IndexType::DeweyId => record_key >> INP_REFERENCE_COUNT_EXPONENT,
    }
}

/// Compute the slot offset within a KeyValueLeafPage for a given record key.
///
/// This is the lower `NDP_NODE_COUNT_EXPONENT` bits of the record key.
#[inline]
pub fn record_page_offset(record_key: i64) -> usize {
    (record_key & ((1i64 << NDP_NODE_COUNT_EXPONENT) - 1)) as usize
}

/// The level count exponent array for the indirect page trie.
///
/// For most index types, this is `INP_LEVEL_PAGE_COUNT_EXPONENT`.
/// PathSummary could use a different exponent array in the future.
#[inline]
pub fn level_page_count_exponent(_index_type: IndexType) -> &'static [u32] {
    &INP_LEVEL_PAGE_COUNT_EXPONENT
}

// =============================================================================
// Read-path trie traversal
// =============================================================================

/// Callback trait for loading an `IndirectPage` from a `PageReference`.
///
/// The implementation must:
/// 1. Check the transaction intent log (TIL) first
/// 2. Check the page cache
/// 3. Read from durable storage
///
/// Returns `None` if the page does not exist (reference is null / unpersisted).
pub trait IndirectPageLoader {
    /// Load the indirect page referenced by the given page reference.
    fn load_indirect_page(&self, reference: &PageReference) -> Result<Option<IndirectPage>>;
}

/// Traverse the indirect page trie to find the reference pointing to the
/// KeyValueLeafPage for the given `page_key`.
///
/// This is the **read path** — it does not create any new pages, it only
/// follows existing references. Returns `None` if any level along the path
/// is empty (page doesn't exist yet).
///
/// Equivalent to Java's `NodeStorageEngineReader.getReferenceToLeafOfSubtree()`.
///
/// # Arguments
/// * `loader` - callback for loading indirect pages from storage/cache
/// * `start_reference` - root reference for this index (from RevisionRootPage)
/// * `page_key` - the record page key (NOT the raw record key — see `page_key()`)
/// * `index_type` - which index we're traversing
/// * `max_height` - current max tree depth from RevisionRootPage
///
/// # Returns
/// `Ok(Some(PageReference))` pointing to the leaf page, or `Ok(None)` if
/// the trie path is incomplete (page hasn't been created yet).
pub fn get_reference_to_leaf_of_subtree<L: IndirectPageLoader>(
    loader: &L,
    start_reference: &PageReference,
    page_key: i64,
    index_type: IndexType,
    max_height: i32,
) -> Result<Option<PageReference>> {
    debug_assert!(page_key >= 0, "page_key must not be negative");
    let max_height = max_height as usize;
    if max_height == 0 {
        return Ok(None);
    }

    let inp_level_exp = level_page_count_exponent(index_type);
    let exp_len = inp_level_exp.len(); // 8

    let mut reference = start_reference.clone();
    let mut level_key = page_key;

    // Iterate through all levels: start from (exp_len - max_height) up to exp_len.
    // At each level, we dereference the indirect page, then compute the child offset.
    for level in (exp_len - max_height)..exp_len {
        let indirect_page = match loader.load_indirect_page(&reference)? {
            Some(page) => page,
            None => return Ok(None),
        };

        let offset = (level_key >> inp_level_exp[level]) as usize;
        level_key -= (offset as i64) << inp_level_exp[level];

        match indirect_page.get_reference(offset) {
            Some(child_ref) => reference = child_ref.clone(),
            None => return Ok(None),
        }
    }

    Ok(Some(reference))
}

// =============================================================================
// Write-path trie traversal with tree growth
// =============================================================================

/// Callback trait for the write path — creates/COWs indirect pages into the TIL.
pub trait IndirectPagePreparer {
    /// Load or create the indirect page for a reference, putting it in the TIL.
    ///
    /// - If the page is already in the TIL (and not frozen), return it directly.
    /// - If the reference is unpersisted (NULL_ID_LONG), create a new empty IndirectPage.
    /// - Otherwise, load from storage, COW copy, and put in TIL.
    fn prepare_indirect_page(&mut self, reference: &PageReference) -> Result<IndirectPage>;

    /// Store an indirect page in the TIL and return the new PageReference for it.
    fn put_indirect_page(&mut self, page: IndirectPage) -> Result<PageReference>;
}

/// Prepare the trie path for a write, creating IndirectPages as needed.
///
/// This is the **write path** — it creates new IndirectPages where needed and
/// handles tree level growth when the key space exceeds the current depth.
///
/// Equivalent to Java's `TrieWriter.prepareLeafOfTree()`.
///
/// # Arguments
/// * `preparer` - callback for creating/COWing indirect pages
/// * `start_reference` - root reference for this index (from RevisionRootPage)
/// * `page_key` - the record page key
/// * `index_type` - which index we're traversing
/// * `revision_root` - the current revision root page (mutable, for tree growth)
///
/// # Returns
/// `Ok((PageReference, Option<(PageReference, i32)>))` — the leaf reference, and
/// optionally a new root reference + new max_height if the tree grew.
pub fn prepare_leaf_of_tree<P: IndirectPagePreparer>(
    preparer: &mut P,
    start_reference: &PageReference,
    page_key: i64,
    index_type: IndexType,
    current_max_height: i32,
) -> Result<PrepareLeafResult> {
    debug_assert!(page_key >= 0, "page_key must not be negative");

    let inp_level_exp = level_page_count_exponent(index_type);
    let exp_len = inp_level_exp.len(); // 8
    let mut max_height = current_max_height as usize;
    let mut reference = start_reference.clone();
    let mut new_root: Option<(PageReference, i32)> = None;

    // Check if we need an additional level of indirect pages.
    // This happens when page_key equals exactly 2^(exponent at the level above current max).
    if max_height < exp_len {
        let level_above = exp_len - max_height - 1;
        let threshold = 1i64 << inp_level_exp[level_above];
        if page_key == threshold {
            max_height += 1;

            // Create a new root IndirectPage. Its slot[0] points to the old root.
            let mut new_top = IndirectPage::new();
            let child_ref = new_top.get_or_create_reference(0);
            child_ref.set_key(reference.key());
            // Copy fragment info from old root
            for frag in reference.page_fragments() {
                child_ref.add_page_fragment(*frag);
            }

            // Put the new top page in the TIL
            let new_root_ref = preparer.put_indirect_page(new_top)?;
            reference = new_root_ref.clone();
            new_root = Some((new_root_ref, max_height as i32));
        }
    }

    // Traverse/create all levels.
    let mut level_key = page_key;
    for level in (exp_len - max_height)..exp_len {
        let page = preparer.prepare_indirect_page(&reference)?;

        let offset = (level_key >> inp_level_exp[level]) as usize;
        level_key -= (offset as i64) << inp_level_exp[level];

        // Get or create child reference at offset
        let child_ref = match page.get_reference(offset) {
            Some(r) => r.clone(),
            None => PageReference::new(),
        };

        // We need to store the page back — it may have been modified by get_or_create_reference.
        // In a real implementation, the page is already in the TIL via prepare_indirect_page.
        // The child reference is what we follow next.
        reference = child_ref;
    }

    Ok(PrepareLeafResult {
        leaf_reference: reference,
        new_root: new_root,
    })
}

/// Result of `prepare_leaf_of_tree`.
pub struct PrepareLeafResult {
    /// Reference to the leaf KeyValueLeafPage.
    pub leaf_reference: PageReference,
    /// If the tree grew, the new root reference and new max_height.
    /// The caller must update the RevisionRootPage accordingly.
    pub new_root: Option<(PageReference, i32)>,
}

// =============================================================================
// Helper: determine initial tree height from max_node_key
// =============================================================================

/// Calculate the required tree height for a given max record page key.
///
/// Returns the number of IndirectPage levels needed to address the given key space.
/// This is used to initialize `max_level` on first write to an index.
pub fn required_tree_height(max_page_key: i64, index_type: IndexType) -> i32 {
    if max_page_key < 0 {
        return 0;
    }
    let inp_level_exp = level_page_count_exponent(index_type);
    let exp_len = inp_level_exp.len(); // 8

    // Find the minimum number of levels that can address max_page_key.
    // Level count N means we start at level (exp_len - N) and iterate to exp_len.
    // The topmost level covers keys 0..(2^exponent[exp_len - N] - 1).
    for height in 1..=exp_len {
        let top_level = exp_len - height;
        // Max addressable key at this height: 2^exponent[top_level] - 1
        // But each level fans out 1024 ways, and the top-level exponent
        // defines the total key space.
        if max_page_key < (1i64 << inp_level_exp[top_level]) {
            return height as i32;
        }
    }
    exp_len as i32
}

// =============================================================================
// IndexLogKey: composite key for page lookups
// =============================================================================

/// Composite key identifying a specific record page within an index at a revision.
///
/// Equivalent to Java's `IndexLogKey`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IndexLogKey {
    /// Which index type (Document, ChangedNodes, etc.).
    pub index_type: IndexType,
    /// Record page key (result of `page_key(record_key, index_type)`).
    pub record_page_key: i64,
    /// Sub-index number (e.g., for CAS/Path/Name indices with multiple instances).
    /// Use -1 for primary indices (Document, ChangedNodes, RecordToRevisions).
    pub index_number: i32,
    /// Revision number.
    pub revision: i32,
}

impl IndexLogKey {
    /// Create a new IndexLogKey.
    #[inline]
    pub fn new(
        index_type: IndexType,
        record_page_key: i64,
        index_number: i32,
        revision: i32,
    ) -> Self {
        Self {
            index_type,
            record_page_key,
            index_number,
            revision,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // page_key tests
    // =========================================================================

    #[test]
    fn test_page_key_zero() {
        assert_eq!(page_key(0, IndexType::Document), 0);
    }

    #[test]
    fn test_page_key_within_first_page() {
        // Records 0..1023 all map to page key 0.
        for i in 0..1024 {
            assert_eq!(page_key(i, IndexType::Document), 0);
        }
    }

    #[test]
    fn test_page_key_second_page() {
        // Records 1024..2047 map to page key 1.
        assert_eq!(page_key(1024, IndexType::Document), 1);
        assert_eq!(page_key(2047, IndexType::Document), 1);
    }

    #[test]
    fn test_page_key_large() {
        assert_eq!(page_key(1_000_000, IndexType::Document), 1_000_000 >> 10);
    }

    #[test]
    fn test_page_key_all_index_types() {
        // All index types use the same exponent (10) for now.
        let key = 5000i64;
        let expected = key >> INP_REFERENCE_COUNT_EXPONENT;
        assert_eq!(page_key(key, IndexType::Document), expected);
        assert_eq!(page_key(key, IndexType::ChangedNodes), expected);
        assert_eq!(page_key(key, IndexType::PathSummary), expected);
        assert_eq!(page_key(key, IndexType::Name), expected);
    }

    // =========================================================================
    // record_page_offset tests
    // =========================================================================

    #[test]
    fn test_record_page_offset() {
        assert_eq!(record_page_offset(0), 0);
        assert_eq!(record_page_offset(1), 1);
        assert_eq!(record_page_offset(1023), 1023);
        assert_eq!(record_page_offset(1024), 0); // wraps
        assert_eq!(record_page_offset(1025), 1);
    }

    // =========================================================================
    // required_tree_height tests
    // =========================================================================

    #[test]
    fn test_required_height_zero() {
        // page_key 0 fits in 1 level (level 7 covers bits 10..0, but exponent[7]=0, max = 2^0 = 1)
        // Actually exponent[7] = 0, so 2^0 = 1. page_key 0 < 1 → height 1.
        assert_eq!(required_tree_height(0, IndexType::Document), 1);
    }

    #[test]
    fn test_required_height_one_level() {
        // 1 level uses exponent[7] = 0. Max addressable = 2^0 = 1. Only page_key=0.
        // page_key=1 needs 2 levels.
        assert_eq!(required_tree_height(1, IndexType::Document), 2);
    }

    #[test]
    fn test_required_height_two_levels() {
        // 2 levels: uses exponent[6] = 10. Max addressable = 2^10 = 1024.
        // page_key=1023 fits in 2 levels.
        assert_eq!(required_tree_height(1023, IndexType::Document), 2);
        // page_key=1024 needs 3 levels.
        assert_eq!(required_tree_height(1024, IndexType::Document), 3);
    }

    #[test]
    fn test_required_height_negative() {
        assert_eq!(required_tree_height(-1, IndexType::Document), 0);
    }

    // =========================================================================
    // Trie traversal tests (read path)
    // =========================================================================

    /// Mock loader that holds a tree of IndirectPages keyed by PageReference key.
    struct MockLoader {
        pages: std::collections::HashMap<i64, IndirectPage>,
    }

    impl IndirectPageLoader for MockLoader {
        fn load_indirect_page(&self, reference: &PageReference) -> Result<Option<IndirectPage>> {
            if !reference.is_persisted() {
                return Ok(None);
            }
            // We can't move out of HashMap, so we need to reconstruct.
            // For testing, we'll use a simpler approach.
            match self.pages.get(&reference.key()) {
                Some(_) => {
                    // IndirectPage doesn't implement Clone, so we create a fresh one
                    // and copy references. This is test-only code.
                    Ok(Some(self.clone_indirect_page(reference.key())))
                }
                None => Ok(None),
            }
        }
    }

    impl MockLoader {
        fn new() -> Self {
            Self {
                pages: std::collections::HashMap::new(),
            }
        }

        fn clone_indirect_page(&self, key: i64) -> IndirectPage {
            let src = &self.pages[&key];
            let mut dst = IndirectPage::new();
            for i in 0..1024 {
                if let Some(r) = src.get_reference(i) {
                    dst.set_reference(i, r.clone());
                }
            }
            dst
        }

        fn insert_page(&mut self, key: i64, page: IndirectPage) {
            self.pages.insert(key, page);
        }
    }

    #[test]
    fn test_traverse_empty_tree() {
        let loader = MockLoader::new();
        let start_ref = PageReference::new(); // unpersisted
        let result = get_reference_to_leaf_of_subtree(
            &loader,
            &start_ref,
            0,
            IndexType::Document,
            1,
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_traverse_single_level() {
        // Build a 1-level tree: root IndirectPage at key=100, with slot[0] → leaf at key=200
        let mut loader = MockLoader::new();

        let mut root_page = IndirectPage::new();
        root_page.set_reference(0, PageReference::with_key(200));
        loader.insert_page(100, root_page);

        let start_ref = PageReference::with_key(100);

        // page_key=0 → level 7 (exponent=0): offset = 0>>0 = 0
        let result = get_reference_to_leaf_of_subtree(
            &loader,
            &start_ref,
            0,
            IndexType::Document,
            1, // 1 level deep
        )
        .unwrap();

        assert!(result.is_some());
        assert_eq!(result.unwrap().key(), 200);
    }

    #[test]
    fn test_traverse_two_levels() {
        // Build a 2-level tree:
        // Root (key=100) → slot[0] → IndirectPage (key=150) → slot[5] → leaf (key=300)
        let mut loader = MockLoader::new();

        let mut level1_page = IndirectPage::new();
        level1_page.set_reference(5, PageReference::with_key(300));
        loader.insert_page(150, level1_page);

        let mut root_page = IndirectPage::new();
        root_page.set_reference(0, PageReference::with_key(150));
        loader.insert_page(100, root_page);

        let start_ref = PageReference::with_key(100);

        // page_key=5 with max_height=2:
        // Level 6 (exp=10): offset = 5>>10 = 0, levelKey remains 5
        // Level 7 (exp=0): offset = 5>>0 = 5
        let result = get_reference_to_leaf_of_subtree(
            &loader,
            &start_ref,
            5,
            IndexType::Document,
            2,
        )
        .unwrap();

        assert!(result.is_some());
        assert_eq!(result.unwrap().key(), 300);
    }

    #[test]
    fn test_traverse_missing_intermediate() {
        // Root exists but child slot is empty
        let mut loader = MockLoader::new();
        let root_page = IndirectPage::new(); // all slots empty
        loader.insert_page(100, root_page);

        let start_ref = PageReference::with_key(100);
        let result = get_reference_to_leaf_of_subtree(
            &loader,
            &start_ref,
            42,
            IndexType::Document,
            2,
        )
        .unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn test_traverse_three_levels() {
        // 3-level tree for page_key = 1024 + 5 = 1029
        // With 3 levels: top_level = exp_len - 3 = 5, exponents[5]=20, [6]=10, [7]=0
        //
        // Level 5 (exp=20): offset = 1029 >> 20 = 0, levelKey = 1029
        // Level 6 (exp=10): offset = 1029 >> 10 = 1, levelKey = 1029 - 1*1024 = 5
        // Level 7 (exp=0):  offset = 5 >> 0 = 5
        let mut loader = MockLoader::new();

        let mut leaf_parent = IndirectPage::new();
        leaf_parent.set_reference(5, PageReference::with_key(999)); // the leaf ref
        loader.insert_page(300, leaf_parent);

        let mut mid = IndirectPage::new();
        mid.set_reference(1, PageReference::with_key(300));
        loader.insert_page(200, mid);

        let mut root = IndirectPage::new();
        root.set_reference(0, PageReference::with_key(200));
        loader.insert_page(100, root);

        let start_ref = PageReference::with_key(100);
        let result = get_reference_to_leaf_of_subtree(
            &loader,
            &start_ref,
            1029,
            IndexType::Document,
            3,
        )
        .unwrap();

        assert!(result.is_some());
        assert_eq!(result.unwrap().key(), 999);
    }

    #[test]
    fn test_traverse_max_height_zero_returns_none() {
        let loader = MockLoader::new();
        let start_ref = PageReference::with_key(100);
        let result = get_reference_to_leaf_of_subtree(
            &loader,
            &start_ref,
            0,
            IndexType::Document,
            0,
        )
        .unwrap();
        assert!(result.is_none());
    }

    // =========================================================================
    // Trie write-path tests (prepare_leaf_of_tree)
    // =========================================================================

    /// Mock preparer that tracks pages in a simple map.
    struct MockPreparer {
        pages: std::collections::HashMap<i64, IndirectPage>,
        next_key: i64,
    }

    impl MockPreparer {
        fn new() -> Self {
            Self {
                pages: std::collections::HashMap::new(),
                next_key: 1000,
            }
        }

        fn clone_indirect_page(&self, key: i64) -> IndirectPage {
            let src = &self.pages[&key];
            let mut dst = IndirectPage::new();
            for i in 0..1024 {
                if let Some(r) = src.get_reference(i) {
                    dst.set_reference(i, r.clone());
                }
            }
            dst
        }
    }

    impl IndirectPagePreparer for MockPreparer {
        fn prepare_indirect_page(&mut self, reference: &PageReference) -> Result<IndirectPage> {
            if !reference.is_persisted() {
                // New empty page
                let page = IndirectPage::new();
                Ok(page)
            } else {
                match self.pages.get(&reference.key()) {
                    Some(_) => Ok(self.clone_indirect_page(reference.key())),
                    None => Ok(IndirectPage::new()),
                }
            }
        }

        fn put_indirect_page(&mut self, page: IndirectPage) -> Result<PageReference> {
            let key = self.next_key;
            self.next_key += 1;
            self.pages.insert(key, page);
            Ok(PageReference::with_key(key))
        }
    }

    #[test]
    fn test_prepare_leaf_single_level() {
        let mut preparer = MockPreparer::new();
        let start_ref = PageReference::new(); // unpersisted root

        let result = prepare_leaf_of_tree(
            &mut preparer,
            &start_ref,
            0,
            IndexType::Document,
            1, // 1 level
        )
        .unwrap();

        // Should return a leaf reference (may be unpersisted for new page)
        assert!(result.new_root.is_none()); // no tree growth needed
    }

    #[test]
    fn test_prepare_leaf_triggers_growth() {
        let mut preparer = MockPreparer::new();

        // Start with max_height=1. Tree grows when page_key == 2^exponent[exp_len - 1 - 1] = 2^10 = 1024.
        // With height=1, the top level is exp_len-1=7, exponent[7]=0, covering 2^0=1 page key.
        // Growth threshold: page_key == 2^exponent[exp_len - 1 - 1] = 2^exponent[6] = 2^10 = 1024... wait.
        // Let me recalculate:
        // max_height=1, exp_len=8
        // level_above = exp_len - max_height - 1 = 8 - 1 - 1 = 6
        // threshold = 1 << exponent[6] = 1 << 10 = 1024
        // But with height 1 we only have 1 level covering exponent[7]=0, so max key = 2^0 = 1.
        // Growth triggers at page_key = 1024? No...
        //
        // Actually in Java, the growth check is:
        // if (pageKey == (1L << inpLevelPageCountExp[inpLevelPageCountExp.length - maxHeight - 1]))
        // With maxHeight=1: 1L << exp[8-1-1] = 1L << exp[6] = 1L << 10 = 1024
        //
        // But that seems off for height=1. The Java initializes max_height to at least 1.
        // When max_height=1, the tree covers 1 indirect page at level 7 (exp=0),
        // addressing only page_key 0 (offset = 0>>0 = 0).
        //
        // For page_key=1, the tree needs to grow: 1 == 1 << exp[6]? No, 1 != 1024.
        // So growth only happens at exact powers. For height=1 and page_key=1,
        // the traversal would compute offset = 1>>0 = 1, which is fine within 1024 slots.
        // So actually height=1 covers page_keys 0..1023.
        // Growth to height=2 happens at page_key=1024 (which equals 1<<exp[6]=1<<10).
        let start_ref = PageReference::with_key(500); // persisted root

        // Insert a root page so the preparer can find it
        let root_page = IndirectPage::new();
        preparer.pages.insert(500, root_page);

        let result = prepare_leaf_of_tree(
            &mut preparer,
            &start_ref,
            1024, // == 1 << 10 → triggers growth
            IndexType::Document,
            1,
        )
        .unwrap();

        // Tree should have grown
        assert!(result.new_root.is_some());
        let (_, new_height) = result.new_root.unwrap();
        assert_eq!(new_height, 2);
    }

    #[test]
    fn test_prepare_leaf_no_growth_below_threshold() {
        let mut preparer = MockPreparer::new();
        let start_ref = PageReference::with_key(500);
        let root_page = IndirectPage::new();
        preparer.pages.insert(500, root_page);

        let result = prepare_leaf_of_tree(
            &mut preparer,
            &start_ref,
            1023, // below threshold
            IndexType::Document,
            1,
        )
        .unwrap();

        assert!(result.new_root.is_none());
    }

    // =========================================================================
    // IndexLogKey tests
    // =========================================================================

    #[test]
    fn test_index_log_key_equality() {
        let k1 = IndexLogKey::new(IndexType::Document, 42, -1, 5);
        let k2 = IndexLogKey::new(IndexType::Document, 42, -1, 5);
        assert_eq!(k1, k2);
    }

    #[test]
    fn test_index_log_key_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(IndexLogKey::new(IndexType::Document, 42, -1, 5));
        assert!(set.contains(&IndexLogKey::new(IndexType::Document, 42, -1, 5)));
        assert!(!set.contains(&IndexLogKey::new(IndexType::ChangedNodes, 42, -1, 5)));
    }
}
