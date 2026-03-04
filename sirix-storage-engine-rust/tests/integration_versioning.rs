//! Integration tests for all versioning strategies.
//!
//! These tests simulate realistic multi-revision workloads:
//! - Multiple revisions writing pages with versioning-aware fragment management
//! - Page combination from fragments for reads
//! - COW modification with preservation and resolution
//! - I/O roundtrips: serialize → write → read → deserialize → combine
//! - Cross-strategy consistency checks
//! - Edge cases: empty pages, single-slot pages, max-slot pages, deletions

use sirix_storage_engine::compression::ByteHandlerPipeline;
use sirix_storage_engine::io::FileChannelWriter;
use sirix_storage_engine::io::reader::StorageReader;
use sirix_storage_engine::io::writer::StorageWriter;
use sirix_storage_engine::page::page_reference::PageReference;
use sirix_storage_engine::page::serialization::serialize_slotted_page;
use sirix_storage_engine::page::serialization::DeserializedPage;
use sirix_storage_engine::page::slotted_page::SlottedPage;
use sirix_storage_engine::types::IndexType;
use sirix_storage_engine::types::PageFragmentKey;
use sirix_storage_engine::types::VersioningType;
use sirix_storage_engine::versioning::combine_record_pages;
use sirix_storage_engine::versioning::combine_record_pages_for_modification;
use sirix_storage_engine::versioning::get_revision_roots;
use sirix_storage_engine::versioning::should_store_full_snapshot;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_page(key: i64, revision: i32, records: &[(usize, u8, &[u8])]) -> SlottedPage {
    let mut page = SlottedPage::new(key, revision, IndexType::Document);
    for &(slot, kind, data) in records {
        page.insert_record(slot, kind, data).unwrap();
    }
    page
}

fn serialize(page: &SlottedPage) -> Vec<u8> {
    let mut buf = Vec::new();
    serialize_slotted_page(page, &mut buf).unwrap();
    buf
}

fn unwrap_kvl(page: DeserializedPage) -> SlottedPage {
    match page {
        DeserializedPage::KeyValueLeaf(p) => p,
        other => panic!("expected KeyValueLeaf, got {:?}", std::mem::discriminant(&other)),
    }
}

// ============================================================================
// Full versioning — integration tests
// ============================================================================

#[test]
fn test_full_single_revision_roundtrip() {
    let page = make_page(0, 1, &[
        (0, 1, b"alpha"),
        (10, 2, b"beta"),
        (100, 3, b"gamma"),
        (500, 4, b"delta"),
    ]);
    let combined = combine_record_pages(VersioningType::Full, &[page], 1).unwrap();
    assert_eq!(combined.populated_count(), 4);
    assert_eq!(combined.read_record(0).unwrap(), (1, &b"alpha"[..]));
    assert_eq!(combined.read_record(10).unwrap(), (2, &b"beta"[..]));
    assert_eq!(combined.read_record(100).unwrap(), (3, &b"gamma"[..]));
    assert_eq!(combined.read_record(500).unwrap(), (4, &b"delta"[..]));
}

#[test]
fn test_full_modification_produces_identical_complete_and_modified() {
    let page = make_page(0, 1, &[
        (0, 1, b"slot0"), (1, 2, b"slot1"), (2, 3, b"slot2"),
    ]);
    let mut ref_ = PageReference::new();
    let container = combine_record_pages_for_modification(
        VersioningType::Full, &[page], 1, &mut ref_, 2, 0, 0,
    ).unwrap();

    // Both views must have the same data
    for slot in [0, 1, 2] {
        let (ck, cd) = container.complete().read_record(slot).unwrap();
        let (mk, md) = container.modified().read_record(slot).unwrap();
        assert_eq!(ck, mk);
        assert_eq!(cd, md);
    }
    assert_eq!(container.complete().populated_count(), 3);
    assert_eq!(container.modified().populated_count(), 3);
}

#[test]
fn test_full_cow_isolation() {
    // After modification, writing to the modified page must not affect the complete page
    let page = make_page(0, 1, &[(0, 1, b"original")]);
    let mut ref_ = PageReference::new();
    let mut container = combine_record_pages_for_modification(
        VersioningType::Full, &[page], 1, &mut ref_, 2, 0, 0,
    ).unwrap();

    // Mutate the modified page
    container.modified_mut().insert_record(5, 9, b"new-record").unwrap();

    // Complete page should still only have slot 0
    assert_eq!(container.complete().populated_count(), 1);
    assert!(!container.complete().is_slot_occupied(5));

    // Modified page should have both
    assert_eq!(container.modified().populated_count(), 2);
    assert_eq!(container.modified().read_record(5).unwrap(), (9, &b"new-record"[..]));
}

#[test]
fn test_full_always_stores_full_snapshot() {
    for rev in 1..=20 {
        assert!(should_store_full_snapshot(VersioningType::Full, rev, 5, 3));
    }
}

#[test]
fn test_full_revision_roots_always_single() {
    for rev in 0..=10 {
        let roots = get_revision_roots(VersioningType::Full, rev, 5);
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0], rev);
    }
}

#[test]
fn test_full_multi_revision_each_independent() {
    // Each revision is a complete page — combining rev5 should not need rev4
    let rev4 = make_page(0, 4, &[(0, 1, b"rev4-data")]);
    let rev5 = make_page(0, 5, &[(0, 1, b"rev5-data"), (1, 2, b"rev5-slot1")]);

    let combined4 = combine_record_pages(VersioningType::Full, &[rev4], 1).unwrap();
    let combined5 = combine_record_pages(VersioningType::Full, &[rev5], 1).unwrap();

    assert_eq!(combined4.populated_count(), 1);
    assert_eq!(combined4.read_record(0).unwrap(), (1, &b"rev4-data"[..]));

    assert_eq!(combined5.populated_count(), 2);
    assert_eq!(combined5.read_record(0).unwrap(), (1, &b"rev5-data"[..]));
    assert_eq!(combined5.read_record(1).unwrap(), (2, &b"rev5-slot1"[..]));
}

// ============================================================================
// Differential versioning — integration tests
// ============================================================================

#[test]
fn test_differential_base_only() {
    // At a full dump revision, only the base page exists
    let base = make_page(0, 0, &[
        (0, 1, b"base-0"), (5, 2, b"base-5"), (10, 3, b"base-10"),
    ]);
    let combined = combine_record_pages(VersioningType::Differential, &[base], 3).unwrap();
    assert_eq!(combined.populated_count(), 3);
    assert_eq!(combined.read_record(0).unwrap(), (1, &b"base-0"[..]));
    assert_eq!(combined.read_record(5).unwrap(), (2, &b"base-5"[..]));
    assert_eq!(combined.read_record(10).unwrap(), (3, &b"base-10"[..]));
}

#[test]
fn test_differential_delta_overwrites_base() {
    let base = make_page(0, 0, &[
        (0, 1, b"base-val"), (5, 2, b"base-5"), (10, 3, b"base-10"),
    ]);
    let delta = make_page(0, 1, &[
        (0, 1, b"delta-val"),   // overwrite slot 0
        (20, 4, b"delta-new"),  // new slot
    ]);
    let combined = combine_record_pages(VersioningType::Differential, &[delta, base], 3).unwrap();

    assert_eq!(combined.populated_count(), 4);
    assert_eq!(combined.read_record(0).unwrap(), (1, &b"delta-val"[..])); // overwritten
    assert_eq!(combined.read_record(5).unwrap(), (2, &b"base-5"[..]));    // from base
    assert_eq!(combined.read_record(10).unwrap(), (3, &b"base-10"[..]));  // from base
    assert_eq!(combined.read_record(20).unwrap(), (4, &b"delta-new"[..])); // new
}

#[test]
fn test_differential_revision_roots_periodic() {
    // revs_to_restore=3: full dumps at 0, 3, 6, 9...
    let roots0 = get_revision_roots(VersioningType::Differential, 0, 3);
    assert_eq!(roots0, vec![0]); // full dump

    let roots1 = get_revision_roots(VersioningType::Differential, 1, 3);
    assert_eq!(roots1, vec![1, 0]); // delta + base

    let roots2 = get_revision_roots(VersioningType::Differential, 2, 3);
    assert_eq!(roots2, vec![2, 0]); // delta + base

    let roots3 = get_revision_roots(VersioningType::Differential, 3, 3);
    assert_eq!(roots3, vec![3]); // full dump again

    let roots4 = get_revision_roots(VersioningType::Differential, 4, 3);
    assert_eq!(roots4, vec![4, 3]); // delta + base=3
}

#[test]
fn test_differential_snapshot_decision() {
    assert!(should_store_full_snapshot(VersioningType::Differential, 3, 3, 1));
    assert!(should_store_full_snapshot(VersioningType::Differential, 6, 3, 1));
    assert!(!should_store_full_snapshot(VersioningType::Differential, 4, 3, 1));
    assert!(!should_store_full_snapshot(VersioningType::Differential, 5, 3, 1));
}

#[test]
fn test_differential_modification_sets_fragment_key() {
    let base = make_page(0, 0, &[(0, 1, b"base")]);
    let mut ref_ = PageReference::with_key(42);
    let _container = combine_record_pages_for_modification(
        VersioningType::Differential, &[base], 3,
        &mut ref_, 1, 0, 0,
    ).unwrap();

    // Should have exactly 1 fragment key referencing revision 0
    assert_eq!(ref_.page_fragments().len(), 1);
    assert_eq!(ref_.page_fragments()[0].revision, 0);
}

#[test]
fn test_differential_full_cycle_3_revisions() {
    // Rev 0: full dump (slots 0, 1, 2)
    let rev0 = make_page(0, 0, &[
        (0, 1, b"r0-s0"), (1, 1, b"r0-s1"), (2, 1, b"r0-s2"),
    ]);
    // Rev 1: delta (update slot 0, add slot 3)
    let rev1 = make_page(0, 1, &[(0, 1, b"r1-s0"), (3, 1, b"r1-s3")]);
    // Rev 2: delta (update slot 1)
    let rev2 = make_page(0, 2, &[(1, 1, b"r2-s1")]);

    // Reconstruct at rev1: [rev1, rev0]
    let combined1 = combine_record_pages(VersioningType::Differential, &[rev1, clone_page(&rev0)], 3).unwrap();
    assert_eq!(combined1.populated_count(), 4);
    assert_eq!(combined1.read_record(0).unwrap(), (1, &b"r1-s0"[..]));
    assert_eq!(combined1.read_record(1).unwrap(), (1, &b"r0-s1"[..]));
    assert_eq!(combined1.read_record(2).unwrap(), (1, &b"r0-s2"[..]));
    assert_eq!(combined1.read_record(3).unwrap(), (1, &b"r1-s3"[..]));

    // Reconstruct at rev2: [rev2, rev0] (differential always uses base, not rev1)
    let combined2 = combine_record_pages(VersioningType::Differential, &[rev2, rev0], 3).unwrap();
    assert_eq!(combined2.populated_count(), 3);
    assert_eq!(combined2.read_record(0).unwrap(), (1, &b"r0-s0"[..])); // from base
    assert_eq!(combined2.read_record(1).unwrap(), (1, &b"r2-s1"[..])); // from delta
    assert_eq!(combined2.read_record(2).unwrap(), (1, &b"r0-s2"[..])); // from base
}

#[test]
fn test_differential_cow_with_full_dump_marks_all_for_preservation() {
    let base = make_page(0, 0, &[
        (0, 1, b"s0"), (1, 1, b"s1"), (2, 1, b"s2"),
    ]);
    let delta = make_page(0, 1, &[(0, 1, b"updated")]);
    let mut ref_ = PageReference::new();
    // current_revision=3 with revs_to_restore=3 => 3 % 3 == 0 => full dump
    let container = combine_record_pages_for_modification(
        VersioningType::Differential, &[delta, base], 3,
        &mut ref_, 3, 0, 0,
    ).unwrap();

    // Complete should be fully merged
    assert_eq!(container.complete().populated_count(), 3);

    // Modified should have preservation marks for all slots (full dump revision)
    assert!(container.modified().is_marked_for_preservation(0));
    assert!(container.modified().is_marked_for_preservation(1));
    assert!(container.modified().is_marked_for_preservation(2));
}

// ============================================================================
// Incremental versioning — integration tests
// ============================================================================

#[test]
fn test_incremental_single_fragment() {
    let page = make_page(0, 1, &[(0, 1, b"solo"), (5, 2, b"five")]);
    let combined = combine_record_pages(VersioningType::Incremental, &[page], 3).unwrap();
    assert_eq!(combined.populated_count(), 2);
    assert_eq!(combined.read_record(0).unwrap(), (1, &b"solo"[..]));
    assert_eq!(combined.read_record(5).unwrap(), (2, &b"five"[..]));
}

#[test]
fn test_incremental_chain_of_3_newest_wins() {
    let rev3 = make_page(0, 3, &[(0, 1, b"newest")]);
    let rev2 = make_page(0, 2, &[(0, 1, b"middle"), (5, 2, b"r2-s5")]);
    let rev1 = make_page(0, 1, &[(0, 1, b"oldest"), (5, 2, b"r1-s5"), (10, 3, b"r1-s10")]);

    let combined = combine_record_pages(VersioningType::Incremental, &[rev3, rev2, rev1], 3).unwrap();
    assert_eq!(combined.populated_count(), 3);
    assert_eq!(combined.read_record(0).unwrap(), (1, &b"newest"[..]));
    assert_eq!(combined.read_record(5).unwrap(), (2, &b"r2-s5"[..]));
    assert_eq!(combined.read_record(10).unwrap(), (3, &b"r1-s10"[..]));
}

#[test]
fn test_incremental_revision_roots() {
    // prevRev=5, revs=3 => [5, 4, 3]
    let roots = get_revision_roots(VersioningType::Incremental, 5, 3);
    assert_eq!(roots, vec![5, 4, 3]);

    // prevRev=1, revs=5 => [1, 0] (can't go below 0)
    let roots2 = get_revision_roots(VersioningType::Incremental, 1, 5);
    assert_eq!(roots2, vec![1, 0]);

    // prevRev=0, revs=3 => [0]
    let roots3 = get_revision_roots(VersioningType::Incremental, 0, 3);
    assert_eq!(roots3, vec![0]);
}

#[test]
fn test_incremental_snapshot_decision() {
    // revs_to_restore=4: full dump when fragment_count >= 3
    // fragment_count=0 always triggers full (treated as first page write)
    assert!(should_store_full_snapshot(VersioningType::Incremental, 5, 4, 0));
    assert!(!should_store_full_snapshot(VersioningType::Incremental, 5, 4, 1));
    assert!(!should_store_full_snapshot(VersioningType::Incremental, 5, 4, 2));
    assert!(should_store_full_snapshot(VersioningType::Incremental, 5, 4, 3));
    assert!(should_store_full_snapshot(VersioningType::Incremental, 5, 4, 10));
    // First revision is always full regardless
    assert!(should_store_full_snapshot(VersioningType::Incremental, 1, 10, 0));
}

#[test]
fn test_incremental_fragment_chain_management() {
    let rev3 = make_page(0, 3, &[(0, 1, b"v3")]);
    let rev2 = make_page(0, 2, &[(5, 2, b"v2")]);
    let rev1 = make_page(0, 1, &[(10, 3, b"v1")]);

    let mut ref_ = PageReference::new();
    ref_.add_page_fragment(PageFragmentKey { revision: 2, offset: 200 });
    ref_.add_page_fragment(PageFragmentKey { revision: 1, offset: 100 });

    let _container = combine_record_pages_for_modification(
        VersioningType::Incremental, &[rev3, rev2, rev1], 3,
        &mut ref_, 4, 0, 0,
    ).unwrap();

    // Fragment chain should be capped at revs_to_restore - 1 = 2
    assert!(ref_.page_fragments().len() <= 2);
    // First fragment should be from rev3
    assert_eq!(ref_.page_fragments()[0].revision, 3);
}

#[test]
fn test_incremental_full_dump_triggers_preservation() {
    // When pages.len() == revs_to_restore, it's a full dump
    let rev3 = make_page(0, 3, &[(0, 1, b"v3")]);
    let rev2 = make_page(0, 2, &[(5, 2, b"v2")]);
    let rev1 = make_page(0, 1, &[(10, 3, b"v1")]);
    let mut ref_ = PageReference::new();

    let container = combine_record_pages_for_modification(
        VersioningType::Incremental, &[rev3, rev2, rev1], 3,
        &mut ref_, 4, 0, 0,
    ).unwrap();

    // Complete should have all 3 slots
    assert_eq!(container.complete().populated_count(), 3);
    // Modified should have preservation marks (full dump)
    assert!(container.modified().is_marked_for_preservation(0));
    assert!(container.modified().is_marked_for_preservation(5));
    assert!(container.modified().is_marked_for_preservation(10));
}

#[test]
fn test_incremental_non_full_dump_no_preservation() {
    // When pages.len() < revs_to_restore, not a full dump
    let rev2 = make_page(0, 2, &[(0, 1, b"v2")]);
    let rev1 = make_page(0, 1, &[(5, 2, b"v1")]);
    let mut ref_ = PageReference::new();

    let container = combine_record_pages_for_modification(
        VersioningType::Incremental, &[rev2, rev1], 3,  // 2 < 3
        &mut ref_, 3, 0, 0,
    ).unwrap();

    assert_eq!(container.complete().populated_count(), 2);
    // No preservation marks since it's not a full dump
    assert!(!container.modified().is_marked_for_preservation(0));
    assert!(!container.modified().is_marked_for_preservation(5));
}

#[test]
fn test_incremental_5_revision_lifecycle() {
    // revs_to_restore=3, simulate 5 revisions
    // Rev 1: full dump
    let rev1 = make_page(0, 1, &[
        (0, 1, b"r1-0"), (1, 1, b"r1-1"), (2, 1, b"r1-2"),
    ]);
    // Rev 2: update slot 0
    let rev2 = make_page(0, 2, &[(0, 1, b"r2-0")]);
    // Rev 3: update slot 1 (chain = [rev3, rev2, rev1] = full dump length)
    let rev3 = make_page(0, 3, &[(1, 1, b"r3-1")]);

    // Reconstruct at rev3
    let c3 = combine_record_pages(VersioningType::Incremental, &[rev3, rev2, rev1], 3).unwrap();
    assert_eq!(c3.read_record(0).unwrap(), (1, &b"r2-0"[..]));
    assert_eq!(c3.read_record(1).unwrap(), (1, &b"r3-1"[..]));
    assert_eq!(c3.read_record(2).unwrap(), (1, &b"r1-2"[..]));

    // Rev 4: new full dump (chain reset), update slot 2
    // Simulate: rev4 is a full dump created by resolving preservation
    let rev4 = make_page(0, 4, &[
        (0, 1, b"r2-0"), (1, 1, b"r3-1"), (2, 1, b"r4-2"),
    ]);
    // Rev 5: update slot 0 again
    let rev5 = make_page(0, 5, &[(0, 1, b"r5-0")]);

    let c5 = combine_record_pages(VersioningType::Incremental, &[rev5, rev4], 3).unwrap();
    assert_eq!(c5.read_record(0).unwrap(), (1, &b"r5-0"[..]));
    assert_eq!(c5.read_record(1).unwrap(), (1, &b"r3-1"[..]));
    assert_eq!(c5.read_record(2).unwrap(), (1, &b"r4-2"[..]));
}

// ============================================================================
// SlidingSnapshot versioning — integration tests
// ============================================================================

#[test]
fn test_sliding_snapshot_read_same_as_incremental() {
    let rev3 = make_page(0, 3, &[(0, 1, b"v3")]);
    let rev2 = make_page(0, 2, &[(5, 2, b"v2")]);
    let rev1 = make_page(0, 1, &[(10, 3, b"v1")]);

    let ss = VersioningType::SlidingSnapshot { window_size: 3 };
    let combined = combine_record_pages(ss, &[clone_page(&rev3), clone_page(&rev2), clone_page(&rev1)], 3).unwrap();
    let inc = combine_record_pages(VersioningType::Incremental, &[rev3, rev2, rev1], 3).unwrap();

    // Read path should produce identical results
    for slot in [0, 5, 10] {
        assert_eq!(combined.read_record(slot).unwrap(), inc.read_record(slot).unwrap());
    }
}

#[test]
fn test_sliding_snapshot_out_of_window_slots_preserved() {
    // Window size 3, 3 fragments: rev3 (in), rev2 (in), rev1 (out-of-window)
    let rev3 = make_page(0, 3, &[(0, 1, b"v3-0")]);
    let rev2 = make_page(0, 2, &[(5, 2, b"v2-5")]);
    let rev1 = make_page(0, 1, &[(10, 3, b"oow-10"), (20, 4, b"oow-20")]);
    let mut ref_ = PageReference::new();

    let container = combine_record_pages_for_modification(
        VersioningType::SlidingSnapshot { window_size: 3 }, &[rev3, rev2, rev1], 3,
        &mut ref_, 4, 0, 0,
    ).unwrap();

    // Complete has all 4 slots
    assert_eq!(container.complete().populated_count(), 4);

    // Out-of-window-only slots (10, 20) are marked for preservation
    assert!(container.modified().is_marked_for_preservation(10));
    assert!(container.modified().is_marked_for_preservation(20));

    // In-window slots (0, 5) are NOT marked
    assert!(!container.modified().is_marked_for_preservation(0));
    assert!(!container.modified().is_marked_for_preservation(5));
}

#[test]
fn test_sliding_snapshot_in_window_slot_not_preserved_even_in_oow_page() {
    // Slot exists in both in-window and out-of-window pages
    let rev3 = make_page(0, 3, &[(0, 1, b"v3-0")]);      // in-window, has slot 0
    let rev2 = make_page(0, 2, &[(5, 2, b"v2-5")]);       // in-window
    let rev1 = make_page(0, 1, &[(0, 1, b"v1-0"), (10, 3, b"oow")]); // out-of-window, also has slot 0
    let mut ref_ = PageReference::new();

    let container = combine_record_pages_for_modification(
        VersioningType::SlidingSnapshot { window_size: 3 }, &[rev3, rev2, rev1], 3,
        &mut ref_, 4, 0, 0,
    ).unwrap();

    // Slot 0 exists in-window (from rev3), so should NOT be marked for preservation
    assert!(!container.modified().is_marked_for_preservation(0));
    // Slot 10 only exists out-of-window, so SHOULD be marked
    assert!(container.modified().is_marked_for_preservation(10));

    // Complete page should have slot 0 from rev3 (newest)
    assert_eq!(container.complete().read_record(0).unwrap(), (1, &b"v3-0"[..]));
}

#[test]
fn test_sliding_snapshot_revision_roots() {
    let ss = VersioningType::SlidingSnapshot { window_size: 4 };
    let roots = get_revision_roots(ss, 10, 4);
    assert_eq!(roots, vec![10, 9, 8, 7]);

    let roots2 = get_revision_roots(ss, 2, 4);
    assert_eq!(roots2, vec![2, 1, 0]);
}

#[test]
fn test_sliding_snapshot_snapshot_decision() {
    let ss = VersioningType::SlidingSnapshot { window_size: 4 };
    assert!(should_store_full_snapshot(ss, 1, 4, 0));   // first revision
    assert!(!should_store_full_snapshot(ss, 5, 4, 1));   // chain too short
    assert!(!should_store_full_snapshot(ss, 5, 4, 2));
    assert!(should_store_full_snapshot(ss, 5, 4, 3));    // chain length threshold
}

#[test]
fn test_sliding_snapshot_no_out_of_window_page() {
    // Only 2 fragments with revs_to_restore=3 => no out-of-window page
    let rev2 = make_page(0, 2, &[(0, 1, b"v2")]);
    let rev1 = make_page(0, 1, &[(5, 2, b"v1")]);
    let mut ref_ = PageReference::new();

    let container = combine_record_pages_for_modification(
        VersioningType::SlidingSnapshot { window_size: 3 }, &[rev2, rev1], 3,
        &mut ref_, 3, 0, 0,
    ).unwrap();

    assert_eq!(container.complete().populated_count(), 2);
    // No preservation marks since there's no out-of-window page
    assert!(!container.modified().is_marked_for_preservation(0));
    assert!(!container.modified().is_marked_for_preservation(5));
}

// ============================================================================
// Preservation resolution — integration tests
// ============================================================================

#[test]
fn test_preservation_resolve_copies_from_complete() {
    let complete = make_page(0, 1, &[
        (0, 1, b"c-s0"), (5, 2, b"c-s5"), (10, 3, b"c-s10"),
    ]);
    let mut modified = SlottedPage::new(0, 2, IndexType::Document);
    modified.mark_slot_for_preservation(0);
    modified.mark_slot_for_preservation(5);
    modified.mark_slot_for_preservation(10);

    modified.resolve_preservation(&complete).unwrap();

    assert_eq!(modified.populated_count(), 3);
    assert_eq!(modified.read_record(0).unwrap(), (1, &b"c-s0"[..]));
    assert_eq!(modified.read_record(5).unwrap(), (2, &b"c-s5"[..]));
    assert_eq!(modified.read_record(10).unwrap(), (3, &b"c-s10"[..]));
}

#[test]
fn test_preservation_explicit_write_takes_precedence() {
    let complete = make_page(0, 1, &[(0, 1, b"complete-val")]);
    let mut modified = SlottedPage::new(0, 2, IndexType::Document);
    modified.insert_record(0, 1, b"modified-val").unwrap();
    modified.mark_slot_for_preservation(0);

    modified.resolve_preservation(&complete).unwrap();

    // The explicitly-written value should win
    assert_eq!(modified.read_record(0).unwrap(), (1, &b"modified-val"[..]));
}

#[test]
fn test_preservation_partial_overlap() {
    let complete = make_page(0, 1, &[
        (0, 1, b"c0"), (1, 1, b"c1"), (2, 1, b"c2"), (3, 1, b"c3"),
    ]);
    let mut modified = SlottedPage::new(0, 2, IndexType::Document);
    // Explicitly write slots 0 and 2
    modified.insert_record(0, 1, b"m0").unwrap();
    modified.insert_record(2, 1, b"m2").unwrap();
    // Mark all for preservation
    for s in 0..4 {
        modified.mark_slot_for_preservation(s);
    }

    modified.resolve_preservation(&complete).unwrap();

    assert_eq!(modified.populated_count(), 4);
    assert_eq!(modified.read_record(0).unwrap(), (1, &b"m0"[..]));  // explicit
    assert_eq!(modified.read_record(1).unwrap(), (1, &b"c1"[..]));  // from complete
    assert_eq!(modified.read_record(2).unwrap(), (1, &b"m2"[..]));  // explicit
    assert_eq!(modified.read_record(3).unwrap(), (1, &b"c3"[..]));  // from complete
}

// ============================================================================
// I/O roundtrip with versioning — write fragments, read back, combine
// ============================================================================

#[test]
fn test_incremental_io_roundtrip_3_revisions() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

    // Write 3 revision fragments to disk
    let rev1 = make_page(0, 1, &[(0, 1, b"r1-s0"), (1, 1, b"r1-s1"), (2, 1, b"r1-s2")]);
    let rev2 = make_page(0, 2, &[(0, 1, b"r2-s0")]);
    let rev3 = make_page(0, 3, &[(1, 1, b"r3-s1")]);

    let mut ref1 = PageReference::new();
    let mut ref2 = PageReference::new();
    let mut ref3 = PageReference::new();
    writer.write_page(&mut ref1, &serialize(&rev1)).unwrap();
    writer.write_page(&mut ref2, &serialize(&rev2)).unwrap();
    writer.write_page(&mut ref3, &serialize(&rev3)).unwrap();
    writer.force_all().unwrap();

    // Read fragments back from disk
    let d1 = unwrap_kvl(writer.read_page(&ref1).unwrap());
    let d2 = unwrap_kvl(writer.read_page(&ref2).unwrap());
    let d3 = unwrap_kvl(writer.read_page(&ref3).unwrap());

    // Combine: newest first
    let combined = combine_record_pages(
        VersioningType::Incremental, &[d3, d2, d1], 3,
    ).unwrap();

    assert_eq!(combined.populated_count(), 3);
    assert_eq!(combined.read_record(0).unwrap(), (1, &b"r2-s0"[..]));
    assert_eq!(combined.read_record(1).unwrap(), (1, &b"r3-s1"[..]));
    assert_eq!(combined.read_record(2).unwrap(), (1, &b"r1-s2"[..]));
}

#[test]
fn test_differential_io_roundtrip_with_lz4() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::lz4()).unwrap();

    // Base page (full dump at rev 0)
    let base = make_page(0, 0, &[
        (0, 1, b"base-alpha"),
        (10, 2, b"base-beta"),
        (50, 3, b"base-gamma"),
    ]);
    // Delta page (rev 1)
    let delta = make_page(0, 1, &[
        (0, 1, b"delta-alpha"),    // overwrite
        (100, 4, b"delta-new"),    // new slot
    ]);

    let mut ref_base = PageReference::new();
    let mut ref_delta = PageReference::new();
    writer.write_page(&mut ref_base, &serialize(&base)).unwrap();
    writer.write_page(&mut ref_delta, &serialize(&delta)).unwrap();
    writer.force_all().unwrap();

    // Read back through LZ4 decompression
    let d_base = unwrap_kvl(writer.read_page(&ref_base).unwrap());
    let d_delta = unwrap_kvl(writer.read_page(&ref_delta).unwrap());

    let combined = combine_record_pages(
        VersioningType::Differential, &[d_delta, d_base], 3,
    ).unwrap();

    assert_eq!(combined.populated_count(), 4);
    assert_eq!(combined.read_record(0).unwrap(), (1, &b"delta-alpha"[..]));
    assert_eq!(combined.read_record(10).unwrap(), (2, &b"base-beta"[..]));
    assert_eq!(combined.read_record(50).unwrap(), (3, &b"base-gamma"[..]));
    assert_eq!(combined.read_record(100).unwrap(), (4, &b"delta-new"[..]));
}

#[test]
fn test_full_io_roundtrip_cow_then_serialize() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

    // Rev 1: initial page
    let rev1 = make_page(0, 1, &[(0, 1, b"initial"), (5, 2, b"five")]);
    let mut ref_ = PageReference::new();
    let container = combine_record_pages_for_modification(
        VersioningType::Full, &[rev1], 1, &mut ref_, 2, 0, 0,
    ).unwrap();

    // Modify the page (simulate a write transaction)
    let (_, mut modified) = container.into_parts();
    modified.insert_record(10, 3, b"new-slot").unwrap();

    // Serialize and write the modified page as rev 2
    let mut ref2 = PageReference::new();
    writer.write_page(&mut ref2, &serialize(&modified)).unwrap();
    writer.force_all().unwrap();

    // Read back and verify
    let read_back = unwrap_kvl(writer.read_page(&ref2).unwrap());
    let combined = combine_record_pages(VersioningType::Full, &[read_back], 1).unwrap();

    assert_eq!(combined.populated_count(), 3);
    assert_eq!(combined.read_record(0).unwrap(), (1, &b"initial"[..]));
    assert_eq!(combined.read_record(5).unwrap(), (2, &b"five"[..]));
    assert_eq!(combined.read_record(10).unwrap(), (3, &b"new-slot"[..]));
}

#[test]
fn test_incremental_io_roundtrip_cow_with_preservation() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

    // Rev 1: base page (full dump)
    let rev1 = make_page(0, 1, &[
        (0, 1, b"r1-s0"), (1, 1, b"r1-s1"), (2, 1, b"r1-s2"),
    ]);
    // Rev 2: delta
    let rev2 = make_page(0, 2, &[(0, 1, b"r2-s0")]);
    // Rev 3: delta (chain = revs_to_restore => full dump)
    let rev3 = make_page(0, 3, &[(1, 1, b"r3-s1")]);

    let mut ref_ = PageReference::new();
    let container = combine_record_pages_for_modification(
        VersioningType::Incremental, &[rev3, rev2, rev1], 3,
        &mut ref_, 4, 0, 0,
    ).unwrap();

    // Simulate: modify slot 0 explicitly, then resolve preservation for the rest
    let (complete, mut modified) = container.into_parts();
    modified.insert_record(0, 1, b"r4-s0").unwrap();
    modified.resolve_preservation(&complete).unwrap();

    // Modified should now have all 3 slots: 0 (explicit), 1 (preserved from r3), 2 (preserved from r1)
    assert_eq!(modified.populated_count(), 3);
    assert_eq!(modified.read_record(0).unwrap(), (1, &b"r4-s0"[..]));
    assert_eq!(modified.read_record(1).unwrap(), (1, &b"r3-s1"[..]));
    assert_eq!(modified.read_record(2).unwrap(), (1, &b"r1-s2"[..]));

    // Write the resolved modified page to disk
    let mut ref4 = PageReference::new();
    writer.write_page(&mut ref4, &serialize(&modified)).unwrap();
    writer.force_all().unwrap();

    // Read back and verify
    let read_back = unwrap_kvl(writer.read_page(&ref4).unwrap());
    assert_eq!(read_back.populated_count(), 3);
    assert_eq!(read_back.read_record(0).unwrap(), (1, &b"r4-s0"[..]));
    assert_eq!(read_back.read_record(1).unwrap(), (1, &b"r3-s1"[..]));
    assert_eq!(read_back.read_record(2).unwrap(), (1, &b"r1-s2"[..]));
}

// ============================================================================
// Edge cases
// ============================================================================

#[test]
fn test_empty_page_combine() {
    let empty = SlottedPage::new(0, 1, IndexType::Document);
    let combined = combine_record_pages(VersioningType::Full, &[empty], 1).unwrap();
    assert_eq!(combined.populated_count(), 0);
}

#[test]
fn test_incremental_empty_older_fragments() {
    // Newest has data, older fragments are empty
    let rev3 = make_page(0, 3, &[(0, 1, b"data")]);
    let rev2 = SlottedPage::new(0, 2, IndexType::Document);
    let rev1 = SlottedPage::new(0, 1, IndexType::Document);

    let combined = combine_record_pages(
        VersioningType::Incremental, &[rev3, rev2, rev1], 3,
    ).unwrap();
    assert_eq!(combined.populated_count(), 1);
    assert_eq!(combined.read_record(0).unwrap(), (1, &b"data"[..]));
}

#[test]
fn test_incremental_data_only_in_oldest() {
    let rev3 = SlottedPage::new(0, 3, IndexType::Document);
    let rev2 = SlottedPage::new(0, 2, IndexType::Document);
    let rev1 = make_page(0, 1, &[(50, 1, b"ancient-data")]);

    let combined = combine_record_pages(
        VersioningType::Incremental, &[rev3, rev2, rev1], 3,
    ).unwrap();
    assert_eq!(combined.populated_count(), 1);
    assert_eq!(combined.read_record(50).unwrap(), (1, &b"ancient-data"[..]));
}

#[test]
fn test_many_slots_across_bitmap_words() {
    // Test slots spanning multiple bitmap u64 words (every 64th slot)
    let mut records: Vec<(usize, u8, &[u8])> = Vec::new();
    let data = b"x";
    for word in 0..8 {
        records.push((word * 64, 1, data));
        records.push((word * 64 + 63, 2, data));
    }
    let page = make_page(0, 1, &records);
    assert_eq!(page.populated_count(), 16);

    let combined = combine_record_pages(VersioningType::Full, &[page], 1).unwrap();
    assert_eq!(combined.populated_count(), 16);
    for word in 0..8 {
        assert!(combined.is_slot_occupied(word * 64));
        assert!(combined.is_slot_occupied(word * 64 + 63));
    }
}

#[test]
fn test_incremental_disjoint_slots_across_fragments() {
    // Each fragment has completely disjoint slots
    let rev3 = make_page(0, 3, &[(0, 1, b"A"), (1, 1, b"B")]);
    let rev2 = make_page(0, 2, &[(100, 2, b"C"), (101, 2, b"D")]);
    let rev1 = make_page(0, 1, &[(500, 3, b"E"), (501, 3, b"F")]);

    let combined = combine_record_pages(
        VersioningType::Incremental, &[rev3, rev2, rev1], 3,
    ).unwrap();
    assert_eq!(combined.populated_count(), 6);
    assert_eq!(combined.read_record(0).unwrap(), (1, &b"A"[..]));
    assert_eq!(combined.read_record(1).unwrap(), (1, &b"B"[..]));
    assert_eq!(combined.read_record(100).unwrap(), (2, &b"C"[..]));
    assert_eq!(combined.read_record(101).unwrap(), (2, &b"D"[..]));
    assert_eq!(combined.read_record(500).unwrap(), (3, &b"E"[..]));
    assert_eq!(combined.read_record(501).unwrap(), (3, &b"F"[..]));
}

#[test]
fn test_differential_node_kind_preserved() {
    let base = make_page(0, 0, &[(0, 10, b"kind-ten"), (5, 20, b"kind-twenty")]);
    let delta = make_page(0, 1, &[(0, 30, b"new-kind")]);

    let combined = combine_record_pages(VersioningType::Differential, &[delta, base], 3).unwrap();
    let (kind0, _) = combined.read_record(0).unwrap();
    assert_eq!(kind0, 30); // from delta
    let (kind5, _) = combined.read_record(5).unwrap();
    assert_eq!(kind5, 20); // from base
}

#[test]
fn test_sliding_snapshot_resolve_preserves_oow_then_write() {
    // Full lifecycle: combine → modify → resolve → write → read back
    let dir = tempfile::tempdir().unwrap();
    let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

    let rev3 = make_page(0, 3, &[(0, 1, b"v3")]);
    let rev2 = make_page(0, 2, &[(5, 2, b"v2")]);
    let rev1 = make_page(0, 1, &[(10, 3, b"oow-v1"), (20, 4, b"oow-v1-20")]);
    let mut ref_ = PageReference::new();

    let container = combine_record_pages_for_modification(
        VersioningType::SlidingSnapshot { window_size: 3 }, &[rev3, rev2, rev1], 3,
        &mut ref_, 4, 0, 0,
    ).unwrap();

    let (complete, mut modified) = container.into_parts();

    // Explicitly write slot 0
    modified.insert_record(0, 1, b"r4-explicit").unwrap();
    // Resolve preservation (slots 10, 20 from out-of-window)
    modified.resolve_preservation(&complete).unwrap();

    assert_eq!(modified.populated_count(), 3);
    assert_eq!(modified.read_record(0).unwrap(), (1, &b"r4-explicit"[..]));
    assert_eq!(modified.read_record(10).unwrap(), (3, &b"oow-v1"[..]));
    assert_eq!(modified.read_record(20).unwrap(), (4, &b"oow-v1-20"[..]));

    // Write to disk and read back
    let mut ref4 = PageReference::new();
    writer.write_page(&mut ref4, &serialize(&modified)).unwrap();
    writer.force_all().unwrap();

    let read_back = unwrap_kvl(writer.read_page(&ref4).unwrap());
    assert_eq!(read_back.populated_count(), 3);
    assert_eq!(read_back.read_record(0).unwrap(), (1, &b"r4-explicit"[..]));
    assert_eq!(read_back.read_record(10).unwrap(), (3, &b"oow-v1"[..]));
    assert_eq!(read_back.read_record(20).unwrap(), (4, &b"oow-v1-20"[..]));
}

// ============================================================================
// Cross-strategy consistency checks
// ============================================================================

#[test]
fn test_all_strategies_produce_same_result_single_fragment() {
    // With a single fragment (no delta/chain), all strategies should produce the same result
    let page = make_page(0, 1, &[
        (0, 1, b"alpha"), (10, 2, b"beta"), (100, 3, b"gamma"),
    ]);

    let full = combine_record_pages(VersioningType::Full, &[clone_page(&page)], 1).unwrap();
    let diff = combine_record_pages(VersioningType::Differential, &[clone_page(&page)], 3).unwrap();
    let inc = combine_record_pages(VersioningType::Incremental, &[clone_page(&page)], 3).unwrap();
    let ss = combine_record_pages(
        VersioningType::SlidingSnapshot { window_size: 3 }, &[page], 3,
    ).unwrap();

    for slot in [0, 10, 100] {
        let f = full.read_record(slot).unwrap();
        assert_eq!(f, diff.read_record(slot).unwrap());
        assert_eq!(f, inc.read_record(slot).unwrap());
        assert_eq!(f, ss.read_record(slot).unwrap());
    }
}

#[test]
fn test_incremental_and_sliding_snapshot_identical_read_2_fragments() {
    let rev2 = make_page(0, 2, &[(0, 1, b"new"), (5, 2, b"v2-only")]);
    let rev1 = make_page(0, 1, &[(0, 1, b"old"), (10, 3, b"v1-only")]);

    let inc = combine_record_pages(
        VersioningType::Incremental, &[clone_page(&rev2), clone_page(&rev1)], 3,
    ).unwrap();
    let ss = combine_record_pages(
        VersioningType::SlidingSnapshot { window_size: 3 }, &[rev2, rev1], 3,
    ).unwrap();

    assert_eq!(inc.populated_count(), ss.populated_count());
    for slot in [0, 5, 10] {
        assert_eq!(inc.read_record(slot).unwrap(), ss.read_record(slot).unwrap());
    }
}

// ============================================================================
// Stress / high slot count tests
// ============================================================================

#[test]
fn test_incremental_many_slots_fragmented() {
    // Rev 1: even slots 0,2,4,...,98
    let mut rev1_records: Vec<(usize, u8, Vec<u8>)> = Vec::new();
    for i in (0..100).step_by(2) {
        rev1_records.push((i, 1, format!("r1-{}", i).into_bytes()));
    }
    let rev1_refs: Vec<(usize, u8, &[u8])> = rev1_records.iter()
        .map(|(s, k, d)| (*s, *k, d.as_slice())).collect();
    let rev1 = make_page(0, 1, &rev1_refs);

    // Rev 2: odd slots 1,3,5,...,99
    let mut rev2_records: Vec<(usize, u8, Vec<u8>)> = Vec::new();
    for i in (1..100).step_by(2) {
        rev2_records.push((i, 2, format!("r2-{}", i).into_bytes()));
    }
    let rev2_refs: Vec<(usize, u8, &[u8])> = rev2_records.iter()
        .map(|(s, k, d)| (*s, *k, d.as_slice())).collect();
    let rev2 = make_page(0, 2, &rev2_refs);

    let combined = combine_record_pages(
        VersioningType::Incremental, &[rev2, rev1], 3,
    ).unwrap();

    assert_eq!(combined.populated_count(), 100);
    // Verify all slots
    for i in 0..100 {
        let (kind, data) = combined.read_record(i).unwrap();
        if i % 2 == 0 {
            assert_eq!(kind, 1);
            assert_eq!(data, format!("r1-{}", i).as_bytes());
        } else {
            assert_eq!(kind, 2);
            assert_eq!(data, format!("r2-{}", i).as_bytes());
        }
    }
}

#[test]
fn test_differential_overwrite_all_slots() {
    // Base has 50 slots, delta overwrites all of them
    let mut base_records: Vec<(usize, u8, Vec<u8>)> = Vec::new();
    let mut delta_records: Vec<(usize, u8, Vec<u8>)> = Vec::new();
    for i in 0..50 {
        base_records.push((i, 1, format!("base-{}", i).into_bytes()));
        delta_records.push((i, 2, format!("delta-{}", i).into_bytes()));
    }
    let base_refs: Vec<(usize, u8, &[u8])> = base_records.iter()
        .map(|(s, k, d)| (*s, *k, d.as_slice())).collect();
    let delta_refs: Vec<(usize, u8, &[u8])> = delta_records.iter()
        .map(|(s, k, d)| (*s, *k, d.as_slice())).collect();
    let base = make_page(0, 0, &base_refs);
    let delta = make_page(0, 1, &delta_refs);

    let combined = combine_record_pages(
        VersioningType::Differential, &[delta, base], 3,
    ).unwrap();

    assert_eq!(combined.populated_count(), 50);
    for i in 0..50 {
        let (kind, data) = combined.read_record(i).unwrap();
        assert_eq!(kind, 2); // all from delta
        assert_eq!(data, format!("delta-{}", i).as_bytes());
    }
}

// ============================================================================
// Helper: clone a SlottedPage (can't impl Clone for external type)
// ============================================================================

fn clone_page(src: &SlottedPage) -> SlottedPage {
    let mut dst = src.new_instance();
    for &slot in &src.populated_slots() {
        let _ = dst.copy_slot_from(src, slot);
    }
    dst
}
