//! Versioning strategy implementations for page combination.
//!
//! Each strategy defines how page fragments are merged for reading (combine_record_pages)
//! and how copy-on-write pages are prepared for writing (combine_record_pages_for_modification).
//!
//! Port of Java's `io.sirix.settings.VersioningType` with HFT-grade bitmap operations.

use crate::constants::BITMAP_WORDS;
use crate::constants::NDP_NODE_COUNT;
use crate::error::Result;
use crate::page::SlottedPage;
use crate::page::page_reference::PageReference;
use crate::types::PageFragmentKey;
use crate::types::VersioningType;
use crate::versioning::PageContainer;

// ---------------------------------------------------------------------------
// combine_record_pages — reconstruct a complete page from fragments (for reads)
// ---------------------------------------------------------------------------

/// Reconstruct a complete SlottedPage from a list of page fragments.
///
/// `pages` are ordered newest-first: `pages[0]` is the most recent fragment.
/// Newer fragments take precedence — a slot filled by a newer fragment is
/// never overwritten by an older one.
///
/// # Arguments
/// * `versioning` — the versioning strategy
/// * `pages` — page fragments, newest first
/// * `revs_to_restore` — max number of revisions in the restore chain
pub fn combine_record_pages(
    versioning: VersioningType,
    pages: &[SlottedPage],
    revs_to_restore: usize,
) -> Result<SlottedPage> {
    assert!(!pages.is_empty(), "combine_record_pages: no pages provided");

    match versioning {
        VersioningType::Full => combine_full(pages),
        VersioningType::Differential => combine_differential(pages),
        VersioningType::Incremental => combine_incremental(pages, revs_to_restore),
        VersioningType::SlidingSnapshot { .. } => combine_incremental(pages, revs_to_restore),
    }
}

/// Full: single complete page, just copy all slots.
#[inline]
fn combine_full(pages: &[SlottedPage]) -> Result<SlottedPage> {
    debug_assert!(pages.len() == 1, "Full versioning expects exactly 1 page");
    let src = &pages[0];
    let mut dst = src.new_instance();
    let slots = src.populated_slots();
    for slot in &slots {
        dst.copy_slot_from(src, *slot)?;
    }
    Ok(dst)
}

/// Differential: latest page + optional full dump base.
fn combine_differential(pages: &[SlottedPage]) -> Result<SlottedPage> {
    debug_assert!(pages.len() <= 2, "Differential expects at most 2 pages");
    let latest = &pages[0];
    let mut dst = latest.new_instance();

    // Copy all slots from the latest page
    let latest_slots = latest.populated_slots();
    for &slot in &latest_slots {
        dst.copy_slot_from(latest, slot)?;
    }

    // Fill gaps from the full dump if present
    if pages.len() == 2 && (dst.populated_count() as usize) < NDP_NODE_COUNT {
        let full_dump = &pages[1];
        let filled_bmp = dst.slot_bitmap();
        let full_slots = full_dump.populated_slots();
        for &slot in &full_slots {
            // O(1) bitmap check: skip if already filled from latest
            if (filled_bmp[slot >> 6] & (1u64 << (slot & 63))) != 0 {
                continue;
            }
            dst.copy_slot_from(full_dump, slot)?;
            if dst.populated_count() as usize == NDP_NODE_COUNT {
                break;
            }
        }
    }

    Ok(dst)
}

/// Incremental / SlidingSnapshot (read path): merge chain newest→oldest.
fn combine_incremental(pages: &[SlottedPage], _revs_to_restore: usize) -> Result<SlottedPage> {
    let first = &pages[0];
    let mut dst = first.new_instance();

    // Use a local bitmap to track filled slots (avoids re-reading dst bitmap each iteration).
    let mut filled_bmp = [0u64; BITMAP_WORDS];
    let mut filled_count: usize = 0;

    for page in pages {
        debug_assert_eq!(page.record_page_key(), first.record_page_key());
        if filled_count == NDP_NODE_COUNT {
            break;
        }

        let slots = page.populated_slots();
        for &slot in &slots {
            // O(1) bitmap check
            if (filled_bmp[slot >> 6] & (1u64 << (slot & 63))) != 0 {
                continue; // already filled from a newer fragment
            }

            dst.copy_slot_from(page, slot)?;
            filled_bmp[slot >> 6] |= 1u64 << (slot & 63);
            filled_count += 1;

            if filled_count == NDP_NODE_COUNT {
                break;
            }
        }
    }

    Ok(dst)
}

// ---------------------------------------------------------------------------
// combine_record_pages_for_modification — COW page preparation (for writes)
// ---------------------------------------------------------------------------

/// Prepare a complete + modified page pair for copy-on-write modification.
///
/// Returns a `PageContainer` where:
/// - `complete` = fully reconstructed page (for readers)
/// - `modified` = page for the current transaction to write into
///
/// For Full versioning, `modified` is a full copy. For other strategies, `modified`
/// starts empty with preservation marks for slots that should be lazily copied
/// at commit time if they weren't explicitly overwritten.
///
/// # Arguments
/// * `versioning` — the versioning strategy
/// * `pages` — page fragments, newest first
/// * `revs_to_restore` — max revisions in the restore chain
/// * `reference` — the PageReference (updated with fragment keys for non-Full)
/// * `current_revision` — the current transaction's revision number
/// * `database_id` — database ID for fragment key
/// * `resource_id` — resource ID for fragment key
pub fn combine_record_pages_for_modification(
    versioning: VersioningType,
    pages: &[SlottedPage],
    revs_to_restore: usize,
    reference: &mut PageReference,
    current_revision: i32,
    _database_id: i64,
    _resource_id: i64,
) -> Result<PageContainer> {
    assert!(!pages.is_empty(), "combine_for_modification: no pages provided");

    match versioning {
        VersioningType::Full => modify_full(pages),
        VersioningType::Differential => {
            modify_differential(pages, revs_to_restore, reference, current_revision)
        }
        VersioningType::Incremental => {
            modify_incremental(pages, revs_to_restore, reference, current_revision)
        }
        VersioningType::SlidingSnapshot { window_size } => {
            modify_sliding_snapshot(pages, revs_to_restore, reference, current_revision, window_size)
        }
    }
}

/// Full: both complete and modified are the same full copy.
fn modify_full(pages: &[SlottedPage]) -> Result<PageContainer> {
    debug_assert!(pages.len() == 1);
    let src = &pages[0];

    // Single page for both complete and modified (optimization from Java)
    let mut page = src.new_instance();
    let slots = src.populated_slots();
    for &slot in &slots {
        page.copy_slot_from(src, slot)?;
    }

    Ok(PageContainer::new_single(page))
}

/// Differential: complete = latest + full_dump merged; modified = lazy copy or full dump.
fn modify_differential(
    pages: &[SlottedPage],
    revs_to_restore: usize,
    reference: &mut PageReference,
    current_revision: i32,
) -> Result<PageContainer> {
    debug_assert!(pages.len() <= 2);
    let first = &pages[0];

    // Update fragment keys on the reference
    let frag = PageFragmentKey {
        revision: first.revision(),
        offset: reference.key(),
    };
    reference.clear_page_fragments();
    reference.add_page_fragment(frag);

    let mut complete = first.new_instance();
    let mut modified = first.new_instance();
    let is_full_dump_revision = current_revision as usize % revs_to_restore == 0;

    // Copy all slots from latest to complete + mark preservation on modified
    let latest_slots = first.populated_slots();
    for &slot in &latest_slots {
        complete.copy_slot_from(first, slot)?;
        modified.mark_slot_for_preservation(slot);
    }

    // Fill gaps from full dump
    if pages.len() == 2 && (complete.populated_count() as usize) < NDP_NODE_COUNT {
        let full_dump = &pages[1];
        let filled_bmp = complete.slot_bitmap();
        let full_slots = full_dump.populated_slots();
        for &slot in &full_slots {
            if (filled_bmp[slot >> 6] & (1u64 << (slot & 63))) != 0 {
                continue;
            }
            complete.copy_slot_from(full_dump, slot)?;
            if is_full_dump_revision {
                modified.mark_slot_for_preservation(slot);
            }
        }
    }

    Ok(PageContainer::new(complete, modified))
}

/// Incremental: complete = all fragments merged; modified = lazy copy if full dump.
fn modify_incremental(
    pages: &[SlottedPage],
    revs_to_restore: usize,
    reference: &mut PageReference,
    _current_revision: i32,
) -> Result<PageContainer> {
    let first = &pages[0];

    // Build fragment chain: current + previous fragments (up to revs_to_restore - 1)
    let mut frags = Vec::with_capacity(revs_to_restore);
    frags.push(PageFragmentKey {
        revision: first.revision(),
        offset: reference.key(),
    });
    for existing in reference.page_fragments() {
        if frags.len() >= revs_to_restore - 1 {
            break;
        }
        frags.push(*existing);
    }
    reference.clear_page_fragments();
    for f in &frags {
        reference.add_page_fragment(*f);
    }

    let mut complete = first.new_instance();
    let mut modified = first.new_instance();
    let is_full_dump = pages.len() == revs_to_restore;

    let mut filled_bmp = [0u64; BITMAP_WORDS];
    let mut filled_count: usize = 0;

    for page in pages {
        if filled_count == NDP_NODE_COUNT {
            break;
        }

        let slots = page.populated_slots();
        for &slot in &slots {
            if (filled_bmp[slot >> 6] & (1u64 << (slot & 63))) != 0 {
                continue;
            }
            complete.copy_slot_from(page, slot)?;
            filled_bmp[slot >> 6] |= 1u64 << (slot & 63);
            filled_count += 1;

            if is_full_dump {
                modified.mark_slot_for_preservation(slot);
            }

            if filled_count == NDP_NODE_COUNT {
                break;
            }
        }
    }

    Ok(PageContainer::new(complete, modified))
}

/// SlidingSnapshot: incremental with window-based GC for out-of-window fragments.
fn modify_sliding_snapshot(
    pages: &[SlottedPage],
    revs_to_restore: usize,
    reference: &mut PageReference,
    _current_revision: i32,
    _window_size: u32,
) -> Result<PageContainer> {
    let first = &pages[0];

    // Build fragment chain (same as incremental)
    let mut frags = Vec::with_capacity(revs_to_restore);
    frags.push(PageFragmentKey {
        revision: first.revision(),
        offset: reference.key(),
    });
    for existing in reference.page_fragments() {
        if frags.len() >= revs_to_restore - 1 {
            break;
        }
        frags.push(*existing);
    }
    reference.clear_page_fragments();
    for f in &frags {
        reference.add_page_fragment(*f);
    }

    let mut complete = first.new_instance();
    let mut modified = first.new_instance();

    let has_out_of_window = pages.len() == revs_to_restore;
    let last_in_window_idx = if has_out_of_window {
        pages.len() - 2
    } else {
        pages.len() - 1
    };

    let mut filled_bmp = [0u64; BITMAP_WORDS];
    let mut in_window_bmp = [0u64; BITMAP_WORDS];
    let mut filled_count: usize = 0;

    // Phase 1: Process in-window fragments
    for i in 0..=last_in_window_idx {
        let page = &pages[i];
        if filled_count == NDP_NODE_COUNT {
            break;
        }

        let slots = page.populated_slots();
        for &slot in &slots {
            in_window_bmp[slot >> 6] |= 1u64 << (slot & 63);

            if (filled_bmp[slot >> 6] & (1u64 << (slot & 63))) != 0 {
                continue;
            }
            complete.copy_slot_from(page, slot)?;
            filled_bmp[slot >> 6] |= 1u64 << (slot & 63);
            filled_count += 1;

            if filled_count == NDP_NODE_COUNT {
                break;
            }
        }
    }

    // Phase 2: Process out-of-window fragment
    if has_out_of_window {
        let oow_page = &pages[pages.len() - 1];
        let oow_slots = oow_page.populated_slots();
        for &slot in &oow_slots {
            // Add to complete if not already filled
            if (filled_bmp[slot >> 6] & (1u64 << (slot & 63))) == 0 {
                complete.copy_slot_from(oow_page, slot)?;
                filled_bmp[slot >> 6] |= 1u64 << (slot & 63);
            }
            // If slot is NOT in the sliding window, mark for preservation
            // (these records are falling out of the window and must be re-written)
            if (in_window_bmp[slot >> 6] & (1u64 << (slot & 63))) == 0 {
                modified.mark_slot_for_preservation(slot);
            }
        }
    }

    Ok(PageContainer::new(complete, modified))
}

// ---------------------------------------------------------------------------
// get_revision_roots — which revisions to read for page reconstruction
// ---------------------------------------------------------------------------

/// Returns the revision numbers needed to reconstruct a page.
///
/// The returned array is ordered from newest to oldest.
///
/// # Arguments
/// * `versioning` — the versioning strategy
/// * `previous_revision` — the revision to reconstruct from
/// * `revs_to_restore` — max chain length
pub fn get_revision_roots(
    versioning: VersioningType,
    previous_revision: i32,
    revs_to_restore: i32,
) -> Vec<i32> {
    match versioning {
        VersioningType::Full => {
            vec![previous_revision]
        }
        VersioningType::Differential => {
            let revisions_since_dump = previous_revision % revs_to_restore;
            let last_full_dump = previous_revision - revisions_since_dump;
            if last_full_dump == previous_revision {
                vec![last_full_dump]
            } else {
                vec![previous_revision, last_full_dump]
            }
        }
        VersioningType::Incremental | VersioningType::SlidingSnapshot { .. } => {
            let mut roots = Vec::with_capacity(revs_to_restore as usize);
            let until = previous_revision - revs_to_restore;
            let mut i = previous_revision;
            while i > until && i >= 0 {
                roots.push(i);
                i -= 1;
            }
            roots
        }
    }
}

// ---------------------------------------------------------------------------
// should_store_full_snapshot — when to write a complete page vs. a fragment
// ---------------------------------------------------------------------------

/// Determine whether a full page snapshot should be stored (vs. a delta fragment).
///
/// # Arguments
/// * `versioning` — the versioning strategy
/// * `current_revision` — the current transaction revision
/// * `revs_to_restore` — chain length threshold
/// * `fragment_count` — number of existing fragments for this page
pub fn should_store_full_snapshot(
    versioning: VersioningType,
    current_revision: i32,
    revs_to_restore: i32,
    fragment_count: usize,
) -> bool {
    // First revision is always full
    if current_revision == 1 || fragment_count == 0 {
        return true;
    }
    match versioning {
        VersioningType::Full => true,
        VersioningType::Differential => current_revision % revs_to_restore == 0,
        VersioningType::Incremental | VersioningType::SlidingSnapshot { .. } => {
            fragment_count >= (revs_to_restore - 1) as usize
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::IndexType;

    fn make_page(key: i64, revision: i32, records: &[(usize, u8, &[u8])]) -> SlottedPage {
        let mut page = SlottedPage::new(key, revision, IndexType::Document);
        for &(slot, kind, data) in records {
            page.insert_record(slot, kind, data).unwrap();
        }
        page
    }

    // =========================================================================
    // get_revision_roots tests
    // =========================================================================

    #[test]
    fn test_revision_roots_full() {
        let roots = get_revision_roots(VersioningType::Full, 5, 3);
        assert_eq!(roots, vec![5]);
    }

    #[test]
    fn test_revision_roots_differential_on_dump() {
        // Revision 6 with revs_to_restore=3: 6 % 3 == 0, so just [6]
        let roots = get_revision_roots(VersioningType::Differential, 6, 3);
        assert_eq!(roots, vec![6]);
    }

    #[test]
    fn test_revision_roots_differential_between_dumps() {
        // Revision 7 with revs_to_restore=3: 7 % 3 == 1, last_full=6
        let roots = get_revision_roots(VersioningType::Differential, 7, 3);
        assert_eq!(roots, vec![7, 6]);
    }

    #[test]
    fn test_revision_roots_incremental() {
        let roots = get_revision_roots(VersioningType::Incremental, 5, 3);
        assert_eq!(roots, vec![5, 4, 3]);
    }

    #[test]
    fn test_revision_roots_incremental_early() {
        let roots = get_revision_roots(VersioningType::Incremental, 1, 5);
        assert_eq!(roots, vec![1, 0]);
    }

    #[test]
    fn test_revision_roots_sliding_snapshot() {
        let roots = get_revision_roots(
            VersioningType::SlidingSnapshot { window_size: 4 }, 10, 4,
        );
        assert_eq!(roots, vec![10, 9, 8, 7]);
    }

    // =========================================================================
    // should_store_full_snapshot tests
    // =========================================================================

    #[test]
    fn test_full_always_stores_full() {
        assert!(should_store_full_snapshot(VersioningType::Full, 5, 3, 4));
    }

    #[test]
    fn test_first_revision_always_full() {
        assert!(should_store_full_snapshot(VersioningType::Incremental, 1, 5, 0));
    }

    #[test]
    fn test_differential_periodic() {
        assert!(should_store_full_snapshot(VersioningType::Differential, 6, 3, 2));
        assert!(!should_store_full_snapshot(VersioningType::Differential, 7, 3, 1));
    }

    #[test]
    fn test_incremental_chain_threshold() {
        assert!(!should_store_full_snapshot(VersioningType::Incremental, 5, 4, 2));
        assert!(should_store_full_snapshot(VersioningType::Incremental, 5, 4, 3));
    }

    // =========================================================================
    // combine_record_pages — Full
    // =========================================================================

    #[test]
    fn test_combine_full_single_page() {
        let page = make_page(0, 1, &[
            (0, 1, b"hello"),
            (5, 2, b"world"),
            (100, 3, b"test"),
        ]);
        let result = combine_record_pages(VersioningType::Full, &[page], 1).unwrap();
        assert_eq!(result.populated_count(), 3);
        assert_eq!(result.read_record(0).unwrap(), (1, &b"hello"[..]));
        assert_eq!(result.read_record(5).unwrap(), (2, &b"world"[..]));
        assert_eq!(result.read_record(100).unwrap(), (3, &b"test"[..]));
    }

    // =========================================================================
    // combine_record_pages — Differential
    // =========================================================================

    #[test]
    fn test_combine_differential_latest_only() {
        let latest = make_page(0, 3, &[(0, 1, b"new-0"), (5, 2, b"new-5")]);
        let result = combine_record_pages(VersioningType::Differential, &[latest], 3).unwrap();
        assert_eq!(result.populated_count(), 2);
        assert_eq!(result.read_record(0).unwrap(), (1, &b"new-0"[..]));
    }

    #[test]
    fn test_combine_differential_latest_plus_base() {
        let latest = make_page(0, 3, &[(0, 1, b"new-0"), (5, 2, b"new-5")]);
        let base = make_page(0, 0, &[(0, 1, b"old-0"), (10, 3, b"base-10"), (20, 4, b"base-20")]);
        let result = combine_record_pages(
            VersioningType::Differential, &[latest, base], 3,
        ).unwrap();
        // slot 0: from latest (new-0, not old-0)
        // slot 5: from latest
        // slot 10: from base (gap fill)
        // slot 20: from base (gap fill)
        assert_eq!(result.populated_count(), 4);
        assert_eq!(result.read_record(0).unwrap(), (1, &b"new-0"[..]));
        assert_eq!(result.read_record(5).unwrap(), (2, &b"new-5"[..]));
        assert_eq!(result.read_record(10).unwrap(), (3, &b"base-10"[..]));
        assert_eq!(result.read_record(20).unwrap(), (4, &b"base-20"[..]));
    }

    // =========================================================================
    // combine_record_pages — Incremental
    // =========================================================================

    #[test]
    fn test_combine_incremental_chain() {
        // Three fragments: rev3 (newest), rev2, rev1 (oldest/base)
        let rev3 = make_page(0, 3, &[(0, 1, b"v3-slot0")]);
        let rev2 = make_page(0, 2, &[(0, 1, b"v2-slot0"), (5, 2, b"v2-slot5")]);
        let rev1 = make_page(0, 1, &[(0, 1, b"v1-slot0"), (5, 2, b"v1-slot5"), (10, 3, b"v1-slot10")]);

        let result = combine_record_pages(
            VersioningType::Incremental, &[rev3, rev2, rev1], 3,
        ).unwrap();

        // slot 0: from rev3 (newest wins)
        assert_eq!(result.read_record(0).unwrap(), (1, &b"v3-slot0"[..]));
        // slot 5: from rev2 (rev3 doesn't have it, rev2 does)
        assert_eq!(result.read_record(5).unwrap(), (2, &b"v2-slot5"[..]));
        // slot 10: from rev1 (only rev1 has it)
        assert_eq!(result.read_record(10).unwrap(), (3, &b"v1-slot10"[..]));
        assert_eq!(result.populated_count(), 3);
    }

    #[test]
    fn test_combine_incremental_newest_wins() {
        let rev2 = make_page(0, 2, &[(0, 1, b"UPDATED")]);
        let rev1 = make_page(0, 1, &[(0, 1, b"ORIGINAL")]);
        let result = combine_record_pages(
            VersioningType::Incremental, &[rev2, rev1], 2,
        ).unwrap();
        assert_eq!(result.read_record(0).unwrap(), (1, &b"UPDATED"[..]));
    }

    // =========================================================================
    // combine_record_pages — SlidingSnapshot
    // =========================================================================

    #[test]
    fn test_combine_sliding_snapshot_same_as_incremental_for_reads() {
        let rev3 = make_page(0, 3, &[(0, 1, b"v3")]);
        let rev2 = make_page(0, 2, &[(5, 2, b"v2")]);
        let rev1 = make_page(0, 1, &[(10, 3, b"v1")]);

        let result = combine_record_pages(
            VersioningType::SlidingSnapshot { window_size: 3 }, &[rev3, rev2, rev1], 3,
        ).unwrap();
        assert_eq!(result.populated_count(), 3);
        assert_eq!(result.read_record(0).unwrap(), (1, &b"v3"[..]));
        assert_eq!(result.read_record(5).unwrap(), (2, &b"v2"[..]));
        assert_eq!(result.read_record(10).unwrap(), (3, &b"v1"[..]));
    }

    // =========================================================================
    // combine_record_pages_for_modification — Full
    // =========================================================================

    #[test]
    fn test_modify_full_both_pages_identical() {
        let page = make_page(0, 1, &[(0, 1, b"data"), (5, 2, b"more")]);
        let mut ref_ = PageReference::new();
        let container = combine_record_pages_for_modification(
            VersioningType::Full, &[page], 1, &mut ref_, 2, 0, 0,
        ).unwrap();

        // Both complete and modified should have the same data
        assert_eq!(container.complete().populated_count(), 2);
        assert_eq!(container.modified().populated_count(), 2);
        assert_eq!(container.complete().read_record(0).unwrap(), (1, &b"data"[..]));
        assert_eq!(container.modified().read_record(0).unwrap(), (1, &b"data"[..]));
    }

    // =========================================================================
    // combine_record_pages_for_modification — Differential
    // =========================================================================

    #[test]
    fn test_modify_differential_complete_merged() {
        let latest = make_page(0, 3, &[(0, 1, b"new")]);
        let base = make_page(0, 0, &[(0, 1, b"old"), (5, 2, b"base")]);
        let mut ref_ = PageReference::new();
        let container = combine_record_pages_for_modification(
            VersioningType::Differential, &[latest, base], 3,
            &mut ref_, 4, 0, 0,
        ).unwrap();

        // Complete should have merged data
        assert_eq!(container.complete().populated_count(), 2);
        assert_eq!(container.complete().read_record(0).unwrap(), (1, &b"new"[..]));
        assert_eq!(container.complete().read_record(5).unwrap(), (2, &b"base"[..]));

        // Modified should have preservation marks (not actual data until resolve)
        // Fragment key should be set on reference
        assert_eq!(ref_.page_fragments().len(), 1);
    }

    // =========================================================================
    // combine_record_pages_for_modification — Incremental
    // =========================================================================

    #[test]
    fn test_modify_incremental_fragment_chain() {
        let rev3 = make_page(0, 3, &[(0, 1, b"v3")]);
        let rev2 = make_page(0, 2, &[(5, 2, b"v2")]);
        let rev1 = make_page(0, 1, &[(10, 3, b"v1")]);
        let mut ref_ = PageReference::new();
        // Add existing fragments
        ref_.add_page_fragment(PageFragmentKey { revision: 2, offset: 100 });

        let container = combine_record_pages_for_modification(
            VersioningType::Incremental, &[rev3, rev2, rev1], 3,
            &mut ref_, 4, 0, 0,
        ).unwrap();

        // Complete should have all 3 slots
        assert_eq!(container.complete().populated_count(), 3);

        // Fragment chain should be updated (limited to revs_to_restore - 1)
        assert!(ref_.page_fragments().len() <= 2);
    }

    // =========================================================================
    // combine_record_pages_for_modification — SlidingSnapshot
    // =========================================================================

    #[test]
    fn test_modify_sliding_snapshot_out_of_window_preservation() {
        // 3 pages: rev3 (in-window), rev2 (in-window), rev1 (out-of-window)
        let rev3 = make_page(0, 3, &[(0, 1, b"v3")]);
        let rev2 = make_page(0, 2, &[(5, 2, b"v2")]);
        let rev1 = make_page(0, 1, &[(10, 3, b"v1"), (20, 4, b"oow")]);
        let mut ref_ = PageReference::new();

        let container = combine_record_pages_for_modification(
            VersioningType::SlidingSnapshot { window_size: 3 }, &[rev3, rev2, rev1], 3,
            &mut ref_, 4, 0, 0,
        ).unwrap();

        // Complete should have all 4 slots
        assert_eq!(container.complete().populated_count(), 4);

        // Out-of-window slots (10, 20) not in window but in complete.
        // Modified should have preservation marks for slots 10 and 20
        // (they fell out of the window and need re-writing)
        assert!(container.modified().is_marked_for_preservation(10));
        assert!(container.modified().is_marked_for_preservation(20));
        // In-window slot 0 should NOT be marked for preservation on modified
        assert!(!container.modified().is_marked_for_preservation(0));
    }

    // =========================================================================
    // Preservation resolution tests
    // =========================================================================

    #[test]
    fn test_preservation_resolve() {
        let complete = make_page(0, 1, &[
            (0, 1, b"slot0"), (5, 2, b"slot5"), (10, 3, b"slot10"),
        ]);
        let mut modified = SlottedPage::new(0, 2, IndexType::Document);
        // Insert one record directly
        modified.insert_record(0, 1, b"modified-0").unwrap();
        // Mark slots 5 and 10 for preservation
        modified.mark_slot_for_preservation(5);
        modified.mark_slot_for_preservation(10);

        // Resolve: slot 0 already occupied → skip. Slots 5,10 → copy from complete.
        modified.resolve_preservation(&complete).unwrap();

        assert_eq!(modified.populated_count(), 3);
        assert_eq!(modified.read_record(0).unwrap(), (1, &b"modified-0"[..]));
        assert_eq!(modified.read_record(5).unwrap(), (2, &b"slot5"[..]));
        assert_eq!(modified.read_record(10).unwrap(), (3, &b"slot10"[..]));
    }

    #[test]
    fn test_preservation_overwrite_takes_precedence() {
        let complete = make_page(0, 1, &[(0, 1, b"original")]);
        let mut modified = SlottedPage::new(0, 2, IndexType::Document);
        modified.insert_record(0, 1, b"overwritten").unwrap();
        modified.mark_slot_for_preservation(0);

        // Resolve: slot 0 is already occupied → preservation skipped
        modified.resolve_preservation(&complete).unwrap();

        assert_eq!(modified.read_record(0).unwrap(), (1, &b"overwritten"[..]));
    }

    // =========================================================================
    // End-to-end versioning lifecycle test
    // =========================================================================

    #[test]
    fn test_incremental_multi_revision_lifecycle() {
        // Simulate 4 revisions of incremental versioning with revs_to_restore=3

        // Rev 1 (full dump): all slots fresh
        let rev1 = make_page(0, 1, &[
            (0, 1, b"r1-s0"), (1, 1, b"r1-s1"), (2, 1, b"r1-s2"),
        ]);

        // Rev 2: only slot 0 updated
        let rev2 = make_page(0, 2, &[(0, 1, b"r2-s0")]);

        // Rev 3: only slot 1 updated
        let rev3 = make_page(0, 3, &[(1, 1, b"r3-s1")]);

        // Combine rev3, rev2, rev1 (newest first)
        let result = combine_record_pages(
            VersioningType::Incremental, &[rev3, rev2, rev1], 3,
        ).unwrap();

        assert_eq!(result.populated_count(), 3);
        // slot 0: from rev2 (rev3 doesn't have it, rev2 is newer than rev1)
        assert_eq!(result.read_record(0).unwrap(), (1, &b"r2-s0"[..]));
        // slot 1: from rev3 (newest)
        assert_eq!(result.read_record(1).unwrap(), (1, &b"r3-s1"[..]));
        // slot 2: from rev1 (only rev1 has it)
        assert_eq!(result.read_record(2).unwrap(), (1, &b"r1-s2"[..]));
    }

    #[test]
    fn test_differential_periodic_full_dump_cycle() {
        // revs_to_restore=3: full dumps at revisions 0, 3, 6, ...

        // Rev 0 (full dump)
        let rev0 = make_page(0, 0, &[
            (0, 1, b"r0-s0"), (1, 1, b"r0-s1"), (2, 1, b"r0-s2"),
        ]);

        // Rev 1 (delta): update slot 0
        let rev1 = make_page(0, 1, &[(0, 1, b"r1-s0")]);

        // Reconstruct at rev1: [rev1, rev0]
        let result = combine_record_pages(
            VersioningType::Differential, &[rev1, rev0], 3,
        ).unwrap();

        assert_eq!(result.populated_count(), 3);
        assert_eq!(result.read_record(0).unwrap(), (1, &b"r1-s0"[..]));
        assert_eq!(result.read_record(1).unwrap(), (1, &b"r0-s1"[..]));
        assert_eq!(result.read_record(2).unwrap(), (1, &b"r0-s2"[..]));

        // Check that rev 3 would be a full dump
        assert!(should_store_full_snapshot(VersioningType::Differential, 3, 3, 1));
        assert!(!should_store_full_snapshot(VersioningType::Differential, 4, 3, 1));
    }
}
