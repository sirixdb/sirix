/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.cache.FrameSlotAllocator;
import io.sirix.index.IndexType;
import io.sirix.index.hot.HOTIncrementalInsert.BiNode;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.OverflowPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Segment-reference routing through {@link HOTIncrementalInsert#splitLeafPage} — the incremental
 * sibling of the rebuild paths' {@code reattachSegmentRefs}.
 *
 * <p>
 * The defect this pins down: the incremental split rebuilds both halves from {@code (key, value)}
 * pairs and abandons the source leaf, so a projection index's segment-reference side map (the
 * durable offsets of out-of-line column segments — {@code
 * docs/PROJECTION_INDEX_STORAGE_REDESIGN.md} §2.3) had no way across. The path was guarded rather
 * than instrumented, and the guard fired for real while building the projection over a 100M-row
 * corpus ("Incremental leaf split would drop 64 segment reference(s) on leaf pageKey=5033"),
 * aborting the build. Every reference must now land on whichever half physically holds its owning
 * slot.
 */
@DisplayName("HOTIncrementalInsert.splitLeafPage — segment-reference routing")
final class HOTIncrementalSplitSegmentRefTest {

  private static final int SLOT_KINDS = 9;
  private static final byte[] VALUE = {7, 7, 7, 7};

  /** The projection index's slot-key shape: {@code (rowGroupId << 16) | slotKind}. */
  private static long slotKey(final long rowGroupId, final int slotKind) {
    return (rowGroupId << 16) | slotKind;
  }

  private static byte[] keyBytes(final long slotKey) {
    final byte[] out = new byte[8];
    PathKeySerializer.INSTANCE.serialize(slotKey, out, 0);
    return out;
  }

  @Test
  @DisplayName("a second-half allocation failure retires the already-built first half")
  void secondHalfFailureClosesFirstHalf() {
    final HOTLeafPage source = new HOTLeafPage(1, 1, IndexType.PATH);
    assertTrue(source.put(keyBytes(0), VALUE));
    assertTrue(source.put(keyBytes(2), VALUE));
    final FrameSlotAllocator frameAllocator = FrameSlotAllocator.getInstance();
    final int frameClass = FrameSlotAllocator.indexForSize(HOTLeafPage.DEFAULT_SIZE);
    final int liveBefore = frameAllocator.liveSlotCount(frameClass);
    final AtomicLong allocationCalls = new AtomicLong();
    final IllegalStateException sentinel = new IllegalStateException("injected second-half allocation failure");

    try {
      final IllegalStateException failure = assertThrows(IllegalStateException.class,
          () -> HOTIncrementalInsert.splitLeafPage(source, keyBytes(1), VALUE, 2, IndexType.PATH, () -> {
            if (allocationCalls.incrementAndGet() == 2) {
              throw sentinel;
            }
            return 100 + allocationCalls.get();
          }));

      assertSame(sentinel, failure);
      assertEquals(2, allocationCalls.get());
      assertEquals(liveBefore, frameAllocator.liveSlotCount(frameClass),
          "the unpublished first half must not retain its 64 KiB frame");
    } finally {
      source.close();
    }
  }

  @Test
  @DisplayName("segment routing failure retires both unpublished split halves")
  void segmentRoutingFailureClosesBothHalves() {
    final AtomicLong allocator = new AtomicLong(1);
    final HOTLeafPage source = new HOTLeafPage(allocator.getAndIncrement(), 1, IndexType.PATH);
    assertTrue(source.put(keyBytes(0), VALUE));
    assertTrue(source.put(keyBytes(2), VALUE));
    final long missingOwnerRefKey = HOTLeafPage.overflowPageRefKey(99, 0);
    final PageReference segmentReference = new PageReference();
    segmentReference.setPage(new OverflowPage(new byte[] {99}));
    source.setPageReference(missingOwnerRefKey, segmentReference);
    final FrameSlotAllocator frameAllocator = FrameSlotAllocator.getInstance();
    final int frameClass = FrameSlotAllocator.indexForSize(HOTLeafPage.DEFAULT_SIZE);
    final int liveBefore = frameAllocator.liveSlotCount(frameClass);

    try {
      assertThrows(IllegalStateException.class, () -> HOTIncrementalInsert.splitLeafPage(source, keyBytes(1), VALUE, 2,
          IndexType.PATH, allocator::getAndIncrement));

      assertEquals(liveBefore, frameAllocator.liveSlotCount(frameClass),
          "neither unpublished half may survive a failed side-reference rehome");
      assertEquals(1, source.segmentRefCount(), "the still-owned source keeps its side reference");
      assertSame(segmentReference, source.getPageReference(missingOwnerRefKey));
    } finally {
      source.close();
    }
  }

  @Test
  @DisplayName("every side-map reference follows its owning slot into the half that holds it")
  void segmentRefsFollowTheirOwningSlot() {
    final AtomicLong allocator = new AtomicLong(1);
    final HOTLeafPage source = new HOTLeafPage(allocator.getAndIncrement(), 1, IndexType.PATH);

    // 40 row groups x 9 slot kinds, ascending — the projection build's write order. Segments hang
    // off the descriptor slot (kind 0) and a mid slot (kind 4), two sub-ids each, so the source
    // carries far more references than the 64 that aborted the 100M build.
    final int rowGroups = 40;
    final Map<Long, PageReference> expected = new HashMap<>();
    final Map<Long, Long> ownerOf = new HashMap<>();
    for (long rg = 1; rg <= rowGroups; rg++) {
      for (int kind = 0; kind < SLOT_KINDS; kind++) {
        assertTrue(source.put(keyBytes(slotKey(rg, kind)), VALUE), "source leaf must hold the slot");
      }
      for (final int kind : new int[] {0, 4}) {
        for (int subId = 0; subId < 2; subId++) {
          final long owner = slotKey(rg, kind);
          final long refKey = HOTLeafPage.overflowPageRefKey(owner, subId);
          final PageReference reference = new PageReference();
          reference.setPage(new OverflowPage(new byte[] {(byte) rg, (byte) kind, (byte) subId}));
          source.setPageReference(refKey, reference);
          expected.put(refKey, reference);
          ownerOf.put(refKey, owner);
        }
      }
    }
    assertEquals(rowGroups * 4, source.segmentRefCount(), "fixture: every segment attached");

    final long carriesBefore = HOTIncrementalInsert.SPLIT_SEGMENT_REF_CARRIES.get();
    final long routedBefore = HOTIncrementalInsert.SPLIT_SEGMENT_REFS_ROUTED.get();
    final byte[] newKey = keyBytes(slotKey(rowGroups + 1, 0));
    final BiNode result =
        HOTIncrementalInsert.splitLeafPage(source, newKey, VALUE, 1, IndexType.PATH, allocator::getAndIncrement);

    assertEquals(carriesBefore + 1, HOTIncrementalInsert.SPLIT_SEGMENT_REF_CARRIES.get(),
        "the ref-carrying split path must have run");
    assertEquals(routedBefore + expected.size(), HOTIncrementalInsert.SPLIT_SEGMENT_REFS_ROUTED.get(),
        "every reference must have been routed");

    final List<HOTLeafPage> leftLeaves = new ArrayList<>();
    final List<HOTLeafPage> rightLeaves = new ArrayList<>();
    collectLeaves(result.left().getPage(), leftLeaves);
    collectLeaves(result.right().getPage(), rightLeaves);
    final List<HOTLeafPage> allLeaves = new ArrayList<>(leftLeaves);
    allLeaves.addAll(rightLeaves);

    int seen = 0;
    for (final HOTLeafPage leaf : allLeaves) {
      for (final long refKey : leaf.overflowPageRefKeysSorted()) {
        seen++;
        final PageReference source0 = expected.get(refKey);
        assertNotNull(source0, "no reference with key " + refKey + " was attached to the source leaf");
        assertSame(source0, leaf.getPageReference(refKey),
            "the split must carry the reference instance, not a copy (refKey=" + refKey + ")");
        assertTrue(leaf.findEntry(keyBytes(ownerOf.get(refKey))) >= 0,
            "reference " + refKey + " landed on a leaf that does not hold its owning slot — a "
                + "reader navigating to that slot would not find its segment page");
      }
    }
    assertEquals(expected.size(), seen, "every reference must survive the split exactly once");

    // A blanket copy onto both halves would also pass a per-leaf residency check only if every
    // owner lived in both halves — it does not: the cut splits the row-group range, so each half
    // must own part of the side map.
    assertTrue(countRefs(leftLeaves) > 0 && countRefs(rightLeaves) > 0,
        "the split bit cuts the row-group range, so both halves must own references (left=" + countRefs(leftLeaves)
            + ", right=" + countRefs(rightLeaves) + ")");

    source.close();
    closeAll(allLeaves);
  }

  @Test
  @DisplayName("a half too large for one page keeps the references its inner leaves own")
  void segmentRefsSurviveAMultiPageHalf() {
    final AtomicLong allocator = new AtomicLong(1);
    final HOTLeafPage source = new HOTLeafPage(allocator.getAndIncrement(), 1, IndexType.PATH);

    // Fill the source to near its byte capacity with fat values, then split it with a value fat
    // enough that the half receiving it no longer fits one page: buildHalf then falls back to a
    // HOTBulkBuilder subtree, and the reference walk has to descend it.
    final byte[] fatValue = new byte[2000];
    final Map<Long, Long> ownerOf = new HashMap<>();
    long slots = 0;
    while (slots < HOTLeafPage.MAX_ENTRIES && source.canFit(keyBytes(slots), fatValue)
        && source.getRemainingSpace() > 2L * fatValue.length) {
      assertTrue(source.put(keyBytes(slots), fatValue), "source leaf must hold the slot");
      if (slots % 3 == 0) {
        final long refKey = HOTLeafPage.overflowPageRefKey(slots, 3);
        final PageReference reference = new PageReference();
        reference.setPage(new OverflowPage(new byte[] {(byte) slots}));
        source.setPageReference(refKey, reference);
        ownerOf.put(refKey, slots);
      }
      slots++;
    }
    assertTrue(slots > 8, "fixture: the source leaf must hold several slots, got " + slots);
    assertTrue(source.segmentRefCount() > 1, "fixture: references attached");

    final BiNode result = HOTIncrementalInsert.splitLeafPage(source, keyBytes(slots), new byte[40 * 1024], 1,
        IndexType.PATH, allocator::getAndIncrement);
    assertTrue(
        result.left().getPage() instanceof HOTIndirectPage || result.right().getPage() instanceof HOTIndirectPage,
        "fixture: one half must have spilled into a multi-page subtree");

    final List<HOTLeafPage> leaves = new ArrayList<>();
    collectLeaves(result.left().getPage(), leaves);
    collectLeaves(result.right().getPage(), leaves);
    int seen = 0;
    for (final HOTLeafPage leaf : leaves) {
      for (final long refKey : leaf.overflowPageRefKeysSorted()) {
        seen++;
        assertTrue(leaf.findEntry(keyBytes(ownerOf.get(refKey))) >= 0,
            "reference " + refKey + " landed on a leaf that does not hold its owning slot");
      }
    }
    assertEquals(ownerOf.size(), seen, "every reference must survive the split exactly once");

    source.close();
    closeAll(leaves);
  }

  private static int countRefs(final List<HOTLeafPage> leaves) {
    int total = 0;
    for (final HOTLeafPage leaf : leaves) {
      total += leaf.segmentRefCount();
    }
    return total;
  }

  private static void collectLeaves(final Page page, final List<HOTLeafPage> out) {
    if (page instanceof HOTLeafPage leaf) {
      out.add(leaf);
    } else if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        final PageReference reference = indirect.getChildReference(i);
        if (reference != null && reference.getPage() != null) {
          collectLeaves(reference.getPage(), out);
        }
      }
    }
  }

  private static void closeAll(final List<HOTLeafPage> leaves) {
    for (final HOTLeafPage leaf : leaves) {
      leaf.close();
    }
  }
}
