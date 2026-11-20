/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import io.sirix.cache.FrameSlotAllocator;
import io.sirix.index.IndexType;
import io.sirix.index.hot.page.HOTIndirectPage;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Leaf consolidation must hold the input leaves it copies or reads during recompression.
 *
 * <p>
 * {@link HOTIncrementalInsert#consolidateNodeLeaves} settles on a pair of leaves, allocates the
 * merged leaf and only then reads the pair. A leaf the page cache owns can be evicted in between:
 * allocating under pressure evicts whatever nobody guards. Its frame is released and handed out
 * again, and the copy then reads another page's bytes and stores them as entries of the merged
 * leaf, which the writer publishes.
 * </p>
 *
 * <p>
 * The tests evict all input leaves from the page-key allocator, which runs in that window. A
 * released frame keeps its bytes until it is reused, so a stale read is usually right by luck; the
 * tests remove the luck both ways. {@link FrameSlotAllocator#setPoisonOnReleaseForTesting}
 * zero-fills a released frame, and in the second mode four foreign leaves of the same layout take
 * the released frames at once, as the next page load would.
 * </p>
 */
@Isolated
@DisplayName("HOT leaf consolidation holds the leaves it copies")
final class HOTIncrementalInsertLeafRetentionTest {

  /** Same merge bound the index writer passes ({@code MAX_ENTRIES * 3 / 4}). */
  private static final int CONSOLIDATION_TARGET = (HOTLeafPage.MAX_ENTRIES * 3) / 4;

  private static final byte[] VALUE = {1, 2, 3};

  @BeforeEach
  void poisonReleasedFrames() {
    FrameSlotAllocator.setPoisonOnReleaseForTesting(true);
  }

  @AfterEach
  void restore() {
    FrameSlotAllocator.setPoisonOnReleaseForTesting(false);
  }

  @ParameterizedTest(name = "caller-owned retention scratch: {0}")
  @ValueSource(booleans = {false, true})
  @DisplayName("full-fanout repeated consolidation retains only original leaves and releases every frame")
  void fullFanoutConsolidationDoesNotRetainPrivateReplacements(final boolean callerOwnedScratch) {
    final FrameSlotAllocator frames = FrameSlotAllocator.getInstance();
    final int frameClass = FrameSlotAllocator.indexForSize(HOTLeafPage.DEFAULT_SIZE);
    final int fanout = HOTIndirectPage.MAX_NODE_ENTRIES;
    final AtomicLong pageKeys = new AtomicLong(1);
    final HOTLeafPage[] originals = new HOTLeafPage[fanout];
    final PageReference[] children = new PageReference[fanout];
    final int[] partials = new int[fanout];
    for (int i = 0; i < fanout; i++) {
      originals[i] = unitLeaf(pageKeys, i << 3, 1);
      children[i] = swizzle(originals[i]);
      partials[i] = i;
    }
    final HOTIndirectPage parent = HOTBulkBuilder.assembleIndirect(new int[] {0, 1, 2, 3, 4}, partials, children, 1, 1,
        IndexType.PATH, pageKeys::getAndIncrement);
    final int liveWithOriginals = frames.liveSlotCount(frameClass);
    final HOTLeafPage[] scratch = new HOTLeafPage[fanout];
    final List<PageReference> dropped = new ArrayList<>();
    HOTIndirectPage consolidated = null;
    try {
      consolidated = callerOwnedScratch
          ? HOTIncrementalInsert.consolidateNodeLeaves(parent, CONSOLIDATION_TARGET, 2, IndexType.PATH,
              pageKeys::getAndIncrement, dropped, scratch)
          : HOTIncrementalInsert.consolidateNodeLeaves(parent, CONSOLIDATION_TARGET, 2, IndexType.PATH,
              pageKeys::getAndIncrement, dropped);

      assertNotSame(parent, consolidated, "mergeable full-fanout input must consolidate");
      assertEquals(2, consolidated.getNumChildren(), "consolidation stops before collapsing the parent");
      assertEquals(1, consolidated.getHeight());
      for (int half = 0; half < 2; half++) {
        final HOTLeafPage replacement =
            assertInstanceOf(HOTLeafPage.class, consolidated.getChildReference(half).getPage());
        assertEquals(fanout / 2, replacement.getEntryCount());
        assertEquals(0, replacement.getGuardCount(), "private replacements must not retain guards");
        for (int entry = 0; entry < fanout / 2; entry++) {
          final int firstByte = (half * (fanout / 2) + entry) << 3;
          assertArrayEquals(unitKey(firstByte, 0), replacement.getKey(entry));
          assertArrayEquals(unitValue(firstByte, 0), replacement.copyStoredValue(entry));
        }
      }
      assertEquals(fanout, dropped.size(), "only original leaves are handed to the caller for retirement");
      assertEquals(fanout, parent.getNumChildren(), "consolidation must preserve its input");
      for (int i = 0; i < fanout; i++) {
        assertSame(children[i], parent.getChildReference(i));
        assertTrue(dropped.contains(children[i]), "every original leaf must be handed to retirement");
        assertEquals(0, originals[i].getGuardCount(), "every original guard must be released");
        assertArrayEquals(unitKey(i << 3, 0), originals[i].getKey(0));
        assertNull(scratch[i], "reusable retention slots must be cleared");
      }
      assertEquals(liveWithOriginals + 2, frames.liveSlotCount(frameClass),
          "consumed private replacements must release their frames without deferred guards");
    } finally {
      closeLeaves(parent, consolidated);
    }
    assertEquals(liveWithOriginals - fanout, frames.liveSlotCount(frameClass),
        "no original or replacement frame may leak");
  }

  @ParameterizedTest(name = "released frames recycled into foreign leaves: {0}")
  @ValueSource(booleans = {false, true})
  @DisplayName("consolidation copies only from leaves it holds, even when allocating the target evicts them")
  void consolidationCopiesOnlyFromLeavesItHolds(final boolean recycle) {
    final AtomicLong pageKeys = new AtomicLong(1);
    final HOTLeafPage left = unitLeaf(pageKeys, 0x00, 6);
    final HOTLeafPage right = unitLeaf(pageKeys, 0x20, 4);
    final HOTLeafPage third = unitLeaf(pageKeys, 0x40, 2);
    final HOTLeafPage fourth = unitLeaf(pageKeys, 0x80, 2);
    // Bits 0..2 of the first key byte; partials 0 and 1 are BiNode-paired, so the first two merge.
    final HOTIndirectPage parent = HOTBulkBuilder.assembleIndirect(new int[] {0, 1, 2}, new int[] {0, 1, 2, 4},
        new PageReference[] {swizzle(left), swizzle(right), swizzle(third), swizzle(fourth)}, 1, 1, IndexType.PATH,
        pageKeys::getAndIncrement);
    final List<HOTLeafPage> foreign = new ArrayList<>();
    // The surviving children are this caller's to hold, so it holds them; the contract on
    // consolidateNodeLeaves says why. The pair is consolidation's own business, and closing it
    // under the allocation is what this test is about.
    assertTrue(third.acquireGuard());
    assertTrue(fourth.acquireGuard());
    final LongSupplier evictingAllocator = () -> {
      if (foreign.isEmpty()) {
        left.close();
        right.close();
        third.close();
        fourth.close();
        foreign.add(unitLeaf(pageKeys, 0x60, recycle
            ? 6
            : 0));
        foreign.add(unitLeaf(pageKeys, 0x70, recycle
            ? 6
            : 0));
        foreign.add(unitLeaf(pageKeys, 0xa0, recycle
            ? 6
            : 0));
        foreign.add(unitLeaf(pageKeys, 0xb0, recycle
            ? 6
            : 0));
      }
      return pageKeys.getAndIncrement();
    };
    final List<PageReference> dropped = new ArrayList<>();
    HOTIndirectPage consolidated = null;
    try {
      consolidated = HOTIncrementalInsert.consolidateNodeLeaves(parent, CONSOLIDATION_TARGET, 2, IndexType.PATH,
          evictingAllocator, dropped);

      assertFalse(foreign.isEmpty(), "input eviction must run while the merge is under way");
      // One merge, and only one. The allocation asked for every input to be retired; the pair was
      // held across its copy, so its entries are its own bytes rather than the foreign leaves that
      // took the frames. The third leaf is a different matter: its retirement has been requested, so
      // the next pass cannot take a guard on it and leaves it alone instead of copying from a leaf
      // that is going away. A cascade would have been wrong here.
      assertEquals(3, consolidated.getNumChildren(), "the BiNode-paired pair must have merged, and nothing else");
      final HOTLeafPage merged = assertInstanceOf(HOTLeafPage.class, consolidated.getChildReference(0).getPage());
      assertEquals(10, merged.getEntryCount(), "the merged leaf holds every entry of the pair");
      for (int i = 0; i < 6; i++) {
        assertArrayEquals(unitKey(0x00, i), merged.getKey(i), "left key " + i);
        assertArrayEquals(unitValue(0x00, i), merged.copyStoredValue(i), "left value " + i);
      }
      for (int i = 0; i < 4; i++) {
        assertArrayEquals(unitKey(0x20, i), merged.getKey(6 + i), "right key " + i);
        assertArrayEquals(unitValue(0x20, i), merged.copyStoredValue(6 + i), "right value " + i);
      }
      assertEquals(2, dropped.size(), "the pair it merged away is what goes to retirement");
      assertTrue(left.isClosed() && right.isClosed(),
          "the deferred retirement of the pair completes once consolidation releases it");
      assertEquals(0, left.getGuardCount(), "consolidation must release every guard it takes");
      assertEquals(0, right.getGuardCount(), "consolidation must release every guard it takes");
      assertEquals(1, third.getGuardCount(), "the surviving children stay the caller's, untouched");
      assertEquals(1, fourth.getGuardCount(), "the surviving children stay the caller's, untouched");
    } finally {
      third.releaseGuard();
      fourth.releaseGuard();
      closeLeaves(parent, consolidated);
      closeLeaf(left);
      closeLeaf(right);
      closeLeaf(third);
      closeLeaf(fourth);
      foreign.forEach(HOTIncrementalInsertLeafRetentionTest::closeLeaf);
    }
  }

  @ParameterizedTest(name = "child: {0}, deferred retirement: {1}, closed before resolution: {2}")
  @CsvSource({"0, false, false", "1, false, false", "0, true, false", "1, true, false", "0, false, true",
      "1, false, true"})
  @DisplayName("consolidation declines without allocating when a leaf of the pair cannot be held")
  void consolidationDeclinesWhenAnyChildCannotBeHeld(final int evictedChild, final boolean deferredRetirement,
      final boolean closedBeforeResolution) {
    final AtomicLong pageKeys = new AtomicLong(1);
    final HOTLeafPage left = unitLeaf(pageKeys, 0x00, 6);
    final HOTLeafPage right = unitLeaf(pageKeys, 0x20, 4);
    final HOTLeafPage third = unitLeaf(pageKeys, 0x40, 2);
    final HOTLeafPage fourth = unitLeaf(pageKeys, 0x80, 2);
    final HOTIndirectPage parent = HOTBulkBuilder.assembleIndirect(new int[] {0, 1, 2}, new int[] {0, 1, 2, 4},
        new PageReference[] {swizzle(left), swizzle(right), swizzle(third), swizzle(fourth)}, 1, 1, IndexType.PATH,
        pageKeys::getAndIncrement);
    final HOTLeafPage[] leaves = {left, right, third, fourth};
    final HOTLeafPage evicted = leaves[evictedChild];
    if (deferredRetirement) {
      assertTrue(evicted.acquireGuard());
    }
    if (closedBeforeResolution) {
      evicted.close();
    } else {
      final PageReference evictingRef = spy(parent.getChildReference(evictedChild));
      doAnswer(invocation -> {
        final Page captured = (Page) invocation.callRealMethod();
        evicted.close();
        return captured;
      }).when(evictingRef).getPage();
      parent.setChildReference(evictedChild, evictingRef);
    }
    final PageReference[] originalChildren = {parent.getChildReference(0), parent.getChildReference(1),
        parent.getChildReference(2), parent.getChildReference(3)};
    final long allocatedBefore = pageKeys.get();
    final List<PageReference> dropped = new ArrayList<>();
    try {
      final HOTIndirectPage consolidated = HOTIncrementalInsert.consolidateNodeLeaves(parent, CONSOLIDATION_TARGET, 2,
          IndexType.PATH, pageKeys::getAndIncrement, dropped);

      assertSame(parent, consolidated, "a pair that cannot be held is not merged");
      assertEquals(4, consolidated.getNumChildren(), "all input children must remain in the parent");
      for (int i = 0; i < originalChildren.length; i++) {
        assertSame(originalChildren[i], consolidated.getChildReference(i), "child " + i + " must remain unchanged");
      }
      assertEquals(!deferredRetirement, evicted.isClosed(), "retirement must occur during child resolution");
      assertTrue(dropped.isEmpty(), "nothing merged, so nothing is handed to retirement");
      assertEquals(allocatedBefore, pageKeys.get(), "no merge target is allocated for a pair that cannot be held");
      for (int i = 0; i < leaves.length; i++) {
        assertEquals(deferredRetirement && i == evictedChild
            ? 1
            : 0, leaves[i].getGuardCount(), "only this test may retain a guard on child " + i);
      }
    } finally {
      if (deferredRetirement) {
        evicted.releaseGuard();
      }
      closeLeaves(parent);
    }
  }

  @ParameterizedTest(name = "failing allocation: {0}, failing retirement: {1}")
  @CsvSource({"1, false", "2, false", "3, false", "4, false", "1, true", "2, true", "3, true", "4, true"})
  @DisplayName("consolidation releases all input guards and speculative leaves after allocation failure")
  void consolidationReleasesEveryChildOnFailure(final int failingAllocation, final boolean failingRetirement) {
    try (final Arena arena = Arena.ofConfined()) {
      final FrameSlotAllocator frames = FrameSlotAllocator.getInstance();
      final int frameClass = FrameSlotAllocator.indexForSize(HOTLeafPage.DEFAULT_SIZE);
      final int liveBefore = frames.liveSlotCount(frameClass);
      final AtomicLong pageKeys = new AtomicLong(1);
      final IllegalStateException retirementFailure = new IllegalStateException("retirement failed");
      // The retirement that fails belongs to the pair, because this test is about consolidation's own
      // cleanup: the pair is what consolidation holds, so the pair's deferred retirement is what its
      // release completes. A survivor is the caller's, and its retirement would run in the caller.
      final HOTLeafPage second = new HOTLeafPage(pageKeys.getAndIncrement(), 1, IndexType.PATH,
          arena.allocate(HOTLeafPage.DEFAULT_SIZE), () -> {
            if (failingRetirement) {
              throw retirementFailure;
            }
          }, new int[HOTLeafPage.MAX_ENTRIES], 0, 0);
      for (int i = 0; i < 4; i++) {
        assertTrue(second.put(unitKey(0x20, i), unitValue(0x20, i)));
      }
      final HOTLeafPage[] leaves =
          {unitLeaf(pageKeys, 0x00, 6), second, unitLeaf(pageKeys, 0x40, 2), unitLeaf(pageKeys, 0x80, 2)};
      final HOTLeafPage[] pair = {leaves[0], leaves[1]};
      final HOTLeafPage[] survivors = {leaves[2], leaves[3]};
      final HOTIndirectPage parent = HOTBulkBuilder.assembleIndirect(new int[] {0, 1, 2}, new int[] {0, 1, 2, 4},
          new PageReference[] {swizzle(leaves[0]), swizzle(leaves[1]), swizzle(leaves[2]), swizzle(leaves[3])}, 1, 1,
          IndexType.PATH, pageKeys::getAndIncrement);
      for (final HOTLeafPage survivor : survivors) {
        assertTrue(survivor.acquireGuard(), "this caller holds the surviving children, as the contract requires");
      }
      final AtomicInteger allocations = new AtomicInteger();
      final IllegalStateException failure = new IllegalStateException("allocation failed");
      final LongSupplier failingAllocator = () -> {
        if (allocations.incrementAndGet() == failingAllocation) {
          for (final HOTLeafPage leaf : leaves) {
            leaf.close();
          }
          throw failure;
        }
        return pageKeys.getAndIncrement();
      };
      final List<PageReference> dropped = new ArrayList<>();
      try {
        assertSame(failure,
            assertThrows(IllegalStateException.class, () -> HOTIncrementalInsert.consolidateNodeLeaves(parent,
                CONSOLIDATION_TARGET, 2, IndexType.PATH, failingAllocator, dropped)));
        assertEquals(failingAllocation, allocations.get());
        assertEquals(failingRetirement
            ? 1
            : 0, failure.getSuppressed().length);
        if (failingRetirement) {
          assertSame(retirementFailure, failure.getSuppressed()[0], "retirement must preserve the allocation failure");
        }
        assertTrue(dropped.isEmpty(), "failed consolidation must not publish retirement requests");
        for (final HOTLeafPage leaf : pair) {
          assertEquals(0, leaf.getGuardCount(), "exception cleanup must release every guard consolidation took");
          assertTrue(leaf.isClosed(), "exception cleanup must finish the deferred retirement of what it held");
        }
        for (final HOTLeafPage survivor : survivors) {
          assertEquals(1, survivor.getGuardCount(), "consolidation must neither take nor drop the caller's guard");
        }
      } finally {
        for (final HOTLeafPage survivor : survivors) {
          survivor.releaseGuard();
        }
        closeLeaves(parent);
        for (final HOTLeafPage leaf : leaves) {
          closeLeaf(leaf);
        }
      }
      for (final HOTLeafPage survivor : survivors) {
        assertTrue(survivor.isClosed(), "the caller's release must finish its own deferred retirement");
      }
      assertEquals(liveBefore, frames.liveSlotCount(frameClass), "no speculative or input frame may remain live");
    }
  }

  @ParameterizedTest(name = "side reference on child (-1 means count refusal): {0}")
  @ValueSource(ints = {-1, 0, 1})
  @DisplayName("ineligible pairs release both guards without allocating or retiring leaves")
  void consolidationReleasesGuardsForIneligiblePairs(final int sideReferenceChild) {
    final AtomicLong pageKeys = new AtomicLong(1);
    final HOTLeafPage left = unitLeaf(pageKeys, 0x00, 6);
    final HOTLeafPage right = unitLeaf(pageKeys, 0x20, 4);
    final HOTLeafPage third = unitLeaf(pageKeys, 0x40, 2);
    final HOTLeafPage fourth = unitLeaf(pageKeys, 0x80, 2);
    final HOTIndirectPage parent = HOTBulkBuilder.assembleIndirect(new int[] {0, 1, 2}, new int[] {0, 1, 2, 4},
        new PageReference[] {swizzle(left), swizzle(right), swizzle(third), swizzle(fourth)}, 1, 1, IndexType.PATH,
        pageKeys::getAndIncrement);
    if (sideReferenceChild >= 0) {
      final HOTLeafPage referencedLeaf = sideReferenceChild == 0
          ? left
          : right;
      referencedLeaf.setPageReference(123L, new PageReference());
    }
    final long allocatedBefore = pageKeys.get();
    final List<PageReference> dropped = new ArrayList<>();
    try {
      final HOTIndirectPage consolidated = HOTIncrementalInsert.consolidateNodeLeaves(parent, sideReferenceChild < 0
          ? 9
          : CONSOLIDATION_TARGET, 2, IndexType.PATH, pageKeys::getAndIncrement, dropped);

      assertSame(parent, consolidated, "entry-count and side-reference exclusions must preserve the parent");
      assertTrue(dropped.isEmpty(), "ineligible leaves must not be retired");
      assertEquals(allocatedBefore, pageKeys.get(), "an ineligible pair needs no replacement");
      assertEquals(0, left.getGuardCount(), "left eligibility guard must be released");
      assertEquals(0, right.getGuardCount(), "right eligibility guard must be released");
      assertEquals(0, third.getGuardCount(), "third child guard must be released");
      assertEquals(0, fourth.getGuardCount(), "fourth child guard must be released");
      assertArrayEquals(unitKey(0x00, 0), left.getKey(0));
      assertArrayEquals(unitKey(0x20, 0), right.getKey(0));
    } finally {
      closeLeaves(parent);
    }
  }

  // ===== fixtures =====

  /** {@code entries} keys sharing the one-byte prefix {@code firstByte} and differing behind it. */
  @Test
  @DisplayName("a standalone caller that does not hold a surviving child is told so, not left to guess")
  void consolidationAssertsItsCallerHoldsTheSurvivingChildren() {
    final AtomicLong pageKeys = new AtomicLong(1);
    final HOTLeafPage left = unitLeaf(pageKeys, 0x00, 6);
    final HOTLeafPage right = unitLeaf(pageKeys, 0x20, 4);
    final HOTLeafPage third = unitLeaf(pageKeys, 0x40, 2);
    final HOTLeafPage fourth = unitLeaf(pageKeys, 0x80, 2);
    // A reference with a durable key and no transaction-log entry is one the page cache owns and can
    // evict, which is exactly the child the contract says a caller must hold. This one is unheld.
    final PageReference cacheBackedThird = swizzle(third).setKey(4242L);
    final HOTIndirectPage parent = HOTBulkBuilder.assembleIndirect(new int[] {0, 1, 2}, new int[] {0, 1, 2, 4},
        new PageReference[] {swizzle(left), swizzle(right), cacheBackedThird, swizzle(fourth)}, 1, 1, IndexType.PATH,
        pageKeys::getAndIncrement);
    final List<PageReference> dropped = new ArrayList<>();
    try {
      final AssertionError broken = assertThrows(AssertionError.class,
          () -> HOTIncrementalInsert.consolidateNodeLeaves(parent, CONSOLIDATION_TARGET, 2, IndexType.PATH,
              pageKeys::getAndIncrement, dropped),
          "the precondition must be stated to the caller that breaks it, not discovered as a height failure");

      assertTrue(broken.getMessage().contains("hold every"),
          "the message must say what the caller owes: " + broken.getMessage());
      assertEquals(0, left.getGuardCount(), "the pair's guards must not leak when the assertion fires");
      assertEquals(0, right.getGuardCount(), "the pair's guards must not leak when the assertion fires");
    } finally {
      closeLeaves(parent);
    }
  }

  @Test
  @DisplayName("a standalone caller that holds the surviving children consolidates normally")
  void consolidationAcceptsACallerThatHoldsTheSurvivingChildren() {
    final AtomicLong pageKeys = new AtomicLong(1);
    final HOTLeafPage left = unitLeaf(pageKeys, 0x00, 6);
    final HOTLeafPage right = unitLeaf(pageKeys, 0x20, 4);
    final HOTLeafPage third = unitLeaf(pageKeys, 0x40, 2);
    final HOTLeafPage fourth = unitLeaf(pageKeys, 0x80, 2);
    final PageReference cacheBackedThird = swizzle(third).setKey(4242L);
    final HOTIndirectPage parent = HOTBulkBuilder.assembleIndirect(new int[] {0, 1, 2}, new int[] {0, 1, 2, 4},
        new PageReference[] {swizzle(left), swizzle(right), cacheBackedThird, swizzle(fourth)}, 1, 1, IndexType.PATH,
        pageKeys::getAndIncrement);
    final List<PageReference> dropped = new ArrayList<>();
    assertTrue(third.acquireGuard(), "this caller holds what the contract says it must");
    try {
      final HOTIndirectPage consolidated = HOTIncrementalInsert.consolidateNodeLeaves(parent, CONSOLIDATION_TARGET, 2,
          IndexType.PATH, pageKeys::getAndIncrement, dropped);

      assertNotSame(parent, consolidated, "the eligible pair must have merged");
      // Both adjacent pairs are mergeable and the loop runs while three children remain, so four
      // children become two.
      assertEquals(2, consolidated.getNumChildren(), "every mergeable pair of this node must merge");
      assertEquals(1, third.getGuardCount(), "consolidation must not take or drop the caller's guard");
    } finally {
      third.releaseGuard();
      closeLeaves(parent);
    }
  }

  /**
   * The cases come from the documented precondition's own classes, not from the shape of the
   * predicate that implements it. The contract names what the caller owes -- "at least every child
   * the page cache can evict: a child with a durable key and no transaction-log entry" -- and then
   * names what needs no guard: "a leaf the transaction log serves, and a leaf the writer built and
   * has not published". With the pair, which the method holds itself, that is four classes, and one
   * case each: the cache-backed survivor the caller must hold, the log-served child, the
   * never-published child, and the pair in either slot.
   */
  @ParameterizedTest(name = "unheld child: key {0}, log key {1}, slot {2} -> assertion fires: {3}")
  @CsvSource({"4242, -1, 2, true", "4242, 0, 2, false", "-1, -1, 2, false", "4242, -1, 1, false", "4242, -1, 0, false"})
  @DisplayName("the precondition names exactly the children the page cache can take away")
  void thePreconditionCoversExactlyTheCacheBackedSurvivors(final long durableKey, final int logKey, final int slot,
      final boolean expectAssertion) {
    final AtomicLong pageKeys = new AtomicLong(1);
    final HOTLeafPage[] leaves = {unitLeaf(pageKeys, 0x00, 6), unitLeaf(pageKeys, 0x20, 4), unitLeaf(pageKeys, 0x40, 2),
        unitLeaf(pageKeys, 0x80, 2)};
    final PageReference[] children = new PageReference[4];
    for (int i = 0; i < 4; i++) {
      children[i] = swizzle(leaves[i]);
    }
    children[slot].setKey(durableKey);
    if (logKey >= 0) {
      children[slot].setLogKey(logKey);
    }
    final HOTIndirectPage parent = HOTBulkBuilder.assembleIndirect(new int[] {0, 1, 2}, new int[] {0, 1, 2, 4},
        children, 1, 1, IndexType.PATH, pageKeys::getAndIncrement);
    final List<PageReference> dropped = new ArrayList<>();
    try {
      if (expectAssertion) {
        assertThrows(AssertionError.class,
            () -> HOTIncrementalInsert.consolidateNodeLeaves(parent, CONSOLIDATION_TARGET, 2, IndexType.PATH,
                pageKeys::getAndIncrement, dropped),
            "an unheld child the cache can evict must be reported to the caller");
      } else {
        final HOTIndirectPage consolidated = HOTIncrementalInsert.consolidateNodeLeaves(parent, CONSOLIDATION_TARGET, 2,
            IndexType.PATH, pageKeys::getAndIncrement, dropped);
        assertNotSame(parent, consolidated, "this child needs no guard, so the merge must proceed");
      }
    } finally {
      closeLeaves(parent);
      for (final HOTLeafPage leaf : leaves) {
        closeLeaf(leaf);
      }
    }
  }

  @Test
  @DisplayName("a retention scratch too small for the fanout is refused before anything is held")
  void consolidationRefusesAScratchSmallerThanTheFanout() {
    final AtomicLong pageKeys = new AtomicLong(1);
    final HOTLeafPage left = unitLeaf(pageKeys, 0x00, 6);
    final HOTLeafPage right = unitLeaf(pageKeys, 0x20, 4);
    final HOTLeafPage third = unitLeaf(pageKeys, 0x40, 2);
    final HOTIndirectPage parent = HOTBulkBuilder.assembleIndirect(new int[] {0, 1}, new int[] {0, 1, 2},
        new PageReference[] {swizzle(left), swizzle(right), swizzle(third)}, 1, 1, IndexType.PATH,
        pageKeys::getAndIncrement);
    final List<PageReference> dropped = new ArrayList<>();
    try {
      final IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
          () -> HOTIncrementalInsert.consolidateNodeLeaves(parent, CONSOLIDATION_TARGET, 2, IndexType.PATH,
              pageKeys::getAndIncrement, dropped, new HOTLeafPage[HOTIndirectPage.MAX_NODE_ENTRIES - 1]),
          "a scratch that cannot hold a full node's children is a programming error, not a decline");

      assertTrue(refused.getMessage().contains(String.valueOf(HOTIndirectPage.MAX_NODE_ENTRIES)),
          "the message must say what the scratch has to hold: " + refused.getMessage());
      assertEquals(0, left.getGuardCount(), "nothing may be held when the arguments are refused");
      assertEquals(0, right.getGuardCount(), "nothing may be held when the arguments are refused");
      assertTrue(dropped.isEmpty(), "a refused call must not retire anything");
    } finally {
      closeLeaves(parent);
      closeLeaf(left);
      closeLeaf(right);
      closeLeaf(third);
    }
  }

  private static HOTLeafPage unitLeaf(final AtomicLong pageKeys, final int firstByte, final int entries) {
    final HOTLeafPage leaf = new HOTLeafPage(pageKeys.getAndIncrement(), 1, IndexType.PATH);
    for (int i = 0; i < entries; i++) {
      assertTrue(leaf.put(unitKey(firstByte, i), unitValue(firstByte, i)), "the fixture leaf must take every key");
    }
    return leaf;
  }

  private static byte[] unitKey(final int firstByte, final int ordinal) {
    return new byte[] {(byte) firstByte, 0x55, (byte) (ordinal + 1)};
  }

  private static byte[] unitValue(final int firstByte, final int ordinal) {
    return new byte[] {VALUE[0], VALUE[1], VALUE[2], (byte) firstByte, (byte) ordinal};
  }

  private static PageReference swizzle(final HOTLeafPage leaf) {
    final PageReference reference = new PageReference();
    reference.setPage(leaf);
    return reference;
  }


  private static void closeLeaves(final HOTIndirectPage... nodes) {
    for (final HOTIndirectPage node : nodes) {
      if (node == null) {
        continue;
      }
      for (int i = 0; i < node.getNumChildren(); i++) {
        if (node.getChildReference(i).getPage() instanceof HOTLeafPage leaf) {
          closeLeaf(leaf);
        }
      }
    }
  }

  private static void closeLeaf(final HOTLeafPage leaf) {
    if (!leaf.isClosed()) {
      leaf.close();
    }
  }
}
