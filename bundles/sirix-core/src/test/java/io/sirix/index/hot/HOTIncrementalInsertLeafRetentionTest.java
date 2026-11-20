/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
      // The merged leaf then absorbs the third leaf as well; the node keeps its fourth child.
      assertEquals(2, consolidated.getNumChildren(), "the BiNode-paired leaves must have merged");
      final HOTLeafPage merged = assertInstanceOf(HOTLeafPage.class, consolidated.getChildReference(0).getPage());
      assertEquals(12, merged.getEntryCount(), "the merged leaf holds every entry of the three siblings");
      for (int i = 0; i < 6; i++) {
        assertArrayEquals(unitKey(0x00, i), merged.getKey(i), "left key " + i);
        assertArrayEquals(unitValue(0x00, i), merged.copyStoredValue(i), "left value " + i);
      }
      for (int i = 0; i < 4; i++) {
        assertArrayEquals(unitKey(0x20, i), merged.getKey(6 + i), "right key " + i);
        assertArrayEquals(unitValue(0x20, i), merged.copyStoredValue(6 + i), "right value " + i);
      }
      for (int i = 0; i < 2; i++) {
        assertArrayEquals(unitKey(0x40, i), merged.getKey(10 + i), "third key " + i);
        assertArrayEquals(unitValue(0x40, i), merged.copyStoredValue(10 + i), "third value " + i);
      }
      assertEquals(3, dropped.size(), "every merged-away leaf is handed to retirement");
      assertTrue(left.isClosed() && right.isClosed() && third.isClosed() && fourth.isClosed(),
          "deferred eviction completes only after copying and recompression finish");
      assertEquals(0, left.getGuardCount(), "consolidation must release every guard it takes");
      assertEquals(0, right.getGuardCount(), "consolidation must release every guard it takes");
      assertEquals(0, third.getGuardCount(), "consolidation must release every guard it takes");
      assertEquals(0, fourth.getGuardCount(), "consolidation must release every guard it takes");
    } finally {
      closeLeaves(parent, consolidated);
      closeLeaf(left);
      closeLeaf(right);
      closeLeaf(third);
      closeLeaf(fourth);
      foreign.forEach(HOTIncrementalInsertLeafRetentionTest::closeLeaf);
    }
  }

  @ParameterizedTest(name = "child: {0}, deferred retirement: {1}, closed before resolution: {2}")
  @CsvSource({"0, false, false", "1, false, false", "2, false, false", "3, false, false", "0, true, false",
      "1, true, false", "2, true, false", "3, true, false", "0, false, true", "1, false, true", "2, false, true",
      "3, false, true"})
  @DisplayName("consolidation declines without allocating when any child cannot be held")
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
      final HOTLeafPage fourth = new HOTLeafPage(pageKeys.getAndIncrement(), 1, IndexType.PATH,
          arena.allocate(HOTLeafPage.DEFAULT_SIZE), () -> {
            if (failingRetirement) {
              throw retirementFailure;
            }
          }, new int[HOTLeafPage.MAX_ENTRIES], 0, 0);
      for (int i = 0; i < 2; i++) {
        assertTrue(fourth.put(unitKey(0x80, i), unitValue(0x80, i)));
      }
      final HOTLeafPage[] leaves =
          {unitLeaf(pageKeys, 0x00, 6), unitLeaf(pageKeys, 0x20, 4), unitLeaf(pageKeys, 0x40, 2), fourth};
      final HOTIndirectPage parent = HOTBulkBuilder.assembleIndirect(new int[] {0, 1, 2}, new int[] {0, 1, 2, 4},
          new PageReference[] {swizzle(leaves[0]), swizzle(leaves[1]), swizzle(leaves[2]), swizzle(leaves[3])}, 1, 1,
          IndexType.PATH, pageKeys::getAndIncrement);
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
        for (final HOTLeafPage leaf : leaves) {
          assertEquals(0, leaf.getGuardCount(), "exception cleanup must release every input guard");
          assertTrue(leaf.isClosed(), "exception cleanup must finish deferred input retirement");
        }
        assertEquals(liveBefore, frames.liveSlotCount(frameClass), "no speculative or input frame may remain live");
      } finally {
        closeLeaves(parent);
        for (final HOTLeafPage leaf : leaves) {
          closeLeaf(leaf);
        }
      }
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

  @Test
  @DisplayName("a survivor that goes away as it is held defers the merge instead of failing")
  void consolidationDeclinesWhenASurvivorGoesAwayAfterThePairIsEligible() {
    final AtomicLong pageKeys = new AtomicLong(1);
    final HOTLeafPage left = unitLeaf(pageKeys, 0x00, 6);
    final HOTLeafPage right = unitLeaf(pageKeys, 0x20, 4);
    final HOTLeafPage third = unitLeaf(pageKeys, 0x40, 2);
    final HOTLeafPage fourth = unitLeaf(pageKeys, 0x80, 2);
    final HOTIndirectPage parent = HOTBulkBuilder.assembleIndirect(new int[] {0, 1, 2}, new int[] {0, 1, 2, 4},
        new PageReference[] {swizzle(left), swizzle(right), swizzle(third), swizzle(fourth)}, 1, 1, IndexType.PATH,
        pageKeys::getAndIncrement);
    // The pair 0x00/0x20 is eligible, so the survivors are resolved and held next. This spy retires
    // the first survivor at exactly that resolution -- the one window this ordering has, after the
    // pair is held and its eligibility read, and before anything is allocated or recompressed.
    final PageReference retiringRef = spy(parent.getChildReference(2));
    doAnswer(invocation -> {
      final Page captured = (Page) invocation.callRealMethod();
      third.close();
      return captured;
    }).when(retiringRef).getPage();
    parent.setChildReference(2, retiringRef);
    final PageReference[] originalChildren = {parent.getChildReference(0), parent.getChildReference(1),
        parent.getChildReference(2), parent.getChildReference(3)};
    final long allocatedBefore = pageKeys.get();
    final long declinedBefore = HOTIncrementalInsert.CONSOLIDATION_SURVIVOR_UNAVAILABLE.get();
    final List<PageReference> dropped = new ArrayList<>();
    try {
      final HOTIndirectPage consolidated = HOTIncrementalInsert.consolidateNodeLeaves(parent, CONSOLIDATION_TARGET, 2,
          IndexType.PATH, pageKeys::getAndIncrement, dropped);

      assertSame(parent, consolidated, "a merge whose survivor went away must leave the node alone");
      assertEquals(declinedBefore + 1, HOTIncrementalInsert.CONSOLIDATION_SURVIVOR_UNAVAILABLE.get(),
          "the decline must be the survivor path, not an earlier refusal");
      assertEquals(allocatedBefore, pageKeys.get(), "a deferred merge must allocate no target");
      assertEquals(4, consolidated.getNumChildren(), "every input child must remain in the parent");
      for (int i = 0; i < originalChildren.length; i++) {
        assertSame(originalChildren[i], consolidated.getChildReference(i), "child " + i + " must remain unchanged");
      }
      assertTrue(dropped.isEmpty(), "nothing merged, so nothing is handed to retirement");
      assertTrue(third.isClosed(), "the survivor this test retired must really be gone");
      assertEquals(0, left.getGuardCount(), "the pair's left guard must be given back");
      assertEquals(0, right.getGuardCount(), "the pair's right guard must be given back");
      assertEquals(0, fourth.getGuardCount(), "a survivor that was held must be given back");
    } finally {
      closeLeaves(parent);
    }
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
