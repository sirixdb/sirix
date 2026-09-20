/*
 * [New BSD License]
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.cache.FrameSlotAllocator;
import io.sirix.index.IndexType;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Leaf consolidation when pouring the right sibling into the merged leaf shortens its common
 * prefix.
 *
 * <p>
 * {@link HOTIncrementalInsert#consolidateNodeLeaves} gates a merge on the pair's entry
 * <em>count</em> and leaves the byte budget to {@link HOTLeafPage#put}. The left sibling's keys
 * share a long prefix, so they are stored with tiny suffixes; the right sibling's first key then
 * shortens that prefix and every entry already poured in grows at once. With a few dozen large
 * posting values that growth exceeds the 64 KiB frame. This is the path of the valid-time interval
 * index failure:
 * {@code consolidateNodeLeaves -> put -> handlePrefixForInsert -> rebuildForShorterPrefix}.
 * </p>
 */
final class HOTConsolidationPrefixShrinkTest {

  /** Same merge bound the index writer passes ({@code MAX_ENTRIES * 3 / 4}). */
  private static final int CONSOLIDATION_TARGET = (HOTLeafPage.MAX_ENTRIES * 3) / 4;

  /** Bytes every heavy key shares; the right sibling's key shares none of them. */
  private static final int SHARED_PREFIX_LEN = 40;

  /** Posting-value size of the production failure ({@code 2 + valueLen == 1165}). */
  private static final int VALUE_LEN = 1163;

  /** {@code [u16 suffixLen][2-byte suffix][u16 valueLen][value]}. */
  private static final int HEAVY_ENTRY_SIZE = 2 + 2 + 2 + VALUE_LEN;

  /** As many heavy entries as one frame holds: 56, leaving 72 bytes. */
  private static final int FULL_LEAF_ENTRIES = HOTLeafPage.DEFAULT_SIZE / HEAVY_ENTRY_SIZE;

  @Test
  @DisplayName("a pair whose merged prefix rebuild overflows is left unmerged instead of failing")
  void consolidationSkipsPairWhosePrefixRebuildOverflows() {
    final AtomicLong allocator = new AtomicLong(1);
    final FrameSlotAllocator frameAllocator = FrameSlotAllocator.getInstance();
    final int frameClass = FrameSlotAllocator.indexForSize(HOTLeafPage.DEFAULT_SIZE);
    final Fixture fixture = fixture(allocator, FULL_LEAF_ENTRIES);
    final int liveBefore = frameAllocator.liveSlotCount(frameClass);
    final List<PageReference> dropped = new ArrayList<>();

    try {
      // 56 + 1 entries pass the count gate; 56 * 40 reclaimed prefix bytes do not pass the byte budget.
      final HOTIndirectPage consolidated = HOTIncrementalInsert.consolidateNodeLeaves(fixture.parent,
          CONSOLIDATION_TARGET, 2, IndexType.VALIDTIME, allocator::getAndIncrement, dropped);

      assertSame(fixture.parent, consolidated, "an unmergeable pair must leave the node as it is");
      assertTrue(dropped.isEmpty(), "no source leaf may be handed to retirement when nothing merged");
      assertEquals(liveBefore, frameAllocator.liveSlotCount(frameClass),
          "the speculative merged leaf must be closed when the pair does not fit");
      assertHeavyLeafIntact(fixture.heavy, FULL_LEAF_ENTRIES);
      assertEquals(1, fixture.light.getEntryCount());
      assertArrayEquals(lightKey(), fixture.light.getKey(0));
    } finally {
      closeAll(fixture.parent);
    }
  }

  @Test
  @DisplayName("a pair whose merged prefix rebuild fits is merged with every key and value preserved")
  void consolidationMergesPairWhosePrefixRebuildFits() {
    final int heavyEntries = 20;
    final AtomicLong allocator = new AtomicLong(1);
    final Fixture fixture = fixture(allocator, heavyEntries);
    final List<PageReference> dropped = new ArrayList<>();
    HOTIndirectPage consolidated = null;

    try {
      consolidated = HOTIncrementalInsert.consolidateNodeLeaves(fixture.parent, CONSOLIDATION_TARGET, 2,
          IndexType.VALIDTIME, allocator::getAndIncrement, dropped);

      assertEquals(2, consolidated.getNumChildren(), "the mergeable left chain must collapse");
      assertEquals(3, dropped.size(), "the three merged-away source leaves are handed to retirement");
      assertTrue(consolidated.getChildReference(0).getPage() instanceof HOTLeafPage);
      final HOTLeafPage merged = (HOTLeafPage) consolidated.getChildReference(0).getPage();
      assertEquals(heavyEntries + 2, merged.getEntryCount());
      assertEquals(0, merged.getCommonPrefixLen(), "the siblings share no leading byte");
      for (int i = 0; i < heavyEntries; i++) {
        assertArrayEquals(heavyKey(i), merged.getKey(i));
        assertArrayEquals(heavyValue(i), merged.copyStoredValue(i));
      }
      assertArrayEquals(lightKey(), merged.getKey(heavyEntries));
      assertArrayEquals(LIGHT_VALUE, merged.copyStoredValue(heavyEntries));
      assertArrayEquals(new byte[] {0x40}, merged.getKey(heavyEntries + 1));
      assertArrayEquals(LIGHT_VALUE, merged.copyStoredValue(heavyEntries + 1));
      assertHeavyLeafIntact(fixture.heavy, heavyEntries);
    } finally {
      closeAll(fixture.parent, consolidated);
    }
  }

  // ===== Fixtures =====

  private static final byte[] LIGHT_VALUE = {0x01, 0x02, 0x03};

  /**
   * A four-leaf node routed on bits 0..2 of the first key byte (the shape of
   * {@code HOTIndirectPageSplitFaithfulTest#consolidationChain}): children {@code 0x00…},
   * {@code 0x20…}, {@code 0x40…}, {@code 0x80…}. Children 0 and 1 are BiNode-paired. Child 0 holds
   * {@code heavyEntries} large values under a 40-byte shared prefix; child 1 holds one key that
   * shares no byte with it.
   */
  private static Fixture fixture(final AtomicLong allocator, final int heavyEntries) {
    final HOTLeafPage heavy = new HOTLeafPage(allocator.getAndIncrement(), 1, IndexType.VALIDTIME);
    final HOTLeafPage light = new HOTLeafPage(allocator.getAndIncrement(), 1, IndexType.VALIDTIME);
    final HOTLeafPage third = new HOTLeafPage(allocator.getAndIncrement(), 1, IndexType.VALIDTIME);
    final HOTLeafPage fourth = new HOTLeafPage(allocator.getAndIncrement(), 1, IndexType.VALIDTIME);
    final HOTLeafPage[] leaves = {heavy, light, third, fourth};
    try {
      for (int i = 0; i < heavyEntries; i++) {
        assertTrue(heavy.put(heavyKey(i), heavyValue(i)), "heavy entry " + i + " must fit its own leaf");
      }
      assertEquals(SHARED_PREFIX_LEN, heavy.getCommonPrefixLen());
      assertTrue(light.put(lightKey(), LIGHT_VALUE));
      assertTrue(third.put(new byte[] {0x40}, LIGHT_VALUE));
      assertTrue(fourth.put(new byte[] {(byte) 0x80}, LIGHT_VALUE));
      final PageReference[] references = {swizzle(heavy), swizzle(light), swizzle(third), swizzle(fourth)};
      final HOTIndirectPage parent = HOTBulkBuilder.assembleIndirect(new int[] {0, 1, 2}, new int[] {0, 1, 2, 4},
          references, 1, 1, allocator::getAndIncrement);
      return new Fixture(parent, heavy, light);
    } catch (final RuntimeException | Error failure) {
      for (final HOTLeafPage leaf : leaves) {
        leaf.close();
      }
      throw failure;
    }
  }

  private record Fixture(HOTIndirectPage parent, HOTLeafPage heavy, HOTLeafPage light) {
  }

  /**
   * {@code 0x00}, 39 shared filler bytes, then a distinguishing ordinal: bits 0..2 are {@code 000}.
   */
  private static byte[] heavyKey(final int ordinal) {
    final byte[] key = new byte[SHARED_PREFIX_LEN + 2];
    Arrays.fill(key, 1, SHARED_PREFIX_LEN, (byte) 0x11);
    key[SHARED_PREFIX_LEN] = (byte) ordinal;
    key[SHARED_PREFIX_LEN + 1] = (byte) 0xA5;
    return key;
  }

  /**
   * Bits 0..2 are {@code 001}: BiNode-paired with the heavy leaf, sharing no leading byte with it.
   */
  private static byte[] lightKey() {
    final byte[] key = new byte[SHARED_PREFIX_LEN + 2];
    Arrays.fill(key, (byte) 0x22);
    key[0] = 0x20;
    return key;
  }

  private static byte[] heavyValue(final int seed) {
    final byte[] value = new byte[VALUE_LEN];
    Arrays.fill(value, (byte) (0x40 + seed));
    return value;
  }

  private static void assertHeavyLeafIntact(final HOTLeafPage heavy, final int entries) {
    assertEquals(entries, heavy.getEntryCount(), "pure consolidation must not mutate its source leaf");
    assertEquals(SHARED_PREFIX_LEN, heavy.getCommonPrefixLen());
    for (int i = 0; i < entries; i++) {
      assertArrayEquals(heavyKey(i), heavy.getKey(i));
      assertArrayEquals(heavyValue(i), heavy.copyStoredValue(i));
    }
  }

  private static PageReference swizzle(final Page page) {
    final PageReference reference = new PageReference();
    reference.setPage(page);
    return reference;
  }

  private static void closeAll(final Page... roots) {
    final Set<HOTLeafPage> leaves = Collections.newSetFromMap(new IdentityHashMap<>());
    for (final Page root : roots) {
      collectLeafPages(root, leaves);
    }
    for (final HOTLeafPage leaf : leaves) {
      if (!leaf.isClosed()) {
        leaf.close();
      }
    }
  }

  private static void collectLeafPages(final Page page, final Set<HOTLeafPage> out) {
    if (page instanceof HOTLeafPage leaf) {
      out.add(leaf);
    } else if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        final PageReference reference = indirect.getChildReference(i);
        if (reference != null && reference.getPage() != null) {
          collectLeafPages(reference.getPage(), out);
        }
      }
    }
  }
}
