/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSegmentFetcher;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionColumnStore.LeafColumnAccess;
import io.sirix.index.projection.ProjectionColumnStore.StringValueExtrema;
import io.sirix.index.projection.ProjectionColumnStore.ZoneIndex;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.index.projection.ProjectionIndexScan.Op;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Witnesses for the bounded top-k's PLAN ({@link ProjectionColumnScan#topKRecordKeys}) and the store
 * memos it is built from, on hand-shaped stores where every expected answer, skip count and decline
 * follows from the construction. The randomized parity tests prove the ANSWER; these prove that the
 * mechanism engaged — a plan that silently falls back to a full document-order walk returns the same
 * keys, and only the skip and tie counters tell the two apart.
 */
final class TopKPlanWitnessTest {

  /** Columns: 0 = long ({@code leaf * 1000 + row}), 1 = double, 2 = boolean, 3 = the shaped string column. */
  private static final byte[] KINDS =
      {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE,
          ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT,};
  private static final int LONGS = 0;
  private static final int STRINGS = 3;
  private static final byte[] EMPTY = "".getBytes(StandardCharsets.UTF_8);

  private record Shaped(List<RowGroupDirectory> directories, ColumnSegmentFetcher fetcher) {
    ProjectionColumnStore fresh() {
      return new ProjectionColumnStore(directories);
    }
  }

  /** The record key of {@code row} on {@code leaf}, by construction. */
  private static long key(final int leaf, final int row) {
    return leaf * 100_000L + 1 + row;
  }

  /**
   * A store of {@code leaves} leaves whose string column holds {@code stringsOf(leaf)} in row order —
   * {@code null} a missing cell, an empty array a rowless leaf — beside all-present long, double and
   * boolean columns.
   */
  private static Shaped buildShaped(final int leaves, final IntFunction<String[]> stringsOf) {
    final Map<Long, byte[]> segmentsByOffset = new HashMap<>();
    final List<RowGroupDirectory> directories = new ArrayList<>(leaves);
    long nextOffset = 1_000;
    final long[] longs = new long[KINDS.length];
    final boolean[] bools = new boolean[KINDS.length];
    final String[] strings = new String[KINDS.length];
    final boolean[] present = new boolean[KINDS.length];
    final boolean[] none = new boolean[KINDS.length];
    for (int leaf = 0; leaf < leaves; leaf++) {
      final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS.clone());
      final String[] values = stringsOf.apply(leaf);
      for (int r = 0; r < values.length; r++) {
        longs[LONGS] = leaf * 1000L + r;
        longs[1] = ProjectionDoubleEncoding.encode(r);
        bools[2] = (r & 1) == 0;
        strings[STRINGS] = values[r] == null
            ? ""
            : values[r];
        Arrays.fill(present, true);
        present[STRINGS] = values[r] != null;
        assertTrue(page.appendRow(key(leaf, r), longs, bools, strings, present, none, none, none),
            "the shaped leaf must accept its rows");
      }
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
          ProjectionIndexColumnSegmentCodec.encode(page.serialize());
      final int columnSegmentCount = encoded.columnSegmentIds().length;
      final int[] ids = new int[columnSegmentCount];
      final long[] offsets = new long[columnSegmentCount];
      for (int i = 0; i < columnSegmentCount; i++) {
        ids[i] = encoded.columnSegmentIds()[i];
        offsets[i] = nextOffset;
        segmentsByOffset.put(nextOffset, encoded.segments()[i]);
        nextOffset += 1 + encoded.segments()[i].length;
      }
      directories.add(new RowGroupDirectory(leaf + 1, encoded.descriptor(), ids, offsets, new byte[ids.length][]));
    }
    final ColumnSegmentFetcher fetcher = wanted -> {
      final byte[][] out = new byte[wanted.length][];
      for (int i = 0; i < wanted.length; i++) {
        out[i] = segmentsByOffset.get(wanted[i]);
      }
      return out;
    };
    return new Shaped(directories, fetcher);
  }

  /** The catalog fetcher's shape: ranged fetches may run concurrently, so memo passes and slabs fan out. */
  private static ColumnSegmentFetcher concurrent(final ColumnSegmentFetcher plain) {
    return new ColumnSegmentFetcher() {
      @Override
      public byte @Nullable [] @Nullable [] fetchAll(final long[] offsets) {
        return plain.fetchAll(offsets);
      }

      @Override
      public boolean rangedFetchIsConcurrent() {
        return true;
      }
    };
  }

  private static long @Nullable [] topK(final ProjectionColumnStore store, final ColumnPredicate[] predicates,
      final boolean descending, final int k, final ColumnSegmentFetcher fetcher) {
    return ProjectionColumnScan.topKRecordKeys(store, predicates, new int[] {STRINGS}, new boolean[] {descending}, k,
        fetcher);
  }

  private static String slotValue(final StringValueExtrema extrema, final int leaf, final int slot) {
    return new String(extrema.bytes(), extrema.offset(leaf, slot), extrema.length(leaf, slot), StandardCharsets.UTF_8);
  }

  private static String cell(final ColumnSlice slice, final int row) {
    final int id = slice.stringDictIds()[row];
    return new String(slice.dictBytes(), slice.dictOffset(id), slice.dictLength(id), StandardCharsets.UTF_8);
  }

  // ==================== the plan ====================

  @Test
  void aNotEqualOnTheFirstKeyBoundsEveryLeafByItsSecondExtremumAndDropsSingleValueLeaves() {
    // ClickBench q25: WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10. The empty string is
    // every leaf's smallest value, so on first extrema alone every leaf ties, the plan collapses to
    // document order and nothing is ever skipped. Bounded by the SECOND distinct extremum the leaves
    // order by their real best matching value ("m0" < "m1" < …), the heap is full after ONE leaf and
    // the stop rule proves the other six away. Leaf 5 holds only the excluded value and is dropped
    // before the walk; leaf 6 has a missing cell, which the predicate makes irrelevant (missing never
    // matches), so its bound stays usable.
    final Shaped fx = buildShaped(8, leaf -> switch (leaf) {
      case 5 -> new String[] {"", "", "", ""};
      case 6 -> new String[] {"", null, "m6", "m6"};
      default -> new String[] {"", "", "m" + leaf, "m" + leaf, "m" + leaf, ""};
    });
    final ColumnPredicate[] notEmpty = {ColumnPredicate.stringNe(STRINGS, EMPTY)};
    for (final ColumnSegmentFetcher fetcher : new ColumnSegmentFetcher[] {fx.fetcher(), concurrent(fx.fetcher())}) {
      final ProjectionColumnStore store = fx.fresh();
      long tied = ProjectionColumnScan.topKPlanTiedCount();
      long skipped = ProjectionColumnScan.topKLeavesSkippedCount();
      assertArrayEquals(new long[] {key(0, 2), key(0, 3)}, topK(store, notEmpty, false, 2, fetcher),
          "the two smallest non-empty values are leaf 0's first two \"m0\" rows");
      assertEquals(0L, ProjectionColumnScan.topKPlanTiedCount() - tied,
          "the second extrema differ per leaf: the plan must NOT collapse to document order");
      assertEquals(6L, ProjectionColumnScan.topKLeavesSkippedCount() - skipped,
          "after leaf 0 fills the heap, every other admitted leaf (1, 2, 3, 4, 6, 7) is skipped by the stop rule");

      skipped = ProjectionColumnScan.topKLeavesSkippedCount();
      assertArrayEquals(new long[] {key(0, 2), key(0, 3), key(0, 4), key(1, 2)}, topK(store, notEmpty, false, 4, fetcher),
          "k=4 crosses into leaf 1");
      assertEquals(4L, ProjectionColumnScan.topKLeavesSkippedCount() - skipped,
          "chunks of 1 and 2 leaves evaluate leaves 0, 1 and 2; the stop rule skips 3, 4, 6 and 7");

      // Descending: the largest values are the "m" ones already, so no refinement fires — except on
      // leaf 5, whose largest IS the excluded literal and which has no second value: dropped.
      tied = ProjectionColumnScan.topKPlanTiedCount();
      skipped = ProjectionColumnScan.topKLeavesSkippedCount();
      assertArrayEquals(new long[] {key(7, 2), key(7, 3)}, topK(store, notEmpty, true, 2, fetcher),
          "descending: leaf 7's \"m7\" rows lead");
      assertEquals(0L, ProjectionColumnScan.topKPlanTiedCount() - tied);
      assertEquals(6L, ProjectionColumnScan.topKLeavesSkippedCount() - skipped,
          "descending: leaf 7 fills the heap and the stop rule proves 6, 4, 3, 2, 1, 0 away");
    }
  }

  @Test
  void aLeafThatMayHideAMatchingRowWithoutTheKeyIsVisitedFirstAndNeverSkipped() {
    // Without a predicate on the order column, a leaf that is not all-present on it may hold a
    // matching row with no key — one only the interpreter can place. Its bound is unusable: the
    // leaf is visited before every known one, and the walk declines on the row. A plan that trusted
    // the leaf's smallest PRESENT value ("z") would order it last, fill the heap from leaf 0 and skip
    // it — a wrong, non-null answer.
    final Shaped fx = buildShaped(3, leaf -> switch (leaf) {
      case 0 -> new String[] {"a", "a"};
      case 1 -> new String[] {"z", null};
      default -> new String[] {"b"};
    });
    final ColumnPredicate[] none = new ColumnPredicate[0];
    final ColumnPredicate[] offKey = {ColumnPredicate.numeric(LONGS, Op.GE, 0L)};
    final ColumnPredicate[] onKey = {ColumnPredicate.stringNe(STRINGS, "q".getBytes(StandardCharsets.UTF_8))};
    for (final ColumnSegmentFetcher fetcher : new ColumnSegmentFetcher[] {fx.fetcher(), concurrent(fx.fetcher())}) {
      final ProjectionColumnStore store = fx.fresh();
      for (final boolean desc : new boolean[] {false, true}) {
        assertNull(topK(store, none, desc, 1, fetcher), "no predicate: the missing cell matches, so decline");
        assertNull(topK(store, offKey, desc, 1, fetcher),
            "a predicate off the order column still admits the missing cell, so decline");
      }
      // A predicate ON the order column makes every leaf's bound usable: the walk answers, best-first.
      final long skipped = ProjectionColumnScan.topKLeavesSkippedCount();
      assertArrayEquals(new long[] {key(0, 0)}, topK(store, onKey, false, 1, fetcher));
      assertEquals(2L, ProjectionColumnScan.topKLeavesSkippedCount() - skipped,
          "leaf 0 (\"a\") fills the heap; leaves 2 (\"b\") and 1 (\"z\") are skipped");
      assertArrayEquals(new long[] {key(1, 0)}, topK(store, onKey, true, 1, fetcher));
    }
  }

  @Test
  void stringBestsSharingAnEightBytePrefixAreOrderedByTheFullComparison() {
    // The best-first order of string leaves is a radix sort on the first eight bytes, refined per run
    // of equal prefixes by the full comparison. Drop the refinement and leaves 0 ("…m"), 1 ("…z"),
    // 2 ("…a") stay in leaf order: leaf 0 fills the heap, leaf 1's bound is strictly worse, the stop
    // rule fires and leaf 2 — the real minimum — is never visited. The answer, not just a counter,
    // is wrong then.
    final Shaped fx = buildShaped(3, leaf -> switch (leaf) {
      case 0 -> new String[] {"longprefix_m"};
      case 1 -> new String[] {"longprefix_z"};
      default -> new String[] {"longprefix_a"};
    });
    for (final ColumnSegmentFetcher fetcher : new ColumnSegmentFetcher[] {fx.fetcher(), concurrent(fx.fetcher())}) {
      final ProjectionColumnStore store = fx.fresh();
      long skipped = ProjectionColumnScan.topKLeavesSkippedCount();
      assertArrayEquals(new long[] {key(2, 0)}, topK(store, new ColumnPredicate[0], false, 1, fetcher));
      assertEquals(2L, ProjectionColumnScan.topKLeavesSkippedCount() - skipped, "leaf 2 fills the heap, 0 and 1 are skipped");
      skipped = ProjectionColumnScan.topKLeavesSkippedCount();
      assertArrayEquals(new long[] {key(1, 0)}, topK(store, new ColumnPredicate[0], true, 1, fetcher));
      assertEquals(2L, ProjectionColumnScan.topKLeavesSkippedCount() - skipped, "descending: leaf 1 leads");
    }
  }

  @Test
  void stringBestsOrderByUnsignedBytesNotSignedOnes() {
    // "é" is 0xC3 0xA9: negative as a signed byte, so a prefix sort that forgets the sign bias visits
    // it FIRST although unsigned (= the interpreter's) order puts every ASCII value before it. The
    // answer survives that (the walk then evaluates everything), the skip count does not: correct
    // order "a" < "m" < "é" fills the heap on leaf 1 and proves the other two away.
    final Shaped fx = buildShaped(3, leaf -> switch (leaf) {
      case 0 -> new String[] {"é"};
      case 1 -> new String[] {"a"};
      default -> new String[] {"m"};
    });
    final ProjectionColumnStore store = fx.fresh();
    long skipped = ProjectionColumnScan.topKLeavesSkippedCount();
    assertArrayEquals(new long[] {key(1, 0)}, topK(store, new ColumnPredicate[0], false, 1, fx.fetcher()));
    assertEquals(2L, ProjectionColumnScan.topKLeavesSkippedCount() - skipped, "ascending: \"a\" leads, two skipped");
    skipped = ProjectionColumnScan.topKLeavesSkippedCount();
    assertArrayEquals(new long[] {key(0, 0)}, topK(store, new ColumnPredicate[0], true, 1, fx.fetcher()));
    assertEquals(2L, ProjectionColumnScan.topKLeavesSkippedCount() - skipped, "descending: \"é\" leads, two skipped");
  }

  @Test
  void aSlabsOwnFullHeapSkipsLeavesTheFrozenGlobalOneCannot() {
    // Chunks 1 and 2 (leaves "a", "b", "c" — one row each) leave a k=4 heap short of full, so the
    // global heap frozen for chunk 3 rules nothing out. Inside chunk 3 the slab evaluating "d"×4 is
    // full after that one leaf and its OWN heap proves "e" away (and "g" after "f"×4 on the second
    // slab); a slab that only consulted the frozen global heap would decode every leaf of the chunk.
    final Shaped fx = buildShaped(7, leaf -> switch (leaf) {
      case 0 -> new String[] {"a"};
      case 1 -> new String[] {"b"};
      case 2 -> new String[] {"c"};
      case 3 -> new String[] {"d", "d", "d", "d"};
      case 4 -> new String[] {"e"};
      case 5 -> new String[] {"f", "f", "f", "f"};
      default -> new String[] {"g"};
    });
    for (final ColumnSegmentFetcher fetcher : new ColumnSegmentFetcher[] {fx.fetcher(), concurrent(fx.fetcher())}) {
      final ProjectionColumnStore store = fx.fresh();
      final long skipped = ProjectionColumnScan.topKLeavesSkippedCount();
      assertArrayEquals(new long[] {key(0, 0), key(1, 0), key(2, 0), key(3, 0)},
          topK(store, new ColumnPredicate[0], false, 4, fetcher));
      assertTrue(ProjectionColumnScan.topKLeavesSkippedCount() - skipped >= 2L,
          "the slab-local heap must skip \"e\" and \"g\" (one slab: \"e\", \"f\" and \"g\")");
    }
  }

  @Test
  void theBoundedWalkNeverFillsAColumnForItself() {
    // The witness the executor test asserts from the outside, here at the store: after a bounded
    // top-k over a fresh store nothing is resident and nothing is retained — every leaf the walk
    // touched was decoded into a per-slab access and dropped.
    final Shaped fx = buildShaped(40, leaf -> new String[] {"v" + (leaf * 7 % 40), "w"});
    final ProjectionColumnStore store = fx.fresh();
    final long windowed = ProjectionColumnStore.windowedLeafAccessCount();
    assertArrayEquals(new long[] {key(0, 0), key(23, 0)}, topK(store, new ColumnPredicate[0], false, 2, fx.fetcher()),
        "\"v0\" on leaf 0 and \"v1\" on leaf 23 (23 * 7 mod 40 = 1)");
    assertEquals(1L, ProjectionColumnStore.windowedLeafAccessCount() - windowed,
        "a fresh store is not resident, so the scan counts as one windowed (non-retaining) access");
    assertFalse(store.columnFilled(STRINGS), "the order column must not have been filled");
    assertFalse(store.recordKeysFilled(), "the record keys must not have been filled");
    assertEquals(0L, store.retainedFillBytes(), "nothing is retained");
    // A resident store serves from its slices and does not count as windowed.
    assertTrue(store.column(STRINGS, fx.fetcher()).length == 40);
    assertTrue(store.recordKeys(fx.fetcher()).length == 40);
    final long windowedResident = ProjectionColumnStore.windowedLeafAccessCount();
    assertArrayEquals(new long[] {key(0, 0), key(23, 0)}, topK(store, new ColumnPredicate[0], false, 2, fx.fetcher()));
    assertEquals(0L, ProjectionColumnStore.windowedLeafAccessCount() - windowedResident,
        "the resident route must not be counted as windowed");
  }

  // ==================== the memos the plan is built from ====================

  /** {@code leaf % 3}: 0 → ["a", "b"], 1 → ["b", missing], 2 → rowless; 4,400 leaves so the memo passes fan out. */
  private static Shaped memoCorpus() {
    return buildShaped(4_400, leaf -> switch (leaf % 3) {
      case 0 -> new String[] {"a", "b"};
      case 1 -> new String[] {"b", null};
      default -> new String[0];
    });
  }

  @Test
  void allPresentLeavesAgreesAcrossItsSourcesOnAFannedOutPass() {
    final Shaped fx = memoCorpus();
    final long[] expected = new long[(4_400 + 63) >>> 6];
    for (int leaf = 0; leaf < 4_400; leaf++) {
      if (leaf % 3 != 1) {
        expected[leaf >>> 6] |= 1L << (leaf & 63); // all-present, or rowless (vacuously)
      }
    }
    final long[] viaConcurrentFetch = fx.fresh().allPresentLeaves(STRINGS, concurrent(fx.fetcher()));
    final long[] viaSerialFetch = fx.fresh().allPresentLeaves(STRINGS, fx.fetcher());
    final ProjectionColumnStore bytesStore = fx.fresh();
    bytesStore.columnBytes(STRINGS, fx.fetcher());
    final long[] viaBytes = bytesStore.allPresentLeaves(STRINGS, fx.fetcher());
    final ProjectionColumnStore sliceStore = fx.fresh();
    sliceStore.column(STRINGS, fx.fetcher());
    final long[] viaSlices = sliceStore.allPresentLeaves(STRINGS, fx.fetcher());
    assertArrayEquals(expected, viaConcurrentFetch, "fetched in concurrent ranges (presence markers)");
    assertArrayEquals(expected, viaSerialFetch, "fetched serially");
    assertArrayEquals(expected, viaBytes, "from the retained body bytes");
    assertArrayEquals(expected, viaSlices, "from the resident slices");
    final long[] allSet = new long[expected.length];
    for (int leaf = 0; leaf < 4_400; leaf++) {
      allSet[leaf >>> 6] |= 1L << (leaf & 63);
    }
    assertArrayEquals(allSet, fx.fresh().allPresentLeaves(LONGS, concurrent(fx.fetcher())),
        "an all-present column is all-present on every leaf, rowless ones included");
    final ProjectionColumnStore fresh = fx.fresh();
    fresh.allPresentLeaves(STRINGS, fx.fetcher());
    assertFalse(fresh.columnFilled(STRINGS), "the presence pass retains nothing");
    assertEquals(0L, fresh.retainedFillBytes());
  }

  @Test
  void stringValueExtremaHoldTwoDistinctValuesPerSideFromFetchedAndResidentLeavesAlike() {
    final Shaped fx = memoCorpus();
    final StringValueExtrema viaFetch = fx.fresh().stringValueExtrema(STRINGS, concurrent(fx.fetcher()));
    final ProjectionColumnStore resident = fx.fresh();
    resident.column(STRINGS, fx.fetcher());
    final StringValueExtrema viaSlices = resident.stringValueExtrema(STRINGS, fx.fetcher());
    for (final StringValueExtrema extrema : new StringValueExtrema[] {viaFetch, viaSlices}) {
      assertEquals(4_400, extrema.leafCount());
      for (int leaf = 0; leaf < 4_400; leaf++) {
        switch (leaf % 3) {
          case 0 -> {
            assertEquals("a", slotValue(extrema, leaf, StringValueExtrema.MIN1), "MIN1 of leaf " + leaf);
            assertEquals("b", slotValue(extrema, leaf, StringValueExtrema.MIN2), "MIN2 of leaf " + leaf);
            assertEquals("b", slotValue(extrema, leaf, StringValueExtrema.MAX1), "MAX1 of leaf " + leaf);
            assertEquals("a", slotValue(extrema, leaf, StringValueExtrema.MAX2), "MAX2 of leaf " + leaf);
          }
          case 1 -> {
            assertEquals("b", slotValue(extrema, leaf, StringValueExtrema.MIN1), "MIN1 of leaf " + leaf);
            assertFalse(extrema.has(leaf, StringValueExtrema.MIN2), "one distinct value: no MIN2 on leaf " + leaf);
            assertEquals("b", slotValue(extrema, leaf, StringValueExtrema.MAX1), "MAX1 of leaf " + leaf);
            assertFalse(extrema.has(leaf, StringValueExtrema.MAX2), "one distinct value: no MAX2 on leaf " + leaf);
            assertEquals(0, extrema.length(leaf, StringValueExtrema.MIN2), "an absent slot has no bytes");
          }
          default -> {
            for (int slot = 0; slot < 4; slot++) {
              assertFalse(extrema.has(leaf, slot), "a rowless leaf has no extremum, leaf " + leaf);
              assertEquals(-1, extrema.id(leaf, slot));
            }
          }
        }
      }
    }
    assertEquals(ProjectionColumnStore.SUPPLEMENTARY_NONE, resident.stringDictSupplementaryMemo(STRINGS),
        "the walk settles the collation verdict as a by-product");
  }

  @Test
  void zoneIndexMirrorsTheDescriptorsOfEveryLeaf() {
    final Shaped fx = memoCorpus();
    final ProjectionColumnStore store = fx.fresh();
    final ZoneIndex zone = store.zoneIndex(LONGS);
    final ColumnSlice[] slices = fx.fresh().column(LONGS, fx.fetcher());
    for (int leaf = 0; leaf < 4_400; leaf++) {
      if (leaf % 3 == 2) {
        assertTrue(!zone.known(leaf) || zone.allMissing(leaf), "a rowless leaf has no range, leaf " + leaf);
        continue;
      }
      assertTrue(zone.known(leaf), "leaf " + leaf);
      assertFalse(zone.allMissing(leaf), "leaf " + leaf);
      assertEquals(leaf * 1000L, zone.min(leaf), "min of leaf " + leaf);
      assertEquals(leaf * 1000L + 1, zone.max(leaf), "max of leaf " + leaf);
      assertEquals(slices[leaf].min(), zone.min(leaf), "the slice's min is the descriptor's, leaf " + leaf);
      assertEquals(slices[leaf].max(), zone.max(leaf), "the slice's max is the descriptor's, leaf " + leaf);
      assertEquals(slices[leaf].flags(), zone.flags(leaf), "flags of leaf " + leaf);
    }
    assertFalse(store.columnFilled(LONGS), "the zone index reads descriptors only");
  }

  @Test
  void leafSetAccessDecodesExactlyTheRequestedLeavesAndRetainsNothing() {
    final Shaped fx = memoCorpus();
    final ProjectionColumnStore store = fx.fresh();
    final ProjectionColumnStore resident = fx.fresh();
    final ColumnSlice[] strings = resident.column(STRINGS, fx.fetcher());
    final ColumnSlice[] longs = resident.column(LONGS, fx.fetcher());
    final long[][] keys = resident.recordKeys(fx.fetcher());
    final int[] set = {0, 1, 2, 4, 4_396, 4_399};
    final LeafColumnAccess access = store.leafSetAccess(fx.fetcher(), null, set, 1, set.length - 1);
    assertTrue(access.windowed());
    for (int i = 1; i < set.length - 1; i++) {
      final int leaf = set[i];
      final ColumnSlice s = access.slice(STRINGS, leaf);
      final ColumnSlice l = access.slice(LONGS, leaf);
      assertEquals(strings[leaf].rowCount(), s.rowCount(), "rows of leaf " + leaf);
      assertEquals(longs[leaf].rowCount(), l.rowCount(), "rows of leaf " + leaf);
      if (s.rowCount() <= 0) {
        assertEquals(0, keys[leaf].length);
        continue;
      }
      assertArrayEquals(strings[leaf].presenceWords(), s.presenceWords(), "presence of leaf " + leaf);
      assertArrayEquals(longs[leaf].numericValues(), l.numericValues(), "longs of leaf " + leaf);
      assertArrayEquals(keys[leaf], access.recordKeys(leaf), "record keys of leaf " + leaf);
      for (int r = 0; r < s.rowCount(); r++) {
        if ((s.presenceWords()[r >>> 6] & 1L << (r & 63)) != 0L) {
          assertEquals(cell(strings[leaf], r), cell(s, r), "row " + r + " of leaf " + leaf);
        }
      }
    }
    assertFalse(store.columnFilled(STRINGS));
    assertFalse(store.columnFilled(LONGS));
    assertFalse(store.recordKeysFilled());
    assertEquals(0L, store.retainedFillBytes(), "a leaf-set access retains nothing in the store");
    assertThrows(IllegalArgumentException.class, () -> store.leafSetAccess(fx.fetcher(), null, new int[] {3, 1}, 0, 2),
        "leaves must ascend");
    assertThrows(IllegalArgumentException.class, () -> store.leafSetAccess(fx.fetcher(), null, new int[] {2, 2}, 0, 2),
        "leaves must be distinct");
    assertThrows(IllegalArgumentException.class, () -> store.leafSetAccess(fx.fetcher(), null, new int[] {4_400}, 0, 1),
        "leaves must be in range");
    assertThrows(IllegalArgumentException.class, () -> store.leafSetAccess(fx.fetcher(), null, new int[] {1}, 0, 2),
        "the range must fit the array");
    assertThrows(IllegalArgumentException.class, () -> store.leafSetAccess(null, null, new int[] {1}, 0, 1),
        "the fetcher is required");
  }

  @Test
  void recordKeysFilledObservesResidencyWithoutCreatingIt() {
    final Shaped fx = buildShaped(4, leaf -> new String[] {"x"});
    final ProjectionColumnStore store = fx.fresh();
    assertFalse(store.recordKeysFilled(), "a fresh store holds no record keys");
    assertFalse(store.recordKeysFilled(), "asking must not fill them");
    assertEquals(4, store.recordKeys(fx.fetcher()).length);
    assertTrue(store.recordKeysFilled(), "after a fill the keys are resident");
  }
}
