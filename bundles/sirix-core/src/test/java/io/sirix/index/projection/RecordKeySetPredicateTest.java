/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.budget.EngineWorkCounters;
import io.sirix.budget.WorkCapture;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSegmentFetcher;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.index.projection.ProjectionIndexScan.Op;
import io.sirix.index.projection.ProjectionIndexScan.PredicateTree;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec.EncodedRowGroup;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The index-routed row source ({@link Op#KEY_IN}): every kernel family must answer a record-key
 * membership predicate exactly — the sliced kernels over resident and windowed access, the
 * conjunctive and the tree evaluator, the whole-leaf byte kernels, and the leaf keep mask — on
 * stores whose leaves carry ORDER EXCEPTIONS (keys out of ascending order), alone and conjoined
 * with ordinary column predicates. The brute-force walk over the fixture's own keys is the oracle.
 */
final class RecordKeySetPredicateTest {

  private static final byte[] KINDS =
      {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};

  /**
   * Columns: 0 = long, 1 = string. {@code keys[leaf][row]} and {@code longs[leaf][row]} mirror the
   * pages.
   */
  private record Fixture(ProjectionColumnStore store, List<byte[]> rawLeaves, ColumnSegmentFetcher fetcher,
      long[][] keys, long[][] longs, boolean[][] present) {
  }

  private static Fixture buildFixture(final long seed, final int leaves, final boolean exceptions) {
    final Random rnd = new Random(seed);
    final Map<Long, byte[]> segmentsByOffset = new HashMap<>();
    final List<RowGroupDirectory> directories = new ArrayList<>(leaves);
    final List<byte[]> rawLeaves = new ArrayList<>(leaves);
    final long[][] keys = new long[leaves][];
    final long[][] longs = new long[leaves][];
    final boolean[][] present = new boolean[leaves][];
    long nextOffset = 1_000;
    long recordKey = 1;
    for (int leaf = 0; leaf < leaves; leaf++) {
      final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS.clone());
      final int rows = leaf == 1
          ? 0 // one rowless leaf
          : 1 + rnd.nextInt(ProjectionIndexRowGroupPage.MAX_ROWS);
      keys[leaf] = new long[rows];
      longs[leaf] = new long[rows];
      present[leaf] = new boolean[rows];
      final long[] values = new long[KINDS.length];
      final boolean[] bools = new boolean[KINDS.length];
      final String[] strings = new String[KINDS.length];
      final boolean[] presentCells = new boolean[KINDS.length];
      final boolean[] unrep = new boolean[KINDS.length];
      final boolean[] nonIntegral = new boolean[KINDS.length];
      final boolean[] nonDoubleSource = new boolean[KINDS.length];
      for (int r = 0; r < rows; r++) {
        // An order exception every ~40 rows: a key below the running maximum, as a record inserted
        // before its siblings is stored. Never a duplicate.
        final long key;
        final boolean exception = exceptions && r > 0 && rnd.nextInt(40) == 0;
        if (exception) {
          key = recordKey - 1 - rnd.nextInt(3) * 2; // odd gaps below keep it distinct from stored keys
          recordKey += 1;
        } else {
          key = recordKey;
          recordKey += 4 + rnd.nextInt(3);
        }
        keys[leaf][r] = key;
        values[0] = rnd.nextLong(-1_000, 1_000);
        longs[leaf][r] = values[0];
        strings[1] = "s" + rnd.nextInt(4);
        presentCells[0] = rnd.nextInt(10) != 0;
        presentCells[1] = true;
        present[leaf][r] = presentCells[0];
        final byte[][] utf8 = new byte[KINDS.length][];
        final int[] utf8Lengths = new int[KINDS.length];
        utf8[1] = strings[1].getBytes(StandardCharsets.UTF_8);
        utf8Lengths[1] = utf8[1].length;
        page.appendExtractedUtf8Row(key, values, bools, utf8, utf8Lengths, null, presentCells, unrep, nonIntegral,
            nonDoubleSource, exception);
      }
      final byte[] raw = page.serialize();
      rawLeaves.add(raw);
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(raw);
      final int segmentCount = encoded.columnSegmentIds().length;
      final int[] ids = new int[segmentCount];
      final long[] offsets = new long[segmentCount];
      for (int i = 0; i < segmentCount; i++) {
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
    return new Fixture(new ProjectionColumnStore(directories), rawLeaves, fetcher, keys, longs, present);
  }

  /**
   * A strictly ascending key set: every {@code oneIn}-th stored key, plus a few keys stored nowhere.
   */
  private static long[] keySet(final Fixture fx, final Random rnd, final int oneIn, final boolean withForeign) {
    final LongOpenHashSet set = new LongOpenHashSet();
    for (final long[] leafKeys : fx.keys()) {
      for (final long key : leafKeys) {
        if (rnd.nextInt(oneIn) == 0) {
          set.add(key);
        }
      }
    }
    if (withForeign) {
      set.add(0L);
      set.add(Long.MAX_VALUE - 7);
      set.add(2L); // between stored keys of the first leaf
    }
    final long[] sorted = set.toLongArray();
    Arrays.sort(sorted);
    return sorted;
  }

  private static long bruteCount(final Fixture fx, final long[] sortedKeys, final boolean longGtZero) {
    long count = 0;
    for (int leaf = 0; leaf < fx.keys().length; leaf++) {
      for (int r = 0; r < fx.keys()[leaf].length; r++) {
        if (Arrays.binarySearch(sortedKeys, fx.keys()[leaf][r]) < 0) {
          continue;
        }
        if (longGtZero && !(fx.present()[leaf][r] && fx.longs()[leaf][r] > 0L)) {
          continue;
        }
        count++;
      }
    }
    return count;
  }

  private static ColumnPredicate[] shape(final long[] sortedKeys, final boolean longGtZero, final boolean keyFirst) {
    final ColumnPredicate keys = ColumnPredicate.recordKeysIn(sortedKeys);
    if (!longGtZero) {
      return new ColumnPredicate[] {keys};
    }
    final ColumnPredicate gt = ColumnPredicate.numeric(0, Op.GT, 0L);
    return keyFirst
        ? new ColumnPredicate[] {keys, gt}
        : new ColumnPredicate[] {gt, keys};
  }

  @Test
  void everyKernelFamilyAgreesWithTheBruteForceWalk() {
    for (final long seed : new long[] {3, 11, 20261008}) {
      for (final boolean exceptions : new boolean[] {false, true}) {
        final Fixture fx = buildFixture(seed, 6, exceptions);
        final Random rnd = new Random(seed * 31);
        final List<long[]> sets = new ArrayList<>();
        sets.add(new long[0]);
        sets.add(keySet(fx, rnd, 1, false)); // every stored key
        sets.add(keySet(fx, rnd, 3, true));
        sets.add(keySet(fx, rnd, 50, true));
        sets.add(new long[] {0L, Long.MAX_VALUE}); // stored nowhere
        for (final long[] set : sets) {
          for (final boolean withLong : new boolean[] {false, true}) {
            for (final boolean keyFirst : new boolean[] {false, true}) {
              if (!withLong && keyFirst) {
                continue;
              }
              final ColumnPredicate[] preds = shape(set, withLong, keyFirst);
              for (int predicate = 0; predicate < preds.length; predicate++) {
                if (preds[predicate].isRecordKeySet()) {
                  preds[predicate] = ColumnPredicate.recordKeysIn(set, fx.keys());
                }
              }
              final long expected = bruteCount(fx, set, withLong);
              final String at = "seed=" + seed + " exceptions=" + exceptions + " set=" + set.length + " long="
                  + withLong + " keyFirst=" + keyFirst;
              final int n = fx.store().rowGroupCount();
              assertEquals(expected, ProjectionColumnScan.conjunctiveCount(fx.store(), preds, fx.fetcher()),
                  "sliced resident " + at);
              assertEquals(expected, ProjectionColumnScan.conjunctiveCount(fx.store(), preds, 0, n, fx.fetcher()),
                  "sliced ranged " + at);
              final long[] keep = ProjectionColumnScan.predicateKeepMask(fx.store(), preds, fx.fetcher());
              assertEquals(expected, ProjectionColumnScan.conjunctiveCount(fx.store(), preds, 0, n,
                  fx.store().windowedLeafAccess(fx.fetcher(), keep, 2)), "sliced windowed " + at);
              assertEquals(expected, ProjectionIndexByteScan.conjunctiveCount(fx.rawLeaves(), preds),
                  "whole-leaf bytes " + at);
              // The same predicates as an AND tree, and the key set under an OR with the long test:
              // the tree evaluator must honour the row source per leaf, under both combiners.
              final PredicateTree andTree = preds.length == 1
                  ? PredicateTree.of(preds, new byte[] {0})
                  : PredicateTree.of(preds, new byte[] {0, 1, PredicateTree.OP_AND});
              assertEquals(expected, treeCount(fx, preds, andTree), "tree AND " + at);
              if (withLong) {
                final PredicateTree orTree = PredicateTree.of(preds, new byte[] {0, 1, PredicateTree.OP_OR});
                final long orExpected =
                    bruteCount(fx, set, false) + bruteCount(fx, allStoredKeys(fx), true) - bruteCount(fx, set, true);
                assertEquals(orExpected, treeCount(fx, preds, orTree), "tree OR " + at);
              }
            }
          }
        }
      }
    }
  }

  @Test
  void orderExceptionLeafWithAnEmptyExactMaskNeverRequestsBodies() throws Exception {
    for (final boolean prepared : new boolean[] {false, true}) {
      for (final boolean tree : new boolean[] {false, true}) {
        final byte[] kinds = {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
            ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG};
        final long[][] leafKeys = {{1, 1_000_000, 3}, {1_001}, {2_001}};
        final long[] values = {7, 2, 3};
        final boolean[] presence = {true, true, true};
        final List<RowGroupDirectory> directories = new ArrayList<>(leafKeys.length);
        final Long2ObjectOpenHashMap<byte[]> segments = new Long2ObjectOpenHashMap<>();
        final LongOpenHashSet excludedBodies = new LongOpenHashSet();
        final LongOpenHashSet keptBodies = new LongOpenHashSet();
        final LongOpenHashSet fetched = new LongOpenHashSet();
        long offset = 1;
        for (int leaf = 0; leaf < leafKeys.length; leaf++) {
          final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds.clone());
          for (int row = 0; row < leafKeys[leaf].length; row++) {
            page.appendExtractedUtf8Row(leafKeys[leaf][row], values, new boolean[kinds.length],
                new byte[kinds.length][], new int[kinds.length], null, presence, new boolean[kinds.length],
                new boolean[kinds.length], new boolean[kinds.length], leaf == 0 && row == 1);
          }
          final EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(page.serialize());
          final int[] ids = encoded.columnSegmentIds();
          final long[] offsets = new long[ids.length];
          for (int segment = 0; segment < ids.length; segment++) {
            offsets[segment] = offset;
            segments.put(offset, encoded.segments()[segment]);
            for (int column = 0; column < kinds.length; column++) {
              if (ids[segment] == ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(column)) {
                (leaf == 1
                    ? keptBodies
                    : excludedBodies).add(offset);
              }
            }
            offset++;
          }
          directories.add(new RowGroupDirectory(leaf + 1, encoded.descriptor(), ids, offsets, new byte[ids.length][]));
        }
        final ColumnSegmentFetcher fetcher = wanted -> {
          final byte[][] result = new byte[wanted.length][];
          for (int i = 0; i < wanted.length; i++) {
            assertFalse(excludedBodies.contains(wanted[i]), "a leaf without an exact match requested a BODY segment");
            fetched.add(wanted[i]);
            result[i] = segments.get(wanted[i]);
          }
          return result;
        };
        final ProjectionColumnStore store = new ProjectionColumnStore(directories);
        final ColumnPredicate predicate = prepared
            ? ColumnPredicate.recordKeysIn(new long[] {1_001}, store.recordKeys(fetcher))
            : ColumnPredicate.recordKeysIn(new long[] {1_001});
        final ColumnPredicate[] predicates = {predicate};
        final PredicateTree predicateTree = tree
            ? PredicateTree.of(predicates, new byte[] {0})
            : null;
        final ColumnPredicate[] flatPredicates = tree
            ? new ColumnPredicate[0]
            : predicates;
        final WorkCapture.Captured<long[]> read = WorkCapture.of(EngineWorkCounters.PROJECTION_LEAVES_PRUNED)
                                                             .and(EngineWorkCounters.PROJECTION_BODY_SEGMENTS)
                                                             .call(() -> {
                                                               final long[] keep =
                                                                   ProjectionColumnScan.predicateKeepMask(store,
                                                                       flatPredicates, predicateTree, fetcher);
                                                               for (int column = 0; column < kinds.length; column++) {
                                                                 final ColumnSlice[] slices =
                                                                     store.columnMaskedView(column, fetcher, keep);
                                                                 assertEquals(0, slices[0].rowCount());
                                                                 assertEquals(values[column],
                                                                     slices[1].numericValues()[0]);
                                                                 assertEquals(0, slices[2].rowCount());
                                                               }
                                                               return keep;
                                                             });
        assertArrayEquals(new long[] {2}, read.result());
        read.work()
            .assertExactly(EngineWorkCounters.PROJECTION_LEAVES_PRUNED, 2, "only the leaf holding key 1001 remains")
            .assertExactly(EngineWorkCounters.PROJECTION_BODY_SEGMENTS, kinds.length, "one BODY per requested column");
        assertTrue(fetched.containsAll(keptBodies), "the kept leaf proves BODY fetches were observed");
      }
    }
  }

  @Test
  void denseSourceMapsOnceAcrossAThousandLeaves() throws Exception {
    final int rows = 1_000_000;
    final long[] keys = new long[rows];
    final long[][] leaves = new long[1_000][];
    for (int leaf = 0; leaf < leaves.length; leaf++) {
      leaves[leaf] = new long[1_000];
      for (int row = 0; row < 1_000; row++) {
        final long key = leaf * 1_000L + row;
        keys[(int) key] = key;
        leaves[leaf][row] = key;
      }
    }
    final WorkCapture.Captured<ColumnPredicate> mapped =
        WorkCapture.of(EngineWorkCounters.PROJECTION_KEY_SET_ADVANCES)
                   .call(() -> ColumnPredicate.recordKeysIn(keys, leaves));
    mapped.work()
          .assertAtMost(EngineWorkCounters.PROJECTION_KEY_SET_ADVANCES, rows + leaves.length,
              "a dense source advances its key set once, independent of the leaf count");
    long members = 0;
    for (final long[] leaf : leaves) {
      for (final long word : mapped.result().keyMasks.get(leaf[0])) {
        members += Long.bitCount(word);
      }
    }
    assertEquals(rows, members);
  }

  private static long[] allStoredKeys(final Fixture fx) {
    final LongArrayList all = new LongArrayList();
    for (final long[] leafKeys : fx.keys()) {
      all.addElements(all.size(), leafKeys);
    }
    final long[] sorted = all.toLongArray();
    Arrays.sort(sorted);
    return sorted;
  }

  private static long treeCount(final Fixture fx, final ColumnPredicate[] leaves, final PredicateTree tree) {
    final ColumnPredicate[] none = new ColumnPredicate[0];
    final long[] keep = ProjectionColumnScan.predicateKeepMask(fx.store(), none, tree, fx.fetcher());
    final ColumnSlice[][] treeCols =
        ProjectionColumnScan.resolveTreeColumnsShared(fx.store(), tree, fx.fetcher(), keep);
    final ColumnSlice[][] predCols = ProjectionColumnScan.resolvePredicateColumnsShared(fx.store(), none, fx.fetcher());
    final int n = fx.store().rowGroupCount();
    return ProjectionColumnScan.rowKeepMasks(fx.store(), none, predCols, tree, treeCols, 0, n, new long[n][]);
  }

  @Test
  void keepMaskDropsExactlyTheLeavesWithoutAMember() {
    final Fixture fx = buildFixture(5, 8, true);
    final long[] set = keySet(fx, new Random(9), 20, true);
    final long[] keep = ProjectionColumnScan.predicateKeepMask(fx.store(),
        new ColumnPredicate[] {ColumnPredicate.recordKeysIn(set)}, fx.fetcher());
    boolean anyDropped = false;
    for (int leaf = 0; leaf < fx.keys().length; leaf++) {
      boolean member = false;
      for (final long key : fx.keys()[leaf]) {
        member |= Arrays.binarySearch(set, key) >= 0;
      }
      final boolean kept = keep == null || (keep[leaf >>> 6] & 1L << (leaf & 63)) != 0L;
      assertEquals(member, kept, "leaf " + leaf);
      anyDropped |= !kept;
    }
    assertTrue(anyDropped, "the rowless leaf at least must be dropped");
    // A set stored nowhere drops every leaf and reads no column: the pruned view is all sentinels.
    final long[] none = ProjectionColumnScan.predicateKeepMask(fx.store(),
        new ColumnPredicate[] {ColumnPredicate.recordKeysIn(new long[] {Long.MAX_VALUE})}, fx.fetcher());
    assertTrue(none != null && ProjectionColumnScan.allPruned(none, 0, fx.store().rowGroupCount()));
    for (final ColumnSlice slice : fx.store().recordKeyPredicateView(fx.fetcher(), none)) {
      assertEquals(0, slice.rowCount());
    }
  }

  @Test
  void membershipWalkHandlesOrderExceptionsAndEmptySets() {
    final long[] keys = {10, 12, 14, 7, 16, 18, 3, 20}; // 7 and 3 are order exceptions
    final long[] set = {3, 7, 12, 20, 99};
    final long[] mask = new long[1];
    Arrays.fill(mask, -1L);
    mask[0] = (1L << keys.length) - 1L;
    assertEquals(4, ProjectionRecordKeySet.andMembership(keys, keys.length, set, mask));
    assertEquals(0b10001010L | 1L << 6, mask[0]);
    // The incoming mask bounds the answer: a row already cleared stays cleared.
    mask[0] = 1L << 7;
    ProjectionRecordKeySet.andMembership(keys, keys.length, set, mask);
    assertEquals(1L << 7, mask[0]);
    mask[0] = -1L;
    assertEquals(0, ProjectionRecordKeySet.andMembership(keys, keys.length, new long[0], mask));
    assertEquals(0L, mask[0]);
    assertTrue(ProjectionRecordKeySet.anyIn(set, 8, 12));
    assertFalse(ProjectionRecordKeySet.anyIn(set, 8, 11));
    assertFalse(ProjectionRecordKeySet.anyIn(set, 100, 200));
    assertFalse(ProjectionRecordKeySet.anyIn(new long[0], 0, Long.MAX_VALUE));
  }

  @Test
  void constructionValidatesTheSet() {
    assertThrows(IllegalArgumentException.class, () -> ColumnPredicate.recordKeysIn(new long[] {2, 1}));
    assertThrows(IllegalArgumentException.class, () -> ColumnPredicate.recordKeysIn(new long[] {1, 1}));
    assertThrows(IllegalArgumentException.class, () -> ColumnPredicate.recordKeysIn(new long[] {-1, 1}));
    final ColumnPredicate p = ColumnPredicate.recordKeysIn(new long[] {1, 5, 9});
    assertTrue(p.isRecordKeySet());
    assertEquals(ProjectionColumnStore.KEYS_COLUMN, p.column);
    assertEquals(Op.KEY_IN, p.op);
    assertArrayEquals(new long[] {1, 5, 9}, p.sortedKeys);
    assertTrue(p.keySetHash != ColumnPredicate.recordKeysIn(new long[] {1, 5, 10}).keySetHash);
    // The page scan is the one evaluator that does not serve the row source: it refuses by name.
    final Fixture fx = buildFixture(1, 2, false);
    assertThrows(IllegalStateException.class,
        () -> ProjectionIndexScan.conjunctiveCount(fx.rawLeaves(), new ColumnPredicate[] {p}));
  }
}
