/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.compiler.translator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.cache.FrameSlotAllocator;
import io.sirix.node.SegmentAccess;
import io.sirix.query.compiler.translator.JoinKernels.BucketedSide;
import io.sirix.query.compiler.translator.JoinKernels.ExtremaFold;
import io.sirix.query.compiler.translator.JoinKernels.SideLanes;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The slab kernels of the dense extrema join against the heap-array kernels they replaced (the
 * pre-lane main branch, kept verbatim here as the reference): packing, the merge of two packed
 * sides and the chained fold of streamed lanes give the same per-group extrema and distinct counts
 * on random sides, over full, sparse and empty mask words, keys outside the domain, bounded and
 * unbounded distinct domains, and overflow.
 */
final class JoinKernelsTest {
  private static final int ROWS = 1024;

  /** A private allocator: the kernels take slabs, never the process allocator's state. */
  private final FrameSlotAllocator allocator = new FrameSlotAllocator(64L << 20);
  private final List<MemorySegment> slabs = new ArrayList<>();

  @AfterEach
  void releaseSlabs() {
    for (final MemorySegment slab : slabs) {
      allocator.releaseSlab(slab);
    }
    slabs.clear();
    assertEquals(0L, allocator.getSlabBytes(), "every slab went back");
    allocator.shutdown();
  }

  // ==================== the reference: heap kernels of the pre-lane main ====================

  /** One side as decoded heap arrays. */
  private static final class HeapSide {
    final long[][] masks;
    final long[][] join;
    final long[][] from;
    final long[][] to;
    final long[][] margin;
    final long[][] distinct;
    final long[][] group;

    HeapSide(final long[][] masks, final long[][] join, final long[][] from, final long[][] to, final long[][] margin,
        final long[][] distinct, final long[][] group) {
      this.masks = masks;
      this.join = join;
      this.from = from;
      this.to = to;
      this.margin = margin;
      this.distinct = distinct;
      this.group = group;
    }
  }

  private static final int STRIDE = 5;

  /** Main's bucketed side: {@code (from, to, margin, distinct, group)} records in a heap array. */
  private static final class HeapBucketed {
    final int[] bucketStart;
    final long[] rows;
    final int count;

    HeapBucketed(final HeapSide side, final long keyMinimum, final int slots, final long groupMinimum) {
      bucketStart = new int[slots + 1];
      int total = 0;
      for (int leaf = 0; leaf < side.masks.length; leaf++) {
        final long[] mask = side.masks[leaf];
        if (mask == null) {
          continue;
        }
        for (int w = 0; w < mask.length; w++) {
          long word = mask[w];
          while (word != 0L) {
            final int row = (w << 6) + Long.numberOfTrailingZeros(word);
            word &= word - 1L;
            final long slot = side.join[leaf][row] - keyMinimum;
            if (slot >= 0 && slot < slots) {
              bucketStart[(int) slot + 1]++;
              total++;
            }
          }
        }
      }
      for (int slot = 0; slot < slots; slot++) {
        bucketStart[slot + 1] += bucketStart[slot];
      }
      count = total;
      rows = new long[total * STRIDE];
      final int[] fill = Arrays.copyOf(bucketStart, slots);
      for (int leaf = 0; leaf < side.masks.length; leaf++) {
        final long[] mask = side.masks[leaf];
        if (mask == null) {
          continue;
        }
        for (int w = 0; w < mask.length; w++) {
          long word = mask[w];
          while (word != 0L) {
            final int row = (w << 6) + Long.numberOfTrailingZeros(word);
            word &= word - 1L;
            final long slot = side.join[leaf][row] - keyMinimum;
            if (slot < 0 || slot >= slots) {
              continue;
            }
            final int at = fill[(int) slot]++ * STRIDE;
            rows[at] = side.from[leaf][row];
            rows[at + 1] = side.to[leaf][row];
            rows[at + 2] = side.margin[leaf][row];
            rows[at + 3] = side.distinct == null
                ? 0L
                : side.distinct[leaf][row];
            rows[at + 4] = side.group == null
                ? 0L
                : side.group[leaf][row] - groupMinimum;
          }
        }
      }
    }
  }

  /** Main's flat fold over heap records. */
  private static final class HeapFold {
    final long[] groupMinimum;
    final long[] groupMaximum;
    final long[] distinctCounts;
    final long[] bits;
    final LongOpenHashSet[] sets;
    final long[] buildRows;
    final int groups;
    final long domainMinimum;
    final boolean distinctBuild;
    final boolean buildMinusProbe;

    HeapFold(final int groups, final long domainMinimum, final int domainWords, final long[] buildRows,
        final boolean distinctBuild, final boolean buildMinusProbe) {
      this.groups = groups;
      this.buildRows = buildRows;
      this.distinctBuild = distinctBuild;
      this.buildMinusProbe = buildMinusProbe;
      groupMinimum = new long[groups];
      groupMaximum = new long[groups];
      distinctCounts = new long[groups];
      Arrays.fill(groupMinimum, Long.MAX_VALUE);
      Arrays.fill(groupMaximum, Long.MIN_VALUE);
      this.domainMinimum = domainMinimum;
      bits = domainWords < 0
          ? null
          : new long[groups * domainWords];
      sets = domainWords < 0
          ? new LongOpenHashSet[groups]
          : null;
    }

    void pair(final long from, final long to, final long margin, final long probeValue, final int buildBegin,
        final int buildEnd) {
      for (int b = buildEnd - STRIDE; b >= buildBegin; b -= STRIDE) {
        if (from >= buildRows[b + 1] || buildRows[b] >= to) {
          continue;
        }
        final int group = (int) buildRows[b + 4];
        final long buildMargin = buildRows[b + 2];
        final long value = buildMinusProbe
            ? Math.subtractExact(buildMargin, margin)
            : Math.subtractExact(margin, buildMargin);
        groupMinimum[group] = Math.min(groupMinimum[group], value);
        groupMaximum[group] = Math.max(groupMaximum[group], value);
        final long distinctValue = distinctBuild
            ? buildRows[b + 3]
            : probeValue;
        if (bits != null) {
          final long bitIndex = (distinctValue - domainMinimum) * groups + group;
          final int bitWord = (int) (bitIndex >>> 6);
          final long bit = 1L << bitIndex;
          if ((bits[bitWord] & bit) == 0L) {
            bits[bitWord] |= bit;
            distinctCounts[group]++;
          }
        } else {
          if (sets[group] == null) {
            sets[group] = new LongOpenHashSet();
          }
          sets[group].add(distinctValue);
        }
      }
    }

    long distinctCount(final int group) {
      return bits != null
          ? distinctCounts[group]
          : sets[group] == null
              ? 0L
              : sets[group].size();
    }

    static void merge(final HeapFold fold, final HeapBucketed build, final HeapBucketed probe, final int slots) {
      for (int slot = 0; slot < slots; slot++) {
        final int buildBegin = build.bucketStart[slot] * STRIDE;
        final int buildEnd = build.bucketStart[slot + 1] * STRIDE;
        final int probeBegin = probe.bucketStart[slot] * STRIDE;
        final int probeEnd = probe.bucketStart[slot + 1] * STRIDE;
        if (buildBegin == buildEnd || probeBegin == probeEnd) {
          continue;
        }
        for (int p = probeBegin; p < probeEnd; p += STRIDE) {
          fold.pair(probe.rows[p], probe.rows[p + 1], probe.rows[p + 2], probe.rows[p + 3], buildBegin, buildEnd);
        }
      }
    }

    static int chained(final HeapFold fold, final HeapBucketed build, final HeapSide probe, final long keyMinimum,
        final int slots) {
      int admitted = 0;
      for (int leaf = 0; leaf < probe.masks.length; leaf++) {
        final long[] mask = probe.masks[leaf];
        if (mask == null) {
          continue;
        }
        for (int w = 0; w < mask.length; w++) {
          long word = mask[w];
          while (word != 0L) {
            final int row = (w << 6) + Long.numberOfTrailingZeros(word);
            word &= word - 1L;
            final long slot = probe.join[leaf][row] - keyMinimum;
            if (slot < 0 || slot >= slots) {
              continue;
            }
            admitted++;
            final int buildBegin = build.bucketStart[(int) slot] * STRIDE;
            final int buildEnd = build.bucketStart[(int) slot + 1] * STRIDE;
            if (buildBegin == buildEnd) {
              continue;
            }
            fold.pair(probe.from[leaf][row], probe.to[leaf][row], probe.margin[leaf][row], probe.distinct == null
                ? 0L
                : probe.distinct[leaf][row], buildBegin, buildEnd);
          }
        }
      }
      return admitted;
    }
  }

  // ==================== fixtures ====================

  /**
   * Random side: {@code leaves} leaves, every value in a small domain so buckets and pairs are dense.
   */
  private HeapSide side(final SplittableRandom random, final int leaves, final int keySpan, final double fullWords,
      final boolean distinct, final boolean group, final int groups) {
    final long[][] masks = new long[leaves][];
    final long[][] join = new long[leaves][];
    final long[][] from = new long[leaves][];
    final long[][] to = new long[leaves][];
    final long[][] margin = new long[leaves][];
    final long[][] distinctLane = distinct
        ? new long[leaves][]
        : null;
    final long[][] groupLane = group
        ? new long[leaves][]
        : null;
    for (int leaf = 0; leaf < leaves; leaf++) {
      if (random.nextInt(8) == 0) {
        continue; // a pruned leaf
      }
      final long[] mask = new long[ROWS >>> 6];
      for (int w = 0; w < mask.length; w++) {
        final double roll = random.nextDouble();
        mask[w] = roll < fullWords
            ? -1L
            : roll < fullWords + 0.1
                ? 0L
                : sparseWord(random);
      }
      masks[leaf] = mask;
      join[leaf] = lane(random, -3, keySpan + 3); // some keys outside the domain
      from[leaf] = lane(random, 0, 100);
      to[leaf] = new long[ROWS];
      for (int row = 0; row < ROWS; row++) {
        to[leaf][row] = from[leaf][row] + 1 + random.nextInt(60);
      }
      margin[leaf] = lane(random, -1000, 1000);
      if (distinctLane != null) {
        distinctLane[leaf] = lane(random, 5, 5 + 200);
      }
      if (groupLane != null) {
        groupLane[leaf] = lane(random, 7, 7 + groups);
      }
    }
    return new HeapSide(masks, join, from, to, margin, distinctLane, groupLane);
  }

  /** A word with about a quarter of its bits set: two independent draws conjoined. */
  private static long sparseWord(final SplittableRandom random) {
    final long first = random.nextLong();
    final long second = random.nextLong();
    return first & second;
  }

  private static long[] lane(final SplittableRandom random, final int low, final int highExclusive) {
    final long[] values = new long[ROWS];
    for (int row = 0; row < ROWS; row++) {
      values[row] = (long) low + random.nextInt(highExclusive - low);
    }
    return values;
  }

  /** The same side as slab lanes (one slab, 64-byte aligned lanes). */
  private SideLanes lanes(final HeapSide side) {
    final int leaves = side.masks.length;
    final long laneBytes = (8L * ROWS + 63L) & -64L;
    final MemorySegment slab = slab(laneBytes * 6L * leaves);
    final long[][] masks = new long[leaves][];
    final MemorySegment[] joinSeg = new MemorySegment[leaves];
    final long[] joinBase = new long[leaves];
    final MemorySegment[] fromSeg = new MemorySegment[leaves];
    final long[] fromBase = new long[leaves];
    final MemorySegment[] toSeg = new MemorySegment[leaves];
    final long[] toBase = new long[leaves];
    final MemorySegment[] marginSeg = new MemorySegment[leaves];
    final long[] marginBase = new long[leaves];
    final MemorySegment[] distinctSeg = side.distinct == null
        ? null
        : new MemorySegment[leaves];
    final long[] distinctBase = side.distinct == null
        ? null
        : new long[leaves];
    final MemorySegment[] groupSeg = side.group == null
        ? null
        : new MemorySegment[leaves];
    final long[] groupBase = side.group == null
        ? null
        : new long[leaves];
    long cursor = 0L;
    for (int leaf = 0; leaf < leaves; leaf++) {
      if (side.masks[leaf] == null) {
        continue;
      }
      masks[leaf] = side.masks[leaf].clone();
      cursor = place(slab, cursor, side.join[leaf], joinSeg, joinBase, leaf);
      cursor = place(slab, cursor, side.from[leaf], fromSeg, fromBase, leaf);
      cursor = place(slab, cursor, side.to[leaf], toSeg, toBase, leaf);
      cursor = place(slab, cursor, side.margin[leaf], marginSeg, marginBase, leaf);
      if (distinctSeg != null) {
        cursor = place(slab, cursor, side.distinct[leaf], distinctSeg, distinctBase, leaf);
      }
      if (groupSeg != null) {
        cursor = place(slab, cursor, side.group[leaf], groupSeg, groupBase, leaf);
      }
    }
    return new SideLanes(masks, joinSeg, joinBase, fromSeg, fromBase, toSeg, toBase, marginSeg, marginBase, distinctSeg,
        distinctBase, groupSeg, groupBase);
  }

  private static long place(final MemorySegment slab, final long cursor, final long[] values,
      final MemorySegment[] segments, final long[] bases, final int leaf) {
    MemorySegment.copy(values, 0, slab, ValueLayout.JAVA_LONG_UNALIGNED, cursor, values.length);
    segments[leaf] = slab;
    bases[leaf] = cursor;
    return cursor + ((8L * values.length + 63L) & -64L);
  }

  private MemorySegment slab(final long bytes) {
    final MemorySegment slab = allocator.allocateSlab(bytes);
    assertNotNull(slab, "the test allocator refused " + bytes + " bytes");
    slabs.add(slab);
    return slab;
  }

  private BucketedSide pack(final SideLanes lanes, final long keyMinimum, final int slots, final long groupMinimum) {
    final int[] bucketStart = new int[slots + 1];
    final int total = BucketedSide.count(lanes, keyMinimum, slots, bucketStart);
    final MemorySegment slab = slab(BucketedSide.slabBytes(slots, total));
    return BucketedSide.pack(slab, lanes, keyMinimum, slots, groupMinimum, bucketStart, total);
  }

  private static long[] extrema(final HeapFold fold) {
    final long[] out = new long[fold.groups * 3];
    for (int g = 0; g < fold.groups; g++) {
      out[3 * g] = fold.groupMinimum[g];
      out[3 * g + 1] = fold.groupMaximum[g];
      out[3 * g + 2] = fold.distinctCount(g);
    }
    return out;
  }

  private static long[] extrema(final ExtremaFold fold, final int groups) {
    final long[] out = new long[groups * 3];
    for (int g = 0; g < groups; g++) {
      out[3 * g] = fold.groupMinimum[g];
      out[3 * g + 1] = fold.groupMaximum[g];
      out[3 * g + 2] = fold.distinctCount(g);
    }
    return out;
  }

  // ==================== tests ====================

  @ParameterizedTest
  @ValueSource(doubles = {0.0, 0.5, 1.0})
  void packedSidesMatchTheHeapPacking(final double fullWords) {
    final SplittableRandom random = new SplittableRandom(Double.doubleToLongBits(fullWords) + 1);
    final int slots = 64;
    final int groups = 5;
    final HeapSide build = side(random, 6, slots, fullWords, true, true, groups);
    final HeapBucketed expected = new HeapBucketed(build, 0L, slots, 7L);
    final BucketedSide packed = pack(lanes(build), 0L, slots, 7L);
    assertEquals(expected.count, packed.count);
    for (int slot = 0; slot <= slots; slot++) {
      assertEquals(expected.bucketStart[slot], packed.bucketStart(slot), "bucket start " + slot);
    }
    // Records: the slab lays them out (from, to, margin, meta, distinct); the heap (.., distinct,
    // meta).
    for (int row = 0; row < expected.count; row++) {
      final long at = packed.rowsBase + row * BucketedSide.STRIDE_BYTES;
      assertEquals(expected.rows[row * STRIDE], SegmentAccess.getLong(packed.slab, at + BucketedSide.FROM));
      assertEquals(expected.rows[row * STRIDE + 1], SegmentAccess.getLong(packed.slab, at + BucketedSide.TO));
      assertEquals(expected.rows[row * STRIDE + 2], SegmentAccess.getLong(packed.slab, at + BucketedSide.MARGIN));
      assertEquals(expected.rows[row * STRIDE + 3], SegmentAccess.getLong(packed.slab, at + BucketedSide.DISTINCT));
      assertEquals(expected.rows[row * STRIDE + 4], SegmentAccess.getLong(packed.slab, at + BucketedSide.META));
    }
  }

  @ParameterizedTest
  @ValueSource(doubles = {0.0, 0.5, 1.0})
  void mergeAndChainedFoldsMatchTheHeapFoldsForEveryOperandPlacement(final double fullWords) {
    for (final boolean distinctBuild : new boolean[] {true, false}) {
      for (final boolean buildMinusProbe : new boolean[] {true, false}) {
        for (final boolean bounded : new boolean[] {true, false}) {
          assertFoldsAgree(fullWords, distinctBuild, buildMinusProbe, bounded);
        }
      }
    }
  }

  private void assertFoldsAgree(final double fullWords, final boolean distinctBuild, final boolean buildMinusProbe,
      final boolean boundedDomain) {
    final SplittableRandom random = new SplittableRandom(Double.doubleToLongBits(fullWords) * 31 + (distinctBuild
        ? 1
        : 0) + (buildMinusProbe
            ? 2
            : 0)
        + (boundedDomain
            ? 4
            : 0));
    final int slots = 48;
    final int groups = 6;
    final HeapSide build = side(random, 4, slots, fullWords, distinctBuild, true, groups);
    final HeapSide probe = side(random, 7, slots, fullWords, !distinctBuild, false, groups);
    final long domainMinimum = 5L;
    final int domainWords = boundedDomain
        ? 4
        : -1;
    // Reference.
    final HeapBucketed heapBuild = new HeapBucketed(build, 0L, slots, 7L);
    final HeapBucketed heapProbe = new HeapBucketed(probe, 0L, slots, 0L);
    final HeapFold mergeReference =
        new HeapFold(groups, domainMinimum, domainWords, heapBuild.rows, distinctBuild, buildMinusProbe);
    HeapFold.merge(mergeReference, heapBuild, heapProbe, slots);
    final HeapFold chainedReference =
        new HeapFold(groups, domainMinimum, domainWords, heapBuild.rows, distinctBuild, buildMinusProbe);
    final int admittedReference = HeapFold.chained(chainedReference, heapBuild, probe, 0L, slots);
    assertArrayEquals(extrema(mergeReference), extrema(chainedReference), "the reference folds agree");
    // Shipped kernels.
    final SideLanes buildLanes = lanes(build);
    final SideLanes probeLanes = lanes(probe);
    final BucketedSide packedBuild = pack(buildLanes, 0L, slots, 7L);
    final BucketedSide packedProbe = pack(probeLanes, 0L, slots, 0L);
    final ExtremaFold merged = new ExtremaFold(groups, boundedDomain
        ? new JoinKernels.DistinctDomain(domainMinimum, domainWords)
        : null, packedBuild, distinctBuild, buildMinusProbe);
    JoinKernels.foldMerge(merged, packedBuild, packedProbe, slots);
    assertArrayEquals(extrema(mergeReference), extrema(merged, groups), "merge of packed sides");
    final ExtremaFold chained = new ExtremaFold(groups, boundedDomain
        ? new JoinKernels.DistinctDomain(domainMinimum, domainWords)
        : null, packedBuild, distinctBuild, buildMinusProbe);
    final int admitted = JoinKernels.foldChained(chained, packedBuild, probeLanes, 0L, slots);
    assertEquals(admittedReference, admitted, "admitted probe rows");
    assertArrayEquals(extrema(chainedReference), extrema(chained, groups), "chained fold of streamed lanes");
  }

  @Test
  void anOverflowingMarginFailsExactlyLikeTheHeapFold() {
    final SplittableRandom random = new SplittableRandom(99);
    final int slots = 4;
    final HeapSide build = side(random, 1, slots, 1.0, false, true, 2);
    final HeapSide probe = side(random, 1, slots, 1.0, true, false, 2);
    build.margin[0][0] = Long.MAX_VALUE;
    probe.margin[0][0] = -10L;
    // Make row 0 of both sides pair: same key, overlapping.
    build.join[0][0] = 1L;
    probe.join[0][0] = 1L;
    build.from[0][0] = 0L;
    build.to[0][0] = 50L;
    probe.from[0][0] = 10L;
    probe.to[0][0] = 20L;
    final HeapBucketed heapBuild = new HeapBucketed(build, 0L, slots, 7L);
    final HeapBucketed heapProbe = new HeapBucketed(probe, 0L, slots, 0L);
    final HeapFold reference = new HeapFold(2, 5L, 4, heapBuild.rows, false, true);
    assertThrows(ArithmeticException.class, () -> HeapFold.merge(reference, heapBuild, heapProbe, slots));
    final BucketedSide packedBuild = pack(lanes(build), 0L, slots, 7L);
    final BucketedSide packedProbe = pack(lanes(probe), 0L, slots, 0L);
    final ExtremaFold fold = new ExtremaFold(2, new JoinKernels.DistinctDomain(5L, 4), packedBuild, false, true);
    assertThrows(ArithmeticException.class, () -> JoinKernels.foldMerge(fold, packedBuild, packedProbe, slots));
  }

  @Test
  void aNativeViewAddressesTheSlabsBytesAtTheSameOffsets() {
    final MemorySegment slab = slab(64L * 1024);
    final SplittableRandom random = new SplittableRandom(11);
    final long[] values = new long[ROWS];
    for (int r = 0; r < ROWS; r++) {
      values[r] = random.nextLong();
    }
    final long base = 4096L + 64L; // a lane at an aligned base past the first page, as a layout places it
    MemorySegment.copy(values, 0, slab, ValueLayout.JAVA_LONG_UNALIGNED, base, ROWS);
    final MemorySegment view = JoinKernels.nativeView(slab);
    assertNotSame(slab, view, "a fresh view, so the compiler sees its construction");
    assertTrue(view.isNative());
    assertEquals(slab.address(), view.address(), "same address");
    assertEquals(slab.byteSize(), view.byteSize(), "same size");
    for (int r = 0; r < ROWS; r++) {
      assertEquals(values[r], (long) JoinKernels.SLAB_LONG.get(view, base + 8L * r), "row " + r);
    }
    final long offsetStraddlingTheEnd = slab.byteSize() - 4L;
    assertThrows(IndexOutOfBoundsException.class, () -> JoinKernels.SLAB_LONG.get(view, offsetStraddlingTheEnd),
        "the view keeps the slab's bounds");
    // The view is a reader's handle, never the allocation: releasing through it is rejected, and the
    // slab goes back through the instance the allocator handed out (releaseSlabs).
    assertFalse(allocator.releaseSlab(view));
    assertEquals(0L, JoinKernels.nativeView(MemorySegment.NULL).byteSize(), "the NULL segment views as an empty one");
  }

  @Test
  void sortRowsOrdersLexicographicallyWithStableTiesLikeTheComparatorDid() {
    final SplittableRandom random = new SplittableRandom(17);
    final int count = 5_000;
    final long[][] columns = new long[4][count + 7]; // columns may be longer than the row count
    for (int i = 0; i < count; i++) {
      // Few distinct leading keys, so the later columns decide most ties; the extremes included.
      columns[0][i] = switch (random.nextInt(8)) {
        case 0 -> Long.MIN_VALUE;
        case 1 -> Long.MAX_VALUE;
        case 2 -> -1L;
        default -> random.nextLong(-3L, 4L);
      };
      columns[1][i] = ~random.nextLong(0L, 50L); // a key the caller stores inverted to sort descending
      columns[2][i] = random.nextLong(0L, 100L);
      columns[3][i] = random.nextLong(); // unique in practice: the record key
    }
    final Integer[] reference = new Integer[count];
    for (int i = 0; i < count; i++) {
      reference[i] = i;
    }
    Arrays.sort(reference, (a, b) -> {
      for (int k = 0; k < 4; k++) {
        final int c = Long.compare(columns[k][a], columns[k][b]);
        if (c != 0) {
          return c;
        }
      }
      return Integer.compare(a, b); // stability: equal keys keep insertion order
    });
    final int[] order = JoinKernels.sortRows(columns, count);
    assertEquals(count, order.length);
    for (int i = 0; i < count; i++) {
      assertEquals(reference[i].intValue(), order[i], "position " + i);
    }
    // The inverted column really sorts descending: the original values decrease along the order
    // wherever column 0 ties.
    for (int i = 1; i < count; i++) {
      if (columns[0][order[i - 1]] == columns[0][order[i]]) {
        assertTrue(~columns[1][order[i - 1]] >= ~columns[1][order[i]], "descending key at " + i);
      }
    }
    assertEquals(0, JoinKernels.sortRows(columns, 0).length);
    assertThrows(IllegalArgumentException.class, () -> JoinKernels.sortRows(new long[][] {new long[3]}, 4));
  }

  @Test
  void aHeapSegmentHasNoViewAndIsReturnedAsItIs() {
    final MemorySegment heap = MemorySegment.ofArray(new long[] {1L, 2L, 3L});
    assertSame(heap, JoinKernels.nativeView(heap));
  }

  @Test
  void aSlabSmallerThanThePackedSideIsRefused() {
    final SplittableRandom random = new SplittableRandom(5);
    final int slots = 8;
    final HeapSide build = side(random, 2, slots, 1.0, false, true, 2);
    final SideLanes lanes = lanes(build);
    final int[] bucketStart = new int[slots + 1];
    final int total = BucketedSide.count(lanes, 0L, slots, bucketStart);
    assertTrue(BucketedSide.slabBytes(slots, total) > 4096L, "the side needs more than one slab page");
    final MemorySegment small = slab(4096L); // slabs round up to whole pages: one page is too small
    final int[] bucketStartCopy = bucketStart.clone();
    assertThrows(IllegalArgumentException.class,
        () -> BucketedSide.pack(small, lanes, 0L, slots, 7L, bucketStartCopy, total));
  }
}
