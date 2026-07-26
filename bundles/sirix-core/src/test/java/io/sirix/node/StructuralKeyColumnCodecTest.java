/*
 * Copyright (c) 2026, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.node;

import io.sirix.settings.Fixed;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates the column codec (round-trip correctness) and measures the compression
 * ratio vs the baseline per-slot {@code delta(value, nodeKey)} varint encoding that
 * flyweight nodes use today.
 */
final class StructuralKeyColumnCodecTest {

  private static final long NULL = Fixed.NULL_NODE_KEY.getStandardProperty();

  @Test
  void emptyColumn() {
    final long[] values = new long[0];
    final byte[] buf = new byte[16];
    final int written = StructuralKeyColumnCodec.encodeByteArray(buf, 0, values);
    assertEquals(3, written);
    assertEquals(3, StructuralKeyColumnCodec.encodedSize(values));
  }

  @Test
  void allNull() {
    final long[] values = repeat(NULL, 500);
    assertEquals(3, StructuralKeyColumnCodec.encodedSize(values));
    final byte[] buf = new byte[16];
    StructuralKeyColumnCodec.encodeByteArray(buf, 0, values);
    for (int i = 0; i < values.length; i++) {
      assertEquals(NULL, StructuralKeyColumnCodec.decodeSlot(buf, 0, i));
    }
  }

  @Test
  void constant() {
    final long[] values = repeat(42L, 500);
    assertEquals(11, StructuralKeyColumnCodec.encodedSize(values));
    final byte[] buf = new byte[32];
    StructuralKeyColumnCodec.encodeByteArray(buf, 0, values);
    for (int i = 0; i < values.length; i++) {
      assertEquals(42L, StructuralKeyColumnCodec.decodeSlot(buf, 0, i));
    }
  }

  @Test
  void monotonicPlusOne() {
    final long[] values = new long[500];
    for (int i = 0; i < values.length; i++) values[i] = 1000L + i;
    assertEquals(11, StructuralKeyColumnCodec.encodedSize(values));
    final byte[] buf = new byte[32];
    StructuralKeyColumnCodec.encodeByteArray(buf, 0, values);
    for (int i = 0; i < values.length; i++) {
      assertEquals(1000L + i, StructuralKeyColumnCodec.decodeSlot(buf, 0, i));
    }
  }

  @Test
  void roundTripGeneralMixed() {
    final long[] values = { 100L, 100L, 100L, 105L, 105L, 105L, 105L, 200L, 200L, 201L, NULL, NULL, 300L };
    final byte[] buf = new byte[128];
    final int written = StructuralKeyColumnCodec.encodeByteArray(buf, 0, values);
    assertTrue(written > 3 && written < 64, "Unexpected encoded size: " + written);
    for (int i = 0; i < values.length; i++) {
      assertEquals(values[i], StructuralKeyColumnCodec.decodeSlot(buf, 0, i),
          "slot " + i);
    }
  }

  @Test
  void roundTripMemorySegment() {
    final long[] values = { 1000L, 1000L, 1001L, 1001L, NULL, 2000L, 2000L, 2000L };
    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment segment = arena.allocate(128);
      final int written = StructuralKeyColumnCodec.encode(segment, 0L, values);
      assertTrue(written > 0);
      for (int i = 0; i < values.length; i++) {
        assertEquals(values[i], StructuralKeyColumnCodec.decodeSlot(segment, 0L, i), "slot " + i);
      }
    }
  }

  @Test
  void dryRunMatchesActualSize() {
    final Random rnd = new Random(0xC0DEC);
    for (int trial = 0; trial < 50; trial++) {
      final int n = 1 + rnd.nextInt(500);
      final long[] values = syntheticDfsParentKeys(rnd, n);
      final int predicted = StructuralKeyColumnCodec.encodedSize(values);
      final byte[] buf = new byte[predicted + 16];
      final int actual = StructuralKeyColumnCodec.encodeByteArray(buf, 0, values);
      assertEquals(predicted, actual, "trial " + trial + " n=" + n);
    }
  }

  /**
   * Compression ratio vs the current per-slot baseline.
   *
   * <p>Baseline = what {@link DeltaVarIntCodec#writeDeltaToSegment} produces today:
   * {@code zigzagVarint(parentKey - nodeKey)} per slot. Baseline is already tight on
   * DFS data because {@code parentKey - nodeKey} fits in 1 byte for typical fan-out.
   *
   * <p>The column codec modestly beats the baseline (≈1.3×) on a synthetic DFS
   * parentKey column — main wins come from sibling-chain repetition compressed to
   * 1 bit per slot. Use this asserted floor as a regression guard, not a marketing
   * target; the big leverage is in all-null and constant columns (see separate tests).
   */
  @Test
  void compressionRatioOnDfsParentKeyColumn() {
    final Random rnd = new Random(0xDF5);
    final int slotsPerPage = 500;
    long sumBaseline = 0;
    long sumColumn = 0;
    for (int page = 0; page < 20; page++) {
      final long[] parents = syntheticDfsParentKeys(rnd, slotsPerPage);
      sumBaseline += baselinePerSlotBytes(parents);
      sumColumn += StructuralKeyColumnCodec.encodedSize(parents);
    }
    final double ratio = (double) sumBaseline / sumColumn;
    System.out.printf("DFS parentKey: baseline=%d bytes, column=%d bytes, ratio=%.2fx%n",
        sumBaseline, sumColumn, ratio);
    assertTrue(ratio >= 1.2,
        "Expected >= 1.2x compression on DFS parentKey; got " + ratio + "x "
        + "(baseline=" + sumBaseline + " column=" + sumColumn + ")");
  }

  /**
   * {@code firstChildKey} column: ~70% NULL (leaves). The partial-NULL pattern gives
   * modest gains because {@code zigzag(NULL - slotIndex)} in the baseline compresses
   * to ~2 bytes, while the column stores 1 bit + an occasional varint.
   */
  @Test
  void compressionRatioOnDfsFirstChildColumn() {
    final Random rnd = new Random(0xFC1);
    final int slotsPerPage = 500;
    long sumBaseline = 0;
    long sumColumn = 0;
    for (int page = 0; page < 20; page++) {
      final long[] children = syntheticDfsFirstChildKeys(rnd, slotsPerPage);
      sumBaseline += baselinePerSlotBytes(children);
      sumColumn += StructuralKeyColumnCodec.encodedSize(children);
    }
    final double ratio = (double) sumBaseline / sumColumn;
    System.out.printf("DFS firstChildKey: baseline=%d bytes, column=%d bytes, ratio=%.2fx%n",
        sumBaseline, sumColumn, ratio);
    assertTrue(ratio >= 1.3,
        "Expected >= 1.3x compression on DFS firstChildKey; got " + ratio + "x");
  }

  /**
   * {@code leftSiblingKey}: current baseline is already ~1 byte/slot because
   * {@code leftSib = slot - 1} zig-zags to a tiny varint. The column codec doesn't
   * beat that in the mixed case. This test asserts we're within 10% of baseline —
   * the codec should never blow up the column size catastrophically even when it
   * doesn't help.
   */
  @Test
  void compressionRatioOnLeftSiblingColumn() {
    final Random rnd = new Random(0x51B);
    final int slotsPerPage = 500;
    long sumBaseline = 0;
    long sumColumn = 0;
    for (int page = 0; page < 20; page++) {
      final long[] leftSib = syntheticDfsLeftSiblings(rnd, slotsPerPage);
      sumBaseline += baselinePerSlotBytes(leftSib);
      sumColumn += StructuralKeyColumnCodec.encodedSize(leftSib);
    }
    final double ratio = (double) sumBaseline / sumColumn;
    System.out.printf("DFS leftSiblingKey: baseline=%d bytes, column=%d bytes, ratio=%.2fx%n",
        sumBaseline, sumColumn, ratio);
    assertTrue(ratio >= 0.9,
        "Column codec should not bloat leftSiblingKey by more than 10%; got " + ratio + "x");
  }

  /**
   * The big win: an all-null column collapses to the 3-byte fixed header regardless
   * of N. For a 500-slot all-null column this is a ~166× ratio.
   */
  @Test
  void compressionRatioOnAllNullColumn() {
    final long[] values = repeat(NULL, 500);
    final int baseline = baselinePerSlotBytes(values);
    final int column = StructuralKeyColumnCodec.encodedSize(values);
    final double ratio = (double) baseline / column;
    System.out.printf("All-null 500 slots: baseline=%d bytes, column=%d bytes, ratio=%.2fx%n",
        baseline, column, ratio);
    assertTrue(ratio >= 100.0,
        "Expected >= 100x on all-null column; got " + ratio + "x");
  }

  /**
   * Constant column (all slots point at the same parent — e.g. all 500 array elements
   * of the root array): 11 bytes regardless of N. For 500 slots this is a ~45× ratio.
   */
  @Test
  void compressionRatioOnConstantColumn() {
    final long[] values = repeat(7L, 500);
    final int baseline = baselinePerSlotBytes(values);
    final int column = StructuralKeyColumnCodec.encodedSize(values);
    final double ratio = (double) baseline / column;
    System.out.printf("Constant 500 slots: baseline=%d bytes, column=%d bytes, ratio=%.2fx%n",
        baseline, column, ratio);
    assertTrue(ratio >= 40.0,
        "Expected >= 40x on constant column; got " + ratio + "x");
  }

  // ==================== Synthetic DFS workload ====================

  /**
   * Approximates the parentKey column of a KVL page after a DFS shred: children follow
   * their parent in key order, sibling groups share parents, and the tree has a
   * typical fan-out of 2-8.
   */
  private static long[] syntheticDfsParentKeys(final Random rnd, final int n) {
    final long[] parents = new long[n];
    // slot 0: root — parent is NULL
    parents[0] = NULL;
    // Maintain a stack of (nodeKey, remainingChildren) entries to model DFS descent.
    final List<long[]> stack = new ArrayList<>();
    stack.add(new long[] { 0L, 4 });
    for (int i = 1; i < n; i++) {
      while (!stack.isEmpty() && stack.get(stack.size() - 1)[1] <= 0) {
        stack.remove(stack.size() - 1);
      }
      if (stack.isEmpty()) {
        parents[i] = NULL;
        stack.add(new long[] { i, 2 + rnd.nextInt(6) });
        continue;
      }
      final long[] top = stack.get(stack.size() - 1);
      parents[i] = top[0];
      top[1]--;
      // 30% chance this node has children — push new frame.
      if (rnd.nextInt(10) < 3) {
        stack.add(new long[] { i, 1 + rnd.nextInt(4) });
      }
    }
    return parents;
  }

  /** firstChildKey column: ~70% null (leaves), rest point at the next slot. */
  private static long[] syntheticDfsFirstChildKeys(final Random rnd, final int n) {
    final long[] fc = new long[n];
    for (int i = 0; i < n; i++) {
      fc[i] = rnd.nextInt(10) < 7 ? NULL : (i + 1L);
    }
    return fc;
  }

  /** leftSiblingKey column: a chain inside each sibling group, NULL at group heads. */
  private static long[] syntheticDfsLeftSiblings(final Random rnd, final int n) {
    final long[] ls = new long[n];
    ls[0] = NULL;
    for (int i = 1; i < n; i++) {
      // 60% of slots are the left neighbor (sibling)
      ls[i] = rnd.nextInt(10) < 6 ? (long) (i - 1) : NULL;
    }
    return ls;
  }

  // ==================== Baseline size model ====================

  /**
   * Size of the current encoding: one zig-zag varint of {@code (value - nodeKey)}
   * per slot, where {@code nodeKey = slotIndex}. Matches what
   * {@link DeltaVarIntCodec#writeDeltaToSegment} writes on the flyweight hot path.
   */
  private static int baselinePerSlotBytes(final long[] values) {
    int total = 0;
    for (int i = 0; i < values.length; i++) {
      total += zigzagVarintSize(values[i] - i);
    }
    return total;
  }

  private static int zigzagVarintSize(final long v) {
    final long zz = (v << 1) ^ (v >> 63);
    if (zz == 0) return 1;
    final int bits = 64 - Long.numberOfLeadingZeros(zz);
    return (bits + 6) / 7;
  }

  private static long[] repeat(final long value, final int n) {
    final long[] arr = new long[n];
    java.util.Arrays.fill(arr, value);
    return arr;
  }

  @Test
  void repeatHelperSanity() {
    assertArrayEquals(new long[] { 7L, 7L, 7L }, repeat(7L, 3));
  }
}
