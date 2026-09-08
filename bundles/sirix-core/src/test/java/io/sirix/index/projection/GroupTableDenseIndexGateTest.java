/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * WHICH shapes a spill hands the dense record layout to.
 *
 * <p>
 * The two layouts are not ordered: at the 3/4 growth threshold the interleaved form reserves a
 * whole stripe in every bucket and so holds {@code (4/3) * stride} lanes per live group, while the
 * dense form packs the stripes and adds one index lane per bucket, holding {@code stride + 4/3}.
 * Those cross at four lanes. Below the crossing the dense form holds MORE memory AND charges a
 * second dependent load per probe to prove the key behind the index hit, so the gate must not admit
 * it there — {@code GROUP BY x ORDER BY count(*)} is exactly such a shape at three lanes.
 * </p>
 */
final class GroupTableDenseIndexGateTest {

  private static final int PARTITIONS = 32;
  private static final int SHIFT = 59;
  /** Well under the growth threshold of the hint below, so no rehash moves an interleaved handle. */
  private static final int GROUPS = 512;
  private static final int HINT = 1 << 12;

  @Test
  @DisplayName("the threshold IS the lane crossing: dense holds fewer lanes exactly from five on")
  void theThresholdIsTheLaneCrossing() {
    for (int stride = 1; stride <= 64; stride++) {
      // (4/3) * stride against stride + 4/3, cleared of the denominator: 4 * stride vs 3 * stride + 4.
      final boolean denseHoldsFewerLanes = 4 * stride > 3 * stride + 4;
      assertEquals(denseHoldsFewerLanes, stride >= GroupTableSpill.DENSE_INDEX_MIN_STRIDE,
          "stride " + stride + ": interleaved " + 4 * stride + "/3 lanes per group, dense " + (3 * stride + 4) + "/3");
    }
    assertEquals(3, NumericGroupAggTable.strideFor(0, false, 0), "GROUP BY x ORDER BY count(*) is three lanes");
    assertTrue(3 < GroupTableSpill.DENSE_INDEX_MIN_STRIDE, "and three lanes is below the crossing");
  }

  @Test
  @DisplayName("stripes at or below the crossing keep interleaved buckets")
  void narrowStripesKeepInterleavedBuckets() {
    assertLayout(0, false, 0, false);
    assertLayout(0, true, 0, false);
  }

  @Test
  @DisplayName("stripes above the crossing get dense records behind the compact index")
  void wideStripesGetDenseRecords() {
    assertLayout(0, true, 1, true);
    assertLayout(1, false, 0, true);
    assertLayout(2, true, 2, true);
  }

  @Test
  @DisplayName("both layouts fold the same rows to the same counts, aux and identity")
  void theLayoutsAgreeOnEveryGroupAndTheKillSwitchSelectsBetweenThem() {
    final long[] keys = keys();
    final NumericGroupAggTable dense = workerTable(1, true, 1);
    final NumericGroupAggTable interleaved = withDenseIndexDisabled(() -> workerTable(1, true, 1));
    assertTrue(handlesAreInsertionOrdinals(fold(dense, keys, true)), "a nine-lane stripe is dense by default");
    final int[] interleavedHandles = fold(interleaved, keys, true);
    assertTrue(handlesAreTheirOwnBuckets(interleaved, keys, interleavedHandles), "the kill switch restores buckets");
    assertFalse(handlesAreInsertionOrdinals(interleavedHandles));

    assertEquals(keys.length, dense.size());
    assertEquals(dense.size(), interleaved.size());
    for (int group = 0; group < keys.length; group++) {
      final int denseHandle = dense.acquireExact(keys[group], 0L, identity(keys[group]), 0);
      final int otherHandle = interleaved.acquireExact(keys[group], 0L, identity(keys[group]), 0);
      assertEquals(keys[group], dense.keyAtAccBase(denseHandle));
      assertEquals(keys[group], interleaved.keyAtAccBase(otherHandle));
      assertArrayEquals(accumulator(interleaved, otherHandle), accumulator(dense, denseHandle), "group " + group);
      assertEquals(interleaved.auxAtAccBase(otherHandle), dense.auxAtAccBase(denseHandle), "aux of group " + group);
      assertEquals(interleaved.identityAtAccBase(otherHandle, 0), dense.identityAtAccBase(denseHandle, 0),
          "identity of group " + group);
    }
    assertEquals(keys.length, dense.size(), "no group was created by the verification probes");
    assertEquals(keys.length, interleaved.size());
  }

  private static void assertLayout(final int aggColumns, final boolean withAux, final int idWidth,
      final boolean expectDense) {
    final int stride = NumericGroupAggTable.strideFor(aggColumns, withAux, idWidth);
    assertEquals(expectDense, stride >= GroupTableSpill.DENSE_INDEX_MIN_STRIDE, "shape of " + stride + " lanes");
    final NumericGroupAggTable table = workerTable(aggColumns, withAux, idWidth);
    assertEquals(stride, table.stride());
    final long[] keys = keys();
    final int[] handles = fold(table, keys, withAux);
    assertEquals(keys.length, table.size());
    assertEquals(expectDense, handlesAreInsertionOrdinals(handles),
        "stride " + stride + ": dense handles are record ordinals");
    assertEquals(!expectDense, handlesAreTheirOwnBuckets(table, keys, handles),
        "stride " + stride + ": an interleaved handle IS the bucket holding its key");
    for (int group = 0; group < keys.length; group++) {
      final int handle = table.acquireExact(keys[group], 0L, identity(keys[group]), 0);
      assertEquals(handles[group], handle, "stride " + stride + ": group " + group + " re-probes to its record");
      assertEquals(group + 1L, table.storageAtAccBase(handle)[table.offsetAtAccBase(handle)],
          "stride " + stride + ": group " + group + " folded its rows");
    }
  }

  /** A worker table exactly as a spill of this shape hands it out. */
  private static NumericGroupAggTable workerTable(final int aggColumns, final boolean withAux, final int idWidth) {
    final Supplier<NumericGroupAggTable> factory =
        () -> new NumericGroupAggTable(aggColumns, HINT, withAux, -1L, idWidth);
    return new GroupTableSpill(PARTITIONS, SHIFT, factory).freshLocal();
  }

  private static <T> T withDenseIndexDisabled(final Supplier<T> body) {
    final String previous = System.setProperty(GroupTableSpill.DENSE_INDEX_PROPERTY, "false");
    try {
      return body.get();
    } finally {
      if (previous == null) {
        System.clearProperty(GroupTableSpill.DENSE_INDEX_PROPERTY);
      } else {
        System.setProperty(GroupTableSpill.DENSE_INDEX_PROPERTY, previous);
      }
    }
  }

  /**
   * Distinct non-zero probe keys, scattered so neither layout's handles are the other's by accident.
   */
  private static long[] keys() {
    final long[] keys = new long[GROUPS];
    for (int group = 0; group < GROUPS; group++) {
      keys[group] = 0x9E37_79B9_7F4A_7C15L * (group + 1);
    }
    return keys;
  }

  private static long[] identity(final long key) {
    return new long[] {key, ~key};
  }

  /**
   * Fold {@code group + 1} rows into each group. The aux lane is stamped only where the shape has
   * one: without it, the lane one slotWidth above the accumulator base is the NEXT stripe's key.
   */
  private static int[] fold(final NumericGroupAggTable table, final long[] keys, final boolean withAux) {
    final int[] handles = new int[keys.length];
    for (int group = 0; group < keys.length; group++) {
      final long[] identity = identity(keys[group]);
      for (int row = 0; row <= group; row++) {
        final int handle = table.acquireExact(keys[group], group, identity, 0);
        if (row == 0 && withAux) {
          table.setAuxAtAccBase(handle, ~(long) group);
        }
        table.storageAtAccBase(handle)[table.offsetAtAccBase(handle)]++;
        handles[group] = handle;
      }
    }
    return handles;
  }

  private static boolean handlesAreInsertionOrdinals(final int[] handles) {
    for (int i = 0; i < handles.length; i++) {
      if (handles[i] != i) {
        return false;
      }
    }
    return true;
  }

  private static boolean handlesAreTheirOwnBuckets(final NumericGroupAggTable table, final long[] keys,
      final int[] handles) {
    for (int i = 0; i < keys.length; i++) {
      if (table.keyAtBucket(handles[i]) != keys[i] || table.accBaseOfBucket(handles[i]) != handles[i]) {
        return false;
      }
    }
    return true;
  }

  private static long[] accumulator(final NumericGroupAggTable table, final int handle) {
    final long[] block = new long[table.slotWidth()];
    System.arraycopy(table.storageAtAccBase(handle), table.offsetAtAccBase(handle), block, 0, block.length);
    return block;
  }
}
