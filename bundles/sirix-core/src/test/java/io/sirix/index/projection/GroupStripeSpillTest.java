package io.sirix.index.projection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The STRIPE spill: a flush copies a worker table's live stripes into per-partition append buffers
 * and the partition's table is built from them, instead of probing every flushed group into a
 * shared table mid-scan.
 *
 * <p>
 * What has to hold: the groups that come out are the ones the table spill produces, lane for lane
 * (counts, exact sums, extrema, first-seen ordinals, first-arrival aux, identity-separated
 * same-probe-key groups, the zero group); the buffers are bounded by a compaction rather than
 * holding a group once per worker flush; and an abort is judged on DEDUPLICATED groups, never on
 * buffered stripes.
 */
final class GroupStripeSpillTest {

  private static final int PARTITIONS = 32;
  private static final int SHIFT = 64 - 5;

  /**
   * One aggregate column, aux on: stripe = key + [count, firstSeen, present, sum, min, max] + aux.
   */
  private static NumericGroupAggTable table(final int hint, final int idWidth) {
    return new NumericGroupAggTable(1, hint, true, 0L, idWidth);
  }

  /**
   * Fold one row {@code (key, value)} into {@code local}, stamping aux on first sight — group value
   * {@code 0} through the side slot, which is where it lives (a zero key lane reads as an empty
   * bucket), exactly as the kernels route it.
   */
  private static void fold(final NumericGroupAggTable local, final long key, final long value, final long ordinal) {
    final GroupBuffer chunk;
    final int base;
    if (key == 0L) {
      chunk = local.acquireZero(ordinal);
      base = 0;
      if (GroupLanes.get(chunk, 0) == 0L) {
        local.setZeroAux(ordinal);
      }
    } else {
      final int handle = local.acquire(key, ordinal);
      if (handle == NumericGroupAggTable.DISCARD_HANDLE) {
        return;
      }
      chunk = local.storageAtAccBase(handle);
      base = local.offsetAtAccBase(handle);
      if (GroupLanes.get(chunk, base) == 0L) {
        local.setAuxAtAccBase(handle, ordinal);
      }
    }
    GroupLanes.increment(chunk, base);
    GroupLanes.increment(chunk, base + 2);
    GroupLanes.add(chunk, base + 3, value);
    GroupLanes.set(chunk, base + 4, Math.min(GroupLanes.get(chunk, base + 4), value));
    GroupLanes.set(chunk, base + 5, Math.max(GroupLanes.get(chunk, base + 5), value));
  }

  /**
   * Every group of every partition, as
   * {@code key -> [count, firstSeen, present, sum, min, max, aux]}.
   */
  private static Map<Long, long[]> drain(final GroupTableSpill spill) {
    final Map<Long, long[]> out = new HashMap<>();
    for (int p = 0; p < PARTITIONS; p++) {
      final NumericGroupAggTable into = spill.takeOrCreate(p, () -> table(16, 0));
      for (int c = 0; c < into.storageChunkCount(); c++) {
        final GroupBuffer chunk = into.storageChunkOrNull(c);
        if (chunk == null) {
          continue;
        }
        for (int off = 0; off < GroupLanes.count(chunk); off += into.stride()) {
          if (GroupLanes.get(chunk, off) != 0L) {
            final int base = off + 1;
            out.put(GroupLanes.get(chunk, off),
                new long[] {GroupLanes.get(chunk, base), GroupLanes.get(chunk, base + 1),
                    GroupLanes.get(chunk, base + 2), GroupLanes.get(chunk, base + 3), GroupLanes.get(chunk, base + 4),
                    GroupLanes.get(chunk, base + 5), GroupLanes.get(chunk, base + 6)});
          }
        }
      }
      if (into.hasZeroKey()) {
        final GroupBuffer zero = into.zeroSlot();
        out.put(0L, new long[] {GroupLanes.get(zero, 0), GroupLanes.get(zero, 1), GroupLanes.get(zero, 2),
            GroupLanes.get(zero, 3), GroupLanes.get(zero, 4), GroupLanes.get(zero, 5), into.zeroAux()});
      }
      into.release();
    }
    return out;
  }

  /**
   * The rows one run folds: several passes over the same key space so that every worker sees every
   * group (what makes the two spills differ in RESIDENT state) and so the zero group is exercised.
   */
  private static Map<Long, long[]> run(final int stripeSpill, final long budget, final int workers,
      final int rowsPerWorker) {
    final int previous = GroupTableSpill.setStripeSpillForTesting(stripeSpill);
    final int threshold = GroupTableSpill.setFlushGroupsForTesting(64);
    try {
      final GroupTableSpill spill =
          new GroupTableSpill(PARTITIONS, SHIFT, hint -> table(hint, 0), 0L, 0, PARTITIONS, budget);
      final List<NumericGroupAggTable> finals = new ArrayList<>();
      for (int w = 0; w < workers; w++) {
        NumericGroupAggTable local = spill.freshLocal();
        for (int r = 0; r < rowsPerWorker; r++) {
          final long key = r % 200; // 0 included: the zero group takes the side slot
          fold(local, key, w * 1_000L + r, (long) w * rowsPerWorker + r);
          if (local.size() >= 64) {
            spill.flush(local);
            local = spill.freshLocal();
          }
        }
        finals.add(local);
      }
      // The post-scan merge takes the partition's spilled table as its base and folds the workers'
      // final tables into it — exactly as the arms do.
      final NumericGroupAggTable[] sources = finals.toArray(new NumericGroupAggTable[0]);
      final int[][][] indexes = new int[sources.length][][];
      for (int i = 0; i < sources.length; i++) {
        indexes[i] = sources[i].buildPartitionIndex(PARTITIONS, SHIFT);
      }
      final Map<Long, long[]> out = new HashMap<>();
      for (int p = 0; p < PARTITIONS; p++) {
        final NumericGroupAggTable into = spill.takeOrCreate(p, () -> table(16, 0));
        NumericGroupAggTable.mergePartitionIndexed(sources, indexes, p, into);
        for (int c = 0; c < into.storageChunkCount(); c++) {
          final GroupBuffer chunk = into.storageChunkOrNull(c);
          if (chunk == null) {
            continue;
          }
          for (int off = 0; off < GroupLanes.count(chunk); off += into.stride()) {
            if (GroupLanes.get(chunk, off) != 0L) {
              final int base = off + 1;
              out.put(GroupLanes.get(chunk, off),
                  new long[] {GroupLanes.get(chunk, base), GroupLanes.get(chunk, base + 1),
                      GroupLanes.get(chunk, base + 2), GroupLanes.get(chunk, base + 3), GroupLanes.get(chunk, base + 4),
                      GroupLanes.get(chunk, base + 5), GroupLanes.get(chunk, base + 6)});
            }
          }
        }
        if (into.hasZeroKey()) {
          final GroupBuffer zero = into.zeroSlot();
          out.put(0L, new long[] {GroupLanes.get(zero, 0), GroupLanes.get(zero, 1), GroupLanes.get(zero, 2),
              GroupLanes.get(zero, 3), GroupLanes.get(zero, 4), GroupLanes.get(zero, 5), into.zeroAux()});
        }
        into.release();
      }
      for (final NumericGroupAggTable t : sources) {
        t.release();
      }
      // Everything read above is on the heap now: return the spill's private workspace.
      spill.close();
      return out;
    } finally {
      GroupTableSpill.setFlushGroupsForTesting(threshold);
      GroupTableSpill.setStripeSpillForTesting(previous);
    }
  }

  @Test
  @DisplayName("the stripe spill produces the table spill's groups lane for lane, zero group and aux included")
  void stripeSpillAgreesWithTheTableSpill() {
    final Map<Long, long[]> tableSpill = run(0, Long.MAX_VALUE, 4, 1_000);
    final Map<Long, long[]> stripeSpill = run(1, Long.MAX_VALUE, 4, 1_000);
    assertEquals(200, tableSpill.size(), "200 distinct keys, the zero group among them");
    assertEquals(tableSpill.keySet(), stripeSpill.keySet(), "the same groups come out of both spills");
    for (final Map.Entry<Long, long[]> e : tableSpill.entrySet()) {
      assertArrayEqualsWithLabel(e.getValue(), stripeSpill.get(e.getKey()), "group " + e.getKey());
    }
    // Not vacuous: the folds really happened.
    final long[] one = tableSpill.get(1L);
    assertEquals(20L, one[0], "key 1 was folded five times per worker over four workers");
    assertEquals(1L, one[1], "the smallest ordinal that ever saw the group is its first-seen");
  }

  private static void assertArrayEqualsWithLabel(final long[] expected, final long[] actual, final String label) {
    assertEquals(expected.length, actual.length, label);
    final String[] lanes = {"count", "firstSeen", "present", "sum", "min", "max", "aux"};
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], actual[i], label + " lane " + lanes[i]);
    }
  }

  @Test
  @DisplayName("a buffer past the compaction ceiling is deduplicated into its partition table, so state stays bounded")
  void buffersCompactInsteadOfHoldingAGroupPerFlush() {
    final int previous = GroupTableSpill.setStripeSpillForTesting(1);
    final int threshold = GroupTableSpill.setFlushGroupsForTesting(64);
    try {
      // Budget 4,096 over 32 partitions: the ceiling is the 4,096-stripe floor, and 64 flushes of
      // 64 groups spill 4,096 stripes over 32 partitions — 128 per partition, of which only 64 are
      // distinct. Without compaction the spill would count every stripe as resident state.
      final GroupTableSpill spill =
          new GroupTableSpill(PARTITIONS, SHIFT, hint -> table(hint, 0), 0L, 0, PARTITIONS, 1_000L);
      final long compactionsBefore = GroupTableSpill.compactionCount();
      for (int flushes = 0; flushes < 64; flushes++) {
        final NumericGroupAggTable local = spill.freshLocal();
        for (long key = 1; key <= 64; key++) {
          fold(local, key, key, flushes * 64L + key);
        }
        spill.flush(local);
      }
      assertTrue(GroupTableSpill.compactionCount() > compactionsBefore, "the buffers were compacted at least once");
      assertTrue(spill.groupsSpilled() < 4_096L, "resident state is 64 deduplicated groups plus what is still "
          + "buffered, never one stripe per flush: " + spill.groupsSpilled());
      assertTrue(!spill.aborted(), "a budget of 1,000 is not exceeded by 64 groups");
      final Map<Long, long[]> groups = drain(spill);
      assertEquals(64, groups.size());
      for (final Map.Entry<Long, long[]> e : groups.entrySet()) {
        assertEquals(64L, e.getValue()[0], "group " + e.getKey() + " was folded once per flush");
        assertEquals(64L * e.getKey(), e.getValue()[3], "and its sum survived every compaction");
      }
      spill.close();
    } finally {
      GroupTableSpill.setFlushGroupsForTesting(threshold);
      GroupTableSpill.setStripeSpillForTesting(previous);
    }
  }

  @Test
  @DisplayName("an abort is judged on deduplicated groups: buffered stripes are compacted before the verdict")
  void abortJudgesGroupsNotStripes() {
    final int previous = GroupTableSpill.setStripeSpillForTesting(1);
    final int threshold = GroupTableSpill.setFlushGroupsForTesting(64);
    try {
      // 50 groups, spilled 40 times = 2,000 stripes against a budget of 100. The distinct state
      // fits; a spill that judged its buffers would abort and split the scan for nothing.
      final GroupTableSpill fits =
          new GroupTableSpill(PARTITIONS, SHIFT, hint -> table(hint, 0), 0L, 0, PARTITIONS, 100L);
      for (int flush = 0; flush < 40; flush++) {
        final NumericGroupAggTable local = fits.freshLocal();
        for (long key = 1; key <= 50; key++) {
          fold(local, key, key, flush * 50L + key);
        }
        fits.flush(local);
      }
      assertTrue(!fits.aborted(), "50 groups under a budget of 100 must not abort, whatever the stripe count");
      assertTrue(fits.groupsSpilled() < 2_000L,
          "the 2,000 stripes were deduplicated to 50 groups plus a buffered remainder: " + fits.groupsSpilled());
      assertEquals(50, drain(fits).size(), "and the groups themselves are the 50 distinct keys");
      fits.close();

      // And the budget still binds on DISTINCT groups: 300 of them over the same budget aborts.
      final GroupTableSpill over =
          new GroupTableSpill(PARTITIONS, SHIFT, hint -> table(hint, 0), 0L, 0, PARTITIONS, 100L);
      final NumericGroupAggTable local = over.freshLocal();
      for (long key = 1; key <= 300; key++) {
        fold(local, key, key, key);
      }
      over.flush(local);
      assertTrue(over.aborted(), "300 distinct groups over a budget of 100 must abort the pass");
      assertEquals(300L, over.groupsSpilled(), "and the estimate sees every group the pass held");
      over.close();
    } finally {
      GroupTableSpill.setFlushGroupsForTesting(threshold);
      GroupTableSpill.setStripeSpillForTesting(previous);
    }
  }

  @Test
  @DisplayName("buffer chunks ramp from a handful of stripes to the full chunk length, so a thin partition costs stripes")
  void bufferChunksRampFromSmallToFull() {
    final int stride = table(16, 0).stride();
    final int full = NumericGroupAggTable.fullChunkLanes(stride);
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      // Pre-filled with full chunks a table handed back: the witness is that a buffer which reaches the
      // full length REUSES them, and that a ramp chunk does not.
      final GroupBuffer[] handedBack = new GroupBuffer[4];
      for (int i = 0; i < handedBack.length; i++) {
        handedBack[i] = workspace.takeChunk(full);
      }
      for (final GroupBuffer chunk : handedBack) {
        workspace.giveChunk(chunk);
      }
      assertEquals(4, workspace.spareChunks(full), "the workspace holds the full chunks handed back");
      final GroupTableSpill.StripeBuffer buffer = new GroupTableSpill.StripeBuffer(stride, full);
      final GroupBuffer source = workspace.newBlock(stride + 1);
      GroupLanes.set(source, 0, 7L); // key lane; append copies from accBase - 1
      buffer.append(source, 1, workspace);
      assertEquals(1, buffer.chunkCount(), "one stripe opens one chunk");
      assertTrue(GroupLanes.count(buffer.chunk(0)) < full / 8,
          "and that chunk is a handful of stripes, not the full length: " + GroupLanes.count(buffer.chunk(0)) + " of "
              + full);
      assertEquals(stride, buffer.usedLanes(0));
      assertEquals(0L, workspace.chunksReused(), "a ramp chunk is never one of the full chunks handed back");
      assertEquals(4, workspace.spareChunks(full));
      // Fill until the ramp reaches the full length: only then does the buffer reuse a full chunk.
      for (int i = 1; i < 100_000 && workspace.chunksReused() == 0L; i++) {
        buffer.append(source, 1, workspace);
      }
      assertTrue(workspace.chunksReused() > 0L,
          "a buffer that fills reaches the full chunk length and reuses the chunks handed back");
      assertEquals(full, GroupLanes.count(buffer.chunk(buffer.chunkCount() - 1)), "the reused chunk is a full one");
      assertEquals(3, workspace.spareChunks(full));
      final long givesBefore = workspace.chunksGiven();
      final int held = buffer.chunkCount();
      buffer.release(workspace);
      assertEquals(0, buffer.chunkCount(), "release empties the buffer");
      assertEquals(0L, buffer.stripes());
      assertEquals(givesBefore + held, workspace.chunksGiven(), "and hands every chunk back");
      assertEquals(4, workspace.spareChunks(full), "the full chunk it reused among them");
    }
  }

  @Test
  @DisplayName("mergeStripes folds whole stripes only, and identity lanes keep same-probe-key groups apart")
  void mergeStripesIsWholeStripesAndIdentityAware() {
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      final NumericGroupAggTable into = table(64, 0).attachWorkspace(workspace);
      final GroupBuffer stripes = workspace.newBlock(into.stride() * 2);
      GroupLanes.set(stripes, 0, 5L);
      GroupLanes.set(stripes, into.stride(), 6L);
      assertThrows(IllegalArgumentException.class, () -> into.mergeStripes(stripes, 0, into.stride() - 1),
          "a partial stripe run would fold lane-misaligned");
      final GroupBuffer emptyKey = workspace.newBlock(into.stride());
      assertThrows(IllegalStateException.class, () -> into.mergeStripes(emptyKey, 0, into.stride()),
          "an empty key lane is not a spilled stripe");
      into.mergeStripes(stripes, 0, GroupLanes.count(stripes));
      assertEquals(2, into.size());

      // Identity mode: two groups share a probe key and are separated by their identity lanes.
      final NumericGroupAggTable ident = table(64, 1).attachWorkspace(workspace);
      final int width = ident.stride();
      final GroupBuffer pair = workspace.newBlock(width * 2);
      GroupLanes.set(pair, 0, 99L);
      GroupLanes.set(pair, width - 1, 1_000L); // identity lane of the first stripe
      GroupLanes.set(pair, width, 99L);
      GroupLanes.set(pair, 2 * width - 1, 2_000L);
      ident.mergeStripes(pair, 0, GroupLanes.count(pair));
      assertEquals(2, ident.size(), "one probe key, two identities, two groups");
      assertTrue(ident.hasProbeKeyCollision(), "and the table reports that the probe key was ambiguous");
    }
  }

  @Test
  @DisplayName("a partition holding no key shares one empty index array instead of allocating one per partition")
  void emptyPartitionsShareOneIndexArray() {
    final NumericGroupAggTable local = table(64, 0);
    local.acquire(1L, 1L);
    final int[][] index = local.buildPartitionIndex(1024, 64 - 10);
    final List<Integer> nonEmpty = new ArrayList<>();
    for (int p = 0; p < index.length; p++) {
      if (index[p].length != 0) {
        nonEmpty.add(p);
      }
    }
    assertEquals(1, nonEmpty.size(), "one key lands in one partition");
    int first = -1;
    for (int p = 0; p < index.length; p++) {
      if (index[p].length == 0) {
        if (first < 0) {
          first = p;
        } else {
          assertSame(index[first], index[p], "empty partitions share one array");
        }
      }
    }
    assertNotSame(index[first], index[nonEmpty.get(0)], "a partition with a key gets its own array");
    local.release();
  }
}
