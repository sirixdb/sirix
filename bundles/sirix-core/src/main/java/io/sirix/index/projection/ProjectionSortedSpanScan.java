/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.StorageEngineReader;
import io.sirix.index.projection.ProjectionSortedGroupScan.Group;
import io.sirix.index.projection.ProjectionSortedGroupScan.Order;
import it.unimi.dsi.fastutil.longs.LongArrays;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.List;

/** Best-first grouped spans, including groups crossing any number of revisioned sorted leaves. */
final class ProjectionSortedSpanScan {
  // Candidate arrays, the exact-sized key arena and the bounded summary cache stay below 48 MiB.
  // Larger views/keys retain the streaming summary scan; no state grows with the number of groups.
  private static final int MAX_LEAVES = 1 << 18;
  private static final int MAX_KEY_BYTES = 16 << 20;
  private static final int CACHE_SIZE = 128;

  private final StorageEngineReader reader;
  private final int indexNumber;
  private final long divisor;
  private final int readBudget;
  private int reads;
  private final int[] leafIds;
  private final long[] leafMinimums;
  private final long[] leafMaximums;
  private final long[] upper;
  private final int[] starts;
  private final int[] ends;
  private final int[] offsets;
  private final long[] completed;
  private byte[] firstKeys = new byte[0];
  private final ProjectionSortedLeaf[] cache = new ProjectionSortedLeaf[CACHE_SIZE];
  private final int[] cachedOrdinals = new int[CACHE_SIZE];
  private byte[] key = new byte[128];
  private byte[] checkKey = new byte[128];
  private byte[] previousKey = new byte[128];
  private final byte[] payload = new byte[ProjectionSortedGroupSummary.PAYLOAD_BYTES];
  private final byte[][] winners;
  private final long[] minimums;
  private final long[] maximums;
  private final long[] scores;
  private int retained;
  // Lookahead (ProjectionSortedGroupScan.SORTED_LOOKAHEAD): the next ordinals of the priority order,
  // taken together and consumed in that order, and the summaries staged for their deterministic
  // leaves. read(ordinal) consumes a staged summary exactly where the one-read-per-step loop would
  // have read it — charging the budget and validating there — so a staged-but-never-consumed leaf
  // is neither charged nor validated, and the stop point is the serial loop's.
  private final int lookahead;
  private final ProjectionSortedGroupScan.@Nullable LookaheadStats stats;
  private final int[] pending;
  private int pendingFrom;
  private int pendingCount;
  private final int[] stagedOrdinals;
  private final int[] stagedLeafIds;
  private final ProjectionSortedLeaf[] staged;
  private int stagedCount;

  private ProjectionSortedSpanScan(final StorageEngineReader reader, final int indexNumber, final int count,
      final int limit, final long divisor, final int lookahead,
      final ProjectionSortedGroupScan.@Nullable LookaheadStats stats) {
    this.reader = reader;
    this.indexNumber = indexNumber;
    this.divisor = divisor;
    this.lookahead = lookahead;
    this.stats = stats;
    pending = new int[lookahead];
    // A run member stages two leaves (the run's last leaf and the one before its first), hence 2x.
    final int stagingCapacity = lookahead > 1
        ? 2 * lookahead
        : 0;
    stagedOrdinals = new int[stagingCapacity];
    stagedLeafIds = new int[stagingCapacity];
    staged = new ProjectionSortedLeaf[stagingCapacity];
    readBudget = count <= CACHE_SIZE
        ? Integer.MAX_VALUE
        : Math.min(4096, Math.max(32, count >>> 3));
    leafIds = new int[count];
    leafMinimums = new long[count];
    leafMaximums = new long[count];
    upper = new long[count];
    starts = new int[count];
    ends = new int[count];
    offsets = new int[count + 1];
    completed = new long[(count + 63) >>> 6];
    winners = new byte[limit + 1][];
    minimums = new long[limit + 1];
    maximums = new long[limit + 1];
    scores = new long[limit + 1];
    Arrays.fill(cachedOrdinals, -1);
  }

  static @Nullable List<Group> topK(final StorageEngineReader reader, final int indexNumber,
      final ProjectionSortedDirectory.Accessor directory, final int limit, final long divisor) {
    return topK(reader, indexNumber, directory, limit, divisor, ProjectionSortedGroupScan.SORTED_LOOKAHEAD, null);
  }

  /** {@code lookahead} candidates taken together per step ({@code 1}: the serial loop); {@code stats} optional. */
  static @Nullable List<Group> topK(final StorageEngineReader reader, final int indexNumber,
      final ProjectionSortedDirectory.Accessor directory, final int limit, final long divisor, final int lookahead,
      final ProjectionSortedGroupScan.@Nullable LookaheadStats stats) {
    if (limit < 1 || limit > 32 || divisor < 1) {
      throw new IllegalArgumentException("unsupported sorted span top-K request");
    }
    if (lookahead < 1 || lookahead > 64) {
      throw new IllegalArgumentException("unsupported sorted lookahead: " + lookahead);
    }
    final int count = directory.dataLeafCount();
    if (count > MAX_LEAVES || reader.hasTrxIntentLog()) {
      return null;
    }
    final ProjectionSortedLeafBounds.Candidates bounds =
        ProjectionSortedLeafBounds.readUnordered(reader, indexNumber, directory);
    if (bounds == null) {
      return null;
    }
    final ProjectionSortedSpanScan scan =
        new ProjectionSortedSpanScan(reader, indexNumber, count, limit, divisor, lookahead, stats);
    if (!scan.capture(directory, bounds)) {
      return null;
    }
    scan.buildUpperBounds();
    // Reuse the candidate permutation; ordering is by logical-leaf upper bound, not physical ID.
    final int[] order = bounds.order();
    final boolean heap =
        count >= CACHE_SIZE * 8 && !"false".equals(System.getProperty("sirix.projection.heapSpanPriority"));
    if (heap) {
      for (int parent = (order.length >>> 1) - 1; parent >= 0; parent--) {
        siftDown(order, scan.upper, parent, order.length);
      }
    } else {
      LongArrays.quickSortIndirect(order, scan.upper);
    }
    try {
      return scan.visit(order, limit, heap);
    } finally {
      if (ProjectionSortedGroupScan.LOOKAHEAD_DIAG) {
        System.err.println("[sortedLookahead] span window=" + lookahead + " reads=" + scan.reads + " budget="
            + scan.readBudget + (stats != null
                ? " " + stats
                : ""));
      }
    }
  }

  private boolean capture(final ProjectionSortedDirectory.Accessor directory,
      final ProjectionSortedLeafBounds.Candidates bounds) {
    // readUnordered has validated sorted, unique positive IDs. N such IDs bounded by [1, N]
    // necessarily cover that complete range, independently of their current document order.
    final int[] boundIds = bounds.leafIds();
    final boolean denseBounds =
        boundIds.length > 0 && boundIds[0] == 1 && boundIds[boundIds.length - 1] == boundIds.length
            && !"false".equals(System.getProperty("sirix.projection.denseSpanBounds"));
    ProjectionSortedDirectory.Accessor.LeafCursor cursor = directory.leaves();
    int at = 0;
    while (cursor.id() != 0) {
      if (at == leafIds.length) {
        throw new IllegalStateException("sorted directory exceeds its declared leaf count");
      }
      final int length = cursor.firstKeyLength();
      if (length > key.length) {
        key = new byte[length];
      }
      cursor.copyFirstKeyTo(key);
      final int prefix = ProjectionSortedGroupScan.stringPrefixLength(key, length);
      if (prefix < 0 || length != prefix + 1 + 2 * Long.BYTES || key[prefix] != 1
          || prefix > MAX_KEY_BYTES - offsets[at]) {
        return false;
      }
      final int leafId = cursor.id();
      leafIds[at] = leafId;
      final int physical = denseBounds
          ? leafId - 1
          : Arrays.binarySearch(boundIds, leafId);
      if (physical < 0 || physical >= boundIds.length) {
        throw new IllegalStateException("sorted leaf is absent from its bounds");
      }
      leafMinimums[at] = bounds.minimums()[physical];
      leafMaximums[at] = bounds.maximums()[physical];
      upper[at] = spanUpper(leafMinimums[at], leafMaximums[at], divisor);
      offsets[at + 1] = offsets[at] + prefix;
      at++;
      cursor.advance();
    }
    if (at != leafIds.length) {
      throw new IllegalStateException("sorted directory is missing declared leaves");
    }
    // Two directory walks avoid growing/copying a large arena or allocating a key per leaf.
    firstKeys = new byte[offsets[at]];
    cursor = directory.leaves();
    for (int i = 0; i < at; i++) {
      cursor.copyFirstKeyTo(key);
      System.arraycopy(key, 0, firstKeys, offsets[i], offsets[i + 1] - offsets[i]);
      if (i > 0 && compareFirst(i - 1, i) > 0) {
        throw new IllegalStateException("sorted directory group prefixes are out of order");
      }
      cursor.advance();
    }
    return true;
  }

  /**
   * A maximal run of equal first-group keys can begin in its predecessor and ends in its final leaf.
   * Its combined leaf extrema bound that complete group. Assign the bound to every touched leaf;
   * intrinsic leaf extrema already cover every group wholly contained in that leaf.
   */
  private void buildUpperBounds() {
    for (int start = 0; start < leafIds.length;) {
      int end = start;
      while (end + 1 < leafIds.length && compareFirst(start, end + 1) == 0) {
        end++;
      }
      long min = leafMinimums[start];
      long max = leafMaximums[start];
      for (int i = Math.max(0, start - 1); i <= end; i++) {
        min = Math.min(min, leafMinimums[i]);
        max = Math.max(max, leafMaximums[i]);
      }
      final long bound = spanUpper(min, max, divisor);
      for (int i = start; i <= end; i++) {
        starts[i] = start;
        ends[i] = end;
        upper[i] = Math.max(upper[i], bound);
      }
      if (start > 0) {
        upper[start - 1] = Math.max(upper[start - 1], bound);
      }
      start = end + 1;
    }
  }

  private @Nullable List<Group> visit(final int[] order, final int limit, final boolean heap) {
    int remaining = order.length;
    while (pendingCount > 0 || remaining > 0) {
      if (pendingCount == 0) {
        remaining = takePending(order, remaining, heap);
      }
      final int ordinal = pending[pendingFrom];
      // Keep K+1 exact groups and use a strict comparison: ties must reach the existing fallback.
      if (retained == winners.length && upper[ordinal] < scores[retained - 1]) {
        break;
      }
      pendingFrom++;
      pendingCount--;
      if (ordinal < ends[ordinal]) {
        // The next leaf starts with the same group: this entire leaf belongs to that group.
        if (!completeRun(starts[ordinal])) {
          return null;
        }
        continue;
      }
      final ProjectionSortedLeaf summary = read(ordinal);
      if (summary == null) {
        return null;
      }
      for (int row = 0; row < summary.rowCount(); row++) {
        final int length = summary.keyLength(row);
        if (length > key.length) {
          key = new byte[length];
        }
        summary.copyKeyTo(row, key);
        final int run = equalsFirst(ordinal, key, length)
            ? starts[ordinal]
            : ordinal + 1 < leafIds.length && equalsFirst(ordinal + 1, key, length)
                ? starts[ordinal + 1]
                : -1;
        if (run >= 0) {
          if (!completeRun(run)) {
            return null;
          }
        } else {
          summary.copyPayloadTo(row, payload, 0);
          offer(length, ProjectionIndexRowGroupCodec.getLongLE(payload, 0),
              ProjectionIndexRowGroupCodec.getLongLE(payload, Long.BYTES));
        }
      }
    }
    return ProjectionSortedGroupScan.finish(winners, minimums, maximums, scores, retained, limit);
  }

  /**
   * Take the next {@code lookahead} ordinals of the priority order — the very sequence the
   * one-at-a-time loop would pop, since popping is deterministic and nothing feeds back into the
   * order — and fetch their deterministic leaves together. Returns the remaining count.
   */
  private int takePending(final int[] order, int remaining, final boolean heap) {
    pendingFrom = 0;
    pendingCount = 0;
    while (pendingCount < lookahead && remaining > 0) {
      final int ordinal = heap
          ? order[0]
          : order[remaining - 1];
      remaining--;
      if (heap && remaining > 0) {
        order[0] = order[remaining];
        siftDown(order, upper, 0, remaining);
      }
      pending[pendingCount++] = ordinal;
    }
    if (pendingCount > 1) {
      stageLeaves();
    }
    return remaining;
  }

  /**
   * The leaves the pending ordinals will read whatever the data says: a run member completes its run
   * from the run's last leaf and the leaf before its first; any other ordinal reads itself. Leaves
   * already cached, already completed or already staged are skipped, and never more than the read
   * budget still allows, so a batch can never fetch what the budget forbids. One batch fetch: k trie
   * descents on one reader (level-hinted where the backend allows) and one coalesced payload read.
   */
  private void stageLeaves() {
    stagedCount = 0;
    final int budgetLeft = readBudget - reads;
    for (int p = 0; p < pendingCount; p++) {
      final int ordinal = pending[p];
      if (ordinal < ends[ordinal]) {
        final int start = starts[ordinal];
        if ((completed[start >>> 6] & 1L << start) != 0) {
          continue;
        }
        stage(ends[start], budgetLeft);
        if (start > 0) {
          stage(start - 1, budgetLeft);
        }
      } else {
        stage(ordinal, budgetLeft);
      }
    }
    if (stagedCount == 0) {
      return;
    }
    for (int i = 0; i < stagedCount; i++) {
      stagedLeafIds[i] = leafIds[stagedOrdinals[i]];
    }
    try {
      ProjectionSortedGroupSummary.readBatch(reader, indexNumber, stagedLeafIds, 0, stagedCount, staged);
    } catch (final RuntimeException batchFailure) {
      // A leaf the serial loop would never reach must not fail the scan; one it would reach fails
      // identically from its own read. Consumption below reads one by one for this window.
      Arrays.fill(staged, 0, stagedCount, null);
      stagedCount = 0;
      return;
    }
    if (stats != null) {
      stats.fetched += stagedCount;
    }
  }

  private void stage(final int ordinal, final int budgetLeft) {
    if (stagedCount >= budgetLeft || stagedCount == stagedOrdinals.length
        || cachedOrdinals[ordinal & (CACHE_SIZE - 1)] == ordinal) {
      return;
    }
    for (int i = 0; i < stagedCount; i++) {
      if (stagedOrdinals[i] == ordinal) {
        return;
      }
    }
    stagedOrdinals[stagedCount++] = ordinal;
  }

  /** The staged summary for {@code ordinal}, handed over once; {@code null} when it was not staged. */
  private @Nullable ProjectionSortedLeaf takeStaged(final int ordinal) {
    for (int i = 0; i < stagedCount; i++) {
      if (stagedOrdinals[i] == ordinal) {
        final ProjectionSortedLeaf summary = staged[i];
        staged[i] = null;
        stagedOrdinals[i] = -1;
        if (summary != null && stats != null) {
          stats.consumed++;
        }
        return summary;
      }
    }
    return null;
  }

  /** Restore a maximum-bound root in the reused primitive candidate permutation. */
  private static void siftDown(final int[] order, final long[] bounds, final int root, final int size) {
    final int value = order[root];
    final long bound = bounds[value];
    int parent = root;
    for (int child = (parent << 1) + 1; child < size; child = (parent << 1) + 1) {
      if (child + 1 < size && bounds[order[child + 1]] > bounds[order[child]]) {
        child++;
      }
      if (bound >= bounds[order[child]]) {
        break;
      }
      order[parent] = order[child];
      parent = child;
    }
    order[parent] = value;
  }

  /** Complete a group once, using only its two edge summaries and single-group interior bounds. */
  private boolean completeRun(final int start) {
    final long bit = 1L << start;
    if ((completed[start >>> 6] & bit) != 0) {
      return true;
    }
    final int end = ends[start];
    final ProjectionSortedLeaf last = read(end);
    if (last == null) {
      return false;
    }
    // The run's group is necessarily the first entry in its final leaf.
    last.copyPayloadTo(0, payload, 0);
    long min = ProjectionIndexRowGroupCodec.getLongLE(payload, 0);
    long max = ProjectionIndexRowGroupCodec.getLongLE(payload, Long.BYTES);
    for (int i = start; i < end; i++) {
      min = Math.min(min, leafMinimums[i]);
      max = Math.max(max, leafMaximums[i]);
    }
    if (start > 0) {
      final ProjectionSortedLeaf before = read(start - 1);
      if (before == null) {
        return false;
      }
      final int row = before.rowCount() - 1;
      final int length = before.keyLength(row);
      if (length > checkKey.length) {
        checkKey = new byte[length];
      }
      before.copyKeyTo(row, checkKey);
      if (equalsFirst(start, checkKey, length)) {
        before.copyPayloadTo(row, payload, 0);
        min = Math.min(min, ProjectionIndexRowGroupCodec.getLongLE(payload, 0));
        max = Math.max(max, ProjectionIndexRowGroupCodec.getLongLE(payload, Long.BYTES));
      }
    }
    final int length = offsets[start + 1] - offsets[start];
    if (length > key.length) {
      key = new byte[length];
    }
    System.arraycopy(firstKeys, offsets[start], key, 0, length);
    offer(length, min, max);
    completed[start >>> 6] |= bit;
    return true;
  }

  private void offer(final int length, final long min, final long max) {
    retained = ProjectionSortedGroupScan.offer(key, length, min, max, Order.SPAN_DESC, divisor, winners, minimums,
        maximums, scores, retained);
  }

  /** Validate every loaded summary against its revisioned bounds and directory neighbours. */
  private @Nullable ProjectionSortedLeaf read(final int ordinal) {
    final int slot = ordinal & (CACHE_SIZE - 1);
    if (cachedOrdinals[slot] == ordinal) {
      return cache[slot];
    }
    // Unselective bounds must not replace the existing parallel path with a full serial scan.
    if (reads == readBudget) {
      return null;
    }
    reads++;
    if (stats != null) {
      stats.charged++;
    }
    final ProjectionSortedLeaf stagedSummary = takeStaged(ordinal);
    final ProjectionSortedLeaf summary = stagedSummary != null
        ? stagedSummary
        : ProjectionSortedGroupSummary.read(reader, indexNumber, leafIds[ordinal]);
    if (summary == null) {
      return null;
    }
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    int previousLength = 0;
    for (int row = 0; row < summary.rowCount(); row++) {
      final int length = summary.keyLength(row);
      if (length > checkKey.length) {
        checkKey = new byte[length];
      }
      summary.copyKeyTo(row, checkKey);
      if (ProjectionSortedGroupScan.stringPrefixLength(checkKey, length) != length
          || summary.payloadLength(row) != payload.length || row == 0 && !equalsFirst(ordinal, checkKey, length)
          || row > 0 && Arrays.compareUnsigned(previousKey, 0, previousLength, checkKey, 0, length) >= 0) {
        throw new IllegalStateException("sorted span summary disagrees with its directory");
      }
      summary.copyPayloadTo(row, payload, 0);
      final long rowMin = ProjectionIndexRowGroupCodec.getLongLE(payload, 0);
      final long rowMax = ProjectionIndexRowGroupCodec.getLongLE(payload, Long.BYTES);
      if (rowMin > rowMax) {
        throw new IllegalStateException("invalid sorted group summary extrema");
      }
      min = Math.min(min, rowMin);
      max = Math.max(max, rowMax);
      if (length > previousKey.length) {
        previousKey = new byte[length];
      }
      System.arraycopy(checkKey, 0, previousKey, 0, length);
      previousLength = length;
    }
    if (summary.rowCount() == 0 || min != leafMinimums[ordinal] || max != leafMaximums[ordinal]
        || ordinal + 1 < leafIds.length && Arrays.compareUnsigned(previousKey, 0, previousLength, firstKeys,
            offsets[ordinal + 1], offsets[ordinal + 2]) > 0) {
      throw new IllegalStateException("sorted span summary disagrees with its bounds or next group");
    }
    cache[slot] = summary;
    cachedOrdinals[slot] = ordinal;
    return summary;
  }

  private int compareFirst(final int left, final int right) {
    return Arrays.compareUnsigned(firstKeys, offsets[left], offsets[left + 1], firstKeys, offsets[right],
        offsets[right + 1]);
  }

  private boolean equalsFirst(final int ordinal, final byte[] group, final int length) {
    return Arrays.equals(firstKeys, offsets[ordinal], offsets[ordinal + 1], group, 0, length);
  }

  static long spanUpper(final long min, final long max, final long divisor) {
    try {
      return Math.subtractExact(max / divisor, min / divisor);
    } catch (final ArithmeticException overflow) {
      return Long.MAX_VALUE;
    }
  }
}
