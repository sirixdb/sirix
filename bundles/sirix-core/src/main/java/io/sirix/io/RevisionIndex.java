/*
 * Copyright (c) 2023, Sirix
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package io.sirix.io;


import java.time.Instant;
import java.util.Arrays;

/**
 * Immutable, cache-friendly index for revision timestamp lookups.
 * 
 * <p>
 * Uses a hybrid search strategy:
 * <ul>
 * <li>{@link Arrays#binarySearch} for small revision counts (low overhead)</li>
 * <li>Eytzinger layout binary search for large counts (cache-optimal access pattern)</li>
 * </ul>
 * 
 * <p>
 * This class is immutable and thread-safe. Multiple readers can search concurrently. Updates are
 * done via copy-on-write using {@link #withNewRevision(long, long)}.
 * 
 * <p>
 * <b>Contract</b>: {@link #findRevision(long)} matches {@link Arrays#binarySearch(long[], long)}
 * semantics:
 * <ul>
 * <li>Returns index if timestamp is found (exact match)</li>
 * <li>Returns {@code -(insertionPoint + 1)} if not found</li>
 * </ul>
 * 
 * @author Johannes Lichtenberger
 * @since 1.0.0
 */
public final class RevisionIndex {

  /**
   * Threshold for choosing simple binary search vs Eytzinger search. For small arrays,
   * Arrays.binarySearch is faster due to lower overhead. Eytzinger layout has higher setup cost but
   * better cache behavior for large arrays. Determined via JMH benchmarking.
   */
  private static final int EYTZINGER_THRESHOLD = 64;

  /**
   * Empty index singleton for databases with no revisions.
   */
  public static final RevisionIndex EMPTY =
      new RevisionIndex(new long[0], new long[0], new long[1], new int[1], 0, 0);

  // ===== HOT PATH DATA (accessed during every search) =====

  /**
   * Timestamps in Eytzinger layout (1-indexed, index 0 unused). BFS order of implicit binary search
   * tree for cache-optimal access.
   */
  private final long[] eytzingerTimestamps;

  /**
   * Mapping from Eytzinger index to sorted index. eytzingerToSorted[k] = the sorted array index of
   * element at Eytzinger position k.
   */
  private final int[] eytzingerToSorted;

  // ===== COLD PATH DATA (accessed only after search finds result) =====

  /**
   * Timestamps in sorted order (epoch milliseconds). timestamps[i] = commit timestamp of revision i.
   */
  private final long[] timestamps;

  /**
   * File offsets for each revision. offsets[i] = byte offset in data file where revision i's root
   * page is stored.
   */
  private final long[] offsets;

  /**
   * Number of revisions in this index. The backing arrays may be LARGER than this (spare append
   * capacity, see {@link #withNewRevision(long, long)}) — every access must be bounded by
   * {@code size}, never by array length.
   */
  private final int size;

  /**
   * Number of leading revisions covered by the Eytzinger arrays. Commits append without
   * rebuilding the Eytzinger layout (rebuilding per commit made every commit O(size) — O(size²)
   * cumulative); the uncovered tail {@code [eytzingerSize, size)} is searched with a plain
   * bounded binary search until the deferred rebuild folds it in.
   */
  private final int eytzingerSize;

  /**
   * Private constructor - use factory methods.
   */
  private RevisionIndex(long[] timestamps, long[] offsets, long[] eytzingerTimestamps, int[] eytzingerToSorted,
      int size, int eytzingerSize) {
    this.timestamps = timestamps;
    this.offsets = offsets;
    this.eytzingerTimestamps = eytzingerTimestamps;
    this.eytzingerToSorted = eytzingerToSorted;
    this.size = size;
    this.eytzingerSize = eytzingerSize;
  }

  /**
   * Create a RevisionIndex from arrays of timestamps and offsets.
   * 
   * @param timestamps array of commit timestamps (epoch millis), must be sorted ascending
   * @param offsets array of file offsets, same length as timestamps
   * @return new RevisionIndex
   * @throws IllegalArgumentException if arrays have different lengths or timestamps not sorted
   */
  public static RevisionIndex create(long[] timestamps, long[] offsets) {
    if (timestamps.length != offsets.length) {
      throw new IllegalArgumentException("timestamps and offsets must have same length");
    }

    if (timestamps.length == 0) {
      return EMPTY;
    }

    // Verify sorted order (paranoid check for production safety)
    for (int i = 1; i < timestamps.length; i++) {
      if (timestamps[i] < timestamps[i - 1]) {
        throw new IllegalArgumentException("timestamps must be sorted ascending");
      }
    }

    // Build Eytzinger layout with mapping
    int n = timestamps.length;
    long[] eytzinger = new long[n + 1]; // 1-indexed
    int[] mapping = new int[n + 1]; // 1-indexed
    buildEytzingerWithMapping(timestamps, n, eytzinger, mapping, 0, 1);

    return new RevisionIndex(Arrays.copyOf(timestamps, n), Arrays.copyOf(offsets, n), eytzinger, mapping, n, n);
  }

  /**
   * Build Eytzinger layout with index mapping via in-order traversal.
   * 
   * <p>
   * The Eytzinger layout stores elements in BFS order of a complete binary tree, enabling
   * cache-optimal binary search with sequential memory access patterns.
   * 
   * @param sorted source array in sorted order (may have spare capacity past {@code n})
   * @param n number of valid elements in {@code sorted}
   * @param eyt destination Eytzinger array (1-indexed)
   * @param mapping destination mapping array (eyt index -> sorted index)
   * @param sortedIdx current position in sorted array
   * @param eytIdx current position in Eytzinger array
   * @return next position in sorted array
   */
  private static int buildEytzingerWithMapping(long[] sorted, int n, long[] eyt, int[] mapping, int sortedIdx,
      int eytIdx) {
    if (eytIdx <= n) {
      // In-order traversal: left, root, right
      sortedIdx = buildEytzingerWithMapping(sorted, n, eyt, mapping, sortedIdx, 2 * eytIdx);
      eyt[eytIdx] = sorted[sortedIdx];
      mapping[eytIdx] = sortedIdx;
      sortedIdx++;
      sortedIdx = buildEytzingerWithMapping(sorted, n, eyt, mapping, sortedIdx, 2 * eytIdx + 1);
    }
    return sortedIdx;
  }

  /**
   * Find the revision number for a given timestamp.
   * 
   * <p>
   * Matches {@link Arrays#binarySearch(long[], long)} contract exactly:
   * <ul>
   * <li>Returns revision index if timestamp matches exactly</li>
   * <li>Returns {@code -(insertionPoint + 1)} if not found</li>
   * </ul>
   * 
   * @param timestamp epoch milliseconds to search for
   * @return revision index if exact match, or {@code -(insertionPoint + 1)} if not found
   */
  public int findRevision(long timestamp) {
    // C1: Empty database
    if (size == 0) {
      return -1; // -(0 + 1)
    }

    // C2: Before first revision (fast path)
    if (timestamp < timestamps[0]) {
      return -1; // -(0 + 1)
    }

    // C3: After last revision (fast path)
    if (timestamp > timestamps[size - 1]) {
      return -(size + 1);
    }

    // Choose search strategy based on size
    // For small arrays, Arrays.binarySearch is fastest (low overhead)
    // For large arrays, Eytzinger layout is faster (cache-optimal access pattern)
    // NOTE: always the BOUNDED binarySearch overload — the backing array may carry spare
    // append capacity past `size`.
    if (size <= EYTZINGER_THRESHOLD || eytzingerSize == 0) {
      return Arrays.binarySearch(timestamps, 0, size, timestamp);
    }

    // Appends since the last Eytzinger rebuild live in the tail [eytzingerSize, size) —
    // route there directly when the target lies past the covered prefix.
    if (timestamp > timestamps[eytzingerSize - 1]) {
      return Arrays.binarySearch(timestamps, eytzingerSize, size, timestamp);
    }

    // Eytzinger search returns lower_bound, convert to binarySearch contract
    int lb = eytzingerLowerBound(timestamp);

    // This branch is highly predictable (exact matches are common use case)
    if (lb < eytzingerSize && timestamps[lb] == timestamp) {
      return lb; // Exact match
    }
    return -(lb + 1); // Not found, insertion point = lb
  }


  /**
   * Eytzinger layout lower_bound search with explicit bound tracking.
   * 
   * <p>
   * Searches the Eytzinger-ordered array and tracks the lower_bound candidate. Uses precomputed
   * mapping to convert Eytzinger indices to sorted indices.
   * 
   * @param target timestamp to search for
   * @return index of first element >= target, or size if all elements < target
   */
  private int eytzingerLowerBound(long target) {
    int k = 1;
    int n = eytzingerSize;
    int bound = n; // Default: past end (target > all elements)

    while (k <= n) {
      long val = eytzingerTimestamps[k];

      if (val >= target) {
        // This element is >= target, update lower_bound candidate
        bound = eytzingerToSorted[k];
        k = 2 * k; // Go left to find smaller candidates
      } else {
        k = 2 * k + 1; // Go right
      }
    }

    return bound;
  }

  /**
   * Create a new RevisionIndex with an additional revision.
   * 
   * <p>
   * This is a copy-on-write operation. The original index is not modified.
   * 
   * @param newOffset file offset of the new revision
   * @param newTimestamp commit timestamp of the new revision (epoch millis)
   * @return new RevisionIndex containing all revisions plus the new one
   * @throws IllegalArgumentException if newTimestamp < last timestamp (not monotonic)
   */
  public RevisionIndex withNewRevision(long newOffset, long newTimestamp) {
    if (size > 0 && newTimestamp < timestamps[size - 1]) {
      throw new IllegalArgumentException(
          "New timestamp must be >= last timestamp. Got " + newTimestamp + " but last is " + timestamps[size - 1]);
    }

    // Amortized append. The previous implementation copied both arrays AND rebuilt the
    // Eytzinger layout on EVERY commit — O(size) per commit, O(size²) cumulative, measured as
    // a monotonic commit-rate decline on long histories. Instead the backing arrays carry
    // spare capacity (doubling growth) and the Eytzinger rebuild is deferred until the
    // uncovered tail is worth folding in; searches bridge via a bounded binary search on the
    // tail (see findRevision).
    //
    // Writing slot `size` into a SHARED backing array is safe under the single-writer
    // contract (RevisionIndexHolder.addRevision runs under the commit lock and the holder
    // only ever advances linearly): every published index reads strictly below its own
    // `size`, so concurrent readers of older indexes never observe the appended slot, and
    // publication happens via the holder's volatile write.
    final int newSize = size + 1;
    final long[] newTimestamps;
    final long[] newOffsets;
    if (timestamps.length >= newSize && offsets.length >= newSize && this != EMPTY) {
      timestamps[size] = newTimestamp;
      offsets[size] = newOffset;
      newTimestamps = timestamps;
      newOffsets = offsets;
    } else {
      final int capacity = Math.max(16, newSize * 2);
      newTimestamps = Arrays.copyOf(timestamps, capacity);
      newOffsets = Arrays.copyOf(offsets, capacity);
      newTimestamps[size] = newTimestamp;
      newOffsets[size] = newOffset;
    }

    // Deferred Eytzinger rebuild: fold the tail in once it exceeds max(threshold, size/8) —
    // amortized O(1) per commit, tail searches stay O(log tail).
    final int tail = newSize - eytzingerSize;
    if (newSize > EYTZINGER_THRESHOLD && tail > Math.max(EYTZINGER_THRESHOLD, newSize >> 3)) {
      final long[] newEytzinger = new long[newSize + 1];
      final int[] newMapping = new int[newSize + 1];
      buildEytzingerWithMapping(newTimestamps, newSize, newEytzinger, newMapping, 0, 1);
      return new RevisionIndex(newTimestamps, newOffsets, newEytzinger, newMapping, newSize, newSize);
    }

    return new RevisionIndex(newTimestamps, newOffsets, eytzingerTimestamps, eytzingerToSorted, newSize,
                             eytzingerSize);
  }

  /**
   * Get the file offset for a revision.
   * 
   * @param revision revision number (0-indexed)
   * @return file offset in bytes
   * @throws IndexOutOfBoundsException if revision < 0 or revision >= size
   */
  public long getOffset(int revision) {
    if (revision < 0 || revision >= size) {
      throw new IndexOutOfBoundsException("Revision " + revision + " out of range [0, " + size + ")");
    }
    return offsets[revision];
  }

  /**
   * Get the timestamp for a revision.
   * 
   * @param revision revision number (0-indexed)
   * @return commit timestamp as Instant
   * @throws IndexOutOfBoundsException if revision < 0 or revision >= size
   */
  public Instant getTimestamp(int revision) {
    if (revision < 0 || revision >= size) {
      throw new IndexOutOfBoundsException("Revision " + revision + " out of range [0, " + size + ")");
    }
    return Instant.ofEpochMilli(timestamps[revision]);
  }

  /**
   * Get the timestamp for a revision as epoch milliseconds.
   * 
   * @param revision revision number (0-indexed)
   * @return commit timestamp in epoch milliseconds
   * @throws IndexOutOfBoundsException if revision < 0 or revision >= size
   */
  public long getTimestampMillis(int revision) {
    if (revision < 0 || revision >= size) {
      throw new IndexOutOfBoundsException("Revision " + revision + " out of range [0, " + size + ")");
    }
    return timestamps[revision];
  }

  /**
   * Bulk-copy the commit timestamps (epoch millis) for the inclusive revision range
   * {@code [fromRevision, toRevision]} into a freshly allocated array, in ascending revision
   * order.
   *
   * <p>
   * This is the allocation-light source for history listings: the timestamps are already
   * resident in this immutable index, so the whole range is served by a single
   * {@link System#arraycopy} with no per-revision page reads, transaction opens, or boxing.
   *
   * <p>
   * Bounds are validated against {@link #size}, never against the backing array length — the
   * backing arrays may carry spare append capacity past {@code size} (see
   * {@link #withNewRevision(long, long)}).
   *
   * @param fromRevision first revision (0-indexed, inclusive)
   * @param toRevision   last revision (0-indexed, inclusive)
   * @return a new array of {@code toRevision - fromRevision + 1} epoch-milli timestamps in
   *         ascending revision order
   * @throws IndexOutOfBoundsException if {@code fromRevision < 0}, {@code toRevision >= size},
   *                                   or {@code fromRevision > toRevision}
   */
  public long[] timestampsMillis(int fromRevision, int toRevision) {
    if (fromRevision < 0 || toRevision >= size || fromRevision > toRevision) {
      throw new IndexOutOfBoundsException(
          "Revision range [" + fromRevision + ", " + toRevision + "] out of bounds [0, " + size + ")");
    }
    final int count = toRevision - fromRevision + 1;
    final long[] result = new long[count];
    System.arraycopy(timestamps, fromRevision, result, 0, count);
    return result;
  }

  /**
   * Get the revision file data for a revision.
   *
   * @param revision revision number (0-indexed)
   * @return RevisionFileData containing offset and timestamp
   * @throws IndexOutOfBoundsException if revision < 0 or revision >= size
   */
  public RevisionFileData getRevisionFileData(int revision) {
    if (revision < 0 || revision >= size) {
      throw new IndexOutOfBoundsException("Revision " + revision + " out of range [0, " + size + ")");
    }
    return new RevisionFileData(offsets[revision], Instant.ofEpochMilli(timestamps[revision]));
  }

  /**
   * Get the number of revisions in this index.
   * 
   * @return number of revisions
   */
  public int size() {
    return size;
  }

  /**
   * Check if this index is empty.
   * 
   * @return true if no revisions, false otherwise
   */
  public boolean isEmpty() {
    return size == 0;
  }

  /**
   * Get the threshold used for search strategy selection. Below this threshold, Arrays.binarySearch
   * is used. Above this threshold, Eytzinger layout is used.
   * 
   * @return threshold for Eytzinger search activation
   */
  public static int getEytzingerThreshold() {
    return EYTZINGER_THRESHOLD;
  }
}

