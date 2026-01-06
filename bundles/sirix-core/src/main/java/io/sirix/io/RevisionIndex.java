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

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import java.time.Instant;
import java.util.Arrays;

/**
 * Immutable, cache-friendly index for revision timestamp lookups.
 * 
 * <p>Uses a hybrid search strategy:
 * <ul>
 *   <li>SIMD linear search for small revision counts (better cache utilization)</li>
 *   <li>Eytzinger layout binary search for large counts (O(log n) with cache prefetching)</li>
 * </ul>
 * 
 * <p>This class is immutable and thread-safe. Multiple readers can search concurrently.
 * Updates are done via copy-on-write using {@link #withNewRevision(long, long)}.
 * 
 * <p><b>Contract</b>: {@link #findRevision(long)} matches {@link Arrays#binarySearch(long[], long)} semantics:
 * <ul>
 *   <li>Returns index if timestamp is found (exact match)</li>
 *   <li>Returns {@code -(insertionPoint + 1)} if not found</li>
 * </ul>
 * 
 * @author Johannes Lichtenberger
 * @since 1.0.0
 */
public final class RevisionIndex {
  
  /**
   * SIMD vector species for long comparisons.
   * Auto-selects best available: AVX-512 (8 lanes), AVX2 (4 lanes), or SSE (2 lanes).
   */
  private static final VectorSpecies<Long> SPECIES = LongVector.SPECIES_PREFERRED;
  
  /**
   * Number of SIMD lanes available.
   */
  private static final int LANES = SPECIES.length();
  
  /**
   * Threshold for choosing SIMD vs Eytzinger search.
   * SIMD linear search is faster for small arrays due to sequential access.
   * Tuned based on typical CPU cache characteristics.
   */
  private static final int SIMD_THRESHOLD = LANES >= 8 ? 128 : LANES >= 4 ? 64 : 32;
  
  /**
   * Empty index singleton for databases with no revisions.
   */
  public static final RevisionIndex EMPTY = new RevisionIndex(
      new long[0], new long[0], new long[1], new int[1], 0);
  
  // ===== HOT PATH DATA (accessed during every search) =====
  
  /**
   * Timestamps in Eytzinger layout (1-indexed, index 0 unused).
   * BFS order of implicit binary search tree for cache-optimal access.
   */
  private final long[] eytzingerTimestamps;
  
  /**
   * Mapping from Eytzinger index to sorted index.
   * eytzingerToSorted[k] = the sorted array index of element at Eytzinger position k.
   */
  private final int[] eytzingerToSorted;
  
  // ===== COLD PATH DATA (accessed only after search finds result) =====
  
  /**
   * Timestamps in sorted order (epoch milliseconds).
   * timestamps[i] = commit timestamp of revision i.
   */
  private final long[] timestamps;
  
  /**
   * File offsets for each revision.
   * offsets[i] = byte offset in data file where revision i's root page is stored.
   */
  private final long[] offsets;
  
  /**
   * Number of revisions in this index.
   */
  private final int size;
  
  /**
   * Private constructor - use factory methods.
   */
  private RevisionIndex(long[] timestamps, long[] offsets, 
                        long[] eytzingerTimestamps, int[] eytzingerToSorted, int size) {
    this.timestamps = timestamps;
    this.offsets = offsets;
    this.eytzingerTimestamps = eytzingerTimestamps;
    this.eytzingerToSorted = eytzingerToSorted;
    this.size = size;
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
    long[] eytzinger = new long[n + 1];  // 1-indexed
    int[] mapping = new int[n + 1];      // 1-indexed
    buildEytzingerWithMapping(timestamps, eytzinger, mapping, 0, 1);
    
    return new RevisionIndex(
        Arrays.copyOf(timestamps, n),
        Arrays.copyOf(offsets, n),
        eytzinger,
        mapping,
        n
    );
  }
  
  /**
   * Build Eytzinger layout with index mapping via in-order traversal.
   * 
   * <p>The Eytzinger layout stores elements in BFS order of a complete binary tree,
   * enabling cache-optimal binary search with sequential memory access patterns.
   * 
   * @param sorted source array in sorted order
   * @param eyt destination Eytzinger array (1-indexed)
   * @param mapping destination mapping array (eyt index -> sorted index)
   * @param sortedIdx current position in sorted array
   * @param eytIdx current position in Eytzinger array
   * @return next position in sorted array
   */
  private static int buildEytzingerWithMapping(long[] sorted, long[] eyt, int[] mapping,
                                                int sortedIdx, int eytIdx) {
    if (eytIdx <= sorted.length) {
      // In-order traversal: left, root, right
      sortedIdx = buildEytzingerWithMapping(sorted, eyt, mapping, sortedIdx, 2 * eytIdx);
      eyt[eytIdx] = sorted[sortedIdx];
      mapping[eytIdx] = sortedIdx;
      sortedIdx++;
      sortedIdx = buildEytzingerWithMapping(sorted, eyt, mapping, sortedIdx, 2 * eytIdx + 1);
    }
    return sortedIdx;
  }
  
  /**
   * Find the revision number for a given timestamp.
   * 
   * <p>Matches {@link Arrays#binarySearch(long[], long)} contract exactly:
   * <ul>
   *   <li>Returns revision index if timestamp matches exactly</li>
   *   <li>Returns {@code -(insertionPoint + 1)} if not found</li>
   * </ul>
   * 
   * @param timestamp epoch milliseconds to search for
   * @return revision index if exact match, or {@code -(insertionPoint + 1)} if not found
   */
  public int findRevision(long timestamp) {
    // C1: Empty database
    if (size == 0) {
      return -1;  // -(0 + 1)
    }
    
    // C2: Before first revision (fast path)
    if (timestamp < timestamps[0]) {
      return -1;  // -(0 + 1)
    }
    
    // C3: After last revision (fast path)
    if (timestamp > timestamps[size - 1]) {
      return -(size + 1);
    }
    
    // Choose search strategy based on size
    int lb;
    if (size <= SIMD_THRESHOLD) {
      lb = searchSimdLowerBound(timestamp);
    } else {
      lb = eytzingerLowerBound(timestamp);
    }
    
    // Convert lower_bound to binarySearch contract
    // This branch is highly predictable (exact matches are common use case)
    if (lb < size && timestamps[lb] == timestamp) {
      return lb;  // Exact match
    }
    return -(lb + 1);  // Not found, insertion point = lb
  }
  
  /**
   * SIMD-accelerated lower_bound search.
   * 
   * <p>Finds the index of the first element >= target using vectorized comparison.
   * Falls back to scalar loop for remainder elements not filling a full SIMD register.
   * 
   * @param target timestamp to search for
   * @return index of first element >= target, or size if all elements < target
   */
  private int searchSimdLowerBound(long target) {
    LongVector targetVec = LongVector.broadcast(SPECIES, target);
    int i = 0;
    
    // Process full SIMD chunks
    int limit = size - (size % LANES);
    for (; i < limit; i += LANES) {
      LongVector chunk = LongVector.fromArray(SPECIES, timestamps, i);
      VectorMask<Long> ge = chunk.compare(VectorOperators.GE, targetVec);
      
      if (ge.anyTrue()) {
        // Found! Return index of first matching lane
        return i + Long.numberOfTrailingZeros(ge.toLong());
      }
    }
    
    // E3: Scalar fallback for remainder (0 to LANES-1 elements)
    for (; i < size; i++) {
      if (timestamps[i] >= target) {
        return i;
      }
    }
    
    return size;  // Not found, insertion point = size
  }
  
  /**
   * Eytzinger layout lower_bound search with explicit bound tracking.
   * 
   * <p>Searches the Eytzinger-ordered array and tracks the lower_bound candidate.
   * Uses precomputed mapping to convert Eytzinger indices to sorted indices.
   * 
   * @param target timestamp to search for
   * @return index of first element >= target, or size if all elements < target
   */
  private int eytzingerLowerBound(long target) {
    int k = 1;
    int n = size;
    int bound = n;  // Default: past end (target > all elements)
    
    while (k <= n) {
      long val = eytzingerTimestamps[k];
      
      if (val >= target) {
        // This element is >= target, update lower_bound candidate
        bound = eytzingerToSorted[k];
        k = 2 * k;  // Go left to find smaller candidates
      } else {
        k = 2 * k + 1;  // Go right
      }
    }
    
    return bound;
  }
  
  /**
   * Create a new RevisionIndex with an additional revision.
   * 
   * <p>This is a copy-on-write operation. The original index is not modified.
   * 
   * @param newOffset file offset of the new revision
   * @param newTimestamp commit timestamp of the new revision (epoch millis)
   * @return new RevisionIndex containing all revisions plus the new one
   * @throws IllegalArgumentException if newTimestamp < last timestamp (not monotonic)
   */
  public RevisionIndex withNewRevision(long newOffset, long newTimestamp) {
    if (size > 0 && newTimestamp < timestamps[size - 1]) {
      throw new IllegalArgumentException(
          "New timestamp must be >= last timestamp. Got " + newTimestamp + 
          " but last is " + timestamps[size - 1]);
    }
    
    // Create expanded arrays
    int newSize = size + 1;
    long[] newTimestamps = Arrays.copyOf(timestamps, newSize);
    long[] newOffsets = Arrays.copyOf(offsets, newSize);
    newTimestamps[size] = newTimestamp;
    newOffsets[size] = newOffset;
    
    // Rebuild Eytzinger layout (O(n) but only on commit, which is already expensive)
    long[] newEytzinger = new long[newSize + 1];
    int[] newMapping = new int[newSize + 1];
    buildEytzingerWithMapping(newTimestamps, newEytzinger, newMapping, 0, 1);
    
    return new RevisionIndex(newTimestamps, newOffsets, newEytzinger, newMapping, newSize);
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
   * Get the SIMD threshold used for search strategy selection.
   * Exposed for testing and tuning.
   * 
   * @return threshold below which SIMD linear search is used
   */
  public static int getSimdThreshold() {
    return SIMD_THRESHOLD;
  }
  
  /**
   * Get the number of SIMD lanes available on this platform.
   * Exposed for testing and tuning.
   * 
   * @return number of long values processed per SIMD instruction
   */
  public static int getSimdLanes() {
    return LANES;
  }
}

