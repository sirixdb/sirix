/*
 * Copyright (c) 2023, Sirix Contributors
 *
 * All rights reserved.
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

package io.sirix.index.vector;

import java.util.Arrays;

/**
 * Result of a k-nearest-neighbor search on a vector index.
 *
 * <p>Contains parallel arrays of document node keys and distances, sorted by
 * distance ascending (nearest first). The {@code count} field indicates how
 * many entries are valid in the arrays (arrays may be larger than count for
 * reuse / pre-allocation).</p>
 *
 * @param nodeKeys  the document node keys of the nearest neighbors (length >= count)
 * @param distances the corresponding distances from the query vector (length >= count)
 * @param count     the number of valid results (0 <= count <= nodeKeys.length)
 * @author Johannes Lichtenberger
 */
public record VectorSearchResult(long[] nodeKeys, float[] distances, int count) {

  /**
   * Compact constructor with validation.
   */
  public VectorSearchResult {
    if (nodeKeys == null) {
      throw new IllegalArgumentException("nodeKeys must not be null");
    }
    if (distances == null) {
      throw new IllegalArgumentException("distances must not be null");
    }
    if (count < 0) {
      throw new IllegalArgumentException("count must be >= 0, was: " + count);
    }
    if (count > nodeKeys.length) {
      throw new IllegalArgumentException(
          "count (" + count + ") must be <= nodeKeys.length (" + nodeKeys.length + ")");
    }
    if (count > distances.length) {
      throw new IllegalArgumentException(
          "count (" + count + ") must be <= distances.length (" + distances.length + ")");
    }
  }

  /**
   * Creates an empty result.
   *
   * @return an empty VectorSearchResult
   */
  public static VectorSearchResult empty() {
    return new VectorSearchResult(new long[0], new float[0], 0);
  }

  /**
   * Returns a document node key at the given index.
   *
   * @param index the result index (0-based)
   * @return the document node key
   * @throws IndexOutOfBoundsException if index >= count
   */
  public long nodeKeyAt(final int index) {
    if (index < 0 || index >= count) {
      throw new IndexOutOfBoundsException("index: " + index + ", count: " + count);
    }
    return nodeKeys[index];
  }

  /**
   * Returns a distance at the given index.
   *
   * @param index the result index (0-based)
   * @return the distance
   * @throws IndexOutOfBoundsException if index >= count
   */
  public float distanceAt(final int index) {
    if (index < 0 || index >= count) {
      throw new IndexOutOfBoundsException("index: " + index + ", count: " + count);
    }
    return distances[index];
  }

  /**
   * Whether the result is empty.
   *
   * @return true if no results were found
   */
  public boolean isEmpty() {
    return count == 0;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof final VectorSearchResult other)) {
      return false;
    }
    if (count != other.count) {
      return false;
    }
    for (int i = 0; i < count; i++) {
      if (nodeKeys[i] != other.nodeKeys[i]) {
        return false;
      }
      if (Float.compare(distances[i], other.distances[i]) != 0) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = Integer.hashCode(count);
    for (int i = 0; i < count; i++) {
      result = 31 * result + Long.hashCode(nodeKeys[i]);
      result = 31 * result + Float.hashCode(distances[i]);
    }
    return result;
  }

  @Override
  public String toString() {
    return "VectorSearchResult{count=" + count
        + ", nodeKeys=" + Arrays.toString(Arrays.copyOf(nodeKeys, count))
        + ", distances=" + Arrays.toString(Arrays.copyOf(distances, count)) + '}';
  }
}
