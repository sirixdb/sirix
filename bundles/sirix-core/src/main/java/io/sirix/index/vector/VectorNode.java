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

import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.RecordSerializer;

import java.util.Arrays;

/**
 * DataRecord implementation storing an HNSW graph node for vector similarity search.
 * Each VectorNode holds a float[] embedding, the document node key it belongs to,
 * and per-layer neighbor lists for the HNSW graph structure.
 *
 * <p>Neighbor arrays are pre-allocated based on maxLayer but neighbor counts track
 * the actual number of neighbors per layer, avoiding unnecessary allocations during
 * graph construction.</p>
 *
 * @author Johannes Lichtenberger
 */
public final class VectorNode implements DataRecord {

  /** Maximum supported vector dimension to guard against corrupt data. */
  public static final int MAX_SUPPORTED_DIMENSION = 65536;

  /** Unique node key within the vector index page. */
  private final long nodeKey;

  /** Key of the document node this vector embedding belongs to. */
  private final long documentNodeKey;

  /** The float vector embedding. Stored as a contiguous array for cache locality. */
  private final float[] vector;

  /** Maximum HNSW layer this node participates in (0-based). */
  private final int maxLayer;

  /**
   * Per-layer neighbor arrays. neighbors[layer][i] holds the node key of the i-th neighbor
   * at that layer. Arrays may be larger than neighborCounts[layer] -- only the first
   * neighborCounts[layer] entries are valid.
   */
  private final long[][] neighbors;

  /** Actual neighbor count per layer. */
  private final int[] neighborCounts;

  /** The revision number when this node was last persisted. */
  private final int previousRevisionNumber;

  /**
   * Constructor for creating a new VectorNode (no neighbors yet).
   *
   * @param nodeKey                unique node key
   * @param documentNodeKey        the document node this vector belongs to
   * @param vector                 the embedding vector (must not be null)
   * @param maxLayer               maximum HNSW layer (0-based, >= 0)
   * @param previousRevisionNumber the revision number
   * @throws IllegalArgumentException if vector is null or maxLayer < 0
   */
  public VectorNode(final long nodeKey, final long documentNodeKey, final float[] vector,
      final int maxLayer, final int previousRevisionNumber) {
    if (vector == null) {
      throw new IllegalArgumentException("Vector must not be null");
    }
    if (vector.length > MAX_SUPPORTED_DIMENSION) {
      throw new IllegalArgumentException(
          "Vector dimension " + vector.length + " exceeds maximum " + MAX_SUPPORTED_DIMENSION);
    }
    if (maxLayer < 0) {
      throw new IllegalArgumentException("maxLayer must be >= 0, was: " + maxLayer);
    }
    this.nodeKey = nodeKey;
    this.documentNodeKey = documentNodeKey;
    this.vector = vector;
    this.maxLayer = maxLayer;
    this.neighbors = new long[maxLayer + 1][];
    this.neighborCounts = new int[maxLayer + 1];
    // Initialize all neighbor arrays to empty (not null) to avoid NPE in graph traversal.
    for (int i = 0; i <= maxLayer; i++) {
      this.neighbors[i] = new long[0];
    }
    this.previousRevisionNumber = previousRevisionNumber;
  }

  /**
   * Constructor for deserialization (all fields provided).
   *
   * @param nodeKey                unique node key
   * @param documentNodeKey        the document node this vector belongs to
   * @param vector                 the embedding vector (must not be null)
   * @param maxLayer               maximum HNSW layer (0-based, >= 0)
   * @param neighbors              per-layer neighbor arrays (must not be null, length == maxLayer + 1)
   * @param neighborCounts         per-layer neighbor counts (must not be null, length == maxLayer + 1)
   * @param previousRevisionNumber the revision number
   * @throws IllegalArgumentException if any argument is invalid
   */
  public VectorNode(final long nodeKey, final long documentNodeKey, final float[] vector,
      final int maxLayer, final long[][] neighbors, final int[] neighborCounts,
      final int previousRevisionNumber) {
    if (vector == null) {
      throw new IllegalArgumentException("Vector must not be null");
    }
    if (vector.length > MAX_SUPPORTED_DIMENSION) {
      throw new IllegalArgumentException(
          "Vector dimension " + vector.length + " exceeds maximum " + MAX_SUPPORTED_DIMENSION);
    }
    if (maxLayer < 0) {
      throw new IllegalArgumentException("maxLayer must be >= 0, was: " + maxLayer);
    }
    if (neighbors == null || neighbors.length != maxLayer + 1) {
      throw new IllegalArgumentException("neighbors array length must be maxLayer + 1");
    }
    if (neighborCounts == null || neighborCounts.length != maxLayer + 1) {
      throw new IllegalArgumentException("neighborCounts array length must be maxLayer + 1");
    }
    this.nodeKey = nodeKey;
    this.documentNodeKey = documentNodeKey;
    this.vector = vector;
    this.maxLayer = maxLayer;
    this.neighbors = neighbors;
    this.neighborCounts = neighborCounts;
    this.previousRevisionNumber = previousRevisionNumber;
  }

  /**
   * Get the document node key this vector embedding belongs to.
   *
   * @return the document node key
   */
  public long getDocumentNodeKey() {
    return documentNodeKey;
  }

  /**
   * Get the embedding vector. Returns the internal array reference for performance.
   * Callers must not modify the returned array.
   *
   * @return the float vector (never null)
   */
  public float[] getVector() {
    return vector;
  }

  /**
   * Get the dimension (length) of the embedding vector.
   *
   * @return the vector dimension
   */
  public int getDimension() {
    return vector.length;
  }

  /**
   * Get the maximum HNSW layer this node participates in.
   *
   * @return the max layer (0-based)
   */
  public int getMaxLayer() {
    return maxLayer;
  }

  /**
   * Get the neighbor node keys for the specified layer.
   * Returns the internal array reference for performance; only the first
   * {@link #getNeighborCount(int)} entries are valid.
   *
   * @param layer the layer (0 to maxLayer inclusive)
   * @return the neighbor keys array, or null if no neighbors set for this layer
   * @throws IndexOutOfBoundsException if layer is out of range
   */
  public long[] getNeighbors(final int layer) {
    return neighbors[layer];
  }

  /**
   * Get the actual number of neighbors at the specified layer.
   *
   * @param layer the layer (0 to maxLayer inclusive)
   * @return the neighbor count
   * @throws IndexOutOfBoundsException if layer is out of range
   */
  public int getNeighborCount(final int layer) {
    return neighborCounts[layer];
  }

  /**
   * Set the neighbors for a specific layer. The provided array is stored directly
   * (no defensive copy) for performance.
   *
   * @param layer        the layer (0 to maxLayer inclusive)
   * @param neighborKeys the neighbor node keys (must not be null)
   * @param count        the number of valid entries in neighborKeys
   * @throws IndexOutOfBoundsException  if layer is out of range
   * @throws IllegalArgumentException   if neighborKeys is null or count > neighborKeys.length
   */
  public void setNeighbors(final int layer, final long[] neighborKeys, final int count) {
    if (neighborKeys == null) {
      throw new IllegalArgumentException("neighborKeys must not be null");
    }
    if (count < 0 || count > neighborKeys.length) {
      throw new IllegalArgumentException(
          "count must be in [0, neighborKeys.length], was: " + count);
    }
    neighbors[layer] = neighborKeys;
    neighborCounts[layer] = count;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return null;
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    return null;
  }

  @Override
  public RecordSerializer getKind() {
    return NodeKind.VECTOR_NODE;
  }

  @Override
  public int getPreviousRevisionNumber() {
    return previousRevisionNumber;
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    return previousRevisionNumber;
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(nodeKey);
    result = 31 * result + Long.hashCode(documentNodeKey);
    result = 31 * result + Arrays.hashCode(vector);
    result = 31 * result + maxLayer;
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof VectorNode other)) {
      return false;
    }
    return nodeKey == other.nodeKey
        && documentNodeKey == other.documentNodeKey
        && maxLayer == other.maxLayer
        && Arrays.equals(vector, other.vector)
        && Arrays.equals(neighborCounts, other.neighborCounts)
        && deepEqualsNeighbors(other);
  }

  private boolean deepEqualsNeighbors(final VectorNode other) {
    for (int layer = 0; layer <= maxLayer; layer++) {
      final int count = neighborCounts[layer];
      if (count != other.neighborCounts[layer]) {
        return false;
      }
      final long[] myNeighbors = neighbors[layer];
      final long[] otherNeighbors = other.neighbors[layer];
      if (myNeighbors == null && otherNeighbors == null) {
        continue;
      }
      if (myNeighbors == null || otherNeighbors == null) {
        return false;
      }
      for (int i = 0; i < count; i++) {
        if (myNeighbors[i] != otherNeighbors[i]) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return "VectorNode{nodeKey=" + nodeKey
        + ", documentNodeKey=" + documentNodeKey
        + ", dimension=" + vector.length
        + ", maxLayer=" + maxLayer
        + ", previousRevision=" + previousRevisionNumber + '}';
  }
}
