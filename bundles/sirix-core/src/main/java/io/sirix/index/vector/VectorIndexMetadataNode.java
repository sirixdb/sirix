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

import java.util.Objects;

/**
 * DataRecord at nodeKey 0 that stores HNSW graph metadata for a vector index.
 * There is exactly one metadata node per vector index, always stored at node key 0.
 * It tracks the entry point, maximum level, dimensionality, distance metric,
 * and total node count of the HNSW graph.
 *
 * @author Johannes Lichtenberger
 */
public final class VectorIndexMetadataNode implements DataRecord {

  /** The node key for this metadata record. */
  private final long nodeKey;

  /** Node key of the HNSW entry point, or -1 if the graph is empty. */
  private long entryPointKey;

  /** Current maximum level in the HNSW graph, or -1 if empty. */
  private int maxLevel;

  /** Dimensionality of the vector embeddings. Fixed at index creation time. */
  private final int dimension;

  /** Distance metric type: "L2", "COSINE", or "INNER_PRODUCT". */
  private final String distanceType;

  /** Total number of vector nodes currently in the index. */
  private long nodeCount;

  /** The revision number when this metadata was last persisted. */
  private final int previousRevisionNumber;

  /**
   * Constructor for creating a new empty vector index metadata node.
   *
   * @param nodeKey                the node key for this metadata record
   * @param dimension              the vector dimension (must be > 0)
   * @param distanceType           the distance metric (must not be null)
   * @param previousRevisionNumber the revision number
   * @throws IllegalArgumentException if dimension <= 0 or distanceType is null
   */
  public VectorIndexMetadataNode(final long nodeKey, final int dimension,
      final String distanceType, final int previousRevisionNumber) {
    if (dimension <= 0) {
      throw new IllegalArgumentException("dimension must be > 0, was: " + dimension);
    }
    if (distanceType == null) {
      throw new IllegalArgumentException("distanceType must not be null");
    }
    this.nodeKey = nodeKey;
    this.entryPointKey = -1;
    this.maxLevel = -1;
    this.dimension = dimension;
    this.distanceType = distanceType;
    this.nodeCount = 0;
    this.previousRevisionNumber = previousRevisionNumber;
  }

  /**
   * Constructor for deserialization (all fields provided).
   *
   * @param nodeKey                the node key for this metadata record
   * @param entryPointKey          the entry point node key (-1 if empty)
   * @param maxLevel               the current max HNSW level (-1 if empty)
   * @param dimension              the vector dimension
   * @param distanceType           the distance metric
   * @param nodeCount              the total number of vector nodes
   * @param previousRevisionNumber the revision number
   * @throws IllegalArgumentException if dimension <= 0 or distanceType is null
   */
  public VectorIndexMetadataNode(final long nodeKey, final long entryPointKey, final int maxLevel,
      final int dimension, final String distanceType, final long nodeCount,
      final int previousRevisionNumber) {
    if (dimension <= 0) {
      throw new IllegalArgumentException("dimension must be > 0, was: " + dimension);
    }
    if (distanceType == null) {
      throw new IllegalArgumentException("distanceType must not be null");
    }
    this.nodeKey = nodeKey;
    this.entryPointKey = entryPointKey;
    this.maxLevel = maxLevel;
    this.dimension = dimension;
    this.distanceType = distanceType;
    this.nodeCount = nodeCount;
    this.previousRevisionNumber = previousRevisionNumber;
  }

  /**
   * Get the entry point node key.
   *
   * @return the entry point key, or -1 if the graph is empty
   */
  public long getEntryPointKey() {
    return entryPointKey;
  }

  /**
   * Set the entry point node key.
   *
   * @param key the new entry point key
   */
  public void setEntryPointKey(final long key) {
    this.entryPointKey = key;
  }

  /**
   * Get the current maximum HNSW level.
   *
   * @return the max level, or -1 if the graph is empty
   */
  public int getMaxLevel() {
    return maxLevel;
  }

  /**
   * Set the current maximum HNSW level.
   *
   * @param level the new max level
   */
  public void setMaxLevel(final int level) {
    this.maxLevel = level;
  }

  /**
   * Get the vector dimension.
   *
   * @return the dimension (always > 0)
   */
  public int getDimension() {
    return dimension;
  }

  /**
   * Get the distance metric type.
   *
   * @return the distance type string (never null)
   */
  public String getDistanceType() {
    return distanceType;
  }

  /**
   * Get the total number of vector nodes in the index.
   *
   * @return the node count
   */
  public long getNodeCount() {
    return nodeCount;
  }

  /**
   * Increment the node count by one.
   */
  public void incrementNodeCount() {
    this.nodeCount++;
  }

  /**
   * Set the node count directly (for deserialization or bulk operations).
   *
   * @param count the node count
   */
  public void setNodeCount(final long count) {
    this.nodeCount = count;
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
    return NodeKind.VECTOR_INDEX_METADATA;
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
    result = 31 * result + Long.hashCode(entryPointKey);
    result = 31 * result + maxLevel;
    result = 31 * result + dimension;
    result = 31 * result + distanceType.hashCode();
    result = 31 * result + Long.hashCode(nodeCount);
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof VectorIndexMetadataNode other)) {
      return false;
    }
    return nodeKey == other.nodeKey
        && entryPointKey == other.entryPointKey
        && maxLevel == other.maxLevel
        && dimension == other.dimension
        && nodeCount == other.nodeCount
        && Objects.equals(distanceType, other.distanceType);
  }

  @Override
  public String toString() {
    return "VectorIndexMetadataNode{entryPointKey=" + entryPointKey
        + ", maxLevel=" + maxLevel
        + ", dimension=" + dimension
        + ", distanceType=" + distanceType
        + ", nodeCount=" + nodeCount
        + ", previousRevision=" + previousRevisionNumber + '}';
  }
}
