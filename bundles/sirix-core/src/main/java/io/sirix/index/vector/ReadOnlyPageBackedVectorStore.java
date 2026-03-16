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

import io.sirix.api.StorageEngineReader;
import io.sirix.index.IndexType;
import io.sirix.index.vector.hnsw.VectorStore;

/**
 * Read-only VectorStore implementation for querying historical revisions of an HNSW graph.
 *
 * <p>This store uses the read-only {@link StorageEngineReader} to access
 * {@link VectorNode} and {@link VectorIndexMetadataNode} records from a committed revision.
 * All mutation methods throw {@link UnsupportedOperationException}.</p>
 *
 * @author Johannes Lichtenberger
 */
public final class ReadOnlyPageBackedVectorStore implements VectorStore {

  /** The storage engine reader for page-level read operations. */
  private final StorageEngineReader storageEngineReader;

  /** The index number (slot) within the VectorPage. */
  private final int indexNumber;

  /** The node key of the metadata record. */
  private final long metadataNodeKey;

  /** Cached entry point key. */
  private long cachedEntryPointKey;

  /** Cached max level. */
  private int cachedMaxLevel;

  /** Cached dimension. */
  private final int cachedDimension;

  /** Whether metadata has been loaded. */
  private boolean metadataLoaded;

  /**
   * Construct a new ReadOnlyPageBackedVectorStore.
   *
   * @param storageEngineReader the storage engine reader (must not be null)
   * @param indexNumber         the index number within the VectorPage
   * @param metadataNodeKey     the node key of the metadata record
   * @throws IllegalArgumentException if storageEngineReader is null
   */
  public ReadOnlyPageBackedVectorStore(final StorageEngineReader storageEngineReader,
      final int indexNumber, final long metadataNodeKey) {
    if (storageEngineReader == null) {
      throw new IllegalArgumentException("storageEngineReader must not be null");
    }
    this.storageEngineReader = storageEngineReader;
    this.indexNumber = indexNumber;
    this.metadataNodeKey = metadataNodeKey;

    // Eagerly load metadata to cache dimension, entry point, and max level.
    final VectorIndexMetadataNode metadata = storageEngineReader.getRecord(
        metadataNodeKey, IndexType.VECTOR, indexNumber);
    if (metadata != null) {
      this.cachedEntryPointKey = metadata.getEntryPointKey();
      this.cachedMaxLevel = metadata.getMaxLevel();
      this.cachedDimension = metadata.getDimension();
      this.metadataLoaded = true;
    } else {
      this.cachedEntryPointKey = -1;
      this.cachedMaxLevel = -1;
      this.cachedDimension = 0;
      this.metadataLoaded = false;
    }
  }

  @Override
  public float[] getVector(final long nodeKey) {
    final VectorNode node = storageEngineReader.getRecord(
        nodeKey, IndexType.VECTOR, indexNumber);
    if (node == null) {
      throw new IllegalArgumentException("No vector node at key: " + nodeKey);
    }
    if (cachedDimension > 0 && node.getDimension() != cachedDimension) {
      throw new IllegalStateException(
          "Dimension mismatch: expected " + cachedDimension
              + " but got " + node.getDimension());
    }
    return node.getVector();
  }

  @Override
  public long[] getNeighbors(final long nodeKey, final int layer) {
    final VectorNode node = storageEngineReader.getRecord(
        nodeKey, IndexType.VECTOR, indexNumber);
    if (node == null) {
      throw new IllegalArgumentException("No vector node at key: " + nodeKey);
    }
    return node.getNeighbors(layer);
  }

  @Override
  public int getNeighborCount(final long nodeKey, final int layer) {
    final VectorNode node = storageEngineReader.getRecord(
        nodeKey, IndexType.VECTOR, indexNumber);
    if (node == null) {
      throw new IllegalArgumentException("No vector node at key: " + nodeKey);
    }
    return node.getNeighborCount(layer);
  }

  @Override
  public int getMaxLayer(final long nodeKey) {
    final VectorNode node = storageEngineReader.getRecord(
        nodeKey, IndexType.VECTOR, indexNumber);
    if (node == null) {
      throw new IllegalArgumentException("No vector node at key: " + nodeKey);
    }
    return node.getMaxLayer();
  }

  @Override
  public void setNeighbors(final long nodeKey, final int layer,
      final long[] neighbors, final int count) {
    throw new UnsupportedOperationException("Read-only vector store does not support mutations");
  }

  @Override
  public long createNode(final long documentNodeKey, final float[] vector, final int maxLayer) {
    throw new UnsupportedOperationException("Read-only vector store does not support mutations");
  }

  @Override
  public void updateEntryPoint(final long entryPointKey, final int maxLevel) {
    throw new UnsupportedOperationException("Read-only vector store does not support mutations");
  }

  @Override
  public long getEntryPointKey() {
    return cachedEntryPointKey;
  }

  @Override
  public int getMaxLevel() {
    return cachedMaxLevel;
  }

  @Override
  public int getDimension() {
    return cachedDimension;
  }

  @Override
  public void markDeleted(final long nodeKey) {
    throw new UnsupportedOperationException("Read-only vector store does not support mutations");
  }

  @Override
  public boolean isDeleted(final long nodeKey) {
    final VectorNode node = storageEngineReader.getRecord(
        nodeKey, IndexType.VECTOR, indexNumber);
    if (node == null) {
      throw new IllegalArgumentException("No vector node at key: " + nodeKey);
    }
    return node.isDeleted();
  }

  @Override
  public long findNodeKeyByDocumentKey(final long documentNodeKey) {
    if (!metadataLoaded) {
      return -1L;
    }
    final VectorIndexMetadataNode metadata = storageEngineReader.getRecord(
        metadataNodeKey, IndexType.VECTOR, indexNumber);
    if (metadata == null) {
      return -1L;
    }
    final long nodeCount = metadata.getNodeCount();
    // Vector nodes start at key 2 (key 0 = doc root, key 1 = metadata).
    final long firstVectorKey = 2L;
    final long lastVectorKey = firstVectorKey + nodeCount - 1;
    for (long key = firstVectorKey; key <= lastVectorKey; key++) {
      final VectorNode node = storageEngineReader.getRecord(
          key, IndexType.VECTOR, indexNumber);
      if (node != null && node.getDocumentNodeKey() == documentNodeKey) {
        return key;
      }
    }
    return -1L;
  }
}
