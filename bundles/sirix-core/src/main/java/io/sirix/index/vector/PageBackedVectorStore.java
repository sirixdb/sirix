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

import io.sirix.access.DatabaseType;
import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.PageContainer;
import io.sirix.index.IndexType;
import io.sirix.index.vector.hnsw.VectorStore;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.VectorPage;
import io.sirix.settings.Constants;

/**
 * VectorStore implementation backed by SirixDB's storage engine for persistent HNSW graph storage.
 *
 * <p>This store uses the page layer ({@link StorageEngineWriter}) to create, read, and modify
 * {@link VectorNode} and {@link VectorIndexMetadataNode} records within a vector index tree.
 * All writes go through the transaction intent log (TIL) and are persisted on commit.</p>
 *
 * <p>Layout within the index tree:
 * <ul>
 *   <li>Key 0: Document root (created by {@code PageUtils.createTree})</li>
 *   <li>Key 1: {@link VectorIndexMetadataNode} — graph-level metadata (entry point, max level)</li>
 *   <li>Key 2+: {@link VectorNode} instances — embedding vectors with neighbor lists</li>
 * </ul>
 *
 * <p>Metadata (entry point key, max level) is cached in-memory after initialization to avoid
 * repeated page lookups on every HNSW traversal step.</p>
 *
 * @author Johannes Lichtenberger
 */
public final class PageBackedVectorStore implements VectorStore {

  /** The storage engine writer for page-level operations. */
  private final StorageEngineWriter storageEngineWriter;

  /** The index number (slot) within the VectorPage. */
  private final int indexNumber;

  /** The vector dimensionality (fixed at creation time). */
  private final int dimension;

  /** The distance metric type string. */
  private final String distanceType;

  /** Cached entry point key (-1 if graph is empty). */
  private long cachedEntryPointKey = -1;

  /** Cached max level (-1 if graph is empty). */
  private int cachedMaxLevel = -1;

  /** The node key of the metadata record within this index tree. */
  private long metadataNodeKey = -1;

  /** Whether the metadata node has been created. */
  private boolean metadataInitialized;

  /**
   * Construct a new PageBackedVectorStore.
   *
   * @param storageEngineWriter the storage engine writer (must not be null)
   * @param indexNumber         the index number within the VectorPage
   * @param dimension           the vector dimensionality (must be > 0)
   * @param distanceType        the distance metric type (must not be null)
   * @throws IllegalArgumentException if dimension <= 0 or distanceType is null
   */
  public PageBackedVectorStore(final StorageEngineWriter storageEngineWriter,
      final int indexNumber, final int dimension, final String distanceType) {
    if (storageEngineWriter == null) {
      throw new IllegalArgumentException("storageEngineWriter must not be null");
    }
    if (dimension <= 0) {
      throw new IllegalArgumentException("dimension must be > 0, was: " + dimension);
    }
    if (distanceType == null) {
      throw new IllegalArgumentException("distanceType must not be null");
    }
    this.storageEngineWriter = storageEngineWriter;
    this.indexNumber = indexNumber;
    this.dimension = dimension;
    this.distanceType = distanceType;
    this.metadataInitialized = false;
  }

  /**
   * Initialize the vector index tree and create the metadata node.
   *
   * <p>This must be called once before any vector operations. It creates the index tree
   * in the VectorPage and inserts the metadata node at the first allocated key.</p>
   *
   * @param databaseType the database type (JSON or XML)
   * @param revision     the current revision number
   */
  public void initializeIndex(final DatabaseType databaseType, final int revision) {
    if (metadataInitialized) {
      return;
    }

    final RevisionRootPage revisionRootPage = storageEngineWriter.getActualRevisionRootPage();
    final PageReference vectorPageRef = revisionRootPage.getVectorPageReference();

    // Get the VectorPage: if never written to disk, it may still be set on the reference
    // from RevisionRootPage's constructor. If it's been committed and reloaded,
    // load it from the storage engine. If neither, create a fresh VectorPage.
    VectorPage vectorPage;
    if (vectorPageRef.getPage() != null) {
      vectorPage = (VectorPage) vectorPageRef.getPage();
    } else if (vectorPageRef.getKey() != Constants.NULL_ID_LONG
        || vectorPageRef.getLogKey() != Constants.NULL_ID_INT) {
      // Page has been written to disk or is in TIL — load it.
      vectorPage = storageEngineWriter.getVectorPage(revisionRootPage);
    } else {
      // Page has never been written — create fresh.
      vectorPage = new VectorPage();
      vectorPageRef.setPage(vectorPage);
    }

    // Add the VectorPage to the TIL so it gets committed.
    storageEngineWriter.appendLogRecord(vectorPageRef,
        PageContainer.getInstance(vectorPage, vectorPage));

    // Create the index tree (document root at key 0).
    vectorPage.createVectorIndexTree(databaseType, storageEngineWriter, indexNumber,
        storageEngineWriter.getLog());

    // Predict the next node key: maxNodeKey + 1.
    final long nextKey = vectorPage.getMaxNodeKey(indexNumber) + 1;

    // Create the metadata node at the next available key.
    final VectorIndexMetadataNode metadataNode = new VectorIndexMetadataNode(
        nextKey, dimension, distanceType, revision);
    storageEngineWriter.createRecord(metadataNode, IndexType.VECTOR, indexNumber);

    this.metadataNodeKey = nextKey;
    this.cachedEntryPointKey = -1;
    this.cachedMaxLevel = -1;
    this.metadataInitialized = true;
  }

  /**
   * Load metadata from an existing index tree (for reopening after commit).
   *
   * @param metadataKey the node key of the metadata record
   */
  public void loadMetadata(final long metadataKey) {
    this.metadataNodeKey = metadataKey;
    final VectorIndexMetadataNode metadata = storageEngineWriter.getRecord(
        metadataKey, IndexType.VECTOR, indexNumber);
    this.cachedEntryPointKey = metadata.getEntryPointKey();
    this.cachedMaxLevel = metadata.getMaxLevel();
    this.metadataInitialized = true;
  }

  /**
   * Get the node key of the metadata record.
   *
   * @return the metadata node key
   */
  public long getMetadataNodeKey() {
    return metadataNodeKey;
  }

  @Override
  public float[] getVector(final long nodeKey) {
    final VectorNode node = storageEngineWriter.getRecord(
        nodeKey, IndexType.VECTOR, indexNumber);
    if (node == null) {
      throw new IllegalArgumentException("No vector node at key: " + nodeKey);
    }
    return node.getVector();
  }

  @Override
  public long[] getNeighbors(final long nodeKey, final int layer) {
    final VectorNode node = storageEngineWriter.getRecord(
        nodeKey, IndexType.VECTOR, indexNumber);
    if (node == null) {
      throw new IllegalArgumentException("No vector node at key: " + nodeKey);
    }
    return node.getNeighbors(layer);
  }

  @Override
  public int getNeighborCount(final long nodeKey, final int layer) {
    final VectorNode node = storageEngineWriter.getRecord(
        nodeKey, IndexType.VECTOR, indexNumber);
    if (node == null) {
      throw new IllegalArgumentException("No vector node at key: " + nodeKey);
    }
    return node.getNeighborCount(layer);
  }

  @Override
  public int getMaxLayer(final long nodeKey) {
    final VectorNode node = storageEngineWriter.getRecord(
        nodeKey, IndexType.VECTOR, indexNumber);
    if (node == null) {
      throw new IllegalArgumentException("No vector node at key: " + nodeKey);
    }
    return node.getMaxLayer();
  }

  @Override
  public void setNeighbors(final long nodeKey, final int layer,
      final long[] neighbors, final int count) {
    final VectorNode node = storageEngineWriter.prepareRecordForModification(
        nodeKey, IndexType.VECTOR, indexNumber);
    node.setNeighbors(layer, neighbors, count);
  }

  @Override
  public long createNode(final long documentNodeKey, final float[] vector, final int maxLayer) {
    validateVector(vector, dimension);

    // Predict the next node key by reading the current max from VectorPage.
    final RevisionRootPage revisionRootPage = storageEngineWriter.getActualRevisionRootPage();
    final VectorPage vectorPage = storageEngineWriter.getVectorPage(revisionRootPage);
    final long nextKey = vectorPage.getMaxNodeKey(indexNumber) + 1;

    // Create the VectorNode with the predicted key.
    final int revision = storageEngineWriter.getRevisionToRepresent();
    final VectorNode vectorNode = new VectorNode(
        nextKey, documentNodeKey, vector, maxLayer, revision);

    // createRecord increments maxNodeKey and stores the record.
    storageEngineWriter.createRecord(vectorNode, IndexType.VECTOR, indexNumber);

    // Update node count in metadata.
    if (metadataInitialized && metadataNodeKey >= 0) {
      final VectorIndexMetadataNode metadata = storageEngineWriter.prepareRecordForModification(
          metadataNodeKey, IndexType.VECTOR, indexNumber);
      metadata.incrementNodeCount();
    }

    return nextKey;
  }

  @Override
  public void updateEntryPoint(final long entryPointKey, final int maxLevel) {
    if (metadataInitialized && metadataNodeKey >= 0) {
      final VectorIndexMetadataNode metadata = storageEngineWriter.prepareRecordForModification(
          metadataNodeKey, IndexType.VECTOR, indexNumber);
      metadata.setEntryPointKey(entryPointKey);
      metadata.setMaxLevel(maxLevel);
    }
    // Update cache.
    this.cachedEntryPointKey = entryPointKey;
    this.cachedMaxLevel = maxLevel;
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
    return dimension;
  }

  @Override
  public void markDeleted(final long nodeKey) {
    // Prepare the record for modification — this copies the record from the complete page
    // to the modified page and returns a mutable reference.
    final VectorNode node = storageEngineWriter.prepareRecordForModification(
        nodeKey, IndexType.VECTOR, indexNumber);
    if (node == null) {
      throw new IllegalArgumentException("No vector node at key: " + nodeKey);
    }
    node.markDeleted();

    // Decrement node count in metadata.
    if (metadataInitialized && metadataNodeKey >= 0) {
      final VectorIndexMetadataNode metadata = storageEngineWriter.prepareRecordForModification(
          metadataNodeKey, IndexType.VECTOR, indexNumber);
      final long currentCount = metadata.getNodeCount();
      if (currentCount > 0) {
        metadata.setNodeCount(currentCount - 1);
      }
    }
  }

  @Override
  public boolean isDeleted(final long nodeKey) {
    final VectorNode node = storageEngineWriter.getRecord(
        nodeKey, IndexType.VECTOR, indexNumber);
    if (node == null) {
      throw new IllegalArgumentException("No vector node at key: " + nodeKey);
    }
    return node.isDeleted();
  }

  @Override
  public long findNodeKeyByDocumentKey(final long documentNodeKey) {
    if (!metadataInitialized || metadataNodeKey < 0) {
      return -1L;
    }
    final VectorIndexMetadataNode metadata = storageEngineWriter.getRecord(
        metadataNodeKey, IndexType.VECTOR, indexNumber);
    final long nodeCount = metadata.getNodeCount();
    // Vector nodes start at key 2 (key 0 = doc root, key 1 = metadata).
    final long firstVectorKey = 2L;
    final long lastVectorKey = firstVectorKey + nodeCount - 1;
    for (long key = firstVectorKey; key <= lastVectorKey; key++) {
      final VectorNode node = storageEngineWriter.getRecord(
          key, IndexType.VECTOR, indexNumber);
      if (node != null && node.getDocumentNodeKey() == documentNodeKey) {
        return key;
      }
    }
    return -1L;
  }

  /**
   * Validates that a vector is non-null, has the expected dimension, and contains no NaN or
   * Infinity values.
   *
   * @param vector            the vector to validate
   * @param expectedDimension the expected dimensionality
   * @throws IllegalArgumentException if validation fails
   */
  public static void validateVector(final float[] vector, final int expectedDimension) {
    if (vector == null) {
      throw new IllegalArgumentException("Vector must not be null");
    }
    if (vector.length != expectedDimension) {
      throw new IllegalArgumentException(
          "Expected dimension " + expectedDimension + " but got " + vector.length);
    }
    if (vector.length > VectorNode.MAX_SUPPORTED_DIMENSION) {
      throw new IllegalArgumentException(
          "Vector dimension " + vector.length + " exceeds maximum "
              + VectorNode.MAX_SUPPORTED_DIMENSION);
    }
    for (int i = 0; i < vector.length; i++) {
      if (Float.isNaN(vector[i]) || Float.isInfinite(vector[i])) {
        throw new IllegalArgumentException(
            "Vector contains NaN or Infinity at index " + i);
      }
    }
  }
}
