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

package io.sirix.index.vector.json;

import io.sirix.access.DatabaseType;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.vector.PageBackedVectorStore;
import io.sirix.index.vector.ReadOnlyPageBackedVectorStore;
import io.sirix.index.vector.VectorIndex;
import io.sirix.index.vector.VectorNode;
import io.sirix.index.vector.VectorSearchResult;
import io.sirix.index.vector.hnsw.HnswGraph;
import io.sirix.index.vector.hnsw.HnswParams;
import io.sirix.index.vector.ops.DistanceFunction;
import io.sirix.index.vector.ops.VectorDistanceType;

/**
 * JSON implementation of the {@link VectorIndex} interface.
 *
 * <p>Manages the lifecycle of HNSW-based vector indexes within a JSON resource:
 * creating the index tree, inserting vectors, and performing k-NN searches.</p>
 *
 * <p>Each method creates the necessary {@link PageBackedVectorStore} or
 * {@link ReadOnlyPageBackedVectorStore} on the fly. For high-frequency insert
 * workloads, callers should batch inserts within a single transaction to amortize
 * store/graph initialization costs.</p>
 *
 * @author Johannes Lichtenberger
 */
public final class JsonVectorIndexImpl implements VectorIndex {

  /**
   * The metadata node is always at key 1 (key 0 is the document root created by
   * {@code PageUtils.createTree}).
   */
  private static final long METADATA_NODE_KEY = 1L;

  @Override
  public void createIndex(final StorageEngineWriter writer, final IndexDef indexDef,
      final DatabaseType databaseType) {
    if (writer == null) {
      throw new IllegalArgumentException("writer must not be null");
    }
    if (indexDef == null) {
      throw new IllegalArgumentException("indexDef must not be null");
    }
    if (indexDef.getType() != IndexType.VECTOR) {
      throw new IllegalArgumentException("indexDef must be of type VECTOR, was: " + indexDef.getType());
    }

    final int indexNumber = indexDef.getID();
    final int dimension = indexDef.getDimension();
    final String distanceType = indexDef.getDistanceType();

    final PageBackedVectorStore store = new PageBackedVectorStore(
        writer, indexNumber, dimension, distanceType);
    store.initializeIndex(databaseType, writer.getRevisionToRepresent());
  }

  @Override
  public void insertVector(final StorageEngineWriter writer, final IndexDef indexDef,
      final long documentNodeKey, final float[] vector) {
    if (writer == null) {
      throw new IllegalArgumentException("writer must not be null");
    }
    if (indexDef == null) {
      throw new IllegalArgumentException("indexDef must not be null");
    }
    if (vector == null) {
      throw new IllegalArgumentException("vector must not be null");
    }
    if (indexDef.getType() != IndexType.VECTOR) {
      throw new IllegalArgumentException("indexDef must be of type VECTOR, was: " + indexDef.getType());
    }
    if (vector.length != indexDef.getDimension()) {
      throw new IllegalArgumentException(
          "vector dimension mismatch: expected " + indexDef.getDimension()
              + ", got " + vector.length);
    }
    PageBackedVectorStore.validateVector(vector, indexDef.getDimension());

    final int indexNumber = indexDef.getID();
    final int dimension = indexDef.getDimension();
    final String distanceType = indexDef.getDistanceType();

    // Create the store and load existing metadata.
    final PageBackedVectorStore store = new PageBackedVectorStore(
        writer, indexNumber, dimension, distanceType);
    store.loadMetadata(METADATA_NODE_KEY);

    // Build HNSW params from the IndexDef configuration.
    final VectorDistanceType distType = VectorDistanceType.valueOf(distanceType);
    final HnswParams params = HnswParams.builder(dimension, distType)
        .m(indexDef.getHnswM())
        .efConstruction(indexDef.getHnswEfConstruction())
        .build();

    // Create the graph with the store.
    final HnswGraph graph = new HnswGraph(store, params);

    // Generate a random level for this node.
    final int randomLevel = graph.generateRandomLevel();

    // Create the node in the store first (so the graph can access it).
    final long nodeKey = store.createNode(documentNodeKey, vector, randomLevel);

    // Insert into the HNSW graph (builds bidirectional links).
    graph.insert(nodeKey);
  }

  @Override
  public void deleteVector(final StorageEngineWriter writer, final IndexDef indexDef,
      final long hnswNodeKey) {
    if (writer == null) {
      throw new IllegalArgumentException("writer must not be null");
    }
    if (indexDef == null) {
      throw new IllegalArgumentException("indexDef must not be null");
    }
    if (indexDef.getType() != IndexType.VECTOR) {
      throw new IllegalArgumentException("indexDef must be of type VECTOR, was: " + indexDef.getType());
    }

    final int indexNumber = indexDef.getID();
    final int dimension = indexDef.getDimension();
    final String distanceType = indexDef.getDistanceType();

    final PageBackedVectorStore store = new PageBackedVectorStore(
        writer, indexNumber, dimension, distanceType);
    store.loadMetadata(METADATA_NODE_KEY);

    // Build HNSW params and graph to perform full neighbor repair on delete.
    final VectorDistanceType distType = VectorDistanceType.valueOf(distanceType);
    final HnswParams params = HnswParams.builder(dimension, distType)
        .m(indexDef.getHnswM())
        .efConstruction(indexDef.getHnswEfConstruction())
        .build();

    final HnswGraph graph = new HnswGraph(store, params);
    graph.delete(hnswNodeKey);
  }

  @Override
  public VectorSearchResult searchKnn(final StorageEngineReader reader, final IndexDef indexDef,
      final float[] query, final int k) {
    return searchKnn(reader, indexDef, query, k, indexDef.getHnswEfSearch());
  }

  @Override
  public VectorSearchResult searchKnn(final StorageEngineReader reader, final IndexDef indexDef,
      final float[] query, final int k, final int efSearch) {
    if (reader == null) {
      throw new IllegalArgumentException("reader must not be null");
    }
    if (indexDef == null) {
      throw new IllegalArgumentException("indexDef must not be null");
    }
    if (query == null) {
      throw new IllegalArgumentException("query must not be null");
    }
    if (k <= 0) {
      throw new IllegalArgumentException("k must be positive, was: " + k);
    }
    if (efSearch <= 0) {
      throw new IllegalArgumentException("efSearch must be positive, was: " + efSearch);
    }
    if (indexDef.getType() != IndexType.VECTOR) {
      throw new IllegalArgumentException("indexDef must be of type VECTOR, was: " + indexDef.getType());
    }
    if (query.length != indexDef.getDimension()) {
      throw new IllegalArgumentException(
          "query dimension mismatch: expected " + indexDef.getDimension()
              + ", got " + query.length);
    }
    PageBackedVectorStore.validateVector(query, indexDef.getDimension());

    final int indexNumber = indexDef.getID();

    // Create read-only store with metadata eagerly loaded.
    final ReadOnlyPageBackedVectorStore roStore = new ReadOnlyPageBackedVectorStore(
        reader, indexNumber, METADATA_NODE_KEY);

    // If the graph is empty, return an empty result.
    if (roStore.getEntryPointKey() == -1) {
      return VectorSearchResult.empty();
    }

    // Build HNSW params from IndexDef.
    final int dimension = indexDef.getDimension();
    final String distanceType = indexDef.getDistanceType();
    final VectorDistanceType distType = VectorDistanceType.valueOf(distanceType);
    final HnswParams params = HnswParams.builder(dimension, distType)
        .m(indexDef.getHnswM())
        .efConstruction(indexDef.getHnswEfConstruction())
        .efSearch(efSearch)
        .build();

    // Create graph for navigation (read-only — no inserts).
    final HnswGraph graph = new HnswGraph(roStore, params);

    // Perform the k-NN search with the provided efSearch. Returns HNSW internal node keys
    // sorted by distance.
    final long[] hnswNodeKeys = graph.searchKnn(query, k, efSearch);

    if (hnswNodeKeys.length == 0) {
      return VectorSearchResult.empty();
    }

    // Translate HNSW node keys to document node keys and compute distances.
    final int resultCount = hnswNodeKeys.length;
    final long[] documentNodeKeys = new long[resultCount];
    final float[] distances = new float[resultCount];
    final DistanceFunction distFn = distType.getDistanceFunction(dimension);

    for (int i = 0; i < resultCount; i++) {
      final long hnswKey = hnswNodeKeys[i];
      final VectorNode vectorNode = reader.getRecord(hnswKey, IndexType.VECTOR, indexNumber);
      documentNodeKeys[i] = vectorNode.getDocumentNodeKey();
      distances[i] = distFn.distance(query, vectorNode.getVector());
    }

    return new VectorSearchResult(documentNodeKeys, distances, resultCount);
  }
}
