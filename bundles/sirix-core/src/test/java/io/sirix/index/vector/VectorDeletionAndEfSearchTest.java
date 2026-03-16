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

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseType;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.vector.hnsw.HnswGraph;
import io.sirix.index.vector.hnsw.HnswParams;
import io.sirix.index.vector.hnsw.InMemoryVectorStore;
import io.sirix.index.vector.json.JsonVectorIndexImpl;
import io.sirix.index.vector.ops.VectorDistanceType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for tombstone-based vector deletion, query-time efSearch tuning,
 * and serialization format versioning.
 */
class VectorDeletionAndEfSearchTest {

  private static final int DIMENSION = 4;
  private static final String DISTANCE_TYPE = "L2";
  private static final int INDEX_DEF_NO = 0;

  /** Lightweight ResourceConfiguration for serialization tests. */
  private static final ResourceConfiguration CONFIG =
      ResourceConfiguration.newBuilder("test").hashKind(HashType.NONE).build();

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  // ---- Tombstone deletion tests (in-memory) ----

  @Test
  @DisplayName("Deleted vectors are excluded from HNSW kNN search results")
  void testDeletedVectorsExcludedFromSearch() {
    final int dim = 4;
    final var store = new InMemoryVectorStore(dim);
    final var params = HnswParams.defaults(dim, VectorDistanceType.L2);
    final var graph = new HnswGraph(store, params);

    // Insert 5 vectors: origin, unit vectors along each axis, one far away.
    final long k0 = store.createNode(0L, new float[]{0f, 0f, 0f, 0f}, 0);
    final long k1 = store.createNode(1L, new float[]{1f, 0f, 0f, 0f}, 0);
    final long k2 = store.createNode(2L, new float[]{0f, 1f, 0f, 0f}, 0);
    final long k3 = store.createNode(3L, new float[]{0f, 0f, 1f, 0f}, 0);
    final long k4 = store.createNode(4L, new float[]{10f, 10f, 10f, 10f}, 0);
    graph.insert(k0);
    graph.insert(k1);
    graph.insert(k2);
    graph.insert(k3);
    graph.insert(k4);

    // Before deletion, origin should be nearest to query near origin.
    final float[] query = {0.1f, 0.1f, 0.1f, 0.0f};
    final long[] before = graph.searchKnn(query, 3);
    assertTrue(before.length >= 1);
    assertEquals(k0, before[0], "Origin should be nearest before deletion");

    // Delete the origin node.
    store.markDeleted(k0);
    assertTrue(store.isDeleted(k0));
    assertFalse(store.isDeleted(k1));

    // After deletion, origin should NOT appear in results.
    final long[] after = graph.searchKnn(query, 3);
    for (final long key : after) {
      assertTrue(key != k0, "Deleted node (k0) must not appear in search results");
    }
  }

  @Test
  @DisplayName("InMemoryVectorStore markDeleted/isDeleted lifecycle")
  void testInMemoryVectorStoreDeleteLifecycle() {
    final var store = new InMemoryVectorStore(4);
    final long k0 = store.createNode(0L, new float[]{1f, 2f, 3f, 4f}, 0);
    final long k1 = store.createNode(1L, new float[]{5f, 6f, 7f, 8f}, 0);

    assertFalse(store.isDeleted(k0));
    assertFalse(store.isDeleted(k1));

    store.markDeleted(k0);
    assertTrue(store.isDeleted(k0));
    assertFalse(store.isDeleted(k1));

    // Marking again is idempotent.
    store.markDeleted(k0);
    assertTrue(store.isDeleted(k0));
  }

  @Test
  @DisplayName("All vectors deleted results in empty search")
  void testAllVectorsDeletedEmptyResult() {
    final int dim = 4;
    final var store = new InMemoryVectorStore(dim);
    final var params = HnswParams.defaults(dim, VectorDistanceType.L2);
    final var graph = new HnswGraph(store, params);

    final long k0 = store.createNode(0L, new float[]{1f, 0f, 0f, 0f}, 0);
    final long k1 = store.createNode(1L, new float[]{0f, 1f, 0f, 0f}, 0);
    graph.insert(k0);
    graph.insert(k1);

    store.markDeleted(k0);
    store.markDeleted(k1);

    final long[] results = graph.searchKnn(new float[]{0.5f, 0.5f, 0f, 0f}, 5);
    assertEquals(0, results.length, "All deleted should yield empty result");
  }

  // ---- Query-time efSearch override tests ----

  @Test
  @DisplayName("efSearch override produces valid results with different values")
  void testEfSearchOverride() {
    final int dim = 16;
    final int n = 100;
    final var rng = new java.util.Random(42);

    final var store = new InMemoryVectorStore(dim);
    final var params = HnswParams.builder(dim, VectorDistanceType.L2)
        .efConstruction(200)
        .efSearch(10) // Low default
        .build();
    final var graph = new HnswGraph(store, params);

    final float[][] vectors = new float[n][dim];
    for (int i = 0; i < n; i++) {
      for (int d = 0; d < dim; d++) {
        vectors[i][d] = rng.nextFloat();
      }
      final int level = graph.generateRandomLevel();
      store.createNode(i, vectors[i], level);
      graph.insert(i);
    }

    final float[] query = vectors[0];

    // Search with low efSearch (default 10).
    final long[] lowEfResults = graph.searchKnn(query, 5);
    assertTrue(lowEfResults.length > 0);

    // Search with higher efSearch override.
    final long[] highEfResults = graph.searchKnn(query, 5, 200);
    assertTrue(highEfResults.length > 0);

    // Both should find the exact match as the first result.
    assertEquals(0L, lowEfResults[0], "Exact match should be first with low efSearch");
    assertEquals(0L, highEfResults[0], "Exact match should be first with high efSearch");
  }

  @Test
  @DisplayName("IndexDef hnswEfSearch field roundtrips through materialize/init")
  void testIndexDefEfSearchRoundtrip() throws Exception {
    final IndexDef original = IndexDefs.createVectorIdxDef(
        128, "L2", Set.of(), 16, 200, 75, 5, IndexDef.DbType.JSON);

    assertEquals(75, original.getHnswEfSearch());

    // Materialize to node tree.
    final var materialized = original.materialize();
    assertNotNull(materialized);

    // Deserialize back via init().
    final IndexDef restored = new IndexDef(IndexDef.DbType.JSON);
    restored.init(materialized);

    assertEquals(75, restored.getHnswEfSearch());
    assertEquals(128, restored.getDimension());
    assertEquals("L2", restored.getDistanceType());
    assertEquals(16, restored.getHnswM());
    assertEquals(200, restored.getHnswEfConstruction());
  }

  @Test
  @DisplayName("Default IndexDef has hnswEfSearch = 50")
  void testIndexDefDefaultEfSearch() {
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        128, "L2", Set.of(), INDEX_DEF_NO, IndexDef.DbType.JSON);
    assertEquals(50, indexDef.getHnswEfSearch());
  }

  // ---- Serialization version byte roundtrip tests ----

  @Test
  @DisplayName("VectorNode serialization roundtrip includes version byte and deleted flag (not deleted)")
  void testVectorNodeSerializationVersionByteNotDeleted() {
    final float[] vector = {1.0f, 2.0f, 3.0f, 4.0f};
    final VectorNode original = new VectorNode(42L, 10L, vector, 0, 5);
    assertFalse(original.isDeleted());

    final VectorNode deserialized = serializeAndDeserializeVectorNode(original);

    assertEquals(42L, deserialized.getNodeKey());
    assertEquals(10L, deserialized.getDocumentNodeKey());
    assertArrayEquals(vector, deserialized.getVector(), 0.0f);
    assertEquals(0, deserialized.getMaxLayer());
    assertEquals(5, deserialized.getPreviousRevisionNumber());
    assertFalse(deserialized.isDeleted());
  }

  @Test
  @DisplayName("VectorNode serialization roundtrip preserves deleted=true flag")
  void testVectorNodeSerializationDeletedFlag() {
    final float[] vector = {1.0f, 2.0f, 3.0f};
    final long[][] neighbors = new long[1][];
    final int[] neighborCounts = new int[1];
    neighbors[0] = new long[]{5L, 10L};
    neighborCounts[0] = 2;

    final VectorNode original = new VectorNode(100L, 50L, vector, 0,
        neighbors, neighborCounts, 3, true);
    assertTrue(original.isDeleted());

    final VectorNode deserialized = serializeAndDeserializeVectorNode(original);

    assertEquals(100L, deserialized.getNodeKey());
    assertEquals(50L, deserialized.getDocumentNodeKey());
    assertArrayEquals(vector, deserialized.getVector(), 0.0f);
    assertEquals(0, deserialized.getMaxLayer());
    assertEquals(3, deserialized.getPreviousRevisionNumber());
    assertTrue(deserialized.isDeleted(), "Deleted flag should survive serialization roundtrip");

    // Neighbors should also survive.
    assertEquals(2, deserialized.getNeighborCount(0));
    assertEquals(5L, deserialized.getNeighbors(0)[0]);
    assertEquals(10L, deserialized.getNeighbors(0)[1]);
  }

  @Test
  @DisplayName("VectorIndexMetadataNode serialization roundtrip with version byte")
  void testMetadataNodeSerializationVersionByte() {
    final VectorIndexMetadataNode original = new VectorIndexMetadataNode(
        0L, 42L, 3, 128, "L2", 1000L, 5);

    final VectorIndexMetadataNode deserialized = serializeAndDeserializeMetadataNode(original);

    assertEquals(0L, deserialized.getNodeKey());
    assertEquals(42L, deserialized.getEntryPointKey());
    assertEquals(3, deserialized.getMaxLevel());
    assertEquals(128, deserialized.getDimension());
    assertEquals("L2", deserialized.getDistanceType());
    assertEquals(1000L, deserialized.getNodeCount());
    assertEquals(5, deserialized.getPreviousRevisionNumber());
  }

  @Test
  @DisplayName("VectorNode equals/hashCode includes deleted field")
  void testVectorNodeEqualityWithDeletedField() {
    final float[] vector = {1.0f, 2.0f, 3.0f};
    final long[][] neighbors = new long[1][];
    final int[] neighborCounts = new int[1];
    neighbors[0] = new long[0];
    neighborCounts[0] = 0;

    final VectorNode notDeleted = new VectorNode(1L, 1L, vector, 0,
        neighbors, neighborCounts, 0, false);
    final VectorNode deleted = new VectorNode(1L, 1L, vector, 0,
        new long[][]{new long[0]}, new int[]{0}, 0, true);

    assertFalse(notDeleted.equals(deleted),
        "VectorNodes with different deleted flags should not be equal");
    // They might have the same hashCode (collision is allowed), but likely different.
  }

  @Test
  @DisplayName("VectorNode markDeleted mutates the deleted flag")
  void testVectorNodeMarkDeleted() {
    final VectorNode node = new VectorNode(1L, 1L, new float[]{1f}, 0, 0);
    assertFalse(node.isDeleted());
    node.markDeleted();
    assertTrue(node.isDeleted());
  }

  // ---- Integration: deletion through full page-backed stack ----

  @Test
  @DisplayName("Delete vector via JsonVectorIndexImpl, verify excluded from search after commit")
  void testPageBackedDeletionIntegration() {
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        DIMENSION, DISTANCE_TYPE, Set.of(), INDEX_DEF_NO, IndexDef.DbType.JSON);

    final VectorIndex vectorIndex = new JsonVectorIndexImpl();

    // Known vectors: v0 at origin, v1 unit x, v2 far away.
    final float[][] vectors = {
        {0.0f, 0.0f, 0.0f, 0.0f},
        {1.0f, 0.0f, 0.0f, 0.0f},
        {10.0f, 10.0f, 10.0f, 10.0f}
    };

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

    // Insert, delete, and commit all in one transaction.
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      vectorIndex.createIndex(writer, indexDef, DatabaseType.JSON);

      for (int i = 0; i < vectors.length; i++) {
        vectorIndex.insertVector(writer, indexDef, 100L + i, vectors[i]);
      }

      // Delete vector v0 (hnswNodeKey=2, since docRoot=0, metadata=1, vectors start at 2).
      vectorIndex.deleteVector(writer, indexDef, 2L);

      trx.commit();
    }

    // Read phase: Search should NOT return docKey=100 (which was v0 at origin).
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final StorageEngineReader reader = rtx.getStorageEngineReader();

      // Diagnostic: verify the deleted flag is persisted for HNSW node key 2.
      final VectorNode deletedNode = reader.getRecord(
          2L, io.sirix.index.IndexType.VECTOR, INDEX_DEF_NO);
      assertNotNull(deletedNode, "VectorNode at key 2 should exist");
      assertTrue(deletedNode.isDeleted(),
          "VectorNode at key 2 should be marked as deleted after commit");

      final float[] query = {0.0f, 0.0f, 0.0f, 0.0f};
      final VectorSearchResult result = vectorIndex.searchKnn(reader, indexDef, query, 3);

      assertNotNull(result);
      for (int i = 0; i < result.count(); i++) {
        assertTrue(result.nodeKeyAt(i) != 100L,
            "Deleted vector (docKey=100) should not appear in search results");
      }
    }
  }


  @Test
  @DisplayName("createRecord with deleted VectorNode persists across commit")
  void testCreateDeletedVectorNodePersistence() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        DIMENSION, DISTANCE_TYPE, Set.of(), INDEX_DEF_NO, IndexDef.DbType.JSON);
    final VectorIndex vectorIndex = new JsonVectorIndexImpl();

    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {
      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      vectorIndex.createIndex(writer, indexDef, DatabaseType.JSON);

      // Create a VectorNode with deleted=true directly.
      final VectorNode deletedNode = new VectorNode(2L, 100L, new float[]{1f, 2f, 3f, 4f}, 0, 0);
      deletedNode.markDeleted();
      assertTrue(deletedNode.isDeleted());
      writer.createRecord(deletedNode, io.sirix.index.IndexType.VECTOR, INDEX_DEF_NO);

      trx.commit();
    }

    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final VectorNode node = reader.getRecord(
          2L, io.sirix.index.IndexType.VECTOR, INDEX_DEF_NO);
      assertNotNull(node, "VectorNode at key 2 should exist");
      assertTrue(node.isDeleted(),
          "VectorNode created with deleted=true should persist across commit/read");
    }
  }

  @Test
  @DisplayName("Deletion via markDeleted persists within same transaction commit")
  void testDirectMarkDeletedPersistence() {
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        DIMENSION, DISTANCE_TYPE, Set.of(), INDEX_DEF_NO, IndexDef.DbType.JSON);

    final VectorIndex vectorIndex = new JsonVectorIndexImpl();
    final float[][] vectors = {
        {0.0f, 0.0f, 0.0f, 0.0f},
        {1.0f, 0.0f, 0.0f, 0.0f},
    };

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

    // Insert and delete in the same transaction, then commit.
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {
      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      vectorIndex.createIndex(writer, indexDef, DatabaseType.JSON);
      for (int i = 0; i < vectors.length; i++) {
        vectorIndex.insertVector(writer, indexDef, 100L + i, vectors[i]);
      }

      // Delete vector at key 2 (the first inserted vector).
      vectorIndex.deleteVector(writer, indexDef, 2L);

      trx.commit();
    }

    // Read from committed revision.
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final VectorNode node = reader.getRecord(
          2L, io.sirix.index.IndexType.VECTOR, INDEX_DEF_NO);
      assertNotNull(node);
      assertTrue(node.isDeleted(),
          "VectorNode deleted flag should persist after commit");
    }
  }

  @Test
  @DisplayName("efSearch override through full page-backed integration stack")
  void testPageBackedEfSearchOverrideIntegration() {
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        DIMENSION, DISTANCE_TYPE, Set.of(), INDEX_DEF_NO, IndexDef.DbType.JSON);

    final VectorIndex vectorIndex = new JsonVectorIndexImpl();

    final float[][] vectors = {
        {0.0f, 0.0f, 0.0f, 0.0f},
        {1.0f, 0.0f, 0.0f, 0.0f},
        {0.0f, 1.0f, 0.0f, 0.0f},
        {0.0f, 0.0f, 1.0f, 0.0f},
    };

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      vectorIndex.createIndex(writer, indexDef, DatabaseType.JSON);

      for (int i = 0; i < vectors.length; i++) {
        vectorIndex.insertVector(writer, indexDef, 200L + i, vectors[i]);
      }

      trx.commit();
    }

    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final StorageEngineReader reader = rtx.getStorageEngineReader();

      // Search with default efSearch.
      final VectorSearchResult defaultResult = vectorIndex.searchKnn(
          reader, indexDef, vectors[0], 2);
      assertNotNull(defaultResult);
      assertTrue(defaultResult.count() > 0);

      // Search with explicitly high efSearch.
      final VectorSearchResult highEfResult = vectorIndex.searchKnn(
          reader, indexDef, vectors[0], 2, 200);
      assertNotNull(highEfResult);
      assertTrue(highEfResult.count() > 0);

      // Both should find the origin as nearest.
      assertEquals(200L, defaultResult.nodeKeyAt(0));
      assertEquals(200L, highEfResult.nodeKeyAt(0));
    }
  }

  // ---- Helper methods ----

  private static VectorNode serializeAndDeserializeVectorNode(final VectorNode original) {
    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    NodeKind.VECTOR_NODE.serialize(sink, original, CONFIG);

    final BytesIn<?> source = sink.asBytesIn();
    return (VectorNode) NodeKind.VECTOR_NODE.deserialize(
        source, original.getNodeKey(), null, CONFIG);
  }

  private static VectorIndexMetadataNode serializeAndDeserializeMetadataNode(
      final VectorIndexMetadataNode original) {
    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    NodeKind.VECTOR_INDEX_METADATA.serialize(sink, original, CONFIG);

    final BytesIn<?> source = sink.asBytesIn();
    return (VectorIndexMetadataNode) NodeKind.VECTOR_INDEX_METADATA.deserialize(
        source, original.getNodeKey(), null, CONFIG);
  }
}
