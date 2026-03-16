package io.sirix.index.vector;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseType;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.index.vector.hnsw.VectorStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for {@link PageBackedVectorStore} and {@link ReadOnlyPageBackedVectorStore}.
 *
 * <p>These tests verify that HNSW graph data (vectors, neighbor lists, metadata) can be
 * correctly created, read, modified, and persisted through SirixDB's page layer.</p>
 */
class PageBackedVectorStoreTest {

  private static final int DIMENSION = 4;
  private static final String DISTANCE_TYPE = "L2";
  private static final int INDEX_NUMBER = 0;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  @DisplayName("Create vector nodes and read back their vectors")
  void testInsertAndReadVectors() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      final PageBackedVectorStore store = new PageBackedVectorStore(
          writer, INDEX_NUMBER, DIMENSION, DISTANCE_TYPE);

      store.initializeIndex(DatabaseType.JSON, 0);

      // Insert two vectors.
      final float[] v1 = {1.0f, 2.0f, 3.0f, 4.0f};
      final float[] v2 = {5.0f, 6.0f, 7.0f, 8.0f};
      final long key1 = store.createNode(100L, v1, 0);
      final long key2 = store.createNode(200L, v2, 1);

      // Keys should be distinct and after the metadata node.
      assertTrue(key1 > store.getMetadataNodeKey(), "First vector key must be after metadata");
      assertTrue(key2 > key1, "Second vector key must be after first");

      // Read back the vectors.
      assertArrayEquals(v1, store.getVector(key1), "Vector 1 mismatch");
      assertArrayEquals(v2, store.getVector(key2), "Vector 2 mismatch");
    }
  }

  @Test
  @DisplayName("Set and get neighbors for a vector node")
  void testSetAndGetNeighbors() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      final PageBackedVectorStore store = new PageBackedVectorStore(
          writer, INDEX_NUMBER, DIMENSION, DISTANCE_TYPE);
      store.initializeIndex(DatabaseType.JSON, 0);

      // Create nodes.
      final float[] v1 = {1.0f, 0.0f, 0.0f, 0.0f};
      final float[] v2 = {0.0f, 1.0f, 0.0f, 0.0f};
      final float[] v3 = {0.0f, 0.0f, 1.0f, 0.0f};
      final long key1 = store.createNode(10L, v1, 1);
      final long key2 = store.createNode(20L, v2, 0);
      final long key3 = store.createNode(30L, v3, 0);

      // Set neighbors for key1 at layer 0.
      final long[] neighbors = {key2, key3};
      store.setNeighbors(key1, 0, neighbors, 2);

      // Read back neighbors.
      assertEquals(2, store.getNeighborCount(key1, 0));
      final long[] readNeighbors = store.getNeighbors(key1, 0);
      assertNotNull(readNeighbors);
      assertEquals(key2, readNeighbors[0]);
      assertEquals(key3, readNeighbors[1]);

      // Layer 1 should have no neighbors.
      assertEquals(0, store.getNeighborCount(key1, 1));
    }
  }

  @Test
  @DisplayName("Update entry point and verify it reads back correctly")
  void testEntryPointPersistence() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      final PageBackedVectorStore store = new PageBackedVectorStore(
          writer, INDEX_NUMBER, DIMENSION, DISTANCE_TYPE);
      store.initializeIndex(DatabaseType.JSON, 0);

      // Initially empty.
      assertEquals(-1, store.getEntryPointKey());
      assertEquals(-1, store.getMaxLevel());

      // Create a node and set it as entry point.
      final float[] v1 = {1.0f, 2.0f, 3.0f, 4.0f};
      final long key1 = store.createNode(100L, v1, 2);
      store.updateEntryPoint(key1, 2);

      // Verify from cache.
      assertEquals(key1, store.getEntryPointKey());
      assertEquals(2, store.getMaxLevel());

      // Verify from storage by reading the metadata record.
      final VectorIndexMetadataNode metadata = writer.getRecord(
          store.getMetadataNodeKey(), IndexType.VECTOR, INDEX_NUMBER);
      assertNotNull(metadata);
      assertEquals(key1, metadata.getEntryPointKey());
      assertEquals(2, metadata.getMaxLevel());
    }
  }

  @Test
  @DisplayName("Dimension returns the configured dimensionality")
  void testDimension() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      final PageBackedVectorStore store = new PageBackedVectorStore(
          writer, INDEX_NUMBER, DIMENSION, DISTANCE_TYPE);

      assertEquals(DIMENSION, store.getDimension());
    }
  }

  @Test
  @DisplayName("Reject vectors with wrong dimensionality")
  void testDimensionMismatchThrows() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      final PageBackedVectorStore store = new PageBackedVectorStore(
          writer, INDEX_NUMBER, DIMENSION, DISTANCE_TYPE);
      store.initializeIndex(DatabaseType.JSON, 0);

      final float[] wrongDim = {1.0f, 2.0f}; // dimension 2 instead of 4
      assertThrows(IllegalArgumentException.class,
          () -> store.createNode(100L, wrongDim, 0));
    }
  }

  @Test
  @DisplayName("MaxLayer is correctly reported for vector nodes")
  void testMaxLayer() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      final PageBackedVectorStore store = new PageBackedVectorStore(
          writer, INDEX_NUMBER, DIMENSION, DISTANCE_TYPE);
      store.initializeIndex(DatabaseType.JSON, 0);

      final float[] v1 = {1.0f, 2.0f, 3.0f, 4.0f};
      final long key1 = store.createNode(100L, v1, 3);

      assertEquals(3, store.getMaxLayer(key1));
    }
  }

  @Test
  @DisplayName("Vectors survive commit and can be read via read-only transaction")
  void testPersistenceAcrossCommit() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

    long vectorKey;
    long metadataKey;

    // Write phase.
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      final PageBackedVectorStore store = new PageBackedVectorStore(
          writer, INDEX_NUMBER, DIMENSION, DISTANCE_TYPE);
      store.initializeIndex(DatabaseType.JSON, 0);

      final float[] v1 = {0.1f, 0.2f, 0.3f, 0.4f};
      vectorKey = store.createNode(42L, v1, 0);
      metadataKey = store.getMetadataNodeKey();

      store.updateEntryPoint(vectorKey, 0);

      trx.commit();
    }

    // Read phase with read-only transaction.
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var rtx = session.beginNodeReadOnlyTrx()) {

      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final ReadOnlyPageBackedVectorStore roStore = new ReadOnlyPageBackedVectorStore(
          reader, INDEX_NUMBER, metadataKey);

      // Verify metadata.
      assertEquals(vectorKey, roStore.getEntryPointKey());
      assertEquals(0, roStore.getMaxLevel());
      assertEquals(DIMENSION, roStore.getDimension());

      // Verify vector.
      final float[] readVector = roStore.getVector(vectorKey);
      assertArrayEquals(new float[]{0.1f, 0.2f, 0.3f, 0.4f}, readVector, 1e-6f);
    }
  }

  @Test
  @DisplayName("Read-only store throws on mutation methods")
  void testReadOnlyMutationsThrow() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

    long metadataKey;

    // Write phase: create the index.
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      final PageBackedVectorStore store = new PageBackedVectorStore(
          writer, INDEX_NUMBER, DIMENSION, DISTANCE_TYPE);
      store.initializeIndex(DatabaseType.JSON, 0);
      metadataKey = store.getMetadataNodeKey();
      trx.commit();
    }

    // Read phase.
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var rtx = session.beginNodeReadOnlyTrx()) {

      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final ReadOnlyPageBackedVectorStore roStore = new ReadOnlyPageBackedVectorStore(
          reader, INDEX_NUMBER, metadataKey);

      assertThrows(UnsupportedOperationException.class,
          () -> roStore.createNode(1L, new float[]{1, 2, 3, 4}, 0));
      assertThrows(UnsupportedOperationException.class,
          () -> roStore.setNeighbors(1L, 0, new long[]{}, 0));
      assertThrows(UnsupportedOperationException.class,
          () -> roStore.updateEntryPoint(1L, 0));
    }
  }

  @Test
  @DisplayName("Multiple vectors with neighbors, committed and read back")
  void testMultipleVectorsWithNeighborsAfterCommit() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

    long key1;
    long key2;
    long key3;
    long metadataKey;

    // Write phase.
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      final PageBackedVectorStore store = new PageBackedVectorStore(
          writer, INDEX_NUMBER, DIMENSION, DISTANCE_TYPE);
      store.initializeIndex(DatabaseType.JSON, 0);

      key1 = store.createNode(10L, new float[]{1, 0, 0, 0}, 1);
      key2 = store.createNode(20L, new float[]{0, 1, 0, 0}, 0);
      key3 = store.createNode(30L, new float[]{0, 0, 1, 0}, 0);

      // Connect key1 -> {key2, key3} at layer 0.
      store.setNeighbors(key1, 0, new long[]{key2, key3}, 2);
      // Connect key1 -> {} at layer 1 (no layer-1 neighbors).
      // Connect key2 -> {key1} at layer 0.
      store.setNeighbors(key2, 0, new long[]{key1}, 1);

      store.updateEntryPoint(key1, 1);
      metadataKey = store.getMetadataNodeKey();

      trx.commit();
    }

    // Read phase.
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var rtx = session.beginNodeReadOnlyTrx()) {

      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final ReadOnlyPageBackedVectorStore roStore = new ReadOnlyPageBackedVectorStore(
          reader, INDEX_NUMBER, metadataKey);

      // Verify entry point.
      assertEquals(key1, roStore.getEntryPointKey());
      assertEquals(1, roStore.getMaxLevel());

      // Verify vectors.
      assertArrayEquals(new float[]{1, 0, 0, 0}, roStore.getVector(key1));
      assertArrayEquals(new float[]{0, 1, 0, 0}, roStore.getVector(key2));
      assertArrayEquals(new float[]{0, 0, 1, 0}, roStore.getVector(key3));

      // Verify neighbors for key1 at layer 0.
      assertEquals(2, roStore.getNeighborCount(key1, 0));
      final long[] key1Neighbors = roStore.getNeighbors(key1, 0);
      assertNotNull(key1Neighbors);
      // After serialization, neighbors are delta-encoded (sorted), so they come back sorted.
      assertTrue(
          (key1Neighbors[0] == key2 && key1Neighbors[1] == key3)
              || (key1Neighbors[0] == key3 && key1Neighbors[1] == key2),
          "key1 neighbors should contain key2 and key3");

      // Verify neighbors for key2 at layer 0.
      assertEquals(1, roStore.getNeighborCount(key2, 0));
      final long[] key2Neighbors = roStore.getNeighbors(key2, 0);
      assertNotNull(key2Neighbors);
      assertEquals(key1, key2Neighbors[0]);

      // Max layers.
      assertEquals(1, roStore.getMaxLayer(key1));
      assertEquals(0, roStore.getMaxLayer(key2));
      assertEquals(0, roStore.getMaxLayer(key3));
    }
  }

  @Test
  @DisplayName("LoadMetadata correctly caches values from existing store")
  void testLoadMetadata() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

    long metadataKey;
    long vectorKey;

    // Write phase: populate the store.
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      final PageBackedVectorStore store = new PageBackedVectorStore(
          writer, INDEX_NUMBER, DIMENSION, DISTANCE_TYPE);
      store.initializeIndex(DatabaseType.JSON, 0);

      vectorKey = store.createNode(1L, new float[]{0.5f, 0.5f, 0.5f, 0.5f}, 0);
      store.updateEntryPoint(vectorKey, 0);
      metadataKey = store.getMetadataNodeKey();

      trx.commit();
    }

    // Reopen in a new write transaction and use loadMetadata.
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      final PageBackedVectorStore store = new PageBackedVectorStore(
          writer, INDEX_NUMBER, DIMENSION, DISTANCE_TYPE);
      store.loadMetadata(metadataKey);

      // Verify the loaded metadata.
      assertEquals(vectorKey, store.getEntryPointKey());
      assertEquals(0, store.getMaxLevel());
      assertEquals(metadataKey, store.getMetadataNodeKey());

      // Can still read the vector.
      assertArrayEquals(new float[]{0.5f, 0.5f, 0.5f, 0.5f}, store.getVector(vectorKey));
    }
  }
}
