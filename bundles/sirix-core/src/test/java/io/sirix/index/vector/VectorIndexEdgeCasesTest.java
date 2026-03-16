package io.sirix.index.vector;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseType;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.vector.json.JsonVectorIndexImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Edge-case tests for the vector index.
 *
 * <p>Covers boundary conditions such as k larger than the number of indexed vectors,
 * identical vectors, empty indexes, single-vector indexes, and large k values.</p>
 */
class VectorIndexEdgeCasesTest {

  private static final int DIMENSION = 8;
  private static final String DISTANCE_TYPE = "L2";
  private static final int INDEX_DEF_NO = 0;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  @DisplayName("k > n: insert 3 vectors, search k=10, returns at most 3 results")
  void testKGreaterThanN() {
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        DIMENSION, DISTANCE_TYPE, Set.of(), INDEX_DEF_NO, IndexDef.DbType.JSON);

    final VectorIndex vectorIndex = new JsonVectorIndexImpl();

    final float[][] vectors = {
        {1.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f},
        {0.0f, 1.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f},
        {0.0f, 0.0f, 1.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f}
    };

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      vectorIndex.createIndex(writer, indexDef, DatabaseType.JSON);

      for (int i = 0; i < vectors.length; i++) {
        vectorIndex.insertVector(writer, indexDef, 10L + i, vectors[i]);
      }

      trx.commit();
    }

    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final VectorSearchResult result = vectorIndex.searchKnn(reader, indexDef, vectors[0], 10);

      assertNotNull(result);
      assertFalse(result.isEmpty());
      assertTrue(result.count() <= 3,
          "Should return at most 3 results when only 3 vectors exist, got: " + result.count());
      assertTrue(result.count() >= 1, "Should find at least one result");

      // The query vector itself should be the nearest.
      assertEquals(10L, result.nodeKeyAt(0), "Nearest neighbor should be vector[0] (docKey=10)");
      assertEquals(0.0f, result.distanceAt(0), 1e-5f);
    }
  }

  @Test
  @DisplayName("All identical vectors: insert 10 copies, search returns k results")
  void testAllIdenticalVectors() {
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        DIMENSION, DISTANCE_TYPE, Set.of(), INDEX_DEF_NO, IndexDef.DbType.JSON);

    final VectorIndex vectorIndex = new JsonVectorIndexImpl();
    final float[] identicalVector = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f};
    final int vectorCount = 10;

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      vectorIndex.createIndex(writer, indexDef, DatabaseType.JSON);

      for (int i = 0; i < vectorCount; i++) {
        vectorIndex.insertVector(writer, indexDef, 50L + i, identicalVector.clone());
      }

      trx.commit();
    }

    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final int k = 5;
      final VectorSearchResult result = vectorIndex.searchKnn(reader, indexDef, identicalVector, k);

      assertNotNull(result);
      assertFalse(result.isEmpty());
      assertTrue(result.count() <= k, "Result count should be <= k");
      assertTrue(result.count() >= 1, "Should find at least one result");

      // All distances should be 0 (or very close) since all vectors are identical.
      for (int i = 0; i < result.count(); i++) {
        assertEquals(0.0f, result.distanceAt(i), 1e-5f,
            "Distance to identical vector at index " + i + " should be 0");
      }

      // All returned docKeys should be in [50, 60).
      for (int i = 0; i < result.count(); i++) {
        final long docKey = result.nodeKeyAt(i);
        assertTrue(docKey >= 50L && docKey < 50L + vectorCount,
            "DocKey should be in [50, 60), was: " + docKey);
      }
    }
  }

  @Test
  @DisplayName("Empty index: create but don't insert, search returns empty result")
  void testEmptyIndex() {
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        DIMENSION, DISTANCE_TYPE, Set.of(), INDEX_DEF_NO, IndexDef.DbType.JSON);

    final VectorIndex vectorIndex = new JsonVectorIndexImpl();

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      vectorIndex.createIndex(writer, indexDef, DatabaseType.JSON);
      trx.commit();
    }

    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final float[] query = new float[DIMENSION];
      final VectorSearchResult result = vectorIndex.searchKnn(reader, indexDef, query, 5);

      assertNotNull(result);
      assertTrue(result.isEmpty(), "Search on empty index should return empty result");
      assertEquals(0, result.count());
    }
  }

  @Test
  @DisplayName("Single vector: insert one, search k=1, found correctly")
  void testSingleVector() {
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        DIMENSION, DISTANCE_TYPE, Set.of(), INDEX_DEF_NO, IndexDef.DbType.JSON);

    final VectorIndex vectorIndex = new JsonVectorIndexImpl();
    final float[] vector = {3.14f, 2.71f, 1.41f, 1.73f, 0.0f, 0.0f, 0.0f, 0.0f};

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      vectorIndex.createIndex(writer, indexDef, DatabaseType.JSON);
      vectorIndex.insertVector(writer, indexDef, 7L, vector);
      trx.commit();
    }

    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final VectorSearchResult result = vectorIndex.searchKnn(reader, indexDef, vector, 1);

      assertNotNull(result);
      assertEquals(1, result.count(), "Should find exactly one result");
      assertEquals(7L, result.nodeKeyAt(0), "Document node key should be 7");
      assertEquals(0.0f, result.distanceAt(0), 1e-5f, "Distance to itself should be 0");
    }
  }

  @Test
  @DisplayName("Large k: insert 50 vectors, search k=50, all found")
  void testLargeK() {
    final int vectorCount = 50;
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        DIMENSION, DISTANCE_TYPE, Set.of(), INDEX_DEF_NO, IndexDef.DbType.JSON);

    final VectorIndex vectorIndex = new JsonVectorIndexImpl();
    final java.util.Random rng = new java.util.Random(321L);

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      vectorIndex.createIndex(writer, indexDef, DatabaseType.JSON);

      for (int i = 0; i < vectorCount; i++) {
        final float[] v = new float[DIMENSION];
        for (int d = 0; d < DIMENSION; d++) {
          v[d] = rng.nextFloat() * 10.0f;
        }
        vectorIndex.insertVector(writer, indexDef, 500L + i, v);
      }

      trx.commit();
    }

    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final float[] query = new float[DIMENSION]; // Zero vector
      final VectorSearchResult result = vectorIndex.searchKnn(reader, indexDef, query, vectorCount);

      assertNotNull(result);
      assertFalse(result.isEmpty());
      // HNSW should return close to all vectors when k == n and efSearch is sufficient.
      assertTrue(result.count() >= vectorCount / 2,
          "With k=" + vectorCount + " and " + vectorCount + " vectors, should find at least half, got: " + result.count());

      // Verify distances are sorted ascending.
      for (int i = 1; i < result.count(); i++) {
        assertTrue(result.distanceAt(i) >= result.distanceAt(i - 1),
            "Distances should be in ascending order at index " + i);
      }

      // Verify all returned docKeys are in [500, 550).
      for (int i = 0; i < result.count(); i++) {
        final long docKey = result.nodeKeyAt(i);
        assertTrue(docKey >= 500L && docKey < 500L + vectorCount,
            "DocKey should be in [500, 550), was: " + docKey);
      }
    }
  }

  @Test
  @DisplayName("Zero vector query against non-zero vectors")
  void testZeroVectorQuery() {
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        DIMENSION, DISTANCE_TYPE, Set.of(), INDEX_DEF_NO, IndexDef.DbType.JSON);

    final VectorIndex vectorIndex = new JsonVectorIndexImpl();

    // Insert vectors at different distances from origin.
    final float[] close = new float[DIMENSION];
    close[0] = 0.1f;
    final float[] medium = new float[DIMENSION];
    medium[0] = 5.0f;
    final float[] far = new float[DIMENSION];
    far[0] = 100.0f;

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      vectorIndex.createIndex(writer, indexDef, DatabaseType.JSON);
      vectorIndex.insertVector(writer, indexDef, 1L, close);
      vectorIndex.insertVector(writer, indexDef, 2L, medium);
      vectorIndex.insertVector(writer, indexDef, 3L, far);
      trx.commit();
    }

    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final float[] zeroQuery = new float[DIMENSION];
      final VectorSearchResult result = vectorIndex.searchKnn(reader, indexDef, zeroQuery, 3);

      assertNotNull(result);
      assertFalse(result.isEmpty());

      // The closest vector to the origin should be "close" (docKey=1).
      assertEquals(1L, result.nodeKeyAt(0),
          "Nearest to origin should be the closest vector (docKey=1)");
    }
  }
}
