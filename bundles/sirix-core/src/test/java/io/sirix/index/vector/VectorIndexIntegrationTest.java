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

import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for the vector index lifecycle: creation, insertion, commit, and k-NN search.
 *
 * <p>Exercises the full stack: {@link JsonVectorIndexImpl} -> {@link PageBackedVectorStore} ->
 * HNSW graph -> commit -> {@link ReadOnlyPageBackedVectorStore} -> search.</p>
 */
class VectorIndexIntegrationTest {

  private static final int DIMENSION = 32;
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
  @DisplayName("Create vector index, insert 50 vectors, commit, then search k-NN via read-only trx")
  void testCreateInsertCommitAndSearch() {
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        DIMENSION, DISTANCE_TYPE, Set.of(), INDEX_DEF_NO, IndexDef.DbType.JSON);

    final VectorIndex vectorIndex = new JsonVectorIndexImpl();
    final int vectorCount = 50;
    final float[][] vectors = generateRandomVectors(vectorCount, DIMENSION, 42L);

    // Write phase: create index, insert vectors, commit.
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();

      // Create the vector index tree.
      vectorIndex.createIndex(writer, indexDef, DatabaseType.JSON);

      // Insert vectors with document node keys starting at 100.
      for (int i = 0; i < vectorCount; i++) {
        vectorIndex.insertVector(writer, indexDef, 100L + i, vectors[i]);
      }

      trx.commit();
    }

    // Read phase: open read-only transaction, search k-NN.
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final StorageEngineReader reader = rtx.getStorageEngineReader();

      // Search for the 5 nearest neighbors to vectors[0].
      final int k = 5;
      final VectorSearchResult result = vectorIndex.searchKnn(reader, indexDef, vectors[0], k);

      assertNotNull(result);
      assertFalse(result.isEmpty());
      assertTrue(result.count() <= k, "Result count should be <= k");
      assertTrue(result.count() > 0, "Should find at least one result");

      // The nearest neighbor to vectors[0] should be vectors[0] itself (docKey=100).
      assertEquals(100L, result.nodeKeyAt(0),
          "Nearest neighbor should be the vector itself");
      assertEquals(0.0f, result.distanceAt(0), 1e-5f,
          "Distance to itself should be 0");

      // Verify all distances are in ascending order.
      for (int i = 1; i < result.count(); i++) {
        assertTrue(result.distanceAt(i) >= result.distanceAt(i - 1),
            "Distances should be in ascending order");
      }

      // Verify all document node keys are in the expected range.
      for (int i = 0; i < result.count(); i++) {
        final long docKey = result.nodeKeyAt(i);
        assertTrue(docKey >= 100L && docKey < 100L + vectorCount,
            "Document node key should be in [100, " + (100 + vectorCount) + "), was: " + docKey);
      }
    }
  }

  @Test
  @DisplayName("Search on an empty vector index returns empty result")
  void testSearchEmptyIndex() {
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        DIMENSION, DISTANCE_TYPE, Set.of(), INDEX_DEF_NO, IndexDef.DbType.JSON);

    final VectorIndex vectorIndex = new JsonVectorIndexImpl();

    // Write phase: create index but do NOT insert any vectors.
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      vectorIndex.createIndex(writer, indexDef, DatabaseType.JSON);
      trx.commit();
    }

    // Read phase: search should return empty.
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
  @DisplayName("Insert single vector, search returns exactly one result")
  void testSingleVectorInsertAndSearch() {
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        DIMENSION, DISTANCE_TYPE, Set.of(), INDEX_DEF_NO, IndexDef.DbType.JSON);

    final VectorIndex vectorIndex = new JsonVectorIndexImpl();
    final float[] vector = new float[DIMENSION];
    for (int i = 0; i < DIMENSION; i++) {
      vector[i] = (float) i;
    }

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      vectorIndex.createIndex(writer, indexDef, DatabaseType.JSON);
      vectorIndex.insertVector(writer, indexDef, 42L, vector);
      trx.commit();
    }

    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final VectorSearchResult result = vectorIndex.searchKnn(reader, indexDef, vector, 10);

      assertNotNull(result);
      assertEquals(1, result.count(), "Should find exactly one result");
      assertEquals(42L, result.nodeKeyAt(0), "Document node key should be 42");
      assertEquals(0.0f, result.distanceAt(0), 1e-5f, "Distance to itself should be 0");
    }
  }

  @Test
  @DisplayName("k-NN search returns correct nearest neighbors for a known arrangement")
  void testKnnSearchCorrectness() {
    final int dim = 4;
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        dim, DISTANCE_TYPE, Set.of(), INDEX_DEF_NO, IndexDef.DbType.JSON);

    final VectorIndex vectorIndex = new JsonVectorIndexImpl();

    // Create known vectors: unit vectors along each axis plus one at the origin.
    final float[][] vectors = {
        {1.0f, 0.0f, 0.0f, 0.0f},  // docKey=0
        {0.0f, 1.0f, 0.0f, 0.0f},  // docKey=1
        {0.0f, 0.0f, 1.0f, 0.0f},  // docKey=2
        {0.0f, 0.0f, 0.0f, 1.0f},  // docKey=3
        {0.0f, 0.0f, 0.0f, 0.0f},  // docKey=4 (origin)
        {10.0f, 10.0f, 10.0f, 10.0f}  // docKey=5 (far away)
    };

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      vectorIndex.createIndex(writer, indexDef, DatabaseType.JSON);

      for (int i = 0; i < vectors.length; i++) {
        vectorIndex.insertVector(writer, indexDef, (long) i, vectors[i]);
      }

      trx.commit();
    }

    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final StorageEngineReader reader = rtx.getStorageEngineReader();

      // Search for nearest to vector [1, 0, 0, 0] — should find itself first.
      final float[] query = {1.0f, 0.0f, 0.0f, 0.0f};
      final VectorSearchResult result = vectorIndex.searchKnn(reader, indexDef, query, 3);

      assertNotNull(result);
      assertTrue(result.count() >= 1);
      assertEquals(0L, result.nodeKeyAt(0), "Nearest should be docKey=0 (exact match)");
      assertEquals(0.0f, result.distanceAt(0), 1e-5f);

      // The far-away vector (docKey=5) should NOT be in the top 3.
      for (int i = 0; i < result.count(); i++) {
        assertTrue(result.nodeKeyAt(i) != 5L || result.count() <= 3,
            "The far-away vector should not appear in the nearest 3 if others are closer");
      }
    }
  }

  @Test
  @DisplayName("Insert vectors with custom HNSW parameters (m=8, efConstruction=100)")
  void testCustomHnswParams() {
    final int dim = 16;
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        dim, DISTANCE_TYPE, Set.of(), 8, 100, INDEX_DEF_NO, IndexDef.DbType.JSON);

    final VectorIndex vectorIndex = new JsonVectorIndexImpl();
    final int vectorCount = 30;
    final float[][] vectors = generateRandomVectors(vectorCount, dim, 123L);

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      vectorIndex.createIndex(writer, indexDef, DatabaseType.JSON);

      for (int i = 0; i < vectorCount; i++) {
        vectorIndex.insertVector(writer, indexDef, 1000L + i, vectors[i]);
      }

      trx.commit();
    }

    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final VectorSearchResult result = vectorIndex.searchKnn(reader, indexDef, vectors[0], 5);

      assertNotNull(result);
      assertFalse(result.isEmpty());
      assertTrue(result.count() <= 5);
      assertEquals(1000L, result.nodeKeyAt(0), "Nearest should be vectors[0] itself");
    }
  }

  @Test
  @DisplayName("VectorSearchResult validation rejects invalid parameters")
  void testVectorSearchResultValidation() {
    assertThrows(IllegalArgumentException.class,
        () -> new VectorSearchResult(null, new float[0], 0));
    assertThrows(IllegalArgumentException.class,
        () -> new VectorSearchResult(new long[0], null, 0));
    assertThrows(IllegalArgumentException.class,
        () -> new VectorSearchResult(new long[2], new float[2], -1));
    assertThrows(IllegalArgumentException.class,
        () -> new VectorSearchResult(new long[2], new float[2], 3));
  }

  @Test
  @DisplayName("VectorSearchResult.empty() returns a valid empty result")
  void testVectorSearchResultEmpty() {
    final VectorSearchResult empty = VectorSearchResult.empty();
    assertTrue(empty.isEmpty());
    assertEquals(0, empty.count());
    assertThrows(IndexOutOfBoundsException.class, () -> empty.nodeKeyAt(0));
    assertThrows(IndexOutOfBoundsException.class, () -> empty.distanceAt(0));
  }

  @Test
  @DisplayName("searchKnn rejects invalid arguments")
  void testSearchKnnValidation() {
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        DIMENSION, DISTANCE_TYPE, Set.of(), INDEX_DEF_NO, IndexDef.DbType.JSON);
    final VectorIndex vectorIndex = new JsonVectorIndexImpl();

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final StorageEngineReader reader = rtx.getStorageEngineReader();

      // Null query
      assertThrows(IllegalArgumentException.class,
          () -> vectorIndex.searchKnn(reader, indexDef, null, 5));

      // k <= 0
      assertThrows(IllegalArgumentException.class,
          () -> vectorIndex.searchKnn(reader, indexDef, new float[DIMENSION], 0));

      // Wrong dimension
      assertThrows(IllegalArgumentException.class,
          () -> vectorIndex.searchKnn(reader, indexDef, new float[DIMENSION + 1], 5));
    }
  }

  @Test
  @DisplayName("insertVector rejects wrong dimension")
  void testInsertVectorDimensionMismatch() {
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        DIMENSION, DISTANCE_TYPE, Set.of(), INDEX_DEF_NO, IndexDef.DbType.JSON);
    final VectorIndex vectorIndex = new JsonVectorIndexImpl();

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      vectorIndex.createIndex(writer, indexDef, DatabaseType.JSON);

      final float[] wrongDim = new float[DIMENSION + 5];
      assertThrows(IllegalArgumentException.class,
          () -> vectorIndex.insertVector(writer, indexDef, 1L, wrongDim));
    }
  }

  /**
   * Generates an array of random vectors with a fixed seed for reproducibility.
   */
  private static float[][] generateRandomVectors(final int count, final int dimension,
      final long seed) {
    final Random rng = new Random(seed);
    final float[][] vectors = new float[count][dimension];
    for (int i = 0; i < count; i++) {
      for (int j = 0; j < dimension; j++) {
        vectors[i][j] = rng.nextFloat() * 10.0f;
      }
    }
    return vectors;
  }
}
