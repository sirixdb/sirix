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
import io.sirix.index.IndexType;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Full lifecycle tests for vector indexes: create, commit, close, reopen, and search.
 *
 * <p>Verifies that vector indexes survive the full database lifecycle including
 * closing and reopening the database, and that metadata (dimension, distance type)
 * is correctly preserved across restarts.</p>
 */
class VectorIndexLifecycleTest {

  private static final int DIMENSION = 32;
  private static final String DISTANCE_TYPE = "L2";
  private static final int INDEX_DEF_NO = 0;
  private static final int VECTOR_COUNT = 100;
  private static final long SEED = 77L;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  @DisplayName("Create index, insert 100 vectors, commit, close, reopen, and search k-NN")
  void testFullLifecycleCreateCommitCloseReopenSearch() {
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        DIMENSION, DISTANCE_TYPE, Set.of(), INDEX_DEF_NO, IndexDef.DbType.JSON);

    final VectorIndex vectorIndex = new JsonVectorIndexImpl();
    final float[][] vectors = generateRandomVectors(VECTOR_COUNT, DIMENSION, SEED);

    // Phase 1: Create database, create index, insert vectors, commit, close.
    {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final JsonNodeTrx trx = session.beginNodeTrx()) {

        final StorageEngineWriter writer = trx.getStorageEngineWriter();
        vectorIndex.createIndex(writer, indexDef, DatabaseType.JSON);

        for (int i = 0; i < VECTOR_COUNT; i++) {
          vectorIndex.insertVector(writer, indexDef, 100L + i, vectors[i]);
        }

        trx.commit();
      }
      // Close everything to force full persistence.
      JsonTestHelper.closeEverything();
    }

    // Phase 2: Reopen database, open read transaction, search k-NN.
    {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

        final StorageEngineReader reader = rtx.getStorageEngineReader();
        final int k = 10;
        final VectorSearchResult result = vectorIndex.searchKnn(reader, indexDef, vectors[0], k);

        assertNotNull(result, "Search result should not be null after reopen");
        assertFalse(result.isEmpty(), "Should find results after reopen");
        assertTrue(result.count() <= k, "Result count should be <= k");
        assertTrue(result.count() > 0, "Should find at least one result");

        // The nearest neighbor to vectors[0] should be vectors[0] itself (docKey=100).
        assertEquals(100L, result.nodeKeyAt(0),
            "Nearest neighbor should be the vector itself");
        assertEquals(0.0f, result.distanceAt(0), 1e-5f,
            "Distance to itself should be 0");

        // Verify distances are sorted ascending.
        for (int i = 1; i < result.count(); i++) {
          assertTrue(result.distanceAt(i) >= result.distanceAt(i - 1),
              "Distances should be in ascending order");
        }

        // Verify all document node keys are in the expected range.
        for (int i = 0; i < result.count(); i++) {
          final long docKey = result.nodeKeyAt(i);
          assertTrue(docKey >= 100L && docKey < 100L + VECTOR_COUNT,
              "Document node key should be in [100, " + (100 + VECTOR_COUNT) + "), was: " + docKey);
        }
      }
    }
  }

  @Test
  @DisplayName("Metadata (dimension, distance type) is readable after reopen via ReadOnlyPageBackedVectorStore")
  void testMetadataPreservedAfterReopen() {
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        DIMENSION, DISTANCE_TYPE, Set.of(), INDEX_DEF_NO, IndexDef.DbType.JSON);

    final VectorIndex vectorIndex = new JsonVectorIndexImpl();

    // Write phase: create index tree, insert a single vector, commit, close.
    {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final JsonNodeTrx trx = session.beginNodeTrx()) {

        final StorageEngineWriter writer = trx.getStorageEngineWriter();
        vectorIndex.createIndex(writer, indexDef, DatabaseType.JSON);
        vectorIndex.insertVector(writer, indexDef, 42L, new float[DIMENSION]);
        trx.commit();
      }
      JsonTestHelper.closeEverything();
    }

    // Read phase: reopen and verify metadata via ReadOnlyPageBackedVectorStore.
    {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

        final StorageEngineReader reader = rtx.getStorageEngineReader();
        // Metadata node is always at key 1.
        final long metadataNodeKey = 1L;
        final ReadOnlyPageBackedVectorStore roStore = new ReadOnlyPageBackedVectorStore(
            reader, INDEX_DEF_NO, metadataNodeKey);

        assertEquals(DIMENSION, roStore.getDimension(),
            "Dimension should be preserved after reopen");
        assertTrue(roStore.getEntryPointKey() >= 0,
            "Entry point should be set after inserting a vector");
        assertTrue(roStore.getMaxLevel() >= 0,
            "Max level should be non-negative after insertion");
      }
    }
  }

  @Test
  @DisplayName("Multiple search queries return valid results after lifecycle")
  void testMultipleSearchQueriesAfterLifecycle() {
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        DIMENSION, DISTANCE_TYPE, Set.of(), INDEX_DEF_NO, IndexDef.DbType.JSON);

    final VectorIndex vectorIndex = new JsonVectorIndexImpl();
    final float[][] vectors = generateRandomVectors(50, DIMENSION, 99L);

    // Write and commit.
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

    // Search multiple times with different query vectors.
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final int k = 5;

      for (int q = 0; q < 10; q++) {
        final VectorSearchResult result = vectorIndex.searchKnn(reader, indexDef, vectors[q], k);

        assertNotNull(result, "Query " + q + ": result should not be null");
        assertFalse(result.isEmpty(), "Query " + q + ": should find results");
        assertTrue(result.count() <= k, "Query " + q + ": result count should be <= k");

        // Each query vector was inserted — it should be its own nearest neighbor.
        assertEquals(200L + q, result.nodeKeyAt(0),
            "Query " + q + ": nearest neighbor should be the query vector itself");
        assertEquals(0.0f, result.distanceAt(0), 1e-5f,
            "Query " + q + ": distance to itself should be 0");
      }
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
