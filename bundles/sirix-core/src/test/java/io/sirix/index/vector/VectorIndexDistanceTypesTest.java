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
 * Tests for all three distance types: L2, COSINE, and INNER_PRODUCT.
 *
 * <p>For each distance type, inserts known vectors where the nearest neighbor
 * can be verified analytically, then confirms the HNSW search returns the
 * expected result.</p>
 */
class VectorIndexDistanceTypesTest {

  private static final int DIMENSION = 4;
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
  @DisplayName("L2 distance: nearest neighbor is the closest by Euclidean distance")
  void testL2Distance() {
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        DIMENSION, "L2", Set.of(), INDEX_DEF_NO, IndexDef.DbType.JSON);

    final VectorIndex vectorIndex = new JsonVectorIndexImpl();

    // Known L2 distances from query [1,0,0,0]:
    //   to [1.1, 0, 0, 0] = 0.1  (nearest)
    //   to [2,   0, 0, 0] = 1.0
    //   to [0,   0, 0, 0] = 1.0
    //   to [5,   5, 5, 5] = sqrt(16+25+25+25) = sqrt(91) ~ 9.54
    final float[][] vectors = {
        {1.1f, 0.0f, 0.0f, 0.0f},  // docKey=10 — nearest
        {2.0f, 0.0f, 0.0f, 0.0f},  // docKey=11
        {0.0f, 0.0f, 0.0f, 0.0f},  // docKey=12
        {5.0f, 5.0f, 5.0f, 5.0f}   // docKey=13 — farthest
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
      final float[] query = {1.0f, 0.0f, 0.0f, 0.0f};
      final VectorSearchResult result = vectorIndex.searchKnn(reader, indexDef, query, 4);

      assertNotNull(result);
      assertFalse(result.isEmpty());

      // Nearest should be [1.1, 0, 0, 0] (docKey=10) with distance ~0.1.
      assertEquals(10L, result.nodeKeyAt(0),
          "L2: nearest neighbor should be docKey=10 ([1.1,0,0,0])");
      assertEquals(0.1f, result.distanceAt(0), 0.01f,
          "L2: distance to nearest should be ~0.1");

      // Farthest in the top-4 should be docKey=13 ([5,5,5,5]).
      assertEquals(13L, result.nodeKeyAt(result.count() - 1),
          "L2: farthest in top-4 should be docKey=13 ([5,5,5,5])");

      // Distances should be sorted ascending.
      for (int i = 1; i < result.count(); i++) {
        assertTrue(result.distanceAt(i) >= result.distanceAt(i - 1),
            "L2: distances should be sorted ascending");
      }
    }
  }

  @Test
  @DisplayName("COSINE distance: nearest neighbor has smallest angular distance")
  void testCosineDistance() {
    // Cosine distance = 1 - cos(a,b).
    // Query: [1,0,0,0].
    // [1,0.01,0,0] is nearly parallel -> cosine dist ~0.
    // [0,1,0,0] is orthogonal -> cosine dist = 1.
    // [-1,0,0,0] is opposite -> cosine dist = 2.
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        DIMENSION, "COSINE", Set.of(), INDEX_DEF_NO, IndexDef.DbType.JSON);

    final VectorIndex vectorIndex = new JsonVectorIndexImpl();

    final float[][] vectors = {
        {1.0f, 0.01f, 0.0f, 0.0f},  // docKey=20 — nearly parallel (nearest)
        {0.0f, 1.0f, 0.0f, 0.0f},   // docKey=21 — orthogonal
        {-1.0f, 0.0f, 0.0f, 0.0f},  // docKey=22 — opposite (farthest)
        {0.7f, 0.7f, 0.0f, 0.0f}    // docKey=23 — 45 degrees
    };

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      vectorIndex.createIndex(writer, indexDef, DatabaseType.JSON);

      for (int i = 0; i < vectors.length; i++) {
        vectorIndex.insertVector(writer, indexDef, 20L + i, vectors[i]);
      }

      trx.commit();
    }

    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final float[] query = {1.0f, 0.0f, 0.0f, 0.0f};
      final VectorSearchResult result = vectorIndex.searchKnn(reader, indexDef, query, 4);

      assertNotNull(result);
      assertFalse(result.isEmpty());

      // Nearest should be the nearly parallel vector (docKey=20).
      assertEquals(20L, result.nodeKeyAt(0),
          "COSINE: nearest neighbor should be docKey=20 (nearly parallel)");
      assertTrue(result.distanceAt(0) < 0.01f,
          "COSINE: distance to nearly parallel vector should be very small, was: " + result.distanceAt(0));

      // The opposite vector (docKey=22) should be the farthest.
      assertEquals(22L, result.nodeKeyAt(result.count() - 1),
          "COSINE: farthest in top-4 should be docKey=22 (opposite direction)");
      assertTrue(result.distanceAt(result.count() - 1) > 1.5f,
          "COSINE: distance to opposite vector should be > 1.5, was: " + result.distanceAt(result.count() - 1));

      // Distances should be sorted ascending.
      for (int i = 1; i < result.count(); i++) {
        assertTrue(result.distanceAt(i) >= result.distanceAt(i - 1),
            "COSINE: distances should be sorted ascending");
      }
    }
  }

  @Test
  @DisplayName("INNER_PRODUCT distance: nearest neighbor has highest dot product")
  void testInnerProductDistance() {
    // Inner product distance = 1 - dot(a,b).
    // Query: [1,0,0,0].
    // [10,0,0,0] -> dot=10 -> dist=1-10=-9 (most similar, smallest distance).
    // [5,0,0,0]  -> dot=5  -> dist=1-5=-4.
    // [0,0,0,0]  -> dot=0  -> dist=1.
    // [-1,0,0,0] -> dot=-1 -> dist=2.
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        DIMENSION, "INNER_PRODUCT", Set.of(), INDEX_DEF_NO, IndexDef.DbType.JSON);

    final VectorIndex vectorIndex = new JsonVectorIndexImpl();

    final float[][] vectors = {
        {10.0f, 0.0f, 0.0f, 0.0f},  // docKey=30 — highest dot product (nearest)
        {5.0f, 0.0f, 0.0f, 0.0f},   // docKey=31
        {0.0f, 0.0f, 0.0f, 0.0f},   // docKey=32 — zero dot product
        {-1.0f, 0.0f, 0.0f, 0.0f}   // docKey=33 — negative dot product (farthest)
    };

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      vectorIndex.createIndex(writer, indexDef, DatabaseType.JSON);

      for (int i = 0; i < vectors.length; i++) {
        vectorIndex.insertVector(writer, indexDef, 30L + i, vectors[i]);
      }

      trx.commit();
    }

    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final float[] query = {1.0f, 0.0f, 0.0f, 0.0f};
      final VectorSearchResult result = vectorIndex.searchKnn(reader, indexDef, query, 4);

      assertNotNull(result);
      assertFalse(result.isEmpty());

      // Nearest should be [10,0,0,0] (docKey=30) with distance = 1 - 10 = -9.
      assertEquals(30L, result.nodeKeyAt(0),
          "INNER_PRODUCT: nearest neighbor should be docKey=30 (highest dot product)");
      assertEquals(-9.0f, result.distanceAt(0), 0.01f,
          "INNER_PRODUCT: distance should be 1 - dot(query, [10,0,0,0]) = -9");

      // Farthest should be [-1,0,0,0] (docKey=33) with distance = 1 - (-1) = 2.
      assertEquals(33L, result.nodeKeyAt(result.count() - 1),
          "INNER_PRODUCT: farthest should be docKey=33 (negative dot product)");
      assertEquals(2.0f, result.distanceAt(result.count() - 1), 0.01f,
          "INNER_PRODUCT: distance to [-1,0,0,0] should be 2.0");

      // Distances should be sorted ascending.
      for (int i = 1; i < result.count(); i++) {
        assertTrue(result.distanceAt(i) >= result.distanceAt(i - 1),
            "INNER_PRODUCT: distances should be sorted ascending");
      }
    }
  }

  @Test
  @DisplayName("L2 distance with normalized vectors produces correct ordering")
  void testL2WithNormalizedVectors() {
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        DIMENSION, "L2", Set.of(), INDEX_DEF_NO, IndexDef.DbType.JSON);

    final VectorIndex vectorIndex = new JsonVectorIndexImpl();

    // Normalized vectors on the unit sphere.
    final float invSqrt2 = (float) (1.0 / Math.sqrt(2.0));
    final float[][] vectors = {
        {1.0f, 0.0f, 0.0f, 0.0f},               // docKey=40
        {invSqrt2, invSqrt2, 0.0f, 0.0f},        // docKey=41 — 45 degrees from query
        {0.0f, 1.0f, 0.0f, 0.0f},                // docKey=42 — 90 degrees from query
        {-1.0f, 0.0f, 0.0f, 0.0f}                // docKey=43 — 180 degrees from query
    };

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      vectorIndex.createIndex(writer, indexDef, DatabaseType.JSON);

      for (int i = 0; i < vectors.length; i++) {
        vectorIndex.insertVector(writer, indexDef, 40L + i, vectors[i]);
      }

      trx.commit();
    }

    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final float[] query = {1.0f, 0.0f, 0.0f, 0.0f};
      final VectorSearchResult result = vectorIndex.searchKnn(reader, indexDef, query, 4);

      assertNotNull(result);
      assertEquals(4, result.count());

      // Nearest should be the query itself (docKey=40), distance = 0.
      assertEquals(40L, result.nodeKeyAt(0));
      assertEquals(0.0f, result.distanceAt(0), 1e-5f);

      // 45-degree vector (docKey=41) should be second.
      assertEquals(41L, result.nodeKeyAt(1),
          "L2 normalized: 45-degree vector should be second nearest");

      // The opposite vector (docKey=43, L2 dist = 2.0) should be farthest.
      assertEquals(43L, result.nodeKeyAt(3),
          "L2 normalized: opposite vector should be farthest");
      assertEquals(2.0f, result.distanceAt(3), 0.01f,
          "L2: distance to opposite unit vector should be 2.0");
    }
  }

  @Test
  @DisplayName("COSINE distance is scale-invariant: same direction at different magnitudes yields distance ~0")
  void testCosineScaleInvariance() {
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        DIMENSION, "COSINE", Set.of(), INDEX_DEF_NO, IndexDef.DbType.JSON);

    final VectorIndex vectorIndex = new JsonVectorIndexImpl();

    // Same direction, different magnitudes — cosine distance should be ~0.
    final float[][] vectors = {
        {1.0f, 1.0f, 0.0f, 0.0f},       // docKey=50 — magnitude sqrt(2)
        {100.0f, 100.0f, 0.0f, 0.0f},   // docKey=51 — magnitude 100*sqrt(2) (same direction)
        {0.001f, 0.001f, 0.0f, 0.0f},   // docKey=52 — magnitude tiny (same direction)
        {0.0f, 0.0f, 1.0f, 0.0f}        // docKey=53 — orthogonal
    };

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      vectorIndex.createIndex(writer, indexDef, DatabaseType.JSON);

      for (int i = 0; i < vectors.length; i++) {
        vectorIndex.insertVector(writer, indexDef, 50L + i, vectors[i]);
      }

      trx.commit();
    }

    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final float[] query = {1.0f, 1.0f, 0.0f, 0.0f};
      final VectorSearchResult result = vectorIndex.searchKnn(reader, indexDef, query, 4);

      assertNotNull(result);
      assertFalse(result.isEmpty());

      // The top 3 results should all be co-linear vectors with distance ~0.
      for (int i = 0; i < Math.min(3, result.count()); i++) {
        final long docKey = result.nodeKeyAt(i);
        assertTrue(docKey == 50L || docKey == 51L || docKey == 52L,
            "COSINE: co-linear vectors should be in top 3, got docKey=" + docKey + " at index " + i);
        assertTrue(result.distanceAt(i) < 0.01f,
            "COSINE: distance to co-linear vector should be ~0, was: " + result.distanceAt(i));
      }

      // The orthogonal vector (docKey=53) should be last with distance ~1.0.
      assertEquals(53L, result.nodeKeyAt(result.count() - 1),
          "COSINE: orthogonal vector should be farthest");
      assertEquals(1.0f, result.distanceAt(result.count() - 1), 0.01f,
          "COSINE: distance to orthogonal vector should be ~1.0");
    }
  }
}
