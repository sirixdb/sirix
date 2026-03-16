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

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Time-travel tests for vector indexes.
 *
 * <p>Verifies that searching at historical revisions returns only the vectors that
 * existed at that point in time, leveraging SirixDB's temporal versioning.</p>
 */
class VectorIndexTimeTravelTest {

  private static final int DIMENSION = 16;
  private static final String DISTANCE_TYPE = "L2";
  private static final int INDEX_DEF_NO = 0;
  private static final int VECTORS_PER_REVISION = 20;
  private static final long SEED = 42L;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  @DisplayName("Search at revision 1 returns only revision-1 vectors; revision 2 returns all vectors")
  void testTimeTravelSearchAcrossRevisions() {
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        DIMENSION, DISTANCE_TYPE, Set.of(), INDEX_DEF_NO, IndexDef.DbType.JSON);

    final VectorIndex vectorIndex = new JsonVectorIndexImpl();
    final Random rng = new Random(SEED);

    // Generate distinct vectors for revision 1 and revision 2.
    final float[][] rev1Vectors = generateRandomVectors(VECTORS_PER_REVISION, DIMENSION, rng);
    final float[][] rev2Vectors = generateRandomVectors(VECTORS_PER_REVISION, DIMENSION, rng);

    // Revision 1: create index tree, insert 20 vectors, commit.
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      vectorIndex.createIndex(writer, indexDef, DatabaseType.JSON);

      for (int i = 0; i < VECTORS_PER_REVISION; i++) {
        vectorIndex.insertVector(writer, indexDef, 1000L + i, rev1Vectors[i]);
      }

      trx.commit();
    }

    // Revision 2: insert 20 more vectors, commit.
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();

      for (int i = 0; i < VECTORS_PER_REVISION; i++) {
        vectorIndex.insertVector(writer, indexDef, 2000L + i, rev2Vectors[i]);
      }

      trx.commit();
    }

    // Search at revision 1: should only find vectors from revision 1.
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1)) {

      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final int k = VECTORS_PER_REVISION;
      final VectorSearchResult result = vectorIndex.searchKnn(reader, indexDef, rev1Vectors[0], k);

      assertNotNull(result);
      assertFalse(result.isEmpty(), "Should find results at revision 1");

      // All results must be from revision 1 (docKeys in [1000, 1020)).
      for (int i = 0; i < result.count(); i++) {
        final long docKey = result.nodeKeyAt(i);
        assertTrue(docKey >= 1000L && docKey < 1000L + VECTORS_PER_REVISION,
            "At revision 1, docKey should be in [1000, 1020), was: " + docKey);
      }
    }

    // Search at revision 2: should find vectors from both revisions.
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(2)) {

      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final int k = VECTORS_PER_REVISION * 2;
      final VectorSearchResult result = vectorIndex.searchKnn(reader, indexDef, rev1Vectors[0], k);

      assertNotNull(result);
      assertFalse(result.isEmpty(), "Should find results at revision 2");

      // Collect all document keys returned.
      final Set<Long> docKeys = new HashSet<>();
      for (int i = 0; i < result.count(); i++) {
        docKeys.add(result.nodeKeyAt(i));
      }

      // At revision 2, the result set should include vectors from revision 1.
      boolean hasRev1 = false;
      for (final long key : docKeys) {
        if (key >= 1000L && key < 1000L + VECTORS_PER_REVISION) {
          hasRev1 = true;
          break;
        }
      }
      assertTrue(hasRev1, "Revision 2 search should include revision-1 vectors");

      // The nearest neighbor to rev1Vectors[0] should still be itself.
      assertEquals(1000L, result.nodeKeyAt(0),
          "Nearest neighbor at revision 2 should still be rev1Vectors[0] (docKey=1000)");
      assertEquals(0.0f, result.distanceAt(0), 1e-5f);
    }
  }

  @Test
  @DisplayName("Revision 1 is isolated from revision 2 additions when searching revision 1")
  void testRevision1IsolatedFromRevision2() {
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        DIMENSION, DISTANCE_TYPE, Set.of(), INDEX_DEF_NO, IndexDef.DbType.JSON);

    final VectorIndex vectorIndex = new JsonVectorIndexImpl();

    // Revision 1: insert 5 vectors at known positions.
    final float[][] rev1Vectors = {
        createUniformVector(DIMENSION, 1.0f),
        createUniformVector(DIMENSION, 2.0f),
        createUniformVector(DIMENSION, 3.0f),
        createUniformVector(DIMENSION, 4.0f),
        createUniformVector(DIMENSION, 5.0f)
    };

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      vectorIndex.createIndex(writer, indexDef, DatabaseType.JSON);

      for (int i = 0; i < rev1Vectors.length; i++) {
        vectorIndex.insertVector(writer, indexDef, 10L + i, rev1Vectors[i]);
      }

      trx.commit();
    }

    // Revision 2: insert 10 additional vectors close to the rev1 distribution.
    final float[][] rev2Vectors = new float[10][];
    for (int i = 0; i < 10; i++) {
      // Values 1.5, 2.5, ..., interleaved with rev1 values 1, 2, ...
      rev2Vectors[i] = createUniformVector(DIMENSION, 1.5f + i);
    }

    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      for (int i = 0; i < rev2Vectors.length; i++) {
        vectorIndex.insertVector(writer, indexDef, 900L + i, rev2Vectors[i]);
      }
      trx.commit();
    }

    // Search at revision 1: all results must be from revision 1 (docKeys in [10, 15)).
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1)) {

      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final VectorSearchResult result = vectorIndex.searchKnn(reader, indexDef, rev1Vectors[0], 10);

      assertNotNull(result);
      assertFalse(result.isEmpty());
      for (int i = 0; i < result.count(); i++) {
        final long docKey = result.nodeKeyAt(i);
        assertTrue(docKey >= 10L && docKey < 15L,
            "Revision 1 should only contain docKeys [10,15), was: " + docKey);
      }
    }

    // Search at revision 2: rev1 vectors should still be findable.
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(2)) {

      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final VectorSearchResult result = vectorIndex.searchKnn(reader, indexDef, rev1Vectors[0], 5);

      assertNotNull(result);
      assertFalse(result.isEmpty());
      // The nearest neighbor to rev1Vectors[0] should still be itself (docKey=10).
      assertEquals(10L, result.nodeKeyAt(0),
          "Revision 2: nearest to rev1Vectors[0] should still be docKey=10");
      assertEquals(0.0f, result.distanceAt(0), 1e-5f);
    }
  }

  /**
   * Creates a vector where all components have the same value.
   */
  private static float[] createUniformVector(final int dimension, final float value) {
    final float[] vector = new float[dimension];
    for (int i = 0; i < dimension; i++) {
      vector[i] = value;
    }
    return vector;
  }

  /**
   * Generates random vectors using the provided RNG instance.
   */
  private static float[][] generateRandomVectors(final int count, final int dimension,
      final Random rng) {
    final float[][] vectors = new float[count][dimension];
    for (int i = 0; i < count; i++) {
      for (int j = 0; j < dimension; j++) {
        vectors[i][j] = rng.nextFloat() * 10.0f;
      }
    }
    return vectors;
  }
}
