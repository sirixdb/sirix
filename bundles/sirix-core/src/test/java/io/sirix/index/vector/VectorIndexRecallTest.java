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
import io.sirix.index.vector.ops.DistanceFunction;
import io.sirix.index.vector.ops.VectorDistanceType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Recall quality tests for the HNSW vector index.
 *
 * <p>Inserts a large number of vectors, performs k-NN searches, and compares the
 * HNSW approximate results against brute-force exact search to verify recall quality.</p>
 */
class VectorIndexRecallTest {

  private static final int DIMENSION = 32;
  private static final String DISTANCE_TYPE = "L2";
  private static final int INDEX_DEF_NO = 0;
  private static final int VECTOR_COUNT = 1000;
  private static final int K = 10;
  private static final int NUM_QUERIES = 10;
  private static final double MIN_RECALL = 0.90;
  private static final long SEED = 12345L;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  @DisplayName("HNSW recall@10 >= 0.90 on 1000 random vectors with L2 distance")
  void testRecallAt10() {
    final IndexDef indexDef = IndexDefs.createVectorIdxDef(
        DIMENSION, DISTANCE_TYPE, Set.of(), INDEX_DEF_NO, IndexDef.DbType.JSON);

    final VectorIndex vectorIndex = new JsonVectorIndexImpl();
    final Random rng = new Random(SEED);
    final float[][] vectors = generateRandomVectors(VECTOR_COUNT, DIMENSION, rng);

    // Insert all vectors.
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {

      final StorageEngineWriter writer = trx.getStorageEngineWriter();
      vectorIndex.createIndex(writer, indexDef, DatabaseType.JSON);

      for (int i = 0; i < VECTOR_COUNT; i++) {
        vectorIndex.insertVector(writer, indexDef, (long) i, vectors[i]);
      }

      trx.commit();
    }

    // Generate query vectors (reuse some from the dataset, some new).
    final float[][] queries = new float[NUM_QUERIES][];
    for (int q = 0; q < NUM_QUERIES; q++) {
      if (q < NUM_QUERIES / 2) {
        // Use vectors from the dataset.
        queries[q] = vectors[rng.nextInt(VECTOR_COUNT)];
      } else {
        // Generate fresh random queries.
        queries[q] = new float[DIMENSION];
        for (int d = 0; d < DIMENSION; d++) {
          queries[q][d] = rng.nextFloat() * 10.0f;
        }
      }
    }

    // Compute brute-force exact k-NN for each query.
    final DistanceFunction distFn = VectorDistanceType.L2.getDistanceFunction(DIMENSION);
    final long[][] exactResults = new long[NUM_QUERIES][K];
    for (int q = 0; q < NUM_QUERIES; q++) {
      exactResults[q] = bruteForceKnn(vectors, queries[q], K, distFn);
    }

    // Search using HNSW and compute recall.
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final StorageEngineReader reader = rtx.getStorageEngineReader();
      double totalRecall = 0.0;

      for (int q = 0; q < NUM_QUERIES; q++) {
        final VectorSearchResult result = vectorIndex.searchKnn(reader, indexDef, queries[q], K);

        assertNotNull(result, "Query " + q + ": result should not be null");
        assertFalse(result.isEmpty(), "Query " + q + ": should find results");

        // Compute recall: fraction of exact top-k that appear in HNSW top-k.
        final Set<Long> hnswKeys = new HashSet<>();
        for (int i = 0; i < result.count(); i++) {
          hnswKeys.add(result.nodeKeyAt(i));
        }

        int hits = 0;
        for (int i = 0; i < K; i++) {
          if (hnswKeys.contains(exactResults[q][i])) {
            hits++;
          }
        }

        final double recall = (double) hits / K;
        totalRecall += recall;
      }

      final double averageRecall = totalRecall / NUM_QUERIES;
      assertTrue(averageRecall >= MIN_RECALL,
          "Average recall@" + K + " should be >= " + MIN_RECALL + ", was: " + averageRecall);
    }
  }

  /**
   * Brute-force exact k-NN search: computes distance from query to every vector,
   * sorts, and returns the top-k document node keys.
   *
   * @param vectors the dataset (indexed by document node key)
   * @param query   the query vector
   * @param k       number of nearest neighbors
   * @param distFn  the distance function
   * @return array of document node keys for the k nearest neighbors
   */
  private static long[] bruteForceKnn(final float[][] vectors, final float[] query,
      final int k, final DistanceFunction distFn) {
    final int n = vectors.length;
    final int[] indices = new int[n];
    final float[] distances = new float[n];

    for (int i = 0; i < n; i++) {
      indices[i] = i;
      distances[i] = distFn.distance(query, vectors[i]);
    }

    // Sort indices by distance ascending.
    final Integer[] boxedIndices = new Integer[n];
    for (int i = 0; i < n; i++) {
      boxedIndices[i] = i;
    }
    Arrays.sort(boxedIndices, (a, b) -> Float.compare(distances[a], distances[b]));

    final int resultCount = Math.min(k, n);
    final long[] result = new long[resultCount];
    for (int i = 0; i < resultCount; i++) {
      // Document node keys are the same as the insertion index.
      result[i] = (long) boxedIndices[i];
    }
    return result;
  }

  /**
   * Generates an array of random vectors with a fixed RNG for reproducibility.
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
