package io.sirix.index.vector.hnsw;

import io.sirix.index.vector.ops.VectorDistanceType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HnswGraphTest {

  @Test
  void singleElementGraph() {
    final int dim = 4;
    final var store = new InMemoryVectorStore(dim);
    final var params = HnswParams.defaults(dim, VectorDistanceType.L2);
    final var graph = new HnswGraph(store, params);

    final float[] vector = {1.0f, 2.0f, 3.0f, 4.0f};
    final long nodeKey = store.createNode(0L, vector, 0);
    graph.insert(nodeKey);

    final long[] results = graph.searchKnn(new float[]{1.0f, 2.0f, 3.0f, 4.0f}, 1);
    assertEquals(1, results.length);
    assertEquals(nodeKey, results[0]);
  }

  @Test
  void insertAndSearch100VectorsDim32() {
    final int dim = 32;
    final int n = 100;
    final int k = 10;
    final var rng = new Random(42);

    final var store = new InMemoryVectorStore(dim);
    final var params = HnswParams.defaults(dim, VectorDistanceType.L2);
    final var graph = new HnswGraph(store, params);

    final float[][] vectors = new float[n][dim];
    final long[] nodeKeys = new long[n];

    for (int i = 0; i < n; i++) {
      for (int d = 0; d < dim; d++) {
        vectors[i][d] = rng.nextFloat();
      }
      final int level = graph.generateRandomLevel();
      nodeKeys[i] = store.createNode(i, vectors[i], level);
      graph.insert(nodeKeys[i]);
    }

    // Search with a query from the dataset
    final float[] query = vectors[0];
    final long[] results = graph.searchKnn(query, k);

    assertTrue(results.length <= k, "Should return at most k results");
    assertTrue(results.length > 0, "Should return at least one result");

    // The exact query vector should be in the results
    boolean foundExact = false;
    for (final long r : results) {
      if (r == nodeKeys[0]) {
        foundExact = true;
        break;
      }
    }
    assertTrue(foundExact, "Exact match should be found in k-NN results");

    // Verify results are in ascending distance order
    final var distFn = VectorDistanceType.L2.getDistanceFunction(dim);
    float prevDist = -1.0f;
    for (final long r : results) {
      final float dist = distFn.distance(query, store.getVector(r));
      assertTrue(dist >= prevDist, "Results should be sorted by distance ascending");
      prevDist = dist;
    }
  }

  @Test
  void recallAtK10With500VectorsDim128() {
    final int dim = 128;
    final int n = 500;
    final int k = 10;
    final int numQueries = 50;
    final var rng = new Random(123);

    final var store = new InMemoryVectorStore(dim);
    final var params = HnswParams.builder(dim, VectorDistanceType.L2)
        .efConstruction(200)
        .efSearch(100)
        .build();
    final var graph = new HnswGraph(store, params);

    final float[][] vectors = new float[n][dim];
    final long[] nodeKeys = new long[n];

    for (int i = 0; i < n; i++) {
      for (int d = 0; d < dim; d++) {
        vectors[i][d] = rng.nextFloat();
      }
      final int level = graph.generateRandomLevel();
      nodeKeys[i] = store.createNode(i, vectors[i], level);
      graph.insert(nodeKeys[i]);
    }

    final var distFn = VectorDistanceType.L2.getDistanceFunction(dim);
    double totalRecall = 0.0;

    for (int q = 0; q < numQueries; q++) {
      final float[] query = new float[dim];
      for (int d = 0; d < dim; d++) {
        query[d] = rng.nextFloat();
      }

      // Brute-force ground truth
      final long[] groundTruth = bruteForceKnn(vectors, nodeKeys, query, k, distFn);

      // HNSW search
      final long[] hnswResults = graph.searchKnn(query, k);

      // Compute recall
      int hits = 0;
      for (final long hr : hnswResults) {
        for (final long gt : groundTruth) {
          if (hr == gt) {
            hits++;
            break;
          }
        }
      }
      totalRecall += (double) hits / k;
    }

    final double avgRecall = totalRecall / numQueries;
    assertTrue(avgRecall >= 0.90,
        "Average recall@" + k + " should be >= 0.90, but was " + String.format("%.4f", avgRecall));
  }

  @ParameterizedTest
  @EnumSource(VectorDistanceType.class)
  void testAllDistanceTypes(final VectorDistanceType distanceType) {
    final int dim = 16;
    final int n = 50;
    final int k = 5;
    final var rng = new Random(distanceType.ordinal() + 1);

    final var store = new InMemoryVectorStore(dim);
    final var params = HnswParams.defaults(dim, distanceType);
    final var graph = new HnswGraph(store, params);

    final float[][] vectors = new float[n][dim];
    final long[] nodeKeys = new long[n];

    for (int i = 0; i < n; i++) {
      for (int d = 0; d < dim; d++) {
        vectors[i][d] = rng.nextFloat();
      }
      // Normalize for cosine/inner product
      if (distanceType == VectorDistanceType.COSINE || distanceType == VectorDistanceType.INNER_PRODUCT) {
        normalize(vectors[i]);
      }
      final int level = graph.generateRandomLevel();
      nodeKeys[i] = store.createNode(i, vectors[i], level);
      graph.insert(nodeKeys[i]);
    }

    final float[] query = new float[dim];
    for (int d = 0; d < dim; d++) {
      query[d] = rng.nextFloat();
    }
    if (distanceType == VectorDistanceType.COSINE || distanceType == VectorDistanceType.INNER_PRODUCT) {
      normalize(query);
    }

    final long[] results = graph.searchKnn(query, k);
    assertTrue(results.length > 0, "Should return results for " + distanceType);
    assertTrue(results.length <= k, "Should return at most k results for " + distanceType);

    // Verify distance ordering
    final var distFn = distanceType.getDistanceFunction(dim);
    float prevDist = -Float.MAX_VALUE;
    for (final long r : results) {
      final float dist = distFn.distance(query, store.getVector(r));
      assertTrue(dist >= prevDist - 1e-6f,
          "Results should be sorted by distance ascending for " + distanceType);
      prevDist = dist;
    }
  }

  @Test
  void recallAtK10With1000VectorsDim32() {
    final int dim = 32;
    final int n = 1000;
    final int k = 10;
    final int numQueries = 100;
    final var rng = new Random(999);

    final var store = new InMemoryVectorStore(dim);
    final var params = HnswParams.builder(dim, VectorDistanceType.L2)
        .efConstruction(200)
        .efSearch(100)
        .build();
    final var graph = new HnswGraph(store, params);

    final float[][] vectors = new float[n][dim];
    final long[] nodeKeys = new long[n];

    for (int i = 0; i < n; i++) {
      for (int d = 0; d < dim; d++) {
        vectors[i][d] = rng.nextFloat();
      }
      final int level = graph.generateRandomLevel();
      nodeKeys[i] = store.createNode(i, vectors[i], level);
      graph.insert(nodeKeys[i]);
    }

    final var distFn = VectorDistanceType.L2.getDistanceFunction(dim);
    double totalRecall = 0.0;

    for (int q = 0; q < numQueries; q++) {
      final float[] query = new float[dim];
      for (int d = 0; d < dim; d++) {
        query[d] = rng.nextFloat();
      }

      final long[] groundTruth = bruteForceKnn(vectors, nodeKeys, query, k, distFn);
      final long[] hnswResults = graph.searchKnn(query, k);

      int hits = 0;
      for (final long hr : hnswResults) {
        for (final long gt : groundTruth) {
          if (hr == gt) {
            hits++;
            break;
          }
        }
      }
      totalRecall += (double) hits / k;
    }

    final double avgRecall = totalRecall / numQueries;
    assertTrue(avgRecall >= 0.95,
        "Average recall@" + k + " should be >= 0.95 for 1000 vectors dim=32, but was "
            + String.format("%.4f", avgRecall));
  }

  @Test
  void twoElementGraph() {
    final int dim = 4;
    final var store = new InMemoryVectorStore(dim);
    final var params = HnswParams.defaults(dim, VectorDistanceType.L2);
    final var graph = new HnswGraph(store, params);

    final long k0 = store.createNode(0L, new float[]{0f, 0f, 0f, 0f}, 0);
    final long k1 = store.createNode(1L, new float[]{1f, 1f, 1f, 1f}, 0);
    graph.insert(k0);
    graph.insert(k1);

    final long[] results = graph.searchKnn(new float[]{0.1f, 0.1f, 0.1f, 0.1f}, 1);
    assertEquals(1, results.length);
    assertEquals(k0, results[0]);
  }

  @Test
  void searchWithKGreaterThanGraphSize() {
    final int dim = 8;
    final int n = 5;
    final var rng = new Random(77);

    final var store = new InMemoryVectorStore(dim);
    final var params = HnswParams.defaults(dim, VectorDistanceType.L2);
    final var graph = new HnswGraph(store, params);

    for (int i = 0; i < n; i++) {
      final float[] vec = new float[dim];
      for (int d = 0; d < dim; d++) {
        vec[d] = rng.nextFloat();
      }
      final long key = store.createNode(i, vec, graph.generateRandomLevel());
      graph.insert(key);
    }

    // Ask for more results than exist in graph
    final float[] query = new float[dim];
    for (int d = 0; d < dim; d++) {
      query[d] = rng.nextFloat();
    }
    final long[] results = graph.searchKnn(query, 100);
    assertTrue(results.length <= n, "Cannot return more results than nodes in graph");
    assertTrue(results.length > 0, "Should return some results");
  }

  @Test
  void searchEmptyGraph() {
    final int dim = 4;
    final var store = new InMemoryVectorStore(dim);
    final var params = HnswParams.defaults(dim, VectorDistanceType.L2);
    final var graph = new HnswGraph(store, params);

    final long[] results = graph.searchKnn(new float[]{1f, 2f, 3f, 4f}, 5);
    assertEquals(0, results.length);
  }

  // --- Helpers ---

  private static long[] bruteForceKnn(final float[][] vectors, final long[] nodeKeys,
                                       final float[] query, final int k,
                                       final io.sirix.index.vector.ops.DistanceFunction distFn) {
    final int n = vectors.length;
    final float[] dists = new float[n];
    final long[] indices = new long[n];

    for (int i = 0; i < n; i++) {
      dists[i] = distFn.distance(query, vectors[i]);
      indices[i] = nodeKeys[i];
    }

    // Simple selection of top-k by sorting
    // Build (dist, index) pairs and sort by dist
    final Integer[] order = new Integer[n];
    for (int i = 0; i < n; i++) {
      order[i] = i;
    }
    Arrays.sort(order, (a, b) -> Float.compare(dists[a], dists[b]));

    final int resultSize = Math.min(k, n);
    final long[] result = new long[resultSize];
    for (int i = 0; i < resultSize; i++) {
      result[i] = indices[order[i]];
    }
    return result;
  }

  private static void normalize(final float[] vector) {
    float norm = 0.0f;
    for (final float v : vector) {
      norm += v * v;
    }
    norm = (float) Math.sqrt(norm);
    if (norm > 0.0f) {
      for (int i = 0; i < vector.length; i++) {
        vector[i] /= norm;
      }
    }
  }
}
