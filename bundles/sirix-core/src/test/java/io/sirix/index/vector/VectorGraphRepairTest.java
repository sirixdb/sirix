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

import io.sirix.index.vector.hnsw.HnswGraph;
import io.sirix.index.vector.hnsw.HnswParams;
import io.sirix.index.vector.hnsw.InMemoryVectorStore;
import io.sirix.index.vector.ops.VectorDistanceType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for HNSW graph repair on node deletion: neighbor reconnection,
 * entry point reassignment, and reverse lookup via findNodeKeyByDocumentKey.
 */
class VectorGraphRepairTest {

  private static final int DIM = 4;

  @Test
  @DisplayName("Delete middle node: remaining vectors still found by search")
  void testDeleteMiddleNode() {
    final var store = new InMemoryVectorStore(DIM);
    final var params = HnswParams.defaults(DIM, VectorDistanceType.L2);
    final var graph = new HnswGraph(store, params);

    // Insert 10 vectors along the first axis at increasing distances.
    final long[] keys = new long[10];
    for (int i = 0; i < 10; i++) {
      keys[i] = store.createNode(100L + i, new float[]{(float) i, 0f, 0f, 0f}, 0);
      graph.insert(keys[i]);
    }

    // Delete node in the middle (key[5] = vector at [5, 0, 0, 0]).
    graph.delete(keys[5]);
    assertTrue(store.isDeleted(keys[5]));

    // Search for every remaining vector — each should find itself.
    for (int i = 0; i < 10; i++) {
      if (i == 5) {
        continue; // Skip deleted node.
      }
      final float[] query = {(float) i, 0f, 0f, 0f};
      final long[] results = graph.searchKnn(query, 1);
      assertTrue(results.length >= 1, "Should find at least 1 result for vector " + i);
      assertEquals(keys[i], results[0],
          "Exact match should be found for vector " + i + " after middle deletion");
    }

    // Verify deleted node never appears in results.
    final long[] allResults = graph.searchKnn(new float[]{5f, 0f, 0f, 0f}, 9);
    for (final long r : allResults) {
      assertFalse(r == keys[5], "Deleted node must not appear in search results");
    }
  }

  @Test
  @DisplayName("Delete entry point: search still works with new entry point")
  void testDeleteEntryPoint() {
    final var store = new InMemoryVectorStore(DIM);
    final var params = HnswParams.defaults(DIM, VectorDistanceType.L2);
    final var graph = new HnswGraph(store, params);

    // Insert 10 vectors.
    final long[] keys = new long[10];
    for (int i = 0; i < 10; i++) {
      keys[i] = store.createNode(200L + i, new float[]{(float) i, (float) i, 0f, 0f}, 0);
      graph.insert(keys[i]);
    }

    // The entry point is the first inserted node (keys[0]).
    final long originalEntryPoint = store.getEntryPointKey();
    assertEquals(keys[0], originalEntryPoint);

    // Delete the entry point.
    graph.delete(keys[0]);
    assertTrue(store.isDeleted(keys[0]));

    // Entry point should have changed.
    final long newEntryPoint = store.getEntryPointKey();
    assertTrue(newEntryPoint != keys[0],
        "Entry point should change after deleting the old entry point");
    assertFalse(store.isDeleted(newEntryPoint),
        "New entry point must not be a deleted node");

    // Search should still work for all remaining vectors.
    for (int i = 1; i < 10; i++) {
      final float[] query = {(float) i, (float) i, 0f, 0f};
      final long[] results = graph.searchKnn(query, 1);
      assertTrue(results.length >= 1,
          "Should find results for vector " + i + " after entry point deletion");
      assertEquals(keys[i], results[0],
          "Exact match should be found for vector " + i);
    }
  }

  @Test
  @DisplayName("Delete all nodes: graph returns empty results")
  void testDeleteAllNodes() {
    final var store = new InMemoryVectorStore(DIM);
    final var params = HnswParams.defaults(DIM, VectorDistanceType.L2);
    final var graph = new HnswGraph(store, params);

    final long k0 = store.createNode(0L, new float[]{1f, 0f, 0f, 0f}, 0);
    final long k1 = store.createNode(1L, new float[]{0f, 1f, 0f, 0f}, 0);
    final long k2 = store.createNode(2L, new float[]{0f, 0f, 1f, 0f}, 0);
    graph.insert(k0);
    graph.insert(k1);
    graph.insert(k2);

    graph.delete(k1);
    graph.delete(k2);
    graph.delete(k0);

    final long[] results = graph.searchKnn(new float[]{0.5f, 0.5f, 0f, 0f}, 5);
    assertEquals(0, results.length, "All nodes deleted should yield empty results");
    assertEquals(-1L, store.getEntryPointKey(), "Entry point should be -1 after all deleted");
  }

  @Test
  @DisplayName("removeFromNeighborList compacts correctly")
  void testRemoveFromNeighborList() {
    final var store = new InMemoryVectorStore(DIM);
    final var params = HnswParams.defaults(DIM, VectorDistanceType.L2);
    final var graph = new HnswGraph(store, params);

    // Create node and manually set its neighbors.
    final long k0 = store.createNode(0L, new float[]{0f, 0f, 0f, 0f}, 0);
    store.setNeighbors(k0, 0, new long[]{10L, 20L, 30L, 40L}, 4);

    // Remove 20L from neighbor list.
    graph.removeFromNeighborList(k0, 20L, 0);

    final int count = store.getNeighborCount(k0, 0);
    assertEquals(3, count);
    final long[] neighbors = store.getNeighbors(k0, 0);
    assertEquals(10L, neighbors[0]);
    assertEquals(30L, neighbors[1]);
    assertEquals(40L, neighbors[2]);
  }

  @Test
  @DisplayName("appendNeighbor respects capacity and avoids duplicates")
  void testAppendNeighbor() {
    final var store = new InMemoryVectorStore(DIM);
    final var params = HnswParams.defaults(DIM, VectorDistanceType.L2);
    final var graph = new HnswGraph(store, params);

    final long k0 = store.createNode(0L, new float[]{0f, 0f, 0f, 0f}, 0);
    store.setNeighbors(k0, 0, new long[]{10L}, 1);

    // Append a new neighbor.
    graph.appendNeighbor(k0, 20L, 0, 4);
    assertEquals(2, store.getNeighborCount(k0, 0));

    // Append duplicate — should be ignored.
    graph.appendNeighbor(k0, 20L, 0, 4);
    assertEquals(2, store.getNeighborCount(k0, 0));

    // Fill to capacity.
    graph.appendNeighbor(k0, 30L, 0, 4);
    graph.appendNeighbor(k0, 40L, 0, 4);
    assertEquals(4, store.getNeighborCount(k0, 0));

    // At capacity — should be ignored.
    graph.appendNeighbor(k0, 50L, 0, 4);
    assertEquals(4, store.getNeighborCount(k0, 0));
  }

  @Test
  @DisplayName("findNodeKeyByDocumentKey works on InMemoryVectorStore")
  void testFindNodeKeyByDocumentKey() {
    final var store = new InMemoryVectorStore(DIM);

    final long k0 = store.createNode(100L, new float[]{1f, 0f, 0f, 0f}, 0);
    final long k1 = store.createNode(200L, new float[]{0f, 1f, 0f, 0f}, 0);
    final long k2 = store.createNode(300L, new float[]{0f, 0f, 1f, 0f}, 0);

    assertEquals(k0, store.findNodeKeyByDocumentKey(100L));
    assertEquals(k1, store.findNodeKeyByDocumentKey(200L));
    assertEquals(k2, store.findNodeKeyByDocumentKey(300L));
    assertEquals(-1L, store.findNodeKeyByDocumentKey(999L));
  }

  @Test
  @DisplayName("Recall preserved after deleting 30% of vectors")
  void testRecallAfterDeletion() {
    final int dim = 16;
    final int n = 100;
    final var rng = new java.util.Random(42);

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

    // Delete 30% of nodes via graph.delete (with repair).
    final boolean[] deleted = new boolean[n];
    for (int i = 0; i < n; i++) {
      if (rng.nextDouble() < 0.3) {
        graph.delete(nodeKeys[i]);
        deleted[i] = true;
      }
    }

    // Verify each surviving node can be found by search.
    int found = 0;
    int surviving = 0;
    for (int i = 0; i < n; i++) {
      if (deleted[i]) {
        continue;
      }
      surviving++;
      final long[] results = graph.searchKnn(vectors[i], 5);
      for (final long r : results) {
        if (r == nodeKeys[i]) {
          found++;
          break;
        }
      }
    }

    final double selfRecall = surviving > 0 ? (double) found / surviving : 1.0;
    assertTrue(selfRecall >= 0.85,
        "Self-recall after 30% deletion should be >= 0.85, was " + String.format("%.4f", selfRecall));
  }
}
