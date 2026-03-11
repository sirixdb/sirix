package io.sirix.query.compiler.optimizer.join;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link JoinGraph} — bitmask adjacency, neighborhood computation,
 * and join selectivity lookups.
 */
final class JoinGraphTest {

  @Test
  void singleEdgeSetsAdjacency() {
    final var graph = new JoinGraph(3);
    graph.addEdge(0, 1, 0.1);

    // R0 and R1 are neighbors
    assertEquals(0b010, graph.neighborhood(0b001)); // neighbors of {R0} = {R1}
    assertEquals(0b001, graph.neighborhood(0b010)); // neighbors of {R1} = {R0}
    assertEquals(0b000, graph.neighborhood(0b100)); // neighbors of {R2} = {}
  }

  @Test
  void chainGraphNeighborhoods() {
    // Chain: R0 — R1 — R2
    final var graph = new JoinGraph(3);
    graph.addEdge(0, 1, 0.1);
    graph.addEdge(1, 2, 0.1);

    // Neighborhood of {R0} = {R1}
    assertEquals(0b010, graph.neighborhood(0b001));
    // Neighborhood of {R1} = {R0, R2}
    assertEquals(0b101, graph.neighborhood(0b010));
    // Neighborhood of {R0, R1} = {R2}
    assertEquals(0b100, graph.neighborhood(0b011));
    // Neighborhood of all = {}
    assertEquals(0b000, graph.neighborhood(0b111));
  }

  @Test
  void starGraphNeighborhoods() {
    // Star: R1 at center, connected to R0, R2, R3
    final var graph = new JoinGraph(4);
    graph.addEdge(1, 0, 0.1);
    graph.addEdge(1, 2, 0.1);
    graph.addEdge(1, 3, 0.1);

    // Neighborhood of center {R1} = {R0, R2, R3}
    assertEquals(0b1101, graph.neighborhood(0b0010));
    // Neighborhood of leaf {R0} = {R1}
    assertEquals(0b0010, graph.neighborhood(0b0001));
    // Neighborhood of {R0, R1} = {R2, R3} (center connects to leaves)
    assertEquals(0b1100, graph.neighborhood(0b0011));
  }

  @Test
  void cliqueGraphNeighborhoods() {
    // Clique: R0 — R1, R0 — R2, R1 — R2
    final var graph = new JoinGraph(3);
    graph.addEdge(0, 1, 0.1);
    graph.addEdge(0, 2, 0.1);
    graph.addEdge(1, 2, 0.1);

    // Neighborhood of any single node = the other two
    assertEquals(0b110, graph.neighborhood(0b001)); // {R1, R2}
    assertEquals(0b101, graph.neighborhood(0b010)); // {R0, R2}
    assertEquals(0b011, graph.neighborhood(0b100)); // {R0, R1}
  }

  @Test
  void joinSelectivitySingleEdge() {
    final var graph = new JoinGraph(2);
    graph.addEdge(0, 1, 0.05);

    assertEquals(0.05, graph.joinSelectivity(0b01, 0b10), 1e-9);
  }

  @Test
  void joinSelectivityMultipleEdges() {
    // Two edges between groups: selectivities multiply
    final var graph = new JoinGraph(4);
    graph.addEdge(0, 2, 0.1);
    graph.addEdge(1, 3, 0.2);

    // {R0, R1} ⋈ {R2, R3}: two edges connect them
    final double sel = graph.joinSelectivity(0b0011, 0b1100);
    assertEquals(0.1 * 0.2, sel, 1e-9);
  }

  @Test
  void hasEdgeBetween() {
    final var graph = new JoinGraph(3);
    graph.addEdge(0, 2, 0.1);

    assertTrue(graph.hasEdgeBetween(0b001, 0b100));  // R0 — R2
    assertTrue(graph.hasEdgeBetween(0b100, 0b001));  // symmetric
    assertFalse(graph.hasEdgeBetween(0b001, 0b010)); // R0 — R1: no edge
  }

  @Test
  void baseCardinalitiesAndCosts() {
    final var graph = new JoinGraph(2);
    graph.setBaseCardinality(0, 1000);
    graph.setBaseCardinality(1, 500);
    graph.setBaseCost(0, 10.0);
    graph.setBaseCost(1, 5.0);

    assertEquals(1000, graph.baseCardinality(0));
    assertEquals(500, graph.baseCardinality(1));
    assertEquals(10.0, graph.baseCost(0));
    assertEquals(5.0, graph.baseCost(1));
  }

  @Test
  void fullSetBitmask() {
    assertEquals(0b111, new JoinGraph(3).fullSet());
    assertEquals(0b1111, new JoinGraph(4).fullSet());
    assertEquals(0b11111, new JoinGraph(5).fullSet());
  }

  @Test
  void rejectsInvalidRelationCount() {
    assertThrows(IllegalArgumentException.class, () -> new JoinGraph(0));
    assertThrows(IllegalArgumentException.class, () -> new JoinGraph(64));
  }
}
