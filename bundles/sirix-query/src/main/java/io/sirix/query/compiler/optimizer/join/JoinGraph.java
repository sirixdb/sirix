package io.sirix.query.compiler.optimizer.join;

/**
 * Dense join graph with bitmask-based neighborhoods.
 *
 * <p>Each relation is identified by an index in [0, relationCount).
 * Adjacency is stored as bitmasks: {@code adjacency[i]} has bit j set
 * iff there is a join edge between relation i and relation j.</p>
 *
 * <p>Edges carry selectivity estimates. Base relations carry cardinality
 * and access cost estimates.</p>
 *
 * <p>Maximum supported relations: 63 (limited by long bitmask).</p>
 */
public final class JoinGraph {

  /** Maximum number of relations supported (long bitmask minus sign bit). */
  public static final int MAX_RELATIONS = 63;

  private final int relationCount;
  private final long[] adjacency;
  private final long[] baseCardinalities;
  private final double[] baseCosts;
  private final JoinEdge[] edges;
  private int edgeCount;

  /**
   * @param relationCount number of base relations (must be in [1, 63])
   */
  public JoinGraph(int relationCount) {
    if (relationCount < 1 || relationCount > MAX_RELATIONS) {
      throw new IllegalArgumentException(
          "Relation count must be in [1, " + MAX_RELATIONS + "]: " + relationCount);
    }
    this.relationCount = relationCount;
    this.adjacency = new long[relationCount];
    this.baseCardinalities = new long[relationCount];
    this.baseCosts = new double[relationCount];
    // Pre-size for worst case: n*(n-1)/2 edges in a complete graph
    this.edges = new JoinEdge[relationCount * (relationCount - 1) / 2];
    this.edgeCount = 0;
  }

  /**
   * Add an undirected join edge between two relations.
   */
  public void addEdge(int rel1, int rel2, double selectivity) {
    checkRelationIndex(rel1);
    checkRelationIndex(rel2);
    // Skip duplicate edges — an edge between rel1 and rel2 already exists
    // if the adjacency bitmask already has the bit set.
    if ((adjacency[rel1] & (1L << rel2)) != 0) {
      return;
    }
    if (edgeCount >= edges.length) {
      throw new IllegalStateException("Edge capacity exceeded: " + edgeCount);
    }
    final var edge = new JoinEdge(rel1, rel2, selectivity);
    edges[edgeCount++] = edge;
    adjacency[rel1] |= 1L << rel2;
    adjacency[rel2] |= 1L << rel1;
  }

  /**
   * Set the cardinality (tuple count) of a base relation.
   */
  public void setBaseCardinality(int relation, long cardinality) {
    checkRelationIndex(relation);
    baseCardinalities[relation] = cardinality;
  }

  /**
   * Set the access cost of scanning a base relation.
   */
  public void setBaseCost(int relation, double cost) {
    checkRelationIndex(relation);
    baseCosts[relation] = cost;
  }

  private void checkRelationIndex(int relation) {
    if (relation < 0 || relation >= relationCount) {
      throw new IllegalArgumentException(
          "Relation index out of bounds: " + relation);
    }
  }

  /**
   * Compute the neighborhood of a set of relations: all relations adjacent
   * to any relation in S but not in S itself.
   *
   * @param S bitmask of relation set
   * @return bitmask of neighboring relations
   */
  public long neighborhood(long S) {
    long result = 0L;
    long remaining = S;
    while (remaining != 0) {
      final int i = Long.numberOfTrailingZeros(remaining);
      result |= adjacency[i];
      remaining &= remaining - 1; // clear lowest bit
    }
    return result & ~S;
  }

  /**
   * Get the combined join selectivity between two disjoint relation sets.
   * Multiplies selectivities of all edges connecting S1 and S2.
   */
  public double joinSelectivity(long S1, long S2) {
    double sel = 1.0;
    boolean found = false;
    for (int i = 0; i < edgeCount; i++) {
      if (edges[i].connects(S1, S2)) {
        sel *= edges[i].selectivity();
        found = true;
      }
    }
    return found ? sel : 1.0;
  }

  /**
   * Check if there is any join edge between the two relation sets.
   */
  public boolean hasEdgeBetween(long S1, long S2) {
    // Quick check via adjacency bitmasks
    long remaining = S1;
    while (remaining != 0) {
      final int i = Long.numberOfTrailingZeros(remaining);
      if ((adjacency[i] & S2) != 0) {
        return true;
      }
      remaining &= remaining - 1;
    }
    return false;
  }

  public int relationCount() {
    return relationCount;
  }

  public long baseCardinality(int relation) {
    return baseCardinalities[relation];
  }

  public double baseCost(int relation) {
    return baseCosts[relation];
  }

  public int edgeCount() {
    return edgeCount;
  }

  public JoinEdge edge(int index) {
    return edges[index];
  }

  /**
   * Returns the bitmask of all relations: (1 << n) - 1.
   */
  public long fullSet() {
    return (1L << relationCount) - 1;
  }
}
