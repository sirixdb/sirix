package io.sirix.query.compiler.optimizer.stats;

/**
 * JSON-aware cost model for comparing physical operator alternatives.
 *
 * <p>Cost = I/O cost + CPU cost, following Weiner et al. Section 4.4.
 * I/O cost is dominated by page reads; CPU cost by per-tuple processing.</p>
 *
 * <p>Key insight: sequential scan reads pages linearly (prefetchable, cheap I/O),
 * while index scan reads pages via random access (unpredictable, expensive I/O).
 * The index wins only when it reads significantly fewer pages.</p>
 *
 * <p>Cost units are abstract — only relative ordering matters for plan selection.</p>
 */
public final class JsonCostModel {

  // Approximate entries per KeyValueLeaf page (64KB page, ~100-200 entries typical)
  private static final double ENTRIES_PER_PAGE = 100.0;

  // I/O cost per page for sequential (linear) scan — cheap due to prefetch
  private static final double SEQ_IO_PER_PAGE = 1.0;

  // I/O cost per page for random (index) access — ~4x more expensive than sequential
  private static final double RANDOM_IO_PER_PAGE = 4.0;

  // CPU cost per tuple (same for both operators — processing cost is roughly equal)
  private static final double CPU_PER_TUPLE = 0.01;

  // Fixed overhead for B+-tree root-to-leaf traversal (page reads for internal nodes)
  private static final double INDEX_LOOKUP_OVERHEAD = 10.0;

  // Estimated B+-tree height when unknown
  private static final int DEFAULT_BTREE_HEIGHT = 3;

  /**
   * Estimate the cost of a sequential scan over all nodes.
   *
   * @param totalNodeCount total nodes in the document
   * @return estimated cost
   */
  public double estimateSequentialScanCost(long totalNodeCount) {
    if (totalNodeCount <= 0) {
      return Double.MAX_VALUE;
    }
    final long pages = Math.max(1L, (long) Math.ceil(totalNodeCount / ENTRIES_PER_PAGE));
    final double ioCost = pages * SEQ_IO_PER_PAGE;
    final double cpuCost = totalNodeCount * CPU_PER_TUPLE;
    return ioCost + cpuCost;
  }

  /**
   * Estimate the cost of an index scan for the given path cardinality.
   *
   * @param pathCardinality number of nodes matching the indexed path
   * @return estimated cost
   */
  public double estimateIndexScanCost(long pathCardinality) {
    if (pathCardinality <= 0) {
      return Double.MAX_VALUE;
    }
    final long matchingPages = Math.max(1L, (long) Math.ceil(pathCardinality / ENTRIES_PER_PAGE));
    final double ioCost = (matchingPages + DEFAULT_BTREE_HEIGHT) * RANDOM_IO_PER_PAGE;
    final double cpuCost = pathCardinality * CPU_PER_TUPLE;
    return INDEX_LOOKUP_OVERHEAD + ioCost + cpuCost;
  }

  /**
   * Compare two costs. Returns true if indexScanCost is strictly cheaper.
   */
  public boolean isIndexScanCheaper(double indexScanCost, double seqScanCost) {
    return Double.compare(indexScanCost, seqScanCost) < 0;
  }

  // --- Join cost estimation (Milestone 3) ---

  /** Hash table build/probe overhead multiplier (accounts for hashing + memory access). */
  private static final double HASH_JOIN_FACTOR = 3.0;

  /** Default join selectivity for equi-joins when no statistics are available. */
  public static final double DEFAULT_JOIN_SELECTIVITY = 0.1;

  /**
   * Estimate the cost of a hash join.
   *
   * <p>Model: build hash table on right input, probe with left input.
   * Cost = factor × (buildCard + probeCard) × CPU_PER_TUPLE.
   * The factor accounts for hashing, memory allocation, and cache misses.</p>
   *
   * @param leftCard  cardinality of the probe (outer) side
   * @param rightCard cardinality of the build (inner) side
   * @return estimated hash join cost
   */
  public double estimateHashJoinCost(long leftCard, long rightCard) {
    return HASH_JOIN_FACTOR * (leftCard + rightCard) * CPU_PER_TUPLE;
  }

  /**
   * Estimate the cost of a nested-loop join.
   *
   * @param outerCard cardinality of the outer relation
   * @param innerCard cardinality of the inner relation
   * @return estimated nested-loop join cost
   */
  public double estimateNestedLoopJoinCost(long outerCard, long innerCard) {
    return outerCard * innerCard * CPU_PER_TUPLE;
  }

  /**
   * Estimate the output cardinality of a join.
   *
   * @param leftCard    cardinality of the left input
   * @param rightCard   cardinality of the right input
   * @param selectivity join predicate selectivity
   * @return estimated output cardinality (at least 1)
   */
  public long estimateJoinCardinality(long leftCard, long rightCard, double selectivity) {
    // Cast to double before multiplying to avoid long overflow for large cardinalities
    return Math.max(1L, (long) ((double) leftCard * rightCard * selectivity));
  }

  /**
   * Estimate join cardinality using default selectivity.
   */
  public long estimateJoinCardinality(long leftCard, long rightCard) {
    return estimateJoinCardinality(leftCard, rightCard, DEFAULT_JOIN_SELECTIVITY);
  }
}
