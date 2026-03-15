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
 * <h3>SirixDB storage model</h3>
 * <ul>
 *   <li><b>Document storage</b>: keyed trie — {@code IndirectPage} tree with
 *       fanout 1024, max 8 levels.  Leaf pages are 64 KB {@code KeyValueLeafPage}
 *       with 1024 slots ({@code Constants.NDP_NODE_COUNT}).  Sequential scans
 *       walk leaf pages linearly, benefiting from io_uring prefetch.</li>
 *   <li><b>Secondary indexes</b> (CAS, PATH, NAME): HOT (Height Optimized Trie).
 *       Compound nodes with fanout 2–32 (BiNode/SpanNode/MultiNode).
 *       SIMD-accelerated partial key search via PEXT + AVX2.
 *       Typical height 3–5 for 1M entries (effective fanout ~16).
 *       Leaf pages hold up to 512 entries with prefix compression.</li>
 * </ul>
 *
 * <p>Cost units are abstract — only relative ordering matters for plan selection.</p>
 */
public final class JsonCostModel {

  // --- Keyed trie page model (document storage) ---

  /** Slots per KeyValueLeafPage — 1024 per SirixDB's slotted page layout. */
  private static final double ENTRIES_PER_PAGE = 1024.0;

  /** I/O cost per page for sequential (linear) scan — cheap due to io_uring prefetch. */
  private static final double SEQ_IO_PER_PAGE = 1.0;

  /** I/O cost per page for random (index) access — ~4x more expensive than sequential. */
  private static final double RANDOM_IO_PER_PAGE = 4.0;

  /** CPU cost per tuple (same for both scan and index — processing cost is roughly equal). */
  private static final double CPU_PER_TUPLE = 0.01;

  // --- HOT index model ---

  /**
   * Fixed overhead for opening an index cursor: resolving the index root
   * reference from the RevisionRootPage (PathPage/CASPage/NamePage).
   */
  private static final double INDEX_SETUP_OVERHEAD = 3.0;

  /**
   * Effective HOT fanout for height estimation.
   * HOT nodes have variable fanout: BiNode=2, SpanNode=2–16, MultiNode=17–32.
   * HeightOptimalSplitter maximizes fanout, producing typical average ~16.
   * Height ≈ log₁₆(n) for n index entries.
   */
  private static final double HOT_EFFECTIVE_FANOUT = 16.0;

  /** Log base for HOT height calculation: ln(16). */
  private static final double LOG_FANOUT = Math.log(HOT_EFFECTIVE_FANOUT);

  /**
   * Entries per HOT leaf page — 512 with prefix compression.
   */
  private static final double HOT_LEAF_ENTRIES = 512.0;

  /**
   * Fraction of HOT internal nodes expected to be cache-cold during traversal.
   * Upper trie levels are frequently accessed and typically resident in
   * the page cache.  HOT's SIMD-accelerated traversal (PEXT + AVX2) also
   * means that per-level CPU cost is negligible (~1–3 cycles).
   * With virtual-thread prefetching, only ~20% of levels cause actual I/O stalls.
   */
  private static final double TRIE_COLD_FRACTION = 0.2;

  /**
   * Estimate the cost of a sequential scan over all nodes.
   *
   * <p>Models a linear walk through the keyed trie's leaf pages.
   * Pages are read sequentially, benefiting from io_uring batched I/O
   * and OS readahead prefetch.</p>
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
   * Estimate the cost of a HOT index scan for the given path cardinality.
   *
   * <p>Models SirixDB's HOT (Height Optimized Trie) index: compound nodes
   * with effective fanout ~16, SIMD-accelerated partial key search (PEXT + AVX2),
   * and virtual-thread prefetching of sibling pages.  Tree height is
   * approximately {@code log₁₆(n)}, giving 3–5 levels for typical index sizes.</p>
   *
   * <p>Each trie level costs ~1–3 CPU cycles (SIMD) but may require a page
   * load if the node is cache-cold.  Upper levels are hot (frequently accessed),
   * so only {@link #TRIE_COLD_FRACTION} of levels cause actual I/O stalls.</p>
   *
   * @param pathCardinality number of nodes matching the indexed path
   * @return estimated cost
   */
  public double estimateIndexScanCost(long pathCardinality) {
    if (pathCardinality <= 0) {
      return Double.MAX_VALUE;
    }
    // HOT trie depth: log_16(n) for effective fanout ~16
    final int trieDepth = pathCardinality > 1
        ? Math.max(1, (int) Math.ceil(Math.log(pathCardinality) / LOG_FANOUT))
        : 1;
    // Effective page reads for trie traversal (upper nodes typically cached,
    // virtual-thread prefetch hides latency for remaining levels)
    final double traversalIO = trieDepth * TRIE_COLD_FRACTION * RANDOM_IO_PER_PAGE;
    // Leaf pages for the matching entries (HOT leaves hold up to 512 entries)
    final long matchingPages = Math.max(1L, (long) Math.ceil(pathCardinality / HOT_LEAF_ENTRIES));
    final double dataIO = matchingPages * RANDOM_IO_PER_PAGE;
    final double cpuCost = pathCardinality * CPU_PER_TUPLE;
    return INDEX_SETUP_OVERHEAD + traversalIO + dataIO + cpuCost;
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
