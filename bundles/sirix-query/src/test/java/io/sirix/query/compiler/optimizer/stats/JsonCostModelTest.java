package io.sirix.query.compiler.optimizer.stats;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link JsonCostModel} cost comparisons.
 *
 * <p>Verifies that the cost model correctly reflects SirixDB's storage:
 * keyed trie with 1024-slot KeyValueLeafPages for document scans,
 * HOT (Height Optimized Trie) with fanout ~16 for index scans.</p>
 */
final class JsonCostModelTest {

  private final JsonCostModel costModel = new JsonCostModel();

  @Test
  void indexScanCheaperForHighSelectivity() {
    // Small path cardinality (100) in a large document (1M nodes)
    // — HOT index scan should be much cheaper than sequential scan.
    final double seqCost = costModel.estimateSequentialScanCost(1_000_000L);
    final double idxCost = costModel.estimateIndexScanCost(100L);

    assertTrue(costModel.isIndexScanCheaper(idxCost, seqCost),
        "Index scan should be cheaper for high selectivity: idxCost=" + idxCost + ", seqCost=" + seqCost);
  }

  @Test
  void sequentialScanCheaperForLowSelectivity() {
    // Path covers nearly all nodes (990K out of 1M)
    // — sequential scan avoids the index overhead and reads pages linearly.
    final double seqCost = costModel.estimateSequentialScanCost(1_000_000L);
    final double idxCost = costModel.estimateIndexScanCost(990_000L);

    assertFalse(costModel.isIndexScanCheaper(idxCost, seqCost),
        "Sequential scan should be cheaper for low selectivity: idxCost=" + idxCost + ", seqCost=" + seqCost);
  }

  @Test
  void indexScanCheaperForModerateSelectivity() {
    // Path covers 10% of nodes (10K out of 100K)
    // — HOT index should win due to fewer pages read.
    final double seqCost = costModel.estimateSequentialScanCost(100_000L);
    final double idxCost = costModel.estimateIndexScanCost(10_000L);

    assertTrue(costModel.isIndexScanCheaper(idxCost, seqCost),
        "Index scan should be cheaper for 10% selectivity: idxCost=" + idxCost + ", seqCost=" + seqCost);
  }

  @Test
  void indexNotWorthItForTinyDocument() {
    // Very small document (50 nodes) fits in a single page (1024 slots).
    // Index setup overhead + HOT traversal + random I/O is more
    // expensive than scanning a single page sequentially.
    final double seqCost = costModel.estimateSequentialScanCost(50L);
    final double idxCost = costModel.estimateIndexScanCost(10L);

    assertTrue(seqCost > 0, "Sequential scan cost should be positive");
    assertTrue(idxCost > 0, "Index scan cost should be positive");
    assertFalse(costModel.isIndexScanCheaper(idxCost, seqCost),
        "For tiny documents (single page), seq scan should be cheaper: "
            + "seqCost=" + seqCost + ", idxCost=" + idxCost);
  }

  @Test
  void unknownCardinalityReturnsFiniteSentinel() {
    final double seqCost = costModel.estimateSequentialScanCost(-1L);
    final double idxCost = costModel.estimateIndexScanCost(-1L);

    assertEquals(JsonCostModel.UNKNOWN_COST, seqCost,
        "Unknown total count should return UNKNOWN_COST sentinel");
    assertEquals(JsonCostModel.UNKNOWN_COST, idxCost,
        "Unknown path cardinality should return UNKNOWN_COST sentinel");
    // Verify the sentinel is finite (not Double.MAX_VALUE) so costs remain summable
    assertTrue(Double.isFinite(seqCost), "UNKNOWN_COST should be finite");
    assertTrue(seqCost + 1.0 > seqCost, "UNKNOWN_COST should be summable without overflow");
  }

  @Test
  void sequentialScanPagesReflect1024SlotPages() {
    // 1024 nodes should fit in exactly 1 page
    final double cost1Page = costModel.estimateSequentialScanCost(1024L);
    // 1025 nodes should need 2 pages
    final double cost2Pages = costModel.estimateSequentialScanCost(1025L);

    assertTrue(cost2Pages > cost1Page,
        "1025 nodes (2 pages) should cost more than 1024 nodes (1 page)");
  }

  @Test
  void hotTrieDepthScalesLogarithmically() {
    // HOT fanout ~16: log_16(100) ≈ 1.7, log_16(100K) ≈ 4.1
    // The cost should scale sublinearly due to logarithmic depth.
    final double smallIdx = costModel.estimateIndexScanCost(100L);
    final double largeIdx = costModel.estimateIndexScanCost(100_000L);

    // 1000x more entries, but cost ratio well under 1000x
    final double ratio = largeIdx / smallIdx;
    assertTrue(ratio < 200,
        "1000x more entries should NOT produce 1000x cost due to log_16 depth: ratio=" + ratio);
    assertTrue(ratio > 5,
        "1000x more entries should produce meaningfully higher cost: ratio=" + ratio);
  }

  @Test
  void hotShallowerThanBinaryTree() {
    // HOT with fanout ~16 should produce lower index costs than a binary tree
    // would (fanout 2), because fewer levels means fewer page reads.
    // With 1M entries: log_16(1M) ≈ 5 vs log_2(1M) ≈ 20
    // We verify the cost is reasonable for large indexes.
    final double idxCost1M = costModel.estimateIndexScanCost(1_000_000L);
    final double seqCost1M = costModel.estimateSequentialScanCost(1_000_000L);

    // Index scanning ALL 1M entries should still be more expensive than seq scan
    // (random I/O per leaf page is 4x sequential)
    assertFalse(costModel.isIndexScanCheaper(idxCost1M, seqCost1M),
        "Scanning ALL 1M entries via index should be more expensive than seq scan");
  }

  @Test
  void crossoverPointIsReasonable() {
    // Find approximate crossover: for a 100K node document, at what path cardinality
    // does index scan become more expensive than seq scan?
    final long totalNodes = 100_000L;
    final double seqCost = costModel.estimateSequentialScanCost(totalNodes);

    long crossover = -1;
    for (long pathCard = totalNodes; pathCard >= 1; pathCard = pathCard * 3 / 4) {
      if (costModel.isIndexScanCheaper(costModel.estimateIndexScanCost(pathCard), seqCost)) {
        crossover = pathCard;
        break;
      }
    }

    assertTrue(crossover > 0,
        "There should be a crossover point for 100K nodes");
    // HOT's efficient traversal means index wins for a large fraction of the data
    assertTrue(crossover > totalNodes / 10,
        "Crossover should be above 10% selectivity: " + crossover + " of " + totalNodes);
    assertTrue(crossover < totalNodes,
        "Crossover should be below 100%: " + crossover + " of " + totalNodes);
  }
}
