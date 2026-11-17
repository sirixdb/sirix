/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Fixed-seed runs of {@link HOTTrieReaderBoundOracle}: {@code lowerBound}, {@code upperBound} and
 * {@code range} of the trie reader against a sorted reference, over randomized key sets and probes
 * (HOT index specification §4.3.1).
 *
 * <p>
 * Only fixed seeds live here, so the class can serve as a mutation-testing target: PIT needs a
 * suite that is green without mutation. Fresh seeds are explored by
 * {@code io.sirix.property.HotBoundSeekOracleRandomTest}; a seed that fails there belongs in the
 * list below as a permanent regression entry.
 * </p>
 */
final class HOTTrieReaderBoundPropertyTest {

  /** 1427 is the tracking issue; the others were drawn once and kept. */
  private static final long[] BULK_SEEDS = {1427L, 20261115L, 0x5EEDL, 7L, 42L, 0xB0B0L, 31032026L, 9001L};

  private static final long[] WRITER_SEEDS = {1427L, 20261116L};

  @TempDir
  Path temporaryDirectory;

  @Test
  @Timeout(value = 300, unit = TimeUnit.SECONDS)
  void bulkBuiltTriesAgreeWithTheSortedReferenceOnEveryProbe() {
    final HOTTrieReaderBoundOracle.Coverage coverage = new HOTTrieReaderBoundOracle.Coverage();
    for (final long seed : BULK_SEEDS) {
      coverage.add(HOTTrieReaderBoundOracle.assertBulkBuiltTriesMatchSortedReference(seed));
    }
    System.out.println("[bound-oracle] bulk-built: " + coverage);
    assertEveryGeometryWasReached(coverage);
    // The two geometries no bit decides occur only among variable-length keys.
    assertTrue(coverage.belowExtendedByFirstKey > 0, "no probe was extended by the first key of its leaf: " + coverage);
    assertTrue(coverage.aboveZeroContinuation > 0, "no probe was a zero continuation above its leaf: " + coverage);
  }

  @Test
  @Timeout(value = 300, unit = TimeUnit.SECONDS)
  void writerBuiltTriesAgreeWithTheSortedReferenceOnEveryProbe() {
    final HOTTrieReaderBoundOracle.Coverage coverage = new HOTTrieReaderBoundOracle.Coverage();
    for (final long seed : WRITER_SEEDS) {
      coverage.add(HOTTrieReaderBoundOracle.assertWriterBuiltTrieMatchesSortedReference(seed,
          temporaryDirectory.resolve("writer-" + seed)));
    }
    System.out.println("[bound-oracle] writer-built: " + coverage);
    assertEveryGeometryWasReached(coverage);
  }

  /**
   * A run that never routes a probe below or above its leaf would pass without testing the walk, so
   * the geometry counts are part of the property.
   */
  private static void assertEveryGeometryWasReached(final HOTTrieReaderBoundOracle.Coverage coverage) {
    assertTrue(coverage.exact > 0, "no stored key was probed: " + coverage);
    assertTrue(coverage.insideLeaf > 0, "no probe fell strictly inside its leaf: " + coverage);
    assertTrue(coverage.belowAtRoutedLeaf > 0, "no probe had its bound at the first key of its leaf: " + coverage);
    assertTrue(coverage.belowInEarlierLeaf > 0,
        "no probe sorted below a leaf while its bound lay in an earlier one: " + coverage);
    assertTrue(coverage.aboveAtNextLeaf > 0, "no probe had its bound at the next leaf: " + coverage);
    assertTrue(coverage.abovePastNextLeaf > 0, "no probe had its bound past the next leaf: " + coverage);
    assertTrue(coverage.rangeKeysCompared > 0, "no range returned a key: " + coverage);
  }
}
