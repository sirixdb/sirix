/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.property;

import io.sirix.index.hot.HOTTrieReaderBoundOracle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

/**
 * Randomly-seeded exploration of the HOT lower- and upper-bound oracle (see
 * {@link HOTTrieReaderBoundOracle}). Every run draws fresh key sets, leaf geometries and probes; a
 * failure names its seed, which then belongs in {@code HOTTrieReaderBoundPropertyTest}'s fixed-seed
 * list as a permanent regression entry.
 *
 * <p>
 * Kept separate from the fixed-seed class on purpose: this class must not be a mutation-testing
 * target, because PIT needs a suite that is green without mutation and a randomly-seeded run can
 * legitimately fail by finding a new defect.
 * </p>
 *
 * <p>
 * The seed counts are small so the ordinary lanes stay short. A soak raises them, for example
 * {@code -Dsirix.hot.boundOracle.bulkSeeds=300 -Dsirix.hot.boundOracle.writerSeeds=30}; the first
 * seed of each run is printed, and the following ones count up from it.
 * </p>
 */
final class HotBoundSeekOracleRandomTest {

  private static final int BULK_SEEDS = Integer.getInteger("sirix.hot.boundOracle.bulkSeeds", 4);
  private static final int WRITER_SEEDS = Integer.getInteger("sirix.hot.boundOracle.writerSeeds", 1);

  @TempDir
  Path temporaryDirectory;

  @Test
  void randomBulkBuiltTriesAgreeWithTheSortedReference() {
    final long firstSeed = System.nanoTime();
    System.out.println("[bound-oracle] random bulk-built seeds: " + BULK_SEEDS + " from " + firstSeed);
    for (int i = 0; i < BULK_SEEDS; i++) {
      HOTTrieReaderBoundOracle.assertBulkBuiltTriesMatchSortedReference(firstSeed + i);
    }
  }

  @Test
  void randomWriterBuiltTriesAgreeWithTheSortedReference() {
    final long firstSeed = System.nanoTime();
    System.out.println("[bound-oracle] random writer-built seeds: " + WRITER_SEEDS + " from " + firstSeed);
    for (int i = 0; i < WRITER_SEEDS; i++) {
      final long seed = firstSeed + i;
      HOTTrieReaderBoundOracle.assertWriterBuiltTrieMatchesSortedReference(seed,
          temporaryDirectory.resolve("writer-" + seed));
    }
  }
}
