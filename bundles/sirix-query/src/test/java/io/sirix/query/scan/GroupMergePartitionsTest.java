package io.sirix.query.scan;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The merge partition count of the grouped scans: the unit of post-scan parallelism AND the unit a
 * hash-range pass is cut from, so with P passes only {@code partitions / P} of them are merged by
 * the pass that owns them.
 */
final class GroupMergePartitionsTest {

  @Test
  @DisplayName("the default splits far past the worker count, so a multi-pass scan still merges in parallel")
  void theDefaultIsWiderThanAnyWorkerPool() {
    final String previous = System.clearProperty(SirixVectorizedExecutor.GROUP_MERGE_PARTITIONS_PROPERTY);
    try {
      final int partitions = SirixVectorizedExecutor.groupMergePartitions(10L);
      assertEquals(1024, partitions, "the default merge split");
      assertEquals(Integer.highestOneBit(partitions), partitions, "a power of two: the shift indexes it");
      // The property exists to bound a pass: eight passes over this split still own 128 partitions,
      // more than any worker pool this runs on.
      assertTrue(partitions / 8 > Runtime.getRuntime().availableProcessors(),
          "even at eight passes a pass must own more partitions than there are workers");
    } finally {
      if (previous != null) {
        System.setProperty(SirixVectorizedExecutor.GROUP_MERGE_PARTITIONS_PROPERTY, previous);
      }
    }
  }

  @Test
  @DisplayName("the property drives the split, bounded by the winner-merge budget")
  void thePropertyDrivesTheSplit() {
    final String previous = System.getProperty(SirixVectorizedExecutor.GROUP_MERGE_PARTITIONS_PROPERTY);
    try {
      System.setProperty(SirixVectorizedExecutor.GROUP_MERGE_PARTITIONS_PROPERTY, "4096");
      assertEquals(4096, SirixVectorizedExecutor.groupMergePartitions(8L), "the property drives it");
      assertEquals(32, SirixVectorizedExecutor.groupMergePartitions(2_000L), "the limit still bounds it");
    } finally {
      if (previous == null) {
        System.clearProperty(SirixVectorizedExecutor.GROUP_MERGE_PARTITIONS_PROPERTY);
      } else {
        System.setProperty(SirixVectorizedExecutor.GROUP_MERGE_PARTITIONS_PROPERTY, previous);
      }
    }
  }

  @Test
  @DisplayName("a selection limit the per-partition top-k cannot prune under narrows the split")
  void aLargeSelectionLimitNarrowsTheSplit() {
    final String previous = System.clearProperty(SirixVectorizedExecutor.GROUP_MERGE_PARTITIONS_PROPERTY);
    try {
      assertEquals(1024, SirixVectorizedExecutor.groupMergePartitions(10L), "LIMIT 10 keeps the whole split");
      assertEquals(32, SirixVectorizedExecutor.groupMergePartitions(1_010L),
          "OFFSET 1000: 1024 partitions would hand the final selection 1024 x 1010 rows");
      assertEquals(32, SirixVectorizedExecutor.groupMergePartitions(Long.MAX_VALUE),
          "an unbounded selection emits every group: the split cannot help it");
      assertEquals(1024, SirixVectorizedExecutor.groupMergePartitions(0L),
          "no limit spec at all leaves the split to the default");
      assertTrue(SirixVectorizedExecutor.groupMergePartitions(64L) >= 512,
          "a limit the partitions still prune under keeps a wide split");
    } finally {
      if (previous != null) {
        System.setProperty(SirixVectorizedExecutor.GROUP_MERGE_PARTITIONS_PROPERTY, previous);
      }
    }
  }

  @Test
  @DisplayName("a configured count is rounded up to a power of two and bounded at both ends")
  void aConfiguredCountIsRoundedAndBounded() {
    final String previous = System.getProperty(SirixVectorizedExecutor.GROUP_MERGE_PARTITIONS_PROPERTY);
    try {
      System.setProperty(SirixVectorizedExecutor.GROUP_MERGE_PARTITIONS_PROPERTY, "100");
      assertEquals(128, SirixVectorizedExecutor.groupMergePartitions(10L), "rounded up to a power of two");
      System.setProperty(SirixVectorizedExecutor.GROUP_MERGE_PARTITIONS_PROPERTY, "2");
      assertEquals(32, SirixVectorizedExecutor.groupMergePartitions(10L),
          "floored: a hash-range pass is cut from these, so too few caps the pass count");
      System.setProperty(SirixVectorizedExecutor.GROUP_MERGE_PARTITIONS_PROPERTY, "999999999");
      assertEquals(1 << 16, SirixVectorizedExecutor.groupMergePartitions(0L), "capped");
      assertEquals(4096, SirixVectorizedExecutor.groupMergePartitions(10L),
          "and a LIMIT 10 caps it lower still: the winner-merge budget binds before the ceiling");
    } finally {
      if (previous == null) {
        System.clearProperty(SirixVectorizedExecutor.GROUP_MERGE_PARTITIONS_PROPERTY);
      } else {
        System.setProperty(SirixVectorizedExecutor.GROUP_MERGE_PARTITIONS_PROPERTY, previous);
      }
    }
  }
}
