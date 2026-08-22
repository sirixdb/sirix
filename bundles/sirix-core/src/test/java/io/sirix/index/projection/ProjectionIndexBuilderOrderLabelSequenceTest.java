/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.node.SirixDeweyID;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The build's own order-label sequence — the one a builder uses when no resolver supplies labels —
 * must stay strictly increasing for as many records as the index can hold.
 *
 * <p>
 * {@link SirixDeweyID#newBetween} advances an append by an unchecked {@code int} addition on the
 * final division, so the sequence silently wraps negative once that division approaches
 * {@link Integer#MAX_VALUE}. From the sequence's starting division of 17 and the default sibling
 * distance of 16 that lands at roughly 134 million consecutive appends — inside the range of the
 * corpora this index is built for — and the wrap is not silent for long: the builder's own
 * monotonicity check turns it into "record order labels are not strictly increasing" and aborts an
 * otherwise valid build.
 *
 * <p>
 * Seeding the sequence near the wrap reproduces in a few hundred iterations what would otherwise
 * take a hundred million records, and asserts the two properties the persisted lane depends on:
 * every label is strictly greater than its predecessor as a {@link SirixDeweyID}, and stays strictly
 * greater under {@link ProjectionIndexRowGroupPage#compareOrderLabels}, which is how the lane
 * actually orders them.
 */
final class ProjectionIndexBuilderOrderLabelSequenceTest {

  private static final int APPENDS = 400;

  @Test
  void theBuildSequenceStaysStrictlyIncreasingAcrossTheIntegerDivisionWrap() {
    // One step short of the wrap: the next few appends must carry rather than go negative.
    SirixDeweyID previous = new SirixDeweyID(new int[] { 1, Integer.MAX_VALUE - 8 });
    byte[] previousBytes = previous.toBytes();

    for (int append = 0; append < APPENDS; append++) {
      final SirixDeweyID next = ProjectionIndexBuilder.nextSequentialOrderLabel(previous);
      assertNotNull(next, "the build sequence must always yield a label");
      assertTrue(previous.compareTo(next) < 0,
          "append " + append + " did not advance the build's order-label sequence: " + previous + " -> " + next);
      for (final int division : next.getDivisionValues()) {
        assertTrue(division > 0, "append " + append + " produced a non-positive division in " + next);
      }

      final byte[] nextBytes = next.toBytes();
      assertTrue(ProjectionIndexRowGroupPage.compareOrderLabels(previousBytes, 0, previousBytes.length,
          nextBytes, 0, nextBytes.length) < 0,
          "append " + append + " did not advance the PERSISTED order label encoding");

      // The directory persists these labels through putLocalLabel, which rejects anything whose
      // level is not 1, and re-reads them through a byte round-trip that RECOMPUTES that level from
      // the odd-division count. A carry that added an odd division would satisfy the first check and
      // fail the second, so assert both — this is the shape constraint the shared carry must meet.
      assertEquals(1, next.getLevel(), "append " + append + " left the local-label level behind: " + next);
      assertEquals(1, new SirixDeweyID(nextBytes).getLevel(),
          "append " + append + " did not survive a byte round-trip at level 1: " + next);

      previous = next;
      previousBytes = nextBytes;
    }
  }

  @Test
  void theBuildSequenceStartsAndAdvancesFromEmpty() {
    final SirixDeweyID first = ProjectionIndexBuilder.nextSequentialOrderLabel(null);
    final SirixDeweyID second = ProjectionIndexBuilder.nextSequentialOrderLabel(first);
    assertTrue(first.compareTo(second) < 0, "the build sequence must advance from its first label");
    final byte[] firstBytes = first.toBytes();
    final byte[] secondBytes = second.toBytes();
    assertTrue(ProjectionIndexRowGroupPage.compareOrderLabels(firstBytes, 0, firstBytes.length,
        secondBytes, 0, secondBytes.length) < 0);
  }
}
