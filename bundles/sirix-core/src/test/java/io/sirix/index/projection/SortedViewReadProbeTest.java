/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.budget.WorkCapture;
import io.sirix.budget.WorkReport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The sorted-view probe shares two static observer fields with every other test of the package, so
 * what it must get right is ownership: count while open, keep a displaced observer working, and
 * hand it back untouched.
 */
@Isolated
final class SortedViewReadProbeTest {

  @AfterEach
  void clearObservers() {
    ProjectionSortedGroupSummary.setReadObserverForTesting(null);
    ProjectionSortedLeafStore.setQueryLeafReadObserverForTesting(null);
  }

  @Test
  void itCountsEachWalkSeparatelyAndStartsEveryCaptureFromZero() throws Exception {
    final SortedViewReadProbe probe = new SortedViewReadProbe();
    final WorkCapture capture = WorkCapture.of().with(probe);

    final WorkReport first = capture.run(() -> {
      fireSummaryRead(1);
      fireSummaryRead(2);
      fireDataLeafRead(9);
    });
    final WorkReport second = capture.run(() -> fireSummaryRead(3));

    assertEquals(2, first.of(probe.summaryReads()));
    assertEquals(1, first.of(probe.dataLeafReads()));
    assertEquals(1, second.of(probe.summaryReads()), "a reused probe must not carry the previous capture's reads");
    assertEquals(0, second.of(probe.dataLeafReads()));
  }

  @Test
  void aDisplacedObserverKeepsFiringAndIsHandedBack() {
    final List<Integer> seenByOwner = new ArrayList<>();
    final IntConsumer owner = seenByOwner::add;
    ProjectionSortedGroupSummary.setReadObserverForTesting(owner);

    final SortedViewReadProbe probe = new SortedViewReadProbe();
    probe.open();
    fireSummaryRead(42);
    probe.close();

    assertEquals(List.of(42), seenByOwner, "an observer the probe displaced must still see the reads");
    assertSame(owner, ProjectionSortedGroupSummary.setReadObserverForTesting(null),
        "closing must restore the displaced observer, not clear the seam");
    assertNull(ProjectionSortedLeafStore.setQueryLeafReadObserverForTesting(null),
        "a seam that was empty must be empty again");
  }

  @Test
  void itCannotBeOpenedTwiceAndClosesIdempotently() {
    final SortedViewReadProbe probe = new SortedViewReadProbe();
    probe.close();
    probe.open();
    assertThrows(IllegalStateException.class, probe::open);
    probe.close();
    probe.close();

    assertNull(ProjectionSortedGroupSummary.setReadObserverForTesting(null));
    assertNull(ProjectionSortedLeafStore.setQueryLeafReadObserverForTesting(null));
  }

  /** Fires the installed summary observer the way {@code ProjectionSortedGroupSummary.read} does. */
  private static void fireSummaryRead(final int leafId) {
    final IntConsumer installed = ProjectionSortedGroupSummary.setReadObserverForTesting(null);
    assertNotNull(installed, "no summary observer is installed");
    ProjectionSortedGroupSummary.setReadObserverForTesting(installed);
    installed.accept(leafId);
  }

  /** Fires the installed data-leaf observer the way {@code ProjectionSortedLeafStore.read} does. */
  private static void fireDataLeafRead(final int leafId) {
    final IntConsumer installed = ProjectionSortedLeafStore.setQueryLeafReadObserverForTesting(null);
    assertNotNull(installed, "no data-leaf observer is installed");
    ProjectionSortedLeafStore.setQueryLeafReadObserverForTesting(installed);
    installed.accept(leafId);
  }
}
