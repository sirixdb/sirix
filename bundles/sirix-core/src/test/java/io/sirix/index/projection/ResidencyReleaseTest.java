/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSegmentFetcher;
import io.sirix.index.projection.ProjectionColumnStore.FillBudgetExceededException;
import io.sirix.index.projection.ProjectionColumnStore.LeafColumnAccess;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * R1 residency (docs/STORAGE_AND_SPEED_PLAN.md §3): a published column fill is retained only while
 * the store's retained total stays within the shared heap-headroom share, and a query scope's exit
 * releases what no open query pins any more.
 *
 * <p>
 * Every assertion here is about an OPTIMISATION, never about an answer: the windowed lanes serve any
 * column without retention, so a refused fill and a released fill must both leave the store able to
 * answer. What the tests pin down is the accounting and the lifetime — bytes charged once, released
 * exactly once, and never released out from under a query that still holds them.
 * </p>
 */
final class ResidencyReleaseTest {

  /** Two long columns: column 0 carries wide random values, column 1 a constant (so 0 is bigger). */
  private static final byte[] KINDS =
      {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG};

  private record Fixture(ProjectionColumnStore store, ColumnSegmentFetcher fetcher) {
  }

  private long previousHeadroom;
  private long previousBudget;
  private boolean previousResidency;
  private boolean previousEvict;

  @BeforeEach
  void setUp() {
    previousHeadroom = HeapHeadroom.setHeadroomForTesting(-1L);
    previousBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(Long.MAX_VALUE >> 1);
    previousResidency = ProjectionColumnStore.setResidencyHeadroomForTesting(true);
    previousEvict = ProjectionColumnStore.setResidencyEvictForTesting(true);
  }

  @AfterEach
  void tearDown() {
    // The share is a process-wide sample: leaving a starved one behind would send every later test in
    // this JVM down the windowed route.
    ProjectionColumnStore.setResidencyEvictForTesting(previousEvict);
    ProjectionColumnStore.setResidencyHeadroomForTesting(previousResidency);
    ProjectionColumnStore.setColumnFillBudgetBytesForTesting(previousBudget);
    HeapHeadroom.setHeadroomForTesting(previousHeadroom);
    ProjectionColumnStore.sampleHeadroomShare();
  }

  /** Pin the share to exactly {@code bytes} (the share is a quarter of the headroom on any real heap). */
  private static void shareOf(final long bytes) {
    HeapHeadroom.setHeadroomForTesting(Math.max(0L, bytes) * 4L);
    final long share = ProjectionColumnStore.sampleHeadroomShare();
    assertEquals(bytes, share, "the share must be a quarter of the pinned headroom on this heap");
  }

  @Test
  @DisplayName("a fill over the headroom share retains nothing and the access goes windowed")
  void aFillOverTheShareIsRefusedAndTheAccessGoesWindowed() {
    final Fixture f = build(6, 200);
    final long fill = f.store().projectedColumnFillBytes(0);
    assertTrue(fill > 0L, "the fixture must project a real fill");

    shareOf(fill - 1);
    assertEquals(fill - 1, ProjectionColumnStore.residencyBudgetBytes(),
        "the effective budget is the headroom share when it is below the static budget");
    assertFalse(f.store().columnFillable(0), "the planner must see the column as not resident-viable");
    assertFalse(f.store().columnsFitWithinBudget(new int[] {0, 1}, -1), "nor the pair");

    final long windowedBefore = ProjectionColumnStore.windowedLeafAccessCount();
    final LeafColumnAccess access = f.store().leafAccess(f.fetcher(), null, new int[] {0}, false);
    assertTrue(access.windowed(), "over the share the access must be the windowed one");
    assertEquals(windowedBefore + 1, ProjectionColumnStore.windowedLeafAccessCount());
    assertEquals(0L, f.store().retainedFillBytes(), "a refused route must retain nothing");

    // …and it still ANSWERS: the windowed slices carry the same values a resident fill would.
    final long windowedSum = sumThrough(access, 0, f.store().leafCount());
    assertThrows(FillBudgetExceededException.class, () -> f.store().column(0, f.fetcher()),
        "the fill door declines rather than retaining over the share");
    assertEquals(0L, f.store().retainedFillBytes(), "a declined fill must charge nothing");

    // MUTATION of the gate: with R1 killed the same store, share and budget retain the fill.
    ProjectionColumnStore.setResidencyHeadroomForTesting(false);
    assertTrue(f.store().columnFillable(0), "without the headroom gate the static budget admits it");
    final LeafColumnAccess resident = f.store().leafAccess(f.fetcher(), null, new int[] {0}, false);
    assertFalse(resident.windowed(), "without the headroom gate the access is resident");
    assertEquals(windowedSum, sumThrough(resident, 0, f.store().leafCount()),
        "the windowed answer must equal the resident one");
    assertTrue(f.store().retainedFillBytes() > 0L, "the resident route retained the fill");
  }

  @Test
  @DisplayName("released bytes return at the query scope's exit, and only over the share")
  void releasedBytesReturnAtTheScopeExit() {
    final Fixture f = build(6, 200);
    final long a = f.store().projectedColumnFillBytes(0);
    final long b = f.store().projectedColumnFillBytes(1);
    assertTrue(a > b, "column 0 must be the fatter one for the ordering assertions");

    shareOf(a + b);
    try (ProjectionResidencyScope scope = ProjectionResidencyScope.open()) {
      assertNotNull(f.store().column(0, f.fetcher()));
      assertNotNull(f.store().column(1, f.fetcher()));
      assertEquals(a + b, f.store().retainedFillBytes(), "both fills charged exactly once");
      assertEquals(1, f.store().residencyPins(0), "the filling query pins what it filled");
      assertEquals(1, f.store().residencyPins(1));
      assertNotNull(scope);
    }
    assertEquals(0, f.store().residencyPins(0), "the exit drops the pins");
    assertEquals(a + b, f.store().retainedFillBytes(), "within the share nothing is released");

    // The share now admits only the smaller column: the exit returns the larger one's bytes.
    final long releasedBefore = ProjectionColumnStore.residencyReleasedBytes();
    shareOf(b);
    try (ProjectionResidencyScope scope = ProjectionResidencyScope.open()) {
      assertTrue(f.store().columnFilled(1), "the second query touches the store it will sweep");
      assertNotNull(scope);
    }
    assertEquals(b, f.store().retainedFillBytes(), "the fat column's bytes returned");
    assertFalse(f.store().columnFilled(0), "…and its slices are gone");
    assertTrue(f.store().columnFilled(1), "…while the one that fits stayed");
    assertEquals(releasedBefore + a, ProjectionColumnStore.residencyReleasedBytes(),
        "the counter reports exactly the charged bytes of the released column");

    // A refill after a release is priced from scratch and works: the release is not a memo.
    shareOf(a + b);
    assertNotNull(f.store().column(0, f.fetcher()));
    assertEquals(a + b, f.store().retainedFillBytes(), "the refill charges the same bytes again");
  }

  @Test
  @DisplayName("a released column lets the next query's fill be resident where retention refused it")
  void theReleaseIsWhatMakesTheNextFillResident() {
    final Fixture f = build(6, 200);
    final long a = f.store().projectedColumnFillBytes(0);
    final long b = f.store().projectedColumnFillBytes(1);
    assertTrue(a > b);

    // Query 1 fills the fat column while the share admits it; the share then falls to the small one.
    shareOf(a);
    try (ProjectionResidencyScope query1 = ProjectionResidencyScope.open()) {
      assertNotNull(f.store().column(0, f.fetcher()));
      assertEquals(a, f.store().retainedFillBytes());
      shareOf(b); // the heap filled up while query 1 ran
      assertNotNull(query1);
    }
    assertEquals(0L, f.store().retainedFillBytes(), "the exit released what no longer fits");

    // Query 2's smaller fill now fits — this is the whole point of releasing.
    try (ProjectionResidencyScope query2 = ProjectionResidencyScope.open()) {
      assertTrue(f.store().columnFillable(1), "the released bytes are what makes this fit");
      assertNotNull(f.store().column(1, f.fetcher()));
      assertEquals(b, f.store().retainedFillBytes());
      assertNotNull(query2);
    }

    // MUTATION — "never release": with BOTH kill switches (no headroom release, no fit-door eviction)
    // the fat column stays and the second fill is refused, which is exactly the state 100M/8 GB was
    // in before R1. Eviction alone would admit the second fill by dropping the first (see
    // aFitDoorEvictsUnpinnedFillsToAdmitTheCurrentOne), so it has to be off for this half.
    final Fixture killed = build(6, 200);
    ProjectionColumnStore.setResidencyHeadroomForTesting(false);
    ProjectionColumnStore.setResidencyEvictForTesting(false);
    ProjectionColumnStore.setColumnFillBudgetBytesForTesting(a);
    try (ProjectionResidencyScope query1 = ProjectionResidencyScope.open()) {
      assertNotNull(killed.store().column(0, killed.fetcher()));
      assertNotNull(query1);
    }
    assertEquals(a, killed.store().retainedFillBytes(), "the kill switch retains for the store's lifetime");
    ProjectionColumnStore.setColumnFillBudgetBytesForTesting(b);
    try (ProjectionResidencyScope query2 = ProjectionResidencyScope.open()) {
      assertFalse(killed.store().columnFillable(1), "the retained column crowds the next query out");
      assertTrue(killed.store().leafAccess(killed.fetcher(), null, new int[] {1}, false).windowed(),
          "…which is what sends it windowed");
      assertNotNull(query2);
    }
    assertEquals(a, killed.store().retainedFillBytes(), "and nothing is ever released");
  }

  @Test
  @DisplayName("a column another query still pins survives that query's exit until the last pin drops")
  void aConcurrentlyPinnedColumnSurvivesTheOtherQuerysExit() throws Exception {
    final Fixture f = build(6, 200);
    final long a = f.store().projectedColumnFillBytes(0);
    final long b = f.store().projectedColumnFillBytes(1);
    assertTrue(a > b, "the pinned column must be the FAT one, so size alone would have released it");

    shareOf(a + b);
    try (ProjectionResidencyScope loader = ProjectionResidencyScope.open()) {
      assertNotNull(f.store().column(0, f.fetcher()));
      assertNotNull(f.store().column(1, f.fetcher()));
      assertNotNull(loader);
    }
    assertEquals(a + b, f.store().retainedFillBytes());

    final CountDownLatch pinned = new CountDownLatch(1);
    final CountDownLatch mayFinish = new CountDownLatch(1);
    final CountDownLatch finished = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread longRunning = new Thread(() -> {
      try (ProjectionResidencyScope query = ProjectionResidencyScope.open()) {
        assertNotNull(f.store().column(0, f.fetcher())); // resident: pins column 0 for this query
        pinned.countDown();
        mayFinish.await();
        assertNotNull(query);
      } catch (final Throwable t) {
        failure.set(t);
      } finally {
        finished.countDown();
      }
    }, "residency-long-running");
    longRunning.setDaemon(true);
    longRunning.start();
    try {
      pinned.await();
      assertEquals(1, f.store().residencyPins(0), "the still-running query holds the pin");

      // A second query ends while the first is still running, under a share that admits neither column.
      shareOf(0L);
      try (ProjectionResidencyScope shortLived = ProjectionResidencyScope.open()) {
        assertTrue(f.store().columnFilled(0), "it serves from the resident column too");
        assertNotNull(shortLived);
      }
      assertTrue(f.store().columnFilled(0), "the pinned column must survive the other query's exit");
      assertEquals(a, f.store().retainedFillBytes(), "only the unpinned column was released");
      assertFalse(f.store().columnFilled(1));
    } finally {
      // Unconditionally: a failing assertion above must not leave an OPEN scope behind, which would
      // pin every later test's store and turn one failure into a cascade.
      mayFinish.countDown();
      finished.await();
      longRunning.join();
    }
    assertEquals(null, failure.get(), "the long-running query must not have failed");
    assertEquals(0, f.store().residencyPins(0), "the last pin dropped");
    assertEquals(0L, f.store().retainedFillBytes(), "…and then the column was released");
    assertFalse(f.store().columnFilled(0));
  }

  @Test
  @DisplayName("the kill switch reproduces the pre-R1 retention exactly: the ledger never decreases")
  void theKillSwitchNeverReleases() {
    ProjectionColumnStore.setResidencyHeadroomForTesting(false);
    final Fixture f = build(6, 200);
    final long a = f.store().projectedColumnFillBytes(0);
    final long b = f.store().projectedColumnFillBytes(1);
    ProjectionColumnStore.setColumnFillBudgetBytesForTesting(a + b);
    // A share far below either fill: with R1 on this would refuse both and release everything.
    HeapHeadroom.setHeadroomForTesting(0L);
    ProjectionColumnStore.sampleHeadroomShare();
    assertEquals(a + b, ProjectionColumnStore.residencyBudgetBytes(), "the killed gate ignores the headroom");

    long highWater = 0L;
    for (int round = 0; round < 3; round++) {
      try (ProjectionResidencyScope query = ProjectionResidencyScope.open()) {
        assertNotNull(f.store().column(0, f.fetcher()));
        assertNotNull(f.store().column(1, f.fetcher()));
        assertNotNull(query);
      }
      final long retained = f.store().retainedFillBytes();
      assertTrue(retained >= highWater, "the killed ledger must never decrease");
      highWater = retained;
    }
    assertEquals(a + b, highWater);
    assertTrue(f.store().columnFilled(0) && f.store().columnFilled(1), "both fills kept for the store's lifetime");
  }

  @Test
  @DisplayName("a fit door evicts the unpinned columns earlier queries left behind to admit the current fill")
  void aFitDoorEvictsUnpinnedFillsToAdmitTheCurrentOne() {
    // The static budget, no headroom gate: exactly the production default of the query JVM.
    ProjectionColumnStore.setResidencyHeadroomForTesting(false);
    final Fixture f = build(6, 200);
    final long a = f.store().projectedColumnFillBytes(0);
    final long b = f.store().projectedColumnFillBytes(1);
    assertTrue(a > b && b > 0L, "column 0 must be the fat one and column 1 a real fill");
    // Room for the fat column alone: after it, the thin one no longer fits BESIDE it.
    ProjectionColumnStore.setColumnFillBudgetBytesForTesting(a + b - 1);

    try (ProjectionResidencyScope earlier = ProjectionResidencyScope.open()) {
      assertNotNull(f.store().column(0, f.fetcher()));
      assertNotNull(earlier);
    }
    assertEquals(a, f.store().retainedFillBytes(), "the earlier query's column stays retained (within budget)");
    assertEquals(0, f.store().residencyPins(0), "…and nothing pins it once that query is gone");

    final long evictionsBefore = ProjectionColumnStore.residencyEvictionCount();
    final long evictedBefore = ProjectionColumnStore.residencyEvictedBytes();
    // A later query prices column 1: it fits the budget on its own, so the door makes room for it.
    assertTrue(f.store().columnFillable(1), "a fill that fits the budget alone must be admitted");
    assertFalse(f.store().columnFilled(0), "…by evicting the unpinned column that held the budget");
    assertEquals(0L, f.store().retainedFillBytes(), "the ledger returned exactly what column 0 charged");
    assertEquals(evictionsBefore + 1, ProjectionColumnStore.residencyEvictionCount());
    assertEquals(evictedBefore + a, ProjectionColumnStore.residencyEvictedBytes());
    try (ProjectionResidencyScope later = ProjectionResidencyScope.open()) {
      assertNotNull(f.store().column(1, f.fetcher()));
      assertNotNull(later);
    }
    assertEquals(b, f.store().retainedFillBytes(), "the admitted fill is retained");

    // MUTATION of the switch: with eviction off the same fit is refused and nothing moves.
    ProjectionColumnStore.setResidencyEvictForTesting(false);
    assertFalse(f.store().columnFillable(0), "first-come-first-served: column 1 crowds column 0 out");
    assertTrue(f.store().columnFilled(1));
    assertEquals(b, f.store().retainedFillBytes());
    ProjectionColumnStore.setResidencyEvictForTesting(true);

    // A fill that could never fit leaves the store exactly as it found it: nothing evicted for nothing.
    ProjectionColumnStore.setColumnFillBudgetBytesForTesting(a - 1);
    assertFalse(f.store().columnFillable(0), "column 0 exceeds the budget on its own");
    assertTrue(f.store().columnFilled(1), "…so the resident column 1 must not have been evicted");
    assertEquals(b, f.store().retainedFillBytes());
    assertEquals(evictionsBefore + 1, ProjectionColumnStore.residencyEvictionCount(), "no eviction");

    // A column the CURRENT query already reads is pinned and never evicted, even when it would make room.
    ProjectionColumnStore.setColumnFillBudgetBytesForTesting(a + b - 1);
    try (ProjectionResidencyScope reader = ProjectionResidencyScope.open()) {
      assertTrue(f.store().columnFilled(1), "observing the resident column pins it for this query");
      assertEquals(1, f.store().residencyPins(1));
      assertFalse(f.store().columnFillable(0), "column 0 cannot be admitted over a pinned column 1");
      assertTrue(f.store().columnFilled(1), "the pinned column survived the refused fit");
      assertEquals(b, f.store().retainedFillBytes());
      assertNotNull(reader);
    }
    // Once that query is gone the same fit evicts column 1 and admits column 0 through the fill door itself.
    assertNotNull(f.store().column(0, f.fetcher()), "the fill door makes room too, not only the planner gates");
    assertFalse(f.store().columnFilled(1));
    assertEquals(a, f.store().retainedFillBytes());
    assertEquals(evictionsBefore + 2, ProjectionColumnStore.residencyEvictionCount());
  }

  /** Sum every present value of {@code col} through the access — the answer must not depend on the route. */
  private static long sumThrough(final LeafColumnAccess access, final int col, final int leaves) {
    long sum = 0;
    for (int leaf = 0; leaf < leaves; leaf++) {
      final ProjectionColumnStore.ColumnSlice slice = access.slice(col, leaf);
      final long[] values = slice.numericValues();
      if (values == null) {
        continue;
      }
      for (int row = 0; row < slice.rowCount(); row++) {
        if ((slice.presenceWords()[row >>> 6] & 1L << (row & 63)) != 0) {
          sum += values[row];
        }
      }
    }
    return sum;
  }

  /** A store of {@code leaves} leaves whose column 0 is wide-random and column 1 constant. */
  private static Fixture build(final int leaves, final int rowsPerLeaf) {
    final Random rnd = new Random(20260830L);
    final Map<Long, byte[]> segmentsByOffset = new HashMap<>();
    final List<RowGroupDirectory> directories = new ArrayList<>(leaves);
    long nextOffset = 1_000;
    for (int leaf = 0; leaf < leaves; leaf++) {
      final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS.clone());
      final long[] longs = new long[KINDS.length];
      final boolean[] bools = new boolean[KINDS.length];
      final String[] strings = new String[KINDS.length];
      long recordKey = leaf * 100_000L + 1;
      for (int row = 0; row < rowsPerLeaf; row++) {
        longs[0] = rnd.nextLong();
        longs[1] = 42L;
        page.appendRow(recordKey++, longs, bools, strings);
      }
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
          ProjectionIndexColumnSegmentCodec.encode(page.serialize());
      final int segments = encoded.columnSegmentIds().length;
      final int[] ids = new int[segments];
      final long[] offsets = new long[segments];
      for (int i = 0; i < segments; i++) {
        ids[i] = encoded.columnSegmentIds()[i];
        offsets[i] = nextOffset;
        segmentsByOffset.put(nextOffset, encoded.segments()[i]);
        nextOffset += 1 + encoded.segments()[i].length;
      }
      directories.add(new RowGroupDirectory(leaf + 1, encoded.descriptor(), ids, offsets, new byte[ids.length][]));
    }
    final ColumnSegmentFetcher fetcher = wanted -> {
      final byte[][] out = new byte[wanted.length][];
      for (int i = 0; i < wanted.length; i++) {
        out[i] = segmentsByOffset.get(wanted[i]);
      }
      return out;
    };
    return new Fixture(new ProjectionColumnStore(directories), fetcher);
  }
}
