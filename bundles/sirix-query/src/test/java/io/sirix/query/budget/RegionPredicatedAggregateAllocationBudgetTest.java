/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.budget;

import com.sun.management.ThreadMXBean;
import io.sirix.query.scan.QryCorpus;
import io.sirix.query.scan.QryFixture;
import io.sirix.query.scan.QryFixture.Outcome;
import io.sirix.query.scan.QryPlanned;
import io.sirix.query.scan.QrySeam;
import io.sirix.query.scan.QrySeam.Counters;
import io.sirix.query.scan.QrySeam.RouteOutcome;
import io.sirix.query.scan.QrySteps;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.sirix.query.scan.QryCorpus.SLOTS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * QRY-502's allocation budget: after warm-up the route's per-page and per-record loop must not
 * allocate, so a query's allocation has no term in page or record count:
 * {@code A(4P) - A(P) <= 4 KiB}. Literal encodings, member-path keys and scratch arrays are
 * resolved once per scan, not once per page.
 *
 * <p>
 * It runs in its own JVM ({@code :sirix-query:regionPredicatedAggregateAllocationTest}) with escape
 * analysis and the Graal JIT off, as in CONV-302's allocation legs, so it counts the allocation as
 * the code writes it. Allocation is counted over every thread of the JVM
 * ({@code getTotalThreadAllocatedBytes}), because executor workers fold pages off the calling
 * thread; each figure is the minimum of several warm runs, since other threads can only add to it.
 * It is skipped where thread allocation accounting is unsupported.
 *
 * <p>
 * The 4 KiB tolerance is the chapter's starting value (decision D4): the first measurement fixes
 * it, and after that it is never widened.
 */
@Isolated
final class RegionPredicatedAggregateAllocationBudgetTest {

  private static final String DB = "qry-allocation-db";
  private static final long TOLERANCE_BYTES = 4L * 1024;
  private static final int P_RECORDS = 2_200;
  private static final int WARMUP = 40;
  private static final int MEASURED = 9;

  private static Path dir;
  private static QryFixture fixture;

  @BeforeAll
  static void storeFixtures() throws Exception {
    dir = Files.createTempDirectory("sirix-qry-allocation-");
    fixture = new QryFixture(dir, DB);
    // Same-length resource names, so the two queries' texts allocate the same.
    fixture.store("p1.jn", QryCorpus.array(QryCorpus.lemireRecords(P_RECORDS, 0xA11CL)));
    fixture.store("p4.jn", QryCorpus.array(QryCorpus.lemireRecords(4 * P_RECORDS, 0xA11CL)));
  }

  @AfterAll
  static void dropDatabase() {
    if (fixture != null) {
      fixture.drop();
    }
  }

  // Planned on the method, not the class: a skipped class or parameterized
  // container leaves this fork with no test Gradle counts as matched ("No tests found").
  @Test
  @QryPlanned(step = QrySteps.ROUTE, value = "QRY-502")
  @DisplayName("QRY-502: a warm query's allocation has no term in page or record count")
  void allocationDoesNotGrowWithPages() throws Exception {
    for (final int which : new int[] {0, 1}) {
      allocationDoesNotGrowWithPages(which);
    }
  }

  private static void allocationDoesNotGrowWithPages(final int which) throws Exception {
    final var platform = ManagementFactory.getThreadMXBean();
    assumeTrue(platform instanceof ThreadMXBean, "no com.sun.management.ThreadMXBean");
    final ThreadMXBean bean = (ThreadMXBean) platform;
    assumeTrue(bean.isThreadAllocatedMemorySupported(), "thread allocation accounting unsupported");
    if (!bean.isThreadAllocatedMemoryEnabled()) {
      bean.setThreadAllocatedMemoryEnabled(true);
    }
    final long pagesP = fixture.maxNodeKey("p1.jn") / SLOTS + 1;
    final long pages4P = fixture.maxNodeKey("p4.jn") / SLOTS + 1;
    assertTrue(pages4P >= 3 * pagesP, "fixture: 4P must hold about four times P's pages: " + pagesP + " vs " + pages4P);
    final String q1 = query(which, "p1.jn");
    final String q4 = query(which, "p4.jn");
    final Outcome e1 = fixture.interpreter(q1);
    final Outcome e4 = fixture.interpreter(q4);
    for (int i = 0; i < WARMUP; i++) {
      assertEquals(e1, fixture.route("p1.jn", q1, 1));
      assertEquals(e4, fixture.route("p4.jn", q4, 1));
    }
    final Counters c1 = QrySeam.around(() -> fixture.route("p1.jn", q1, 1));
    final Counters c4 = QrySeam.around(() -> fixture.route("p4.jn", q4, 1));
    assertEquals(RouteOutcome.SERVED, c1.outcome(), "the measured query must be the route's: " + c1);
    assertEquals(RouteOutcome.SERVED, c4.outcome(), "the measured query must be the route's: " + c4);
    final long a1 = minimumAllocation(bean, "p1.jn", q1, e1);
    final long a4 = minimumAllocation(bean, "p4.jn", q4, e4);
    assertTrue(a4 - a1 <= TOLERANCE_BYTES, "A(4P) - A(P) = " + (a4 - a1) + " bytes > " + TOLERANCE_BYTES + " (A(P) = "
        + a1 + " over " + pagesP + " pages, A(4P) = " + a4 + " over " + pages4P + " pages)");
  }

  private static String query(final int which, final String resource) {
    return which == 0
        ? QryCorpus.adminSum(fixture.src(resource))
        : QryCorpus.nestedMembershipCount(fixture.src(resource));
  }

  private static long minimumAllocation(final ThreadMXBean bean, final String resource, final String query,
      final Outcome expected) throws Exception {
    long minimum = Long.MAX_VALUE;
    for (int i = 0; i < MEASURED; i++) {
      final long before = bean.getTotalThreadAllocatedBytes();
      final Outcome actual = fixture.route(resource, query, 1);
      final long allocated = bean.getTotalThreadAllocatedBytes() - before;
      assertEquals(expected, actual);
      minimum = Math.min(minimum, allocated);
    }
    return minimum;
  }
}
