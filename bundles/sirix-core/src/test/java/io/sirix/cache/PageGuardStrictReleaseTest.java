/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.page.KeyValueLeafPage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A {@link PageGuard} must hold a real guard, and must say so when it does not.
 *
 * <p>{@code close()} used to return quietly when the page was already closed or its guard count had
 * reached zero, justified by the cache paths that force-released guards they did not own. Those
 * drains are gone, which makes both states impossible under a live guard — {@code close()} defers on
 * a guarded page, and nothing may release a guard it did not take. The tolerance was worse than dead
 * code: an unbacked guard object, produced by {@code wrapAlreadyGuarded} over a failed
 * {@code acquireGuard()}, took the count-is-zero exit and reported nothing, so the bug surfaced
 * later as someone else's page being freed early.</p>
 */
public final class PageGuardStrictReleaseTest {

  private static final int SIXTY_FOUR_KB = 64 * 1024;

  private Arena arena;

  @BeforeEach
  void setUp() {
    arena = Arena.ofConfined();
  }

  @AfterEach
  void tearDown() {
    if (arena != null) {
      arena.close();
    }
  }

  private KeyValueLeafPage newPage() {
    return new KeyValueLeafPage(1L, IndexType.DOCUMENT,
        new ResourceConfiguration.Builder("pageGuardStrictReleaseResource").build(), 1,
        arena.allocate(SIXTY_FOUR_KB), null);
  }

  @Test
  void aProperlyAcquiredGuardReleasesCleanly() {
    final KeyValueLeafPage page = newPage();
    assertTrue(page.acquireGuard(), "precondition: the page can be guarded");

    try (PageGuard guard = PageGuard.wrapAlreadyGuarded(page)) {
      assertEquals(1, page.getGuardCount(), "the guard must be held for the scope's duration");
      assertEquals(page, guard.page());
    }

    assertEquals(0, page.getGuardCount(), "closing the guard must release exactly one guard");
    assertFalse(page.isClosed(), "a non-orphaned page stays alive after its last guard");

    page.close(); // don't leave it in the leak census that -Dsirix.debug.memory.leaks reports
  }

  @Test
  void wrappingAnUnguardedPageIsReportedOnRelease() {
    final KeyValueLeafPage page = newPage();

    // What an ignored acquireGuard() result produces: a guard object backed by nothing. Releasing
    // it would drive the count to -1, or, if another holder had a guard, take THEIR guard and free
    // the page under them.
    final long before = PageGuard.getUnguardedReleaseCount();
    final PageGuard unbacked = PageGuard.wrapAlreadyGuarded(page);

    // Reported, not thrown: guards are released from finally blocks and close paths, so throwing
    // here would abandon the rest of a teardown — a worse bug than the one being announced.
    unbacked.close();

    assertEquals(before + 1, PageGuard.getUnguardedReleaseCount(),
        "releasing a guard that was never acquired must be recorded, not silently skipped");
    assertEquals(0, page.getGuardCount(), "the bogus release must not drive the count negative");

    page.close();
  }

  /**
   * The wrapper catches only the zero-count case — the call site is what has to be right.
   *
   * <p>When another holder's guard is live, an unbacked wrapper's release is indistinguishable from a
   * legitimate one: the count is positive either way, and {@link PageGuard} keeps no record of having
   * acquired. So it releases, and the real holder loses its guard. That is not a hole this class can
   * close; it is why {@code wrapAlreadyGuarded}'s callers must check {@code acquireGuard()}'s result,
   * and why the acquiring constructor is the better default. Pinned here so the limit is documented
   * behaviour rather than a surprise to the next caller.</p>
   */
  @Test
  void wrapAlreadyGuardedCannotDetectAStolenGuard() {
    final KeyValueLeafPage page = newPage();
    assertTrue(page.acquireGuard(), "precondition: some holder has a guard");

    final long before = PageGuard.getUnguardedReleaseCount();
    PageGuard.wrapAlreadyGuarded(page).close();

    assertEquals(0, page.getGuardCount(),
        "documented limit: with a positive count the wrapper cannot tell whose guard it is");
    assertEquals(before, PageGuard.getUnguardedReleaseCount(),
        "and it does not even register as anomalous — the call site is the only place this is "
            + "detectable, which is what PageScanIterator and ColumnarScanAxis now do");

    page.close();
  }

  @Test
  void guardingAnOrphanedPageIsRejectedUpFront() {
    final KeyValueLeafPage page = newPage();
    page.markOrphaned();

    // acquireGuard() refuses to resurrect an orphan from zero guards — it may already be mid-
    // teardown. The acquiring constructor turns that into an exception rather than a silent
    // unguarded wrapper.
    assertThrows(IllegalStateException.class, () -> new PageGuard(page),
        "an orphaned page with no holders must not be guardable");
    assertEquals(0, page.getGuardCount(), "a failed acquire must not have incremented");

    page.close();
  }
}
