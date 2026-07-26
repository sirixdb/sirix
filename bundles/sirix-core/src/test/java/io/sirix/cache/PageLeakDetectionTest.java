/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.page.KeyValueLeafPage;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.ref.WeakReference;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end check that collection-based leak detection can actually fire.
 *
 * <p>Two mechanisms are supposed to cover disjoint halves of the problem: the {@code ALL_LIVE_PAGES}
 * registry reports pages still REACHABLE at shutdown, and a {@code Cleaner} action reports pages that
 * became UNREACHABLE without {@code close()}. The second was dead — the registry held strong
 * references, so a tracked page could never be collected, and {@code PAGES_FINALIZED_WITHOUT_CLOSE}
 * was structurally pinned at zero for as long as tracking was enabled (the only time it is wired up
 * at all). A zero there looked like "no leaks" when it meant "cannot observe leaks".</p>
 *
 * <p>Runs only with {@code -Dsirix.debug.memory.leaks=true}; the flag is read once at class-init, so
 * there is no way to enable it from inside a suite that has already loaded the page class.
 * {@code WeakIdentitySetTest} pins the underlying registry semantics unconditionally.</p>
 */
public final class PageLeakDetectionTest {

  private static final int SIXTY_FOUR_KB = 64 * 1024;

  @Test
  void aPageDroppedWithoutCloseIsReportedOnceCollected() {
    assumeTrue(KeyValueLeafPage.DEBUG_MEMORY_LEAKS,
        "needs -Dsirix.debug.memory.leaks=true; the flag is read at class-init");

    final long before = KeyValueLeafPage.PAGES_FINALIZED_WITHOUT_CLOSE.get();

    try (final Arena arena = Arena.ofConfined()) {
      // Scoped so the only strong reference dies there. Arena-backed, so the frame itself is the
      // arena's to reclaim — what is under test is the detection, not the allocation. The weak
      // reference lets us observe collection without keeping the page alive.
      final WeakReference<KeyValueLeafPage> doomed = createAndAbandonPage(arena);

      final long deadline = System.currentTimeMillis() + 20_000L;
      while (KeyValueLeafPage.PAGES_FINALIZED_WITHOUT_CLOSE.get() == before
          && System.currentTimeMillis() < deadline) {
        System.gc();
        try {
          Thread.sleep(25L);
        } catch (final InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          break;
        }
      }

      assertTrue(KeyValueLeafPage.PAGES_FINALIZED_WITHOUT_CLOSE.get() > before,
          "a page collected without close() must be counted — a zero here used to mean the "
              + "registry was pinning every page, not that nothing leaked");
      assertNull(doomed.get(),
          "and the registry must not be what keeps it alive; the live census is only meaningful if "
              + "it lists reachable pages");
    }
  }

  /** Kept out of the test method so no local slot in the caller's frame keeps the page alive. */
  private static WeakReference<KeyValueLeafPage> createAndAbandonPage(final Arena arena) {
    final KeyValueLeafPage doomed = new KeyValueLeafPage(1L, IndexType.DOCUMENT,
        new ResourceConfiguration.Builder("pageLeakDetectionResource").build(), 1,
        arena.allocate(SIXTY_FOUR_KB), null);
    assertTrue(KeyValueLeafPage.ALL_LIVE_PAGES.contains(doomed), "precondition: it is registered");
    return new WeakReference<>(doomed);
  }
}
