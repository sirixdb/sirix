/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.JsonTestHelper;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The record-page invalidation sweep must never free a page another holder is still guarding.
 *
 * <p>The sweep used to drain guards it did not own — {@code while (getGuardCount() > 0)
 * releaseGuard()} — and then close the page. That forges the "I'm done" signal on behalf of the real
 * holder: the off-heap slot is released mid-read, and the holder's own release then drives the count
 * negative. It is reachable because the sweep is database-scoped while the truncation that triggers
 * it is resource-scoped, so a live transaction on a sibling resource can be holding a guard on a
 * page whose bytes were never truncated at all.</p>
 *
 * <p>{@code markOrphaned()} + {@code close()} is the protocol {@code TransactionIntentLog.closePage}
 * already used for the same reason: unmapping is what the stale-offset hazard requires, and the
 * orphan bit makes the holder's last release perform the teardown. Note {@code close()} alone is not
 * enough — {@link KeyValueLeafPage#close()} returns early on a guarded page without arranging any
 * later teardown, which would leak the slot instead of freeing it.</p>
 */
public final class RecordPageSweepGuardTest {

  private static final long DATABASE_ID = 987_654_331L;
  private static final long RESOURCE_ID = 5L;
  private static final int SIXTY_FOUR_KB = 64 * 1024;

  private Arena arena;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
    arena = Arena.ofConfined();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
    if (arena != null) {
      arena.close();
    }
  }

  private static PageReference keyFor(final long offset) {
    return new PageReference().setKey(offset).setDatabaseId(DATABASE_ID).setResourceId(RESOURCE_ID);
  }

  private KeyValueLeafPage newPage() {
    return new KeyValueLeafPage(1L, IndexType.DOCUMENT,
        new ResourceConfiguration.Builder("recordPageSweepGuardResource").build(), 1,
        arena.allocate(SIXTY_FOUR_KB), null);
  }

  @Test
  void sweepUnmapsButDoesNotFreeAGuardedRecordPage() {
    final BufferManager buffers = Databases.getGlobalBufferManager();
    final Cache<PageReference, KeyValueLeafPage> recordCache = buffers.getRecordPageCache();

    final PageReference key = keyFor(4096L);
    final KeyValueLeafPage page = newPage();
    recordCache.put(key, page);

    // Stand in for a transaction that is mid-read on this page — exactly what a sibling resource's
    // transaction is when a database-scoped sweep runs for a resource-scoped truncation.
    assertTrue(page.acquireGuard(), "precondition: the page can be guarded");

    Databases.clearCachesForDatabase(DATABASE_ID);

    assertNull(recordCache.get(key),
        "the sweep must still unmap the entry — that is what makes a reused offset safe");
    assertFalse(page.isClosed(),
        "the sweep must not free a page a live reader is still using; teardown defers to the last "
            + "releaseGuard");
    assertEquals(1, page.getGuardCount(),
        "the sweep must not forge the holder's release — a drained count underflows when the real "
            + "holder finishes");

    // The holder finishes normally: the slot is reclaimed at that point, not before.
    page.releaseGuard();
    assertTrue(page.isClosed(), "the last release must complete the deferred teardown");
  }

  /**
   * The record-page FRAGMENT cache is swept by the same methods and was the half left behind.
   *
   * <p>Versioning fragments are shared across transactions by construction — that is the whole point
   * of the fragment cache — so a guard held here is, if anything, more likely to belong to someone
   * other than the sweeping thread than one on a combined page.</p>
   */
  @Test
  void sweepUnmapsButDoesNotFreeAGuardedRecordPageFragment() {
    final BufferManager buffers = Databases.getGlobalBufferManager();
    final Cache<PageReference, KeyValueLeafPage> fragmentCache = buffers.getRecordPageFragmentCache();

    final PageReference key = keyFor(8192L);
    final KeyValueLeafPage fragment = newPage();
    fragmentCache.put(key, fragment);
    assertTrue(fragment.acquireGuard(), "precondition: the fragment can be guarded");

    Databases.clearCachesForDatabase(DATABASE_ID);

    assertNull(fragmentCache.get(key), "the sweep must still unmap the fragment");
    assertFalse(fragment.isClosed(),
        "the sweep must not free a fragment a live merge is still reading");
    assertEquals(1, fragment.getGuardCount(), "the sweep must not forge the holder's release");

    fragment.releaseGuard();
    assertTrue(fragment.isClosed(), "the last release must complete the deferred teardown");
  }
}
