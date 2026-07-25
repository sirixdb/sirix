/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Taking a page out of a cache must hand back the page that was actually removed.
 *
 * <p>{@code TransactionIntentLog.put} used to {@code get} the cached page and then {@code remove}
 * the key as two separate operations, closing whatever the {@code get} had returned. Those are not
 * the same page under concurrency: a reader caching a page for that reference in between leaves the
 * {@code remove} evicting an instance nobody examines. It is then out of the cache, never enters a
 * TIL container, and is never closed — an owner-less 64 KiB frame per lost race, which is why the
 * residue grew with reader count and commit rate rather than staying flat.</p>
 *
 * <p>{@code removeAndGet} closes the window by doing both inside one {@code compute}. This pins the
 * contract it has to satisfy: the returned instance is the one unmapped, and it is handed over
 * OPEN — removal transfers ownership, it does not free.</p>
 */
public final class RemoveAndGetAtomicityTest {

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

  private KeyValueLeafPage newPage(final long pageKey) {
    return new KeyValueLeafPage(pageKey, IndexType.DOCUMENT,
        new ResourceConfiguration.Builder("removeAndGetResource").build(), 1,
        arena.allocate(SIXTY_FOUR_KB), null);
  }

  @Test
  void removeAndGetReturnsTheInstanceItUnmapped() {
    final ShardedPageCache<KeyValueLeafPage> cache = new ShardedPageCache<>(64L * SIXTY_FOUR_KB);
    final PageReference key = new PageReference().setKey(4096L).setDatabaseId(1L).setResourceId(1L);

    final KeyValueLeafPage first = newPage(1L);
    cache.put(key, first);

    // Stand in for the racing insert: whatever is mapped at removal time is what must come back,
    // not whatever an earlier get() happened to see.
    final KeyValueLeafPage second = newPage(2L);
    cache.put(key, second);

    final KeyValueLeafPage removed = cache.removeAndGet(key);

    assertSame(second, removed, "removeAndGet must return the mapping it actually removed");
    assertNull(cache.get(key), "and the entry must be gone");
    assertFalse(removed.isClosed(),
        "removal transfers ownership to the caller — it must not free the page, or the caller's own "
            + "teardown would be a double free");

    first.close();
    second.close();
  }

  @Test
  void removeAndGetOnAnAbsentKeyIsNull() {
    final ShardedPageCache<KeyValueLeafPage> cache = new ShardedPageCache<>(64L * SIXTY_FOUR_KB);
    final PageReference key = new PageReference().setKey(8192L).setDatabaseId(1L).setResourceId(1L);

    assertNull(cache.removeAndGet(key), "an absent key removes nothing and returns nothing");
  }
}
