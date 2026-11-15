package io.sirix.access;

import io.sirix.cache.Allocators;
import io.sirix.cache.BufferManager;
import io.sirix.cache.PageCache;
import io.sirix.cache.ShardedPageCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Fresh-process counterpart to the explicitly preinitialized first-wins startup fixture. */
final class AllocatorColdStartupTest {
  @BeforeEach
  void resetCacheOwnerForMutationReplay() throws Exception {
    final Field manager = Databases.class.getDeclaredField("GLOBAL_BUFFER_MANAGER");
    manager.setAccessible(true);
    final BufferManager existing = (BufferManager) manager.get(null);
    if (existing != null) {
      existing.close();
    }
    manager.set(null, null);
  }

  @Test
  void preDatabaseCacheAccessInitializesTheConfiguredProcessAllowance() {
    final BufferManager manager = Databases.getGlobalBufferManager();
    assertTrue(Allocators.getInstance().isInitialized());
    assertEquals(128L << 20, Allocators.getInstance().getMaxBufferSize());
    assertEquals(64L << 20, ((ShardedPageCache<?>) manager.getRecordPageCache()).getMaxWeightBytes());
    assertEquals(24L << 20, ((ShardedPageCache<?>) manager.getRecordPageFragmentCache()).getMaxWeightBytes());
    assertEquals(7, ((PageCache) manager.getPageCache()).maxEntries());
  }
}
