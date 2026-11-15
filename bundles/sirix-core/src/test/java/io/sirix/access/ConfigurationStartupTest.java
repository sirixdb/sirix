package io.sirix.access;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.Allocators;
import io.sirix.cache.BufferManager;
import io.sirix.cache.BufferManagerImpl;
import io.sirix.cache.PageCache;
import io.sirix.cache.ShardedPageCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Runs alone: global allocator and cache policy deliberately cannot be resized between tests. */
final class ConfigurationStartupTest {
  @TempDir
  Path directory;

  @BeforeEach
  void resetManagerForAnIndependentStartupObservation() throws Exception {
    // PIT can reuse its JVM. Reset only the cache owner; the allocator's first allowance stays fixed.
    final Field manager = Databases.class.getDeclaredField("GLOBAL_BUFFER_MANAGER");
    manager.setAccessible(true);
    final BufferManager existing = (BufferManager) manager.get(null);
    if (existing != null) {
      existing.close();
    }
    manager.set(null, null);
  }

  @Test
  void persistedRequestsCannotResizeTheProcessAndLogsDescribeEffectiveCaps() {
    final long allowance = 64L << 20;
    Allocators.getInstance().init(allowance);
    final Logger logger = (Logger) LoggerFactory.getLogger(Databases.class);
    final Logger cacheLogger = (Logger) LoggerFactory.getLogger(BufferManagerImpl.class);
    final ListAppender<ILoggingEvent> events = new ListAppender<>();
    events.start();
    logger.addAppender(events);
    cacheLogger.addAppender(events);
    final Path first = directory.resolve("first");
    final Path second = directory.resolve("second");
    try {
      final BufferManager manager = Databases.getGlobalBufferManager();
      assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(first).setMaxSegmentAllocationSize(128L << 20)));
      assertEquals(allowance, Allocators.getInstance().getMaxBufferSize());
      assertEquals(32L << 20, ((ShardedPageCache<?>) manager.getRecordPageCache()).getMaxWeightBytes());
      assertEquals(12L << 20, ((ShardedPageCache<?>) manager.getRecordPageFragmentCache()).getMaxWeightBytes());
      assertEquals(7, ((PageCache) manager.getPageCache()).maxEntries());
      assertTrue(events.list.stream().anyMatch(event -> event.getFormattedMessage().equals("  - PageCache: 7 entries")));
      assertTrue(events.list.stream().anyMatch(event -> event.getFormattedMessage().equals(
          "  - RecordPageCache: 33554432 bytes (32 MB) (default: 50% of budget)")));
      assertTrue(events.list.stream().anyMatch(event -> event.getFormattedMessage().equals(
          "  - RecordPageFragmentCache: 12582912 bytes (12 MB) (default: 18.75% of budget)")));
      try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(first)) {
        assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(second).setMaxSegmentAllocationSize(256L << 20)));
        assertEquals(allowance, Allocators.getInstance().getMaxBufferSize());
        assertTrue(events.list.stream().anyMatch(event -> event.getFormattedMessage().equals(
            "Allocator startup request: 268435456 bytes; effective process allowance: 67108864 bytes. "
                + "The first initialization wins; later database requests do not resize it.")));
        assertEquals(128L << 20, database.getDatabaseConfig().getMaxSegmentAllocationSize());
      }
    } finally {
      logger.detachAppender(events);
      cacheLogger.detachAppender(events);
      events.stop();
      Databases.removeDatabase(first);
      Databases.removeDatabase(second);
    }
  }
}
