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
    assertEquals(128L << 20, DatabaseConfiguration.defaultSegmentAllocationSize(),
        "the launcher cap must also be the pre-database cache startup request");
    // Odd allowance exercises the remainder of floor(3 * allowance / 16), not just aligned grants.
    final long allowance = (64L << 20) + 7;
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
      assertTrue(
          Databases.createJsonDatabase(new DatabaseConfiguration(first).setMaxSegmentAllocationSize(128L << 20)));
      assertEquals(allowance, Allocators.getInstance().getMaxBufferSize());
      assertEquals(33_554_435L, ((ShardedPageCache<?>) manager.getRecordPageCache()).getMaxWeightBytes());
      assertEquals(12_582_913L, ((ShardedPageCache<?>) manager.getRecordPageFragmentCache()).getMaxWeightBytes());
      assertEquals(7, ((PageCache) manager.getPageCache()).maxEntries());
      for (final String expected : new String[] {
          "Initializing global BufferManager with effective allocator allowance: 67108871 bytes",
          "  - RevisionRootPageCache size: 20000 entries (default)", "  - NamesCache size: 500 (fixed)",
          "  - PathSummaryCache size: 20 (fixed)",
          "GLOBAL ClockSweeper threads started (PostgreSQL bgwriter pattern)"}) {
        assertTrue(events.list.stream().anyMatch(event -> event.getFormattedMessage().equals(expected)), expected);
      }
      assertTrue(
          events.list.stream().anyMatch(event -> event.getFormattedMessage().equals("  - PageCache: 7 entries")));
      assertTrue(
          events.list.stream()
                     .anyMatch(
                         event -> event.getFormattedMessage()
                                       .equals(
                                           "  - RecordPageCache: 33554435 bytes (32 MB) (default: 50% of budget)")));
      assertTrue(
          events.list.stream()
                     .anyMatch(
                         event -> event.getFormattedMessage()
                                       .equals(
                                           "  - RecordPageFragmentCache: 12582913 bytes (12 MB) (default: 18.75% of budget)")));
      try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(first)) {
        assertTrue(
            Databases.createJsonDatabase(new DatabaseConfiguration(second).setMaxSegmentAllocationSize(256L << 20)));
        assertEquals(allowance, Allocators.getInstance().getMaxBufferSize());
        assertTrue(
            events.list.stream()
                       .anyMatch(
                           event -> event.getFormattedMessage()
                                         .equals(
                                             "Allocator startup request: 268435456 bytes; effective process allowance: 67108871 bytes. "
                                                 + "The first initialization wins; later database requests do not resize it.")));
        assertEquals(128L << 20, database.getDatabaseConfig().getMaxSegmentAllocationSize());
      }
      final String previousEntries = System.getProperty("sirix.cache.page.max.entries");
      final String previousBytes = System.getProperty("sirix.cache.page");
      try {
        System.clearProperty("sirix.cache.page.max.entries");
        System.clearProperty("sirix.cache.page");
        Databases.reinitializeBufferManagerForTesting(48L << 20, 8L << 20, 3_000L);
        assertEquals(3, ((PageCache) Databases.getGlobalBufferManager().getPageCache()).maxEntries());
        assertEquals(48L << 20,
            ((ShardedPageCache<?>) Databases.getGlobalBufferManager().getRecordPageCache()).getMaxWeightBytes());
        assertEquals(8L << 20, ((ShardedPageCache<?>) Databases.getGlobalBufferManager()
                                                               .getRecordPageFragmentCache()).getMaxWeightBytes());
        Databases.reinitializeBufferManagerForTesting(48L << 20, 8L << 20);
        assertEquals(50_000, ((PageCache) Databases.getGlobalBufferManager().getPageCache()).maxEntries());
        for (final String expected : new String[] {"  - RecordPageCache: 50331648 bytes (48 MB)",
            "  - RecordPageFragmentCache: 8388608 bytes (8 MB)",
            "  - PageCache legacy estimate: 3000 bytes; effective entry cap logged by PageCache",
            "  - PageCache legacy estimate: 50000000 bytes; effective entry cap logged by PageCache",
            "  - PageCache: 3 entries", "  - PageCache: 50000 entries"}) {
          assertTrue(events.list.stream().anyMatch(event -> event.getFormattedMessage().equals(expected)), expected);
        }
      } finally {
        if (previousEntries != null) {
          System.setProperty("sirix.cache.page.max.entries", previousEntries);
        }
        if (previousBytes != null) {
          System.setProperty("sirix.cache.page", previousBytes);
        }
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
