package io.sirix.cache;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.sirix.page.IndirectPage;
import io.sirix.page.PageReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ResourceLock("SYSTEM_PROPERTIES")
final class PageCacheConfigurationTest {
  private String previousEntries;
  private String previousBytes;

  @BeforeEach
  void saveProperties() {
    previousEntries = System.getProperty(PageCache.MAX_ENTRIES_PROPERTY);
    previousBytes = System.getProperty(PageCache.LEGACY_BYTES_PROPERTY);
    System.clearProperty(PageCache.MAX_ENTRIES_PROPERTY);
    System.clearProperty(PageCache.LEGACY_BYTES_PROPERTY);
  }

  @AfterEach
  void restoreProperties() {
    restore(PageCache.MAX_ENTRIES_PROPERTY, previousEntries);
    restore(PageCache.LEGACY_BYTES_PROPERTY, previousBytes);
  }

  private static void restore(final String property, final String value) {
    if (value == null) {
      System.clearProperty(property);
    } else {
      System.setProperty(property, value);
    }
  }

  @Test
  void legacyBytesSetAnEffectiveCountAndExplicitEntriesTakePrecedence() {
    System.setProperty(PageCache.LEGACY_BYTES_PROPERTY, "3000");
    assertEquals(3, new PageCache(50_000_000L).maxEntries());
    System.setProperty(PageCache.MAX_ENTRIES_PROPERTY, "2");
    final PageCache cache = new PageCache(50_000_000L);
    assertEquals(2, cache.maxEntries());
    for (int i = 0; i < 12; i++) {
      cache.put(new PageReference().setKey(i), new IndirectPage());
    }
    cache.cleanUp();
    assertEquals(2, cache.asMap().size());
    assertEquals(10, cache.getStatistics().evictionCount());
  }

  @Test
  void constructorEstimateUsesTheSameUnits() {
    assertThrows(IllegalArgumentException.class, () -> new PageCache(0L));
    assertThrows(IllegalArgumentException.class, () -> new PageCache(-1L));
    assertEquals(5, new PageCache(5_999L).maxEntries());
    assertEquals(1, new PageCache(1L).maxEntries());
    assertEquals(50_000, new PageCache(50_000_000L).maxEntries());
  }

  @Test
  void invalidConstructorArgumentsAreRejectedEvenWithOverrides() {
    System.setProperty(PageCache.MAX_ENTRIES_PROPERTY, "7");
    System.setProperty(PageCache.LEGACY_BYTES_PROPERTY, "3000");
    assertThrows(IllegalArgumentException.class, () -> new PageCache(0));
    assertThrows(IllegalArgumentException.class, () -> new PageCache(-1));
    assertThrows(IllegalArgumentException.class, () -> new PageCache(0L));
    assertThrows(IllegalArgumentException.class, () -> new PageCache(-1L));
  }

  @Test
  void malformedAndOverflowingCapsDoNotFallBack() {
    System.setProperty(PageCache.LEGACY_BYTES_PROPERTY, "8589934592G");
    assertThrows(IllegalArgumentException.class, () -> new PageCache(50_000_000L));
    System.clearProperty(PageCache.LEGACY_BYTES_PROPERTY);
    System.setProperty(PageCache.MAX_ENTRIES_PROPERTY, "2147483648");
    assertThrows(IllegalArgumentException.class, () -> new PageCache(50_000_000L));
    System.setProperty(PageCache.MAX_ENTRIES_PROPERTY, "0");
    assertThrows(IllegalArgumentException.class, () -> new PageCache(50_000_000L));
  }

  @Test
  void legacyEntryBoundaryIsCheckedWithoutWrapping() {
    System.setProperty(PageCache.LEGACY_BYTES_PROPERTY, "2147483647999");
    assertEquals(Integer.MAX_VALUE, new PageCache(50_000_000L).maxEntries());
    System.setProperty(PageCache.LEGACY_BYTES_PROPERTY, "2147483648000");
    final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
        () -> new PageCache(50_000_000L));
    assertTrue(failure.getMessage().contains(PageCache.LEGACY_BYTES_PROPERTY));
    assertTrue(failure.getMessage().contains("2147483648000"));
    System.setProperty(PageCache.MAX_ENTRIES_PROPERTY, "2147483647");
    assertEquals(Integer.MAX_VALUE, new PageCache(50_000_000L).maxEntries(),
        "a valid explicit entry cap takes precedence over an unrepresentable legacy estimate");
  }

  @Test
  void invalidEntryOverrideNeverFallsBackToLegacyBytes() {
    System.setProperty(PageCache.LEGACY_BYTES_PROPERTY, "3000");
    for (final String value : new String[] {"", "typo", "0", "-1", "2147483648"}) {
      System.setProperty(PageCache.MAX_ENTRIES_PROPERTY, value);
      final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
          () -> new PageCache(50_000_000L));
      assertTrue(failure.getMessage().contains(PageCache.MAX_ENTRIES_PROPERTY));
      assertTrue(failure.getMessage().contains("=" + value));
    }
  }

  @Test
  void creationLogReportsTheEffectiveEntries() {
    final Logger logger = (Logger) LoggerFactory.getLogger(PageCache.class);
    final ListAppender<ILoggingEvent> events = new ListAppender<>();
    events.start();
    logger.addAppender(events);
    try {
      System.setProperty(PageCache.MAX_ENTRIES_PROPERTY, "7");
      assertEquals(7, new PageCache(50_000_000L).maxEntries());
      assertTrue(events.list.stream().anyMatch(event ->
          event.getFormattedMessage().equals("PageCache created with maxEntries: 7")));
    } finally {
      logger.detachAppender(events);
      events.stop();
    }
  }
}
