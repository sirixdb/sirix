/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The segment verdict cache hands out the table ITSELF and bounds what it keeps by bytes.
 */
final class SegmentVerdictCacheTest {

  private static GlobalVerdictCacheKey key(final int segmentHeader) {
    return new GlobalVerdictCacheKey(1L, 2L, 3, segmentHeader, 100, "STR_CONTAINS", "676f6f676c65");
  }

  @Test
  @DisplayName("a table comes back as the very array that was put — sharing is the point")
  void handsOutTheSameArray() {
    final SegmentVerdictCache cache = new SegmentVerdictCache(1L << 20);
    final byte[] table = new byte[101];
    cache.put(key(7), table);
    assertSame(table, cache.get(key(7)), "the settled entries of one query must land where the next one reads");
    assertSame(table, cache.get(key(7)));
    assertNull(cache.get(key(8)), "another segment's dictionary is another table");
    cache.remove(key(7));
    assertNull(cache.get(key(7)));
  }

  @Test
  @DisplayName("the key tells revisions, dictionaries, entry counts, ops and literals apart")
  void keysDiffer() {
    final SegmentVerdictCache cache = new SegmentVerdictCache(1L << 20);
    final GlobalVerdictCacheKey base = key(7);
    cache.put(base, new byte[101]);
    assertNull(cache.get(new GlobalVerdictCacheKey(1L, 2L, 4, 7L, 100, "STR_CONTAINS", "676f6f676c65")),
        "a later revision may have rewritten the header under the same key");
    assertNull(cache.get(new GlobalVerdictCacheKey(1L, 2L, 3, 7L, 101, "STR_CONTAINS", "676f6f676c65")),
        "a dictionary with more entries is a different table");
    assertNull(cache.get(new GlobalVerdictCacheKey(1L, 2L, 3, 7L, 100, "STR_STARTS_WITH", "676f6f676c65")));
    assertNull(cache.get(new GlobalVerdictCacheKey(1L, 2L, 3, 7L, 100, "STR_CONTAINS", "676f6f676c66")));
    assertNull(cache.get(new GlobalVerdictCacheKey(1L, 9L, 3, 7L, 100, "STR_CONTAINS", "676f6f676c65")));
  }

  @Test
  @DisplayName("retention is bounded by table BYTES, not by a count")
  void evictsByWeight() throws Exception {
    final SegmentVerdictCache cache = new SegmentVerdictCache(1000L);
    for (int segment = 0; segment < 50; segment++) {
      cache.put(key(segment), new byte[100]);
    }
    // Caffeine evicts on its maintenance schedule, so the bound is met eventually, not on return.
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    long retained;
    do {
      retained = 0L;
      for (final byte[] table : cache.asMap().values()) {
        retained += table.length;
      }
      if (retained <= 1000L) {
        break;
      }
      Thread.sleep(10);
    } while (System.nanoTime() < deadline);
    assertTrue(retained <= 1000L, "5000 bytes were put against a 1000-byte budget; " + retained + " are retained");
    assertTrue(retained > 0L, "the budget keeps what fits");
  }

  @Test
  @DisplayName("compute under the bin lock is refused, as it is for the global verdicts")
  void refusesComputeUnderTheBinLock() {
    final SegmentVerdictCache cache = new SegmentVerdictCache(1L << 20);
    assertThrows(UnsupportedOperationException.class, () -> cache.get(key(1), (k, v) -> new byte[1]));
  }

  @Test
  @DisplayName("a budget that is not positive is refused")
  void rejectsANonPositiveBudget() {
    assertThrows(IllegalArgumentException.class, () -> new SegmentVerdictCache(0L));
    assertThrows(IllegalArgumentException.class, () -> new SegmentVerdictCache(-1L));
  }

  @Test
  @DisplayName("clear drops every table")
  void clearDropsEverything() {
    final SegmentVerdictCache cache = new SegmentVerdictCache(1L << 20);
    cache.put(key(1), new byte[10]);
    cache.put(key(2), new byte[10]);
    cache.clear();
    assertEquals(0, cache.asMap().size());
  }
}
