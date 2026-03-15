package io.sirix.query.compiler.optimizer.stats;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link StatisticsCatalog} — thread-safe in-memory histogram catalog.
 */
final class StatisticsCatalogTest {

  private StatisticsCatalog catalog;

  @BeforeEach
  void setUp() {
    catalog = StatisticsCatalog.getInstance();
    catalog.clear();
  }

  @Test
  @DisplayName("Put and get histogram by (db, resource, path)")
  void putAndGet() {
    final Histogram hist = buildSimpleHistogram(100);
    catalog.put("db1", "res1", "price", hist);

    assertSame(hist, catalog.get("db1", "res1", "price"));
    assertEquals(1, catalog.size());
  }

  @Test
  @DisplayName("Get returns null for missing entry")
  void getMissingReturnsNull() {
    assertNull(catalog.get("db1", "res1", "nonexistent"));
  }

  @Test
  @DisplayName("Get with null arguments returns null")
  void getNullArgumentsReturnsNull() {
    assertNull(catalog.get(null, "res1", "price"));
    assertNull(catalog.get("db1", null, "price"));
    assertNull(catalog.get("db1", "res1", null));
  }

  @Test
  @DisplayName("Put overwrites existing entry")
  void putOverwrites() {
    final Histogram hist1 = buildSimpleHistogram(100);
    final Histogram hist2 = buildSimpleHistogram(200);
    catalog.put("db1", "res1", "price", hist1);
    catalog.put("db1", "res1", "price", hist2);

    assertSame(hist2, catalog.get("db1", "res1", "price"));
    assertEquals(1, catalog.size());
  }

  @Test
  @DisplayName("Different paths are stored independently")
  void differentPathsIndependent() {
    final Histogram hist1 = buildSimpleHistogram(100);
    final Histogram hist2 = buildSimpleHistogram(200);
    catalog.put("db1", "res1", "price", hist1);
    catalog.put("db1", "res1", "category", hist2);

    assertSame(hist1, catalog.get("db1", "res1", "price"));
    assertSame(hist2, catalog.get("db1", "res1", "category"));
    assertEquals(2, catalog.size());
  }

  @Test
  @DisplayName("Remove deletes specific entry")
  void remove() {
    final Histogram hist = buildSimpleHistogram(100);
    catalog.put("db1", "res1", "price", hist);
    final Histogram removed = catalog.remove("db1", "res1", "price");

    assertSame(hist, removed);
    assertNull(catalog.get("db1", "res1", "price"));
    assertEquals(0, catalog.size());
  }

  @Test
  @DisplayName("Remove missing entry returns null")
  void removeMissingReturnsNull() {
    assertNull(catalog.remove("db1", "res1", "nonexistent"));
  }

  @Test
  @DisplayName("Invalidate removes all entries for a db/resource pair")
  void invalidate() {
    catalog.put("db1", "res1", "price", buildSimpleHistogram(100));
    catalog.put("db1", "res1", "name", buildSimpleHistogram(200));
    catalog.put("db1", "res2", "age", buildSimpleHistogram(300));
    catalog.put("db2", "res1", "price", buildSimpleHistogram(400));

    catalog.invalidate("db1", "res1");

    assertNull(catalog.get("db1", "res1", "price"));
    assertNull(catalog.get("db1", "res1", "name"));
    assertNotNull(catalog.get("db1", "res2", "age"));
    assertNotNull(catalog.get("db2", "res1", "price"));
    assertEquals(2, catalog.size());
  }

  @Test
  @DisplayName("Clear removes all entries")
  void clear() {
    catalog.put("db1", "res1", "price", buildSimpleHistogram(100));
    catalog.put("db2", "res2", "name", buildSimpleHistogram(200));

    catalog.clear();

    assertEquals(0, catalog.size());
    assertNull(catalog.get("db1", "res1", "price"));
  }

  @Test
  @DisplayName("Put with null arguments throws NullPointerException")
  void putNullThrows() {
    final Histogram hist = buildSimpleHistogram(100);
    assertThrows(NullPointerException.class, () -> catalog.put(null, "res1", "price", hist));
    assertThrows(NullPointerException.class, () -> catalog.put("db1", null, "price", hist));
    assertThrows(NullPointerException.class, () -> catalog.put("db1", "res1", null, hist));
    assertThrows(NullPointerException.class, () -> catalog.put("db1", "res1", "price", null));
  }

  @Test
  @DisplayName("Singleton instance is shared")
  void singletonShared() {
    assertSame(StatisticsCatalog.getInstance(), StatisticsCatalog.getInstance());
  }

  @Test
  @DisplayName("Catalog evicts LRU entries when exceeding MAX_ENTRIES")
  void evictsWhenExceedingMaxEntries() {
    // Fill to capacity
    for (int i = 0; i < StatisticsCatalog.MAX_ENTRIES; i++) {
      catalog.put("db" + i, "res", "path", buildSimpleHistogram(10));
    }
    assertEquals(StatisticsCatalog.MAX_ENTRIES, catalog.size());

    // Add one more — LRU eviction keeps size at MAX_ENTRIES
    catalog.put("dbOverflow", "res", "path", buildSimpleHistogram(10));

    assertEquals(StatisticsCatalog.MAX_ENTRIES, catalog.size(),
        "Size should stay at MAX after LRU eviction, got " + catalog.size());
    assertNotNull(catalog.get("dbOverflow", "res", "path"),
        "Newly added entry should be present after eviction");
    // Oldest entry (db0) should have been evicted
    assertNull(catalog.get("db0", "res", "path"),
        "Oldest entry should be evicted by LRU");
  }

  private static Histogram buildSimpleHistogram(int count) {
    final var builder = new Histogram.Builder(10);
    for (int i = 0; i < count; i++) {
      builder.addValue(i);
    }
    builder.setDistinctCount(count);
    return builder.build();
  }
}
