package io.sirix.query.compiler.optimizer;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link PlanCache} — LRU cache for optimized query plans.
 */
final class PlanCacheTest {

  private PlanCache cache;

  @BeforeEach
  void setUp() {
    cache = new PlanCache(4); // small capacity for testing LRU eviction
  }

  @Test
  @DisplayName("Cache miss returns null and increments miss counter")
  void cacheMissReturnsNull() {
    assertNull(cache.get("SELECT * FROM t"));
    assertEquals(0, cache.hits());
    assertEquals(1, cache.misses());
  }

  @Test
  @DisplayName("Put and get returns cached AST")
  void putAndGet() {
    final AST ast = new AST(XQ.FlowrExpr, null);
    cache.put("query1", ast);

    assertSame(ast, cache.get("query1"));
    assertEquals(1, cache.hits());
    assertEquals(0, cache.misses());
  }

  @Test
  @DisplayName("Cache size reflects number of entries")
  void cacheSize() {
    assertEquals(0, cache.size());
    cache.put("q1", new AST(XQ.FlowrExpr, null));
    assertEquals(1, cache.size());
    cache.put("q2", new AST(XQ.ForBind, null));
    assertEquals(2, cache.size());
  }

  @Test
  @DisplayName("LRU eviction removes least-recently-used entry")
  void lruEviction() {
    cache.put("q1", new AST(XQ.FlowrExpr, null));
    cache.put("q2", new AST(XQ.ForBind, null));
    cache.put("q3", new AST(XQ.LetBind, null));
    cache.put("q4", new AST(XQ.Selection, null));

    // Access q1 to make it recently used
    cache.get("q1");

    // Insert q5 — should evict q2 (least recently used)
    cache.put("q5", new AST(XQ.Start, null));

    assertNotNull(cache.get("q1"), "q1 should survive (recently accessed)");
    assertNull(cache.get("q2"), "q2 should be evicted (LRU)");
    assertNotNull(cache.get("q3"), "q3 should survive");
    assertNotNull(cache.get("q4"), "q4 should survive");
    assertNotNull(cache.get("q5"), "q5 should be present");
  }

  @Test
  @DisplayName("Invalidate removes specific entry")
  void invalidate() {
    cache.put("q1", new AST(XQ.FlowrExpr, null));
    cache.invalidate("q1");

    assertNull(cache.get("q1"));
    assertEquals(0, cache.size());
  }

  @Test
  @DisplayName("Clear removes all entries and resets counters")
  void clear() {
    cache.put("q1", new AST(XQ.FlowrExpr, null));
    cache.put("q2", new AST(XQ.ForBind, null));
    cache.get("q1");
    cache.get("nonexistent");

    cache.clear();

    assertEquals(0, cache.size());
    assertEquals(0, cache.hits());
    assertEquals(0, cache.misses());
  }

  @Test
  @DisplayName("Hit ratio computed correctly")
  void hitRatio() {
    assertEquals(0.0, cache.hitRatio()); // no queries yet

    cache.put("q1", new AST(XQ.FlowrExpr, null));
    cache.get("q1"); // hit
    cache.get("q1"); // hit
    cache.get("q2"); // miss

    assertEquals(2.0 / 3.0, cache.hitRatio(), 0.001);
  }

  @Test
  @DisplayName("Put with null key or value is no-op")
  void putNullNoOp() {
    cache.put(null, new AST(XQ.FlowrExpr, null));
    cache.put("q1", null);
    assertEquals(0, cache.size());
  }

  @Test
  @DisplayName("Constructor with non-positive maxSize throws")
  void invalidMaxSizeThrows() {
    assertThrows(IllegalArgumentException.class, () -> new PlanCache(0));
    assertThrows(IllegalArgumentException.class, () -> new PlanCache(-1));
  }

  @Test
  @DisplayName("Default constructor uses DEFAULT_MAX_SIZE")
  void defaultConstructor() {
    final PlanCache defaultCache = new PlanCache();
    // Should be able to add many entries without eviction
    for (int i = 0; i < PlanCache.DEFAULT_MAX_SIZE; i++) {
      defaultCache.put("q" + i, new AST(XQ.FlowrExpr, null));
    }
    assertEquals(PlanCache.DEFAULT_MAX_SIZE, defaultCache.size());
  }

  @Test
  @DisplayName("Concurrent access does not corrupt cache")
  void concurrentAccess() throws Exception {
    final PlanCache concurrentCache = new PlanCache(64);
    final int threadCount = 8;
    final int opsPerThread = 500;
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch doneLatch = new CountDownLatch(threadCount);
    final AtomicInteger errors = new AtomicInteger(0);

    for (int t = 0; t < threadCount; t++) {
      final int threadId = t;
      new Thread(() -> {
        try {
          startLatch.await();
          for (int i = 0; i < opsPerThread; i++) {
            final String key = "q" + (threadId * opsPerThread + i) % 100;
            if (i % 3 == 0) {
              concurrentCache.put(key, new AST(XQ.FlowrExpr, null));
            } else if (i % 3 == 1) {
              concurrentCache.get(key); // may be null (miss)
            } else {
              concurrentCache.invalidate(key);
            }
          }
        } catch (Exception e) {
          errors.incrementAndGet();
        } finally {
          doneLatch.countDown();
        }
      }).start();
    }

    startLatch.countDown();
    doneLatch.await();

    assertEquals(0, errors.get(), "No errors should occur during concurrent access");
    assertTrue(concurrentCache.size() >= 0, "Size should be non-negative");
    assertTrue(concurrentCache.hits() + concurrentCache.misses() > 0,
        "Some operations should have been recorded");
  }

  @Test
  @DisplayName("Cached AST deep copy prevents mutation corruption")
  void deepCopyPreventsMutationCorruption() {
    // Build an AST with properties and children
    final AST original = new AST(XQ.FlowrExpr, null);
    final AST child = new AST(XQ.ForBind, null);
    child.setProperty("costBased.preferIndex", true);
    original.addChild(child);

    // Cache a deep copy (simulating what SirixOptimizer does)
    cache.put("q1", original.copyTree());

    // Mutate the original (simulating 10-stage pipeline)
    original.getChild(0).setProperty("costBased.preferIndex", false);
    original.addChild(new AST(XQ.LetBind, null));

    // Retrieve from cache — should be unaffected by mutations
    final AST cached = cache.get("q1");
    assertNotNull(cached);
    assertEquals(1, cached.getChildCount(),
        "Cached AST should still have 1 child (not 2)");
    assertEquals(true, cached.getChild(0).getProperty("costBased.preferIndex"),
        "Cached property should be true (not mutated to false)");
  }
}
