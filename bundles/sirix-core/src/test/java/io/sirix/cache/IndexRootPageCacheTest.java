package io.sirix.cache;

import io.sirix.page.CASPage;
import io.sirix.page.PageReference;
import io.sirix.page.ValidTimeIndexPage;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class IndexRootPageCacheTest {
  @Test
  void rootsAreIsolatedByRevisionDatabaseAndResource() {
    final PageCache cache = new PageCache(32);
    final PageReference reference = new PageReference().setKey(100).setDatabaseId(1).setResourceId(2);
    final AtomicInteger loads = new AtomicInteger();
    final CASPage old = (CASPage) cache.get(reference.revisionCacheKey(1), (_, _) -> {
      loads.incrementAndGet();
      return new CASPage();
    });
    final CASPage newer = (CASPage) cache.get(reference.revisionCacheKey(2), (_, _) -> {
      loads.incrementAndGet();
      return new CASPage();
    });
    assertNotSame(old, newer);
    assertSame(old, cache.get(reference.revisionCacheKey(1), (_, _) -> {
      throw new AssertionError("a repeated revision must not read its root again");
    }));
    assertEquals(2, loads.get());
    assertNull(cache.get(reference));
    assertNull(cache.get(new PageReference().setKey(100).setDatabaseId(9).setResourceId(2).revisionCacheKey(1)));
    assertNull(cache.get(new PageReference().setKey(100).setDatabaseId(1).setResourceId(9).revisionCacheKey(1)));
    cache.clear();
    assertNull(cache.get(reference.revisionCacheKey(1)));
    assertNull(cache.get(reference.revisionCacheKey(2)));
  }

  @Test
  void everyAdmissionRouteRefusesUnrevisionedIndexMetadata() {
    final PageCache cache = new PageCache(32);
    final PageReference reference = new PageReference().setKey(100).setDatabaseId(1).setResourceId(2);
    final ValidTimeIndexPage page = new ValidTimeIndexPage();
    cache.put(reference, page);
    assertNull(cache.get(reference));
    cache.putIfAbsent(reference, page);
    assertNull(cache.get(reference));
    cache.putAll(Map.of(reference, page));
    assertNull(cache.get(reference));
    assertSame(page, cache.get(reference, (_, _) -> page));
    assertNull(cache.get(reference));
    cache.put(reference.revisionCacheKey(1), page);
    assertSame(page, cache.get(reference.revisionCacheKey(1)));
    assertThrows(IllegalArgumentException.class, () -> reference.revisionCacheKey(-1));
  }
}
