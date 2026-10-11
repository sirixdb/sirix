package io.sirix.cache;

import io.sirix.page.CASPage;
import io.sirix.page.ChangeLogIndexPage;
import io.sirix.page.DeweyIDPage;
import io.sirix.page.IndirectPage;
import io.sirix.page.NamePage;
import io.sirix.page.PageReference;
import io.sirix.page.PathPage;
import io.sirix.page.PathSummaryPage;
import io.sirix.page.ProjectionIndexPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.TemporalAggregateIndexPage;
import io.sirix.page.ValidTimeIndexPage;
import io.sirix.page.VectorPage;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class IndexRootPageCacheTest {
  @Test
  @SuppressWarnings("unchecked")
  void pageMissDoesNotLoadValidTimeStore() throws ReflectiveOperationException {
    final AtomicInteger intervalLoads = new AtomicInteger();
    final String cacheClass = PageCache.class.getName();
    final ClassLoader loader = new ClassLoader(PageCache.class.getClassLoader()) {
      @Override
      protected Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
        if (name.startsWith("io.sirix.index.interval.")) {
          intervalLoads.incrementAndGet();
        }
        if (!name.equals(cacheClass) && !name.startsWith(cacheClass + "$")) {
          return super.loadClass(name, resolve);
        }
        synchronized (getClassLoadingLock(name)) {
          Class<?> loaded = findLoadedClass(name);
          if (loaded == null) {
            try (final InputStream input = getResourceAsStream(name.replace('.', '/') + ".class")) {
              if (input == null) {
                throw new ClassNotFoundException(name);
              }
              final byte[] bytes = input.readAllBytes();
              loaded = defineClass(name, bytes, 0, bytes.length);
            } catch (final IOException exception) {
              throw new ClassNotFoundException(name, exception);
            }
          }
          if (resolve) {
            resolveClass(loaded);
          }
          return loaded;
        }
      }
    };
    final Cache<PageReference, Page> cache =
        (Cache<PageReference, Page>) loader.loadClass(cacheClass).getConstructor(int.class).newInstance(32);
    final PageReference key = new PageReference().setKey(100).revisionCacheKey(1);
    final CASPage page = new CASPage();
    assertSame(page, cache.get(key, (_, _) -> page));
    assertSame(page, cache.get(key, (_, _) -> {
      throw new AssertionError("cached root must be reused");
    }));
    assertEquals(0, intervalLoads.get(), "metadata loads must not initialize the valid-time store");
    cache.clear();
  }

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

  @Test
  void everyRevisionOwnedContainerBypassesBothCacheEntryPoints() {
    final PageCache cache = new PageCache(32);
    for (final Page page : List.of(new CASPage(), new ChangeLogIndexPage(), new DeweyIDPage(), new NamePage(),
        new PathPage(), new PathSummaryPage(), new ProjectionIndexPage(), new TemporalAggregateIndexPage(),
        new ValidTimeIndexPage(), new VectorPage(), new RevisionRootPage())) {
      final PageReference reference = new PageReference();
      cache.put(reference, page);
      assertNull(cache.get(reference), page.getClass().getSimpleName());
      assertSame(page, cache.get(reference, (key, previous) -> page));
      assertTrue(cache.asMap().isEmpty(), page.getClass().getSimpleName());
    }
    final PageReference reference = new PageReference();
    final IndirectPage shared = new IndirectPage();
    assertSame(shared, cache.get(reference, (key, previous) -> shared));
    assertSame(shared, cache.get(reference), "ordinary indirect pages must still be cached");
    cache.clear();
  }
}
