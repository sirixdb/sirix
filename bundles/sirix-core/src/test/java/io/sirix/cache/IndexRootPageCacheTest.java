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
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class IndexRootPageCacheTest {
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
