/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * HOT page caches must be dropped by the truncate/rollback invalidation paths.
 *
 * <p>HOT leaf FRAGMENTS are cached by their durable offset, which is only a stable identity while
 * the data file is append-only. {@code truncateTo} shortens the file and the next commit REUSES the
 * freed offsets, so a fragment left in the cache under a reused offset would be merged into a live
 * leaf as pre-truncation content — silent wrong data, with no exception to notice. The record-page
 * caches were already cleared by these paths; the HOT caches were not, which is exactly the gap the
 * fragment cache turned into a correctness bug.</p>
 */
public final class HOTFragmentCacheInvalidationTest {

  private static final String RESOURCE_NAME = "hotFragmentCacheResource";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());
    }
  }

  @AfterEach
  void tearDown() throws IOException {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  /** A HOT page planted under a given database/resource id, as the fragment loader would key it. */
  private static PageReference keyFor(final long databaseId, final long resourceId, final long offset) {
    return new PageReference().setKey(offset).setDatabaseId(databaseId).setResourceId(resourceId);
  }

  @Test
  void clearCachesForDatabaseDropsHotLeafAndFragmentEntries() {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
         final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long databaseId;
      final long resourceId;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        databaseId = wtx.getStorageEngineWriter().getDatabaseId();
        resourceId = wtx.getStorageEngineWriter().getResourceId();
      }

      final BufferManager buffers = Databases.getGlobalBufferManager();
      final Cache<PageReference, HOTLeafPage> leafCache = buffers.getHOTLeafPageCache();
      final Cache<PageReference, HOTLeafPage> fragmentCache = buffers.getHOTLeafFragmentCache();

      final PageReference leafKey = keyFor(databaseId, resourceId, 4096L);
      final PageReference fragmentKey = keyFor(databaseId, resourceId, 8192L);
      leafCache.put(leafKey, new HOTLeafPage(1L, 1, IndexType.DOCUMENT));
      fragmentCache.put(fragmentKey, new HOTLeafPage(1L, 1, IndexType.DOCUMENT));
      assertTrue(leafCache.get(leafKey) != null, "precondition: leaf cached");
      assertTrue(fragmentCache.get(fragmentKey) != null, "precondition: fragment cached");

      // The invalidation the truncate/rollback paths run. Truncation reuses the freed offsets, so
      // anything still cached under them would be served as if it were the new content.
      Databases.clearCachesForDatabase(databaseId);

      assertEquals(null, leafCache.get(leafKey),
          "truncate/rollback invalidation must drop cached HOT leaves");
      assertEquals(null, fragmentCache.get(fragmentKey),
          "truncate/rollback invalidation must drop cached HOT fragments — a reused offset would "
              + "otherwise merge pre-truncation bytes into a live leaf");
    }
  }

  /**
   * HOT invalidation must not be conditional on the record/page/revision caches holding anything.
   *
   * <p>A HOT-only resource, or a second clear immediately after a first, leaves those three counters
   * at zero while HOT entries are still cached. Gating the HOT sweep on them (for instance by placing
   * it inside the summary-log guard) silently skips exactly the case the sweep exists for.</p>
   */
  @Test
  void clearCachesForDatabaseDropsHotEntriesEvenWhenNoOtherCacheMatches() {
    final long databaseId = 987_654_321L;
    final long resourceId = 42L;

    final BufferManager buffers = Databases.getGlobalBufferManager();
    final Cache<PageReference, HOTLeafPage> leafCache = buffers.getHOTLeafPageCache();
    final Cache<PageReference, HOTLeafPage> fragmentCache = buffers.getHOTLeafFragmentCache();

    final PageReference leafKey = keyFor(databaseId, resourceId, 4096L);
    final PageReference fragmentKey = keyFor(databaseId, resourceId, 8192L);
    leafCache.put(leafKey, new HOTLeafPage(1L, 1, IndexType.DOCUMENT));
    fragmentCache.put(fragmentKey, new HOTLeafPage(1L, 1, IndexType.DOCUMENT));

    // Nothing was ever cached for this database in the record/fragment/page/revision caches, so all
    // four removal counters stay at zero.
    Databases.clearCachesForDatabase(databaseId);

    assertEquals(null, leafCache.get(leafKey),
        "HOT leaf invalidation must not depend on the record/page/revision caches being non-empty");
    assertEquals(null, fragmentCache.get(fragmentKey),
        "HOT fragment invalidation must not depend on the record/page/revision caches being "
            + "non-empty — a HOT-only resource is precisely the stale-fragment case");
  }

  /** The per-resource sweep must drop that resource's HOT entries and leave its siblings alone. */
  @Test
  void clearCachesForResourceDropsOnlyThatResourcesHotEntries() {
    final long databaseId = 987_654_322L;
    final long targetResourceId = 7L;
    final long siblingResourceId = 8L;

    final BufferManager buffers = Databases.getGlobalBufferManager();
    final Cache<PageReference, HOTLeafPage> leafCache = buffers.getHOTLeafPageCache();
    final Cache<PageReference, HOTLeafPage> fragmentCache = buffers.getHOTLeafFragmentCache();

    final PageReference targetLeafKey = keyFor(databaseId, targetResourceId, 4096L);
    final PageReference targetFragmentKey = keyFor(databaseId, targetResourceId, 8192L);
    final PageReference siblingFragmentKey = keyFor(databaseId, siblingResourceId, 12288L);
    leafCache.put(targetLeafKey, new HOTLeafPage(1L, 1, IndexType.DOCUMENT));
    fragmentCache.put(targetFragmentKey, new HOTLeafPage(1L, 1, IndexType.DOCUMENT));
    fragmentCache.put(siblingFragmentKey, new HOTLeafPage(1L, 1, IndexType.DOCUMENT));

    buffers.clearCachesForResource(databaseId, targetResourceId);

    assertEquals(null, leafCache.get(targetLeafKey),
        "closing a resource must drop its cached HOT leaves");
    assertEquals(null, fragmentCache.get(targetFragmentKey),
        "closing a resource must drop its cached HOT fragments");
    assertTrue(fragmentCache.get(siblingFragmentKey) != null,
        "the per-resource sweep must not evict another resource's HOT fragments");
  }
}
