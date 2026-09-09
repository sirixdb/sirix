/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.brackit.query.atomic.QNm;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.BufferManager;
import io.sirix.cache.Cache;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Regression for eviction racing a posting delete's read-only presence preflight. */
final class HOTPostingDeleteEvictionTest {

  private static final String RESOURCE = "posting-delete-eviction";
  private static final int NAME_INDEX_NUMBER = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON).getID();
  private static final int ENTRY_COUNT = 20_000;

  @TempDir
  Path temporaryDirectory;

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  void everyExistingPostingIsDeletedUnderConcurrentPoisoningEviction() throws Exception {
    final Path databasePath = temporaryDirectory.resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    final AtomicBoolean stopEvictor = new AtomicBoolean();
    Thread evictor = null;
    try {
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
      }

      final int baseRevision = insertBaseRevision(databasePath);
      final int deletedRevision;
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
          JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        final HOTIndexWriter<QNm> writer = HOTIndexWriter.create(wtx.getStorageEngineWriter(),
            NameKeySerializer.INSTANCE, IndexType.NAME, NAME_INDEX_NUMBER);
        final BufferManager bufferManager = wtx.getStorageEngineWriter().getBufferManager();

        FrameSlotAllocator.setPoisonOnReleaseForTesting(true);
        final Thread hammer = new Thread(() -> {
          while (!stopEvictor.get()) {
            evictUnguardedLeaves(bufferManager.getHOTLeafPageCache());
            bufferManager.getPageCache().clear();
          }
        }, "posting-delete-evict-hammer");
        hammer.setDaemon(true);
        evictor = hammer;
        hammer.start();

        try {
          for (int i = 0; i < ENTRY_COUNT; i++) {
            assertTrue(writer.remove(key(i), i), "eviction must not turn existing posting " + i + " into a miss");
          }
          assertFalse(writer.remove(new QNm("absent-under-eviction"), 1L));
        } finally {
          stopEvictor.set(true);
          hammer.join(TimeUnit.SECONDS.toMillis(10L));
        }
        wtx.commit();
        deletedRevision = session.getMostRecentRevisionNumber();
      }

      assertHistoricalAndDeletedRevisions(databasePath, baseRevision, deletedRevision);
    } finally {
      stopEvictor.set(true);
      if (evictor != null) {
        evictor.join(TimeUnit.SECONDS.toMillis(10L));
      }
      FrameSlotAllocator.setPoisonOnReleaseForTesting(false);
      Databases.getGlobalBufferManager().clearAllCaches();
      Databases.removeDatabase(databasePath);
    }
  }

  private static int insertBaseRevision(final Path databasePath) throws IOException {
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final HOTIndexWriter<QNm> writer = HOTIndexWriter.create(wtx.getStorageEngineWriter(), NameKeySerializer.INSTANCE,
          IndexType.NAME, NAME_INDEX_NUMBER);
      for (int i = 0; i < ENTRY_COUNT; i++) {
        writer.indexNodeKey(key(i), i);
      }
      wtx.commit();
      return session.getMostRecentRevisionNumber();
    }
  }

  private static void assertHistoricalAndDeletedRevisions(final Path databasePath, final int baseRevision,
      final int deletedRevision) throws IOException {
    Databases.getGlobalBufferManager().clearAllCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeReadOnlyTrx baseRtx = session.beginNodeReadOnlyTrx(baseRevision);
        JsonNodeReadOnlyTrx deletedRtx = session.beginNodeReadOnlyTrx(deletedRevision)) {
      final HOTIndexReader<QNm> baseReader = HOTIndexReader.create(baseRtx.getStorageEngineReader(),
          NameKeySerializer.INSTANCE, IndexType.NAME, NAME_INDEX_NUMBER);
      final HOTIndexReader<QNm> deletedReader = HOTIndexReader.create(deletedRtx.getStorageEngineReader(),
          NameKeySerializer.INSTANCE, IndexType.NAME, NAME_INDEX_NUMBER);
      for (int i = 0; i < ENTRY_COUNT; i++) {
        final NodeReferences historical = baseReader.get(key(i), SearchMode.EQUAL);
        assertNotNull(historical, "base revision lost posting " + i);
        assertTrue(historical.contains(i), "base revision posting has wrong node key " + i);
        assertNull(deletedReader.get(key(i), SearchMode.EQUAL), "delete revision retained posting " + i);
      }
    }
  }

  private static QNm key(final int index) {
    return new QNm("eviction-key-" + index);
  }

  /** Maximum-rate, guard-respecting eviction equivalent to the cache's pressure sweep. */
  private static void evictUnguardedLeaves(final Cache<PageReference, HOTLeafPage> cache) {
    final var entries = new ArrayList<>(cache.asMap().entrySet());
    for (final var entry : entries) {
      final HOTLeafPage leaf = entry.getValue();
      if (leaf == null || leaf.isClosed() || leaf.getGuardCount() != 0) {
        continue;
      }
      if (leaf.isHot()) {
        leaf.clearHot();
        continue;
      }
      cache.remove(entry.getKey());
      entry.getKey().setPage(null);
      if (!leaf.isClosed() && leaf.getGuardCount() == 0 && !leaf.isHot()) {
        leaf.close();
      }
    }
  }
}
