package io.sirix.access.trx.page;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.PageContainer;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.io.StorageType;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class AsyncFlushLogBoundaryTest {

  private static final String RESOURCE = "async-flush-log-boundary";

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    NodeStorageEngineWriter.asyncFlushFaultHook = null;
  }

  @AfterEach
  void tearDown() {
    NodeStorageEngineWriter.asyncFlushFaultHook = null;
    JsonTestHelper.deleteEverything();
  }

  @Test
  void rotatesAtExactlyOneSnapshotWindow() {
    final int limit = NodeStorageEngineWriter.MAX_ASYNC_FLUSH_LOG_ENTRY_COUNT;

    assertFalse(NodeStorageEngineWriter.isAsyncFlushLogBoundaryReached(0));
    assertFalse(NodeStorageEngineWriter.isAsyncFlushLogBoundaryReached(limit - 1));
    assertTrue(NodeStorageEngineWriter.isAsyncFlushLogBoundaryReached(limit));
    assertTrue(NodeStorageEngineWriter.isAsyncFlushLogBoundaryReached(limit + 1));
    assertTrue(NodeStorageEngineWriter.isAsyncFlushLogBoundaryReached(Integer.MAX_VALUE));
  }

  @Test
  void rejectsNegativeLiveEntryCount() {
    assertThrows(IllegalArgumentException.class,
        () -> NodeStorageEngineWriter.isAsyncFlushLogBoundaryReached(-1));
  }

  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void liveTilBoundaryRotatesBeforeTheNextMutation() {
    final AtomicInteger flushes = new AtomicInteger();
    NodeStorageEngineWriter.asyncFlushFaultHook = (writer, site) -> {
      if ("prepare".equals(site)) {
        flushes.incrementAndGet();
      }
    };

    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .storeDiffs(false)
                                                   .hashKind(HashType.NONE)
                                                   .buildPathSummary(false)
                                                   .versioningApproach(VersioningType.FULL)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .build());

      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
           final JsonNodeTrx wtx = session.beginNodeTrx(Integer.MAX_VALUE,
                                                        AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
        final long arrayNodeKey = wtx.insertArrayAsFirstChild().getNodeKey();
        final NodeStorageEngineWriter writer = (NodeStorageEngineWriter) wtx.getStorageEngineWriter();
        final TransactionIntentLog log = writer.getLog();
        final ResourceConfiguration config = session.getResourceConfig();

        // Populate distinct record-page identities directly so the logical mutation count stays at
        // one. The effective async node cap is 131,072, hence only the live-TIL boundary can rotate
        // before the mutation below. These pages are intentionally unattached test records: the
        // real async serializer writes and retires them, while the document mutation proves the
        // production safe-point predicate initiated that epoch.
        long pageKey = 1_000_000L;
        while (log.liveEntryCount() < NodeStorageEngineWriter.MAX_ASYNC_FLUSH_LOG_ENTRY_COUNT) {
          final KeyValueLeafPage page =
              new KeyValueLeafPage(pageKey++, IndexType.DOCUMENT, config, writer.getRevisionNumber(),
                                   null, null, false);
          final PageReference reference = new PageReference().setDatabaseId(config.getDatabaseId())
                                                             .setResourceId(config.getID());
          log.put(reference, PageContainer.getInstance(page, page));
        }

        assertEquals(0, flushes.get());
        assertEquals(0, log.getCurrentGeneration());
        assertEquals(NodeStorageEngineWriter.MAX_ASYNC_FLUSH_LOG_ENTRY_COUNT, log.liveEntryCount());

        assertTrue(wtx.moveTo(arrayNodeKey));
        wtx.insertStringValueAsFirstChild("trigger");

        assertEquals(1, flushes.get(),
            "the production live-TIL predicate must rotate before the next mutation");
        assertEquals(1, log.getCurrentGeneration());
        wtx.commit();
      }
    }

    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
         final JsonResourceSession session = database.beginResourceSession(RESOURCE);
         final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      assertTrue(rtx.moveToFirstChild());
      assertTrue(rtx.moveToFirstChild());
      assertEquals("trigger", rtx.getValue());
      assertEquals(1, session.getMostRecentRevisionNumber());
    }
  }
}
