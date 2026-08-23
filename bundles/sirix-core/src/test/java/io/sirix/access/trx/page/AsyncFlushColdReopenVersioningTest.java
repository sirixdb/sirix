/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.InternalNodeTrx;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.io.StorageType;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import io.sirix.node.RevisionReferencesNode;
import io.sirix.settings.Constants;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Multi-epoch async-flush integrity coverage across every versioning strategy. */
final class AsyncFlushColdReopenVersioningTest {

  private static final String RESOURCE = "async-flush-cold-reopen-versioning";
  private static final String REFERENCE_RESOURCE = "async-flush-cold-reopen-reference";
  private static final int AUTO_FLUSH_THRESHOLD = 128;
  private static final int INSERTED_RECORDS = 4_000;
  private static final int HASHED_RECORDS = 512;

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

  @ParameterizedTest(name = "{0} multi-epoch async import survives a cold reopen")
  @EnumSource(VersioningType.class)
  @Timeout(value = 3, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void multiEpochImportSurvivesColdReopen(final VersioningType versioningType) {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .storeDiffs(false)
                                                   .hashKind(HashType.NONE)
                                                   .buildPathSummary(false)
                                                   .versioningApproach(versioningType)
                                                   .maxNumberOfRevisionsToRestore(3)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx(AUTO_FLUSH_THRESHOLD, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
        final long arrayNodeKey = wtx.insertArrayAsFirstChild().getNodeKey();
        for (int i = 0; i < INSERTED_RECORDS; i++) {
          wtx.moveTo(arrayNodeKey);
          wtx.insertStringValueAsFirstChild("spill-value-" + i);
        }
        wtx.commit();
      }
    }

    // Database close intentionally keeps global caches warm. Clear every process-local page and
    // revision metadata cache so the second open must reconstruct the tree from file offsets.
    NodeStorageEngineWriter.asyncFlushFaultHook = null;
    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final JsonResourceSession session = database.beginResourceSession(RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      assertTrue(rtx.moveToFirstChild());
      assertEquals(INSERTED_RECORDS, rtx.getChildCount());
      assertTrue(rtx.moveToFirstChild());
      // insertStringValueAsFirstChild reverses insertion order. Read every KVL after clearing all
      // process-local caches: parent metadata alone can report the right child count even when a
      // structural spill persisted an unreadable or stale record-page path.
      for (int expected = INSERTED_RECORDS - 1; expected >= 0; expected--) {
        assertEquals("spill-value-" + expected, rtx.getValue(),
            "value at array position " + (INSERTED_RECORDS - 1 - expected));
        if (expected > 0) {
          assertTrue(rtx.moveToRightSibling(), "missing array value " + (expected - 1));
        }
      }
      assertFalse(rtx.moveToRightSibling(), "unexpected value after the expected array tail");
    }
  }

  @ParameterizedTest(name = "{0} flushed mutations remain visible to their writer")
  @EnumSource(VersioningType.class)
  @Timeout(value = 2, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void flushedMutationsRemainVisibleAndSurviveColdReopen(final VersioningType versioningType) {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    final long updatedNodeKey;
    final long deletedNodeKey;
    final long insertedNodeKey;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      createVersionedResource(database, versioningType);
      final long arrayNodeKey;
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx()) {
        arrayNodeKey = wtx.insertArrayAsFirstChild().getNodeKey();
        updatedNodeKey = wtx.insertStringValueAsFirstChild("before-update").getNodeKey();
        assertTrue(wtx.moveTo(arrayNodeKey));
        deletedNodeKey = wtx.insertStringValueAsFirstChild("delete-me").getNodeKey();
        wtx.commit();
      }

      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx(Integer.MAX_VALUE, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
        assertTrue(wtx.moveTo(updatedNodeKey));
        wtx.setStringValue("after-update");
        assertTrue(wtx.moveTo(deletedNodeKey));
        wtx.remove();
        assertTrue(wtx.moveTo(arrayNodeKey));
        insertedNodeKey = wtx.insertStringValueAsFirstChild("inserted").getNodeKey();

        final var writer = wtx.getStorageEngineWriter();
        writer.asyncFlush();
        writer.awaitPendingAsyncFlush();
        rotatePastFlushedDocumentPage(writer);

        assertTrue(wtx.moveTo(insertedNodeKey));
        assertEquals("inserted", wtx.getValue());
        assertTrue(wtx.moveTo(updatedNodeKey));
        assertEquals("after-update", wtx.getValue());
        wtx.setStringValue("after-boundary-update");
        assertFalse(wtx.moveTo(deletedNodeKey));
        wtx.commit();
      }
    }

    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final JsonResourceSession session = database.beginResourceSession(RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      assertTrue(rtx.moveTo(insertedNodeKey));
      assertEquals("inserted", rtx.getValue());
      assertTrue(rtx.moveTo(updatedNodeKey));
      assertEquals("after-boundary-update", rtx.getValue());
      assertFalse(rtx.moveTo(deletedNodeKey));
    }
  }

  @ParameterizedTest(name = "{0} inserted-node binds release exact durable cursor pages")
  @EnumSource(VersioningType.class)
  @Timeout(value = 3, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void insertedNodeBindsReleaseExactDurableCursorPages(final VersioningType versioningType) {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    final long[] durablePageNodeKeys = new long[3];
    final long[] insertedNodeKeys = new long[3];
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      createVersionedResource(database, versioningType);
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx()) {
        ((InternalNodeTrx<?>) wtx).setBulkInsertion(true);
        final long arrayNodeKey = wtx.insertArrayAsFirstChild().getNodeKey();
        for (int i = 0; i < (4 * Constants.NDP_NODE_COUNT); i++) {
          assertTrue(wtx.moveTo(arrayNodeKey));
          final long nodeKey = wtx.insertStringValueAsFirstChild("fixture-" + i).getNodeKey();
          final long pageKey = nodeKey >> Constants.NDP_NODE_COUNT_EXPONENT;
          final int slotOffset = (int) (nodeKey & (Constants.NDP_NODE_COUNT - 1));
          if (pageKey >= 1 && pageKey <= durablePageNodeKeys.length && slotOffset == Constants.NDP_NODE_COUNT / 2) {
            durablePageNodeKeys[(int) pageKey - 1] = nodeKey;
          }
        }
        wtx.commit();
      }

      for (final long nodeKey : durablePageNodeKeys) {
        assertTrue(nodeKey > 0);
      }

      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx(Integer.MAX_VALUE, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
        for (int i = 0; i < durablePageNodeKeys.length; i++) {
          assertTrue(wtx.moveTo(durablePageNodeKeys[i]));
          wtx.setStringValue("durable-" + i);
        }
        final StorageEngineWriter writer = wtx.getStorageEngineWriter();
        writer.asyncFlush();
        writer.awaitPendingAsyncFlush();
        rotatePastFlushedDocumentPage(writer);

        for (int i = 0; i < durablePageNodeKeys.length; i++) {
          assertTrue(wtx.moveTo(durablePageNodeKeys[i]));
          insertedNodeKeys[i] = wtx.insertStringValueAsRightSibling("inserted-" + i).getNodeKey();
        }
        wtx.commit();
      }
    }

    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final JsonResourceSession session = database.beginResourceSession(RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      for (int i = 0; i < durablePageNodeKeys.length; i++) {
        assertTrue(rtx.moveTo(durablePageNodeKeys[i]));
        assertEquals("durable-" + i, rtx.getValue());
        assertTrue(rtx.moveTo(insertedNodeKeys[i]));
        assertEquals("inserted-" + i, rtx.getValue());
      }
    }
  }

  @ParameterizedTest(name = "{0} rollback invalidates reclaimable async-page offsets")
  @EnumSource(VersioningType.class)
  @Timeout(value = 2, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void rollbackInvalidatesReclaimableAsyncPageOffsets(final VersioningType versioningType) {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    final long valueNodeKey;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      createVersionedResource(database, versioningType);
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertArrayAsFirstChild();
        valueNodeKey = wtx.insertStringValueAsFirstChild("base").getNodeKey();
        wtx.commit();
      }

      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx(Integer.MAX_VALUE, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
        assertTrue(wtx.moveTo(valueNodeKey));
        wtx.setStringValue("aborted");
        final var writer = wtx.getStorageEngineWriter();
        writer.asyncFlush();
        writer.awaitPendingAsyncFlush();
        rotatePastFlushedDocumentPage(writer);
        assertTrue(wtx.moveTo(valueNodeKey));
        assertEquals("aborted", wtx.getValue());

        final NodeStorageEngineReader reader = (NodeStorageEngineReader) writer.getStorageEngineReader();
        final PageReference abortedReference = currentDocumentPageReference(writer, valueNodeKey);
        final long abortedOffset = abortedReference.getKey();
        cachePageFragments(reader, abortedReference);
        final PageReference cacheKey = new PageReference().setKey(abortedOffset)
                                                          .setDatabaseId(reader.getDatabaseId())
                                                          .setResourceId(reader.getResourceId());
        final var fragmentCache = versioningType == VersioningType.FULL
            ? reader.getBufferManager().getRecordPageCache()
            : reader.getBufferManager().getRecordPageFragmentCache();
        assertNotNull(fragmentCache.get(cacheKey));

        wtx.rollback();
        assertNull(fragmentCache.get(cacheKey));
        assertTrue(wtx.moveTo(valueNodeKey));
        wtx.setStringValue("successor");
        final StorageEngineWriter successorWriter = wtx.getStorageEngineWriter();
        successorWriter.asyncFlush();
        successorWriter.awaitPendingAsyncFlush();
        final PageReference successorReference = currentDocumentPageReference(successorWriter, valueNodeKey);
        assertEquals(abortedOffset, successorReference.getKey());
        wtx.commit();

        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          assertTrue(rtx.moveTo(valueNodeKey));
          assertEquals("successor", rtx.getValue());
        }
      }
    }

    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final JsonResourceSession session = database.beginResourceSession(RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      assertTrue(rtx.moveTo(valueNodeKey));
      assertEquals("successor", rtx.getValue());
    }
  }

  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void storageOnlyEpochsPreserveIncrementalBulkHashMetadata() {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      createRollingResource(database, RESOURCE);
      createRollingResource(database, REFERENCE_RESOURCE);

      final AtomicInteger storageEpochs = new AtomicInteger();
      NodeStorageEngineWriter.asyncFlushFaultHook = (writer, site) -> {
        if ("prepare".equals(site)) {
          storageEpochs.incrementAndGet();
        }
      };
      try {
        insertHashedArray(database, RESOURCE, AUTO_FLUSH_THRESHOLD, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH);
      } finally {
        NodeStorageEngineWriter.asyncFlushFaultHook = null;
      }
      assertTrue(storageEpochs.get() > 1, "the fixture must cross multiple storage-only epoch boundaries");

      insertHashedArray(database, REFERENCE_RESOURCE, Integer.MAX_VALUE, AfterCommitState.KEEP_OPEN);
    }

    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final JsonResourceSession asyncSession = database.beginResourceSession(RESOURCE);
        final JsonResourceSession referenceSession = database.beginResourceSession(REFERENCE_RESOURCE);
        final JsonNodeReadOnlyTrx asyncRtx = asyncSession.beginNodeReadOnlyTrx();
        final JsonNodeReadOnlyTrx referenceRtx = referenceSession.beginNodeReadOnlyTrx()) {
      assertEquals(HASHED_RECORDS + 1L, asyncRtx.getDescendantCount(),
          "the document-root count must span every storage-only epoch");
      assertEquals(referenceRtx.getDescendantCount(), asyncRtx.getDescendantCount());
      assertEquals(referenceRtx.getHash(), asyncRtx.getHash(),
          "storage-only rotations must preserve the incremental rolling hash");

      assertTrue(asyncRtx.moveToFirstChild());
      assertTrue(referenceRtx.moveToFirstChild());
      assertEquals(HASHED_RECORDS, asyncRtx.getDescendantCount());
      assertEquals(referenceRtx.getDescendantCount(), asyncRtx.getDescendantCount());
      assertEquals(referenceRtx.getHash(), asyncRtx.getHash());
    }
  }

  private static void createRollingResource(final Database<JsonResourceSession> database, final String resourceName) {
    database.createResource(ResourceConfiguration.newBuilder(resourceName)
                                                 .storeDiffs(false)
                                                 .hashKind(HashType.ROLLING)
                                                 .buildPathSummary(false)
                                                 .versioningApproach(VersioningType.FULL)
                                                 .storageType(StorageType.FILE_CHANNEL)
                                                 .build());
  }

  private static void createVersionedResource(final Database<JsonResourceSession> database,
      final VersioningType versioningType) {
    database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                 .storeDiffs(false)
                                                 .hashKind(HashType.NONE)
                                                 .buildPathSummary(false)
                                                 .versioningApproach(versioningType)
                                                 .maxNumberOfRevisionsToRestore(3)
                                                 .storageType(StorageType.FILE_CHANNEL)
                                                 .build());
  }

  private static void rotatePastFlushedDocumentPage(final StorageEngineWriter writer) {
    final long changedNodeKey = writer.getActualRevisionRootPage().getMaxNodeKeyInChangedNodesIndex() + 1;
    writer.createRecord(new RevisionReferencesNode(changedNodeKey, new int[] {1}), IndexType.CHANGED_NODES, -1);
    writer.asyncFlush();
    writer.awaitPendingAsyncFlush();
  }

  private static PageReference currentDocumentPageReference(final StorageEngineWriter writer, final long nodeKey) {
    final NodeStorageEngineReader reader = (NodeStorageEngineReader) writer.getStorageEngineReader();
    final var revisionRoot = writer.getActualRevisionRootPage();
    final PageReference documentRoot = reader.getPageReference(revisionRoot, IndexType.DOCUMENT, -1);
    final PageReference reference = reader.getLeafPageReference(documentRoot,
        nodeKey >> Constants.NDP_NODE_COUNT_EXPONENT, -1, IndexType.DOCUMENT, revisionRoot);
    assertNotNull(reference);
    reference.refreshTransactionLogReference();
    assertTrue(reference.getKey() >= 0);
    return reference;
  }

  private static void cachePageFragments(final NodeStorageEngineReader reader, final PageReference reference) {
    final var fragments = reader.getPageFragments(reference);
    try {
      assertFalse(fragments.pages().isEmpty());
    } finally {
      for (final var page : fragments.pages()) {
        ((KeyValueLeafPage) page).releaseGuard();
      }
    }
  }

  private static void insertHashedArray(final Database<JsonResourceSession> database, final String resourceName,
      final int flushThreshold, final AfterCommitState afterCommitState) {
    try (final JsonResourceSession session = database.beginResourceSession(resourceName);
        final JsonNodeTrx wtx = session.beginNodeTrx(flushThreshold, afterCommitState)) {
      ((InternalNodeTrx<?>) wtx).setBulkInsertion(true);
      final long arrayNodeKey = wtx.insertArrayAsFirstChild().getNodeKey();
      for (int i = 0; i < HASHED_RECORDS; i++) {
        assertTrue(wtx.moveTo(arrayNodeKey));
        wtx.insertStringValueAsFirstChild("hash-value-" + i);
      }
      wtx.commit();
    }
  }
}
