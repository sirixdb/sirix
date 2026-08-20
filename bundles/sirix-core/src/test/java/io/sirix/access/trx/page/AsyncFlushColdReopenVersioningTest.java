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
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Multi-epoch async-flush integrity coverage across every versioning strategy. */
final class AsyncFlushColdReopenVersioningTest {

  private static final String RESOURCE = "async-flush-cold-reopen-versioning";
  private static final int AUTO_FLUSH_THRESHOLD = 128;
  private static final int INSERTED_RECORDS = 4_000;

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
           final JsonNodeTrx wtx = session.beginNodeTrx(AUTO_FLUSH_THRESHOLD,
                                                        AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
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
        assertEquals("spill-value-" + expected, rtx.getValue(), "value at array position "
            + (INSERTED_RECORDS - 1 - expected));
        if (expected > 0) {
          assertTrue(rtx.moveToRightSibling(), "missing array value " + (expected - 1));
        }
      }
      assertFalse(rtx.moveToRightSibling(), "unexpected value after the expected array tail");
    }
  }
}
