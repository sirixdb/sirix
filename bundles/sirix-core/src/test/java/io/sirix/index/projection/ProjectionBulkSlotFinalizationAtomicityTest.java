/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixIOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ResourceLock("PROJECTION_BULK_FINALIZE_BEFORE_SPLICE_TEST_HOOK")
final class ProjectionBulkSlotFinalizationAtomicityTest {

  private static final String RESOURCE_NAME = "projection-bulk-finalization-atomicity";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  @BeforeEach
  void setUp() {
    ProjectionIndexHOTStorage.setBulkFinalizeBeforeSpliceTestHook(null);
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());
    }
  }

  @AfterEach
  void tearDown() {
    ProjectionIndexHOTStorage.setBulkFinalizeBeforeSpliceTestHook(null);
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @Test
  void prePublicationFailureDiscardsPendingStateAndRequiresRollback() {
    final IllegalStateException sentinel = new IllegalStateException("injected before bulk root publication");
    final long ownerSlotKey = 17L;

    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage = ProjectionIndexHOTStorage.forBulkBuild(wtx.getStorageEngineWriter(), 0);
      storage.beginBulkSlotAccumulation();
      storage.writeSlotValue(ownerSlotKey, new byte[] {1, 2, 3});
      storage.putSegmentPage(ownerSlotKey, 0, new byte[] {4, 5, 6, 7});
      assertTrue(storage.isBulkAccumulating());
      assertTrue(storage.hasPendingBulkSideAttaches());

      ProjectionIndexHOTStorage.setBulkFinalizeBeforeSpliceTestHook(() -> {
        throw sentinel;
      });
      final IllegalStateException failure =
          assertThrows(IllegalStateException.class, storage::finalizeBulkSlotAccumulation);
      assertSame(sentinel, failure, "the original pre-publication failure must escape unchanged");
      assertFalse(storage.isBulkAccumulating(), "a failed accumulator is irreversibly spent");
      assertFalse(storage.hasPendingBulkSideAttaches(), "deferred side payloads must not survive the failure");
      assertEquals(0, storage.bulkSplicedEntryCount(), "no bulk root was published");

      final SirixIOException nextWriteFailure =
          assertThrows(SirixIOException.class, () -> storage.writeSlotValue(ownerSlotKey + 1, new byte[] {8}));
      assertSame(sentinel, nextWriteFailure.getCause(), "later mutations must report the latched cause");
      final SirixIOException commitFailure = assertThrows(SirixIOException.class, wtx::commit);
      assertSame(sentinel, commitFailure.getCause(), "a discarded accumulator must never be committable");

      ProjectionIndexHOTStorage.setBulkFinalizeBeforeSpliceTestHook(null);
      wtx.rollback();

      final ProjectionIndexHOTStorage recovered = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 0);
      recovered.writeSlotValue(ownerSlotKey, new byte[] {9});
      wtx.commit();
      assertEquals(1, session.getMostRecentRevisionNumber(),
          "rollback must replace the poisoned page writer with a clean one");
    } finally {
      ProjectionIndexHOTStorage.setBulkFinalizeBeforeSpliceTestHook(null);
    }
  }
}
