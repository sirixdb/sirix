/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixIOException;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ResourceLock("HOT_STRUCTURAL_PUBLICATION_TEST_HOOK")
final class HOTStructuralPublicationAtomicityTest {

  private static final String RESOURCE_NAME = "hot-structural-publication-atomicity";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();
  private static final int ROW_GROUPS = 1_500;
  private static final int COLUMNS = 8;

  @BeforeEach
  void setUp() throws IOException {
    AbstractHOTIndexWriter.setStructuralPublicationTestHook(null);
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(
          ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());
    }
  }

  @AfterEach
  void tearDown() {
    AbstractHOTIndexWriter.setStructuralPublicationTestHook(null);
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @Test
  @Timeout(value = 600, unit = TimeUnit.SECONDS)
  void sharedStructuralPublicationFailurePoisonsWriterUntilRollback() {
    final IllegalStateException sentinel = new IllegalStateException("injected after structural publication");
    final List<Long> order = new ArrayList<>(ROW_GROUPS);
    for (long rowGroup = 1; rowGroup <= ROW_GROUPS; rowGroup++) {
      order.add(rowGroup);
    }
    Collections.shuffle(order, new Random(42));

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage poisonedStorage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 0);
      final byte[] descriptor = new byte[24];
      final byte[] segment = new byte[600];
      AbstractHOTIndexWriter.setStructuralPublicationTestHook(() -> {
        throw sentinel;
      });

      final IllegalStateException publishedFailure = assertThrows(IllegalStateException.class, () -> {
        for (final long rowGroup : order) {
          poisonedStorage.putBlob(descriptorSlotKey(rowGroup), descriptor);
          for (int column = 0; column < COLUMNS; column++) {
            poisonedStorage.putColumnSegmentSlot(columnSegmentSlotKey(rowGroup, column), segment);
          }
        }
      });
      assertSame(sentinel, publishedFailure, "the publication seam must preserve the original failure");

      final SirixIOException nextWriteFailure = assertThrows(SirixIOException.class,
          () -> poisonedStorage.putBlob(descriptorSlotKey(ROW_GROUPS + 1L), descriptor));
      assertSame(sentinel, nextWriteFailure.getCause(),
          "the next projection write through the shared HOT writer must report the latched structural cause");

      final SirixIOException commitFailure = assertThrows(SirixIOException.class, wtx::commit);
      assertSame(sentinel, commitFailure.getCause(),
          "commit must reject the published partial graph with the original structural cause");

      AbstractHOTIndexWriter.setStructuralPublicationTestHook(null);
      wtx.rollback();

      final ProjectionIndexHOTStorage recoveredStorage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 0);
      recoveredStorage.putBlob(descriptorSlotKey(1), descriptor);
      wtx.commit();
      assertEquals(1, session.getMostRecentRevisionNumber(),
          "rollback must replace the poisoned page writer with a clean commit-capable writer");
    } finally {
      AbstractHOTIndexWriter.setStructuralPublicationTestHook(null);
    }
  }

  private static long descriptorSlotKey(final long rowGroupId) {
    return rowGroupId << 16;
  }

  private static long columnSegmentSlotKey(final long rowGroupId, final int column) {
    return (rowGroupId << 16) | (column + 1L);
  }
}
