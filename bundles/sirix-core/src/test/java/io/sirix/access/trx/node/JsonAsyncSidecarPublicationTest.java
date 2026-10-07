package io.sirix.access.trx.node;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.diff.JsonDiffSidecar;
import io.sirix.io.StorageType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class JsonAsyncSidecarPublicationTest {
  @TempDir
  Path directory;

  @AfterEach
  void clearHook() {
    AbstractNodeTrxImpl.asyncCommitTestHook = null;
  }

  @Test
  void publishesFrozenPayloadOnlyAfterItsRevisionIsDurable() throws Exception {
    Databases.createJsonDatabase(new DatabaseConfiguration(directory));
    try (final var database = Databases.openJsonDatabase(directory)) {
      database.createResource(
          ResourceConfiguration.newBuilder("resource").storageType(StorageType.FILE_CHANNEL).build());
      try (final var session = database.beginResourceSession("resource")) {
        try (final var writer = session.beginNodeTrx()) {
          writer.insertNumberValueAsFirstChild(0);
          writer.commit();
        }
        final var sidecar = session.getResourceConfig()
                                   .getResource()
                                   .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                                   .resolve("diffFromRev1toRev2.json");
        final var entered = new CountDownLatch(1);
        final var release = new CountDownLatch(1);
        AbstractNodeTrxImpl.asyncCommitTestHook = stage -> {
          if (stage.equals("before-harden")) {
            entered.countDown();
            try {
              assertTrue(release.await(30, TimeUnit.SECONDS));
            } catch (final InterruptedException failure) {
              Thread.currentThread().interrupt();
              throw new IllegalStateException(failure);
            }
          }
        };
        try (final var writer = session.beginNodeTrx(1, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT)) {
          try {
            assertTrue(writer.moveTo(1));
            writer.setNumberValue(1);
            writer.setNumberValue(2);
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            assertEquals(1, session.getMostRecentRevisionNumber());
            assertFalse(Files.exists(sidecar), "a prepared cache cannot precede revision publication");
          } finally {
            release.countDown();
          }
          writer.awaitPendingAsyncCommit();
          assertEquals(2, session.getMostRecentRevisionNumber());
          final var diff = JsonDiffSidecar.read(sidecar, "resource", 1, 2, false);
          assertEquals(1,
              diff.getAsJsonArray("diffs").get(0).getAsJsonObject().getAsJsonObject("update").get("value").getAsInt(),
              "the callback must not read the mutable successor epoch's value");
          assertEquals(2, writer.getNumberValue().intValue());
          writer.commit();
        } finally {
          release.countDown();
          AbstractNodeTrxImpl.asyncCommitTestHook = null;
        }
      }
    }
  }
}
