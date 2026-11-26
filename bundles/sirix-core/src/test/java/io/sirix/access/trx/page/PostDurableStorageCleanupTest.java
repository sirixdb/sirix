package io.sirix.access.trx.page;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.InternalResourceSession;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.cache.PageContainer;
import io.sirix.page.PageReference;
import io.sirix.io.StorageType;
import io.sirix.io.fault.FaultInjectingStorage;
import io.sirix.io.fault.FaultInjectingWriter;
import io.sirix.io.fault.FaultInjectingWriter.Point;
import io.sirix.io.fault.FaultInjectingWriter.SimulatedCrashException;
import java.io.IOException;
import java.io.UncheckedIOException;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.UberPage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doThrow;

/**
 * TXN-307's storage boundary owns cleanup even without a node transaction above it. The oracle is
 * the committed record and resource ownership: hardening releases its pages/log, later reads use
 * durable data, and a cleanup error retains repair responsibility without changing the result.
 */
@Isolated
final class PostDurableStorageCleanupTest {
  @TempDir
  Path directory;

  @BeforeEach
  void installStorage() {
    FaultInjectingStorage.install(directory);
  }

  @AfterEach
  void uninstallStorage() {
    FaultInjectingStorage.uninstall();
  }

  @ParameterizedTest
  @ValueSource(strings = {"success", "spill", "spill-and-marker", "intent-log"})
  void hardeningReleasesEpochResourcesEvenWhenSpillCleanupFails(final String failures) throws IOException {
    final boolean failSpill = failures.startsWith("spill");
    final Path databasePath = directory.resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(
          ResourceConfiguration.newBuilder("data").storageType(StorageType.FILE_CHANNEL).storeDiffs(false).build());
      try (final JsonResourceSession session = database.beginResourceSession("data")) {
        try (final JsonNodeTrx seed = session.beginNodeTrx()) {
          seed.insertArrayAsFirstChild();
          seed.commit();
        }
        try (final NodeStorageEngineWriter storage = (NodeStorageEngineWriter) session.createStorageEngineWriter()) {
          storage.prepareRecordForModification(1, IndexType.DOCUMENT, -1);
          storage.captureRecordForDeferredRead(1);
          assertEquals(1, storage.retainedRecordCaptureCount());
          storage.asyncFlushStorageEpoch();
          storage.awaitPendingAsyncFlush();
          final KeyValueLeafPage durablePage = storage.getModifiedPageForRead(0, IndexType.DOCUMENT, -1);
          assertNotNull(durablePage);
          assertFalse(durablePage.isClosed());
          storage.prepareRecordForModification(1, IndexType.DOCUMENT, -1);
          final NodeStorageEngineReader reader = (NodeStorageEngineReader) storage.delegate();
          reader.getRecord(1, IndexType.DOCUMENT, -1);
          final KeyValueLeafPage guardedPage = reader.getCurrentPage();
          assertNotNull(guardedPage);
          assertTrue(storage.getLog().size() > 0);
          final FaultInjectingWriter backend = FaultInjectingStorage.lastWriter();
          final Path marker = ((InternalResourceSession<?, ?>) session).getCommitFile();
          if (failSpill) {
            backend.injectAt(Point.BEFORE_DISCARD_SPILLED_WRITES, 1);
          }
          if (failures.equals("spill-and-marker")) {
            backend.beforeDiscardSpilledWrites(() -> {
              try {
                Files.delete(marker);
                Files.createDirectory(marker);
                Files.createFile(marker.resolve("blocker"));
              } catch (final IOException failure) {
                throw new UncheckedIOException(failure);
              }
            });
          }
          final UberPage pending = storage.commitWritePages(null, null, false);
          final OutOfMemoryError pageCloseFailure = new OutOfMemoryError("intent-log page close");
          if (failures.equals("intent-log")) {
            // The serialized revision is complete. Model a page-owner teardown failure in the
            // remaining intent log; the real committed pages and caches still have to be released.
            final KeyValueLeafPage failingPage = mock(KeyValueLeafPage.class);
            doThrow(pageCloseFailure).when(failingPage).close();
            storage.appendLogRecord(new PageReference(), PageContainer.getInstance(failingPage, failingPage));
          }
          assertDoesNotThrow(() -> storage.hardenCommit(pending, false));
          assertEquals(0, storage.retainedRecordCaptureCount(), "hardening must release captured heap records");
          assertTrue(durablePage.isClosed(), "hardening must retire pages borrowed from durable storage");
          assertNull(reader.getCurrentPage(), "hardening must release the cursor guard before clearing the log");
          assertEquals(0, guardedPage.getGuardCount());
          assertEquals(0, storage.getLog().size(), "hardening owns the entire intent log");
          final DataRecord record = storage.getRecord(1, IndexType.DOCUMENT, -1);
          assertNotNull(record);
          assertEquals(1, record.getNodeKey(), "reads after hardening must resolve the committed record");
          if (failSpill) {
            assertFalse(backend.isArmedFor(Point.BEFORE_DISCARD_SPILLED_WRITES));
            assertInstanceOf(SimulatedCrashException.class, storage.postCommitCleanupFailure(),
                "a later marker failure must not overwrite the first cleanup cause");
            assertTrue(Files.exists(((InternalResourceSession<?, ?>) session).getCommitFile()),
                "the storage boundary must flag repair before node orchestration observes the result");
          } else if (failures.equals("intent-log")) {
            assertSame(pageCloseFailure, storage.postCommitCleanupFailure());
            assertTrue(Files.exists(marker));
          } else {
            assertNull(storage.postCommitCleanupFailure());
          }
          if (failures.equals("spill-and-marker")) {
            Files.delete(marker.resolve("blocker"));
            Files.delete(marker);
          }
        }
      }
    }
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession("data");
        final var reader = session.beginNodeReadOnlyTrx()) {
      assertEquals(2, session.getMostRecentRevisionNumber());
      assertTrue(reader.moveToFirstChild());
      assertTrue(reader.isArray());
      assertEquals(0, reader.getChildCount());
    }
  }
}
