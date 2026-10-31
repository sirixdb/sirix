package io.sirix.access.trx.page;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.DatabaseType;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.InternalResourceSession;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.IOStorage;
import io.sirix.io.StorageType;
import io.sirix.io.Writer;
import io.sirix.page.DeweyIDPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.UberPage;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Successor setup reuses checked roots without confusing historical content and allocation
 * frontiers.
 */
final class WriterReinstantiationTest {

  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void writerReadsDeweyContainerForCurrentAndHistoricalRoots(final VersioningType versioning) {
    for (final boolean storeDeweyIDs : new boolean[] {false, true}) {
      final Path databasePath = directory.resolve("writer-dewey-" + storeDeweyIDs);
      Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
      try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
        database.createResource(ResourceConfiguration.newBuilder("data")
                                                     .storageType(StorageType.FILE_CHANNEL)
                                                     .useDeweyIDs(storeDeweyIDs)
                                                     .versioningApproach(versioning)
                                                     .build());
        try (final JsonResourceSession session = database.beginResourceSession("data");
            final JsonNodeTrx trx = session.beginNodeTrx()) {
          final NodeStorageEngineWriter writer = (NodeStorageEngineWriter) trx.getStorageEngineWriter();
          final RevisionRootPage bootstrapRoot = writer.getActualRevisionRootPage();
          final DeweyIDPage bootstrapPage = writer.getDeweyIDPage(bootstrapRoot);
          assertNotNull(bootstrapPage, "the writer must expose its Dewey-ID container");
          assertSame(writer.getStorageEngineReader().getDeweyIDPage(bootstrapRoot), bootstrapPage);

          trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1]"), JsonNodeTrx.Commit.NO);
          trx.commit();

          final NodeStorageEngineWriter successor = (NodeStorageEngineWriter) trx.getStorageEngineWriter();
          final RevisionRootPage currentRoot = successor.getActualRevisionRootPage();
          final DeweyIDPage currentPage = successor.getDeweyIDPage(currentRoot);
          assertNotNull(currentPage);
          assertSame(successor.getStorageEngineReader().getDeweyIDPage(currentRoot), currentPage);
          try (final JsonNodeReadOnlyTrx historical = session.beginNodeReadOnlyTrx(1)) {
            final var reader = historical.getStorageEngineReader();
            final RevisionRootPage historicalRoot = reader.getActualRevisionRootPage();
            final DeweyIDPage historicalPage = successor.getDeweyIDPage(historicalRoot);
            assertNotNull(historicalPage);
            assertEquals(reader.getDeweyIDPage(historicalRoot).getMaxNodeKey(), historicalPage.getMaxNodeKey());
          }
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void successorReadsEachRequiredRootOnce(final VersioningType versioning) {
    final Path databasePath = directory.resolve("roots");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("data")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(versioning)
                                                   .build());
      try (final JsonResourceSession session = database.beginResourceSession("data")) {
        try (final JsonNodeTrx trx = session.beginNodeTrx()) {
          trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1]"), JsonNodeTrx.Commit.NO);
          trx.commit();
          trx.moveToDocumentRoot();
          assertTrue(trx.moveToFirstChild());
          trx.insertNumberValueAsLastChild(2);
          trx.commit();
        }
        try (final JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
          final var storageReader = reader.getStorageEngineReader();
          final ResourceConfiguration configuration = session.getResourceConfig();
          final var internalSession = (InternalResourceSession<?, ?>) storageReader.getResourceSession();
          final RevisionRootPage latestRoot = storageReader.getActualRevisionRootPage();
          for (final int representedRevision : new int[] {2, 1}) {
            final IOStorage storage = StorageType.FILE_CHANNEL.getInstance(configuration);
            try {
              final Writer backend = mock(Writer.class, delegatesTo(storage.createWriter()));
              try (final StorageEngineWriter successor =
                  new StorageEngineWriterFactory(DatabaseType.JSON).createStorageEngineWriter(internalSession,
                      new UberPage(storageReader.getUberPage()), backend, 100, representedRevision, 2, 2, true,
                      storageReader.getBufferManager())) {
                assertEquals(3, successor.getRevisionNumber());
                assertEquals(latestRoot.getMaxNodeKeyInDocumentIndex(),
                    successor.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex(),
                    "revert must not reuse document identities allocated in later revisions");
                assertNotSame(latestRoot, successor.getActualRevisionRootPage());
                verify(backend, times(1)).readRevisionRootPage(2, configuration);
                verify(backend, times(representedRevision == 1
                    ? 1
                    : 0)).readRevisionRootPage(1, configuration);
              }
            } finally {
              storage.close();
            }
          }
        }
        // Exercise the public revert path as well: old content, fresh identities, immutable history.
        try (final JsonNodeTrx trx = session.beginNodeTrx()) {
          trx.revertTo(1);
          trx.moveToDocumentRoot();
          assertTrue(trx.moveToFirstChild());
          assertEquals(1, trx.getChildCount());
          trx.insertNumberValueAsLastChild(3);
          final long newKey = trx.getNodeKey();
          assertTrue(newKey > 3, "the reverted transaction must retain the latest allocation frontier");
          trx.commit();
        }
        try (final JsonNodeReadOnlyTrx historical = session.beginNodeReadOnlyTrx(2)) {
          assertTrue(historical.moveToFirstChild());
          assertEquals(2, historical.getChildCount());
          assertTrue(historical.moveToLastChild());
          assertEquals(2, historical.getNumberValue().intValue());
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void disabledDeweyContainerKeepsItsDurableOffsetAcrossCommitsAndReopen(final VersioningType versioning) {
    final Path databasePath = directory.resolve("dewey");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    final long offset;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("data")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .useDeweyIDs(false)
                                                   .versioningApproach(versioning)
                                                   .build());
      try (final JsonResourceSession session = database.beginResourceSession("data");
          final JsonNodeTrx trx = session.beginNodeTrx()) {
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1]"), JsonNodeTrx.Commit.NO);
        trx.commit();
        try (final JsonNodeReadOnlyTrx committed = session.beginNodeReadOnlyTrx(1)) {
          offset = committed.getStorageEngineReader().getActualRevisionRootPage().getDeweyIdPageReference().getKey();
        }
        assertTrue(offset >= 0, "bootstrap must still persist the container");
        for (int revision = 2; revision <= 4; revision++) {
          final var writer = (NodeStorageEngineWriter) trx.getStorageEngineWriter();
          assertNull(writer.getLog().get(writer.getActualRevisionRootPage().getDeweyIdPageReference()),
              "an unchanged container must not enter the next revision's intent log");
          trx.moveToDocumentRoot();
          assertTrue(trx.moveToFirstChild());
          trx.insertNumberValueAsLastChild(revision);
          trx.commit();
          assertEquals(offset,
              trx.getStorageEngineWriter().getActualRevisionRootPage().getDeweyIdPageReference().getKey());
        }
      }
    }
    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession("data")) {
      for (int revision = 1; revision <= 4; revision++) {
        try (final JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
          final var reader = trx.getStorageEngineReader();
          assertEquals(offset, reader.getActualRevisionRootPage().getDeweyIdPageReference().getKey());
          assertEquals(1, reader.getDeweyIDPage(reader.getActualRevisionRootPage()).getMaxNodeKey());
          assertTrue(trx.moveToFirstChild());
          assertEquals(revision, trx.getChildCount());
        }
      }
    }
  }
}
