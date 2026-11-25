package io.sirix.access.trx.page;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.DatabaseType;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.api.Database;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.api.StorageEngineReader;
import io.sirix.cache.BufferManagerImpl;
import io.sirix.io.InterruptedFirstCommitRecovery;
import io.sirix.io.Reader;
import io.sirix.io.StorageType;
import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** TXN-613: isolated and session-owned readers obey revision bounds and release their lifetimes. */
final class StorageEngineReaderContractTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(DatabaseType.class)
  void revisionBoundsAndClosedState(final DatabaseType type) {
    final Path databasePath = directory.resolve("database");
    final DatabaseConfiguration databaseConfig = new DatabaseConfiguration(databasePath);
    if (type == DatabaseType.JSON) {
      Databases.createJsonDatabase(databaseConfig);
    } else {
      Databases.createXmlDatabase(databaseConfig);
    }
    try (final Database<?> database = type == DatabaseType.JSON
        ? Databases.openJsonDatabase(databasePath)
        : Databases.openXmlDatabase(databasePath)) {
      database.createResource(
          ResourceConfiguration.newBuilder("resource").storageType(StorageType.FILE_CHANNEL).build());
      try (final ResourceSession<?, ?> session = database.beginResourceSession("resource")) {
        try (final NodeTrx writer = session.beginNodeTrx()) {
          writer.commit();
        }
        final StorageEngineReader bound = session.createStorageEngineReader();
        try (bound) {
          assertSame(session, bound.getResourceSession());
          assertEquals(0, bound.loadRevRoot(0).getRevision());
          assertEquals(1, bound.loadRevRoot(1).getRevision());
          assertThrows(IllegalArgumentException.class, () -> bound.loadRevRoot(-1));
          assertThrows(IllegalArgumentException.class, () -> bound.loadRevRoot(2));
        }
        assertThrows(AssertionError.class, bound::getResourceSession);

        final ResourceConfiguration config =
            ResourceConfiguration.deserializeForTool(databasePath.resolve("resources/resource"));
        final InterruptedFirstCommitRecovery.ReaderAndUberPage opened =
            InterruptedFirstCommitRecovery.openReaderForTool(config);
        try (final Reader disk = opened.reader();
            final BufferManagerImpl buffers = BufferManagerImpl.forVerification();
            final StorageEngineReader tool =
                NodeStorageEngineReader.forTool(config, opened.uberPage(), disk, buffers)) {
          assertEquals(0, tool.loadRevRoot(0).getRevision());
          assertEquals(1, tool.loadRevRoot(1).getRevision());
          assertThrows(IllegalArgumentException.class, () -> tool.loadRevRoot(-1));
          assertThrows(IllegalArgumentException.class, () -> tool.loadRevRoot(2));
          assertThrows(NullPointerException.class, tool::getResourceSession);
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(DatabaseType.class)
  void closingAHistoricalReaderReleasesItsReclamationWatermark(final DatabaseType type) {
    final Path databasePath = directory.resolve("epochs");
    final DatabaseConfiguration config = new DatabaseConfiguration(databasePath);
    if (type == DatabaseType.JSON) {
      Databases.createJsonDatabase(config);
    } else {
      Databases.createXmlDatabase(config);
    }
    try (final Database<?> database = type == DatabaseType.JSON
        ? Databases.openJsonDatabase(databasePath)
        : Databases.openXmlDatabase(databasePath)) {
      database.createResource(
          ResourceConfiguration.newBuilder("resource").storageType(StorageType.FILE_CHANNEL).build());
      try (final ResourceSession<?, ?> session = database.beginResourceSession("resource")) {
        final var epochs = ((AbstractResourceSession<?, ?>) session).getRevisionEpochTracker();
        try (final StorageEngineReader reader = session.createStorageEngineReader()) {
          try (final NodeTrx writer = session.beginNodeTrx()) {
            writer.commit();
          }
          assertEquals(0, epochs.minActiveRevision(), "an open revision-0 reader prevents reclamation past 0");
          assertEquals(0, reader.getRevisionNumber());
        }
        try (final StorageEngineReader current = session.createStorageEngineReader()) {
          assertEquals(1, current.getRevisionNumber());
          assertEquals(1, epochs.minActiveRevision(), "closed readers must no longer hold back the current reader");
        }
      }
    }
  }

}
