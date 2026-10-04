/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.DatabaseType;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.access.trx.node.json.JsonResourceSessionImpl;
import io.sirix.access.trx.node.json.objectvalue.NumberValue;
import io.sirix.cache.BufferManager;
import io.sirix.io.Writer;
import io.sirix.page.UberPage;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.projection.ProjectionBulkLoad;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Isolated
final class WriterConstructionFailureCleanupTest {

  @AfterEach
  void clearFault() {
    NodeStorageEngineWriter.asyncFlushFaultHook = null;
    ProjectionBulkLoad.clearActive();
    ProjectionIndexCatalog.clearCache();
  }

  @Test
  void earlyFactoryFailureClosesBackendWithoutReplacingPrimaryFailure() {
    final JsonResourceSessionImpl session = mock(JsonResourceSessionImpl.class);
    final Writer backend = mock(Writer.class);
    final OutOfMemoryError primary = new OutOfMemoryError("injected before reader creation");
    final IllegalStateException cleanup = new IllegalStateException("backend close failure");
    when(session.getResourceConfig()).thenThrow(primary);
    doThrow(cleanup).when(backend).close();
    final StorageEngineWriterFactory factory = new StorageEngineWriterFactory(DatabaseType.JSON);
    assertSame(primary, assertThrows(OutOfMemoryError.class, () -> factory.createStorageEngineWriter(session,
        new UberPage(), backend, 1, 0, 0, 0, true, mock(BufferManager.class))));
    verify(backend).close();
    assertEquals(1, primary.getSuppressed().length);
    assertSame(cleanup, primary.getSuppressed()[0]);
  }

  @Test
  void rollbackRecoversAfterSuccessorFailsHalfwayThroughConstruction(@TempDir final Path directory) {
    final Path databasePath = directory.resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(
          ResourceConfiguration.newBuilder("data").storageType(StorageType.FILE_CHANNEL).storeDiffs(false).build());
      try (final JsonResourceSession session = database.beginResourceSession("data");
          final JsonNodeTrx trx = session.beginNodeTrx()) {
        trx.insertNumberValueAsFirstChild(1);
        final long valueKey = trx.getNodeKey();
        final AtomicReference<NodeStorageEngineWriter> partial = new AtomicReference<>();
        final OutOfMemoryError failure = new OutOfMemoryError("injected halfway through successor construction");
        NodeStorageEngineWriter.asyncFlushFaultHook = (writer, site) -> {
          if ("constructor-before-local-caches".equals(site)) {
            partial.set(writer);
            throw failure;
          }
        };
        assertSame(failure, assertThrows(OutOfMemoryError.class, trx::commit));
        NodeStorageEngineWriter.asyncFlushFaultHook = null;
        final NodeStorageEngineWriter failed = partial.get();
        assertNotNull(failed);
        assertTrue(failed.delegate().isClosed(), "the failed constructor's reader must release its epoch ticket");
        assertEquals(0, failed.log.liveEntryCount(), "the factory must release the failed writer's intent log");
        assertEquals(1, session.getMostRecentRevisionNumber(), "the predecessor commit is already durable");
        assertDoesNotThrow(trx::rollback, "rollback must recover without entering the already-closed predecessor");
        assertTrue(trx.moveTo(valueKey));
        assertEquals(1, trx.getNumberValue().intValue());
        trx.setNumberValue(2);
        trx.commit();
        try (final var reader = session.beginNodeReadOnlyTrx(1)) {
          assertTrue(reader.moveTo(valueKey));
          assertEquals(1, reader.getNumberValue().intValue());
        }
      }
      try (final JsonResourceSession reopened = database.beginResourceSession("data");
          final JsonNodeTrx next = reopened.beginNodeTrx()) {
        assertTrue(next.moveToFirstChild());
        assertEquals(2, next.getNumberValue().intValue());
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = AfterCommitState.class, names = {"KEEP_OPEN", "KEEP_OPEN_ASYNC_COMMIT"})
  void rollbackRetiresProjectionOwnersAfterSuccessorConstructionFailure(final AfterCommitState mode,
      @TempDir final Path directory) {
    assertProjectionFailureRecovery(mode, directory, false);
  }

  @ParameterizedTest
  @EnumSource(value = AfterCommitState.class, names = {"KEEP_OPEN", "KEEP_OPEN_ASYNC_COMMIT"})
  void closeRetiresProjectionOwnersAfterSuccessorConstructionFailure(final AfterCommitState mode,
      @TempDir final Path directory) {
    assertProjectionFailureRecovery(mode, directory, true);
  }

  private static void assertProjectionFailureRecovery(final AfterCommitState mode, final Path directory,
      final boolean close) {
    final Path databasePath = directory.resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(
          ResourceConfiguration.newBuilder("data").storageType(StorageType.FILE_CHANNEL).storeDiffs(false).build());
      database.createResource(ResourceConfiguration.newBuilder("unrelated")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .storeDiffs(false)
                                                   .build());
      try (final JsonResourceSession session = database.beginResourceSession("data");
          final JsonResourceSession unrelatedSession = database.beginResourceSession("unrelated");
          final JsonNodeTrx unrelatedTrx = unrelatedSession.beginNodeTrx();
          final JsonNodeTrx trx = session.beginNodeTrx(1, mode)) {
        final IndexDef firstDef = projectionDefinition(0);
        final IndexDef secondDef = projectionDefinition(1);
        final String resourceKey = session.getResourceConfig().getResource().toString();
        final String unrelatedKey = unrelatedSession.getResourceConfig().getResource().toString();
        final JsonIndexController unrelatedController =
            unrelatedSession.getWtxIndexController(unrelatedTrx.getRevisionNumber());
        unrelatedController.createProjectionIndexesAtLoadStart(Set.of(firstDef), unrelatedTrx);
        final ProjectionBulkLoad unrelated = ProjectionBulkLoad.active(unrelatedKey, 0, unrelatedTrx);
        final JsonIndexController controller = session.getWtxIndexController(trx.getRevisionNumber());
        controller.createProjectionIndexesAtLoadStart(Set.of(firstDef, secondDef), trx);
        final ProjectionBulkLoad first = ProjectionBulkLoad.active(resourceKey, 0, trx);
        final ProjectionBulkLoad second = ProjectionBulkLoad.active(resourceKey, 1, trx);
        assertNotNull(first);
        assertNotNull(second);
        assertNotNull(unrelated);
        final long arrayKey = trx.insertArrayAsFirstChild().getNodeKey();
        final AtomicReference<NodeStorageEngineWriter> partial = new AtomicReference<>();
        final OutOfMemoryError failure = new OutOfMemoryError("injected intermediate successor failure");
        NodeStorageEngineWriter.asyncFlushFaultHook = (writer, site) -> {
          if ("constructor-before-local-caches".equals(site)) {
            partial.set(writer);
            throw failure;
          }
        };
        try {
          assertSame(failure, assertThrows(OutOfMemoryError.class, trx::insertObjectAsFirstChild));
        } finally {
          NodeStorageEngineWriter.asyncFlushFaultHook = null;
        }
        final NodeStorageEngineWriter failed = partial.get();
        assertNotNull(failed);
        assertTrue(failed.delegate().isClosed());
        assertEquals(0, failed.log.liveEntryCount());
        assertEquals(1, session.getMostRecentRevisionNumber());
        if (close) {
          assertDoesNotThrow(trx::close);
        } else {
          assertDoesNotThrow(trx::rollback);
        }
        assertTrue(first.isFinished());
        assertTrue(second.isFinished());
        assertNull(ProjectionBulkLoad.active(resourceKey, 0, trx));
        assertNull(ProjectionBulkLoad.active(resourceKey, 1, trx));
        assertSame(unrelated, ProjectionBulkLoad.active(unrelatedKey, 0, unrelatedTrx));
        assertFalse(unrelated.isFinished());
        final JsonNodeTrx recovered = close
            ? session.beginNodeTrx()
            : trx;
        try {
          assertTrue(recovered.moveTo(arrayKey));
          final long objectKey = recovered.insertObjectAsFirstChild().getNodeKey();
          recovered.commit();
          try (final var reader = session.beginNodeReadOnlyTrx()) {
            assertTrue(reader.moveTo(objectKey));
            assertTrue(reader.isObject());
            assertEquals(2,
                session.getRtxIndexController(reader.getRevisionNumber()).getIndexes().getIndexDefs().size());
          }
          try (final var reader = session.beginNodeReadOnlyTrx(1)) {
            assertTrue(reader.moveTo(arrayKey));
            assertFalse(reader.hasFirstChild());
          }
        } finally {
          if (close) {
            recovered.close();
          }
          unrelatedTrx.rollback();
        }
        assertFalse(ProjectionBulkLoad.anyActive());
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = AfterCommitState.class, names = {"KEEP_OPEN", "KEEP_OPEN_ASYNC_COMMIT"})
  void successfulIntermediateProjectionEpochsKeepTheirOwnerAndQueryResults(final AfterCommitState mode,
      @TempDir final Path directory) {
    final Path databasePath = directory.resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(
          ResourceConfiguration.newBuilder("data").storageType(StorageType.FILE_CHANNEL).storeDiffs(false).build());
      final IndexDef definition = projectionDefinition(0);
      try (final JsonResourceSession session = database.beginResourceSession("data");
          final JsonNodeTrx trx = session.beginNodeTrx(1, mode)) {
        final String resourceKey = session.getResourceConfig().getResource().toString();
        final JsonIndexController controller = session.getWtxIndexController(trx.getRevisionNumber());
        controller.createProjectionIndexesAtLoadStart(Set.of(definition), trx);
        final ProjectionBulkLoad owner = ProjectionBulkLoad.active(resourceKey, 0, trx);
        assertNotNull(owner);
        final long arrayKey = trx.insertArrayAsFirstChild().getNodeKey();
        for (final int value : new int[] {7, 11}) {
          assertTrue(trx.moveTo(arrayKey));
          trx.insertObjectAsLastChild();
          trx.insertObjectRecordAsFirstChild("value", new NumberValue(value));
          trx.awaitPendingAsyncCommit();
          assertSame(owner, ProjectionBulkLoad.active(resourceKey, 0, trx));
          assertFalse(owner.isFinished());
        }
        trx.commit();
        assertTrue(owner.isFinished());
        assertFalse(ProjectionBulkLoad.anyActive());
      }
      ProjectionIndexCatalog.clearCache();
      try (final JsonResourceSession session = database.beginResourceSession("data");
          final var reader = session.beginNodeReadOnlyTrx()) {
        final var handle = ProjectionIndexCatalog.load(session, reader.getRevisionNumber(), definition);
        assertNotNull(handle);
        final List<byte[]> leaves = handle.rowGroupPayloads(ProjectionIndexCatalog.rowGroupMaterializer(session,
            reader.getRevisionNumber(), definition.getID(), handle.rowGroupCount()));
        int rows = 0;
        long sum = 0;
        for (final byte[] leaf : leaves) {
          final ProjectionIndexRowGroupPage page = ProjectionIndexRowGroupPage.deserialize(leaf);
          for (int row = 0; row < page.getRowCount(); row++) {
            rows++;
            sum += page.numericColumn(0)[row];
          }
        }
        assertEquals(2, rows);
        assertEquals(18, sum);
        assertEquals(1, session.getRtxIndexController(reader.getRevisionNumber()).getIndexes().getIndexDefs().size());
      }
    }
  }

  private static IndexDef projectionDefinition(final int id) {
    return IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
        List.of(parse("/[]/value", PathParser.Type.JSON)), List.of(Type.LON), id, IndexDef.DbType.JSON);
  }

}
