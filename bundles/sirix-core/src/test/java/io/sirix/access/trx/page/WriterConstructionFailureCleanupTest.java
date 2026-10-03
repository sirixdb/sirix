/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.DatabaseType;
import io.sirix.access.trx.node.json.JsonResourceSessionImpl;
import io.sirix.cache.BufferManager;
import io.sirix.io.Writer;
import io.sirix.page.UberPage;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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
}
