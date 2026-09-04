/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.ResourceStore;
import io.sirix.access.trx.node.InternalResourceSession.Abort;
import io.sirix.access.trx.page.StorageEngineWriterFactory;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.cache.BufferManager;
import io.sirix.io.IOStorage;
import io.sirix.node.interfaces.Node;
import io.sirix.page.UberPage;
import io.sirix.settings.Fixed;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.concurrent.Semaphore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class AbstractResourceSessionWriteFailureCleanupTest {

  @Test
  void listenerBindingFailureClosesUnregisteredWriterAndLeavesPermitReusable() {
    final RuntimeException bindingFailure = new IllegalStateException("projection listener binding failed");
    final RuntimeException closeFailure = new IllegalStateException("writer close failed");
    final StorageEngineWriter failedWriter = writerWithDocumentNode();
    doThrow(closeFailure).when(failedWriter).close();
    final StorageEngineWriter succeedingWriter = writerWithDocumentNode();
    final JsonNodeTrx succeedingTrx = mock(JsonNodeTrx.class);

    final FailingResourceSession session =
        new FailingResourceSession(bindingFailure, succeedingTrx, failedWriter, succeedingWriter);
    try {
      final RuntimeException thrown = assertThrows(RuntimeException.class, session::beginNodeTrx);

      assertSame(bindingFailure, thrown, "cleanup must not replace the listener-binding failure");
      assertEquals(1, thrown.getSuppressed().length);
      assertSame(closeFailure, thrown.getSuppressed()[0]);
      verify(failedWriter).close();
      assertEquals(1, session.writeLock.availablePermits(), "failed construction must release exactly one permit");

      final JsonNodeTrx opened = session.beginNodeTrx();
      assertSame(succeedingTrx, opened, "a subsequent writer must be available immediately");
      assertEquals(0, session.writeLock.availablePermits());
      verify(succeedingWriter, never()).close();

      final int transactionId = opened.getId();
      session.closeWriteTransaction(transactionId);
      succeedingWriter.close();
      verify(succeedingWriter).close();
      assertEquals(1, session.writeLock.availablePermits());
    } finally {
      session.close();
    }
  }

  private static StorageEngineWriter writerWithDocumentNode() {
    final StorageEngineWriter writer = mock(StorageEngineWriter.class);
    when(writer.getRecord(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), io.sirix.index.IndexType.DOCUMENT,
        -1)).thenReturn(mock(Node.class));
    return writer;
  }

  /** Deterministic seam: the first node-transaction construction models listener binding failure. */
  private static final class FailingResourceSession extends AbstractResourceSession<JsonNodeReadOnlyTrx, JsonNodeTrx> {

    private final ArrayDeque<StorageEngineWriter> writers = new ArrayDeque<>();
    private final RuntimeException firstConstructionFailure;
    private final JsonNodeTrx succeedingTrx;
    private int constructionAttempts;

    @SuppressWarnings({"rawtypes", "unchecked"})
    private FailingResourceSession(final RuntimeException firstConstructionFailure, final JsonNodeTrx succeedingTrx,
        final StorageEngineWriter... writers) {
      super((ResourceStore) mock(ResourceStore.class), ResourceConfiguration.newBuilder("resource").build(),
          mock(BufferManager.class), mock(IOStorage.class), uberPageAtRevisionZero(), new Semaphore(1), null,
          mock(StorageEngineWriterFactory.class));
      this.firstConstructionFailure = firstConstructionFailure;
      this.succeedingTrx = succeedingTrx;
      for (final StorageEngineWriter writer : writers) {
        this.writers.addLast(writer);
      }
    }

    @Override
    public StorageEngineWriter createPageTransaction(final int id, final int representRevision,
        final int storedRevision, final Abort abort, final boolean isBoundToNodeTrx) {
      return writers.removeFirst();
    }

    @Override
    public JsonNodeReadOnlyTrx createNodeReadOnlyTrx(final int nodeTrxId, final StorageEngineReader storageEngineReader,
        final Node documentNode) {
      throw new UnsupportedOperationException("not used by this test");
    }

    @Override
    public JsonNodeTrx createNodeReadWriteTrx(final int nodeTrxId, final StorageEngineWriter storageEngineWriter,
        final int maxNodeCount, final Duration autoCommitDelay, final Node documentNode,
        final AfterCommitState afterCommitState) {
      if (constructionAttempts++ == 0) {
        throw firstConstructionFailure;
      }
      when(succeedingTrx.getId()).thenReturn(nodeTrxId);
      return succeedingTrx;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <C extends IndexController<JsonNodeReadOnlyTrx, JsonNodeTrx>> C getRtxIndexController(final int revision) {
      return (C) mock(IndexController.class);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <C extends IndexController<JsonNodeReadOnlyTrx, JsonNodeTrx>> C getWtxIndexController(final int revision) {
      return (C) mock(IndexController.class);
    }

    private static UberPage uberPageAtRevisionZero() {
      final UberPage uberPage = mock(UberPage.class);
      when(uberPage.getRevisionNumber()).thenReturn(0);
      return uberPage;
    }
  }
}
