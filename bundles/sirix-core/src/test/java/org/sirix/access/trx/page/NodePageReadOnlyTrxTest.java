package org.sirix.access.trx.page;

import org.junit.Test;
import org.sirix.access.trx.node.IndexController;
import org.sirix.access.trx.node.InternalResourceManager;
import org.sirix.cache.BufferManager;
import org.sirix.cache.TransactionIntentLog;
import org.sirix.io.Reader;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.UberPage;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public final class NodePageReadOnlyTrxTest {

  @Test
  public void recordPageOffset() {
    final var trx = new NodePageReadOnlyTrx(1, mock(InternalResourceManager.class), new UberPage(), 0, mock(Reader.class), mock(
        TransactionIntentLog.class), mock(IndexController.class), mock(BufferManager.class), mock(RevisionRootPageReader.class));

    assertEquals(1, trx.recordPageOffset(1));

    // 0 - 511 on first page, 512 - 1023 on second page.
    assertEquals(511, trx.recordPageOffset(1023));
  }
}