package org.sirix.access.trx.page;

import org.junit.Test;
import org.sirix.access.trx.node.InternalResourceManager;
import org.sirix.cache.BufferManager;
import org.sirix.cache.TransactionIntentLog;
import org.sirix.io.IOStorage;
import org.sirix.io.Reader;
import org.sirix.page.UberPage;
import org.sirix.settings.Constants;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public final class NodePageReadOnlyTrxTest {

  @Test
  public void testPageKey() {
    final var trx = new NodePageReadOnlyTrx(1, mock(InternalResourceManager.class), new UberPage(), 0,
        mock(Reader.class), mock(TransactionIntentLog.class), mock(BufferManager.class),
        mock(RevisionRootPageReader.class));

    assertEquals(0, trx.pageKey(1));
    assertEquals(1023 / Constants.NDP_NODE_COUNT, trx.pageKey(1023));
    assertEquals(1024 / Constants.NDP_NODE_COUNT, trx.pageKey(1024));
  }

  @Test
  public void testRecordPageOffset() {
    final var trx = new NodePageReadOnlyTrx(1, mock(InternalResourceManager.class), new UberPage(), 0,
        mock(Reader.class), mock(TransactionIntentLog.class), mock(BufferManager.class),
        mock(RevisionRootPageReader.class));

    assertEquals(1, trx.recordPageOffset(1));
    assertEquals(Constants.NDP_NODE_COUNT - 1, trx.recordPageOffset(1023));
  }
}