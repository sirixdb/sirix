package io.sirix.access.trx.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.InternalResourceSession;
import io.sirix.api.StorageEngineReader;
import io.sirix.cache.BufferManager;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.node.NodeKind;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.UberPage;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Assert;
import org.junit.Test;
import io.sirix.io.Reader;
import io.sirix.settings.Constants;

import java.lang.foreign.Arena;

import static io.sirix.cache.LinuxMemorySegmentAllocator.SIXTYFOUR_KB;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class NodeStorageEngineReaderTest {

  @Test
  public void testPageKey() {
    final InternalResourceSession<?, ?> resourceManagerMock = createResourceManagerMock();

    try (final var trx = new NodeStorageEngineReader(1, resourceManagerMock, new UberPage(), 0, mock(Reader.class),
        mock(BufferManager.class), mock(RevisionRootPageReader.class), mock(TransactionIntentLog.class))) {
      assertEquals(0, trx.pageKey(1, IndexType.DOCUMENT));
      assertEquals(1023 / Constants.NDP_NODE_COUNT, trx.pageKey(1023, IndexType.DOCUMENT));
      assertEquals(1024 / Constants.NDP_NODE_COUNT, trx.pageKey(1024, IndexType.DOCUMENT));
    }
  }

  @Test
  public void testRecordPageOffset() {
    Assert.assertEquals(1, StorageEngineReader.recordPageOffset(1));
    assertEquals(Constants.NDP_NODE_COUNT - 1, StorageEngineReader.recordPageOffset(1023));
  }

  @Test(expected = IllegalStateException.class)
  public void testGetValueFailsFastForUnsupportedFixedSlotMaterialization() {
    final InternalResourceSession<?, ?> resourceManagerMock = createResourceManagerMock();
    final ResourceConfiguration config = new ResourceConfiguration.Builder("foobar").build();

    try (final var trx = new NodeStorageEngineReader(1, resourceManagerMock, new UberPage(), 0, mock(Reader.class),
        mock(BufferManager.class), mock(RevisionRootPageReader.class), mock(TransactionIntentLog.class))) {
      try (Arena arena = Arena.ofConfined()) {
        final KeyValueLeafPage page =
            new KeyValueLeafPage(0L, IndexType.DOCUMENT, config, 1, arena.allocate(SIXTYFOUR_KB), null);
        try {
          final long nodeKey = 7L;
          final int slot = StorageEngineReader.recordPageOffset(nodeKey);
          final int fixedSize = NodeKind.STRING_VALUE.layoutDescriptor().fixedSlotSizeInBytes();
          page.setSlot(new byte[fixedSize], slot);
          page.markSlotAsFixedFormat(slot, NodeKind.STRING_VALUE);

          trx.getValue(page, nodeKey);
        } finally {
          page.close();
        }
      }
    }
  }

  @NonNull
  private InternalResourceSession<?, ?> createResourceManagerMock() {
    final var resourceManagerMock = mock(InternalResourceSession.class);
    when(resourceManagerMock.getResourceConfig()).thenReturn(new ResourceConfiguration.Builder("foobar").build());

    // Mock RevisionEpochTracker to prevent NullPointerException
    final var epochTrackerMock = mock(io.sirix.access.trx.RevisionEpochTracker.class);
    final var ticketMock = mock(io.sirix.access.trx.RevisionEpochTracker.Ticket.class);
    when(epochTrackerMock.register(org.mockito.ArgumentMatchers.anyInt())).thenReturn(ticketMock);
    when(resourceManagerMock.getRevisionEpochTracker()).thenReturn(epochTrackerMock);

    return resourceManagerMock;
  }
}
