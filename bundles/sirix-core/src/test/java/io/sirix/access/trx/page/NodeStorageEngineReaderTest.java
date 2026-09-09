package io.sirix.access.trx.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.InternalResourceSession;
import io.sirix.api.StorageEngineReader;
import io.sirix.cache.BufferManager;
import io.sirix.cache.EmptyCache;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.io.Reader;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageFragmentKeyImpl;
import io.sirix.page.PageReference;
import io.sirix.page.ProjectionIndexPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.UberPage;
import io.sirix.settings.Constants;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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

  @Test
  public void genericRecordRouteRejectsProjectionIndexesWithoutMaterializingAReference() {
    final InternalResourceSession<?, ?> resourceManagerMock = createResourceManagerMock();
    final RevisionRootPage revisionRootPage = new RevisionRootPage();
    final ProjectionIndexPage projectionIndexPage =
        (ProjectionIndexPage) revisionRootPage.getProjectionIndexPageReference().getPage();
    for (int index = 0; index < 4; index++) {
      projectionIndexPage.getIndirectPageReference(index);
    }
    final RevisionRootPageReader revisionRootPageReader = mock(RevisionRootPageReader.class);
    when(revisionRootPageReader.loadRevisionRootPage(any(StorageEngineReader.class), eq(0))).thenReturn(
        revisionRootPage);

    try (final var trx = new NodeStorageEngineReader(1, resourceManagerMock, new UberPage(), 0, mock(Reader.class),
        mock(BufferManager.class), revisionRootPageReader, mock(TransactionIntentLog.class))) {
      assertThrows(IllegalArgumentException.class,
          () -> trx.getPageReference(revisionRootPage, IndexType.PROJECTION, 4));
      assertThrows(IllegalArgumentException.class, () -> trx.getLeafPageReference(0L, 4, IndexType.PROJECTION));
      assertNull(projectionIndexPage.getIndexReference(4));
      assertEquals(4, projectionIndexPage.getReferencesCount());
    }
  }

  @Test
  public void hotFragmentRevisionMismatchFailsClosedAndReleasesTheLoadedWindow() {
    final long headOffset = 4_096L;
    final long fragmentOffset = 8_192L;
    final HOTLeafPage head = new HOTLeafPage(1L, 3, IndexType.PATH);
    final HOTLeafPage fragment = new HOTLeafPage(1L, 2, IndexType.PATH);
    try {
      final InternalResourceSession<?, ?> resourceManagerMock = createResourceManagerMock();
      final Reader pageReader = mock(Reader.class);
      final BufferManager bufferManager = mock(BufferManager.class);
      final EmptyCache<PageReference, HOTLeafPage> fragmentCache = new EmptyCache<>();
      when(bufferManager.getHOTLeafFragmentCache()).thenReturn(fragmentCache);
      when(pageReader.read(any(PageReference.class), any(ResourceConfiguration.class))).thenAnswer(invocation -> {
        final PageReference reference = invocation.getArgument(0);
        if (reference.getKey() == headOffset) {
          return head;
        }
        if (reference.getKey() == fragmentOffset) {
          return fragment;
        }
        return null;
      });

      final PageReference chainReference =
          new PageReference().setKey(headOffset)
                             .setPageFragments(List.of(new PageFragmentKeyImpl(1, fragmentOffset, 0L, 0L)));
      try (final var trx = new NodeStorageEngineReader(1, resourceManagerMock, new UberPage(), 3, pageReader,
          bufferManager, mock(RevisionRootPageReader.class), mock(TransactionIntentLog.class))) {
        final SirixIOException failure =
            assertThrows(SirixIOException.class, () -> trx.loadHOTLeafFragments(chainReference));
        assertTrue(failure.getMessage().contains("metadata revision=1, physical header revision=2"));
      }

      assertTrue("the caller-owned head must be retired when the window fails", head.isClosed());
      assertTrue("the mismatched guarded fragment must be released when validation fails", fragment.isClosed());
    } finally {
      if (!head.isClosed()) {
        head.close();
      }
      if (!fragment.isClosed()) {
        fragment.close();
      }
    }
  }

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
