package io.sirix.access.trx.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.RevisionEpochTracker;
import io.sirix.access.trx.RevisionEpochTracker.Ticket;
import io.sirix.access.trx.node.InternalResourceSession;
import io.sirix.cache.BufferManager;
import io.sirix.cache.EmptyCache;
import io.sirix.cache.ShardedPageCache;
import io.sirix.index.IndexType;
import io.sirix.io.Reader;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.UberPage;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.foreign.Arena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class HOTGuardedPageHandoffTest {

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void coldCachedAndSwizzledLoadsEachTransferExactlyOneGuard(final VersioningType versioning) {
    final ResourceConfiguration config = new ResourceConfiguration.Builder("guard-handoff")
        .versioningApproach(versioning).build();
    final InternalResourceSession<?, ?> session = mock(InternalResourceSession.class);
    final RevisionEpochTracker tracker = mock(RevisionEpochTracker.class);
    when(tracker.register(anyInt())).thenReturn(mock(Ticket.class));
    when(session.getRevisionEpochTracker()).thenReturn(tracker);
    when(session.getResourceConfig()).thenReturn(config);
    final Reader disk = mock(Reader.class);
    final BufferManager buffers = mock(BufferManager.class);
    final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);
    when(buffers.getHOTLeafPageCache()).thenReturn(cache);
    when(buffers.getHOTLeafFragmentCache()).thenReturn(new EmptyCache<>());

    try (final Arena arena = Arena.ofShared();
        final NodeStorageEngineReader storage = new NodeStorageEngineReader(1, session, new UberPage(), 1, disk,
            buffers, mock(RevisionRootPageReader.class), null)) {
      final HOTLeafPage leaf = new HOTLeafPage(123L, 1, IndexType.PATH,
          arena.allocate(HOTLeafPage.DEFAULT_SIZE), null, new int[HOTLeafPage.MAX_ENTRIES], 0, 0);
      final PageReference reference = new PageReference().setKey(123L);
      when(disk.read(any(PageReference.class), any(ResourceConfiguration.class))).thenReturn(leaf);

      final HOTLeafPage cold = (HOTLeafPage) storage.loadHOTPageAndGuard(reference);
      try {
        assertSame(leaf, cold);
        assertEquals(1, cold.getGuardCount());
      } finally {
        cold.releaseGuard();
      }
      reference.setPage(null);
      final HOTLeafPage cached = (HOTLeafPage) storage.loadHOTPageAndGuard(reference);
      try {
        assertSame(leaf, cached);
        assertEquals(1, cached.getGuardCount());
      } finally {
        cached.releaseGuard();
      }
      final HOTLeafPage swizzled = (HOTLeafPage) storage.loadHOTPageAndGuard(reference);
      try {
        assertSame(leaf, swizzled);
        assertEquals(1, swizzled.getGuardCount());
        cache.clear();
        assertFalse(swizzled.isClosed());
      } finally {
        swizzled.releaseGuard();
      }
      assertTrue(leaf.isClosed());
      assertEquals(0, leaf.getGuardCount());
      verify(disk, times(1)).read(any(PageReference.class), any(ResourceConfiguration.class));
    } finally {
      cache.clear();
    }
  }
}
