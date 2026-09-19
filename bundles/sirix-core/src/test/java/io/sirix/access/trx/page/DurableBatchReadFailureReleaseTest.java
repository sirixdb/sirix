/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.RevisionEpochTracker;
import io.sirix.access.trx.RevisionEpochTracker.Ticket;
import io.sirix.access.trx.node.InternalResourceSession;
import io.sirix.cache.BufferManager;
import io.sirix.cache.EmptyCache;
import io.sirix.cache.ShardedPageCache;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.io.Reader;
import io.sirix.io.Writer;
import io.sirix.io.ram.RAMStorage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.OverflowPage;
import io.sirix.page.PageFragmentKeyImpl;
import io.sirix.page.PageReference;
import io.sirix.page.UberPage;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Pages the reader's batched durable reads obtained from the backend must be released when the
 * batch fails part way: through the scalar fallback a backend without the batch primitive gets, and
 * when a side-map read, batched or single, resolves to a page of the wrong kind. A backend that
 * hands out the instances it stores keeps them: the same failures leave its pages open.
 */
final class DurableBatchReadFailureReleaseTest {

  private static final byte[] KEY = {5, 7, 11};

  private static final byte[] VALUE = {13, 17, 19, 23};

  private final ResourceConfiguration config = new ResourceConfiguration.Builder("durable-batch-release").build();
  private final Reader disk = mock(Reader.class);
  private final ShardedPageCache<HOTLeafPage> hotLeafCache = new ShardedPageCache<>(1024L * 1024L);

  private InternalResourceSession<?, ?> session;
  private BufferManager buffers;
  private Arena arena;
  private NodeStorageEngineReader storage;

  @BeforeEach
  void openReader() {
    session = mock(InternalResourceSession.class);
    final RevisionEpochTracker tracker = mock(RevisionEpochTracker.class);
    when(tracker.register(anyInt())).thenReturn(mock(Ticket.class));
    when(session.getRevisionEpochTracker()).thenReturn(tracker);
    when(session.getResourceConfig()).thenReturn(config);
    buffers = mock(BufferManager.class);
    when(buffers.getHOTLeafPageCache()).thenReturn(hotLeafCache);
    when(buffers.getHOTLeafFragmentCache()).thenReturn(new EmptyCache<>());
    arena = Arena.ofShared();
    storage = readerOver(disk);
  }

  @AfterEach
  void closeReader() {
    try {
      storage.close();
    } finally {
      hotLeafCache.clear();
      arena.close();
    }
  }

  @Test
  void scalarFallbackReleasesTheFragmentsReadBeforeTheFailingOne() {
    final AtomicInteger headReleases = new AtomicInteger();
    final AtomicInteger firstReleases = new AtomicInteger();
    final AtomicInteger secondReleases = new AtomicInteger();
    final HOTLeafPage head = leaf(100L, headReleases);
    final HOTLeafPage first = leaf(200L, firstReleases);
    final HOTLeafPage second = leaf(300L, secondReleases);
    final SirixIOException injected = new SirixIOException("injected read failure of the last fragment");
    final PageReference chain = chainFailingAtItsLastFragment(head, first, second, injected);

    assertSame(injected, assertThrows(SirixIOException.class, () -> storage.loadHOTLeafFragments(chain)));

    assertTrue(first.isClosed(), "a fragment read before the failure must be released");
    assertTrue(second.isClosed(), "a fragment read before the failure must be released");
    assertEquals(1, firstReleases.get());
    assertEquals(1, secondReleases.get());
    assertTrue(head.isClosed());
    assertEquals(1, headReleases.get());
  }

  @Test
  void scalarFallbackLeavesTheFragmentsOfASharedPageBackendOpen() {
    when(disk.returnsSharedPages()).thenReturn(true);
    final AtomicInteger firstReleases = new AtomicInteger();
    final AtomicInteger secondReleases = new AtomicInteger();
    final HOTLeafPage first = leaf(200L, firstReleases);
    final HOTLeafPage second = leaf(300L, secondReleases);
    final SirixIOException injected = new SirixIOException("injected read failure of the last fragment");
    final PageReference chain = chainFailingAtItsLastFragment(leaf(100L, new AtomicInteger()), first, second, injected);

    assertSame(injected, assertThrows(SirixIOException.class, () -> storage.loadHOTLeafFragments(chain)));

    assertFalse(first.isClosed(), "a fragment the backend still owns must not be released");
    assertFalse(second.isClosed(), "a fragment the backend still owns must not be released");
    assertEquals(0, firstReleases.get());
    assertEquals(0, secondReleases.get());
  }

  @Test
  void sideMapBatchReleasesItsPagesWhenAMemberHasTheWrongKind() {
    final AtomicInteger releases = new AtomicInteger();
    final HOTLeafPage misplaced = leaf(20L, releases);
    when(disk.read(any(PageReference[].class), any(ResourceConfiguration.class))).thenReturn(
        new Page[] {new OverflowPage(new byte[] {1, 2, 3}), misplaced});

    assertThrows(SirixIOException.class, () -> storage.readSideOverflowPageBatch(new long[] {10L, 20L}));

    assertTrue(misplaced.isClosed(), "a batch member the caller never receives must be released");
    assertEquals(1, releases.get());
  }

  @Test
  void singleSideMapReadReleasesAPageOfTheWrongKind() {
    final AtomicInteger releases = new AtomicInteger();
    final HOTLeafPage misplaced = leaf(20L, releases);
    when(disk.read(any(PageReference.class), any(ResourceConfiguration.class))).thenReturn(misplaced);

    assertThrows(SirixIOException.class, () -> storage.readSideOverflowPage(new PageReference().setKey(20L)));

    assertTrue(misplaced.isClosed(), "a page the caller never receives must be released");
    assertEquals(1, releases.get());
  }

  @Test
  void anInMemoryStoreKeepsItsPagesWhenASideMapReferenceDangles() {
    final ResourceConfiguration ramConfig = new ResourceConfiguration.Builder("durable-batch-release-ram").build();
    ramConfig.resourcePath = Path.of("durable-batch-release-ram");
    final RAMStorage ram = new RAMStorage(ramConfig);
    final Writer ramWriter = ram.createWriter();
    final AtomicInteger releases = new AtomicInteger();
    final HOTLeafPage stored = leaf(20L, releases);
    assertTrue(stored.put(KEY, VALUE));
    final PageReference segmentReference = new PageReference();
    final PageReference leafReference = new PageReference();
    ramWriter.write(ramConfig, segmentReference, new OverflowPage(new byte[] {1, 2, 3}), null);
    ramWriter.write(ramConfig, leafReference, stored, null);
    final NodeStorageEngineReader overRam = readerOver(ram.createReader());
    try {
      assertThrows(SirixIOException.class,
          () -> overRam.readSideOverflowPageBatch(new long[] {segmentReference.getKey(), leafReference.getKey()}));
      assertThrows(SirixIOException.class,
          () -> overRam.readSideOverflowPage(new PageReference().setKey(leafReference.getKey())));
    } finally {
      overRam.close();
    }

    final HOTLeafPage reread = assertInstanceOf(HOTLeafPage.class, ram.createReader().read(leafReference, ramConfig));
    assertSame(stored, reread);
    assertFalse(reread.isClosed(), "the only copy of an in-memory store's page must survive a failed read");
    assertEquals(0, releases.get());
    assertArrayEquals(VALUE, reread.copyStoredValue(reread.findEntry(KEY)));
  }

  private NodeStorageEngineReader readerOver(final Reader backend) {
    return new NodeStorageEngineReader(1, session, new UberPage(), 1, backend, buffers,
        mock(RevisionRootPageReader.class), null);
  }

  /**
   * A three-fragment chain behind {@code head} whose batch read falls back to scalar reads, the last
   * of which fails with {@code injected}.
   */
  private PageReference chainFailingAtItsLastFragment(final HOTLeafPage head, final HOTLeafPage first,
      final HOTLeafPage second, final SirixIOException injected) {
    final PageReference chain = new PageReference().setKey(100L);
    chain.addPageFragment(new PageFragmentKeyImpl(3, 200L, 0L, 0L));
    chain.addPageFragment(new PageFragmentKeyImpl(2, 300L, 0L, 0L));
    chain.addPageFragment(new PageFragmentKeyImpl(1, 400L, 0L, 0L));
    when(disk.read(any(PageReference[].class), any(ResourceConfiguration.class))).thenReturn(null);
    when(disk.read(any(PageReference.class), any(ResourceConfiguration.class))).thenAnswer(invocation -> {
      final long key = invocation.<PageReference>getArgument(0).getKey();
      if (key == 100L) {
        return head;
      }
      if (key == 200L) {
        return first;
      }
      if (key == 300L) {
        return second;
      }
      throw injected;
    });
    return chain;
  }

  private HOTLeafPage leaf(final long pageKey, final AtomicInteger releases) {
    return new HOTLeafPage(pageKey, 1, IndexType.PATH, arena.allocate(HOTLeafPage.DEFAULT_SIZE),
        releases::incrementAndGet, new int[HOTLeafPage.MAX_ENTRIES], 0, 0);
  }
}
