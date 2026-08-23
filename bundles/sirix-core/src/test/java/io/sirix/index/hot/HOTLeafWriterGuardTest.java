/*
 * Copyright (c) 2026, SirixDB
 *
 * All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.page.HOTTrieWriter;
import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.BufferManager;
import io.sirix.cache.PageContainer;
import io.sirix.cache.ShardedPageCache;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Deterministic lifetime tests for writer-side HOT leaf copy-on-write transfers. */
class HOTLeafWriterGuardTest {

  private static final long PAGE_WEIGHT = 1024L;

  /**
   * Reloads that lose the guard before the writer may keep one. Deliberately above the writer's
   * tight-loop spin budget of 256 attempts: a writer that gives up on a fixed attempt count aborts
   * the transaction here, while one bounded by a wall-clock deadline still makes progress.
   */
  private static final int RETIRED_RELOADS = 300;

  @Test
  void trieWriterKeepsSourceGuardedAcrossPressureAndTilTransfer() {
    final LeafState source = leafState(1L);
    final HOTLeafPage modified = mock(HOTLeafPage.class);
    final PageReference reference = reference(1L);
    final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);
    cache.put(reference, source.page());
    reference.setPage(source.page());

    when(source.page().copy()).thenAnswer(_ -> {
      assertEquals(1, source.guards().get(), "the source must be guarded before copy allocation");
      cache.evictUnderPressure();
      assertNull(cache.get(reference), "the writer must own the source before copy allocation");
      assertFalse(source.closed().get());
      return modified;
    });

    final TransactionIntentLog log = detachingLog(reference, source, modified, cache);
    final StorageEngineWriter storageEngineWriter = storageEngineWriter(log, cache);
    final PageContainer result = new HOTTrieWriter().prepareKeyedLeafForModification(storageEngineWriter, log,
        reference, new byte[] {1}, IndexType.PATH, 0);

    assertSame(source.page(), result.getComplete());
    assertSame(modified, result.getModified());
    assertEquals(0, source.guards().get(), "the writer must release its guard exactly once");
    assertEquals(0L, cache.size(), "TIL admission must detach the shared cache mapping");
    assertFalse(source.closed().get(), "the detached source is now owned by the TIL");
    verify(source.page(), times(1)).releaseGuard();
  }

  @Test
  void trieWriterRejectsLateGuardEvictionAndReloadsByIdentity() throws Exception {
    final LeafState retired = leafState(2L);
    final LeafState replacement = leafState(2L);
    final HOTLeafPage modified = mock(HOTLeafPage.class);
    final PageReference reference = reference(2L);
    final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);
    cache.put(reference, retired.page());
    reference.setPage(retired.page());

    final CountDownLatch evictionObservedZero = new CountDownLatch(1);
    final CountDownLatch writerAcquiredGuard = new CountDownLatch(1);
    final CountDownLatch allowEviction = new CountDownLatch(1);
    final AtomicBoolean interceptZeroObservation = new AtomicBoolean(true);
    doAnswer(_ -> {
      final int observed = retired.guards().get();
      if (observed == 0 && interceptZeroObservation.compareAndSet(true, false)) {
        evictionObservedZero.countDown();
        assertTrue(allowEviction.await(5, TimeUnit.SECONDS));
      }
      return observed;
    }).when(retired.page()).getGuardCount();
    // The latch must only open once the guard is DEFINITIVELY held: it is what releases the blocked
    // evictor, and this test pins down the interleaving where the writer owns a real guard and
    // eviction retires the page underneath it anyway. Signalling between the increment and the
    // re-check below let the evictor retire while acquireGuard was still in flight, so the writer
    // backed its own guard out and never released one — leaving the retired page unclosed. That
    // window was wide enough to lose on the Windows runner.
    doAnswer(_ -> {
      if (retired.closed().get() || retired.orphaned().get()) {
        return false;
      }
      retired.guards().incrementAndGet();
      if (retired.closed().get() || retired.orphaned().get()) {
        retired.guards().decrementAndGet();
        return false;
      }
      writerAcquiredGuard.countDown();
      return true;
    }).when(retired.page()).acquireGuard();

    final TransactionIntentLog log = detachingLog(reference, replacement, modified, cache);
    when(replacement.page().copy()).thenReturn(modified);
    final StorageEngineWriter storageEngineWriter = storageEngineWriter(log, cache);
    when(storageEngineWriter.loadHOTPage(reference)).thenReturn(replacement.page());

    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final Future<?> eviction = executor.submit(cache::evictUnderPressure);
      assertTrue(evictionObservedZero.await(5, TimeUnit.SECONDS));
      final Future<PageContainer> copy =
          executor.submit(() -> new HOTTrieWriter().prepareKeyedLeafForModification(storageEngineWriter, log, reference,
              new byte[] {2}, IndexType.PATH, 0));
      assertTrue(writerAcquiredGuard.await(5, TimeUnit.SECONDS));
      allowEviction.countDown();

      eviction.get(5, TimeUnit.SECONDS);
      final PageContainer result = copy.get(5, TimeUnit.SECONDS);
      assertSame(replacement.page(), result.getComplete());
    } finally {
      allowEviction.countDown();
      executor.shutdownNow();
    }

    assertTrue(retired.orphaned().get(), "the eviction that observed zero first must retire its page");
    assertTrue(retired.closed().get(), "the rejected page must close on the writer's guard release");
    verify(retired.page(), never()).copy();
    verify(retired.page(), times(1)).releaseGuard();
    verify(replacement.page(), times(1)).releaseGuard();
  }

  @Test
  void trieWriterOutlastsAnEvictionStormThatKeepsRetiringTheReloadedLeaf() {
    final LeafState retired = leafState(9L);
    final LeafState replacement = leafState(9L);
    final HOTLeafPage modified = mock(HOTLeafPage.class);
    final PageReference reference = reference(9L);
    final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);
    reference.setPage(retired.page());
    retired.page().retire();

    // Every reload loses the guard again, exactly as pressure eviction retiring each freshly
    // published instance looks to the writer. The count exceeds any fixed tight-loop attempt
    // budget: the storm has to be outlasted in wall-clock time, not in attempts.
    final AtomicInteger retiredLoads = new AtomicInteger();
    final TransactionIntentLog log = detachingLog(reference, replacement, modified, cache);
    when(replacement.page().copy()).thenReturn(modified);
    final StorageEngineWriter storageEngineWriter = storageEngineWriter(log, cache);
    when(storageEngineWriter.loadHOTPage(reference)).thenAnswer(_ -> retiredLoads.getAndIncrement() < RETIRED_RELOADS
        ? retired.page()
        : replacement.page());

    final PageContainer result = new HOTTrieWriter().prepareKeyedLeafForModification(storageEngineWriter, log,
        reference, new byte[] {9}, IndexType.PATH, 0);

    assertSame(replacement.page(), result.getComplete(), "the storm must end in the live instance, not in a failure");
    assertSame(modified, result.getModified());
    assertEquals(RETIRED_RELOADS + 1, retiredLoads.get(), "every retired reload must be retried, not skipped");
    assertEquals(0, replacement.guards().get(), "the writer must release its guard exactly once");
    verify(retired.page(), never()).copy();
    verify(replacement.page(), times(1)).releaseGuard();
  }

  @Test
  void abstractIndexWriterKeepsCombinedSourceGuardedUntilTilOwnsIt() {
    final LeafState source = leafState(3L);
    final HOTLeafPage modified = mock(HOTLeafPage.class);
    final PageReference reference = reference(3L);
    final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);
    cache.put(reference, source.page());
    reference.setPage(source.page());

    when(source.page().copy()).thenAnswer(_ -> {
      assertEquals(1, source.guards().get(), "the combined source must be guarded before copy allocation");
      cache.evictUnderPressure();
      assertNull(cache.get(reference), "the index writer must own the versioning source before copy");
      return modified;
    });

    final TransactionIntentLog log = detachingLog(reference, source, modified, cache);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class, RETURNS_DEEP_STUBS);
    when(storageEngineWriter.getLog()).thenReturn(log);
    attachCache(storageEngineWriter, cache);
    when(storageEngineWriter.getResourceSession().getResourceConfig()).thenReturn(
        ResourceConfiguration.newBuilder("hot-writer-guard").versioningApproach(VersioningType.FULL).build());
    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);

    assertSame(modified, invokeCow(writer, reference, source.page()));
    assertEquals(0, source.guards().get());
    assertEquals(0L, cache.size());
    assertFalse(source.closed().get());
    verify(source.page(), times(1)).releaseGuard();
  }

  @Test
  void abstractIndexWriterOutlastsAnEvictionStormThatKeepsRetiringTheReloadedLeaf() {
    final LeafState retired = leafState(10L);
    final LeafState replacement = leafState(10L);
    final HOTLeafPage modified = mock(HOTLeafPage.class);
    final PageReference reference = reference(10L);
    final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);
    retired.page().retire();

    final AtomicInteger retiredLoads = new AtomicInteger();
    final TransactionIntentLog log = detachingLog(reference, replacement, modified, cache);
    when(replacement.page().copy()).thenReturn(modified);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class, RETURNS_DEEP_STUBS);
    when(storageEngineWriter.getLog()).thenReturn(log);
    attachCache(storageEngineWriter, cache);
    when(storageEngineWriter.getResourceSession().getResourceConfig()).thenReturn(
        ResourceConfiguration.newBuilder("hot-writer-guard-storm").versioningApproach(VersioningType.FULL).build());
    when(storageEngineWriter.loadHOTPage(reference)).thenAnswer(_ -> retiredLoads.getAndIncrement() < RETIRED_RELOADS
        ? retired.page()
        : replacement.page());

    assertSame(modified, invokeCow(new TestIndexWriter(storageEngineWriter), reference, retired.page()));
    assertEquals(RETIRED_RELOADS + 1, retiredLoads.get(), "every retired reload must be retried, not skipped");
    assertEquals(0, replacement.guards().get(), "the writer must release its guard exactly once");
    verify(retired.page(), never()).copy();
    verify(replacement.page(), times(1)).releaseGuard();
  }

  @Test
  void trieWriterRetiresDetachedSourceWhenCopyFailsBeforeTilAdmission() {
    final LeafState source = leafState(4L);
    final PageReference reference = reference(4L);
    final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);
    cache.put(reference, source.page());
    reference.setPage(source.page());
    when(source.page().copy()).thenThrow(new OutOfMemoryError("injected copy failure"));

    final TransactionIntentLog log = mock(TransactionIntentLog.class);
    when(log.get(reference)).thenReturn(null);
    final StorageEngineWriter storageEngineWriter = storageEngineWriter(log, cache);

    final HOTTrieWriter writer = new HOTTrieWriter();
    assertThrows(OutOfMemoryError.class, () -> writer.prepareKeyedLeafForModification(storageEngineWriter, log,
        reference, new byte[] {4}, IndexType.PATH, 0));

    assertEquals(0, source.guards().get());
    assertTrue(source.orphaned().get());
    assertTrue(source.closed().get());
    assertEquals(0L, cache.size());
    verify(source.page(), times(1)).releaseGuard();
    verify(log, never()).put(same(reference), any(PageContainer.class));
  }

  @Test
  void trieWriterPreservesCopyFailureWhenGuardReleaseAlsoFails() {
    final LeafState source = leafState(41L);
    final PageReference reference = reference(41L);
    final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);
    cache.put(reference, source.page());
    reference.setPage(source.page());
    final AssertionError copyFailure = new AssertionError("injected copy failure");
    final IllegalStateException releaseFailure = new IllegalStateException("injected guard release failure");
    when(source.page().copy()).thenThrow(copyFailure);
    failGuardReleaseAfterStateTransition(source, releaseFailure);

    final TransactionIntentLog log = mock(TransactionIntentLog.class);
    when(log.get(reference)).thenReturn(null);
    final StorageEngineWriter storageEngineWriter = storageEngineWriter(log, cache);

    final AssertionError thrown = assertThrows(AssertionError.class,
        () -> new HOTTrieWriter().prepareKeyedLeafForModification(storageEngineWriter, log, reference, new byte[] {41},
            IndexType.PATH, 0));

    assertSame(copyFailure, thrown);
    assertEquals(1, thrown.getSuppressed().length);
    assertSame(releaseFailure, thrown.getSuppressed()[0]);
    assertEquals(0, source.guards().get());
    assertTrue(source.closed().get());
  }

  @Test
  void abstractIndexWriterPreservesCombineFailureWhenGuardReleaseAlsoFails() {
    final LeafState source = leafState(42L);
    final PageReference reference = reference(42L);
    final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);
    cache.put(reference, source.page());
    reference.setPage(source.page());
    final AssertionError combineFailure = new AssertionError("injected combine failure");
    final IllegalStateException releaseFailure = new IllegalStateException("injected guard release failure");
    when(source.page().copy()).thenThrow(combineFailure);
    failGuardReleaseAfterStateTransition(source, releaseFailure);

    final TransactionIntentLog log = mock(TransactionIntentLog.class);
    when(log.get(reference)).thenReturn(null);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class, RETURNS_DEEP_STUBS);
    when(storageEngineWriter.getLog()).thenReturn(log);
    attachCache(storageEngineWriter, cache);
    when(storageEngineWriter.getResourceSession().getResourceConfig()).thenReturn(
        ResourceConfiguration.newBuilder("hot-writer-release-failure").versioningApproach(VersioningType.FULL).build());

    final AssertionError thrown = assertThrows(AssertionError.class,
        () -> invokeCow(new TestIndexWriter(storageEngineWriter), reference, source.page()));

    assertSame(combineFailure, thrown);
    assertEquals(1, thrown.getSuppressed().length);
    assertSame(releaseFailure, thrown.getSuppressed()[0]);
    assertEquals(0, source.guards().get());
    assertTrue(source.closed().get());
  }

  @Test
  void trieWriterRetiresDetachedPagesWhenTilPutFailsBeforePublication() {
    final LeafState source = leafState(5L);
    final LeafState modified = leafState(6L);
    final PageReference reference = reference(5L);
    final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);
    cache.put(reference, source.page());
    reference.setPage(source.page());
    when(source.page().copy()).thenReturn(modified.page());

    final TransactionIntentLog log = mock(TransactionIntentLog.class);
    when(log.get(reference)).thenReturn(null);
    doAnswer(_ -> {
      throw new IllegalStateException("injected pre-publication TIL failure");
    }).when(log).put(same(reference), any(PageContainer.class));

    final StorageEngineWriter storageEngineWriter = storageEngineWriter(log, cache);
    assertThrows(IllegalStateException.class,
        () -> new HOTTrieWriter().prepareKeyedLeafForModification(storageEngineWriter, log, reference, new byte[] {5},
            IndexType.PATH, 0));

    assertTrue(source.closed().get(), "the detached complete page has no owner after failed publication");
    assertTrue(modified.closed().get(), "the detached modified page has no owner after failed publication");
    assertEquals(0, source.guards().get());
    verify(source.page(), times(1)).retire();
    verify(modified.page(), times(1)).retire();
  }

  @Test
  void trieWriterPreservesPagesWhenTilPublishesBeforeThrowing() {
    final LeafState source = leafState(7L);
    final LeafState modified = leafState(8L);
    final PageReference reference = reference(7L);
    final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);
    cache.put(reference, source.page());
    reference.setPage(source.page());
    when(source.page().copy()).thenReturn(modified.page());

    final AtomicReference<PageContainer> published = new AtomicReference<>();
    final TransactionIntentLog log = mock(TransactionIntentLog.class);
    when(log.get(reference)).thenAnswer(_ -> published.get());
    doAnswer(invocation -> {
      published.set(invocation.getArgument(1));
      throw new IllegalStateException("injected post-publication TIL failure");
    }).when(log).put(same(reference), any(PageContainer.class));

    final StorageEngineWriter storageEngineWriter = storageEngineWriter(log, cache);
    assertThrows(IllegalStateException.class,
        () -> new HOTTrieWriter().prepareKeyedLeafForModification(storageEngineWriter, log, reference, new byte[] {7},
            IndexType.PATH, 0));

    assertSame(source.page(), published.get().getComplete());
    assertSame(modified.page(), published.get().getModified());
    assertEquals(0, source.guards().get());
    assertFalse(source.orphaned().get(), "the published complete page remains TIL-owned");
    assertFalse(modified.orphaned().get(), "the published modified page remains TIL-owned");
    verify(source.page(), never()).retire();
    verify(modified.page(), never()).retire();
  }

  private static TransactionIntentLog detachingLog(final PageReference reference, final LeafState source,
      final HOTLeafPage modified, final ShardedPageCache<HOTLeafPage> cache) {
    final TransactionIntentLog log = mock(TransactionIntentLog.class);
    when(log.get(reference)).thenReturn(null);
    doAnswer(invocation -> {
      final PageContainer container = invocation.getArgument(1);
      assertSame(source.page(), container.getComplete());
      assertSame(modified, container.getModified());
      assertEquals(1, source.guards().get(), "the source guard must cover cache-to-TIL detachment");
      cache.removePage(source.page());
      reference.setPage(null);
      return null;
    }).when(log).put(same(reference), any(PageContainer.class));
    return log;
  }

  private static PageReference reference(final long key) {
    return new PageReference().setKey(key).setDatabaseId(1).setResourceId(1);
  }

  private static StorageEngineWriter storageEngineWriter(final TransactionIntentLog log,
      final ShardedPageCache<HOTLeafPage> cache) {
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    when(storageEngineWriter.getLog()).thenReturn(log);
    attachCache(storageEngineWriter, cache);
    return storageEngineWriter;
  }

  private static void attachCache(final StorageEngineWriter storageEngineWriter,
      final ShardedPageCache<HOTLeafPage> cache) {
    final BufferManager bufferManager = mock(BufferManager.class);
    when(bufferManager.getHOTLeafPageCache()).thenReturn(cache);
    when(storageEngineWriter.getBufferManager()).thenReturn(bufferManager);
  }

  private static LeafState leafState(final long pageKey) {
    final HOTLeafPage page = mock(HOTLeafPage.class);
    final AtomicInteger guards = new AtomicInteger();
    final AtomicBoolean orphaned = new AtomicBoolean();
    final AtomicBoolean closed = new AtomicBoolean();
    final AtomicReference<PageReference> lastCacheKey = new AtomicReference<>();

    when(page.getPageKey()).thenReturn(pageKey);
    when(page.getActualMemorySize()).thenReturn(PAGE_WEIGHT);
    when(page.estimatedRetainedHeapBytes()).thenReturn(0L);
    when(page.getGuardCount()).thenAnswer(_ -> guards.get());
    when(page.isOrphaned()).thenAnswer(_ -> orphaned.get());
    when(page.isClosed()).thenAnswer(_ -> closed.get());
    when(page.isHot()).thenReturn(false);
    when(page.lastCacheKey()).thenAnswer(_ -> lastCacheKey.get());
    doAnswer(invocation -> {
      lastCacheKey.set(invocation.getArgument(0));
      return null;
    }).when(page).setLastCacheKey(any(PageReference.class));
    when(page.acquireGuard()).thenAnswer(_ -> {
      if (closed.get() || orphaned.get()) {
        return false;
      }
      guards.incrementAndGet();
      if (closed.get() || orphaned.get()) {
        guards.decrementAndGet();
        return false;
      }
      return true;
    });
    doAnswer(_ -> {
      final int remaining = guards.decrementAndGet();
      if (remaining < 0) {
        throw new IllegalStateException("guard underflow in test leaf");
      }
      if (remaining == 0 && orphaned.get()) {
        closed.set(true);
      }
      return null;
    }).when(page).releaseGuard();
    doAnswer(_ -> {
      orphaned.set(true);
      if (guards.get() == 0) {
        closed.set(true);
      }
      return null;
    }).when(page).retire();
    return new LeafState(page, guards, orphaned, closed);
  }

  private static void failGuardReleaseAfterStateTransition(final LeafState source,
      final RuntimeException releaseFailure) {
    doAnswer(_ -> {
      final int remaining = source.guards().decrementAndGet();
      if (remaining < 0) {
        throw new IllegalStateException("guard underflow in release-failure test leaf");
      }
      if (remaining == 0 && source.orphaned().get()) {
        source.closed().set(true);
      }
      throw releaseFailure;
    }).when(source.page()).releaseGuard();
  }

  private static HOTLeafPage invokeCow(final TestIndexWriter writer, final PageReference reference,
      final HOTLeafPage source) {
    try {
      final Method method = AbstractHOTIndexWriter.class.getDeclaredMethod("cowHOTLeafForModification",
          PageReference.class, HOTLeafPage.class);
      method.setAccessible(true);
      return (HOTLeafPage) method.invoke(writer, reference, source);
    } catch (final InvocationTargetException invocationFailure) {
      final Throwable cause = invocationFailure.getCause();
      if (cause instanceof RuntimeException runtimeFailure) {
        throw runtimeFailure;
      }
      if (cause instanceof Error error) {
        throw error;
      }
      throw new AssertionError(cause);
    } catch (final ReflectiveOperationException reflectionFailure) {
      throw new AssertionError(reflectionFailure);
    }
  }

  private record LeafState(HOTLeafPage page, AtomicInteger guards, AtomicBoolean orphaned, AtomicBoolean closed) {
  }

  private static final class TestIndexWriter extends AbstractHOTIndexWriter<Long> {
    private byte[] keyBuffer = new byte[Long.BYTES];

    private TestIndexWriter(final StorageEngineWriter storageEngineWriter) {
      super(storageEngineWriter, IndexType.PATH, 0);
    }

    @Override
    protected byte[] getKeyBuffer() {
      return keyBuffer;
    }

    @Override
    protected void setKeyBuffer(final byte[] newBuffer) {
      keyBuffer = newBuffer;
    }

    @Override
    protected int serializeKey(final Long key, final byte[] buffer, final int offset) {
      return 0;
    }
  }
}
