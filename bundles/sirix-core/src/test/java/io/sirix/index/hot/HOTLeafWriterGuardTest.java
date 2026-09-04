/*
 * Copyright (c) 2026, SirixDB
 *
 * All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.access.ResourceConfiguration;
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
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
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
   *
   * <p>
   * Each reload produces a DISTINCT instance, which is what an eviction storm actually looks like:
   * the loader publishes a fresh page and eviction retires it before the writer can guard it. Handing
   * back the same retired instance over and over would not be a storm at all — that instance can
   * never become guardable again, so it is a dead end, and the writer is required to say so rather
   * than to keep retrying it.
   * </p>
   */
  private static final int RETIRED_RELOADS = 300;

  /**
   * How long a retirement stays in flight in the starvation tests. Long enough that a writer which
   * only spins and yields racks up attempts by the hundred thousand, short enough to keep the test
   * quick.
   */
  private static final long RETIREMENT_IN_FLIGHT_MILLIS = 300L;

  /** Attempts the writer spends spinning and yielding before its back-off starts parking. */
  private static final int PRE_PARK_ATTEMPTS = 512;

  /**
   * Attempts per millisecond of waiting a PARKED writer can reach, and the whole point of the two
   * starvation tests: the writer has to WAIT for the retiring thread, and a waiter that never gives
   * up its core starves the thread it is waiting for.
   *
   * <p>
   * Derived, not sampled. The parked back-off tops out at one attempt per 100 µs, so 10 per
   * millisecond, doubled here so that timer granularity and a loaded runner cannot make a correct
   * writer look like a spinning one. Measuring a RATE rather than a total is what keeps the bound
   * honest when the runner stalls the retiring thread past {@link #RETIREMENT_IN_FLIGHT_MILLIS} — a
   * longer wait then buys proportionally more attempts instead of failing the test. A yielding writer
   * sits far above the rate either way: ~56 per millisecond where a yield is expensive (Linux
   * {@code sched_yield}) and thousands where it is not ({@code SwitchToThread} with no ready thread
   * on the core, which is the livelock this pins down).
   * </p>
   */
  private static final int MAX_ATTEMPTS_PER_MILLI_WHILE_RETIRING = 20;

  @Test
  void guardedReadProbeReloadsInsteadOfReadingALeafLostBeforeGuardAcquisition() {
    final LeafState raced = leafState(1L);
    final LeafState replacement = leafState(1L);
    final PageReference reference = reference(1L);
    reference.setPage(raced.page());

    when(raced.page().acquireGuard()).thenAnswer(_ -> {
      raced.orphaned().set(true);
      raced.closed().set(true);
      return false;
    });
    final TransactionIntentLog log = mock(TransactionIntentLog.class);
    when(log.get(reference)).thenReturn(null);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class, RETURNS_DEEP_STUBS);
    when(storageEngineWriter.getLog()).thenReturn(log);
    when(storageEngineWriter.loadHOTPage(reference)).thenReturn(replacement.page());

    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter);
    final HOTLeafPage guarded = writer.acquireReadLeaf(reference);
    try {
      assertSame(replacement.page(), guarded,
          "a lost resolve-to-guard race must reload, never escape the retired page as a read result");
      assertEquals(1, replacement.guards().get());
    } finally {
      guarded.releaseGuard();
    }

    verify(raced.page(), times(1)).acquireGuard();
    verify(replacement.page(), times(1)).acquireGuard();
    assertEquals(0, replacement.guards().get());
  }


  @Test
  void abstractIndexWriterRejectsLateGuardEvictionAndReloadsByIdentity() throws Exception {
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
    when(replacement.page().copyForRevision(anyInt())).thenReturn(modified);
    final StorageEngineWriter storageEngineWriter = storageEngineWriter(log, cache);
    when(storageEngineWriter.loadHOTPage(reference)).thenReturn(replacement.page());

    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final Future<?> eviction = executor.submit(cache::evictUnderPressure);
      assertTrue(evictionObservedZero.await(5, TimeUnit.SECONDS));
      final Future<HOTLeafPage> copy =
          executor.submit(() -> invokeCow(new TestIndexWriter(storageEngineWriter), reference, retired.page()));
      assertTrue(writerAcquiredGuard.await(5, TimeUnit.SECONDS));
      allowEviction.countDown();

      eviction.get(5, TimeUnit.SECONDS);
      assertSame(modified, copy.get(5, TimeUnit.SECONDS));
    } finally {
      allowEviction.countDown();
      executor.shutdownNow();
    }

    assertTrue(retired.orphaned().get(), "the eviction that observed zero first must retire its page");
    assertTrue(retired.closed().get(), "the rejected page must close on the writer's guard release");
    verify(retired.page(), never()).copyForRevision(anyInt());
    verify(retired.page(), times(1)).releaseGuard();
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

    when(source.page().copyForRevision(anyInt())).thenAnswer(_ -> {
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
    final LeafState firstRetired = retiredLeafState(10L);
    final LeafState replacement = leafState(10L);
    final HOTLeafPage modified = mock(HOTLeafPage.class);
    final PageReference reference = reference(10L);
    final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);

    final AtomicInteger retiredLoads = new AtomicInteger();
    final TransactionIntentLog log = detachingLog(reference, replacement, modified, cache);
    when(replacement.page().copyForRevision(anyInt())).thenReturn(modified);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class, RETURNS_DEEP_STUBS);
    when(storageEngineWriter.getLog()).thenReturn(log);
    attachCache(storageEngineWriter, cache);
    when(storageEngineWriter.getResourceSession().getResourceConfig()).thenReturn(
        ResourceConfiguration.newBuilder("hot-writer-guard-storm").versioningApproach(VersioningType.FULL).build());
    when(storageEngineWriter.loadHOTPage(reference)).thenAnswer(_ -> retiredLoads.getAndIncrement() < RETIRED_RELOADS
        ? retiredLeafState(10L).page()
        : replacement.page());

    assertSame(modified, invokeCow(new TestIndexWriter(storageEngineWriter), reference, firstRetired.page()));
    assertEquals(RETIRED_RELOADS + 1, retiredLoads.get(), "every retired reload must be retried, not skipped");
    assertEquals(0, replacement.guards().get(), "the writer must release its guard exactly once");
    verify(firstRetired.page(), never()).copyForRevision(anyInt());
    verify(replacement.page(), times(1)).releaseGuard();
  }

  @Test
  void abstractIndexWriterRetiresDetachedSourceWhenCopyFailsBeforeTilAdmission() {
    final LeafState source = leafState(4L);
    final PageReference reference = reference(4L);
    final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);
    cache.put(reference, source.page());
    reference.setPage(source.page());
    final OutOfMemoryError copyFailure = new OutOfMemoryError("injected copy failure");
    when(source.page().copyForRevision(anyInt())).thenThrow(copyFailure);

    final TransactionIntentLog log = mock(TransactionIntentLog.class);
    when(log.get(reference)).thenReturn(null);
    final StorageEngineWriter storageEngineWriter = storageEngineWriter(log, cache);

    assertThrows(OutOfMemoryError.class,
        () -> invokeCow(new TestIndexWriter(storageEngineWriter), reference, source.page()));

    assertEquals(0, source.guards().get());
    assertTrue(source.orphaned().get());
    assertTrue(source.closed().get());
    assertEquals(0L, cache.size());
    verify(source.page(), times(1)).releaseGuard();
    verify(log, never()).put(same(reference), any(PageContainer.class));
    verify(storageEngineWriter).markTransactionRollbackOnly(same(copyFailure));
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
    when(source.page().copyForRevision(anyInt())).thenThrow(combineFailure);
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
  void abstractIndexWriterRetiresDetachedPagesWhenTilPutFailsBeforePublication() {
    final LeafState source = leafState(5L);
    final LeafState modified = leafState(6L);
    final PageReference reference = reference(5L);
    final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);
    cache.put(reference, source.page());
    reference.setPage(source.page());
    when(source.page().copyForRevision(anyInt())).thenReturn(modified.page());

    final TransactionIntentLog log = mock(TransactionIntentLog.class);
    when(log.get(reference)).thenReturn(null);
    doAnswer(_ -> {
      throw new IllegalStateException("injected pre-publication TIL failure");
    }).when(log).put(same(reference), any(PageContainer.class));

    final StorageEngineWriter storageEngineWriter = storageEngineWriter(log, cache);
    assertThrows(IllegalStateException.class,
        () -> invokeCow(new TestIndexWriter(storageEngineWriter), reference, source.page()));

    assertTrue(source.closed().get(), "the detached complete page has no owner after failed publication");
    assertTrue(modified.closed().get(), "the detached modified page has no owner after failed publication");
    assertEquals(0, source.guards().get());
    verify(source.page(), times(1)).retire();
    verify(modified.page(), times(1)).retire();
  }

  @Test
  void abstractIndexWriterPreservesPagesWhenTilPublishesBeforeThrowing() {
    final LeafState source = leafState(7L);
    final LeafState modified = leafState(8L);
    final PageReference reference = reference(7L);
    final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);
    cache.put(reference, source.page());
    reference.setPage(source.page());
    when(source.page().copyForRevision(anyInt())).thenReturn(modified.page());

    final AtomicReference<PageContainer> published = new AtomicReference<>();
    final TransactionIntentLog log = mock(TransactionIntentLog.class);
    when(log.get(reference)).thenAnswer(_ -> published.get());
    doAnswer(invocation -> {
      published.set(invocation.getArgument(1));
      throw new IllegalStateException("injected post-publication TIL failure");
    }).when(log).put(same(reference), any(PageContainer.class));

    final StorageEngineWriter storageEngineWriter = storageEngineWriter(log, cache);
    assertThrows(IllegalStateException.class,
        () -> invokeCow(new TestIndexWriter(storageEngineWriter), reference, source.page()));

    assertSame(source.page(), published.get().getComplete());
    assertSame(modified.page(), published.get().getModified());
    assertEquals(0, source.guards().get());
    assertFalse(source.orphaned().get(), "the published complete page remains TIL-owned");
    assertFalse(modified.orphaned().get(), "the published modified page remains TIL-owned");
    verify(source.page(), never()).retire();
    verify(modified.page(), never()).retire();
  }

  /**
   * A HOT leaf retired while a guard is still live is neither closed nor guardable — the deferred
   * teardown state — and resolution keeps handing that same instance back until the holder's last
   * release closes it. The writer therefore has to WAIT for the retiring thread, and waiting means
   * giving up the core: a retry that only yields keeps out-competing the very thread it is waiting
   * for, which on a small runner is a livelock rather than a slow wait.
   */
  @Test
  void abstractIndexWriterYieldsTheCoreToTheThreadRetiringTheLeaf() throws Exception {
    final LeafState retiring = leafState(11L);
    final LeafState replacement = leafState(11L);
    final HOTLeafPage modified = mock(HOTLeafPage.class);
    final PageReference reference = reference(11L);
    final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);

    // Deferred teardown: retire under a live guard, so the page is orphaned but NOT closed and stays
    // that way for as long as this test holds the guard.
    assertTrue(retiring.page().acquireGuard());
    retiring.page().retire();
    assertTrue(retiring.orphaned().get());
    assertFalse(retiring.closed().get(), "a guarded retirement must defer the close");

    final AtomicInteger guardAttempts = countGuardAttempts(retiring);
    final AtomicBoolean retirementCompleted = new AtomicBoolean();

    final TransactionIntentLog log = detachingLog(reference, replacement, modified, cache);
    when(replacement.page().copyForRevision(anyInt())).thenReturn(modified);
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class, RETURNS_DEEP_STUBS);
    when(storageEngineWriter.getLog()).thenReturn(log);
    attachCache(storageEngineWriter, cache);
    when(storageEngineWriter.getResourceSession().getResourceConfig()).thenReturn(
        ResourceConfiguration.newBuilder("hot-writer-guard-starvation")
                             .versioningApproach(VersioningType.FULL)
                             .build());
    when(storageEngineWriter.loadHOTPage(reference)).thenAnswer(_ -> retirementCompleted.get()
        ? replacement.page()
        : retiring.page());

    final Thread retiringThread = new Thread(() -> {
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(RETIREMENT_IN_FLIGHT_MILLIS));
      retirementCompleted.set(true);
    }, "hot-leaf-retirement");
    retiringThread.start();
    final long startedNanos = System.nanoTime();
    final long waitedMillis;
    try {
      assertSame(modified, invokeCow(new TestIndexWriter(storageEngineWriter), reference, retiring.page()),
          "the writer must outlast the in-flight retirement, not abort on it");
    } finally {
      waitedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
      retiringThread.join(TimeUnit.SECONDS.toMillis(10L));
      retiring.page().releaseGuard();
    }

    assertTrue(retirementCompleted.get(), "the retiring thread must have been scheduled while the writer waited");
    assertParkedRatherThanSpun(guardAttempts.get(), waitedMillis);
    verify(retiring.page(), never()).copyForRevision(anyInt());
    assertEquals(0, replacement.guards().get(), "the writer must release its guard exactly once");
  }

  /**
   * Assert the writer waited by parking rather than by spinning, as a RATE over however long the wait
   * actually lasted.
   *
   * @param attempts guard acquisitions the writer made while waiting
   * @param waitedMillis how long the writer waited
   */
  private static void assertParkedRatherThanSpun(final int attempts, final long waitedMillis) {
    final long ceiling = PRE_PARK_ATTEMPTS + MAX_ATTEMPTS_PER_MILLI_WHILE_RETIRING * Math.max(1L, waitedMillis);
    assertTrue(attempts <= ceiling, "the writer must park rather than spin while a retirement is in flight, but made "
        + attempts + " attempts in " + waitedMillis + " ms (ceiling " + ceiling + ")");
  }

  /**
   * Closing is terminal, so a reload that keeps producing a CLOSED leaf can never make progress.
   * Resolution really does hand one back — the transaction-intent log keeps containers whose HOT
   * leaves an incremental merge already released — so the writer must report the lost leaf on the
   * first reload instead of retrying it until the wall-clock budget runs out.
   */
  @Test
  void abstractIndexWriterReportsAClosedLeafInsteadOfRetryingIt() {
    final LeafState closedLeaf = leafState(13L);
    closedLeaf.page().retire();
    assertTrue(closedLeaf.closed().get(), "an unguarded retirement closes immediately");

    final PageReference reference = reference(13L);
    final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(1024L * 1024L);
    final TransactionIntentLog log = mock(TransactionIntentLog.class);
    when(log.get(reference)).thenReturn(null);
    final AtomicInteger reloads = new AtomicInteger();
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class, RETURNS_DEEP_STUBS);
    when(storageEngineWriter.getLog()).thenReturn(log);
    attachCache(storageEngineWriter, cache);
    when(storageEngineWriter.getResourceSession().getResourceConfig()).thenReturn(
        ResourceConfiguration.newBuilder("hot-writer-closed-reload").versioningApproach(VersioningType.FULL).build());
    when(storageEngineWriter.loadHOTPage(reference)).thenAnswer(_ -> {
      reloads.incrementAndGet();
      return closedLeaf.page();
    });

    final IllegalStateException thrown = assertThrows(IllegalStateException.class,
        () -> invokeCow(new TestIndexWriter(storageEngineWriter), reference, closedLeaf.page()));

    assertTrue(thrown.getMessage().contains("disappeared"),
        "a closed leaf is a lost leaf, not an exhausted retry budget: " + thrown.getMessage());
    assertEquals(1, reloads.get(), "a closed leaf must be reported on its first reload, not retried");
    verify(closedLeaf.page(), never()).copyForRevision(anyInt());
  }

  /**
   * Re-stub {@code acquireGuard} so the surrounding test can count how hard the writer retries,
   * keeping the guard semantics {@link #leafState(long)} installed.
   *
   * @param leaf the leaf whose guard acquisitions are counted
   * @return the live attempt counter
   */
  private static AtomicInteger countGuardAttempts(final LeafState leaf) {
    final AtomicInteger attempts = new AtomicInteger();
    when(leaf.page().acquireGuard()).thenAnswer(_ -> {
      attempts.incrementAndGet();
      if (leaf.closed().get() || leaf.orphaned().get()) {
        return false;
      }
      leaf.guards().incrementAndGet();
      if (leaf.closed().get() || leaf.orphaned().get()) {
        leaf.guards().decrementAndGet();
        return false;
      }
      return true;
    });
    return attempts;
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
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class, RETURNS_DEEP_STUBS);
    when(storageEngineWriter.getLog()).thenReturn(log);
    attachCache(storageEngineWriter, cache);
    when(storageEngineWriter.getResourceSession().getResourceConfig()).thenReturn(
        ResourceConfiguration.newBuilder("hot-writer-canonical").versioningApproach(VersioningType.FULL).build());
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

  /**
   * A freshly published leaf that eviction retired before anyone could guard it — the storm's unit of
   * work. Each call mints a distinct instance, because eviction retiring the SAME instance twice is
   * not a storm.
   *
   * @param pageKey the page key the instance reports
   * @return the retired leaf's state
   */
  private static LeafState retiredLeafState(final long pageKey) {
    final LeafState leaf = leafState(pageKey);
    leaf.page().retire();
    return leaf;
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

    private HOTLeafPage acquireReadLeaf(final PageReference reference) {
      rootReference = reference;
      return acquireLeafForRead(new byte[Long.BYTES]);
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
