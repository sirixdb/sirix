/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.PageContainer;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.io.StorageType;
import io.sirix.page.IndirectPage;
import io.sirix.page.OverflowPage;
import io.sirix.page.PageReference;
import io.sirix.settings.Constants;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.LoggerFactory;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression gate for the asynchronous snapshot flush's FAILURE path.
 *
 * <p>
 * A 100M-row ClickBench load stalled forever in {@code awaitPendingAsyncFlush}: a fault on the
 * flush path left the backpressure permit un-returned, the rollback that the fault triggered parked
 * on that permit with no worker left to hand it back, and the original exception — still travelling
 * as the primary of a try-with-resources whose close never returned — was never printed. Nothing at
 * all appeared in the log for two hours.
 *
 * <p>
 * Each test here pins one of the four properties that turns that silence into a fast, named
 * failure: the permit is returned on every path, the throwable is logged the moment it happens, the
 * writer is poisoned so the next request rethrows the REAL cause, and rollback completes instead of
 * hanging. The {@link Timeout} on every test is itself an assertion — a leaked permit now costs the
 * bounded wait rather than forever, and either way the test must not reach it. It runs in
 * {@code SEPARATE_THREAD} mode on purpose: the regression these guard against is an unbounded park,
 * and the default same-thread timeout only reports AFTER the method returns — which such a
 * regression never does, so it would wedge the whole suite instead of failing this one test.
 *
 * <p>
 * Verified non-vacuous against the pre-fix code (permit leak restored, cause consumed on read): the
 * two cause-chain tests fail with "the real cause was lost on the way out", and the
 * preparation-fault test parks indefinitely — the incident itself.
 */
final class AsyncFlushFailurePathTest {

  private static final String RESOURCE = "async-flush-failure-resource";

  /** Distinctive enough that finding it in a cause chain or a log line proves provenance. */
  private static final String FAULT_MESSAGE = "injected async-flush fault (test)";

  /**
   * Auto-commit threshold. Small enough that the 6,000-node import below rotates the log several
   * times, so a fault can be aimed at a flush that is NOT the first one — the case where a previous
   * flush has already set the in-flight flag and the await really does reach the semaphore.
   */
  private static final int MAX_NODES_BEFORE_FLUSH = 1024;

  private static final int INSERTED_RECORDS = 6_000;

  private ListAppender<ILoggingEvent> logAppender;
  private Logger writerLogger;
  private Level originalLevel;

  @Test
  @Timeout(value = 3, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  @DisplayName("Staged side payloads move to a fixed native reservoir and rollback closes it")
  void stagedSidePayloadUsesNativeReservoir() {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                             .storeDiffs(false)
                                             .hashKind(HashType.NONE)
                                             .buildPathSummary(false)
                                             .versioningApproach(VersioningType.FULL)
                                             .storageType(StorageType.FILE_CHANNEL)
                                             .build());

      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx()) {
        final NodeStorageEngineWriter engineWriter = (NodeStorageEngineWriter) wtx.getStorageEngineWriter();
        final byte[] payload = new byte[32 * 1024];
        for (int i = 0; i < payload.length; i++) {
          payload[i] = (byte) (i * 31);
        }
        final OverflowPage producerPage = new OverflowPage(payload);
        final PageReference reference = new PageReference();
        reference.setPage(producerPage);

        assertTrue(engineWriter.stageUncommittedOverflowPage(reference));
        final OverflowPage stagedPage = (OverflowPage) reference.getPage();
        assertNotSame(producerPage, stagedPage);
        assertFalse(stagedPage.isHeapBacked(),
            "the pending reference must not retain the producer's segment byte[] across young GCs");
        assertArrayEquals(payload, stagedPage.getDataBytes());
        final MemorySegment detachedCompatibilityView = stagedPage.getData();
        assertEquals(payload[17], detachedCompatibilityView.get(ValueLayout.JAVA_BYTE, 17));

        wtx.rollback();

        assertNull(reference.getPage());
        assertFalse(reference.hasPendingPageWrite());
        assertTrue(stagedPage.isClosed());
        assertThrows(IllegalStateException.class, stagedPage::getDataBytes,
            "rollback must invalidate stale views before its native reservoir can be reused");
        assertEquals(payload[17], detachedCompatibilityView.get(ValueLayout.JAVA_BYTE, 17),
            "compatibility callers receive a detached immutable view, never the reusable reservoir");

        assertTrue(engineWriter.isClosed(), "rollback must retire the old writer before replacing it");
        final PageReference staleWriterReference = new PageReference();
        final OverflowPage staleWriterPage = new OverflowPage(new byte[1024]);
        staleWriterReference.setPage(staleWriterPage);
        assertThrows(IllegalStateException.class, () -> engineWriter.stageUncommittedOverflowPage(staleWriterReference),
            "a stale writer handle must not allocate a new native reservoir after close");
        assertSame(staleWriterPage, staleWriterReference.getPage());
        assertEquals(0, engineWriter.stagedSidePageCount());
        assertEquals(0L, engineWriter.stagedSidePagePayloadBytes());
      }
    }
  }

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    NodeStorageEngineWriter.asyncFlushFaultHook = null;

    writerLogger = (Logger) LoggerFactory.getLogger(NodeStorageEngineWriter.class);
    originalLevel = writerLogger.getLevel();
    writerLogger.setLevel(Level.ERROR);
    logAppender = new ListAppender<>();
    logAppender.start();
    writerLogger.addAppender(logAppender);
  }

  @AfterEach
  void tearDown() {
    NodeStorageEngineWriter.asyncFlushFaultHook = null;
    if (writerLogger != null) {
      writerLogger.detachAppender(logAppender);
      writerLogger.setLevel(originalLevel);
    }
    if (logAppender != null) {
      logAppender.stop();
    }
    JsonTestHelper.deleteEverything();
  }

  @Test
  @Timeout(value = 3, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  @DisplayName("A flush worker that dies is logged at once, poisons the writer with the REAL cause, and leaves the permit balanced")
  void flushWorkerFailure_isLoudPoisonsTheWriterAndBalancesThePermit() {
    final AtomicReference<NodeStorageEngineWriter> writer = new AtomicReference<>();
    final AtomicInteger faults = new AtomicInteger();
    NodeStorageEngineWriter.asyncFlushFaultHook = (engineWriter, site) -> {
      if ("write".equals(site)) {
        writer.compareAndSet(null, engineWriter);
        faults.incrementAndGet();
        throw new IllegalStateException(FAULT_MESSAGE);
      }
    };

    final Throwable thrown = assertThrows(Throwable.class, this::runAsyncFlushImport,
        "a flush whose worker died must fail the import, not let it look successful");

    assertTrue(faults.get() > 0, "the fault hook never fired — the test proved nothing");
    assertCauseChainContains(thrown, FAULT_MESSAGE);
    assertLoggedAtError("Background snapshot flush FAILED");

    final NodeStorageEngineWriter engineWriter = writer.get();
    assertNotNull(engineWriter, "the hook must have captured the writer it fired on");
    assertEquals(1, engineWriter.availableFlushPermits(),
        "the flush permit must be back after everything closed — a missing permit is the hang");
    assertTrue(engineWriter.isClosed(),
        "rollback after a fenced worker failure must still let the resource session close the poisoned writer");
  }

  @Test
  @Timeout(value = 3, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  @DisplayName("A fault while STARTING a flush hands the permit back, so the rollback it triggers completes instead of parking")
  void flushPreparationFailure_doesNotLeakThePermit_soRollbackCompletes() {
    // Aim at the third flush: by then an earlier flush has set the in-flight flag, so the rollback
    // that follows really does reach the semaphore. This is the shape that hung the 100M load —
    // before the fix the permit taken here was never returned and the await parked forever.
    final AtomicReference<NodeStorageEngineWriter> writer = new AtomicReference<>();
    final AtomicInteger prepareCalls = new AtomicInteger();
    NodeStorageEngineWriter.asyncFlushFaultHook = (engineWriter, site) -> {
      if ("prepare".equals(site) && prepareCalls.incrementAndGet() == 3) {
        writer.set(engineWriter);
        throw new IllegalStateException(FAULT_MESSAGE);
      }
    };

    final Throwable thrown = assertThrows(Throwable.class, this::runAsyncFlushImport,
        "a flush that cannot even be started must fail the import");

    assertTrue(prepareCalls.get() >= 3, "the import did not rotate often enough to arm the fault");
    assertCauseChainContains(thrown, FAULT_MESSAGE);
    assertLoggedAtError("Async snapshot flush could not be started");

    final NodeStorageEngineWriter engineWriter = writer.get();
    assertNotNull(engineWriter, "the hook must have captured the writer it fired on");
    assertEquals(1, engineWriter.availableFlushPermits(),
        "the permit taken by the failed flush must have been handed back by the finally");
  }

  @Test
  @Timeout(value = 3, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  @DisplayName("A poisoned writer keeps reporting the original cause, and never reports a bare latch")
  void poisonedWriter_rethrowsTheOriginalCauseOnEverySubsequentRequest() {
    final AtomicReference<NodeStorageEngineWriter> writer = new AtomicReference<>();
    NodeStorageEngineWriter.asyncFlushFaultHook = (engineWriter, site) -> {
      if ("write".equals(site)) {
        writer.compareAndSet(null, engineWriter);
        throw new IllegalStateException(FAULT_MESSAGE);
      }
    };

    assertThrows(Throwable.class, this::runAsyncFlushImport);

    final NodeStorageEngineWriter engineWriter = writer.get();
    assertNotNull(engineWriter, "the hook must have captured the writer it fired on");

    // Disarm, so what follows can only come from the latched state, never from a fresh fault.
    NodeStorageEngineWriter.asyncFlushFaultHook = null;

    // The writer is closed and latched. Both entry points must still name the original fault
    // rather than answering with the terminal-failure latch alone, which is what the pre-fix code
    // did once it had consumed (and discarded) the throwable.
    assertCauseChainContains(assertThrows(Throwable.class, engineWriter::asyncFlush), FAULT_MESSAGE);
    assertCauseChainContains(assertThrows(Throwable.class, engineWriter::awaitPendingAsyncFlush), FAULT_MESSAGE);
  }

  @Test
  @Timeout(value = 3, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  @DisplayName("Rollback cancels staged side payloads before later teardown failures and preserves the first cause")
  void rollbackFailure_discardsSideBatchesFirstAndRetainsTheFirstCause() {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                             .storeDiffs(false)
                                             .hashKind(HashType.NONE)
                                             .buildPathSummary(false)
                                             .versioningApproach(VersioningType.FULL)
                                             .storageType(StorageType.FILE_CHANNEL)
                                             .build());

      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx()) {
        final NodeStorageEngineWriter engineWriter = (NodeStorageEngineWriter) wtx.getStorageEngineWriter();
        final ProjectionIndexHOTStorage storage = ProjectionIndexHOTStorage.forBulkBuild(engineWriter, 0);
        storage.putBlob(0, new byte[8 * 1024]);

        assertEquals(1, engineWriter.stagedSidePageCount());
        assertTrue(engineWriter.stagedSidePagePayloadBytes() > 0L);

        NodeStorageEngineWriter.asyncFlushFaultHook = (writer, site) -> {
          if ("rollback-after-guard-close".equals(site)) {
            throw new IllegalStateException("injected guard cleanup failure");
          }
          if ("rollback-after-log-clear".equals(site)) {
            throw new IllegalArgumentException("injected log cleanup failure");
          }
        };

        final IllegalStateException thrown;
        try {
          thrown = assertThrows(IllegalStateException.class, engineWriter::rollback);
        } finally {
          NodeStorageEngineWriter.asyncFlushFaultHook = null;
        }

        assertEquals("injected guard cleanup failure", thrown.getMessage());
        assertEquals(1, thrown.getSuppressed().length);
        assertEquals("injected log cleanup failure", thrown.getSuppressed()[0].getMessage());
        assertEquals(0, engineWriter.stagedSidePageCount(),
            "rollback must drop both reusable side arrays before guard/TIL teardown");
        assertEquals(0L, engineWriter.stagedSidePagePayloadBytes(),
            "rollback must release every pending OverflowPage payload before later teardown can fail");
      }
    }
  }

  @Test
  @Timeout(value = 3, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  @DisplayName("Rollback fences a failed side-page worker, releases its payloads, and installs a fresh writer")
  void workerFailure_rollbackCompletesAndReplacesThePoisonedWriter() {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                             .storeDiffs(false)
                                             .hashKind(HashType.NONE)
                                             .buildPathSummary(false)
                                             .versioningApproach(VersioningType.FULL)
                                             .storageType(StorageType.FILE_CHANNEL)
                                             .build());

      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx()) {
        final NodeStorageEngineWriter failedWriter = (NodeStorageEngineWriter) wtx.getStorageEngineWriter();
        final ProjectionIndexHOTStorage storage = ProjectionIndexHOTStorage.forBulkBuild(failedWriter, 0);
        storage.putBlob(0, new byte[8 * 1024]);
        assertEquals(1, failedWriter.stagedSidePageCount());

        final AtomicInteger faults = new AtomicInteger();
        NodeStorageEngineWriter.asyncFlushFaultHook = (writer, site) -> {
          if (writer == failedWriter && "write".equals(site)) {
            faults.incrementAndGet();
            throw new IllegalStateException(FAULT_MESSAGE);
          }
        };

        try {
          failedWriter.asyncFlush();
          wtx.rollback();
        } finally {
          NodeStorageEngineWriter.asyncFlushFaultHook = null;
        }

        assertEquals(1, faults.get(), "the append worker fault must fire exactly once");
        assertTrue(failedWriter.isClosed(), "the poisoned writer must be closed after rollback replaces it");
        assertEquals(0, failedWriter.stagedSidePageCount());
        assertEquals(0L, failedWriter.stagedSidePagePayloadBytes());

        final NodeStorageEngineWriter replacementWriter = (NodeStorageEngineWriter) wtx.getStorageEngineWriter();
        assertNotSame(failedWriter, replacementWriter,
            "rollback must bind the transaction to a fresh writer after aborting the poisoned one");
        assertFalse(replacementWriter.isClosed(), "the replacement writer must remain usable");
        wtx.rollback();
      }
    }
  }

  @Test
  @Timeout(value = 3, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  @DisplayName("A mid-side-write failure publishes no offsets and invalidates every native view")
  void sideWriteFailureCancelsTheWholeNativeBatch() {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                             .storeDiffs(false)
                                             .hashKind(HashType.NONE)
                                             .buildPathSummary(false)
                                             .versioningApproach(VersioningType.FULL)
                                             .storageType(StorageType.FILE_CHANNEL)
                                             .build());

      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx()) {
        final NodeStorageEngineWriter failedWriter = (NodeStorageEngineWriter) wtx.getStorageEngineWriter();
        final PageReference[] references = new PageReference[3];
        final OverflowPage[] stagedPages = new OverflowPage[3];
        for (int i = 0; i < references.length; i++) {
          final byte[] payload = new byte[8 * 1024 + i * 257];
          for (int offset = 0; offset < payload.length; offset++) {
            payload[offset] = (byte) (offset * 17 + i);
          }
          final PageReference reference = new PageReference();
          reference.setPage(new OverflowPage(payload));
          assertTrue(failedWriter.stageUncommittedOverflowPage(reference));
          references[i] = reference;
          stagedPages[i] = (OverflowPage) reference.getPage();
          assertFalse(stagedPages[i].isHeapBacked());
        }

        final AtomicInteger writes = new AtomicInteger();
        NodeStorageEngineWriter.asyncFlushFaultHook = (writer, site) -> {
          if (writer == failedWriter && "side-write".equals(site) && writes.incrementAndGet() == 2) {
            throw new IllegalStateException(FAULT_MESSAGE);
          }
        };

        try {
          failedWriter.asyncFlush();
          wtx.rollback();
        } finally {
          NodeStorageEngineWriter.asyncFlushFaultHook = null;
        }

        assertEquals(2, writes.get());
        assertTrue(failedWriter.isClosed());
        assertEquals(0, failedWriter.stagedSidePageCount());
        assertEquals(0L, failedWriter.stagedSidePagePayloadBytes());
        for (int i = 0; i < references.length; i++) {
          assertEquals(Constants.NULL_ID_LONG, references[i].getKey(),
              "a partial batch must never publish offset " + i);
          assertFalse(references[i].hasPendingPageWrite());
          assertNull(references[i].getPage());
          assertTrue(stagedPages[i].isClosed());
          assertThrows(IllegalStateException.class, stagedPages[i]::getDataBytes);
        }
      }
    }
  }

  @Test
  @Timeout(value = 3, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  @DisplayName("Close releases every independent owner and publishes terminal flags after earlier teardown failures")
  void closeFailure_attemptsEveryCleanupAndRetainsTheFirstCause() {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                             .storeDiffs(false)
                                             .hashKind(HashType.NONE)
                                             .buildPathSummary(false)
                                             .versioningApproach(VersioningType.FULL)
                                             .storageType(StorageType.FILE_CHANNEL)
                                             .build());

      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx()) {
        final NodeStorageEngineWriter engineWriter = (NodeStorageEngineWriter) wtx.getStorageEngineWriter();
        final ProjectionIndexHOTStorage storage = ProjectionIndexHOTStorage.forBulkBuild(engineWriter, 0);
        storage.putBlob(0, new byte[8 * 1024]);
        assertEquals(1, engineWriter.stagedSidePageCount());

        final AtomicInteger completedSteps = new AtomicInteger();
        NodeStorageEngineWriter.asyncFlushFaultHook = (writer, site) -> {
          if (writer != engineWriter) {
            return;
          }
          switch (site) {
            case "close-after-side-batch-discard" -> {
              completedSteps.addAndGet(1);
              throw new IllegalStateException("injected side-batch close failure");
            }
            case "close-after-reader-close" -> {
              completedSteps.addAndGet(2);
              throw new IllegalArgumentException("injected reader close failure");
            }
            case "close-after-log-close" -> completedSteps.addAndGet(4);
            case "close-after-storage-writer-close" -> completedSteps.addAndGet(8);
            case "close-after-buffer-release" -> completedSteps.addAndGet(16);
            case "close-after-terminal-flags" -> completedSteps.addAndGet(32);
            default -> {
            }
          }
        };

        final IllegalStateException thrown;
        try {
          thrown = assertThrows(IllegalStateException.class, engineWriter::close);
        } finally {
          NodeStorageEngineWriter.asyncFlushFaultHook = null;
        }

        assertEquals("injected side-batch close failure", thrown.getMessage());
        assertEquals(1, thrown.getSuppressed().length);
        assertEquals("injected reader close failure", thrown.getSuppressed()[0].getMessage());
        assertEquals(63, completedSteps.get(),
            "an early close failure must not skip log, backend writer, buffer, or terminal publication");
        assertEquals(0, engineWriter.stagedSidePageCount());
        assertEquals(0L, engineWriter.stagedSidePagePayloadBytes());
        assertTrue(engineWriter.isClosed());

        // A teardown error is reported after the writer has become terminally closed; retry is a no-op.
        engineWriter.close();
      }
    }
  }

  @Test
  @Timeout(value = 3, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  @DisplayName("An unbound writer retains the resource permit until aborted trie offsets are invalidated and closed")
  void unboundCloseBlocksSuccessorUntilAbortedTrieTailIsSafeToReuse() throws Exception {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                             .storeDiffs(false)
                                             .hashKind(HashType.NONE)
                                             .buildPathSummary(false)
                                             .versioningApproach(VersioningType.FULL)
                                             .storageType(StorageType.FILE_CHANNEL)
                                             .build());

      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        final NodeStorageEngineWriter failedWriter = (NodeStorageEngineWriter) session.createStorageEngineWriter();
        final PageReference reference =
            new PageReference().setDatabaseId(failedWriter.getDatabaseId()).setResourceId(failedWriter.getResourceId());
        final IndirectPage page = new IndirectPage();
        failedWriter.getLog().put(reference, PageContainer.getInstance(page, page));

        // The first epoch pins structural pages; the second can retire this childless indirect page
        // through the foreground direct-write path. Prove the regression is not merely exercising an
        // ordinary unbound close with no reclaimable offset in play.
        failedWriter.asyncFlush();
        failedWriter.awaitPendingAsyncFlush();
        failedWriter.asyncFlush();
        failedWriter.awaitPendingAsyncFlush();
        assertTrue(reference.getKey() != Constants.NULL_ID_LONG,
            "the setup never published the pinned indirect page, so no aborted tail could be reused");
        assertEquals(Constants.NULL_ID_INT, reference.getLogKey());

        final CountDownLatch closeBeforeInvalidation = new CountDownLatch(1);
        final CountDownLatch allowCloseToFinish = new CountDownLatch(1);
        final AtomicInteger safetyBarriers = new AtomicInteger();
        NodeStorageEngineWriter.asyncFlushFaultHook = (writer, site) -> {
          if (writer != failedWriter) {
            return;
          }
          switch (site) {
            case "close-after-log-close" -> {
              closeBeforeInvalidation.countDown();
              awaitLatch(allowCloseToFinish, "test did not release the paused unbound close");
            }
            case "close-after-trie-cache-invalidate" -> safetyBarriers.compareAndSet(0, 1);
            case "close-after-storage-writer-close" -> safetyBarriers.compareAndSet(1, 2);
            default -> {
            }
          }
        };

        final ExecutorService executor = Executors.newFixedThreadPool(2);
        StorageEngineWriter successor = null;
        Future<?> closeFuture = null;
        Future<StorageEngineWriter> successorFuture = null;
        try {
          closeFuture = executor.submit(failedWriter::close);
          assertTrue(closeBeforeInvalidation.await(30, TimeUnit.SECONDS),
              "unbound close never reached the pre-invalidation boundary");

          final CountDownLatch successorStarted = new CountDownLatch(1);
          successorFuture = executor.submit(() -> {
            successorStarted.countDown();
            return session.createStorageEngineWriter();
          });
          assertTrue(successorStarted.await(30, TimeUnit.SECONDS), "successor task never started");
          final Future<StorageEngineWriter> blockedSuccessor = successorFuture;
          assertThrows(TimeoutException.class, () -> blockedSuccessor.get(2, TimeUnit.SECONDS),
              "a successor acquired the resource while aborted-offset caches were still live");

          allowCloseToFinish.countDown();
          closeFuture.get(30, TimeUnit.SECONDS);
          successor = successorFuture.get(30, TimeUnit.SECONDS);
          assertEquals(2, safetyBarriers.get(),
              "the successor permit must be handed off only after cache invalidation and backend close");
        } finally {
          allowCloseToFinish.countDown();
          NodeStorageEngineWriter.asyncFlushFaultHook = null;
          if (closeFuture != null && !closeFuture.isDone()) {
            closeFuture.get(30, TimeUnit.SECONDS);
          }
          if (successor == null && successorFuture != null && successorFuture.isDone()) {
            successor = successorFuture.get();
          }
          if (successor != null) {
            successor.close();
          }
          executor.shutdownNow();
          assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        }
      }
    }
  }

  private static void awaitLatch(final CountDownLatch latch, final String timeoutMessage) {
    try {
      if (!latch.await(30, TimeUnit.SECONDS)) {
        throw new AssertionError(timeoutMessage);
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("interrupted while waiting for test coordination", e);
    }
  }

  /**
   * A {@code KEEP_OPEN_ASYNC_FLUSH} import large enough to rotate the transaction log several times.
   * Mirrors the shape of the bulk loaders: one long transaction, background flushes, a single commit
   * at the end.
   */
  private void runAsyncFlushImport() {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                             .storeDiffs(false)
                                             .hashKind(HashType.NONE)
                                             .buildPathSummary(false)
                                             .versioningApproach(VersioningType.FULL)
                                             .storageType(StorageType.FILE_CHANNEL)
                                             .build());

      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx =
              session.beginNodeTrx(MAX_NODES_BEFORE_FLUSH, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
        final long arrayNodeKey = wtx.insertArrayAsFirstChild().getNodeKey();
        for (int i = 0; i < INSERTED_RECORDS; i++) {
          wtx.moveTo(arrayNodeKey);
          wtx.insertStringValueAsFirstChild("item-" + i);
        }
        wtx.commit();
      }
    }
  }

  private static void assertCauseChainContains(final Throwable thrown, final String needle) {
    for (Throwable t = thrown; t != null; t = t.getCause() == t
        ? null
        : t.getCause()) {
      if (t.getMessage() != null && t.getMessage().contains(needle)) {
        return;
      }
    }
    throw new AssertionError("no exception in the chain names \"" + needle
        + "\" — the real cause was lost on the way out. Chain: " + describe(thrown));
  }

  private void assertLoggedAtError(final String needle) {
    final boolean found =
        logAppender.list.stream()
                        .anyMatch(
                            event -> event.getLevel() == Level.ERROR && event.getFormattedMessage().contains(needle));
    if (!found) {
      throw new AssertionError("nothing was logged at ERROR containing \"" + needle
          + "\" — a flush failure that says nothing is the original defect. Saw: " + logAppender.list);
    }
  }

  private static String describe(final Throwable thrown) {
    final StringBuilder chain = new StringBuilder(256);
    for (Throwable t = thrown; t != null; t = t.getCause() == t
        ? null
        : t.getCause()) {
      if (!chain.isEmpty()) {
        chain.append(" <- ");
      }
      chain.append(t.getClass().getSimpleName()).append('(').append(t.getMessage()).append(')');
    }
    return chain.toString();
  }
}
