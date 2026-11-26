package io.sirix.access.trx.node;

import io.brackit.query.atomic.QNm;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.User;
import io.sirix.api.Database;
import io.sirix.api.NodeTrx;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.ResourceSession;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.cache.PageGuard;
import io.sirix.exception.SirixUsageException;
import io.sirix.exception.SirixIOException;
import io.sirix.io.StorageType;
import io.sirix.io.IOStorage;
import io.sirix.io.Writer;
import io.sirix.index.IndexType;
import java.util.concurrent.atomic.AtomicBoolean;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.IntConsumer;
import org.mockito.MockedStatic;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;

/**
 * Recovery/admission oracles: TXN-607 and V05/V06, as specified by docs/operations.md (Marker
 * recovery), docs/hooks.md and ResourceSession writer-admission contracts.
 */
@Isolated
@Timeout(30)
@Order(Integer.MAX_VALUE)
final class NodeTransactionAbortLifecycleTest {

  @TempDir
  Path directory;

  private Field flushHook;

  @AfterEach
  void clearHooks() throws ReflectiveOperationException {
    AbstractNodeTrxImpl.asyncCommitTestHook = null;
    AbstractResourceSession.recoveryTestHook = null;
    if (flushHook != null) {
      flushHook.set(null, null);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void terminalHardeningFailureAbortsDirtySuccessorAfterTheWorkerFence(final String format) throws Exception {
    final CountDownLatch entered = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final IllegalStateException hardeningFailure = new IllegalStateException("hardening failed");
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx transaction = fixture.first.beginNodeTrx(1, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT);
      final StorageEngineWriter predecessor = transaction.getStorageEngineWriter();
      final ScheduledExecutorService scheduler = scheduler(transaction);
      AbstractNodeTrxImpl.asyncCommitTestHook = stage -> {
        if (stage.equals("before-harden")) {
          entered.countDown();
          await(release);
          throw hardeningFailure;
        }
      };
      final AtomicReference<Throwable> observed = new AtomicReference<>();
      final Thread owner = new Thread(() -> {
        try {
          fixture.first.close();
        } catch (final Throwable failure) {
          observed.set(failure);
        }
      });
      try {
        setValue(transaction, 1);
        setValue(transaction, 2);
        entered.await();
        final StorageEngineWriter successor = transaction.getStorageEngineWriter();
        assertEquals(2, value(transaction));
        owner.start();
        final Semaphore permit = (Semaphore) field(AbstractNodeTrxImpl.class, "asyncCommitPermit").get(transaction);
        while (!permit.hasQueuedThreads()) {
          if (!owner.isAlive()) {
            fail("session teardown returned before the hardening worker was released");
          }
          if (Thread.interrupted()) {
            throw new InterruptedException();
          }
          Thread.onSpinWait();
        }
        assertFalse(transaction.isClosed());
        assertFalse(predecessor.isClosed());
        assertFalse(successor.isClosed());
        assertFalse(scheduler.isShutdown());
        assertTrue(fixture.second.hasRunningNodeWriteTrx());
        release.countDown();
        owner.join();

        assertSame(hardeningFailure, observed.get().getCause());
        assertTrue(fixture.first.isClosed());
        assertTrue(transaction.isClosed());
        assertTrue(predecessor.isClosed());
        assertTrue(successor.isClosed());
        assertTrue(scheduler.isTerminated());
        assertNull(((InternalResourceSession<?, ?>) fixture.second).getPendingRevisionRoot(2));
        assertEquals(1, fixture.second.getMostRecentRevisionNumber());
        assertTrue(Files.exists(((InternalResourceSession<?, ?>) fixture.second).getCommitFile()));
        assertWriterReusable(fixture);
      } finally {
        release.countDown();
        owner.join();
        AbstractNodeTrxImpl.asyncCommitTestHook = null;
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"json,false", "json,true", "xml,false", "xml,true"})
  void stalledFlushRetainsNodeOwnershipUntilWorkerExitAndRetry(final String format,
      final boolean workerExitsBeforePageClose) throws Exception {
    final CountDownLatch entered = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx transaction = fixture.first.beginNodeTrx();
      final StorageEngineWriter writer = transaction.getStorageEngineWriter();
      final ScheduledExecutorService scheduler = scheduler(transaction);
      final Semaphore permit = (Semaphore) field(writer.getClass(), "flushPermit").get(writer);
      final BiConsumer<StorageEngineWriter, String> hook = (owner, site) -> {
        if (owner == writer && site.equals("write")) {
          entered.countDown();
          await(release);
        }
      };
      flushHook = field(writer.getClass(), "asyncFlushFaultHook");
      flushHook.set(null, hook);
      final StorageEngineWriter bound = mock(StorageEngineWriter.class, delegatesTo(writer));
      doAnswer(invocation -> {
        try {
          writer.close();
        } catch (final RuntimeException failure) {
          if (workerExitsBeforePageClose) {
            release.countDown();
            permit.acquireUninterruptibly();
            permit.release();
          }
          throw failure;
        }
        return null;
      }).when(bound).close();
      field(AbstractNodeTrxImpl.class, "storageEngineWriter").set(transaction, bound);
      final InternalNodeReadOnlyTrx<?> reader =
          (InternalNodeReadOnlyTrx<?>) field(AbstractNodeTrxImpl.class, "nodeReadOnlyTrx").get(transaction);
      reader.setPageReadTransaction(bound);
      ((AbstractResourceSession<?, ?>) fixture.first).storageEngineWriterMap.put(transaction.getId(), bound);
      try {
        setValue(transaction, 1);
        writer.asyncFlush();
        entered.await();
        final IllegalStateException timeout = new IllegalStateException("stalled flush timeout");
        field(writer.getClass(), "asyncFlushError").set(writer, timeout);
        field(writer.getClass(), "asyncFlushTimedOut").setBoolean(writer, true);
        final RuntimeException observed = assertThrows(RuntimeException.class, fixture.first::close);
        assertSame(timeout, observed.getCause());
        assertFalse(fixture.first.isClosed());
        assertFalse(transaction.isClosed());
        assertFalse(scheduler.isShutdown());
        assertTrue(fixture.second.hasRunningNodeWriteTrx());
        assertFalse(writer.isClosed());
        release.countDown();
        permit.acquireUninterruptibly();
        permit.release();

        fixture.first.close();
        assertTrue(fixture.first.isClosed());
        assertTrue(transaction.isClosed());
        assertTrue(writer.isClosed());
        assertTrue(scheduler.isTerminated());
        assertWriterReusable(fixture);
      } finally {
        release.countDown();
        permit.acquireUninterruptibly();
        permit.release();
        flushHook.set(null, null);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void publicCloseStillRejectsDirtyTransactions(final String format) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx transaction = fixture.first.beginNodeTrx();
      setValue(transaction, 1);
      assertThrows(SirixUsageException.class, transaction::close);
      assertFalse(transaction.isClosed());
      assertEquals(1, value(transaction));
      assertTrue(fixture.second.hasRunningNodeWriteTrx());
      fixture.first.close();
      assertTrue(transaction.isClosed());
      assertTrue(scheduler(transaction).isTerminated());
      assertWriterReusable(fixture);
    }
  }

  @ParameterizedTest
  @CsvSource({"json,before-successor", "xml,before-successor", "json,submission", "xml,submission"})
  void inlineDoubleFailureReleasesSlotAndRetainsPrimary(final String format, final String failingStage)
      throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx transaction = fixture.first.beginNodeTrx(1, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT);
      final RuntimeException primary = failingStage.equals("submission")
          ? new RejectedExecutionException("submission rejected before worker creation")
          : new IllegalStateException(failingStage);
      final IllegalArgumentException hardening = new IllegalArgumentException("hardening");
      if (failingStage.equals("before-successor")) {
        flushHook = field(transaction.getStorageEngineWriter().getClass(), "asyncFlushFaultHook");
        flushHook.set(null, (BiConsumer<StorageEngineWriter, String>) (owner, stage) -> {
          if (stage.equals("constructor-before-local-caches")) {
            throw primary;
          }
        });
      }
      AbstractNodeTrxImpl.asyncCommitTestHook = stage -> {
        if (stage.equals("before-harden")) {
          throw hardening;
        }
      };
      setValue(transaction, 1);
      if (failingStage.equals("submission")) {
        try (MockedStatic<CompletableFuture> submission = mockStatic(CompletableFuture.class, CALLS_REAL_METHODS)) {
          submission.when(() -> CompletableFuture.runAsync(any(Runnable.class))).thenThrow(primary);
          assertSame(primary, assertThrows(RejectedExecutionException.class, () -> setValue(transaction, 2)));
        }
      } else {
        assertSame(primary, assertThrows(IllegalStateException.class, () -> setValue(transaction, 2)));
      }
      AbstractNodeTrxImpl.asyncCommitTestHook = null;
      if (flushHook != null) {
        flushHook.set(null, null);
      }
      assertSame(hardening, primary.getSuppressed()[0]);
      for (int attempt = 0; attempt < 2; attempt++) {
        assertSame(primary, assertThrows(SirixIOException.class, transaction::awaitPendingAsyncCommit).getCause());
      }
      assertThrows(RuntimeException.class, fixture.first::close);
      assertTrue(transaction.isClosed());
      assertTrue(scheduler(transaction).isTerminated());
      assertTrue(Files.exists(((InternalResourceSession<?, ?>) fixture.second).getCommitFile()));
      assertWriterReusable(fixture);
    }
  }

  @ParameterizedTest
  @CsvSource({"json,false,close-after-side-batch-discard", "json,true,close-after-side-batch-discard",
      "xml,false,close-after-side-batch-discard", "xml,true,close-after-side-batch-discard",
      "json,false,close-after-reader-close", "json,true,close-after-reader-close", "xml,false,close-after-reader-close",
      "xml,true,close-after-reader-close", "json,false,close-after-log-close", "json,true,close-after-log-close",
      "xml,false,close-after-log-close", "xml,true,close-after-log-close",
      "json,false,close-after-storage-writer-close", "json,true,close-after-storage-writer-close",
      "xml,false,close-after-storage-writer-close", "xml,true,close-after-storage-writer-close",
      "json,false,close-after-buffer-release", "json,true,close-after-buffer-release",
      "xml,false,close-after-buffer-release", "xml,true,close-after-buffer-release",
      "json,false,close-after-terminal-flags", "json,true,close-after-terminal-flags",
      "xml,false,close-after-terminal-flags", "xml,true,close-after-terminal-flags"})
  void terminalWriterFailureRequiresRecoveryWithoutStrandingAdmission(final String format, final boolean unbound,
      final String failingStage) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx transaction = unbound
          ? null
          : fixture.first.beginNodeTrx();
      final StorageEngineWriter writer = unbound
          ? fixture.first.createStorageEngineWriter(1)
          : transaction.getStorageEngineWriter();
      final IllegalStateException failure = new IllegalStateException("backend cleanup failed");
      flushHook = field(writer.getClass(), "asyncFlushFaultHook");
      flushHook.set(null, (BiConsumer<StorageEngineWriter, String>) (owner, stage) -> {
        if (owner == writer && stage.equals(failingStage)) {
          throw failure;
        }
      });
      if (unbound) {
        assertSame(failure, assertThrows(RuntimeException.class, writer::close));
      } else {
        assertSame(failure, assertThrows(RuntimeException.class, transaction::close));
        assertTrue(scheduler(transaction).isTerminated());
        transaction.close();
      }
      flushHook.set(null, null);
      assertTrue(writer.isClosed());
      assertFalse(fixture.second.hasRunningNodeWriteTrx());
      assertTrue(Files.exists(((InternalResourceSession<?, ?>) fixture.second).getCommitFile()));
      assertWriterReusable(fixture);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void failedMarkerCreationSurvivesSessionClose(final String format) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx transaction = fixture.first.beginNodeTrx();
      final Path marker = ((InternalResourceSession<?, ?>) fixture.first).getCommitFile();
      Files.createDirectory(marker);
      final IllegalStateException failure = new IllegalStateException("backend cleanup failed");
      final StorageEngineWriter writer = transaction.getStorageEngineWriter();
      flushHook = field(writer.getClass(), "asyncFlushFaultHook");
      flushHook.set(null, (BiConsumer<StorageEngineWriter, String>) (owner, stage) -> {
        if (owner == writer && stage.equals("close-after-storage-writer-close")) {
          throw failure;
        }
      });
      assertSame(failure, assertThrows(RuntimeException.class, transaction::close));
      flushHook.set(null, null);
      fixture.first.close();
      final RuntimeException refused = assertThrows(RuntimeException.class, fixture.second::beginNodeTrx);
      assertTrue(refused.getMessage().contains(marker.toString()));
      assertFalse(fixture.second.hasRunningNodeWriteTrx());
      Files.delete(marker);
      assertWriterReusable(fixture);
    }
  }

  @ParameterizedTest
  @CsvSource({"json,truncation", "xml,truncation", "json,index catalogue invalidation",
      "xml,index catalogue invalidation", "json,resource cache invalidation", "xml,resource cache invalidation"})
  void recoveryFailureRejectsAdmissionAndCanBeRetried(final String format, final String failingStage) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final InternalResourceSession<?, ?> session = (InternalResourceSession<?, ?>) fixture.second;
      session.requireCommitRecovery();
      final Path data = fixture.second.getResourceConfig()
                                      .getResource()
                                      .resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
                                      .resolve("sirix.data");
      final long durableSize = Files.size(data);
      Files.write(data, new byte[4096], StandardOpenOption.APPEND);
      final IllegalStateException failure = new IllegalStateException(failingStage);
      AbstractResourceSession.recoveryTestHook = stage -> {
        if (stage.equals(failingStage)) {
          throw failure;
        }
      };
      final RuntimeException refused = assertThrows(RuntimeException.class, fixture.second::beginNodeTrx);
      assertSame(failure, refused.getCause());
      assertTrue(refused.getMessage().contains(session.getCommitFile().toString()));
      assertTrue(refused.getMessage().contains(failingStage));
      assertTrue(Files.exists(session.getCommitFile()));
      assertFalse(fixture.second.hasRunningNodeWriteTrx());
      AbstractResourceSession.recoveryTestHook = null;
      try (NodeTrx next = fixture.second.beginNodeTrx()) {
        assertTrue(Files.size(data) <= durableSize);
        setCursor(next);
        assertEquals(0, value(next));
      }
      assertWriterReusable(fixture);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void postBeaconCleanupFailureRecoversPublishedRevision(final String format) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx transaction = fixture.first.beginNodeTrx(1, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT);
      final StorageEngineWriter predecessor = transaction.getStorageEngineWriter();
      flushHook = field(predecessor.getClass(), "asyncFlushFaultHook");
      final IllegalStateException failure = new IllegalStateException("post-beacon close");
      final AtomicBoolean fired = new AtomicBoolean();
      flushHook.set(null, (BiConsumer<StorageEngineWriter, String>) (owner, stage) -> {
        if (owner == predecessor && stage.equals("close-after-storage-writer-close")) {
          fired.set(true);
          throw failure;
        }
      });
      final CountDownLatch release = new CountDownLatch(1);
      AbstractNodeTrxImpl.asyncCommitTestHook = stage -> {
        if (stage.equals("before-harden")) {
          await(release);
        }
      };
      try {
        setValue(transaction, 1);
        setValue(transaction, 2);
      } finally {
        release.countDown();
      }
      assertDoesNotThrow(transaction::awaitPendingAsyncCommit);
      assertTrue(fired.get(), "the retired-writer failure must actually fire");
      assertThrows(IllegalStateException.class, transaction::commit);
      assertThrows(IllegalStateException.class, transaction::rollback);
      assertThrows(SirixUsageException.class, transaction::close);
      assertTrue(Files.exists(((InternalResourceSession<?, ?>) fixture.first).getCommitFile()));
      flushHook.set(null, null);
      assertDoesNotThrow(fixture.first::close);
      assertTrue(scheduler(transaction).isTerminated());
      assertEquals(2, fixture.second.getMostRecentRevisionNumber());
      try (NodeTrx next = fixture.second.beginNodeTrx()) {
        setCursor(next);
        assertEquals(1, value(next));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void readerGuardFailureStillReleasesNodeOwnership(final String format) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx transaction = fixture.first.beginNodeTrx();
      final AbstractNodeReadOnlyTrx<?, ?, ?> reader =
          (AbstractNodeReadOnlyTrx<?, ?, ?>) field(AbstractNodeTrxImpl.class, "nodeReadOnlyTrx").get(transaction);
      reader.releaseCurrentPageGuard();
      final PageGuard guard = mock(PageGuard.class);
      final IllegalStateException failure = new IllegalStateException("reader guard close");
      doThrow(failure).when(guard).close();
      field(AbstractNodeReadOnlyTrx.class, "currentPageGuard").set(reader, guard);
      assertSame(failure, assertThrows(RuntimeException.class, transaction::close));
      assertTrue(reader.isClosed());
      assertTrue(transaction.isClosed());
      assertTrue(scheduler(transaction).isTerminated());
      assertFalse(fixture.second.hasRunningNodeWriteTrx());
      transaction.close();
      assertFalse(fixture.second.hasRunningNodeWriteTrx());
      assertWriterReusable(fixture);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void unsupportedRecoveryRefusesAdmissionWithoutLeakingPermit(final String format) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final AbstractResourceSession<?, ?> session = (AbstractResourceSession<?, ?>) fixture.second;
      session.requireCommitRecovery();
      final IOStorage original = session.storage;
      final IOStorage unsupported = mock(IOStorage.class, delegatesTo(original));
      doAnswer(invocation -> {
        final Writer writer = mock(Writer.class, delegatesTo(original.createWriter()));
        doReturn(false).when(writer).supportsTruncateTo();
        return writer;
      }).when(unsupported).createWriter();
      field(AbstractResourceSession.class, "storage").set(session, unsupported);
      try {
        for (int attempt = 0; attempt < 2; attempt++) {
          final RuntimeException refused = assertThrows(RuntimeException.class, fixture.second::beginNodeTrx);
          assertTrue(refused.getMessage().contains(session.getCommitFile().toString()));
          assertTrue(refused.getMessage().contains("truncate support"));
          assertTrue(Files.exists(session.getCommitFile()), "durable recovery must remain required");
          assertFalse(fixture.second.hasRunningNodeWriteTrx());
        }
      } finally {
        field(AbstractResourceSession.class, "storage").set(session, original);
      }
      assertWriterReusable(fixture);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void inMemoryRecoveryRequestNeverCreatesAMarkerOrPoisonsAdmission(final String format) throws Exception {
    try (Fixture fixture = new Fixture(format, directory, StorageType.IN_MEMORY)) {
      final AbstractResourceSession<?, ?> session = (AbstractResourceSession<?, ?>) fixture.first;
      final Path marker = session.getCommitFile();
      session.requireCommitRecovery();
      assertFalse(Files.exists(marker), "nondurable recovery must never write a marker");
      try (NodeTrx writer = fixture.second.beginNodeTrx()) {
        assertFalse(writer.isClosed());
      }
      assertFalse(fixture.second.hasRunningNodeWriteTrx());
      assertFalse(Files.exists(marker));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void inMemoryCommitFailureNeverCreatesRecoveryMarkersOrBlocksTheNextWriter(final String format) throws Exception {
    try (Fixture fixture = new Fixture(format, directory, StorageType.IN_MEMORY)) {
      final AbstractResourceSession<?, ?> session = (AbstractResourceSession<?, ?>) fixture.first;
      final Path marker = session.getCommitFile();
      assertFalse(Files.exists(marker));
      try (NodeTrx seed = fixture.first.beginNodeTrx()) {
        if (seed instanceof JsonNodeTrx json) {
          json.insertNumberValueAsFirstChild(0);
        } else {
          final XmlNodeTrx xml = (XmlNodeTrx) seed;
          xml.insertElementAsFirstChild(new QNm("root"));
          xml.insertTextAsFirstChild("0");
        }
        seed.commit();
      }
      assertFalse(Files.exists(marker), "a successful RAM commit must also leave no marker");
      final NodeTrx transaction = fixture.first.beginNodeTrx();
      final StorageEngineWriter pageWriter = transaction.getStorageEngineWriter();
      final Field backend = field(pageWriter.getClass(), "storagePageReaderWriter");
      final Writer original = (Writer) backend.get(pageWriter);
      final Writer failed = mock(Writer.class, delegatesTo(original));
      final IllegalStateException failure = new IllegalStateException("RAM publication failed");
      final AtomicBoolean reachedPublication = new AtomicBoolean();
      doAnswer(invocation -> {
        reachedPublication.set(true);
        // This boundary follows commitWritePages, where durable backends establish their marker.
        assertFalse(Files.exists(marker), "RAM must not create even a transient commit marker");
        throw failure;
      }).when(failed).writeUberPageReference(any(), any(), any(), any());
      backend.set(pageWriter, failed);
      try {
        setValue(transaction, 1);
        assertSame(failure, assertThrows(IllegalStateException.class, transaction::commit));
        assertTrue(reachedPublication.get(), "failure must occur after page writes, not in a pre-commit hook");
        assertFalse(Files.exists(marker));
      } finally {
        backend.set(pageWriter, original);
        fixture.first.close();
      }
      assertTrue(transaction.isClosed());
      assertFalse(Files.exists(marker), "failed-owner teardown must not establish durable recovery");
      assertFalse(fixture.second.hasRunningNodeWriteTrx());
      assertWriterReusable(fixture);
      assertFalse(Files.exists(marker));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void failedMarkerDeletionPreservesPublishedRevisionAndBlocksFalseCleanAdmission(final String format)
      throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx transaction = fixture.first.beginNodeTrx(1, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT);
      final Path marker = ((InternalResourceSession<?, ?>) fixture.second).getCommitFile();
      final CountDownLatch entered = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);
      AbstractNodeTrxImpl.asyncCommitTestHook = stage -> {
        if (stage.equals("before-harden")) {
          entered.countDown();
          await(release);
        }
      };
      try {
        setValue(transaction, 1);
        setValue(transaction, 2);
        entered.await();
        Files.delete(marker);
        Files.createDirectory(marker);
        Files.createFile(marker.resolve("blocker"));
      } finally {
        release.countDown();
      }
      // The marker deletion is the LAST step of hardenCommit, so it runs after the beacon write:
      // revision 2 is durable, and the asynchronous path reports that success instead of the
      // cleanup failure (#1380). spec: TXN-307
      transaction.awaitPendingAsyncCommit();
      assertEquals(2, fixture.first.getMostRecentRevisionNumber(), "the durable revision must be published");
      assertTrue(Files.isDirectory(marker), "the undeletable marker must still demand repair");
      // The successor epoch's own edit is still uncommitted, so the owner session refuses a clean
      // close exactly as it would without any cleanup failure.
      assertThrows(RuntimeException.class, fixture.first::close);
      assertTrue(transaction.isClosed());
      assertTrue(scheduler(transaction).isTerminated());
      assertThrows(RuntimeException.class, fixture.second::beginNodeTrx);
      assertFalse(fixture.second.hasRunningNodeWriteTrx());
      Files.delete(marker.resolve("blocker"));
      Files.delete(marker);
      try (NodeTrx next = fixture.second.beginNodeTrx()) {
        assertEquals(2, fixture.second.getMostRecentRevisionNumber());
        setCursor(next);
        assertEquals(1, value(next));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void removedResourceCannotPoisonRecreatedResourceAtTheSamePath(final String format) {
    try (Fixture fixture = new Fixture(format, directory)) {
      ((InternalResourceSession<?, ?>) fixture.first).requireCommitRecovery();
    }
    Databases.removeDatabase(directory.resolve("database"));
    try (Fixture recreated = new Fixture(format, directory)) {
      assertWriterReusable(recreated);
    }
  }

  @ParameterizedTest
  @CsvSource({"json,true", "xml,true", "json,false", "xml,false"})
  void pendingRootCleanupRetainsFirstFailure(final String format, final boolean failHardening) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx transaction = fixture.first.beginNodeTrx(1, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT);
      final InternalResourceSession<?, ?> original = (InternalResourceSession<?, ?>) fixture.first;
      final AtomicInteger publications = new AtomicInteger();
      final AtomicBoolean cleanupFired = new AtomicBoolean();
      final CompletableFuture<Void> notified = new CompletableFuture<>();
      original.addRevisionListener(revision -> {
        if (revision == 2) {
          publications.incrementAndGet();
          notified.complete(null);
        }
      });
      final InternalResourceSession<?, ?> session = mock(InternalResourceSession.class, delegatesTo(original));
      final IllegalStateException primary = new IllegalStateException("hardening");
      final IllegalArgumentException cleanup = new IllegalArgumentException("pending root cleanup");
      doAnswer(invocation -> {
        if (failHardening) {
          assertEquals(0, publications.get(), "failed hardening cannot publish a notification");
        }
        assertEquals(failHardening
            ? 1
            : 2, fixture.first.getMostRecentRevisionNumber());
        original.clearPendingRevisionRoot(2);
        cleanupFired.set(true);
        throw cleanup;
      }).when(session).clearPendingRevisionRoot(2);
      field(AbstractNodeTrxImpl.class, "resourceSession").set(transaction, session);
      final CountDownLatch release = new CountDownLatch(1);
      AbstractNodeTrxImpl.asyncCommitTestHook = stage -> {
        if (stage.equals("before-harden")) {
          await(release);
          if (failHardening) {
            throw primary;
          }
        }
      };
      try {
        setValue(transaction, 1);
        setValue(transaction, 2);
      } finally {
        release.countDown();
      }
      if (failHardening) {
        assertSame(primary, assertThrows(RuntimeException.class, transaction::awaitPendingAsyncCommit).getCause());
        assertSame(cleanup, primary.getSuppressed()[0]);
      } else {
        assertDoesNotThrow(transaction::awaitPendingAsyncCommit);
        assertThrows(IllegalStateException.class, transaction::commit);
        assertThrows(IllegalStateException.class, transaction::rollback);
        assertEquals(0, cleanup.getSuppressed().length);
      }
      assertTrue(cleanupFired.get(), "the pending-root cleanup failure must actually fire");
      if (!failHardening) {
        notified.get(10, TimeUnit.SECONDS);
        assertTrue(publications.get() >= 1, "hardened publication remains observable after retirement failure");
      }
      assertNull(original.getPendingRevisionRoot(2));
      assertTrue(Files.isRegularFile(original.getCommitFile()),
          "retirement failure must require recovery before admission");
      assertEquals(failHardening
          ? 1
          : 2, fixture.first.getMostRecentRevisionNumber());
      if (failHardening) {
        assertThrows(RuntimeException.class, fixture.first::close);
      } else {
        assertThrows(SirixUsageException.class, transaction::close);
        assertDoesNotThrow(fixture.first::close);
      }
      assertTrue(scheduler(transaction).isTerminated());
      try (NodeTrx next = fixture.second.beginNodeTrx()) {
        setCursor(next);
        assertEquals(failHardening
            ? 0
            : 1, value(next), "only a hardened revision may survive recovery");
        setValue(next, 3);
        next.commit();
      }
      try (NodeTrx next = fixture.second.beginNodeTrx()) {
        setCursor(next);
        assertEquals(3, value(next));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void recoveryBackendOpenFailureNamesMarkerAndReleasesAdmission(final String format) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final AbstractResourceSession<?, ?> session = (AbstractResourceSession<?, ?>) fixture.second;
      session.requireCommitRecovery();
      final IOStorage original = session.storage;
      final IOStorage failed = mock(IOStorage.class, delegatesTo(original));
      final IllegalStateException failure = new IllegalStateException("backend open");
      doThrow(failure).when(failed).createWriter();
      field(AbstractResourceSession.class, "storage").set(session, failed);
      try {
        final RuntimeException refused = assertThrows(RuntimeException.class, fixture.second::beginNodeTrx);
        assertSame(failure, refused.getCause());
        assertTrue(refused.getMessage().contains(session.getCommitFile().toString()));
        assertTrue(refused.getMessage().contains("backend open"));
        assertFalse(fixture.second.hasRunningNodeWriteTrx());
      } finally {
        field(AbstractResourceSession.class, "storage").set(session, original);
      }
      assertWriterReusable(fixture);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void closedRollbackCannotMarkAnotherWritersResourceForRecovery(final String format) {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx previous = fixture.first.beginNodeTrx();
      previous.close();
      final Path marker = ((InternalResourceSession<?, ?>) fixture.second).getCommitFile();
      try (NodeTrx next = fixture.second.beginNodeTrx()) {
        assertFalse(Files.exists(marker));
        assertThrows(IllegalStateException.class, previous::rollback);
        assertTrue(previous.isClosed());
        assertFalse(Files.exists(marker));
        setValue(next, 3);
        next.commit();
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void failedRollbackRejectsMutationUntilReconstructionSucceeds(final String format) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      try (NodeTrx transaction = fixture.first.beginNodeTrx()) {
        setValue(transaction, 1);
        final StorageEngineWriter original = transaction.getStorageEngineWriter();
        final StorageEngineWriter writer = mock(StorageEngineWriter.class, delegatesTo(original));
        final IllegalStateException failure = new IllegalStateException("rollback cleanup");
        doThrow(failure).doAnswer(invocation -> original.rollback()).when(writer).rollback();
        field(AbstractNodeTrxImpl.class, "storageEngineWriter").set(transaction, writer);
        assertSame(failure, assertThrows(RuntimeException.class, transaction::rollback));
        assertThrows(SirixUsageException.class, () -> setValue(transaction, 2));
        assertThrows(SirixUsageException.class, transaction::commit);
        transaction.rollback();
        setCursor(transaction);
        assertEquals(0, value(transaction));
        setValue(transaction, 3);
        transaction.commit();
      }
      try (NodeTrx reader = fixture.second.beginNodeTrx()) {
        setCursor(reader);
        assertEquals(3, value(reader));
      }
    }
  }

  @SuppressWarnings("unchecked")
  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void untimedRevertCompletesNotificationDelivery(final String format) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx transaction = fixture.first.beginNodeTrx();
      setValue(transaction, 1);
      transaction.commit();
      final TransactionExecutionLock execution =
          (TransactionExecutionLock) field(AbstractNodeTrxImpl.class, "executionLock").get(transaction);
      final AtomicInteger called = new AtomicInteger();
      final CompletableFuture<Void> observed = new CompletableFuture<>();
      final IntConsumer listener = revision -> {
        try {
          assertFalse(execution.isHeldByCurrentThread());
          assertLifecycleRejected(fixture, transaction);
          try (NodeReadOnlyTrx view = fixture.first.beginNodeReadOnlyTrx(revision)) {
            assertEquals(revision, view.getRevisionNumber());
            setCursor(view);
            assertEquals(revision == 1
                ? 0
                : 1, value(view));
          }
          called.incrementAndGet();
          observed.complete(null);
        } catch (final Throwable failure) {
          observed.completeExceptionally(failure);
          throw failure;
        }
      };
      final InternalResourceSession<?, ?> session = (InternalResourceSession<?, ?>) fixture.first;
      session.addRevisionListener(listener);
      transaction.revertTo(1);
      observed.get(10, TimeUnit.SECONDS);
      assertTrue(called.get() > 0);
      session.removeRevisionListener(listener);
      transaction.rollback();
      transaction.close();
      assertFalse(fixture.second.hasRunningNodeWriteTrx());
      try (NodeTrx next = fixture.second.beginNodeTrx()) {
        setCursor(next);
        assertEquals(1, value(next));
      }
    }
  }

  @ParameterizedTest(name = "lineage {index}: {arguments}")
  @CsvSource({"json,false,false", "json,false,true", "json,true,false", "json,true,true", "xml,false,false",
      "xml,false,true", "xml,true,false", "xml,true,true"})
  void lineageChangesFenceHardeningBeforeWriterRecovery(final String format, final boolean truncate,
      final boolean failHardening) throws Exception {
    final CountDownLatch entered = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final IllegalStateException primary = new IllegalStateException("hardening failed");
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx transaction = fixture.first.beginNodeTrx(1, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT);
      final StorageEngineWriter predecessor = transaction.getStorageEngineWriter();
      AbstractNodeTrxImpl.asyncCommitTestHook = stage -> {
        if (stage.equals("before-harden")) {
          entered.countDown();
          await(release);
          if (failHardening) {
            throw primary;
          }
        }
      };
      setValue(transaction, 1);
      setValue(transaction, 2);
      assertTrue(entered.await(10, TimeUnit.SECONDS));
      final StorageEngineWriter successor = transaction.getStorageEngineWriter();
      final CompletableFuture<Throwable> changed = new CompletableFuture<>();
      final Thread command = new Thread(() -> {
        try {
          if (truncate) {
            transaction.truncateTo(1);
          } else {
            transaction.revertTo(1);
          }
          changed.complete(null);
        } catch (final Throwable failure) {
          changed.complete(failure);
        }
      });
      try {
        command.start();
        final Semaphore slot = (Semaphore) field(AbstractNodeTrxImpl.class, "asyncCommitPermit").get(transaction);
        while (!slot.hasQueuedThreads()) {
          if (changed.isDone()) {
            fail("lineage changed before the hardening worker exited");
          }
          if (Thread.interrupted()) {
            throw new InterruptedException();
          }
          Thread.onSpinWait();
        }
        assertFalse(predecessor.isClosed());
        assertFalse(successor.isClosed());
        assertSame(successor, transaction.getStorageEngineWriter());
        assertEquals(1, fixture.first.getMostRecentRevisionNumber());
        release.countDown();
        final Throwable failure = changed.get(10, TimeUnit.SECONDS);
        if (failHardening) {
          assertTrue(failure instanceof SirixIOException);
          assertSame(primary, failure.getCause());
        } else {
          assertNull(failure);
        }
        if (!failHardening && !truncate) {
          transaction.close();
        }
        try {
          fixture.first.close();
        } catch (final RuntimeException cleanupFailure) {
          if (failHardening) {
            assertSame(primary, cleanupFailure.getCause());
          } else {
            assertTrue(truncate);
            assertTrue(Files.exists(((InternalResourceSession<?, ?>) fixture.second).getCommitFile()));
          }
        }
        assertTrue(transaction.isClosed());
        assertTrue(scheduler(transaction).isTerminated());
        assertFalse(fixture.second.hasRunningNodeWriteTrx());
        try (NodeTrx next = fixture.second.beginNodeTrx()) {
          setCursor(next);
          assertEquals(failHardening || truncate
              ? 0
              : 1, value(next));
          setValue(next, 3);
          next.commit();
        }
        try (NodeTrx next = fixture.second.beginNodeTrx()) {
          setCursor(next);
          assertEquals(3, value(next));
        }
      } finally {
        release.countDown();
        command.join(10000);
        AbstractNodeTrxImpl.asyncCommitTestHook = null;
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void nodeTruncationCanTargetTheRevisionBeingHardened(final String format) throws Exception {
    final CountDownLatch entered = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx transaction = fixture.first.beginNodeTrx(1, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT);
      AbstractNodeTrxImpl.asyncCommitTestHook = stage -> {
        if (stage.equals("before-harden")) {
          entered.countDown();
          await(release);
        }
      };
      setValue(transaction, 1);
      setValue(transaction, 2);
      assertTrue(entered.await(10, TimeUnit.SECONDS));
      final CompletableFuture<Throwable> result = new CompletableFuture<>();
      final Thread command = new Thread(() -> {
        try {
          transaction.truncateTo(2);
          result.complete(null);
        } catch (final Throwable failure) {
          result.complete(failure);
        }
      });
      try {
        command.start();
        final Semaphore permit = (Semaphore) field(AbstractNodeTrxImpl.class, "asyncCommitPermit").get(transaction);
        while (!permit.hasQueuedThreads()) {
          assertFalse(result.isDone(), "target validation must follow pending hardening");
          Thread.onSpinWait();
        }
        release.countDown();
        assertNull(result.get(10, TimeUnit.SECONDS));
        assertEquals(2, fixture.first.getMostRecentRevisionNumber());
        try (NodeReadOnlyTrx reader = fixture.first.beginNodeReadOnlyTrx(2)) {
          setCursor(reader);
          assertEquals(1, value(reader));
        }
        transaction.rollback();
        transaction.close();
      } finally {
        release.countDown();
        command.join(10000);
      }
    }
  }

  @ParameterizedTest(name = "page hardening fence {arguments}")
  @CsvSource({"json,false,false,false", "json,false,true,false", "json,true,false,false", "json,true,true,false",
      "xml,false,false,false", "xml,false,true,false", "xml,true,false,false", "xml,true,true,false",
      "json,false,false,true", "json,true,false,true", "xml,false,false,true", "xml,true,false,true"})
  void rawPageTruncationFencesBoundHardening(final String format, final boolean predecessorEntry,
      final boolean failHardening, final boolean interrupt) throws Exception {
    final CountDownLatch entered = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final IllegalStateException primary = new IllegalStateException("hardening failed");
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx transaction = fixture.first.beginNodeTrx(1, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT);
      final StorageEngineWriter predecessor = transaction.getStorageEngineWriter();
      AbstractNodeTrxImpl.asyncCommitTestHook = stage -> {
        if (stage.equals("before-harden")) {
          entered.countDown();
          await(release);
          if (failHardening) {
            throw primary;
          }
        }
      };
      setValue(transaction, 1);
      setValue(transaction, 2);
      assertTrue(entered.await(10, TimeUnit.SECONDS));
      final StorageEngineWriter page = predecessorEntry
          ? predecessor
          : transaction.getStorageEngineWriter();
      final CompletableFuture<Throwable> result = new CompletableFuture<>();
      final AtomicBoolean interrupted = new AtomicBoolean();
      final Thread command = new Thread(() -> {
        try {
          page.truncateTo(1);
          result.complete(null);
        } catch (final Throwable failure) {
          result.complete(failure);
        } finally {
          interrupted.set(Thread.currentThread().isInterrupted());
        }
      });
      try {
        command.start();
        final Semaphore slot = (Semaphore) field(AbstractNodeTrxImpl.class, "asyncCommitPermit").get(transaction);
        while (!slot.hasQueuedThreads()) {
          assertFalse(result.isDone(), "raw page maintenance returned before hardening exited");
          Thread.onSpinWait();
        }
        assertFalse(predecessor.isClosed());
        assertEquals(1, fixture.first.getMostRecentRevisionNumber());
        if (interrupt) {
          command.interrupt();
        }
        release.countDown();
        final Throwable failure = result.get(10, TimeUnit.SECONDS);
        command.join(10000);
        assertEquals(interrupt, interrupted.get());
        if (failHardening) {
          assertTrue(failure instanceof SirixIOException);
          assertSame(primary, failure.getCause());
        } else if (interrupt) {
          assertTrue(failure instanceof RuntimeException);
          assertEquals(2, fixture.first.getMostRecentRevisionNumber());
        } else if (predecessorEntry) {
          assertTrue(predecessor.isClosed());
          assertTrue(failure instanceof IllegalStateException);
          assertEquals(2, fixture.first.getMostRecentRevisionNumber());
          try (NodeReadOnlyTrx view = fixture.first.beginNodeReadOnlyTrx(2)) {
            setCursor(view);
            assertEquals(1, value(view));
          }
        } else {
          assertNull(failure);
          assertEquals(1, fixture.first.getMostRecentRevisionNumber());
        }
        try {
          fixture.first.close();
        } catch (final RuntimeException cleanup) {
          assertTrue(failHardening || !predecessorEntry);
        }
      } finally {
        release.countDown();
        command.join(10000);
      }
    }
  }

  @ParameterizedTest(name = "page append fence {arguments}")
  @CsvSource({"json,page,false,false", "json,page,true,false", "json,node,false,false", "json,node,true,false",
      "xml,page,false,false", "xml,page,true,false", "xml,node,false,false", "xml,node,true,false",
      "json,page,false,true", "json,node,false,true", "xml,page,false,true", "xml,node,false,true",
      "json,unbound,false,false", "json,unbound,true,false", "xml,unbound,false,false", "xml,unbound,true,false",
      "json,unbound,false,true", "xml,unbound,false,true"})
  void truncationFencesAsyncAppend(final String format, final String entry, final boolean failAppend,
      final boolean interrupt) throws Exception {
    final CountDownLatch entered = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final IllegalStateException primary = new IllegalStateException("append failed");
    try (Fixture fixture = new Fixture(format, directory)) {
      final boolean unbound = entry.equals("unbound");
      final NodeTrx transaction = unbound
          ? null
          : fixture.first.beginNodeTrx(0, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH);
      final StorageEngineWriter page = unbound
          ? fixture.first.createStorageEngineWriter()
          : transaction.getStorageEngineWriter();
      flushHook = field(page.getClass(), "asyncFlushFaultHook");
      flushHook.set(null, (BiConsumer<StorageEngineWriter, String>) (owner, stage) -> {
        if (owner == page && stage.equals("write")) {
          entered.countDown();
          await(release);
          if (failAppend) {
            throw primary;
          }
        }
      });
      if (unbound) {
        page.prepareRecordForModification(format.equals("json")
            ? 1
            : 2, IndexType.DOCUMENT, -1);
      } else {
        setValue(transaction, 1);
      }
      page.asyncFlush();
      assertTrue(entered.await(10, TimeUnit.SECONDS));
      final CompletableFuture<Throwable> result = new CompletableFuture<>();
      final AtomicBoolean interrupted = new AtomicBoolean();
      final Thread command = new Thread(() -> {
        try {
          if (entry.equals("node")) {
            transaction.truncateTo(0);
          } else {
            page.truncateTo(0);
          }
          result.complete(null);
        } catch (final Throwable failure) {
          result.complete(failure);
        } finally {
          interrupted.set(Thread.currentThread().isInterrupted());
        }
      });
      try {
        command.start();
        final Semaphore permit = (Semaphore) field(page.getClass(), "flushPermit").get(page);
        while (!permit.hasQueuedThreads()) {
          assertFalse(result.isDone(), "maintenance returned while append owned the pages");
          Thread.onSpinWait();
        }
        assertEquals(1, fixture.first.getMostRecentRevisionNumber());
        if (interrupt) {
          command.interrupt();
        }
        release.countDown();
        final Throwable failure = result.get(10, TimeUnit.SECONDS);
        command.join(10000);
        assertEquals(interrupt, interrupted.get());
        if (failAppend) {
          assertSame(primary, failure.getCause());
          assertEquals(1, fixture.first.getMostRecentRevisionNumber());
        } else if (interrupt) {
          assertTrue(failure instanceof RuntimeException);
          assertEquals(1, fixture.first.getMostRecentRevisionNumber());
        } else {
          assertNull(failure);
          assertEquals(0, fixture.first.getMostRecentRevisionNumber());
        }
        try {
          fixture.first.close();
        } catch (final RuntimeException cleanup) {
          assertTrue(failAppend || Files.exists(((InternalResourceSession<?, ?>) fixture.second).getCommitFile()));
        }
      } finally {
        release.countDown();
        command.join(10000);
      }
    }
  }

  private static void assertLifecycleRejected(final Fixture fixture, final NodeTrx transaction) {
    assertTrue(assertThrows(IllegalStateException.class, transaction::commit).getMessage().contains("callback"));
    assertTrue(assertThrows(IllegalStateException.class, transaction::rollback).getMessage().contains("callback"));
    assertTrue(assertThrows(IllegalStateException.class, transaction::close).getMessage().contains("callback"));
    assertTrue(
        assertThrows(IllegalStateException.class, () -> ((InternalNodeTrx<?>) transaction).abort()).getMessage()
                                                                                                   .contains(
                                                                                                       "callback"));
    assertTrue(assertThrows(IllegalStateException.class, fixture.first::close).getMessage().contains("callback"));
    assertTrue(
        assertThrows(IllegalStateException.class, fixture.first::beginNodeTrx).getMessage().contains("callback"));
    assertTrue(
        assertThrows(IllegalStateException.class, fixture.first::createStorageEngineWriter).getMessage()
                                                                                           .contains("callback"));
    assertTrue(
        assertThrows(IllegalStateException.class, () -> fixture.first.createStorageEngineWriter(1)).getMessage()
                                                                                                   .contains(
                                                                                                       "callback"));
    assertTrue(
        assertThrows(IllegalStateException.class, () -> transaction.revertTo(1)).getMessage().contains("callback"));
    assertTrue(
        assertThrows(IllegalStateException.class, () -> transaction.truncateTo(0)).getMessage().contains("callback"));
    assertTrue(assertThrows(IllegalStateException.class, transaction::awaitPendingAsyncCommit).getMessage()
                                                                                              .contains("callback"));
    assertTrue(assertThrows(IllegalStateException.class,
        () -> transaction.markRollbackOnly(new IllegalStateException("from callback"))).getMessage()
                                                                                       .contains("callback"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  // V05-EXECUTION-CLOSE: docs/hooks.md requires pre-wait rejection and subsequent safe reuse.
  void enclosingExecutionCannotEnterATerminationWait(final String format) throws Exception {
    try (Fixture fixture = assertDoesNotThrow(() -> new Fixture(format, directory))) {
      final NodeTrx transaction = fixture.first.beginNodeTrx(1, TimeUnit.DAYS);
      transaction.runLocked(() -> {
        assertThrows(IllegalStateException.class, transaction::close);
        assertThrows(IllegalStateException.class, fixture.first::close);
      });
      assertFalse(transaction.isClosed());
      assertFalse(fixture.first.isClosed());
      transaction.close();
      assertTrue(transaction.isClosed());
      assertTrue(scheduler(transaction).isTerminated());
      assertWriterReusable(fixture);
    }
  }

  @ParameterizedTest
  @CsvSource({"json,false,false,false", "json,false,false,true", "json,false,true,false", "json,false,true,true",
      "json,true,false,false", "json,true,false,true", "json,true,true,false", "json,true,true,true",
      "xml,false,false,false", "xml,false,false,true", "xml,false,true,false", "xml,false,true,true",
      "xml,true,false,false", "xml,true,false,true", "xml,true,true,false", "xml,true,true,true"})
  void lateConcurrentCloseRetainsOwnerUntilPermitRelease(final String format, final boolean abort, final boolean timed,
      final boolean failWriter) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx transaction = timed
          ? fixture.first.beginNodeTrx(1, TimeUnit.DAYS)
          : fixture.first.beginNodeTrx();
      final AbstractResourceSession<?, ?> session = (AbstractResourceSession<?, ?>) fixture.first;
      final ScheduledExecutorService executor = scheduler(transaction);
      final ScheduledExecutorService gatedExecutor = mock(ScheduledExecutorService.class, delegatesTo(executor));
      field(AbstractNodeTrxImpl.class, "commitScheduler").set(transaction, gatedExecutor);
      final CountDownLatch beforeRelease = new CountDownLatch(1);
      final CountDownLatch allowRelease = new CountDownLatch(1);
      final AtomicReference<Thread> firstCloser = new AtomicReference<>();
      doAnswer(invocation -> {
        final boolean terminated = executor.isTerminated();
        if (terminated && Thread.currentThread() == firstCloser.get()) {
          beforeRelease.countDown();
          await(allowRelease);
        }
        return terminated;
      }).when(gatedExecutor).isTerminated();
      final IllegalStateException primary = new IllegalStateException("terminal writer close failed");
      if (failWriter) {
        final StorageEngineWriter writer = transaction.getStorageEngineWriter();
        flushHook = field(writer.getClass(), "asyncFlushFaultHook");
        final AtomicInteger failures = new AtomicInteger();
        flushHook.set(null, (BiConsumer<StorageEngineWriter, String>) (owner, stage) -> {
          if (owner == writer && stage.equals("close-after-terminal-flags") && failures.getAndIncrement() == 0) {
            throw primary;
          }
        });
      }
      if (abort) {
        setValue(transaction, 1);
      }
      final CompletableFuture<Throwable> firstFinished = new CompletableFuture<>();
      final CompletableFuture<Void> secondFinished = new CompletableFuture<>();
      final Thread first = new Thread(() -> {
        try {
          if (abort) {
            fixture.first.close();
          } else {
            transaction.close();
          }
          firstFinished.complete(null);
        } catch (final Throwable failure) {
          firstFinished.complete(failure);
        }
      });
      final Thread second = new Thread(() -> {
        try {
          fixture.first.close();
          secondFinished.complete(null);
        } catch (final Throwable failure) {
          secondFinished.completeExceptionally(failure);
        }
      });
      firstCloser.set(first);
      try {
        first.start();
        assertTrue(beforeRelease.await(10, TimeUnit.SECONDS));
        assertTrue(transaction.isClosed());
        assertFalse(session.isClosed());
        assertTrue(fixture.second.hasRunningNodeWriteTrx());
        second.start();
        secondFinished.get(10, TimeUnit.SECONDS);
        assertFalse(firstFinished.isDone());
        assertTrue(session.isClosed());

        assertFalse(fixture.second.hasRunningNodeWriteTrx());
        try (NodeTrx next = fixture.second.beginNodeTrx()) {
          setCursor(next);
          assertEquals(0, value(next));
          allowRelease.countDown();
          assertSame(failWriter
              ? primary
              : null, firstFinished.get(10, TimeUnit.SECONDS));
          transaction.close();
          fixture.first.close();
          assertTrue(fixture.second.hasRunningNodeWriteTrx());
          setValue(next, 3);
          next.commit();
        }
        assertFalse(fixture.second.hasRunningNodeWriteTrx());
        assertTrue(executor.isTerminated());
        try (NodeTrx next = fixture.second.beginNodeTrx()) {
          setCursor(next);
          assertEquals(3, value(next));
        }
      } finally {
        allowRelease.countDown();
        first.join(10000);
        second.join(10000);
      }
    }
  }

  @ParameterizedTest(name = "admission {index}: {arguments}")
  @CsvSource({"json,false,false,false", "json,false,false,true", "json,false,true,false", "json,false,true,true",
      "json,true,false,false", "json,true,false,true", "json,true,true,false", "json,true,true,true",
      "xml,false,false,false", "xml,false,false,true", "xml,false,true,false", "xml,false,true,true",
      "xml,true,false,false", "xml,true,false,true", "xml,true,true,false", "xml,true,true,true"})
  void queuedWriterAdmissionDoesNotHoldTheSessionMonitor(final String format, final boolean pageOwner,
      final boolean pageAdmission, final boolean closeSession) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final AutoCloseable owner = pageOwner
          ? fixture.first.createStorageEngineWriter(fixture.first.getMostRecentRevisionNumber())
          : fixture.first.beginNodeTrx();
      final AbstractResourceSession<?, ?> session = (AbstractResourceSession<?, ?>) fixture.first;
      final CompletableFuture<AutoCloseable> admitted = new CompletableFuture<>();
      final CompletableFuture<Void> closed = new CompletableFuture<>();
      final Thread admission = new Thread(() -> {
        try {
          admitted.complete(pageAdmission
              ? fixture.first.createStorageEngineWriter(fixture.first.getMostRecentRevisionNumber())
              : fixture.first.beginNodeTrx());
        } catch (final Throwable failure) {
          admitted.completeExceptionally(failure);
        }
      });
      final Thread closer = new Thread(() -> {
        try {
          if (closeSession) {
            fixture.first.close();
          } else {
            owner.close();
          }
          closed.complete(null);
        } catch (final Throwable failure) {
          closed.completeExceptionally(failure);
        }
      });
      try {
        admission.start();
        while (!session.writeLock.hasQueuedThreads()) {
          if (admitted.isDone()) {
            fail("admission did not queue behind the existing owner");
          }
          if (Thread.interrupted()) {
            throw new InterruptedException();
          }
          Thread.onSpinWait();
        }
        closer.start();
        closed.get(5, TimeUnit.SECONDS);
        if (closeSession) {
          final ExecutionException rejected =
              assertThrows(ExecutionException.class, () -> admitted.get(5, TimeUnit.SECONDS));
          assertTrue(rejected.getCause() instanceof IllegalStateException);
          assertTrue(session.isClosed());

          assertFalse(fixture.second.hasRunningNodeWriteTrx());
        } else {
          try (AutoCloseable next = admitted.get(5, TimeUnit.SECONDS)) {
            assertTrue(fixture.second.hasRunningNodeWriteTrx());
            if (next instanceof NodeTrx node) {
              setCursor(node);
              assertEquals(0, value(node));
              setValue(node, 3);
              node.commit();
            } else {
              assertFalse(((StorageEngineWriter) next).isClosed());
            }
          }
          assertFalse(fixture.second.hasRunningNodeWriteTrx());
        }
        try (NodeTrx next = fixture.second.beginNodeTrx()) {
          setCursor(next);
          assertEquals(!closeSession && !pageAdmission
              ? 3
              : 0, value(next));
        }
      } finally {
        admission.join(25000);
        closer.join(1000);
        owner.close();
        if (admitted.isDone() && !admitted.isCompletedExceptionally()) {
          admitted.get().close();
        }
      }
    }
  }

  @Test
  void asyncPostHookNavigationCannotMoveTheCountOnlyWriterCursor() throws Exception {
    try (Fixture fixture = new Fixture("json", directory)) {
      final long firstKey;
      final long secondKey;
      try (JsonNodeTrx setup = (JsonNodeTrx) fixture.first.beginNodeTrx()) {
        setup.moveTo(1);
        setup.remove();
        setup.moveToDocumentRoot();
        setup.insertArrayAsFirstChild();
        setup.insertNumberValueAsFirstChild(10);
        firstKey = setup.getNodeKey();
        setup.insertNumberValueAsRightSibling(20);
        secondKey = setup.getNodeKey();
        setup.commit();
      }
      final JsonNodeTrx writer = (JsonNodeTrx) fixture.first.beginNodeTrx(3, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT);
      final CountDownLatch hardening = new CountDownLatch(1);
      final CountDownLatch releaseHardening = new CountDownLatch(1);
      final CountDownLatch onSecondNode = new CountDownLatch(1);
      final CountDownLatch changed = new CountDownLatch(1);
      final CompletableFuture<Void> observed = new CompletableFuture<>();
      final AtomicReference<NodeReadOnlyTrx> snapshot = new AtomicReference<>();
      AbstractNodeTrxImpl.asyncCommitTestHook = stage -> {
        if (stage.equals("before-harden")) {
          hardening.countDown();
          await(releaseHardening);
        }
      };
      writer.addPostCommitHook(view -> {
        try {
          snapshot.set(view);
          assertFalse(view instanceof NodeTrx);
          assertEquals(3, view.getRevisionNumber());
          assertTrue(view.moveTo(secondKey));
          assertEquals(20, value(view));
          onSecondNode.countDown();
          await(changed);
          assertEquals(secondKey, view.getNodeKey());
          assertEquals(20, value(view));
          assertTrue(view.moveTo(firstKey));
          assertEquals(13, value(view));
          observed.complete(null);
        } catch (final Throwable failure) {
          observed.completeExceptionally(failure);
          throw failure;
        }
      });
      try {
        assertTrue(writer.moveTo(firstKey));
        writer.setNumberValue(11);
        writer.setNumberValue(12);
        writer.setNumberValue(13);
        writer.setNumberValue(14);
        assertTrue(hardening.await(10, TimeUnit.SECONDS));
        releaseHardening.countDown();
        assertTrue(onSecondNode.await(10, TimeUnit.SECONDS));
        writer.runLocked(() -> {
          assertEquals(firstKey, writer.getNodeKey());
          writer.setNumberValue(15);
        });
        assertEquals(firstKey, writer.getNodeKey());
        assertEquals(15, writer.getNumberValue().intValue());
        assertTrue(writer.moveTo(secondKey));
        assertEquals(20, writer.getNumberValue().intValue());
        changed.countDown();
        observed.get(10, TimeUnit.SECONDS);
        fixture.first.close();
        assertTrue(snapshot.get().isClosed());
        assertFalse(fixture.second.hasRunningNodeWriteTrx());
        try (NodeTrx next = fixture.second.beginNodeTrx()) {
          assertTrue(next.moveTo(firstKey));
          assertEquals(13, value(next));
          assertTrue(next.moveTo(secondKey));
          assertEquals(20, value(next));
        }
      } finally {
        changed.countDown();
        releaseHardening.countDown();
      }
    }
  }

  @SuppressWarnings("unchecked")
  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void refusingNodeOwnerRetainsOrdinaryReadersUntilRetry(final String format) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx writer = fixture.first.beginNodeTrx(0, 1, TimeUnit.DAYS);
      final AbstractResourceSession<?, ?> session = (AbstractResourceSession<?, ?>) fixture.first;
      final NodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(1);
      setCursor(reader);
      final StorageEngineReader page = reader.getStorageEngineReader();
      final InternalNodeTrx<?> owner = mock(InternalNodeTrx.class, delegatesTo(writer));
      ((Map<Integer, NodeReadOnlyTrx>) (Map<?, ?>) session.nodeTrxMap).put(writer.getId(), owner);
      final IllegalStateException primary = new IllegalStateException("owner refused before its callback fence");
      doThrow(primary).doAnswer(invocation -> {
        ((InternalNodeTrx<?>) writer).abort();
        return null;
      }).when(owner).abort();
      assertSame(primary, assertThrows(IllegalStateException.class, session::close));
      assertFalse(reader.isClosed());
      assertFalse(page.isClosed());
      assertTrue(reader.moveTo(0));
      setCursor(reader);
      assertEquals(0, value(reader));
      assertFalse(session.isClosed());
      assertTrue(fixture.second.hasRunningNodeWriteTrx());
      session.close();
      assertTrue(reader.isClosed());
      assertTrue(page.isClosed());
      assertTrue(session.isClosed());
      assertWriterReusable(fixture);
    }
  }

  private static void assertWriterReusable(final Fixture fixture) {
    assertFalse(fixture.second.hasRunningNodeWriteTrx());
    try (NodeTrx next = fixture.second.beginNodeTrx()) {
      setCursor(next);
      assertEquals(0, value(next));
      setValue(next, 3);
      next.commit();
    }
    try (NodeTrx next = fixture.second.beginNodeTrx()) {
      setCursor(next);
      assertEquals(3, value(next));
    }
  }

  private static void setCursor(final NodeReadOnlyTrx transaction) {
    assertTrue(transaction.moveTo(transaction instanceof JsonNodeReadOnlyTrx
        ? 1
        : 2));
  }

  private static void setValue(final NodeTrx transaction, final int value) {
    setCursor(transaction);
    if (transaction instanceof JsonNodeTrx json) {
      json.setNumberValue(value);
    } else {
      ((XmlNodeTrx) transaction).setValue(Integer.toString(value));
    }
  }

  private static int value(final NodeReadOnlyTrx transaction) {
    return transaction instanceof JsonNodeReadOnlyTrx json
        ? json.getNumberValue().intValue()
        : Integer.parseInt(transaction.getValue());
  }

  private static ScheduledExecutorService scheduler(final NodeTrx transaction) throws ReflectiveOperationException {
    return (ScheduledExecutorService) field(AbstractNodeTrxImpl.class, "commitScheduler").get(transaction);
  }

  private static Field field(final Class<?> type, final String name) throws ReflectiveOperationException {
    final Field field = type.getDeclaredField(name);
    field.setAccessible(true);
    return field;
  }

  private static void await(final CountDownLatch latch) {
    try {
      latch.await();
    } catch (final InterruptedException failure) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(failure);
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final Database<? extends ResourceSession<?, ?>> firstDatabase;
    private final Database<? extends ResourceSession<?, ?>> secondDatabase;
    private final ResourceSession<?, ?> first;
    private final ResourceSession<?, ?> second;

    Fixture(final String format, final Path directory) {
      this(format, directory, StorageType.FILE_CHANNEL);
    }

    Fixture(final String format, final Path directory, final StorageType storageType) {
      final Path path = directory.resolve("database");
      if (format.equals("json")) {
        Databases.createJsonDatabase(new DatabaseConfiguration(path));
      } else {
        Databases.createXmlDatabase(new DatabaseConfiguration(path));
      }
      firstDatabase = open(format, path, new User("first", UUID.randomUUID()));
      secondDatabase = open(format, path, new User("second", UUID.randomUUID()));
      firstDatabase.createResource(ResourceConfiguration.newBuilder("resource")
                                                        .storageType(storageType)
                                                        .storeDiffs(false)
                                                        .hashKind(HashType.NONE)
                                                        .buildPathSummary(false)
                                                        .build());
      first = firstDatabase.beginResourceSession("resource");
      second = secondDatabase.beginResourceSession("resource");
      if (storageType == StorageType.IN_MEMORY) {
        return;
      }
      try (NodeTrx transaction = first.beginNodeTrx()) {
        if (transaction instanceof JsonNodeTrx json) {
          json.insertNumberValueAsFirstChild(0);
        } else {
          final XmlNodeTrx xml = (XmlNodeTrx) transaction;
          xml.insertElementAsFirstChild(new QNm("root"));
          xml.insertTextAsFirstChild("0");
        }
        transaction.commit();
      }
    }

    private static Database<? extends ResourceSession<?, ?>> open(final String format, final Path path,
        final User user) {
      return format.equals("json")
          ? Databases.openJsonDatabase(path, user)
          : Databases.openXmlDatabase(path, user);
    }

    @Override
    public void close() {
      try {
        first.close();
      } finally {
        try {
          second.close();
        } finally {
          try {
            secondDatabase.close();
          } finally {
            firstDatabase.close();
          }
        }
      }
    }
  }
}
