package io.sirix.access.trx.node;

import io.brackit.query.atomic.QNm;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.User;
import io.sirix.api.Database;
import io.sirix.api.NodeTrx;
import io.sirix.api.NodeCursor;
import io.sirix.api.ResourceSession;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.cache.PageGuard;
import io.sirix.exception.SirixUsageException;
import io.sirix.exception.SirixIOException;
import io.sirix.io.StorageType;
import io.sirix.io.IOStorage;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import static org.mockito.Mockito.verify;

@Isolated
@Timeout(30)
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
        assertEquals(workerExitsBeforePageClose, writer.isClosed());
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
  @CsvSource({"json,before-successor", "xml,before-successor", "json,before-submit", "xml,before-submit"})
  void inlineDoubleFailureReleasesSlotAndRetainsPrimary(final String format, final String failingStage)
      throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx transaction = fixture.first.beginNodeTrx(1, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT);
      final IllegalStateException primary = new IllegalStateException(failingStage);
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
        if (stage.equals(failingStage)) {
          throw primary;
        }
        if (stage.equals("before-harden")) {
          throw hardening;
        }
      };
      setValue(transaction, 1);
      assertSame(primary, assertThrows(IllegalStateException.class, () -> setValue(transaction, 2)));
      AbstractNodeTrxImpl.asyncCommitTestHook = null;
      if (flushHook != null) {
        flushHook.set(null, null);
      }
      assertSame(hardening, primary.getSuppressed()[0]);
      final Semaphore permit = (Semaphore) field(AbstractNodeTrxImpl.class, "asyncCommitPermit").get(transaction);
      assertEquals(1, permit.availablePermits());
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
      assertEquals(1, ((AbstractResourceSession<?, ?>) fixture.first).writeLock.availablePermits());
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
      flushHook.set(null, (BiConsumer<StorageEngineWriter, String>) (owner, stage) -> {
        if (owner == predecessor && stage.equals("close-after-storage-writer-close")) {
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
      final RuntimeException observed = assertThrows(RuntimeException.class, transaction::awaitPendingAsyncCommit);
      assertSame(failure, observed.getCause());
      flushHook.set(null, null);
      assertThrows(RuntimeException.class, fixture.first::close);
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
      verify(guard).close();
      assertTrue(reader.isClosed());
      assertTrue(transaction.isClosed());
      assertTrue(scheduler(transaction).isTerminated());
      assertEquals(1, ((AbstractResourceSession<?, ?>) fixture.first).writeLock.availablePermits());
      transaction.close();
      assertEquals(1, ((AbstractResourceSession<?, ?>) fixture.first).writeLock.availablePermits());
      assertWriterReusable(fixture);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void unsupportedRecoveryRefusesAdmissionWithoutLeakingPermit(final String format) {
    try (Fixture fixture = new Fixture(format, directory, StorageType.IN_MEMORY)) {
      final InternalResourceSession<?, ?> session = (InternalResourceSession<?, ?>) fixture.second;
      session.requireCommitRecovery();
      for (int attempt = 0; attempt < 2; attempt++) {
        final RuntimeException refused = assertThrows(RuntimeException.class, fixture.second::beginNodeTrx);
        assertTrue(refused.getMessage().contains(session.getCommitFile().toString()));
        assertTrue(refused.getMessage().contains("truncate support"));
        assertFalse(fixture.second.hasRunningNodeWriteTrx());
      }
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
      final RuntimeException failure = assertThrows(RuntimeException.class, transaction::awaitPendingAsyncCommit);
      assertTrue(failure.getCause().getCause() instanceof DirectoryNotEmptyException);
      assertTrue(failure.getCause().getMessage().contains(marker.toString()));
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
      original.addRevisionListener(revision -> {
        if (revision == 2) {
          publications.incrementAndGet();
        }
      });
      final InternalResourceSession<?, ?> session = mock(InternalResourceSession.class, delegatesTo(original));
      final IllegalStateException primary = new IllegalStateException("hardening");
      final IllegalArgumentException cleanup = new IllegalArgumentException("pending root cleanup");
      doAnswer(invocation -> {
        assertEquals(failHardening
            ? 0
            : 1, publications.get(), "publish before retiring the pending root");
        assertEquals(failHardening
            ? 1
            : 2, fixture.first.getMostRecentRevisionNumber());
        original.clearPendingRevisionRoot(2);
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
      final Throwable expected = failHardening
          ? primary
          : cleanup;
      assertSame(expected, assertThrows(RuntimeException.class, transaction::awaitPendingAsyncCommit).getCause());
      if (failHardening) {
        assertSame(cleanup, primary.getSuppressed()[0]);
      } else {
        assertEquals(0, cleanup.getSuppressed().length);
      }
      assertNull(original.getPendingRevisionRoot(2));
      assertTrue(Files.isRegularFile(original.getCommitFile()),
          "retirement failure must require recovery before admission");
      assertEquals(failHardening
          ? 1
          : 2, fixture.first.getMostRecentRevisionNumber());
      assertThrows(RuntimeException.class, fixture.first::close);
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

  @ParameterizedTest
  @CsvSource({"json,false,transaction,FILE_CHANNEL",
      "json,false,transaction,IN_MEMORY",
      "json,false,session,FILE_CHANNEL",
      "json,false,session,IN_MEMORY",
      "json,false,rollback,FILE_CHANNEL",
      "json,false,rollback,IN_MEMORY",
      "json,true,transaction,FILE_CHANNEL",
      "json,true,transaction,IN_MEMORY",
      "json,true,session,FILE_CHANNEL",
      "json,true,session,IN_MEMORY",
      "json,true,rollback,FILE_CHANNEL",
      "json,true,rollback,IN_MEMORY",
      "xml,false,transaction,FILE_CHANNEL",
      "xml,false,transaction,IN_MEMORY",
      "xml,false,session,FILE_CHANNEL",
      "xml,false,session,IN_MEMORY",
      "xml,false,rollback,FILE_CHANNEL",
      "xml,false,rollback,IN_MEMORY",
      "xml,true,transaction,FILE_CHANNEL",
      "xml,true,transaction,IN_MEMORY",
      "xml,true,session,FILE_CHANNEL",
      "xml,true,session,IN_MEMORY",
      "xml,true,rollback,FILE_CHANNEL",
      "xml,true,rollback,IN_MEMORY"})
  void timedPostCommitCloseReleasesAdmissionWithoutJoiningItself(final String format, final boolean closeSession,
      final String externalAction, final StorageType storageType)
      throws Exception {
    try (Fixture fixture = new Fixture(format, directory, storageType)) {
      final NodeTrx transaction = fixture.first.beginNodeTrx(1, TimeUnit.SECONDS);
      final ScheduledExecutorService scheduler = scheduler(transaction);
      final ReentrantLock lock = (ReentrantLock) field(AbstractNodeTrxImpl.class, "lock").get(transaction);
      final CountDownLatch entered = new CountDownLatch(1);
      final CountDownLatch allowClose = new CountDownLatch(1);
      final CountDownLatch allowHookReturn = new CountDownLatch(1);
      final CompletableFuture<Void> closedInHook = new CompletableFuture<>();
      final CompletableFuture<Boolean> externalFinished = new CompletableFuture<>();
      final AtomicInteger laterHooks = new AtomicInteger();
      final Thread external = new Thread(() -> {
        try {
          switch (externalAction) {
            case "transaction" -> transaction.close();
            case "session" -> fixture.first.close();
            case "rollback" -> assertThrows(IllegalStateException.class, transaction::rollback);
            default -> throw new AssertionError(externalAction);
          }
          externalFinished.complete(externalAction.equals("rollback") || scheduler.isTerminated());
        } catch (final Throwable failure) {
          externalFinished.completeExceptionally(failure);
        }
      });
      transaction.runLocked(() -> {
        if (storageType == StorageType.IN_MEMORY) {
          assertEmptyDocument(transaction);
        } else {
          setValue(transaction, 1);
        }
        transaction.addPostCommitHook(closing -> {
          entered.countDown();
          await(allowClose);
          try {
            if (closeSession) {
              fixture.first.close();
            } else {
              closing.close();
            }
            closedInHook.complete(null);
            await(allowHookReturn);
          } catch (final Throwable failure) {
            closedInHook.completeExceptionally(failure);
            throw failure;
          }
        });
        transaction.addPostCommitHook(closed -> laterHooks.incrementAndGet());
      });
      try {
        assertTrue(entered.await(10, TimeUnit.SECONDS));
        external.start();
        while (!lock.hasQueuedThread(external)) {
          if (!external.isAlive()) {
            fail("external close returned before the scheduler callback exited");
          }
          if (Thread.interrupted()) {
            throw new InterruptedException();
          }
          Thread.onSpinWait();
        }
        allowClose.countDown();
        closedInHook.get(10, TimeUnit.SECONDS);
        assertTrue(transaction.isClosed());
        assertEquals(closeSession, fixture.first.isClosed());
        assertTrue(scheduler.isShutdown());
        assertFalse(scheduler.isTerminated());
        assertFalse(externalFinished.isDone());
        assertFalse(fixture.second.hasRunningNodeWriteTrx());
        final Path marker = ((InternalResourceSession<?, ?>) fixture.second).getCommitFile();
        assertFalse(Files.exists(marker));
        if (externalAction.equals("session") && !closeSession) {
          assertThrows(IllegalStateException.class, fixture.first::beginNodeTrx);
        }
        try (NodeTrx next = fixture.second.beginNodeTrx()) {
          if (storageType == StorageType.IN_MEMORY) {
            assertEmptyDocument(next);
            if (next instanceof JsonNodeTrx json) {
              json.insertNumberValueAsFirstChild(3);
            } else {
              final XmlNodeTrx xml = (XmlNodeTrx) next;
              xml.insertElementAsFirstChild(new QNm("root"));
              xml.insertTextAsFirstChild("3");
            }
            assertEquals(3, value(next));
            next.rollback();
            assertEmptyDocument(next);
          } else {
            setCursor(next);
            assertEquals(1, value(next));
            setValue(next, 3);
            next.commit();
          }
        }
        allowHookReturn.countDown();
        assertTrue(externalFinished.get(10, TimeUnit.SECONDS));
        assertTrue(scheduler.awaitTermination(10, TimeUnit.SECONDS));
        assertFalse(Files.exists(marker));
        assertEquals(0, laterHooks.get());
        transaction.close();
        assertEquals(1, ((AbstractResourceSession<?, ?>) fixture.second).writeLock.availablePermits());
        try (NodeTrx next = fixture.second.beginNodeTrx()) {
          if (storageType == StorageType.IN_MEMORY) {
            assertEmptyDocument(next);
          } else {
            setCursor(next);
            assertEquals(3, value(next));
          }
        }
      } finally {
        allowClose.countDown();
        allowHookReturn.countDown();
        external.join(10000);
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"json,false,false,false",
      "json,false,false,true",
      "json,false,true,false",
      "json,false,true,true",
      "json,true,false,false",
      "json,true,false,true",
      "json,true,true,false",
      "json,true,true,true",
      "xml,false,false,false",
      "xml,false,false,true",
      "xml,false,true,false",
      "xml,false,true,true",
      "xml,true,false,false",
      "xml,true,false,true",
      "xml,true,true,false",
      "xml,true,true,true"})
  void lateConcurrentCloseRetainsOwnerUntilPermitRelease(final String format, final boolean abort,
      final boolean timed, final boolean failWriter) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx transaction = timed
          ? fixture.first.beginNodeTrx(1, TimeUnit.DAYS)
          : fixture.first.beginNodeTrx();
      final int transactionId = transaction.getId();
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
        assertSame(transaction, session.nodeTrxMap.get(transactionId));
        assertFalse(session.isClosed());
        assertEquals(0, session.writeLock.availablePermits());
        second.start();
        secondFinished.get(10, TimeUnit.SECONDS);
        assertFalse(firstFinished.isDone());
        assertTrue(session.isClosed());
        assertTrue(session.nodeTrxMap.isEmpty());
        assertTrue(session.storageEngineWriterMap.isEmpty());
        assertEquals(1, session.writeLock.availablePermits());
        try (NodeTrx next = fixture.second.beginNodeTrx()) {
          setCursor(next);
          assertEquals(0, value(next));
          allowRelease.countDown();
          assertSame(failWriter ? primary : null, firstFinished.get(10, TimeUnit.SECONDS));
          transaction.close();
          fixture.first.close();
          assertEquals(0, session.writeLock.availablePermits());
          setValue(next, 3);
          next.commit();
        }
        assertEquals(1, session.writeLock.availablePermits());
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

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void ordinaryPostCommitHooksContinueInOrder(final String format) throws Exception {
    try (Fixture fixture = new Fixture(format, directory);
        NodeTrx transaction = fixture.first.beginNodeTrx()) {
      final AtomicInteger calls = new AtomicInteger();
      transaction.addPostCommitHook(committed -> assertEquals(0, calls.getAndIncrement()));
      transaction.addPostCommitHook(committed -> assertEquals(1, calls.getAndIncrement()));
      setValue(transaction, 1);
      transaction.commit();
      assertEquals(2, calls.get());
      assertFalse(transaction.isClosed());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void asynchronousPostCommitCloseStopsLaterHooks(final String format) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx transaction = fixture.first.beginNodeTrx(1, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT);
      final AtomicInteger laterHooks = new AtomicInteger();
      transaction.addPostCommitHook(closing -> closing.close());
      transaction.addPostCommitHook(closed -> laterHooks.incrementAndGet());
      setValue(transaction, 1);
      assertThrows(IllegalStateException.class, () -> setValue(transaction, 2));
      assertTrue(transaction.isClosed());
      assertTrue(scheduler(transaction).isTerminated());
      assertEquals(0, laterHooks.get());
      try (NodeTrx next = fixture.second.beginNodeTrx()) {
        setCursor(next);
        assertEquals(1, value(next));
      }
    }
  }

  private static void assertEmptyDocument(final NodeTrx transaction) {
    final NodeCursor cursor = (NodeCursor) transaction;
    assertTrue(cursor.moveToDocumentRoot());
    assertFalse(cursor.hasFirstChild());
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

  private static void setCursor(final NodeTrx transaction) {
    if (transaction instanceof JsonNodeTrx json) {
      assertTrue(json.moveTo(1));
    } else {
      assertTrue(((XmlNodeTrx) transaction).moveTo(2));
    }
  }

  private static void setValue(final NodeTrx transaction, final int value) {
    setCursor(transaction);
    if (transaction instanceof JsonNodeTrx json) {
      json.setNumberValue(value);
    } else {
      ((XmlNodeTrx) transaction).setValue(Integer.toString(value));
    }
  }

  private static int value(final NodeTrx transaction) {
    return transaction instanceof JsonNodeTrx json
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
