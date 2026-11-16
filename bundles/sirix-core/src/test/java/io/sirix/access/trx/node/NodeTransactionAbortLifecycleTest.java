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
import io.sirix.index.interval.ValidTimeReadCache;
import io.sirix.exception.SirixUsageException;
import io.sirix.exception.SirixIOException;
import io.sirix.io.StorageType;
import io.sirix.io.IOStorage;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.IntConsumer;
import java.util.function.Consumer;
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
        assertEquals(0, publications.get(), "notification waits for retirement and async slot release");
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

  @ParameterizedTest(name = "callback {index}: {arguments}")
  @CsvSource({"json,explicit", "xml,explicit", "json,count", "xml,count", "json,timed", "xml,timed",
      "json,async", "xml,async"})
  void callbacksRejectLifecycleBeforeLockingAndRunAfterExecution(final String format, final String mode)
      throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx transaction = switch (mode) {
        case "count" -> fixture.first.beginNodeTrx(1, 1, TimeUnit.DAYS);
        case "timed" -> fixture.first.beginNodeTrx(1, TimeUnit.SECONDS);
        case "async" -> fixture.first.beginNodeTrx(1, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT);
        default -> fixture.first.beginNodeTrx(0, 1, TimeUnit.DAYS);
      };
      final ReentrantLock execution = (ReentrantLock) field(AbstractNodeTrxImpl.class, "executionLock").get(transaction);
      final CountDownLatch beforeEntered = new CountDownLatch(1);
      final CountDownLatch releaseBefore = new CountDownLatch(1);
      final AtomicInteger before = new AtomicInteger();
      final AtomicInteger after = new AtomicInteger();
      final AtomicInteger notifications = new AtomicInteger();
      final CompletableFuture<Void> postCompleted = new CompletableFuture<>();
      final CompletableFuture<Void> published = new CompletableFuture<>();
      transaction.addPreCommitHook(unused -> {
        assertLifecycleRejected(fixture, transaction);
        before.incrementAndGet();
        if (mode.equals("count")) {
          beforeEntered.countDown();
          await(releaseBefore);
        }
      });
      ((InternalResourceSession<?, ?>) fixture.first).addRevisionListener(revision -> {
        try {
          assertFalse(execution.isHeldByCurrentThread());
          assertFalse(((TransactionExecutionLock) execution).isInScheduledTask());
          assertLifecycleRejected(fixture, transaction);
          notifications.incrementAndGet();
          published.complete(null);
        } catch (final Throwable failure) {
          published.completeExceptionally(failure);
          throw failure;
        }
      });
      transaction.addPostCommitHook(view -> {
        try {
          assertFalse(view instanceof NodeTrx);
          assertTrue(view != transaction);
          assertEquals(0, view.getNodeKey());
          assertFalse(execution.isHeldByCurrentThread());
          assertFalse(((TransactionExecutionLock) execution).isInScheduledTask());
          assertLifecycleRejected(fixture, transaction);
          assertTrue(fixture.second.hasRunningNodeWriteTrx());
          after.incrementAndGet();
          postCompleted.complete(null);
        } catch (final Throwable failure) {
          postCompleted.completeExceptionally(failure);
          throw failure;
        }
      });
      setValue(transaction, 1);
      if (mode.equals("explicit")) {
        transaction.runLocked(() -> transaction.commit());
      } else if (mode.equals("count")) {
        final CompletableFuture<Void> mutated = new CompletableFuture<>();
        final Thread setter = new Thread(() -> {
          try {
            setValue(transaction, 2);
            mutated.complete(null);
          } catch (final Throwable failure) {
            mutated.completeExceptionally(failure);
          }
        });
        try {
          setter.start();
          assertTrue(beforeEntered.await(10, TimeUnit.SECONDS));
          scheduler(transaction).execute(() -> transaction.runLocked(() -> {}));
          while (!execution.hasQueuedThreads()) {
            if (mutated.isDone()) {
              fail("setter exited before the scheduler queued on execution");
            }
            if (Thread.interrupted()) {
              throw new InterruptedException();
            }
            Thread.onSpinWait();
          }
          releaseBefore.countDown();
          mutated.get(10, TimeUnit.SECONDS);
        } finally {
          releaseBefore.countDown();
          setter.join(10000);
        }
      } else if (mode.equals("async")) {
        setValue(transaction, 2);
      }
      postCompleted.get(10, TimeUnit.SECONDS);
      published.get(10, TimeUnit.SECONDS);
      assertTrue(before.get() > 0);
      assertTrue(after.get() > 0);
      assertTrue(notifications.get() > 0);
      assertFalse(transaction.isClosed());
      fixture.first.close();
      assertTrue(transaction.isClosed());
      assertTrue(scheduler(transaction).isTerminated());
      assertEquals(1, ((AbstractResourceSession<?, ?>) fixture.second).writeLock.availablePermits());
      try (NodeTrx next = fixture.second.beginNodeTrx()) {
        setCursor(next);
        assertEquals(1, value(next));
        setValue(next, 3);
        next.commit();
      }
    }
  }

  @ParameterizedTest(name = "reader lifetime {index}: {arguments}")
  @CsvSource({"json,explicit", "xml,explicit", "json,count", "xml,count", "json,timed", "xml,timed",
      "json,async", "xml,async"})
  void externalCloseFencesIndependentRevisionReaders(final String format, final String mode) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx transaction = switch (mode) {
        case "count" -> fixture.first.beginNodeTrx(1, 1, TimeUnit.DAYS);
        case "timed" -> fixture.first.beginNodeTrx(1, TimeUnit.SECONDS);
        case "async" -> fixture.first.beginNodeTrx(1, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT);
        default -> fixture.first.beginNodeTrx(0, 1, TimeUnit.DAYS);
      };
      final TransactionExecutionLock execution =
          (TransactionExecutionLock) field(AbstractNodeTrxImpl.class, "executionLock").get(transaction);
      final Condition ready = (Condition) field(TransactionExecutionLock.class, "ready").get(execution);
      final CountDownLatch releaseHardening = new CountDownLatch(1);
      if (mode.equals("async")) {
        AbstractNodeTrxImpl.asyncCommitTestHook = stage -> {
          if (stage.equals("before-harden")) {
            await(releaseHardening);
          }
        };
      }
      final CountDownLatch entered = new CountDownLatch(1);
      final CountDownLatch observeCloser = new CountDownLatch(1);
      final CountDownLatch allowReturn = new CountDownLatch(1);
      final CompletableFuture<Void> closeWaiting = new CompletableFuture<>();
      final CompletableFuture<Void> closed = new CompletableFuture<>();
      final CompletableFuture<Void> triggered = new CompletableFuture<>();
      final AtomicReference<NodeReadOnlyTrx> snapshot = new AtomicReference<>();
      final Consumer<NodeReadOnlyTrx> observeRevision = view -> {
        try {
          assertFalse(execution.isHeldByCurrentThread());
          entered.countDown();
          await(observeCloser);
          boolean waiting = false;
          while (!waiting && allowReturn.getCount() != 0) {
            execution.lock();
            try {
              waiting = execution.getWaitQueueLength(ready) > 0;
            } finally {
              execution.unlock();
            }
            Thread.onSpinWait();
          }
          assertFalse(transaction.isClosed());
          snapshot.set(view);
          assertFalse(view instanceof NodeTrx);
          assertEquals(2, view.getRevisionNumber());
          setCursor(view);
          assertEquals(1, value(view));
          closeWaiting.complete(null);
          await(allowReturn);
        } catch (final Throwable failure) {
          closeWaiting.completeExceptionally(failure);
          throw failure;
        }
      };
      transaction.addPostCommitHook(observeRevision::accept);
      final Thread trigger = new Thread(() -> {
        try {
          setValue(transaction, 1);
          if (mode.equals("explicit")) {
            transaction.commit();
          } else if (!mode.equals("timed")) {
            setValue(transaction, 2);
          }
          triggered.complete(null);
        } catch (final Throwable failure) {
          triggered.completeExceptionally(failure);
        }
      });
      final Thread closer = new Thread(() -> {
        try {
          fixture.first.close();
          closed.complete(null);
        } catch (final Throwable failure) {
          closed.completeExceptionally(failure);
        }
      });
      try {
        trigger.start();
        if (mode.equals("async")) {
          triggered.get(10, TimeUnit.SECONDS);
          releaseHardening.countDown();
        }
        assertTrue(entered.await(10, TimeUnit.SECONDS));
        closer.start();
        observeCloser.countDown();
        closeWaiting.get(10, TimeUnit.SECONDS);
        assertFalse(closed.isDone());
        assertFalse(transaction.isClosed());
        assertTrue(fixture.second.hasRunningNodeWriteTrx());
        allowReturn.countDown();
        triggered.get(10, TimeUnit.SECONDS);
        closed.get(10, TimeUnit.SECONDS);
        assertTrue(transaction.isClosed());
        assertTrue(scheduler(transaction).isTerminated());
        assertTrue(snapshot.get().isClosed());
        assertEquals(1, ((AbstractResourceSession<?, ?>) fixture.second).writeLock.availablePermits());
        try (NodeTrx next = fixture.second.beginNodeTrx()) {
          setCursor(next);
          assertEquals(1, value(next));
          setValue(next, 3);
          next.commit();
        }
      } finally {
        releaseHardening.countDown();
        observeCloser.countDown();
        allowReturn.countDown();
        trigger.join(10000);
        closer.join(10000);
      }
    }
  }

  @SuppressWarnings("unchecked")
  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void publicationBeforeOwnerRegistrationStillUsesTheExecutionBoundary(final String format) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx transaction = fixture.first.beginNodeTrx(1, TimeUnit.DAYS);
      final AbstractResourceSession<?, ?> session = (AbstractResourceSession<?, ?>) fixture.first;
      final TransactionExecutionLock execution =
          (TransactionExecutionLock) field(AbstractNodeTrxImpl.class, "executionLock").get(transaction);
      final Map<Integer, NodeReadOnlyTrx> owners =
          (Map<Integer, NodeReadOnlyTrx>) (Map<?, ?>) session.nodeTrxMap;
      final AtomicInteger called = new AtomicInteger();
      final CompletableFuture<Void> observed = new CompletableFuture<>();
      session.addRevisionListener(revision -> {
        try {
          assertFalse(execution.isHeldByCurrentThread());
          assertLifecycleRejected(fixture, transaction);
          called.incrementAndGet();
          observed.complete(null);
        } catch (final Throwable failure) {
          observed.completeExceptionally(failure);
          throw failure;
        }
      });
      setValue(transaction, 1);
      final int id = transaction.getId();
      assertSame(transaction, owners.remove(id));
      try {
        transaction.commit();
        assertTrue(called.get() > 0);
        observed.get(10, TimeUnit.SECONDS);
      } finally {
        owners.put(id, transaction);
      }
      transaction.close();
      assertEquals(1, session.writeLock.availablePermits());
      try (NodeTrx next = fixture.second.beginNodeTrx()) {
        setCursor(next);
        assertEquals(1, value(next));
      }
    }
  }

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
            assertEquals(revision == 1 ? 0 : 1, value(view));
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
      assertEquals(1, ((AbstractResourceSession<?, ?>) fixture.second).writeLock.availablePermits());
      try (NodeTrx next = fixture.second.beginNodeTrx()) {
        setCursor(next);
        assertEquals(1, value(next));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void finalSessionNotificationsStillRejectLifecycleBeforeClosedChecks(final String format) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx retired = fixture.first.beginNodeTrx();
      retired.close();
      final StorageEngineWriter unbound = fixture.first.createStorageEngineWriter();
      final CompletableFuture<Void> notified = new CompletableFuture<>();
      ((InternalResourceSession<?, ?>) fixture.first).addRevisionListener(revision -> {
        try {
          assertTrue(fixture.first.isClosed());
          assertLifecycleRejected(fixture, retired);
          notified.complete(null);
        } catch (final Throwable failure) {
          notified.completeExceptionally(failure);
          throw failure;
        }
      });
      fixture.first.close();
      notified.get(10, TimeUnit.SECONDS);
      assertTrue(unbound.isClosed());
      assertEquals(1, ((AbstractResourceSession<?, ?>) fixture.second).writeLock.availablePermits());
      assertWriterReusable(fixture);
    }
  }

  @ParameterizedTest(name = "lineage {index}: {arguments}")
  @CsvSource({"json,false,false", "json,false,true", "json,true,false", "json,true,true",
      "xml,false,false", "xml,false,true", "xml,true,false", "xml,true,true"})
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
        assertEquals(1, ((AbstractResourceSession<?, ?>) fixture.second).writeLock.availablePermits());
        try (NodeTrx next = fixture.second.beginNodeTrx()) {
          setCursor(next);
          assertEquals(failHardening || truncate ? 0 : 1, value(next));
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

  private static void assertLifecycleRejected(final Fixture fixture, final NodeTrx transaction) {
    assertTrue(assertThrows(IllegalStateException.class, transaction::commit).getMessage().contains("callback"));
    assertTrue(assertThrows(IllegalStateException.class, transaction::rollback).getMessage().contains("callback"));
    assertTrue(assertThrows(IllegalStateException.class, transaction::close).getMessage().contains("callback"));
    assertTrue(assertThrows(IllegalStateException.class,
        () -> ((InternalNodeTrx<?>) transaction).abort()).getMessage().contains("callback"));
    assertTrue(assertThrows(IllegalStateException.class, fixture.first::close).getMessage().contains("callback"));
    assertTrue(assertThrows(IllegalStateException.class, fixture.first::beginNodeTrx)
        .getMessage().contains("callback"));
    assertTrue(assertThrows(IllegalStateException.class, fixture.first::createStorageEngineWriter)
        .getMessage().contains("callback"));
    assertTrue(assertThrows(IllegalStateException.class, () -> fixture.first.createStorageEngineWriter(1))
        .getMessage().contains("callback"));
    assertTrue(assertThrows(IllegalStateException.class, () -> transaction.revertTo(1))
        .getMessage().contains("callback"));
    assertTrue(assertThrows(IllegalStateException.class, () -> transaction.truncateTo(0))
        .getMessage().contains("callback"));
    assertTrue(assertThrows(IllegalStateException.class, transaction::awaitPendingAsyncCommit)
        .getMessage().contains("callback"));
    assertTrue(assertThrows(IllegalStateException.class,
        () -> transaction.markRollbackOnly(new IllegalStateException("from callback")))
        .getMessage().contains("callback"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void enclosingExecutionCannotEnterATerminationWait(final String format) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx transaction = fixture.first.beginNodeTrx(1, TimeUnit.DAYS);
      transaction.runLocked(() -> {
        assertThrows(IllegalStateException.class, transaction::close);
        assertThrows(IllegalStateException.class, fixture.first::close);
      });
      assertFalse(transaction.isClosed());
      assertFalse(fixture.first.isClosed());
      transaction.close();
      assertTrue(scheduler(transaction).isTerminated());
      assertEquals(1, ((AbstractResourceSession<?, ?>) fixture.second).writeLock.availablePermits());
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
          final ExecutionException rejected = assertThrows(ExecutionException.class,
              () -> admitted.get(5, TimeUnit.SECONDS));
          assertTrue(rejected.getCause() instanceof IllegalStateException);
          assertTrue(session.isClosed());
          assertTrue(session.nodeTrxMap.isEmpty());
          assertEquals(1, session.writeLock.availablePermits());
        } else {
          try (AutoCloseable next = admitted.get(5, TimeUnit.SECONDS)) {
            assertEquals(0, session.writeLock.availablePermits());
            if (next instanceof NodeTrx node) {
              setCursor(node);
              assertEquals(0, value(node));
              setValue(node, 3);
              node.commit();
            } else {
              assertFalse(((StorageEngineWriter) next).isClosed());
            }
          }
          assertEquals(1, session.writeLock.availablePermits());
        }
        try (NodeTrx next = fixture.second.beginNodeTrx()) {
          setCursor(next);
          assertEquals(!closeSession && !pageAdmission ? 3 : 0, value(next));
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

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void postNotificationFailureCannotVetoDurabilityOrStrandLaterHooks(final String format) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx transaction = fixture.first.beginNodeTrx();
      final AtomicInteger later = new AtomicInteger();
      final AssertionError error = new AssertionError("notification failed");
      final AtomicReference<NodeReadOnlyTrx> failedView = new AtomicReference<>();
      transaction.addPostCommitHook(view -> { failedView.set(view); throw error; });
      transaction.addPostCommitHook(unused -> later.incrementAndGet());
      setValue(transaction, 3);
      assertDoesNotThrow(() -> { transaction.commit(); });
      assertEquals(1, later.get());
      assertTrue(failedView.get().isClosed());
      assertFalse(Files.exists(((InternalResourceSession<?, ?>) fixture.first).getCommitFile()));
      assertSame(error, assertThrows(AssertionError.class, transaction::close));
      assertTrue(transaction.isClosed());
      assertEquals(1, ((AbstractResourceSession<?, ?>) fixture.second).writeLock.availablePermits());
      try (NodeTrx next = fixture.second.beginNodeTrx()) {
        setCursor(next);
        assertEquals(3, value(next));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void ordinaryPostCommitHooksContinueInOrder(final String format) throws Exception {
    try (Fixture fixture = new Fixture(format, directory);
        NodeTrx transaction = fixture.first.beginNodeTrx()) {
      final List<String> calls = new ArrayList<>();
      final AtomicReference<NodeReadOnlyTrx> first = new AtomicReference<>();
      final AtomicReference<NodeReadOnlyTrx> second = new AtomicReference<>();
      transaction.addPostCommitHook(committed -> {
        first.set(committed);
        assertFalse(committed instanceof NodeTrx);
        assertEquals(2, committed.getRevisionNumber());
        assertEquals(0, committed.getNodeKey());
        setCursor(committed);
        assertEquals(1, value(committed));
        calls.add("first");
      });
      transaction.addPostCommitHook(committed -> {
        second.set(committed);
        assertTrue(committed != first.get());
        assertEquals(0, committed.getNodeKey());
        assertEquals(2, committed.getRevisionNumber());
        calls.add("second");
      });
      setValue(transaction, 1);
      transaction.commit();
      assertEquals(List.of("first", "second"), calls);
      assertTrue(first.get().isClosed());
      assertTrue(second.get().isClosed());
      assertFalse(transaction.isClosed());
    }
  }


  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void asyncHooksPreserveSuccessorOverlapAndCompletionOrder(final String format) throws Exception {
    final CountDownLatch hardening = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx transaction = fixture.first.beginNodeTrx(2, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT);
      final TransactionExecutionLock execution =
          (TransactionExecutionLock) field(AbstractNodeTrxImpl.class, "executionLock").get(transaction);
      final AtomicReference<Thread> dispatcher = new AtomicReference<>();
      AbstractNodeTrxImpl.asyncCommitTestHook = stage -> {
        if (stage.equals("before-harden")) {
          dispatcher.set(Thread.currentThread());
          hardening.countDown();
          await(release);
        }
      };
      final List<String> calls = new ArrayList<>();
      final CompletableFuture<Void> notified = new CompletableFuture<>();
      transaction.addPostCommitHook(unused -> {
        assertSame(dispatcher.get(), Thread.currentThread());
        assertFalse(execution.isHeldByCurrentThread());
        assertFalse(execution.isInScheduledTask());
        assertLifecycleRejected(fixture, transaction);
        calls.add("first");
      });
      transaction.addPostCommitHook(unused -> {
        calls.add("second");
        notified.complete(null);
      });
      final CompletableFuture<Void> mutated = new CompletableFuture<>();
      final Thread foreground = new Thread(() -> {
        try {
          setValue(transaction, 1);
          setValue(transaction, 2);
          setValue(transaction, 3);
          setValue(transaction, 4);
          mutated.complete(null);
        } catch (final Throwable failure) {
          mutated.completeExceptionally(failure);
        }
      });
      try {
        foreground.start();
        assertTrue(hardening.await(10, TimeUnit.SECONDS));
        mutated.get(10, TimeUnit.SECONDS);
        assertEquals(1, release.getCount());
        assertFalse(notified.isDone());
        assertTrue(calls.isEmpty());
        release.countDown();
        notified.get(10, TimeUnit.SECONDS);
        assertEquals(List.of("first", "second"), calls);
        fixture.first.close();
        assertTrue(transaction.isClosed());
        assertEquals(1, ((AbstractResourceSession<?, ?>) fixture.second).writeLock.availablePermits());
        try (NodeTrx next = fixture.second.beginNodeTrx()) {
          setCursor(next);
          assertEquals(2, value(next));
        }
      } finally {
        release.countDown();
        foreground.join(10000);
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"json,false", "json,true", "xml,false", "xml,true"})
  void sessionCloseClosesSharedReadersCreatedByFencedCallbacks(final String format, final boolean failClose)
      throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx transaction = fixture.first.beginNodeTrx(0, 1, TimeUnit.DAYS);
      final AbstractResourceSession<?, ?> session = (AbstractResourceSession<?, ?>) fixture.first;
      final TransactionExecutionLock execution =
          (TransactionExecutionLock) field(AbstractNodeTrxImpl.class, "executionLock").get(transaction);
      final Condition ready = (Condition) field(TransactionExecutionLock.class, "ready").get(execution);
      final CountDownLatch entered = new CountDownLatch(1);
      final CountDownLatch openReader = new CountDownLatch(1);
      final CountDownLatch observeCloser = new CountDownLatch(1);
      final CompletableFuture<Void> closeWaiting = new CompletableFuture<>();
      final CountDownLatch returnCallback = new CountDownLatch(1);
      final CompletableFuture<NodeReadOnlyTrx> opened = new CompletableFuture<>();
      final AtomicReference<NodeReadOnlyTrx> independent = new AtomicReference<>();
      final IllegalStateException injected = new IllegalStateException("late reader close failed");
      final CompletableFuture<Void> committed = new CompletableFuture<>();
      final CompletableFuture<Void> closed = new CompletableFuture<>();
      transaction.addPostCommitHook(unused -> {
        try {
          entered.countDown();
          await(observeCloser);
          boolean waiting = false;
          while (!waiting) {
            execution.lock();
            try {
              waiting = execution.getWaitQueueLength(ready) > 0;
            } finally {
              execution.unlock();
            }
            if (Thread.interrupted()) {
              throw new AssertionError("callback interrupted before close queued");
            }
            Thread.onSpinWait();
          }
          closeWaiting.complete(null);
          await(openReader);
          final NodeReadOnlyTrx reader = session.getOrCreateSharedReadOnlyTrx(2);
          assertTrue(reader.moveTo(format.equals("json") ? 1 : 2));
          final NodeReadOnlyTrx other = session.beginNodeReadOnlyTrx(1);
          assertTrue(other.moveTo(format.equals("json") ? 1 : 2));
          independent.set(other);
          opened.complete(reader);
          await(returnCallback);
        } catch (final Throwable failure) {
          closeWaiting.completeExceptionally(failure);
          opened.completeExceptionally(failure);
          throw failure;
        }
      });
      final Thread commit = new Thread(() -> {
        try {
          setValue(transaction, 1);
          transaction.commit();
          committed.complete(null);
        } catch (final Throwable failure) {
          committed.completeExceptionally(failure);
        }
      });
      final Thread close = new Thread(() -> {
        try {
          session.close();
          closed.complete(null);
        } catch (final Throwable failure) {
          closed.completeExceptionally(failure);
        }
      });
      try {
        commit.start();
        assertTrue(entered.await(10, TimeUnit.SECONDS));
        close.start();
        observeCloser.countDown();
        closeWaiting.get(10, TimeUnit.SECONDS);
        openReader.countDown();
        final NodeReadOnlyTrx reader = opened.get(10, TimeUnit.SECONDS);
        final StorageEngineReader pageReader = reader.getStorageEngineReader();
        if (failClose) {
          final StorageEngineReader failing = mock(StorageEngineReader.class, delegatesTo(pageReader));
          doAnswer(invocation -> {
            pageReader.close();
            throw injected;
          }).when(failing).close();
          field(AbstractNodeReadOnlyTrx.class, "storageEngineReader").set(reader, failing);
        }
        final Field guard = field(AbstractNodeReadOnlyTrx.class, "currentPageGuard");
        assertTrue(guard.get(reader) instanceof PageGuard);
        assertFalse(reader.isClosed());
        returnCallback.countDown();
        committed.get(10, TimeUnit.SECONDS);
        if (failClose) {
          assertSame(injected, assertThrows(ExecutionException.class,
              () -> closed.get(10, TimeUnit.SECONDS)).getCause());
        } else {
          closed.get(10, TimeUnit.SECONDS);
        }
        assertTrue(independent.get().isClosed());
        assertNull(guard.get(independent.get()));
        assertTrue(reader.isClosed());
        assertNull(guard.get(reader));
        assertTrue(pageReader.isClosed());
        assertTrue(session.nodeTrxMap.isEmpty());
        assertEquals(1, session.writeLock.availablePermits());
      } finally {
        observeCloser.countDown();
        openReader.countDown();
        returnCallback.countDown();
        commit.join(10000);
        close.join(10000);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void asyncCallbackErrorsRemainObservableAfterIndependentDeliveryAndClose(final String format) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx transaction = fixture.first.beginNodeTrx(1, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT);
      final AssertionError primary = new AssertionError("async listener failed");
      final AssertionError later = new AssertionError("async hook failed");
      final CompletableFuture<Void> delivered = new CompletableFuture<>();
      final InternalResourceSession<?, ?> session = (InternalResourceSession<?, ?>) fixture.first;
      final IntConsumer listener = revision -> { throw primary; };
      session.addRevisionListener(listener);
      transaction.addPostCommitHook(unused -> { throw later; });
      transaction.addPostCommitHook(unused -> delivered.complete(null));
      setValue(transaction, 1);
      setValue(transaction, 2);
      delivered.get(10, TimeUnit.SECONDS);
      session.removeRevisionListener(listener);
      assertSame(primary, assertThrows(AssertionError.class, fixture.first::close));
      assertTrue(transaction.isClosed());
      assertTrue(scheduler(transaction).isTerminated());
      assertTrue(List.of(primary.getSuppressed()).contains(later));
      assertFalse(Files.exists(session.getCommitFile()));
      assertEquals(1, ((AbstractResourceSession<?, ?>) fixture.second).writeLock.availablePermits());
      try (NodeTrx next = fixture.second.beginNodeTrx()) {
        setCursor(next);
        assertEquals(1, value(next));
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
        assertEquals(1, ((AbstractResourceSession<?, ?>) fixture.second).writeLock.availablePermits());
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

  @ParameterizedTest
  @CsvSource({"json,false,false,false", "json,false,false,true", "json,false,true,false", "json,false,true,true",
      "json,true,false,false", "json,true,false,true", "json,true,true,false", "json,true,true,true",
      "xml,false,false,false", "xml,false,false,true", "xml,false,true,false", "xml,false,true,true",
      "xml,true,false,false", "xml,true,false,true", "xml,true,true,false", "xml,true,true,true"})
  void sessionCloseFencesCallbacksBeforeInitialReaders(final String format, final boolean shared,
      final boolean interruptCloser, final boolean failCleanup) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx writer = fixture.first.beginNodeTrx(0, 1, TimeUnit.DAYS);
      final AbstractResourceSession<?, ?> session = (AbstractResourceSession<?, ?>) fixture.first;
      final TransactionExecutionLock execution =
          (TransactionExecutionLock) field(AbstractNodeTrxImpl.class, "executionLock").get(writer);
      final Condition ready = (Condition) field(TransactionExecutionLock.class, "ready").get(execution);
      final CountDownLatch inspectClose = new CountDownLatch(1);
      final CountDownLatch navigate = new CountDownLatch(1);
      final CountDownLatch returnCallback = new CountDownLatch(1);
      final CompletableFuture<NodeReadOnlyTrx> opened = new CompletableFuture<>();
      final CompletableFuture<Void> waiting = new CompletableFuture<>();
      final CompletableFuture<Void> navigated = new CompletableFuture<>();
      final CompletableFuture<Void> committed = new CompletableFuture<>();
      final CompletableFuture<Throwable> closed = new CompletableFuture<>();
      final AtomicBoolean interruptedAtReturn = new AtomicBoolean();
      final AtomicInteger notifications = new AtomicInteger();
      final IllegalStateException primary = new IllegalStateException("cache close failed");
      final IllegalArgumentException secondary = new IllegalArgumentException("reader close failed");
      if (failCleanup) {
        final ValidTimeReadCache cache = mock(ValidTimeReadCache.class);
        doThrow(primary).when(cache).clear();
        field(AbstractResourceSession.class, "validTimeReadCache").set(session, cache);
      }
      ((InternalResourceSession<?, ?>) session).addRevisionListener(revision -> {
        if (notifications.getAndIncrement() != 0) {
          return;
        }
        try {
          final NodeReadOnlyTrx reader = shared
              ? session.getOrCreateSharedReadOnlyTrx(revision)
              : session.beginNodeReadOnlyTrx(revision);
          setCursor(reader);
          assertEquals(1, value(reader));
          opened.complete(reader);
          await(inspectClose);
          boolean queued = false;
          while (!queued && returnCallback.getCount() != 0) {
            execution.lock();
            try {
              queued = execution.getWaitQueueLength(ready) > 0;
            } finally {
              execution.unlock();
            }
            Thread.onSpinWait();
          }
          waiting.complete(null);
          await(navigate);
          assertFalse(reader.isClosed());
          assertTrue(reader.moveTo(0));
          setCursor(reader);
          assertEquals(1, value(reader));
          navigated.complete(null);
          await(returnCallback);
        } catch (final Throwable failure) {
          opened.completeExceptionally(failure);
          waiting.completeExceptionally(failure);
          navigated.completeExceptionally(failure);
          throw failure;
        }
      });
      final Thread commit = new Thread(() -> {
        try {
          setValue(writer, 1);
          writer.commit();
          committed.complete(null);
        } catch (final Throwable failure) {
          committed.completeExceptionally(failure);
        }
      });
      final Thread close = new Thread(() -> {
        Throwable failure = null;
        try {
          session.close();
        } catch (final Throwable next) {
          failure = next;
        } finally {
          interruptedAtReturn.set(Thread.currentThread().isInterrupted());
          closed.complete(failure);
        }
      });
      try {
        commit.start();
        final NodeReadOnlyTrx reader = opened.get(10, TimeUnit.SECONDS);
        final StorageEngineReader page = reader.getStorageEngineReader();
        final NodeReadOnlyTrx adverse = arrangeReaderBeforeWriter(session, reader, writer);
        doAnswer(invocation -> {
          reader.close();
          if (failCleanup) {
            throw secondary;
          }
          return null;
        }).when(adverse).close();
        final Field guard = field(AbstractNodeReadOnlyTrx.class, "currentPageGuard");
        assertTrue(guard.get(reader) instanceof PageGuard);
        close.start();
        inspectClose.countDown();
        waiting.get(10, TimeUnit.SECONDS);
        if (interruptCloser) {
          close.interrupt();
        }
        navigate.countDown();
        navigated.get(10, TimeUnit.SECONDS);
        assertFalse(closed.isDone());
        assertFalse(page.isClosed());
        assertTrue(guard.get(reader) instanceof PageGuard);
        returnCallback.countDown();
        committed.get(10, TimeUnit.SECONDS);
        final Throwable failure = closed.get(10, TimeUnit.SECONDS);
        if (failCleanup) {
          assertSame(primary, failure);
          final Throwable[] suppressed = failure.getSuppressed();
          if (interruptCloser && suppressed.length == 2) {
            assertInterruptedFileCleanup(suppressed[0]);
            assertSame(secondary, suppressed[1]);
          } else {
            assertEquals(1, suppressed.length);
            assertSame(secondary, suppressed[0]);
          }
        } else if (interruptCloser && failure != null) {
          assertInterruptedFileCleanup(failure);
        } else {
          assertNull(failure);
        }
        assertEquals(interruptCloser, interruptedAtReturn.get());
        assertTrue(reader.isClosed());
        assertTrue(page.isClosed());
        assertNull(guard.get(reader));
        assertTrue(session.nodeTrxMap.isEmpty());
        assertTrue(session.storageEngineReaderMap.isEmpty());
        assertTrue(session.storageEngineWriterMap.isEmpty());
        assertEquals(1, session.writeLock.availablePermits());
        verify(adverse).close();
        try (NodeTrx next = fixture.second.beginNodeTrx()) {
          setCursor(next);
          assertEquals(1, value(next));
        } catch (final SirixIOException cancellation) {
          assertTrue(interruptCloser);
          Throwable cause = cancellation;
          while (cause.getCause() != null) {
            cause = cause.getCause();
          }
          assertTrue(cause instanceof ClosedChannelException);
          assertEquals(1, session.writeLock.availablePermits());
          fixture.firstDatabase.close();
          fixture.secondDatabase.close();
          try (Database<? extends ResourceSession<?, ?>> reopened =
              Fixture.open(format, directory.resolve("database"), new User("reopened", UUID.randomUUID()));
              ResourceSession<?, ?> resource = reopened.beginResourceSession("resource");
              NodeTrx next = resource.beginNodeTrx()) {
            setCursor(next);
            assertEquals(1, value(next));
          }
        }
      } finally {
        inspectClose.countDown();
        navigate.countDown();
        returnCallback.countDown();
        commit.join(10000);
        close.join(10000);
      }
    }
  }

  private static void assertInterruptedFileCleanup(final Throwable failure) {
    assertTrue(failure instanceof SirixIOException);
    Throwable cause = failure;
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }
    assertTrue(cause instanceof ClosedByInterruptException);
  }

  @SuppressWarnings("unchecked")
  private static NodeReadOnlyTrx arrangeReaderBeforeWriter(final AbstractResourceSession<?, ?> session,
      final NodeReadOnlyTrx reader, final NodeTrx writer) throws ReflectiveOperationException {
    final Map<Integer, NodeReadOnlyTrx> nodes = (Map<Integer, NodeReadOnlyTrx>) (Map<?, ?>) session.nodeTrxMap;
    final Map<Object, NodeReadOnlyTrx> shared =
        (Map<Object, NodeReadOnlyTrx>) field(AbstractResourceSession.class, "sharedTrxMap").get(session);
    for (int attempt = 0; attempt < 128; attempt++) {
      final NodeReadOnlyTrx candidate = mock(NodeReadOnlyTrx.class, delegatesTo(reader));
      final InternalNodeTrx<?> writerOwner = mock(InternalNodeTrx.class, delegatesTo(writer));
      nodes.put(reader.getId(), candidate);
      nodes.put(writer.getId(), writerOwner);
      for (final Map.Entry<Object, NodeReadOnlyTrx> entry : shared.entrySet()) {
        if (entry.getValue().getId() == reader.getId()) {
          shared.put(entry.getKey(), candidate);
        }
      }
      final Set<NodeReadOnlyTrx> snapshot = Collections.newSetFromMap(new IdentityHashMap<>());
      snapshot.addAll(shared.values());
      snapshot.addAll(nodes.values());
      assertEquals(2, snapshot.size());
      assertTrue(snapshot.contains(writerOwner));
      if (snapshot.iterator().next() == candidate) {
        assertSame(candidate, snapshot.iterator().next());
        return candidate;
      }
    }
    throw new AssertionError("Could not arrange the adverse identity snapshot order");
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
      assertTrue(session.nodeTrxMap.containsKey(reader.getId()));
      assertFalse(session.isClosed());
      assertEquals(0, session.writeLock.availablePermits());
      session.close();
      assertTrue(reader.isClosed());
      assertTrue(page.isClosed());
      assertTrue(session.isClosed());
      assertEquals(1, session.writeLock.availablePermits());
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
    assertTrue(transaction.moveTo(transaction instanceof JsonNodeReadOnlyTrx ? 1 : 2));
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
