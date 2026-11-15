package io.sirix.access.trx.node;

import io.brackit.query.atomic.QNm;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.User;
import io.sirix.api.Database;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.exception.SirixUsageException;
import io.sirix.io.StorageType;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
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

@Isolated
@Timeout(30)
final class NodeTransactionAbortLifecycleTest {
  @TempDir
  Path directory;

  private Field flushHook;

  @AfterEach
  void clearHooks() throws ReflectiveOperationException {
    AbstractNodeTrxImpl.asyncCommitTestHook = null;
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
      final Path path = directory.resolve("database");
      if (format.equals("json")) {
        Databases.createJsonDatabase(new DatabaseConfiguration(path));
      } else {
        Databases.createXmlDatabase(new DatabaseConfiguration(path));
      }
      firstDatabase = open(format, path, new User("first", UUID.randomUUID()));
      secondDatabase = open(format, path, new User("second", UUID.randomUUID()));
      firstDatabase.createResource(ResourceConfiguration.newBuilder("resource")
                                                       .storageType(StorageType.FILE_CHANNEL)
                                                       .storeDiffs(false)
                                                       .hashKind(HashType.NONE)
                                                       .buildPathSummary(false)
                                                       .build());
      first = firstDatabase.beginResourceSession("resource");
      second = secondDatabase.beginResourceSession("resource");
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
