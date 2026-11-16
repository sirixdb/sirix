package io.sirix.access.trx.node;

import io.brackit.query.atomic.QNm;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.User;
import io.sirix.api.Database;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.index.IndexType;
import io.sirix.index.interval.ValidTimeReadCache;
import io.sirix.io.StorageType;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.page.UberPage;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.channels.ClosedByInterruptException;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

@Isolated
@Timeout(60)
final class SharedCallbackLifetimeTest {
  @TempDir
  Path directory;

  static Stream<Arguments> readerCases() {
    return Stream.of("json", "xml").flatMap(format ->
        Stream.of("node", "standalone", "queued").flatMap(mode ->
            Stream.of("direct", "shared", "page").flatMap(kind ->
                Stream.of(false, true).map(during -> Arguments.of(format, mode, kind, during)))));
  }

  @ParameterizedTest(name = "shared listener {arguments}")
  @MethodSource("readerCases")
  void crossSessionNotificationsFenceEveryReaderRepresentation(final String format, final String mode,
      final String kind, final boolean during) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final AbstractResourceSession<?, ?> owner = fixture.first;
      final boolean interrupt = kind.equals("page") || mode.equals("queued");
      final boolean failClose = kind.equals("shared") || mode.equals("queued");
      final IllegalStateException primary = new IllegalStateException("independent cache failure");
      final IllegalArgumentException later = new IllegalArgumentException("independent page failure");
      if (failClose) {
        final ValidTimeReadCache cache = mock(ValidTimeReadCache.class);
        doThrow(primary).when(cache).clear();
        field(AbstractResourceSession.class, "validTimeReadCache").set(owner, cache);
      }
      final AtomicBoolean once = new AtomicBoolean();
      final CountDownLatch entered = new CountDownLatch(1);
      final CountDownLatch navigate = new CountDownLatch(1);
      final CountDownLatch finish = new CountDownLatch(1);
      final CompletableFuture<Void> navigated = new CompletableFuture<>();
      final AtomicReference<NodeReadOnlyTrx> node = new AtomicReference<>();
      final AtomicReference<StorageEngineReader> page = new AtomicReference<>();
      fixture.second.addRevisionListener(revision -> {
        if (!once.compareAndSet(false, true)) {
          return;
        }
        try {
          if (!during) {
            openAndRead(owner, kind, node, page);
          }
          entered.countDown();
          await(navigate);
          if (during) {
            openAndRead(owner, kind, node, page);
          } else {
            read(node.get(), page.get());
          }
          assertFalse(owner.isClosed());
          assertThrows(IllegalStateException.class, owner::close);
          assertThrows(IllegalStateException.class, fixture.second::close);
          navigated.complete(null);
          await(finish);
        } catch (final Throwable failure) {
          navigated.completeExceptionally(failure);
          throw failure;
        }
      });
      final NodeTrx writer = mode.equals("node") ? fixture.second.beginNodeTrx(0, 1, TimeUnit.DAYS) : null;
      final StorageEngineWriter unbound = mode.equals("queued") ? fixture.second.createStorageEngineWriter() : null;
      final CompletableFuture<Void> published = new CompletableFuture<>();
      final Thread publisher = new Thread(() -> {
        try {
          if (writer != null) {
            assertTrue(writer.moveTo(format.equals("json") ? 1 : 2));
            if (writer instanceof JsonNodeTrx json) {
              json.setNumberValue(1);
            } else {
              ((XmlNodeTrx) writer).setValue("1");
            }
            writer.commit();
          } else if (unbound != null) {
            fixture.second.close();
          } else {
            fixture.second.setLastCommittedUberPage(new UberPage(2));
          }
          published.complete(null);
        } catch (final Throwable failure) {
          published.completeExceptionally(failure);
        }
      });
      final CompletableFuture<Void> closed = new CompletableFuture<>();
      final AtomicBoolean restored = new AtomicBoolean();
      final Thread closer = new Thread(() -> {
        try {
          owner.close();
          closed.complete(null);
        } catch (final Throwable failure) {
          closed.completeExceptionally(failure);
        } finally {
          restored.set(Thread.currentThread().isInterrupted());
        }
      });
      try {
        publisher.start();
        assertTrue(entered.await(10, TimeUnit.SECONDS));
        closer.start();
        @SuppressWarnings("unchecked")
        final Queue<CompletableFuture<Void>> completions =
            (Queue<CompletableFuture<Void>>) field(AbstractResourceSession.class, "notificationCompletions").get(owner);
        final CompletableFuture<Void> completion = completions.peek();
        assertTrue(completion != null);
        while (completion.getNumberOfDependents() == 0) {
          assertFalse(closed.isDone(), "reader owner retired before the admitted listener completed");
          Thread.onSpinWait();
        }
        if (interrupt) {
          closer.interrupt();
        }
        navigate.countDown();
        navigated.get(10, TimeUnit.SECONDS);
        assertFalse(page.get().isClosed());
        if (failClose) {
          final StorageEngineReader independent = owner.createStorageEngineReader(1);
          final StorageEngineReader failing = mock(StorageEngineReader.class, delegatesTo(independent));
          doAnswer(invocation -> {
            independent.close();
            throw later;
          }).when(failing).close();
          owner.storageEngineReaderMap.put(independent.getTrxId(), failing);
        }
        finish.countDown();
        published.get(10, TimeUnit.SECONDS);
        if (failClose) {
          assertSame(primary, assertThrows(ExecutionException.class,
              () -> closed.get(10, TimeUnit.SECONDS)).getCause());
          assertTrue(List.of(primary.getSuppressed()).contains(later));
        } else {
          closed.get(10, TimeUnit.SECONDS);
        }
        closer.join(10000);
        assertEquals(interrupt, restored.get());
        assertTrue(owner.isClosed());
        assertTrue(page.get().isClosed());
        if (node.get() != null) {
          assertTrue(node.get().isClosed());
          assertNull(field(AbstractNodeReadOnlyTrx.class, "currentPageGuard").get(node.get()));
        }
        assertTrue(owner.nodeTrxMap.isEmpty());
        assertTrue(owner.storageEngineReaderMap.isEmpty());
        if (writer != null) {
          writer.close();
        }
        assertEquals(1, owner.writeLock.availablePermits());
      } finally {
        navigate.countDown();
        finish.countDown();
        publisher.join(10000);
        closer.join(10000);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void notificationAdmissionCannotCrossReaderRetirement(final String format) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final ReentrantReadWriteLock ownership = (ReentrantReadWriteLock)
          field(AbstractResourceSession.class, "notificationOwnership").get(fixture.first);
      final CompletableFuture<Void> delivered = new CompletableFuture<>();
      fixture.second.addRevisionListener(revision -> {
        try (NodeReadOnlyTrx reader = fixture.second.beginNodeReadOnlyTrx(revision)) {
          assertTrue(reader.moveTo(format.equals("json") ? 1 : 2));
          assertEquals(0, number(reader));
          delivered.complete(null);
        } catch (final Throwable failure) {
          delivered.completeExceptionally(failure);
          throw failure;
        }
      });
      final CompletableFuture<Void> published = new CompletableFuture<>();
      final Thread publisher = new Thread(() -> {
        try {
          fixture.second.setLastCommittedUberPage(new UberPage(2));
          published.complete(null);
        } catch (final Throwable failure) {
          published.completeExceptionally(failure);
        }
      });
      fixture.first.beginNotificationRetirement();
      try {
        publisher.start();
        while (!ownership.hasQueuedThread(publisher)) {
          assertFalse(published.isDone());
          Thread.onSpinWait();
        }
        assertFalse(delivered.isDone());
      } finally {
        fixture.first.endNotificationRetirement();
      }
      published.get(10, TimeUnit.SECONDS);
      delivered.get(10, TimeUnit.SECONDS);
      publisher.join(10000);
    }
  }

  private static void openAndRead(final AbstractResourceSession<?, ?> owner, final String kind,
      final AtomicReference<NodeReadOnlyTrx> node, final AtomicReference<StorageEngineReader> page) {
    if (kind.equals("page")) {
      page.set(owner.createStorageEngineReader(1));
    } else {
      node.set(kind.equals("shared") ? owner.getOrCreateSharedReadOnlyTrx(1) : owner.beginNodeReadOnlyTrx(1));
      page.set(node.get().getStorageEngineReader());
    }
    read(node.get(), page.get());
  }

  private static void read(final NodeReadOnlyTrx node, final StorageEngineReader page) {
    assertFalse(page.isClosed());
    if (node != null) {
      assertTrue(node.moveTo(node instanceof JsonNodeReadOnlyTrx ? 1 : 2));
      assertEquals(0, number(node));
    } else {
      final DataRecord record = page.getRecord(1, IndexType.DOCUMENT, -1);
      assertEquals(1, record.getNodeKey());
    }
  }

  static Stream<Arguments> truncateCases() {
    return Stream.of("json", "xml").flatMap(format -> Stream.of(false, true).flatMap(interrupt ->
        Stream.of(false, true).map(error -> Arguments.of(format, interrupt, error))));
  }

  @ParameterizedTest(name = "truncate view {arguments}")
  @MethodSource("truncateCases")
  void truncationFencesPinnedDistantPages(final String format, final boolean interrupt, final boolean error)
      throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx writer = fixture.first.beginNodeTrx(0, 1, TimeUnit.DAYS);
      assertTrue(writer.moveTo(1));
      writer.remove();
      final long distant;
      if (writer instanceof JsonNodeTrx json) {
        json.moveTo(0);
        json.insertArrayAsFirstChild();
        json.insertNumberValueAsFirstChild(0);
        for (int value = 1; value < 12000; value++) {
          json.insertNumberValueAsRightSibling(value);
        }
        distant = json.getNodeKey();
      } else {
        final XmlNodeTrx xml = (XmlNodeTrx) writer;
        xml.moveTo(0);
        xml.insertElementAsFirstChild(new QNm("root"));
        xml.insertElementAsFirstChild(new QNm("n"));
        xml.insertTextAsFirstChild("0");
        for (int value = 1; value < 12000; value++) {
          xml.moveToParent();
          xml.insertElementAsRightSibling(new QNm("n"));
          xml.insertTextAsFirstChild(Integer.toString(value));
        }
        distant = xml.getNodeKey();
      }
      final TransactionExecutionLock execution =
          (TransactionExecutionLock) field(AbstractNodeTrxImpl.class, "executionLock").get(writer);
      final Condition ready = (Condition) field(TransactionExecutionLock.class, "ready").get(execution);
      final CountDownLatch entered = new CountDownLatch(1);
      final CountDownLatch inspect = new CountDownLatch(1);
      final CountDownLatch finish = new CountDownLatch(1);
      final CompletableFuture<Void> waiting = new CompletableFuture<>();
      final CompletableFuture<Void> navigated = new CompletableFuture<>();
      final AtomicReference<NodeReadOnlyTrx> view = new AtomicReference<>();
      final CompletableFuture<Void> truncated = new CompletableFuture<>();
      final AssertionError injected = new AssertionError("hook failure after distant navigation");
      writer.addPostCommitHook(pinned -> {
        view.set(pinned);
        try {
          assertEquals(2, pinned.getRevisionNumber());
          assertThrows(IllegalStateException.class, () -> writer.truncateTo(1));
          entered.countDown();
          await(inspect);
          boolean queued = false;
          while (!queued) {
            assertFalse(truncated.isDone(), "truncation returned before the pinned view completed");
            execution.lock();
            try {
              queued = execution.getWaitQueueLength(ready) > 0;
            } finally {
              execution.unlock();
            }
            Thread.onSpinWait();
          }
          waiting.complete(null);
          assertTrue(pinned.moveTo(distant));
          assertEquals(11999, number(pinned));
          navigated.complete(null);
          await(finish);
          if (error) {
            throw injected;
          }
        } catch (final Throwable failure) {
          waiting.completeExceptionally(failure);
          navigated.completeExceptionally(failure);
          throw failure;
        }
      });
      final CompletableFuture<Void> committed = new CompletableFuture<>();
      final Thread commit = new Thread(() -> {
        try {
          writer.commit();
          committed.complete(null);
        } catch (final Throwable failure) {
          committed.completeExceptionally(failure);
        }
      });
      final AtomicBoolean restored = new AtomicBoolean();
      final Thread truncate = new Thread(() -> {
        try {
          writer.truncateTo(1);
          truncated.complete(null);
        } catch (final Throwable failure) {
          truncated.completeExceptionally(failure);
        } finally {
          restored.set(Thread.currentThread().isInterrupted());
        }
      });
      try {
        commit.start();
        assertTrue(entered.await(10, TimeUnit.SECONDS));
        truncate.start();
        inspect.countDown();
        waiting.get(10, TimeUnit.SECONDS);
        if (interrupt) {
          truncate.interrupt();
        }
        navigated.get(10, TimeUnit.SECONDS);
        assertFalse(truncated.isDone());
        finish.countDown();
        committed.get(10, TimeUnit.SECONDS);
        if (interrupt) {
          final Throwable failure = assertThrows(ExecutionException.class,
              () -> truncated.get(10, TimeUnit.SECONDS)).getCause();
          assertTrue(causedByInterrupt(failure), failure.toString());
        } else {
          truncated.get(10, TimeUnit.SECONDS);
          assertEquals(1, fixture.first.getMostRecentRevisionNumber());
        }
        truncate.join(10000);
        assertEquals(interrupt, restored.get());
        assertTrue(view.get().isClosed());
        Throwable cleanup = null;
        try {
          writer.close();
        } catch (final RuntimeException | Error failure) {
          cleanup = failure;
        }
        if (error) {
          assertTrue(contains(cleanup, injected));
        } else if (!interrupt && cleanup != null) {
          throw new AssertionError(cleanup);
        }
        assertTrue(writer.isClosed());
        assertEquals(1, fixture.first.writeLock.availablePermits());
      } finally {
        inspect.countDown();
        finish.countDown();
        commit.join(10000);
        truncate.join(10000);
      }
    }
  }

  private static boolean causedByInterrupt(final Throwable failure) {
    return failure instanceof ClosedByInterruptException
        || failure.getCause() != null && causedByInterrupt(failure.getCause());
  }

  private static boolean contains(final Throwable failure, final Throwable expected) {
    if (failure == null) {
      return false;
    }
    if (failure == expected || contains(failure.getCause(), expected)) {
      return true;
    }
    for (final Throwable suppressed : failure.getSuppressed()) {
      if (contains(suppressed, expected)) {
        return true;
      }
    }
    return false;
  }

  private static int number(final NodeReadOnlyTrx node) {
    return node instanceof JsonNodeReadOnlyTrx json ? json.getNumberValue().intValue()
        : Integer.parseInt(node.getValue());
  }

  private static Field field(final Class<?> type, final String name) throws ReflectiveOperationException {
    final Field result = type.getDeclaredField(name);
    result.setAccessible(true);
    return result;
  }

  private static void await(final CountDownLatch latch) {
    try {
      latch.await();
    } catch (final InterruptedException failure) {
      Thread.currentThread().interrupt();
      throw new AssertionError(failure);
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final Database<? extends ResourceSession<?, ?>> firstDatabase;
    private final Database<? extends ResourceSession<?, ?>> secondDatabase;
    private final AbstractResourceSession<?, ?> first;
    private final AbstractResourceSession<?, ?> second;

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
          .storageType(StorageType.FILE_CHANNEL).storeDiffs(false).hashKind(HashType.NONE)
          .buildPathSummary(false).build());
      first = (AbstractResourceSession<?, ?>) firstDatabase.beginResourceSession("resource");
      second = (AbstractResourceSession<?, ?>) secondDatabase.beginResourceSession("resource");
      try (NodeTrx writer = first.beginNodeTrx()) {
        if (writer instanceof JsonNodeTrx json) {
          json.insertNumberValueAsFirstChild(0);
        } else {
          final XmlNodeTrx xml = (XmlNodeTrx) writer;
          xml.insertElementAsFirstChild(new QNm("root"));
          xml.insertTextAsFirstChild("0");
        }
        writer.commit();
      }
    }

    private static Database<? extends ResourceSession<?, ?>> open(final String format, final Path path,
        final User user) {
      return format.equals("json") ? Databases.openJsonDatabase(path, user) : Databases.openXmlDatabase(path, user);
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
