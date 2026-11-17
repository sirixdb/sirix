package io.sirix.access;

import io.brackit.query.atomic.QNm;
import io.sirix.access.trx.node.AbstractNodeTrxImpl;
import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.InternalResourceSession;
import io.sirix.api.Database;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.api.StorageEngineWriter;
import io.sirix.page.RevisionRootPage;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.io.StorageType;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import org.mockito.MockedStatic;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.atomic.AtomicBoolean;
import io.sirix.access.trx.TransactionManagerImpl;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.any;

@Isolated
@Timeout(30)
final class DatabaseNotificationLifecycleTest {
  @TempDir
  Path directory;

  @AfterEach
  void resetProbe() throws ReflectiveOperationException {
    final Field hook = field(AbstractNodeTrxImpl.class, "asyncCommitTestHook");
    hook.set(null, null);
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void timedCommitResumesAfterTemporaryAdmissionRefusal(final String format) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      assertTrue(fixture.firstDatabase.createResource(
          ResourceConfiguration.newBuilder("other").storageType(StorageType.FILE_CHANNEL).build()));
      final ResourceSession<?, ?> blockerSession = fixture.firstDatabase.beginResourceSession("other");
      final NodeTrx blocker = blockerSession.beginNodeTrx(0, 1, TimeUnit.DAYS);
      final DatabaseNotifications notifications = fixture.notifications();
      final DatabaseNotifications observed = mock(DatabaseNotifications.class, delegatesTo(notifications));
      final CountDownLatch refused = new CountDownLatch(1);
      final CountDownLatch published = new CountDownLatch(1);
      final ThreadFactory threads = Executors.defaultThreadFactory();
      final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(threads);
      final ScheduledExecutorService controlled = mock(ScheduledExecutorService.class, delegatesTo(executor));
      final AtomicReference<Runnable> tick = new AtomicReference<>();
      doAnswer(invocation -> {
        tick.set(invocation.getArgument(0));
        return mock(ScheduledFuture.class);
      }).when(controlled).scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
      doAnswer(invocation -> {
        try {
          notifications.runPreCommit(invocation.getArgument(0));
        } catch (final IllegalStateException failure) {
          refused.countDown();
          throw failure;
        }
        return null;
      }).when(observed).runPreCommit(any());
      field(AbstractResourceSession.class, "notifications").set(fixture.second, observed);
      final NodeTrx writer;
      try (MockedStatic<Executors> factory = mockStatic(Executors.class)) {
        factory.when(Executors::defaultThreadFactory).thenReturn(threads);
        factory.when(() -> Executors.newSingleThreadScheduledExecutor(any(ThreadFactory.class))).thenReturn(controlled);
        writer = fixture.second.beginNodeTrx(0, 10, TimeUnit.MILLISECONDS);
      }
      assertNotNull(tick.get());
      final CompletableFuture<Void> closed = new CompletableFuture<>();
      final AtomicReference<Thread> closer = new AtomicReference<>();
      try {
        writer.runLocked(() -> {
          writer.addPreCommitHook(unused -> {
          });
          writer.addPostCommitHook(unused -> published.countDown());
          setValue(writer, 1);
        });
        blocker.runLocked(() -> {
          closer.set(start(fixture.firstDatabase::close, closed));
          try {
            final ReentrantLock execution =
                (ReentrantLock) field(AbstractNodeTrxImpl.class, "executionLock").get(blocker);
            while (!execution.hasQueuedThread(closer.get())) {
              if (closed.isDone()) {
                closed.get(10, TimeUnit.SECONDS);
                throw new AssertionError("Handle close returned before its blocked writer cleanup");
              }
              Thread.onSpinWait();
            }
            executor.scheduleWithFixedDelay(tick.get(), 0, 10, TimeUnit.MILLISECONDS);
            assertTrue(refused.await(10, TimeUnit.SECONDS));
            assertEquals(1, fixture.second.getMostRecentRevisionNumber());
          } catch (final Exception failure) {
            throw new AssertionError(failure);
          }
        });
        closed.get(10, TimeUnit.SECONDS);
        assertTrue(published.await(10, TimeUnit.SECONDS));
        writer.runLocked(() -> {
          try (NodeReadOnlyTrx view = fixture.second.beginNodeReadOnlyTrx()) {
            assertValue(view, 1);
          }
        });
      } finally {
        blocker.close();
        blockerSession.close();
        writer.rollback();
        writer.close();
        if (closer.get() != null) {
          closer.get().join(10000);
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void overlappingRetirementsKeepAdmissionPausedUntilBothOwnersLeave(final String format) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final CountDownLatch entered = new CountDownLatch(1);
      final CountDownLatch leave = new CountDownLatch(1);
      final AtomicInteger calls = new AtomicInteger();
      final CompletableFuture<Void> completed = new CompletableFuture<>();
      Thread contender = null;
      try {
        try (var first = fixture.notifications().retire()) {
          contender = start(() -> {
            try (var second = fixture.notifications().retire()) {
              entered.countDown();
              await(leave);
            }
          }, completed);
          assertTrue(entered.await(10, TimeUnit.SECONDS));
          fixture.notifications().submit(calls::incrementAndGet);
          assertEquals(0, calls.get());
        }
        assertThrows(IllegalStateException.class, () -> fixture.notifications().runPreCommit(calls::incrementAndGet));
        assertEquals(0, calls.get());
      } finally {
        leave.countDown();
        if (contender != null) {
          contender.join(10000);
        }
      }
      completed.get(10, TimeUnit.SECONDS);
      fixture.notifications().awaitIdle(Duration.ofSeconds(10));
      assertEquals(1, calls.get());
      fixture.notifications().runPreCommit(calls::incrementAndGet);
      assertEquals(2, calls.get());
    }
  }

  static Stream<Arguments> ownerCases() {
    return Stream.of("json", "xml")
                 .flatMap(format -> Stream.of("session", "handle", "local").map(entry -> Arguments.of(format, entry)));
  }

  @ParameterizedTest
  @MethodSource("ownerCases")
  void executionOwnerCanQuiesceWhileCloseWaitsForExecution(final String format, final String entry) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx writer = fixture.second.beginNodeTrx(2, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT);
      final CountDownLatch hardening = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);
      field(AbstractNodeTrxImpl.class, "asyncCommitTestHook").set(null, (Consumer<String>) stage -> {
        if (stage.equals("before-harden")) {
          hardening.countDown();
          await(release);
          assertTrue(fixture.firstDatabase.existsResource("resource"));
          final long resource = fixture.firstDatabase.getResourceID("resource");
          assertEquals("resource", fixture.firstDatabase.getResourceName(resource));
        }
      });
      setValue(writer, 1);
      setValue(writer, 2);
      setValue(writer, 3);
      assertTrue(hardening.await(10, TimeUnit.SECONDS));
      final CompletableFuture<Void> truncated = new CompletableFuture<>();
      final CompletableFuture<Void> closed = new CompletableFuture<>();
      final Thread truncator = start(() -> writer.truncateTo(1), truncated);
      Thread closer = null;
      try {
        final Semaphore permit = (Semaphore) field(AbstractNodeTrxImpl.class, "asyncCommitPermit").get(writer);
        while (!permit.hasQueuedThreads()) {
          assertFalse(truncated.isDone());
          Thread.onSpinWait();
        }
        closer = start(() -> {
          try {
            switch (entry) {
              case "session" -> fixture.second.close();
              case "handle" -> fixture.secondDatabase.close();
              case "local" -> fixture.local.close();
              default -> throw new AssertionError(entry);
            }
          } catch (final RuntimeException failure) {
            assertFalse(hasTimeout(failure), "ordinary ownership overlap must not time out");
            assertTrue(writer.isClosed());
            assertTrue(fixture.second.isClosed());
          }
        }, closed);
        awaitRetirement(fixture, closed);
        release.countDown();
        truncated.get(10, TimeUnit.SECONDS);
        closed.get(10, TimeUnit.SECONDS);
        assertTrue(writer.isClosed());
        assertTrue(fixture.second.isClosed());
      } finally {
        release.countDown();
        truncator.join(10000);
        if (closer != null) {
          closer.join(10000);
        }
      }
    }
  }

  static Stream<Arguments> entryCases() {
    return Stream.of("json", "xml")
                 .flatMap(format -> Stream.of("pre", "post", "listener")
                                          .flatMap(phase -> Stream.of("handle", "local", "session", "writer", "commit",
                                              "rollback", "truncate", "page-close", "page-truncate", "page-commit",
                                              "page-rollback", "manager", "shutdown", "remove", "resource-remove",
                                              "resource-create", "facade-close", "facade-commit", "facade-rollback")
                                                                  .map(entry -> Arguments.of(format, phase, entry))));
  }

  @ParameterizedTest(name = "callback lifecycle {arguments}")
  @MethodSource("entryCases")
  void allLifecycleEntryPointsRejectCallbackReentry(final String format, final String phase, final String entry)
      throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx writer = fixture.second.beginNodeTrx();
      final CompletableFuture<Void> checked = new CompletableFuture<>();
      final Runnable operation = action(fixture, writer, entry);
      final AtomicBoolean invoked = new AtomicBoolean();
      final Consumer<NodeReadOnlyTrx> callback = view -> {
        if (!invoked.compareAndSet(false, true)) {
          return;
        }
        try {
          final IllegalStateException failure = assertThrows(IllegalStateException.class, operation::run);
          assertTrue(failure.getMessage().contains("callback"));
          assertTrue(fixture.firstDatabase.isOpen());
          assertFalse(fixture.first.isClosed());
          checked.complete(null);
        } catch (final Throwable failure) {
          checked.completeExceptionally(failure);
          throw failure;
        }
      };
      if (phase.equals("pre")) {
        writer.addPreCommitHook(callback::accept);
      } else if (phase.equals("post")) {
        writer.addPostCommitHook(callback::accept);
      } else {
        fixture.second.addRevisionListener(revision -> callback.accept(null));
      }
      setValue(writer, 1);
      writer.commit();
      checked.get(10, TimeUnit.SECONDS);
      fixture.notifications().awaitIdle(Duration.ofSeconds(10));
      writer.close();
    }
  }

  private static Runnable action(final Fixture fixture, final NodeTrx writer, final String entry) {
    final StorageEngineWriter pageWriter = writer.getStorageEngineWriter();
    return switch (entry) {
      case "handle" -> fixture.firstDatabase::close;
      case "local" -> fixture.local::close;
      case "session" -> fixture.first::close;
      case "writer" -> writer::close;
      case "commit" -> writer::commit;
      case "rollback" -> writer::rollback;
      case "truncate" -> () -> writer.truncateTo(1);
      case "page-close" -> pageWriter::close;
      case "page-truncate" -> () -> pageWriter.truncateTo(1);
      case "page-commit" -> () -> pageWriter.commit(null, null, false, false);
      case "page-rollback" -> pageWriter::rollback;
      case "manager" -> Databases.MANAGER::close;
      case "shutdown" -> Databases::shutdown;
      case "remove" -> () -> Databases.removeDatabase(fixture.path);
      case "resource-remove" -> () -> fixture.firstDatabase.removeResource("resource");
      case "resource-create" ->
        () -> fixture.firstDatabase.createResource(ResourceConfiguration.newBuilder("forbidden").build());
      case "facade-close" -> () -> new TransactionManagerImpl().beginTransaction().add(writer).close();
      case "facade-commit" -> () -> new TransactionManagerImpl().beginTransaction().add(writer).commit();
      case "facade-rollback" -> () -> new TransactionManagerImpl().beginTransaction().add(writer).rollback();
      default -> throw new AssertionError(entry);
    };
  }

  static Stream<Arguments> handleCycles() {
    return Stream.of("json", "xml")
                 .flatMap(format -> Stream.of("pre", "post", "listener").map(phase -> Arguments.of(format, phase)));
  }

  @ParameterizedTest
  @MethodSource("handleCycles")
  void closingHandleRejectsCallbackBeforeItsClosingWait(final String format, final String phase) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx writer = fixture.second.beginNodeTrx();
      final CountDownLatch entered = new CountDownLatch(1);
      final CountDownLatch reenter = new CountDownLatch(1);
      final CompletableFuture<Void> checked = new CompletableFuture<>();
      final Consumer<NodeReadOnlyTrx> callback = view -> {
        try {
          entered.countDown();
          await(reenter);
          assertThrows(IllegalStateException.class, fixture.firstDatabase::close);
          checked.complete(null);
        } catch (final Throwable failure) {
          checked.completeExceptionally(failure);
          throw failure;
        }
      };
      if (phase.equals("pre")) {
        writer.addPreCommitHook(callback::accept);
      } else if (phase.equals("post")) {
        writer.addPostCommitHook(callback::accept);
      } else {
        fixture.second.addRevisionListener(revision -> callback.accept(null));
      }
      final CompletableFuture<Void> committed = new CompletableFuture<>();
      final Thread commit = start(() -> {
        setValue(writer, 1);
        writer.commit();
      }, committed);
      final CompletableFuture<Void> closed = new CompletableFuture<>();
      Thread closer = null;
      try {
        assertTrue(entered.await(10, TimeUnit.SECONDS));
        closer = start(fixture.firstDatabase::close, closed);
        final Field closing = field(DatabaseHandle.class, "closing");
        while (!closing.getBoolean(fixture.firstDatabase)) {
          assertFalse(closed.isDone());
          Thread.onSpinWait();
        }
        reenter.countDown();
        checked.get(10, TimeUnit.SECONDS);
        committed.get(10, TimeUnit.SECONDS);
        closed.get(10, TimeUnit.SECONDS);
        writer.close();
      } finally {
        reenter.countDown();
        commit.join(10000);
        if (closer != null) {
          closer.join(10000);
        }
      }
    }
  }

  static Stream<Arguments> boundedCases() {
    return Stream.of("json", "xml")
                 .flatMap(format -> Stream.of("post", "listener")
                                          .flatMap(phase -> Stream
                                                                  .of("handle", "local", "session", "truncate",
                                                                      "page-truncate", "manager", "shutdown")
                                                                  .map(entry -> Arguments.of(format, phase, entry))));
  }

  @ParameterizedTest(name = "bounded close {arguments}")
  @MethodSource("boundedCases")
  void unreturnedDeliveryRetainsReadersDropsQueueAndAllowsRetry(final String format, final String phase,
      final String entry) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      fixture.notifications().setCloseTimeout(Duration.ofMillis(25));
      final NodeTrx writer = fixture.second.beginNodeTrx();
      final CountDownLatch entered = new CountDownLatch(1);
      final CountDownLatch resume = new CountDownLatch(1);
      final CompletableFuture<Void> readAfterTimeout = new CompletableFuture<>();
      final AtomicReference<NodeReadOnlyTrx> borrowed = new AtomicReference<>();
      final AtomicInteger deliveries = new AtomicInteger();
      final Consumer<NodeReadOnlyTrx> callback = view -> {
        if (deliveries.incrementAndGet() != 1) {
          return;
        }
        try (NodeReadOnlyTrx independent = fixture.first.beginNodeReadOnlyTrx(2)) {
          borrowed.set(independent);
          assertValue(independent, 1);
          entered.countDown();
          await(resume);
          assertValue(independent, 1);
          readAfterTimeout.complete(null);
        } catch (final Throwable failure) {
          readAfterTimeout.completeExceptionally(failure);
          throw failure;
        }
      };
      if (phase.equals("post")) {
        writer.addPostCommitHook(callback::accept);
        writer.addPostCommitHook(view -> deliveries.incrementAndGet());
      } else {
        fixture.second.addRevisionListener(revision -> callback.accept(null));
        fixture.second.addRevisionListener(revision -> deliveries.incrementAndGet());
      }
      try {
        setValue(writer, 1);
        writer.commit();
        assertTrue(entered.await(10, TimeUnit.SECONDS));
        final CompletableFuture<Void> closed = new CompletableFuture<>();
        final Thread closer = start(action(fixture, writer, entry), closed);
        final Throwable timeout =
            assertThrows(ExecutionException.class, () -> closed.get(10, TimeUnit.SECONDS)).getCause();
        assertTrue(hasTimeout(timeout), timeout.toString());
        closer.join(10000);
        assertEquals(1, deliveries.get());
        assertFalse(borrowed.get().isClosed());
        assertFalse(fixture.first.isClosed());
        assertTrue(DatabasesInternals.getOpenDatabases().get(fixture.path).contains(fixture.firstDatabase));
        assertEquals(2, fixture.second.getMostRecentRevisionNumber());
        resume.countDown();
        readAfterTimeout.get(10, TimeUnit.SECONDS);
        fixture.notifications().awaitIdle(Duration.ofSeconds(10));
        assertEquals(1, deliveries.get());
        fixture.firstDatabase.close();
        fixture.secondDatabase.close();
        assertTrue(borrowed.get().isClosed());
      } finally {
        resume.countDown();
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void asyncDurabilityAndBlockedDeliveryDoNotBlockSuccessorOrWriterClose(final String format) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx writer = fixture.second.beginNodeTrx(2, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT);
      final CountDownLatch hardening = new CountDownLatch(1);
      final CountDownLatch durable = new CountDownLatch(1);
      final CountDownLatch entered = new CountDownLatch(1);
      final CountDownLatch finish = new CountDownLatch(1);
      final CompletableFuture<Void> read = new CompletableFuture<>();
      final AtomicReference<Thread> dispatcher = new AtomicReference<>();
      final AtomicReference<NodeReadOnlyTrx> view = new AtomicReference<>();
      final List<Integer> order = new ArrayList<>();
      final Field hook = field(AbstractNodeTrxImpl.class, "asyncCommitTestHook");
      hook.set(null, (Consumer<String>) stage -> {
        if (stage.equals("before-harden")) {
          hardening.countDown();
          await(durable);
        }
      });
      writer.addPostCommitHook(pinned -> {
        view.set(pinned);
        dispatcher.set(Thread.currentThread());
        try {
          assertEquals(2, pinned.getRevisionNumber());
          assertValue(pinned, 2);
          order.add(1);
          entered.countDown();
          await(finish);
          assertValue(pinned, 2);
          read.complete(null);
        } catch (final Throwable failure) {
          read.completeExceptionally(failure);
          throw failure;
        }
      });
      writer.addPostCommitHook(pinned -> order.add(2));
      final Thread caller = Thread.currentThread();
      try {
        final CompletableFuture<Void> mutated = new CompletableFuture<>();
        final Thread foreground = start(() -> {
          setValue(writer, 1);
          setValue(writer, 2);
          setValue(writer, 3);
          setValue(writer, 4);
        }, mutated);
        assertTrue(hardening.await(10, TimeUnit.SECONDS));
        mutated.get(10, TimeUnit.SECONDS);
        assertEquals(1, entered.getCount());
        durable.countDown();
        assertTrue(entered.await(10, TimeUnit.SECONDS));
        assertNotSame(caller, dispatcher.get());
        final CompletableFuture<Void> closed = new CompletableFuture<>();
        final Thread closer = start(() -> {
          writer.rollback();
          writer.close();
        }, closed);
        closed.get(10, TimeUnit.SECONDS);
        assertEquals(1, finish.getCount());
        assertFalse(view.get().isClosed());
        finish.countDown();
        read.get(10, TimeUnit.SECONDS);
        fixture.notifications().awaitIdle(Duration.ofSeconds(10));
        assertEquals(List.of(1, 2), order);
        assertTrue(view.get().isClosed());
        foreground.join(10000);
        closer.join(10000);
      } finally {
        durable.countDown();
        finish.countDown();
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void preHookVetoRemainsSynchronousAndRetryable(final String format) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx writer = fixture.second.beginNodeTrx();
      final IllegalStateException veto = new IllegalStateException("pre-hook veto");
      final AtomicInteger calls = new AtomicInteger();
      final Thread caller = Thread.currentThread();
      writer.addPreCommitHook(view -> {
        assertSame(caller, Thread.currentThread());
        assertEquals(1, fixture.second.getMostRecentRevisionNumber());
        if (calls.getAndIncrement() == 0) {
          throw veto;
        }
      });
      setValue(writer, 1);
      assertSame(veto, assertThrows(IllegalStateException.class, writer::commit));
      assertEquals(1, fixture.second.getMostRecentRevisionNumber());
      writer.commit();
      assertEquals(2, fixture.second.getMostRecentRevisionNumber());
      writer.close();
    }
  }

  @ParameterizedTest
  @MethodSource("handleCycles")
  void anotherDatabaseRemainsIndependentDuringCallbacks(final String format, final String phase) throws Exception {
    try (Fixture source = new Fixture(format, directory.resolve("source"));
        Fixture other = new Fixture(format, directory.resolve("other"))) {
      final NodeTrx writer = source.second.beginNodeTrx();
      final CompletableFuture<Void> checked = new CompletableFuture<>();
      final AtomicBoolean once = new AtomicBoolean();
      final Consumer<NodeReadOnlyTrx> callback = unused -> {
        if (!once.compareAndSet(false, true)) {
          return;
        }
        try {
          try (NodeTrx nested = other.second.beginNodeTrx()) {
            nested.addPreCommitHook(view -> {
              assertThrows(IllegalStateException.class, source.firstDatabase::close);
              assertThrows(IllegalStateException.class, writer::commit);
            });
            setValue(nested, 1);
            nested.commit();
          }
          other.first.close();
          other.firstDatabase.close();
          Databases.removeDatabase(other.path);
          assertTrue(source.firstDatabase.isOpen());
          checked.complete(null);
        } catch (final Throwable failure) {
          checked.completeExceptionally(failure);
          throw failure;
        }
      };
      if (phase.equals("pre")) {
        writer.addPreCommitHook(callback::accept);
      } else if (phase.equals("post")) {
        writer.addPostCommitHook(callback::accept);
      } else {
        source.second.addRevisionListener(revision -> callback.accept(null));
      }
      setValue(writer, 1);
      writer.commit();
      checked.get(10, TimeUnit.SECONDS);
      source.notifications().awaitIdle(Duration.ofSeconds(10));
      writer.close();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void notificationErrorCannotVetoDurabilityOrSkipTheNextView(final String format) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx writer = fixture.second.beginNodeTrx();
      final AssertionError primary = new AssertionError("reported asynchronous callback failure");
      final CompletableFuture<Void> first = new CompletableFuture<>();
      final CompletableFuture<Void> next = new CompletableFuture<>();
      final List<String> order = new ArrayList<>();
      writer.addPostCommitHook(view -> {
        try {
          assertValue(view, 1);
          order.add("failed");
          first.complete(null);
        } catch (final Throwable failure) {
          first.completeExceptionally(failure);
          throw failure;
        }
        throw primary;
      });
      writer.addPostCommitHook(view -> {
        try {
          assertValue(view, 1);
          assertEquals(2, view.getRevisionNumber());
          order.add("continued");
          next.complete(null);
        } catch (final Throwable failure) {
          next.completeExceptionally(failure);
          throw failure;
        }
      });
      setValue(writer, 1);
      writer.commit();
      first.get(10, TimeUnit.SECONDS);
      next.get(10, TimeUnit.SECONDS);
      fixture.notifications().awaitIdle(Duration.ofSeconds(10));
      assertEquals(List.of("failed", "continued"), order);
      writer.close();
      try (NodeReadOnlyTrx view = fixture.second.beginNodeReadOnlyTrx(2)) {
        assertValue(view, 1);
      }
    }
  }

  static Stream<Arguments> boundedPreCases() {
    return Stream.of("json", "xml")
                 .flatMap(format -> Stream
                                          .of("handle", "local", "session", "writer", "truncate", "page-truncate",
                                              "manager", "shutdown", "facade-close")
                                          .map(entry -> Arguments.of(format, entry)));
  }

  @ParameterizedTest
  @MethodSource("boundedPreCases")
  void blockedPreHookCannotPinAnExternalCloserForever(final String format, final String entry) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      fixture.notifications().setCloseTimeout(Duration.ofMillis(25));
      final NodeTrx writer = fixture.second.beginNodeTrx();
      final CountDownLatch entered = new CountDownLatch(1);
      final CountDownLatch finish = new CountDownLatch(1);
      final CompletableFuture<Void> read = new CompletableFuture<>();
      final AtomicReference<NodeReadOnlyTrx> borrowed = new AtomicReference<>();
      writer.addPreCommitHook(unused -> {
        try (NodeReadOnlyTrx view = fixture.first.beginNodeReadOnlyTrx(1)) {
          borrowed.set(view);
          assertValue(view, 0);
          entered.countDown();
          await(finish);
          assertValue(view, 0);
          read.complete(null);
        } catch (final Throwable failure) {
          read.completeExceptionally(failure);
          throw failure;
        }
      });
      final CompletableFuture<Void> committed = new CompletableFuture<>();
      final Thread commit = start(() -> {
        setValue(writer, 1);
        writer.commit();
      }, committed);
      try {
        assertTrue(entered.await(10, TimeUnit.SECONDS));
        final CompletableFuture<Void> closed = new CompletableFuture<>();
        final Thread closer = start(action(fixture, writer, entry), closed);
        final Throwable failure =
            assertThrows(ExecutionException.class, () -> closed.get(10, TimeUnit.SECONDS)).getCause();
        assertTrue(hasTimeout(failure));
        assertFalse(borrowed.get().isClosed());
        assertEquals(1, fixture.second.getMostRecentRevisionNumber());
        finish.countDown();
        read.get(10, TimeUnit.SECONDS);
        committed.get(10, TimeUnit.SECONDS);
        fixture.firstDatabase.close();
        writer.close();
        closer.join(10000);
      } finally {
        finish.countDown();
        commit.join(10000);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void notificationViewUsesDurableRootWhilePendingCleanupIsBlocked(final String format) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx writer = fixture.second.beginNodeTrx(2, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT);
      final StorageEngineWriter predecessor = writer.getStorageEngineWriter();
      final CountDownLatch hardening = new CountDownLatch(1);
      final CountDownLatch durable = new CountDownLatch(1);
      final CountDownLatch clear = new CountDownLatch(1);
      final CountDownLatch readAgain = new CountDownLatch(1);
      final CompletableFuture<Void> opened = new CompletableFuture<>();
      final CompletableFuture<Void> read = new CompletableFuture<>();
      final AtomicReference<RevisionRootPage> pending = new AtomicReference<>();
      final InternalResourceSession<?, ?> proxy = mock(InternalResourceSession.class, delegatesTo(fixture.second));
      doAnswer(invocation -> {
        await(clear);
        fixture.second.clearPendingRevisionRoot(invocation.getArgument(0));
        return null;
      }).when(proxy).clearPendingRevisionRoot(anyInt());
      field(AbstractNodeTrxImpl.class, "resourceSession").set(writer, proxy);
      field(AbstractNodeTrxImpl.class, "asyncCommitTestHook").set(null, (Consumer<String>) stage -> {
        if (stage.equals("before-harden")) {
          hardening.countDown();
          await(durable);
        }
      });
      writer.addPostCommitHook(view -> {
        try {
          assertNotSame(pending.get(), view.getStorageEngineReader().getActualRevisionRootPage());
          assertValue(view, 2);
          opened.complete(null);
          await(readAgain);
          assertTrue(predecessor.isClosed());
          assertValue(view, 2);
          read.complete(null);
        } catch (final Throwable failure) {
          opened.completeExceptionally(failure);
          read.completeExceptionally(failure);
          throw failure;
        }
      });
      try {
        setValue(writer, 1);
        setValue(writer, 2);
        setValue(writer, 3);
        assertTrue(hardening.await(10, TimeUnit.SECONDS));
        pending.set(fixture.second.getPendingRevisionRoot(2));
        assertNotNull(pending.get());
        durable.countDown();
        opened.get(10, TimeUnit.SECONDS);
        clear.countDown();
        writer.awaitPendingAsyncCommit();
        readAgain.countDown();
        read.get(10, TimeUnit.SECONDS);
        writer.rollback();
        writer.close();
      } finally {
        durable.countDown();
        clear.countDown();
        readAgain.countDown();
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "xml"})
  void inlineHardeningFallbackStillDeliversDurableHooks(final String format) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx writer = fixture.second.beginNodeTrx(2, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT);
      final IllegalStateException primary = new IllegalStateException("submission failed before worker creation");
      final CompletableFuture<Void> delivered = new CompletableFuture<>();
      field(AbstractNodeTrxImpl.class, "asyncCommitTestHook").set(null, (Consumer<String>) stage -> {
        if (stage.equals("before-submit")) {
          throw primary;
        }
      });
      writer.addPostCommitHook(view -> {
        try {
          assertEquals(2, view.getRevisionNumber());
          assertValue(view, 2);
          delivered.complete(null);
        } catch (final Throwable failure) {
          delivered.completeExceptionally(failure);
          throw failure;
        }
      });
      setValue(writer, 1);
      setValue(writer, 2);
      assertSame(primary, assertThrows(IllegalStateException.class, () -> setValue(writer, 3)));
      delivered.get(10, TimeUnit.SECONDS);
      writer.close();
    }
  }

  @ParameterizedTest
  @CsvSource({"json,false", "xml,false", "json,true", "xml,true"})
  void refusedNodeExecutionRetainsItsPageWriterUntilRetry(final String format, final boolean local) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      fixture.notifications().setCloseTimeout(Duration.ofMillis(25));
      final NodeTrx writer = fixture.second.beginNodeTrx(0, 1, TimeUnit.DAYS);
      final StorageEngineWriter pageWriter = writer.getStorageEngineWriter();
      final NodeReadOnlyTrx reader = fixture.second.beginNodeReadOnlyTrx(1);
      final Runnable close = local
          ? fixture.local::close
          : fixture.second::close;
      final CountDownLatch entered = new CountDownLatch(1);
      final CountDownLatch finish = new CountDownLatch(1);
      final CompletableFuture<Void> finished = new CompletableFuture<>();
      final Thread owner = start(() -> writer.runLocked(() -> {
        entered.countDown();
        await(finish);
        assertFalse(pageWriter.isClosed());
        setValue(writer, 1);
      }), finished);
      try {
        assertTrue(entered.await(10, TimeUnit.SECONDS));
        final RuntimeException failure = assertThrows(RuntimeException.class, close::run);
        assertTrue(hasTimeout(failure));
        assertFalse(pageWriter.isClosed());
        assertFalse(writer.isClosed());
        assertFalse(fixture.second.isClosed());
        assertFalse(reader.isClosed());
        assertValue(reader, 0);
        if (local) {
          assertTrue(fixture.local.isOpen());
          assertTrue(fixture.firstDatabase.existsResource("resource"));
          assertThrows(IllegalStateException.class, () -> fixture.firstDatabase.createResource(
              ResourceConfiguration.newBuilder("late").storageType(StorageType.FILE_CHANNEL).build()));
          assertFalse(Files.exists(fixture.path.resolve("data").resolve("late")));
        }
        finish.countDown();
        finished.get(10, TimeUnit.SECONDS);
        close.run();
        assertTrue(writer.isClosed());
        assertTrue(pageWriter.isClosed());
        assertTrue(fixture.second.isClosed());
        assertTrue(reader.isClosed());
        if (local) {
          assertFalse(fixture.local.isOpen());
        }
      } finally {
        finish.countDown();
        owner.join(10000);
      }
    }
  }

  static Stream<Arguments> rawPublicationCases() {
    return Stream.of("json", "xml")
                 .flatMap(format -> Stream.of(false, true).map(truncate -> Arguments.of(format, truncate)));
  }

  @ParameterizedTest
  @MethodSource("rawPublicationCases")
  void idleRawPagePublicationDeliversBeforeSequentialHandleCleanup(final String format, final boolean truncate)
      throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx writer = fixture.second.beginNodeTrx();
      setValue(writer, 1);
      writer.commit();
      final int revision = truncate
          ? 1
          : 2;
      final CompletableFuture<Void> observed = new CompletableFuture<>();
      fixture.second.addRevisionListener(published -> {
        try (NodeReadOnlyTrx reader = fixture.first.beginNodeReadOnlyTrx(published)) {
          assertEquals(revision, published);
          assertValue(reader, truncate
              ? 0
              : 1);
          observed.complete(null);
        } catch (final Throwable failure) {
          observed.completeExceptionally(failure);
          throw failure;
        }
      });
      if (truncate) {
        writer.getStorageEngineWriter().truncateTo(1);
      } else {
        writer.getStorageEngineWriter().close();
      }
      fixture.firstDatabase.close();
      fixture.secondDatabase.close();
      observed.get(10, TimeUnit.SECONDS);
      assertFalse(fixture.firstDatabase.isOpen());
      assertFalse(fixture.secondDatabase.isOpen());
    }
  }

  static Stream<Arguments> resourceLifecycleCases() {
    return Stream.of("json", "xml")
                 .flatMap(
                     format -> Stream.of(false, true)
                                     .flatMap(local -> Stream.of(false, true)
                                                             .flatMap(remove -> Stream.of("pre", "post", "listener")
                                                                                      .map(phase -> Arguments.of(format,
                                                                                          local, remove, phase)))));
  }

  @ParameterizedTest(name = "resource monitor order {arguments}")
  @MethodSource("resourceLifecycleCases")
  void resourceLifecycleQuiescesBeforeMetadataMonitors(final String format, final boolean local, final boolean remove,
      final String phase) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final AbstractResourceSession<?, ?> retained = retainedResourceForRemoval(fixture, remove);
      final NodeTrx writer = fixture.second.beginNodeTrx();
      final CountDownLatch entered = new CountDownLatch(1);
      final CountDownLatch inspect = new CountDownLatch(1);
      final CompletableFuture<Void> read = new CompletableFuture<>();
      final AtomicBoolean invoked = new AtomicBoolean();
      final Consumer<NodeReadOnlyTrx> callback = unused -> {
        if (!invoked.compareAndSet(false, true)) {
          return;
        }
        try {
          entered.countDown();
          await(inspect);
          assertMetadataReadable(fixture);
          read.complete(null);
        } catch (final Throwable failure) {
          read.completeExceptionally(failure);
          throw failure;
        }
      };
      if (phase.equals("pre")) {
        writer.addPreCommitHook(callback::accept);
      } else if (phase.equals("post")) {
        writer.addPostCommitHook(callback::accept);
      } else {
        fixture.second.addRevisionListener(revision -> callback.accept(null));
      }
      final CompletableFuture<Void> committed = new CompletableFuture<>();
      final Thread commit = start(() -> {
        setValue(writer, 1);
        writer.commit();
      }, committed);
      final CompletableFuture<Void> changed = new CompletableFuture<>();
      Thread lifecycle = null;
      try {
        assertTrue(entered.await(10, TimeUnit.SECONDS));
        lifecycle = start(resourceLifecycle(fixture, local, remove), changed);
        awaitRetirement(fixture, changed);
        assertFalse(changed.isDone());
        inspect.countDown();
        read.get(10, TimeUnit.SECONDS);
        committed.get(10, TimeUnit.SECONDS);
        changed.get(10, TimeUnit.SECONDS);
        assertResourceChange(fixture, retained, remove);
        writer.close();
      } finally {
        inspect.countDown();
        commit.join(10000);
        if (lifecycle != null) {
          lifecycle.join(10000);
        }
      }
    }
  }

  static Stream<Arguments> refusedResourceLifecycleCases() {
    return Stream.of("json", "xml")
                 .flatMap(
                     format -> Stream.of(false, true)
                                     .flatMap(
                                         local -> Stream.of(false, true)
                                                        .flatMap(remove -> Stream.of(false, true)
                                                                                 .map(interrupt -> Arguments.of(format,
                                                                                     local, remove, interrupt)))));
  }

  @ParameterizedTest(name = "resource retirement refusal {arguments}")
  @MethodSource("refusedResourceLifecycleCases")
  void resourceLifecycleRefusalPreservesFilesAndOwnersForRetry(final String format, final boolean local,
      final boolean remove, final boolean interrupt) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final AbstractResourceSession<?, ?> retained = retainedResourceForRemoval(fixture, remove);
      fixture.notifications()
             .setCloseTimeout(interrupt
                 ? Duration.ofSeconds(10)
                 : Duration.ofMillis(25));
      final NodeTrx writer = fixture.second.beginNodeTrx();
      final CountDownLatch entered = new CountDownLatch(1);
      final CountDownLatch inspect = new CountDownLatch(1);
      final CompletableFuture<Void> read = new CompletableFuture<>();
      writer.addPostCommitHook(unused -> {
        try {
          entered.countDown();
          await(inspect);
          assertMetadataReadable(fixture);
          read.complete(null);
        } catch (final Throwable failure) {
          read.completeExceptionally(failure);
          throw failure;
        }
      });
      final AtomicBoolean interrupted = new AtomicBoolean();
      final CompletableFuture<Void> changed = new CompletableFuture<>();
      Thread lifecycle = null;
      try {
        setValue(writer, 1);
        writer.commit();
        assertTrue(entered.await(10, TimeUnit.SECONDS));
        lifecycle = start(() -> {
          try {
            resourceLifecycle(fixture, local, remove).run();
          } finally {
            interrupted.set(Thread.currentThread().isInterrupted());
          }
        }, changed);
        if (interrupt) {
          awaitRetirement(fixture, changed);
          lifecycle.interrupt();
        }
        final Throwable failure =
            assertThrows(ExecutionException.class, () -> changed.get(10, TimeUnit.SECONDS)).getCause();
        assertTrue(interrupt
            ? failure.getCause() instanceof InterruptedException
            : hasTimeout(failure));
        assertEquals(interrupt, interrupted.get());
        assertEquals(remove, fixture.firstDatabase.existsResource("other"));
        assertEquals(remove,
            Files.exists(fixture.path.resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve("other")));
        if (retained != null) {
          assertFalse(retained.isClosed());
          try (NodeReadOnlyTrx view = retained.beginNodeReadOnlyTrx()) {
            assertEquals(0, view.getRevisionNumber());
          }
        }
        assertFalse(fixture.first.isClosed());
        assertFalse(writer.isClosed());
        inspect.countDown();
        read.get(10, TimeUnit.SECONDS);
        fixture.notifications().awaitIdle(Duration.ofSeconds(10));
        resourceLifecycle(fixture, local, remove).run();
        assertResourceChange(fixture, retained, remove);
        writer.close();
      } finally {
        inspect.countDown();
        if (lifecycle != null) {
          lifecycle.join(10000);
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void eagerDerivedRebuildQuiescesOutsideSessionAdmission(final boolean local) throws Exception {
    try (Fixture fixture = new Fixture("json", directory)) {
      assertTrue(fixture.firstDatabase.createResource(ResourceConfiguration.newBuilder("other")
                                                                           .storageType(StorageType.FILE_CHANNEL)
                                                                           .validTimePaths("lo", "hi")
                                                                           .storeDiffs(false)
                                                                           .build()));
      assertTrue(fixture.local.createResource(ResourceConfiguration.newBuilder("other$derived")
                                                                   .storageType(StorageType.FILE_CHANNEL)
                                                                   .aggregateKernel(true)
                                                                   .storeDiffs(false)
                                                                   .build(),
          null));
      final NodeTrx writer = fixture.second.beginNodeTrx();
      final CountDownLatch entered = new CountDownLatch(1);
      final CountDownLatch inspect = new CountDownLatch(1);
      final CompletableFuture<Void> read = new CompletableFuture<>();
      writer.addPostCommitHook(unused -> {
        try {
          entered.countDown();
          await(inspect);
          assertMetadataReadable(fixture);
          read.complete(null);
        } catch (final Throwable failure) {
          read.completeExceptionally(failure);
          throw failure;
        }
      });
      final CompletableFuture<Void> opened = new CompletableFuture<>();
      final AtomicReference<ResourceSession<?, ?>> other = new AtomicReference<>();
      Thread opener = null;
      try {
        setValue(writer, 1);
        writer.commit();
        assertTrue(entered.await(10, TimeUnit.SECONDS));
        opener = start(() -> other.set((local
            ? fixture.local
            : fixture.firstDatabase).beginResourceSession("other")), opened);
        awaitRetirement(fixture, opened);
        assertFalse(opened.isDone());
        inspect.countDown();
        read.get(10, TimeUnit.SECONDS);
        opened.get(10, TimeUnit.SECONDS);
        assertNotNull(((AbstractResourceSession<?, ?>) other.get()).getDerivedResource());
        assertTrue(fixture.firstDatabase.existsResource("other"));
        writer.close();
      } finally {
        inspect.countDown();
        if (opener != null) {
          opener.join(10000);
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void warmSessionOpenDoesNotEnterNotificationRetirement(final boolean local) throws Exception {
    try (Fixture fixture = new Fixture("json", directory)) {
      assertTrue(fixture.firstDatabase.createResource(ResourceConfiguration.newBuilder("other")
                                                                           .storageType(StorageType.FILE_CHANNEL)
                                                                           .validTimePaths("lo", "hi")
                                                                           .storeDiffs(false)
                                                                           .build()));
      fixture.firstDatabase.beginResourceSession("other");
      fixture.notifications().setCloseTimeout(Duration.ofMinutes(1));
      final NodeTrx writer = fixture.second.beginNodeTrx();
      final CountDownLatch entered = new CountDownLatch(1);
      final CountDownLatch finish = new CountDownLatch(1);
      writer.addPostCommitHook(unused -> {
        entered.countDown();
        await(finish);
      });
      final CompletableFuture<Void> opened = new CompletableFuture<>();
      Thread opener = null;
      try {
        setValue(writer, 1);
        writer.commit();
        assertTrue(entered.await(10, TimeUnit.SECONDS));
        opener = start(() -> assertEquals(0, (local
            ? fixture.local
            : fixture.firstDatabase).beginResourceSession("other").getMostRecentRevisionNumber()), opened);
        opened.get(10, TimeUnit.SECONDS);
        assertEquals(1, finish.getCount());
        finish.countDown();
        fixture.notifications().awaitIdle(Duration.ofSeconds(10));
        writer.close();
      } finally {
        finish.countDown();
        if (opener != null) {
          opener.join(10000);
        }
      }
    }
  }

  private static AbstractResourceSession<?, ?> retainedResourceForRemoval(final Fixture fixture, final boolean remove) {
    if (!remove) {
      return null;
    }
    assertTrue(fixture.firstDatabase.createResource(
        ResourceConfiguration.newBuilder("other").storageType(StorageType.FILE_CHANNEL).build()));
    final AbstractResourceSession<?, ?> retained =
        (AbstractResourceSession<?, ?>) fixture.local.beginResourceSession("other");
    retained.retainForDerivedIndexes();
    fixture.firstDatabase.beginResourceSession("other").close();
    assertFalse(retained.isClosed());
    return retained;
  }

  private static Runnable resourceLifecycle(final Fixture fixture, final boolean local, final boolean remove) {
    final Database<?> database = local
        ? fixture.local
        : fixture.firstDatabase;
    return remove
        ? () -> database.removeResource("other")
        : () -> assertTrue(database.createResource(
            ResourceConfiguration.newBuilder("other").storageType(StorageType.FILE_CHANNEL).build()));
  }

  private static boolean notificationsPaused(final DatabaseNotifications notifications) throws Exception {
    synchronized (field(DatabaseNotifications.class, "progress").get(notifications)) {
      return !field(DatabaseNotifications.class, "accepting").getBoolean(notifications);
    }
  }

  private static void awaitRetirement(final Fixture fixture, final CompletableFuture<Void> changed) throws Exception {
    while (!notificationsPaused(fixture.notifications())) {
      if (changed.isDone()) {
        changed.get(10, TimeUnit.SECONDS);
        throw new AssertionError("Resource lifecycle returned before its notification fence");
      }
      Thread.onSpinWait();
    }
  }

  private static void assertMetadataReadable(final Fixture fixture) {
    assertTrue(fixture.firstDatabase.existsResource("resource"));
    assertTrue(fixture.firstDatabase.listResources()
                                    .stream()
                                    .anyMatch(path -> path.getFileName().toString().equals("resource")));
    final long id = fixture.firstDatabase.getResourceID("resource");
    assertEquals("resource", fixture.firstDatabase.getResourceName(id));
    assertSame(fixture.first, fixture.firstDatabase.beginResourceSession("resource"));
  }

  private static void assertResourceChange(final Fixture fixture, final AbstractResourceSession<?, ?> retained,
      final boolean remove) {
    assertEquals(!remove, fixture.firstDatabase.existsResource("other"));
    if (remove) {
      assertTrue(retained.isClosed());
      assertTrue(fixture.firstDatabase.createResource(
          ResourceConfiguration.newBuilder("other").storageType(StorageType.FILE_CHANNEL).build()));
    }
    try (ResourceSession<?, ?> session = fixture.firstDatabase.beginResourceSession("other");
        NodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
      assertEquals(0, reader.getRevisionNumber());
      assertTrue(reader.moveTo(0));
      assertFalse(reader.moveTo(1));
    }
  }

  static Stream<Arguments> outerGateCases() {
    return Stream.of("json", "xml")
                 .flatMap(format -> Stream.concat(
                     Stream.of(false, true)
                           .flatMap(database -> Stream.of("create", "remove", "session", "handle", "local")
                                                      .map(action -> Arguments.of(format, database, action))),
                     Stream.of(Arguments.of(format, true, "database"))));
  }

  @ParameterizedTest(name = "outer mutation gate {arguments}")
  @MethodSource("outerGateCases")
  void heldMutationGateCanEnterLifecycleBeforeQueuedCreator(final String format, final boolean databaseGate,
      final String action) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final AbstractResourceSession<?, ?> retained = retainedResourceForRemoval(fixture, action.equals("remove"));
      fixture.notifications().setCloseTimeout(Duration.ofSeconds(2));
      final boolean invalidates = action.equals("local") || action.equals("database");
      final AtomicBoolean created = new AtomicBoolean();
      final AtomicReference<IllegalStateException> rejected = new AtomicReference<>();
      final CompletableFuture<Void> finished = new CompletableFuture<>();
      Thread contender = null;
      try {
        try (var outer = databaseGate
            ? MutationGate.acquireDatabase(fixture.path)
            : MutationGate.acquireResource(fixture.path, "other")) {
          contender = start(() -> {
            try {
              created.set(fixture.secondDatabase.createResource(
                  ResourceConfiguration.newBuilder("other").storageType(StorageType.FILE_CHANNEL).build()));
            } catch (final IllegalStateException failure) {
              if (!invalidates) {
                throw failure;
              }
              assertFalse(fixture.secondDatabase.isOpen());
              rejected.set(failure);
            }
          }, finished);
          awaitMutationGateWaiter(databaseGate, contender, finished);
          assertFalse(finished.isDone());
          switch (action) {
            case "create" -> assertTrue(fixture.firstDatabase.createResource(
                ResourceConfiguration.newBuilder("other").storageType(StorageType.FILE_CHANNEL).build()));
            case "remove" -> {
              fixture.firstDatabase.removeResource("other");
              assertTrue(retained.isClosed());
              assertFalse(fixture.firstDatabase.existsResource("other"));
            }
            case "session" -> {
              fixture.first.close();
              assertTrue(fixture.first.isClosed());
            }
            case "handle" -> {
              fixture.firstDatabase.close();
              assertFalse(fixture.firstDatabase.isOpen());
            }
            case "local" -> {
              fixture.local.close();
              assertFalse(fixture.local.isOpen());
            }
            case "database" -> {
              Databases.removeDatabase(fixture.path);
              assertFalse(Files.exists(fixture.path));
            }
            default -> throw new AssertionError(action);
          }
          assertFalse(finished.isDone());
        }
        finished.get(10, TimeUnit.SECONDS);
        if (invalidates) {
          assertNotNull(rejected.get());
          assertTrue(rejected.get().getMessage().contains("clos"));
        } else {
          assertEquals(!action.equals("create"), created.get());
          try (ResourceSession<?, ?> other = fixture.secondDatabase.beginResourceSession("other");
              NodeReadOnlyTrx reader = other.beginNodeReadOnlyTrx()) {
            assertEquals(0, reader.getRevisionNumber());
            assertTrue(reader.moveTo(0));
            assertFalse(reader.moveTo(1));
          }
        }
      } finally {
        if (contender != null) {
          contender.join(10000);
          assertFalse(contender.isAlive());
        }
      }
    }
  }

  static Stream<Arguments> callbackGateCases() {
    return Stream.of("json", "xml")
                 .flatMap(format -> Stream.of("pre", "post", "listener")
                                          .flatMap(phase -> Stream.of(false, true)
                                                                  .flatMap(database -> Stream.of(false, true)
                                                                                             .map(alias -> Arguments.of(
                                                                                                 format, phase,
                                                                                                 database, alias)))));
  }

  @ParameterizedTest(name = "callback gate rejection {arguments}")
  @MethodSource("callbackGateCases")
  void callbackRejectsBeforeWaitingOnAnOuterMutationGate(final String format, final String phase,
      final boolean databaseGate, final boolean alias) throws Exception {
    try (Fixture fixture = new Fixture(format, directory)) {
      final NodeTrx writer = fixture.second.beginNodeTrx();
      final Path target = alias
          ? fixture.path.resolve("uncreated").resolve("..")
          : fixture.path;
      final CompletableFuture<Void> checked = new CompletableFuture<>();
      final AtomicBoolean invoked = new AtomicBoolean();
      final Consumer<NodeReadOnlyTrx> callback = unused -> {
        if (!invoked.compareAndSet(false, true)) {
          return;
        }
        try {
          final IllegalStateException failure = assertThrows(IllegalStateException.class, () -> {
            try (var gate = databaseGate
                ? MutationGate.acquireDatabase(target)
                : MutationGate.acquireResource(target, "other")) {
            }
          });
          assertTrue(failure.getMessage().contains("callback"));
          assertMetadataReadable(fixture);
          checked.complete(null);
        } catch (final Throwable failure) {
          checked.completeExceptionally(failure);
          throw failure;
        }
      };
      if (phase.equals("pre")) {
        writer.addPreCommitHook(callback::accept);
      } else if (phase.equals("post")) {
        writer.addPostCommitHook(callback::accept);
      } else {
        fixture.second.addRevisionListener(revision -> callback.accept(null));
      }
      final CompletableFuture<Void> committed = new CompletableFuture<>();
      Thread commit = null;
      try {
        try (var outer = databaseGate
            ? MutationGate.acquireDatabase(fixture.path)
            : MutationGate.acquireResource(fixture.path, "other")) {
          commit = start(() -> {
            setValue(writer, 1);
            writer.commit();
          }, committed);
          checked.get(10, TimeUnit.SECONDS);
          committed.get(10, TimeUnit.SECONDS);
        }
        fixture.notifications().awaitIdle(Duration.ofSeconds(10));
        writer.close();
      } finally {
        if (commit != null) {
          commit.join(10000);
          assertFalse(commit.isAlive());
        }
      }
    }
  }

  private static void awaitMutationGateWaiter(final boolean database, final Thread contender,
      final CompletableFuture<Void> finished) throws Exception {
    final Map<?, ?> registry = (Map<?, ?>) field(MutationGate.class, database
        ? "DATABASES"
        : "RESOURCES").get(null);
    while (true) {
      for (final Object entry : registry.values()) {
        final Object lock = field(entry.getClass(), "lock").get(entry);
        if (database
            ? ((ReentrantReadWriteLock) lock).hasQueuedThread(contender)
            : ((ReentrantLock) lock).hasQueuedThread(contender)) {
          return;
        }
      }
      if (finished.isDone()) {
        finished.get(10, TimeUnit.SECONDS);
        throw new AssertionError("Contender returned before waiting for the held mutation gate");
      }
      if (Thread.currentThread().isInterrupted()) {
        throw new AssertionError("Interrupted waiting for the mutation-gate contender");
      }
      Thread.onSpinWait();
    }
  }

  private static Thread start(final Runnable work, final CompletableFuture<Void> result) {
    final Thread thread = new Thread(() -> {
      try {
        work.run();
        result.complete(null);
      } catch (final Throwable failure) {
        result.completeExceptionally(failure);
      }
    });
    thread.start();
    return thread;
  }

  private static boolean hasTimeout(final Throwable failure) {
    if (failure instanceof TimeoutException) {
      return true;
    }
    if (failure.getCause() != null && hasTimeout(failure.getCause())) {
      return true;
    }
    for (final Throwable suppressed : failure.getSuppressed()) {
      if (hasTimeout(suppressed)) {
        return true;
      }
    }
    return false;
  }

  private static void setValue(final NodeTrx writer, final int value) {
    assertTrue(writer.moveTo(writer instanceof JsonNodeTrx
        ? 1
        : 2));
    if (writer instanceof JsonNodeTrx json) {
      json.setNumberValue(value);
    } else {
      ((XmlNodeTrx) writer).setValue(Integer.toString(value));
    }
  }

  private static void assertValue(final NodeReadOnlyTrx reader, final int expected) {
    assertTrue(reader.moveTo(reader instanceof JsonNodeReadOnlyTrx
        ? 1
        : 2));
    assertEquals(expected, reader instanceof JsonNodeReadOnlyTrx json
        ? json.getNumberValue().intValue()
        : Integer.parseInt(reader.getValue()));
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
    private final Path path;
    private final Database<? extends ResourceSession<?, ?>> firstDatabase;
    private final Database<? extends ResourceSession<?, ?>> secondDatabase;
    private final AbstractResourceSession<?, ?> first;
    private final AbstractResourceSession<?, ?> second;
    private final LocalDatabase<?, ?> local;

    Fixture(final String format, final Path directory) throws ReflectiveOperationException {
      path = directory.resolve("database");
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
      first = (AbstractResourceSession<?, ?>) firstDatabase.beginResourceSession("resource");
      second = (AbstractResourceSession<?, ?>) secondDatabase.beginResourceSession("resource");
      local = ((Databases.OpenDatabase<?>) field(DatabaseHandle.class, "owner").get(firstDatabase)).localDatabase;
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

    DatabaseNotifications notifications() {
      return second.getNotifications();
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
        firstDatabase.close();
      } finally {
        secondDatabase.close();
      }
    }
  }
}
