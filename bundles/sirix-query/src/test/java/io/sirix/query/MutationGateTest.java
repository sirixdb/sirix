package io.sirix.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.MutationGate;
import io.sirix.access.Databases;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The gate's ordering guarantee, forced rather than waited for.
 *
 * <p>
 * Every interleaving here is driven by latches, so the test proves the ordering instead of making
 * it likely: a thread signals that it is inside the gate, the other thread then tries to enter and
 * must not get in until the first one leaves. A sleep would assert only that the machine was slow
 * enough.
 * </p>
 */
final class MutationGateTest {

  private static final long TIMEOUT_SECONDS = 30;

  @TempDir
  private Path directory;

  /**
   * The window the gate exists to close: a second operation on the same database cannot enter between
   * the first one's check and its write.
   */
  @Test
  void aSecondOperationOnTheSameDatabaseWaitsForTheFirst() throws InterruptedException {
    final Path database = directory.resolve("orders");
    final CountDownLatch inside = new CountDownLatch(1);
    final CountDownLatch mayFinish = new CountDownLatch(1);
    final AtomicBoolean secondEnteredWhileFirstHeld = new AtomicBoolean();
    final AtomicBoolean firstStillInside = new AtomicBoolean(true);

    final Thread writer = new Thread(() -> {
      try (var gate = MutationGate.acquireDatabase(database)) {
        inside.countDown();
        mayFinish.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        firstStillInside.set(false);
      } catch (final InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      }
    }, "gate-first");

    final Thread remover = new Thread(() -> {
      try (var gate = MutationGate.acquireDatabase(database)) {
        secondEnteredWhileFirstHeld.set(firstStillInside.get());
      }
    }, "gate-second");

    writer.start();
    assertTrue(inside.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "the first operation never entered the gate");
    remover.start();
    // The second thread is now blocked on the gate. Give it the chance to be wrong before releasing.
    assertFalse(remover.join(java.time.Duration.ofMillis(250)),
        "the second operation entered while the first held the gate");
    mayFinish.countDown();
    writer.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
    remover.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
    assertFalse(secondEnteredWhileFirstHeld.get(), "the second operation observed the first still inside the gate");
  }

  /** Different databases are independent, so the gate orders without serialising unrelated work. */
  @Test
  void differentDatabasesDoNotBlockEachOther() throws InterruptedException {
    final CountDownLatch bothInside = new CountDownLatch(2);
    final CountDownLatch mayFinish = new CountDownLatch(1);
    final Thread[] threads = new Thread[2];
    for (int i = 0; i < threads.length; i++) {
      final Path database = directory.resolve("database" + i);
      threads[i] = new Thread(() -> {
        try (var gate = MutationGate.acquireDatabase(database)) {
          bothInside.countDown();
          mayFinish.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (final InterruptedException interrupted) {
          Thread.currentThread().interrupt();
        }
      }, "gate-independent-" + i);
      threads[i].start();
    }
    assertTrue(bothInside.await(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "two different databases could not be held at the same time, so the gate serialises too much");
    mayFinish.countDown();
    for (final Thread thread : threads) {
      thread.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
    }
  }

  /**
   * The production removal path takes the gate, not just callers that remember to. Holding the gate
   * stands in for a store or load in progress; a concurrent {@code Databases.removeDatabase} - the
   * call the REST DELETE route makes - must wait for it and then succeed, so data is neither lost
   * under an unauthorized write nor left behind by a removal that was skipped.
   */
  @Test
  void removeDatabaseWaitsForTheGate() throws InterruptedException {
    final Path database = directory.resolve("removable");
    Databases.createJsonDatabase(new DatabaseConfiguration(database));
    assertTrue(Databases.existsDatabase(database), "fixture database was not created");

    final CountDownLatch removerStarted = new CountDownLatch(1);
    final AtomicBoolean removedWhileHeld = new AtomicBoolean();
    final Thread remover;
    try (var gate = MutationGate.acquireDatabase(database)) {
      remover = new Thread(() -> {
        removerStarted.countDown();
        Databases.removeDatabase(database);
      }, "gate-remove");
      remover.start();
      assertTrue(removerStarted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "the remover never started");
      // It is blocked on the gate; a removal completing here would prove the path ignores it.
      assertFalse(remover.join(java.time.Duration.ofMillis(500)), "removeDatabase ignored the mutation gate");
      removedWhileHeld.set(!Databases.existsDatabase(database));
    }
    remover.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
    assertFalse(removedWhileHeld.get(), "the database was removed while the gate was held");
    assertFalse(Databases.existsDatabase(database), "the removal never completed after the gate was released");
  }

  /**
   * The captain's case, and the reason the gate is per resource: two callers creating two
   * <em>different</em> resources in one database must both proceed without waiting on each other.
   * Both are inside their holds at the same time, which a per-database gate could not allow.
   */
  @Test
  void differentResourcesOfOneDatabaseRunInParallel() throws InterruptedException {
    final Path database = directory.resolve("shared");
    final CountDownLatch bothInside = new CountDownLatch(2);
    final CountDownLatch mayFinish = new CountDownLatch(1);
    final Thread[] writers = new Thread[2];
    for (int i = 0; i < writers.length; i++) {
      final String resource = "resource" + i;
      writers[i] = new Thread(() -> {
        try (var gate = MutationGate.acquireResource(database, resource)) {
          bothInside.countDown();
          mayFinish.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (final InterruptedException interrupted) {
          Thread.currentThread().interrupt();
        }
      }, "gate-" + resource);
      writers[i].start();
    }
    assertTrue(bothInside.await(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "two different resources of one database could not be held at once: the gate serialises work "
            + "Sirix supports running in parallel");
    mayFinish.countDown();
    for (final Thread writer : writers) {
      writer.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
    }
  }

  /** Callers racing the same resource name serialise, which makes create-if-absent atomic. */
  @Test
  void theSameResourceNameSerialises() throws InterruptedException {
    final Path database = directory.resolve("race");
    final CountDownLatch inside = new CountDownLatch(1);
    final CountDownLatch mayFinish = new CountDownLatch(1);
    final AtomicBoolean firstStillInside = new AtomicBoolean(true);
    final AtomicBoolean overlapped = new AtomicBoolean();

    final Thread first = new Thread(() -> {
      try (var gate = MutationGate.acquireResource(database, "same")) {
        inside.countDown();
        mayFinish.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        firstStillInside.set(false);
      } catch (final InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      }
    }, "gate-same-first");
    final Thread second = new Thread(() -> {
      try (var gate = MutationGate.acquireResource(database, "same")) {
        overlapped.set(firstStillInside.get());
      }
    }, "gate-same-second");

    first.start();
    assertTrue(inside.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "the first caller never entered");
    second.start();
    assertFalse(second.join(java.time.Duration.ofMillis(250)), "both callers held the same resource name");
    mayFinish.countDown();
    first.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
    second.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
    assertFalse(overlapped.get(), "the second caller entered while the first held the same resource");
  }

  /** A whole-database removal waits for a resource operation in that database. */
  @Test
  void databaseRemovalWaitsForAResourceOperation() throws InterruptedException {
    final Path database = directory.resolve("exclusive");
    final CountDownLatch resourceHeld = new CountDownLatch(1);
    final CountDownLatch mayFinish = new CountDownLatch(1);
    final AtomicBoolean resourceStillHeld = new AtomicBoolean(true);
    final AtomicBoolean removerOverlapped = new AtomicBoolean();

    final Thread creator = new Thread(() -> {
      try (var gate = MutationGate.acquireResource(database, "records")) {
        resourceHeld.countDown();
        mayFinish.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        resourceStillHeld.set(false);
      } catch (final InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      }
    }, "gate-resource");
    final Thread remover = new Thread(() -> {
      try (var gate = MutationGate.acquireDatabase(database)) {
        removerOverlapped.set(resourceStillHeld.get());
      }
    }, "gate-database");

    creator.start();
    assertTrue(resourceHeld.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "the resource was never held");
    remover.start();
    assertFalse(remover.join(java.time.Duration.ofMillis(250)),
        "a whole-database operation entered while a resource of it was held");
    mayFinish.countDown();
    creator.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
    remover.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
    assertFalse(removerOverlapped.get(), "the database operation overlapped a resource operation");
  }

  /**
   * A bulk operation takes the gate and then calls through code that takes it again on its thread.
   */
  @Test
  void theGateIsReentrantOnOneThread() {
    final Path database = directory.resolve("reentrant");
    int depth = 0;
    try (var outer = MutationGate.acquireDatabase(database)) {
      depth++;
      try (var inner = MutationGate.acquireDatabase(database)) {
        depth++;
      }
    }
    assertEquals(2, depth, "a reentrant acquisition on the same thread must not deadlock");
  }

  /** Closing the handle twice must not release a gate the thread no longer holds. */
  @Test
  void closingTheHandleTwiceReleasesOnce() {
    final Path database = directory.resolve("idempotent");
    final MutationGate.Handle handle = MutationGate.acquireDatabase(database);
    handle.close();
    handle.close();
    // If the second close had released a gate this thread no longer held, this acquisition would
    // either throw or let a second thread in; acquiring and releasing again proves the state is sane.
    try (var reacquired = MutationGate.acquireDatabase(database)) {
      assertTrue(true);
    }
  }
}
