package io.sirix.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.MutationGate;
import io.sirix.access.ResourceConfiguration;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

final class MutationGateRegressionTest {
  private static final long TIMEOUT_SECONDS = 30;

  @TempDir
  Path directory;

  @ParameterizedTest
  @CsvSource({"fresh, ./fresh", "./fresh, fresh", "fresh, fresh//", "fresh//, fresh",
      "fresh, unused/../fresh", "unused/../fresh, fresh", "././fresh, fresh/.", "fresh/., ././fresh"})
  void resourceAliasesShareOneHold(final String firstName, final String secondName) throws Exception {
    assertEquals("fresh", ResourceConfiguration.newBuilder(firstName).build().getName());
    assertEquals("fresh", ResourceConfiguration.newBuilder(secondName).build().getName());
    final Path database = directory.resolve("aliases");
    final AtomicBoolean firstInside = new AtomicBoolean(true);
    final FutureTask<Boolean> contender = new FutureTask<>(() -> {
      try (var gate = MutationGate.acquireResource(database, secondName)) {
        return firstInside.get();
      }
    });
    try (var gate = MutationGate.acquireResource(database, firstName)) {
      final Thread waiting = Thread.ofPlatform().daemon().start(contender);
      awaitWaiting(waiting);
      assertFalse(contender.isDone(), "aliases of one resource must wait on the same hold");
      firstInside.set(false);
    }
    assertFalse(contender.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "the second alias must enter only after the first alias releases its hold");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void creationWaitingOnGateDoesNotOwnLifecycleMonitor(final boolean xml) throws Exception {
    final Path path = directory.resolve("database");
    final FutureTask<Boolean> contender = new FutureTask<>(() -> create(path, xml));
    final Thread waiting;
    try (var gate = MutationGate.acquireDatabase(path)) {
      waiting = Thread.ofPlatform().daemon().start(contender);
      awaitWaiting(waiting);
      final FutureTask<Boolean> lifecycle = new FutureTask<>(() -> Databases.existsDatabase(path));
      Thread.ofPlatform().daemon().start(lifecycle);
      assertFalse(lifecycle.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
          "a creator waiting on the gate must leave the lifecycle monitor available");
      assertTrue(create(path, xml), "the gate owner must be able to create the database reentrantly");
    }
    assertFalse(contender.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    waiting.join();
    Databases.removeDatabase(path);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void resourceRemovalWaitsWithoutBlockingOtherResources(final boolean xml) throws Exception {
    final Path path = directory.resolve("database");
    assertTrue(create(path, xml));
    try (var database = xml ? Databases.openXmlDatabase(path) : Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("held").build()));
      final FutureTask<Void> removal = new FutureTask<>(() -> {
        database.removeResource("held");
        return null;
      });
      try (var gate = MutationGate.acquireResource(path, "held")) {
        final Thread waiting = Thread.ofPlatform().daemon().start(removal);
        awaitWaiting(waiting);
        assertFalse(removal.isDone(), "removal must wait for the resource's authorized write");
        assertTrue(database.existsResource("held"));
        try (var other = MutationGate.acquireResource(path, "other")) {
          assertTrue(database.createResource(ResourceConfiguration.newBuilder("other").build()));
        }
      }
      removal.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      assertFalse(database.existsResource("held"));
      assertTrue(database.existsResource("other"));
    }
    Databases.removeDatabase(path);
  }

  @Test
  void idleDatabaseAndResourceEntriesAreReclaimed() throws Exception {
    final int databasesBefore = registrySize("DATABASES");
    final int resourcesBefore = registrySize("RESOURCES");
    for (int i = 0; i < 10_000; i++) {
      final Path database = directory.resolve("database" + i);
      try (var outer = MutationGate.acquireDatabase(database);
          var resource = MutationGate.acquireResource(database, "resource" + i);
          var reentrant = MutationGate.acquireResource(database, "resource" + i)) {
        assertEquals(databasesBefore + 1, registrySize("DATABASES"));
        assertEquals(resourcesBefore + 1, registrySize("RESOURCES"));
      }
    }
    assertEquals(databasesBefore, registrySize("DATABASES"), "idle database names must not accumulate");
    assertEquals(resourcesBefore, registrySize("RESOURCES"), "idle resource names must not accumulate");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void reclamationPreservesExclusionForOwnersAndWaiters(final boolean resource) throws Exception {
    final Path database = directory.resolve("contended");
    final AtomicInteger inside = new AtomicInteger();
    final List<FutureTask<Void>> tasks = new ArrayList<>();
    for (int i = 0; i < 8; i++) {
      final FutureTask<Void> task = new FutureTask<>(() -> {
        for (int attempt = 0; attempt < 1_000; attempt++) {
          try (var gate = resource
              ? MutationGate.acquireResource(database, "same")
              : MutationGate.acquireDatabase(database)) {
            assertEquals(0, inside.getAndIncrement(), "one target must never have two gate owners");
            Thread.yield();
            assertEquals(1, inside.getAndDecrement());
          }
        }
        return null;
      });
      tasks.add(task);
      Thread.ofPlatform().daemon().start(task);
    }
    for (final FutureTask<Void> task : tasks) {
      task.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
    assertEquals(0, inside.get());
  }

  private static boolean create(final Path path, final boolean xml) {
    final DatabaseConfiguration configuration = new DatabaseConfiguration(path);
    return xml ? Databases.createXmlDatabase(configuration) : Databases.createJsonDatabase(configuration);
  }

  private static void awaitWaiting(final Thread thread) {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
    while (thread.getState() != Thread.State.WAITING) {
      assertTrue(thread.isAlive(), "operation completed instead of waiting for the held gate");
      assertTrue(System.nanoTime() < deadline, "operation never waited for the held gate");
      Thread.yield();
    }
  }

  private static int registrySize(final String name) throws Exception {
    final Field field = MutationGate.class.getDeclaredField(name);
    field.setAccessible(true);
    return ((Map<?, ?>) field.get(null)).size();
  }
}
