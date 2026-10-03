package io.sirix.access;

import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.exception.SirixDatabaseLockException;
import io.sirix.exception.SirixUsageException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class DatabaseOwnershipTest {
  @TempDir
  Path directory;

  private Path createDatabase() {
    final Path path = directory.resolve("database");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    return path;
  }

  @Test
  void handlesShareWriterAndLatestViewUntilLastClose() {
    final Path path = createDatabase();
    final int initialRevision;
    try (final Database<JsonResourceSession> first = Databases.openJsonDatabase(path);
        final Database<JsonResourceSession> second = Databases.openJsonDatabase(path)) {
      assertNotSame(first, second);
      assertTrue(first.createResource(ResourceConfiguration.newBuilder("resource").build()));
      final JsonResourceSession session = first.beginResourceSession("resource");
      final JsonResourceSession otherSession = second.beginResourceSession("resource");
      assertNotSame(session, otherSession);
      initialRevision = session.getMostRecentRevisionNumber();
      try (final JsonNodeReadOnlyTrx pinned = otherSession.beginNodeReadOnlyTrx();
          final JsonNodeTrx writer = session.beginNodeTrx()) {
        assertSame(writer, session.getNodeTrx().orElseThrow());
        assertThrows(SirixUsageException.class, otherSession::beginNodeTrx);
        writer.insertStringValueAsFirstChild("committed");
        writer.commit();
        assertEquals(initialRevision + 1, otherSession.getMostRecentRevisionNumber());
        assertEquals(initialRevision, pinned.getRevisionNumber());
        assertFalse(pinned.moveToFirstChild(), "an already pinned reader retains the initial revision");
      }
      first.close();
      first.close();
      assertFalse(first.isOpen());
      assertThrows(IllegalStateException.class, () -> first.beginResourceSession("resource"));
      assertTrue(second.isOpen());
      assertTrue(session.isClosed());
      assertFalse(otherSession.isClosed());
      try (final JsonNodeReadOnlyTrx reader = otherSession.beginNodeReadOnlyTrx()) {
        assertTrue(reader.moveToFirstChild());
        assertEquals("committed", reader.getValue());
      }
      try (final JsonNodeTrx writer = otherSession.beginNodeTrx()) {
        writer.moveToFirstChild();
        writer.setStringValue("second");
        writer.commit();
      }
    }
    assertTrue(Files.exists(path.resolve(".lock")), "close must retain the lock inode");
    try (final Database<JsonResourceSession> reopened = Databases.openJsonDatabase(path);
        final JsonResourceSession session = reopened.beginResourceSession("resource");
        final JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
      assertEquals(initialRevision + 2, session.getMostRecentRevisionNumber());
      assertTrue(reader.moveToFirstChild());
      assertEquals("second", reader.getValue());
    } finally {
      Databases.removeDatabase(path);
    }
  }

  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  void symbolicAndNormalizedPathsShareTheCanonicalOwner() throws Exception {
    final Path path = createDatabase();
    final Path alias = directory.resolve("alias");
    Files.createSymbolicLink(alias, path);
    try (final Database<JsonResourceSession> first = Databases.openJsonDatabase(path);
        final Database<JsonResourceSession> normalized = Databases.openJsonDatabase(path.resolve("."));
        final Database<JsonResourceSession> symbolic = Databases.openJsonDatabase(alias)) {
      assertTrue(first.createResource(ResourceConfiguration.newBuilder("resource").build()));
      final JsonResourceSession session = first.beginResourceSession("resource");
      final int revision = session.getMostRecentRevisionNumber();
      assertSame(session.getRtxIndexController(revision),
          normalized.beginResourceSession("resource").getRtxIndexController(revision));
      assertSame(session.getRtxIndexController(revision),
          symbolic.beginResourceSession("resource").getRtxIndexController(revision));
      assertEquals(path.toRealPath(), symbolic.getDatabaseConfig().getDatabaseFile());
      first.close();
      normalized.close();
      assertTrue(symbolic.isOpen());
      assertChildRefused(path);
    } finally {
      Databases.removeDatabase(path);
    }
  }

  @Test
  void concurrentOpensShareResourceState() throws Exception {
    final Path path = createDatabase();
    try (final Database<JsonResourceSession> first = Databases.openJsonDatabase(path)) {
      assertTrue(first.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (final ExecutorService pool = Executors.newFixedThreadPool(8)) {
        final CyclicBarrier barrier = new CyclicBarrier(8);
        final List<Future<Database<JsonResourceSession>>> futures = new ArrayList<>(8);
        for (int i = 0; i < 8; i++) {
          futures.add(pool.submit(() -> {
            barrier.await(30, TimeUnit.SECONDS);
            final Database<JsonResourceSession> handle = Databases.openJsonDatabase(path);
            try {
              handle.beginResourceSession("resource");
              return handle;
            } catch (final RuntimeException | Error e) {
              handle.close();
              throw e;
            }
          }));
        }
        final JsonResourceSession session = first.beginResourceSession("resource");
        for (final Future<Database<JsonResourceSession>> future : futures) {
          try (final Database<JsonResourceSession> handle = future.get(30, TimeUnit.SECONDS)) {
            final JsonResourceSession other = handle.beginResourceSession("resource");
            assertSame(session.getRtxIndexController(session.getMostRecentRevisionNumber()),
                other.getRtxIndexController(other.getMostRecentRevisionNumber()));
          }
        }
      }
    } finally {
      Databases.removeDatabase(path);
    }
  }

  @Test
  void xmlHandlesAlsoShareResourceState() {
    final Path path = directory.resolve("xml");
    assertTrue(Databases.createXmlDatabase(new DatabaseConfiguration(path)));
    try (final Database<XmlResourceSession> first = Databases.openXmlDatabase(path);
        final Database<XmlResourceSession> second = Databases.openXmlDatabase(path)) {
      assertTrue(first.createResource(ResourceConfiguration.newBuilder("resource").build()));
      final XmlResourceSession session = first.beginResourceSession("resource");
      final XmlResourceSession other = second.beginResourceSession("resource");
      assertNotSame(session, other);
      assertSame(session.getRtxIndexController(session.getMostRecentRevisionNumber()),
          other.getRtxIndexController(other.getMostRecentRevisionNumber()));
    } finally {
      Databases.removeDatabase(path);
    }
  }

  @Test
  void shutdownSnapshotClosesHandlesAndReleasesProcessOwnership() throws Exception {
    final Path path = createDatabase();
    try (final Database<JsonResourceSession> first = Databases.openJsonDatabase(path);
        final Database<JsonResourceSession> second = Databases.openJsonDatabase(path)) {
      final Map<Path, Set<Database<?>>> snapshot = DatabasesInternals.getOpenDatabases();
      final Set<Database<?>> handles = snapshot.get(path.toRealPath());
      assertEquals(Set.of(first, second), handles);
      for (final Database<?> handle : handles) {
        handle.close();
      }
      assertFalse(first.isOpen());
      assertFalse(second.isOpen());
      assertFalse(DatabasesInternals.getOpenDatabases().containsKey(path.toRealPath()));
      assertChildOpened(path);
    } finally {
      Databases.removeDatabase(path);
    }
  }

  @Test
  void closingLastUserSessionAllowsResourceRemovalAndRecreation() {
    final Path path = createDatabase();
    try (final Database<JsonResourceSession> first = Databases.openJsonDatabase(path);
        final Database<JsonResourceSession> second = Databases.openJsonDatabase(path)) {
      assertTrue(first.createResource(ResourceConfiguration.newBuilder("resource").build()));
      final JsonResourceSession session = first.beginResourceSession("resource");
      try (final JsonNodeTrx writer = session.beginNodeTrx()) {
        writer.insertStringValueAsFirstChild("old");
        writer.commit();
      }
      final JsonResourceSession other = second.beginResourceSession("resource");
      session.close();
      assertThrows(IllegalStateException.class, () -> first.removeResource("resource"));
      try (final JsonNodeReadOnlyTrx reader = other.beginNodeReadOnlyTrx()) {
        assertTrue(reader.moveToFirstChild());
        assertEquals("old", reader.getValue());
      }
      other.close();
      assertSame(first, first.removeResource("resource"));
      assertTrue(second.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (final JsonResourceSession fresh = first.beginResourceSession("resource");
          final JsonNodeReadOnlyTrx reader = fresh.beginNodeReadOnlyTrx()) {
        assertFalse(reader.moveToFirstChild());
      }
    } finally {
      Databases.removeDatabase(path);
    }
  }

  @Test
  void differentUsersCommitAlternatelyWithSharedLatestViewAndCorrectAuthors() {
    final Path path = createDatabase();
    final User alice = new User("Alice", UUID.randomUUID());
    final User bob = new User("Bob", UUID.randomUUID());
    try (final Database<JsonResourceSession> first = Databases.openJsonDatabase(path, alice);
        final Database<JsonResourceSession> second = Databases.openJsonDatabase(path, bob)) {
      assertTrue(first.createResource(ResourceConfiguration.newBuilder("resource").build()));
      final JsonResourceSession aliceSession = first.beginResourceSession("resource");
      final JsonResourceSession bobSession = second.beginResourceSession("resource");
      assertEquals(Optional.of(alice), aliceSession.getUser());
      assertEquals(Optional.of(bob), bobSession.getUser());
      final int initialRevision = aliceSession.getMostRecentRevisionNumber();
      for (int commit = 0; commit < 6; commit++) {
        final boolean byAlice = (commit & 1) == 0;
        final JsonResourceSession writerSession = byAlice
            ? aliceSession
            : bobSession;
        final JsonResourceSession readerSession = byAlice
            ? bobSession
            : aliceSession;
        final User author = byAlice
            ? alice
            : bob;
        final String value = author.getName() + commit;
        try (final JsonNodeTrx writer = writerSession.beginNodeTrx()) {
          assertEquals(Optional.of(author), writer.getUser());
          if (commit == 0) {
            writer.insertStringValueAsFirstChild(value);
          } else {
            assertTrue(writer.moveToFirstChild());
            writer.setStringValue(value);
          }
          writer.commit();
          assertEquals(Optional.of(author), writer.getUser());
        }
        assertEquals(initialRevision + commit + 1, readerSession.getMostRecentRevisionNumber());
        try (final JsonNodeReadOnlyTrx reader = readerSession.beginNodeReadOnlyTrx()) {
          assertTrue(reader.moveToFirstChild());
          assertEquals(value, reader.getValue());
          assertEquals(Optional.of(author), reader.getUser());
        }
      }
      for (int commit = 0; commit < 6; commit++) {
        final User author = (commit & 1) == 0
            ? alice
            : bob;
        final int revision = initialRevision + commit + 1;
        assertEquals(author, aliceSession.getHistory(revision, revision).getFirst().getUser());
        assertEquals(author, bobSession.getHistory(revision, revision).getFirst().getUser());
      }
      first.close();
      assertFalse(bobSession.isClosed());
      try (final JsonNodeTrx writer = bobSession.beginNodeTrx()) {
        assertEquals(Optional.of(bob), writer.getUser());
        assertTrue(writer.moveToFirstChild());
        writer.setStringValue("after Alice closes");
        writer.commit();
      }
    } finally {
      Databases.removeDatabase(path);
    }
  }

  @Test
  void missingLockFileIsCreatedAndWrongTypeOpenReleasesOwnership() throws Exception {
    final Path path = createDatabase();
    final Path lock = path.resolve(".lock");
    assertTrue(Files.exists(lock));
    Files.delete(lock);
    assertThrows(SirixUsageException.class, () -> Databases.openXmlDatabase(path));
    assertChildOpened(path);
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.isOpen());
      assertTrue(Files.exists(lock));
      assertTrue(Databases.existsDatabase(path));
    } finally {
      Databases.removeDatabase(path);
    }
  }

  @Test
  void staleHandleCannotReleaseRecreatedDatabase() throws Exception {
    final Path path = createDatabase();
    try (final Database<JsonResourceSession> stale = Databases.openJsonDatabase(path)) {
      Databases.removeDatabase(path);
      assertFalse(stale.isOpen());
      assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
      try (final Database<JsonResourceSession> current = Databases.openJsonDatabase(path)) {
        stale.close();
        assertTrue(current.isOpen());
        assertChildRefused(path);
      }
    } finally {
      Databases.removeDatabase(path);
    }
  }

  @Test
  void secondProcessIsRefusedUntilLastLocalHandleCloses() throws Exception {
    final Path path = createDatabase();
    try (final Database<JsonResourceSession> first = Databases.openJsonDatabase(path);
        final Database<JsonResourceSession> second = Databases.openJsonDatabase(path)) {
      assertChildRefused(path);
      first.close();
      assertChildRefused(path);
      second.close();
      assertChildOpened(path);
    } finally {
      Databases.removeDatabase(path);
    }
  }

  @Test
  void ownerExitReleasesLock() throws Exception {
    assertOwnerRelease(false);
  }

  @Test
  void killedOwnerReleasesLock() throws Exception {
    assertOwnerRelease(true);
  }

  @Test
  void concurrentCreatorsPublishExactlyOneUsableDatabase() throws Exception {
    final Path path = directory.resolve("concurrent-creation");
    Files.createDirectory(path);
    final Process first = child(path, "create");
    Process second = null;
    try {
      second = child(path, "create");
      assertEquals("READY", childResult(first));
      assertEquals("READY", childResult(second));
      first.getOutputStream().write('\n');
      first.getOutputStream().flush();
      second.getOutputStream().write('\n');
      second.getOutputStream().flush();
      final String firstResult = childResult(first);
      final String secondResult = childResult(second);
      final int created = (firstResult.equals("CREATED")
          ? 1
          : 0)
          + (secondResult.equals("CREATED")
              ? 1
              : 0);
      assertEquals(1, created, firstResult + "; " + secondResult);
      assertTrue(firstResult.equals("CREATED") || firstResult.equals("EXISTS")
          || firstResult.equals("LOCKED " + path.toRealPath()));
      assertTrue(secondResult.equals("CREATED") || secondResult.equals("EXISTS")
          || secondResult.equals("LOCKED " + path.toRealPath()));
      assertTrue(first.waitFor(30, TimeUnit.SECONDS));
      assertTrue(second.waitFor(30, TimeUnit.SECONDS));
      assertEquals(0, first.exitValue());
      assertEquals(0, second.exitValue());
      assertTrue(Databases.existsDatabase(path));
      try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
        assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
        try (final JsonResourceSession session = database.beginResourceSession("resource");
            final JsonNodeTrx writer = session.beginNodeTrx()) {
          writer.insertStringValueAsFirstChild("created");
          writer.commit();
        }
      }
    } finally {
      stopChild(first);
      if (second != null) {
        stopChild(second);
      }
      Databases.removeDatabase(path);
    }
  }

  private void assertOwnerRelease(final boolean kill) throws Exception {
    final Path path = createDatabase();
    final Process owner = child(path, "hold");
    try {
      assertEquals("OPEN", childResult(owner));
      final SirixDatabaseLockException failure =
          assertThrows(SirixDatabaseLockException.class, () -> Databases.openJsonDatabase(path));
      assertEquals(path.toRealPath(), failure.getDatabasePath());
      assertTrue(failure.getMessage().contains(path.toRealPath().toString()));
      assertFalse(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
      assertThrows(SirixDatabaseLockException.class, () -> Databases.removeDatabase(path));
      assertChildRefused(path);
      if (kill) {
        owner.destroyForcibly();
      } else {
        owner.getOutputStream().write('\n');
        owner.getOutputStream().flush();
      }
      assertTrue(owner.waitFor(30, TimeUnit.SECONDS), "child owner must exit");
      assertChildOpened(path);
      try (final Database<JsonResourceSession> reopened = Databases.openJsonDatabase(path)) {
        assertTrue(reopened.isOpen());
      }
    } finally {
      stopChild(owner);
      Databases.removeDatabase(path);
    }
  }

  private void assertChildRefused(final Path path) throws Exception {
    final Process process = child(path, "open");
    try {
      assertEquals("LOCKED " + path.toRealPath(), childResult(process));
      assertTrue(process.waitFor(30, TimeUnit.SECONDS));
      assertEquals(0, process.exitValue());
    } finally {
      stopChild(process);
    }
  }

  private void assertChildOpened(final Path path) throws Exception {
    final Process process = child(path, "open");
    try {
      assertEquals("OPEN", childResult(process));
      assertTrue(process.waitFor(30, TimeUnit.SECONDS));
      assertEquals(0, process.exitValue());
    } finally {
      stopChild(process);
    }
  }

  private Process child(final Path path, final String mode) throws Exception {
    final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    return new ProcessBuilder(java, "-Xmx256m", "--enable-preview", "--enable-native-access=ALL-UNNAMED",
        "--add-modules", "jdk.incubator.vector", "-Dsirix.allocator.maxSize=64M", "-cp",
        System.getProperty("java.class.path"), Child.class.getName(), path.toString(), mode).redirectError(
            directory.resolve("child-" + System.nanoTime() + ".log").toFile()).start();
  }

  private static String childResult(final Process process) throws Exception {
    // The bound detects a broken child handshake, not database latency. Always reap our child
    // in the caller's finally so a failed assertion cannot leave an owner running in CI.
    return CompletableFuture.supplyAsync(() -> {
      try {
        final BufferedReader reader =
            new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
        String line;
        while ((line = reader.readLine()) != null) {
          if (line.equals("OPEN") || line.equals("READY") || line.equals("CREATED") || line.equals("EXISTS")
              || line.startsWith("LOCKED ")) {
            return line;
          }
        }
        return "child exited without a result";
      } catch (final Exception e) {
        throw new IllegalStateException(e);
      }
    }).get(30, TimeUnit.SECONDS);
  }

  private static void stopChild(final Process process) throws Exception {
    if (process.isAlive()) {
      process.destroyForcibly();
    }
    assertTrue(process.waitFor(30, TimeUnit.SECONDS), "our child must be reaped");
  }

  /** Executed in a separate JVM so OS lock refusal and crash release are real process events. */
  public static final class Child {
    public static void main(final String[] args) throws Exception {
      final Path path = Path.of(args[0]);
      try {
        if (args[1].equals("create")) {
          System.out.println("READY");
          System.out.flush();
          System.in.read();
          System.out.println(Databases.createJsonDatabase(new DatabaseConfiguration(path))
              ? "CREATED"
              : "EXISTS");
          return;
        }
        try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
          if (!database.isOpen()) {
            throw new IllegalStateException("Child database is closed");
          }
          System.out.println("OPEN");
          System.out.flush();
          if (args[1].equals("hold")) {
            System.in.read();
          }
        }
      } catch (final SirixDatabaseLockException e) {
        if (!e.getMessage().contains(path.toRealPath().toString())) {
          throw e;
        }
        System.out.println("LOCKED " + e.getDatabasePath());
      }
    }
  }
}
