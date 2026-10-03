package io.sirix.access;

import io.brackit.query.atomic.QNm;
import io.sirix.api.Database;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.access.trx.node.AbstractNodeTrxImpl;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.exception.SirixDatabaseLockException;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.SirixUsageException;
import io.sirix.utils.SirixFiles;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doThrow;

final class DatabaseOwnershipTest {
  @TempDir
  Path directory;

  @BeforeEach
  void canonicalizeDirectory() throws IOException {
    directory = directory.toRealPath();
  }

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
      final Set<Database<?>> handles = Objects.requireNonNull(snapshot.get(path.toRealPath()));
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
  @EnabledOnOs({OS.LINUX, OS.MAC})
  void openerWithUnlinkedLockDescriptorCannotJoinRecreatedDatabase() throws Exception {
    final Path path = createDatabase();
    final Path lockPath = path.resolve(".lock");
    final AtomicReference<Process> replacement = new AtomicReference<>();
    try {
      try (final MockedStatic<FileChannel> channels = mockStatic(FileChannel.class, invocation -> {
        final Object result = invocation.callRealMethod();
        if (invocation.getMethod().getName().equals("open") && invocation.getRawArguments().length == 2
            && lockPath.equals(invocation.getArgument(0))
            && invocation.getRawArguments()[1] instanceof OpenOption[] options && options.length == 2
            && options[0] == StandardOpenOption.CREATE && options[1] == StandardOpenOption.WRITE) {
          final FileChannel stale = (FileChannel) result;
          try {
            final Process owner = child(path, "replace");
            replacement.set(owner);
            assertEquals("OPEN", childResult(owner));
          } catch (final Throwable failure) {
            stale.close();
            throw failure;
          }
        }
        return result;
      })) {
        assertThrows(SirixDatabaseLockException.class, () -> {
          try (final Database<JsonResourceSession> unexpected = Databases.openJsonDatabase(path)) {
            assertTrue(unexpected.isOpen());
          }
        });
      }
      assertChildRefused(path);
    } finally {
      if (replacement.get() != null) {
        stopChild(replacement.get());
      }
      Databases.removeDatabase(path);
    }
  }

  @ParameterizedTest
  @CsvSource({"JSON,false", "JSON,true", "XML,false", "XML,true"})
  void removalKeepsOwnershipReservedUntilChannelsClose(final DatabaseType type, final boolean forceRemoval)
      throws Exception {
    final Path path = directory.resolve("removing-" + type);
    assertTrue(createDatabase(path, type));
    final Path configuration = path.resolve(DatabaseConfiguration.DatabasePaths.CONFIG_BINARY.getFile());
    final Path lockPath = path.resolve(".lock");
    final AtomicBoolean contentsChecked = new AtomicBoolean();
    final AtomicBoolean lockOnlyChecked = new AtomicBoolean();
    try (final Database<?> handle = openDatabase(path, type);
        final ExecutorService competitor = Executors.newSingleThreadExecutor()) {
      assertTrue(handle.createResource(ResourceConfiguration.newBuilder("resource").build()));
      if (!forceRemoval) {
        handle.close();
      }
      try (final MockedStatic<Files> files = mockStatic(Files.class, invocation -> {
        if (invocation.getMethod().getName().equals("deleteIfExists")) {
          if (configuration.equals(invocation.getArgument(0))) {
            assertRemovalCompetitorsRefused(path, competitor, false);
            contentsChecked.set(true);
          } else if (lockPath.equals(invocation.getArgument(0))) {
            assertRemovalCompetitorsRefused(path, competitor, true);
            lockOnlyChecked.set(true);
          }
        }
        return invocation.callRealMethod();
      })) {
        Databases.removeDatabase(path);
        assertTrue(contentsChecked.get());
        assertTrue(lockOnlyChecked.get());
        assertFalse(handle.isOpen());
        assertFalse(Files.exists(path));
      }
      Files.createDirectory(path);
      final Process nextOwner = child(path, "lock-only");
      try {
        assertEquals("OPEN", childResult(nextOwner));
      } finally {
        stopChild(nextOwner);
      }
      assertTrue(createDatabase(path, type));
      try (final Database<?> recreated = openDatabase(path, type)) {
        assertTrue(recreated.createResource(ResourceConfiguration.newBuilder("resource").build()));
      }
    } finally {
      Databases.removeDatabase(path);
    }
  }

  private void assertRemovalCompetitorsRefused(final Path path, final ExecutorService competitor,
      final boolean lockOnly) throws Exception {
    final List<RuntimeException> failures = competitor.submit(() -> {
      final List<RuntimeException> rejected = new ArrayList<>(5);
      final Path alias = path.resolve(".");
      for (final DatabaseType type : DatabaseType.values()) {
        rejected.add(assertThrows(RuntimeException.class, () -> {
          try (final Database<?> unexpected = openDatabase(alias, type)) {
            assertTrue(unexpected.isOpen());
          }
        }));
        if (lockOnly) {
          rejected.add(assertThrows(RuntimeException.class, () -> createDatabase(alias, type)));
        } else {
          assertFalse(createDatabase(alias, type));
        }
      }
      rejected.add(assertThrows(RuntimeException.class, () -> Databases.removeDatabase(alias)));
      return rejected;
    }).get(30, TimeUnit.SECONDS);
    final Process contender = child(path, "lock-only");
    try {
      assertEquals("LOCKED " + path.toRealPath(), childResult(contender),
          "local competitors must not release the remover's process lock");
      assertTrue(contender.waitFor(30, TimeUnit.SECONDS));
      assertEquals(0, contender.exitValue());
    } finally {
      stopChild(contender);
    }
    assertTrue(failures.stream().allMatch(IllegalStateException.class::isInstance));
  }

  @ParameterizedTest
  @CsvSource({"JSON,false", "JSON,true", "XML,false", "XML,true"})
  void deletionBackendsStayOutOfConcurrentPublicSnapshots(final DatabaseType type, final boolean forceRemoval)
      throws Exception {
    final Path path = directory.resolve("deleting-" + type);
    final Path otherPath = directory.resolve("snapshot-handles-" + type);
    assertTrue(createDatabase(path, type));
    assertTrue(createDatabase(otherPath, type));
    final Path resources = path.resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile());
    final AtomicBoolean observing = new AtomicBoolean();
    final AtomicBoolean stop = new AtomicBoolean();
    final CompletableFuture<Void> firstSnapshot = new CompletableFuture<>();
    final AtomicReference<Future<?>> snapshots = new AtomicReference<>();
    final AtomicReference<Future<?>> handleChanges = new AtomicReference<>();
    try (final Database<?> handle = openDatabase(path, type);
        final ExecutorService workers = Executors.newFixedThreadPool(2)) {
      assertTrue(handle.createResource(ResourceConfiguration.newBuilder("resource").build()));
      if (!forceRemoval) {
        handle.close();
      }
      try {
        try (final MockedStatic<Files> files = mockStatic(Files.class, invocation -> {
          if (invocation.getMethod().getName().equals("list") && resources.equals(invocation.getArgument(0))
              && observing.compareAndSet(false, true)) {
            snapshots.set(workers.submit(() -> {
              try {
                do {
                  final Map<Path, Set<Database<?>>> snapshot = DatabasesInternals.getOpenDatabases();
                  for (final Set<Database<?>> handles : snapshot.values()) {
                    handles.forEach(Database::close);
                  }
                  assertFalse(snapshot.containsKey(path), "a deletion backend must not be exposed to shutdown");
                  firstSnapshot.complete(null);
                } while (!stop.get());
              } catch (final RuntimeException | Error failure) {
                firstSnapshot.completeExceptionally(failure);
                throw failure;
              }
            }));
            firstSnapshot.get(30, TimeUnit.SECONDS);
            handleChanges.set(workers.submit(() -> {
              for (int i = 0; i < 32; i++) {
                openDatabase(otherPath, type).close();
              }
            }));
          }
          return invocation.callRealMethod();
        })) {
          Databases.removeDatabase(path);
          assertTrue(observing.get());
          Objects.requireNonNull(handleChanges.get()).get(30, TimeUnit.SECONDS);
          assertFalse(handle.isOpen());
          assertFalse(Files.exists(path));
        }
      } finally {
        stop.set(true);
        if (snapshots.get() != null) {
          Objects.requireNonNull(snapshots.get()).get(30, TimeUnit.SECONDS);
        }
      }
    } finally {
      Databases.removeDatabase(path);
      Databases.removeDatabase(otherPath);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void removalFinalizesPendingLockDeletionAfterBothChannelsClose(final boolean openHandle) throws Exception {
    final Path path = createDatabase();
    final Path lockPath = path.resolve(".lock");
    final List<FileChannel> lockChannels = new ArrayList<>(2);
    final AtomicBoolean deletingLock = new AtomicBoolean();
    try (final MockedStatic<FileChannel> channels = trackLockChannels(lockPath, lockChannels);
        final Database<JsonResourceSession> database = openHandle
            ? Databases.openJsonDatabase(path)
            : null;
        final MockedStatic<Files> files = mockStatic(Files.class, invocation -> {
          if (invocation.getMethod().getName().equals("deleteIfExists") && lockPath.equals(invocation.getArgument(0))) {
            deletingLock.set(true);
            assertEquals(2, lockChannels.size());
            assertTrue(lockChannels.stream().allMatch(FileChannel::isOpen));
            assertChildRefused(path);
            return true;
          }
          if ((invocation.getMethod().getName().equals("delete")
              || invocation.getMethod().getName().equals("deleteIfExists")) && path.equals(invocation.getArgument(0))) {
            completePendingLockDeletion(lockPath, lockChannels, deletingLock);
          }
          return invocation.callRealMethod();
        })) {
      Databases.removeDatabase(path);
      assertTrue(deletingLock.get());
      assertFalse(Files.exists(path));
    } finally {
      Databases.removeDatabase(path);
    }
  }

  @ParameterizedTest
  @EnumSource(DatabaseType.class)
  void failedCreationFinalizesPendingLockDeletionAfterBothChannelsClose(final DatabaseType type) throws Exception {
    final Path path = directory.resolve("failed-creation");
    final Path lockPath = path.resolve(".lock");
    final Path keySelector = path.resolve(DatabaseConfiguration.DatabasePaths.KEY_SELECTOR.getFile());
    final List<FileChannel> lockChannels = new ArrayList<>(2);
    final AtomicBoolean deletingLock = new AtomicBoolean();
    try (final MockedStatic<FileChannel> channels = trackLockChannels(lockPath, lockChannels);
        final MockedStatic<Files> files = mockStatic(Files.class, invocation -> {
          if (invocation.getMethod().getName().equals("createDirectory")
              && keySelector.equals(invocation.getArgument(0))) {
            throw new IOException("injected creation failure");
          }
          if (invocation.getMethod().getName().equals("deleteIfExists") && lockPath.equals(invocation.getArgument(0))) {
            deletingLock.set(true);
            assertEquals(2, lockChannels.size());
            assertTrue(lockChannels.stream().allMatch(FileChannel::isOpen));
            assertChildRefused(path);
            return true;
          }
          if ((invocation.getMethod().getName().equals("delete")
              || invocation.getMethod().getName().equals("deleteIfExists")) && path.equals(invocation.getArgument(0))) {
            completePendingLockDeletion(lockPath, lockChannels, deletingLock);
          }
          return invocation.callRealMethod();
        })) {
      assertFalse(createDatabase(path, type));
      assertTrue(deletingLock.get());
      assertFalse(Files.exists(path));
    } finally {
      Databases.removeDatabase(path);
    }
  }

  private static MockedStatic<FileChannel> trackLockChannels(final Path lockPath, final List<FileChannel> channels) {
    return mockStatic(FileChannel.class, invocation -> {
      final Object result = invocation.callRealMethod();
      if (invocation.getMethod().getName().equals("open") && invocation.getRawArguments().length == 2
          && lockPath.equals(invocation.getArgument(0)) && invocation.getRawArguments()[1] instanceof OpenOption[]) {
        channels.add((FileChannel) result);
      }
      return result;
    });
  }

  private static void completePendingLockDeletion(final Path lockPath, final List<FileChannel> channels,
      final AtomicBoolean deletionRequested) {
    assertTrue(deletionRequested.get());
    assertEquals(2, channels.size());
    assertTrue(channels.stream().noneMatch(FileChannel::isOpen),
        "both lock channels must close before directory deletion");
    assertTrue(lockPath.toFile().delete(), "the pending lock file must disappear before deleting its directory");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void incompleteContentCleanupFailsBeforeLockFileDeletion(final boolean failedCreation) throws Exception {
    final Path path = directory.resolve("incomplete-cleanup");
    if (!failedCreation) {
      assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    }
    final Path keySelector = path.resolve(DatabaseConfiguration.DatabasePaths.KEY_SELECTOR.getFile());
    final Path resources = path.resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile());
    try {
      try (final MockedStatic<SirixFiles> removal = mockStatic(SirixFiles.class, invocation -> {
        if (invocation.getMethod().getName().equals("recursiveRemove")
            && keySelector.equals(invocation.getArgument(0))) {
          return null;
        }
        return invocation.callRealMethod();
      }); final MockedStatic<Files> files = mockStatic(Files.class, invocation -> {
        if (failedCreation && invocation.getMethod().getName().equals("createDirectory")
            && resources.equals(invocation.getArgument(0))) {
          throw new IOException("injected creation failure");
        }
        return invocation.callRealMethod();
      })) {
        final SirixIOException failure = assertThrows(SirixIOException.class, () -> {
          if (failedCreation) {
            Databases.createJsonDatabase(new DatabaseConfiguration(path));
          } else {
            Databases.removeDatabase(path);
          }
        });
        assertTrue(failure.getCause() instanceof DirectoryNotEmptyException);
        assertTrue(Files.exists(keySelector));
        assertTrue(Files.exists(path.resolve(".lock")));
      }
      Databases.removeDatabase(path);
      assertFalse(Files.exists(path));
    } finally {
      Databases.removeDatabase(path);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void directoryFinalizationPreservesAConcurrentReplacement(final boolean failedCreation) throws Exception {
    final Path path = directory.resolve("replaced-during-cleanup");
    if (!failedCreation) {
      assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    }
    final Path keySelector = path.resolve(DatabaseConfiguration.DatabasePaths.KEY_SELECTOR.getFile());
    final AtomicReference<Process> replacement = new AtomicReference<>();
    try {
      try (final MockedStatic<Files> files = mockStatic(Files.class, invocation -> {
        if (failedCreation && invocation.getMethod().getName().equals("createDirectory")
            && keySelector.equals(invocation.getArgument(0))) {
          throw new IOException("injected creation failure");
        }
        if ((invocation.getMethod().getName().equals("delete")
            || invocation.getMethod().getName().equals("deleteIfExists")) && path.equals(invocation.getArgument(0))) {
          final Process owner = child(path, "replace");
          replacement.set(owner);
          assertEquals("OPEN", childResult(owner));
        }
        return invocation.callRealMethod();
      })) {
        if (failedCreation) {
          assertFalse(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
        } else {
          Databases.removeDatabase(path);
        }
        assertNotNull(replacement.get());
        assertTrue(Databases.existsDatabase(path));
        assertChildRefused(path);
      }
      stopChild(replacement.get());
      try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
        assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
        try (final JsonResourceSession session = database.beginResourceSession("resource");
            final JsonNodeTrx writer = session.beginNodeTrx()) {
          writer.insertStringValueAsFirstChild("replacement");
          writer.commit();
        }
      }
      try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
          final JsonResourceSession session = database.beginResourceSession("resource");
          final JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
        assertTrue(reader.moveToFirstChild());
        assertEquals("replacement", reader.getValue());
      }
    } finally {
      if (replacement.get() != null) {
        stopChild(replacement.get());
      }
      Databases.removeDatabase(path);
    }
  }

  @ParameterizedTest
  @EnumSource(DatabaseType.class)
  void failedOpenLeavesDirectoryRecoverableByCreate(final DatabaseType type) throws Exception {
    final Path path = directory.resolve("empty");
    Files.createDirectory(path);
    assertThrows(SirixIOException.class, () -> openDatabase(path, type));
    assertTrue(Files.exists(path.resolve(".lock")));
    assertTrue(createDatabase(path, type));
    try (final Database<?> database = openDatabase(path, type)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
    } finally {
      Databases.removeDatabase(path);
    }
  }

  @ParameterizedTest
  @EnumSource(DatabaseType.class)
  void killedCreatorLeavesLockOnlyDirectoryRecoverable(final DatabaseType type) throws Exception {
    final Path path = directory.resolve("interrupted-creation");
    Files.createDirectory(path);
    final Process creator = child(path, "lock-only");
    try {
      assertEquals("OPEN", childResult(creator));
      assertThrows(SirixDatabaseLockException.class, () -> createDatabase(path, type));
      stopChild(creator);
      assertTrue(createDatabase(path, type));
      try (final Database<?> database = openDatabase(path, type)) {
        assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      }
    } finally {
      stopChild(creator);
      Databases.removeDatabase(path);
    }
  }

  @ParameterizedTest
  @CsvSource({"JSON,false", "JSON,true", "XML,false", "XML,true"})
  void timedCommitHookCanOpenAnotherDatabaseDuringCleanup(final DatabaseType type, final boolean forceRemoval)
      throws Exception {
    final Path path = directory.resolve("timed-" + type);
    final Path hookTarget = path.resolveSibling(path.getFileName() + "-hook-target");
    assertTrue(createDatabase(path, type));
    assertTrue(createDatabase(hookTarget, type));
    final Process process = child(path, forceRemoval
        ? "timed-remove"
        : "timed-close");
    try {
      assertEquals("READY", childResult(process));
      assertChildRefused(path);
      process.getOutputStream().write('\n');
      process.getOutputStream().flush();
      assertEquals("OPEN", childResult(process));
      assertTrue(process.waitFor(30, TimeUnit.SECONDS));
      assertEquals(0, process.exitValue());
      if (forceRemoval) {
        assertFalse(Files.exists(path));
      } else {
        assertTrue(Files.exists(path.resolve(".lock")));
        try (final Database<?> reopened = openDatabase(path, type);
            final ResourceSession<?, ?> session = reopened.beginResourceSession("resource")) {
          assertTrue(session.getMostRecentRevisionNumber() > 0);
        }
      }
    } finally {
      stopChild(process);
      Databases.removeDatabase(path);
      Databases.removeDatabase(hookTarget);
    }
  }

  @ParameterizedTest
  @CsvSource({"JSON,false,session", "JSON,true,session", "XML,false,session", "XML,true,session", "JSON,false,create",
      "JSON,true,create", "XML,false,create", "XML,true,create"})
  void timedCommitHookCannotAdmitResourcesOnClosingHandle(final DatabaseType type, final boolean forceRemoval,
      final String callback) throws Exception {
    final Path path = directory.resolve("timed-handle-" + callback + "-" + type);
    assertTrue(createDatabase(path, type));
    final Process process = child(path, "timed-" + callback + (forceRemoval
        ? "-remove"
        : "-close"));
    try {
      assertEquals("READY", childResult(process));
      assertChildRefused(path);
      process.getOutputStream().write('\n');
      process.getOutputStream().flush();
      assertEquals("OPEN", childResult(process));
      assertTrue(process.waitFor(30, TimeUnit.SECONDS));
      assertEquals(0, process.exitValue());
      if (forceRemoval) {
        assertFalse(Files.exists(path));
      } else {
        try (final Database<?> reopened = openDatabase(path, type)) {
          assertTrue(reopened.existsResource("other"));
          assertFalse(reopened.existsResource("from-hook"));
          try (final ResourceSession<?, ?> session = reopened.beginResourceSession("resource")) {
            assertTrue(session.getMostRecentRevisionNumber() > 0);
          }
        }
      }
    } finally {
      stopChild(process);
      Databases.removeDatabase(path);
    }
  }

  @ParameterizedTest
  @CsvSource({"JSON,false", "JSON,true", "XML,false", "XML,true"})
  void failedCleanupRetainsWritersAndOwnershipUntilRetry(final DatabaseType type, final boolean forceRemoval)
      throws Exception {
    final Path path = directory.resolve("failed-cleanup-" + type);
    assertTrue(createDatabase(path, type));
    try (final Database<?> failing = openDatabase(path, type)) {
      assertTrue(failing.createResource(ResourceConfiguration.newBuilder("failed").build()));
      assertTrue(failing.createResource(ResourceConfiguration.newBuilder("healthy").build()));
      final ResourceSession<?, ?> session = failing.beginResourceSession("failed");
      final ResourceSession<?, ?> healthy = failing.beginResourceSession("healthy");
      final NodeTrx writer = session.beginNodeTrx();
      if (writer instanceof JsonNodeTrx jsonWriter) {
        jsonWriter.insertStringValueAsFirstChild("pending");
      } else {
        ((XmlNodeTrx) writer).insertElementAsFirstChild(new QNm("pending"));
      }
      setWriterField(writer, "asyncCommitFailure", new IOException("injected hardening failure"));
      try (final Database<?> sibling = forceRemoval
          ? openDatabase(path, type)
          : null) {
        final ResourceSession<?, ?> siblingSession = sibling == null
            ? null
            : sibling.beginResourceSession("healthy");
        assertThrows(SirixIOException.class, () -> {
          if (forceRemoval) {
            Databases.removeDatabase(path);
          } else {
            failing.close();
          }
        });
        assertTrue(failing.isOpen());
        assertFalse(session.isClosed());
        assertFalse(writer.isClosed());
        assertTrue(healthy.isClosed(), "other resources must still be quiesced");
        if (sibling != null) {
          assertFalse(sibling.isOpen(), "other handles must still be quiesced");
          assertTrue(Objects.requireNonNull(siblingSession).isClosed());
        }
        assertTrue(
            Objects.requireNonNull(DatabasesInternals.getOpenDatabases().get(path.toRealPath())).contains(failing));
        assertThrows(IllegalStateException.class, () -> failing.removeResource("failed"));
        assertChildRefused(path);
        assertSame(session, failing.beginResourceSession("failed"));
        assertTrue(failing.createResource(ResourceConfiguration.newBuilder("after-failure").build()));
        try (final Database<?> reopened = openDatabase(path, type)) {
          final ResourceSession<?, ?> other = reopened.beginResourceSession("failed");
          assertThrows(SirixUsageException.class, other::beginNodeTrx);
          assertSame(session.getRtxIndexController(session.getMostRecentRevisionNumber()),
              other.getRtxIndexController(other.getMostRecentRevisionNumber()));
        }
      } finally {
        setWriterField(writer, "asyncCommitFailure", null);
        setWriterField(writer, "asyncCommitTerminalFailure", false);
      }
      if (forceRemoval) {
        Databases.removeDatabase(path);
        assertFalse(Files.exists(path));
        assertTrue(createDatabase(path, type));
      } else {
        failing.close();
      }
      assertFalse(failing.isOpen());
      assertTrue(session.isClosed());
      assertTrue(writer.isClosed());
      assertChildOpened(path, type);
    } finally {
      Databases.removeDatabase(path);
    }
  }

  @ParameterizedTest
  @EnumSource(DatabaseType.class)
  void delayedSessionReleaseStaysRegisteredUntilItReleasesItsBackend(final DatabaseType type) throws Exception {
    final Path path = directory.resolve("session-generation");
    assertTrue(createDatabase(path, type));
    try (final Database<?> first = openDatabase(path, type); final Database<?> second = openDatabase(path, type)) {
      assertTrue(first.createResource(ResourceConfiguration.newBuilder("resource").build()));
      final ResourceSession<?, ?> old = first.beginResourceSession("resource");
      final Field ownerField = DatabaseHandle.class.getDeclaredField("owner");
      ownerField.setAccessible(true);
      final Databases.OpenDatabase<?> owner = (Databases.OpenDatabase<?>) ownerField.get(first);
      final CompletableFuture<Void> closed = new CompletableFuture<>();
      final Thread closer = Thread.ofPlatform().daemon().unstarted(() -> {
        try {
          old.close();
          closed.complete(null);
        } catch (final Throwable failure) {
          closed.completeExceptionally(failure);
        }
      });
      try {
        synchronized (owner.localDatabase) {
          closer.start();
          final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
          final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
          ThreadInfo state = threads.getThreadInfo(closer.threadId());
          while ((state == null || state.getLockOwnerId() != Thread.currentThread().threadId()) && closer.isAlive()
              && System.nanoTime() < deadline) {
            Thread.onSpinWait();
            state = threads.getThreadInfo(closer.threadId());
          }
          assertNotNull(state);
          assertEquals(Thread.currentThread().threadId(), state.getLockOwnerId());
          assertEquals(System.identityHashCode(owner.localDatabase), state.getLockInfo().getIdentityHashCode());
          assertThrows(IllegalStateException.class, () -> second.removeResource("resource"));
        }
        closed.get(30, TimeUnit.SECONDS);
        second.removeResource("resource");
        assertTrue(second.createResource(ResourceConfiguration.newBuilder("resource").build()));
        try (final ResourceSession<?, ?> fresh = second.beginResourceSession("resource");
            final ResourceSession<?, ?> other = first.beginResourceSession("resource")) {
          old.close();
          final int revision = fresh.getMostRecentRevisionNumber();
          try (final NodeTrx writer = fresh.beginNodeTrx()) {
            writer.commit();
          }
          assertEquals(revision + 1, other.getMostRecentRevisionNumber());
          try (final NodeTrx writer = other.beginNodeTrx()) {
            writer.commit();
          }
          assertEquals(revision + 2, fresh.getMostRecentRevisionNumber());
        }
      } finally {
        closer.join(TimeUnit.SECONDS.toMillis(30));
        assertFalse(closer.isAlive(), "session closer must finish");
      }
    } finally {
      Databases.removeDatabase(path);
    }
  }

  @Test
  void failedBackendCloseRetainsOwnershipUntilRetry() throws Exception {
    final Path path = createDatabase();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      final Field ownerField = DatabaseHandle.class.getDeclaredField("owner");
      ownerField.setAccessible(true);
      final Databases.OpenDatabase<?> owner = (Databases.OpenDatabase<?>) ownerField.get(database);
      final Field storeField = LocalDatabase.class.getDeclaredField("resourceStore");
      storeField.setAccessible(true);
      final Object originalStore = storeField.get(owner.localDatabase);
      final ResourceStore<?> failingStore = mock(ResourceStore.class);
      doThrow(new IllegalStateException("injected backend cleanup failure")).doNothing().when(failingStore).close();
      storeField.set(owner.localDatabase, failingStore);
      try {
        assertThrows(IllegalStateException.class, database::close);
        assertTrue(database.isOpen());
        assertTrue(
            Objects.requireNonNull(DatabasesInternals.getOpenDatabases().get(path.toRealPath())).contains(database));
        assertChildRefused(path);
        database.close();
        assertFalse(database.isOpen());
        assertChildOpened(path);
      } finally {
        storeField.set(owner.localDatabase, originalStore);
      }
    } finally {
      Databases.removeDatabase(path);
    }
  }

  @Test
  void failedRemovalBackendCloseRetainsOwnershipUntilRetry() throws Exception {
    final Path path = createDatabase();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      final Field ownerField = DatabaseHandle.class.getDeclaredField("owner");
      ownerField.setAccessible(true);
      final Databases.OpenDatabase<?> owner = (Databases.OpenDatabase<?>) ownerField.get(database);
      final Field storeField = LocalDatabase.class.getDeclaredField("resourceStore");
      storeField.setAccessible(true);
      final Object originalStore = storeField.get(owner.localDatabase);
      final ResourceStore<?> failingStore = mock(ResourceStore.class);
      doThrow(new IllegalStateException("injected backend cleanup failure")).doNothing().when(failingStore).close();
      storeField.set(owner.localDatabase, failingStore);
      try {
        assertThrows(IllegalStateException.class, () -> Databases.removeDatabase(path));
        assertFalse(database.isOpen());
        assertChildRefused(path);
        Databases.removeDatabase(path);
        assertFalse(Files.exists(path));
        assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
        assertChildOpened(path);
      } finally {
        storeField.set(owner.localDatabase, originalStore);
      }
    } finally {
      Databases.removeDatabase(path);
    }
  }

  private static void setWriterField(final NodeTrx writer, final String name, final @Nullable Object value)
      throws Exception {
    final Field field = AbstractNodeTrxImpl.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(writer, value);
  }

  private static boolean createDatabase(final Path path, final DatabaseType type) {
    final DatabaseConfiguration config = new DatabaseConfiguration(path);
    return type == DatabaseType.JSON
        ? Databases.createJsonDatabase(config)
        : Databases.createXmlDatabase(config);
  }

  private static Database<?> openDatabase(final Path path, final DatabaseType type) {
    return type == DatabaseType.JSON
        ? Databases.openJsonDatabase(path)
        : Databases.openXmlDatabase(path);
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
      assertTrue(Objects.requireNonNull(failure.getMessage()).contains(path.toRealPath().toString()));
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
    assertChildOpened(path, DatabaseType.JSON);
  }

  private void assertChildOpened(final Path path, final DatabaseType type) throws Exception {
    final Process process = child(path, type == DatabaseType.JSON
        ? "open"
        : "open-xml");
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
        "--add-modules", "jdk.incubator.vector", "-Dsirix.allocator.maxSize=64M", "-Djava.io.tmpdir=" + directory,
        "-cp", System.getProperty("java.class.path"), Child.class.getName(), path.toString(), mode).redirectError(
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
        if (args[1].equals("timed-close") || args[1].equals("timed-remove") || args[1].equals("timed-session-close")
            || args[1].equals("timed-session-remove") || args[1].equals("timed-create-close")
            || args[1].equals("timed-create-remove")) {
          final String callback = args[1].startsWith("timed-session-")
              ? "session"
              : args[1].startsWith("timed-create-")
                  ? "create"
                  : "database";
          timedCleanup(path, args[1].endsWith("-remove"), callback);
          return;
        }
        if (args[1].equals("lock-only")) {
          try (final DatabaseLock lock = DatabaseLock.acquire(path)) {
            System.out.println("OPEN");
            System.out.flush();
            System.in.read();
          }
          return;
        }
        if (args[1].equals("replace")) {
          Databases.removeDatabase(path);
          if (!Databases.createJsonDatabase(new DatabaseConfiguration(path))) {
            throw new IllegalStateException("Replacement database was not created");
          }
        }
        if (args[1].equals("create")) {
          System.out.println("READY");
          System.out.flush();
          System.in.read();
          System.out.println(Databases.createJsonDatabase(new DatabaseConfiguration(path))
              ? "CREATED"
              : "EXISTS");
          return;
        }
        try (final Database<?> database = args[1].equals("open-xml")
            ? Databases.openXmlDatabase(path)
            : Databases.openJsonDatabase(path)) {
          if (!database.isOpen()) {
            throw new IllegalStateException("Child database is closed");
          }
          System.out.println("OPEN");
          System.out.flush();
          if (args[1].equals("hold") || args[1].equals("replace")) {
            System.in.read();
          }
        }
      } catch (final SirixDatabaseLockException e) {
        if (!Objects.requireNonNull(e.getMessage()).contains(path.toRealPath().toString())) {
          throw e;
        }
        System.out.println("LOCKED " + e.getDatabasePath());
      }
    }

    private static void timedCleanup(final Path path, final boolean forceRemoval, final String callback)
        throws Exception {
      final DatabaseType type = Databases.getDatabaseType(path);
      final Path hookTarget = path.resolveSibling(path.getFileName() + "-hook-target");
      final CountDownLatch hookEntered = new CountDownLatch(1);
      final CountDownLatch allowHook = new CountDownLatch(1);
      final AtomicBoolean firstHook = new AtomicBoolean(true);
      final CompletableFuture<Void> hookOpened = new CompletableFuture<>();
      final CompletableFuture<Void> cleanupFinished = new CompletableFuture<>();
      try (final Database<?> database = openDatabase(path, type)) {
        assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
        if (!callback.equals("database")) {
          assertTrue(database.createResource(ResourceConfiguration.newBuilder("other").build()));
          try (final ResourceSession<?, ?> other = database.beginResourceSession("other")) {
            assertFalse(other.isClosed());
          }
        }
        final ResourceSession<?, ?> session = database.beginResourceSession("resource");
        final NodeTrx writer = session.beginNodeTrx(10, TimeUnit.MILLISECONDS);
        writer.addPreCommitHook(unused -> {
          if (firstHook.compareAndSet(true, false)) {
            hookEntered.countDown();
            try {
              assertTrue(allowHook.await(30, TimeUnit.SECONDS));
              assertTrue(Objects.requireNonNull(DatabasesInternals.getOpenDatabases().get(path.toRealPath()))
                                .contains(database));
              switch (callback) {
                case "session" ->
                  assertThrows(IllegalStateException.class, () -> database.beginResourceSession("other"));
                case "create" -> {
                  assertThrows(IllegalStateException.class,
                      () -> database.createResource(ResourceConfiguration.newBuilder("from-hook").build()));
                  assertFalse(Files.exists(
                      path.resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve("from-hook")));
                }
                case "database" -> {
                  try (final Database<?> other = openDatabase(hookTarget, type)) {
                    assertTrue(other.isOpen());
                  }
                  if (forceRemoval) {
                    assertThrows(IllegalStateException.class, () -> {
                      try (final Database<?> unexpected = openDatabase(path, type)) {
                        assertTrue(unexpected.isOpen());
                      }
                    });
                    assertThrows(IllegalStateException.class, () -> Databases.removeDatabase(path));
                  } else {
                    try (final Database<?> concurrent = openDatabase(path, type)) {
                      assertTrue(concurrent.isOpen());
                    }
                  }
                }
                default -> throw new IllegalArgumentException(callback);
              }
              hookOpened.complete(null);
            } catch (final Throwable failure) {
              hookOpened.completeExceptionally(failure);
              throw new IllegalStateException(failure);
            }
          }
        });
        assertTrue(hookEntered.await(30, TimeUnit.SECONDS));
        final Field lockField = AbstractNodeTrxImpl.class.getDeclaredField("lock");
        lockField.setAccessible(true);
        final ReentrantLock transactionLock = (ReentrantLock) Objects.requireNonNull(lockField.get(writer));
        final Thread closer = Thread.ofPlatform().daemon().unstarted(() -> {
          try {
            if (forceRemoval) {
              Databases.removeDatabase(path);
            } else {
              database.close();
            }
            cleanupFinished.complete(null);
          } catch (final Throwable failure) {
            cleanupFinished.completeExceptionally(failure);
          }
        });
        try {
          closer.start();
          final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
          while (!transactionLock.hasQueuedThread(closer) && !cleanupFinished.isDone()
              && System.nanoTime() < deadline) {
            Thread.sleep(1);
          }
          assertTrue(transactionLock.hasQueuedThread(closer));
          System.out.println("READY");
          System.out.flush();
          System.in.read();
          allowHook.countDown();
          hookOpened.get(30, TimeUnit.SECONDS);
          cleanupFinished.get(30, TimeUnit.SECONDS);
          assertFalse(database.isOpen());
          assertTrue(session.isClosed());
          assertTrue(writer.isClosed());
          System.out.println("OPEN");
          System.out.flush();
        } finally {
          allowHook.countDown();
          closer.join(TimeUnit.SECONDS.toMillis(30));
          assertFalse(closer.isAlive());
        }
      }
    }
  }
}
