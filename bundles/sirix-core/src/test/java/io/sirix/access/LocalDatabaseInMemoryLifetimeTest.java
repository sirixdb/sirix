/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access;

import io.sirix.access.InMemoryLifetimeFixture.Model;
import io.sirix.api.Database;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.exception.SirixUsageException;
import io.sirix.io.StorageType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import static io.sirix.access.InMemoryLifetimeFixture.commitFirstValue;
import static io.sirix.access.InMemoryLifetimeFixture.commitNextValue;
import static io.sirix.access.InMemoryLifetimeFixture.createDatabase;
import static io.sirix.access.InMemoryLifetimeFixture.createResource;
import static io.sirix.access.InMemoryLifetimeFixture.hasContent;
import static io.sirix.access.InMemoryLifetimeFixture.insertValueAfter;
import static io.sirix.access.InMemoryLifetimeFixture.open;
import static io.sirix.access.InMemoryLifetimeFixture.resourceNames;
import static io.sirix.access.InMemoryLifetimeFixture.valueAt;
import static io.sirix.access.InMemoryLifetimeFixture.valueOf;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * TXN-615 acceptance criteria on one database instance and its reopen: AC2 (handles), AC3
 * (threads), AC4 (reopen), AC5 (mixed database), AC8 (dirty transaction at close), AC9 (removal)
 * and AC11 (leftover verification). Each test cites the clause it is derived from; everything is
 * observed through the public API.
 */
final class LocalDatabaseInMemoryLifetimeTest {

  private static final String VOLATILE = "volatile";

  private static final String DURABLE = "durable";

  @TempDir
  Path directory;

  @AfterEach
  void removeDatabase() {
    Databases.removeDatabase(directory.resolve("database"));
  }

  private Path databasePath() {
    return directory.resolve("database");
  }

  static Stream<Arguments> modelsAndCloseOrders() {
    return Stream.of(Model.values()).flatMap(model -> Stream.of(Arguments.of(model, true), Arguments.of(model, false)));
  }

  /**
   * AC2, clause 2: only the last handle's close ends the instance; handles may close in either order.
   */
  @ParameterizedTest
  @MethodSource("modelsAndCloseOrders")
  void closingOneOfTwoHandlesKeepsTheStateUntilTheLastCloses(final Model model, final boolean closeFirstHandleFirst) {
    createDatabase(model, databasePath());
    final Database<? extends ResourceSession<?, ?>> first = open(model, databasePath());
    final Database<? extends ResourceSession<?, ?>> second = open(model, databasePath());
    final long key;
    createResource(first, VOLATILE, StorageType.IN_MEMORY);
    try (ResourceSession<?, ?> session = first.beginResourceSession(VOLATILE)) {
      key = commitFirstValue(session, model, "0");
    }
    final Database<? extends ResourceSession<?, ?>> closedFirst = closeFirstHandleFirst
        ? first
        : second;
    final Database<? extends ResourceSession<?, ?>> remaining = closeFirstHandleFirst
        ? second
        : first;
    closedFirst.close();
    try (ResourceSession<?, ?> session = remaining.beginResourceSession(VOLATILE)) {
      assertEquals(1, session.getMostRecentRevisionNumber());
      assertEquals("0", valueAt(session, model, 1, key), "a handle's close must not discard the shared state");
    }
    remaining.close();
    try (Database<? extends ResourceSession<?, ?>> reopened = open(model, databasePath())) {
      assertFalse(resourceNames(reopened).contains(VOLATILE), "the last close discards the state");
    }
  }

  /** AC3, clause 2: threads share one handle and session, each with its own transactions. */
  @ParameterizedTest
  @EnumSource(Model.class)
  void threadsSharingAHandleAndSessionSeeTheirOwnSnapshots(final Model model) throws Exception {
    createDatabase(model, databasePath());
    final ExecutorService readerThread = Executors.newSingleThreadExecutor();
    final ExecutorService writerThread = Executors.newSingleThreadExecutor();
    try (Database<? extends ResourceSession<?, ?>> database = open(model, databasePath())) {
      createResource(database, VOLATILE, StorageType.IN_MEMORY);
      try (ResourceSession<?, ?> session = database.beginResourceSession(VOLATILE)) {
        final long first = commitFirstValue(session, model, "0");
        final CountDownLatch readerOpen = new CountDownLatch(1);
        final CountDownLatch committed = new CountDownLatch(1);
        final CompletableFuture<String[]> reads = CompletableFuture.supplyAsync(() -> {
          try (NodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(1)) {
            readerOpen.countDown();
            await(committed);
            // [held reader's view of the first value, held reader's view of the new value]
            return new String[] {valueOf(reader, model, first), String.valueOf(reader.getRevisionNumber())};
          }
        }, readerThread);
        await(readerOpen);
        final long second =
            CompletableFuture.supplyAsync(() -> commitNextValue(session, model, first, "1"), writerThread)
                             .get(60, TimeUnit.SECONDS);
        committed.countDown();
        final String[] held = reads.get(60, TimeUnit.SECONDS);
        assertEquals("0", held[0]);
        assertEquals("1", held[1], "the held reader keeps revision 1");
        final String afterwards = CompletableFuture.supplyAsync(() -> valueAt(session, model, 2, second), readerThread)
                                                   .get(60, TimeUnit.SECONDS);
        assertEquals("1", afterwards, "a reader opened after the commit sees it");
        assertNull(CompletableFuture.supplyAsync(() -> valueAt(session, model, 1, second), readerThread)
                                    .get(60, TimeUnit.SECONDS),
            "revision 1 never sees the later commit");
      }
    } finally {
      readerThread.shutdownNow();
      writerThread.shutdownNow();
    }
  }

  /** AC4, clause 5: after the last close, the former IN_MEMORY name is gone and reusable. */
  @ParameterizedTest
  @EnumSource(Model.class)
  void reopenedDatabaseHoldsNoFormerInMemoryResource(final Model model) {
    createDatabase(model, databasePath());
    final long key;
    try (Database<? extends ResourceSession<?, ?>> database = open(model, databasePath())) {
      createResource(database, VOLATILE, StorageType.IN_MEMORY);
      try (ResourceSession<?, ?> session = database.beginResourceSession(VOLATILE)) {
        key = commitFirstValue(session, model, "0");
      }
    }
    assertFalse(Files.exists(resourceDirectory(VOLATILE)), "the final close removes the on-disk footprint");
    try (Database<? extends ResourceSession<?, ?>> reopened = open(model, databasePath())) {
      assertFalse(resourceNames(reopened).contains(VOLATILE), "not listed");
      assertFalse(reopened.existsResource(VOLATILE), "does not exist");
      assertThrows(SirixUsageException.class, () -> reopened.beginResourceSession(VOLATILE),
          "fails as a name that never existed");
      for (final String name : resourceNames(reopened)) {
        reopened.beginResourceSession(name).close();
      }
      assertThrows(SirixUsageException.class, () -> reopened.getResourceID(VOLATILE), "no resource ID");

      createResource(reopened, VOLATILE, StorageType.IN_MEMORY);
      try (ResourceSession<?, ?> session = reopened.beginResourceSession(VOLATILE)) {
        assertEquals(0, session.getMostRecentRevisionNumber(), "a recreated name starts at bootstrap revision 0");
        assertFalse(hasContent(session), "with no content");
        assertNull(valueAt(session, model, 0, key));
      }
    }
  }

  @ParameterizedTest
  @EnumSource(Model.class)
  void longResourceNamesAreRetiredAndReusable(final Model model) {
    final String name = "v".repeat(220);
    createDatabase(model, databasePath());
    try (Database<? extends ResourceSession<?, ?>> database = open(model, databasePath())) {
      createResource(database, name, StorageType.IN_MEMORY);
      try (ResourceSession<?, ?> session = database.beginResourceSession(name)) {
        commitFirstValue(session, model, "0");
      }
    }
    assertFalse(Files.exists(resourceDirectory(name)));
    try (Database<? extends ResourceSession<?, ?>> reopened = open(model, databasePath())) {
      assertFalse(reopened.existsResource(name));
      assertFalse(resourceNames(reopened).contains(name));
      createResource(reopened, name, StorageType.IN_MEMORY);
      try (ResourceSession<?, ?> session = reopened.beginResourceSession(name)) {
        assertEquals(0, session.getMostRecentRevisionNumber());
        assertFalse(hasContent(session));
      }
    }
    assertFalse(Files.exists(resourceDirectory(name)));
  }

  /** AC5, clause 3 and 5: in a mixed database only the IN_MEMORY resource vanishes. */
  @ParameterizedTest
  @EnumSource(Model.class)
  void mixedDatabaseKeepsItsPersistentResource(final Model model) {
    createDatabase(model, databasePath());
    final long durableFirst;
    final long durableSecond;
    try (Database<? extends ResourceSession<?, ?>> database = open(model, databasePath())) {
      createResource(database, DURABLE, StorageType.FILE_CHANNEL);
      createResource(database, VOLATILE, StorageType.IN_MEMORY);
      try (ResourceSession<?, ?> durable = database.beginResourceSession(DURABLE);
          ResourceSession<?, ?> volatileSession = database.beginResourceSession(VOLATILE)) {
        durableFirst = commitFirstValue(durable, model, "d0");
        durableSecond = commitNextValue(durable, model, durableFirst, "d1");
        final long volatileKey = commitFirstValue(volatileSession, model, "v0");
        assertEquals("v0", valueAt(volatileSession, model, 1, volatileKey), "both readable before close");
      }
      assertEquals(Set.of(DURABLE, VOLATILE), resourceNames(database), "both listed before close");
      assertTrue(database.existsResource(VOLATILE));
      assertTrue(database.existsResource(DURABLE));
    }
    try (Database<? extends ResourceSession<?, ?>> reopened = open(model, databasePath())) {
      assertEquals(Set.of(DURABLE), resourceNames(reopened));
      assertFalse(reopened.existsResource(VOLATILE));
      try (ResourceSession<?, ?> durable = reopened.beginResourceSession(DURABLE)) {
        assertEquals(2, durable.getMostRecentRevisionNumber());
        assertEquals("d0", valueAt(durable, model, 1, durableFirst));
        assertNull(valueAt(durable, model, 1, durableSecond));
        assertEquals("d1", valueAt(durable, model, 2, durableSecond));
      }
      assertDoesNotThrow(() -> reopened.getResourceID(DURABLE), "the persistent resource keeps its ID");
      assertThrows(SirixUsageException.class, () -> reopened.getResourceID(VOLATILE));
    }
  }

  /** AC8, clause 3: close aborts uncommitted work, never commits it, and still discards the state. */
  @ParameterizedTest
  @EnumSource(Model.class)
  void closingWithADirtyWriterCommitsNothing(final Model model) {
    createDatabase(model, databasePath());
    final long committedKey;
    final long uncommittedKey;
    final Database<? extends ResourceSession<?, ?>> database = open(model, databasePath());
    createResource(database, VOLATILE, StorageType.IN_MEMORY);
    final ResourceSession<?, ?> session = database.beginResourceSession(VOLATILE);
    committedKey = commitFirstValue(session, model, "0");
    final NodeTrx writer = session.beginNodeTrx();
    assertTrue(writer.moveTo(committedKey));
    uncommittedKey = insertValueAfter(writer, model, "uncommitted");
    assertDoesNotThrow(database::close, "closing the database aborts the dirty writer");
    // JAPI-103: after the close, operations that need live state fail clearly.
    assertThrows(IllegalStateException.class, session::beginNodeReadOnlyTrx);
    try (Database<? extends ResourceSession<?, ?>> reopened = open(model, databasePath())) {
      assertFalse(resourceNames(reopened).contains(VOLATILE));
      createResource(reopened, VOLATILE, StorageType.IN_MEMORY);
      try (ResourceSession<?, ?> recreated = reopened.beginResourceSession(VOLATILE)) {
        assertEquals(0, recreated.getMostRecentRevisionNumber());
        assertNull(valueAt(recreated, model, 0, uncommittedKey), "nothing uncommitted was committed");
      }
    }
  }

  /**
   * AC9, clause 8: removal discards at once, refuses with open sessions, removeDatabase closes first.
   */
  @ParameterizedTest
  @EnumSource(Model.class)
  void removalDiscardsTheStateAndFollowsThePersistentPreconditions(final Model model) {
    createDatabase(model, databasePath());
    try (Database<? extends ResourceSession<?, ?>> database = open(model, databasePath())) {
      createResource(database, VOLATILE, StorageType.IN_MEMORY);
      createResource(database, DURABLE, StorageType.FILE_CHANNEL);
      final long key;
      try (ResourceSession<?, ?> session = database.beginResourceSession(VOLATILE)) {
        key = commitFirstValue(session, model, "0");
      }

      // Refused while a session is open, exactly as for a persistent resource.
      final Class<? extends Throwable> persistentRefusal;
      try (ResourceSession<?, ?> durable = database.beginResourceSession(DURABLE)) {
        persistentRefusal = assertThrows(RuntimeException.class, () -> database.removeResource(DURABLE)).getClass();
      }
      try (ResourceSession<?, ?> open = database.beginResourceSession(VOLATILE)) {
        assertSame(persistentRefusal,
            assertThrows(RuntimeException.class, () -> database.removeResource(VOLATILE)).getClass());
        assertEquals("0", valueAt(open, model, 1, key), "a refused removal changes nothing");
      }

      database.removeResource(VOLATILE);
      assertFalse(database.existsResource(VOLATILE));
      assertFalse(resourceNames(database).contains(VOLATILE));
      assertThrows(SirixUsageException.class, () -> database.getResourceID(VOLATILE), "a removed name has no ID");
      assertFalse(Files.exists(resourceDirectory(VOLATILE)), "removal takes the on-disk footprint with it");
      createResource(database, VOLATILE, StorageType.IN_MEMORY);
      try (ResourceSession<?, ?> recreated = database.beginResourceSession(VOLATILE)) {
        assertEquals(0, recreated.getMostRecentRevisionNumber());
        assertFalse(hasContent(recreated));
      }
      try (ResourceSession<?, ?> session = database.beginResourceSession(VOLATILE)) {
        commitFirstValue(session, model, "again");
      }

      // removeDatabase with this handle still open closes the instance first.
      Databases.removeDatabase(databasePath());
    }
    createDatabase(model, databasePath());
    try (Database<? extends ResourceSession<?, ?>> recreated = open(model, databasePath())) {
      assertTrue(resourceNames(recreated).isEmpty(), "a database recreated at the path is empty");
    }
  }

  /** AC11, clause 6: cleanup never touches a resource it cannot prove to be IN_MEMORY. */
  @ParameterizedTest
  @EnumSource(Model.class)
  void leftoverCleanupNeverRemovesUnprovenResources(final Model model) throws IOException {
    createDatabase(model, databasePath());
    try (Database<? extends ResourceSession<?, ?>> database = open(model, databasePath())) {
      for (final String name : new String[] {"missing", "empty", "garbled", "emptydata"}) {
        createResource(database, name, StorageType.FILE_CHANNEL);
      }
    }
    final Path resources = databasePath().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile());
    final Path settings = ResourceConfiguration.ResourcePaths.CONFIG_BINARY.getPath();
    Files.delete(resources.resolve("missing").resolve(settings));
    Files.write(resources.resolve("empty").resolve(settings), new byte[0]);
    Files.writeString(resources.resolve("garbled").resolve(settings), "{\"storageKind\":", StandardCharsets.UTF_8);
    try (Stream<Path> dataFiles =
        Files.list(resources.resolve("emptydata").resolve(ResourceConfiguration.ResourcePaths.DATA.getPath()))) {
      for (final Path dataFile : dataFiles.toList()) {
        if (Files.isRegularFile(dataFile)) {
          Files.write(dataFile, new byte[0]);
        }
      }
    }

    // A new instance (where leftovers are looked for) and its final close (where state is discarded).
    open(model, databasePath()).close();

    for (final String name : new String[] {"missing", "empty", "garbled", "emptydata"}) {
      assertTrue(Files.isDirectory(resources.resolve(name)), name + " must survive the in-memory cleanup");
    }
    assertTrue(Files.isRegularFile(resources.resolve("garbled").resolve(settings)));
  }

  /** Reserved names stay reserved: they would collide with derived state or retired directories. */
  @ParameterizedTest
  @EnumSource(Model.class)
  void reservedResourceNamesAreRefused(final Model model) {
    createDatabase(model, databasePath());
    try (Database<? extends ResourceSession<?, ?>> database = open(model, databasePath())) {
      for (final String reserved : new String[] {"name$derived", "name$retired-0"}) {
        assertThrows(SirixUsageException.class, () -> database.createResource(
            ResourceConfiguration.newBuilder(reserved).storageType(StorageType.IN_MEMORY).build()), reserved);
        assertFalse(resourceNames(database).contains(reserved));
      }
    }
  }

  /** Clause 1 and 3: closing one database never touches another open database's IN_MEMORY state. */
  @ParameterizedTest
  @EnumSource(Model.class)
  void closingOneDatabaseLeavesAnotherOpenDatabaseAlone(final Model model) {
    final Path otherPath = directory.resolve("other");
    createDatabase(model, databasePath());
    createDatabase(model, otherPath);
    try (Database<? extends ResourceSession<?, ?>> other = open(model, otherPath)) {
      createResource(other, VOLATILE, StorageType.IN_MEMORY);
      final long key;
      try (ResourceSession<?, ?> session = other.beginResourceSession(VOLATILE)) {
        key = commitFirstValue(session, model, "kept");
      }
      try (Database<? extends ResourceSession<?, ?>> database = open(model, databasePath())) {
        createResource(database, VOLATILE, StorageType.IN_MEMORY);
        try (ResourceSession<?, ?> session = database.beginResourceSession(VOLATILE)) {
          commitFirstValue(session, model, "discarded");
        }
      }
      try (ResourceSession<?, ?> session = other.beginResourceSession(VOLATILE)) {
        assertEquals(1, session.getMostRecentRevisionNumber());
        assertEquals("kept", valueAt(session, model, 1, key));
      }
    } finally {
      Databases.removeDatabase(otherPath);
    }
  }

  @ParameterizedTest
  @MethodSource("modelsAndCloseOrders")
  void derivedLeftoversAreRetiredBeforeSourceNameReuse(final Model model, final boolean retryHiddenCreation)
      throws IOException {
    assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"), "needs POSIX permissions");
    createDatabase(model, databasePath());
    final Path resources = resourceDirectory(VOLATILE).getParent();
    final Path derived = resourceDirectory(VOLATILE + "$derived");
    final Set<PosixFilePermission> parentPermissions = Files.getPosixFilePermissions(resources);
    try (Database<? extends ResourceSession<?, ?>> database = open(model, databasePath())) {
      createResource(database, VOLATILE, StorageType.IN_MEMORY);
      createResource(database, "derived-fixture", StorageType.FILE_CHANNEL);
      try (ResourceSession<?, ?> session = database.beginResourceSession(VOLATILE)) {
        commitFirstValue(session, model, "discarded");
      }
      try (ResourceSession<?, ?> session = database.beginResourceSession("derived-fixture")) {
        commitFirstValue(session, model, "derived");
      }
      Files.move(resourceDirectory("derived-fixture"), derived);
      final Set<PosixFilePermission> childPermissions = Files.getPosixFilePermissions(derived);
      Files.setPosixFilePermissions(derived, PosixFilePermissions.fromString("r-xr-xr-x"));
      try {
        assumeTrue(!Files.isWritable(derived), "running as a user who ignores permissions");
        if (retryHiddenCreation) {
          Files.setPosixFilePermissions(resources, PosixFilePermissions.fromString("r-xr-xr-x"));
          database.removeResource(VOLATILE);
          assertTrue(Files.exists(resourceDirectory(VOLATILE)
              .resolve(ResourceConfiguration.ResourcePaths.CONFIG_BINARY.getPath())));
          assertTrue(Files.exists(derived));
          assertFalse(database.existsResource(VOLATILE));
          Files.setPosixFilePermissions(resources, parentPermissions);
        } else {
          database.removeResource(VOLATILE);
          assertFalse(Files.exists(resourceDirectory(VOLATILE)));
        }
        createResource(database, VOLATILE, StorageType.IN_MEMORY);
        assertFalse(Files.exists(derived));
        try (ResourceSession<?, ?> session = database.beginResourceSession(VOLATILE)) {
          assertEquals(0, session.getMostRecentRevisionNumber());
          assertFalse(hasContent(session));
        }
      } finally {
        Files.setPosixFilePermissions(resources, parentPermissions);
        try (var paths = Files.list(resources)) {
          for (final Path path : paths.toList()) {
            if (Files.isDirectory(path)) {
              Files.setPosixFilePermissions(path, childPermissions);
            }
          }
        }
      }
    }
    try (Database<? extends ResourceSession<?, ?>> reopened = open(model, databasePath())) {
      assertTrue(resourceNames(reopened).isEmpty());
    }
    try (var paths = Files.list(resources)) {
      assertTrue(paths.toList().isEmpty());
    }
  }

  private Path resourceDirectory(final String name) {
    return databasePath().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(name);
  }

  private static void await(final CountDownLatch latch) {
    try {
      assertTrue(latch.await(60, TimeUnit.SECONDS), "the other thread did not arrive");
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
