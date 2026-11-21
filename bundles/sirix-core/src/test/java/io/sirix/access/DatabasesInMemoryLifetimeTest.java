/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access;

import io.brackit.query.atomic.QNm;
import io.sirix.access.InMemoryLifetimeFixture.Model;
import io.sirix.api.Database;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.SirixUsageException;
import io.sirix.io.StorageType;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.lang.reflect.Field;
import java.lang.ref.Reference;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
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
import static io.sirix.access.InMemoryLifetimeFixture.open;
import static io.sirix.access.InMemoryLifetimeFixture.resourceNames;
import static io.sirix.access.InMemoryLifetimeFixture.valueAt;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

/**
 * TXN-615 acceptance criteria on database instances: AC6 (alias paths), AC7 (close racing open),
 * AC10 (abrupt end, forked JVM), AC12 (failed cleanup), AC13 (memory released at close and at
 * removal, forked JVM) and AC15 (failed teardown). Each test cites the clause it is derived from;
 * everything is observed through the public API or a child process's result.
 */
final class DatabasesInMemoryLifetimeTest {

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

  /**
   * AC6, clause 2: every spelling of the database's directory reaches the same instance and state.
   */
  @ParameterizedTest
  @EnumSource(Model.class)
  void aliasPathsShareOneInstanceAndItsState(final Model model) throws IOException {
    createDatabase(model, databasePath());
    final Path dotted = directory.resolve("elsewhere").resolve("..").resolve("database");
    Files.createDirectories(directory.resolve("elsewhere"));
    final Path linked = directory.resolve("link");
    try {
      Files.createSymbolicLink(linked, databasePath());
    } catch (final UnsupportedOperationException | IOException unsupported) {
      assumeTrue(false, "symbolic links are not supported here: " + unsupported);
    }
    final long key;
    try (Database<? extends ResourceSession<?, ?>> viaDots = open(model, dotted);
        Database<? extends ResourceSession<?, ?>> viaLink = open(model, linked)) {
      createResource(viaDots, VOLATILE, StorageType.IN_MEMORY);
      try (ResourceSession<?, ?> session = viaDots.beginResourceSession(VOLATILE)) {
        key = commitFirstValue(session, model, "0");
      }
      try (ResourceSession<?, ?> session = viaLink.beginResourceSession(VOLATILE)) {
        assertEquals("0", valueAt(session, model, 1, key), "a commit through one spelling is seen through another");
      }
    }
    try (Database<? extends ResourceSession<?, ?>> reopened = open(model, databasePath())) {
      assertFalse(resourceNames(reopened).contains(VOLATILE), "closing every alias handle discards the state");
    }
  }

  /**
   * AC7, clause 4: an open racing the final close sees either the whole committed state or the empty
   * reopened database, never a partial state or an internal error. No internal hook: the two
   * operations simply start together, many times.
   */
  @ParameterizedTest
  @EnumSource(Model.class)
  void anOpenRacingTheFinalCloseNeverSeesAPartialState(final Model model) throws Exception {
    createDatabase(model, databasePath());
    final ExecutorService closer = Executors.newSingleThreadExecutor();
    final ExecutorService opener = Executors.newSingleThreadExecutor();
    int sawState = 0;
    int sawEmpty = 0;
    try {
      for (int round = 0; round < 60; round++) {
        final Database<? extends ResourceSession<?, ?>> database = open(model, databasePath());
        if (!database.existsResource(VOLATILE)) {
          createResource(database, VOLATILE, StorageType.IN_MEMORY);
        }
        final long first;
        final long second;
        try (ResourceSession<?, ?> session = database.beginResourceSession(VOLATILE)) {
          assertEquals(0, session.getMostRecentRevisionNumber(), "every round starts from a fresh resource");
          first = commitFirstValue(session, model, "a");
          second = commitNextValue(session, model, first, "b");
        }
        final CyclicBarrier start = new CyclicBarrier(2);
        final CompletableFuture<Void> close = CompletableFuture.runAsync(() -> {
          awaitBarrier(start);
          database.close();
        }, closer);
        final CompletableFuture<Boolean> observed = CompletableFuture.supplyAsync(() -> {
          awaitBarrier(start);
          try (Database<? extends ResourceSession<?, ?>> racing = open(model, databasePath())) {
            if (!resourceNames(racing).contains(VOLATILE)) {
              assertFalse(racing.existsResource(VOLATILE));
              return false;
            }
            try (ResourceSession<?, ?> session = racing.beginResourceSession(VOLATILE)) {
              assertEquals(2, session.getMostRecentRevisionNumber(), "a visible resource holds all its revisions");
              assertEquals("a", valueAt(session, model, 1, first));
              assertNull(valueAt(session, model, 1, second));
              assertEquals("b", valueAt(session, model, 2, second));
            }
            return true;
          }
        }, opener);
        close.get(60, TimeUnit.SECONDS);
        if (observed.get(60, TimeUnit.SECONDS)) {
          sawState++;
        } else {
          sawEmpty++;
        }
        // The racing handle was the last one either way: the state is gone now.
        try (Database<? extends ResourceSession<?, ?>> after = open(model, databasePath())) {
          assertFalse(resourceNames(after).contains(VOLATILE));
        }
      }
    } finally {
      closer.shutdownNow();
      opener.shutdownNow();
    }
    assertEquals(60, sawState + sawEmpty);
  }

  /**
   * AC12, clause 7: a final close whose cleanup fails still discards the state and ends the instance;
   * a later open gets a new instance, and the name becomes reusable once cleanup can complete. Needs
   * POSIX permissions and a non-root user.
   */
  @ParameterizedTest
  @EnumSource(Model.class)
  void aFailedCleanupStillEndsTheInstanceAndIsRetried(final Model model) throws IOException {
    assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"), "needs POSIX permissions");
    createDatabase(model, databasePath());
    final Path resources = databasePath().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile());
    final Set<PosixFilePermission> writable = Files.getPosixFilePermissions(resources);
    final Database<? extends ResourceSession<?, ?>> database = open(model, databasePath());
    createResource(database, VOLATILE, StorageType.IN_MEMORY);
    final long key;
    try (ResourceSession<?, ?> session = database.beginResourceSession(VOLATILE)) {
      key = commitFirstValue(session, model, "0");
    }
    Files.setPosixFilePermissions(resources, PosixFilePermissions.fromString("r-xr-xr-x"));
    try {
      assumeTrue(!Files.isWritable(resources), "running as a user who ignores permissions");
      assertThrows(SirixIOException.class, database::close, "close reports the failed cleanup");
      assertFalse(database.isOpen(), "a failure removing leftovers still ends the instance");

      try (Database<? extends ResourceSession<?, ?>> reopened = open(model, databasePath())) {
        // A rejoined failed instance would refuse every call as closed.
        assertFalse(resourceNames(reopened).contains(VOLATILE), "the leftover is treated as absent");
        assertFalse(reopened.existsResource(VOLATILE));
        assertFalse(
            reopened.createResource(
                ResourceConfiguration.newBuilder(VOLATILE).storageType(StorageType.IN_MEMORY).build()),
            "the name is not reusable while its cleanup still cannot complete");
        Files.setPosixFilePermissions(resources, writable);
        createResource(reopened, VOLATILE, StorageType.IN_MEMORY);
        try (ResourceSession<?, ?> session = reopened.beginResourceSession(VOLATILE)) {
          assertEquals(0, session.getMostRecentRevisionNumber(), "the committed state did not survive the failure");
          assertNull(valueAt(session, model, 0, key));
        }
      }
    } finally {
      Files.setPosixFilePermissions(resources, writable);
    }
  }

  @ParameterizedTest
  @EnumSource(Model.class)
  void failedTombstoneDeletionStillEndsTheInstance(final Model model) throws IOException {
    assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"), "needs POSIX permissions");
    createDatabase(model, databasePath());
    final Path resources = databasePath().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile());
    final Path resource = resources.resolve(VOLATILE);
    final Database<? extends ResourceSession<?, ?>> database = open(model, databasePath());
    createResource(database, VOLATILE, StorageType.IN_MEMORY);
    try (ResourceSession<?, ?> session = database.beginResourceSession(VOLATILE)) {
      commitFirstValue(session, model, "0");
    }
    final Set<PosixFilePermission> writable = Files.getPosixFilePermissions(resource);
    Files.setPosixFilePermissions(resource, PosixFilePermissions.fromString("r-xr-xr-x"));
    try {
      assumeTrue(!Files.isWritable(resource), "running as a user who ignores permissions");
      assertTrue(Files.isWritable(resources));
      assertThrows(SirixIOException.class, database::close);
      assertFalse(database.isOpen());
      assertFalse(Files.exists(resource));
      final Path tombstone;
      try (var paths = Files.list(resources)) {
        final List<Path> remaining = paths.toList();
        assertEquals(1, remaining.size());
        tombstone = remaining.getFirst();
      }
      try (Database<? extends ResourceSession<?, ?>> reopened = open(model, databasePath())) {
        try {
          assertFalse(reopened.existsResource(VOLATILE));
          assertTrue(resourceNames(reopened).isEmpty());
          createResource(reopened, VOLATILE, StorageType.IN_MEMORY);
          try (ResourceSession<?, ?> session = reopened.beginResourceSession(VOLATILE)) {
            assertEquals(0, session.getMostRecentRevisionNumber());
            assertFalse(hasContent(session));
          }
        } finally {
          Files.setPosixFilePermissions(tombstone, writable);
        }
      }
      try (var paths = Files.list(resources)) {
        assertTrue(paths.toList().isEmpty());
      }
    } finally {
      try (var paths = Files.list(resources)) {
        for (final Path path : paths.toList()) {
          if (Files.isDirectory(path)) {
            Files.setPosixFilePermissions(path, writable);
          }
        }
      }
      if (database.isOpen()) {
        database.close();
      }
    }
  }

  @ParameterizedTest
  @EnumSource(Model.class)
  void failedForcedRemovalReleasesAdmission(final Model model) throws IOException {
    assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"), "needs POSIX permissions");
    createDatabase(model, databasePath());
    final Path resources = databasePath().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile());
    final Database<? extends ResourceSession<?, ?>> database = open(model, databasePath());
    createResource(database, VOLATILE, StorageType.IN_MEMORY);
    try (ResourceSession<?, ?> session = database.beginResourceSession(VOLATILE)) {
      commitFirstValue(session, model, "0");
    }
    final Path resource = resources.resolve(VOLATILE);
    final Set<PosixFilePermission> writable = Files.getPosixFilePermissions(resource);
    Files.setPosixFilePermissions(resource, PosixFilePermissions.fromString("r-xr-xr-x"));
    try {
      assumeTrue(!Files.isWritable(resource), "running as a user who ignores permissions");
      assertThrows(SirixIOException.class, () -> Databases.removeDatabase(databasePath()));
      assertFalse(database.isOpen());
    } finally {
      try (var paths = Files.list(resources)) {
        for (final Path path : paths.toList()) {
          if (Files.isDirectory(path)) {
            Files.setPosixFilePermissions(path, writable);
          }
        }
      }
      if (database.isOpen()) {
        database.close();
      }
    }
    try (Database<? extends ResourceSession<?, ?>> reopened = open(model, databasePath())) {
      assertTrue(resourceNames(reopened).isEmpty());
    }
    Databases.removeDatabase(databasePath());
    createDatabase(model, databasePath());
    try (Database<? extends ResourceSession<?, ?>> recreated = open(model, databasePath())) {
      assertTrue(resourceNames(recreated).isEmpty());
    }
  }

  static Stream<Arguments> modelsAndResourceAdmissions() {
    return Stream.of(Model.values()).flatMap(model -> Stream.of("exists", "list", "open", "create")
                                                           .map(admission -> Arguments.of(model, admission)));
  }

  @ParameterizedTest
  @MethodSource("modelsAndResourceAdmissions")
  void failedInitialScanIsRetriedBeforeResourceAdmission(final Model model, final String admission) throws Exception {
    assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"), "needs POSIX permissions");
    final Process process = child("-Xmx256m", AbruptExitChild.class, databasePath().toString(), model.name());
    try {
      assertEquals("HALTING", childResult(process));
      assertTrue(process.waitFor(60, TimeUnit.SECONDS));
      assertEquals(0, process.exitValue());
    } finally {
      process.destroyForcibly();
    }
    final Path resources = databasePath().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile());
    final Set<PosixFilePermission> readable = Files.getPosixFilePermissions(resources);
    Files.setPosixFilePermissions(resources, PosixFilePermissions.fromString("--x--x--x"));
    try {
      assumeTrue(!Files.isReadable(resources), "running as a user who ignores permissions");
      try (Database<? extends ResourceSession<?, ?>> database = open(model, databasePath())) {
        try {
          assertThrows(SirixIOException.class, () -> database.existsResource(VOLATILE));
        } finally {
          Files.setPosixFilePermissions(resources, readable);
        }
        switch (admission) {
          case "exists" -> assertFalse(database.existsResource(VOLATILE));
          case "list" -> assertEquals(Set.of(DURABLE), resourceNames(database));
          case "open" -> assertThrows(SirixUsageException.class, () -> database.beginResourceSession(VOLATILE));
          case "create" -> createResource(database, VOLATILE, StorageType.IN_MEMORY);
          default -> throw new IllegalArgumentException(admission);
        }
        if (!admission.equals("create")) {
          createResource(database, VOLATILE, StorageType.IN_MEMORY);
        }
        try (ResourceSession<?, ?> session = database.beginResourceSession(VOLATILE)) {
          assertEquals(0, session.getMostRecentRevisionNumber());
          assertFalse(hasContent(session));
          final long key = commitFirstValue(session, model, "fresh");
          assertEquals("fresh", valueAt(session, model, 1, key));
        }
        assertTrue(database.existsResource(VOLATILE));
        assertEquals(Set.of(DURABLE, VOLATILE), resourceNames(database));
        try (ResourceSession<?, ?> session = database.beginResourceSession(DURABLE)) {
          assertEquals(2, session.getMostRecentRevisionNumber());
          assertTrue(hasContent(session));
        }
      }
    } finally {
      Files.setPosixFilePermissions(resources, readable);
    }
  }

  @ParameterizedTest
  @EnumSource(Model.class)
  void finalCloseRetainsKnownCleanupWhenDirectoryScanningFails(final Model model) throws IOException {
    assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"), "needs POSIX permissions");
    createDatabase(model, databasePath());
    final Path resources = databasePath().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile());
    final Database<? extends ResourceSession<?, ?>> database = open(model, databasePath());
    createResource(database, VOLATILE, StorageType.IN_MEMORY);
    try (ResourceSession<?, ?> session = database.beginResourceSession(VOLATILE)) {
      final long key = commitFirstValue(session, model, "old");
      assertEquals("old", valueAt(session, model, 1, key));
    }
    final Set<PosixFilePermission> readable = Files.getPosixFilePermissions(resources);
    Files.setPosixFilePermissions(resources, PosixFilePermissions.fromString("--x--x--x"));
    try {
      assumeTrue(!Files.isReadable(resources), "running as a user who ignores permissions");
      assertThrows(SirixIOException.class, database::close);
      assertFalse(database.isOpen());
    } finally {
      Files.setPosixFilePermissions(resources, readable);
      if (database.isOpen()) {
        database.close();
      }
    }
    assertDoesNotThrow(database::close);
    try (var paths = Files.list(resources)) {
      assertTrue(paths.toList().isEmpty());
    }
  }

  static Stream<Arguments> modelsAndRenameFailures() {
    return Stream.of(Model.values()).flatMap(model -> Stream.of(Arguments.of(model, false), Arguments.of(model, true)));
  }

  @ParameterizedTest
  @MethodSource("modelsAndRenameFailures")
  void repeatedCloseRetriesOnlyRemainingCleanup(final Model model, final boolean renameFails) throws IOException {
    assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"), "needs POSIX permissions");
    createDatabase(model, databasePath());
    final Path resources = databasePath().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile());
    final Database<? extends ResourceSession<?, ?>> database = open(model, databasePath());
    createResource(database, VOLATILE, StorageType.IN_MEMORY);
    try (ResourceSession<?, ?> session = database.beginResourceSession(VOLATILE)) {
      commitFirstValue(session, model, "old");
    }
    final Path denied = renameFails ? resources : resources.resolve(VOLATILE);
    final Set<PosixFilePermission> writable = Files.getPosixFilePermissions(denied);
    Files.setPosixFilePermissions(denied, PosixFilePermissions.fromString("r-xr-xr-x"));
    try {
      assumeTrue(!Files.isWritable(denied), "running as a user who ignores permissions");
      assertThrows(SirixIOException.class, database::close);
      assertFalse(database.isOpen());
      assertThrows(SirixIOException.class, database::close);
    } finally {
      Files.setPosixFilePermissions(resources, writable);
      try (var paths = Files.list(resources)) {
        for (final Path path : paths.toList()) {
          if (Files.isDirectory(path)) {
            Files.setPosixFilePermissions(path, writable);
          }
        }
      }
      if (database.isOpen()) {
        database.close();
      }
    }
    assertDoesNotThrow(database::close);
    try (var paths = Files.list(resources)) {
      assertTrue(paths.toList().isEmpty());
    }
  }

  @ParameterizedTest
  @EnumSource(Model.class)
  void oldClosePreservesANewerResource(final Model model) throws IOException {
    assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"), "needs POSIX permissions");
    createDatabase(model, databasePath());
    final Path resources = databasePath().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile());
    final Database<? extends ResourceSession<?, ?>> old = open(model, databasePath());
    createResource(old, VOLATILE, StorageType.IN_MEMORY);
    try (ResourceSession<?, ?> session = old.beginResourceSession(VOLATILE)) {
      commitFirstValue(session, model, "old");
    }
    final Set<PosixFilePermission> writable = Files.getPosixFilePermissions(resources);
    Files.setPosixFilePermissions(resources, PosixFilePermissions.fromString("r-xr-xr-x"));
    try {
      assumeTrue(!Files.isWritable(resources), "running as a user who ignores permissions");
      assertThrows(SirixIOException.class, old::close);
    } finally {
      Files.setPosixFilePermissions(resources, writable);
      if (old.isOpen()) {
        old.close();
      }
    }
    try (Database<? extends ResourceSession<?, ?>> current = open(model, databasePath())) {
      createResource(current, VOLATILE, StorageType.IN_MEMORY);
      try (ResourceSession<?, ?> session = current.beginResourceSession(VOLATILE)) {
        final long key = commitFirstValue(session, model, "new");
        assertDoesNotThrow(old::close);
        assertTrue(current.existsResource(VOLATILE));
        assertEquals("new", valueAt(session, model, 1, key));
      }
    }
    assertDoesNotThrow(old::close);
  }

  @ParameterizedTest
  @EnumSource(Model.class)
  void closedSessionsDoNotRetainCommittedHistories(final Model model) throws Exception {
    for (final String mode : new String[] {"close-retain-sessions", "remove-retain-sessions"}) {
      final Process process = child("-Xmx160m", MemoryChild.class, directory.resolve(mode).toString(), model.name(), mode);
      try {
        assertEquals("COMPLETED", childResult(process));
        assertTrue(process.waitFor(300, TimeUnit.SECONDS));
        assertEquals(0, process.exitValue());
      } finally {
        process.destroyForcibly();
      }
    }
  }

  /**
   * AC15, clause 7: when session teardown fails, the close throws and discards nothing. The instance
   * stays open, owned and complete, an open rejoins it with every committed revision, and a retry
   * finishes the close. The failure is injected at the teardown boundary; everything asserted is
   * public behaviour.
   */
  @ParameterizedTest
  @EnumSource(Model.class)
  void aFailedTeardownKeepsTheCompleteStateUntilARetryFinishes(final Model model) throws Exception {
    createDatabase(model, databasePath());
    final Database<? extends ResourceSession<?, ?>> first = open(model, databasePath());
    createResource(first, VOLATILE, StorageType.IN_MEMORY);
    final long key;
    try (ResourceSession<?, ?> session = first.beginResourceSession(VOLATILE)) {
      key = commitFirstValue(session, model, "0");
    }
    final Field ownerField = DatabaseHandle.class.getDeclaredField("owner");
    ownerField.setAccessible(true);
    final Databases.OpenDatabase<?> owner = (Databases.OpenDatabase<?>) ownerField.get(first);
    final Field storeField = LocalDatabase.class.getDeclaredField("resourceStore");
    storeField.setAccessible(true);
    final ResourceStore<?> originalStore = (ResourceStore<?>) storeField.get(owner.localDatabase);
    final ResourceStore<?> failingOnce = spy(originalStore);
    doThrow(new IllegalStateException("injected teardown failure")).doCallRealMethod().when(failingOnce).close();
    storeField.set(owner.localDatabase, failingOnce);
    try {
      assertThrows(IllegalStateException.class, first::close, "the teardown failure is reported");
      assertTrue(first.isOpen(), "the instance stays open for a retry");
      try (Database<? extends ResourceSession<?, ?>> rejoined = open(model, databasePath());
          ResourceSession<?, ?> session = rejoined.beginResourceSession(VOLATILE)) {
        assertEquals(1, session.getMostRecentRevisionNumber(), "nothing was discarded");
        assertEquals("0", valueAt(session, model, 1, key));
      }
      first.close();
      assertFalse(first.isOpen(), "the retry finishes the close");
    } finally {
      storeField.set(owner.localDatabase, originalStore);
    }
    try (Database<? extends ResourceSession<?, ?>> reopened = open(model, databasePath())) {
      assertFalse(resourceNames(reopened).contains(VOLATILE), "the finished close discarded the state");
    }
  }

  /**
   * AC10, clause 6: a process that ends without closing leaves nothing a later process exposes as an
   * IN_MEMORY resource, and the persistent neighbour keeps its revisions and files.
   */
  @ParameterizedTest
  @EnumSource(Model.class)
  void aFreshProcessAfterAnAbruptExitSeesOnlyThePersistentResource(final Model model) throws Exception {
    final Process child = child("-Xmx256m", AbruptExitChild.class, databasePath().toString(), model.name());
    try {
      assertEquals("HALTING", childResult(child));
      assertTrue(child.waitFor(60, TimeUnit.SECONDS));
    } finally {
      child.destroyForcibly();
    }
    final Path durableFiles = databasePath().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile())
                                            .resolve(DURABLE)
                                            .resolve(ResourceConfiguration.ResourcePaths.DATA.getPath());
    final long durableBytes = directorySize(durableFiles);
    assertTrue(durableBytes > 0, "the child committed the persistent resource");
    try (Database<? extends ResourceSession<?, ?>> reopened = open(model, databasePath())) {
      assertEquals(Set.of(DURABLE), resourceNames(reopened));
      assertFalse(reopened.existsResource(VOLATILE));
      try (ResourceSession<?, ?> durable = reopened.beginResourceSession(DURABLE)) {
        assertEquals(2, durable.getMostRecentRevisionNumber());
        assertTrue(hasContent(durable));
      }
      createResource(reopened, VOLATILE, StorageType.IN_MEMORY);
      try (ResourceSession<?, ?> recreated = reopened.beginResourceSession(VOLATILE)) {
        assertEquals(0, recreated.getMostRecentRevisionNumber());
      }
    }
    assertEquals(durableBytes, directorySize(durableFiles), "the persistent resource's files are untouched");
  }

  /**
   * AC13, clause 3: the final close releases the IN_MEMORY state's memory. A child with a small heap
   * fills and closes many databases; a control child with the same heap that keeps them open must run
   * out of memory, which proves the heap bound bites.
   */
  @ParameterizedTest
  @EnumSource(Model.class)
  void finalCloseReleasesTheMemoryOfTheInMemoryState(final Model model) throws Exception {
    // ExitOnOutOfMemoryError: an exhausted heap in any thread ends the child with exit code 3.
    final Process releasing =
        child("-Xmx160m", MemoryChild.class, directory.resolve("release").toString(), model.name(), "close");
    try {
      assertEquals("COMPLETED", childResult(releasing), "closing each database releases its state");
      assertTrue(releasing.waitFor(300, TimeUnit.SECONDS));
      assertEquals(0, releasing.exitValue());
    } finally {
      releasing.destroyForcibly();
    }
    final Process retaining =
        child("-Xmx160m", MemoryChild.class, directory.resolve("retain").toString(), model.name(), "keep-open");
    try {
      assertTrue(retaining.waitFor(300, TimeUnit.SECONDS));
      assertEquals(OUT_OF_MEMORY_EXIT, retaining.exitValue(),
          "control: retaining every database must exhaust the same heap");
    } finally {
      retaining.destroyForcibly();
    }
  }

  /**
   * AC13, clause 8: removing an IN_MEMORY resource from an open database discards its state at once.
   * The child keeps one database open and creates, fills and removes many resources in it, under the
   * heap the control of {@link #finalCloseReleasesTheMemoryOfTheInMemoryState} exhausts by retaining
   * the same amount of state.
   */
  @ParameterizedTest
  @EnumSource(Model.class)
  void removalReleasesTheMemoryOfTheRemovedResource(final Model model) throws Exception {
    final Process removing =
        child("-Xmx160m", MemoryChild.class, directory.resolve("remove").toString(), model.name(), "remove");
    try {
      assertEquals("COMPLETED", childResult(removing), "removing each resource releases its state");
      assertTrue(removing.waitFor(300, TimeUnit.SECONDS));
      assertEquals(0, removing.exitValue());
    } finally {
      removing.destroyForcibly();
    }
  }

  /** The JVM's exit code under -XX:+ExitOnOutOfMemoryError. */
  private static final int OUT_OF_MEMORY_EXIT = 3;

  private Process child(final String heap, final Class<?> main, final String... arguments) throws IOException {
    final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    final List<String> command = new ArrayList<>(List.of(java, heap, "-XX:+ExitOnOutOfMemoryError", "--enable-preview",
        "--enable-native-access=ALL-UNNAMED", "--add-modules", "jdk.incubator.vector", "-Dsirix.allocator.maxSize=64M",
        "-Djava.io.tmpdir=" + directory, "-cp", System.getProperty("java.class.path"), main.getName()));
    command.addAll(List.of(arguments));
    return new ProcessBuilder(command).redirectError(directory.resolve("child-" + System.nanoTime() + ".log").toFile())
                                      .start();
  }

  private static String childResult(final Process process) throws Exception {
    // The bound detects a broken child, not database latency.
    return CompletableFuture.supplyAsync(() -> {
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
        String line;
        String last = "child exited without a result";
        while ((line = reader.readLine()) != null) {
          last = line;
          if (line.equals("HALTING") || line.equals("COMPLETED")) {
            return line;
          }
        }
        return last;
      } catch (final IOException e) {
        throw new IllegalStateException(e);
      }
    }).get(300, TimeUnit.SECONDS);
  }

  private static long directorySize(final Path path) throws IOException {
    try (var files = Files.walk(path)) {
      long size = 0;
      for (final Path file : files.toList()) {
        if (Files.isRegularFile(file)) {
          size += Files.size(file);
        }
      }
      return size;
    }
  }

  private static void awaitBarrier(final CyclicBarrier barrier) {
    try {
      barrier.await(60, TimeUnit.SECONDS);
    } catch (final Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /** Child for AC10: commit to an IN_MEMORY and a persistent resource, then halt without closing. */
  static final class AbruptExitChild {
    public static void main(final String[] arguments) {
      final Path path = Path.of(arguments[0]);
      final Model model = Model.valueOf(arguments[1]);
      createDatabase(model, path);
      final Database<? extends ResourceSession<?, ?>> database = open(model, path);
      createResource(database, DURABLE, StorageType.FILE_CHANNEL);
      createResource(database, VOLATILE, StorageType.IN_MEMORY);
      try (ResourceSession<?, ?> durable = database.beginResourceSession(DURABLE);
          ResourceSession<?, ?> volatileSession = database.beginResourceSession(VOLATILE)) {
        final long first = commitFirstValue(durable, model, "d0");
        commitNextValue(durable, model, first, "d1");
        commitFirstValue(volatileSession, model, "v0");
      }
      System.out.println("HALTING");
      System.out.flush();
      // An abrupt end: no close, no shutdown hooks.
      Runtime.getRuntime().halt(0);
    }
  }

  /**
   * Child for AC13: fill many IN_MEMORY databases, closing each ({@code close}) or keeping all open
   * ({@code keep-open}); or fill as many IN_MEMORY resources in one open database, removing each
   * ({@code remove}).
   */
  static final class MemoryChild {
    /** Committed per database; together the databases need several times the child's heap. */
    private static final int VALUES = 512;

    private static final int VALUE_CHARS = 16 * 1024;

    private static final int DATABASES = 40;

    public static void main(final String[] arguments) {
      final Path root = Path.of(arguments[0]);
      final Model model = Model.valueOf(arguments[1]);
      final String mode = arguments[2];
      final boolean close = mode.equals("close") || mode.equals("close-retain-sessions");
      final boolean remove = mode.equals("remove") || mode.equals("remove-retain-sessions");
      final boolean retainSessions = mode.endsWith("-retain-sessions");
      final Database<? extends ResourceSession<?, ?>> shared;
      if (remove) {
        createDatabase(model, root.resolve("database"));
        shared = open(model, root.resolve("database"));
      } else {
        shared = null;
      }
      // Kept reachable on purpose in the control mode.
      final List<Database<? extends ResourceSession<?, ?>>> kept = new ArrayList<>();
      final List<ResourceSession<?, ?>> closedSessions = new ArrayList<>();
      for (int i = 0; i < DATABASES; i++) {
        final Database<? extends ResourceSession<?, ?>> database;
        final String name;
        if (shared != null) {
          database = shared;
          name = VOLATILE + "-" + i;
        } else {
          final Path path = root.resolve("database-" + i);
          createDatabase(model, path);
          database = open(model, path);
          name = VOLATILE;
        }
        createResource(database, name, StorageType.IN_MEMORY);
        try (ResourceSession<?, ?> session = database.beginResourceSession(name);
            NodeTrx writer = session.beginNodeTrx()) {
          fill(writer, model, new SplittableRandom(i));
          writer.commit();
          if (retainSessions) {
            closedSessions.add(session);
          }
        }
        if (remove) {
          database.removeResource(name);
        } else if (close) {
          database.close();
        } else {
          kept.add(database);
        }
      }
      Reference.reachabilityFence(closedSessions);
      System.out.println("COMPLETED");
      System.out.flush();
      Runtime.getRuntime().halt(0);
    }

    private static void fill(final NodeTrx writer, final Model model, final SplittableRandom random) {
      if (model == Model.JSON) {
        final JsonNodeTrx json = (JsonNodeTrx) writer;
        json.insertArrayAsFirstChild();
        json.insertStringValueAsFirstChild(incompressible(random));
        for (int i = 1; i < VALUES; i++) {
          json.insertStringValueAsRightSibling(incompressible(random));
        }
      } else {
        final XmlNodeTrx xml = (XmlNodeTrx) writer;
        xml.insertElementAsFirstChild(new QNm("root"));
        xml.insertElementAsFirstChild(new QNm("item"));
        xml.insertTextAsFirstChild(incompressible(random));
        for (int i = 1; i < VALUES; i++) {
          xml.moveToParent();
          xml.insertElementAsRightSibling(new QNm("item"));
          xml.insertTextAsFirstChild(incompressible(random));
        }
      }
    }

    /** Random letters and digits: no page encoding or compressor can shrink the state much. */
    private static String incompressible(final SplittableRandom random) {
      final char[] chars = new char[VALUE_CHARS];
      for (int i = 0; i < chars.length; i++) {
        chars[i] = ALPHABET.charAt(random.nextInt(ALPHABET.length()));
      }
      return new String(chars);
    }

    private static final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  }
}
