/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access;

import io.sirix.access.InMemoryLifetimeFixture.Model;
import io.sirix.api.Database;
import io.sirix.api.ResourceSession;
import io.sirix.exception.SirixIOException;
import io.sirix.io.StorageType;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Set;
import java.util.UUID;
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
 * AC12 (failed cleanup) and AC15 (failed teardown). AC10 (abrupt end) and AC13 (memory release) run
 * in {@link InMemoryLifetimeForkedJvmTest}. Each test cites the clause it is derived from;
 * everything here is observed through the public API.
 */
final class DatabasesInMemoryLifetimeTest {

  private static final String VOLATILE = "volatile";

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

  static Stream<Arguments> cleanupRetryOwners() {
    return Stream.of(Model.values())
                 .flatMap(
                     model -> Stream.of("direct", "facade", "new-instance").map(owner -> Arguments.of(model, owner)));
  }

  /** TXN-615 clause 7: retry the ended instance's footprint without retiring a newer instance. */
  @ParameterizedTest
  @MethodSource("cleanupRetryOwners")
  void repeatedCloseFinishesOnlyTheEndedInstancesCleanup(final Model model, final String owner) throws IOException {
    assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"), "needs POSIX permissions");
    createDatabase(model, databasePath());
    final DatabaseConfiguration configuration;
    try (Database<? extends ResourceSession<?, ?>> initialized = open(model, databasePath())) {
      configuration = initialized.getDatabaseConfig();
    }
    final Database<? extends ResourceSession<?, ?>> database = owner.equals("direct")
        ? configuration.getDatabaseType().createDatabase(configuration, new User("admin", UUID.randomUUID()))
        : open(model, databasePath());
    createResource(database, VOLATILE, StorageType.IN_MEMORY);
    try (ResourceSession<?, ?> session = database.beginResourceSession(VOLATILE)) {
      commitFirstValue(session, model, "old");
    }
    final Path resources = databasePath().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile());
    final Path resource = resources.resolve(VOLATILE);
    final Set<PosixFilePermission> writable = Files.getPosixFilePermissions(resource);
    Files.setPosixFilePermissions(resource, PosixFilePermissions.fromString("r-xr-xr-x"));
    try {
      assumeTrue(!Files.isWritable(resource), "running as a user who ignores permissions");
      assertThrows(SirixIOException.class, database::close);
      final Path tombstone;
      try (var paths = Files.list(resources)) {
        tombstone = paths.toList().getFirst();
      }
      if (owner.equals("new-instance")) {
        try (Database<? extends ResourceSession<?, ?>> newer = open(model, databasePath())) {
          createResource(newer, VOLATILE, StorageType.IN_MEMORY);
          final long key;
          try (ResourceSession<?, ?> session = newer.beginResourceSession(VOLATILE)) {
            key = commitFirstValue(session, model, "new");
          }
          Files.setPosixFilePermissions(tombstone, writable);
          database.close();
          assertFalse(Files.exists(tombstone));
          try (ResourceSession<?, ?> session = newer.beginResourceSession(VOLATILE)) {
            assertEquals("new", valueAt(session, model, 1, key));
          }
        }
      } else {
        Files.setPosixFilePermissions(tombstone, writable);
        database.close();
        assertFalse(Files.exists(tombstone));
      }
      database.close();
    } finally {
      try (var paths = Files.list(resources)) {
        for (final Path path : paths.toList()) {
          if (Files.isDirectory(path)) {
            Files.setPosixFilePermissions(path, writable);
          }
        }
      }
      database.close();
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

  static Stream<Arguments> modelsAndPendingScanAdmissions() {
    return Stream.of(Model.values())
                 .flatMap(model -> Stream.of("exists", "list", "open", "create")
                                         .map(admission -> Arguments.of(model, admission)));
  }

  /**
   * TXN-615 clause 6: incomplete discovery blocks admission; successful discovery preserves
   * persistent state.
   */
  @ParameterizedTest
  @MethodSource("modelsAndPendingScanAdmissions")
  void pendingInitialScanBlocksAdmissionUntilDiscoverySucceeds(final Model model, final String admission)
      throws IOException {
    assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"), "needs POSIX permissions");
    createDatabase(model, databasePath());
    final String persistent = "persistent";
    final long key;
    try (Database<? extends ResourceSession<?, ?>> database = open(model, databasePath())) {
      createResource(database, persistent, StorageType.FILE_CHANNEL);
      try (ResourceSession<?, ?> session = database.beginResourceSession(persistent)) {
        key = commitFirstValue(session, model, "kept");
      }
    }
    final Path resources = databasePath().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile());
    final Set<PosixFilePermission> readable = Files.getPosixFilePermissions(resources);
    Files.setPosixFilePermissions(resources, PosixFilePermissions.fromString("--x--x--x"));
    try {
      assumeTrue(!Files.isReadable(resources), "running as a user who ignores permissions");
      try (Database<? extends ResourceSession<?, ?>> database = open(model, databasePath())) {
        try {
          switch (admission) {
            case "exists" -> assertThrows(SirixIOException.class, () -> database.existsResource(persistent));
            case "list" -> assertThrows(SirixIOException.class, database::listResources);
            case "open" -> assertThrows(SirixIOException.class, () -> database.beginResourceSession(persistent));
            case "create" -> assertThrows(SirixIOException.class, () -> database.createResource(
                ResourceConfiguration.newBuilder(VOLATILE).storageType(StorageType.IN_MEMORY).build()));
            default -> throw new IllegalArgumentException(admission);
          }
        } finally {
          Files.setPosixFilePermissions(resources, readable);
        }
        switch (admission) {
          case "exists" -> assertTrue(database.existsResource(persistent));
          case "list" -> assertEquals(Set.of(persistent), resourceNames(database));
          case "open" -> {
            try (ResourceSession<?, ?> session = database.beginResourceSession(persistent)) {
              assertEquals("kept", valueAt(session, model, 1, key));
            }
          }
          case "create" -> createResource(database, VOLATILE, StorageType.IN_MEMORY);
          default -> throw new IllegalArgumentException(admission);
        }
        try (ResourceSession<?, ?> session = database.beginResourceSession(persistent)) {
          assertEquals("kept", valueAt(session, model, 1, key), "discovery preserves committed persistent content");
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

  @ParameterizedTest
  @EnumSource(Model.class)
  void failedTraversalRemainsPendingUntilRepeatedClose(final Model model) throws IOException {
    assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"), "needs POSIX permissions");
    createDatabase(model, databasePath());
    final Path resources = databasePath().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile());
    final Database<? extends ResourceSession<?, ?>> database = open(model, databasePath());
    createResource(database, VOLATILE, StorageType.IN_MEMORY);
    try (ResourceSession<?, ?> session = database.beginResourceSession(VOLATILE)) {
      final long key = commitFirstValue(session, model, "old");
      assertEquals("old", valueAt(session, model, 1, key));
    }
    final Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(resources);
    Files.setPosixFilePermissions(resources, PosixFilePermissions.fromString("r--r--r--"));
    try {
      assumeTrue(!Files.isExecutable(resources), "running as a user who ignores permissions");
      assertFalse(Files.notExists(resources.resolve(VOLATILE)));
      assertThrows(SirixIOException.class, database::close);
      assertFalse(database.isOpen());
    } finally {
      Files.setPosixFilePermissions(resources, permissions);
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
    final Path denied = renameFails
        ? resources
        : resources.resolve(VOLATILE);
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

  private static void awaitBarrier(final CyclicBarrier barrier) {
    try {
      barrier.await(60, TimeUnit.SECONDS);
    } catch (final Exception e) {
      throw new IllegalStateException(e);
    }
  }

}
