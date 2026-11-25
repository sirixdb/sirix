package io.sirix.backup;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.SirixUsageException;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.aggregate.DerivedIndexMaintainer;
import io.sirix.index.aggregate.DerivedIndexTestSupport;
import io.sirix.index.aggregate.TemporalAggregateDeclarations;
import io.sirix.io.StorageType;
import io.sirix.io.InterruptedFirstCommitRecovery;
import io.sirix.io.IOStorage;
import io.sirix.io.Reader;
import io.sirix.page.RevisionRootPage;
import io.sirix.utils.SirixFiles;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TXN-613: a backup copies the hidden {@code $derived} resources that hold temporal-aggregate data,
 * a restore verifies and reports them, and a backup that cannot quiesce derived maintenance is
 * refused instead of copying a store that is being written.
 */
final class BackupDerivedResourceTest {

  private static final String RESOURCE = "records";
  private static final String DERIVED = RESOURCE + DerivedIndexMaintainer.SUFFIX;
  private static final int REVISIONS = 4;

  @TempDir
  Path tempDir;

  private Path sourceDb;

  @BeforeEach
  void createSourceDatabaseWithTemporalAggregate() {
    sourceDb = tempDir.resolve("source-db");
    Databases.createJsonDatabase(new DatabaseConfiguration(sourceDb));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(sourceDb)) {
      createTemporalResource(database, RESOURCE);
    }
  }

  private static void createTemporalResource(final Database<JsonResourceSession> database, final String resourceName) {
    database.createResource(ResourceConfiguration.newBuilder(resourceName)
                                                 .storageType(StorageType.FILE_CHANNEL)
                                                 .validTimePaths("lo", "hi")
                                                 .customCommitTimestamps(true)
                                                 .storeDiffs(false)
                                                 .build());
    try (final JsonResourceSession session = database.beginResourceSession(resourceName);
        final JsonNodeTrx writer = session.beginNodeTrx()) {
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
          [{"g":1,"n":10,"lo":"2024-01-01T00:00:00Z","hi":"2025-01-01T00:00:00Z"}]
          """), JsonNodeTrx.Commit.NO);
      final IndexDef projection = IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
          List.of(parse("/[]/g", PathParser.Type.JSON), parse("/[]/n", PathParser.Type.JSON),
              parse("/[]/lo", PathParser.Type.JSON), parse("/[]/hi", PathParser.Type.JSON)),
          List.of(Type.LON, Type.LON, Type.STR, Type.STR), 0, IndexDef.DbType.JSON);
      session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(projection), writer);
      TemporalAggregateDeclarations.declare(session.getWtxIndexController(writer.getRevisionNumber()), writer, "/[]",
          List.of("g"), List.of("sum(n)"));
      writer.commit("first", Instant.parse("2024-01-01T00:00:00Z"));
      writer.moveToDocumentRoot();
      writer.moveToFirstChild();
      writer.moveToFirstChild();
      writer.moveToFirstChild();
      writer.moveToRightSibling();
      writer.moveToFirstChild();
      for (int revision = 2; revision <= REVISIONS; revision++) {
        writer.setNumberValue(revision * 10);
        writer.commit("revision " + revision, Instant.parse("2024-0" + revision + "-01T00:00:00Z"));
      }
      assertEquals(REVISIONS, session.getMostRecentRevisionNumber(), "test setup: source revisions");
      DerivedIndexTestSupport.await(session);
    }
  }

  @Test
  @DisplayName("TXN-613: the backup copies the hidden derived resource and the restore verifies and reports it")
  void backupCopiesAndRestoreVerifiesTheDerivedResource() {
    final Path backupDir = tempDir.resolve("backup");
    final Path restoredDir = tempDir.resolve("restored-db");

    final BackupManager.BackupSummary backup = BackupManager.backupDatabase(sourceDb, backupDir);
    assertEquals(1, backup.resourcesCopied(), "a hidden derived resource is not a visible resource");
    final BackupManager.ResourceSummary derivedBackup = onlyDerived(backup);
    assertEquals(DERIVED, derivedBackup.resourceName());
    assertTrue(derivedBackup.mostRecentRevision() >= 1, "the derived store must carry published revisions");
    assertTrue(derivedBackup.bytesCopied() > 0, "the derived resource must contribute copied bytes");
    assertEquals(1, backup.derivedResourcesCopied());
    assertEquals(backup.resources().getFirst().bytesCopied() + derivedBackup.bytesCopied(),
        backup.totalBytesCopied() - databaseLevelBytes(backupDir),
        "the derived bytes must be part of the reported total");
    assertTrue(Files.isRegularFile(derivedFile(backupDir, "sirix.data")), "the backup needs the derived page store");
    assertTrue(Files.isRegularFile(derivedFile(backupDir, "sirix.revisions")),
        "the backup needs the derived revision index");
    assertTrue(Files.isRegularFile(derivedDir(backupDir).resolve("ressetting.obj")),
        "the backup needs the derived settings");
    assertFalse(Files.exists(derivedDir(backupDir).resolve("log").resolve(".commit")),
        "the in-flight commit marker of the derived store must not be copied");

    final BackupManager.BackupSummary restore = BackupManager.restoreDatabase(backupDir, restoredDir);
    final BackupManager.ResourceSummary derivedRestore = onlyDerived(restore);
    assertEquals(DERIVED, derivedRestore.resourceName(), "the restore must report the derived resource it restored");
    // The restore verifies the derived store before any source session can install a maintainer, so
    // this revision is the copied state itself and not one a rebuild could have produced.
    assertEquals(derivedBackup.mostRecentRevision(), derivedRestore.mostRecentRevision(),
        "the restored derived store opens at the backed-up revision");
    assertTrue(derivedRestore.bytesCopied() > 0, "the restore must report the restored derived bytes");
  }

  @Test
  @DisplayName("TXN-613: restoring a backup whose derived resource was truncated fails and rolls the target back")
  void restoreRefusesATruncatedDerivedResource() throws IOException {
    final Path backupDir = tempDir.resolve("backup");
    BackupManager.backupDatabase(sourceDb, backupDir);

    final Path derivedData = derivedFile(backupDir, "sirix.data");
    assertTrue(Files.size(derivedData) > 12288L, "test setup: the derived store holds committed pages");
    try (final FileChannel channel = FileChannel.open(derivedData, StandardOpenOption.WRITE)) {
      channel.truncate(12288L);
    }

    final Path restoredDir = tempDir.resolve("restored-db");
    assertThrows(SirixIOException.class, () -> BackupManager.restoreDatabase(backupDir, restoredDir),
        "a truncated derived resource must fail restore verification");
    assertFalse(Files.exists(restoredDir), "a failed restore must leave no partial target");
  }

  @Test
  @DisplayName("TXN-613: zeroed derived beacon slots cannot masquerade as a fresh revision-0 store")
  void restoreRefusesDerivedStoresWithZeroedBeacons() throws IOException {
    final Path backupDir = tempDir.resolve("backup");
    final BackupManager.BackupSummary backup = BackupManager.backupDatabase(sourceDb, backupDir);
    assertTrue(onlyDerived(backup).mostRecentRevision() > 0);
    final Path dataFile = derivedFile(backupDir, "sirix.data");
    final byte[] originalData = Files.readAllBytes(dataFile);
    final byte[] originalRevisions = Files.readAllBytes(derivedFile(backupDir, "sirix.revisions"));
    assertTrue(originalData.length > IOStorage.DATA_REGION_START);
    try (final FileChannel channel = FileChannel.open(dataFile, StandardOpenOption.WRITE)) {
      final ByteBuffer zeros = ByteBuffer.allocate((int) (IOStorage.DATA_REGION_START - IOStorage.PRIMARY_BEACON_OFFSET));
      while (zeros.hasRemaining()) {
        channel.write(zeros, IOStorage.PRIMARY_BEACON_OFFSET + zeros.position());
      }
    }
    final byte[] damagedData = Files.readAllBytes(dataFile);
    assertArrayEquals(Arrays.copyOfRange(originalData, 0, (int) IOStorage.PRIMARY_BEACON_OFFSET),
        Arrays.copyOfRange(damagedData, 0, (int) IOStorage.PRIMARY_BEACON_OFFSET), "the superblock stays intact");
    assertArrayEquals(Arrays.copyOfRange(originalData, (int) IOStorage.DATA_REGION_START, originalData.length),
        Arrays.copyOfRange(damagedData, (int) IOStorage.DATA_REGION_START, damagedData.length),
        "all committed pages, including revision 0, stay intact");
    assertArrayEquals(originalRevisions, Files.readAllBytes(derivedFile(backupDir, "sirix.revisions")),
        "the revision records stay intact");
    final Path restoredDir = tempDir.resolve("restored-db");
    final SirixIOException refusal = assertThrows(SirixIOException.class,
        () -> BackupManager.restoreDatabase(backupDir, restoredDir));
    assertTrue(refusal.getMessage().contains(DERIVED));
    assertFalse(Files.exists(restoredDir), "a missing committed beacon must leave no restored target");
  }

  @Test
  @DisplayName("TXN-613: a backup that cannot quiesce derived maintenance is refused, never silently partial")
  void backupRefusesWhileDerivedMaintenanceCannotQuiesce() throws Exception {
    final Path backupDir = tempDir.resolve("backup");
    final CountDownLatch held = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(sourceDb);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      final DerivedIndexMaintainer.Resource resource =
          Objects.requireNonNull(((AbstractResourceSession<?, ?>) session).getDerivedResource(),
              "test setup: the derived resource is installed");
      assertThrows(IllegalArgumentException.class, () -> resource.tryLockHistory(Duration.ofSeconds(-1)),
          "a negative quiescence timeout is a caller error");
      assertThrows(IllegalArgumentException.class, () -> resource.tryLockHistory(Duration.ofSeconds(Long.MAX_VALUE)),
          "a quiescence timeout beyond the nanosecond range is a caller error");
      final Thread blocker = new Thread(() -> {
        resource.lockHistory();
        try {
          held.countDown();
          release.await();
        } catch (final InterruptedException interrupted) {
          Thread.currentThread().interrupt();
        } finally {
          resource.unlockHistory();
        }
      }, "derived-history-blocker");
      blocker.setDaemon(true);
      blocker.start();
      try {
        assertTrue(held.await(60, TimeUnit.SECONDS), "test setup: derived maintenance is held mid-build");
        final SirixIOException refusal =
            assertThrows(SirixIOException.class, () -> BackupManager.backupDatabase(sourceDb, backupDir));
        assertTrue(refusal.getMessage().contains(DERIVED),
            () -> "the refusal must name the derived resource: " + refusal.getMessage());
      } finally {
        release.countDown();
        blocker.join(TimeUnit.SECONDS.toMillis(60));
      }
    }
    assertFalse(Files.exists(backupDir), "a refused backup must leave no partial target");
  }

  @Test
  @DisplayName("TXN-613: a rebuilt hidden sibling cannot collide with the next restored resource")
  void backupReservesIdsForRebuiltDerivedResources() {
    SirixFiles.recursiveRemove(derivedDir(sourceDb));
    final Path backupDir = tempDir.resolve("backup");
    final Path restoredDir = tempDir.resolve("restored-db");
    BackupManager.backupDatabase(sourceDb, backupDir);
    final long derivedId = ResourceConfiguration.deserialize(derivedDir(backupDir)).getID();
    assertTrue(DatabaseConfiguration.deserialize(backupDir).getMaxResourceID() > derivedId,
        "the backup's persisted allocation counter must reserve the rebuilt sibling's ID");
    BackupManager.restoreDatabase(backupDir, restoredDir);
    try (final Database<JsonResourceSession> restored = Databases.openJsonDatabase(restoredDir)) {
      assertTrue(restored.createResource(ResourceConfiguration.newBuilder("next").build()));
      final long nextId = ResourceConfiguration.deserialize(restoredDir.resolve("resources/next")).getID();
      assertTrue(nextId > derivedId, "a new resource must not reuse the copied hidden resource's ID");
      try (final JsonResourceSession next = restored.beginResourceSession("next");
          final JsonNodeTrx writer = next.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[99]"));
      }
    }
  }

  @Test
  @DisplayName("TXN-613: a pending rewind is refused while source and derived history are locked")
  void backupRefusesAPendingDerivedRewind() {
    final Path backupDir = tempDir.resolve("backup");
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(sourceDb);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      final DerivedIndexMaintainer.Resource resource =
          Objects.requireNonNull(((AbstractResourceSession<?, ?>) session).getDerivedResource());
      DerivedIndexTestSupport.await(session);
      resource.pause();
      try {
        try (final JsonNodeTrx writer = session.beginNodeTrx()) {
          writer.truncateTo(2);
        }
        assertEquals(2, session.getMostRecentRevisionNumber());
        final SirixIOException failure = assertThrows(SirixIOException.class,
            () -> BackupManager.backupDatabase(sourceDb, backupDir));
        assertTrue(failure.getMessage().contains(DERIVED));
        assertFalse(Files.exists(backupDir), "a refused backup must leave no target");
      } finally {
        resource.resume();
      }
    }
  }

  @Test
  @DisplayName("TXN-613: restore refuses a cold pair whose derived mappings exceed the source head")
  void restoreRefusesDerivedMappingsBeyondTheSourceHead() throws IOException {
    final Path backupDir = tempDir.resolve("backup");
    BackupManager.backupDatabase(sourceDb, backupDir);
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(sourceDb);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      final DerivedIndexMaintainer.Resource resource =
          Objects.requireNonNull(((AbstractResourceSession<?, ?>) session).getDerivedResource());
      resource.pause();
      try {
        try (final JsonNodeTrx writer = session.beginNodeTrx()) {
          writer.truncateTo(2);
        }
        final Path sourceResource = sourceDb.resolve("resources").resolve(RESOURCE);
        final Path backupResource = backupDir.resolve("resources").resolve(RESOURCE);
        try (final Stream<Path> paths = Files.walk(sourceResource)) {
          for (final Path path : paths.toList()) {
            final Path target = backupResource.resolve(sourceResource.relativize(path));
            if (Files.isDirectory(path)) {
              Files.createDirectories(target);
            } else {
              Files.copy(path, target, StandardCopyOption.REPLACE_EXISTING);
            }
          }
        }
      } finally {
        resource.resume();
      }
    }
    final Path restoredDir = tempDir.resolve("restored-db");
    final SirixIOException failure = assertThrows(SirixIOException.class,
        () -> BackupManager.restoreDatabase(backupDir, restoredDir));
    assertTrue(failure.getMessage().contains(DERIVED));
    assertFalse(Files.exists(restoredDir));
  }

  @Test
  @DisplayName("TXN-613: restore rejects damaged restart metadata below an intact derived revision root")
  void restoreRefusesDamagedDerivedRestartMetadata() throws IOException {
    final Path backupDir = tempDir.resolve("backup");
    BackupManager.backupDatabase(sourceDb, backupDir);
    final ResourceConfiguration config = ResourceConfiguration.deserialize(derivedDir(backupDir));
    final InterruptedFirstCommitRecovery.StorageAndUberPage opened =
        InterruptedFirstCommitRecovery.openStorageForTool(config);
    final long metadataOffset;
    final int revision = opened.uberPage().getRevisionNumber();
    try (final Reader reader = opened.storage().createReader()) {
      final RevisionRootPage root = reader.readRevisionRootPage(revision, config);
      metadataOffset = root.getTemporalAggregateIndexPageReference().getKey();
    } finally {
      opened.storage().close();
    }
    assertTrue(metadataOffset >= IOStorage.DATA_REGION_START);
    try (final FileChannel channel = FileChannel.open(derivedFile(backupDir, "sirix.data"), StandardOpenOption.WRITE)) {
      final ByteBuffer damage = ByteBuffer.allocate(16);
      while (damage.hasRemaining()) {
        channel.write(damage, metadataOffset + damage.position());
      }
    }
    final InterruptedFirstCommitRecovery.StorageAndUberPage damaged =
        InterruptedFirstCommitRecovery.openStorageForTool(config);
    try (final Reader reader = damaged.storage().createReader()) {
      assertEquals(revision, reader.readRevisionRootPage(revision, config).getRevision(),
          "the revision root remains readable; only its referenced metadata is damaged");
    } finally {
      damaged.storage().close();
    }
    final Path restoredDir = tempDir.resolve("restored-db");
    final SirixIOException failure = assertThrows(SirixIOException.class,
        () -> BackupManager.restoreDatabase(backupDir, restoredDir));
    assertTrue(failure.getMessage().contains(DERIVED));
    assertFalse(Files.exists(restoredDir));
  }

  @Test
  @DisplayName("TXN-613: restore refuses incomplete derived structure before a source can rebuild it")
  void restoreRefusesIncompleteDerivedStructure() throws IOException {
    final Path backupDir = tempDir.resolve("backup");
    BackupManager.backupDatabase(sourceDb, backupDir);
    SirixFiles.recursiveRemove(derivedDir(backupDir).resolve("indexes"));
    final Path restoredDir = tempDir.resolve("restored-db");
    final SirixIOException failure = assertThrows(SirixIOException.class,
        () -> BackupManager.restoreDatabase(backupDir, restoredDir));
    assertTrue(failure.getMessage().contains(DERIVED));
    assertFalse(Files.exists(restoredDir));
  }

  @Test
  @DisplayName("TXN-613: retrying a cleaned backup target verifies the reissued history's bytes")
  void backupRetriesACleanedTargetAfterHistoryReissue() {
    final Path backupDir = tempDir.resolve("retry-backup");
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(sourceDb)) {
      createTemporalResource(database, "other");
      final List<Path> resources = database.listResources();
      final String first = resources.getFirst().getFileName().toString();
      final String second = resources.getLast().getFileName().toString();
      try (final JsonResourceSession session = database.beginResourceSession(first);
          final JsonResourceSession busy = database.beginResourceSession(second)) {
        final DerivedIndexMaintainer.Resource maintained =
            Objects.requireNonNull(((AbstractResourceSession<?, ?>) session).getDerivedResource());
        maintained.pause();
        try {
          reissueHistory(session, maintained, 1);
          final int previousDerivedHead = derivedHead(sourceDb, first);
          try (final JsonNodeTrx blocker = busy.beginNodeTrx()) {
            assertThrows(SirixUsageException.class, () -> BackupManager.backupDatabase(sourceDb, backupDir));
          }
          assertFalse(Files.exists(backupDir), "the second resource's busy writer removes the partial target");
          reissueHistory(session, maintained, 20);
          final int expectedDerivedHead = derivedHead(sourceDb, first);
          assertEquals(previousDerivedHead, expectedDerivedHead,
              "the reissued history reuses revision numbers with different persisted contents");
          final BackupManager.BackupSummary retry = BackupManager.backupDatabase(sourceDb, backupDir);
          assertEquals(2, retry.resourcesCopied());
          assertEquals(REVISIONS, retry.resources().stream()
                                      .filter(resource -> resource.resourceName().equals(first))
                                      .findFirst().orElseThrow().mostRecentRevision());
          assertEquals(expectedDerivedHead, retry.derivedResources().stream()
                                                .filter(resource -> resource.resourceName().equals(first + "$derived"))
                                                .findFirst().orElseThrow().mostRecentRevision());
          final BackupManager.BackupSummary restored =
              BackupManager.restoreDatabase(backupDir, tempDir.resolve("retried-restored"));
          assertEquals(retry.derivedResources().stream().map(BackupManager.ResourceSummary::mostRecentRevision).toList(),
              restored.derivedResources().stream().map(BackupManager.ResourceSummary::mostRecentRevision).toList());
        } finally {
          maintained.resume();
        }
      }
    }
  }

  private static void reissueHistory(final JsonResourceSession session,
      final DerivedIndexMaintainer.Resource maintained, final int groups) {
    try (final JsonNodeTrx writer = session.beginNodeTrx()) {
      writer.truncateTo(1);
    }
    maintained.drain();
    try (final JsonNodeTrx writer = session.beginNodeTrx()) {
      final StringBuilder document = new StringBuilder("[");
      for (int group = 0; group < groups; group++) {
        if (group > 0) {
          document.append(',');
        }
        document.append("{\"g\":").append(group).append(",\"n\":10000000000,\"lo\":\"2024-01-01T00:00:00Z\",")
                .append("\"hi\":\"2025-01-01T00:00:00Z\"}");
      }
      document.append(']');
      for (int revision = 2; revision <= REVISIONS; revision++) {
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.remove();
        writer.moveToDocumentRoot();
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(document.toString()), JsonNodeTrx.Commit.NO);
        writer.commit("reissued groups=" + groups, Instant.parse("2024-0" + revision + "-01T00:00:00Z"));
        maintained.drain();
      }
    }
  }

  private static int derivedHead(final Path database, final String sourceName) {
    return InterruptedFirstCommitRecovery.probeUberPage(
        ResourceConfiguration.deserialize(database.resolve("resources").resolve(sourceName + "$derived")))
                                        .getRevisionNumber();
  }

  private static BackupManager.ResourceSummary onlyDerived(final BackupManager.BackupSummary summary) {
    assertEquals(1, summary.derivedResources().size(), () -> "exactly one derived resource in " + summary);
    return summary.derivedResources().getFirst();
  }

  /** Bytes of everything outside {@code resources/}: dbsetting.obj, keyselector/ and so on. */
  private static long databaseLevelBytes(final Path databaseDir) {
    try (final Stream<Path> paths = Files.walk(databaseDir)) {
      return paths.filter(Files::isRegularFile)
                  .filter(path -> !path.startsWith(databaseDir.resolve("resources")))
                  .mapToLong(path -> {
                    try {
                      return Files.size(path);
                    } catch (final IOException failure) {
                      throw new UncheckedIOException(failure);
                    }
                  })
                  .sum();
    } catch (final IOException failure) {
      throw new UncheckedIOException(failure);
    }
  }

  private static Path derivedDir(final Path databaseDir) {
    return databaseDir.resolve("resources").resolve(DERIVED);
  }

  private static Path derivedFile(final Path databaseDir, final String fileName) {
    return derivedDir(databaseDir).resolve("data").resolve(fileName);
  }
}
