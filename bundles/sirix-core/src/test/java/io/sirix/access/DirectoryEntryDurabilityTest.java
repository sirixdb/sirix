/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.backup.BackupManager;
import io.sirix.crash.DirectoryCrashModel;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.SirixUsageException;
import io.sirix.io.IOStorage;
import io.sirix.io.StorageType;
import io.sirix.io.Writer;
import io.sirix.io.filechannel.FileChannelStorage;
import io.sirix.io.memorymapped.MMStorage;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.bytepipe.Encryptor;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mockStatic;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * TXN-616 conformance: each row of the directory-entry table survives a crash that loses every
 * create, rename and delete no directory barrier covered.
 *
 * <p>
 * Each case runs its operation under {@link DirectoryCrashModel}, materializes the post-crash state
 * the model allows at the instant the operation returns (or, for the marker case, at the instant
 * the commit's catalogue barrier completes), drops every process cache and asserts the row's
 * guarantee on the materialized copy. File contents are kept as they are, so a failure here is a
 * lost or resurrected name, never lost bytes.
 */
@Isolated
final class DirectoryEntryDurabilityTest {

  private static final String RESOURCE = "data";
  private static final String DOCUMENT = "[{\"id\":1},{\"id\":2}]";

  @TempDir
  Path temp;

  private Path root;
  private Path links;
  private Path crashed;

  @BeforeEach
  void setUp() throws IOException {
    root = Files.createDirectories(temp.resolve("fs"));
    links = temp.resolve("links");
    crashed = temp.resolve("crashed");
    assumeTrue(DirectoryCrashModel.supported(root), "the crash model needs file keys (not available on Windows)");
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void catalogueOfACommittedRevisionSurvivesACrash(final StorageType storageType) throws IOException {
    final Path database = createDatabaseWithResource(storageType);
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(database);
        final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
      try (final DirectoryCrashModel model = DirectoryCrashModel.start(root, links);
          final JsonNodeTrx trx = session.beginNodeTrx()) {
        model.governContents(path -> path.getFileName().toString().endsWith(".xml"));
        session.getWtxIndexController(trx.getRevisionNumber()).createIndexes(Set.of(casIndex()), trx);
        trx.commit();
        model.materialize(crashed);
      }
    }
    reopenCold();
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(crashedDatabase());
        final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
      assertEquals(2, session.getMostRecentRevisionNumber());
      assertEquals(1, session.getRtxIndexController(2).getIndexes().getIndexDefs().size(),
          "revision 2 committed an index; its catalogue indexes/2.xml must survive the crash");
    }
  }

  @Test
  void replacedDatabaseSettingsSurviveACrash() throws IOException {
    final Path database = root.resolve("db");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(database)));
    final DatabaseConfiguration config = DatabaseConfiguration.deserialize(database);
    try (final DirectoryCrashModel model = DirectoryCrashModel.start(root, links)) {
      model.governContents(DirectoryEntryDurabilityTest::isSettings);
      config.setMaximumResourceID(41);
      DatabaseConfiguration.serialize(config);
      model.materialize(crashed);
    }
    reopenCold();
    assertEquals(41, DatabaseConfiguration.deserialize(crashedDatabase()).getMaxResourceID(),
        "a returned settings replacement must survive the crash");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void createdDatabaseSurvivesACrash(final boolean existingTarget) throws IOException {
    final Path database = root.resolve("db");
    try (final DirectoryCrashModel model = DirectoryCrashModel.start(root, links)) {
      model.governContents(DirectoryEntryDurabilityTest::isSettings);
      if (existingTarget) {
        Files.createDirectory(database);
      }
      assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(database)));
      model.materialize(crashed);
    }
    reopenCold();
    assertTrue(Databases.existsDatabase(crashedDatabase()), "a created database must survive the crash");
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(crashedDatabase())) {
      assertTrue(db.createResource(resource(StorageType.FILE_CHANNEL)));
    }
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void createdResourceSurvivesACrash(final StorageType storageType) throws IOException {
    final Path database = root.resolve("db");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(database)));
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(database)) {
      try (final DirectoryCrashModel model = DirectoryCrashModel.start(root, links)) {
        model.governContents(DirectoryEntryDurabilityTest::isSettings);
        assertTrue(db.createResource(resource(storageType)));
        model.materialize(crashed);
      }
    }
    assertCreatedResourceSurvived();
  }

  @Test
  void createdEncryptedResourceAndItsKeysetSurviveACrash() throws IOException {
    final Path database = root.resolve("db");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(database)));
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(database)) {
      try (final DirectoryCrashModel model = DirectoryCrashModel.start(root, links)) {
        model.governContents(path -> isSettings(path) || path.getFileName().toString().equals("encryptionKey.json"));
        assertTrue(
            db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .byteHandlerPipeline(new ByteHandlerPipeline(
                                                       new Encryptor(database.resolve("resources").resolve(RESOURCE))))
                                                   .storeDiffs(false)
                                                   .build()));
        model.materialize(crashed);
      }
    }
    // The settings bind the keyset to the original resource path (a separate defect), so reading the
    // crashed copy would use the original key; compare the persisted keyset bytes instead.
    final Path keyset = Path.of("resources", RESOURCE, "encryption", "encryptionKey.json");
    assertEquals(Files.readString(database.resolve(keyset), StandardCharsets.UTF_8),
        Files.readString(crashedDatabase().resolve(keyset), StandardCharsets.UTF_8),
        "the keyset must survive the crash with its contents");
    assertCreatedResourceSurvived();
  }

  /**
   * A resource whose creation was interrupted after its revisions file existed but before its data
   * file did is rebuilt from scratch (TXN-611); the data file created then is a name its first commit
   * relies on.
   */
  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void dataFileRecreatedForAnInterruptedCreationSurvivesACrash(final StorageType storageType) throws IOException {
    final Path database = root.resolve("db");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(database)));
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(database)) {
      assertTrue(db.createResource(resource(storageType)));
    }
    final Path data = database.resolve("resources").resolve(RESOURCE).resolve("data");
    Files.delete(data.resolve("sirix.data"));
    Files.write(data.resolve("sirix.revisions"), new byte[0]);
    reopenCold();
    final int committed;
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(database);
        final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
      try (final DirectoryCrashModel model = DirectoryCrashModel.start(root, links);
          final JsonNodeTrx trx = session.beginNodeTrx()) {
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(DOCUMENT), JsonNodeTrx.Commit.NO);
        trx.commit();
        committed = session.getMostRecentRevisionNumber();
        model.materialize(crashed);
      }
    }
    assertStorageFilesSurvived();
    reopenCold();
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(crashedDatabase());
        final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
      assertEquals(committed, session.getMostRecentRevisionNumber(), "the acknowledged commit must survive the crash");
      assertEquals(DOCUMENT, serialize(session, committed));
    }
  }

  private void assertCreatedResourceSurvived() {
    assertStorageFilesSurvived();
    reopenCold();
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(crashedDatabase())) {
      assertTrue(db.existsResource(RESOURCE), "a created resource must survive the crash");
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        assertEquals(0, session.getMostRecentRevisionNumber());
        try (final JsonNodeTrx trx = session.beginNodeTrx()) {
          trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(DOCUMENT), JsonNodeTrx.Commit.NO);
          trx.commit();
        }
        assertEquals(DOCUMENT, serialize(session, 1));
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void removedResourceStaysRemovedAfterACrash(final StorageType storageType) throws IOException {
    final Path database = createDatabaseWithResource(storageType);
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(database)) {
      try (final DirectoryCrashModel model = DirectoryCrashModel.start(root, links)) {
        db.removeResource(RESOURCE);
        model.materialize(crashed);
      }
      try (final Stream<Path> resources = Files.list(database.resolve("resources"))) {
        assertEquals(List.of(), resources.toList(), "a completed removal leaves nothing behind");
      }
    }
    reopenCold();
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(crashedDatabase())) {
      assertFalse(db.existsResource(RESOURCE), "a removed resource must not reappear after the crash");
      assertTrue(db.listResources().isEmpty(), "no remnant of the removal may be listed: " + db.listResources());
      assertTrue(db.createResource(resource(storageType)), "the removed name must be free again");
    }
    try (final Stream<Path> resources = Files.list(crashedDatabase().resolve("resources"))) {
      assertEquals(List.of(RESOURCE), resources.map(path -> path.getFileName().toString()).toList(),
          "the next open deletes what a crashed removal left behind");
    }
  }

  @Test
  void removedDatabaseStaysRemovedAfterACrash() throws IOException {
    final Path database = createDatabaseWithResource(StorageType.FILE_CHANNEL);
    try (final DirectoryCrashModel model = DirectoryCrashModel.start(root, links)) {
      Databases.removeDatabase(database);
      model.materialize(crashed);
    }
    reopenCold();
    assertFalse(Databases.existsDatabase(crashedDatabase()), "a removed database must not reappear after the crash");
    Databases.removeDatabase(crashedDatabase());
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(crashedDatabase())),
        "removing the leftover of a crashed removal frees the path");
  }

  @Test
  void databaseRemovalRetryCannotSkipTheFailedSettingsBarrier() throws IOException {
    final Path database = root.resolve("db");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(database)));
    try (final DirectoryCrashModel model = DirectoryCrashModel.start(root, links)) {
      try (final MockedStatic<FileChannel> ignored = refuseDirectoryOpen(database)) {
        assertThrows(SirixIOException.class, () -> Databases.removeDatabase(database));
      }
      assertFalse(Files.exists(database.resolve("dbsetting.obj")));
      Databases.removeDatabase(database);
      model.materialize(crashed);
    }
    reopenCold();
    assertFalse(Databases.existsDatabase(crashedDatabase()), "the successful retry must not resurrect settings");
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void resourceRemovalRetryCannotSkipTheFailedRenameBarrier(final StorageType storageType) throws IOException {
    final Path database = createDatabaseWithResource(storageType);
    final Path resources = database.resolve("resources");
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(database);
        final DirectoryCrashModel model = DirectoryCrashModel.start(root, links)) {
      try (final MockedStatic<FileChannel> ignored = refuseDirectoryOpen(resources)) {
        assertThrows(SirixIOException.class, () -> db.removeResource(RESOURCE));
      }
      assertFalse(Files.exists(resources.resolve(RESOURCE)), "the rename precedes its failed barrier");
      db.removeResource(RESOURCE);
      assertThrows(SirixUsageException.class, () -> db.getResourceID(RESOURCE));
      model.materialize(crashed);
    }
    reopenCold();
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(crashedDatabase())) {
      assertFalse(db.existsResource(RESOURCE), "the successful retry must not resurrect the resource");
      assertTrue(db.listResources().isEmpty());
      assertTrue(db.createResource(resource(storageType)));
    }
  }

  @ParameterizedTest
  @CsvSource({"FILE_CHANNEL,sirix.data", "FILE_CHANNEL,sirix.revisions",
      "MEMORY_MAPPED,sirix.data", "MEMORY_MAPPED,sirix.revisions"})
  void storageFileCreationRetriesItsFailedBarrier(final StorageType storageType, final String filename)
      throws IOException {
    final Path database = createDatabaseWithResource(storageType);
    final ResourceConfiguration config =
        ResourceConfiguration.deserialize(database.resolve("resources").resolve(RESOURCE));
    final Path data = config.resourcePath.resolve("data");
    final Path failedFile = data.resolve(filename);
    final IOStorage storage = storageType == StorageType.FILE_CHANNEL
        ? new FileChannelStorage(config, Caffeine.newBuilder().buildAsync())
        : new MMStorage(config, Caffeine.newBuilder().buildAsync());
    try {
      Files.delete(data.resolve("sirix.data"));
      Files.delete(data.resolve("sirix.revisions"));
      try (final DirectoryCrashModel model = DirectoryCrashModel.start(root, links)) {
        try (final MockedStatic<FileChannel> ignored = mockStatic(FileChannel.class, invocation -> {
          if (invocation.getMethod().getName().equals("open") && data.equals(invocation.getArgument(0))
              && Files.exists(failedFile)) {
            throw new IOException("directory open refused");
          }
          return invocation.callRealMethod();
        })) {
          assertThrows(SirixIOException.class, storage::createWriter);
        }
        assertFalse(Files.exists(failedFile), "a failed publication must leave the file absent for retry");
        try (final Writer writer = storage.createWriter()) {
          model.materialize(crashed);
        }
        final int published = model.barriers().size();
        try (final Writer writer = storage.createWriter()) {
          assertEquals(published, model.barriers().size(), "opening existing files must add no barrier");
        }
      }
    } finally {
      storage.close();
    }
    assertStorageFilesSurvived();
  }

  private static MockedStatic<FileChannel> refuseDirectoryOpen(final Path directory) {
    return mockStatic(FileChannel.class, invocation -> {
      if (invocation.getMethod().getName().equals("open") && directory.equals(invocation.getArgument(0))) {
        throw new IOException("directory open refused");
      }
      return invocation.callRealMethod();
    });
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void backupSurvivesACrash(final boolean existingTarget) throws IOException {
    final Path database = createDatabaseWithResource(StorageType.FILE_CHANNEL);
    final Path backup = root.resolve("backup");
    try (final DirectoryCrashModel model = DirectoryCrashModel.start(root, links)) {
      model.governContents(path -> path.startsWith("backup"));
      if (existingTarget) {
        Files.createDirectory(backup);
      }
      BackupManager.backupDatabase(database, backup);
      model.materialize(crashed);
    }
    reopenCold();
    final Path restored = temp.resolve("restored");
    BackupManager.restoreDatabase(crashed.resolve("backup"), restored);
    assertCommittedDocument(restored);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void restoredDatabaseSurvivesACrash(final boolean existingTarget) throws IOException {
    final Path database = createDatabaseWithResource(StorageType.FILE_CHANNEL);
    final Path backup = temp.resolve("backup");
    BackupManager.backupDatabase(database, backup);
    Databases.clearGlobalCaches();
    try (final DirectoryCrashModel model = DirectoryCrashModel.start(root, links)) {
      model.governContents(path -> path.startsWith("restored"));
      if (existingTarget) {
        Files.createDirectory(root.resolve("restored"));
      }
      BackupManager.restoreDatabase(backup, root.resolve("restored"));
      model.materialize(crashed);
    }
    reopenCold();
    assertCommittedDocument(crashed.resolve("restored"));
  }

  /**
   * TXN-602 under TXN-616: a crash at the instant the catalogue of revision 2 became durable, before
   * the revision committed, loses the recovery marker (its directory is never synced) but keeps the
   * catalogue named for the uncommitted revision and the pages appended for it. Without the marker no
   * recovery runs, yet the committed revision stays readable, a writer is admitted, and the reissued
   * revision 2 does not inherit the stale catalogue.
   */
  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void lostRecoveryMarkerLeavesCommittedStateAndWriterAdmissionIntact(final StorageType storageType)
      throws IOException {
    final Path database = createDatabaseWithResource(storageType);
    final AtomicBoolean reachedCrash = new AtomicBoolean();
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(database);
        final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
      try (final DirectoryCrashModel model = DirectoryCrashModel.start(root, links)) {
        model.afterEachBarrier(directory -> {
          if (directory.getFileName().toString().equals("indexes") && reachedCrash.compareAndSet(false, true)) {
            try {
              model.materialize(crashed);
            } catch (final IOException e) {
              throw new UncheckedIOException(e);
            }
          }
        });
        try (final JsonNodeTrx trx = session.beginNodeTrx()) {
          trx.moveToDocumentRoot();
          trx.moveToFirstChild();
          trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"id\":0}"), JsonNodeTrx.Commit.NO);
          session.getWtxIndexController(trx.getRevisionNumber()).createIndexes(Set.of(casIndex()), trx);
          trx.commit();
        }
      }
    }
    assertTrue(reachedCrash.get(), "the commit never reached its catalogue barrier");
    final Path crashedResource = crashedDatabase().resolve("resources").resolve(RESOURCE);
    assertFalse(Files.exists(crashedResource.resolve("log").resolve(".commit")),
        "precondition: the crash state lost the recovery marker");
    assertTrue(Files.exists(crashedResource.resolve("indexes").resolve("2.xml")),
        "precondition: the crash state kept the catalogue of the uncommitted revision");

    reopenCold();
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(crashedDatabase());
        final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
      assertEquals(1, session.getMostRecentRevisionNumber(), "the uncommitted revision must not appear");
      assertEquals(DOCUMENT, serialize(session, 1));
      assertTrue(session.getRtxIndexController(1).getIndexes().getIndexDefs().isEmpty());
      try (final JsonNodeTrx trx = session.beginNodeTrx()) {
        trx.moveToDocumentRoot();
        trx.moveToFirstChild();
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"id\":3}"), JsonNodeTrx.Commit.NO);
        trx.commit();
      }
    }
    reopenCold();
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(crashedDatabase());
        final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
      assertEquals(2, session.getMostRecentRevisionNumber());
      assertEquals(DOCUMENT, serialize(session, 1));
      assertEquals("[{\"id\":3},{\"id\":1},{\"id\":2}]", serialize(session, 2));
      assertTrue(session.getRtxIndexController(2).getIndexes().getIndexDefs().isEmpty(),
          "the reissued revision 2 must not inherit the uncommitted revision's catalogue");
    }
  }

  private Path createDatabaseWithResource(final StorageType storageType) {
    final Path database = root.resolve("db");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(database)));
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(database)) {
      assertTrue(db.createResource(resource(storageType)));
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE);
          final JsonNodeTrx trx = session.beginNodeTrx()) {
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(DOCUMENT), JsonNodeTrx.Commit.NO);
        trx.commit();
      }
    }
    return database;
  }

  /** The persisted file set of the resource (FMT-101), checked before a reopen could recreate it. */
  private void assertStorageFilesSurvived() {
    final Path data = crashedDatabase().resolve("resources").resolve(RESOURCE).resolve("data");
    assertTrue(Files.isRegularFile(data.resolve("sirix.data")), "the data file must survive the crash");
    assertTrue(Files.isRegularFile(data.resolve("sirix.revisions")), "the revisions file must survive the crash");
  }

  /** The database and resource settings, including the temporary file a replacement writes. */
  private static boolean isSettings(final Path relative) {
    final String name = relative.getFileName().toString();
    return name.startsWith("dbsetting") || name.equals("ressetting.obj");
  }

  private Path crashedDatabase() {
    return crashed.resolve("db");
  }

  private static void assertCommittedDocument(final Path database) {
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(database);
        final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
      assertEquals(1, session.getMostRecentRevisionNumber());
      assertEquals(DOCUMENT, serialize(session, 1));
    }
  }

  private static void reopenCold() {
    Databases.clearGlobalCaches();
  }

  private static String serialize(final JsonResourceSession session, final int revision) {
    final StringWriter out = new StringWriter();
    JsonSerializer.newBuilder(session, out, revision).build().call();
    return out.toString();
  }

  private static ResourceConfiguration resource(final StorageType storageType) {
    return ResourceConfiguration.newBuilder(RESOURCE).storageType(storageType).storeDiffs(false).build();
  }

  private static IndexDef casIndex() {
    return IndexDefs.createCASIdxDef(false, Type.INR, Set.of(parse("/[]/id", PathParser.Type.JSON)), 0,
        IndexDef.DbType.JSON);
  }
}
