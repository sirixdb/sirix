package io.sirix.access;

import io.sirix.exception.UnsupportedStoreFeatureException;
import io.sirix.backup.BackupManager;
import io.sirix.access.trx.node.InternalResourceSession;
import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.access.trx.node.json.JsonResourceSessionImpl;
import io.sirix.access.trx.page.StorageEngineWriterFactory;
import io.sirix.cache.BufferManager;
import io.sirix.access.trx.node.InternalResourceSession.Abort;
import io.sirix.page.UberPage;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import org.junit.jupiter.api.Test;
import io.sirix.exception.SirixIOException;
import io.sirix.io.IOStorage;
import io.sirix.io.Reader;
import io.sirix.page.PageReference;
import io.sirix.io.InterruptedFirstCommitRecovery;
import io.sirix.io.PageHasher;
import io.sirix.io.StorageType;
import io.sirix.io.RevisionFileData;
import io.sirix.index.aggregate.DerivedIndexMaintainer;
import io.sirix.index.aggregate.TemporalAggregateStorage;
import io.sirix.page.StoreFeatures;
import io.sirix.utils.SirixFiles;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.Semaphore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verify;

/** Fault images use the independent beacon contract, never the feature codec being tested. */
class StoreFeaturesLifecycleTest {
  @TempDir
  Path directory;
  private Path databasePath;
  private ResourceConfiguration config;
  private int storeNumber;

  void createStore(final StorageType storageType) {
    databasePath = directory.resolve("db-" + storeNumber++);
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("resource").storageType(storageType).build());
      try (final var session = database.beginResourceSession("resource"); final var trx = session.beginNodeTrx()) {
        config = session.getResourceConfig();
        trx.commit();
      }
    }
  }

  @ParameterizedTest
  @EnumSource(DatabaseType.class)
  void databaseHandlesDoNotReadResourceBeacons(final DatabaseType type) throws Exception {
    databasePath = directory.resolve("handles");
    final var databaseConfig = new DatabaseConfiguration(databasePath);
    if (type == DatabaseType.JSON) {
      Databases.createJsonDatabase(databaseConfig);
    } else {
      Databases.createXmlDatabase(databaseConfig);
    }
    try (final var database = openDatabase(type)) {
      for (int resource = 0; resource < 16; resource++) {
        assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource-" + resource)
                                                     .storageType((resource & 1) == 0
                                                         ? StorageType.FILE_CHANNEL
                                                         : StorageType.MEMORY_MAPPED)
                                                     .build()));
      }
    }
    Databases.clearGlobalCaches();
    final Path resources = databasePath.toRealPath().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile());
    final int[] resourceChannels = {0};
    try (final var channels = mockStatic(FileChannel.class, invocation -> {
      final Object result = invocation.callRealMethod();
      if (invocation.getRawArguments().length == 2 && invocation.getArgument(0) instanceof Path path
          && path.startsWith(resources)) {
        resourceChannels[0]++;
      }
      return result;
    }); final var database = openDatabase(type)) {
      assertTrue(database.isOpen());
      assertEquals(0, resourceChannels[0], "a new database owner must not inspect resource beacons");
      try (final var reused = openDatabase(type)) {
        assertTrue(reused.isOpen());
        assertEquals(0, resourceChannels[0], "reusing an owner must not inspect resource beacons");
      }
      try (final var session = database.beginResourceSession("resource-0");
          final var reader = session.beginNodeReadOnlyTrx()) {
        assertEquals(0, reader.getRevisionNumber());
      }
      assertTrue(resourceChannels[0] > 0, "the I/O observer must detect an actual resource open");
    }
  }

  private Database<?> openDatabase(final DatabaseType type) {
    return switch (type) {
      case JSON -> Databases.openJsonDatabase(databasePath);
      case XML -> Databases.openXmlDatabase(databasePath);
    };
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void readOnlyBitsAllowReadsAndRefuseEveryWriterBeforeRecovery(final StorageType storageType) throws IOException {
    createStore(storageType);
    replaceBeacon(IOStorage.PRIMARY_BEACON_OFFSET, 2, new StoreFeatures(0, Integer.MIN_VALUE, 0));
    replaceBeacon(IOStorage.SECONDARY_BEACON_OFFSET, 2, new StoreFeatures(0, Integer.MIN_VALUE, 0));
    Files.createFile(marker());
    final byte[] before = Files.readAllBytes(dataFile());
    try (final var database = Databases.openJsonDatabase(databasePath);
        final var session = database.beginResourceSession("resource")) {
      try (final var reader = session.beginNodeReadOnlyTrx()) {
        assertEquals(1, reader.getRevisionNumber());
      }
      assertThrows(UnsupportedStoreFeatureException.class, session::beginNodeTrx);
      assertThrows(UnsupportedStoreFeatureException.class, session::createStorageEngineWriter);
      final var internal = (InternalResourceSession<?, ?>) session;
      assertThrows(UnsupportedStoreFeatureException.class, () -> {
        try (final var ignored = internal.createPageTransaction(99, 1, 1, Abort.NO, false)) {
          // A direct page-writer request has the same policy as the node-writer entry point.
        }
      });
    }
    assertArrayEquals(before, Files.readAllBytes(dataFile()));
    assertTrue(Files.exists(marker()), "a refused writer must not run marker recovery");
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void unknownIncompatPrimaryNeverFallsBackToCompatibleSecondary(final StorageType storageType) throws IOException {
    createStore(storageType);
    replaceBeacon(IOStorage.PRIMARY_BEACON_OFFSET, 2, new StoreFeatures(1, 0, 0));
    final byte[] before = Files.readAllBytes(dataFile());
    assertThrows(UnsupportedStoreFeatureException.class, () -> {
      final IOStorage storage = StorageType.getStorage(config);
      try (final Reader reader = storage.createReader()) {
        reader.readUberPageReference();
      } finally {
        storage.close();
      }
    });
    try (final var database = Databases.openJsonDatabase(databasePath)) {
      final var failure =
          assertThrows(UnsupportedStoreFeatureException.class, () -> database.beginResourceSession("resource"));
      assertTrue(failure.getMessage().contains("incompat"));
    }
    assertArrayEquals(before, Files.readAllBytes(dataFile()));
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void toolOpenAcceptsFeatureFreeStore(final StorageType storageType) {
    createStore(storageType);
    final var opened = InterruptedFirstCommitRecovery.openStorageForTool(config);
    try {
      assertEquals(1, opened.uberPage().getRevisionNumber());
      assertEquals(StoreFeatures.NONE, opened.uberPage().getFeatures());
      assertTrue(opened.storage().exists());
    } finally {
      opened.storage().close();
    }
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void toolOpenRefusesEachUnknownWordWithoutChangingBytes(final StorageType storageType) throws IOException {
    createStore(storageType);
    for (final StoreFeatures features : new StoreFeatures[] {new StoreFeatures(1, 0, 0), new StoreFeatures(0, 1, 0),
        new StoreFeatures(0, 0, 1)}) {
      replaceBeacon(IOStorage.PRIMARY_BEACON_OFFSET, 2, features);
      replaceBeacon(IOStorage.SECONDARY_BEACON_OFFSET, 2, features);
      final byte[] before = Files.readAllBytes(dataFile());
      assertThrows(UnsupportedStoreFeatureException.class,
          () -> InterruptedFirstCommitRecovery.openStorageForTool(config));
      assertArrayEquals(before, Files.readAllBytes(dataFile()));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void readOnlySalvageDoesNotHealRevisionRecords(final boolean secondaryOnly) throws IOException {
    createStore(StorageType.FILE_CHANNEL);
    installDamagedRevisionFixture(new StoreFeatures(0, 1, 0));
    if (secondaryOnly) {
      zeroBeacon(IOStorage.PRIMARY_BEACON_OFFSET);
    }
    final byte[] dataBefore = Files.readAllBytes(dataFile());
    final byte[] revisionsBefore = Files.readAllBytes(revisionsFile());
    try (final var database = Databases.openJsonDatabase(databasePath);
        final var session = database.beginResourceSession("resource");
        final var reader = session.beginNodeReadOnlyTrx()) {
      assertEquals(1, reader.getRevisionNumber());
      assertThrows(UnsupportedStoreFeatureException.class, session::beginNodeTrx);
      final IOStorage storage = StorageType.getStorage(config);
      try (final Reader direct = storage.createReader()) {
        assertTrue(direct.getRevisionFileData(1).offset() >= IOStorage.DATA_REGION_START);
      } finally {
        storage.close();
      }
    }
    assertArrayEquals(dataBefore, Files.readAllBytes(dataFile()));
    assertArrayEquals(revisionsBefore, Files.readAllBytes(revisionsFile()));
  }

  @Test
  void featureFreeSalvageStillHealsRevisionRecords() throws IOException {
    createStore(StorageType.FILE_CHANNEL);
    installDamagedRevisionFixture(StoreFeatures.NONE);
    try (final var database = Databases.openJsonDatabase(databasePath);
        final var session = database.beginResourceSession("resource");
        final var reader = session.beginNodeReadOnlyTrx()) {
      assertEquals(1, reader.getRevisionNumber());
    }
    assertTrue(Files.size(revisionsFile()) >= IOStorage.revisionsFileOffset(2));
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void damagedRevisionToolRefusalPreservesBothFiles(final StorageType storageType) throws IOException {
    for (final StoreFeatures features : new StoreFeatures[] {new StoreFeatures(1, 0, 0), new StoreFeatures(0, 1, 0),
        new StoreFeatures(0, 0, 1)}) {
      createStore(storageType);
      installDamagedRevisionFixture(features);
      final byte[] dataBefore = Files.readAllBytes(dataFile());
      final byte[] revisionsBefore = Files.readAllBytes(revisionsFile());
      assertThrows(UnsupportedStoreFeatureException.class,
          () -> InterruptedFirstCommitRecovery.openStorageForTool(config));
      assertArrayEquals(dataBefore, Files.readAllBytes(dataFile()));
      assertArrayEquals(revisionsBefore, Files.readAllBytes(revisionsFile()));
      final Path backup = directory.resolve("damaged-backup");
      assertThrows(UnsupportedStoreFeatureException.class, () -> BackupManager.backupDatabase(databasePath, backup));
      assertArrayEquals(dataBefore, Files.readAllBytes(dataFile()));
      assertArrayEquals(revisionsBefore, Files.readAllBytes(revisionsFile()));
      assertFalse(Files.exists(backup));
      final Path restored = directory.resolve("damaged-restored");
      final var failure =
          assertThrows(SirixIOException.class, () -> BackupManager.restoreDatabase(databasePath, restored));
      assertTrue(failure.getMessage().contains("Unknown store feature bits"));
      assertArrayEquals(dataBefore, Files.readAllBytes(dataFile()));
      assertArrayEquals(revisionsBefore, Files.readAllBytes(revisionsFile()));
      assertFalse(Files.exists(restored));
    }
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void parentOpenPreservesUnsupportedDerivedSibling(final StorageType storageType) throws IOException {
    createDerivedStore(storageType, false);
    replaceBeacon(IOStorage.PRIMARY_BEACON_OFFSET, 2, new StoreFeatures(1, 0, 0));
    replaceBeacon(IOStorage.SECONDARY_BEACON_OFFSET, 2, new StoreFeatures(1, 0, 0));
    Databases.clearGlobalCaches();
    final byte[] dataBefore = Files.readAllBytes(dataFile());
    final byte[] revisionsBefore = Files.readAllBytes(revisionsFile());
    try (final var database = Databases.openJsonDatabase(databasePath);
        final var source = database.beginResourceSession("resource");
        final var reader = source.beginNodeReadOnlyTrx()) {
      assertEquals(0, reader.getRevisionNumber());
      assertThrows(UnsupportedStoreFeatureException.class, () -> {
        final var opened = InterruptedFirstCommitRecovery.openStorageAndLoadUberPage(config);
        opened.storage().close();
      });
    }
    assertArrayEquals(dataBefore, Files.readAllBytes(dataFile()));
    assertArrayEquals(revisionsBefore, Files.readAllBytes(revisionsFile()));
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void unsupportedOpenPreservesMissingFilesAndSpills(final StorageType storageType) throws IOException {
    for (final StoreFeatures features : new StoreFeatures[] {new StoreFeatures(1, 0, 0), new StoreFeatures(0, 1, 0)}) {
      createStore(storageType);
      replaceBeacon(IOStorage.PRIMARY_BEACON_OFFSET, 2, features);
      replaceBeacon(IOStorage.SECONDARY_BEACON_OFFSET, 2, features);
      Files.delete(revisionsFile());
      final Path spill = Files.createDirectories(config.resourcePath.resolve("projection-sort-spill"));
      Files.writeString(spill.resolve("orphan.tmp"), "preserve this run");
      Databases.clearGlobalCaches();
      final Map<Path, byte[]> before = snapshotResource();
      assertEquals(features, InterruptedFirstCommitRecovery.probeFeatures(config));
      assertResourceUnchanged(before);
      try (final var database = Databases.openJsonDatabase(databasePath)) {
        assertThrows(UnsupportedStoreFeatureException.class, () -> database.beginResourceSession("resource"));
      }
      assertThrows(UnsupportedStoreFeatureException.class, () -> StorageType.getStorage(config));
      assertResourceUnchanged(before);
      assertFalse(Files.exists(revisionsFile()));
    }
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void readOnlyOpenPreservesOrphanSpills(final StorageType storageType) throws IOException {
    createStore(storageType);
    replaceBeacon(IOStorage.PRIMARY_BEACON_OFFSET, 2, new StoreFeatures(0, 1, 0));
    replaceBeacon(IOStorage.SECONDARY_BEACON_OFFSET, 2, new StoreFeatures(0, 1, 0));
    final Path spill = Files.createDirectories(config.resourcePath.resolve("projection-sort-spill"));
    Files.writeString(spill.resolve("orphan.tmp"), "preserve this run");
    Databases.clearGlobalCaches();
    final Map<Path, byte[]> before = snapshotResource();
    try (final var database = Databases.openJsonDatabase(databasePath);
        final var session = database.beginResourceSession("resource");
        final var reader = session.beginNodeReadOnlyTrx()) {
      assertEquals(1, reader.getRevisionNumber());
    }
    assertResourceUnchanged(before);
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void xmlOpenPreservesUnsupportedFilesAndSpills(final StorageType storageType) throws IOException {
    for (final boolean missing : new boolean[] {false, true}) {
      databasePath = directory.resolve("xml-" + storeNumber++);
      Databases.createXmlDatabase(new DatabaseConfiguration(databasePath));
      try (final var database = Databases.openXmlDatabase(databasePath)) {
        database.createResource(ResourceConfiguration.newBuilder("resource").storageType(storageType).build());
        try (final var session = database.beginResourceSession("resource"); final var writer = session.beginNodeTrx()) {
          config = session.getResourceConfig();
          writer.commit();
        }
      }
      replaceBeacon(IOStorage.PRIMARY_BEACON_OFFSET, 2, new StoreFeatures(0, 1, 0));
      replaceBeacon(IOStorage.SECONDARY_BEACON_OFFSET, 2, new StoreFeatures(0, 1, 0));
      if (missing) {
        Files.delete(revisionsFile());
      }
      final Path spill = Files.createDirectories(config.resourcePath.resolve("projection-sort-spill"));
      Files.writeString(spill.resolve("orphan.tmp"), "preserve this run");
      Databases.clearGlobalCaches();
      final Map<Path, byte[]> before = snapshotResource();
      try (final var database = Databases.openXmlDatabase(databasePath)) {
        if (missing) {
          assertThrows(UnsupportedStoreFeatureException.class, () -> database.beginResourceSession("resource"));
        } else {
          try (final var session = database.beginResourceSession("resource");
              final var reader = session.beginNodeReadOnlyTrx()) {
            assertEquals(1, reader.getRevisionNumber());
          }
        }
      }
      assertResourceUnchanged(before);
    }
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void unsupportedDerivedDamageNeverTriggersRebuild(final StorageType storageType) throws IOException {
    for (final StoreFeatures features : new StoreFeatures[] {new StoreFeatures(1, 0, 0), new StoreFeatures(0, 1, 0)}) {
      for (int damage = 0; damage < 3; damage++) {
        createDerivedStore(storageType, damage == 2);
        replaceBeacon(IOStorage.PRIMARY_BEACON_OFFSET, 2, features);
        replaceBeacon(IOStorage.SECONDARY_BEACON_OFFSET, 2, features);
        if (damage == 0) {
          SirixFiles.recursiveRemove(config.resourcePath.resolve("log"));
        } else if (damage == 1) {
          removeRevisionRecords();
        }
        Databases.clearGlobalCaches();
        final Map<Path, byte[]> before = snapshotResource();
        try (final var database = Databases.openJsonDatabase(databasePath);
            final var source = database.beginResourceSession("resource");
            final var reader = source.beginNodeReadOnlyTrx()) {
          assertEquals(0, reader.getRevisionNumber());
        }
        assertResourceUnchanged(before);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void toolsRefuseHiddenDerivedFeaturesBeforeOpeningParents(final StorageType storageType) throws IOException {
    for (final StoreFeatures features : new StoreFeatures[] {new StoreFeatures(1, 0, 0), new StoreFeatures(0, 1, 0),
        new StoreFeatures(0, 0, 1)}) {
      createDerivedStore(storageType, false);
      replaceBeacon(IOStorage.PRIMARY_BEACON_OFFSET, 2, features);
      replaceBeacon(IOStorage.SECONDARY_BEACON_OFFSET, 2, features);
      Files.delete(revisionsFile());
      Databases.clearGlobalCaches();
      final Map<Path, byte[]> before = snapshotResource();
      final Path backup = directory.resolve("hidden-backup");
      final Path restored = directory.resolve("hidden-restored");
      assertThrows(UnsupportedStoreFeatureException.class, () -> BackupManager.backupDatabase(databasePath, backup));
      final var failure =
          assertThrows(SirixIOException.class, () -> BackupManager.restoreDatabase(databasePath, restored));
      assertTrue(failure.getMessage().contains("Unknown store feature bits"));
      assertFalse(Files.exists(backup));
      assertFalse(Files.exists(restored));
      assertResourceUnchanged(before);
      assertFalse(Files.exists(revisionsFile()));
    }
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void featureFreeDerivedSnapshotDamageStillRebuilds(final StorageType storageType) throws IOException {
    createDerivedStore(storageType, true);
    Databases.clearGlobalCaches();
    try (final var database = Databases.openJsonDatabase(databasePath);
        final var source = database.beginResourceSession("resource")) {
      assertEquals(0, source.getMostRecentRevisionNumber());
      assertNotNull(((AbstractResourceSession<?, ?>) source).getDerivedResource());
    }
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void backupAndRestoreToolsRefuseUnknownFeatures(final StorageType storageType) throws IOException {
    createStore(storageType);
    final Path backup = directory.resolve("backup");
    final Path restored = directory.resolve("restored");
    Files.createFile(marker());
    for (final StoreFeatures features : new StoreFeatures[] {new StoreFeatures(1, 0, 0), new StoreFeatures(0, 1, 0),
        new StoreFeatures(0, 0, 1)}) {
      replaceBeacon(IOStorage.PRIMARY_BEACON_OFFSET, 2, features);
      replaceBeacon(IOStorage.SECONDARY_BEACON_OFFSET, 2, features);
      final byte[] before = Files.readAllBytes(dataFile());
      assertThrows(UnsupportedStoreFeatureException.class, () -> BackupManager.backupDatabase(databasePath, backup));
      final var failure =
          assertThrows(SirixIOException.class, () -> BackupManager.restoreDatabase(databasePath, restored));
      assertTrue(failure.getMessage().contains("Unknown store feature bits"));
      assertArrayEquals(before, Files.readAllBytes(dataFile()));
      assertFalse(Files.exists(backup));
      assertFalse(Files.exists(restored));
      assertTrue(Files.exists(marker()), "refused backup must not run recovery");
    }
  }

  @Test
  void toolRefusalClosesItsOwnedStorage() {
    createStore(StorageType.FILE_CHANNEL);
    final IOStorage storage = mock(IOStorage.class);
    final Reader reader = mock(Reader.class);
    final var reference = new PageReference();
    reference.setPage(new UberPage(2, new StoreFeatures(0, 0, 1)));
    when(storage.exists()).thenReturn(true);
    when(storage.createReader()).thenReturn(reader);
    when(reader.readUberPageReference()).thenReturn(reference);
    try (final var storageTypes = mockStatic(StorageType.class)) {
      storageTypes.when(() -> StorageType.getStorage(config)).thenReturn(storage);
      assertThrows(UnsupportedStoreFeatureException.class,
          () -> InterruptedFirstCommitRecovery.openStorageForTool(config));
    }
    verify(reader).close();
    verify(storage).close();
  }

  @Test
  void providerSessionConstructionRefusesUnknownIncompatBits() {
    createStore(StorageType.FILE_CHANNEL);
    assertThrows(UnsupportedStoreFeatureException.class, () -> {
      try (final var ignored = sessionBoundary(new StoreFeatures(1, 0, 0), new Semaphore(1))) {
        // Covers providers which supply their own uber page without AbstractReader's beacon path.
      }
    });
  }

  @Test
  void readOnlyFeatureRefusalPrecedesWriterLockContention() {
    createStore(StorageType.FILE_CHANNEL);
    final var unavailableWriterLock = mock(Semaphore.class);
    try (final var session = sessionBoundary(new StoreFeatures(0, 1, 0), unavailableWriterLock)) {
      assertThrows(UnsupportedStoreFeatureException.class, session::beginNodeTrx);
      assertThrows(UnsupportedStoreFeatureException.class, session::createStorageEngineWriter);
      verifyNoInteractions(unavailableWriterLock);
    }
  }

  @SuppressWarnings("unchecked")
  private JsonResourceSessionImpl sessionBoundary(final StoreFeatures features, final Semaphore writeLock) {
    final ResourceStore<JsonResourceSession> resourceStore = mock(ResourceStore.class);
    return new JsonResourceSessionImpl(resourceStore, config, mock(BufferManager.class), mock(IOStorage.class),
        new UberPage(2, features), writeLock, null, "feature-test", mock(StorageEngineWriterFactory.class));
  }

  @Test
  void backupRechecksFeaturesAfterAcquiringItsWriter() {
    createStore(StorageType.FILE_CHANNEL);
    final Path target = directory.resolve("racing-backup");
    final var activated = new StoreFeatures(0, 0, 1);
    try (final var realDatabase = Databases.openJsonDatabase(databasePath);
        final var realSession = realDatabase.beginResourceSession("resource")) {
      // Control the external session boundary deterministically: a real commit activates a feature
      // after backup's read-only preflight and before it acquires its real writer. No sleeps/races.
      final Database<?> database = mock(Database.class);
      final var session = mock(JsonResourceSession.class);
      when(database.listResources()).thenReturn(List.of(config.resourcePath));
      when(database.beginResourceSession("resource")).thenAnswer(invocation -> session);
      when(session.beginNodeReadOnlyTrx()).thenAnswer(invocation -> realSession.beginNodeReadOnlyTrx());
      when(session.getMostRecentRevisionNumber()).thenAnswer(invocation -> realSession.getMostRecentRevisionNumber());
      when(session.beginNodeTrx()).thenAnswer(invocation -> {
        try (final var concurrentWriter = realSession.beginNodeTrx()) {
          concurrentWriter.getStorageEngineWriter().getUberPage().addFeatures(activated);
          concurrentWriter.commit();
        }
        return realSession.beginNodeTrx();
      });
      try (final var databases = mockStatic(Databases.class, CALLS_REAL_METHODS)) {
        databases.when(() -> Databases.openJsonDatabase(databasePath)).thenReturn(database);
        assertThrows(UnsupportedStoreFeatureException.class, () -> BackupManager.backupDatabase(databasePath, target));
      }
      assertFalse(Files.exists(target), "a refused backup must remove its partial target");
    }
    assertStoredFeatures(2, activated);
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void bootstrapRecoveryCannotEraseActiveFeatures(final StorageType storageType) throws IOException {
    for (final int activeSlots : new int[] {1, 2, 3}) {
      for (final boolean unwrittenInactive : new boolean[] {false, true}) {
        createStore(storageType);
        final var active = new StoreFeatures(0, 1, 0);
        replaceBeacon(IOStorage.PRIMARY_BEACON_OFFSET, 1, (activeSlots & 1) != 0
            ? active
            : StoreFeatures.NONE);
        replaceBeacon(IOStorage.SECONDARY_BEACON_OFFSET, 1, (activeSlots & 2) != 0
            ? active
            : StoreFeatures.NONE);
        if (unwrittenInactive && activeSlots != 3) {
          zeroBeacon(activeSlots == 1
              ? IOStorage.SECONDARY_BEACON_OFFSET
              : IOStorage.PRIMARY_BEACON_OFFSET);
        }
        removeRevisionRecords();
        Databases.clearGlobalCaches();
        final byte[] before = Files.readAllBytes(dataFile());
        try (final var database = Databases.openJsonDatabase(databasePath)) {
          assertThrows(RuntimeException.class, () -> database.beginResourceSession("resource"));
        }
        assertArrayEquals(before, Files.readAllBytes(dataFile()));
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void featureFreeBootstrapRepairAcceptsAnUnwrittenSlot(final StorageType storageType) throws IOException {
    for (final long unwrittenSlot : new long[] {IOStorage.PRIMARY_BEACON_OFFSET, IOStorage.SECONDARY_BEACON_OFFSET}) {
      createStore(storageType);
      replaceBeacon(IOStorage.PRIMARY_BEACON_OFFSET, 1, StoreFeatures.NONE);
      replaceBeacon(IOStorage.SECONDARY_BEACON_OFFSET, 1, StoreFeatures.NONE);
      zeroBeacon(unwrittenSlot);
      removeRevisionRecords();
      Databases.clearGlobalCaches();
      try (final var database = Databases.openJsonDatabase(databasePath);
          final var session = database.beginResourceSession("resource")) {
        assertEquals(0, session.getMostRecentRevisionNumber());
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void secondaryFallbackRetainsFeatureWords(final StorageType storageType) throws IOException {
    createStore(storageType);
    final var features = new StoreFeatures(0, 0, Integer.MIN_VALUE);
    replaceBeacon(IOStorage.SECONDARY_BEACON_OFFSET, 2, features);
    zeroBeacon(IOStorage.PRIMARY_BEACON_OFFSET);
    assertStoredFeatures(1, features);
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void committedFeaturesSurviveRollbackPastTheirFirstUseAndNextCommit(final StorageType storageType) {
    createStore(storageType);
    final var features = new StoreFeatures(0, 0, Integer.MIN_VALUE);
    try (final var database = Databases.openJsonDatabase(databasePath);
        final var session = database.beginResourceSession("resource")) {
      try (final var trx = session.beginNodeTrx()) {
        trx.getStorageEngineWriter().getUberPage().addFeatures(features);
        trx.commit();
      }
      try (final var writer = session.createStorageEngineWriter()) {
        writer.truncateTo(1);
        assertEquals(1, session.getMostRecentRevisionNumber());
        try (final var reader = session.beginNodeReadOnlyTrx()) {
          assertEquals(features, reader.getStorageEngineReader().getUberPage().getFeatures());
        }
      }
    }
    assertStoredFeatures(1, features);
    try (final var database = Databases.openJsonDatabase(databasePath);
        final var session = database.beginResourceSession("resource");
        final var trx = session.beginNodeTrx()) {
      trx.commit();
    }
    assertStoredFeatures(2, features);
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void pendingAsyncBaseCannotBypassWriterCompatibility(final StorageType storageType) {
    createStore(storageType);
    try (final var database = Databases.openJsonDatabase(databasePath);
        final var session = database.beginResourceSession("resource")) {
      final var internal = (InternalResourceSession<?, ?>) session;
      final var pending = new UberPage(2, new StoreFeatures(0, 1, 0));
      assertThrows(UnsupportedStoreFeatureException.class, () -> {
        try (final var ignored = internal.createPageTransaction(99, 1, 1, Abort.NO, false, pending)) {
          // A successor epoch must not write through unknown requirements staged by its predecessor.
        }
      });
    }
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void uncommittedStagingDoesNotPublishFeatureWords(final StorageType storageType) {
    createStore(storageType);
    try (final var database = Databases.openJsonDatabase(databasePath);
        final var session = database.beginResourceSession("resource");
        final var trx = session.beginNodeTrx()) {
      trx.getStorageEngineWriter().getUberPage().addFeatures(new StoreFeatures(0, 0, 4));
      try (final var reader = session.beginNodeReadOnlyTrx()) {
        assertEquals(StoreFeatures.NONE, reader.getStorageEngineReader().getUberPage().getFeatures());
      }
      trx.rollback();
    }
    assertStoredFeatures(1, StoreFeatures.NONE);
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void crashBetweenFeatureBeaconWriteAndCommitKeepsCommittedWords(final StorageType storageType) throws IOException {
    createStore(storageType);
    // The secondary was written for revision 2, but the primary still commits revision 1.
    // This future feature must disappear with the uncommitted revision during marker recovery.
    final var committed = new StoreFeatures(0, 0, 4);
    replaceBeacon(IOStorage.PRIMARY_BEACON_OFFSET, 2, committed);
    replaceBeacon(IOStorage.SECONDARY_BEACON_OFFSET, 3, new StoreFeatures(1, 2, 12));
    Files.createFile(marker());
    try (final var database = Databases.openJsonDatabase(databasePath);
        final var session = database.beginResourceSession("resource");
        final var trx = session.beginNodeTrx()) {
      assertEquals(committed, trx.getStorageEngineWriter().getUberPage().getFeatures());
      trx.rollback();
    }
    assertStoredFeatures(1, committed);
    // The repaired secondary must carry the full feature payload, not just the revision count.
    assertArrayEquals(readSlot(IOStorage.PRIMARY_BEACON_OFFSET), readSlot(IOStorage.SECONDARY_BEACON_OFFSET));
  }

  private void assertStoredFeatures(final int revision, final StoreFeatures features) {
    try (final var database = Databases.openJsonDatabase(databasePath);
        final var session = database.beginResourceSession("resource");
        final var trx = session.beginNodeReadOnlyTrx()) {
      assertEquals(revision, trx.getRevisionNumber());
      assertEquals(features, trx.getStorageEngineReader().getUberPage().getFeatures());
    }
  }

  private Path dataFile() {
    return config.resourcePath.resolve("data").resolve(IOStorage.FILENAME);
  }

  private Path revisionsFile() {
    return config.resourcePath.resolve("data").resolve(IOStorage.REVISIONS_FILENAME);
  }

  private void createDerivedStore(final StorageType storageType, final boolean invalidSnapshot) throws IOException {
    databasePath = directory.resolve("derived-" + storeNumber++);
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    final String name = "index-fixture";
    try (final var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("resource")
                                                   .storageType(storageType)
                                                   .useConventionalValidTimePaths()
                                                   .build());
      database.createResource(ResourceConfiguration.newBuilder(name)
                                                   .storageType(storageType)
                                                   .aggregateKernel(true)
                                                   .build());
      try (final var derived = database.beginResourceSession(name); final var writer = derived.beginNodeTrx()) {
        config = derived.getResourceConfig();
        if (invalidSnapshot) {
          try (final var aggregate = new TemporalAggregateStorage(writer.getStorageEngineWriter(), 0)) {
            aggregate.put(ByteBuffer.allocate(5).put((byte) 4).putInt(-1).array(),
                ByteBuffer.allocate(4).putInt(1).array());
          }
        }
        writer.commit();
        if (invalidSnapshot) {
          assertThrows(IllegalStateException.class, () -> DerivedIndexMaintainer.validateSnapshot(derived));
        }
      }
    }
    final Path derivedPath = config.resourcePath.resolveSibling("resource" + DerivedIndexMaintainer.SUFFIX);
    Files.move(config.resourcePath, derivedPath);
    config = ResourceConfiguration.deserialize(derivedPath);
  }

  private Map<Path, byte[]> snapshotResource() throws IOException {
    final Map<Path, byte[]> snapshot = new HashMap<>();
    try (final var paths = Files.walk(config.resourcePath)) {
      for (final Path path : paths.toList()) {
        snapshot.put(config.resourcePath.relativize(path), Files.isDirectory(path) ? null : Files.readAllBytes(path));
      }
    }
    return snapshot;
  }

  private void assertResourceUnchanged(final Map<Path, byte[]> before) throws IOException {
    final Map<Path, byte[]> after = snapshotResource();
    assertEquals(before.keySet(), after.keySet());
    for (final var entry : before.entrySet()) {
      assertArrayEquals(entry.getValue(), after.get(entry.getKey()), entry.getKey().toString());
    }
  }

  private void installDamagedRevisionFixture(final StoreFeatures features) throws IOException {
    final RevisionFileData revision;
    final IOStorage storage = StorageType.getStorage(config);
    try (final Reader reader = storage.createReader()) {
      revision = reader.getRevisionFileData(0, 2)[1];
    } finally {
      storage.close();
    }
    replaceBeacon(IOStorage.PRIMARY_BEACON_OFFSET, 2, features);
    replaceBeacon(IOStorage.SECONDARY_BEACON_OFFSET, 2, features);
    final ByteBuffer entry = ByteBuffer.allocate(48).order(ByteOrder.LITTLE_ENDIAN);
    entry.putInt(1).putInt(0).putLong(revision.offset()).putLong(revision.timestamp().toEpochMilli());
    entry.putLong(IOStorage.expectedRevisionRecordChecksum(revision.offset(), revision.timestamp().toEpochMilli(),
        revision.pageHash())).putLong(revision.pageHash());
    entry.putLong(PageHasher.computeLong(entry.array(), 0, 40, PageHasher.DEFAULT_ALGORITHM));
    try (final var channel = FileChannel.open(dataFile(), StandardOpenOption.WRITE)) {
      for (final long beacon : new long[] {IOStorage.PRIMARY_BEACON_OFFSET, IOStorage.SECONDARY_BEACON_OFFSET}) {
        entry.clear();
        final long offset = beacon + IOStorage.BEACON_SLOT_BYTES - 16L * 48 + 48;
        while (entry.hasRemaining()) {
          channel.write(entry, offset + entry.position());
        }
      }
    }
    try (final var channel = FileChannel.open(revisionsFile(), StandardOpenOption.WRITE)) {
      channel.truncate(IOStorage.revisionsFileOffset(1));
    }
    Databases.clearGlobalCaches();
  }

  private Path marker() {
    return config.resourcePath.resolve(ResourceConfiguration.ResourcePaths.TRANSACTION_INTENT_LOG.getPath())
                              .resolve(".commit");
  }

  private void replaceBeacon(final long offset, final int revisions, final StoreFeatures features) throws IOException {
    final ByteBuffer payload = ByteBuffer.allocate(22).order(ByteOrder.LITTLE_ENDIAN);
    payload.put(new byte[] {3, 0, (byte) 0x80, 14, (byte) 0xc1, 12});
    payload.putInt(features.incompat()).putInt(features.readOnlyCompat()).putInt(features.compat()).putInt(revisions);
    final ByteBuffer slot = ByteBuffer.allocate(IOStorage.BEACON_SLOT_BYTES).order(ByteOrder.LITTLE_ENDIAN);
    slot.putInt(payload.capacity()).put(payload.array());
    slot.order(ByteOrder.BIG_ENDIAN).putLong(PageHasher.computeLong(payload.array()));
    slot.clear();
    try (final var channel = FileChannel.open(dataFile(), StandardOpenOption.WRITE)) {
      while (slot.hasRemaining()) {
        channel.write(slot, offset + slot.position());
      }
      channel.force(true);
    }
  }

  private void removeRevisionRecords() throws IOException {
    try (final var channel = FileChannel.open(config.resourcePath.resolve("data").resolve(IOStorage.REVISIONS_FILENAME),
        StandardOpenOption.WRITE)) {
      channel.truncate(0);
    }
  }

  private void zeroBeacon(final long offset) throws IOException {
    final ByteBuffer slot = ByteBuffer.allocate(IOStorage.BEACON_SLOT_BYTES);
    try (final var channel = FileChannel.open(dataFile(), StandardOpenOption.WRITE)) {
      while (slot.hasRemaining()) {
        channel.write(slot, offset + slot.position());
      }
      channel.force(true);
    }
  }

  private byte[] readSlot(final long offset) throws IOException {
    final ByteBuffer slot = ByteBuffer.allocate(IOStorage.BEACON_SLOT_BYTES);
    try (final var channel = FileChannel.open(dataFile(), StandardOpenOption.READ)) {
      while (slot.hasRemaining()) {
        assertTrue(channel.read(slot, offset + slot.position()) > 0);
      }
    }
    return slot.array();
  }
}
