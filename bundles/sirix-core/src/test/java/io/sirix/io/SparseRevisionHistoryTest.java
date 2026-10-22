package io.sirix.io;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.access.trx.node.InternalResourceSession;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.SirixRevisionPrunedException;
import io.sirix.io.filechannel.FileChannelStorage;
import io.sirix.io.memorymapped.MMStorage;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class SparseRevisionHistoryTest {
  private static final Instant FIRST = Instant.parse("2020-01-01T00:00:00Z");
  private static final StorageType[] STORAGE_TYPES = {StorageType.FILE_CHANNEL, StorageType.MEMORY_MAPPED};

  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @Test
  void codecDistinguishesTombstonesFromCorruption() {
    final ByteBuffer buffer = ByteBuffer.allocate(40);
    buffer.position(8);
    IOStorage.writePrunedRevisionRecord(buffer, FIRST.toEpochMilli());
    assertEquals(40, buffer.position());
    assertEquals(ByteOrder.BIG_ENDIAN, buffer.order());
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    assertEquals(Long.MIN_VALUE, buffer.getLong(8));
    assertEquals(0x5052554E45440001L, buffer.getLong(32));
    final ByteBuffer checksumInput = ByteBuffer.allocate(24).order(ByteOrder.LITTLE_ENDIAN);
    checksumInput.putLong(Long.MIN_VALUE).putLong(FIRST.toEpochMilli()).putLong(0x5052554E45440001L);
    assertEquals(LongHashFunction.xx3().hashBytes(checksumInput.array()), buffer.getLong(24));
    final RevisionFileData data = IOStorage.decodeRevisionRecord(2, buffer.getLong(8), buffer.getLong(16),
        buffer.getLong(24), buffer.getLong(32));
    assertTrue(data.isPruned());
    assertEquals(FIRST, data.timestamp());
    final SirixRevisionPrunedException pruned =
        assertThrows(SirixRevisionPrunedException.class, () -> data.requireReadable(2));
    assertEquals(2, pruned.getRevision());
    assertEquals(FIRST, pruned.getTimestamp());
    assertTrue(pruned.getMessage().contains("pruned by retention"));
    assertThrows(SirixIOException.class, () -> IOStorage.decodeRevisionRecord(2, buffer.getLong(8), buffer.getLong(16),
        buffer.getLong(24) ^ 1, buffer.getLong(32)));
    for (final long offset : new long[] {Long.MIN_VALUE, -1, 0, IOStorage.DATA_REGION_START - 1, Long.MAX_VALUE}) {
      assertThrows(SirixIOException.class,
          () -> IOStorage.decodeRevisionRecord(2, offset, 1, IOStorage.revisionRecordChecksum(offset, 1, 42), 42));
    }
    // The tag is only reserved for the sentinel offset; a real page may have exactly this hash.
    assertFalse(IOStorage
                         .decodeRevisionRecord(2, IOStorage.DATA_REGION_START, 1,
                             IOStorage.revisionRecordChecksum(IOStorage.DATA_REGION_START, 1,
                                 IOStorage.PRUNED_REVISION_TAG),
                             IOStorage.PRUNED_REVISION_TAG)
                         .isPruned());
    assertThrows(SirixIOException.class, () -> IOStorage.decodeRevisionRecord(2, 0, 0, 0, 0));
    assertThrows(IllegalArgumentException.class, () -> IOStorage.writePrunedRevisionRecord(ByteBuffer.allocate(31), 1));
  }

  @Test
  void denseIndexPreservesMarkersAcrossSearchAppendAndTruncate() {
    final RevisionIndex index = RevisionIndex.create(new long[] {0, 10, 20, 30},
        new long[] {12288, IOStorage.PRUNED_REVISION_OFFSET, IOStorage.PRUNED_REVISION_OFFSET, 16384});
    assertEquals(4, index.size());
    assertEquals(1, index.findRevision(10));
    assertEquals(-4, index.findRevision(25));
    assertTrue(index.isPruned(1));
    assertTrue(index.getRevisionFileData(1).isPruned());
    assertEquals(IOStorage.PRUNED_REVISION_TAG, index.getRevisionFileData(1).pageHash());
    assertArrayEquals(new long[] {0, 10, 20, 30}, index.timestampsMillis(0, 3));
    assertThrows(SirixRevisionPrunedException.class, () -> index.getOffset(1));
    assertThrows(SirixRevisionPrunedException.class, () -> index.truncateTo(2));
    assertThrows(IndexOutOfBoundsException.class, () -> index.isPruned(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> index.isPruned(4));
    final RevisionIndex appended = index.withNewRevision(20000, 40);
    assertEquals(5, appended.size());
    assertTrue(appended.truncateTo(3).isPruned(2));
    assertEquals(4, index.size());
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void readersAndTransactionsPreserveSparseTimeline(final VersioningType versioning) throws Exception {
    for (final StorageType type : STORAGE_TYPES) {
      final Path databasePath = directory.resolve(type.name());
      final ResourceConfiguration config = createHistory(databasePath, type, versioning, false);
      final Path revisions = revisionsPath(config);
      writeTombstone(revisions, 2, FIRST.plusSeconds(2));
      writeTombstone(revisions, 3, FIRST.plusSeconds(3));
      Databases.clearGlobalCaches();
      final IOStorage storage = storage(config);
      try {
        storage.loadRevisionIndex(storage.getRevisionIndexHolder());
        final RevisionIndex index = storage.getRevisionIndexHolder().get();
        assertEquals(6, index.size());
        assertTrue(index.isPruned(2));
        try (final Reader reader = storage.createReader()) {
          assertTrue(reader.getRevisionFileData(2).isPruned());
          assertTrue(reader.getRevisionFileData(0, 6)[3].isPruned());
          assertEquals(FIRST.plusSeconds(2), reader.readRevisionRootPageCommitTimestamp(2));
          assertThrows(SirixRevisionPrunedException.class, () -> reader.readRevisionRootPage(2, config));
          assertEquals(4, reader.readRevisionRootPage(4, config).getRevision());
        }
        try (final Writer writer = storage.createWriter()) {
          assertThrows(SirixRevisionPrunedException.class, () -> writer.truncateTo(2));
        }
      } finally {
        storage.close();
      }
      try (final var database = Databases.openJsonDatabase(databasePath);
          final var session = database.beginResourceSession("history")) {
        assertEquals(0, session.getRevisionNumber(Instant.EPOCH.minusSeconds(1)));
        assertEquals(5, session.getRevisionNumber(FIRST.plusSeconds(50)));
        assertEquals(1, session.getRevisionNumber(FIRST.plusMillis(1500)));
        assertEquals(2, session.getRevisionNumber(FIRST.plusSeconds(2)));
        assertEquals(2, session.getRevisionNumber(FIRST.plusMillis(2500)));
        assertEquals(2, session.getRevisionNumber(FIRST.plusSeconds(50), 2));
        assertEquals(2, session.getRevisionNumber(FIRST.plusMillis(2500), 4));
        assertEquals(0, session.getRevisionNumber(Instant.EPOCH.minusSeconds(1), 4));
        assertArrayEquals(new long[] {FIRST.plusSeconds(3).toEpochMilli(), FIRST.plusSeconds(2).toEpochMilli()},
            session.getHistoryTimestamps(2, 3));
        try (final var before = session.beginNodeReadOnlyTrx(Instant.EPOCH.minusSeconds(1));
            final var preceding = session.beginNodeReadOnlyTrx(FIRST.plusMillis(1500));
            final var after = session.beginNodeReadOnlyTrx(FIRST.plusSeconds(50))) {
          assertEquals(0, before.getRevisionNumber());
          assertEquals(1, preceding.getRevisionNumber());
          assertEquals(5, after.getRevisionNumber());
        }
        assertThrows(SirixRevisionPrunedException.class, () -> session.beginNodeReadOnlyTrx(2));
        assertThrows(SirixRevisionPrunedException.class, () -> session.beginNodeReadOnlyTrx(FIRST.plusMillis(2500)));
        assertThrows(SirixRevisionPrunedException.class, () -> session.createStorageEngineReader(2));
        assertThrows(SirixRevisionPrunedException.class, () -> session.createStorageEngineWriter(2));
        assertThrows(SirixRevisionPrunedException.class, () -> session.openPathSummary(2));
        assertThrows(SirixRevisionPrunedException.class,
            () -> ((InternalResourceSession<?, ?>) session).createPageTransaction(100, 2, 5,
                InternalResourceSession.Abort.NO, false));
        try (final var writer = session.beginNodeTrx()) {
          final byte[] before = Files.readAllBytes(revisions);
          assertThrows(SirixRevisionPrunedException.class, () -> writer.revertTo(2));
          assertThrows(SirixRevisionPrunedException.class, () -> writer.truncateTo(2));
          assertThrows(SirixRevisionPrunedException.class, () -> writer.getStorageEngineWriter().truncateTo(2));
          assertEquals(5, session.getMostRecentRevisionNumber());
          assertArrayEquals(before, Files.readAllBytes(revisions));
          // Failed targets leave the writer usable; rollback still selects its live committed root.
          writer.rollback();
        }
        try (final var reader = session.beginNodeReadOnlyTrx(4)) {
          assertTrue(reader.moveToFirstChild());
          assertTrue(reader.moveToFirstChild());
          assertEquals(4, reader.getNumberValue().intValue());
          assertThrows(SirixRevisionPrunedException.class, () -> reader.getStorageEngineReader().loadRevRoot(2));
        }
      }
      Databases.clearGlobalCaches();
      try (final var database = Databases.openJsonDatabase(databasePath);
          final var session = database.beginResourceSession("history");
          final var writer = session.beginNodeTrx()) {
        writer.commit(null, FIRST.plusSeconds(6));
        assertEquals(6, session.getMostRecentRevisionNumber());
        assertThrows(SirixRevisionPrunedException.class, () -> session.beginNodeReadOnlyTrx(2));
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void duplicateTimestampSelectionIsUnchangedByPruning(final VersioningType versioning) throws Exception {
    for (final StorageType type : STORAGE_TYPES) {
      final Path databasePath = directory.resolve(type.name());
      final ResourceConfiguration config = createHistory(databasePath, type, versioning, true);
      final int selected;
      try (final var database = Databases.openJsonDatabase(databasePath);
          final var session = database.beginResourceSession("history")) {
        selected = session.getRevisionNumber(FIRST.plusSeconds(2));
        assertEquals(3, session.getRevisionNumber(FIRST.plusSeconds(2), 4));
      }
      writeTombstone(revisionsPath(config), selected, FIRST.plusSeconds(2));
      // Both equal-time records are pruned, preventing a live-preference tie from hiding a bug.
      writeTombstone(revisionsPath(config), 3, FIRST.plusSeconds(2));
      Databases.clearGlobalCaches();
      try (final var database = Databases.openJsonDatabase(databasePath);
          final var session = database.beginResourceSession("history")) {
        assertEquals(selected, session.getRevisionNumber(FIRST.plusSeconds(2)));
        assertEquals(3, session.getRevisionNumber(FIRST.plusSeconds(2), 4));
        assertThrows(SirixRevisionPrunedException.class, () -> session.beginNodeReadOnlyTrx(FIRST.plusSeconds(2)));
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void corruptTombstonesAndPrunedEndpointsNeverPublishPartialIndex(final VersioningType versioning) throws Exception {
    for (final StorageType type : STORAGE_TYPES) {
      final ResourceConfiguration config = createHistory(directory.resolve(type.name()), type, versioning, false);
      final Path revisions = revisionsPath(config);
      final byte[] original = Files.readAllBytes(revisions);
      for (int scenario = 0; scenario < 6; scenario++) {
        final int revision = scenario == 0
            ? 0
            : scenario == 1
                ? 5
                : 2;
        Files.write(revisions, original);
        writeTombstone(revisions, revision, revision == 0
            ? Instant.EPOCH
            : FIRST.plusSeconds(revision));
        if (scenario >= 2) {
          try (final FileChannel channel = FileChannel.open(revisions, StandardOpenOption.WRITE)) {
            if (scenario == 2) {
              channel.write(ByteBuffer.wrap(new byte[8]), IOStorage.revisionsFileOffset(revision) + 16);
            } else if (scenario == 5) {
              channel.truncate(IOStorage.revisionsFileOffset(revision) + 16);
            } else {
              final long offset = scenario == 3
                  ? IOStorage.PRUNED_REVISION_OFFSET
                  : -1;
              final long timestamp = FIRST.plusSeconds(revision).toEpochMilli();
              final ByteBuffer invalid = ByteBuffer.allocate(32).order(ByteOrder.LITTLE_ENDIAN);
              invalid.putLong(offset)
                     .putLong(timestamp)
                     .putLong(IOStorage.revisionRecordChecksum(offset, timestamp, 42))
                     .putLong(42)
                     .flip();
              channel.write(invalid, IOStorage.revisionsFileOffset(revision));
            }
          }
        }
        Databases.clearGlobalCaches();
        final IOStorage storage = storage(config);
        final RevisionIndexHolder holder = new RevisionIndexHolder();
        try {
          if (revision == 2) {
            assertThrows(SirixIOException.class, () -> storage.loadRevisionIndex(holder));
          } else {
            assertThrows(SirixRevisionPrunedException.class, () -> storage.loadRevisionIndex(holder));
          }
          assertSame(RevisionIndex.EMPTY, holder.get());
        } finally {
          storage.close();
        }
      }
    }
  }

  private static ResourceConfiguration createHistory(final Path databasePath, final StorageType type,
      final VersioningType versioning, final boolean duplicateTimestamp) {
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("history")
                                                   .storageType(type)
                                                   .versioningApproach(versioning)
                                                   .customCommitTimestamps(true)
                                                   .build());
      try (final var session = database.beginResourceSession("history"); final var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"value\":1}"), JsonNodeTrx.Commit.NO);
        for (int revision = 1; revision <= 5; revision++) {
          writer.moveToDocumentRoot();
          assertTrue(writer.moveToFirstChild());
          assertTrue(writer.moveToFirstChild());
          writer.setNumberValue(revision);
          writer.commit(null, FIRST.plusSeconds(duplicateTimestamp && revision == 3
              ? 2
              : revision));
        }
      }
    }
    Databases.clearGlobalCaches();
    return ResourceConfiguration.deserialize(databasePath.resolve("resources/history"));
  }

  private static Path revisionsPath(final ResourceConfiguration config) {
    return config.resourcePath.resolve("data").resolve(IOStorage.REVISIONS_FILENAME);
  }

  private static IOStorage storage(final ResourceConfiguration config) {
    return config.getStorageType() == StorageType.FILE_CHANNEL
        ? new FileChannelStorage(config, Caffeine.newBuilder().buildAsync())
        : new MMStorage(config, Caffeine.newBuilder().buildAsync());
  }

  private static void writeTombstone(final Path path, final int revision, final Instant timestamp) throws Exception {
    final ByteBuffer buffer = ByteBuffer.allocate(IOStorage.REVISIONS_FILE_RECORD_SIZE);
    IOStorage.writePrunedRevisionRecord(buffer, timestamp.toEpochMilli());
    buffer.flip();
    try (final FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
      long offset = IOStorage.revisionsFileOffset(revision);
      while (buffer.hasRemaining()) {
        offset += channel.write(buffer, offset);
      }
      channel.force(false);
    }
  }
}
