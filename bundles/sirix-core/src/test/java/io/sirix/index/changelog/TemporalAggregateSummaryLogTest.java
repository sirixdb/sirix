package io.sirix.index.changelog;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class TemporalAggregateSummaryLogTest {
  private static final byte[] KINDS = {ChangeLogSlab.LONG};

  @TempDir
  File directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void packedGroupsAreAddressableAndBarrierReplacementIsAtomic(final VersioningType versioning) {
    final var path = directory.toPath().resolve("summary");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    final TreeMap<byte[], byte[]> groups = new TreeMap<>(Arrays::compareUnsigned);
    for (int group = 0; group < 500; group++) {
      groups.put(key(group), new byte[] {1, 0, 0, 2, 2, 2, 0});
    }
    final byte[] large = new byte[3000];
    Arrays.fill(large, (byte) 17);
    groups.put(key(501), large);
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("records")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(versioning)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("records");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        write(writer, groups, 0, false);
        final TemporalAggregateSummaryLog.WriteWork work = TemporalAggregateSummaryLog.lastWrite();
        assertEquals(501, work.groups());
        assertTrue(work.slots() < 20, "Small groups share slots instead of a slot per group");
        assertTrue(work.bytes() < 15000);
        writer.commit();
        try (StorageEngineReader source = session.createStorageEngineReader(1)) {
          verifyGroups(source, groups);
          try (var reader =
              new TemporalAggregateSummaryLog.Reader(source, 0, 1, 7, new TemporalAggregateCatchUpBudget.Budget())) {
            final long before = ChangeLogStorage.chunksRead();
            reader.visitGroup(key(501), (key, value) -> assertArrayEquals(large, value));
            assertEquals(2, ChangeLogStorage.chunksRead() - before);
            final long cached = ChangeLogStorage.chunksRead();
            reader.visitGroup(key(501), (key, value) -> assertArrayEquals(large, value));
            assertEquals(cached, ChangeLogStorage.chunksRead());
            reader.visitGroup(key(700), (key, value) -> {
              throw new AssertionError("Absent group");
            });
          }
        }
        write(writer, groups, 0, false);
        final TreeMap<byte[], byte[]> replacement = new TreeMap<>(Arrays::compareUnsigned);
        replacement.put(key(499), new byte[] {9, 8, 7});
        write(writer, replacement, 0, false);
        writer.commit();
        try (StorageEngineReader source = session.createStorageEngineReader(2)) {
          verifyGroups(source, replacement);
        }
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("records");
        StorageEngineReader source = session.createStorageEngineReader(1)) {
      verifyGroups(source, groups);
    }
  }

  @Test
  void missingCoverageUnsupportedAndOversizedGroupsDecline() {
    final var path = directory.toPath().resolve("declines");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(
          ResourceConfiguration.newBuilder("records").storageType(StorageType.FILE_CHANNEL).build());
      try (JsonResourceSession session = database.beginResourceSession("records");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        final TreeMap<byte[], byte[]> groups = new TreeMap<>(Arrays::compareUnsigned);
        groups.put(key(1), null);
        write(writer, groups, 0, false);
        writer.commit();
        try (StorageEngineReader source = session.createStorageEngineReader()) {
          assertThrows(TemporalAggregateCatchUpBudget.LimitExceeded.class,
              () -> new TemporalAggregateSummaryLog.Reader(source, 0, 1, 8,
                  new TemporalAggregateCatchUpBudget.Budget()));
          try (var reader =
              new TemporalAggregateSummaryLog.Reader(source, 0, 1, 7, new TemporalAggregateCatchUpBudget.Budget())) {
            assertThrows(TemporalAggregateCatchUpBudget.LimitExceeded.class, () -> reader.visit((key, value) -> {
              throw new AssertionError("Oversized operand cannot escape");
            }));
          }
        }
        write(writer, groups, -1, true);
        writer.commit();
        try (StorageEngineReader source = session.createStorageEngineReader()) {
          assertThrows(TemporalAggregateCatchUpBudget.LimitExceeded.class,
              () -> new TemporalAggregateSummaryLog.Reader(source, 0, 2, 7,
                  new TemporalAggregateCatchUpBudget.Budget()));
        }
      }
    }
  }

  private static void write(final JsonNodeTrx writer, final TreeMap<byte[], byte[]> groups, final long unsupported,
      final boolean unservable) {
    final ChangeLogSlab slab = new ChangeLogSlab(0, writer.getRevisionNumber(),
        writer.getStorageEngineWriter().getRevisionToRepresent(), KINDS, List.of());
    try (ChangeLogStorage storage = new ChangeLogStorage(writer.getStorageEngineWriter(), 0)) {
      storage.initialize(1, KINDS);
      storage.write(slab);
      TemporalAggregateSummaryLog.write(storage, slab, 7, groups, unsupported, unservable);
    }
  }

  private static void verifyGroups(final StorageEngineReader source, final Map<byte[], byte[]> expected) {
    try (var reader = new TemporalAggregateSummaryLog.Reader(source, 0, source.getRevisionNumber(), 7,
        new TemporalAggregateCatchUpBudget.Budget())) {
      final TreeMap<byte[], byte[]> actual = new TreeMap<>(Arrays::compareUnsigned);
      reader.visit(actual::put);
      assertEquals(expected.size(), actual.size());
      expected.forEach((key, value) -> assertArrayEquals(value, actual.get(key)));
      final long before = ChangeLogStorage.chunksRead();
      reader.visitGroup(key(499), (key, value) -> assertArrayEquals(expected.get(key), value));
      assertEquals(before, ChangeLogStorage.chunksRead(), "A point seek reuses its already-read packed block");
    }
  }

  private static byte[] key(final int group) {
    return ByteBuffer.allocate(10).put((byte) 1).put((byte) 1).putLong(group).array();
  }
}
