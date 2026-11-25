package io.sirix.index.aggregate;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class DerivedRevisionMappingTest {
  @TempDir
  File directory;

  @Test
  void windowsBoundHistoricalReadsPreserveGapsAndExcludeTheFrontier() {
    final var path = directory.toPath().resolve("mappings");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("index").storageType(StorageType.FILE_CHANNEL).build());
      try (JsonResourceSession session = database.beginResourceSession("index");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        for (int revision = 1; revision <= 130; revision++) {
          if (revision != 65) {
            mapping(revision).write(writer.getStorageEngineWriter());
          }
        }
        mapping(Integer.MAX_VALUE - 1).write(writer.getStorageEngineWriter());
        mapping(Integer.MAX_VALUE).write(writer.getStorageEngineWriter());
        writer.commit();

        try (StorageEngineReader reader = session.createStorageEngineReader()) {
          final List<DerivedRevisionMapping> first = DerivedRevisionMapping.readWindow(reader, 1);
          assertEquals(64, first.size());
          for (int offset = 0; offset < first.size(); offset++) {
            assertEquals(mapping(offset + 1), first.get(offset));
          }
          final List<DerivedRevisionMapping> crossing = DerivedRevisionMapping.readWindow(reader, 64);
          assertEquals(63, crossing.size());
          assertEquals(mapping(64), crossing.getFirst());
          assertEquals(mapping(66), crossing.get(1), "Missing batch interiors must remain absent");
          assertEquals(mapping(127), crossing.getLast());
          assertTrue(DerivedRevisionMapping.readWindow(reader, 131).isEmpty());
          assertEquals(List.of(mapping(Integer.MAX_VALUE - 1), mapping(Integer.MAX_VALUE)),
              DerivedRevisionMapping.readWindow(reader, Integer.MAX_VALUE - 1));
          assertEquals(List.of(mapping(Integer.MAX_VALUE)),
              DerivedRevisionMapping.readWindow(reader, Integer.MAX_VALUE));
          assertEquals(mapping(64), DerivedRevisionMapping.floor(reader, 65, 16));
          assertEquals(mapping(1), DerivedRevisionMapping.floor(reader, 1, 16));
          assertEquals(mapping(130), DerivedRevisionMapping.floor(reader, 146, 16));
          assertNull(DerivedRevisionMapping.floor(reader, 147, 16));
          assertNull(DerivedRevisionMapping.floor(reader, 65, 0));
          assertEquals(mapping(Integer.MAX_VALUE), DerivedRevisionMapping.floor(reader, Integer.MAX_VALUE, 16));
          assertEquals(mapping(130), DerivedRevisionMapping.floor(reader, 2048));
          assertEquals(mapping(130), DerivedRevisionMapping.floor(reader, Integer.MAX_VALUE - 2));
          assertEquals(mapping(130), DerivedRevisionMapping.floor(reader, Integer.MAX_VALUE - 2, Integer.MAX_VALUE));
          assertEquals(mapping(Integer.MAX_VALUE), DerivedRevisionMapping.floor(reader, Integer.MAX_VALUE));
          assertThrows(IllegalArgumentException.class, () -> DerivedRevisionMapping.floor(reader, 0));
          assertEquals(131, DerivedRevisionMapping.readAll(reader).size());
          assertEquals(Integer.MAX_VALUE, DerivedRevisionMapping.frontier(reader));
          assertThrows(IllegalArgumentException.class, () -> DerivedRevisionMapping.readWindow(reader, 0));
          assertThrows(IllegalArgumentException.class, () -> DerivedRevisionMapping.readWindow(reader, -1));
        }
      }
    }
  }

  @Test
  void backupValidationBoundsEveryMappingIncludingThoseBeyondTheFrontier() {
    final var path = directory.toPath().resolve("backup-bound");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("index").storageType(StorageType.FILE_CHANNEL).build());
      try (JsonResourceSession session = database.beginResourceSession("index")) {
        try (StorageEngineReader empty = session.createStorageEngineReader()) {
          DerivedIndexMaintainer.validateBackupSnapshot(empty, 0);
          assertThrows(IllegalArgumentException.class, () -> DerivedIndexMaintainer.validateBackupSnapshot(empty, -1));
        }
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          mapping(2).write(writer.getStorageEngineWriter());
          mapping(1).write(writer.getStorageEngineWriter());
          writer.commit();
        }
        try (StorageEngineReader reader = session.createStorageEngineReader()) {
          assertEquals(1, DerivedRevisionMapping.frontier(reader));
          DerivedIndexMaintainer.validateBackupSnapshot(reader, 2);
          assertThrows(IllegalStateException.class, () -> DerivedIndexMaintainer.validateBackupSnapshot(reader, 1),
              "a mapping past the copied source head must be rejected even when the frontier fits");
          assertThrows(IllegalStateException.class, () -> DerivedIndexMaintainer.validateBackupSnapshot(reader, 0));
        }
      }
    }
  }

  @Test
  void backupValidationRefusesAFrontierWithoutItsDurableMapping() {
    final var path = directory.toPath().resolve("missing-frontier-mapping");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("index").storageType(StorageType.FILE_CHANNEL).build());
      try (JsonResourceSession session = database.beginResourceSession("index")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          mapping(1).write(writer.getStorageEngineWriter());
          // Persisted DerivedRevisionMapping protocol: prefix 4, key -1 is the restart frontier.
          // Model a damaged restart record advertising source 2 without its corresponding mapping.
          try (TemporalAggregateStorage storage = new TemporalAggregateStorage(writer.getStorageEngineWriter(), 0)) {
            storage.put(ByteBuffer.allocate(5).put((byte) 4).putInt(-1).array(),
                ByteBuffer.allocate(4).putInt(2).array());
          }
          writer.commit();
        }
        try (StorageEngineReader reader = session.createStorageEngineReader()) {
          assertThrows(IllegalStateException.class, () -> DerivedIndexMaintainer.validateBackupSnapshot(reader, 2),
              "TXN-612/613: bounds alone cannot make a frontier with a missing mapping restartable");
        }
      }
    }
  }

  private static DerivedRevisionMapping mapping(final int revision) {
    return new DerivedRevisionMapping(revision, 1, 0, revision * 1000L, revision, -revision);
  }
}
