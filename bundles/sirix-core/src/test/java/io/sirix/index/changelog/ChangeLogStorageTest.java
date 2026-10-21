package io.sirix.index.changelog;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.io.StorageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

final class ChangeLogStorageTest {
  @Test
  void wideProvenanceIsChunkedAndVersioned(@TempDir final Path directory) {
    final Path path = directory.resolve("wide-log");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("data").storageType(StorageType.FILE_CHANNEL).build());
      try (JsonResourceSession session = database.beginResourceSession("data");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        writer.insertObjectAsFirstChild();
        final byte[] kinds = new byte[4096];
        Arrays.fill(kinds, ChangeLogSlab.LONG);
        final ChangeLogSlab.Row row = new ChangeLogSlab.Row(true, kinds.length);
        row.flags[0] = ChangeLogSlab.PRESENT | ChangeLogSlab.NON_INTEGRAL;
        row.flags[4095] = ChangeLogSlab.PRESENT | ChangeLogSlab.UNREPRESENTABLE;
        try (ChangeLogStorage log = new ChangeLogStorage(writer.getStorageEngineWriter(), 0)) {
          log.initialize(1, kinds);
          log.provenance(1, row);
          assertEquals(4097, Objects.requireNonNull(log.provenance(1)).length);
        }
        writer.commit();
        try (ChangeLogStorage log = new ChangeLogStorage(writer.getStorageEngineWriter(), 0)) {
          assertEquals(ChangeLogSlab.NON_INTEGRAL, Objects.requireNonNull(log.provenance(1))[1]);
          assertEquals(ChangeLogSlab.UNREPRESENTABLE, Objects.requireNonNull(log.provenance(1))[4096]);
          log.provenance(1, null);
          assertNull(log.provenance(1));
        }
        writer.commit();
        writer.revertTo(1);
        try (ChangeLogStorage log = new ChangeLogStorage(writer.getStorageEngineWriter(), 0)) {
          assertEquals(ChangeLogSlab.NON_INTEGRAL, Objects.requireNonNull(log.provenance(1))[1]);
          assertEquals(ChangeLogSlab.UNREPRESENTABLE, Objects.requireNonNull(log.provenance(1))[4096]);
        }
        writer.rollback();
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 5})
  void narrowEmptySlabsSurviveCommitAndReopen(final int columns, @TempDir final Path directory) {
    final Path path = directory.resolve("empty-log");
    final byte[] kinds = new byte[columns];
    Arrays.fill(kinds, ChangeLogSlab.LONG);
    final ChangeLogSlab slab = new ChangeLogSlab(0, 1, 0, kinds, List.of());
    assertEquals(30 + columns, slab.encode().length);
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("data").storageType(StorageType.FILE_CHANNEL).build());
      try (JsonResourceSession session = database.beginResourceSession("data");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        try (ChangeLogStorage log = new ChangeLogStorage(writer.getStorageEngineWriter(), 0)) {
          log.initialize(1, kinds);
          log.write(slab);
        }
        writer.commit();
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("data");
        StorageEngineReader reader = session.createStorageEngineReader(1)) {
      final ChangeLogSlab decoded = ChangeLogStorage.read(reader, 0, 1);
      assertEquals(0, decoded.projection());
      assertEquals(1, decoded.revision());
      assertEquals(0, decoded.baseRevision());
      assertArrayEquals(kinds, decoded.kinds());
      assertEquals(0, decoded.entries().size());
      assertArrayEquals(slab.encode(), decoded.encode());
    }
  }
}
