package io.sirix.index.changelog;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.io.StorageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;

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
}
