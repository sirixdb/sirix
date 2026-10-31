package io.sirix.io.bytepipe;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class RepeatedLz4PersistenceTest {
  @Test
  void repeatedHandlersOpenWriteTransactionAndColdReadCommittedData(@TempDir final Path tempDir) {
    final Path databasePath = tempDir.resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("resource")
          .byteHandlerPipeline(new ByteHandlerPipeline(new FFILz4Compressor(), new FFILz4Compressor()))
          .build());
      try (final var session = database.beginResourceSession("resource"); final var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"name\":\"pipeline\"}"));
        writer.commit();
      }
    }
    try (final var database = Databases.openJsonDatabase(databasePath);
        final var session = database.beginResourceSession("resource");
        final var reader = session.beginNodeReadOnlyTrx()) {
      assertTrue(reader.moveTo(2));
      assertEquals("pipeline", reader.getValue());
    }
  }
}
