package io.sirix.io.bytepipe;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;
import java.nio.file.Path;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class RepeatedLz4PersistenceTest {
  @ParameterizedTest
  @ValueSource(ints = {1, 63, 65, 4095, 4097})
  void storedFramesExposeOnlyTheirPayload(final int payloadSize) {
    final MemorySegment frame = MemorySegment.ofArray(new byte[Integer.BYTES + payloadSize]);
    frame.set(JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN), 0, -payloadSize);
    final MemorySegment payload = frame.asSlice(Integer.BYTES, payloadSize);
    payload.fill((byte) 42);

    try (final var result = new FFILz4Compressor().decompressScoped(frame)) {
      assertEquals(payloadSize, result.segment().byteSize());
      assertEquals(-1L, payload.mismatch(result.segment()));
      assertTrue(result.backingBuffer().byteSize() >= payloadSize);
      assertEquals(result.backingBuffer().address(), result.segment().address());
      assertEquals(42, result.segment().get(JAVA_BYTE, payloadSize - 1));
    }
  }

  @Test
  void repeatedHandlersOpenWriteTransactionAndColdReadCommittedData(@TempDir final Path tempDir) {
    final Path databasePath = tempDir.resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(
          ResourceConfiguration.newBuilder("resource")
                               .byteHandlerPipeline(
                                   new ByteHandlerPipeline(new FFILz4Compressor(), new FFILz4Compressor()))
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
