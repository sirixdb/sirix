package io.sirix.io.ram;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.io.StorageType;
import io.sirix.io.Writer;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.bytepipe.DeflateCompressor;
import io.sirix.io.bytepipe.FFILz4Compressor;
import io.sirix.exception.SirixIOException;
import io.sirix.node.Bytes;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.UberPage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Public commit visibility under VER-101/102 and exact timestamp readback under VER-201. */
final class RAMStorageCommitTest {
  @TempDir
  private Path directory;

  @ParameterizedTest
  @ValueSource(strings = {"none", "lz4", "deflate"})
  void committedValueAndTimestampRemainReadableAfterReopeningWriter(final String compression) {
    // Literal VER-201 microsecond fixtures reject millisecond truncation; VER-101 requires head 1.
    final Instant first = Instant.parse("2021-03-04T05:06:07.123456Z");
    final Instant second = Instant.parse("2021-03-04T05:06:07.123457Z");
    final ByteHandlerPipeline pipeline = switch (compression) {
      case "lz4" -> new ByteHandlerPipeline(new FFILz4Compressor());
      case "deflate" -> new ByteHandlerPipeline(new DeflateCompressor());
      default -> new ByteHandlerPipeline();
    };
    Databases.createJsonDatabase(new DatabaseConfiguration(directory));
    try (final var database = Databases.openJsonDatabase(directory)) {
      database.createResource(ResourceConfiguration.newBuilder("ram")
          .storageType(StorageType.IN_MEMORY).byteHandlerPipeline(pipeline).customCommitTimestamps(true).build());
      try (final var session = database.beginResourceSession("ram")) {
        try (final var writer = session.beginNodeTrx()) {
          writer.insertObjectAsFirstChild();
          writer.insertObjectRecordAsFirstChild("value", new StringValue("committed"));
          writer.commit(null, first);
          assertEquals(1, session.getMostRecentRevisionNumber());
        }
        Databases.clearCachesForDatabase(database.getDatabaseConfig().getDatabaseId());
        try (final var reader = session.beginNodeReadOnlyTrx(1)) {
          assertEquals(first, reader.getRevisionTimestamp());
          assertPayload(reader, "committed");
        }
        try (final var writer = session.beginNodeTrx()) {
          assertPayload(writer, "committed");
          writer.setStringValue("updated");
          writer.commit(null, second);
        }
        assertEquals(2, session.getMostRecentRevisionNumber());
        Databases.clearCachesForDatabase(database.getDatabaseConfig().getDatabaseId());
        assertArrayEquals(new long[] {1_614_834_367_123_457L, 1_614_834_367_123_456L},
            session.getHistoryTimestampsMicros());
        assertEquals(1, session.getRevisionNumber(first));
        assertEquals(2, session.getRevisionNumber(second));
        try (final var reader = session.beginNodeReadOnlyTrx(1)) {
          assertEquals(first, reader.getRevisionTimestamp());
          assertPayload(reader, "committed");
        }
        try (final var reader = session.beginNodeReadOnlyTrx(2)) {
          assertEquals(second, reader.getRevisionTimestamp());
          assertPayload(reader, "updated");
        }
      }
      try (final var session = database.beginResourceSession("ram");
          final var reader = session.beginNodeReadOnlyTrx(2)) {
        assertEquals(2, session.getMostRecentRevisionNumber());
        assertEquals(second, reader.getRevisionTimestamp());
        assertPayload(reader, "updated");
      }
    }
  }

  private static void assertPayload(final JsonNodeReadOnlyTrx reader, final String expected) {
    reader.moveToDocumentRoot();
    assertTrue(reader.moveToFirstChild());
    assertTrue(reader.moveToFirstChild());
    assertEquals("value", reader.getName().getLocalName());
    assertEquals(expected, reader.getValue());
  }

  @Test
  void storageOwnsWrittenSnapshotsAndTruncationRemovesTheirRevisionMetadata() {
    final Instant timestamp = Instant.parse("2021-03-04T05:06:07.123456Z");
    Databases.createJsonDatabase(new DatabaseConfiguration(directory));
    try (final var database = Databases.openJsonDatabase(directory)) {
      database.createResource(ResourceConfiguration.newBuilder("ram").storageType(StorageType.IN_MEMORY).build());
      try (final var session = database.beginResourceSession("ram");
          final var bytes = Bytes.elasticHeapByteBuffer()) {
        final RAMStorage storage = new RAMStorage(session.getResourceConfig());
        final Writer writer = storage.createWriter();
        final RevisionRootPage root = new RevisionRootPage();
        root.assignCommitTimestampMicros(timestamp);
        final PageReference beacon = new PageReference();
        // Writer's fluent contract lets a caller publish after writing the revision root.
        writer.write(session.getResourceConfig(), new PageReference(), root, bytes)
            .writeUberPageReference(session.getResourceConfig(), beacon, new UberPage(1), bytes)
            .forceAll();
        // PageReference keys are storage offsets; negative keys denote unwritten references.
        assertTrue(beacon.getKey() >= 0, "published references must have valid storage keys");
        root.assignCommitTimestampMicros(timestamp.plusSeconds(1));
        bytes.clear();
        assertEquals(timestamp, storage.createReader().readRevisionRootPageCommitTimestamp(0));
        writer.truncate();
        assertFalse(storage.exists());
        final SirixIOException missing = assertThrows(SirixIOException.class,
            () -> storage.createReader().readRevisionRootPageCommitTimestamp(0));
        assertTrue(missing.getMessage().contains("revision 0"), "missing revision diagnostics identify the request");
      }
    }
  }
}
