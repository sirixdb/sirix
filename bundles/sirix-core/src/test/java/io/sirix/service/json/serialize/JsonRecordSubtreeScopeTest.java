package io.sirix.service.json.serialize;

import com.google.gson.JsonParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class JsonRecordSubtreeScopeTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void recordRootsSelectOnlyTheirOwnSubtrees(final boolean bytes) {
    final Path databasePath = directory.resolve("records");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("res").build());
      try (final var session = database.beginResourceSession("res")) {
        try (final var writer = session.beginNodeTrx()) {
          writer.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader("[{\"a\":\"first\",\"c\":\"continued\"},{\"b\":\"second\"},\"primitive\"]"),
              JsonNodeTrx.Commit.NO);
          writer.commit();
        }
        try (final var cursor = session.beginNodeReadOnlyTrx()) {
          assertTrue(cursor.moveToFirstChild());
          final long arrayKey = cursor.getNodeKey();
          assertTrue(cursor.moveToFirstChild());
          final long objectKey = cursor.getNodeKey();
          assertTrue(cursor.moveToFirstChild());
          final long fieldKey = cursor.getNodeKey();
          assertTrue(cursor.moveTo(objectKey));
          assertTrue(cursor.moveToRightSibling());
          assertTrue(cursor.moveToRightSibling());
          final long valueKey = cursor.getNodeKey();

          assertPayload(session, bytes, arrayKey, 0, "[{\"a\":\"first\",\"c\":\"continued\"},{\"b\":\"second\"}]");
          assertPayload(session, bytes, objectKey, 0, "{\"a\":\"first\",\"c\":\"continued\"}");
          assertPayload(session, bytes, fieldKey, 0, "{\"a\":\"first\"}");
          assertPayload(session, bytes, valueKey, 0, "\"primitive\"");
          assertPayload(session, bytes, objectKey, fieldKey, "{\"value\":[{\"c\":\"continued\"}]}");
          assertPayload(session, bytes, objectKey, objectKey, "{\"value\":[]}");
        }
      }
    }
  }

  private static void assertPayload(final JsonResourceSession session, final boolean bytes, final long root,
      final long cursor, final String expected) {
    final var stream = new ByteArrayOutputStream();
    final var writer = new StringWriter();
    final var builder = bytes
        ? JsonRecordSerializer.newBuilder(session, 2, stream)
        : JsonRecordSerializer.newBuilder(session, 2, writer);
    builder.rootNodeKey(root).startNodeKey(cursor).build().call();
    final String actual = bytes ? stream.toString(StandardCharsets.UTF_8) : writer.toString();
    assertEquals(JsonParser.parseString(expected), JsonParser.parseString(actual));
  }
}
