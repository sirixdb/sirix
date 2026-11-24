package io.sirix.service.json.serialize;

import com.google.gson.JsonParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
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
import java.util.Map;

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

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void namedRecordRootsRetainNamesInPlainAndMetadataOutput(final boolean bytes) {
    final String fixture = "{\"arr\":[1,2,3],\"obj\":{\"a\":1,\"b\":2,\"c\":3},"
        + "\"scalar\":\"first\",\"emptyArray\":[],\"emptyObject\":{},\"q\\\"\\\\\\n水\":[1,2,3]}";
    final Map<String, String> expectedPages = Map.of(
        "arr", "{\"arr\":[1,2]}",
        "obj", "{\"obj\":{\"a\":1,\"b\":2}}",
        "scalar", "{\"scalar\":\"first\"}",
        "emptyArray", "{\"emptyArray\":[]}",
        "emptyObject", "{\"emptyObject\":{}}",
        "q\"\\\n水", "{\"q\\\"\\\\\\n水\":[1,2]}");
    final Path databasePath = directory.resolve("named-records");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("res").hashKind(HashType.NONE).build());
      try (final var session = database.beginResourceSession("res")) {
        try (final var writer = session.beginNodeTrx()) {
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(fixture), JsonNodeTrx.Commit.NO);
          writer.commit();
          writer.commit();
        }
        try (final var cursor = session.beginNodeReadOnlyTrx(1)) {
          assertTrue(cursor.moveToFirstChild());
          assertTrue(cursor.moveToFirstChild());
          do {
            final long root = cursor.getNodeKey();
            final String name = cursor.getName().stringValue();
            final int childCount = switch (name) {
              case "arr", "obj", "q\"\\\n水" -> 3;
              default -> 0;
            };
            for (int metadata = 0; metadata <= 3; metadata++) {
              final var page = JsonParser.parseString(serializeNamedPage(session, bytes, root, 0, metadata, 1))
                                         .getAsJsonObject();
              if (metadata == 0) {
                assertEquals(JsonParser.parseString(expectedPages.get(name)), page);
              } else {
                assertEquals(name, page.get("key").getAsString());
                assertEquals(root, page.getAsJsonObject("metadata").get("nodeKey").getAsLong());
                if (metadata >= 2) {
                  assertEquals(childCount, page.getAsJsonObject("metadata").get("childCount").getAsInt());
                }
                final var values = page.getAsJsonArray("value");
                assertEquals(childCount == 0 ? (name.equals("scalar") ? 1 : 0) : 2, values.size());
                if (childCount > 0) {
                  assertEquals(1, values.get(0).getAsJsonObject().get("value").getAsInt());
                  assertEquals(2, values.get(1).getAsJsonObject().get("value").getAsInt());
                } else if (name.equals("scalar")) {
                  assertEquals("first", values.get(0).getAsJsonObject().get("value").getAsString());
                }
              }
              final var history = JsonParser.parseString(serializeNamedPage(session, bytes, root, 0, metadata, 1, 2))
                                            .getAsJsonObject().getAsJsonArray("sirix");
              assertEquals(2, history.size());
              assertEquals(page, history.get(0).getAsJsonObject().get("revision"));
              assertEquals(page, history.get(1).getAsJsonObject().get("revision"));
              if (metadata > 0 && childCount > 0) {
                assertTrue(cursor.moveToFirstChild());
                final long lastLoaded = cursor.getNodeKey();
                assertTrue(cursor.moveTo(root));
                final var continued = JsonParser.parseString(
                    serializeNamedPage(session, bytes, root, lastLoaded, metadata, 1)).getAsJsonObject();
                assertEquals(name, continued.get("key").getAsString());
                assertEquals(root, continued.getAsJsonObject("metadata").get("nodeKey").getAsLong());
                assertEquals(2, continued.getAsJsonArray("value").size());
                assertEquals(2, continued.getAsJsonArray("value").get(0).getAsJsonObject().get("value").getAsInt());
                assertEquals(3, continued.getAsJsonArray("value").get(1).getAsJsonObject().get("value").getAsInt());
              }
            }
          } while (cursor.moveToRightSibling());
        }
      }
    }
  }

  private static String serializeNamedPage(final JsonResourceSession session, final boolean bytes, final long root,
      final long lastLoaded, final int metadata, final int... revisions) {
    final var stream = new ByteArrayOutputStream();
    final var writer = new StringWriter();
    final var builder = bytes
        ? JsonRecordSerializer.newBuilder(session, 2, stream, revisions)
        : JsonRecordSerializer.newBuilder(session, 2, writer, revisions);
    builder.rootNodeKey(root).startNodeKey(lastLoaded);
    switch (metadata) {
      case 1 -> builder.withNodeKeyMetaData(true);
      case 2 -> builder.withNodeKeyAndChildCountMetaData(true);
      case 3 -> builder.withMetaData(true);
      default -> { }
    }
    builder.build().call();
    return bytes ? stream.toString(StandardCharsets.UTF_8) : writer.toString();
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
