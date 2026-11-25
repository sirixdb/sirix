package io.sirix.service.json.serialize;

import com.google.gson.JsonElement;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class JsonEmptyDocumentSerializationTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void emptyRevisionsEmitNullInEveryOutputMode(final boolean bytes) {
    final Path path = directory.resolve("empty");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (final var db = Databases.openJsonDatabase(path)) {
      db.createResource(ResourceConfiguration.newBuilder("res").build());
      try (final var session = db.beginResourceSession("res")) {
        try (final var writer = session.beginNodeTrx()) {
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"a\":1}"), JsonNodeTrx.Commit.NO);
          writer.commit();
          writer.moveToDocumentRoot();
          assertTrue(writer.moveToFirstChild());
          writer.remove();
          writer.commit();
          writer.moveToDocumentRoot();
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"b\":2}"), JsonNodeTrx.Commit.NO);
          writer.commit();
        }
        try (final var cursor = session.beginNodeReadOnlyTrx(2)) {
          assertTrue(cursor.moveTo(0));
          assertFalse(cursor.hasFirstChild());
        }
        for (int metadata = 0; metadata <= 3; metadata++) {
          for (int mode = 0; mode <= 6; mode++) {
            final String empty = serialize(session, bytes, metadata, mode, 2);
            assertEquals("null", empty.trim(), "metadata=" + metadata + ", mode=" + mode);
            final var history = JsonParser.parseString(serialize(session, bytes, metadata, mode, 1, 2, 3))
                                          .getAsJsonObject()
                                          .getAsJsonArray("sirix");
            assertEquals(3, history.size());
            for (int index = 0; index < 3; index++) {
              assertEquals(index + 1, history.get(index).getAsJsonObject().get("revisionNumber").getAsInt());
            }
            final JsonElement emptyRevision = history.get(1).getAsJsonObject().get("revision");
            assertTrue(emptyRevision.isJsonNull());
            if (metadata == 0) {
              assertEquals(JsonParser.parseString("{\"a\":1}"), history.get(0).getAsJsonObject().get("revision"));
              assertEquals(JsonParser.parseString("{\"b\":2}"), history.get(2).getAsJsonObject().get("revision"));
            }
          }
        }
        final var query = new StringWriter();
        JsonSerializer.newBuilder(session, query, 2).startNodeKey(0).isXQueryResultSequence().build().call();
        final var item = JsonParser.parseString(query.toString()).getAsJsonObject();
        assertEquals(2, item.get("revisionNumber").getAsInt());
        assertTrue(item.get("revision").isJsonNull());
        final var directLimited = new StringWriter();
        JsonLimitedSerializer.newBuilder(session, directLimited, 2)
                             .startNodeKey(0)
                             .maxLevel(1)
                             .isXQueryResultSequence()
                             .build()
                             .call();
        assertEquals(item, JsonParser.parseString(directLimited.toString()));
      }
    }
  }

  private static String serialize(final JsonResourceSession session, final boolean bytes, final int metadata,
      final int mode, final int... revisions) {
    final var stream = new ByteArrayOutputStream();
    final var writer = new StringWriter();
    if (mode >= 4) {
      final var builder = bytes
          ? JsonRecordSerializer.newBuilder(session, 2, stream, revisions)
          : JsonRecordSerializer.newBuilder(session, 2, writer, revisions);
      builder.rootNodeKey(0);
      if (mode == 5)
        builder.maxLevel(2);
      if (mode == 6)
        builder.maxChildren(1).numberOfNodes(10);
      switch (metadata) {
        case 1 -> builder.withNodeKeyMetaData(true);
        case 2 -> builder.withNodeKeyAndChildCountMetaData(true);
        case 3 -> builder.withMetaData(true);
        default -> {
          // Mode 0 retains the corresponding builder default.
        }
      }
      builder.build().call();
    } else {
      final var builder = bytes
          ? JsonSerializer.newBuilder(session, stream, revisions)
          : JsonSerializer.newBuilder(session, writer, revisions);
      builder.startNodeKey(0);
      switch (mode) {
        case 1 -> builder.maxLevel(2);
        case 2 -> builder.maxChildren(1);
        case 3 -> builder.numberOfNodes(10);
        default -> {
          // Mode 0 retains the corresponding builder default.
        }
      }
      switch (metadata) {
        case 1 -> builder.withNodeKeyMetaData(true);
        case 2 -> builder.withNodeKeyAndChildCountMetaData(true);
        case 3 -> builder.withMetaData(true);
        default -> {
          // Mode 0 retains the corresponding builder default.
        }
      }
      builder.build().call();
    }
    return bytes
        ? stream.toString(StandardCharsets.UTF_8)
        : writer.toString();
  }
}
