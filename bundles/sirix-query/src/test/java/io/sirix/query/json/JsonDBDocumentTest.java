package io.sirix.query.json;

import com.google.gson.JsonParser;
import io.brackit.query.ErrorCode;
import io.brackit.query.QueryException;
import io.brackit.query.jdm.type.AnyJsonItemType;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.query.JsonDBSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class JsonDBDocumentTest {
  @TempDir
  Path directory;

  // REST-203: document context identity is key 0 even after deletion of its payload.
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void documentItemsPreserveIdentityAndSerializeTheirSelectedRevision(final boolean empty) {
    final Path path = directory.resolve("json");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (final var database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("res").build());
      final var collection = new JsonDBCollectionImpl("json", database);
      try (final var session = database.beginResourceSession("res")) {
        try (final var writer = session.beginNodeTrx()) {
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"a\":1}"), JsonNodeTrx.Commit.NO);
          writer.commit();
          if (empty) {
            writer.moveToDocumentRoot();
            assertTrue(writer.moveToFirstChild());
            writer.remove();
            writer.commit();
          }
        }
        try (final var cursor = session.beginNodeReadOnlyTrx()) {
          final var item = JsonItemFactory.INSTANCE.getSequence(cursor, collection);
          assertEquals(0, ((JsonDBItem) item).getNodeKey());
          assertSame(collection, ((JsonDBItem) item).getCollection());
          assertSame(session, ((JsonDBItem) item).getResourceSession());
          assertEquals(AnyJsonItemType.ANY_JSON_ITEM, item.itemType());
          assertTrue(item.booleanValue());
          assertEquals(ErrorCode.ERR_ITEM_HAS_NO_TYPED_VALUE,
              assertThrows(QueryException.class, item::atomize).getCode());
          if (!empty) {
            assertTrue(cursor.moveToFirstChild());
          }
          assertSame(cursor, ((JsonDBItem) item).getTrx());
          assertEquals(0, cursor.getNodeKey());
          final StringBuilder output = new StringBuilder();
          try (final var serializer = new JsonDBSerializer(output, false)) {
            serializer.serialize(item);
          }
          final var revision = JsonParser.parseString(output.toString())
                                         .getAsJsonObject()
                                         .getAsJsonArray("rest")
                                         .get(0)
                                         .getAsJsonObject();
          assertEquals(empty
              ? 2
              : 1, revision.get("revisionNumber").getAsInt());
          assertEquals(JsonParser.parseString(empty
              ? "null"
              : "{\"a\":1}"), revision.get("revision"));
        }
      }
    }
  }
}
