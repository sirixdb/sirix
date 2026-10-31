package io.sirix.query.json;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class JsonCollectionRootTest {
  private static final Instant FIRST = Instant.parse("2026-01-01T00:00:00Z");
  @TempDir
  Path directory;

  @Test
  void everyRootUsesTheFactoryAndTransfersOnlyReturnedReaders() {
    final Path path = directory.resolve("db");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (final var db = Databases.openJsonDatabase(path)) {
      db.createResource(ResourceConfiguration.newBuilder("root")
                                             .storageType(StorageType.FILE_CHANNEL)
                                             .customCommitTimestamps(true)
                                             .build());
      final var collection = new JsonDBCollectionImpl("root", db);
      try (final var session = db.beginResourceSession("root")) {
        final List<Supplier<JsonDBItem>> readers =
            List.of(collection::getDocument, () -> collection.getDocument("root"), () -> collection.getDocument(-1),
                () -> collection.getDocument("root", -1), () -> collection.getDocument(FIRST),
                () -> collection.getDocument("root", FIRST));
        assertEmptyReaders(collection, session, readers);
        final List<String> kinds = List.of("string", "integer", "decimal", "true", "false", "null", "object", "array");
        int revision = 0;
        for (final String kind : kinds) {
          try (final var writer = session.beginNodeTrx()) {
            writer.moveToDocumentRoot();
            if (writer.moveToFirstChild()) {
              writer.remove();
              writer.moveToDocumentRoot();
            }
            switch (kind) {
              case "string" -> writer.insertStringValueAsFirstChild("text");
              case "integer" -> writer.insertNumberValueAsFirstChild(new BigInteger("123456789012345678901234567890"));
              case "decimal" -> writer.insertNumberValueAsFirstChild(new BigDecimal("0.12345678901234567890123456789"));
              case "true", "false" -> writer.insertBooleanValueAsFirstChild(Boolean.parseBoolean(kind));
              case "null" -> writer.insertNullValueAsFirstChild();
              default -> writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(kind.equals("object")
                  ? "{}"
                  : "[]"), JsonNodeTrx.Commit.NO);
            }
            writer.commit(null, FIRST.plusSeconds(revision++));
          }
          final int currentRevision = revision;
          final List<Supplier<JsonDBItem>> currentReaders =
              List.of(collection::getDocument, () -> collection.getDocument("root"),
                  () -> collection.getDocument(currentRevision), () -> collection.getDocument("root", currentRevision),
                  () -> collection.getDocument(FIRST.plusSeconds(currentRevision - 1)),
                  () -> collection.getDocument("root", FIRST.plusSeconds(currentRevision - 1)));
          for (final var reader : currentReaders) {
            final int baseline = session.activeTrxCount();
            final var item = reader.get();
            assertNotNull(item, kind);
            assertSame(collection, item.getCollection());
            assertEquals(currentRevision, item.getTrx().getRevisionNumber());
            assertFalse(item.getTrx().isClosed());
            assertEquals(baseline + 1, session.activeTrxCount());
            item.getTrx().close();
            assertEquals(baseline, session.activeTrxCount());
          }
          final int baseline = session.activeTrxCount();
          final JsonDBItem item;
          try (final var stream = collection.getDocuments()) {
            item = stream.next();
            assertNotNull(item, kind);
            assertNull(stream.next());
          }
          assertFalse(item.getTrx().isClosed());
          assertEquals(baseline + 1, session.activeTrxCount());
          item.getTrx().close();
          assertEquals(baseline, session.activeTrxCount());
        }
        try (final var writer = session.beginNodeTrx()) {
          writer.moveToDocumentRoot();
          writer.moveToFirstChild();
          writer.remove();
          writer.commit(null, FIRST.plusSeconds(revision));
        }
        assertEmptyReaders(collection, session, List.of(collection::getDocument, () -> collection.getDocument("root")));
      }
    }
  }

  private static void assertEmptyReaders(final JsonDBCollection collection, final JsonResourceSession session,
      final List<Supplier<JsonDBItem>> readers) {
    final int baseline = session.activeTrxCount();
    for (final var reader : readers) {
      assertNull(reader.get());
      assertEquals(baseline, session.activeTrxCount());
    }
    try (final var stream = collection.getDocuments()) {
      assertNull(stream.next());
    }
    assertEquals(baseline, session.activeTrxCount());
  }
}
