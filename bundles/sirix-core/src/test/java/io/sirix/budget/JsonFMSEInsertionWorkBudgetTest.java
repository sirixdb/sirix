package io.sirix.budget;

import com.google.gson.stream.JsonReader;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.diff.algorithm.fmse.json.JsonFMSE;
import io.sirix.io.StorageType;
import io.sirix.metrics.TransactionMetrics;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class JsonFMSEInsertionWorkBudgetTest {
  private static final WorkCounter READERS = WorkCounter.alwaysOn("nodeReadersOpened",
      "read transactions opened while inserting primitive object fields", TransactionMetrics::totalReadOnlyTrxOpened);

  @TempDir
  Path directory;

  @ParameterizedTest
  @CsvSource({"0,16", "1,16", "2,16", "3,16", "0,128"})
  void primitiveFieldsReuseTheExistingReader(final int firstKind, final int fields) throws Exception {
    final String expected = object(firstKind, fields);
    final Path oldPath = directory.resolve("old");
    final Path newPath = directory.resolve("new");
    createDatabase(oldPath, "{}");
    createDatabase(newPath, expected);
    Databases.clearGlobalCaches();
    try (final var oldDatabase = Databases.openJsonDatabase(oldPath);
        final var newDatabase = Databases.openJsonDatabase(newPath);
        final var oldSession = oldDatabase.beginResourceSession("doc");
        final var newSession = newDatabase.beginResourceSession("doc")) {
      try (final var writer = oldSession.beginNodeTrx();
          final var reader = newSession.beginNodeReadOnlyTrx();
          final var diff = JsonFMSE.createInstance()) {
        final var capture = WorkCapture.of(READERS).run(() -> diff.diff(writer, reader));
        capture.assertExactly(READERS, 0, "FMSE must not open a reader per primitive field");
      }
      assertEquals(2, oldSession.getMostRecentRevisionNumber());
      assertJson(expected, oldSession, 2);
      assertJson("{}", oldSession, 1);
    }
  }

  @Test
  void subtreeCopyProvidesANonzeroReaderControl() throws Exception {
    final Path oldPath = directory.resolve("old");
    final Path newPath = directory.resolve("new");
    createDatabase(oldPath, "{}");
    createDatabase(newPath, "{\"field\":42}");
    Databases.clearGlobalCaches();
    try (final var oldDatabase = Databases.openJsonDatabase(oldPath);
        final var newDatabase = Databases.openJsonDatabase(newPath);
        final var oldSession = oldDatabase.beginResourceSession("doc");
        final var newSession = newDatabase.beginResourceSession("doc");
        final var writer = oldSession.beginNodeTrx();
        final var reader = newSession.beginNodeReadOnlyTrx()) {
      assertTrue(writer.moveToFirstChild());
      assertTrue(reader.moveToFirstChild());
      assertTrue(reader.moveToFirstChild());
      final var capture = WorkCapture.of(READERS).run(() -> writer.copySubtreeAsFirstChild(reader));
      capture.assertExactly(READERS, 1, "the former per-field copy path must register a reader");
      writer.commit();
      assertJson("{\"field\":42}", oldSession, 2);
    }
  }

  private static String object(final int firstKind, final int fields) {
    final StringBuilder json = new StringBuilder(fields * 32).append('{');
    for (int field = 0; field < fields; field++) {
      if (field != 0) {
        json.append(',');
      }
      json.append('"').append('k').append(field).append("\":");
      switch ((field + firstKind) % 4) {
        case 0 -> json.append("9007199254740993");
        case 1 -> json.append('"').append('v').append(field).append('"');
        case 2 -> json.append(field % 8 < 4);
        case 3 -> json.append("null");
        default -> throw new AssertionError();
      }
    }
    return json.append('}').toString();
  }

  private static void createDatabase(final Path path, final String content) throws Exception {
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (final var database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("doc")
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .hashKind(HashType.NONE)
                                                              .buildPathSummary(false)
                                                              .useDeweyIDs(false)
                                                              .build()));
      try (final var session = database.beginResourceSession("doc");
          final var writer = session.beginNodeTrx();
          final JsonReader reader = JsonShredder.createStringReader(content)) {
        new JsonShredder.Builder(writer, reader, InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
      }
    }
  }

  private static void assertJson(final String expected, final JsonResourceSession session, final int revision) {
    final StringBuilder result = new StringBuilder();
    new JsonSerializer.Builder(session, result, revision).build().call();
    assertEquals(expected, result.toString());
  }
}
