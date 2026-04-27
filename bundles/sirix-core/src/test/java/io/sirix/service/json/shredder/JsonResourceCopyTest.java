package io.sirix.service.json.shredder;

import io.sirix.JsonTestHelper;
import io.sirix.access.trx.node.json.objectvalue.ArrayValue;
import io.sirix.axis.DescendantAxis;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.serialize.JsonSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class JsonResourceCopyTest {

  private static final Path JSON = Paths.get("src", "test", "resources", "json");

  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void test_when_inserted_as_first_child_or_as_right_sibling_if_is_as_excepted() {
    JsonTestHelper.createTestDocument();
    final var databaseForReads = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    final var databaseForWrites = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH2.getFile());
    try (final var sessionForReads = databaseForReads.beginResourceSession(JsonTestHelper.RESOURCE);
        final var sessionForWrites = databaseForWrites.beginResourceSession(JsonTestHelper.RESOURCE);
        final var rtx = sessionForReads.beginNodeReadOnlyTrx();
        final var wtx = sessionForWrites.beginNodeTrx()) {
      new JsonResourceCopy.Builder(wtx, rtx, InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();

      rtx.moveToDocumentRoot();
      wtx.moveToDocumentRoot();

      var readAxis = new DescendantAxis(rtx);
      var writeAxis = new DescendantAxis(wtx);

      while (readAxis.hasNext()) {
        assertTrue(writeAxis.hasNext());
        assertEquals(readAxis.nextLong(), writeAxis.nextLong());
      }

      assertFalse(writeAxis.hasNext());

      // iter#32 fusion: legacy "tada" OBJECT_KEY+ARRAY pair (keys 15-16) collapses to
      // OBJECT_NAMED_ARRAY at key 10. Legacy trailing empty ARRAY (25) becomes key 17.
      rtx.moveTo(10);
      wtx.moveTo(17);

      new JsonResourceCopy.Builder(wtx, rtx, InsertPosition.AS_RIGHT_SIBLING).commitAfterwards().build().call();

      try (final var writer = new StringWriter()) {
        final var serializer = JsonSerializer.newBuilder(sessionForWrites, writer).build();
        serializer.call();
        assertEquals(Files.readString(JSON.resolve("jsonResourceCopy").resolve("expected1")), writer.toString());
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  @Test
  public void test_copy_all_revisions() {
    JsonTestHelper.createTestDocument();

    final var rtxDatabase = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    final var wtxDatabase = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH2.getFile());

    try (final var wtx = rtxDatabase.beginResourceSession(JsonTestHelper.RESOURCE).beginNodeTrx()) {
      // iter#32 P2 fusion node-key remap of the standard test fixture (verified via
      // DescendantAxis dump):
      //   {"foo":"bar"} OBJECT  legacy 17 -> fused 11
      //   "foo":"bar" inside    legacy 18 (OBJECT_KEY+STRING) -> fused 12 (OBJECT_NAMED_STRING)
      //   {"baz":false} OBJECT  legacy 20 -> fused 13
      //   "baz":false inside    legacy 21 (OBJECT_KEY+BOOLEAN) -> fused 14 (OBJECT_NAMED_BOOLEAN)
      //   "boo" STRING_VALUE    legacy 23 -> fused 15
      //   {} empty OBJECT       legacy 24 -> fused 16
      //   [] empty ARRAY        legacy 25 -> fused 17
      wtx.moveTo(11); // {"foo":"bar"} OBJECT — first element of tada array
      wtx.remove();
      wtx.commit();

      wtx.moveTo(14); // OBJECT_NAMED_BOOLEAN "baz":false (fused) — replace value with array
      wtx.replaceObjectRecordValue(new ArrayValue());
      wtx.commit();

      wtx.moveTo(15); // STRING_VALUE "boo"
      wtx.setStringValue("bar");
      wtx.commit();

      wtx.moveTo(17); // empty trailing ARRAY
      wtx.insertObjectAsFirstChild();
      wtx.commit();
    }

    try (final var sessionForReads = rtxDatabase.beginResourceSession(JsonTestHelper.RESOURCE);
        final var rtx = sessionForReads.beginNodeReadOnlyTrx(1);
        final var sessionForWrites = wtxDatabase.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = sessionForWrites.beginNodeTrx()) {
      new JsonResourceCopy.Builder(wtx, rtx, InsertPosition.AS_FIRST_CHILD).commitAfterwards()
                                                                           .copyAllRevisionsUpToMostRecent()
                                                                           .build()
                                                                           .call();

      assertEquals(sessionForReads.getMostRecentRevisionNumber(), sessionForWrites.getMostRecentRevisionNumber());

      try (final var writer = new StringWriter()) {
        final var serializer = JsonSerializer.newBuilder(sessionForWrites, writer).build();
        serializer.call();
        assertEquals(Files.readString(JSON.resolve("jsonResourceCopy").resolve("expected2")), writer.toString());
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}
