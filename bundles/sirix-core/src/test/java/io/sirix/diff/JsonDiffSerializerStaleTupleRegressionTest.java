package io.sirix.diff;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.objectvalue.NumberValue;
import io.sirix.access.trx.node.json.objectvalue.ObjectValue;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for stale diff tuples in the commit-time update-diff serialization
 * (found by {@code JsonModelBasedOracleTest}, seed 987654321).
 *
 * <p>Updating a node and then removing one of its ancestors within the same transaction used to
 * leave the UPDATED tuple in the operations log with a node key that no longer resolves in the
 * new revision. {@code JsonDiffSerializer} then read from an unpositioned cursor and threw an
 * NPE from {@code getName()} during commit — or, for other node kinds, silently wrote a corrupt
 * diff file.
 */
final class JsonDiffSerializerStaleTupleRegressionTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  void updateDescendantThenRemoveAncestorInSameTransactionCommitsCleanly() throws Exception {
    try (final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
         final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeTrx wtx = session.beginNodeTrx()) {

      // r1: { "a": { "b": 1 }, "keep": "x" }
      wtx.insertObjectAsFirstChild();
      final long rootKey = wtx.getNodeKey();
      wtx.insertObjectRecordAsFirstChild("a", new ObjectValue());
      final long aRecordKey = wtx.getNodeKey();
      wtx.insertObjectRecordAsFirstChild("b", new NumberValue(1));
      final long bRecordKey = wtx.getNodeKey();
      wtx.moveTo(rootKey);
      wtx.insertObjectRecordAsLastChild("keep", new StringValue("x"));
      wtx.commit();

      // r2: update the descendant "b", then remove its ancestor record "a" — the UPDATED tuple
      // for "b" must not survive into the serialized diff. Before the fix this commit threw an
      // NPE from JsonDiffSerializer.
      wtx.moveTo(bRecordKey);
      wtx.setNumberValue(2);
      wtx.moveTo(aRecordKey);
      wtx.remove();
      wtx.commit();

      final JsonArray diffs = readDiffs(session, 1, 2);
      assertEquals(1, diffs.size(), "expected exactly the delete of record 'a': " + diffs);
      final JsonObject only = diffs.get(0).getAsJsonObject();
      assertTrue(only.has("delete"), "expected a delete entry: " + diffs);
      assertEquals(aRecordKey, only.getAsJsonObject("delete").get("nodeKey").getAsLong());
    }
  }

  @Test
  void insertThenRemoveSameNodeInSameTransactionProducesEmptyDiff() throws Exception {
    try (final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
         final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeTrx wtx = session.beginNodeTrx()) {

      // r1: {}
      wtx.insertObjectAsFirstChild();
      final long rootKey = wtx.getNodeKey();
      wtx.commit();

      // r2: insert a record and remove it again within the same transaction — the node exists in
      // neither revision boundary, so the diff must contain no operation referring to it.
      wtx.moveTo(rootKey);
      wtx.insertObjectRecordAsFirstChild("ephemeral", new StringValue("gone"));
      wtx.remove();
      wtx.commit();

      final JsonArray diffs = readDiffs(session, 1, 2);
      assertEquals(0, diffs.size(), "expected an empty diff: " + diffs);
    }
  }

  @Test
  void removeSubtreeWithEarlierInsertedDescendantOmitsTheInsert() throws Exception {
    try (final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
         final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeTrx wtx = session.beginNodeTrx()) {

      // r1: { "a": {} }
      wtx.insertObjectAsFirstChild();
      wtx.insertObjectRecordAsFirstChild("a", new ObjectValue());
      final long aRecordKey = wtx.getNodeKey();
      wtx.commit();

      // r2: insert a fresh descendant under "a", then remove the whole "a" subtree. The
      // INSERTED tuple of the descendant must be purged; only the delete of "a" remains.
      wtx.moveTo(aRecordKey);
      wtx.insertObjectRecordAsFirstChild("fresh", new NumberValue(7));
      wtx.moveTo(aRecordKey);
      wtx.remove();
      wtx.commit();

      final JsonArray diffs = readDiffs(session, 1, 2);
      assertEquals(1, diffs.size(), "expected exactly the delete of record 'a': " + diffs);
      final JsonObject only = diffs.get(0).getAsJsonObject();
      assertTrue(only.has("delete"), "expected a delete entry: " + diffs);
      assertFalse(only.has("insert"), "insert of a removed descendant must not appear: " + diffs);
    }
  }

  private static JsonArray readDiffs(final JsonResourceSession session, final int oldRevision,
      final int newRevision) throws Exception {
    final Path diffFile = session.getResourceConfig()
                                 .getResource()
                                 .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                                 .resolve("diffFromRev" + oldRevision + "toRev" + newRevision + ".json");
    assertTrue(Files.exists(diffFile), "diff file must exist: " + diffFile);
    final JsonObject diff = JsonParser.parseString(Files.readString(diffFile)).getAsJsonObject();
    return diff.getAsJsonArray("diffs");
  }
}
