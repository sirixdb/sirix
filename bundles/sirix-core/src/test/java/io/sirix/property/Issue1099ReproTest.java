package io.sirix.property;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.trx.node.json.objectvalue.ArrayValue;
import io.sirix.access.trx.node.json.objectvalue.NullValue;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.node.NodeKind;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Regression test for issue #1099 (found by the model-based oracle, seed 1207632449600).
 *
 * <p>Removing an array element whose subtree holds the only references to nested
 * {@code __array__} path-summary entries removes those entries and fixes up the neighbouring
 * records — but the fix-ups go through {@code prepareRecordForModification}, which installs
 * fresh copies in the reader's node mapping while the stale twins stay wired into the
 * in-memory parent/child/sibling references created at bulk-load. A subsequent
 * {@code removeField} in the SAME transaction walked those stale references
 * ({@code PathSummaryReader.moveToFirstChild} fast path), resurrected the removed
 * {@code __array__} entry, and crashed with "Failed to move to nodeKey: N".
 *
 * <p>The distilled sequence needs: a field whose array gains a nested array in a later
 * revision than the field itself, removal of that array element, then removal of the field —
 * all without an intervening commit after the element removal.
 */
final class Issue1099ReproTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  void removeFieldAfterNestedArrayElementRemovalInSameTransaction() {
    try (final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
         final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeTrx wtx = session.beginNodeTrx()) {

      wtx.insertObjectAsFirstChild();
      wtx.commit();

      // Churn on removed-and-recreated fields, mirroring the oracle sequence: exercises the
      // path-summary remove/recreate machinery before the interesting revisions.
      toRootObject(wtx);
      wtx.insertObjectRecordAsLastChild("k5109", new ArrayValue());
      toRootObject(wtx);
      moveToRecord(wtx, "k5109");
      wtx.remove();
      toRootObject(wtx);
      wtx.insertObjectRecordAsLastChild("k1208", new StringValue(""));
      toRootObject(wtx);
      moveToRecord(wtx, "k1208");
      wtx.remove();
      toRootObject(wtx);
      wtx.insertObjectRecordAsFirstChild("k2859", new NullValue());
      wtx.commit();

      toRootObject(wtx);
      wtx.insertObjectRecordAsLastChild("k6479", new NullValue());
      toRootObject(wtx);
      wtx.insertObjectRecordAsLastChild("k3139", new ArrayValue());
      wtx.commit();

      // Build [[]] under k3139 across this revision: the inner arrays create the nested
      // __array__ path-summary entries the later removal must clean up.
      toRootObject(wtx);
      moveToRecord(wtx, "k3139");
      wtx.insertArrayAsFirstChild();
      toRootObject(wtx);
      moveToRecord(wtx, "k3139");
      moveToChild(wtx, 0);
      wtx.insertArrayAsFirstChild();
      toRootObject(wtx);
      wtx.insertObjectRecordAsFirstChild("k2239", new StringValue("aizjotz"));
      wtx.commit();

      // Remove the array element (with its nested array), then every field — in ONE
      // transaction. Before the fix, removing k3139 crashed with
      // "Failed to move to nodeKey: 9" inside PathSummaryWriter.removePathSummaryNode.
      toRootObject(wtx);
      moveToRecord(wtx, "k3139");
      moveToChild(wtx, 0);
      wtx.remove();

      for (final String key : new String[] {"k2239", "k2859", "k6479", "k3139"}) {
        toRootObject(wtx);
        moveToRecord(wtx, key);
        wtx.remove();
      }
      wtx.commit();

      toRootObject(wtx);
      assertTrue(!wtx.hasFirstChild(), "root object must be empty after removing all fields");
    }
  }

  /**
   * Removing an array element that is itself an array must decrement the removed subtree
   * ROOT's {@code __array__} path-summary entry too — the PostOrderAxis removal loop covers
   * only descendants, so before the fix the root's entry leaked with a reference count no
   * document node backs.
   */
  @Test
  void removingNestedArrayElementReleasesItsArrayPathEntry() {
    try (final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
         final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeTrx wtx = session.beginNodeTrx()) {

      wtx.insertObjectAsFirstChild();
      wtx.commit();

      // {"field": [[[]]]} — three __array__ path levels: field's own layer + two nested.
      toRootObject(wtx);
      wtx.insertObjectRecordAsLastChild("field", new ArrayValue());
      toRootObject(wtx);
      moveToRecord(wtx, "field");
      wtx.insertArrayAsFirstChild();
      wtx.insertArrayAsFirstChild();
      wtx.commit();
      assertEquals(3, countArrayPathEntries(session), "three nested array layers expected");

      // Remove the outer element (an array containing an array): both nested layers must go;
      // only the field's own __array__ layer stays (still referenced by the field itself).
      toRootObject(wtx);
      moveToRecord(wtx, "field");
      moveToChild(wtx, 0);
      wtx.remove();
      wtx.commit();
      assertEquals(1, countArrayPathEntries(session),
          "removed nested arrays must release their __array__ path-summary entries");
    }
  }

  private static int countArrayPathEntries(final JsonResourceSession session) {
    try (final PathSummaryReader summary = session.openPathSummary()) {
      summary.moveToDocumentRoot();
      int count = 0;
      for (final var axis = new DescendantAxis(summary); axis.hasNext();) {
        axis.nextLong();
        if (summary.getName() != null && "__array__".equals(summary.getName().getLocalName())) {
          count++;
        }
      }
      return count;
    }
  }

  private static void toRootObject(final JsonNodeTrx wtx) {
    wtx.moveToDocumentRoot();
    assertTrue(wtx.moveToFirstChild(), "document root must have a child");
  }

  private static void moveToRecord(final JsonNodeTrx wtx, final String key) {
    assertTrue(wtx.moveToFirstChild(), "object must have children when looking up key '" + key + "'");
    while (true) {
      final NodeKind kind = wtx.getKind();
      if (kind.playsObjectKeyRole() && key.equals(wtx.getName().getLocalName())) {
        return;
      }
      if (!wtx.moveToRightSibling()) {
        fail("record '" + key + "' not found in object");
      }
    }
  }

  private static void moveToChild(final JsonNodeTrx wtx, final int index) {
    assertTrue(wtx.moveToFirstChild(), "container must have children");
    for (int i = 0; i < index; i++) {
      assertTrue(wtx.moveToRightSibling(), "container must have a child at index " + index);
    }
  }
}
