package io.sirix.access.node.json;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.api.Database;
import io.sirix.api.RevisionInfo;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.StringWriter;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class JsonMultiRevisionTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  void testCreateMultipleRevisions() throws Exception {
    final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      // Revision 1: insert initial object
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"key\":\"value1\"}"),
            JsonNodeTrx.Commit.NO);
        wtx.commit();
      }

      // Revision 2: modify value
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // object
        wtx.moveToFirstChild(); // object key "key"
        wtx.moveToFirstChild(); // string value "value1"
        wtx.setStringValue("value2");
        wtx.commit();
      }

      // Revision 3: add a new field
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // object
        wtx.moveToFirstChild(); // object key "key"
        wtx.insertObjectRecordAsRightSibling("extra",
            new StringValue("data"));
        wtx.commit();
      }

      // Verify each revision has different data
      final String rev1 = serializeRevision(session, 1);
      final String rev2 = serializeRevision(session, 2);
      final String rev3 = serializeRevision(session, 3);

      JSONAssert.assertEquals("{\"key\":\"value1\"}", rev1, true);
      JSONAssert.assertEquals("{\"key\":\"value2\"}", rev2, true);
      JSONAssert.assertEquals("{\"key\":\"value2\",\"extra\":\"data\"}", rev3, true);
    }
  }

  @Test
  void testReadOldRevision() throws Exception {
    final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      // Revision 1
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"name\":\"original\"}"),
            JsonNodeTrx.Commit.NO);
        wtx.commit();
      }

      // Revision 2
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.moveToFirstChild();
        wtx.moveToFirstChild();
        wtx.setStringValue("modified");
        wtx.commit();
      }

      // Read revision 1 and verify it still has original data
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1)) {
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild(); // object
        rtx.moveToFirstChild(); // object key "name"
        rtx.moveToFirstChild(); // string value
        assertEquals("original", rtx.getValue());
      }
    }
  }

  @Test
  void testRevisionIsolation() throws Exception {
    final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      // Revision 1
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"alpha\"]"),
            JsonNodeTrx.Commit.NO);
        wtx.commit();
      }

      // Open read-only trx on revision 1
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1);
           final JsonNodeTrx wtx = session.beginNodeTrx()) {

        // Modify via write trx (creates revision 2)
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // array
        wtx.moveToFirstChild(); // "alpha"
        wtx.insertStringValueAsRightSibling("beta");
        wtx.commit();

        // Read-only trx on revision 1 should still see only "alpha"
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild(); // array
        rtx.moveToFirstChild(); // "alpha"
        assertEquals("alpha", rtx.getValue());
        // No right sibling should exist in revision 1
        assertTrue(!rtx.moveToRightSibling());
      }
    }
  }

  @Test
  void testSerializeSpecificRevision() throws Exception {
    final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      // Revision 1
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"status\":\"initial\"}"),
            JsonNodeTrx.Commit.NO);
        wtx.commit();
      }

      // Revision 2
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.moveToFirstChild();
        wtx.moveToFirstChild();
        wtx.setStringValue("updated");
        wtx.commit();
      }

      // Serialize revision 1 specifically
      final String rev1Output = serializeRevision(session, 1);
      JSONAssert.assertEquals("{\"status\":\"initial\"}", rev1Output, true);
    }
  }

  @Test
  void testGetMostRecentRevisionNumber() throws Exception {
    final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      for (int i = 1; i <= 3; i++) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          if (i == 1) {
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[" + i + "]"),
                JsonNodeTrx.Commit.NO);
          } else {
            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild(); // array
            wtx.moveToFirstChild(); // number
            wtx.setNumberValue(i);
          }
          wtx.commit();
        }
      }

      assertEquals(3, session.getMostRecentRevisionNumber());
    }
  }

  @Test
  void testReadAfterReopen() throws Exception {
    final String jsonInput = "{\"persistent\":true}";

    // Create and populate
    final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(jsonInput),
            JsonNodeTrx.Commit.NO);
        wtx.commit();
      }
    }

    // Close the database
    JsonTestHelper.closeEverything();

    // Reopen and verify data is still accessible
    final Database<JsonResourceSession> reopened = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = reopened.beginResourceSession(JsonTestHelper.RESOURCE)) {
      final String serialized = serializeRevision(session, 1);
      JSONAssert.assertEquals(jsonInput, serialized, true);
    }
  }

  @Test
  void testConcurrentReadOnlyTrxDifferentRevisions() throws Exception {
    final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      // Revision 1
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1]"),
            JsonNodeTrx.Commit.NO);
        wtx.commit();
      }

      // Revision 2
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.moveToFirstChild();
        wtx.setNumberValue(2);
        wtx.commit();
      }

      // Open two read-only transactions simultaneously on different revisions
      try (final JsonNodeReadOnlyTrx rtx1 = session.beginNodeReadOnlyTrx(1);
           final JsonNodeReadOnlyTrx rtx2 = session.beginNodeReadOnlyTrx(2)) {

        rtx1.moveToDocumentRoot();
        rtx1.moveToFirstChild();
        rtx1.moveToFirstChild();

        rtx2.moveToDocumentRoot();
        rtx2.moveToFirstChild();
        rtx2.moveToFirstChild();

        assertEquals("1", rtx1.getValue());
        assertEquals("2", rtx2.getValue());
        assertEquals(1, rtx1.getRevisionNumber());
        assertEquals(2, rtx2.getRevisionNumber());
      }
    }
  }

  @Test
  void testCreate10Revisions() throws Exception {
    final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      // Revision 1: initial array with one element
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"item0\"]"),
            JsonNodeTrx.Commit.NO);
        wtx.commit();
      }

      // Revisions 2-10: add one element each
      for (int i = 1; i < 10; i++) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild(); // array
          wtx.moveToLastChild();  // last element
          wtx.insertStringValueAsRightSibling("item" + i);
          wtx.commit();
        }
      }

      assertEquals(10, session.getMostRecentRevisionNumber());

      // Verify revision 1 has 1 element, revision 10 has 10 elements
      try (final JsonNodeReadOnlyTrx rtx1 = session.beginNodeReadOnlyTrx(1)) {
        rtx1.moveToDocumentRoot();
        rtx1.moveToFirstChild(); // array
        assertEquals(1, rtx1.getChildCount());
      }

      try (final JsonNodeReadOnlyTrx rtx10 = session.beginNodeReadOnlyTrx(10)) {
        rtx10.moveToDocumentRoot();
        rtx10.moveToFirstChild(); // array
        assertEquals(10, rtx10.getChildCount());
      }
    }
  }

  @Test
  void testRevisionSpecificNodeCounts() throws Exception {
    final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      // Revision 1: simple object
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"a\":1}"),
            JsonNodeTrx.Commit.NO);
        wtx.commit();
      }

      // Revision 2: larger object
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // object
        wtx.moveToFirstChild(); // object key "a"
        wtx.insertObjectRecordAsRightSibling("b",
            new StringValue("two"));
        wtx.commit();
      }

      // Revision 1 should have fewer descendants than revision 2
      final long descCount1;
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1)) {
        rtx.moveToDocumentRoot();
        descCount1 = rtx.getDescendantCount();
      }

      final long descCount2;
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(2)) {
        rtx.moveToDocumentRoot();
        descCount2 = rtx.getDescendantCount();
      }

      assertTrue(descCount2 > descCount1,
          "Revision 2 should have more descendants than revision 1");
    }
  }

  @Test
  void testModifyAndCommitMultipleTimes() throws Exception {
    final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      // Revision 1: insert
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"first\",\"second\",\"third\"]"),
            JsonNodeTrx.Commit.NO);
        wtx.commit();
      }

      // Revision 2: modify
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // array
        wtx.moveToFirstChild(); // "first"
        wtx.setStringValue("FIRST");
        wtx.commit();
      }

      // Revision 3: delete
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // array
        wtx.moveToLastChild();  // "third"
        wtx.remove();
        wtx.commit();
      }

      final String rev1 = serializeRevision(session, 1);
      final String rev2 = serializeRevision(session, 2);
      final String rev3 = serializeRevision(session, 3);

      JSONAssert.assertEquals("[\"first\",\"second\",\"third\"]", rev1, true);
      JSONAssert.assertEquals("[\"FIRST\",\"second\",\"third\"]", rev2, true);
      JSONAssert.assertEquals("[\"FIRST\",\"second\"]", rev3, true);

      assertEquals(3, session.getMostRecentRevisionNumber());
    }
  }

  @Test
  void testGetHistoryReturnsAllRevisions() throws Exception {
    final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      for (int i = 1; i <= 5; i++) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          if (i == 1) {
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[" + i + "]"),
                JsonNodeTrx.Commit.NO);
          } else {
            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild();
            wtx.moveToFirstChild();
            wtx.setNumberValue(i);
          }
          wtx.commit();
        }
      }

      final List<RevisionInfo> history = session.getHistory();
      assertEquals(5, history.size());

      // History is returned in reverse order (most recent first)
      for (final RevisionInfo info : history) {
        assertTrue(info.getRevision() >= 1 && info.getRevision() <= 5,
            "Revision should be between 1 and 5, got: " + info.getRevision());
      }
    }
  }

  @Test
  void testGetHistoryFromTo() throws Exception {
    final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      for (int i = 1; i <= 5; i++) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          if (i == 1) {
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[" + i + "]"),
                JsonNodeTrx.Commit.NO);
          } else {
            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild();
            wtx.moveToFirstChild();
            wtx.setNumberValue(i);
          }
          wtx.commit();
        }
      }

      // getHistory(fromRevision, toRevision) — fromRevision must be > toRevision
      final List<RevisionInfo> subset = session.getHistory(4, 2);
      assertEquals(3, subset.size());

      // Verify the returned revisions cover 2, 3, 4
      for (final RevisionInfo info : subset) {
        final int rev = info.getRevision();
        assertTrue(rev >= 2 && rev <= 4,
            "Revision should be between 2 and 4, got: " + rev);
      }
    }
  }

  @Test
  void testRevisionTimestamps() throws Exception {
    final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      for (int i = 1; i <= 3; i++) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          if (i == 1) {
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"rev\":" + i + "}"),
                JsonNodeTrx.Commit.NO);
          } else {
            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild();
            wtx.moveToFirstChild();
            wtx.moveToFirstChild();
            wtx.setNumberValue(i);
          }
          wtx.commit();
        }
      }

      Instant previousTimestamp = null;
      for (int rev = 1; rev <= 3; rev++) {
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
          final Instant timestamp = rtx.getRevisionTimestamp();
          assertNotNull(timestamp, "Revision timestamp should not be null for rev " + rev);

          if (previousTimestamp != null) {
            assertTrue(!timestamp.isBefore(previousTimestamp),
                "Revision " + rev + " timestamp should not be before revision " + (rev - 1));
          }
          previousTimestamp = timestamp;
        }
      }
    }
  }

  @Test
  void testSerializeMultipleRevisionsSequentially() throws Exception {
    final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      // Revision 1
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"count\":1}"),
            JsonNodeTrx.Commit.NO);
        wtx.commit();
      }

      // Revision 2
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.moveToFirstChild();
        wtx.moveToFirstChild();
        wtx.setNumberValue(2);
        wtx.commit();
      }

      // Revision 3
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.moveToFirstChild();
        wtx.moveToFirstChild();
        wtx.setNumberValue(3);
        wtx.commit();
      }

      final String rev1 = serializeRevision(session, 1);
      final String rev2 = serializeRevision(session, 2);
      final String rev3 = serializeRevision(session, 3);

      JSONAssert.assertEquals("{\"count\":1}", rev1, true);
      JSONAssert.assertEquals("{\"count\":2}", rev2, true);
      JSONAssert.assertEquals("{\"count\":3}", rev3, true);

      // All three should be different
      assertTrue(!rev1.equals(rev2), "Rev 1 and Rev 2 should differ");
      assertTrue(!rev2.equals(rev3), "Rev 2 and Rev 3 should differ");
      assertTrue(!rev1.equals(rev3), "Rev 1 and Rev 3 should differ");
    }
  }

  private String serializeRevision(final JsonResourceSession session, final int revision) throws Exception {
    try (final StringWriter writer = new StringWriter()) {
      new JsonSerializer.Builder(session, writer, revision).build().call();
      return writer.toString();
    }
  }
}
