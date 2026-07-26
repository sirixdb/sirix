package io.sirix.service.json.serialize;

import io.sirix.JsonTestHelper;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.StringWriter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Regression test for issue #1069: {@link JsonSerializer#call()} delegates to the limited
 * serializer whenever a limit (e.g. {@code maxLevel}) is set. The "serialize all revisions"
 * convention ({@code revisions[0] < 0}) must be expanded into an explicit revision list before
 * delegating — pre-fix the limited serializer tried to open revision {@code -1} and threw.
 */
public final class JsonSerializerAllRevisionsWithLimitTest {

  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void testAllRevisionsWithMaxLevelSerializesEveryRevision() {
    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final var wtx = session.beginNodeTrx()) {
        // Revision 1 (insertSubtreeAsFirstChild commits implicitly).
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"a\":\"one\"}"));

        // Revision 2: add a second field.
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // top-level object
        wtx.insertObjectRecordAsFirstChild("b", new StringValue("two"));
        wtx.commit();
      }

      assertEquals(2, session.getMostRecentRevisionNumber());

      // Pre-fix this threw while trying to open revision -1.
      final StringWriter allRevisionsWriter = new StringWriter();
      JsonSerializer.newBuilder(session, allRevisionsWriter)
                    .revisions(new int[] {-1})
                    .maxLevel(3)
                    .build()
                    .call();
      final String allRevisions = allRevisionsWriter.toString();

      final StringWriter explicitRevisionsWriter = new StringWriter();
      JsonSerializer.newBuilder(session, explicitRevisionsWriter)
                    .revisions(new int[] {1, 2})
                    .maxLevel(3)
                    .build()
                    .call();
      final String explicitRevisions = explicitRevisionsWriter.toString();

      assertFalse("serialization output must not be empty", allRevisions.isEmpty());
      assertEquals("revisions {-1} must serialize the same output as the explicit list {1, 2}", explicitRevisions,
          allRevisions);
      assertTrue("output must contain revision 1 content, but was: " + allRevisions, allRevisions.contains("\"a\""));
      assertTrue("output must contain revision 2 content, but was: " + allRevisions, allRevisions.contains("\"b\""));
    }
  }
}
