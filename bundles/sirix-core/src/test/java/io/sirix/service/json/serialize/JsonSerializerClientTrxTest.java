package io.sirix.service.json.serialize;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.StringWriter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The {@code JsonSerializer.Builder(JsonNodeReadOnlyTrx, Appendable)} overload: serialize through a
 * cursor the CLIENT already opened, instead of opening (and closing) one per call.
 *
 * <p>The contract has three halves, and all three are load-bearing:
 * <ol>
 *   <li><b>Identical output.</b> Borrowing must be an optimization, not a dialect. Every case here
 *       compares byte-for-byte against the same serializer run in the owning mode.</li>
 *   <li><b>The cursor is given back untouched.</b> The client keeps ownership: the transaction is
 *       still open afterwards and its cursor is back on the node it was on.</li>
 *   <li><b>Where the cursor happened to be must not change the output.</b> This is the subtle one —
 *       {@code emitRevisionStartNode} inspects the cursor to decide how to open the revision, so a
 *       borrowed cursor left deep in the document produced different nesting and indentation than a
 *       freshly opened one until the serializer reset it to the document root.</li>
 * </ol>
 */
public final class JsonSerializerClientTrxTest {

  private static final String DOC =
      "{\"a\":{\"b\":[1,2,3],\"c\":\"x\"},\"d\":[{\"e\":true},{\"f\":null}],\"g\":42}";

  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  /** Create the test resource with a single revision holding {@link #DOC}. */
  private static void createResource() {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(DOC));
      wtx.commit();
    }
  }

  /** Serialize with a serializer-owned transaction (the classic path). */
  private static String serializeOwning(final JsonResourceSession session, final boolean indent,
      final long startNodeKey) {
    final StringWriter writer = new StringWriter();
    final JsonSerializer.Builder builder = new JsonSerializer.Builder(session, writer);
    if (indent) {
      builder.prettyPrint();
    }
    if (startNodeKey > 0) {
      builder.startNodeKey(startNodeKey);
    }
    builder.build().call();
    return writer.toString();
  }

  /** Serialize through a client-owned cursor. */
  private static String serializeBorrowing(final JsonNodeReadOnlyTrx rtx, final boolean indent,
      final long startNodeKey) {
    final StringWriter writer = new StringWriter();
    final JsonSerializer.Builder builder = new JsonSerializer.Builder(rtx, writer);
    if (indent) {
      builder.prettyPrint();
    }
    if (startNodeKey > 0) {
      builder.startNodeKey(startNodeKey);
    }
    builder.build().call();
    return writer.toString();
  }

  @Test
  public void borrowedCursorProducesIdenticalOutput() {
    createResource();
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      final String owning = serializeOwning(session, false, 0);
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertEquals("borrowing a client cursor must not change the output", owning,
                     serializeBorrowing(rtx, false, 0));
      }
    }
  }

  @Test
  public void borrowedCursorProducesIdenticalPrettyPrintedOutput() {
    createResource();
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      final String owning = serializeOwning(session, true, 0);
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertEquals("indentation must match the owning path exactly", owning,
                     serializeBorrowing(rtx, true, 0));
      }
    }
  }

  @Test
  public void borrowedCursorIsRestoredAndLeftOpen() {
    createResource();
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertTrue(rtx.moveToFirstChild());   // the top-level object
        assertTrue(rtx.moveToFirstChild());   // its first member
        final long positionBefore = rtx.getNodeKey();

        serializeBorrowing(rtx, false, 0);

        assertFalse("the serializer must not close a client's transaction", rtx.isClosed());
        assertEquals("the cursor must be handed back where the client left it", positionBefore,
                     rtx.getNodeKey());
        // And it is still usable.
        assertTrue(rtx.moveToParent());
      }
    }
  }

  @Test
  public void outputDoesNotDependOnWhereTheCursorWas() {
    createResource();
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      final String fromDocumentRoot;
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        fromDocumentRoot = serializeBorrowing(rtx, true, 0);
      }
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        // Park the cursor deep inside the document before serializing. Without the document-root
        // reset this emitted a differently nested/indented document.
        assertTrue(rtx.moveToFirstChild());
        assertTrue(rtx.moveToFirstChild());
        assertTrue(rtx.moveToFirstChild());
        assertEquals("a parked cursor must serialize the same document", fromDocumentRoot,
                     serializeBorrowing(rtx, true, 0));
      }
    }
  }

  @Test
  public void borrowedCursorHonoursStartNodeKey() {
    createResource();
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      final long startNodeKey;
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertTrue(rtx.moveToFirstChild());
        assertTrue(rtx.moveToFirstChild());
        startNodeKey = rtx.getNodeKey();
      }

      final String owning = serializeOwning(session, false, startNodeKey);
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        // Park the cursor somewhere else entirely: startNodeKey, not the cursor, selects the
        // subtree.
        assertTrue(rtx.moveToFirstChild());
        assertEquals("startNodeKey must select the subtree in the borrowed path too", owning,
                     serializeBorrowing(rtx, false, startNodeKey));
      }
    }
  }
}
