package io.sirix.access.trx.node;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins {@code AbstractNodeReadOnlyTrx.moveTo}'s self-move short-circuit.
 *
 * <p>Re-anchoring at the node the cursor already sits on used to repeat the whole singleton bind --
 * page lookup, slot lookup, kind decode, flyweight rebind -- to arrive back where it already was.
 * The query layer does exactly that on entry to every field access, which put
 * {@code JsonDBObject.moveRtx} at 71.8% inclusive of a warm filter scan.
 *
 * <p>The short-circuit is restricted to READ-ONLY transactions, and that restriction is the point
 * of this test. A write transaction must resolve through its transaction-intent log on every move:
 * after an async epoch rotation the TIL container is copied on write to a new modified-page
 * instance while the superseded frozen instance stays OPEN for the background flush, so reusing a
 * binding on {@code !isClosed()} alone keeps serving the frozen page for the rest of the epoch and
 * durably corrupts the sibling chain (#1077). An unrestricted version of this optimisation broke
 * {@code DiffFileCreationTest}, {@code HashTest} and {@code OverallTest} in exactly that way --
 * silently, as wrong hashes and a missing diff operation rather than an exception.
 *
 * <p><b>What these two cases do and do not catch.</b> Both PASS with the restriction removed, so
 * neither detects the write-path corruption; verified by deleting the {@code cachedWriter == null}
 * guard and re-running. The detectors are the three suites named above -- {@code HashTest} fails
 * immediately on a rolling/postorder hash mismatch. What is pinned here is the read-only
 * short-circuit's own contract (a self-move keeps the cursor usable and navigable, not merely
 * returning cached scalars) and the writer's read-your-own-write behaviour. They are guard-rails
 * against a future change that breaks those, not a reproduction of #1077 -- reproducing it needs
 * an async epoch rotation mid-transaction, which is what the hash suites exercise incidentally.
 */
@DisplayName("moveTo self-move short-circuit Tests")
public final class SelfMoveShortCircuitTest {

  private static final String RESOURCE = JsonTestHelper.RESOURCE;

  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  private static void shred(final String json) {
    Databases.createJsonDatabase(new io.sirix.access.DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      database.createResource(io.sirix.access.ResourceConfiguration.newBuilder(RESOURCE).build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
           final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json));
        wtx.commit();
      }
    }
  }

  @Test
  @DisplayName("repeated moveTo of the current node keeps the cursor fully usable")
  void selfMoveIsIdempotentForReaders() {
    shred("{\"title\":\"Saleslady\",\"year\":1938,\"active\":true}");

    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
         final JsonResourceSession session = database.beginResourceSession(RESOURCE);
         final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      assertTrue(rtx.moveToFirstChild(), "cursor must reach the object");
      assertTrue(rtx.moveToFirstChild(), "cursor must reach the first field");

      final long key = rtx.getNodeKey();
      final var kind = rtx.getKind();
      final String name = rtx.getName().getLocalName();

      // The short-circuit path: every one of these is a move to where the cursor already is.
      for (int i = 0; i < 5; i++) {
        assertTrue(rtx.moveTo(key), "self-move must report success");
        assertEquals(key, rtx.getNodeKey(), "self-move must not shift the cursor");
        assertEquals(kind, rtx.getKind(), "node kind must survive a self-move");
        assertEquals(name, rtx.getName().getLocalName(), "node name must survive a self-move");
      }

      // ...and navigation from that position must still work, i.e. the binding is live and not
      // merely a cached scalar.
      assertTrue(rtx.moveToRightSibling(), "navigation after a self-move must still work");
      assertEquals("year", rtx.getName().getLocalName(), "must land on the next field");

      assertTrue(rtx.moveTo(key), "self-move back to the original node");
      assertEquals(name, rtx.getName().getLocalName(), "moving back must restore the first field");
    }
  }

  @Test
  @DisplayName("a writer sees its own uncommitted change after re-anchoring at the same node")
  void writerSeesItsOwnWriteAfterSelfMove() {
    shred("{\"title\":\"Saleslady\",\"year\":1938}");

    try (final var database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
         final JsonResourceSession session = database.beginResourceSession(RESOURCE);
         final JsonNodeTrx wtx = session.beginNodeTrx()) {

      wtx.moveToFirstChild();          // object
      wtx.moveToFirstChild();          // title
      wtx.moveToRightSibling();        // year
      final long yearKey = wtx.getNodeKey();

      wtx.setNumberValue(2026);

      // Re-anchor at the node just written. A writer must NOT take the read-only short-circuit:
      // it has to resolve through the TIL, or it reads back the pre-write value from a stale
      // page binding.
      assertTrue(wtx.moveTo(yearKey), "self-move must report success for a writer too");
      assertEquals(2026, wtx.getNumberValue().intValue(),
                   "a writer re-anchoring at a node it just wrote must see the NEW value");

      wtx.commit();

      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertTrue(rtx.moveTo(yearKey), "the committed node must be reachable");
        assertEquals(2026, rtx.getNumberValue().intValue(), "the write must have been durable");
      }
    }
  }
}
