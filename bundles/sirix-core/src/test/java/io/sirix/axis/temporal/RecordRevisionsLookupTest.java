package io.sirix.axis.temporal;

import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the contract of {@link RecordRevisionsLookup}.
 *
 * <p>The {@code RECORD_TO_REVISIONS} index is populated by the JSON write path
 * when {@code storeNodeHistory=true} (the default). The lookup must:
 * <ul>
 *   <li>return a non-empty ascending {@code int[]} for an existing node;</li>
 *   <li>return {@code null} when {@code storeNodeHistory} is off, even though
 *       the underlying trie infrastructure could surface a stale record from
 *       another index sub-tree.</li>
 * </ul>
 */
final class RecordRevisionsLookupTest {

  private static final String JSON_R1 = "{\"a\":1,\"b\":2}";
  private static final String JSON_R2 = "{\"a\":1,\"b\":2,\"c\":3}";

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  void revisionsForExistingNode_returnsAscendingNonEmptyArray_whenHistoryStored() throws Exception {
    final var config = ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
        .buildPathSummary(true)
        .build(); // storeNodeHistory defaults to true

    try (final var database = JsonTestHelper.getDatabaseWithResourceConfig(
            JsonTestHelper.PATHS.PATH1.getFile(), config);
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {

      try (final var wtx = session.beginNodeTrx();
           final var reader = JsonShredder.createStringReader(JSON_R1)) {
        wtx.insertSubtreeAsFirstChild(reader);
        wtx.commit();
      }
      try (final var wtx = session.beginNodeTrx();
           final var reader = JsonShredder.createStringReader(JSON_R2)) {
        // Replace the document so a new revision is materialised.
        wtx.moveToDocumentRoot();
        if (wtx.hasFirstChild()) {
          wtx.moveToFirstChild();
          wtx.remove();
        }
        wtx.insertSubtreeAsFirstChild(reader);
        wtx.commit();
      }

      try (final var rtx = session.beginNodeReadOnlyTrx(session.getMostRecentRevisionNumber())) {
        // Node 1 is the document root — created in revision 1 in this resource.
        final int[] revs = RecordRevisionsLookup.revisionsFor(rtx, 1L);
        assertNotNull(revs, "with storeNodeHistory on, the document root must have an index entry");
        assertTrue(revs.length >= 1, "revisions array must be non-empty");
        for (int i = 1; i < revs.length; i++) {
          assertTrue(revs[i] > revs[i - 1],
              "revisions must be strictly ascending: " + revs[i - 1] + " -> " + revs[i]);
        }
        assertTrue(revs[0] >= 1, "first revision must be >= 1");
        assertTrue(revs[revs.length - 1] <= session.getMostRecentRevisionNumber(),
            "last revision must be <= mostRecentRevisionNumber");
      }
    }
  }

  @Test
  void revisionsForExistingNode_returnsNull_whenHistoryDisabled() throws Exception {
    final var config = ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
        .buildPathSummary(true)
        .storeNodeHistory(false)
        .build();

    try (final var database = JsonTestHelper.getDatabaseWithResourceConfig(
            JsonTestHelper.PATHS.PATH1.getFile(), config);
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {

      try (final var wtx = session.beginNodeTrx();
           final var reader = JsonShredder.createStringReader(JSON_R1)) {
        wtx.insertSubtreeAsFirstChild(reader);
        wtx.commit();
      }

      try (final var rtx = session.beginNodeReadOnlyTrx(session.getMostRecentRevisionNumber())) {
        // The trie infrastructure is shared across index types; the lookup must
        // gate on the config flag rather than trust whatever record happens to
        // resolve at the queried key in an unrelated sub-tree.
        final int[] revs = RecordRevisionsLookup.revisionsFor(rtx, 1L);
        assertNull(revs, "lookup must short-circuit on storeNodeHistory=false");
      }
    }
  }
}
