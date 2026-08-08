package io.sirix.query.json;

import io.brackit.query.atomic.Int32;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Pins the element-list memo of {@code AbstractJsonDBArray} against structural mutation.
 *
 * <p>{@code len()} and {@code length()} answer from the memoized element list rather than from
 * {@code rtx.getChildCount()}, because re-anchoring the cursor on the array node for every size
 * query dragged it back to the array's page between elements and defeated the same-page fast path
 * — 78 % of all allocations in a scan. That memo is only sound while it is dropped by every
 * mutation, and {@code append} and {@code remove(int)} were not dropping it: {@code modify} (the
 * {@code replaceAt}/{@code insert} path) was.
 *
 * <p>The staleness is only reachable when the array is bound to a WRITE transaction, which is how
 * an updating query binds it — a read-only cursor sits on the last committed revision and does not
 * observe uncommitted structure either way. Each case below therefore mutates through a
 * {@link JsonNodeTrx}, and each materializes the memo FIRST: without a preceding materialization
 * the accessors fall through to the child count and pass regardless.
 */
final class JsonDBArrayMutationMemoTest {

  private static final String COLL = "memoColl";
  private static final String RES = "memoRes";

  private Path testDir;
  private BasicJsonDBStore store;

  @BeforeEach
  void setUp() throws Exception {
    testDir = Files.createTempDirectory("sirix-json-array-memo-test");
    store = BasicJsonDBStore.newBuilder().location(testDir).build();
  }

  @AfterEach
  void tearDown() {
    if (store != null) {
      store.close();
    }
    if (testDir != null) {
      Databases.removeDatabase(testDir);
    }
  }

  /**
   * Loads {@code json} and returns it bound to a fresh write transaction positioned at the document
   * root, mirroring how an updating query binds an array item.
   */
  private JsonDBArray writableArray(final String json) {
    store.create(COLL, RES, json);
    final JsonDBCollection coll = store.lookup(COLL);
    final JsonResourceSession session = ((JsonDBArray) coll.getDocument(RES)).getResourceSession();
    final JsonNodeTrx wtx = session.beginNodeTrx();
    wtx.moveToDocumentRoot();
    return new JsonDBArray(wtx, coll);
  }

  @Test
  @DisplayName("append is visible to len(), length() and values()")
  void appendDropsTheMemo() {
    final JsonDBArray array = writableArray("[1,2,3]");

    assertEquals(3, array.values().size(), "precondition: the memo is materialized");
    assertEquals(3, array.len());

    array.append(new Int32(4));

    // Pre-fix all three answered 3: the memo still held the three elements it was built from.
    assertEquals(4, array.len(), "len() must see the appended element");
    assertEquals("4", array.length().stringValue(), "length() must see the appended element");

    final List<Sequence> values = array.values();
    assertEquals(4, values.size(), "values() must re-materialize after an append");
    assertEquals("4", ((Item) values.get(3)).atomize().stringValue(),
                 "the re-materialized list must end with the appended element");
  }

  @Test
  @DisplayName("remove is visible to len(), length() and values()")
  void removeDropsTheMemo() {
    final JsonDBArray array = writableArray("[1,2,3]");

    assertEquals(3, array.values().size(), "precondition: the memo is materialized");

    array.remove(0);

    // Pre-fix all three answered 3, and values() still handed back the removed element.
    assertEquals(2, array.len(), "len() must see the removal");
    assertEquals("2", array.length().stringValue(), "length() must see the removal");

    final List<Sequence> values = array.values();
    assertEquals(2, values.size(), "values() must re-materialize after a removal");
    assertEquals("2", ((Item) values.get(0)).atomize().stringValue(),
                 "the removed head must be gone from the re-materialized list");
  }

  @Test
  @DisplayName("replaceAt and insert keep the memo consistent too")
  void modifyPathStaysConsistent() {
    // modify() already dropped the memo before this change; the assertions guard the whole set of
    // mutators against a future edit that drops one of them again.
    final JsonDBArray array = writableArray("[1,2,3]");

    assertEquals(3, array.values().size());
    array.insert(0, new Int32(9));
    assertEquals(4, array.len(), "insert must be visible");

    assertEquals(4, array.values().size());
    array.replaceAt(0, new Int32(8));
    assertEquals(4, array.len(), "replaceAt keeps the width");
    assertEquals("8", ((Item) array.values().get(0)).atomize().stringValue(),
                 "replaceAt must be visible in the re-materialized list");
  }

  @Test
  @DisplayName("consecutive mutations each drop the memo")
  void repeatedMutationsStayConsistent() {
    final JsonDBArray array = writableArray("[1,2,3]");

    assertEquals(3, array.values().size());
    array.append(new Int32(4));
    assertEquals(4, array.values().size());
    array.append(new Int32(5));
    assertEquals(5, array.len(), "a second append on an already-invalidated memo must still count");
    array.remove(0);
    assertEquals(4, array.len(), "a removal after two appends must still count");
    assertEquals(4, array.values().size());
  }
}
