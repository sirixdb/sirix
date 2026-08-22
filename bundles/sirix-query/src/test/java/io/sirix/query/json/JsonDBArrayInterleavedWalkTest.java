package io.sirix.query.json;

import io.brackit.query.jdm.Item;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.Writer;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the invariant {@code AbstractJsonDBArray.at(int)} now carries:
 *
 * <p>
 * <b>{@code at()} NEVER materializes the element list — at any index, in any access order, under any
 * interleaving.</b> Only an explicit {@link io.brackit.query.jdm.json.Array#values()} does. Every test
 * here asserts that first and its element values second, because a wrong element fails loudly while an
 * unnoticed materialization only shows up as a heap that will not fit a corpus.
 *
 * <p>
 * This is not a hypothetical concurrency worry, it is the shape of a real query. brackit binds one
 * array item per variable, so {@code let $doc := jn:doc(...)} puts THAT object into every tuple of
 * the pipeline — including the tuples an operator spills. Deciding whether a spilled column is copied
 * or held by reference is done by RENDERING it under a 64 KiB cap
 * ({@code TupleSerializer.serializeToJson}), and the renderer walks an array with the very same
 * {@code len()}/{@code at(i)} protocol the array unbox uses ({@code StringSerializer}). So spilling
 * one tuple starts a second walk from index 0 across the array the {@code for} loop is streaming, and
 * abandons it a hundred-odd elements in.
 *
 * <p>
 * With a single anchor that was fatal: the streaming walk came back to an anchor it had not set, took
 * the "random access" branch, and materialized every element into a list cached for the life of the
 * query. Measured on a 100 M-element corpus: 99,999,968 live items, 4.8 GB, {@code OutOfMemoryError}
 * — while the diagnostic proved no sibling hop had ever failed, which is what the fallback was
 * believed to be for. The observed anchor when it fired was 134, i.e. exactly where 64 KiB of
 * rendered records runs out.
 */
final class JsonDBArrayInterleavedWalkTest {

  private static final String COLL = "interleavedWalkColl";
  private static final String RES = "interleavedWalkRes";

  /** Elements. Enough that a materialization would be an obvious mistake, small enough to be fast. */
  private static final int ELEMENTS = 600;

  /** Where the streaming walk has reached when the second consumer barges in. */
  private static final int STREAM_POSITION = 400;

  /** How far the barging consumer gets before it gives up — the 64 KiB render cap, in miniature. */
  private static final int INTERLOPER_REACH = 134;

  private Path testDir;
  private BasicJsonDBStore store;

  @BeforeEach
  void setUp() throws Exception {
    testDir = Files.createTempDirectory("sirix-json-interleaved-walk-test");
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

  private JsonDBArray loadArray(final String json) {
    store.create(COLL, RES, json);
    final JsonDBCollection coll = store.lookup(COLL);
    return (JsonDBArray) coll.getDocument(RES);
  }

  /** {@code [0,1,2,...]} — the element at index i atomizes to i, so a wrong element is visible. */
  private static String numbers(final int count) {
    final StringBuilder json = new StringBuilder(count * 6 + 2);
    json.append('[');
    for (int i = 0; i < count; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append(i);
    }
    return json.append(']').toString();
  }

  private static Object valuesFieldOf(final AbstractJsonDBArray<?> array) throws Exception {
    final Field values = AbstractJsonDBArray.class.getDeclaredField("values");
    values.setAccessible(true);
    return values.get(array);
  }

  /**
   * The invariant this class exists for. Asserted before element values in every test.
   *
   * @param array the array under test
   * @param materializationsBefore the counter reading taken before the access pattern
   */
  private static void assertNothingWasMaterialized(final AbstractJsonDBArray<?> array,
      final long materializationsBefore) throws Exception {
    assertNull(valuesFieldOf(array),
               "at() must never leave a materialized element list behind — a list of the array's own "
                   + "size, retained for the lifetime of the query, is the 4.8 GB failure this pins");
    assertEquals(materializationsBefore, AbstractJsonDBArray.materializations(),
                 "no element list may be built at all");
  }

  /**
   * A writer that gives up once it has taken {@code limit} characters, standing in for brackit's
   * {@code CappedWriter}: the spill's probe renders a column only far enough to learn that it is too
   * big to inline, then abandons the walk mid-array. Abandoning is the adversarial part — it leaves
   * the array anchored a hundred-odd elements in rather than at the end.
   */
  private static final class GivesUpAfter extends Writer {
    /** Thrown to abandon the render, exactly as the 64 KiB cap does. */
    static final class LimitReached extends RuntimeException {
      @java.io.Serial
      private static final long serialVersionUID = 1L;
    }

    private final int limit;
    private int taken;

    GivesUpAfter(final int limit) {
      this.limit = limit;
    }

    @Override
    public void write(final char[] buffer, final int offset, final int length) {
      taken += length;
      if (taken >= limit) {
        throw new LimitReached();
      }
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
    }
  }

  private static int elementAt(final JsonDBArray array, final int index) {
    final Item item = (Item) array.at(index);
    assertNotNull(item, "element " + index + " must exist");
    return Integer.parseInt(item.atomize().stringValue());
  }

  @Test
  @DisplayName("at() never materializes when a second walk steals the anchor mid-stream")
  void anInterleavedWalkNeitherMaterializesNorLosesTheAnchor() throws Exception {
    final JsonDBArray array = loadArray(numbers(ELEMENTS));
    assertEquals(ELEMENTS, array.len());

    final long reanchorsBefore = AbstractJsonDBArray.positionalReanchors();
    final long materializationsBefore = AbstractJsonDBArray.materializations();

    // The streaming walk, exactly as brackit's array unbox drives it.
    for (int i = 0; i < STREAM_POSITION; i++) {
      assertEquals(i, elementAt(array, i), "the streaming walk must see element " + i);
    }

    // The interloper: a second consumer starts over at 0 and abandons its walk part way through.
    for (int i = 0; i < INTERLOPER_REACH; i++) {
      assertEquals(i, elementAt(array, i), "the interleaved walk must see element " + i);
    }

    // The streaming walk resumes where it left off. This is the call that used to materialize.
    for (int i = STREAM_POSITION; i < ELEMENTS; i++) {
      assertEquals(i, elementAt(array, i), "the streaming walk must resume at element " + i);
    }

    assertNothingWasMaterialized(array, materializationsBefore);

    // Two walks, so two starts that cannot be served by a hop; every other one of the 1134 accesses
    // is a single sibling hop off one of the two anchors. Three would mean the anchors thrash.
    assertEquals(2L, AbstractJsonDBArray.positionalReanchors() - reanchorsBefore,
                 "each walk may pay one positional re-anchor to start, and not one more");
  }

  @Test
  @DisplayName("at() never materializes when a CAPPED render abandons a walk over the same instance")
  void aCappedRenderDuringAWalkDoesNotMaterialize() throws Exception {
    final JsonDBArray array = loadArray(numbers(ELEMENTS));

    final long materializationsBefore = AbstractJsonDBArray.materializations();

    for (int i = 0; i < STREAM_POSITION; i++) {
      assertEquals(i, elementAt(array, i));
    }

    // The production anchor thief, in miniature: brackit's own serializer walking this array from 0
    // with len()/at(i) while the streaming walk holds its place, and giving up part way — which is
    // what the 64 KiB inline cap does to a spilled $doc column.
    boolean abandoned = false;
    try (StringSerializer serializer = new StringSerializer(new PrintWriter(new GivesUpAfter(256)))) {
      serializer.serialize(array);
    } catch (final GivesUpAfter.LimitReached expected) {
      abandoned = true;
    }
    assertTrue(abandoned, "precondition: the render must ABANDON the walk, not complete it");

    assertNothingWasMaterialized(array, materializationsBefore);

    for (int i = STREAM_POSITION; i < ELEMENTS; i++) {
      assertEquals(i, elementAt(array, i), "the streaming walk must survive a render of its own array");
    }

    assertNothingWasMaterialized(array, materializationsBefore);
  }

  @Test
  @DisplayName("at() never materializes for random access, at any index and in any order")
  void randomAccessIsCorrectWithoutMaterializing() throws Exception {
    final JsonDBArray array = loadArray(numbers(ELEMENTS));

    final long materializationsBefore = AbstractJsonDBArray.materializations();

    // Descending, which is the access order the anchors cannot help with — it must still be right.
    for (int i = ELEMENTS - 1; i >= 0; i -= 37) {
      assertEquals(i, elementAt(array, i), "descending access must be exact at " + i);
    }
    // A shuffle of jumps in both directions.
    for (final int i : new int[] { 0, 599, 1, 598, 300, 42, 599, 0, 301 }) {
      assertEquals(i, elementAt(array, i), "jumping access must be exact at " + i);
    }

    assertNothingWasMaterialized(array, materializationsBefore);
  }

  @Test
  @DisplayName("an anchor the writer deleted behind the item's back is re-derived, not walked from")
  void anAnchorIsRefusedOnceItIsNoLongerTheArraysChild() {
    store.create(COLL, RES, "[0,1,2,3,4,5]");
    final JsonDBCollection coll = store.lookup(COLL);
    final JsonResourceSession session = ((JsonDBArray) coll.getDocument(RES)).getResourceSession();
    final JsonNodeTrx wtx = session.beginNodeTrx();
    wtx.moveToDocumentRoot();
    final JsonDBArray array = new JsonDBArray(wtx, coll);
    final long arrayKey = array.getNodeKey();

    // Anchor the walk on element 2.
    assertEquals(2, elementAt(array, 2));

    // Delete that very element STRAIGHT THROUGH THE TRANSACTION. This item never hears about it —
    // no invalidateScanState() runs — so it is left holding an anchor on a node that is gone.
    wtx.moveTo(arrayKey);
    wtx.moveToFirstChild();
    wtx.moveToRightSibling();
    wtx.moveToRightSibling();
    wtx.remove();

    assertEquals(0, elementAt(array, 0));
    assertEquals(1, elementAt(array, 1));
    assertEquals(3, elementAt(array, 2), "index 2 is the old element 3 once element 2 is gone");
    assertEquals(5, elementAt(array, 4));
    assertNull(array.at(5), "the array is one element shorter");

    wtx.rollback();
    wtx.close();
  }

  @Test
  @DisplayName("an anchor is re-derived when a sibling is inserted before it")
  void anAnchorIsRefusedOnceItsOrdinalChanges() {
    store.create(COLL, RES, "[0,1,2]");
    final JsonDBCollection coll = store.lookup(COLL);
    final JsonResourceSession session = ((JsonDBArray) coll.getDocument(RES)).getResourceSession();
    final JsonNodeTrx wtx = session.beginNodeTrx();
    wtx.moveToDocumentRoot();
    final JsonDBArray array = new JsonDBArray(wtx, coll);
    final long arrayKey = array.getNodeKey();

    assertEquals(0, elementAt(array, 0));
    assertEquals(3, array.values().size());

    wtx.moveTo(arrayKey);
    wtx.insertNumberValueAsFirstChild(99);

    assertEquals(99, elementAt(array, 0));
    assertEquals(4, array.len());
    assertEquals(0, elementAt(array, 1));
    assertEquals(2, elementAt(array, 3));

    wtx.rollback();
    wtx.close();
  }

  @Test
  @DisplayName("materialized values are discarded when the transaction rolls back")
  void rollbackInvalidatesMaterializedValues() {
    store.create(COLL, RES, "[0,1,2]");
    final JsonDBCollection coll = store.lookup(COLL);
    final JsonResourceSession session = ((JsonDBArray) coll.getDocument(RES)).getResourceSession();
    final JsonNodeTrx wtx = session.beginNodeTrx();
    wtx.moveToDocumentRoot();
    final JsonDBArray array = new JsonDBArray(wtx, coll);
    final long arrayKey = array.getNodeKey();

    wtx.moveTo(arrayKey);
    wtx.insertNumberValueAsFirstChild(99);
    assertEquals(4, array.values().size());
    assertEquals(99, elementAt(array, 0));

    wtx.rollback();

    assertEquals(3, array.values().size());
    assertEquals(0, elementAt(array, 0));
    assertEquals(2, elementAt(array, 2));
    wtx.close();
  }

  @Test
  @DisplayName("materialized values are discarded when the transaction reverts")
  void revertInvalidatesMaterializedValues() {
    store.create(COLL, RES, "[0,1,2]");
    final JsonDBCollection coll = store.lookup(COLL);
    final JsonResourceSession session = ((JsonDBArray) coll.getDocument(RES)).getResourceSession();
    final JsonNodeTrx wtx = session.beginNodeTrx();
    wtx.moveToDocumentRoot();
    final JsonDBArray array = new JsonDBArray(wtx, coll);
    final long arrayKey = array.getNodeKey();

    wtx.moveTo(arrayKey);
    wtx.insertNumberValueAsFirstChild(99);
    wtx.commit();
    assertEquals(4, array.values().size());
    assertEquals(99, elementAt(array, 0));

    wtx.revertTo(1);

    assertEquals(3, array.values().size());
    assertEquals(0, elementAt(array, 0));
    assertEquals(2, elementAt(array, 2));
    wtx.rollback();
    wtx.close();
  }

  @Test
  @DisplayName("an explicit values() still materializes, and at() then answers from that list")
  void anExplicitValuesCallStillMemoizes() throws Exception {
    final JsonDBArray array = loadArray(numbers(ELEMENTS));

    final long materializationsBefore = AbstractJsonDBArray.materializations();

    assertEquals(ELEMENTS, array.values().size(), "values() is the one caller whose contract IS the whole list");
    assertNotNull(valuesFieldOf(array));
    assertEquals(materializationsBefore + 1L, AbstractJsonDBArray.materializations());

    assertEquals(7, elementAt(array, 7), "at() must answer from the memo once it exists");
    assertNull(array.at(ELEMENTS), "and still report the end of the array");
  }
}
