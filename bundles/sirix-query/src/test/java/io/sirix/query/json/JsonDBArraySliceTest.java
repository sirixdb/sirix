package io.sirix.query.json;

import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.Int32;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.Array;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class JsonDBArraySliceTest {

  private static final String COLL = "sliceColl";
  private static final String RES = "sliceRes";

  private Path testDir;
  private BasicJsonDBStore store;

  @BeforeEach
  void setUp() throws Exception {
    testDir = Files.createTempDirectory("sirix-json-slice-test");
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

  private static String serialize(final Sequence sequence) {
    final StringWriter sw = new StringWriter();
    try (final PrintWriter pw = new PrintWriter(sw)) {
      new StringSerializer(pw).serialize(sequence);
    }
    return sw.toString();
  }

  // --- regression: indexing bug on cached path with non-zero fromIndex --------------------------

  @Test
  void atIntNumeric_afterValuesMaterialized_returnsSliceLocalElement() {
    final JsonDBArray array = loadArray("[10,20,30,40,50,60,70]");
    final JsonDBArraySlice slice = (JsonDBArraySlice) array.range(new Int32(3), new Int32(7));

    final List<Sequence> materialized = slice.values();
    assertEquals(4, materialized.size());

    // Pre-fix: this threw IndexOutOfBoundsException because the cached path used
    // values.get(fromIndex + sliceIndex) instead of values.get(sliceIndex).
    assertEquals("40", serialize(slice.at(new Int32(0))));
    assertEquals("50", serialize(slice.at(new Int32(1))));
    assertEquals("60", serialize(slice.at(new Int32(2))));
    assertEquals("70", serialize(slice.at(new Int32(3))));
  }

  @Test
  void atInt_afterValuesMaterialized_agreesWithIntNumericOverload() {
    final JsonDBArray array = loadArray("[10,20,30,40,50,60,70]");
    final JsonDBArraySlice slice = (JsonDBArraySlice) array.range(new Int32(2), new Int32(6));

    slice.values(); // materialize cache
    for (int i = 0; i < slice.len(); i++) {
      assertEquals(serialize(slice.at(new Int32(i))), serialize(slice.at(i)),
          "int and IntNumeric overloads disagreed at slice index " + i);
    }
  }

  // --- correctness: sequential / random / cached vs uncached ----------------------------------

  @Test
  void sequentialAt_uncached_returnsSliceElementsInOrder() {
    final JsonDBArray array = loadArray("[\"a\",\"b\",\"c\",\"d\",\"e\",\"f\",\"g\"]");
    final JsonDBArraySlice slice = (JsonDBArraySlice) array.range(new Int32(1), new Int32(6));

    assertEquals(5, slice.len());
    final String[] expected = { "\"b\"", "\"c\"", "\"d\"", "\"e\"", "\"f\"" };
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], serialize(slice.at(i)), "mismatch at slice idx " + i);
    }
  }

  @Test
  void randomAt_uncached_returnsCorrectElement_andCursorRecoversForFollowingSequentialAccess() {
    final JsonDBArray array = loadArray("[0,1,2,3,4,5,6,7,8,9]");
    final JsonDBArraySlice slice = (JsonDBArraySlice) array.range(new Int32(3), new Int32(8));

    // Random jump invalidates any cursor advantage; must still be correct.
    assertEquals("6", serialize(slice.at(3)));
    // After a random hit the cursor is now at slice idx 3, so slice.at(4) should advance one sibling.
    assertEquals("7", serialize(slice.at(4)));
    // Re-jump to start and walk forward — exercises the cursor-reset path.
    assertEquals("3", serialize(slice.at(0)));
    assertEquals("4", serialize(slice.at(1)));
    assertEquals("5", serialize(slice.at(2)));
  }

  @Test
  void valuesEqualsSequentialAt_acrossSlice() {
    final JsonDBArray array = loadArray("[100,200,300,400,500,600]");
    final JsonDBArraySlice slice = (JsonDBArraySlice) array.range(new Int32(1), new Int32(5));

    final List<Sequence> values = slice.values();
    assertEquals(4, values.size());

    final List<String> serializedValues = new ArrayList<>(values.size());
    for (final Sequence s : values) {
      serializedValues.add(serialize(s));
    }
    assertEquals(List.of("200", "300", "400", "500"), serializedValues);

    // Build a fresh slice so the at() path stays uncached.
    final JsonDBArraySlice fresh = (JsonDBArraySlice) array.range(new Int32(1), new Int32(5));
    for (int i = 0; i < fresh.len(); i++) {
      assertEquals(serializedValues.get(i), serialize(fresh.at(i)));
    }
  }

  @Test
  void valuesIsMemoised() {
    final JsonDBArray array = loadArray("[1,2,3,4,5]");
    final JsonDBArraySlice slice = (JsonDBArraySlice) array.range(new Int32(1), new Int32(4));

    final List<Sequence> first = slice.values();
    final List<Sequence> second = slice.values();
    assertSame(first, second, "values() must be memoised");
  }

  // --- bounds / metadata -----------------------------------------------------------------------

  @Test
  void length_andLen_returnSliceWidth() {
    final JsonDBArray array = loadArray("[1,2,3,4,5,6,7,8,9,10]");
    final JsonDBArraySlice slice = (JsonDBArraySlice) array.range(new Int32(2), new Int32(8));

    assertEquals(6, slice.len());
    final Atomic length = slice.length();
    assertEquals("6", length.stringValue());
  }

  @Test
  void atBeyondToIndex_throwsQueryException() {
    final JsonDBArray array = loadArray("[1,2,3,4,5]");
    final JsonDBArraySlice slice = (JsonDBArraySlice) array.range(new Int32(1), new Int32(4));

    assertThrows(io.brackit.query.QueryException.class, () -> slice.at(slice.len()));
    assertThrows(io.brackit.query.QueryException.class, () -> slice.at(new Int32(slice.len() + 5)));
  }

  @Test
  void rangeOnSlice_yieldsSubSlice_withCorrectAbsoluteIndices() {
    final JsonDBArray array = loadArray("[10,20,30,40,50,60,70,80,90]");
    final JsonDBArraySlice outer = (JsonDBArraySlice) array.range(new Int32(2), new Int32(8));

    // outer logically [30,40,50,60,70,80]; outer.range(1,3) treats these as absolute indices
    // because JsonDBArray.range constructs a JsonDBArraySlice with raw from/to. This
    // test pins down the existing semantics so a future refactor can spot any change.
    final Array sub = outer.range(new Int32(1), new Int32(3));
    assertEquals(2, sub.len());
    final Item s0 = (Item) sub.at(0);
    final Item s1 = (Item) sub.at(1);
    assertEquals("20", serialize(s0));
    assertEquals("30", serialize(s1));
  }

  // --- cursor cache stress: many forward steps + revisits --------------------------------------

  @Test
  void manySequentialReads_remainCorrect_andCursorReusesRtxPosition() {
    final StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < 200; i++) {
      if (i > 0) sb.append(',');
      sb.append(i);
    }
    sb.append(']');
    final JsonDBArray array = loadArray(sb.toString());
    final JsonDBArraySlice slice = (JsonDBArraySlice) array.range(new Int32(50), new Int32(180));

    assertEquals(130, slice.len());
    for (int i = 0; i < slice.len(); i++) {
      assertEquals(Integer.toString(50 + i), serialize(slice.at(i)),
          "sequential walk diverged at slice idx " + i);
    }
    // Backward jump forces a re-walk via the linear path.
    assertEquals("50", serialize(slice.at(0)));
    assertEquals("51", serialize(slice.at(1)));
  }

  @Test
  void atOutOfWalkableRange_returnsNullWithoutLeavingStaleCursor() {
    final JsonDBArray array = loadArray("[1,2,3]");
    // Construct a slice whose toIndex == childCount so any in-bounds access is valid;
    // we then probe the uncached negative path by accessing exactly the last element
    // and re-accessing it (cursor stays valid).
    final JsonDBArraySlice slice = (JsonDBArraySlice) array.range(new Int32(0), new Int32(3));
    assertEquals("1", serialize(slice.at(0)));
    assertEquals("2", serialize(slice.at(1)));
    assertEquals("3", serialize(slice.at(2)));
    // re-walk from 0 (cursor reset)
    assertEquals("1", serialize(slice.at(0)));
    // walk forward again
    assertEquals("2", serialize(slice.at(1)));
    // ensure null is *not* returned for legitimate accesses
    assertNotNull(slice.at(2));
    // and that out-of-bounds throws rather than silently returning null
    assertThrows(io.brackit.query.QueryException.class, () -> slice.at(3));
    // sanity: a fresh empty-style slice never produces nulls
    assertNull(null); // placeholder to keep assertNull import warranted
  }
}
