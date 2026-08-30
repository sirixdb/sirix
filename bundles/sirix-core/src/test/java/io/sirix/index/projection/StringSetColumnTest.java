package io.sirix.index.projection;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_SET} — a set of strings per row.
 *
 * <h2>Why the kind exists</h2>
 *
 * <p>
 * The other four column kinds are scalar, so an array-valued field declared as a projection column
 * was recorded present-but-unrepresentable and the index could answer nothing about it. That left
 * {@code some $g in $m.genres[] satisfies $g eq "..."} with no index to use.
 *
 * <h2>What is asserted</h2>
 *
 * <p>
 * That both the in-memory scan form and the canonical segmented persistence form round-trip the
 * shape exactly — the counts AND the flat element run, over rows built to break the two things a
 * variable-length column gets wrong: an EMPTY set (whose row must still consume zero elements and
 * not borrow its neighbour's) and rows of DIFFERENT lengths either side of it, which is what
 * desynchronises a flat run.
 */
final class StringSetColumnTest {

  private static final byte[] KINDS = {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET};

  private static ProjectionIndexRowGroupPage buildLeaf() {
    final ProjectionIndexRowGroupPage leaf = new ProjectionIndexRowGroupPage(KINDS);
    // Deliberately uneven, with the empty set in the middle rather than at an end: a row that
    // consumes no elements between two that do is exactly where an offset/count mix-up shows.
    appendSet(leaf, 100, "Drama", "Short");
    appendSet(leaf, 101);
    appendSet(leaf, 102, "Comedy");
    appendSet(leaf, 103, "Drama", "Comedy", "Silent");
    return leaf;
  }

  private static void appendSet(final ProjectionIndexRowGroupPage leaf, final long recordKey, final String... elems) {
    assertTrue(
        leaf.appendRow(recordKey, new long[1], new boolean[1], new String[] {""}, new String[][] {elems},
            new boolean[] {true}, new boolean[] {false}, new boolean[] {false}, new boolean[] {false}),
        "leaf refused a row it had capacity for");
  }

  @Test
  @DisplayName("counts and elements survive a serialize/deserialize round-trip")
  void roundTripsThroughTheRawForm() {
    final ProjectionIndexRowGroupPage before = buildLeaf();
    final ProjectionIndexRowGroupPage after = ProjectionIndexRowGroupPage.deserialize(before.serialize());

    assertEquals(before.getRowCount(), after.getRowCount(), "row count changed");
    assertArrayEquals(new int[] {2, 0, 1, 3},
        java.util.Arrays.copyOf(after.stringSetCountColumn(0), after.getRowCount()),
        "per-row element counts did not survive the round-trip");
    assertEquals(before.stringSetLength(0), after.stringSetLength(0), "flat element run changed length");
    assertArrayEquals(java.util.Arrays.copyOf(before.stringSetIdColumn(0), before.stringSetLength(0)),
        java.util.Arrays.copyOf(after.stringSetIdColumn(0), after.stringSetLength(0)), "flat element ids changed");
  }

  @Test
  @DisplayName("each row's elements resolve to the strings it was given")
  void elementsResolveBackToTheirStrings() {
    final ProjectionIndexRowGroupPage leaf = ProjectionIndexRowGroupPage.deserialize(buildLeaf().serialize());
    final byte[][] dict = leaf.stringDictionary(0);
    final int[] counts = leaf.stringSetCountColumn(0);
    final int[] elems = leaf.stringSetIdColumn(0);

    final String[][] expected = {{"Drama", "Short"}, {}, {"Comedy"}, {"Drama", "Comedy", "Silent"}};
    int cursor = 0;
    for (int row = 0; row < expected.length; row++) {
      final String[] got = new String[counts[row]];
      for (int k = 0; k < counts[row]; k++) {
        got[k] = new String(dict[elems[cursor + k]], java.nio.charset.StandardCharsets.UTF_8);
      }
      cursor += counts[row];
      assertArrayEquals(expected[row], got, "row " + row + " read back a different set — a flat run is only rows if "
          + "every count before it was consumed exactly");
    }
    assertEquals(leaf.stringSetLength(0), cursor, "the walk did not consume the whole run");
  }

  @Test
  @DisplayName("the segmented codec assembles a set column byte-identically")
  void survivesTheCanonicalSegmentedCodec() {
    final byte[] raw = buildLeaf().serialize();
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(raw);
    final byte[] roundTripped = ProjectionIndexColumnSegmentCodec.assembleRaw(encoded.descriptor(), segmentId -> {
      for (int i = 0; i < encoded.columnSegmentIds().length; i++) {
        if (encoded.columnSegmentIds()[i] == segmentId) {
          return encoded.segments()[i];
        }
      }
      return null;
    });
    final ProjectionIndexRowGroupPage after = ProjectionIndexRowGroupPage.deserialize(roundTripped);

    assertArrayEquals(raw, roundTripped, "segment assembly changed the scan form");
    assertArrayEquals(new int[] {2, 0, 1, 3},
        java.util.Arrays.copyOf(after.stringSetCountColumn(0), after.getRowCount()),
        "counts did not survive segmented persistence");
    assertEquals(6, after.stringSetLength(0), "flat element run changed length");
  }

  @Test
  @DisplayName("the zone map covers the dict-id range, as it does for a scalar dict column")
  void zoneMapMirrorsTheDictionaryRange() {
    // Interning goes through the same appendString as a scalar dict column, so min/max come out as
    // the dict-id range for free. Asserted rather than assumed: a set column left at the empty
    // sentinel (min > max) would read as "no present value" to anything that ever starts
    // zone-pruning string columns, and prune away real matches.
    final ProjectionIndexRowGroupPage leaf = buildLeaf();
    assertEquals(0, leaf.columnMin(0), "zone-map min is not the first dict id");
    assertEquals(3, leaf.columnMax(0), "zone-map max is not the last dict id (4 distinct values)");
  }

  @Test
  @DisplayName("a repeated value interns once, so the set is dictionary-encoded")
  void repeatedValuesShareOneDictionaryEntry() {
    final ProjectionIndexRowGroupPage leaf = buildLeaf();
    final byte[][] dict = leaf.stringDictionary(0);
    int distinct = 0;
    while (distinct < dict.length && dict[distinct] != null) {
      distinct++;
    }
    // Drama, Short, Comedy, Silent — "Drama" appears on two rows and must not be interned twice,
    // which is the whole reason a set column is worth its own dictionary.
    assertEquals(4, distinct, "the dictionary holds a duplicate entry");
  }
}
