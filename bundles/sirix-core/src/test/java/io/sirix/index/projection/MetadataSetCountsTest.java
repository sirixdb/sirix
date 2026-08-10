package io.sirix.index.projection;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The index-wide value→row-count summary carried in the projection's slot-0 metadata.
 *
 * <h2>Why it lives here rather than per leaf</h2>
 *
 * <p>A bare {@code count(... satisfies $g eq lit)} over the whole index needs ONE number. The
 * per-leaf form of the same summary was tried in the row-group descriptors and overflowed the 64 KB
 * HOT leaf pages, because a few hundred bytes multiplied by thousands of leaves. Written once, the
 * same information costs a few hundred bytes total and rides in a blob every covering lookup reads
 * anyway.
 *
 * <h2>What is asserted</h2>
 *
 * <p>The wire format round-trips, the bounds are enforced rather than allowed to produce an
 * oversized slot, and — the one that matters for correctness — a value the index does not hold
 * answers ZERO rather than "unknown". That distinction is what lets an absent literal be answered
 * without a scan: the map lists every value the column holds, so a miss is a real zero.
 */
final class MetadataSetCountsTest {

  private static ProjectionIndexMetadata withCounts(final Map<String, Long> genres) {
    final Map<Integer, Map<String, Long>> byColumn = new LinkedHashMap<>();
    byColumn.put(1, genres);
    return new ProjectionIndexMetadata("/[]", new String[] {"/[]/year", "/[]/genres/[]"},
                                       new String[] {"year", "genres"},
                                       new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
                                                   ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET},
                                       3401, 1, byColumn);
  }

  private static Map<String, Long> sampleGenres() {
    final Map<String, Long> genres = new LinkedHashMap<>();
    genres.put("Drama", 1_349_952L);
    genres.put("Comedy", 1_007_808L);
    genres.put("Silent", 684_576L);
    return genres;
  }

  @Test
  @DisplayName("counts survive serialize/parse")
  void roundTrips() {
    final ProjectionIndexMetadata parsed =
        ProjectionIndexMetadata.parse(withCounts(sampleGenres()).serialize());
    assertNotNull(parsed, "metadata did not parse");
    assertEquals(1_349_952L, parsed.setValueRowCount(1, "Drama"));
    assertEquals(684_576L, parsed.setValueRowCount(1, "Silent"));
  }

  @Test
  @DisplayName("a value the index does not hold answers zero, not unknown")
  void absentValueIsZeroNotUnknown() {
    final ProjectionIndexMetadata parsed =
        ProjectionIndexMetadata.parse(withCounts(sampleGenres()).serialize());
    // The whole point of the summary for a rare literal: 0 without touching a leaf. Returning null
    // here would send the query to a full scan to discover the same thing.
    assertEquals(0L, parsed.setValueRowCount(1, "Nowhere"));
  }

  @Test
  @DisplayName("a column with no summary answers unknown, so the caller falls back")
  void unsummarisedColumnIsUnknown() {
    final ProjectionIndexMetadata parsed =
        ProjectionIndexMetadata.parse(withCounts(sampleGenres()).serialize());
    assertNull(parsed.setValueRowCount(0, "anything"),
               "a column carrying no summary must be distinguishable from one whose value is "
                   + "absent — otherwise an unsummarised column silently answers every count 0");
  }

  @Test
  @DisplayName("a high-cardinality column is omitted rather than overflowing the slot")
  void overCardinalityColumnIsOmitted() {
    final Map<String, Long> many = new LinkedHashMap<>();
    for (int i = 0; i < 5_000; i++) {           // past MAX_SET_COUNTS_VALUES
      many.put("value-" + i, (long) i);
    }
    final byte[] payload = withCounts(many).serialize();
    assertEquals(0, ProjectionIndexMetadata.parse(payload).setValueRowCount(1, "value-1") == null
                        ? 0 : 1,
                 "a column past the cardinality cap must carry no summary — 33k distinct titles "
                     + "would be ~400 KB against a 64 KB slot");
    // And the payload must stay small enough to be a slot value at all.
    assertEquals(true, payload.length < 65_535,
                 "metadata payload grew past the slot limit: " + payload.length);
  }

  @Test
  @DisplayName("metadata without any summary still round-trips")
  void noSummaryRoundTrips() {
    final ProjectionIndexMetadata plain =
        new ProjectionIndexMetadata("/[]", new String[] {"/[]/year"}, new String[] {"year"},
                                    new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG},
                                    10, 1);
    final ProjectionIndexMetadata parsed = ProjectionIndexMetadata.parse(plain.serialize());
    assertNotNull(parsed, "plain metadata did not parse");
    assertNull(parsed.setValueRowCount(0, "x"), "a plain payload must report no summary");
  }
}
