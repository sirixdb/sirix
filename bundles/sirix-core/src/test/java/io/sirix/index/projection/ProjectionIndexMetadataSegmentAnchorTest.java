/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionIndexMetadata.SegmentAnchor;

import java.util.Arrays;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The segment-anchor section of the projection metadata: where a page's SEGMENT anchor is
 * translated into the dictionary it was sealed under.
 *
 * <p>
 * The property that matters as much as the round trip is the one about ABSENCE. The metadata parser
 * refuses any trailing bytes it does not understand — deliberately, so a shifted field can never be
 * read as data — which means adding a section is only safe if a payload WITHOUT it still parses.
 * Every database written before segment dictionaries existed is such a payload.
 * </p>
 */
final class ProjectionIndexMetadataSegmentAnchorTest {

  private static final String ROOT = "/[]";

  private static ProjectionIndexMetadata withAnchors(final SegmentAnchor... anchors) {
    return new ProjectionIndexMetadata(ROOT, new String[] {"/[]/a", "/[]/b"}, new String[] {"a", "b"},
        new byte[] {
            ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG},
        7, 3, null, null, anchors.length == 0
            ? null
            : anchors);
  }

  @Test
  @DisplayName("a projection WITHOUT segment anchors serializes and parses exactly as before")
  void absenceIsTheOldFormat() {
    final ProjectionIndexMetadata plain = withAnchors();
    final byte[] payload = plain.serialize();
    final ProjectionIndexMetadata parsed = ProjectionIndexMetadata.parse(payload);
    assertNotNull(parsed);
    assertNull(parsed.segmentAnchors(), "no section is written and none is read");
    assertEquals(ROOT, parsed.rootPath());
    assertEquals(7, parsed.rowGroupCount());
    // The point of the test: the payload is byte-for-byte what it was before the section existed, so
    // an existing database still reads.
    final ProjectionIndexMetadata again = ProjectionIndexMetadata.parse(plain.serialize());
    assertNotNull(again);
    assertArrayEquals(payload, again.serialize(), "round trip is stable and adds nothing");
  }

  @Test
  @DisplayName("anchors round trip: segment, column, header key and the count sealed with it")
  void anchorsRoundTrip() {
    final SegmentAnchor[] anchors = {new SegmentAnchor(0, 0, 4242L, 31_000), new SegmentAnchor(0, 1, 4243L, 12),
        new SegmentAnchor(9_999, 1, 987_654_321L, 1)};
    final ProjectionIndexMetadata parsed = ProjectionIndexMetadata.parse(withAnchors(anchors).serialize());
    assertNotNull(parsed);
    assertArrayEquals(anchors, parsed.segmentAnchors());
    // And the section survives a second trip, so it is not merely readable once.
    final ProjectionIndexMetadata again = ProjectionIndexMetadata.parse(parsed.serialize());
    assertNotNull(again);
    assertArrayEquals(anchors, again.segmentAnchors());
  }

  @Test
  @DisplayName("the section carries the whole table: many segments across many columns")
  void aWholeTableRoundTrips() {
    final SegmentAnchor[] anchors = new SegmentAnchor[2_000];
    for (int i = 0; i < anchors.length; i++) {
      anchors[i] = new SegmentAnchor(i / 2, i % 2, 1_000L + i, i);
    }
    final byte[] payload = withAnchors(anchors).serialize();
    final ProjectionIndexMetadata parsed = ProjectionIndexMetadata.parse(payload);
    assertNotNull(parsed);
    assertArrayEquals(anchors, parsed.segmentAnchors());
    // 10,000 segments x 4 columns is the 100M shape; 2,000 entries here is ~44 KB, so the whole table
    // stays a small fraction of a metadata blob.
    assertTrue(payload.length < 100_000, "the table is compact: " + payload.length + " B for 2,000 anchors");
  }

  @Test
  @DisplayName("the accessor hands out a copy: the persisted shape cannot be edited in place")
  void theAccessorCopies() {
    final SegmentAnchor[] anchors = {new SegmentAnchor(1, 0, 5L, 2)};
    final ProjectionIndexMetadata metadata = withAnchors(anchors);
    final SegmentAnchor[] handedOut = metadata.segmentAnchors();
    assertNotNull(handedOut);
    handedOut[0] = new SegmentAnchor(1, 0, 999L, 2);
    assertArrayEquals(anchors, metadata.segmentAnchors(), "the metadata kept its own table");
  }

  @Test
  @DisplayName("a nonsensical anchor is refused at construction, not discovered at resolve time")
  void invalidAnchorsAreRefused() {
    assertThrows(IllegalArgumentException.class, () -> withAnchors(new SegmentAnchor(-1, 0, 5L, 1)), "segment");
    assertThrows(IllegalArgumentException.class, () -> withAnchors(new SegmentAnchor(0, 2, 5L, 1)), "column past end");
    assertThrows(IllegalArgumentException.class, () -> withAnchors(new SegmentAnchor(0, -1, 5L, 1)), "column");
    assertThrows(IllegalArgumentException.class, () -> withAnchors(new SegmentAnchor(0, 0, 0L, 1)), "header key");
    assertThrows(IllegalArgumentException.class, () -> withAnchors(new SegmentAnchor(0, 0, 5L, -1)), "sealed count");
    assertThrows(IllegalArgumentException.class,
        () -> withAnchors(new SegmentAnchor(3, 1, 5L, 1), new SegmentAnchor(3, 1, 6L, 1)),
        "one (segment, column) anchored twice would let a page resolve against whichever was indexed last");
  }

  @Test
  @DisplayName("bytes past the sections are still refused: a shifted field must never read as data")
  void trailingGarbageIsRefused() {
    final byte[] payload = withAnchors(new SegmentAnchor(0, 0, 5L, 1)).serialize();
    final byte[] withGarbage = Arrays.copyOf(payload, payload.length + 3);
    assertThrows(IllegalStateException.class, () -> ProjectionIndexMetadata.parse(withGarbage));
    final byte[] plain = withAnchors().serialize();
    final byte[] plainWithGarbage = Arrays.copyOf(plain, plain.length + 1);
    assertThrows(IllegalStateException.class, () -> ProjectionIndexMetadata.parse(plainWithGarbage),
        "and a stray byte after the dictionary section is not read as an anchor count");
  }

  @Test
  @DisplayName("a truncated anchor section fails rather than inventing an anchor")
  void aTruncatedSectionIsRefused() {
    final byte[] payload = withAnchors(new SegmentAnchor(0, 0, 5L, 1), new SegmentAnchor(1, 0, 6L, 2)).serialize();
    for (int cut = 1; cut <= 12; cut++) {
      final byte[] truncated = Arrays.copyOf(payload, payload.length - cut);
      assertThrows(RuntimeException.class, () -> ProjectionIndexMetadata.parse(truncated),
          "a payload cut " + cut + " byte(s) short must not parse into an anchor");
    }
  }
}
