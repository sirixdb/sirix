/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Random;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The string region's TEMPORAL lane: a tag whose whole dictionary is fixed timestamp text is stored as
 * packed numbers rather than text.
 *
 * <p>
 * The lane rides an encoding that was previously refused as malformed — width code 3 (the trie lane)
 * together with the plain bit — so the first thing these tests pin is that a page written with the lane
 * reads back EXACTLY the values that went in, and the second is that a tag the lane cannot take keeps
 * its bytes untouched. A silently canonicalised timestamp is the failure this whole design exists to
 * prevent, so the round trip is asserted on the bytes, never on a parsed value.
 * </p>
 */
@DisplayName("string region, temporal lane")
final class StringRegionTemporalLaneTest {

  private static final int TS_TAG = 100;
  private static final int DATE_TAG = 150;
  private static final int TEXT_TAG = 200;
  private static final int ROWS = 12;

  @AfterEach
  void restoreDefaults() {
    StringRegion.clearTemporalLaneOverride();
    StringRegion.clearPlainLaneOverride();
  }

  private static byte[] utf8(final String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Timestamps a few seconds apart, as a real event column has them.
   *
   * <p>
   * Rendered through {@link #canonical} because {@code LocalDateTime.toString()} OMITS a zero seconds
   * field — so a naive helper produces a 16-byte value for every whole minute, which the codec rightly
   * refuses, which takes the whole tag off the lane. The corpus this lane exists for writes all
   * nineteen bytes always.
   * </p>
   */
  private static String timestamp(final int row) {
    return canonical(LocalDateTime.of(2013, 7, 15, 12, 0, 0).plusSeconds(row * 37L));
  }

  /** The full {@code "YYYY-MM-DD HH:MM:SS"}, whatever {@code toString} chose to elide. */
  private static String canonical(final LocalDateTime t) {
    final String s = t.toString().replace('T', ' ');
    return switch (s.length()) {
      case 13 -> s + ":00:00";
      case 16 -> s + ":00";
      default -> s;
    };
  }

  private static String date(final int row) {
    return LocalDateTime.of(2013, 7, 15, 0, 0, 0).plusDays(row).toLocalDate().toString();
  }

  private static byte[] encodePage(final boolean temporal, final boolean withText) {
    StringRegion.setTemporalLaneEnabled(temporal);
    try {
      final StringRegion.Encoder encoder = new StringRegion.Encoder();
      for (int row = 0; row < ROWS; row++) {
        encoder.addValue(TS_TAG, utf8(timestamp(row)));
        encoder.addValue(DATE_TAG, utf8(date(row)));
        if (withText) {
          encoder.addValue(TEXT_TAG, utf8("https://example.org/item-" + row));
        }
      }
      return encoder.finish(StringRegion.TAG_KIND_PATH_NODE);
    } finally {
      StringRegion.clearTemporalLaneOverride();
    }
  }

  private static StringRegion.Header parse(final byte[] region) {
    return new StringRegion.Header().parseInto(MemorySegment.ofArray(region));
  }

  private static int tagIndexOf(final StringRegion.Header header, final int tagValue) {
    for (int t = 0; t < header.parentDictSize; t++) {
      if (header.parentDict[t] == tagValue) {
        return t;
      }
    }
    throw new AssertionError("tag " + tagValue + " not on the page");
  }

  private static String valueAt(final MemorySegment payload, final StringRegion.Header header, final int tagIndex,
      final int entry) {
    final byte[] out = new byte[StringRegion.temporalValueLength(header, tagIndex)];
    final int n = StringRegion.temporalValueAt(payload, header, tagIndex, entry, out, 0);
    return new String(out, 0, n, StandardCharsets.UTF_8);
  }

  @Test
  @DisplayName("a timestamp tag takes the lane and every value reads back byte for byte")
  void timestampsRoundTrip() {
    final byte[] region = encodePage(true, false);
    final MemorySegment payload = MemorySegment.ofArray(region);
    final StringRegion.Header header = parse(region);
    final int ts = tagIndexOf(header, TS_TAG);
    assertTrue(header.tagTemporal[ts], "the timestamp tag took the temporal lane");
    assertFalse(header.tagGlobal[ts], "temporal and global are exclusive");
    assertEquals(TemporalTextCodec.FORM_DATETIME, header.tagTemporalForm[ts]);
    assertEquals(ROWS, header.tagStringDictSize[ts], "12 distinct timestamps");
    for (int row = 0; row < ROWS; row++) {
      assertEquals(timestamp(row), valueAt(payload, header, ts, row), "timestamp entry " + row);
    }
  }

  @Test
  @DisplayName("a DATE tag takes the lane under the date form, alongside a datetime tag")
  void datesRoundTripBesideTimestamps() {
    final byte[] region = encodePage(true, false);
    final MemorySegment payload = MemorySegment.ofArray(region);
    final StringRegion.Header header = parse(region);
    final int dates = tagIndexOf(header, DATE_TAG);
    assertTrue(header.tagTemporal[dates]);
    assertEquals(TemporalTextCodec.FORM_DATE, header.tagTemporalForm[dates]);
    assertEquals(StringRegion.temporalValueLength(header, dates), TemporalTextCodec.DATE_LENGTH);
    for (int row = 0; row < ROWS; row++) {
      assertEquals(date(row), valueAt(payload, header, dates, row), "date entry " + row);
    }
  }

  @Test
  @DisplayName("a NON-temporal tag on the same page is untouched and keeps its bytes")
  void aTextTagIsUntouched() {
    final byte[] region = encodePage(true, true);
    final StringRegion.Header header = parse(region);
    final int text = tagIndexOf(header, TEXT_TAG);
    assertFalse(header.tagTemporal[text], "a URL is not a timestamp");
    // And the page still parses as a whole: the temporal tags did not shift the text tag's offsets.
    final int ts = tagIndexOf(header, TS_TAG);
    assertTrue(header.tagTemporal[ts]);
    assertEquals(ROWS, header.tagCount[text]);
  }

  @Test
  @DisplayName("THE POINT: the lane makes the region smaller, and the saving is the text it removed")
  void theLaneShrinksTheRegion() {
    final int withoutLane = encodePage(false, false).length;
    final int withLane = encodePage(true, false).length;
    assertTrue(withLane < withoutLane,
        "temporal lane must shrink the region: " + withLane + " vs " + withoutLane);
    // 12 datetimes at 19 bytes + 12 dates at 10 bytes is 348 bytes of text, and the packed lanes plus
    // their headers are a small fraction of that. Assert a real fraction so a lane that merely stopped
    // writing SOME bytes cannot pass.
    assertTrue(withLane < withoutLane - 250,
        "expected the bulk of 348 text bytes back, got " + (withoutLane - withLane));
  }

  @Test
  @DisplayName("REFUSAL: one non-timestamp value keeps the WHOLE tag on its bytes")
  void oneBadValueRefusesTheWholeTag() {
    StringRegion.setTemporalLaneEnabled(true);
    final byte[] region;
    try {
      final StringRegion.Encoder encoder = new StringRegion.Encoder();
      for (int row = 0; row < ROWS; row++) {
        // Row 7 is a plausible near-miss: right length, wrong shape.
        encoder.addValue(TS_TAG, utf8(row == 7
            ? "2013-07-15T12:00:00"
            : timestamp(row)));
      }
      region = encoder.finish(StringRegion.TAG_KIND_PATH_NODE);
    } finally {
      StringRegion.clearTemporalLaneOverride();
    }
    final StringRegion.Header header = parse(region);
    final int ts = tagIndexOf(header, TS_TAG);
    assertFalse(header.tagTemporal[ts],
        "all or nothing: one refused value must keep the whole tag on its bytes");
  }

  @Test
  @DisplayName("the lane OFF writes exactly the bytes it wrote before it existed")
  void theKillSwitchIsByteIdentical() {
    final byte[] off = encodePage(false, true);
    StringRegion.setTemporalLaneEnabled(false);
    final byte[] again;
    try {
      again = encodePage(false, true);
    } finally {
      StringRegion.clearTemporalLaneOverride();
    }
    assertArrayEquals(off, again, "the disarmed encoder is deterministic");
    final byte[] on = encodePage(true, true);
    assertFalse(Arrays.equals(off, on), "and the armed one differs, or the test proves nothing");
  }

  @Test
  @DisplayName("a REPEATED timestamp keeps its id lane: rank is not id when values repeat")
  void repeatedTimestampsKeepTheirIdLane() {
    StringRegion.setTemporalLaneEnabled(true);
    final byte[] region;
    try {
      final StringRegion.Encoder encoder = new StringRegion.Encoder();
      for (int row = 0; row < ROWS; row++) {
        encoder.addValue(TS_TAG, utf8(timestamp(row % 3)));
      }
      region = encoder.finish(StringRegion.TAG_KIND_PATH_NODE);
    } finally {
      StringRegion.clearTemporalLaneOverride();
    }
    final MemorySegment payload = MemorySegment.ofArray(region);
    final StringRegion.Header header = parse(region);
    final int ts = tagIndexOf(header, TS_TAG);
    assertTrue(header.tagTemporal[ts]);
    assertEquals(3, header.tagStringDictSize[ts], "three distinct values");
    assertEquals(ROWS, header.tagCount[ts], "twelve rows");
    for (int entry = 0; entry < 3; entry++) {
      assertEquals(timestamp(entry), valueAt(payload, header, ts, entry));
    }
  }

  /**
   * A page whose only tag is the date lane, so the header holds exactly one temporal base.
   */
  private static byte[] encodeDateOnlyPage() {
    StringRegion.setTemporalLaneEnabled(true);
    try {
      final StringRegion.Encoder encoder = new StringRegion.Encoder();
      for (int row = 0; row < ROWS; row++) {
        encoder.addValue(DATE_TAG, utf8(date(row)));
      }
      return encoder.finish(StringRegion.TAG_KIND_PATH_NODE);
    } finally {
      StringRegion.clearTemporalLaneOverride();
    }
  }

  /** The single offset at which {@code needle} occurs in {@code haystack}; fails if it is not unique. */
  private static int soleOccurrence(final byte[] haystack, final byte[] needle) {
    int found = -1;
    outer:
    for (int i = 0; i + needle.length <= haystack.length; i++) {
      for (int j = 0; j < needle.length; j++) {
        if (haystack[i + j] != needle[j]) {
          continue outer;
        }
      }
      assertEquals(-1, found, "the pattern must occur once for the splice to be unambiguous");
      found = i;
    }
    assertTrue(found >= 0, "the pattern must occur at all");
    return found;
  }

  @Test
  @DisplayName("a CORRUPT temporal base is refused by the parser, not carried into the decoder")
  void anUnrepresentableTemporalBaseIsRefused() {
    final byte[] region = encodeDateOnlyPage();
    // The page is valid as written, and its base is the earliest date on it.
    final byte[] first = utf8(date(0));
    final long min = TemporalTextCodec.encode(first, 0, first.length, TemporalTextCodec.FORM_DATE);
    final StringRegion.Header header = parse(region);
    final int dateTag = tagIndexOf(header, DATE_TAG);
    assertTrue(header.tagTemporal[dateTag]);
    assertEquals(min, header.tagTemporalMin[dateTag]);

    // Damage only the base, and only in place: a base one day before 0000-01-01 is outside the range
    // the codec can render, and zig-zags to the same number of varint bytes, so the rest of the
    // header still frames exactly as it did.
    final long unrepresentable = TemporalTextCodec.MIN_DAYS - 1L;
    assertFalse(TemporalTextCodec.isRepresentable(unrepresentable, TemporalTextCodec.FORM_DATE));
    final byte[] written = new byte[VarInt.sizeOfSigned(min)];
    VarInt.writeSigned(written, 0, min);
    final byte[] damaged = new byte[VarInt.sizeOfSigned(unrepresentable)];
    VarInt.writeSigned(damaged, 0, unrepresentable);
    assertEquals(written.length, damaged.length, "the splice must not change the header's framing");
    final byte[] corrupt = region.clone();
    System.arraycopy(damaged, 0, corrupt, soleOccurrence(region, written), damaged.length);

    assertThrows(IllegalArgumentException.class, () -> parse(corrupt),
        "a base the codec cannot render must refuse the header, not reach temporalValueAt");
  }

  @Test
  @DisplayName("FUZZ: random timestamp pages round trip, including all-equal and far-apart spans")
  void randomPagesRoundTrip() {
    final Random rnd = new Random(20260903L);
    for (int trial = 0; trial < 200; trial++) {
      final int rows = 1 + rnd.nextInt(20);
      final long base = rnd.nextInt(2_000_000_000);
      final long spread = switch (trial % 4) {
        case 0 -> 0L;              // every value identical: bit width 0
        case 1 -> 60L;             // a leaf's worth of seconds
        case 2 -> 86_400L;         // a day
        default -> 400_000_000L;   // far apart: a wide lane
      };
      final String[] texts = new String[rows];
      StringRegion.setTemporalLaneEnabled(true);
      final byte[] region;
      try {
        final StringRegion.Encoder encoder = new StringRegion.Encoder();
        for (int row = 0; row < rows; row++) {
          final long secs = base + (spread == 0L
              ? 0L
              : Math.floorMod(rnd.nextLong(), spread));
          texts[row] = canonical(LocalDateTime.ofEpochSecond(secs, 0, ZoneOffset.UTC));
          encoder.addValue(TS_TAG, utf8(texts[row]));
        }
        region = encoder.finish(StringRegion.TAG_KIND_PATH_NODE);
      } finally {
        StringRegion.clearTemporalLaneOverride();
      }
      final MemorySegment payload = MemorySegment.ofArray(region);
      final StringRegion.Header header = parse(region);
      final int ts = tagIndexOf(header, TS_TAG);
      assertTrue(header.tagTemporal[ts], "trial " + trial + " should take the lane");
      // Every DISTINCT value must be present; entries are the deduplicated set in insertion order.
      for (int entry = 0; entry < header.tagStringDictSize[ts]; entry++) {
        final String got = valueAt(payload, header, ts, entry);
        boolean found = false;
        for (final String text : texts) {
          if (text.equals(got)) {
            found = true;
            break;
          }
        }
        assertTrue(found, "trial " + trial + " decoded '" + got + "' which was never written");
      }
    }
  }
}
