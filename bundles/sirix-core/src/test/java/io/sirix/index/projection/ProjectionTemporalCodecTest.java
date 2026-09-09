package io.sirix.index.projection;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The text ⇆ epoch contract behind the declared temporal projection column kinds.
 *
 * <p>
 * Three properties are load-bearing and each is pinned here rather than inferred from the serving
 * tests, where a formatting bug would surface as one differing character in a serialized record:
 * every canonical value makes the round trip byte-for-byte (a formatter that dropped seconds or
 * zero-padded wrongly fails the exact-string assertions), a value that is not exactly canonical is
 * refused rather than approximated, and the literal→bound rewrite is EXACT for both shapes it
 * accepts and declines everything else.
 */
final class ProjectionTemporalCodecTest {

  private static final byte TS = ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP;
  private static final byte DT = ProjectionIndexRowGroupPage.COLUMN_KIND_DATE;

  private static byte[] utf8(final String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static String formatTimestamp(final long epochSeconds) {
    final byte[] out = new byte[ProjectionTemporalCodec.MAX_TEXT_LENGTH];
    final int length = ProjectionTemporalCodec.formatTimestamp(epochSeconds, out, 0);
    return new String(out, 0, length, StandardCharsets.UTF_8);
  }

  private static String formatDate(final long epochDays) {
    final byte[] out = new byte[ProjectionTemporalCodec.MAX_TEXT_LENGTH];
    final int length = ProjectionTemporalCodec.formatDate(epochDays, out, 0);
    return new String(out, 0, length, StandardCharsets.UTF_8);
  }

  @Test
  @DisplayName("epoch anchors and zero padding are exact — the mutation witness for the formatter")
  void formatterWritesEveryFieldZeroPadded() {
    // Fixed points: a formatter that dropped the seconds, emitted a one-digit month/day/hour, or
    // used the wrong separator changes at least one of these strings.
    assertEquals("1970-01-01T00:00:00", formatTimestamp(0L));
    assertEquals("2013-01-02T03:04:05", formatTimestamp(1_357_095_845L));
    assertEquals("2013-07-15T10:10:00", formatTimestamp(1_373_883_000L));
    assertEquals("2013-12-31T23:59:59", formatTimestamp(1_388_534_399L));
    assertEquals("1969-12-31T23:59:59", formatTimestamp(-1L)); // floor, not truncate-toward-zero
    assertEquals("1970-01-01", formatDate(0L));
    assertEquals("2013-07-15", formatDate(15_901L));
    assertEquals("1969-12-31", formatDate(-1L));
    assertEquals(19, formatTimestamp(0L).length());
    assertEquals(10, formatDate(0L).length());
  }

  @Test
  @DisplayName("every canonical value makes the round trip byte-for-byte")
  void roundTripIsExact() {
    // A day per week across 30 years plus a spread of times of day, and the leap-day neighbourhood.
    for (long day = -8000; day <= 20_000; day += 7) {
      final String dateText = formatDate(day);
      assertEquals(day, ProjectionTemporalCodec.parseDateDays(utf8(dateText), 0, dateText.length()), dateText);
      assertEquals(LocalDate.ofEpochDay(day).toString(), dateText, "date text disagrees with java.time");
      for (final int secondOfDay : new int[] {0, 1, 59, 60, 3599, 3600, 43_199, 43_200, 86_399}) {
        final long epoch = day * ProjectionTemporalCodec.SECONDS_PER_DAY + secondOfDay;
        final String text = formatTimestamp(epoch);
        assertEquals(epoch, ProjectionTemporalCodec.parseTimestampSeconds(utf8(text), 0, text.length()), text);
        assertEquals(LocalDateTime.ofEpochSecond(epoch, 0, ZoneOffset.UTC).toString().length() == 19
            ? LocalDateTime.ofEpochSecond(epoch, 0, ZoneOffset.UTC).toString()
            : LocalDateTime.ofEpochSecond(epoch, 0, ZoneOffset.UTC).toString() + ":00", text,
            "timestamp text disagrees with java.time");
      }
    }
    // Leap years, both directions.
    assertEquals("2024-02-29", formatDate(ProjectionTemporalCodec.daysFromCivil(2024, 2, 29)));
    assertEquals(ProjectionTemporalCodec.daysFromCivil(2000, 2, 29),
        ProjectionTemporalCodec.parseDateDays(utf8("2000-02-29"), 0, 10));
    // 1900 and 2013 are NOT leap years — Feb 29 must be refused, not silently rolled to March 1.
    assertEquals(ProjectionTemporalCodec.NOT_CANONICAL,
        ProjectionTemporalCodec.parseDateDays(utf8("1900-02-29"), 0, 10));
    assertEquals(ProjectionTemporalCodec.NOT_CANONICAL,
        ProjectionTemporalCodec.parseDateDays(utf8("2013-02-29"), 0, 10));
  }

  @Test
  @DisplayName("a value that is not exactly canonical is refused, never approximated")
  void nonCanonicalValuesAreRefused() {
    for (final String bad : new String[] {"2013-7-15T10:00:00", "2013-07-15 10:00:00", "2013-07-15T10:00:00Z",
        "2013-07-15T10:00:00.500", "2013-07-15T10:00", "2013-07-15", "", " 2013-07-15T10:00:00", "2013-13-01T00:00:00",
        "2013-00-01T00:00:00", "2013-07-32T00:00:00", "2013-07-00T00:00:00", "2013-07-15T24:00:00",
        "2013-07-15T10:60:00", "2013-07-15T10:00:60", "2013-07-15t10:00:00", "2013/07/15T10:00:00",
        "20x3-07-15T10:00:00"}) {
      assertEquals(ProjectionTemporalCodec.NOT_CANONICAL,
          ProjectionTemporalCodec.parseTimestampSeconds(utf8(bad), 0, bad.length()),
          "timestamp parser accepted '" + bad + "'");
    }
    for (final String bad : new String[] {"2013-7-15", "2013-07-15T00:00:00", "13-07-15", "2013-07-1", "",
        "2013-07-150", "2013/07/15", "2013-07-3x"}) {
      assertEquals(ProjectionTemporalCodec.NOT_CANONICAL,
          ProjectionTemporalCodec.parseDateDays(utf8(bad), 0, bad.length()), "date parser accepted '" + bad + "'");
    }
    // The build error names the column, the declared shape and the offending text.
    final String message = ProjectionTemporalCodec.notCanonical(TS, 7, utf8("2013-7-15"), 0, 9).getMessage();
    assertTrue(message.contains("column 7"), message);
    assertTrue(message.contains("YYYY-MM-DDTHH:MM:SS"), message);
    assertTrue(message.contains("2013-7-15"), message);
    assertTrue(ProjectionTemporalCodec.notCanonical(DT, 0, "x").getMessage().contains("YYYY-MM-DD"));
  }

  @Test
  @DisplayName("a full canonical literal denotes exactly one stored value")
  void fullLiteralIsExact() {
    final long[] out = new long[2];
    assertEquals(ProjectionTemporalCodec.BOUND_EXACT,
        ProjectionTemporalCodec.boundsForLiteral(TS, utf8("2013-07-15T10:10:00"), 0, 19, out));
    assertEquals(1_373_883_000L, out[0]);
    assertEquals(ProjectionTemporalCodec.BOUND_EXACT,
        ProjectionTemporalCodec.boundsForLiteral(DT, utf8("2013-07-15"), 0, 10, out));
    assertEquals(15_901L, out[0]);
  }

  @Test
  @DisplayName("a canonical prefix denotes the half-open range of everything that starts with it")
  void prefixLiteralIsAHalfOpenRange() {
    final long[] out = new long[2];
    // Timestamp column, day prefix.
    assertEquals(ProjectionTemporalCodec.BOUND_RANGE,
        ProjectionTemporalCodec.boundsForLiteral(TS, utf8("2013-07-15"), 0, 10, out));
    assertEquals(ProjectionTemporalCodec.parseTimestampSeconds(utf8("2013-07-15T00:00:00"), 0, 19), out[0]);
    assertEquals(ProjectionTemporalCodec.parseTimestampSeconds(utf8("2013-07-16T00:00:00"), 0, 19), out[1]);
    // Minute and hour prefixes.
    assertEquals(ProjectionTemporalCodec.BOUND_RANGE,
        ProjectionTemporalCodec.boundsForLiteral(TS, utf8("2013-07-15T10:10"), 0, 16, out));
    assertEquals(out[0] + 60, out[1]);
    assertEquals(ProjectionTemporalCodec.BOUND_RANGE,
        ProjectionTemporalCodec.boundsForLiteral(TS, utf8("2013-07-15T10"), 0, 13, out));
    assertEquals(out[0] + 3600, out[1]);
    // Month prefix, including the December carry into the next year.
    assertEquals(ProjectionTemporalCodec.BOUND_RANGE,
        ProjectionTemporalCodec.boundsForLiteral(DT, utf8("2013-07"), 0, 7, out));
    assertEquals(ProjectionTemporalCodec.daysFromCivil(2013, 7, 1), out[0]);
    assertEquals(ProjectionTemporalCodec.daysFromCivil(2013, 8, 1), out[1]);
    assertEquals(ProjectionTemporalCodec.BOUND_RANGE,
        ProjectionTemporalCodec.boundsForLiteral(DT, utf8("2013-12"), 0, 7, out));
    assertEquals(ProjectionTemporalCodec.daysFromCivil(2014, 1, 1), out[1]);
    // Year prefix.
    assertEquals(ProjectionTemporalCodec.BOUND_RANGE,
        ProjectionTemporalCodec.boundsForLiteral(DT, utf8("2013"), 0, 4, out));
    assertEquals(ProjectionTemporalCodec.daysFromCivil(2013, 1, 1), out[0]);
    assertEquals(ProjectionTemporalCodec.daysFromCivil(2014, 1, 1), out[1]);
  }

  @Test
  @DisplayName("the range is exactly the set of canonical values the string comparison selects")
  void prefixRangeMatchesStringSemantics() {
    final long[] out = new long[2];
    final String literal = "2013-07";
    assertEquals(ProjectionTemporalCodec.BOUND_RANGE,
        ProjectionTemporalCodec.boundsForLiteral(TS, utf8(literal), 0, literal.length(), out));
    // Every second of an hour a day, across four months either side of the prefix: the numeric
    // membership test and the string prefix test must agree on every single value.
    final long from = ProjectionTemporalCodec.daysFromCivil(2013, 5, 1) * ProjectionTemporalCodec.SECONDS_PER_DAY;
    final long to = ProjectionTemporalCodec.daysFromCivil(2013, 10, 1) * ProjectionTemporalCodec.SECONDS_PER_DAY;
    for (long epoch = from; epoch < to; epoch += 3607) {
      final String text = formatTimestamp(epoch);
      final boolean byText = text.startsWith(literal);
      final boolean byBounds = epoch >= out[0] && epoch < out[1];
      assertEquals(byText, byBounds, text);
      // And the ordering rewrites: `< lit` / `> lit` on the text versus the derived bounds.
      assertEquals(text.compareTo(literal) < 0, epoch < out[0], "lt rewrite diverges at " + text);
      assertEquals(text.compareTo(literal) > 0, epoch >= out[0], "gt rewrite diverges at " + text);
    }
  }

  @Test
  @DisplayName("any other literal shape declines rather than guessing a bound")
  void otherLiteralShapesDecline() {
    final long[] out = new long[2];
    for (final String bad : new String[] {"2013-0", "2013-07-1", "2013-07-15T", "2013-07-15T1", "2013-07-15T10:",
        "2013-07-15T10:1", "2013-13", "2013-07-32", "", "x", "2013-07-15T10:10:00.5", "2013-07-15 10:10:00"}) {
      assertEquals(ProjectionTemporalCodec.BOUND_DECLINE,
          ProjectionTemporalCodec.boundsForLiteral(TS, utf8(bad), 0, bad.length(), out),
          "timestamp bound accepted '" + bad + "'");
    }
    // A date column refuses a literal LONGER than its own canonical shape: expressible, but the
    // rule stays the two shapes it can prove.
    assertEquals(ProjectionTemporalCodec.BOUND_DECLINE,
        ProjectionTemporalCodec.boundsForLiteral(DT, utf8("2013-07-15T00:00:00"), 0, 19, out));
    // And a non-temporal kind is never given a bound at all.
    assertEquals(ProjectionTemporalCodec.BOUND_DECLINE, ProjectionTemporalCodec.boundsForLiteral(
        ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT, utf8("2013-07-15"), 0, 10, out));
  }

  @Test
  @DisplayName("the expressible substring windows are divisions of the epoch; the rest decline")
  void substringWindowsAreArithmetic() {
    final long[] d = new long[3];
    assertTrue(ProjectionTemporalCodec.substringDerivation(TS, 1, 16, d));
    assertArrayEquals(new long[] {60L, 0L, ProjectionTemporalCodec.DISPLAY_ISO_MINUTE}, d);
    assertTrue(ProjectionTemporalCodec.substringDerivation(TS, 1, 10, d));
    assertArrayEquals(new long[] {86_400L, 0L, ProjectionTemporalCodec.DISPLAY_ISO_DATE}, d);
    assertTrue(ProjectionTemporalCodec.substringDerivation(TS, 15, 2, d));
    assertArrayEquals(new long[] {60L, 60L, ProjectionTemporalCodec.DISPLAY_TWO_DIGIT}, d);
    assertTrue(ProjectionTemporalCodec.substringDerivation(TS, 12, 2, d));
    assertArrayEquals(new long[] {3_600L, 24L, ProjectionTemporalCodec.DISPLAY_TWO_DIGIT}, d);
    assertTrue(ProjectionTemporalCodec.substringDerivation(DT, 1, 10, d));
    assertArrayEquals(new long[] {0L, 0L, ProjectionTemporalCodec.DISPLAY_ISO_DATE}, d);
    // A calendar month is not a division of the epoch, and neither is a window that straddles a
    // separator or starts mid-field.
    for (final int[] window : new int[][] {{1, 7}, {1, 4}, {1, 11}, {6, 2}, {9, 2}, {13, 2}, {15, 3}, {2, 2}}) {
      assertFalse(ProjectionTemporalCodec.substringDerivation(TS, window[0], window[1], d),
          "accepted substring(" + window[0] + ", " + window[1] + ")");
    }
    assertFalse(ProjectionTemporalCodec.substringDerivation(DT, 1, 7, d));
    assertFalse(ProjectionTemporalCodec.substringDerivation(DT, 1, 4, d));
  }

  @Test
  @DisplayName("each display renders exactly the characters its window would have cut")
  void displaysMatchTheirSubstring() {
    final long[] d = new long[3];
    for (long epoch = 1_356_998_400L; epoch < 1_388_534_400L; epoch += 98_765) { // all of 2013
      final String text = formatTimestamp(epoch);
      for (final int[] window : new int[][] {{1, 19}, {1, 16}, {1, 13}, {1, 10}, {12, 2}, {15, 2}, {18, 2}}) {
        assertTrue(ProjectionTemporalCodec.substringDerivation(TS, window[0], window[1], d));
        long derived = epoch;
        if (d[0] > 0) {
          derived /= d[0];
        }
        if (d[1] > 0) {
          derived %= d[1];
        }
        assertEquals(text.substring(window[0] - 1, window[0] - 1 + window[1]),
            ProjectionTemporalCodec.formatToString((byte) d[2], derived),
            "display " + d[2] + " diverges from substring(" + window[0] + ", " + window[1] + ") of " + text);
      }
    }
  }
}
