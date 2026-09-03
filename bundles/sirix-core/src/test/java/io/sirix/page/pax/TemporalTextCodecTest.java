/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The temporal text codec, whose whole value rests on one property: an ACCEPTED value must render back
 * to the bytes it was given, and anything that would not must be REFUSED rather than approximated.
 *
 * <p>
 * These tests therefore spend most of their effort on refusal and on exhaustive round trips, not on a
 * handful of happy examples. A codec that silently canonicalises {@code "2013-7-5"} into
 * {@code "2013-07-05"} would pass any example-based test written by the person who wrote the encoder.
 * </p>
 */
final class TemporalTextCodecTest {

  private static byte[] utf8(final String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static long encode(final String s) {
    final byte[] b = utf8(s);
    return TemporalTextCodec.encode(b, 0, b.length, TemporalTextCodec.formOf(b.length));
  }

  private static String roundTrip(final String s) {
    final byte[] b = utf8(s);
    final int form = TemporalTextCodec.formOf(b.length);
    final long v = TemporalTextCodec.encode(b, 0, b.length, form);
    if (v == TemporalTextCodec.REFUSED) {
      return null;
    }
    final byte[] out = new byte[TemporalTextCodec.lengthOf(form)];
    final int n = TemporalTextCodec.decode(v, form, out, 0);
    assertEquals(out.length, n, "decode reports what it wrote");
    return new String(out, 0, n, StandardCharsets.UTF_8);
  }

  @Test
  @DisplayName("EXHAUSTIVE: every date from 1600-01-01 to 2400-12-31 round trips to the same bytes")
  void everyDateRoundTrips() {
    LocalDate d = LocalDate.of(1600, 1, 1);
    final LocalDate end = LocalDate.of(2400, 12, 31);
    int checked = 0;
    while (!d.isAfter(end)) {
      final String s = d.toString();
      assertEquals(s, roundTrip(s), "date round trip");
      // And the encoding agrees with the platform's own epoch-day arithmetic.
      assertEquals(d.toEpochDay(), encode(s), "epoch day for " + s);
      d = d.plusDays(1);
      checked++;
    }
    assertTrue(checked > 292_000, "swept " + checked + " days");
  }

  @Test
  @DisplayName("EXHAUSTIVE: every second of a leap day and a DST-shaped day round trips")
  void everySecondOfADayRoundTrips() {
    for (final LocalDate day : new LocalDate[] {LocalDate.of(2000, 2, 29), LocalDate.of(2013, 7, 15),
        LocalDate.of(1969, 12, 31), LocalDate.of(2024, 3, 31)}) {
      for (int sec = 0; sec < 86_400; sec++) {
        final LocalDateTime t = day.atStartOfDay().plusSeconds(sec);
        final String s = t.toString().replace('T', ' ');
        final String text = s.length() == 16
            ? s + ":00"
            : s.length() == 13
                ? s + ":00:00"
                : s;
        assertEquals(text, roundTrip(text), "datetime round trip at " + text);
      }
    }
  }

  @Test
  @DisplayName("the encoded datetime is seconds since the epoch, negative before it")
  void theEncodingIsEpochSeconds() {
    assertEquals(0L, encode("1970-01-01 00:00:00"));
    assertEquals(1L, encode("1970-01-01 00:00:01"));
    assertEquals(-1L, encode("1969-12-31 23:59:59"));
    assertEquals(-86_400L, encode("1969-12-31 00:00:00"));
    assertEquals(1_373_890_496L, encode("2013-07-15 12:14:56"));
    assertEquals(0L, encode("1970-01-01"));
    assertEquals(-1L, encode("1969-12-31"));
  }

  @Test
  @DisplayName("a PRE-EPOCH datetime decodes with the right time of day (floor division, not truncation)")
  void preEpochTimesUseFloorDivision() {
    // Truncating division would render 1969-12-31 23:59:59 as 1970-01-01 -00:00:-1 or similar.
    for (final String s : new String[] {"1969-12-31 23:59:59", "1969-12-31 00:00:00", "1900-01-01 00:00:01",
        "1969-07-20 20:17:40", "0001-01-01 00:00:00"}) {
      assertEquals(s, roundTrip(s), "pre-epoch " + s);
    }
  }

  @Test
  @DisplayName("REFUSAL: anything that would not render back identically is refused")
  void nonCanonicalTextIsRefused() {
    final String[] refused = {
        // Wrong lengths entirely.
        "", "2013", "2013-07-15 12:14", "2013-07-15 12:14:56.0", "2013-07-15 12:14:56Z",
        // Right length, wrong shape: these are the dangerous ones.
        "2013-07-15T12:14:56", // ISO 'T' separator
        "2013-07-15  12:14:5", // double space, short second
        "2013/07/15 12:14:56", // slashes
        "2013-07-15 12.14.56", // dots for colons
        "+2013-07-15", "2013-7-15 ", " 2013-07-15",
        // Right shape, impossible values.
        "2013-13-01", "2013-00-01", "2013-07-32", "2013-07-00",
        "2013-02-29", // 2013 is not a leap year
        "1900-02-29", // century non-leap
        "2100-02-29", // century non-leap
        "2013-07-15 24:00:00", "2013-07-15 12:60:00", "2013-07-15 12:14:60",
        // Non-digits in digit positions.
        "20a3-07-15", "2013-0x-15", "2013-07-1z", "2013-07-15 1a:14:56",
    };
    for (final String s : refused) {
      assertEquals(null, roundTrip(s), "must be refused: '" + s + "'");
    }
  }

  @Test
  @DisplayName("SYSTEMATIC: corrupting ANY separator position, one at a time, is refused")
  void everySeparatorPositionIsChecked() {
    // A curated list of bad examples over-constrains: "2013/07/15" is caught by the SECOND separator
    // even when the first is unchecked, so a missing check at position 4 hides behind it. Corrupt each
    // separator position on its own instead, and every check has to exist on its own.
    for (final String canonical : new String[] {"2013-07-15", "2013-07-15 12:14:56"}) {
      final int[] separators = canonical.length() == TemporalTextCodec.DATE_LENGTH
          ? new int[] {4, 7}
          : new int[] {4, 7, 10, 13, 16};
      for (final int pos : separators) {
        for (final char wrong : new char[] {'x', '/', '.', ':', '-', ' ', 'T', '0'}) {
          if (wrong == canonical.charAt(pos)) {
            continue;
          }
          final String corrupted =
              canonical.substring(0, pos) + wrong + canonical.substring(pos + 1);
          assertEquals(canonical.length(), corrupted.length(), "corruption kept the length");
          assertEquals(null, roundTrip(corrupted),
              "position " + pos + " of '" + canonical + "' set to '" + wrong + "' must be refused");
        }
      }
    }
  }

  @Test
  @DisplayName("SYSTEMATIC: a non-digit in ANY digit position, one at a time, is refused")
  void everyDigitPositionIsChecked() {
    for (final String canonical : new String[] {"2013-07-15", "2013-07-15 12:14:56"}) {
      for (int pos = 0; pos < canonical.length(); pos++) {
        if (!Character.isDigit(canonical.charAt(pos))) {
          continue;
        }
        for (final char wrong : new char[] {'x', '-', ' ', '/', 'a', '\u007f'}) {
          final String corrupted =
              canonical.substring(0, pos) + wrong + canonical.substring(pos + 1);
          assertEquals(null, roundTrip(corrupted),
              "position " + pos + " of '" + canonical + "' set to '" + wrong + "' must be refused");
        }
      }
    }
  }

  @Test
  @DisplayName("REFUSAL is total: a leap-day is accepted in a leap year and refused otherwise")
  void leapDayFollowsTheCalendar() {
    assertEquals("2000-02-29", roundTrip("2000-02-29"), "2000 is a leap year (divisible by 400)");
    assertEquals("2024-02-29", roundTrip("2024-02-29"));
    assertEquals(null, roundTrip("1900-02-29"), "1900 is not (divisible by 100, not 400)");
    assertEquals(null, roundTrip("2023-02-29"));
    assertTrue(TemporalTextCodec.isLeapYear(2000));
    assertTrue(TemporalTextCodec.isLeapYear(2024));
    assertTrue(!TemporalTextCodec.isLeapYear(1900));
    assertTrue(!TemporalTextCodec.isLeapYear(2023));
    assertEquals(29, TemporalTextCodec.daysInMonth(2024, 2));
    assertEquals(28, TemporalTextCodec.daysInMonth(2023, 2));
    assertEquals(31, TemporalTextCodec.daysInMonth(2023, 12));
  }

  @Test
  @DisplayName("the codec is INJECTIVE: distinct accepted texts never share an encoding")
  void distinctTextsEncodeDistinctly() {
    final Random rnd = new Random(20260903L);
    final HashMap<Long, String> seen = new HashMap<>();
    for (int i = 0; i < 60_000; i++) {
      final LocalDateTime t = LocalDateTime.of(1970, 1, 1, 0, 0, 0).plusSeconds(rnd.nextInt(2_000_000_000));
      final String s = t.toString().replace('T', ' ');
      final String text = s.length() == 16
          ? s + ":00"
          : s.length() == 13
              ? s + ":00:00"
              : s;
      final long v = encode(text);
      assertNotEquals(TemporalTextCodec.REFUSED, v, text);
      final String previous = seen.put(v, text);
      if (previous != null) {
        assertEquals(previous, text, "two distinct texts collided on " + v);
      }
    }
  }

  @Test
  @DisplayName("formOf pre-filters by length only, and never accepts on its own")
  void formOfIsALengthFilter() {
    assertEquals(TemporalTextCodec.FORM_DATE, TemporalTextCodec.formOf(10));
    assertEquals(TemporalTextCodec.FORM_DATETIME, TemporalTextCodec.formOf(19));
    assertEquals(TemporalTextCodec.FORM_REFUSED, TemporalTextCodec.formOf(0));
    assertEquals(TemporalTextCodec.FORM_REFUSED, TemporalTextCodec.formOf(11));
    assertEquals(TemporalTextCodec.FORM_REFUSED, TemporalTextCodec.formOf(18));
    // A ten-byte NON-date passes the length filter and is still refused by encode.
    assertEquals(TemporalTextCodec.FORM_DATE, TemporalTextCodec.formOf("hello-worl".length()));
    assertEquals(TemporalTextCodec.REFUSED, encode("hello-worl"));
  }

  @Test
  @DisplayName("the codec reads and writes at an OFFSET inside a larger buffer")
  void offsetsAreHonoured() {
    final byte[] buf = "xxxx2013-07-15 12:14:56yyyy".getBytes(StandardCharsets.UTF_8);
    final long v = TemporalTextCodec.encode(buf, 4, 19, TemporalTextCodec.FORM_DATETIME);
    assertEquals(1_373_890_496L, v);
    final byte[] out = new byte[30];
    Arrays.fill(out, (byte) '#');
    TemporalTextCodec.decode(v, TemporalTextCodec.FORM_DATETIME, out, 7);
    assertEquals("2013-07-15 12:14:56", new String(out, 7, 19, StandardCharsets.UTF_8));
    assertEquals('#', (char) out[6], "nothing written before the offset");
    assertEquals('#', (char) out[26], "nothing written past the end");
  }

  @Test
  @DisplayName("bad arguments are refused rather than silently corrupting a buffer")
  void badArgumentsAreRefused() {
    final byte[] b = utf8("2013-07-15");
    assertThrows(IndexOutOfBoundsException.class, () -> TemporalTextCodec.encode(b, 4, 10, TemporalTextCodec.FORM_DATE));
    assertThrows(IndexOutOfBoundsException.class,
        () -> TemporalTextCodec.decode(0L, TemporalTextCodec.FORM_DATE, new byte[9], 0));
    assertThrows(IllegalArgumentException.class,
        () -> TemporalTextCodec.decode(0L, TemporalTextCodec.FORM_REFUSED, new byte[32], 0));
    assertThrows(IllegalArgumentException.class, () -> TemporalTextCodec.daysInMonth(2024, 13));
    // A day count far outside the renderable year range must fail loudly, not render a 5-digit year.
    assertThrows(IllegalArgumentException.class,
        () -> TemporalTextCodec.decode(4_000_000L, TemporalTextCodec.FORM_DATE, new byte[32], 0));
    // Length that does not match the form is refused, not read past.
    assertEquals(TemporalTextCodec.REFUSED, TemporalTextCodec.encode(b, 0, 10, TemporalTextCodec.FORM_DATETIME));
  }

  @Test
  @DisplayName("a day count that WRAPS the civil conversion is refused, not rendered as a plausible date")
  void wrappingDayCountsAreRefused() {
    // civilFromDays packs the year into the high half of a long. 1_568_703_873_082 days is year
    // 4_294_967_296 exactly -- 2^32 -- so the year field shifts out entirely and reads back as 0
    // with a month of 1 and a day of 1. A range check made AFTER the conversion sees "0000-01-01",
    // accepts it, and the codec writes a date that was never asked for.
    final long wraps = 1_568_703_873_082L;
    final byte[] out = new byte[TemporalTextCodec.DATETIME_LENGTH];
    Arrays.fill(out, (byte) '#');
    assertThrows(IllegalArgumentException.class,
        () -> TemporalTextCodec.decode(wraps, TemporalTextCodec.FORM_DATE, out, 0));
    assertThrows(IllegalArgumentException.class,
        () -> TemporalTextCodec.decode(wraps * 86_400L, TemporalTextCodec.FORM_DATETIME, out, 0));
    for (final byte b : out) {
      assertEquals('#', (char) b, "a refused value must not have written anything");
    }
    assertFalse(TemporalTextCodec.isRepresentable(wraps, TemporalTextCodec.FORM_DATE));
    assertFalse(TemporalTextCodec.isRepresentable(wraps * 86_400L, TemporalTextCodec.FORM_DATETIME));
  }

  @Test
  @DisplayName("the representable range is exactly 0000-01-01..9999-12-31, edges included")
  void theRepresentableRangeIsTheRenderedRange() {
    assertEquals(TemporalTextCodec.MIN_DAYS, TemporalTextCodec.daysFromCivil(0, 1, 1));
    assertEquals(TemporalTextCodec.MAX_DAYS, TemporalTextCodec.daysFromCivil(9999, 12, 31));
    final byte[] out = new byte[TemporalTextCodec.DATETIME_LENGTH];
    TemporalTextCodec.decode(TemporalTextCodec.MIN_DAYS, TemporalTextCodec.FORM_DATE, out, 0);
    assertEquals("0000-01-01", new String(out, 0, TemporalTextCodec.DATE_LENGTH, StandardCharsets.UTF_8));
    TemporalTextCodec.decode(TemporalTextCodec.MAX_DAYS, TemporalTextCodec.FORM_DATE, out, 0);
    assertEquals("9999-12-31", new String(out, 0, TemporalTextCodec.DATE_LENGTH, StandardCharsets.UTF_8));
    TemporalTextCodec.decode(TemporalTextCodec.MIN_SECONDS, TemporalTextCodec.FORM_DATETIME, out, 0);
    assertEquals("0000-01-01 00:00:00", new String(out, StandardCharsets.UTF_8));
    TemporalTextCodec.decode(TemporalTextCodec.MAX_SECONDS, TemporalTextCodec.FORM_DATETIME, out, 0);
    assertEquals("9999-12-31 23:59:59", new String(out, StandardCharsets.UTF_8));
    // One step past either edge is refused on both forms, and REFUSED is not a form at all.
    for (final long value : new long[] {TemporalTextCodec.MIN_DAYS - 1, TemporalTextCodec.MAX_DAYS + 1}) {
      assertFalse(TemporalTextCodec.isRepresentable(value, TemporalTextCodec.FORM_DATE), "days " + value);
      assertThrows(IllegalArgumentException.class,
          () -> TemporalTextCodec.decode(value, TemporalTextCodec.FORM_DATE, new byte[32], 0));
    }
    for (final long value : new long[] {TemporalTextCodec.MIN_SECONDS - 1, TemporalTextCodec.MAX_SECONDS + 1}) {
      assertFalse(TemporalTextCodec.isRepresentable(value, TemporalTextCodec.FORM_DATETIME), "seconds " + value);
      assertThrows(IllegalArgumentException.class,
          () -> TemporalTextCodec.decode(value, TemporalTextCodec.FORM_DATETIME, new byte[32], 0));
    }
    assertFalse(TemporalTextCodec.isRepresentable(0L, TemporalTextCodec.FORM_REFUSED));
    // Long.MIN_VALUE is the REFUSED sentinel: it must never be mistaken for a renderable instant.
    assertFalse(TemporalTextCodec.isRepresentable(TemporalTextCodec.REFUSED, TemporalTextCodec.FORM_DATE));
    assertFalse(TemporalTextCodec.isRepresentable(TemporalTextCodec.REFUSED, TemporalTextCodec.FORM_DATETIME));
  }

  @Test
  @DisplayName("every value ENCODE produces is representable, so the bound refuses nothing honest")
  void everyEncodedValueIsRepresentable() {
    LocalDate d = LocalDate.of(1, 1, 1);
    final LocalDate end = LocalDate.of(9999, 12, 31);
    while (!d.isAfter(end)) {
      final long days = encode(d.toString());
      assertNotEquals(TemporalTextCodec.REFUSED, days, "date " + d);
      assertTrue(TemporalTextCodec.isRepresentable(days, TemporalTextCodec.FORM_DATE), "date " + d);
      assertTrue(TemporalTextCodec.isRepresentable(days * 86_400L + 86_399L, TemporalTextCodec.FORM_DATETIME),
          "datetime " + d);
      d = d.plusDays(97);
    }
  }

  @Test
  @DisplayName("the civil/day conversions are exact inverses across the whole rendered range")
  void civilConversionsAreInverses() {
    for (long days = TemporalTextCodec.daysFromCivil(1, 1, 1); days <= TemporalTextCodec.daysFromCivil(9999, 12, 31);
        days += 7) {
      final long ymd = TemporalTextCodec.civilFromDays(days);
      final int y = (int) (ymd >> 32);
      final int m = (int) (ymd >> 16) & 0xFFFF;
      final int d = (int) ymd & 0xFFFF;
      assertEquals(days, TemporalTextCodec.daysFromCivil(y, m, d), "inverse at " + y + "-" + m + "-" + d);
      assertEquals(LocalDate.ofEpochDay(days), LocalDate.of(y, m, d), "agrees with the platform at " + days);
    }
  }
}
