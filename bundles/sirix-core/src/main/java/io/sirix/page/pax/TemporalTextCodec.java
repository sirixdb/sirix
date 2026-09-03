/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import java.util.Objects;

/**
 * A BIJECTIVE codec for the two fixed timestamp texts a record corpus stores by the hundred million:
 * {@code "YYYY-MM-DD"} and {@code "YYYY-MM-DD HH:MM:SS"}.
 *
 * <p>
 * Three columns of a ClickBench-shaped corpus (LocalEventTime, EventTime, ClientEventTime) are 17.6 %
 * of the string region measured at 19.31 GB written at 100M — 19 bytes of text plus a length per value,
 * for a quantity that is an integer. Stored as a day or second count they are four bytes and, being
 * near-sorted within a leaf, delta-pack to far less. Nothing about the encoding is corpus-specific: it
 * accepts exactly the two ISO-8601 profiles above and refuses everything else.
 * </p>
 *
 * <h2>Why refusal is the interesting half</h2>
 *
 * A text codec that is merely {@code parse} then {@code render} is a DATA-LOSS bug waiting for its
 * input: {@code "2013-7-5"}, {@code "2013-07-15T12:34:56"}, a trailing space, or a second field of
 * {@code 60} all parse under a lenient reading and render back as something else. This codec is
 * therefore defined by its canonical form and REFUSES any byte sequence it could not reproduce
 * exactly — every accepted encoding satisfies {@code decode(encode(s)) == s} BY CONSTRUCTION, not by
 * hope. Callers must treat {@link #REFUSED} as "store the text", which is always correct and never
 * worse than today.
 *
 * <h2>Shape</h2>
 *
 * Encoding returns a primitive and touches no allocation: a {@code long} of days (DATE) or seconds
 * (DATETIME) since 1970-01-01, signed, so a lane of them is monotone within a leaf and packs under the
 * existing frame-of-reference machinery. The FORM is a property of the tag, not of the value — a column
 * is all dates or all datetimes — so it is decided once per tag by {@link #formOf} and never stored
 * per value.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class TemporalTextCodec {

  /** Not one of the accepted forms: the caller must store the text unchanged. */
  public static final int FORM_REFUSED = 0;

  /** {@code "YYYY-MM-DD"}, encoded as signed DAYS since 1970-01-01. */
  public static final int FORM_DATE = 1;

  /** {@code "YYYY-MM-DD HH:MM:SS"}, encoded as signed SECONDS since 1970-01-01T00:00:00. */
  public static final int FORM_DATETIME = 2;

  /** Returned by {@link #encode} when the bytes are not exactly the form claimed. */
  public static final long REFUSED = Long.MIN_VALUE;

  /** Text length of {@link #FORM_DATE}. */
  public static final int DATE_LENGTH = 10;

  /** Text length of {@link #FORM_DATETIME}. */
  public static final int DATETIME_LENGTH = 19;

  private static final int SECONDS_PER_DAY = 86_400;

  /** Days in each month of a non-leap year, indexed from 1. */
  private static final int[] MONTH_LENGTHS = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

  private TemporalTextCodec() {
    throw new AssertionError("no instances");
  }

  /**
   * The form {@code length} could encode, by length alone — a cheap pre-filter that lets a caller skip
   * the byte inspection for values that cannot possibly qualify. A positive answer is NOT acceptance;
   * only {@link #encode} accepts.
   *
   * @param length candidate text length in bytes
   * @return {@link #FORM_DATE}, {@link #FORM_DATETIME} or {@link #FORM_REFUSED}
   */
  public static int formOf(final int length) {
    if (length == DATE_LENGTH) {
      return FORM_DATE;
    }
    if (length == DATETIME_LENGTH) {
      return FORM_DATETIME;
    }
    return FORM_REFUSED;
  }

  /** Text length a form renders, or {@code 0} for {@link #FORM_REFUSED}. */
  public static int lengthOf(final int form) {
    return switch (form) {
      case FORM_DATE -> DATE_LENGTH;
      case FORM_DATETIME -> DATETIME_LENGTH;
      default -> 0;
    };
  }

  /**
   * Encode {@code len} bytes at {@code off} under {@code form}, or {@link #REFUSED} when they are not
   * EXACTLY that form in canonical shape: ASCII digits in every digit position, {@code '-'} separators,
   * a single {@code ' '} before the time, zero-padded fields, a calendar-valid date (leap years
   * included) and {@code 00:00:00}..{@code 23:59:59}. A leading {@code '+'}, a {@code 'T'} separator,
   * a {@code 60} second and a year outside {@code 0000}..{@code 9999} are all refused.
   *
   * @param src the buffer
   * @param off offset of the first byte
   * @param len byte length, which must match {@link #lengthOf}
   * @param form {@link #FORM_DATE} or {@link #FORM_DATETIME}
   * @return days (DATE) or seconds (DATETIME) since the epoch, or {@link #REFUSED}
   * @throws NullPointerException if {@code src} is null
   * @throws IndexOutOfBoundsException if the range is not within {@code src}
   */
  public static long encode(final byte[] src, final int off, final int len, final int form) {
    Objects.checkFromIndexSize(off, len, src.length);
    // FORM_REFUSED has a length of zero, so comparing against lengthOf alone would let a zero-length
    // range through and read past the array. A sentinel must never be validated against its own value.
    if (form != FORM_DATE && form != FORM_DATETIME) {
      return REFUSED;
    }
    if (len != lengthOf(form)) {
      return REFUSED;
    }
    final int year = digits4(src, off);
    if (year < 0 || src[off + 4] != '-') {
      return REFUSED;
    }
    final int month = digits2(src, off + 5);
    if (month < 1 || month > 12 || src[off + 7] != '-') {
      return REFUSED;
    }
    final int day = digits2(src, off + 8);
    if (day < 1 || day > daysInMonth(year, month)) {
      return REFUSED;
    }
    final long days = daysFromCivil(year, month, day);
    if (form == FORM_DATE) {
      return days;
    }
    if (src[off + 10] != ' ') {
      return REFUSED;
    }
    final int hour = digits2(src, off + 11);
    if (hour < 0 || hour > 23 || src[off + 13] != ':') {
      return REFUSED;
    }
    final int minute = digits2(src, off + 14);
    if (minute < 0 || minute > 59 || src[off + 16] != ':') {
      return REFUSED;
    }
    final int second = digits2(src, off + 17);
    if (second < 0 || second > 59) {
      return REFUSED;
    }
    return days * SECONDS_PER_DAY + hour * 3600L + minute * 60L + second;
  }

  /**
   * Render {@code value} back into {@code dst} at {@code off} under {@code form} — the exact bytes
   * {@link #encode} was given.
   *
   * @param value the encoded days or seconds
   * @param form {@link #FORM_DATE} or {@link #FORM_DATETIME}
   * @param dst destination buffer
   * @param off offset to write at
   * @return bytes written
   * @throws IllegalArgumentException if {@code form} is not an accepted form, or {@code value} lies
   *         outside the representable {@code 0000}..{@code 9999} year range
   * @throws IndexOutOfBoundsException if the rendered text does not fit
   */
  public static int decode(final long value, final int form, final byte[] dst, final int off) {
    final int len = lengthOf(form);
    if (len == 0) {
      throw new IllegalArgumentException("not an encodable form: " + form);
    }
    Objects.checkFromIndexSize(off, len, dst.length);
    final long days;
    int secondOfDay = 0;
    if (form == FORM_DATE) {
      days = value;
    } else {
      // Floor division: a pre-epoch instant has a negative second count but a positive time of day.
      days = Math.floorDiv(value, (long) SECONDS_PER_DAY);
      secondOfDay = (int) Math.floorMod(value, (long) SECONDS_PER_DAY);
    }
    final long ymd = civilFromDays(days);
    final int year = (int) (ymd >> 32);
    final int month = (int) (ymd >> 16) & 0xFFFF;
    final int day = (int) ymd & 0xFFFF;
    if (year < 0 || year > 9999) {
      throw new IllegalArgumentException("year outside the representable range: " + year);
    }
    write4(dst, off, year);
    dst[off + 4] = '-';
    write2(dst, off + 5, month);
    dst[off + 7] = '-';
    write2(dst, off + 8, day);
    if (form == FORM_DATE) {
      return DATE_LENGTH;
    }
    dst[off + 10] = ' ';
    write2(dst, off + 11, secondOfDay / 3600);
    dst[off + 13] = ':';
    write2(dst, off + 14, secondOfDay / 60 % 60);
    dst[off + 16] = ':';
    write2(dst, off + 17, secondOfDay % 60);
    return DATETIME_LENGTH;
  }

  /** Days in {@code month} of {@code year}, Gregorian. */
  public static int daysInMonth(final int year, final int month) {
    if (month < 1 || month > 12) {
      throw new IllegalArgumentException("month out of range: " + month);
    }
    if (month == 2 && isLeapYear(year)) {
      return 29;
    }
    return MONTH_LENGTHS[month];
  }

  /** Whether {@code year} is a Gregorian leap year. */
  public static boolean isLeapYear(final int year) {
    return (year & 3) == 0 && (year % 100 != 0 || year % 400 == 0);
  }

  /**
   * Days since 1970-01-01 for a proleptic Gregorian date (Howard Hinnant's {@code days_from_civil}):
   * exact for every year in range and free of both lookup tables and division by a variable.
   */
  public static long daysFromCivil(final int year, final int month, final int day) {
    final int y = year - (month <= 2
        ? 1
        : 0);
    final int era = (y >= 0
        ? y
        : y - 399) / 400;
    final int yoe = y - era * 400;
    final int doy = (153 * (month + (month > 2
        ? -3
        : 9)) + 2) / 5 + day - 1;
    final int doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return (long) era * 146_097 + doe - 719_468;
  }

  /**
   * The inverse of {@link #daysFromCivil}, packed as {@code year << 32 | month << 16 | day}.
   */
  public static long civilFromDays(final long days) {
    final long z = days + 719_468;
    final long era = (z >= 0
        ? z
        : z - 146_096) / 146_097;
    final long doe = z - era * 146_097;
    final long yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    final long y = yoe + era * 400;
    final long doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    final long mp = (5 * doy + 2) / 153;
    final long d = doy - (153 * mp + 2) / 5 + 1;
    final long m = mp + (mp < 10
        ? 3
        : -9);
    return (y + (m <= 2
        ? 1
        : 0)) << 32 | m << 16 | d;
  }

  /** Four ASCII digits as an int, or {@code -1} if any byte is not a digit. */
  private static int digits4(final byte[] src, final int off) {
    final int a = digit(src[off]);
    final int b = digit(src[off + 1]);
    final int c = digit(src[off + 2]);
    final int d = digit(src[off + 3]);
    if ((a | b | c | d) < 0) {
      return -1;
    }
    return a * 1000 + b * 100 + c * 10 + d;
  }

  /** Two ASCII digits as an int, or {@code -1} if either byte is not a digit. */
  private static int digits2(final byte[] src, final int off) {
    final int a = digit(src[off]);
    final int b = digit(src[off + 1]);
    if ((a | b) < 0) {
      return -1;
    }
    return a * 10 + b;
  }

  private static int digit(final byte b) {
    final int d = b - '0';
    return d >= 0 && d <= 9
        ? d
        : -1;
  }

  private static void write4(final byte[] dst, final int off, final int value) {
    dst[off] = (byte) ('0' + value / 1000);
    dst[off + 1] = (byte) ('0' + value / 100 % 10);
    dst[off + 2] = (byte) ('0' + value / 10 % 10);
    dst[off + 3] = (byte) ('0' + value % 10);
  }

  private static void write2(final byte[] dst, final int off, final int value) {
    dst[off] = (byte) ('0' + value / 10);
    dst[off + 1] = (byte) ('0' + value % 10);
  }
}
