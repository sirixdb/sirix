package io.sirix.utils;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/** Primitive canonical UTC parsing and shared proleptic Gregorian calendar arithmetic. */
public final class UtcTimestampParser {
  private static final int[] DAYS_IN_MONTH = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  private static final long DAYS_TO_EPOCH_ERA_SHIFT = 719_468L;

  private UtcTimestampParser() {}

  /** Owner-confined reusable result; absence is separate from every representable instant. */
  public static final class ParsedInstant {
    private long epochSecond;
    private int nano;
    private boolean present;

    public long epochSecond() {
      return epochSecond;
    }

    public int nano() {
      return nano;
    }

    public boolean isPresent() {
      return present;
    }

    public void clear() {
      epochSecond = 0;
      nano = 0;
      present = false;
    }

    private void set(final long seconds, final int nanos) {
      epochSecond = seconds;
      nano = nanos;
      present = true;
    }
  }

  /**
   * Parse exactly as {@link Instant#parse}, into caller-owned storage. The canonical four-digit UTC
   * form, with zero to nine fractional digits, creates no temporary objects. Other spellings,
   * including leap seconds, end-of-day and extended years, retain the JDK parser and its errors. A
   * failed parse clears the result.
   */
  public static void parse(final String text, final ParsedInstant result) {
    Objects.requireNonNull(result).clear();
    Objects.requireNonNull(text);
    if (!tryCanonical(text, result)) {
      final Instant instant = Instant.parse(text);
      result.set(instant.getEpochSecond(), instant.getNano());
    }
  }

  /** Missing or invalid index fields are unbounded, as in the original index writer. */
  public static void parseNullable(final @Nullable String text, final ParsedInstant result) {
    if (text == null) {
      result.clear();
      return;
    }
    try {
      parse(text, result);
    } catch (final DateTimeParseException e) {
      // Invalid field values are absent bounds, not index construction failures.
    }
  }

  @SuppressWarnings("StringConcatToTextBlock") // Error Prone crashes on charAt offset arithmetic.
  private static boolean tryCanonical(final String text, final ParsedInstant result) {
    final int length = text.length();
    if ((length != 20 && (length < 22 || length > 30)) || text.charAt(length - 1) != 'Z' || text.charAt(4) != '-'
        || text.charAt(7) != '-' || text.charAt(10) != 'T' || text.charAt(13) != ':' || text.charAt(16) != ':') {
      return false;
    }
    final int year = digits4(text);
    final int month = digits2(text, 5);
    final int day = digits2(text, 8);
    final int hour = digits2(text, 11);
    final int minute = digits2(text, 14);
    final int second = digits2(text, 17);
    if (!validCivil(year, month, day) || hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0
        || second > 59) {
      return false;
    }
    int nano = 0;
    if (length != 20) {
      if (text.charAt(19) != '.') {
        return false;
      }
      for (int i = 20; i < length - 1; i++) {
        final int digit = text.charAt(i) - '0';
        if (digit < 0 || digit > 9) {
          return false;
        }
        nano = nano * 10 + digit;
      }
      for (int i = length - 21; i < 9; i++) {
        nano *= 10;
      }
    }
    result.set(daysFromCivil(year, month, day) * 86_400L + hour * 3_600L + minute * 60L + second, nano);
    return true;
  }

  private static int digits4(final String text) {
    final int a = text.charAt(0) - '0';
    final int b = text.charAt(1) - '0';
    final int c = text.charAt(2) - '0';
    final int d = text.charAt(3) - '0';
    return (a | b | c | d) < 0 || a > 9 || b > 9 || c > 9 || d > 9
        ? -1
        : a * 1000 + b * 100 + c * 10 + d;
  }

  private static int digits2(final String text, final int offset) {
    final int a = text.charAt(offset) - '0';
    final int b = text.charAt(offset + 1) - '0';
    return (a | b) < 0 || a > 9 || b > 9
        ? -1
        : a * 10 + b;
  }

  /** Whether a date is valid in the four-digit ISO year range, including year zero. */
  public static boolean validCivil(final int year, final int month, final int day) {
    if (year < 0 || year > 9999 || month < 1 || month > 12 || day < 1) {
      return false;
    }
    final int max = month == 2 && isLeapYear(year)
        ? 29
        : DAYS_IN_MONTH[month - 1];
    return day <= max;
  }

  private static boolean isLeapYear(final int year) {
    return (year & 3) == 0 && (year % 100 != 0 || year % 400 == 0);
  }

  /** Epoch day using Hinnant's days_from_civil; callers validate civil fields before use. */
  public static long daysFromCivil(final int year, final int month, final int day) {
    final long y = year - (month <= 2
        ? 1
        : 0);
    final long era = (y >= 0
        ? y
        : y - 399) / 400;
    final long yearOfEra = y - era * 400; // [0, 399]
    final long dayOfYear = (153 * (month + (month > 2
        ? -3
        : 9)) + 2) / 5 + day - 1; // [0, 365]
    final long dayOfEra = yearOfEra * 365 + yearOfEra / 4 - yearOfEra / 100 + dayOfYear;
    return era * 146_097 + dayOfEra - DAYS_TO_EPOCH_ERA_SHIFT;
  }
}
