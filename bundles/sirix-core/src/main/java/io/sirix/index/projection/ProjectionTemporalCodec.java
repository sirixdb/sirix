package io.sirix.index.projection;

import java.nio.charset.StandardCharsets;

/**
 * The canonical text ⇆ epoch codec behind the declared {@code TIMESTAMP} and {@code DATE}
 * projection column kinds.
 *
 * <h2>Why a codec rather than a parser</h2> A projection column declared {@code xs:dateTime} or
 * {@code xs:date} stores ONE signed long per row — epoch seconds UTC for
 * {@link ProjectionIndexRowGroupPage#COLUMN_KIND_TIMESTAMP}, epoch days UTC for
 * {@link ProjectionIndexRowGroupPage#COLUMN_KIND_DATE} — in the same lane a
 * {@link ProjectionIndexRowGroupPage#COLUMN_KIND_NUMERIC_LONG} column uses, so every zone map, FOR
 * packing, sort, group table and numeric predicate kernel works on it unchanged, and the column
 * costs 8 packed bits per row instead of a per-leaf string dictionary. What the numeric lane cannot
 * do is give the query back the bytes the document held. This class is the only place that maps
 * between them, in both directions, so the round trip cannot drift.
 *
 * <h2>Exactly one shape, validated per value</h2> The accepted text is the ISO-8601 form with no
 * zone, no fraction and no alternative spelling: {@code dddd-dd-ddTdd:dd:dd} (19 chars) for a
 * timestamp and {@code dddd-dd-dd} (10 chars) for a date, with a calendar-valid date and
 * {@code 00:00:00..23:59:59}. There is no sniffing and no partial acceptance: a value of any other
 * shape in a declared temporal column is a BUILD error, because storing it would either lose the
 * original bytes (no formatter can reproduce {@code "2013-7-15"} from a number) or silently make
 * the column answer a different question than the document says. Absent and non-string cells keep
 * the ordinary present/unrepresentable discipline — this is a shape rule, not a type rule.
 *
 * <h2>Formatting is allocation-free</h2> Every {@code format*} method writes ASCII into a
 * caller-owned {@code byte[]}; {@link #scratch()} hands out a per-thread buffer big enough for the
 * longest form, so an emission loop allocates nothing but the one {@link String} the result item
 * actually keeps.
 *
 * <h2>Proleptic Gregorian, no leap seconds</h2> The civil ⇆ days conversion is Howard Hinnant's
 * branch-free {@code days_from_civil} / {@code civil_from_days}, which agrees with
 * {@code java.time.LocalDate.toEpochDay()} over the whole four-digit year range and needs no
 * object. Second 60 is not accepted: it is not a value any JSON corpus produced by a UTC formatter
 * holds, and accepting it would break the round trip (60 normalises to the next minute's 00).
 */
public final class ProjectionTemporalCodec {

  /** {@code dddd-dd-ddTdd:dd:dd}. */
  public static final int TIMESTAMP_TEXT_LENGTH = 19;

  /** {@code dddd-dd-dd}. */
  public static final int DATE_TEXT_LENGTH = 10;

  /** Longest text any {@code format*} method writes. */
  public static final int MAX_TEXT_LENGTH = TIMESTAMP_TEXT_LENGTH;

  /**
   * Returned by the parsers for text that is not exactly canonical.
   *
   * <p>
   * Safe as a sentinel: the canonical shape spells a four-digit year, so every parsable value lies in
   * {@code [-62167219200, 253402300799]} seconds (or {@code [-719528, 2932896]} days) — many orders
   * of magnitude away from {@link Long#MIN_VALUE}.
   */
  public static final long NOT_CANONICAL = Long.MIN_VALUE;

  public static final int SECONDS_PER_MINUTE = 60;
  public static final int SECONDS_PER_HOUR = 3600;
  public static final int SECONDS_PER_DAY = 86_400;

  /** {@link #boundsForLiteral} verdict: the literal is not a canonical value or unit prefix. */
  public static final int BOUND_DECLINE = 0;

  /**
   * {@link #boundsForLiteral} verdict: {@code out[0]} is the one stored value the literal denotes.
   */
  public static final int BOUND_EXACT = 1;

  /**
   * {@link #boundsForLiteral} verdict: the literal denotes the half-open range
   * {@code [out[0], out[1])}.
   */
  public static final int BOUND_RANGE = 2;

  /** No display transform — the caller emits the raw long. */
  public static final byte DISPLAY_NONE = 0;

  /** Epoch seconds → {@code dddd-dd-ddTdd:dd:dd}. */
  public static final byte DISPLAY_ISO_SECOND = 1;

  /** Minutes since the epoch → {@code dddd-dd-ddTdd:dd}. */
  public static final byte DISPLAY_ISO_MINUTE = 2;

  /** Hours since the epoch → {@code dddd-dd-ddTdd}. */
  public static final byte DISPLAY_ISO_HOUR = 3;

  /** Epoch days → {@code dddd-dd-dd}. */
  public static final byte DISPLAY_ISO_DATE = 4;

  /** A two-digit calendar field ({@code 00..99}) → {@code dd}. */
  public static final byte DISPLAY_TWO_DIGIT = 5;

  /**
   * The kill switch: {@code -Dsirix.projection.temporalKinds=false} makes a declared timestamp/date
   * column build and serve as {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_DICT}, exactly as
   * before this lever existed.
   *
   * <p>
   * The property is read ONCE, into a field the only consumer reads per COLUMN (the declared-type →
   * column-kind mapping), never per value — so the volatile that lets a test flip it inside one JVM
   * costs nothing on any path that runs per row.
   */
  private static volatile boolean temporalKindsEnabled =
      !"false".equalsIgnoreCase(System.getProperty("sirix.projection.temporalKinds"));

  /**
   * Per-thread ASCII scratch for the formatters. Emission runs on the query's worker threads and one
   * buffer per thread outlives every query, so a formatted value costs one array write plus the
   * {@link String} the caller keeps.
   */
  private static final ThreadLocal<byte[]> SCRATCH = ThreadLocal.withInitial(() -> new byte[MAX_TEXT_LENGTH]);

  /** Days from 0000-03-01 to 1970-01-01, the shift in Hinnant's civil ⇆ days pair. */
  private static final long DAYS_TO_EPOCH_ERA_SHIFT = 719_468L;

  private static final int[] DAYS_IN_MONTH = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

  private ProjectionTemporalCodec() {
    throw new AssertionError("no instances");
  }

  /** Whether declared temporal types map to the temporal column kinds (see the kill switch). */
  public static boolean temporalKindsEnabled() {
    return temporalKindsEnabled;
  }

  /**
   * Test seam for the kill switch: flip it inside one JVM so both arms can be proven in ONE run,
   * instead of a second forked process nobody remembers to launch.
   *
   * <p>
   * Only the BUILD reads it (through the declared-type mapping), so a store already built keeps the
   * kinds it was built with — which is exactly the production behaviour a redeployment sees.
   *
   * @return the previous setting, for the caller to restore
   */
  public static boolean setTemporalKindsEnabledForTesting(final boolean enabled) {
    final boolean previous = temporalKindsEnabled;
    temporalKindsEnabled = enabled;
    return previous;
  }

  /** A per-thread ASCII buffer of {@link #MAX_TEXT_LENGTH} bytes for the formatters. */
  public static byte[] scratch() {
    return SCRATCH.get();
  }

  // ==== parsing ================================================================================

  /**
   * Parse canonical {@code dddd-dd-ddTdd:dd:dd} UTF-8 into epoch seconds UTC.
   *
   * @return the epoch second, or {@link #NOT_CANONICAL} when the slice is not exactly canonical
   */
  public static long parseTimestampSeconds(final byte[] utf8, final int off, final int len) {
    if (utf8 == null || len != TIMESTAMP_TEXT_LENGTH || off < 0 || off > utf8.length - len) {
      return NOT_CANONICAL;
    }
    if (utf8[off + 4] != '-' || utf8[off + 7] != '-' || utf8[off + 10] != 'T' || utf8[off + 13] != ':'
        || utf8[off + 16] != ':') {
      return NOT_CANONICAL;
    }
    final int year = digits4(utf8, off);
    final int month = digits2(utf8, off + 5);
    final int day = digits2(utf8, off + 8);
    final int hour = digits2(utf8, off + 11);
    final int minute = digits2(utf8, off + 14);
    final int second = digits2(utf8, off + 17);
    if (year < 0 || !validCivil(year, month, day) || hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0
        || second > 59) {
      return NOT_CANONICAL;
    }
    return daysFromCivil(year, month, day) * SECONDS_PER_DAY + hour * (long) SECONDS_PER_HOUR
        + minute * (long) SECONDS_PER_MINUTE + second;
  }

  /**
   * Parse canonical {@code dddd-dd-dd} UTF-8 into epoch days UTC.
   *
   * @return the epoch day, or {@link #NOT_CANONICAL} when the slice is not exactly canonical
   */
  public static long parseDateDays(final byte[] utf8, final int off, final int len) {
    if (utf8 == null || len != DATE_TEXT_LENGTH || off < 0 || off > utf8.length - len) {
      return NOT_CANONICAL;
    }
    if (utf8[off + 4] != '-' || utf8[off + 7] != '-') {
      return NOT_CANONICAL;
    }
    final int year = digits4(utf8, off);
    final int month = digits2(utf8, off + 5);
    final int day = digits2(utf8, off + 8);
    if (year < 0 || !validCivil(year, month, day)) {
      return NOT_CANONICAL;
    }
    return daysFromCivil(year, month, day);
  }

  /**
   * Parse one canonical value for {@code kind}.
   *
   * @param kind {@link ProjectionIndexRowGroupPage#COLUMN_KIND_TIMESTAMP} or
   *        {@link ProjectionIndexRowGroupPage#COLUMN_KIND_DATE}
   * @return the stored long, or {@link #NOT_CANONICAL}
   */
  public static long parse(final byte kind, final byte[] utf8, final int off, final int len) {
    return kind == ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP
        ? parseTimestampSeconds(utf8, off, len)
        : kind == ProjectionIndexRowGroupPage.COLUMN_KIND_DATE
            ? parseDateDays(utf8, off, len)
            : NOT_CANONICAL;
  }

  /** {@link #parse} over a {@link String} — the XML/attribute extraction path, off the bulk lane. */
  public static long parse(final byte kind, final String value) {
    if (value == null) {
      return NOT_CANONICAL;
    }
    final byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
    return parse(kind, utf8, 0, utf8.length);
  }

  /**
   * The build error a non-canonical value in a declared temporal column raises: it names the column,
   * the declared shape and the offending text, so the fix (fix the data, or declare the column
   * {@code string}) is readable off the message.
   */
  public static IllegalArgumentException notCanonical(final byte kind, final int column, final byte[] utf8,
      final int off, final int len) {
    final String text = utf8 == null || off < 0 || len < 0 || off > utf8.length - len
        ? "<unreadable>"
        : new String(utf8, off, Math.min(len, 64), StandardCharsets.UTF_8);
    return notCanonical(kind, column, text);
  }

  /** {@link #notCanonical(byte, int, byte[], int, int)} for an already-decoded value. */
  public static IllegalArgumentException notCanonical(final byte kind, final int column, final String text) {
    final boolean timestamp = kind == ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP;
    return new IllegalArgumentException("projection column " + column + " is declared " + (timestamp
        ? "xs:dateTime and accepts exactly 'YYYY-MM-DDTHH:MM:SS' (UTC, no zone, no fraction)"
        : "xs:date and accepts exactly 'YYYY-MM-DD'") + ", but the record holds '" + text
        + "'. Fix the value, or declare the column 'string' to keep it as text.");
  }

  // ==== formatting =============================================================================

  /**
   * Write {@code dddd-dd-ddTdd:dd:dd} for {@code epochSeconds}.
   *
   * @return {@link #TIMESTAMP_TEXT_LENGTH}
   */
  public static int formatTimestamp(final long epochSeconds, final byte[] out, final int off) {
    final long days = Math.floorDiv(epochSeconds, (long) SECONDS_PER_DAY);
    final int secondOfDay = (int) Math.floorMod(epochSeconds, (long) SECONDS_PER_DAY);
    writeCivilDate(days, out, off);
    out[off + 10] = 'T';
    writeTwoDigits(secondOfDay / SECONDS_PER_HOUR, out, off + 11);
    out[off + 13] = ':';
    writeTwoDigits(secondOfDay / SECONDS_PER_MINUTE % 60, out, off + 14);
    out[off + 16] = ':';
    writeTwoDigits(secondOfDay % SECONDS_PER_MINUTE, out, off + 17);
    return TIMESTAMP_TEXT_LENGTH;
  }

  /**
   * Write {@code dddd-dd-dd} for {@code epochDays}.
   *
   * @return {@link #DATE_TEXT_LENGTH}
   */
  public static int formatDate(final long epochDays, final byte[] out, final int off) {
    writeCivilDate(epochDays, out, off);
    return DATE_TEXT_LENGTH;
  }

  /**
   * Write the text {@code display} denotes for {@code value}.
   *
   * @param display one of the {@code DISPLAY_*} constants other than {@link #DISPLAY_NONE}
   * @return the number of bytes written
   */
  public static int formatDisplay(final byte display, final long value, final byte[] out, final int off) {
    switch (display) {
      case DISPLAY_ISO_SECOND -> {
        return formatTimestamp(value, out, off);
      }
      case DISPLAY_ISO_MINUTE -> {
        formatTimestamp(Math.multiplyExact(value, (long) SECONDS_PER_MINUTE), out, off);
        return 16;
      }
      case DISPLAY_ISO_HOUR -> {
        formatTimestamp(Math.multiplyExact(value, (long) SECONDS_PER_HOUR), out, off);
        return 13;
      }
      case DISPLAY_ISO_DATE -> {
        return formatDate(value, out, off);
      }
      case DISPLAY_TWO_DIGIT -> {
        if (value < 0 || value > 99) {
          throw new IllegalArgumentException("two-digit temporal field out of range: " + value);
        }
        writeTwoDigits((int) value, out, off);
        return 2;
      }
      default -> throw new IllegalArgumentException("not a temporal display kind: " + display);
    }
  }

  /**
   * {@link #formatDisplay} into the per-thread {@link #scratch()} buffer and hand back the text.
   *
   * <p>
   * The one allocation on the emission path — the {@link String} the result item keeps. Decoding as
   * ISO-8859-1 is exact and takes the JDK's byte-to-latin1 fast path, because every byte a formatter
   * writes is an ASCII digit or separator.
   */
  public static String formatToString(final byte display, final long value) {
    final byte[] buffer = SCRATCH.get();
    final int length = formatDisplay(display, value, buffer, 0);
    return new String(buffer, 0, length, StandardCharsets.ISO_8859_1);
  }

  /** The untransformed display for a temporal column kind, or {@link #DISPLAY_NONE} for any other. */
  public static byte displayOf(final byte columnKind) {
    return columnKind == ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP
        ? DISPLAY_ISO_SECOND
        : columnKind == ProjectionIndexRowGroupPage.COLUMN_KIND_DATE
            ? DISPLAY_ISO_DATE
            : DISPLAY_NONE;
  }

  // ==== literal → numeric bounds ===============================================================

  /**
   * Map a STRING literal compared against a temporal column onto the column's stored longs.
   *
   * <p>
   * Two literal shapes are expressible, and only two. A FULL canonical value denotes exactly one
   * stored long ({@link #BOUND_EXACT}). A shorter literal that is a canonical prefix ending on a unit
   * boundary — year, month, day, and for a timestamp also hour and minute — denotes the half-open
   * range of every value that starts with it ({@link #BOUND_RANGE}: {@code [out[0], out[1])}). Both
   * are exact rewrites of the STRING comparison, because every stored value has the same fixed width:
   * no value can equal a short literal, and a value sorts before/after it exactly as its epoch sorts
   * against the range. Anything else — a different width, a bad separator, an impossible calendar
   * date, a prefix ending mid-field like {@code "2013-0"} — returns {@link #BOUND_DECLINE} and the
   * caller must leave the comparison to the interpreter rather than guess a bound.
   *
   * @param out2 receives the exact value in {@code [0]}, or the inclusive low and EXCLUSIVE high
   * @return one of {@link #BOUND_DECLINE}, {@link #BOUND_EXACT}, {@link #BOUND_RANGE}
   */
  public static int boundsForLiteral(final byte kind, final byte[] literalUtf8, final int off, final int len,
      final long[] out2) {
    if (out2 == null || out2.length < 2) {
      throw new IllegalArgumentException("out2 must hold at least two longs");
    }
    final boolean timestamp = kind == ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP;
    if (!timestamp && kind != ProjectionIndexRowGroupPage.COLUMN_KIND_DATE) {
      return BOUND_DECLINE;
    }
    if (literalUtf8 == null || off < 0 || len < 0 || off > literalUtf8.length - len) {
      return BOUND_DECLINE;
    }
    final int fullLength = timestamp
        ? TIMESTAMP_TEXT_LENGTH
        : DATE_TEXT_LENGTH;
    if (len > fullLength || !isUnitBoundary(timestamp, len)) {
      return BOUND_DECLINE;
    }
    // Every prefix carries the year; the deeper fields appear only at their own boundaries.
    if (len >= 5 && literalUtf8[off + 4] != '-') {
      return BOUND_DECLINE;
    }
    if (len >= 8 && literalUtf8[off + 7] != '-') {
      return BOUND_DECLINE;
    }
    if (len >= 11 && literalUtf8[off + 10] != 'T') {
      return BOUND_DECLINE;
    }
    if (len >= 14 && literalUtf8[off + 13] != ':') {
      return BOUND_DECLINE;
    }
    if (len >= 17 && literalUtf8[off + 16] != ':') {
      return BOUND_DECLINE;
    }
    final int year = digits4(literalUtf8, off);
    if (year < 0) {
      return BOUND_DECLINE;
    }
    final int month = len >= 7
        ? digits2(literalUtf8, off + 5)
        : 1;
    final int day = len >= 10
        ? digits2(literalUtf8, off + 8)
        : 1;
    final int hour = len >= 13
        ? digits2(literalUtf8, off + 11)
        : 0;
    final int minute = len >= 16
        ? digits2(literalUtf8, off + 14)
        : 0;
    final int second = len >= 19
        ? digits2(literalUtf8, off + 17)
        : 0;
    if (!validCivil(year, month, day) || hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0
        || second > 59) {
      return BOUND_DECLINE;
    }
    final long days = daysFromCivil(year, month, day);
    if (!timestamp) {
      out2[0] = days;
      if (len == DATE_TEXT_LENGTH) {
        return BOUND_EXACT;
      }
      out2[1] = len == 7
          ? daysFromCivil(year + (month == 12
              ? 1
              : 0), month == 12
                  ? 1
                  : month + 1,
              1)
          : daysFromCivil(year + 1, 1, 1);
      return BOUND_RANGE;
    }
    final long low =
        days * SECONDS_PER_DAY + hour * (long) SECONDS_PER_HOUR + minute * (long) SECONDS_PER_MINUTE + second;
    out2[0] = low;
    if (len == TIMESTAMP_TEXT_LENGTH) {
      return BOUND_EXACT;
    }
    out2[1] = switch (len) {
      case 16 -> low + SECONDS_PER_MINUTE;
      case 13 -> low + SECONDS_PER_HOUR;
      case 10 -> low + SECONDS_PER_DAY;
      case 7 -> daysFromCivil(year + (month == 12
          ? 1
          : 0), month == 12
              ? 1
              : month + 1,
          1) * SECONDS_PER_DAY;
      default -> daysFromCivil(year + 1, 1, 1) * SECONDS_PER_DAY;
    };
    return BOUND_RANGE;
  }

  /** The literal lengths that end on a calendar unit boundary of the kind's canonical shape. */
  private static boolean isUnitBoundary(final boolean timestamp, final int len) {
    return len == 4 || len == 7 || len == 10 || (timestamp && (len == 13 || len == 16 || len == 19));
  }

  // ==== substring shapes as arithmetic =========================================================

  /**
   * Express {@code substring(col, start, length)} over a temporal column as arithmetic on its stored
   * long plus a display kind.
   *
   * <p>
   * {@code out3} receives {@code [divisor, modulus, display]} in the shape the group kernels'
   * existing {@code (v idiv D) mod M} transform takes ({@code 0} = the operation is absent). Only the
   * windows that fall on a field boundary of the canonical text are expressible: a leading window is
   * a truncation (an integer divide), and a two-digit field window is a truncation followed by a
   * modulus. Every other window — {@code substring(t,1,7)}, which asks for a calendar month and is
   * not a division of the epoch, or a window straddling a separator — returns {@code false}, and the
   * caller keeps whatever it did before.
   *
   * <p>
   * The arithmetic is only equal to the text operation while the value is NON-NEGATIVE: the kernels'
   * {@code idiv} truncates toward zero as XQuery does, and truncation and the text's truncation part
   * company below the epoch. Callers must gate on the column's zone minimum; see
   * {@link ProjectionIndexRegistry.Handle#temporalColumnNonNegative}.
   *
   * @param start1Based the {@code fn:substring} start position (1-based, as written in the query)
   * @return {@code false} when the window is not expressible as arithmetic
   */
  public static boolean substringDerivation(final byte kind, final int start1Based, final int length,
      final long[] out3) {
    if (out3 == null || out3.length < 3) {
      throw new IllegalArgumentException("out3 must hold at least three longs");
    }
    if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_DATE) {
      // The only date window that is a division of the epoch DAY is the whole value.
      if (start1Based == 1 && length == DATE_TEXT_LENGTH) {
        return derivation(out3, 0L, 0L, DISPLAY_ISO_DATE);
      }
      return false;
    }
    if (kind != ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP) {
      return false;
    }
    if (start1Based == 1) {
      return switch (length) {
        case TIMESTAMP_TEXT_LENGTH -> derivation(out3, 0L, 0L, DISPLAY_ISO_SECOND);
        case 16 -> derivation(out3, SECONDS_PER_MINUTE, 0L, DISPLAY_ISO_MINUTE);
        case 13 -> derivation(out3, SECONDS_PER_HOUR, 0L, DISPLAY_ISO_HOUR);
        case 10 -> derivation(out3, SECONDS_PER_DAY, 0L, DISPLAY_ISO_DATE);
        default -> false;
      };
    }
    if (length != 2) {
      return false;
    }
    // Two-digit field windows: hour at 12, minute at 15, second at 18 of dddd-dd-ddTdd:dd:dd.
    return switch (start1Based) {
      case 12 -> derivation(out3, SECONDS_PER_HOUR, 24L, DISPLAY_TWO_DIGIT);
      case 15 -> derivation(out3, SECONDS_PER_MINUTE, 60L, DISPLAY_TWO_DIGIT);
      case 18 -> derivation(out3, 1L, 60L, DISPLAY_TWO_DIGIT);
      default -> false;
    };
  }

  private static boolean derivation(final long[] out3, final long divisor, final long modulus, final byte display) {
    out3[0] = divisor;
    out3[1] = modulus;
    out3[2] = display;
    return true;
  }

  // ==== civil ⇆ days ===========================================================================

  /**
   * Days since 1970-01-01 for a proleptic-Gregorian civil date. Hinnant's {@code days_from_civil};
   * exact for every four-digit year and branch-free but for the two sign corrections.
   */
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

  /** Write {@code dddd-dd-dd} for a day count — Hinnant's {@code civil_from_days}, inlined. */
  private static void writeCivilDate(final long epochDays, final byte[] out, final int off) {
    final long z = epochDays + DAYS_TO_EPOCH_ERA_SHIFT;
    final long era = (z >= 0
        ? z
        : z - 146_096) / 146_097;
    final long dayOfEra = z - era * 146_097; // [0, 146096]
    final long yearOfEra = (dayOfEra - dayOfEra / 1460 + dayOfEra / 36_524 - dayOfEra / 146_096) / 365; // [0, 399]
    final long dayOfYear = dayOfEra - (365 * yearOfEra + yearOfEra / 4 - yearOfEra / 100);
    final long monthPrime = (5 * dayOfYear + 2) / 153; // [0, 11]
    final int day = (int) (dayOfYear - (153 * monthPrime + 2) / 5) + 1;
    final int month = (int) (monthPrime + (monthPrime < 10
        ? 3
        : -9));
    final long year = yearOfEra + era * 400 + (month <= 2
        ? 1
        : 0);
    if (year < 0 || year > 9999) {
      throw new IllegalArgumentException(
          "temporal value " + epochDays + " falls outside the four-digit year range: " + year);
    }
    final int y = (int) year;
    out[off] = (byte) ('0' + y / 1000);
    out[off + 1] = (byte) ('0' + y / 100 % 10);
    out[off + 2] = (byte) ('0' + y / 10 % 10);
    out[off + 3] = (byte) ('0' + y % 10);
    out[off + 4] = '-';
    writeTwoDigits(month, out, off + 5);
    out[off + 7] = '-';
    writeTwoDigits(day, out, off + 8);
  }

  private static void writeTwoDigits(final int value, final byte[] out, final int off) {
    out[off] = (byte) ('0' + value / 10);
    out[off + 1] = (byte) ('0' + value % 10);
  }

  /** Four ASCII digits as an int, or {@code -1} when any byte is not a digit. */
  private static int digits4(final byte[] utf8, final int off) {
    final int a = utf8[off] - '0';
    final int b = utf8[off + 1] - '0';
    final int c = utf8[off + 2] - '0';
    final int d = utf8[off + 3] - '0';
    if ((a | b | c | d) < 0 || a > 9 || b > 9 || c > 9 || d > 9) {
      return -1;
    }
    return a * 1000 + b * 100 + c * 10 + d;
  }

  /** Two ASCII digits as an int, or {@code -1} when either byte is not a digit. */
  private static int digits2(final byte[] utf8, final int off) {
    final int a = utf8[off] - '0';
    final int b = utf8[off + 1] - '0';
    if ((a | b) < 0 || a > 9 || b > 9) {
      return -1;
    }
    return a * 10 + b;
  }

  private static boolean validCivil(final int year, final int month, final int day) {
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
}
