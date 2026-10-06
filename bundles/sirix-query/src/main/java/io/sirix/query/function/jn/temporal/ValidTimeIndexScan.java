package io.sirix.query.function.jn.temporal;

import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.Object;
import io.sirix.query.function.DateTimeToInstant;
import io.sirix.query.json.AtomicStrJsonDBItem;
import io.sirix.query.json.StoredDateTimeParser;
import org.jspecify.annotations.Nullable;

import java.time.Instant;

public final class ValidTimeIndexScan {

  private static final DateTimeToInstant DATE_TIME_TO_INSTANT = new DateTimeToInstant();

  private ValidTimeIndexScan() {}

  /**
   * The exact instant predicate used by linear scans and demand-time interval-index verification:
   * reads the configured valid-time fields off {@code obj} and tests
   * {@code validFrom <= validTime <= validTo}, where an <em>absent</em> (or unparseable) bound is
   * treated as unbounded on that side — a missing {@code validTo} is "valid from {@code validFrom}
   * onward", a missing {@code validFrom} is "valid up to {@code validTo}". A record with neither
   * bound carries no interval and never matches.
   *
   * <p>
   * Data-shape problems (absent fields, non-string values, unparseable dates) are handled explicitly
   * above; anything else — notably I/O failures while reading the fields — propagates. A previous
   * catch-all here mapped such failures to {@code false}, silently dropping records from query
   * results (e.g. under file-descriptor exhaustion every record read failed and {@code jn:valid-at}
   * returned an empty sequence instead of an error).
   * </p>
   */
  static boolean isValidAtTime(final Object obj, final Instant validTime, final String validFromField,
      final String validToField) {
    return isValidAtTime(obj, validTime, validFromField, validToField, false, false);
  }

  static boolean isValidAtTime(final Object obj, final Instant validTime, final String validFromField,
      final String validToField, final boolean strictStart, final boolean strictEnd) {
    final Sequence validFromSeq = obj.get(new QNm(validFromField));
    final Sequence validToSeq = obj.get(new QNm(validToField));

    // Open-ended intervals: a null (absent/unparseable) bound is unbounded on that side. This mirrors
    // the interval index exactly — its writer maps a null bound to the domain min/max and registers
    // the record (ValidTimeIntervalIndexWriter.toInterval) — so all paths still return the same set.
    final int fromComparison = compareBound(validTime, validFromSeq);
    final int toComparison = compareBound(validTime, validToSeq);
    if (fromComparison == Integer.MIN_VALUE && toComparison == Integer.MIN_VALUE) {
      return false;
    }
    if (fromComparison != Integer.MIN_VALUE && (fromComparison < 0 || (strictStart && fromComparison == 0))) {
      return false;
    }
    if (toComparison != Integer.MIN_VALUE && (toComparison > 0 || (strictEnd && toComparison == 0))) {
      return false;
    }
    return true;
  }

  private static int compareBound(final Instant validTime, final Sequence value) {
    if (value instanceof AtomicStrJsonDBItem stored) {
      final long millis = stored.epochMillis();
      if (millis != StoredDateTimeParser.NOT_FIXED_UTC) {
        final int seconds = Long.compare(validTime.getEpochSecond(), millis / 1_000);
        return seconds != 0
            ? seconds
            : Integer.compare(validTime.getNano(), 0);
      }
    }
    final Instant bound = value == null
        ? null
        : parseInstant(value);
    return bound == null
        ? Integer.MIN_VALUE
        : validTime.compareTo(bound);
  }

  private static @Nullable Instant parseInstant(final Sequence seq) {
    if (seq instanceof DateTime dt) {
      return DATE_TIME_TO_INSTANT.convert(dt);
    }
    if (seq instanceof Str str) {
      try {
        return Instant.parse(str.stringValue());
      } catch (Exception e) {
        return null;
      }
    }
    return null;
  }
}
