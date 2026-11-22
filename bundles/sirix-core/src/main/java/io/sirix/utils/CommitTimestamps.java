package io.sirix.utils;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

/**
 * Conversions for the stored commit timestamp unit of VER-201: a signed count of microseconds since
 * the epoch, held in a 64-bit field.
 *
 * <p>
 * Conversion from an {@link Instant} truncates toward the past to a whole microsecond and rejects,
 * rather than wraps, an instant whose microsecond value does not fit a signed 64-bit field. The
 * representable range spans roughly &plusmn;292,000 years around the epoch, so only instants far
 * outside any revision timeline are rejected.
 *
 * <p>
 * Numeric conversions are allocation-free except {@link #toInstant(long)}, which necessarily
 * materializes the {@link Instant} it returns. Text output uses a shared immutable formatter.
 * Commit and open-by-time paths use the primitive {@code long} form.
 */
public final class CommitTimestamps {

  /** Microseconds per second. */
  public static final long MICROS_PER_SECOND = 1_000_000L;

  /** Nanoseconds per microsecond. */
  private static final long NANOS_PER_MICRO = 1_000L;

  private static final DateTimeFormatter ISO_FORMATTER = new DateTimeFormatterBuilder().appendInstant(6).toFormatter();

  private CommitTimestamps() {
    throw new AssertionError("May never be instantiated.");
  }

  /**
   * Convert an instant to the stored unit, truncating toward the past.
   *
   * @param instant the instant to convert, never {@code null}
   * @return epoch microseconds of {@code instant}, truncated toward the past
   * @throws NullPointerException if {@code instant} is {@code null}
   * @throws IllegalArgumentException if the microsecond value does not fit a signed 64-bit field
   */
  public static long toEpochMicros(final Instant instant) {
    // Hot path: commit and every open-by-time probe. Primitive arithmetic only, no allocation.
    // Instant.getNano() is always in [0, 999999999] relative to a possibly negative epoch second,
    // so the integer division truncates toward the past for negative instants as well.
    final long seconds = instant.getEpochSecond();
    final long subSecondMicros = instant.getNano() / NANOS_PER_MICRO;
    try {
      if (seconds < 0 && subSecondMicros > 0) {
        // Scaling the most negative representable second would overflow even though the complete
        // value fits, so scale the following second and subtract, as Instant.toEpochMilli does.
        return Math.addExact(Math.multiplyExact(seconds + 1, MICROS_PER_SECOND), subSecondMicros - MICROS_PER_SECOND);
      }
      return Math.addExact(Math.multiplyExact(seconds, MICROS_PER_SECOND), subSecondMicros);
    } catch (final ArithmeticException e) {
      throw new IllegalArgumentException(
          "Commit timestamp " + instant + " is outside the representable epoch-microsecond range.", e);
    }
  }

  /**
   * Convert a stored epoch-microsecond value back to an instant. The result carries the exact stored
   * microsecond and no finer digits.
   *
   * @param epochMicros epoch microseconds
   * @return the instant denoted by {@code epochMicros}
   */
  public static Instant toInstant(final long epochMicros) {
    return Instant.ofEpochSecond(Math.floorDiv(epochMicros, MICROS_PER_SECOND),
        Math.floorMod(epochMicros, MICROS_PER_SECOND) * NANOS_PER_MICRO);
  }

  /**
   * Format a commit timestamp as UTC ISO text with exactly six fractional digits (VER-201).
   *
   * @param instant the commit instant, never {@code null}
   * @return ISO timestamp text, preserving trailing fractional zeros
   * @throws NullPointerException if {@code instant} is {@code null}
   */
  public static String format(final Instant instant) {
    return ISO_FORMATTER.format(instant);
  }

  /**
   * Read the wall clock in the stored unit, truncated toward the past.
   *
   * @return the current epoch microseconds
   */
  public static long nowEpochMicros() {
    return toEpochMicros(Instant.now());
  }
}
