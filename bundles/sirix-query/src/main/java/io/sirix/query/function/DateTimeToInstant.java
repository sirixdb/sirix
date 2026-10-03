package io.sirix.query.function;

import io.brackit.query.atomic.DTD;
import io.brackit.query.atomic.DateTime;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static java.util.Objects.requireNonNull;

/**
 * Reduces an {@code xs:dateTime} to the instant on the timeline it denotes.
 *
 * <p>
 * The value's own timezone decides that instant: {@code 2020-06-15T14:00:00+02:00} and
 * {@code 2020-06-15T12:00:00Z} name the same point. An absent timezone means the implicit one,
 * which Sirix takes to be UTC — the same rule {@code io.sirix.index.InstantKeyCodec} applies when
 * it reduces an instant to an index key, so a converted probe and a stored key agree.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public class DateTimeToInstant {

  /** Widest offset a {@link ZoneOffset} represents; XSD itself allows only ±14:00. */
  private static final long MAX_OFFSET_SECONDS = 18L * 3600L;

  public DateTimeToInstant() {}

  public Instant convert(final DateTime dateTime) {
    requireNonNull(dateTime, "dateTime");

    final int micros = dateTime.getMicros();
    final int seconds = micros / 1_000_000;
    final int nanos = (micros % 1_000_000) * 1_000;

    final OffsetDateTime dt = OffsetDateTime.of(dateTime.getYear(), dateTime.getMonth(), dateTime.getDay(),
        dateTime.getHours(), dateTime.getMinutes(), seconds, nanos, offsetOf(dateTime.getTimezone()));

    return dt.toInstant();
  }

  /**
   * The offset the value is stated in.
   *
   * @param timezone the value's timezone as a day-time duration, or {@code null} when it carries
   *        none
   * @return the matching offset, or {@link ZoneOffset#UTC} for an absent timezone
   * @throws IllegalArgumentException if the duration is too wide to be an offset
   */
  private static ZoneOffset offsetOf(final DTD timezone) {
    if (timezone == null) {
      return ZoneOffset.UTC;
    }
    // Every component counts, so no part of a hand-built duration is dropped silently; a parsed
    // timezone only ever sets hours and minutes.
    final long magnitude = (timezone.getDays() * 24L + timezone.getHours()) * 3600L + timezone.getMinutes() * 60L
        + timezone.getMicros() / 1_000_000L;
    final long totalSeconds = timezone.isNegative() ? -magnitude : magnitude;
    if (totalSeconds < -MAX_OFFSET_SECONDS || totalSeconds > MAX_OFFSET_SECONDS) {
      throw new IllegalArgumentException("timezone is not a representable offset: " + timezone);
    }
    // ofTotalSeconds interns the common offsets, so the hot path allocates nothing here.
    return ZoneOffset.ofTotalSeconds((int) totalSeconds);
  }
}
