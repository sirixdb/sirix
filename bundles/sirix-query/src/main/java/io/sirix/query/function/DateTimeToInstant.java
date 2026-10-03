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
 * The timezone rules for timestamp inputs are documented in {@code README.md}, under Time-Travel
 * Queries.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public class DateTimeToInstant {

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
   * @param timezone the value's timezone as a day-time duration, or {@code null} when it carries none
   * @return the offset used by {@link #convert(DateTime)}
   * @throws IllegalArgumentException if the duration is not an xs:dateTime timezone
   */
  private static ZoneOffset offsetOf(final DTD timezone) {
    if (timezone == null) {
      return ZoneOffset.UTC;
    }
    final int hours = timezone.getHours();
    final int minutes = timezone.getMinutes();
    if (timezone.getDays() != 0 || timezone.getMicros() != 0 || minutes < 0 || minutes > 59 || hours > 14
        || (hours == 14 && minutes != 0)) {
      throw new IllegalArgumentException("timezone is not an xs:dateTime offset: " + timezone);
    }
    return timezone.isNegative()
        ? ZoneOffset.ofHoursMinutes(-hours, -minutes)
        : ZoneOffset.ofHoursMinutes(hours, minutes);
  }
}
