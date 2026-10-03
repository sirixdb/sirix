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
   * @throws IllegalArgumentException if the duration is not an xs:dateTime timezone
   */
  private static ZoneOffset offsetOf(final DTD timezone) {
    if (timezone == null) {
      return ZoneOffset.UTC;
    }
    final int hours = timezone.getHours();
    final int minutes = timezone.getMinutes();
    if (timezone.getDays() != 0 || timezone.getMicros() != 0 || minutes < 0 || minutes > 59
        || hours > 14 || (hours == 14 && minutes != 0)) {
      throw new IllegalArgumentException("timezone is not an xs:dateTime offset: " + timezone);
    }
    return timezone.isNegative()
        ? ZoneOffset.ofHoursMinutes(-hours, -minutes)
        : ZoneOffset.ofHoursMinutes(hours, minutes);
  }
}
