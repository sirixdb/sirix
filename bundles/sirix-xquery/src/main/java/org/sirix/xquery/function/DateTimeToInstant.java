package org.sirix.xquery.function;

import org.brackit.xquery.atomic.DateTime;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public class DateTimeToInstant {
  public DateTimeToInstant() {
  }

  public Instant convert(DateTime dateTime) {
    final int seconds = dateTime.getMicros() / 1000000;
    final int nanos = (dateTime.getMicros() % 1000000) * 1000;

    final OffsetDateTime dt = OffsetDateTime.of(dateTime.getYear(), dateTime.getMonth(), dateTime.getDay(),
        dateTime.getHours(), dateTime.getMinutes(), seconds, nanos, ZoneOffset.UTC);

    return dt.toInstant();
  }
}