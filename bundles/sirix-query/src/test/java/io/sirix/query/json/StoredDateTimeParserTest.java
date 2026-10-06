package io.sirix.query.json;

import io.brackit.query.atomic.DateTime;
import io.sirix.query.function.DateTimeToInstant;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class StoredDateTimeParserTest {
  @Test
  void fixedLayoutAgreesWithIndependentCalendarAndGeneralParser() {
    final var conversion = new DateTimeToInstant();
    for (int year = 1; year <= 9999; year += 17) {
      for (int month = 1; month <= 12; month++) {
        final LocalDate day = LocalDate.of(year, month, 1).withDayOfMonth(LocalDate.of(year, month, 1).lengthOfMonth());
        final String text = day + "T23:59:59Z";
        final byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        final long expected = (day.toEpochDay() * 86400 + 86399) * 1000;
        assertEquals(expected, StoredDateTimeParser.epochMillis(bytes, 0, bytes.length), text);
        assertEquals(conversion.convert(new DateTime(text)).toEpochMilli(), expected, text);
      }
    }
  }

  @Test
  void onlyValidFixedLayoutUtcIsAccepted() {
    for (final String text : new String[] {"", "0000-01-01T00:00:00Z", "2023-02-29T00:00:00Z", "2024-01-00T00:00:00Z",
        "2024-00-01T00:00:00Z", "2024-01-01T24:00:00Z", "2024-01-01T00:60:00Z", "2024-01-01T00:00:60Z",
        "2024-01-01T00:00:00", "2024-01-01T00:00:00z", "2024-01-01T00:00:00+00:00", "2024-01-01T00:00:00.0Z",
        " 2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z ", "-0001-01-01T00:00:00Z"}) {
      final byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
      assertEquals(StoredDateTimeParser.NOT_FIXED_UTC, StoredDateTimeParser.epochMillis(bytes, 0, bytes.length), text);
    }
    final byte[] bytes = "__2024-02-29T03:04:05Z__".getBytes(StandardCharsets.UTF_8);
    assertEquals(1709175845000L, StoredDateTimeParser.epochMillis(bytes, 2, 20));
    assertEquals(StoredDateTimeParser.NOT_FIXED_UTC, StoredDateTimeParser.epochMillis(bytes, -1, 20));
    assertEquals(StoredDateTimeParser.NOT_FIXED_UTC, StoredDateTimeParser.epochMillis(bytes, 5, 20));
    assertEquals(StoredDateTimeParser.NOT_FIXED_UTC, StoredDateTimeParser.epochMillis(bytes, Integer.MAX_VALUE, 20));
    assertEquals(StoredDateTimeParser.NOT_FIXED_UTC, StoredDateTimeParser.epochMillis(null, 0, 20));
  }
}
