package io.sirix.utils;

import io.sirix.index.interval.IntervalDomain;
import io.sirix.index.interval.ValidTimeIntervalIndexWriter;
import io.sirix.utils.UtcTimestampParser.ParsedInstant;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.Random;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class UtcTimestampParserTest {
  @Test
  void canonicalAndOffsetInputsMatchInstantAcrossTheFourDigitYearRange() {
    final Random random = new Random(0x51_71_2026L);
    final ParsedInstant result = new ParsedInstant();
    for (int iteration = 0; iteration < 20_000; iteration++) {
      final int year = iteration < 10
          ? (iteration & 1) * 9999
          : random.nextInt(10_000);
      final LocalDate date = LocalDate.of(year, 1, 1).plusDays(random.nextInt(LocalDate.of(year, 1, 1).lengthOfYear()));
      final String base = String.format(Locale.ROOT, "%04d-%02d-%02dT%02d:%02d:%02d", year, date.getMonthValue(),
          date.getDayOfMonth(), random.nextInt(24), random.nextInt(60), random.nextInt(60));
      final int precision = iteration % 10;
      final String fraction = precision == 0
          ? ""
          : "." + String.format(Locale.ROOT, "%09d", random.nextInt(1_000_000_000)).substring(0, precision);
      final String canonical = base + fraction + "Z";
      assertParity(canonical, result);
      final ZoneOffset offset = ZoneOffset.ofTotalSeconds((random.nextInt(145) - 72) * 900);
      assertParity(base + fraction + offset.getId(), result);
      assertParity(canonical.toLowerCase(), result);
      final int position = random.nextInt(canonical.length());
      assertParity(canonical.substring(0, position) + "!" + canonical.substring(position + 1), result);
    }
  }

  @Test
  void boundariesLeapSecondsEndOfDayAndMalformedInputsRetainJdkSemantics() {
    final ParsedInstant result = new ParsedInstant();
    final String[] inputs =
        {"0000-02-29T00:00:00Z", "0001-01-01T00:00:00Z", "9999-12-31T23:59:59.999999999Z", "2000-02-29T12:34:56Z",
            "1900-02-29T00:00:00Z", "2024-02-30T00:00:00Z", "2016-12-31T23:59:60Z", "2016-12-31T23:59:60.123456789Z",
            "2016-12-31T22:59:60Z", "2024-12-31T24:00:00Z", "2024-12-31T24:00:00.000000001Z", "2024-01-01T00:00:00.Z",
            "2024-01-01T00:00:00.1234567890Z", "2024-01-01T00:00:00+01:02:03", "2024-01-01T00:00:00-18:00",
            "2024-01-01T00:00:00+18:01", "+10000-01-01T00:00:00Z", "-0001-12-31T23:59:59Z",
            "-1000000000-01-01T00:00:00Z", "+1000000000-12-31T23:59:59.999999999Z", "+1000000001-01-01T00:00:00Z", "",
            "Z", "2024-00-01T00:00:00Z", "2024-13-01T00:00:00Z", "2024-01-00T00:00:00Z", "2024-01-01T00:60:00Z",
            "2024-01-01T00:00:61Z", "2024-01-01T00:00:00Z ", "２０２４-01-01T00:00:00Z"};
    for (final String input : inputs) {
      assertParity(input, result);
    }
    assertThrows(NullPointerException.class, () -> UtcTimestampParser.parse(null, result));
    UtcTimestampParser.parseNullable(null, result);
    assertFalse(result.isPresent());
    UtcTimestampParser.parse("2024-01-01T00:00:00Z", result);
    UtcTimestampParser.parseNullable("broken", result);
    assertFalse(result.isPresent(), "a malformed bound must clear the previous record's value");
  }

  @Test
  void primitiveIntervalsMatchExistingInstantMappingIncludingOverflowAndDuplicates() {
    final ParsedInstant from = new ParsedInstant();
    final ParsedInstant to = new ParsedInstant();
    final String[] values = {null, "broken", Instant.MIN.toString(), "0000-01-01T00:00:00Z",
        "1969-12-31T23:59:59.999999999Z", "1970-01-01T00:00:00Z", "2024-02-29T00:00:00.123456789Z",
        "2024-02-29T00:00:00.123Z", "9999-12-31T23:59:59Z", Instant.MAX.toString()};
    for (final long origin : new long[] {0, -1, Long.MIN_VALUE, Long.MAX_VALUE}) {
      final IntervalDomain domain = new IntervalDomain(48, origin);
      final ValidTimeIntervalIndexWriter writer =
          new ValidTimeIntervalIndexWriter(null, null, null, null, domain, "vf", "vt");
      for (final String value : values) {
        final Instant instant = ValidTimeIntervalIndexWriter.parseInstant(value);
        if (instant == null) {
          continue;
        }
        long expectedPoint;
        boolean expectedExact;
        try {
          final long millis = instant.toEpochMilli();
          expectedPoint = domain.toDomain(millis);
          final long shifted = Math.subtractExact(millis, origin);
          expectedExact = instant.getNano() % 1_000_000 == 0 && shifted >= 0 && shifted < domain.maxValue();
        } catch (final ArithmeticException e) {
          expectedPoint = instant.isBefore(Instant.EPOCH)
              ? 1
              : domain.maxValue();
          // A subtraction overflow can occur after a successful conversion; retain the JDK point.
          try {
            expectedPoint = domain.toDomain(instant.toEpochMilli());
          } catch (final ArithmeticException overflow) {
            // The instant lies outside epoch-millisecond range.
          }
          expectedExact = false;
        }
        assertEquals(expectedPoint, domain.point(instant.getEpochSecond(), instant.getNano()));
        assertEquals(expectedExact, domain.isExact(instant.getEpochSecond(), instant.getNano()));
      }
      for (final String lo : values) {
        for (final String hi : values) {
          UtcTimestampParser.parseNullable(lo, from);
          UtcTimestampParser.parseNullable(hi, to);
          final Instant lower = ValidTimeIntervalIndexWriter.parseInstant(lo);
          final Instant upper = ValidTimeIntervalIndexWriter.parseInstant(hi);
          for (int count = 0; count < 3; count++) {
            assertEquals(writer.toInterval(lower, upper, count, count),
                writer.toPrimitiveInterval(from, to, count, count));
          }
        }
      }
    }
    for (final long millis : new long[] {Long.MIN_VALUE, Long.MIN_VALUE + 1, -1, 0, 1, Long.MAX_VALUE - 1,
        Long.MAX_VALUE}) {
      final Instant instant = Instant.ofEpochMilli(millis);
      final IntervalDomain domain = new IntervalDomain();
      assertEquals(domain.toDomain(millis), domain.point(instant.getEpochSecond(), instant.getNano()));
    }
  }

  private static void assertParity(final String text, final ParsedInstant result) {
    final Instant expected;
    try {
      expected = Instant.parse(text);
    } catch (final DateTimeParseException e) {
      assertThrows(e.getClass(), () -> UtcTimestampParser.parse(text, result), text);
      assertFalse(result.isPresent());
      return;
    }
    UtcTimestampParser.parse(text, result);
    assertTrue(result.isPresent(), text);
    assertEquals(expected.getEpochSecond(), result.epochSecond(), text);
    assertEquals(expected.getNano(), result.nano(), text);
  }
}
