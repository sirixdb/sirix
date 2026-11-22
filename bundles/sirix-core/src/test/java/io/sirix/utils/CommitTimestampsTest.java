package io.sirix.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Instant;
import java.time.format.DateTimeFormatter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The stored commit-timestamp unit of VER-201: epoch microseconds, truncated toward the past, with
 * an unrepresentable instant rejected rather than wrapped.
 */
final class CommitTimestampsTest {

  @ParameterizedTest
  @CsvSource({"2021-03-04T05:06:07Z,2021-03-04T05:06:07Z", "2021-03-04T05:06:07.123Z,2021-03-04T05:06:07.123Z",
      "2021-03-04T05:06:07.123456Z,2021-03-04T05:06:07.123456Z",
      "1969-12-31T23:59:59.999999Z,1969-12-31T23:59:59.999999Z"})
  void isoOutputPreservesStoredMicroseconds(final String input, final String expected) {
    final Instant supplied = Instant.parse(input);
    final Instant stored = CommitTimestamps.toInstant(CommitTimestamps.toEpochMicros(supplied));
    final String emitted = DateTimeFormatter.ISO_INSTANT.format(stored);
    assertEquals(expected, emitted);
    assertEquals(supplied, Instant.parse(emitted));
  }

  @Test
  void epochIsZeroAndRoundTrips() {
    assertEquals(0L, CommitTimestamps.toEpochMicros(Instant.EPOCH));
    assertEquals(Instant.EPOCH, CommitTimestamps.toInstant(0L));
  }

  @Test
  void conversionKeepsWholeMicrosecondsExactly() {
    final Instant instant = Instant.parse("2026-11-21T12:34:56.123456Z");
    assertEquals(1_795_264_496_123_456L, CommitTimestamps.toEpochMicros(instant));
    assertEquals(instant, CommitTimestamps.toInstant(CommitTimestamps.toEpochMicros(instant)));
  }

  @Test
  void subMicrosecondDigitsTruncateTowardThePast() {
    final Instant withNanos = Instant.parse("2026-11-21T12:34:56.123456789Z");
    final long micros = CommitTimestamps.toEpochMicros(withNanos);
    assertEquals(CommitTimestamps.toEpochMicros(Instant.parse("2026-11-21T12:34:56.123456Z")), micros);
    assertTrue(CommitTimestamps.toInstant(micros).isBefore(withNanos), "truncation must not move an instant forward");
    assertEquals(Instant.parse("2026-11-21T12:34:56.123456Z"), CommitTimestamps.toInstant(micros));
  }

  @Test
  void truncationOfPreEpochInstantsAlsoGoesTowardThePast() {
    // Instant.getNano() is a non-negative offset from a negative epoch second, so a naive
    // nanos/1000 on a signed nanosecond count would round the wrong way here.
    assertEquals(-1L, CommitTimestamps.toEpochMicros(Instant.parse("1969-12-31T23:59:59.999999999Z")));
    assertEquals(-1_500_000L, CommitTimestamps.toEpochMicros(Instant.ofEpochSecond(-2L, 500_000_000)));
    assertEquals(-2_000_000L, CommitTimestamps.toEpochMicros(Instant.ofEpochSecond(-2L)));
    final Instant beforeEpoch = Instant.parse("1952-03-04T05:06:07.000123456Z");
    final long micros = CommitTimestamps.toEpochMicros(beforeEpoch);
    assertTrue(micros < 0, "instant before the epoch must convert to a negative microsecond count");
    assertEquals(Instant.parse("1952-03-04T05:06:07.000123Z"), CommitTimestamps.toInstant(micros));
    assertTrue(CommitTimestamps.toInstant(micros).isBefore(beforeEpoch));
  }

  @Test
  void theWholeSignedRangeRoundTrips() {
    for (final long micros : new long[] {Long.MIN_VALUE, Long.MIN_VALUE + 1, -1_000_000_000_001L, -1L, 0L, 1L, 999_999L,
        1_000_000L, 1_795_264_496_123_456L, Long.MAX_VALUE}) {
      assertEquals(micros, CommitTimestamps.toEpochMicros(CommitTimestamps.toInstant(micros)),
          "round trip of " + micros);
    }
  }

  @Test
  void oneMicrosecondStepsAreRepresentable() {
    final long micros = CommitTimestamps.toEpochMicros(Instant.parse("2026-11-21T12:34:56.123456Z"));
    assertEquals(micros + 1, CommitTimestamps.toEpochMicros(Instant.parse("2026-11-21T12:34:56.123457Z")));
    assertEquals(micros + 1, CommitTimestamps.toEpochMicros(CommitTimestamps.toInstant(micros).plusNanos(1_000L)));
  }

  @Test
  void unrepresentableInstantsAreRejectedRatherThanWrapped() {
    final IllegalArgumentException max =
        assertThrows(IllegalArgumentException.class, () -> CommitTimestamps.toEpochMicros(Instant.MAX));
    assertTrue(max.getMessage().contains("epoch-microsecond range"), max.getMessage());
    assertThrows(IllegalArgumentException.class, () -> CommitTimestamps.toEpochMicros(Instant.MIN));
    // Just past the representable range on both ends.
    assertThrows(IllegalArgumentException.class,
        () -> CommitTimestamps.toEpochMicros(CommitTimestamps.toInstant(Long.MAX_VALUE).plusNanos(1_000L)));
    assertThrows(IllegalArgumentException.class,
        () -> CommitTimestamps.toEpochMicros(CommitTimestamps.toInstant(Long.MIN_VALUE).minusNanos(1_000L)));
  }

  @Test
  @SuppressWarnings("NullAway") // Deliberately pass null to verify the argument guard.
  void nullInstantIsRejected() {
    assertThrows(NullPointerException.class, () -> CommitTimestamps.toEpochMicros(null));
  }

  @Test
  void theClockIsReadInTheStoredUnit() {
    final long before = CommitTimestamps.toEpochMicros(Instant.now());
    final long now = CommitTimestamps.nowEpochMicros();
    final long after = CommitTimestamps.toEpochMicros(Instant.now());
    assertTrue(before <= now && now <= after, "clock reading must lie between two surrounding readings");
  }
}
