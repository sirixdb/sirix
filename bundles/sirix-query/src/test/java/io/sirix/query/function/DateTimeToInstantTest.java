package io.sirix.query.function;

import io.brackit.query.atomic.DTD;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.jdm.Type;
import io.sirix.index.InstantKeyCodec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Regression tests for {@link DateTimeToInstant}, which used to convert every {@code xs:dateTime}
 * as if its local fields were already UTC. A value stated in {@code +02:00} then named an instant
 * two hours later than it denotes, so transaction- and valid-time probes landed on the wrong point
 * on the timeline.
 *
 * @author Johannes Lichtenberger
 */
final class DateTimeToInstantTest {

  private static final DateTimeToInstant CONVERTER = new DateTimeToInstant();

  @Test
  void honoursAPositiveOffset() {
    assertConverts("2020-06-15T14:00:00+02:00", "2020-06-15T12:00:00Z");
  }

  @Test
  void honoursANegativeOffset() {
    assertConverts("2020-06-15T07:00:00-05:00", "2020-06-15T12:00:00Z");
  }

  @Test
  void honoursASubHourOffset() {
    assertConverts("2020-06-15T17:30:00+05:30", "2020-06-15T12:00:00Z");
    assertConverts("2020-06-15T08:30:00-03:30", "2020-06-15T12:00:00Z");
    assertConverts("2020-06-15T11:30:00-00:30", "2020-06-15T12:00:00Z");
  }

  @Test
  void honoursTheXsdOffsetLimits() {
    assertConverts("2020-06-15T14:00:00+14:00", "2020-06-15T00:00:00Z");
    assertConverts("2020-06-15T00:00:00-14:00", "2020-06-15T14:00:00Z");
  }

  @Test
  void anOffsetMayRollTheDateOver() {
    assertConverts("2020-06-15T01:00:00+02:00", "2020-06-14T23:00:00Z");
    assertConverts("2020-06-15T23:00:00-02:00", "2020-06-16T01:00:00Z");
  }

  @Test
  void keepsUtcValuesUnchanged() {
    assertConverts("2020-06-15T12:00:00Z", "2020-06-15T12:00:00Z");
    assertConverts("2020-06-15T12:00:00+00:00", "2020-06-15T12:00:00Z");
  }

  @Test
  void readsAnAbsentTimezoneAsTheImplicitUtcOne() {
    assertConverts("2020-06-15T12:00:00", "2020-06-15T12:00:00Z");
  }

  @Test
  void preservesSubSecondPrecisionAcrossTheOffsetShift() {
    assertConverts("2020-06-15T14:00:00.123456+02:00", "2020-06-15T12:00:00.123456Z");
  }

  @Test
  void convertsValuesOnEitherSideOfTheEpoch() {
    assertConverts("1969-12-31T23:00:00-02:00", "1970-01-01T01:00:00Z");
    assertConverts("2038-01-19T04:14:07+01:00", "2038-01-19T03:14:07Z");
  }

  /**
   * Values denoting one instant in different offsets must converge, because the CAS index reduces
   * them to one key on write and {@code jn:valid-at} probes that index with the converted instant.
   */
  @Test
  void equalInstantsInDifferentOffsetsConverge() {
    assertEquals(CONVERTER.convert(new DateTime("2020-06-15T12:00:00Z")),
        CONVERTER.convert(new DateTime("2020-06-15T14:00:00+02:00")));
  }

  /**
   * The converted instant must re-encode to the very index key the stored value carries: that round
   * trip is what {@code ValidTimeIndexScan} performs when it turns the instant back into an
   * {@code xs:dateTime} to bound a range scan.
   */
  @Test
  void agreesWithTheInstantIndexKeyForTheSameValue() {
    assertSameIndexKey("2020-06-15T14:00:00+02:00");
    assertSameIndexKey("2020-06-15T07:00:00-05:00");
    assertSameIndexKey("2020-06-15T12:00:00Z");
    assertSameIndexKey("2020-06-15T12:00:00");
  }

  @ParameterizedTest
  @MethodSource("invalidTimezones")
  void rejectsDurationsThatAreNotXsdTimezones(final boolean negative, final int days, final int hours,
      final int minutes, final int micros) {
    final DateTime invalid = new DateTime((short) 2020, (byte) 6, (byte) 15, (byte) 12, (byte) 0, 0,
        new DTD(negative, days, (byte) hours, (byte) minutes, micros));
    assertThrows(IllegalArgumentException.class, () -> CONVERTER.convert(invalid));
  }

  private static Stream<Arguments> invalidTimezones() {
    return Stream.of(false, true)
                 .flatMap(negative -> Stream.of(Arguments.of(negative, 1, 0, 0, 0), Arguments.of(negative, 0, 15, 0, 0),
                     Arguments.of(negative, 0, 18, 0, 0), Arguments.of(negative, 0, 14, 1, 0),
                     Arguments.of(negative, 0, 0, 60, 0), Arguments.of(negative, 0, 0, -1, 0),
                     Arguments.of(negative, 0, 0, 0, 1), Arguments.of(negative, 0, 0, 0, 1_000_000)));
  }

  @Test
  @SuppressWarnings("NullAway") // Deliberately pass null to verify rejection.
  void rejectsAMissingValue() {
    assertThrows(NullPointerException.class, () -> CONVERTER.convert(null));
  }

  private static void assertConverts(final String lexical, final String expectedInstant) {
    assertEquals(Instant.parse(expectedInstant), CONVERTER.convert(new DateTime(lexical)), "converting " + lexical);
  }

  private static void assertSameIndexKey(final String lexical) {
    final DateTime value = new DateTime(lexical);
    final Instant converted = CONVERTER.convert(value);
    assertArrayEquals(InstantKeyCodec.toBytes(value, Type.DATI),
        InstantKeyCodec.toBytes(new DateTime(converted.toString()), Type.DATI), "index key for " + lexical);
  }
}
