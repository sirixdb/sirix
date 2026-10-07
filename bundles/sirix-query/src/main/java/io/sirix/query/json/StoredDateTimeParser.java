package io.sirix.query.json;

import io.sirix.index.projection.ProjectionTemporalCodec;

/** Fixed-layout UTC timestamps, without decoding, substrings or calendar allocations. */
public final class StoredDateTimeParser {
  public static final long NOT_FIXED_UTC = Long.MIN_VALUE;

  private StoredDateTimeParser() {}

  /**
   * Returns epoch millis for {@code YYYY-MM-DDTHH:MM:SSZ}, or {@link #NOT_FIXED_UTC}. Every other
   * spelling, including invalid civil dates and year zero, belongs to the general xs:dateTime parser
   * so its values and errors remain authoritative.
   */
  public static long epochMillis(final byte[] utf8, final int offset, final int length) {
    if (utf8 == null || length != 20 || offset < 0 || offset > utf8.length - length || utf8[offset + 19] != 'Z'
        || (utf8[offset] == '0' && utf8[offset + 1] == '0' && utf8[offset + 2] == '0' && utf8[offset + 3] == '0')) {
      return NOT_FIXED_UTC;
    }
    final long seconds = ProjectionTemporalCodec.parseTimestampSeconds(utf8, offset, 19);
    return seconds == ProjectionTemporalCodec.NOT_CANONICAL
        ? NOT_FIXED_UTC
        : seconds * 1_000;
  }
}
