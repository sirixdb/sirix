/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index;

import io.brackit.query.QueryException;
import io.brackit.query.atomic.AbstractTimeInstant;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.Date;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.Time;
import io.brackit.query.jdm.Type;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Storage helper for the XQuery instant family — {@code xs:dateTime}, {@code xs:date} and
 * {@code xs:time}: it reduces a value to the absolute instant it denotes, so every HOT index
 * consumer agrees on representation and a byte-ordered scan orders them chronologically.
 *
 * <h2>Encode the ABSOLUTE INSTANT, never the calendar components</h2>
 * <p>
 * The value is reduced to the point on the timeline it denotes and that is what gets written, so
 * unsigned byte order is chronological order and two spellings of the same instant produce the same
 * key (which is what {@code eq} means for these types). An earlier version of this class
 * canonicalized to UTC and then read the components back off the ORIGINAL type, which silently
 * discarded whatever the canonicalization had shifted:
 *
 * <ul>
 * <li><b>{@code xs:date}</b> — an offset shift lands in a time-of-day an {@code xs:date} cannot
 * carry ({@code Date.getHours()} is always {@code 0}), so it was dropped and
 * {@code 2020-01-01+02:00} produced BYTE-IDENTICAL keys to {@code 2019-12-31Z} — two DIFFERENT
 * instants sharing one index entry.</li>
 * <li><b>{@code xs:time}</b> — comparison attaches the reference date {@code 1972-12-31}, whose
 * ±1-day rollover an {@code xs:time} cannot hold ({@code Time.getDay()} is always {@code 0}), so
 * byte order came out INVERTED for any non-UTC offset.</li>
 * </ul>
 *
 * <p>
 * Both are fixed by canonicalizing through a {@link DateTime} — which has room for every component
 * — and encoding that. A collision is the fatal failure mode: a CAS key is the index entry's
 * identity, so two DISTINCT values sharing one key merge their posting lists and corrupt equality
 * lookups and deletes, not just ranges. Two spellings of the SAME instant sharing a key is the
 * opposite — it is the required behaviour, and it is why {@code "12:00:00Z"} and
 * {@code "14:00:00+02:00"} match one another.
 *
 *
 * <h2>Untimezoned values</h2>
 * <p>
 * A value without a timezone is interpreted in the implicit one (UTC here), so it encodes to the
 * same instant as its {@code Z} spelling and the two share a key. That matches {@code eq} under a
 * UTC implicit timezone. Note brackit's own {@code compareTo} instead applies a has-timezone
 * tiebreak and answers "less" in BOTH directions for such a pair, i.e. it is not a total order
 * there; no encoding can reproduce that, and reproducing it is not desirable.
 *
 * @author Johannes Lichtenberger
 */
public final class InstantKeyCodec {

  private InstantKeyCodec() {
    throw new AssertionError("no instances");
  }

  /**
   * Whether {@code type} is one of the instant types this helper handles.
   *
   * <p>
   * Deliberately NOT including {@code xs:duration} and its subtypes: durations are only partially
   * ordered (a month is not a fixed number of days), so they have no place in an ordered index key.
   *
   * @param type the type to test, may be {@code null}
   * @return {@code true} iff {@code type} is {@code xs:dateTime}, {@code xs:date} or {@code xs:time}
   */
  public static boolean isInstantType(final Type type) {
    return type != null && (type.instanceOf(Type.DATI) || type.instanceOf(Type.DATE) || type.instanceOf(Type.TIME));
  }

  /** Bytes an encoded instant occupies: year(2) month(1) day(1) hours(1) minutes(1) micros(4). */
  public static final int BYTES = 10;

  /**
   * Reference date XQuery uses to compare bare {@code xs:time} values (§10.4 op:time-less-than).
   * Encoding relative to it is what preserves the ±1-day carry a timezone conversion can produce.
   */
  private static final short TIME_REFERENCE_YEAR = 1972;
  private static final byte TIME_REFERENCE_MONTH = 12;
  private static final byte TIME_REFERENCE_DAY = 31;

  /**
   * Encode {@code value} into {@code dest} at {@code offset}, as the absolute instant it denotes.
   *
   * @param value the value; a raw {@code Str} carrying the lexical form is accepted and coerced,
   *        because that is what the index builders hand over
   * @param type the declared content type, used to coerce {@code value} when it is untyped
   * @param dest destination buffer, must hold {@link #BYTES} from {@code offset}
   * @param offset where to start writing
   * @return {@link #BYTES}
   */
  public static int encode(final Atomic value, final Type type, final byte[] dest, final int offset) {
    requireNonNull(value, "value");
    requireNonNull(dest, "dest");
    Objects.checkFromIndexSize(offset, BYTES, dest.length);

    final DateTime utc = toUtcInstant(asTimeInstant(value, type));
    final int signFlippedYear = (utc.getYear() ^ 0x8000) & 0xFFFF;
    int pos = offset;
    dest[pos++] = (byte) (signFlippedYear >>> 8);
    dest[pos++] = (byte) signFlippedYear;
    dest[pos++] = utc.getMonth();
    dest[pos++] = utc.getDay();
    dest[pos++] = utc.getHours();
    dest[pos++] = utc.getMinutes();
    final int micros = utc.getMicros();
    dest[pos++] = (byte) (micros >>> 24);
    dest[pos++] = (byte) (micros >>> 16);
    dest[pos++] = (byte) (micros >>> 8);
    dest[pos] = (byte) micros;
    return BYTES;
  }

  /** Encode into a right-sized array, for callers that hand back a {@code byte[]}. */
  public static byte[] toBytes(final Atomic value, final Type type) {
    final byte[] out = new byte[BYTES];
    encode(value, type, out, 0);
    return out;
  }

  /**
   * Reduce any instant to the UTC point on the timeline it denotes, WITHOUT routing through the
   * original type — that is the step whose omission dropped the offset shift for {@code xs:date} (no
   * time-of-day) and {@code xs:time} (no date). A {@link DateTime} has room for every component, so
   * nothing is lost, and an absent timezone means the implicit one (UTC).
   */
  private static DateTime toUtcInstant(final AbstractTimeInstant instant) {
    final DateTime asDateTime;
    if (instant instanceof DateTime dateTime) {
      asDateTime = dateTime;
    } else if (instant instanceof Date) {
      // An xs:date denotes midnight on that day in its own timezone.
      asDateTime = new DateTime(instant.getYear(), instant.getMonth(), instant.getDay(), (byte) 0, (byte) 0, 0,
          instant.getTimezone());
    } else {
      // An xs:time is compared against the reference date, which is what carries the ±1-day
      // rollover a timezone conversion can produce.
      asDateTime = new DateTime(TIME_REFERENCE_YEAR, TIME_REFERENCE_MONTH, TIME_REFERENCE_DAY, instant.getHours(),
          instant.getMinutes(), instant.getMicros(), instant.getTimezone());
    }
    if (asDateTime.getTimezone() == null) {
      // Already in the implicit timezone; canonicalize() would have nothing to normalize away.
      return asDateTime;
    }
    return (DateTime) asDateTime.canonicalize();
  }

  /**
   * Recover the typed instant from stored lexical bytes, so comparisons are chronological.
   *
   * @param bytes buffer holding the UTF-8 lexical form
   * @param offset offset of the lexical form
   * @param length its length in bytes
   * @param type the declared content type, which selects the returned atomic's class
   * @return the decoded value
   */
  public static Atomic decode(final byte[] bytes, final int offset, final int length, final Type type) {
    requireNonNull(bytes, "bytes");
    if (length < BYTES) {
      throw new IllegalArgumentException("instant key holds " + length + " bytes, expected " + BYTES);
    }
    Objects.checkFromIndexSize(offset, BYTES, bytes.length);

    final short year = (short) ((((bytes[offset] & 0xFF) << 8) | (bytes[offset + 1] & 0xFF)) ^ 0x8000);
    final DateTime dateTime = new DateTime(year, bytes[offset + 2], bytes[offset + 3], bytes[offset + 4],
        bytes[offset + 5], ((bytes[offset + 6] & 0xFF) << 24) | ((bytes[offset + 7] & 0xFF) << 16)
            | ((bytes[offset + 8] & 0xFF) << 8) | (bytes[offset + 9] & 0xFF),
        AbstractTimeInstant.UTC_TIMEZONE);
    if (type != null && type.instanceOf(Type.DATE)) {
      return new Date(dateTime);
    }
    if (type != null && type.instanceOf(Type.TIME)) {
      return new Time(dateTime);
    }
    return dateTime;
  }

  /**
   * Recover the typed instant from its lexical form.
   *
   * @param lexical the lexical form
   * @param type the declared content type, which selects the returned atomic's class
   * @return the typed value
   */
  public static Atomic fromLexical(final String lexical, final Type type) {
    requireNonNull(lexical, "lexical");
    try {
      if (type != null && type.instanceOf(Type.DATE)) {
        return new Date(lexical);
      }
      if (type != null && type.instanceOf(Type.TIME)) {
        return new Time(lexical);
      }
      return new DateTime(lexical);
    } catch (final QueryException e) {
      throw new IllegalArgumentException("value '" + lexical + "' is not a valid " + type, e);
    }
  }

  /**
   * Coerce to a typed instant. The CAS index builder and the change listener both hand a raw
   * {@link io.brackit.query.atomic.Str} carrying the document's lexical form while the declared type
   * says {@code xs:dateTime}, so the typed object has to be reconstructed rather than assumed.
   */
  private static AbstractTimeInstant asTimeInstant(final Atomic value, final Type type) {
    if (value instanceof AbstractTimeInstant instant) {
      return instant;
    }
    return (AbstractTimeInstant) fromLexical(value.stringValue(), type);
  }
}
