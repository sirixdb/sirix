/*
 * Copyright (c) 2024, SirixDB
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package io.sirix.index.hot;

import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.Bool;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.jdm.Type;
import io.sirix.index.InstantKeyCodec;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import org.jspecify.annotations.Nullable;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Order-preserving serializer for CAS (Content-and-Structure) index keys.
 *
 * <p>
 * Serializes {@link CASValue} to bytes such that the byte order matches the natural comparison
 * order defined by {@link CASValue#compareTo(CASValue)}:
 * </p>
 * <ol>
 * <li>pathNodeKey (8 bytes, sign-flipped for order preservation)</li>
 * <li>type ID (2 bytes)</li>
 * <li>value (N bytes, order-preserving encoding)</li>
 * </ol>
 *
 * <h2>Order Preservation</h2>
 * <ul>
 * <li><b>pathNodeKey:</b> XOR with sign bit for unsigned byte comparison</li>
 * <li><b>Integer values:</b> sign-flipped two's-complement (lossless for the full 64-bit
 * range)</li>
 * <li><b>Floating-point values:</b> IEEE 754 order-preserving encoding</li>
 * <li><b>String values:</b> UTF-8 (already lexicographically ordered)</li>
 * </ul>
 *
 * <h2>Zero Allocation</h2>
 * <p>
 * All methods write to caller-provided buffers. No ByteBuffer allocation.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class CASKeySerializer implements HOTKeySerializer<CASValue> {

  // The xs:boolean, xs:float and out-of-range xs:integer encodings all changed here, so a HOT CAS
  // index written by an older build holds keys this one will not seek to. Nothing to migrate: all
  // three were returning wrong answers, so such an index holds no correct data to preserve. Delete
  // and rebuild any local scratch resource that predates this.

  /**
   * Sign-flip constant for order-preserving encoding of signed longs.
   */
  private static final long SIGN_FLIP = 0x8000_0000_0000_0000L;

  /**
   * Type IDs for stable serialization (independent of Type.ordinal()).
   */
  private static final short TYPE_STRING = 1;
  private static final short TYPE_BOOLEAN = 2;
  private static final short TYPE_DOUBLE = 3;
  private static final short TYPE_FLOAT = 4;
  private static final short TYPE_INTEGER = 5;
  private static final short TYPE_DECIMAL = 7;

  /**
   * The instant family. These carry real ids so the content type ROUND-TRIPS — {@code CASValue} then
   * reports {@code xs:dateTime} rather than {@code xs:string} on the way back out, which is what lets
   * the typed {@code CASFilterRange#inRange} comparison order them chronologically.
   *
   * <p>
   * The stored VALUE is {@link InstantKeyCodec}'s fixed-width binary instant, whose byte order IS
   * chronological order, so {@link #isByteOrderPreserving} reports {@code true} for them and range
   * queries take the bounded cursor. That was not always so — see the history block below, which
   * records what an earlier lexical encoding got wrong and what any replacement must preserve. Note
   * that an id is still emphatically NOT a claim of byte-orderability: the two questions are
   * separate, and {@code xs:duration} carries no id and is not byte-ordered.
   * </p>
   */
  private static final short TYPE_DATETIME = 8;
  private static final short TYPE_DATE = 9;
  private static final short TYPE_TIME = 10;

  // HISTORY, kept because the failure modes below are easy to reintroduce. An EARLIER binary instant
  // codec on this branch had all of the defects listed here; the current InstantKeyCodec does not,
  // and CASKeySerializerEdgeCaseTest.Instants asserts the two properties that matter (distinct values
  // keep distinct keys, and byte order agrees with brackit's compareTo) rather than trusting this
  // note. Do NOT read the list below as a description of the shipped encoder — it is a description of
  // what an instant encoding must avoid. Measured against brackit's own comparison semantics, the
  // earlier attempt failed as follows:
  // * xs:date — canonicalizing to UTC moves the offset into a time-of-day that xs:date cannot
  // hold (brackit's Date.getHours() is always 0), so the residue is dropped and
  // xs:date("2020-01-01+02:00") encoded to the SAME 10 bytes as xs:date("2019-12-31Z"),
  // which compareTo reports as different values.
  // * xs:time — the reference-date comparison (1972-12-31) carries a ±1-day rollover that
  // xs:time cannot hold (Time.getDay() is always 0), so byte order came out INVERTED against
  // compareTo for any non-UTC offset.
  // * mixed timezoned/untimezoned values collided, and brackit orders that pair inconsistently
  // (it reports "less" in BOTH directions), so no byte encoding can agree with it.
  // Colliding keys are the fatal part: two distinct values sharing one CAS key merge their posting
  // lists, which corrupts equality lookups and deletes, not just ranges — strictly worse than a
  // mis-ordered range, which costs only the range. Any future change to InstantKeyCodec must keep
  // the codec INJECTIVE first and order-preserving second, in that priority.

  private static final short TYPE_OTHER = 0;



  /**
   * Singleton instance (stateless, thread-safe).
   */
  public static final CASKeySerializer INSTANCE = new CASKeySerializer();

  private CASKeySerializer() {
    // Singleton
  }

  @Override
  public int serialize(CASValue key, byte[] dest, int offset) {
    requireNonNull(key, "Key cannot be null");
    int start = offset;

    // 1. Path node key (8 bytes, sign-flipped)
    long signFlipped = key.getPathNodeKey() ^ SIGN_FLIP;
    dest[offset++] = (byte) (signFlipped >>> 56);
    dest[offset++] = (byte) (signFlipped >>> 48);
    dest[offset++] = (byte) (signFlipped >>> 40);
    dest[offset++] = (byte) (signFlipped >>> 32);
    dest[offset++] = (byte) (signFlipped >>> 24);
    dest[offset++] = (byte) (signFlipped >>> 16);
    dest[offset++] = (byte) (signFlipped >>> 8);
    dest[offset++] = (byte) signFlipped;

    // 2. Type ID (2 bytes)
    Type type = key.getType();
    short typeId = getTypeId(type);
    dest[offset++] = (byte) (typeId >>> 8);
    dest[offset++] = (byte) typeId;

    // 3. Value (order-preserving encoding)
    Atomic atomicValue = key.getAtomicValue();
    if (atomicValue != null) {
      offset += encodeAtomicOrderPreserving(atomicValue, type, typeId, dest, offset);
    }

    if (atomicValue == null) {
      throw new IllegalArgumentException("CASValue has no atomic value");
    }

    // NOT an error when the value region is empty: the empty string encodes to zero bytes, and a
    // bare 10-byte header is its correct key — it sorts below every non-empty value of the same
    // type, which is exactly right. Rejecting a zero-length region instead made `$x >= ""` throw
    // out of the reader and would have made indexing a `""` value throw out of the writer.
    return offset - start;
  }

  /**
   * A CAS key is a 10-byte header plus a value region the encoders bound themselves: 8 bytes for
   * every numeric family, 1 for a boolean, and at most {@link #MAX_STRING_VALUE_BYTES} for a string,
   * which {@link #encodeAtomicOrderPreserving} truncates to. So the bound is a constant and no key of
   * any type can exceed it.
   */
  @Override
  public int maxSerializedLength(final CASValue key) {
    return HEADER_BYTES + MAX_STRING_VALUE_BYTES;
  }

  /**
   * The path class record of a serialized CAS key, read in place. The pathNodeKey is the first eight
   * bytes, big-endian and sign-flipped, so this is one unaligned-style read plus an XOR — no
   * {@link CASValue}, no atomic, no UTF-8 decode. Exists because the per-entry PCR filter used to go
   * through {@link #deserialize}, which materializes the whole key just to look at its first eight
   * bytes.
   *
   * @param bytes buffer holding a serialized CAS key
   * @param offset offset of the key within {@code bytes}
   * @return the key's pathNodeKey
   */
  public static long pathNodeKeyAt(final byte[] bytes, final int offset) {
    requireNonNull(bytes, "bytes");
    Objects.checkFromIndexSize(offset, Long.BYTES, bytes.length);
    final long signFlipped = ((long) (bytes[offset] & 0xFF) << 56) | ((long) (bytes[offset + 1] & 0xFF) << 48)
        | ((long) (bytes[offset + 2] & 0xFF) << 40) | ((long) (bytes[offset + 3] & 0xFF) << 32)
        | ((long) (bytes[offset + 4] & 0xFF) << 24) | ((long) (bytes[offset + 5] & 0xFF) << 16)
        | ((long) (bytes[offset + 6] & 0xFF) << 8) | (bytes[offset + 7] & 0xFF);
    return signFlipped ^ SIGN_FLIP;
  }

  @Override
  public CASValue deserialize(byte[] bytes, int offset, int length) {
    // Read path node key (8 bytes)
    long signFlipped = ((long) (bytes[offset] & 0xFF) << 56) | ((long) (bytes[offset + 1] & 0xFF) << 48)
        | ((long) (bytes[offset + 2] & 0xFF) << 40) | ((long) (bytes[offset + 3] & 0xFF) << 32)
        | ((long) (bytes[offset + 4] & 0xFF) << 24) | ((long) (bytes[offset + 5] & 0xFF) << 16)
        | ((long) (bytes[offset + 6] & 0xFF) << 8) | ((long) (bytes[offset + 7] & 0xFF));
    long pathNodeKey = signFlipped ^ SIGN_FLIP;

    // Read type ID (2 bytes)
    short typeId = (short) (((bytes[offset + 8] & 0xFF) << 8) | (bytes[offset + 9] & 0xFF));
    Type type = getTypeFromId(typeId);

    // Read value
    int valueOffset = offset + 10;
    int valueLength = length - 10;
    Atomic atomicValue = decodeAtomic(bytes, valueOffset, valueLength, type);

    return new CASValue(atomicValue, type, pathNodeKey);
  }

  /** Bytes a key spends before its value: 8 for the pathNodeKey plus 2 for the type id. */
  static final int HEADER_BYTES = 10;

  /**
   * Maximum bytes available for string value encoding. Header is 10 bytes (8 for pathNodeKey + 2 for
   * typeId), buffer is 256 bytes.
   */
  static final int MAX_STRING_VALUE_BYTES = 246;

  /** {@code Long.MIN_VALUE} as a decimal, for the saturating integer parse. */
  private static final BigDecimal LONG_MIN = BigDecimal.valueOf(Long.MIN_VALUE);

  /** {@code Long.MAX_VALUE} as a decimal, for the saturating integer parse. */
  private static final BigDecimal LONG_MAX = BigDecimal.valueOf(Long.MAX_VALUE);

  /**
   * Encodes an atomic value in order-preserving format.
   *
   * @param value the atomic value
   * @param type the type
   * @param dest destination buffer
   * @param offset offset to write at
   * @return number of bytes written
   */
  private int encodeAtomicOrderPreserving(Atomic value, Type type, short typeId, byte[] dest, int offset) {
    // Dispatch on the id the caller already computed, NOT by re-walking the type hierarchy.
    // Type.instanceOf is a parent-pointer chase, and this method used to redo the whole ladder that
    // getTypeId had just walked -- so every key paid the dispatch twice. A switch over the id is a
    // tableswitch: no chain walks at all. Measured on the string CAS path, where serializing a key
    // cost MORE than the entire PEXT descent it feeds (134 ns against 79 ns).
    switch (typeId) {
      case TYPE_INTEGER:
        // Integer family (xs:integer and subtypes): lossless 64-bit encoding, not double.
        return encodeIntegerOrderPreserving(value, dest, offset);
      case TYPE_DOUBLE:
      case TYPE_DECIMAL:
        return encodeNumericOrderPreserving(value, dest, offset, false);
      case TYPE_FLOAT:
        // Rounded to float precision FIRST, because the two sides of an xs:float index reach this
        // method in different shapes and must land on one key. CASIndexBuilder stores every value as
        // a Str, so the stored side takes the parse branch below and yields Double.parseDouble("1.1")
        // = 1.1d; ScanCASIndex cast the probe to the content type, so the probe side arrives as a Flt
        // and yields (double) 1.1f = 1.100000023841858. Those are different keys, so `eq 1.1` on an
        // xs:float index found nothing at all. Narrowing both through float makes them agree, and it
        // is also the precision the index claims to hold — decodeAtomic already hands back Flt.
        return encodeNumericOrderPreserving(value, dest, offset, true);
      case TYPE_BOOLEAN:
        // Boolean: 0 for false, 1 for true (already ordered)
        dest[offset] = booleanValueOf(value)
            ? (byte) 1
            : (byte) 0;
        return 1;
      case TYPE_DATETIME:
      case TYPE_DATE:
      case TYPE_TIME:
        // The absolute instant, so byte order is chronological order — see InstantKeyCodec.
        return InstantKeyCodec.encode(value, type, dest, offset);
      default: {
        // String: UTF-8 is already lexicographically ordered
        final String str = value.stringValue();
        // Truncate to fit buffer (preserves lexicographic ordering for prefixes)
        final int cap = Math.min(dest.length - offset, MAX_STRING_VALUE_BYTES);
        // A value is serialized once per indexed node, so the ASCII case — which is very nearly all
        // of them — writes straight into dest instead of through a throwaway byte[]. One ASCII char
        // is one UTF-8 byte, so the bytes and the truncation point are identical either way; only
        // the leading `cap` chars have to be ASCII, since anything beyond them is truncated away.
        final int asciiLen = Math.min(str.length(), cap);
        if (AsciiKeyBytes.isAsciiPrefix(str, asciiLen)) {
          return AsciiKeyBytes.writeAsciiPrefix(str, asciiLen, dest, offset);
        }
        final byte[] utf8 = str.getBytes(StandardCharsets.UTF_8);
        final int maxLen = Math.min(utf8.length, cap);
        System.arraycopy(utf8, 0, dest, offset, maxLen);
        return maxLen;
      }
    }
  }

  /**
   * Encodes a numeric value using IEEE 754 order-preserving encoding.
   *
   * <p>
   * This ensures that byte comparison matches numeric comparison:
   * </p>
   * <ul>
   * <li>NaN is canonicalized to MAX_VALUE (sorts last)</li>
   * <li>Positive values: XOR sign bit</li>
   * <li>Negative values: XOR all bits</li>
   * </ul>
   */
  private int encodeNumericOrderPreserving(Atomic value, byte[] dest, int offset, boolean roundToFloat) {
    double d;
    if (value instanceof Numeric numeric) {
      d = numeric.doubleValue();
    } else {
      // Value is a string representation of a number - parse it
      try {
        d = Double.parseDouble(value.stringValue());
      } catch (NumberFormatException e) {
        // Can't parse as number - treat as 0 (or could throw)
        d = 0.0;
      }
    }

    if (roundToFloat) {
      // Before the NaN canonicalization, so a value that is finite as a double but overflows float
      // becomes an infinity here and keeps its own key rather than colliding with NaN's.
      d = (float) d;
    }

    // Canonicalize NaN to sort last
    if (Double.isNaN(d)) {
      d = Double.MAX_VALUE;
    }

    long bits = Double.doubleToLongBits(d);

    // Order-preserving transformation:
    // Positive numbers: flip sign bit (so they sort after negatives)
    // Negative numbers: flip all bits (so -1 > -2 in byte order)
    if (d >= 0) {
      bits ^= SIGN_FLIP;
    } else {
      bits ^= 0xFFFF_FFFF_FFFF_FFFFL;
    }

    // Write big-endian
    dest[offset] = (byte) (bits >>> 56);
    dest[offset + 1] = (byte) (bits >>> 48);
    dest[offset + 2] = (byte) (bits >>> 40);
    dest[offset + 3] = (byte) (bits >>> 32);
    dest[offset + 4] = (byte) (bits >>> 24);
    dest[offset + 5] = (byte) (bits >>> 16);
    dest[offset + 6] = (byte) (bits >>> 8);
    dest[offset + 7] = (byte) bits;

    return 8;
  }

  /**
   * Encodes an integer-family value (xs:integer and its subtypes such as xs:long and xs:int) as a
   * lossless, order-preserving 8-byte big-endian key.
   *
   * <p>
   * The signed 64-bit value is sign-flipped (XOR with the sign bit) so that unsigned byte comparison
   * matches signed numeric order. Unlike {@link #encodeNumericOrderPreserving}, the value is not
   * routed through {@code double}, so integers above 2<sup>53</sup> keep full precision. The encoding
   * is exact for the entire signed 64-bit range; xs:integer magnitudes beyond {@code Long} range are
   * narrowed by {@link Numeric#longValue()}.
   * </p>
   */
  private int encodeIntegerOrderPreserving(Atomic value, byte[] dest, int offset) {
    final long v;
    if (value instanceof Numeric numeric) {
      v = saturatingLong(numeric);
    } else {
      // Value is a string representation of an integer - parse it.
      long parsed;
      try {
        parsed = Long.parseLong(value.stringValue().trim());
      } catch (NumberFormatException e) {
        // NOT zero. xs:integer is unbounded, so a literal beyond Long's range is a perfectly legal
        // index value that Long.parseLong refuses — and mapping it to 0 put it on the key for zero,
        // in the MIDDLE of the key space, where `eq 0` returned it and every range query placed it on
        // the wrong side of every bound. Saturating keeps it at the end of the key space it actually
        // belongs to, so ordering survives; only two values that both overflow the same end share a
        // key, which narrowsNumeric reports as lossy so the caller re-checks.
        parsed = saturatingLong(value.stringValue());
      }
      v = parsed;
    }

    // Sign-flip so unsigned byte order matches signed numeric order.
    final long bits = v ^ SIGN_FLIP;

    dest[offset] = (byte) (bits >>> 56);
    dest[offset + 1] = (byte) (bits >>> 48);
    dest[offset + 2] = (byte) (bits >>> 40);
    dest[offset + 3] = (byte) (bits >>> 32);
    dest[offset + 4] = (byte) (bits >>> 24);
    dest[offset + 5] = (byte) (bits >>> 16);
    dest[offset + 6] = (byte) (bits >>> 8);
    dest[offset + 7] = (byte) bits;

    return 8;
  }

  /**
   * {@code value} as an {@code xs:boolean}, NOT as its effective boolean value.
   *
   * <p>
   * {@link Atomic#booleanValue()} is XQuery's EBV, and on a {@link Str} the EBV is "is the string
   * non-empty" — so it answers {@code true} for {@code "false"} just as readily as for
   * {@code "true"}. {@code CASIndexBuilder} stores every indexed value as a {@code Str}, so calling
   * it here mapped EVERY boolean in the index onto byte 1: {@code true} and {@code false} shared one
   * key and one posting list, while a probe — which {@code ScanCASIndex} casts to the content type,
   * giving a real {@link Bool} — encoded {@code false} to byte 0 and matched nothing at all.
   * </p>
   *
   * @param value the atomic being serialized
   * @return its boolean value, {@code false} for any lexical form that is not {@code true} or
   *         {@code 1}
   */
  private static boolean booleanValueOf(final Atomic value) {
    if (value instanceof final Bool bool) {
      return bool.booleanValue();
    }
    // The xs:boolean lexical space is exactly these four spellings; anything else is not of the type
    // and CASIndexBuilder's AtomicUtil#toType probe would already have rejected the node.
    final String lexical = value.stringValue().trim();
    return "true".equals(lexical) || "1".equals(lexical);
  }

  /**
   * {@code numeric} as a {@code long}, clamped to the {@code long} range rather than wrapped.
   *
   * <p>
   * BOTH sides of an integer index must clamp the same way, and this is the side that is easy to
   * miss. {@link Numeric#longValue()} is exact for the primitive-backed types but WRAPS for brackit's
   * arbitrary-precision {@code Int}, whose implementation is {@code BigDecimal#longValue()}: 2^63
   * comes back as {@code Long.MIN_VALUE} and 2^70 as {@code 0}. Saturating only the lexical side —
   * which is the side {@code CASIndexBuilder} always takes, since it stores every value as a
   * {@code Str} — while the probe side wrapped was WORSE than both wrapping: the stored value landed
   * at one end of the key space and the probe at the other, so an equality query that used to match
   * (both sides collapsing onto zero) began missing entirely.
   * </p>
   *
   * @param numeric the value being encoded
   * @return the clamped value
   */
  private static long saturatingLong(final Numeric numeric) {
    // Fast path: these wrap a primitive, so longValue() is exact and costs a field read. The careful
    // path below allocates, but only for a magnitude past int range, and the STORED side never
    // reaches it at all.
    if (numeric instanceof Int32 || numeric instanceof Int64) {
      return numeric.longValue();
    }
    return saturatingLong(numeric.stringValue());
  }

  /**
   * {@code lexical} as a {@code long}, clamped to the {@code long} range rather than wrapped or
   * zeroed.
   *
   * @param lexical the integer literal
   * @return the clamped value, or {@code 0} when {@code lexical} is not a number at all
   */
  private static long saturatingLong(final String lexical) {
    try {
      final BigDecimal exact = new BigDecimal(lexical.trim());
      if (exact.compareTo(LONG_MIN) <= 0) {
        return Long.MIN_VALUE;
      }
      if (exact.compareTo(LONG_MAX) >= 0) {
        return Long.MAX_VALUE;
      }
      return exact.longValue();
    } catch (final NumberFormatException e) {
      return 0L;
    }
  }

  /**
   * Whether the serialized key's unsigned BYTE order is the type's own value order — i.e. whether a
   * bounded byte-range cursor may decide a range query for this content type at all.
   *
   * <p>
   * True for the families {@link #encodeAtomicOrderPreserving} encodes deliberately: the integer
   * family, the floating-point family, {@code xs:decimal}, {@code xs:boolean}, {@code xs:string} —
   * and the instant family, which {@link InstantKeyCodec} stores as a fixed-width BINARY instant
   * whose byte order is chronological order.
   *
   * <p>
   * FALSE for everything else, which falls through to that method's string branch and is stored as
   * its raw lexical form. {@code xs:duration} is the family that matters there: lexical text order is
   * not duration order, so a byte-bounded scan would silently drop matching records, and callers fall
   * back to a typed {@link io.sirix.index.cas.CASFilterRange#inRange} comparison — which is exactly
   * what a {@code false} answer here makes {@code CASIndex} do. The instant family was in this
   * paragraph while it was stored lexically; it is not any more, and the block comment at the top of
   * this class records what that encoding got wrong.
   *
   * <p>
   * <b>This predicate is deliberately NOT "does the type have an id".</b> Those are different
   * questions, and fusing them is how the instant family briefly ended up on the byte-bounded fast
   * path with an encoding that was not order-preserving (see the burned-id note at the top of this
   * class). A type gets {@code true} here only when its encoder is known to preserve order.
   *
   * @param type the index's content type
   * @return {@code true} iff byte order over the encoded value equals value order
   */
  public static boolean isByteOrderPreserving(final Type type) {
    if (type == null) {
      return false;
    }
    final short id = getTypeId(type);
    return id == TYPE_STRING || id == TYPE_BOOLEAN || id == TYPE_DOUBLE || id == TYPE_FLOAT || id == TYPE_INTEGER
        || id == TYPE_DECIMAL || id == TYPE_DATETIME || id == TYPE_DATE || id == TYPE_TIME;
  }

  /**
   * Whether a numeric probe survives its encoder unchanged.
   *
   * <p>
   * The numeric encoders are fixed-width but NOT lossless: {@code encodeIntegerOrderPreserving}
   * narrows through {@code Numeric#longValue()} and {@code encodeNumericOrderPreserving} funnels
   * decimals through {@code double}, so values that differ past the encoder's precision share one key
   * and one merged posting list. The test is a round trip rather than a range check — encode the
   * value the way the serializer will and ask whether it comes back equal — because that is exactly
   * the property the seek depends on, and it needs no per-type precision arithmetic.
   * </p>
   *
   * @param value the probe
   * @param id the encoder that will be used
   * @return {@code true} when the encoding cannot distinguish {@code value} from other values
   */
  private static boolean narrowsNumeric(final Atomic value, final short id) {
    if (!(value instanceof final Numeric numeric)) {
      // A non-numeric atomic under a numeric index: the encoders fall back to 0 rather than failing,
      // so every such probe collapses onto the key for zero.
      return true;
    }
    if (id == TYPE_FLOAT) {
      // NARROWED to float by the encoder, so this is NOT the "the encoder is the double" case below.
      // Two distinct doubles that round to one float share a key: a probe of 1.10000001 lands on
      // stored 1.1's key, and every double above Float.MAX_VALUE collapses onto +Infinity's. Nothing
      // casts the probe on the way in — CASIndex builds its CASValue from whatever atomic the caller
      // passed — so a Dbl or Str probe reaches the float encoder unchanged and must be re-checked
      // unless it survives the narrowing exactly.
      final double d = numeric.doubleValue();
      return Double.isNaN(d) || (double) (float) d != d;
    }
    if (id == TYPE_DOUBLE) {
      // For these two the encoder IS the double, so the key round-trips by construction:
      // encodeNumericOrderPreserving stores numeric.doubleValue() verbatim, a float widens to double
      // exactly, and equality on an xs:double/xs:float index is therefore double equality. Only NaN
      // loses anything, because the encoder canonicalizes it onto Double.MAX_VALUE's key. Infinities
      // are NOT saturated — encodeNumericOrderPreserving touches only NaN, so they keep their own
      // distinct bit patterns and round-trip like any other value.
      //
      // Testing these through the decimal round trip below reported very nearly every value lossy:
      // "0.1" does not compare equal to 0.1000000000000000055511151231257827021181583404541015625,
      // so only dyadic rationals (0.5, 2.0) came back false. That sent every double/float EQUALITY
      // query through exactMatches' per-candidate document re-read, and every double/float RANGE
      // query off the bounded byte cursor into the O(index) typed full scan — a large regression on
      // exactly the indexes this predicate was added to keep correct.
      return Double.isNaN(numeric.doubleValue());
    }
    if (id != TYPE_INTEGER) {
      // xs:decimal: UNCONDITIONALLY lossy, and the round-trip test that used to stand here was
      // unsound rather than merely expensive. It asked "is the PROBE exactly a double", but the
      // collision that matters is between the probe's key and a STORED value's key, and the probe
      // cannot see the stored values. A probe of 0.5 IS exactly a double and was reported lossless,
      // so the re-check was switched off — while a stored 0.5000000000000000001 encodes to that same
      // double and came back as a hit for `eq 0.5`. Answering true for every decimal is sound, and no
      // slower in practice: the old test already said true for every non-dyadic literal, which is
      // essentially every price and measurement anyone indexes.
      //
      // This costs the EQUALITY path a re-check and nothing else. The range gates deliberately do not
      // consult this predicate for the numeric families, because their fallback re-derived its
      // comparison value from the very same narrowed key — see CASIndex#openHOTIndexWithRangeFilter.
      return true;
    }
    // Lossless across the whole signed 64-bit range and SATURATING outside it, so a probe inside the
    // range can only be reported lossy by genuinely being outside it. Two values collide only when
    // both overflow the same end, and this round trip catches exactly that.
    final BigDecimal exact;
    try {
      exact = new BigDecimal(numeric.stringValue());
    } catch (final NumberFormatException e) {
      // Not a decimal literal at all (a special value, or a lexical form BigDecimal declines), so
      // nothing here can prove the encoding keeps it apart from its neighbours.
      return true;
    }
    final long encoded = saturatingLong(numeric);
    if (encoded == Long.MAX_VALUE || encoded == Long.MIN_VALUE) {
      // The saturation sentinels are shared: every value past the range collapses onto them, and so
      // does the genuine Long.MAX_VALUE/MIN_VALUE. Saturating keeps ORDER (which zeroing destroyed)
      // but cannot keep the values apart, so these two keys are the one place an integer index needs
      // the re-check. Every other integer round-trips exactly and pays nothing.
      return true;
    }
    return exact.compareTo(BigDecimal.valueOf(encoded)) != 0;
  }

  /**
   * Whether serializing {@code value} under {@code type} loses information, so that a byte-exact seek
   * on the resulting key answers with a SUPERSET of the values that actually match.
   *
   * <p>
   * For the lexical family the rule is "does this type reach the TRUNCATING branch", NOT "is this
   * type a string". {@link #encodeAtomicOrderPreserving} switches on the type id and has no
   * {@link #TYPE_OTHER} case, so every type {@link #getTypeId} does not recognise —
   * {@code xs:anyURI}, {@code xs:untypedAtomic}, {@code xs:duration}, {@code xs:hexBinary},
   * {@code xs:QName}, and anything else a user names through {@code jn:create-cas-index} — falls into
   * {@code default:}, the string encoder, and is capped at {@link #MAX_STRING_VALUE_BYTES} exactly as
   * a string is. Testing for {@link #TYPE_STRING} alone left that whole family answering with an
   * unchecked superset. A caller that seeks on a key this reports {@code true} for must re-check its
   * hits against the real values, because nothing in the index can separate them.
   * </p>
   *
   * <p>
   * <b>Truncation is not the only way a CAS key loses information</b>, and the two are reported
   * differently. Numeric NARROWING is detected by {@link #narrowsNumeric} and reported here:
   * {@link #encodeNumericOrderPreserving} routes {@code xs:decimal} through {@code doubleValue()} and
   * {@link #encodeIntegerOrderPreserving} narrows through {@code longValue()}, so values differing
   * past the encoder's precision share one key and one posting list. The re-check a {@code true}
   * answer triggers in {@code CASIndex} is TYPED — it picks a numeric or a byte comparison from the
   * INDEX's content type rather than from the candidate's node kind — so it closes the narrowing case
   * as well as the truncation one. Dispatching on the node instead is what made a numeric index over
   * XML compare {@code "1.50"} against {@code "1.5"} lexically and drop the row.
   * </p>
   *
   * <p>
   * RANGE callers must not consult this predicate; they want {@link #truncates}. Numeric narrowing is
   * monotone, so a bounded cursor still places every stored key correctly against the bound, and the
   * O(index) fallback a {@code true} answer used to trigger re-derived its comparison value from the
   * very same narrowed key — the identical answer at vastly higher cost.
   * </p>
   *
   * <p>
   * <b>The boundary is {@code >=}, not {@code >}</b>, and the difference is a real over-match rather
   * than a rounding preference. A value measuring EXACTLY {@link #MAX_STRING_VALUE_BYTES} is itself
   * stored losslessly, but the encoder caps every LONGER value at the same 246 bytes, so a 250-byte
   * stored value sharing that prefix produces a byte-identical key of identical length — and the
   * chunk walk filters on the composite length, so nothing downstream separates them either. At
   * exactly the cap the seek is therefore GUARANTEED to over-match, which is precisely where a
   * {@code >} test switched the caller's re-check off.
   * </p>
   *
   * @param value the atomic being probed for, may be {@code null}
   * @param type the index's declared content type
   * @return {@code true} when the key cannot distinguish {@code value} from other values
   */
  public static boolean losesInformation(final @Nullable Atomic value, final Type type) {
    if (value == null) {
      return false;
    }
    final short id = getTypeId(requireNonNull(type, "type"));
    if (id == TYPE_INTEGER || id == TYPE_DECIMAL || id == TYPE_DOUBLE || id == TYPE_FLOAT) {
      return narrowsNumeric(value, id);
    }
    return truncates(value, id);
  }

  /**
   * Whether {@code type} is encoded by one of the NARROWING numeric encoders.
   *
   * <p>
   * The distinction a value re-check needs: for these types the same logical number can be stored as
   * a {@code NumericValueNode} (JSON) or as raw lexical bytes (XML), and two lexically different
   * forms — {@code 1.50} and {@code 1.5} — are the same value. A re-check that compared bytes would
   * drop one of them.
   * </p>
   *
   * @param type the index's declared content type
   * @return {@code true} for the integer, decimal, double and float families
   */
  public static boolean isNumericFamily(final Type type) {
    final short id = getTypeId(requireNonNull(type, "type"));
    return id == TYPE_INTEGER || id == TYPE_DECIMAL || id == TYPE_DOUBLE || id == TYPE_FLOAT;
  }

  /**
   * Whether serializing {@code value} under {@code type} TRUNCATES it — the lexical half of
   * {@link #losesInformation}, reported on its own because the two halves need opposite remedies.
   *
   * <p>
   * A truncated bound breaks the bounded cursor's ordering, and no re-derivation from the stored key
   * can repair it, because the bytes the encoder dropped are not in the index either. Numeric
   * narrowing is different: the encoder is monotone, so the cursor still places every stored key
   * correctly against the bound, and a caller that reacts to narrowing by abandoning the cursor pays
   * an O(index) scan to reach exactly the same answer. Range callers therefore consult THIS, not
   * {@link #losesInformation}.
   * </p>
   *
   * @param value the bound being serialized, may be {@code null}
   * @param type the index's declared content type
   * @return {@code true} when the encoder caps {@code value} at {@link #MAX_STRING_VALUE_BYTES}
   */
  public static boolean truncates(final @Nullable Atomic value, final Type type) {
    if (value == null) {
      return false;
    }
    return truncates(value, getTypeId(requireNonNull(type, "type")));
  }

  private static boolean truncates(final Atomic value, final short id) {
    if (id != TYPE_STRING && id != TYPE_OTHER) {
      return false;
    }
    final String str = value.stringValue();
    // Cheap bounds first — this runs once per equality query, but the UTF-8 measurement below walks
    // the string and there is no reason to do it for the ~every value that is obviously short.
    // One char is at least one UTF-8 byte, so a char count at or past the cap already settles it;
    // and no char exceeds three UTF-8 bytes (a surrogate PAIR is four bytes across two chars, i.e.
    // two per char), so a char count under a third of the cap settles the other direction.
    final int chars = str.length();
    if (chars >= MAX_STRING_VALUE_BYTES) {
      return true;
    }
    // ORDER-DEPENDENT, and the dependence is the point: the test above caps `chars` below 246, so
    // the product below cannot overflow. Removing it — on the reasoning that utf8Length subsumes it
    // — would let `chars * 3` go negative for a string past ~715M chars, and a negative product
    // satisfies `< MAX_STRING_VALUE_BYTES`, reporting a gigabyte-long value as losslessly
    // representable and switching the caller's re-check off for exactly the value that needs it.
    // STRICT `<`, matching the `>=` boundary: 82 three-byte chars measure exactly 246, which
    // collides, so the short-circuit must not claim it is safe.
    if (chars * 3 < MAX_STRING_VALUE_BYTES) {
      return false;
    }
    return utf8Length(str) >= MAX_STRING_VALUE_BYTES;
  }

  /**
   * UTF-8 length of {@code str}, counted without encoding it.
   *
   * <p>
   * An UNPAIRED surrogate is counted as three bytes while {@code String.getBytes(UTF_8)} — what the
   * serializer actually calls — substitutes a single {@code '?'}. The error is one-directional (this
   * never under-counts), so truncation is never MISSED; it can only be over-reported, which costs the
   * caller a re-check it did not need. Counting the substitution instead would make this method's
   * answer depend on the JDK's malformed-input policy rather than on the string.
   * </p>
   *
   * @param str the string to measure
   * @return its length in UTF-8 bytes, never below what the serializer writes
   */
  private static int utf8Length(final String str) {
    int length = 0;
    for (int i = 0, n = str.length(); i < n; i++) {
      final char c = str.charAt(i);
      if (c < 0x80) {
        length += 1;
      } else if (c < 0x800) {
        length += 2;
      } else if (Character.isHighSurrogate(c) && i + 1 < n && Character.isLowSurrogate(str.charAt(i + 1))) {
        length += 4;
        i++;
      } else {
        length += 3;
      }
    }
    return length;
  }

  /**
   * Gets a stable type ID for serialization.
   */
  private static short getTypeId(Type type) {
    // Ordered by FREQUENCY, not taxonomy. Type.instanceOf walks the parent chain, so every test that
    // fails before the right one costs a pointer chase -- and this runs once per key serialized, on
    // the insert path and on every lookup's probe. Putting the instant family first (which reads
    // more tidily) made every string key pay three failing walks before reaching its own branch.
    // The one ordering constraint that is NOT about frequency: xs:integer must be tested before
    // xs:decimal, since integers are decimals but need the lossless 64-bit encoding.
    if (type.instanceOf(Type.STR)) {
      return TYPE_STRING;
    } else if (type.instanceOf(Type.BOOL)) {
      return TYPE_BOOLEAN;
    } else if (type.instanceOf(Type.DBL)) {
      return TYPE_DOUBLE;
    } else if (type.instanceOf(Type.FLO)) {
      return TYPE_FLOAT;
    } else if (type.instanceOf(Type.INR)) {
      // Integer family: xs:integer and all subtypes (xs:long, xs:int, xs:short, ...).
      // Checked before xs:decimal because xs:integer instanceOf xs:decimal is true,
      // yet integers use the lossless 64-bit encoding, not the IEEE-754 double encoding.
      return TYPE_INTEGER;
    } else if (type.instanceOf(Type.DATI)) {
      return TYPE_DATETIME;
    } else if (type.instanceOf(Type.DATE)) {
      return TYPE_DATE;
    } else if (type.instanceOf(Type.TIME)) {
      return TYPE_TIME;
    } else if (type.instanceOf(Type.DEC)) {
      return TYPE_DECIMAL;
    }
    return TYPE_OTHER;
  }

  /**
   * Gets a Type from a stable ID.
   */
  private static Type getTypeFromId(short typeId) {
    return switch (typeId) {
      case TYPE_STRING -> Type.STR;
      case TYPE_BOOLEAN -> Type.BOOL;
      case TYPE_DOUBLE -> Type.DBL;
      case TYPE_FLOAT -> Type.FLO;
      case TYPE_INTEGER -> Type.LON;
      case TYPE_DECIMAL -> Type.DEC;
      case TYPE_DATETIME -> Type.DATI;
      case TYPE_DATE -> Type.DATE;
      case TYPE_TIME -> Type.TIME;
      default -> Type.STR; // Fallback
    };
  }

  /**
   * Decodes an atomic value from bytes.
   */
  private static Atomic decodeAtomic(byte[] bytes, int offset, int length, Type type) {
    if (InstantKeyCodec.isInstantType(type)) {
      return InstantKeyCodec.decode(bytes, offset, length, type);
    }
    if (type.instanceOf(Type.INR)) {
      // Integer family: reverse the lossless 64-bit sign-flipped encoding.
      long bits = ((long) (bytes[offset] & 0xFF) << 56) | ((long) (bytes[offset + 1] & 0xFF) << 48)
          | ((long) (bytes[offset + 2] & 0xFF) << 40) | ((long) (bytes[offset + 3] & 0xFF) << 32)
          | ((long) (bytes[offset + 4] & 0xFF) << 24) | ((long) (bytes[offset + 5] & 0xFF) << 16)
          | ((long) (bytes[offset + 6] & 0xFF) << 8) | ((long) (bytes[offset + 7] & 0xFF));
      long longValue = bits ^ SIGN_FLIP;
      return new io.brackit.query.atomic.Int64(longValue);
    } else if (type.isNumeric()) {
      // Decode IEEE 754 order-preserving format (xs:double, xs:float, xs:decimal)
      long bits = ((long) (bytes[offset] & 0xFF) << 56) | ((long) (bytes[offset + 1] & 0xFF) << 48)
          | ((long) (bytes[offset + 2] & 0xFF) << 40) | ((long) (bytes[offset + 3] & 0xFF) << 32)
          | ((long) (bytes[offset + 4] & 0xFF) << 24) | ((long) (bytes[offset + 5] & 0xFF) << 16)
          | ((long) (bytes[offset + 6] & 0xFF) << 8) | ((long) (bytes[offset + 7] & 0xFF));

      // Reverse the order-preserving transformation
      if ((bits & SIGN_FLIP) != 0) {
        // Was positive: flip sign bit back
        bits ^= SIGN_FLIP;
      } else {
        // Was negative: flip all bits back
        bits ^= 0xFFFF_FFFF_FFFF_FFFFL;
      }

      double d = Double.longBitsToDouble(bits);

      // Return the appropriate type based on the stored type
      if (type.instanceOf(Type.DEC)) {
        return new io.brackit.query.atomic.Dec(java.math.BigDecimal.valueOf(d));
      } else if (type.instanceOf(Type.FLO)) {
        return new io.brackit.query.atomic.Flt((float) d);
      } else {
        return new io.brackit.query.atomic.Dbl(d);
      }
    } else if (type.instanceOf(Type.BOOL)) {
      return new io.brackit.query.atomic.Bool(bytes[offset] == 1);
    } else {
      // String
      String str = new String(bytes, offset, length, StandardCharsets.UTF_8);
      return new io.brackit.query.atomic.Str(str);
    }
  }
}

