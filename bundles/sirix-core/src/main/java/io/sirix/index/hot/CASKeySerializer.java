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
import io.brackit.query.atomic.Numeric;
import io.brackit.query.jdm.Type;
import io.sirix.index.InstantKeyCodec;
import io.sirix.index.redblacktree.keyvalue.CASValue;

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
   * The stored VALUE is the raw lexical form (the string branch of
   * {@link #encodeAtomicOrderPreserving}), and {@link #isByteOrderPreserving} deliberately reports
   * {@code false} for them, so every range query is decided by that typed comparison and never by a
   * byte-bounded cursor. An id is emphatically NOT a claim of byte-orderability — see the predicate's
   * javadoc.
   * </p>
   */
  private static final short TYPE_DATETIME = 8;
  private static final short TYPE_DATE = 9;
  private static final short TYPE_TIME = 10;

  // These were briefly stored through a BINARY instant codec on this branch. That was wrong and is
  // reverted to the lexical form. Measured against brackit's own comparison semantics:
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
  // lists, which corrupts equality lookups and deletes, not just ranges. The lexical form is
  // injective, so it is what gets stored; chronological ORDER comes from the typed
  // CASFilterRange#inRange comparison, which isByteOrderPreserving=false routes every instant range
  // query through. That gate is the actual fix for the original defect — a byte-bounded cursor
  // deciding instant ranges on lexical bytes and silently dropping records.

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
      case TYPE_FLOAT:
      case TYPE_DECIMAL:
        return encodeNumericOrderPreserving(value, dest, offset);
      case TYPE_BOOLEAN:
        // Boolean: 0 for false, 1 for true (already ordered)
        dest[offset] = value.booleanValue()
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
  private int encodeNumericOrderPreserving(Atomic value, byte[] dest, int offset) {
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
      v = numeric.longValue();
    } else {
      // Value is a string representation of an integer - parse it.
      long parsed;
      try {
        parsed = Long.parseLong(value.stringValue().trim());
      } catch (NumberFormatException e) {
        parsed = 0L;
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
   * Whether the serialized key's unsigned BYTE order is the type's own value order — i.e. whether a
   * bounded byte-range cursor may decide a range query for this content type at all.
   *
   * <p>
   * True for the families {@link #encodeAtomicOrderPreserving} encodes deliberately: the integer
   * family, the floating-point family, {@code xs:decimal}, {@code xs:boolean} and {@code xs:string}.
   * FALSE for everything else, which falls through to that method's string branch and is stored as
   * its raw lexical form: {@code xs:dateTime}, {@code xs:date}, {@code xs:time} and
   * {@code xs:duration} all order textually there, which is not chronological order. Text order puts
   * {@code "…T12:00:00.5Z"} below {@code "…T12:00:00Z"} ({@code '.'} &lt; {@code 'Z'}) and mixes
   * timezone spellings arbitrarily ({@code '+'}, {@code '-'} &lt; {@code 'Z'}), so a byte-bounded
   * scan silently drops matching records. Callers must fall back to a typed
   * {@link io.sirix.index.cas.CASFilterRange#inRange} comparison for those — which is exactly what a
   * {@code false} answer here makes {@code CASIndex} do.
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

