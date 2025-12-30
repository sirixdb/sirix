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
import io.sirix.index.redblacktree.keyvalue.CASValue;

import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

/**
 * Order-preserving serializer for CAS (Content-and-Structure) index keys.
 *
 * <p>Serializes {@link CASValue} to bytes such that the byte order matches
 * the natural comparison order defined by {@link CASValue#compareTo(CASValue)}:</p>
 * <ol>
 *   <li>pathNodeKey (8 bytes, sign-flipped for order preservation)</li>
 *   <li>type ID (2 bytes)</li>
 *   <li>value (N bytes, order-preserving encoding)</li>
 * </ol>
 *
 * <h2>Order Preservation</h2>
 * <ul>
 *   <li><b>pathNodeKey:</b> XOR with sign bit for unsigned byte comparison</li>
 *   <li><b>Numeric values:</b> IEEE 754 order-preserving encoding</li>
 *   <li><b>String values:</b> UTF-8 (already lexicographically ordered)</li>
 * </ul>
 *
 * <h2>Zero Allocation</h2>
 * <p>All methods write to caller-provided buffers. No ByteBuffer allocation.</p>
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
  private static final short TYPE_LONG = 6;
  private static final short TYPE_DECIMAL = 7;
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
      offset += encodeAtomicOrderPreserving(atomicValue, type, dest, offset);
    }

    int bytesWritten = offset - start;
    if (bytesWritten == 10) {
      // Only header, no value - shouldn't happen for valid CASValue
      throw new IllegalArgumentException("CASValue has no atomic value");
    }

    return bytesWritten;
  }

  @Override
  public CASValue deserialize(byte[] bytes, int offset, int length) {
    // Read path node key (8 bytes)
    long signFlipped =
        ((long) (bytes[offset]     & 0xFF) << 56) |
        ((long) (bytes[offset + 1] & 0xFF) << 48) |
        ((long) (bytes[offset + 2] & 0xFF) << 40) |
        ((long) (bytes[offset + 3] & 0xFF) << 32) |
        ((long) (bytes[offset + 4] & 0xFF) << 24) |
        ((long) (bytes[offset + 5] & 0xFF) << 16) |
        ((long) (bytes[offset + 6] & 0xFF) << 8)  |
        ((long) (bytes[offset + 7] & 0xFF));
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

  /**
   * Maximum bytes available for string value encoding.
   * Header is 10 bytes (8 for pathNodeKey + 2 for typeId), buffer is 256 bytes.
   */
  private static final int MAX_STRING_VALUE_BYTES = 246;

  /**
   * Encodes an atomic value in order-preserving format.
   *
   * @param value  the atomic value
   * @param type   the type
   * @param dest   destination buffer
   * @param offset offset to write at
   * @return number of bytes written
   */
  private int encodeAtomicOrderPreserving(Atomic value, Type type, byte[] dest, int offset) {
    if (type.isNumeric()) {
      return encodeNumericOrderPreserving(value, dest, offset);
    } else if (type.instanceOf(Type.BOOL)) {
      // Boolean: 0 for false, 1 for true (already ordered)
      dest[offset] = value.booleanValue() ? (byte) 1 : (byte) 0;
      return 1;
    } else {
      // String: UTF-8 is already lexicographically ordered
      String str = value.stringValue();
      byte[] utf8 = str.getBytes(StandardCharsets.UTF_8);
      // Truncate to fit buffer (preserves lexicographic ordering for prefixes)
      int maxLen = Math.min(utf8.length, dest.length - offset);
      maxLen = Math.min(maxLen, MAX_STRING_VALUE_BYTES);
      System.arraycopy(utf8, 0, dest, offset, maxLen);
      return maxLen;
    }
  }

  /**
   * Encodes a numeric value using IEEE 754 order-preserving encoding.
   *
   * <p>This ensures that byte comparison matches numeric comparison:</p>
   * <ul>
   *   <li>NaN is canonicalized to MAX_VALUE (sorts last)</li>
   *   <li>Positive values: XOR sign bit</li>
   *   <li>Negative values: XOR all bits</li>
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
    dest[offset]     = (byte) (bits >>> 56);
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
   * Gets a stable type ID for serialization.
   */
  private static short getTypeId(Type type) {
    if (type.instanceOf(Type.STR)) {
      return TYPE_STRING;
    } else if (type.instanceOf(Type.BOOL)) {
      return TYPE_BOOLEAN;
    } else if (type.instanceOf(Type.DBL)) {
      return TYPE_DOUBLE;
    } else if (type.instanceOf(Type.FLO)) {
      return TYPE_FLOAT;
    } else if (type.instanceOf(Type.INT)) {
      return TYPE_INTEGER;
    } else if (type.instanceOf(Type.LON)) {
      return TYPE_LONG;
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
      case TYPE_INTEGER -> Type.INT;
      case TYPE_LONG -> Type.LON;
      case TYPE_DECIMAL -> Type.DEC;
      default -> Type.STR; // Fallback
    };
  }

  /**
   * Decodes an atomic value from bytes.
   */
  private static Atomic decodeAtomic(byte[] bytes, int offset, int length, Type type) {
    if (type.isNumeric()) {
      // Decode IEEE 754 order-preserving format
      long bits =
          ((long) (bytes[offset]     & 0xFF) << 56) |
          ((long) (bytes[offset + 1] & 0xFF) << 48) |
          ((long) (bytes[offset + 2] & 0xFF) << 40) |
          ((long) (bytes[offset + 3] & 0xFF) << 32) |
          ((long) (bytes[offset + 4] & 0xFF) << 24) |
          ((long) (bytes[offset + 5] & 0xFF) << 16) |
          ((long) (bytes[offset + 6] & 0xFF) << 8)  |
          ((long) (bytes[offset + 7] & 0xFF));

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
      } else if (type.instanceOf(Type.INT)) {
        return new io.brackit.query.atomic.Int32((int) d);
      } else if (type.instanceOf(Type.LON)) {
        return new io.brackit.query.atomic.Int64((long) d);
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

