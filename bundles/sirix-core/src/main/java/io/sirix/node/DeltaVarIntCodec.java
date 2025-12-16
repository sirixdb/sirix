/*
 * Copyright (c) 2024, Sirix Contributors
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

package io.sirix.node;

import io.sirix.settings.Fixed;

/**
 * High-performance codec for delta-encoded node keys using zigzag + varint encoding.
 * 
 * <p>Inspired by Umbra-style storage optimization: instead of storing absolute 8-byte keys,
 * we store deltas relative to the current nodeKey. Since siblings are usually adjacent
 * (delta = ±1) and parents are nearby, deltas are small and compress well.
 * 
 * <h2>Encoding Strategy</h2>
 * <ul>
 *   <li><b>NULL keys</b>: Encoded as a single byte 0x00 (zigzag of 0 with special meaning)</li>
 *   <li><b>Delta encoding</b>: Store (targetKey - baseKey) instead of absolute key</li>
 *   <li><b>Zigzag encoding</b>: Maps signed to unsigned (0→0, -1→1, 1→2, -2→3, 2→4, ...)</li>
 *   <li><b>Varint encoding</b>: Variable-length unsigned integer (7 bits per byte, MSB = continue)</li>
 * </ul>
 * 
 * <h2>Space Savings Example</h2>
 * <pre>
 * Absolute key 1000000 (8 bytes) → Delta +1 → Zigzag 2 → Varint 1 byte
 * Absolute key -1 (NULL, 8 bytes) → Special marker → 1 byte
 * </pre>
 * 
 * <h2>Performance</h2>
 * <ul>
 *   <li>No allocations during encode/decode</li>
 *   <li>Branchless zigzag encoding</li>
 *   <li>Unrolled varint for common cases (1-2 bytes)</li>
 * </ul>
 * 
 * @author Johannes Lichtenberger
 */
public final class DeltaVarIntCodec {
  
  /**
   * Sentinel value for NULL node keys.
   * We use 0 in zigzag encoding as the NULL marker since delta=0 (self-reference) is meaningless.
   */
  private static final int NULL_MARKER = 0;
  
  /**
   * The actual NULL key value from the Fixed enum.
   */
  private static final long NULL_KEY = Fixed.NULL_NODE_KEY.getStandardProperty();
  
  private DeltaVarIntCodec() {
    // Utility class
  }
  
  // ==================== ENCODING ====================
  
  /**
   * Encode a node key as a delta from a base key.
   * 
   * <p>For structural pointers (siblings, children), the base is typically the current nodeKey.
   * For parent pointers, the base is also the current nodeKey.
   * 
   * @param sink    output sink to write to
   * @param key     the key to encode (may be NULL_NODE_KEY)
   * @param baseKey the reference key for delta calculation
   */
  public static void encodeDelta(BytesOut<?> sink, long key, long baseKey) {
    if (key == NULL_KEY) {
      // NULL key: write single byte marker
      sink.writeByte((byte) NULL_MARKER);
      return;
    }
    
    // Calculate delta
    long delta = key - baseKey;
    
    // Zigzag encode (signed → unsigned)
    // This maps small positive/negative values to small unsigned values:
    // 0 → 0, -1 → 1, 1 → 2, -2 → 3, 2 → 4, ...
    // But we reserve 0 for NULL, so we add 1 to shift everything up
    long zigzag = zigzagEncode(delta) + 1;
    
    // Varint encode
    writeVarLong(sink, zigzag);
  }
  
  /**
   * Encode an absolute key (no delta, used for nodeKey itself).
   * Uses varint encoding directly for unsigned values.
   * 
   * @param sink output sink
   * @param key  the absolute key (must be non-negative)
   */
  public static void encodeAbsolute(BytesOut<?> sink, long key) {
    if (key < 0) {
      throw new IllegalArgumentException("Absolute key must be non-negative: " + key);
    }
    writeVarLong(sink, key);
  }
  
  /**
   * Encode a revision number which can be -1 (NULL_REVISION_NUMBER).
   * Uses zigzag encoding to handle the -1 case efficiently.
   * 
   * @param sink output sink
   * @param revision the revision number (can be -1)
   */
  public static void encodeRevision(BytesOut<?> sink, int revision) {
    // Use zigzag encoding: -1 → 1, 0 → 0, 1 → 2, etc.
    // Then varint encode
    long zigzag = zigzagEncode(revision);
    writeVarLong(sink, zigzag);
  }
  
  /**
   * Decode a revision number which can be -1 (NULL_REVISION_NUMBER).
   * 
   * @param source input source
   * @return the decoded revision number
   */
  public static int decodeRevision(BytesIn<?> source) {
    long zigzag = readVarLong(source);
    return (int) zigzagDecode(zigzag);
  }
  
  /**
   * Encode a signed integer (like nameKey which is a hash and can be negative).
   * Uses zigzag encoding to handle negative values efficiently.
   * 
   * @param sink output sink
   * @param value the signed integer value
   */
  public static void encodeSigned(BytesOut<?> sink, int value) {
    long zigzag = zigzagEncode(value);
    writeVarLong(sink, zigzag);
  }
  
  /**
   * Decode a signed integer.
   * 
   * @param source input source
   * @return the decoded signed integer
   */
  public static int decodeSigned(BytesIn<?> source) {
    long zigzag = readVarLong(source);
    return (int) zigzagDecode(zigzag);
  }
  
  // ==================== DECODING ====================
  
  /**
   * Decode a delta-encoded node key.
   * 
   * @param source  input source to read from
   * @param baseKey the reference key for delta calculation
   * @return the decoded absolute key, or NULL_NODE_KEY if null marker
   */
  public static long decodeDelta(BytesIn<?> source, long baseKey) {
    long zigzag = readVarLong(source);
    
    if (zigzag == NULL_MARKER) {
      return NULL_KEY;
    }
    
    // Undo the +1 shift and zigzag decode
    long delta = zigzagDecode(zigzag - 1);
    
    return baseKey + delta;
  }
  
  /**
   * Decode an absolute key (no delta).
   * 
   * @param source input source
   * @return the absolute key
   */
  public static long decodeAbsolute(BytesIn<?> source) {
    return readVarLong(source);
  }
  
  // ==================== ZIGZAG ENCODING ====================
  
  /**
   * Zigzag encode a signed long to unsigned.
   * Maps: 0→0, -1→1, 1→2, -2→3, 2→4, ...
   * 
   * <p>This is branchless and uses arithmetic shift.
   */
  private static long zigzagEncode(long value) {
    return (value << 1) ^ (value >> 63);
  }
  
  /**
   * Zigzag decode an unsigned long to signed.
   * Inverse of zigzagEncode.
   */
  private static long zigzagDecode(long encoded) {
    return (encoded >>> 1) ^ -(encoded & 1);
  }
  
  // ==================== VARINT ENCODING ====================
  
  /**
   * Write a variable-length unsigned long.
   * Uses 7 bits per byte, MSB indicates continuation.
   * 
   * <p>Optimized for small values (1-2 bytes for values 0-16383).
   */
  private static void writeVarLong(BytesOut<?> sink, long value) {
    // Fast path for small values (fits in 1 byte: 0-127)
    if ((value & ~0x7FL) == 0) {
      sink.writeByte((byte) value);
      return;
    }
    
    // Fast path for 2-byte values (128-16383)
    if ((value & ~0x3FFFL) == 0) {
      sink.writeByte((byte) ((value & 0x7F) | 0x80));
      sink.writeByte((byte) (value >>> 7));
      return;
    }
    
    // General case for larger values
    while ((value & ~0x7FL) != 0) {
      sink.writeByte((byte) ((value & 0x7F) | 0x80));
      value >>>= 7;
    }
    sink.writeByte((byte) value);
  }
  
  /**
   * Read a variable-length unsigned long.
   * 
   * <p>Optimized with fast paths for common 1-byte and 2-byte cases.
   * Most delta-encoded values (siblings ±1, nearby parents) fit in 1-2 bytes.
   */
  private static long readVarLong(BytesIn<?> source) {
    byte b = source.readByte();
    
    // Fast path: 1 byte (0-127) - most common case for small deltas
    if ((b & 0x80) == 0) {
      return b;
    }
    
    long result = b & 0x7F;
    b = source.readByte();
    
    // Fast path: 2 bytes (128-16383) - second most common
    if ((b & 0x80) == 0) {
      return result | ((long) b << 7);
    }
    
    // General case for larger values (rare for structural keys)
    result |= (long) (b & 0x7F) << 7;
    int shift = 14;
    do {
      b = source.readByte();
      result |= (long) (b & 0x7F) << shift;
      shift += 7;
      
      if (shift > 63) {
        throw new IllegalStateException("Varint too long (more than 10 bytes)");
      }
    } while ((b & 0x80) != 0);
    
    return result;
  }
  
  // ==================== SIZE ESTIMATION ====================
  
  /**
   * Estimate the encoded size of a delta-encoded key (for buffer sizing).
   * 
   * @param key     the key to encode
   * @param baseKey the reference key
   * @return estimated bytes needed
   */
  public static int estimateEncodedSize(long key, long baseKey) {
    if (key == NULL_KEY) {
      return 1;
    }
    
    long delta = key - baseKey;
    long zigzag = zigzagEncode(delta) + 1;
    return varLongSize(zigzag);
  }
  
  /**
   * Calculate the number of bytes needed for a varint.
   */
  private static int varLongSize(long value) {
    int size = 1;
    while ((value & ~0x7FL) != 0) {
      size++;
      value >>>= 7;
    }
    return size;
  }
}
