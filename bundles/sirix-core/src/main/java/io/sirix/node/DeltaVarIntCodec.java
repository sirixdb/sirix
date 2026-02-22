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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

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
  
  /**
   * Encode a signed long value using zigzag + varint encoding.
   * Optimized for small values: values -64 to 63 use 1 byte, -8192 to 8191 use 2 bytes.
   * 
   * <p>This is ideal for JSON numbers which are often small integers stored as long.
   * 
   * @param sink output sink
   * @param value the signed long value
   */
  public static void encodeSignedLong(BytesOut<?> sink, long value) {
    long zigzag = zigzagEncode(value);
    writeVarLong(sink, zigzag);
  }
  
  /**
   * Decode a signed long value from zigzag + varint encoding.
   * 
   * @param source input source
   * @return the decoded signed long
   */
  public static long decodeSignedLong(BytesIn<?> source) {
    long zigzag = readVarLong(source);
    return zigzagDecode(zigzag);
  }
  
  /**
   * Encode an unsigned long value using varint encoding.
   * Optimized for small values: values 0-127 use 1 byte, 128-16383 use 2 bytes.
   * 
   * @param sink output sink
   * @param value the unsigned long value (must be non-negative)
   */
  public static void encodeUnsignedLong(BytesOut<?> sink, long value) {
    writeVarLong(sink, value);
  }
  
  /**
   * Decode an unsigned long value from varint encoding.
   * 
   * @param source input source
   * @return the decoded unsigned long
   */
  public static long decodeUnsignedLong(BytesIn<?> source) {
    return readVarLong(source);
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
  
  // ==================== DIRECT SEGMENT WRITE METHODS ====================
  // These methods eliminate per-byte ensureCapacity() overhead by using direct segment writes
  
  /**
   * Encode a node key as a delta from a base key, writing directly to a GrowingMemorySegment.
   * This is the high-performance variant that eliminates per-byte capacity checks.
   * 
   * @param seg     the segment to write to
   * @param key     the key to encode (may be NULL_NODE_KEY)
   * @param baseKey the reference key for delta calculation
   */
  public static void encodeDelta(GrowingMemorySegment seg, long key, long baseKey) {
    if (key == NULL_KEY) {
      // NULL key: write single byte marker
      seg.writeByte((byte) NULL_MARKER);
      return;
    }
    
    // Calculate delta and zigzag encode (add 1 to reserve 0 for NULL)
    long delta = key - baseKey;
    long zigzag = zigzagEncode(delta) + 1;
    
    // Use direct varint write (single ensureCapacity call)
    seg.writeVarLong(zigzag);
  }
  
  /**
   * Encode a node key as a delta from a base key, writing directly to a PooledGrowingSegment.
   * This is the high-performance variant that eliminates per-byte capacity checks.
   * 
   * @param seg     the segment to write to
   * @param key     the key to encode (may be NULL_NODE_KEY)
   * @param baseKey the reference key for delta calculation
   */
  public static void encodeDelta(PooledGrowingSegment seg, long key, long baseKey) {
    if (key == NULL_KEY) {
      // NULL key: write single byte marker
      seg.writeByte((byte) NULL_MARKER);
      return;
    }
    
    // Calculate delta and zigzag encode (add 1 to reserve 0 for NULL)
    long delta = key - baseKey;
    long zigzag = zigzagEncode(delta) + 1;
    
    // Use direct varint write (single ensureCapacity call)
    seg.writeVarLong(zigzag);
  }
  
  /**
   * Encode an absolute key directly to a GrowingMemorySegment.
   * 
   * @param seg the segment to write to
   * @param key the absolute key (must be non-negative)
   */
  public static void encodeAbsolute(GrowingMemorySegment seg, long key) {
    if (key < 0) {
      throw new IllegalArgumentException("Absolute key must be non-negative: " + key);
    }
    seg.writeVarLong(key);
  }
  
  /**
   * Encode an absolute key directly to a PooledGrowingSegment.
   * 
   * @param seg the segment to write to
   * @param key the absolute key (must be non-negative)
   */
  public static void encodeAbsolute(PooledGrowingSegment seg, long key) {
    if (key < 0) {
      throw new IllegalArgumentException("Absolute key must be non-negative: " + key);
    }
    seg.writeVarLong(key);
  }
  
  /**
   * Encode a signed long value using zigzag + varint directly to a GrowingMemorySegment.
   * 
   * @param seg   the segment to write to
   * @param value the signed long value
   */
  public static void encodeSignedLong(GrowingMemorySegment seg, long value) {
    long zigzag = zigzagEncode(value);
    seg.writeVarLong(zigzag);
  }
  
  /**
   * Encode a signed long value using zigzag + varint directly to a PooledGrowingSegment.
   * 
   * @param seg   the segment to write to
   * @param value the signed long value
   */
  public static void encodeSignedLong(PooledGrowingSegment seg, long value) {
    long zigzag = zigzagEncode(value);
    seg.writeVarLong(zigzag);
  }
  
  /**
   * Encode an unsigned long value using varint directly to a GrowingMemorySegment.
   * 
   * @param seg   the segment to write to
   * @param value the unsigned long value (must be non-negative)
   */
  public static void encodeUnsignedLong(GrowingMemorySegment seg, long value) {
    seg.writeVarLong(value);
  }
  
  /**
   * Encode an unsigned long value using varint directly to a PooledGrowingSegment.
   * 
   * @param seg   the segment to write to
   * @param value the unsigned long value (must be non-negative)
   */
  public static void encodeUnsignedLong(PooledGrowingSegment seg, long value) {
    seg.writeVarLong(value);
  }
  
  // ==================== MEMORY SEGMENT SUPPORT ====================
  // These methods support zero-allocation access by reading directly from MemorySegment
  
  /**
   * Decode a delta-encoded key directly from MemorySegment.
   * ZERO ALLOCATION - reads directly from memory.
   *
   * @param segment the MemorySegment containing the data
   * @param offset  the byte offset to start reading from
   * @param baseKey the reference key for delta calculation
   * @return the decoded key
   */
  public static long decodeDeltaFromSegment(MemorySegment segment, int offset, long baseKey) {
    long zigzag = readVarLongFromSegment(segment, offset);
    
    if (zigzag == NULL_MARKER) {
      return NULL_KEY;
    }
    
    // Undo the +1 shift and zigzag decode
    long delta = zigzagDecode(zigzag - 1);
    return baseKey + delta;
  }
  
  /**
   * Read unsigned varint from MemorySegment.
   * ZERO ALLOCATION - reads directly from memory.
   *
   * @param segment the MemorySegment containing the data
   * @param offset  the byte offset to start reading from
   * @return the decoded unsigned long value
   */
  public static long readVarLongFromSegment(MemorySegment segment, int offset) {
    byte b = segment.get(ValueLayout.JAVA_BYTE, offset);
    
    // Fast path: 1 byte (0-127) - most common case for small deltas
    if ((b & 0x80) == 0) {
      return b;
    }
    
    long result = b & 0x7F;
    b = segment.get(ValueLayout.JAVA_BYTE, offset + 1);
    
    // Fast path: 2 bytes (128-16383) - second most common
    if ((b & 0x80) == 0) {
      return result | ((long) b << 7);
    }
    
    // General case for larger values (rare for structural keys)
    result |= (long) (b & 0x7F) << 7;
    int pos = offset + 2;
    int shift = 14;
    do {
      b = segment.get(ValueLayout.JAVA_BYTE, pos++);
      result |= (long) (b & 0x7F) << shift;
      shift += 7;
      
      if (shift > 63) {
        throw new IllegalStateException("Varint too long (more than 10 bytes)");
      }
    } while ((b & 0x80) != 0);
    
    return result;
  }
  
  /**
   * Read signed varint (zigzag decoded) from MemorySegment.
   * ZERO ALLOCATION - reads directly from memory.
   *
   * @param segment the MemorySegment containing the data
   * @param offset  the byte offset to start reading from
   * @return the decoded signed integer value
   */
  public static int decodeSignedFromSegment(MemorySegment segment, int offset) {
    long zigzag = readVarLongFromSegment(segment, offset);
    return (int) zigzagDecode(zigzag);
  }
  
  /**
   * Read signed long (zigzag decoded) from MemorySegment.
   * ZERO ALLOCATION - reads directly from memory.
   *
   * @param segment the MemorySegment containing the data
   * @param offset  the byte offset to start reading from
   * @return the decoded signed long value
   */
  public static long decodeSignedLongFromSegment(MemorySegment segment, int offset) {
    long zigzag = readVarLongFromSegment(segment, offset);
    return zigzagDecode(zigzag);
  }
  
  /**
   * Get the byte length of a varint at the given offset.
   * Used for computing field offsets during flyweight cursor parsing.
   * ZERO ALLOCATION - reads directly from memory.
   *
   * @param segment the MemorySegment containing the data
   * @param offset  the byte offset to start reading from
   * @return the number of bytes this varint occupies
   */
  public static int varintLength(MemorySegment segment, int offset) {
    int len = 0;
    while ((segment.get(ValueLayout.JAVA_BYTE, offset + len) & 0x80) != 0) {
      len++;
    }
    return len + 1;
  }
  
  /**
   * Get length of a delta-encoded value (includes NULL check).
   * ZERO ALLOCATION - reads directly from memory.
   *
   * @param segment the MemorySegment containing the data
   * @param offset  the byte offset to start reading from
   * @return the number of bytes this delta-encoded value occupies
   */
  public static int deltaLength(MemorySegment segment, int offset) {
    byte first = segment.get(ValueLayout.JAVA_BYTE, offset);
    if (first == NULL_MARKER) {
      return 1;  // NULL is single byte
    }
    return varintLength(segment, offset);
  }
  
  /**
   * Read a fixed 8-byte long value from MemorySegment.
   * Used for hash values which are stored as fixed-length.
   * ZERO ALLOCATION - reads directly from memory.
   *
   * @param segment the MemorySegment containing the data
   * @param offset  the byte offset to start reading from
   * @return the 8-byte long value
   */
  public static long readLongFromSegment(MemorySegment segment, int offset) {
    return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
  }

  // ==================== DIRECT SEGMENT WRITE METHODS (for in-place mutation) ====================

  /**
   * Write a fixed 8-byte long value directly to a MemorySegment.
   * Used for hash values which are always fixed-width, enabling in-place mutation.
   * ZERO ALLOCATION.
   *
   * @param segment the MemorySegment to write to
   * @param offset  the byte offset to write at
   * @param value   the 8-byte long value
   */
  public static void writeLongToSegment(MemorySegment segment, long offset, long value) {
    segment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, value);
  }

  /**
   * Compute the encoded byte width of a delta-encoded key.
   * This is critical for in-place mutation: if the new width matches the old width,
   * we can overwrite in-place without re-serializing the record.
   *
   * @param key     the key to encode
   * @param baseKey the reference key for delta calculation
   * @return the number of bytes this delta would occupy
   */
  public static int computeDeltaEncodedWidth(long key, long baseKey) {
    if (key == NULL_KEY) {
      return 1;
    }
    long delta = key - baseKey;
    long zigzag = zigzagEncode(delta) + 1;
    return varLongSize(zigzag);
  }

  /**
   * Compute the encoded byte width of a signed integer (zigzag + varint).
   *
   * @param value the signed integer value
   * @return the number of bytes this value would occupy
   */
  public static int computeSignedEncodedWidth(int value) {
    long zigzag = zigzagEncode(value);
    return varLongSize(zigzag);
  }

  /**
   * Compute the encoded byte width of a signed long (zigzag + varint).
   *
   * @param value the signed long value
   * @return the number of bytes this value would occupy
   */
  public static int computeSignedLongEncodedWidth(long value) {
    long zigzag = zigzagEncode(value);
    return varLongSize(zigzag);
  }

  /**
   * Write a delta-encoded key directly to a MemorySegment at the given offset.
   * ZERO ALLOCATION - writes directly to page memory.
   *
   * @param segment the MemorySegment to write to
   * @param offset  the byte offset to start writing at
   * @param key     the key to encode (may be NULL_NODE_KEY)
   * @param baseKey the reference key for delta calculation
   * @return the number of bytes written
   */
  public static int writeDeltaToSegment(MemorySegment segment, long offset, long key, long baseKey) {
    if (key == NULL_KEY) {
      segment.set(ValueLayout.JAVA_BYTE, offset, (byte) NULL_MARKER);
      return 1;
    }

    long delta = key - baseKey;
    long zigzag = zigzagEncode(delta) + 1;
    return writeVarLongToSegment(segment, offset, zigzag);
  }

  /**
   * Write a signed integer (zigzag + varint) directly to a MemorySegment.
   * ZERO ALLOCATION.
   *
   * @param segment the MemorySegment to write to
   * @param offset  the byte offset to start writing at
   * @param value   the signed integer value
   * @return the number of bytes written
   */
  public static int writeSignedToSegment(MemorySegment segment, long offset, int value) {
    long zigzag = zigzagEncode(value);
    return writeVarLongToSegment(segment, offset, zigzag);
  }

  /**
   * Write a signed long (zigzag + varint) directly to a MemorySegment.
   * ZERO ALLOCATION.
   *
   * @param segment the MemorySegment to write to
   * @param offset  the byte offset to start writing at
   * @param value   the signed long value
   * @return the number of bytes written
   */
  public static int writeSignedLongToSegment(MemorySegment segment, long offset, long value) {
    long zigzag = zigzagEncode(value);
    return writeVarLongToSegment(segment, offset, zigzag);
  }

  /**
   * Write an unsigned varint directly to a MemorySegment.
   * ZERO ALLOCATION - optimized with fast paths for 1-2 byte values.
   *
   * @param segment the MemorySegment to write to
   * @param offset  the byte offset to start writing at
   * @param value   the unsigned long value
   * @return the number of bytes written
   */
  public static int writeVarLongToSegment(MemorySegment segment, long offset, long value) {
    // Fast path for 1-byte values (0-127)
    if ((value & ~0x7FL) == 0) {
      segment.set(ValueLayout.JAVA_BYTE, offset, (byte) value);
      return 1;
    }

    // Fast path for 2-byte values (128-16383)
    if ((value & ~0x3FFFL) == 0) {
      segment.set(ValueLayout.JAVA_BYTE, offset, (byte) ((value & 0x7F) | 0x80));
      segment.set(ValueLayout.JAVA_BYTE, offset + 1, (byte) (value >>> 7));
      return 2;
    }

    // General case
    int bytesWritten = 0;
    while ((value & ~0x7FL) != 0) {
      segment.set(ValueLayout.JAVA_BYTE, offset + bytesWritten, (byte) ((value & 0x7F) | 0x80));
      value >>>= 7;
      bytesWritten++;
    }
    segment.set(ValueLayout.JAVA_BYTE, offset + bytesWritten, (byte) value);
    return bytesWritten + 1;
  }

  /**
   * Read the encoded width (number of bytes) of a delta-encoded value
   * at the given offset in a MemorySegment.
   * Equivalent to {@link #deltaLength} but uses long offset for large segments.
   *
   * @param segment the MemorySegment
   * @param offset  the byte offset
   * @return the number of bytes this encoded value occupies
   */
  public static int readDeltaEncodedWidth(MemorySegment segment, long offset) {
    byte first = segment.get(ValueLayout.JAVA_BYTE, offset);
    if (first == NULL_MARKER) {
      return 1;
    }
    return readVarintWidth(segment, offset);
  }

  /**
   * Read the byte width of a varint at the given offset.
   * Scans continuation bits without decoding the value.
   *
   * @param segment the MemorySegment
   * @param offset  the byte offset
   * @return number of bytes the varint occupies
   */
  public static int readVarintWidth(MemorySegment segment, long offset) {
    int len = 0;
    while ((segment.get(ValueLayout.JAVA_BYTE, offset + len) & 0x80) != 0) {
      len++;
    }
    return len + 1;
  }

  /**
   * Read the byte width of a signed varint at the given offset.
   * Signed values use zigzag encoding, but the byte-level representation
   * is the same as unsigned varint.
   *
   * @param segment the MemorySegment
   * @param offset  the byte offset
   * @return number of bytes the signed varint occupies
   */
  public static int readSignedVarintWidth(MemorySegment segment, long offset) {
    return readVarintWidth(segment, offset);
  }

  // ==================== LONG-OFFSET SEGMENT DECODE METHODS ====================

  /**
   * Decode a delta-encoded key from MemorySegment using long offset.
   * ZERO ALLOCATION.
   *
   * @param segment the MemorySegment
   * @param offset  the byte offset (long for large pages)
   * @param baseKey the reference key for delta calculation
   * @return the decoded absolute key
   */
  public static long decodeDeltaFromSegment(MemorySegment segment, long offset, long baseKey) {
    long zigzag = readVarLongFromSegment(segment, offset);

    if (zigzag == NULL_MARKER) {
      return NULL_KEY;
    }

    long delta = zigzagDecode(zigzag - 1);
    return baseKey + delta;
  }

  /**
   * Read unsigned varint from MemorySegment using long offset.
   * ZERO ALLOCATION.
   *
   * @param segment the MemorySegment
   * @param offset  the byte offset (long)
   * @return the decoded unsigned long value
   */
  public static long readVarLongFromSegment(MemorySegment segment, long offset) {
    byte b = segment.get(ValueLayout.JAVA_BYTE, offset);

    if ((b & 0x80) == 0) {
      return b;
    }

    long result = b & 0x7F;
    b = segment.get(ValueLayout.JAVA_BYTE, offset + 1);

    if ((b & 0x80) == 0) {
      return result | ((long) b << 7);
    }

    result |= (long) (b & 0x7F) << 7;
    long pos = offset + 2;
    int shift = 14;
    do {
      b = segment.get(ValueLayout.JAVA_BYTE, pos++);
      result |= (long) (b & 0x7F) << shift;
      shift += 7;

      if (shift > 63) {
        throw new IllegalStateException("Varint too long (more than 10 bytes)");
      }
    } while ((b & 0x80) != 0);

    return result;
  }

  /**
   * Read signed integer (zigzag decoded) from MemorySegment using long offset.
   * ZERO ALLOCATION.
   *
   * @param segment the MemorySegment
   * @param offset  the byte offset (long)
   * @return the decoded signed integer value
   */
  public static int decodeSignedFromSegment(MemorySegment segment, long offset) {
    long zigzag = readVarLongFromSegment(segment, offset);
    return (int) zigzagDecode(zigzag);
  }

  /**
   * Read signed long (zigzag decoded) from MemorySegment using long offset.
   * ZERO ALLOCATION.
   *
   * @param segment the MemorySegment
   * @param offset  the byte offset (long)
   * @return the decoded signed long value
   */
  public static long decodeSignedLongFromSegment(MemorySegment segment, long offset) {
    long zigzag = readVarLongFromSegment(segment, offset);
    return zigzagDecode(zigzag);
  }
}
