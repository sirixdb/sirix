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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;

/**
 * Primitive-specialized interface for serializing long keys to byte arrays.
 *
 * <p>
 * This interface avoids boxing overhead by using primitive {@code long} parameters instead of
 * {@code Long} objects. Use this for PATH index keys and other long-valued keys.
 * </p>
 *
 * <h2>Order Preservation</h2>
 * <p>
 * Implementations MUST ensure signed long ordering is preserved in the unsigned byte
 * representation. This typically requires XORing the sign bit:
 * </p>
 * 
 * <pre>
 * long signFlipped = key ^ 0x8000000000000000L;
 * </pre>
 *
 * <h2>Zero Allocation</h2>
 * <p>
 * All methods use primitives and caller-provided buffers to ensure zero allocations on the hot
 * path.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public interface HOTLongKeySerializer {

  /**
   * The number of bytes required to serialize a long key.
   */
  int SERIALIZED_SIZE = 8;

  /**
   * Serializes the primitive long key into the destination buffer.
   *
   * <p>
   * This method is designed for zero-allocation operation on the hot path.
   * </p>
   *
   * @param key the primitive long key to serialize
   * @param dest the destination buffer (must have at least 8 bytes available)
   * @param offset the offset in the destination buffer to start writing
   * @return the number of bytes written (always 8 for long keys)
   * @throws ArrayIndexOutOfBoundsException if dest is too small
   */
  int serialize(long key, byte[] dest, int offset);

  /**
   * Deserializes a primitive long key from bytes.
   *
   * @param bytes the byte array containing the serialized key
   * @param offset the offset to start reading from
   * @param length the number of bytes to read (must be 8)
   * @return the deserialized primitive long key
   */
  long deserialize(byte[] bytes, int offset, int length);

  /**
   * Compares two serialized keys lexicographically (unsigned byte comparison).
   *
   * @param a first key bytes
   * @param aOff offset in first array
   * @param aLen length of first key
   * @param b second key bytes
   * @param bOff offset in second array
   * @param bLen length of second key
   * @return negative if a &lt; b, zero if equal, positive if a &gt; b
   */
  default int compare(byte[] a, int aOff, int aLen, byte[] b, int bOff, int bLen) {
    return Arrays.compareUnsigned(a, aOff, aOff + aLen, b, bOff, bOff + bLen);
  }

  /**
   * Serializes directly to a MemorySegment for maximum performance.
   *
   * <p>
   * This avoids the byte array intermediary by writing directly to off-heap memory.
   * </p>
   *
   * @param key the primitive long key to serialize
   * @param dest the destination MemorySegment
   * @param offset the offset in the segment
   * @return the number of bytes written (always 8)
   */
  default int serializeTo(long key, MemorySegment dest, long offset) {
    // Default: use byte array method
    byte[] temp = new byte[8];
    serialize(key, temp, 0);
    MemorySegment.copy(temp, 0, dest, ValueLayout.JAVA_BYTE, offset, 8);
    return 8;
  }
}

