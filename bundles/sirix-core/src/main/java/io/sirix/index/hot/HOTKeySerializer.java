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
 * Generic interface for serializing index keys to byte arrays. Used for object keys like
 * {@code CASValue} and {@code QNm}.
 *
 * <p>
 * For primitive long keys (e.g., PATH index), use {@link HOTLongKeySerializer} to avoid boxing
 * overhead.
 * </p>
 *
 * <h2>Order Preservation</h2>
 * <p>
 * Implementations MUST ensure that the byte representation preserves the natural ordering of keys:
 * </p>
 * 
 * <pre>
 * ∀ a, b: a.compareTo(b) &lt; 0  ⟺  compare(serialize(a), serialize(b)) &lt; 0
 * </pre>
 *
 * <h2>Zero Allocation</h2>
 * <p>
 * The {@code serialize} method writes to a caller-provided buffer to avoid allocations on the hot
 * path. Callers should use thread-local buffers.
 * </p>
 *
 * @param <K> the key type
 * @author Johannes Lichtenberger
 */
public interface HOTKeySerializer<K> {

  /**
   * Serializes the key into the destination buffer.
   *
   * <p>
   * This method is designed for zero-allocation operation on the hot path. The caller provides a
   * reusable buffer (typically from a ThreadLocal).
   * </p>
   *
   * @param key the key to serialize (must not be null)
   * @param dest the destination buffer
   * @param offset the offset in the destination buffer to start writing
   * @return the number of bytes written
   * @throws IllegalArgumentException if key is null or serializes to empty
   * @throws ArrayIndexOutOfBoundsException if dest is too small
   */
  int serialize(K key, byte[] dest, int offset);

  /**
   * Deserializes a key from bytes.
   *
   * <p>
   * This is less performance-critical than serialization, so allocation is acceptable.
   * </p>
   *
   * @param bytes the byte array containing the serialized key
   * @param offset the offset to start reading from
   * @param length the number of bytes to read
   * @return the deserialized key
   */
  K deserialize(byte[] bytes, int offset, int length);

  /**
   * Compares two serialized keys lexicographically (unsigned byte comparison).
   *
   * <p>
   * Default implementation uses {@link Arrays#compareUnsigned(byte[], int, int, byte[], int, int)}.
   * </p>
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
   * Default implementation uses the byte array method. Override for direct MemorySegment writes when
   * possible.
   * </p>
   *
   * @param key the key to serialize
   * @param dest the destination MemorySegment
   * @param offset the offset in the segment
   * @return the number of bytes written
   */
  default int serializeTo(K key, MemorySegment dest, long offset) {
    // Default: use byte array intermediary
    byte[] temp = new byte[256];
    int len = serialize(key, temp, 0);
    MemorySegment.copy(temp, 0, dest, ValueLayout.JAVA_BYTE, offset, len);
    return len;
  }
}

