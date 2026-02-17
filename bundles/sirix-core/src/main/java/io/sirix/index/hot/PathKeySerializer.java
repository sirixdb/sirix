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

/**
 * Serializer for PATH index keys (path node keys as primitive longs).
 *
 * <p>
 * Uses order-preserving encoding by XORing the sign bit, ensuring that signed long comparison
 * matches unsigned byte comparison:
 * </p>
 * 
 * <pre>
 * -1 &lt; 0 &lt; 1 in signed comparison
 * serialize(-1) &lt; serialize(0) &lt; serialize(1) in unsigned byte comparison
 * </pre>
 *
 * <h2>Zero Allocation</h2>
 * <p>
 * All methods use primitive types and caller-provided buffers. No boxing, no ByteBuffer allocation.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class PathKeySerializer implements HOTLongKeySerializer {

  /**
   * Sign-flip constant for order-preserving encoding. XORing with this flips the sign bit, making
   * unsigned byte comparison equivalent to signed long comparison.
   */
  private static final long SIGN_FLIP = 0x8000_0000_0000_0000L;

  /**
   * Singleton instance (stateless, thread-safe).
   */
  public static final PathKeySerializer INSTANCE = new PathKeySerializer();

  private PathKeySerializer() {
    // Singleton
  }

  @Override
  public int serialize(long pathNodeKey, byte[] dest, int offset) {
    // XOR sign bit for order-preserving encoding
    long signFlipped = pathNodeKey ^ SIGN_FLIP;

    // Direct byte writes (big-endian for lexicographic comparison)
    dest[offset] = (byte) (signFlipped >>> 56);
    dest[offset + 1] = (byte) (signFlipped >>> 48);
    dest[offset + 2] = (byte) (signFlipped >>> 40);
    dest[offset + 3] = (byte) (signFlipped >>> 32);
    dest[offset + 4] = (byte) (signFlipped >>> 24);
    dest[offset + 5] = (byte) (signFlipped >>> 16);
    dest[offset + 6] = (byte) (signFlipped >>> 8);
    dest[offset + 7] = (byte) signFlipped;

    return SERIALIZED_SIZE;
  }

  @Override
  public long deserialize(byte[] bytes, int offset, int length) {
    if (length != SERIALIZED_SIZE) {
      throw new IllegalArgumentException("Expected 8 bytes, got " + length);
    }

    // Read big-endian long
    long signFlipped = ((long) (bytes[offset] & 0xFF) << 56) | ((long) (bytes[offset + 1] & 0xFF) << 48)
        | ((long) (bytes[offset + 2] & 0xFF) << 40) | ((long) (bytes[offset + 3] & 0xFF) << 32)
        | ((long) (bytes[offset + 4] & 0xFF) << 24) | ((long) (bytes[offset + 5] & 0xFF) << 16)
        | ((long) (bytes[offset + 6] & 0xFF) << 8) | ((long) (bytes[offset + 7] & 0xFF));

    // XOR sign bit to restore original value
    return signFlipped ^ SIGN_FLIP;
  }

  @Override
  public int serializeTo(long key, MemorySegment dest, long offset) {
    // Direct MemorySegment write - sign-flipped, big-endian
    // Note: JAVA_LONG is native endian, so we use explicit big-endian layout
    long signFlipped = key ^ SIGN_FLIP;
    dest.set(ValueLayout.JAVA_LONG_UNALIGNED.withOrder(java.nio.ByteOrder.BIG_ENDIAN), offset, signFlipped);
    return SERIALIZED_SIZE;
  }

  /**
   * Deserializes directly from a MemorySegment.
   *
   * @param src the source MemorySegment
   * @param offset the offset to read from
   * @return the deserialized primitive long key
   */
  public long deserializeFrom(MemorySegment src, long offset) {
    long signFlipped = src.get(ValueLayout.JAVA_LONG_UNALIGNED.withOrder(java.nio.ByteOrder.BIG_ENDIAN), offset);
    return signFlipped ^ SIGN_FLIP;
  }
}

