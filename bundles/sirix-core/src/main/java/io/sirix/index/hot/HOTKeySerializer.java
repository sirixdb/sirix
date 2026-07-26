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

  /**
   * Number of bytes used to encode the chunk index trailer of a chunked composite key.
   * Big-endian 32-bit unsigned, so {@code chunkIdx} can range over all positive ints.
   * Sized to fit {@code nodeKey >>> 16} for any 64-bit nodeKey up to ~2^48 — comfortably above
   * any practical Sirix dataset.
   */
  int CHUNK_IDX_BYTES = 4;

  /**
   * Append a 4-byte big-endian {@code chunkIdx} trailer to a serialised key. The {@code key} is
   * first serialised at {@code offset}; the chunkIdx bytes follow immediately after. Callers
   * MUST size {@code dest} to at least {@code prefixLen + CHUNK_IDX_BYTES} starting from
   * {@code offset}.
   *
   * <p>Big-endian byte order keeps adjacent chunkIdx values adjacent in lex order, so a
   * {@code (prefix, chunkIdx)} composite preserves the sortedness needed by
   * {@code HOTRangeCursor}'s navigate-then-walk-forward primitive when ranging over all chunks
   * of one logical key (PROJECTION pattern).</p>
   *
   * @param key       the logical key (e.g. a {@code QNm} for NAME, a {@code CASValue} for CAS)
   * @param chunkIdx  the chunk index — typically {@code (int) (nodeKey >>> 16)} so each chunk
   *                  covers one Roaring 16-bit container
   * @param dest      destination buffer
   * @param offset    offset in {@code dest}
   * @return total number of bytes written ({@code prefixLen + CHUNK_IDX_BYTES})
   */
  default int serializeWithChunkIdx(K key, int chunkIdx, byte[] dest, int offset) {
    final int prefixLen = serialize(key, dest, offset);
    writeChunkIdxBE(dest, offset + prefixLen, chunkIdx);
    return prefixLen + CHUNK_IDX_BYTES;
  }

  /**
   * Same as {@link #serialize(Object, byte[], int)} but explicitly named to advertise
   * "this is the prefix of a chunked composite key." The default delegates to {@code serialize}
   * because the chunked composite is just {@code (prefix ‖ chunkIdx_be4)}.
   *
   * @return the prefix length (everything before the chunkIdx trailer)
   */
  default int serializePrefix(K key, byte[] dest, int offset) {
    return serialize(key, dest, offset);
  }

  /**
   * Read the trailing 4-byte big-endian {@code chunkIdx} from a chunked composite key.
   *
   * @param keyBytes  the full composite key bytes
   * @param keyOffset offset in {@code keyBytes} where the composite key starts
   * @param keyLen    full length of the composite key (prefix + {@link #CHUNK_IDX_BYTES})
   * @return the chunkIdx
   */
  static int readChunkIdx(byte[] keyBytes, int keyOffset, int keyLen) {
    final int chunkIdxStart = keyOffset + keyLen - CHUNK_IDX_BYTES;
    return ((keyBytes[chunkIdxStart] & 0xFF) << 24)
        | ((keyBytes[chunkIdxStart + 1] & 0xFF) << 16)
        | ((keyBytes[chunkIdxStart + 2] & 0xFF) << 8)
        | (keyBytes[chunkIdxStart + 3] & 0xFF);
  }

  /**
   * Write a 4-byte big-endian {@code chunkIdx} into the destination buffer at the given offset.
   */
  static void writeChunkIdxBE(byte[] dest, int offset, int chunkIdx) {
    dest[offset]     = (byte) (chunkIdx >>> 24);
    dest[offset + 1] = (byte) (chunkIdx >>> 16);
    dest[offset + 2] = (byte) (chunkIdx >>> 8);
    dest[offset + 3] = (byte) chunkIdx;
  }
}

