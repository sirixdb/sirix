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

import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

/**
 * Serializer for {@link NodeReferences} (node key bitmaps).
 *
 * <p>
 * Uses a hybrid format optimized for both small and large sets:
 * </p>
 * <ul>
 * <li><b>Small sets (&lt; 64 entries):</b> Packed format - more compact, lower overhead</li>
 * <li><b>Large sets:</b> Roaring64Bitmap native serialization - compressed, efficient</li>
 * </ul>
 *
 * <h2>Format</h2>
 * 
 * <pre>
 * Packed format:  [0x00][count:1][nodeKey0:8][nodeKey1:8]...[nodeKeyN:8]
 * Roaring format: [0xFF][roaring bitmap bytes...]
 * Tombstone:      [0xFE] (empty bitmap, marks deletion)
 * </pre>
 *
 * @author Johannes Lichtenberger
 */
public final class NodeReferencesSerializer {

  /**
   * Format marker for packed representation.
   */
  private static final byte PACKED_FORMAT = 0x00;

  /**
   * Format marker for Roaring bitmap representation.
   */
  private static final byte ROARING_FORMAT = (byte) 0xFF;

  /**
   * Format marker for tombstone (deleted entry).
   */
  private static final byte TOMBSTONE_FORMAT = (byte) 0xFE;

  /**
   * Threshold for switching from packed to Roaring format.
   *
   * <p>Packed format stores each node key as 8 raw bytes: total = 2 + count*8.
   * At 64 entries, packed = 514 bytes while Roaring typically compresses to 200-400 bytes.
   * Below this threshold packed is more compact; above it Roaring wins.</p>
   */
  private static final int PACKED_THRESHOLD = 64;

  private NodeReferencesSerializer() {
    // Utility class
  }

  /**
   * Serializes NodeReferences to bytes.
   *
   * @param refs the node references to serialize
   * @return serialized bytes
   */
  public static byte[] serialize(NodeReferences refs) {
    requireNonNull(refs, "refs cannot be null");
    Roaring64Bitmap bitmap = refs.getNodeKeys();

    // Tombstone check
    if (bitmap.isEmpty()) {
      return new byte[] {TOMBSTONE_FORMAT};
    }

    long cardinality = bitmap.getLongCardinality();

    if (cardinality <= PACKED_THRESHOLD) {
      return serializePacked(bitmap, (int) cardinality);
    } else {
      return serializeRoaring(bitmap);
    }
  }

  /**
   * Serializes to a caller-provided buffer, returning bytes written.
   *
   * @param refs the node references to serialize
   * @param dest destination buffer
   * @param offset offset to write at
   * @return number of bytes written
   */
  public static int serialize(NodeReferences refs, byte[] dest, int offset) {
    requireNonNull(refs, "refs cannot be null");
    Roaring64Bitmap bitmap = refs.getNodeKeys();

    // Tombstone check
    if (bitmap.isEmpty()) {
      dest[offset] = TOMBSTONE_FORMAT;
      return 1;
    }

    long cardinality = bitmap.getLongCardinality();

    if (cardinality <= PACKED_THRESHOLD) {
      return serializePacked(bitmap, (int) cardinality, dest, offset);
    } else {
      return serializeRoaring(bitmap, dest, offset);
    }
  }

  /**
   * Deserializes NodeReferences from bytes.
   *
   * @param bytes the serialized bytes
   * @return the deserialized NodeReferences
   */
  public static NodeReferences deserialize(byte[] bytes) {
    return deserialize(bytes, 0, bytes.length);
  }

  /**
   * Deserializes NodeReferences from a byte range.
   *
   * @param bytes the byte array
   * @param offset offset to start reading
   * @param length number of bytes to read
   * @return the deserialized NodeReferences
   */
  public static NodeReferences deserialize(byte[] bytes, int offset, int length) {
    requireNonNull(bytes, "bytes cannot be null");
    if (offset < 0 || length < 0) {
      throw new IllegalArgumentException(
          "offset and length must be non-negative: offset=" + offset + ", length=" + length);
    }
    if (offset + length > bytes.length) {
      throw new IllegalArgumentException(
          "offset + length (" + (offset + length) + ") exceeds array length (" + bytes.length + ")");
    }

    if (length == 0) {
      return new NodeReferences();
    }

    byte format = bytes[offset];

    if (format == TOMBSTONE_FORMAT) {
      // Tombstone - return empty references
      return new NodeReferences();
    } else if (format == PACKED_FORMAT) {
      return deserializePacked(bytes, offset + 1, length - 1);
    } else if (format == ROARING_FORMAT) {
      return deserializeRoaring(bytes, offset + 1, length - 1);
    } else {
      throw new IllegalArgumentException("Unknown NodeReferences format: " + format);
    }
  }

  /**
   * Checks if the serialized data represents a tombstone (deletion).
   *
   * @param bytes the serialized bytes
   * @param offset offset to check
   * @param length length of data
   * @return true if tombstone
   */
  public static boolean isTombstone(byte[] bytes, int offset, int length) {
    return length == 1 && bytes[offset] == TOMBSTONE_FORMAT;
  }

  /**
   * Merges two NodeReferences (OR operation on bitmaps).
   *
   * <p><b>WARNING: This method mutates {@code a} in-place.</b> The bitmap of {@code a} is
   * modified by OR-ing in the entries from {@code b}. If you need both originals unchanged,
   * clone {@code a} before calling this method.</p>
   *
   * @param a the references to merge INTO (modified in-place)
   * @param b the references to merge from (not modified)
   * @return {@code a} after modification
   */
  public static NodeReferences merge(NodeReferences a, NodeReferences b) {
    a.getNodeKeys().or(b.getNodeKeys());
    return a;
  }

  // ==================== Private Methods ====================

  private static byte[] serializePacked(Roaring64Bitmap bitmap, int count) {
    // Format: [PACKED_FORMAT:1][count:1][nodeKey0:8]...[nodeKeyN:8]
    byte[] buf = new byte[2 + count * 8];
    serializePacked(bitmap, count, buf, 0);
    return buf;
  }

  private static int serializePacked(Roaring64Bitmap bitmap, int count, byte[] dest, int offset) {
    int start = offset;
    dest[offset++] = PACKED_FORMAT;
    dest[offset++] = (byte) count;

    LongIterator iter = bitmap.getLongIterator();
    while (iter.hasNext()) {
      long key = iter.next();
      // Write big-endian long
      dest[offset++] = (byte) (key >>> 56);
      dest[offset++] = (byte) (key >>> 48);
      dest[offset++] = (byte) (key >>> 40);
      dest[offset++] = (byte) (key >>> 32);
      dest[offset++] = (byte) (key >>> 24);
      dest[offset++] = (byte) (key >>> 16);
      dest[offset++] = (byte) (key >>> 8);
      dest[offset++] = (byte) key;
    }

    return offset - start;
  }

  private static byte[] serializeRoaring(Roaring64Bitmap bitmap) {
    final int size = (int) bitmap.serializedSizeInBytes();
    final byte[] buf = new byte[1 + size];
    buf[0] = ROARING_FORMAT;
    try {
      bitmap.serialize(ByteBuffer.wrap(buf, 1, size));
    } catch (java.io.IOException e) {
      throw new IllegalStateException("Unexpected I/O error during in-memory Roaring64Bitmap serialization", e);
    }
    return buf;
  }

  private static int serializeRoaring(Roaring64Bitmap bitmap, byte[] dest, int offset) {
    dest[offset] = ROARING_FORMAT;
    final int size = (int) bitmap.serializedSizeInBytes();
    try {
      bitmap.serialize(ByteBuffer.wrap(dest, offset + 1, size));
    } catch (java.io.IOException e) {
      throw new IllegalStateException("Unexpected I/O error during in-memory Roaring64Bitmap serialization", e);
    }
    return 1 + size;
  }

  private static NodeReferences deserializePacked(byte[] bytes, int offset, int length) {
    if (length < 1) {
      return new NodeReferences();
    }

    final int count = bytes[offset] & 0xFF;

    // Validate that count entries fit within the available bytes
    final int requiredBytes = 1 + count * 8;
    if (requiredBytes > length) {
      throw new IllegalArgumentException(
          "Packed count " + count + " requires " + requiredBytes
              + " bytes but only " + length + " available");
    }

    final Roaring64Bitmap bitmap = new Roaring64Bitmap();

    int pos = offset + 1;
    for (int i = 0; i < count; i++) {
      final long key = ((long) (bytes[pos] & 0xFF) << 56) | ((long) (bytes[pos + 1] & 0xFF) << 48)
          | ((long) (bytes[pos + 2] & 0xFF) << 40) | ((long) (bytes[pos + 3] & 0xFF) << 32)
          | ((long) (bytes[pos + 4] & 0xFF) << 24) | ((long) (bytes[pos + 5] & 0xFF) << 16)
          | ((long) (bytes[pos + 6] & 0xFF) << 8) | ((long) (bytes[pos + 7] & 0xFF));
      bitmap.add(key);
      pos += 8;
    }

    return new NodeReferences(bitmap);
  }

  private static NodeReferences deserializeRoaring(byte[] bytes, int offset, int length) {
    final Roaring64Bitmap bitmap = new Roaring64Bitmap();
    try {
      bitmap.deserialize(ByteBuffer.wrap(bytes, offset, length));
    } catch (java.io.IOException e) {
      throw new IllegalStateException("Unexpected I/O error during in-memory Roaring64Bitmap deserialization", e);
    }
    return new NodeReferences(bitmap);
  }
}

