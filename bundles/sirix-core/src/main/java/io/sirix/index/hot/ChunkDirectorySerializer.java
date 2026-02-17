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

package io.sirix.index.hot;

import io.sirix.page.PageReference;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Serializer for {@link ChunkDirectory}.
 *
 * <p>
 * <b>Binary Format:</b>
 * </p>
 * 
 * <pre>
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │ [4 bytes] chunkCount                                                    │
 * ├─────────────────────────────────────────────────────────────────────────┤
 * │ For each chunk (repeated chunkCount times):                             │
 * │   [4 bytes] chunkIndex                                                  │
 * │   [8 bytes] pageKey                                                     │
 * │   [4 bytes] pageFragmentCount                                           │
 * │   [N bytes] pageFragments (if count > 0)                                │
 * └─────────────────────────────────────────────────────────────────────────┘
 * </pre>
 *
 * @author Johannes Lichtenberger
 */
public final class ChunkDirectorySerializer {

  /** Header size: chunkCount (4 bytes). */
  private static final int HEADER_SIZE = 4;

  /** Entry size without fragments: chunkIndex (4) + pageKey (8) + fragmentCount (4). */
  private static final int ENTRY_BASE_SIZE = 4 + 8 + 4;

  private ChunkDirectorySerializer() {
    // Static utility class
  }

  /**
   * Calculate the serialized size of a ChunkDirectory.
   *
   * @param dir the directory
   * @return the serialized size in bytes
   */
  public static int serializedSize(@NonNull ChunkDirectory dir) {
    Objects.requireNonNull(dir, "dir must not be null");

    int size = HEADER_SIZE;
    for (int i = 0; i < dir.chunkCount(); i++) {
      PageReference ref = dir.getChunkRefAtPosition(i);
      size += ENTRY_BASE_SIZE;
      if (ref != null) {
        size += ref.getPageFragments().size() * 16; // Each fragment: revision(4) + key(8) + dbId(2) + resId(2)
      }
    }
    return size;
  }

  /**
   * Serialize a ChunkDirectory to a byte array.
   *
   * @param dir the directory
   * @param dest the destination buffer
   * @param offset the offset to write at
   * @return the number of bytes written
   */
  public static int serialize(@NonNull ChunkDirectory dir, byte[] dest, int offset) {
    Objects.requireNonNull(dir, "dir must not be null");
    Objects.requireNonNull(dest, "dest must not be null");

    ByteBuffer buffer = ByteBuffer.wrap(dest, offset, dest.length - offset);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    return serializeToBuffer(dir, buffer);
  }

  /**
   * Serialize a ChunkDirectory to a ByteBuffer.
   *
   * @param dir the directory
   * @param buffer the buffer
   * @return the number of bytes written
   */
  public static int serializeToBuffer(@NonNull ChunkDirectory dir, @NonNull ByteBuffer buffer) {
    Objects.requireNonNull(dir, "dir must not be null");
    Objects.requireNonNull(buffer, "buffer must not be null");

    int startPos = buffer.position();

    // Write chunk count
    buffer.putInt(dir.chunkCount());

    // Write each chunk entry
    for (int i = 0; i < dir.chunkCount(); i++) {
      int chunkIndex = dir.getChunkIndex(i);
      PageReference ref = dir.getChunkRefAtPosition(i);

      buffer.putInt(chunkIndex);
      buffer.putLong(ref != null
          ? ref.getKey()
          : -1);

      // Write page fragments
      List<io.sirix.page.interfaces.PageFragmentKey> fragments = ref != null
          ? ref.getPageFragments()
          : List.of();
      buffer.putInt(fragments.size());
      for (io.sirix.page.interfaces.PageFragmentKey fragment : fragments) {
        buffer.putInt(fragment.revision());
        buffer.putLong(fragment.key());
        buffer.putShort((short) fragment.databaseId());
        buffer.putShort((short) fragment.resourceId());
      }
    }

    return buffer.position() - startPos;
  }

  /**
   * Deserialize a ChunkDirectory from a byte array.
   *
   * @param bytes the source bytes
   * @param offset the offset to read from
   * @param length the number of bytes to read
   * @return the deserialized ChunkDirectory
   */
  public static ChunkDirectory deserialize(byte[] bytes, int offset, int length) {
    Objects.requireNonNull(bytes, "bytes must not be null");
    if (length < HEADER_SIZE) {
      throw new IllegalArgumentException("Not enough bytes for ChunkDirectory header");
    }

    ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, length);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    return deserializeFromBuffer(buffer);
  }

  /**
   * Deserialize a ChunkDirectory from a ByteBuffer.
   *
   * @param buffer the buffer
   * @return the deserialized ChunkDirectory
   */
  public static ChunkDirectory deserializeFromBuffer(@NonNull ByteBuffer buffer) {
    Objects.requireNonNull(buffer, "buffer must not be null");

    int chunkCount = buffer.getInt();
    if (chunkCount < 0) {
      throw new IllegalArgumentException("Invalid chunkCount: " + chunkCount);
    }

    if (chunkCount == 0) {
      return new ChunkDirectory();
    }

    int[] chunkIndices = new int[chunkCount];
    PageReference[] chunkRefs = new PageReference[chunkCount];

    for (int i = 0; i < chunkCount; i++) {
      int chunkIndex = buffer.getInt();
      long pageKey = buffer.getLong();
      int fragmentCount = buffer.getInt();

      chunkIndices[i] = chunkIndex;

      PageReference ref = new PageReference();
      ref.setKey(pageKey);

      if (fragmentCount > 0) {
        List<io.sirix.page.interfaces.PageFragmentKey> fragments = new ArrayList<>(fragmentCount);
        for (int j = 0; j < fragmentCount; j++) {
          int revision = buffer.getInt();
          long key = buffer.getLong();
          int dbId = buffer.getShort() & 0xFFFF;
          int resId = buffer.getShort() & 0xFFFF;
          fragments.add(new io.sirix.page.PageFragmentKeyImpl(revision, key, dbId, resId));
        }
        ref.setPageFragments(fragments);
      }

      chunkRefs[i] = ref;
    }

    return new ChunkDirectory(chunkCount, chunkIndices, chunkRefs);
  }

  /**
   * Check if a byte array contains a valid ChunkDirectory tombstone marker. A tombstone is
   * represented by an empty ChunkDirectory (chunkCount = 0).
   *
   * @param bytes the bytes
   * @param offset the offset
   * @param length the length
   * @return true if this is a tombstone
   */
  public static boolean isTombstone(byte[] bytes, int offset, int length) {
    if (length < HEADER_SIZE) {
      return false;
    }
    ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, length);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    int chunkCount = buffer.getInt();
    return chunkCount == 0;
  }
}

