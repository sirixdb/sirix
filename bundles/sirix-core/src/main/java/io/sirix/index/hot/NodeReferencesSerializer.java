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
import io.sirix.page.HOTLeafPage;
import org.jspecify.annotations.Nullable;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.util.Arrays;

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
   * <p>
   * Packed format stores each node key as 8 raw bytes: total = 2 + count*8. At 64 entries, packed =
   * 514 bytes while Roaring typically compresses to 200-400 bytes. Below this threshold packed is
   * more compact; above it Roaring wins.
   * </p>
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
   * Serializes an already-sorted, duplicate-free run of node keys, without materialising a
   * {@link NodeReferences} or (below {@link #PACKED_THRESHOLD}) a {@link Roaring64Bitmap}.
   *
   * <p>
   * For the bulk index build this is the difference between a handful of objects per indexed value
   * and none: the builder already has the run sorted ascending in a reusable array, which is exactly
   * what the packed format wants, so the bitmap round-trip {@code add-all → iterate → write} buys
   * nothing. The output is byte-identical to {@link #serialize(NodeReferences)} over a bitmap holding
   * the same keys.
   * </p>
   *
   * @param nodeKeys the backing array
   * @param from index of the first key, inclusive
   * @param to index one past the last key, exclusive
   * @param scratch a bitmap the method may clear and reuse for runs above the packed threshold; never
   *        retained
   * @return the serialized payload
   * @throws IllegalArgumentException if the run is empty
   */
  public static byte[] serializeAscendingRun(final long[] nodeKeys, final int from, final int to,
      final Roaring64Bitmap scratch) {
    requireNonNull(nodeKeys, "nodeKeys cannot be null");
    requireNonNull(scratch, "scratch cannot be null");
    final int count = to - from;
    if (count <= 0) {
      throw new IllegalArgumentException("run must be non-empty: from=" + from + ", to=" + to);
    }

    if (count <= PACKED_THRESHOLD) {
      final byte[] buf = new byte[2 + count * 8];
      buf[0] = PACKED_FORMAT;
      buf[1] = (byte) count;
      int pos = 2;
      for (int i = from; i < to; i++) {
        final long key = nodeKeys[i];
        buf[pos] = (byte) (key >>> 56);
        buf[pos + 1] = (byte) (key >>> 48);
        buf[pos + 2] = (byte) (key >>> 40);
        buf[pos + 3] = (byte) (key >>> 32);
        buf[pos + 4] = (byte) (key >>> 24);
        buf[pos + 5] = (byte) (key >>> 16);
        buf[pos + 6] = (byte) (key >>> 8);
        buf[pos + 7] = (byte) key;
        pos += 8;
      }
      return buf;
    }

    scratch.clear();
    for (int i = from; i < to; i++) {
      scratch.add(nodeKeys[i]);
    }
    return serializeRoaring(scratch);
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
   * Computes the exact number of bytes needed to serialize the given NodeReferences, without actually
   * writing any data.
   *
   * @param refs the node references
   * @return number of bytes needed
   */
  public static int computeSerializedSize(NodeReferences refs) {
    requireNonNull(refs, "refs cannot be null");
    final Roaring64Bitmap bitmap = refs.getNodeKeys();
    if (bitmap.isEmpty()) {
      return 1; // tombstone
    }
    final long cardinality = bitmap.getLongCardinality();
    if (cardinality <= PACKED_THRESHOLD) {
      return 2 + (int) cardinality * 8; // format + count + keys
    }
    return 1 + (int) bitmap.serializedSizeInBytes(); // format + roaring bytes
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
   * Accumulates a lookup's chunk payloads into the cheapest sufficient representation: a sorted
   * {@code long[]} while the result stays small (the average CAS posting list holds one or two node
   * keys), spilling to a {@link Roaring64Bitmap} past {@link #COMPACT_LIMIT} or when a Roaring-format
   * chunk appears. The emitted {@link NodeReferences#ofSortedArray} result costs one right-sized
   * array + one wrapper instead of a bitmap container tree per lookup — the single largest allocation
   * the read path had left.
   *
   * <p>
   * Sortedness precondition: chunks must be appended in ascending composite-key order (the chunk
   * walk's natural order — chunkIdx-major, bit16-minor, duplicate-free), which makes every append
   * strictly ascending. Not thread-safe; pool per reader or per iterator.
   */
  public static final class ChunkAccumulator {

    private static final int COMPACT_LIMIT = 512;

    private long[] keys = new long[8];
    private int count;
    private @Nullable Roaring64Bitmap bitmap;

    /** Drop all accumulated state (also called implicitly by {@link #toNodeReferencesAndReset}). */
    public void reset() {
      count = 0;
      bitmap = null;
    }

    private void add(final long key) {
      Roaring64Bitmap spilled = bitmap;
      if (spilled != null) {
        spilled.add(key);
        return;
      }
      if (count == keys.length) {
        if (count >= COMPACT_LIMIT) {
          spilled = new Roaring64Bitmap();
          for (int i = 0; i < count; i++) {
            spilled.add(keys[i]);
          }
          bitmap = spilled;
          count = 0;
          spilled.add(key);
          return;
        }
        keys = Arrays.copyOf(keys, count * 2);
      }
      keys[count++] = key;
    }

    /**
     * Append one chunk payload, expanding each stored bit16 to {@code high | bit16}. Same format
     * handling as {@link #mergeChunkInto}: packed reads straight off slot memory, tombstones and empty
     * payloads are skipped, the (rare) Roaring format round-trips through a heap array.
     *
     * @param leaf the leaf page holding the chunk slot
     * @param ref the slot's packed value handle from {@link HOTLeafPage#valueRef(int)}
     * @param high the pre-shifted chunk base ({@code chunkIdx << 16}, chunkIdx treated unsigned)
     */
    public void addChunk(final HOTLeafPage leaf, final long ref, final long high) {
      final int length = HOTLeafPage.refLength(ref);
      if (length <= 0) {
        return;
      }
      final byte format = leaf.refByteAt(ref, 0);
      if (format == TOMBSTONE_FORMAT) {
        return;
      }
      if (format == PACKED_FORMAT) {
        if (length < 2) {
          return;
        }
        final int chunkCount = leaf.refByteAt(ref, 1) & 0xFF;
        if (chunkCount == 0 || 2 + chunkCount * 8 > length) {
          return;
        }
        for (int i = 0; i < chunkCount; i++) {
          // Stored big-endian; refLongAt reads little-endian off the segment.
          final long bit16 = Long.reverseBytes(leaf.refLongAt(ref, 2 + i * 8)) & 0xFFFFL;
          add(high | bit16);
        }
        return;
      }
      if (format == ROARING_FORMAT) {
        final byte[] bytes = new byte[length - 1];
        leaf.copyRefInto(ref, 1, bytes, 0, length - 1);
        final Roaring64Bitmap chunkBitmap = new Roaring64Bitmap();
        try {
          chunkBitmap.deserialize(ByteBuffer.wrap(bytes));
        } catch (IOException e) {
          throw new IllegalStateException("Unexpected I/O error during in-memory Roaring64Bitmap deserialization", e);
        }
        final LongIterator it = chunkBitmap.getLongIterator();
        while (it.hasNext()) {
          add(high | (it.next() & 0xFFFFL));
        }
        return;
      }
      throw new IllegalArgumentException("Unknown NodeReferences format: " + format);
    }

    /**
     * Emit the accumulated result — compact when it stayed small, bitmap-backed when it spilled — and
     * reset for reuse. {@code null} when nothing live was accumulated.
     */
    public @Nullable NodeReferences toNodeReferencesAndReset() {
      final Roaring64Bitmap spilled = bitmap;
      if (spilled != null) {
        reset();
        return spilled.isEmpty()
            ? null
            : NodeReferences.owning(spilled);
      }
      final int resultCount = count;
      if (resultCount == 0) {
        return null;
      }
      count = 0;
      return NodeReferences.ofSortedArray(Arrays.copyOf(keys, resultCount), resultCount);
    }
  }

  /**
   * Identity sentinel returned by {@link #mergePackedSingleBitFromSlot} when the new bit is already
   * present in the slot's packed set — the caller skips the slot rewrite entirely. (The array-based
   * {@link #mergePackedSingleBit} signals the same case by returning its {@code existing} argument;
   * with the payload still in slot memory there is no such array to return.)
   */
  public static final byte[] MERGE_UNCHANGED = new byte[0];

  /**
   * {@link #mergePackedSingleBit} against a payload still resident in its leaf slot: binary-search
   * and splice directly off slot memory via the leaf's value-ref accessors. This is the insert-time
   * hot path — every listener-driven index insert merges a single-bit payload into its chunk bucket,
   * and the copying variant first materialized the whole existing bucket (up to {@code 2 + 64*8}
   * bytes) just to read it.
   *
   * @param leaf the leaf holding the existing payload
   * @param ref the slot's packed value handle from {@link HOTLeafPage#valueRef(int)}
   * @param newValue buffer holding the new single-entry packed payload
   * @param newOffset offset of the payload in {@code newValue}
   * @param newLen length of the payload
   * @return {@link #MERGE_UNCHANGED} when the bit is already present; a freshly built merged payload
   *         otherwise; {@code null} when the shapes don't qualify (caller falls back to the copying
   *         slow path)
   */
  public static byte @Nullable [] mergePackedSingleBitFromSlot(final HOTLeafPage leaf, final long ref,
      final byte[] newValue, final int newOffset, final int newLen) {
    // New value must be a single-entry packed payload: [PACKED][count=1][key:8] == 10 bytes.
    if (newLen != 2 + 8 || newValue[newOffset] != PACKED_FORMAT || newValue[newOffset + 1] != 1) {
      return null;
    }
    final int existingLen = HOTLeafPage.refLength(ref);
    if (existingLen < 2 || leaf.refByteAt(ref, 0) != PACKED_FORMAT) {
      return null;
    }
    final int count = leaf.refByteAt(ref, 1) & 0xFF;
    // A full-or-overflowing bucket would switch representation; leave that to the slow path.
    if (count >= PACKED_THRESHOLD || existingLen != 2 + count * 8) {
      return null;
    }

    final long newKey = readKeyBE(newValue, newOffset + 2);

    // Binary search the sorted-ascending packed keys for newKey (stored BE; refLongAt reads LE).
    int lo = 0;
    int hi = count; // exclusive
    while (lo < hi) {
      final int mid = (lo + hi) >>> 1;
      final int cmp = Long.compareUnsigned(Long.reverseBytes(leaf.refLongAt(ref, 2 + mid * 8)), newKey);
      if (cmp < 0) {
        lo = mid + 1;
      } else if (cmp > 0) {
        hi = mid;
      } else {
        return MERGE_UNCHANGED; // already present — merged set unchanged
      }
    }

    // Insert newKey at position lo, preserving ascending order — one allocation, two slot copies.
    final byte[] merged = new byte[2 + (count + 1) * 8];
    merged[0] = PACKED_FORMAT;
    merged[1] = (byte) (count + 1);
    final int insAt = 2 + lo * 8;
    if (lo > 0) {
      leaf.copyRefInto(ref, 2, merged, 2, lo * 8);
    }
    writeKeyBE(merged, insAt, newKey);
    if (lo < count) {
      leaf.copyRefInto(ref, insAt, merged, insAt + 8, (count - lo) * 8);
    }
    return merged;
  }

  /**
   * {@link #isTombstone(byte[], int, int)} against a payload still resident in its leaf slot.
   *
   * @param leaf the leaf holding the payload
   * @param ref the slot's packed value handle from {@link HOTLeafPage#valueRef(int)}
   * @return {@code true} iff the payload is the single-byte tombstone marker
   */
  public static boolean isTombstone(final HOTLeafPage leaf, final long ref) {
    return HOTLeafPage.refLength(ref) == 1 && leaf.refByteAt(ref, 0) == TOMBSTONE_FORMAT;
  }

  /**
   * Merge one serialized chunk bitmap — still resident in its leaf's off-heap slot memory — into
   * {@code target}, expanding each stored bit16 to {@code high | bit16}. The read path's chunk
   * reassembly ran {@code getValue → deserialize → NodeReferences → Roaring64Bitmap → iterate} per
   * chunk before this: two heap copies and two container allocations to move at most 64 longs. This
   * reads the packed format directly off the slot via the leaf's zero-alloc value-ref accessors; only
   * the (rare, > {@link #PACKED_THRESHOLD}-cardinality) Roaring format still round-trips through a
   * heap array.
   *
   * @param leaf the leaf page holding the chunk slot
   * @param ref the slot's packed value handle from {@link HOTLeafPage#valueRef(int)}
   * @param high the pre-shifted chunk base ({@code chunkIdx << 16}, chunkIdx treated unsigned)
   * @param target the accumulator, or {@code null} if none exists yet
   * @return the accumulator — {@code target} if given, freshly created on the first merged bit, or
   *         unchanged/{@code null} for a tombstone or empty chunk
   */
  public static @Nullable Roaring64Bitmap mergeChunkInto(final HOTLeafPage leaf, final long ref, final long high,
      @Nullable Roaring64Bitmap target) {
    final int length = HOTLeafPage.refLength(ref);
    if (length <= 0) {
      return target;
    }
    final byte format = leaf.refByteAt(ref, 0);
    if (format == TOMBSTONE_FORMAT) {
      return target;
    }
    if (format == PACKED_FORMAT) {
      if (length < 2) {
        return target;
      }
      final int count = leaf.refByteAt(ref, 1) & 0xFF;
      if (count == 0 || 2 + count * 8 > length) {
        return target;
      }
      if (target == null) {
        target = new Roaring64Bitmap();
      }
      for (int i = 0; i < count; i++) {
        // Stored big-endian; refLongAt reads little-endian off the segment.
        final long bit16 = Long.reverseBytes(leaf.refLongAt(ref, 2 + i * 8)) & 0xFFFFL;
        target.add(high | bit16);
      }
      return target;
    }
    if (format == ROARING_FORMAT) {
      final byte[] bytes = new byte[length - 1];
      leaf.copyRefInto(ref, 1, bytes, 0, length - 1);
      final Roaring64Bitmap chunkBitmap = new Roaring64Bitmap();
      try {
        chunkBitmap.deserialize(ByteBuffer.wrap(bytes));
      } catch (IOException e) {
        throw new IllegalStateException("Unexpected I/O error during in-memory Roaring64Bitmap deserialization", e);
      }
      if (chunkBitmap.isEmpty()) {
        return target;
      }
      if (target == null) {
        target = new Roaring64Bitmap();
      }
      final LongIterator it = chunkBitmap.getLongIterator();
      while (it.hasNext()) {
        target.add(high | (it.next() & 0xFFFFL));
      }
      return target;
    }
    throw new IllegalArgumentException("Unknown NodeReferences format: " + format);
  }

  /**
   * {@link #isTombstone(byte[], int, int)} over a slot value still in off-heap memory.
   *
   * <p>
   * Allocation-free: the predicate reads one byte, so callers that only need to classify a value must
   * not copy the whole payload out first. The sliding-snapshot carry-forward runs this per entry of
   * an aging fragment on the default commit path, where values are serialized bitmaps or projection
   * descriptors — copying each one to test a single byte is pure garbage.
   * </p>
   *
   * @param value the slot value slice ({@code byteSize() == 0} for an absent value)
   * @return {@code true} if the slice is the single-byte tombstone marker
   */
  public static boolean isTombstone(final MemorySegment value) {
    return value.byteSize() == 1 && value.get(ValueLayout.JAVA_BYTE, 0) == TOMBSTONE_FORMAT;
  }

  /**
   * Merges two NodeReferences (OR operation on bitmaps).
   *
   * <p>
   * <b>WARNING: This method mutates {@code a} in-place.</b> The bitmap of {@code a} is modified by
   * OR-ing in the entries from {@code b}. If you need both originals unchanged, clone {@code a}
   * before calling this method.
   * </p>
   *
   * @param a the references to merge INTO (modified in-place)
   * @param b the references to merge from (not modified)
   * @return {@code a} after modification
   */
  public static NodeReferences merge(NodeReferences a, NodeReferences b) {
    a.getNodeKeys().or(b.getNodeKeys());
    return a;
  }

  /**
   * Allocation-free fast path for merging a single-key value into an existing value when both are in
   * {@link #PACKED_FORMAT}. This is the dominant secondary-index churn path:
   * {@code HOTIndexWriter.addNodeKeyToChunk} always serializes the inserted value as a one-entry
   * packed payload, and most live buckets are packed (below {@link #PACKED_THRESHOLD}).
   *
   * <p>
   * Avoids the two {@link Roaring64Bitmap} + two {@link NodeReferences} allocations a deserialize /
   * {@link #merge} / {@link #serialize} round-trip incurs. Returns:
   * <ul>
   * <li>{@code existing} (the same reference) when the new key is already present — the merged set is
   * unchanged, so the caller can skip rewriting the slot entirely;</li>
   * <li>a freshly allocated packed {@code byte[]} of {@code count + 1} keys (sorted ascending,
   * byte-identical to the slow path's output) when the key was absent;</li>
   * <li>{@code null} when the fast path does not apply: the existing value is not packed, the new
   * value is not a single packed key, or adding a key would cross {@link #PACKED_THRESHOLD} into the
   * Roaring representation. The caller must then fall back to the deserialize path.</li>
   * </ul>
   *
   * <p>
   * Relies on the packed format being sorted ascending by unsigned key, which holds for every value
   * this class emits ({@link #serializePacked} iterates {@link Roaring64Bitmap} in ascending order).
   * The binary search for presence depends on that ordering.
   *
   * @param existing the existing serialized value (exact length, not a tombstone)
   * @param newValue buffer holding the inserted serialized value
   * @param newOffset offset of the inserted value within {@code newValue}
   * @param newLen length of the inserted value
   * @return see above
   */
  public static byte @Nullable [] mergePackedSingleBit(final byte[] existing, final byte[] newValue,
      final int newOffset, final int newLen) {
    // New value must be a single-entry packed payload: [PACKED][count=1][key:8] == 10 bytes.
    if (newLen != 2 + 8 || newValue[newOffset] != PACKED_FORMAT || newValue[newOffset + 1] != 1) {
      return null;
    }
    // Existing must be packed and well-formed.
    if (existing.length < 2 || existing[0] != PACKED_FORMAT) {
      return null;
    }
    final int count = existing[1] & 0xFF;
    // A full-or-overflowing bucket would switch representation; leave that to the slow path.
    if (count >= PACKED_THRESHOLD || existing.length != 2 + count * 8) {
      return null;
    }

    final long newKey = readKeyBE(newValue, newOffset + 2);

    // Binary search the sorted-ascending packed keys for newKey.
    int lo = 0;
    int hi = count; // exclusive
    while (lo < hi) {
      final int mid = (lo + hi) >>> 1;
      final int cmp = Long.compareUnsigned(readKeyBE(existing, 2 + mid * 8), newKey);
      if (cmp < 0) {
        lo = mid + 1;
      } else if (cmp > 0) {
        hi = mid;
      } else {
        return existing; // already present — merged set unchanged
      }
    }

    // Insert newKey at position lo, preserving ascending order.
    final byte[] merged = new byte[2 + (count + 1) * 8];
    merged[0] = PACKED_FORMAT;
    merged[1] = (byte) (count + 1);
    final int insAt = 2 + lo * 8;
    System.arraycopy(existing, 2, merged, 2, lo * 8);
    writeKeyBE(merged, insAt, newKey);
    System.arraycopy(existing, insAt, merged, insAt + 8, (count - lo) * 8);
    return merged;
  }

  private static long readKeyBE(final byte[] b, final int p) {
    return ((long) (b[p] & 0xFF) << 56) | ((long) (b[p + 1] & 0xFF) << 48) | ((long) (b[p + 2] & 0xFF) << 40)
        | ((long) (b[p + 3] & 0xFF) << 32) | ((long) (b[p + 4] & 0xFF) << 24) | ((long) (b[p + 5] & 0xFF) << 16)
        | ((long) (b[p + 6] & 0xFF) << 8) | ((long) (b[p + 7] & 0xFF));
  }

  private static void writeKeyBE(final byte[] b, final int p, final long key) {
    b[p] = (byte) (key >>> 56);
    b[p + 1] = (byte) (key >>> 48);
    b[p + 2] = (byte) (key >>> 40);
    b[p + 3] = (byte) (key >>> 32);
    b[p + 4] = (byte) (key >>> 24);
    b[p + 5] = (byte) (key >>> 16);
    b[p + 6] = (byte) (key >>> 8);
    b[p + 7] = (byte) key;
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
    } catch (IOException e) {
      throw new IllegalStateException("Unexpected I/O error during in-memory Roaring64Bitmap serialization", e);
    }
    return buf;
  }

  private static int serializeRoaring(Roaring64Bitmap bitmap, byte[] dest, int offset) {
    dest[offset] = ROARING_FORMAT;
    final int size = (int) bitmap.serializedSizeInBytes();
    try {
      bitmap.serialize(ByteBuffer.wrap(dest, offset + 1, size));
    } catch (IOException e) {
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
          "Packed count " + count + " requires " + requiredBytes + " bytes but only " + length + " available");
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

    return NodeReferences.owning(bitmap);
  }

  private static NodeReferences deserializeRoaring(byte[] bytes, int offset, int length) {
    final Roaring64Bitmap bitmap = new Roaring64Bitmap();
    try {
      bitmap.deserialize(ByteBuffer.wrap(bytes, offset, length));
    } catch (IOException e) {
      throw new IllegalStateException("Unexpected I/O error during in-memory Roaring64Bitmap deserialization", e);
    }
    return NodeReferences.owning(bitmap);
  }
}

