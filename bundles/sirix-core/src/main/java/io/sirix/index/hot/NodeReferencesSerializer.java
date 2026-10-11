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

import io.sirix.access.trx.page.HOTRangeCursor;
import io.sirix.access.trx.page.HOTTrieReader;
import io.sirix.exception.SirixCorruptionException;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.io.HashAlgorithm;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.OverflowPage;
import io.sirix.page.PageReference;
import org.jspecify.annotations.Nullable;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Serializer for {@link NodeReferences} (node key bitmaps).
 *
 * <p>
 * Uses a hybrid format optimized for both small and large sets:
 * </p>
 * <ul>
 * <li><b>Small sets (up to 64 entries):</b> Packed format - more compact, lower overhead</li>
 * <li><b>Large sets:</b> Roaring64Bitmap native serialization - compressed, efficient</li>
 * </ul>
 *
 * <h2>Inline payload formats</h2>
 * 
 * <pre>
 * Packed format:  [0x00][count:1][nodeKey0:8][nodeKey1:8]...[nodeKeyN:8]
 * Roaring format: [0xFF][roaring bitmap bytes...]
 * Tombstone:      [0xFE] (empty bitmap, marks deletion)
 * </pre>
 *
 * Referenced chunk markers ({@link #REFERENCED_FORMAT}) must be resolved through their owning
 * leaf's side map before decoding an inline payload.
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
   * Format marker of a <em>referenced</em> chunk: the payload lives in an {@link OverflowPage} on the
   * leaf's side map. The marker layout is specified in docs/DISK_FORMAT.md, "CAS and VALIDTIME
   * posting chunks". A versioned leaf carries the marker so sliding carry-forward does not re-emit
   * chunk bytes. Readers resolve the page through the storage engine ({@code readSideOverflowPage});
   * every structural move of the owning entry routes the side reference by the marker
   * ({@link HOTLeafPage#findReferencedPostingOwner}).
   */
  public static final byte REFERENCED_FORMAT = (byte) 0xFD;
  /** Length of a referenced-chunk marker. */
  public static final int REFERENCED_LENGTH = 1 + Long.BYTES + Integer.BYTES + Long.BYTES;

  /** Encode a referenced-chunk marker. */
  public static byte[] encodeReferenced(final long refKey, final int payloadLength, final long payloadHash) {
    if (payloadLength <= 0 || payloadLength > 0xFFFF) {
      throw new IllegalArgumentException("referenced payload length must be in [1, 65535]: " + payloadLength);
    }
    final byte[] marker = new byte[REFERENCED_LENGTH];
    marker[0] = REFERENCED_FORMAT;
    for (int i = 0; i < Long.BYTES; i++) {
      marker[1 + i] = (byte) (refKey >>> (56 - 8 * i));
    }
    for (int i = 0; i < Integer.BYTES; i++) {
      marker[1 + Long.BYTES + i] = (byte) (payloadLength >>> (24 - 8 * i));
    }
    for (int i = 0; i < Long.BYTES; i++) {
      marker[1 + Long.BYTES + Integer.BYTES + i] = (byte) (payloadHash >>> (56 - 8 * i));
    }
    return marker;
  }

  /** Whether {@code bytes[offset..offset+length)} is a referenced-chunk marker. */
  public static boolean isReferenced(final byte[] bytes, final int offset, final int length) {
    return length == REFERENCED_LENGTH && bytes[offset] == REFERENCED_FORMAT;
  }

  /** Whether the slot value {@code ref} of {@code leaf} is a referenced-chunk marker. */
  public static boolean isReferenced(final HOTLeafPage leaf, final long ref) {
    return HOTLeafPage.refLength(ref) == REFERENCED_LENGTH && leaf.refByteAt(ref, 0) == REFERENCED_FORMAT;
  }

  /** The side-map key of a referenced-chunk marker held in a heap array. */
  public static long referencedKey(final byte[] marker, final int offset) {
    long key = 0L;
    for (int i = 0; i < Long.BYTES; i++) {
      key = (key << 8) | (marker[offset + 1 + i] & 0xFFL);
    }
    return key;
  }

  /** The payload length recorded in a referenced-chunk marker held in a heap array. */
  public static int referencedPayloadLength(final byte[] marker, final int offset) {
    int length = 0;
    for (int i = 0; i < Integer.BYTES; i++) {
      length = (length << 8) | (marker[offset + 1 + Long.BYTES + i] & 0xFF);
    }
    return length;
  }

  /** The side-map key of the referenced-chunk marker at slot value {@code ref}. */
  public static long referencedKey(final HOTLeafPage leaf, final long ref) {
    return leaf.refLongBEAt(ref, 1);
  }

  /** The payload length recorded in the referenced-chunk marker at slot value {@code ref}. */
  public static int referencedPayloadLength(final HOTLeafPage leaf, final long ref) {
    int length = 0;
    for (int i = 0; i < Integer.BYTES; i++) {
      length = (length << 8) | (leaf.refByteAt(ref, 1 + Long.BYTES + i) & 0xFF);
    }
    return length;
  }

  public static long referencedPayloadHash(final byte[] marker, final int offset) {
    return readKeyBE(marker, offset + 1 + Long.BYTES + Integer.BYTES);
  }

  public static long referencedPayloadHash(final HOTLeafPage leaf, final long ref) {
    return leaf.refLongBEAt(ref, 1 + Long.BYTES + Integer.BYTES);
  }

  /**
   * Resolve the payload of a referenced chunk through a side-page reader: the leaf's side reference
   * under the marker's key, read via {@code readSideOverflowPage}, length-checked and optionally
   * hash-checked against the marker.
   *
   * @throws IllegalStateException when the side reference or its page is missing or the length
   *         disagrees
   * @throws SirixCorruptionException when checksum verification is enabled and the payload hash
   *         disagrees
   */
  public static byte[] resolveReferencedPayload(final HOTLeafPage leaf, final long refKey, final int payloadLength,
      final long payloadHash, final boolean verifyChecksums,
      final Function<PageReference, @Nullable OverflowPage> reader) {
    if (payloadLength <= 0 || payloadLength > 0xFFFF) {
      throw new IllegalStateException("Invalid referenced posting payload length: " + payloadLength);
    }
    final PageReference sideReference = leaf.getPageReference(refKey);
    if (sideReference == null) {
      throw new IllegalStateException(
          "referenced posting chunk " + refKey + " has no side reference on leaf " + leaf.getPageKey());
    }
    final OverflowPage page = reader.apply(sideReference);
    if (page == null) {
      throw new IllegalStateException(
          "referenced posting chunk " + refKey + " on leaf " + leaf.getPageKey() + " has an unresolvable side page");
    }
    if (page.dataLength() != payloadLength) {
      throw new IllegalStateException("referenced posting chunk " + refKey + " on leaf " + leaf.getPageKey() + " has "
          + page.dataLength() + " payload bytes but its marker records " + payloadLength);
    }
    final byte[] payload = page.getDataBytes();
    if (verifyChecksums) {
      final long actualHash = ProjectionIndexColumnSegmentCodec.contentHash(payload);
      if (actualHash != payloadHash) {
        throw new SirixCorruptionException(sideReference.getKey(),
            "referenced posting chunk " + refKey + " on leaf " + leaf.getPageKey(),
            HashAlgorithm.longToBytes(payloadHash), HashAlgorithm.longToBytes(actualHash));
      }
    }
    return payload;
  }

  /**
   * Read the one low-16 bit of a delta, or -1 for its tombstone. The caller guards/validates the
   * leaf.
   */
  static long readDeltaBit(final HOTLeafPage leaf, final long ref) {
    if (isTombstone(leaf, ref)) {
      return -1;
    }
    if (HOTLeafPage.refLength(ref) != 2 + Long.BYTES || leaf.refByteAt(ref, 0) != PACKED_FORMAT
        || leaf.refByteAt(ref, 1) != 1) {
      throw new IllegalArgumentException("a posting delta must contain exactly one packed chunk bit");
    }
    return requireChunkBit16(leaf.refLongBEAt(ref, 2));
  }

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
        writeKeyBE(buf, pos, nodeKeys[i]);
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
    if (offset > bytes.length - length) {
      throw new IllegalArgumentException("offset + length exceeds array length: offset=" + offset + ", length=" + length
          + ", arrayLength=" + bytes.length);
    }

    if (length == 0) {
      return new NodeReferences();
    }

    byte format = bytes[offset];

    if (format == TOMBSTONE_FORMAT) {
      if (length != 1) {
        throw new IllegalArgumentException("Tombstone payload must be exactly one byte, but has " + length);
      }
      // Tombstone - return empty references
      return new NodeReferences();
    } else if (format == PACKED_FORMAT) {
      return deserializePacked(bytes, offset + 1, length - 1);
    } else if (format == ROARING_FORMAT) {
      return deserializeRoaring(bytes, offset + 1, length - 1);
    } else if (format == REFERENCED_FORMAT) {
      throw new IllegalStateException("Resolve referenced posting payloads through their side pages before decoding");

    } else {
      throw new IllegalArgumentException("Unknown NodeReferences format: " + format);
    }
  }

  /**
   * Deserialize one chunk-local posting list and enforce its unsigned-16-bit value domain.
   *
   * <p>
   * The general {@link #deserialize(byte[], int, int)} method intentionally accepts arbitrary 64-bit
   * node keys. HOT secondary indexes store only the low 16 bits in each composite-key chunk, however;
   * accepting a wider value there either preserves corrupt storage or aliases it when the chunk index
   * is recombined. This contextual entry point keeps those semantics explicit.
   * </p>
   *
   * <p>
   * Validation is allocation-free after the ordinary deserialize: Roaring iterates in unsigned order,
   * so checking its last value proves the complete set is within {@code [0, 65535]} without creating
   * an iterator or copying the bitmap.
   * </p>
   *
   * @param bytes serialized chunk payload
   * @return deserialized, validated chunk references
   */
  public static NodeReferences deserializeChunk(final byte[] bytes) {
    requireNonNull(bytes, "bytes cannot be null");
    if (bytes.length == 0) {
      throw new IllegalArgumentException("Posting-list chunk payload must not be empty");
    }
    return requireChunkReferences16(deserialize(bytes, 0, bytes.length));
  }

  /**
   * Range variant of {@link #deserializeChunk(byte[])}.
   *
   * @param bytes serialized payload backing array
   * @param offset first payload byte
   * @param length payload length
   * @return deserialized, validated chunk references
   */
  public static NodeReferences deserializeChunk(final byte[] bytes, final int offset, final int length) {
    requireNonNull(bytes, "bytes cannot be null");
    if (length <= 0) {
      throw new IllegalArgumentException("Posting-list chunk payload must not be empty: length=" + length);
    }
    return requireChunkReferences16(deserialize(bytes, offset, length));
  }

  /**
   * Validate a serialized chunk payload without materializing its common packed representation.
   *
   * <p>
   * This is for pass-through paths such as resurrection over a tombstone: they retain the incoming
   * bytes unchanged, but still must not publish malformed chunk-local values. Packed and tombstone
   * payloads are checked directly in the caller's array with no copy or allocation; Roaring payloads
   * use {@link #deserializeChunk(byte[], int, int)} because validating their compressed container
   * structure necessarily requires decoding it.
   * </p>
   *
   * @param bytes serialized payload backing array
   * @param offset first payload byte
   * @param length payload length
   */
  public static void requireValidChunkPayload(final byte[] bytes, final int offset, final int length) {
    requireNonNull(bytes, "bytes cannot be null");
    if (offset < 0 || length <= 0 || offset > bytes.length - length) {
      throw new IllegalArgumentException("Chunk payload range must be non-empty and within the array: offset=" + offset
          + ", length=" + length + ", arrayLength=" + bytes.length);
    }

    final byte format = bytes[offset];
    if (format == TOMBSTONE_FORMAT) {
      if (length != 1) {
        throw new IllegalArgumentException("Tombstone payload must be exactly one byte, but has " + length);
      }
      return;
    }
    if (format != PACKED_FORMAT) {
      // Also supplies the canonical unknown/Roaring error handling; this is the cold representation.
      deserializeChunk(bytes, offset, length);
      return;
    }
    if (length < 2) {
      throw new IllegalArgumentException("Packed payload has no count byte");
    }

    final int count = bytes[offset + 1] & 0xFF;
    if (count == 0 || count > PACKED_THRESHOLD) {
      throw new IllegalArgumentException("Packed count must be in [1, " + PACKED_THRESHOLD + "]: " + count);
    }
    final int requiredLength = 2 + count * Long.BYTES;
    if (requiredLength != length) {
      throw new IllegalArgumentException(
          "Packed count " + count + " requires exactly " + requiredLength + " bytes but has " + length);
    }

    long previousBit16 = 0L;
    int position = offset + 2;
    for (int i = 0; i < count; i++) {
      final long bit16 = requireChunkBit16(readKeyBE(bytes, position));
      if (i > 0 && Long.compareUnsigned(previousBit16, bit16) >= 0) {
        throw new IllegalArgumentException(
            "Packed posting-list chunk bits must be strictly increasing at entries " + (i - 1) + " and " + i);
      }
      previousBit16 = bit16;
      position += Long.BYTES;
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
   * Drain a composite-bounded cursor sweep, merging every chunk slot whose composite key starts with
   * {@code prefixBuf[0..prefixLen)} into one bitmap of {@code (chunkIdx << 16) | bit16} node keys.
   * Shared by the CAS/NAME and primitive-long index writers' same-transaction {@code get} — the two
   * walks were hand-mirrored copies, and both read UNPINNED leaves under optimistic stamps, so the
   * torn-read discipline lives once, here: each slot's (composite, payload) copies are validated
   * against the cursor's leaf stamp BEFORE the payload reaches {@link #deserialize} or the merge, and
   * a torn read re-evaluates the SAME slot on a refreshed leaf copy.
   *
   * @param cursor a cursor positioned by the caller over the composite range of {@code prefixBuf}
   * @param prefixBuf buffer holding the serialized logical key
   * @param prefixLen serialized length of the logical key
   * @return the merged node keys, or {@code null} when no chunk holds a live reference
   */
  public static @Nullable Roaring64Bitmap mergeChunksInPrefixRange(final HOTRangeCursor cursor, final byte[] prefixBuf,
      final int prefixLen) {
    return mergeChunksInPrefixRange(cursor, prefixBuf, prefixLen, false);
  }

  /** Variant for prefix-free CAS/VALIDTIME keys, whose chunk ranges also contain delta slots. */
  public static @Nullable Roaring64Bitmap mergeChunksInPrefixRange(final HOTRangeCursor cursor, final byte[] prefixBuf,
      final int prefixLen, final boolean postingDeltas) {
    requireNonNull(cursor, "cursor cannot be null");
    requireNonNull(prefixBuf, "prefixBuf cannot be null");
    final int compositeLen = prefixLen + HOTKeySerializer.CHUNK_IDX_BYTES;
    final int deltaLen = compositeLen + PostingDeltas.SUFFIX_BYTES;
    Roaring64Bitmap merged = null;
    int tornRounds = 0;
    while (cursor.hasNext()) {
      final HOTLeafPage leaf = cursor.currentLeafPage();
      final int idx = cursor.currentEntryIndex();
      byte[] composite = null;
      byte[] chunkBytes = null;
      try {
        final byte[] candidate = leaf.getKey(idx);
        if (candidate != null
            && matchesPostingSlot(candidate, prefixBuf, prefixLen, compositeLen, deltaLen, postingDeltas)) {
          composite = candidate;
          // Preserve zero-length vs unreadable instead of letting getValue() collapse both to an
          // absent value. A matched composite slot must carry a canonical chunk payload.
          chunkBytes = leaf.copyStoredValue(idx);
          if (candidate.length == compositeLen && isReferenced(chunkBytes, 0, chunkBytes.length)) {
            chunkBytes =
                resolveReferencedPayload(leaf, referencedKey(chunkBytes, 0), referencedPayloadLength(chunkBytes, 0),
                    referencedPayloadHash(chunkBytes, 0), cursor.verifyChecksumsOnRead(), cursor::readSideOverflowPage);
          }
        }
      } catch (RuntimeException e) {
        if (cursor.validateLeaf()) {
          throw e; // stable bytes — genuine corruption, not a torn read
        }
        cursor.recoverTorn(++tornRounds);
        continue;
      }
      if (!cursor.validateLeaf()) {
        cursor.recoverTorn(++tornRounds);
        continue;
      }
      tornRounds = 0;
      if (composite != null && chunkBytes != null && !isTombstone(chunkBytes, 0, chunkBytes.length)) {
        // The copies are validated heap bytes now — safe to hand to the deserializer.
        merged = mergeChunkOrDelta(merged, composite, chunkBytes, compositeLen, deltaLen);
      }
      cursor.advance();
    }
    return merged;
  }

  /** Select only this logical key's base chunks and, when enabled, valid delta suffixes. */
  private static boolean matchesPostingSlot(final byte[] candidate, final byte[] prefixBuf, final int prefixLen,
      final int compositeLen, final int deltaLen, final boolean postingDeltas) {
    return (candidate.length == compositeLen || (postingDeltas && candidate.length == deltaLen))
        && Arrays.compareUnsigned(candidate, 0, prefixLen, prefixBuf, 0, prefixLen) == 0
        && (candidate.length == compositeLen
            || PostingDeltas.isDelta(HOTKeySerializer.readChunkIdx(candidate, 0, deltaLen) & 0xFFFFFFFFL));
  }

  /** Merge one stamp-validated base or delta payload, in the cursor's application order. */
  private static @Nullable Roaring64Bitmap mergeChunkOrDelta(@Nullable Roaring64Bitmap merged, final byte[] composite,
      final byte[] chunkBytes, final int compositeLen, final int deltaLen) {
    final long trailer = HOTKeySerializer.readChunkIdx(composite, 0, compositeLen) & 0xFFFFFFFFL;
    if (composite.length == deltaLen) {
      final long suffix = HOTKeySerializer.readChunkIdx(composite, 0, deltaLen) & 0xFFFFFFFFL;
      // A delta slot follows its base chunk in application order.
      final long deltaHigh = trailer << 16;
      final boolean remove = PostingDeltas.isRemove(suffix);
      if (chunkBytes.length != 2 + Long.BYTES || chunkBytes[0] != PACKED_FORMAT || chunkBytes[1] != 1) {
        throw new IllegalArgumentException("a posting delta must contain exactly one packed chunk bit");
      }
      final long nodeKey = deltaHigh | requireChunkBit16(readKeyBE(chunkBytes, 2));
      if (remove) {
        if (merged != null) {
          merged.removeLong(nodeKey);
        }
      } else {
        if (merged == null) {
          merged = new Roaring64Bitmap();
        }
        merged.addLong(nodeKey);
      }
    } else {
      final Roaring64Bitmap chunkBitmap = deserializeChunk(chunkBytes).getNodeKeys();
      if (merged == null) {
        merged = new Roaring64Bitmap();
      }
      // chunkIdx is UNSIGNED (see ChunkAccumulator#addChunk): mask before widening, exactly as the
      // reader-side call sites do. Sign-extending here would make the writer's same-transaction
      // view reconstruct a different node key than the reader for the very same stored chunk.
      final long high = trailer << 16;
      final LongIterator bIt = chunkBitmap.getLongIterator();
      while (bIt.hasNext()) {
        merged.add(high | bIt.next());
      }
    }
    return merged;
  }

  /**
   * Accumulates a lookup's chunk payloads into the cheapest sufficient representation: a sorted
   * {@code long[]} for small packed chunks (the average CAS posting list holds one or two node keys),
   * spilling to a {@link Roaring64Bitmap} past {@link #COMPACT_LIMIT}. Chunk-local callers may retain
   * an owned Roaring payload directly through {@link #forChunkLookup()}, avoiding enumeration for
   * cardinality without changing the general lookup's compact-result contract. The emitted
   * {@link NodeReferences#ofSortedArray} result costs one right-sized array + one wrapper instead of
   * a bitmap container tree per lookup — the single largest allocation the read path had left.
   *
   * <p>
   * Sortedness precondition: chunks must be appended in ascending composite-key order (the chunk
   * walk's natural order — chunkIdx-major, bit16-minor, duplicate-free), which makes every append
   * strictly ascending. Deltas may arrive out of posting order and use bounded sorted insertion. Not
   * thread-safe; pool per reader or per iterator.
   */
  public static final class ChunkAccumulator {

    private static final int COMPACT_LIMIT = 512;
    private final boolean retainRoaringChunks;

    public ChunkAccumulator() {
      this(false);
    }

    private ChunkAccumulator(final boolean retainRoaringChunks) {
      this.retainRoaringChunks = retainRoaringChunks;
    }

    /** Chunk-local reads retain decoded bitmaps instead of enumerating their bits for a count. */
    public static ChunkAccumulator forChunkLookup() {
      return new ChunkAccumulator(true);
    }

    private long[] keys = new long[8];
    private int count;
    /** Last appended key, kept as a scalar so the ordering guard needs no bounds-checked array read. */
    private long lastKey;
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
      // The compact result is binary-searched by NodeReferences.contains, so the array MUST come
      // out strictly ascending. It does for a well-formed trie (chunkIdx-major slot order, ascending
      // bit16 within a payload), but that rests on the walk being lex-monotonic — the property the
      // detector/heal machinery exists because we do not assume everywhere. One compare per key buys
      // the guarantee unconditionally: a non-ascending append spills to a bitmap, which is
      // order-insensitive and de-duplicates.
      if (count > 0 && Long.compareUnsigned(key, lastKey) <= 0) {
        spillToBitmap().add(key);
        return;
      }
      if (count == keys.length) {
        if (count >= COMPACT_LIMIT) {
          spillToBitmap().add(key);
          return;
        }
        keys = Arrays.copyOf(keys, count * 2);
      }
      keys[count++] = key;
      lastKey = key;
    }

    /** Delta order is chronological, so insert within the bounded compact run without spilling. */
    private void addDelta(final long key) {
      if (bitmap != null || count == 0 || Long.compareUnsigned(key, lastKey) > 0) {
        add(key);
        return;
      }
      int low = 0;
      int high = count - 1;
      while (low <= high) {
        final int mid = (low + high) >>> 1;
        final int cmp = Long.compareUnsigned(keys[mid], key);
        if (cmp < 0) {
          low = mid + 1;
        } else if (cmp > 0) {
          high = mid - 1;
        } else {
          return;
        }
      }
      if (count == keys.length) {
        if (count >= COMPACT_LIMIT) {
          spillToBitmap().add(key);
          return;
        }
        keys = Arrays.copyOf(keys, count * 2);
      }
      System.arraycopy(keys, low, keys, low + 1, count - low);
      keys[low] = key;
      count++;
      // This key is no greater than lastKey, which remains the sorted run's maximum.
    }

    /** Move everything accumulated so far into a bitmap and switch to it. */
    private Roaring64Bitmap spillToBitmap() {
      final Roaring64Bitmap spilled = new Roaring64Bitmap();
      for (int i = 0; i < count; i++) {
        spilled.add(keys[i]);
      }
      bitmap = spilled;
      count = 0;
      return spilled;
    }

    /**
     * Append one chunk payload, expanding each stored bit16 to {@code high | bit16}. Same format
     * handling as {@link #mergePackedSingleBitFromSlot(HOTLeafPage, long, byte[], int, int)}: packed
     * reads straight off slot memory, tombstones and empty payloads are skipped, the (rare) Roaring
     * format round-trips through a heap array.
     *
     * @param leaf the leaf page holding the chunk slot
     * @param ref the slot's packed value handle from {@link HOTLeafPage#valueRef(int)}
     * @param high the pre-shifted chunk base ({@code chunkIdx << 16}, chunkIdx treated unsigned)
     */
    public void addChunk(final HOTLeafPage leaf, final long ref, final long high) {
      // Kept for tests that build a leaf directly and have no reader. Deliberately NOT for
      // production use: with no trie to validate against, a torn slot read cannot be distinguished
      // from real corruption, so it is either merged or rethrown — never retried. Every production
      // caller passes the reader.
      addChunk(leaf, ref, high, null);
    }

    /**
     * Torn-read-aware variant of {@link #addChunk(HOTLeafPage, long, long)} for callers reading an
     * UNPINNED leaf under optimistic stamps. The slot's declared payload length is clamped against the
     * leaf's slot capacity before any allocation, a thrown read is distinguished from real corruption
     * by validating {@code trie}'s current-leaf stamp, and a Roaring payload is copied out and
     * validated BEFORE it reaches the deserializer — garbage bytes there risk absurd allocations, not
     * just wrong answers.
     *
     * <p>
     * A {@code false} return means the merge read torn bytes and this accumulator's state can no longer
     * be trusted: the caller must {@link #reset()} and re-walk its aggregate from a validated position.
     * {@code true} means the merge completed — subject to the caller's own batch validation, since the
     * packed fast path merges straight off slot memory.
     *
     * @param leaf the leaf page holding the chunk slot
     * @param ref the slot's packed value handle from {@link HOTLeafPage#valueRef(int)}
     * @param high the pre-shifted chunk base ({@code chunkIdx << 16}, chunkIdx treated unsigned)
     * @param trie the trie reader whose current leaf is {@code leaf}, or {@code null} when the caller
     *        pins pages and torn reads are impossible
     * @return {@code false} iff a torn read was detected and the accumulator must be reset
     */
    public boolean addChunk(final HOTLeafPage leaf, final long ref, final long high,
        final @Nullable HOTTrieReader trie) {
      try {
        return addChunkFromSlot(leaf, ref, high, trie);
      } catch (RuntimeException e) {
        if (trie == null || trie.validateCurrentLeaf()) {
          throw e; // stable bytes — genuine corruption, not a torn read
        }
        return false;
      }
    }

    /**
     * Apply one delta slot ({@code C ‖ suffix}): add or remove its node keys in chunk {@code chunkIdx}.
     *
     * @return {@code false} on a torn read (the caller restarts the walk)
     */
    public boolean applyDelta(final HOTLeafPage leaf, final long ref, final long chunkIdx, final long suffix,
        final @Nullable HOTTrieReader trie) {
      try {
        final long bit = readDeltaBit(leaf, ref);
        if (bit >= 0) {
          final long key = (chunkIdx << 16) | bit;
          if (PostingDeltas.isRemove(suffix)) {
            remove(key);
          } else {
            addDelta(key);
          }
        }
        return true;
      } catch (RuntimeException e) {
        if (trie == null || trie.validateCurrentLeaf()) {
          throw e;
        }
        return false;
      }
    }

    private void remove(final long key) {
      final Roaring64Bitmap spilled = bitmap;
      if (spilled != null) {
        spilled.removeLong(key);
        return;
      }
      int low = 0;
      int high = count - 1;
      while (low <= high) {
        final int mid = (low + high) >>> 1;
        final int cmp = Long.compareUnsigned(keys[mid], key);
        if (cmp < 0) {
          low = mid + 1;
        } else if (cmp > 0) {
          high = mid - 1;
        } else {
          System.arraycopy(keys, mid + 1, keys, mid, count - mid - 1);
          count--;
          if (count > 0) {
            lastKey = keys[count - 1];
          }
          return;
        }
      }
    }

    private boolean addChunkFromSlot(final HOTLeafPage leaf, final long ref, final long high,
        final @Nullable HOTTrieReader trie) {
      final int length = HOTLeafPage.refLength(ref);
      if (length < 0) {
        throw new IllegalStateException("Chunk slot does not address a readable value");
      }
      if (length == 0) {
        throw new IllegalArgumentException("Posting-list chunk payload must not be empty");
      }
      if (length > leaf.slotCapacity()) {
        // A payload no slot can hold: on a torn read the wrapper's validation fails and the caller
        // retries; on stable bytes this escapes as the storage corruption it is.
        throw new IllegalStateException(
            "Chunk payload of " + length + " bytes exceeds the slot capacity of " + leaf.slotCapacity() + " bytes");
      }
      final byte format = leaf.refByteAt(ref, 0);
      if (format == TOMBSTONE_FORMAT) {
        if (length != 1) {
          throw new IllegalArgumentException("Tombstone payload must be exactly one byte, but slot has " + length);
        }
        return true;
      }
      if (format == PACKED_FORMAT) {
        if (length < 2) {
          throw new IllegalArgumentException("Packed payload has no count byte: length=" + length);
        }
        final int chunkCount = leaf.refByteAt(ref, 1) & 0xFF;
        if (chunkCount == 0 || chunkCount > PACKED_THRESHOLD) {
          throw new IllegalArgumentException("Packed count must be in [1, " + PACKED_THRESHOLD + "]: " + chunkCount);
        }
        final int requiredLength = 2 + chunkCount * Long.BYTES;
        if (requiredLength != length) {
          // Canonical slot payloads have neither truncation nor ignored trailing bytes. Treat both
          // as corruption: accepting the latter could let an interrupted rewrite hide postings.
          throw new IllegalArgumentException(
              "Packed count " + chunkCount + " requires exactly " + requiredLength + " bytes but slot has " + length);
        }
        long previousBit16 = 0L;
        for (int i = 0; i < chunkCount; i++) {
          final long bit16 = requireChunkBit16(leaf.refLongBEAt(ref, 2 + i * Long.BYTES));
          if (i > 0 && Long.compareUnsigned(previousBit16, bit16) >= 0) {
            throw new IllegalArgumentException(
                "Packed posting-list chunk bits must be strictly increasing at entries " + (i - 1) + " and " + i);
          }
          add(high | bit16);
          previousBit16 = bit16;
        }
        return true;
      }
      if (format == ROARING_FORMAT) {
        if (length < 2) {
          // Same treatment as a packed payload whose declared count overruns the slot: a Roaring
          // marker with no bitmap behind it is storage corruption, not an empty posting list.
          throw new IllegalArgumentException("Roaring payload of " + length + " byte(s) carries no bitmap");
        }
        final byte[] bytes = new byte[length - 1];
        leaf.copyRefInto(ref, 1, bytes, 0, length - 1);
        if (trie != null && !trie.validateCurrentLeaf()) {
          // Torn copy — never hand it to the Roaring deserializer, whose failure modes on garbage
          // include huge container allocations, not just exceptions.
          return false;
        }
        final Roaring64Bitmap chunkBitmap = new Roaring64Bitmap();
        try {
          chunkBitmap.deserialize(ByteBuffer.wrap(bytes));
        } catch (IOException e) {
          throw new IllegalStateException("Unexpected I/O error during in-memory Roaring64Bitmap deserialization", e);
        }
        if (!chunkBitmap.isEmpty()) {
          requireChunkBit16(chunkBitmap.last());
        }
        if (retainRoaringChunks && high == 0 && count == 0 && bitmap == null) {
          // Deserialization owns this bitmap. Keep it directly for a chunk-local lookup,
          // so cardinality does not enumerate and rebuild its bits.
          bitmap = chunkBitmap;
          return true;
        }
        final LongIterator it = chunkBitmap.getLongIterator();
        while (it.hasNext()) {
          add(high | it.next());
        }
        return true;
      }
      if (format == REFERENCED_FORMAT) {
        if (length != REFERENCED_LENGTH) {
          throw new IllegalArgumentException(
              "Referenced chunk marker must be " + REFERENCED_LENGTH + " bytes, but slot has " + length);
        }
        if (trie == null) {
          throw new IllegalStateException("a referenced posting chunk needs a trie reader to resolve its side page");
        }
        final long refKey = referencedKey(leaf, ref);
        final int payloadLength = referencedPayloadLength(leaf, ref);
        final byte[] payload = resolveReferencedPayload(leaf, refKey, payloadLength, referencedPayloadHash(leaf, ref),
            trie.verifyChecksumsOnRead(), trie::readSideOverflowPage);
        if (!trie.validateCurrentLeaf()) {
          return false; // the marker (and so the side reference) may have been torn — restart the walk
        }
        return addChunkFromBytes(payload, high);
      }
      throw new IllegalArgumentException("Unknown NodeReferences format: " + format);
    }

    /** The heap-array twin of {@code addChunkFromSlot} for a resolved referenced payload. */
    private boolean addChunkFromBytes(final byte[] bytes, final long high) {
      if (bytes.length == 0) {
        throw new IllegalArgumentException("Posting-list chunk payload must not be empty");
      }
      final byte format = bytes[0];
      if (format == TOMBSTONE_FORMAT) {
        if (bytes.length != 1) {
          throw new IllegalArgumentException("Referenced tombstone payload must be exactly one byte");
        }
        return true;
      }
      if (format == PACKED_FORMAT) {
        return addPackedChunkFromBytes(bytes, high);
      }
      if (format == ROARING_FORMAT) {
        if (bytes.length < 2) {
          throw new IllegalArgumentException("Referenced Roaring payload carries no bitmap");
        }
        final Roaring64Bitmap chunkBitmap = new Roaring64Bitmap();
        try {
          chunkBitmap.deserialize(ByteBuffer.wrap(bytes, 1, bytes.length - 1));
        } catch (IOException e) {
          throw new IllegalStateException("Unexpected I/O error during in-memory Roaring64Bitmap deserialization", e);
        }
        if (!chunkBitmap.isEmpty()) {
          requireChunkBit16(chunkBitmap.last());
        }
        if (retainRoaringChunks && high == 0 && count == 0 && bitmap == null) {
          bitmap = chunkBitmap;
          return true;
        }
        final LongIterator it = chunkBitmap.getLongIterator();
        while (it.hasNext()) {
          add(high | it.next());
        }
        return true;
      }
      throw new IllegalArgumentException("Unknown NodeReferences format in a referenced payload: " + format);
    }

    private boolean addPackedChunkFromBytes(final byte[] bytes, final long high) {
      if (bytes.length < 2) {
        throw new IllegalArgumentException("Packed referenced payload has no count byte");
      }
      final int chunkCount = bytes[1] & 0xFF;
      if (chunkCount == 0 || chunkCount > PACKED_THRESHOLD || 2 + chunkCount * Long.BYTES != bytes.length) {
        throw new IllegalArgumentException(
            "Packed referenced payload is malformed: count=" + chunkCount + ", length=" + bytes.length);
      }
      long previousBit16 = 0L;
      for (int i = 0; i < chunkCount; i++) {
        final long bit16 = requireChunkBit16(readKeyBE(bytes, 2 + i * Long.BYTES));
        if (i > 0 && Long.compareUnsigned(previousBit16, bit16) >= 0) {
          throw new IllegalArgumentException("Packed posting-list chunk bits must be strictly increasing");
        }
        add(high | bit16);
        previousBit16 = bit16;
      }
      return true;
    }

    /**
     * Emit the accumulated result — compact for small packed lists, otherwise bitmap-backed — and reset
     * for reuse. {@code null} when nothing live was accumulated.
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
      return NodeReferences.ofSortedArray(Arrays.copyOf(keys, resultCount));
    }
  }

  /**
   * Identity sentinel returned by the allocating
   * {@link #mergePackedSingleBitFromSlot(HOTLeafPage, long, byte[], int, int)} API when the new bit
   * is already present in the slot's packed set — the caller skips the slot rewrite entirely. A
   * distinct sentinel is needed because the payload is still in slot memory: there is no existing
   * array to hand back by identity the way a copying merge would.
   */
  public static final byte[] MERGE_UNCHANGED = new byte[0];

  /** Largest canonical packed payload, including its format and count bytes. */
  public static final int MAX_PACKED_PAYLOAD_LENGTH = 2 + PACKED_THRESHOLD * Long.BYTES;

  /** The scratch-based packed merge did not see two qualifying packed payload shapes. */
  public static final int PACKED_MERGE_NOT_APPLICABLE = -1;

  /** The scratch-based packed merge found the incoming bit in the resident payload already. */
  public static final int PACKED_MERGE_UNCHANGED = 0;

  /** {@link #removePackedSingleBitFromSlot} did not see a packed payload. */
  public static final int PACKED_REMOVE_NOT_APPLICABLE = -1;

  /** The packed payload is valid, but does not contain the requested bit. */
  public static final int PACKED_REMOVE_ABSENT = -2;

  /** The requested bit was the packed payload's final entry; the caller must delete the slot. */
  public static final int PACKED_REMOVE_EMPTY = 0;

  /**
   * Single-bit packed merge against a payload still resident in its leaf slot: binary-search and
   * splice directly off slot memory via the leaf's value-ref accessors. This is the insert-time hot
   * path — every listener-driven index insert merges a single-bit payload into its chunk bucket, and
   * the copying variant first materialized the whole existing bucket (up to {@code 2 + 64*8} bytes)
   * just to read it.
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
    final int mergePlan = packedSingleBitMergePlan(leaf, ref, newValue, newOffset, newLen);
    if (mergePlan == PACKED_MERGE_NOT_APPLICABLE) {
      return null;
    }
    if (mergePlan == PACKED_MERGE_UNCHANGED) {
      return MERGE_UNCHANGED;
    }

    final int resultLen = HOTLeafPage.refLength(ref) + Long.BYTES;
    final byte[] merged = new byte[resultLen];
    writePackedSingleBitMerge(leaf, ref, newValue, newOffset, mergePlan - 1, merged, 0, resultLen);
    return merged;
  }

  /**
   * Allocation-free twin of
   * {@link #mergePackedSingleBitFromSlot(HOTLeafPage, long, byte[], int, int)}. A positive return is
   * the exact payload length written at {@code scratchOffset}; primitive status values distinguish a
   * non-qualifying shape and an unchanged set without allocating a carrier.
   *
   * <p>
   * The complete resident packed payload is range- and ordering-validated before the first scratch
   * byte is written. A corruption failure therefore leaves caller-owned scratch unchanged. Neither
   * input array is retained.
   * </p>
   *
   * @param leaf the leaf holding the existing payload
   * @param ref the slot's packed value handle from {@link HOTLeafPage#valueRef(int)}
   * @param newValue buffer holding the new single-entry packed payload
   * @param newOffset offset of the payload in {@code newValue}
   * @param newLen length of the payload
   * @param scratch caller-owned result buffer
   * @param scratchOffset first result byte in {@code scratch}
   * @return {@link #PACKED_MERGE_NOT_APPLICABLE}, {@link #PACKED_MERGE_UNCHANGED}, or the positive
   *         exact result length written into {@code scratch}
   * @throws IllegalArgumentException if the incoming bit or a resident packed payload is malformed
   * @throws IndexOutOfBoundsException if an input range is invalid or a required result does not fit
   */
  public static int mergePackedSingleBitFromSlot(final HOTLeafPage leaf, final long ref, final byte[] newValue,
      final int newOffset, final int newLen, final byte[] scratch, final int scratchOffset) {
    requireNonNull(scratch, "scratch cannot be null");
    if (scratchOffset < 0 || scratchOffset > scratch.length) {
      throw new IndexOutOfBoundsException("scratchOffset=" + scratchOffset + " scratch.length=" + scratch.length);
    }

    final int mergePlan = packedSingleBitMergePlan(leaf, ref, newValue, newOffset, newLen);
    if (mergePlan <= PACKED_MERGE_UNCHANGED) {
      return mergePlan;
    }

    final int resultLen = HOTLeafPage.refLength(ref) + Long.BYTES;
    if (scratchOffset > scratch.length - resultLen) {
      throw new IndexOutOfBoundsException("Packed merge result of " + resultLen
          + " bytes does not fit scratch at offset " + scratchOffset + " (length=" + scratch.length + ')');
    }
    writePackedSingleBitMerge(leaf, ref, newValue, newOffset, mergePlan - 1, scratch, scratchOffset, resultLen);
    return resultLen;
  }

  /**
   * Validate both merge inputs and return {@code insertionIndex + 1}, or a primitive merge status.
   * Returning the shifted index reserves zero for the unchanged result.
   */
  private static int packedSingleBitMergePlan(final HOTLeafPage leaf, final long ref, final byte[] newValue,
      final int newOffset, final int newLen) {
    requireNonNull(leaf, "leaf cannot be null");
    requireNonNull(newValue, "newValue cannot be null");
    if (newOffset < 0 || newLen < 0 || newOffset > newValue.length - newLen) {
      throw new IndexOutOfBoundsException(
          "newOffset=" + newOffset + " newLen=" + newLen + " newValue.length=" + newValue.length);
    }

    // New value must be a single-entry packed payload: [PACKED][count=1][key:8] == 10 bytes.
    if (newLen != 2 + Long.BYTES || newValue[newOffset] != PACKED_FORMAT || newValue[newOffset + 1] != 1) {
      return PACKED_MERGE_NOT_APPLICABLE;
    }
    final int existingLen = HOTLeafPage.refLength(ref);
    if (existingLen < 2 || leaf.refByteAt(ref, 0) != PACKED_FORMAT) {
      return PACKED_MERGE_NOT_APPLICABLE;
    }
    final int count = leaf.refByteAt(ref, 1) & 0xFF;
    if (count == 0 || count > PACKED_THRESHOLD) {
      throw new IllegalArgumentException("Packed count must be in [1, " + PACKED_THRESHOLD + "]: " + count);
    }
    if (existingLen != 2 + count * Long.BYTES) {
      throw new IllegalArgumentException("Packed count " + count + " requires exactly " + (2 + count * Long.BYTES)
          + " bytes but slot has " + existingLen);
    }
    final long newKey = requireChunkBit16(readKeyBE(newValue, newOffset + 2));

    // Validate canonical strict ordering while locating the insertion point. Packed chunks contain
    // at most 64 entries, so this bounded linear pass is cheaper than letting a malformed ordering
    // make binary search silently miss an existing posting and is allocation-free on the hot path.
    int insertionIndex = count;
    boolean alreadyPresent = false;
    long previousKey = 0L;
    for (int i = 0; i < count; i++) {
      final long existingKey = requireChunkBit16(leaf.refLongBEAt(ref, 2 + i * Long.BYTES));
      if (i > 0 && Long.compareUnsigned(previousKey, existingKey) >= 0) {
        throw new IllegalArgumentException(
            "Packed posting keys must be strictly increasing at entries " + (i - 1) + " and " + i);
      }
      final int comparison = Long.compareUnsigned(existingKey, newKey);
      if (comparison == 0) {
        // Do not return until the bounded scan has validated the entire resident payload. A corrupt
        // suffix must not be hidden merely because the incoming bit happens to occur before it.
        alreadyPresent = true;
      }
      if (comparison > 0 && insertionIndex == count) {
        insertionIndex = i;
      }
      previousKey = existingKey;
    }

    if (alreadyPresent) {
      return PACKED_MERGE_UNCHANGED;
    }

    // A full bucket must switch representation, but only after its resident entries have passed
    // the same canonical/range validation as every smaller packed bucket. Returning early above the
    // scan would let a corrupt 64-entry payload escape into the copying fallback.
    if (count == PACKED_THRESHOLD) {
      return PACKED_MERGE_NOT_APPLICABLE;
    }

    return insertionIndex + 1;
  }

  /** Write one already-validated merge plan into caller-owned storage. */
  private static void writePackedSingleBitMerge(final HOTLeafPage leaf, final long ref, final byte[] newValue,
      final int newOffset, final int insertionIndex, final byte[] destination, final int destinationOffset,
      final int resultLen) {
    final int count = (HOTLeafPage.refLength(ref) - 2) / Long.BYTES;
    final long newKey = readKeyBE(newValue, newOffset + 2);
    destination[destinationOffset] = PACKED_FORMAT;
    destination[destinationOffset + 1] = (byte) (count + 1);
    final int insAt = destinationOffset + 2 + insertionIndex * Long.BYTES;
    if (insertionIndex > 0) {
      leaf.copyRefInto(ref, 2, destination, destinationOffset + 2, insertionIndex * Long.BYTES);
    }
    writeKeyBE(destination, insAt, newKey);
    if (insertionIndex < count) {
      leaf.copyRefInto(ref, 2 + insertionIndex * Long.BYTES, destination, insAt + Long.BYTES,
          (count - insertionIndex) * Long.BYTES);
    }
    assert resultLen == 2 + (count + 1) * Long.BYTES;
  }

  /**
   * Remove one posting bit from a packed payload while it is still resident in a HOT leaf slot.
   *
   * <p>
   * The applicable path is allocation-free: it validates the packed header, binary-searches the
   * sorted unsigned keys through {@link HOTLeafPage#refLongBEAt(long, int)}, and splices the two
   * surviving ranges directly into caller-owned scratch. The returned positive value is the exact
   * payload length written at {@code scratchOffset}. Primitive status constants distinguish a
   * different representation, an absent bit, and removal of the final bit without allocating a result
   * carrier.
   * </p>
   *
   * <p>
   * A payload bearing the packed marker must be canonical: its count is in
   * {@code [1, PACKED_THRESHOLD]} and its stored length is exactly {@code 2 + count * 8}. Violations
   * fail loudly instead of falling through to a copying path that could mistake corrupt postings for
   * an absent bit.
   * </p>
   *
   * @param leaf leaf holding the existing payload
   * @param ref packed value handle returned by {@link HOTLeafPage#valueRef(int)}
   * @param bit posting bit to remove, in the chunk-local unsigned-16-bit domain
   * @param scratch caller-owned destination for a non-empty result
   * @param scratchOffset first destination byte in {@code scratch}
   * @return {@link #PACKED_REMOVE_NOT_APPLICABLE}, {@link #PACKED_REMOVE_ABSENT},
   *         {@link #PACKED_REMOVE_EMPTY}, or the positive result length written to scratch
   * @throws IllegalArgumentException if {@code bit}, {@code ref}, or a packed payload is malformed
   * @throws IndexOutOfBoundsException if a required result does not fit in {@code scratch}
   */
  public static int removePackedSingleBitFromSlot(final HOTLeafPage leaf, final long ref, final long bit,
      final byte[] scratch, final int scratchOffset) {
    requireNonNull(leaf, "leaf cannot be null");
    requireNonNull(scratch, "scratch cannot be null");
    if ((bit & ~0xFFFFL) != 0L) {
      throw new IllegalArgumentException("posting-list chunk bit must be in [0, 65535]: " + bit);
    }
    if (scratchOffset < 0 || scratchOffset > scratch.length) {
      throw new IndexOutOfBoundsException("scratchOffset=" + scratchOffset + " scratch.length=" + scratch.length);
    }

    final int existingLen = HOTLeafPage.refLength(ref);
    if (existingLen < 0) {
      throw new IllegalArgumentException("Unreadable HOT leaf value reference");
    }
    if (existingLen == 0 || leaf.refByteAt(ref, 0) != PACKED_FORMAT) {
      return PACKED_REMOVE_NOT_APPLICABLE;
    }
    if (existingLen < 2) {
      throw new IllegalArgumentException("Packed payload has no count byte: length=" + existingLen);
    }

    final int count = leaf.refByteAt(ref, 1) & 0xFF;
    if (count == 0 || count > PACKED_THRESHOLD) {
      throw new IllegalArgumentException("Packed count must be in [1, " + PACKED_THRESHOLD + "]: " + count);
    }
    final int requiredLen = 2 + count * Long.BYTES;
    if (existingLen != requiredLen) {
      throw new IllegalArgumentException(
          "Packed count " + count + " requires exactly " + requiredLen + " bytes but slot has " + existingLen);
    }

    int removalIndex = -1;
    long previousKey = 0L;
    for (int i = 0; i < count; i++) {
      final long existingKey = requireChunkBit16(leaf.refLongBEAt(ref, 2 + i * Long.BYTES));
      if (i > 0 && Long.compareUnsigned(previousKey, existingKey) >= 0) {
        throw new IllegalArgumentException(
            "Packed posting keys must be strictly increasing at entries " + (i - 1) + " and " + i);
      }
      if (existingKey == bit) {
        removalIndex = i;
      }
      previousKey = existingKey;
    }
    if (removalIndex < 0) {
      return PACKED_REMOVE_ABSENT;
    }
    if (count == 1) {
      return PACKED_REMOVE_EMPTY;
    }

    final int resultLen = existingLen - Long.BYTES;
    if (scratchOffset > scratch.length - resultLen) {
      throw new IndexOutOfBoundsException("Packed removal result of " + resultLen
          + " bytes does not fit scratch at offset " + scratchOffset + " (length=" + scratch.length + ')');
    }
    scratch[scratchOffset] = PACKED_FORMAT;
    scratch[scratchOffset + 1] = (byte) (count - 1);

    final int prefixBytes = removalIndex * Long.BYTES;
    if (prefixBytes > 0) {
      leaf.copyRefInto(ref, 2, scratch, scratchOffset + 2, prefixBytes);
    }
    final int suffixEntries = count - removalIndex - 1;
    if (suffixEntries > 0) {
      leaf.copyRefInto(ref, 2 + (removalIndex + 1) * Long.BYTES, scratch, scratchOffset + 2 + prefixBytes,
          suffixEntries * Long.BYTES);
    }
    return resultLen;
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
   * Validate the domain stored inside one posting-list chunk before combining it with the chunk
   * index. Silently masking a wider value would alias it to an unrelated posting.
   */
  private static long requireChunkBit16(final long bit16) {
    if ((bit16 & ~0xFFFFL) != 0L) {
      throw new IllegalArgumentException(
          "Posting-list chunk bit must be in [0, 65535]: " + Long.toUnsignedString(bit16));
    }
    return bit16;
  }

  /** Validate a deserialized chunk by its maximum unsigned value, without an iterator or copy. */
  private static NodeReferences requireChunkReferences16(final NodeReferences references) {
    final Roaring64Bitmap bitmap = references.getNodeKeys();
    if (!bitmap.isEmpty()) {
      requireChunkBit16(bitmap.last());
    }
    return references;
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
      throw new IllegalArgumentException("Packed payload has no count byte");
    }

    final int count = bytes[offset] & 0xFF;
    if (count == 0 || count > PACKED_THRESHOLD) {
      throw new IllegalArgumentException("Packed count must be in [1, " + PACKED_THRESHOLD + "]: " + count);
    }

    // Canonical packed payloads have neither truncation nor ignored trailing bytes.
    final int requiredBytes = 1 + count * 8;
    if (requiredBytes != length) {
      throw new IllegalArgumentException(
          "Packed count " + count + " requires exactly " + requiredBytes + " bytes but has " + length);
    }

    final Roaring64Bitmap bitmap = new Roaring64Bitmap();

    int pos = offset + 1;
    long previousKey = 0L;
    for (int i = 0; i < count; i++) {
      final long key = readKeyBE(bytes, pos);
      if (i > 0 && Long.compareUnsigned(previousKey, key) >= 0) {
        throw new IllegalArgumentException(
            "Packed keys must be strictly increasing at entries " + (i - 1) + " and " + i);
      }
      bitmap.add(key);
      previousKey = key;
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
