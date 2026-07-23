/*
 * Copyright (c) 2023, Sirix Contributors
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

package io.sirix.page;

import io.sirix.node.LE;
import io.sirix.BinaryEncodingVersion;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.User;
import io.sirix.api.StorageEngineReader;
import io.sirix.cache.Allocators;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.index.IndexType;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.bytepipe.FFILz4Compressor;
import io.sirix.io.bytepipe.JavaLz4BlockDecoder;
import io.sirix.exception.SirixIOException;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import io.sirix.node.Utils;
import io.sirix.node.Bytes;
import io.sirix.node.interfaces.RecordSerializer;
import io.sirix.page.delegates.BitmapReferencesPage;
import io.sirix.page.delegates.FullReferencesPage;
import io.sirix.page.delegates.ReferencesPage4;
import io.sirix.page.interfaces.Page;
import io.sirix.node.DeltaVarIntCodec;
import io.sirix.node.StructuralKeyColumnCodec;
import io.sirix.page.pax.BooleanRegion;
import io.sirix.page.pax.NumberRegion;
import io.sirix.page.pax.ObjectKeyNameKeyRegion;
import io.sirix.page.pax.PathNodeKeyRegion;
import io.sirix.page.pax.RegionTable;
import io.sirix.page.pax.StringRegion;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import io.sirix.settings.VersioningType;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import io.sirix.node.BytesOut;
import io.sirix.node.BytesIn;
import io.sirix.node.MemorySegmentBytesIn;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.BitSet;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * All Page types.
 */
@SuppressWarnings("SwitchStatementWithTooFewBranches")
public enum PageKind {
  /**
   * {@link KeyValueLeafPage}.
   */
  KEYVALUELEAFPAGE((byte) 1, KeyValueLeafPage.class) {
    /**
     * Thread-local scratch for the compact directory read during slotted-page
     * deserialization. Allocating a fresh int[populatedCount] per page at
     * ~1M pages per scan × 20 threads showed up as 30% of all allocation
     * samples (async-profiler alloc mode). Capacity is NDP_NODE_COUNT so
     * it covers the worst case and never needs to grow.
     */
    private final ThreadLocal<int[]> compactDirScratch =
        ThreadLocal.withInitial(() -> new int[Constants.NDP_NODE_COUNT]);

    /**
     * Thread-local 160-byte scratch for reading the on-disk header + bitmap
     * section. Avoids a fresh new byte[160] on every page deserialize; at
     * 1M pages × 20 threads × N iters that was ~10% of byte[] allocation
     * samples.
     */
    private final ThreadLocal<byte[]> headerBitmapScratch =
        ThreadLocal.withInitial(() -> new byte[PageLayout.DISK_HEADER_BITMAP_SIZE]);

    @Override
    public Page deserializePage(final ResourceConfiguration resourceConfig, final BytesIn<?> source,
        final SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      final BinaryEncodingVersion binaryVersion = readVersionAndFlags(source);

      switch (binaryVersion) {
        case V0 -> { return deserializeSlottedPage(resourceConfig, source); }
        default -> throw new IllegalStateException("Unknown binary encoding version: " + binaryVersion);
      }
    }

    private Page deserializeSlottedPage(final ResourceConfiguration resourceConfig, final BytesIn<?> source) {
      // The offset-table template dedup (and the compressed heap codec) is
      // always active on the KVL on-disk wire format. A page with a
      // {@code templateCount == 0} marker falls back to the plain heap-copy
      // path — this is the degenerate case when a page's records have no
      // offset-table structure (e.g. raw slab bytes from test writes).
      final boolean offsetTableDedup = true;
      final long recordPageKey = Utils.getVarLong(source);
      final int revision = source.readInt();
      final IndexType indexType = IndexType.getType(source.readByte());

      final MemorySegmentAllocator memorySegmentAllocator = Allocators.getInstance();

      // 1. Read header (32B) + bitmap (128B) — 160 bytes (on-disk format, never changes)
      final byte[] headerBitmapBytes = headerBitmapScratch.get();
      source.read(headerBitmapBytes);
      final MemorySegment headerBitmapSeg = MemorySegment.ofArray(headerBitmapBytes);
      final int populatedCount = PageLayout.getPopulatedCount(headerBitmapSeg);

      // 2. Read the prefix of uncompressed fixed-size headers:
      //   - populatedCount (int)           — matches the compactDir array length
      //   - onDiskHeapSize (int)           — needed to size the decompress output
      //   - templateCount  (byte)          — > 0 when offset-table dedup is active
      //   - templatePoolBytes (int)        — size of the template pool inside the
      //                                       compressed blob (0 when templateCount == 0)
      //
      // The remaining data (compactDir + templatePool + slotTemplateIds + heap)
      // is delivered as a single LZ4/ZeroRunByteCodec-compressed blob. This gives
      // the codec cross-section visibility (the old outer-page LZ4 was already
      // doing this for us; folding that pass into the inner encoder lets us
      // drop the outer pipeline entirely for `-Dsirix.compression=none` parity).
      final int populatedCountHeader = source.readInt();
      if (populatedCountHeader != populatedCount) {
        throw new SirixIOException(
            "populatedCount header mismatch: bitmap=" + populatedCount
                + " prefix=" + populatedCountHeader);
      }
      final int onDiskHeapSize = source.readInt();

      // Read the template pool + per-slot templateIds upfront so we can
      // compute the in-memory heap size before allocating the slotted page.
      final int templateCount;
      final int templatePoolBytes;
      final byte[] templatePool;
      final byte[] slotTemplateIds;
      final int[] templateOffsets;
      final int[] inMemDataLengths;
      final byte[] zeroHashBitmap;
      final boolean hashElisionActive;
      final long[] parentKeyValues;
      final byte[] parentKeyWidths;
      final boolean parentKeyColumnActive;
      final byte[] pathNodeKeyColumnBytes; // raw PathNodeKeyRegion payload (bitmap-indexed)
      final byte[] pathNodeKeyWidths;      // per-slot varint width after reinject
      final boolean pathNodeKeyColumnActive;
      final boolean valueElisionActive;
      final byte[] valueElidedTypes;       // per-elided-slot type byte (length = elidedCount)
      final short[] valueOffs;             // per-slot value offset (in-data offset for fused-NUMBER)
      final byte[] valueWidths;            // per-slot value width (post-inject width on heap)
      // Lever 4: nameKey-elision per-slot scratches.
      final boolean nameKeyElisionActive;
      final short[] nameKeyOffs;           // per-slot nameKey field offset
      final byte[] nameKeyWidths;          // per-slot nameKey width on the in-memory heap
      final int[] compactDir = compactDirScratch.get();
      int inMemHeapSize;
      if (offsetTableDedup) {
        templateCount = source.readByte() & 0xFF;

        if (templateCount == 0) {
          // Degenerate / fallback: no dedup was possible. The writer staged
          // compactDir + heap into a single blob and compressed it; we now
          // decompress and parse the compactDir, leaving the heap bytes to be
          // materialized into the slotted-page allocation further below.
          templatePoolBytes = source.readInt();
          templatePool = null;
          templateOffsets = null;
          slotTemplateIds = null;
          inMemDataLengths = null;
          zeroHashBitmap = null;
          hashElisionActive = false;
          parentKeyValues = null;
          parentKeyWidths = null;
          parentKeyColumnActive = false;
          pathNodeKeyColumnBytes = null;
          pathNodeKeyWidths = null;
          pathNodeKeyColumnActive = false;
          valueElisionActive = false;
          valueElidedTypes = null;
          valueOffs = null;
          valueWidths = null;
          nameKeyElisionActive = false;
          nameKeyOffs = null;
          nameKeyWidths = null;
          inMemHeapSize = onDiskHeapSize;

          final int compactDirBytes = 4 * populatedCount;
          final int totalBlobBytes = compactDirBytes + onDiskHeapSize;
          final int compressedLen = source.readInt();
          final byte codec = source.readByte();

          final MemorySegment blobStaging = v1StagingScratch(totalBlobBytes);
          if (codec == 0) {
            final byte[] rleBuf = V1_HEAP_RLE_SCRATCH.get();
            final byte[] rle = rleBuf.length >= compressedLen ? rleBuf : new byte[compressedLen];
            if (rle != rleBuf) {
              V1_HEAP_RLE_SCRATCH.set(rle);
            }
            source.read(rle, 0, compressedLen);
            ZeroRunByteCodec.decode(rle, 0, compressedLen, blobStaging, 0L);
          } else if (codec == 2) {
            final byte[] rleBuf = V1_HEAP_RLE_SCRATCH.get();
            final byte[] rle = rleBuf.length >= compressedLen ? rleBuf : new byte[compressedLen];
            if (rle != rleBuf) {
              V1_HEAP_RLE_SCRATCH.set(rle);
            }
            source.read(rle, 0, compressedLen);
            ByteRunCodec.decode(rle, 0, compressedLen, blobStaging, 0L);
          } else if (codec == 3) {
            final byte[] rleBuf = V1_HEAP_RLE_SCRATCH.get();
            final byte[] rle = rleBuf.length >= compressedLen ? rleBuf : new byte[compressedLen];
            if (rle != rleBuf) {
              V1_HEAP_RLE_SCRATCH.set(rle);
            }
            source.read(rle, 0, compressedLen);
            SirixLZ77Codec.decode(rle, 0, compressedLen, blobStaging, 0L);
          } else {
            throw new SirixIOException("unknown inline-body codec: " + codec);
          }

          // Parse compactDir from the first section of the blob.
          for (int i = 0; i < populatedCount; i++) {
            final int b0 = blobStaging.get(ValueLayout.JAVA_BYTE, (long) i * 4) & 0xFF;
            final int b1 = blobStaging.get(ValueLayout.JAVA_BYTE, (long) i * 4 + 1) & 0xFF;
            final int b2 = blobStaging.get(ValueLayout.JAVA_BYTE, (long) i * 4 + 2) & 0xFF;
            final int b3 = blobStaging.get(ValueLayout.JAVA_BYTE, (long) i * 4 + 3) & 0xFF;
            compactDir[i] = (b0 << 24) | (b1 << 16) | (b2 << 8) | b3;
          }
          // Stash the staging blob and the heap offset (compactDirBytes) so
          // the record-expansion loop below consumes from the correct spot.
          BLOB_HEAP_OFFSET_HOLDER.set((long) compactDirBytes);
          BLOB_STAGING_HOLDER.set(blobStaging);
        } else {
          // Dedup path. Read the structural-flags byte + the compressed blob
          // header (length + codec) and decompress into the staging buffer.
          // The blob holds, in order:
          //   compactDir bytes (4 × populatedCount)
          //   templatePool bytes (templatePoolBytes)
          //   slotTemplateIds (populatedCount bytes)
          //   [if hashElisionActive] zeroHashBitmap (ceil(populatedCount/8) bytes)
          //   [if parentKeyColumnActive] int columnLen + column bytes
          //   heap bytes (onDiskHeapSize)
          final int structuralFlags = source.readByte() & 0xFF;
          hashElisionActive = (structuralFlags & STRUCT_FLAG_HASH_ELISION) != 0;
          parentKeyColumnActive = (structuralFlags & STRUCT_FLAG_PARENT_KEY_COLUMN) != 0;
          pathNodeKeyColumnActive = (structuralFlags & STRUCT_FLAG_PATH_NODE_KEY_COLUMN) != 0;
          valueElisionActive = (structuralFlags & STRUCT_FLAG_VALUE_ELISION) != 0;
          nameKeyElisionActive = (structuralFlags & STRUCT_FLAG_NAME_KEY_ELISION) != 0;
          templatePoolBytes = source.readInt();

          final int compactDirBytes = 4 * populatedCount;
          final int hashBitmapBytes = hashElisionActive
              ? ((populatedCount + 7) >>> 3) : 0;
          // parentKey column length is inside the blob, but we need to know
          // the blob size upfront to decompress. Read the column-length int
          // (after the fixed-size sections) by pre-reading the first 4 bytes
          // of the column chunk after decompression — we don't need a separate
          // header because the column length is stored inside the blob
          // immediately after zeroHashBitmap.
          final int compressedLen = source.readInt();
          final byte codec = source.readByte();

          // Two-phase decompress: we don't know the parentKey column bytes
          // until we've decoded enough of the blob to parse its 4-byte length
          // prefix. Since the blob is always decompressed in full, we size
          // the staging buffer pessimistically and trust totalBlobBytes to
          // match after the fact.
          //
          // Total blob bytes we'll verify:
          //   structural = compactDir + templatePool + slotTemplateIds + hashBitmap + (4 + colLen)
          //   blob = structural + onDiskHeapSize
          //
          // Because colLen is inside the compressed blob, we size upper bound
          // via uncompressedSize embedded in the codec's frame header.
          // ZeroRunByteCodec uses an explicit uncompressedSize varint, and
          // LZ4 gives us the exact uncompressed length via decompress's
          // return value. We therefore allocate using onDiskHeapSize +
          // maxStructural where maxStructural includes a worst-case
          // parentKey column of populatedCount × 10 bytes.
          final int maxParentKeyColBytes = parentKeyColumnActive
              ? 4 + populatedCount * 11 : 0;
          // pathNodeKey column size upper bound: 4 (len) + 1 + 256*4 + 2 + 128 + slotCount.
          final int maxPathNodeKeyColBytes = pathNodeKeyColumnActive
              ? 4 + 1 + 256 * 4 + 2 + 128 + populatedCount : 0;
          // valueElision section size upper bound: 4 (len) + 2 bytes/elided slot.
          final int maxValueElisionBytes = valueElisionActive
              ? 4 + (populatedCount * 2) : 0;
          // nameKeyElision section size upper bound: 4 (len) + 1 byte/elided slot.
          final int maxNameKeyElisionBytes = nameKeyElisionActive
              ? 4 + populatedCount : 0;
          final int maxBlobBytes = compactDirBytes + templatePoolBytes + populatedCount
              + hashBitmapBytes + maxParentKeyColBytes + maxPathNodeKeyColBytes
              + maxValueElisionBytes + maxNameKeyElisionBytes + onDiskHeapSize;

          final MemorySegment blobStaging = v1StagingScratch(maxBlobBytes);
          final int actualBlobBytes;
          if (codec == 1) {
            final MemorySegment compressedIn = v1Lz4OutScratch(compressedLen);
            final byte[] tmp = V1_HEAP_RLE_SCRATCH.get();
            final byte[] tmpBuf = tmp.length >= compressedLen ? tmp : new byte[compressedLen];
            if (tmpBuf != tmp) {
              V1_HEAP_RLE_SCRATCH.set(tmpBuf);
            }
            source.read(tmpBuf, 0, compressedLen);
            MemorySegment.copy(tmpBuf, 0, compressedIn, ValueLayout.JAVA_BYTE, 0L, compressedLen);
            final FFILz4Compressor lz4 = V1_HEAP_LZ4.get();
            final MemorySegment blobView = blobStaging.asSlice(0, maxBlobBytes);
            if (lz4 == null) {
              // Pure-Java fallback: LZ4-bodied pages stay readable without liblz4.
              actualBlobBytes = JavaLz4BlockDecoder.decompressSafe(
                  compressedIn, 0L, compressedLen, blobView, 0L, maxBlobBytes);
            } else {
              actualBlobBytes = lz4.decompressSegment(
                  compressedIn.asSlice(0, compressedLen), blobView, compressedLen);
              if (actualBlobBytes < 0) {
                throw new SirixIOException("body LZ4 decompress returned " + actualBlobBytes);
              }
            }
          } else if (codec == 0) {
            final byte[] rleBuf = V1_HEAP_RLE_SCRATCH.get();
            final byte[] rle = rleBuf.length >= compressedLen ? rleBuf : new byte[compressedLen];
            if (rle != rleBuf) {
              V1_HEAP_RLE_SCRATCH.set(rle);
            }
            source.read(rle, 0, compressedLen);
            actualBlobBytes = ZeroRunByteCodec.decode(rle, 0, compressedLen, blobStaging, 0L);
          } else if (codec == 2) {
            final byte[] rleBuf = V1_HEAP_RLE_SCRATCH.get();
            final byte[] rle = rleBuf.length >= compressedLen ? rleBuf : new byte[compressedLen];
            if (rle != rleBuf) {
              V1_HEAP_RLE_SCRATCH.set(rle);
            }
            source.read(rle, 0, compressedLen);
            actualBlobBytes = ByteRunCodec.decode(rle, 0, compressedLen, blobStaging, 0L);
          } else if (codec == 3) {
            final byte[] rleBuf = V1_HEAP_RLE_SCRATCH.get();
            final byte[] rle = rleBuf.length >= compressedLen ? rleBuf : new byte[compressedLen];
            if (rle != rleBuf) {
              V1_HEAP_RLE_SCRATCH.set(rle);
            }
            source.read(rle, 0, compressedLen);
            actualBlobBytes = SirixLZ77Codec.decode(rle, 0, compressedLen, blobStaging, 0L);
          } else {
            throw new SirixIOException("unknown body codec: " + codec);
          }

          // Parse compactDir from the decompressed blob (big-endian 4-byte ints).
          long blobPos = 0;
          for (int i = 0; i < populatedCount; i++) {
            final int b0 = blobStaging.get(ValueLayout.JAVA_BYTE, blobPos) & 0xFF;
            final int b1 = blobStaging.get(ValueLayout.JAVA_BYTE, blobPos + 1) & 0xFF;
            final int b2 = blobStaging.get(ValueLayout.JAVA_BYTE, blobPos + 2) & 0xFF;
            final int b3 = blobStaging.get(ValueLayout.JAVA_BYTE, blobPos + 3) & 0xFF;
            compactDir[i] = (b0 << 24) | (b1 << 16) | (b2 << 8) | b3;
            blobPos += 4;
          }
          // Parse template pool from the blob.
          templatePool = TEMPLATE_POOL_SCRATCH.get();
          if (templatePool.length < templatePoolBytes) {
            throw new SirixIOException("template pool too large: " + templatePoolBytes);
          }
          MemorySegment.copy(blobStaging, ValueLayout.JAVA_BYTE, blobPos,
              templatePool, 0, templatePoolBytes);
          blobPos += templatePoolBytes;

          templateOffsets = TEMPLATE_OFFSETS_SCRATCH.get();
          OffsetTableTemplatePool.parseTemplateOffsets(templatePool, templatePoolBytes,
              templateCount, templateOffsets);

          slotTemplateIds = SLOT_TEMPLATE_IDS_SCRATCH.get();
          if (slotTemplateIds.length < populatedCount) {
            throw new SirixIOException("slot template ids buffer too small: " + populatedCount);
          }
          MemorySegment.copy(blobStaging, ValueLayout.JAVA_BYTE, blobPos,
              slotTemplateIds, 0, populatedCount);
          blobPos += populatedCount;

          // Read zero-hash bitmap when hash elision is active.
          if (hashElisionActive) {
            zeroHashBitmap = SLOT_ZERO_HASH_BITMAP_SCRATCH.get();
            MemorySegment.copy(blobStaging, ValueLayout.JAVA_BYTE, blobPos,
                zeroHashBitmap, 0, hashBitmapBytes);
            blobPos += hashBitmapBytes;
          } else {
            zeroHashBitmap = null;
          }

          // Read parentKey column when active. Column bytes are stored
          // behind a 4-byte length prefix so we can bound the slice.
          if (parentKeyColumnActive) {
            final int cb0 = blobStaging.get(ValueLayout.JAVA_BYTE, blobPos) & 0xFF;
            final int cb1 = blobStaging.get(ValueLayout.JAVA_BYTE, blobPos + 1) & 0xFF;
            final int cb2 = blobStaging.get(ValueLayout.JAVA_BYTE, blobPos + 2) & 0xFF;
            final int cb3 = blobStaging.get(ValueLayout.JAVA_BYTE, blobPos + 3) & 0xFF;
            final int colLen = (cb0 << 24) | (cb1 << 16) | (cb2 << 8) | cb3;
            blobPos += 4;
            if (colLen < 0 || colLen > maxParentKeyColBytes - 4) {
              throw new SirixIOException("invalid parentKey column length: " + colLen);
            }
            byte[] scratch = PARENT_KEY_COLUMN_SCRATCH.get();
            if (scratch.length < colLen) {
              scratch = new byte[Math.max(colLen, scratch.length * 2)];
              PARENT_KEY_COLUMN_SCRATCH.set(scratch);
            }
            MemorySegment.copy(blobStaging, ValueLayout.JAVA_BYTE, blobPos,
                scratch, 0, colLen);
            blobPos += colLen;
            parentKeyValues = SLOT_PARENT_KEY_SCRATCH.get();
            for (int i = 0; i < populatedCount; i++) {
              parentKeyValues[i] = StructuralKeyColumnCodec.decodeSlot(scratch, 0, i);
            }
            parentKeyWidths = SLOT_PARENT_KEY_WIDTH_SCRATCH.get();
          } else {
            parentKeyValues = null;
            parentKeyWidths = null;
          }

          // Read pathNodeKey column when active. Layout: int length prefix + payload.
          if (pathNodeKeyColumnActive) {
            final int pb0 = blobStaging.get(ValueLayout.JAVA_BYTE, blobPos) & 0xFF;
            final int pb1 = blobStaging.get(ValueLayout.JAVA_BYTE, blobPos + 1) & 0xFF;
            final int pb2 = blobStaging.get(ValueLayout.JAVA_BYTE, blobPos + 2) & 0xFF;
            final int pb3 = blobStaging.get(ValueLayout.JAVA_BYTE, blobPos + 3) & 0xFF;
            final int pnkColLen = (pb0 << 24) | (pb1 << 16) | (pb2 << 8) | pb3;
            blobPos += 4;
            if (pnkColLen < 0 || pnkColLen > maxPathNodeKeyColBytes - 4) {
              throw new SirixIOException("invalid pathNodeKey column length: " + pnkColLen);
            }
            byte[] pnkScratch = PATH_NODE_KEY_COLUMN_SCRATCH.get();
            if (pnkScratch.length < pnkColLen) {
              pnkScratch = new byte[Math.max(pnkColLen, pnkScratch.length * 2)];
              PATH_NODE_KEY_COLUMN_SCRATCH.set(pnkScratch);
            }
            MemorySegment.copy(blobStaging, ValueLayout.JAVA_BYTE, blobPos,
                pnkScratch, 0, pnkColLen);
            blobPos += pnkColLen;
            pathNodeKeyColumnBytes = pnkScratch;
            pathNodeKeyWidths = SLOT_PATH_NODE_KEY_WIDTH_SCRATCH.get();
          } else {
            pathNodeKeyColumnBytes = null;
            pathNodeKeyWidths = null;
          }

          // Read value-elision section when active. Layout: int elidedCount +
          // elidedCount × (1 byte type, 1 byte width). In slot-ascending order.
          // After read we expand into per-slot scratches: SLOT_VALUE_TYPE_READ
          // (length elidedCount) holds type bytes packed; valueOffs and
          // valueWidths are populated by the per-slot pre-walk below.
          if (valueElisionActive) {
            final int vb0 = blobStaging.get(ValueLayout.JAVA_BYTE, blobPos) & 0xFF;
            final int vb1 = blobStaging.get(ValueLayout.JAVA_BYTE, blobPos + 1) & 0xFF;
            final int vb2 = blobStaging.get(ValueLayout.JAVA_BYTE, blobPos + 2) & 0xFF;
            final int vb3 = blobStaging.get(ValueLayout.JAVA_BYTE, blobPos + 3) & 0xFF;
            final int elidedCount = (vb0 << 24) | (vb1 << 16) | (vb2 << 8) | vb3;
            blobPos += 4;
            if (elidedCount < 0 || elidedCount > populatedCount) {
              throw new SirixIOException("invalid value-elision count: " + elidedCount);
            }
            byte[] typeScratch = SLOT_VALUE_TYPE_READ_SCRATCH.get();
            final int typeBytes = elidedCount * 2;
            if (typeScratch.length < typeBytes) {
              typeScratch = new byte[Math.max(typeBytes, typeScratch.length * 2)];
              SLOT_VALUE_TYPE_READ_SCRATCH.set(typeScratch);
            }
            MemorySegment.copy(blobStaging, ValueLayout.JAVA_BYTE, blobPos,
                typeScratch, 0, typeBytes);
            blobPos += typeBytes;
            valueElidedTypes = typeScratch;
            valueOffs = SLOT_VALUE_OFF_SCRATCH.get();
            valueWidths = SLOT_VALUE_WIDTH_SCRATCH.get();
          } else {
            valueElidedTypes = null;
            valueOffs = null;
            valueWidths = null;
          }

          // Lever 4: read name-key elision section when active. Layout:
          //   int elidedCount + elidedCount × (1 byte width). In slot-ascending order.
          // We expand into nameKeyOffs/nameKeyWidths during the per-slot pre-walk
          // below so the strip-pass and re-inject pass have direct slot indexing.
          //
          // The elided-slot widths buffer is read into the per-thread
          // SLOT_NAME_KEY_WIDTH_PACKED_SCRATCH (length = elidedCount). The
          // pre-walk maps these to per-slot widths via a slot-ascending cursor.
          final byte[] nameKeyElidedWidthsPacked;
          if (nameKeyElisionActive) {
            final int nb0 = blobStaging.get(ValueLayout.JAVA_BYTE, blobPos) & 0xFF;
            final int nb1 = blobStaging.get(ValueLayout.JAVA_BYTE, blobPos + 1) & 0xFF;
            final int nb2 = blobStaging.get(ValueLayout.JAVA_BYTE, blobPos + 2) & 0xFF;
            final int nb3 = blobStaging.get(ValueLayout.JAVA_BYTE, blobPos + 3) & 0xFF;
            final int elidedCount = (nb0 << 24) | (nb1 << 16) | (nb2 << 8) | nb3;
            blobPos += 4;
            if (elidedCount < 0 || elidedCount > populatedCount) {
              throw new SirixIOException("invalid name-key elision count: " + elidedCount);
            }
            byte[] widthScratch = SLOT_NAME_KEY_WIDTH_PACKED_SCRATCH.get();
            if (widthScratch.length < elidedCount) {
              widthScratch = new byte[Math.max(elidedCount, widthScratch.length * 2)];
              SLOT_NAME_KEY_WIDTH_PACKED_SCRATCH.set(widthScratch);
            }
            MemorySegment.copy(blobStaging, ValueLayout.JAVA_BYTE, blobPos,
                widthScratch, 0, elidedCount);
            blobPos += elidedCount;
            nameKeyElidedWidthsPacked = widthScratch;
            nameKeyOffs = SLOT_NAME_KEY_OFF_SCRATCH.get();
            nameKeyWidths = SLOT_NAME_KEY_WIDTH_SCRATCH.get();
          } else {
            nameKeyElidedWidthsPacked = null;
            nameKeyOffs = null;
            nameKeyWidths = null;
          }

          // Compute in-memory lengths:
          //   onDiskLen + (fc - 1) + (hashStripped ? 8 : 0) + parentKeyWidth(slot)
          //   + pnkWidth(slot) + valueWidth(slot)
          // parentKeyWidth and pnkWidth are derived from the template for slots
          // whose kind has the corresponding field; we compute them once per slot
          // and stash them for the record-expansion loop below. valueWidth is
          // read from the value-elision section when value-elision is active.
          //
          // The pathNodeKey column is bitmap-indexed by slot (0..1023), so we
          // walk the page bitmap in parallel with the populated-slot loop to
          // map entry index i → slot bit for PathNodeKeyRegion lookup.
          inMemDataLengths = SLOT_DATALEN_SCRATCH.get();
          int running = 0;
          // Walk the bitmap in parallel to get each entry's slot bit.
          int bmIdx = 0;
          long bmWord = 0L;
          // Counter into the per-elided-slot (type, width) section. Increments
          // each time we encounter a fused-NUMBER slot when value-elision active.
          int valueElidedReadCursor = 0;
          // Lever 4: cursor into the nameKey-elision packed-widths section.
          // Increments each time we encounter a fused OBJECT_NAMED_* slot
          // when name-key-elision is active.
          int nameKeyElidedReadCursor = 0;
          for (int i = 0; i < populatedCount; i++) {
            while (bmWord == 0) {
              bmWord = PageLayout.getBitmapWord(headerBitmapSeg, bmIdx++);
              if (bmIdx > PageLayout.BITMAP_WORDS) {
                throw new SirixIOException(
                    "bitmap exhausted at entry " + i + " / " + populatedCount);
              }
            }
            final int slotBit = ((bmIdx - 1) << 6)
                | Long.numberOfTrailingZeros(bmWord);
            bmWord &= bmWord - 1;
            final int onDiskLen = compactDir[i] >>> 8;
            final int kindId = compactDir[i] & 0xFF;
            final int templateId = slotTemplateIds[i] & 0xFF;
            final int fc = OffsetTableTemplatePool.templateFieldCount(templatePool, templateOffsets, templateId);
            if (OffsetTableTemplatePool.templateKindId(templatePool, templateOffsets, templateId) != kindId) {
              throw new SirixIOException(
                  "V1 kindId mismatch at slot " + i + ": compactDir=" + kindId
                      + " template=" + OffsetTableTemplatePool.templateKindId(templatePool, templateOffsets, templateId));
            }
            int inMemLen = onDiskLen + (fc - 1);
            if (hashElisionActive && ((zeroHashBitmap[i >>> 3] >>> (i & 7)) & 1) == 1) {
              inMemLen += NodeFieldLayout.HASH_WIDTH;
            }
            int pkWidth = 0;
            if (parentKeyColumnActive) {
              final int pkFieldIdx = NodeFieldLayout.parentKeyFieldIndexForKind(kindId);
              if (pkFieldIdx >= 0
                  && parentKeyValues[i] != Fixed.NULL_NODE_KEY.getStandardProperty()) {
                // parentKey is always field 0 so offset = 0, width = offset[1].
                // Compute width via the template's next-field offset or (if
                // it's the only field) the on-disk record's dataBytes = onDiskLen - 2.
                final int pkOff = OffsetTableTemplatePool.templateFieldOffset(templatePool,
                    templateOffsets, templateId, pkFieldIdx);
                pkWidth = OffsetTableTemplatePool.templateFieldWidth(templatePool,
                    templateOffsets, templateId, pkFieldIdx,
                    (onDiskLen - 2) + /* re-add stripped bytes */
                        ((hashElisionActive && ((zeroHashBitmap[i >>> 3] >>> (i & 7)) & 1) == 1)
                            ? NodeFieldLayout.HASH_WIDTH : 0));
                // The template's dataBytes refers to IN-MEMORY layout, so we
                // must pass the in-memory dataBytes here (post-hash-reinject).
                // Actually templateFieldOffset gives offset from dataStart of
                // IN-MEMORY layout; width is the same in in-memory view. We
                // used onDiskLen - 2 above which is the on-disk dataBytes
                // AFTER hash strip; add HASH_WIDTH back when applicable AND
                // add pkWidth itself — but that requires an iterative fixpoint.
                //
                // Simpler: for ALL kinds in use, parentKey is field 0 so
                // offset = 0 and next-field offset = templateOffsets[templateId][fieldIdx=1].
                // For fc > 1 (all kinds that have parentKey), the width is
                // simply template.fieldOffset(1).
                if (fc > 1) {
                  pkWidth = OffsetTableTemplatePool.templateFieldOffset(templatePool,
                      templateOffsets, templateId, 1) - pkOff;
                }
                inMemLen += pkWidth;
              }
            }
            // pathNodeKey width reconstruction: read from the template via its
            // offset-table entries. pnk is at a kind-specific interior index,
            // never field 0, so unlike parentKey we cannot assume pnkOff = 0.
            // Width is template.offset[pnkFieldIdx+1] - template.offset[pnkFieldIdx]
            // for non-terminal fields; for the (rare) case of pnk at the last
            // field, we use templateFieldWidth with the reconstructed dataBytes.
            int pnkWidth = 0;
            if (pathNodeKeyColumnActive && pathNodeKeyColumnBytes != null) {
              final int pnkFieldIdx = NodeFieldLayout.pathNodeKeyFieldIndexForKind(kindId);
              if (pnkFieldIdx >= 0 && fc > 1) {
                // Consult the column's bitmap: if the bit for this slot is set
                // the writer stripped the pnk varint and we must reinject.
                // Otherwise the pnk was not a column participant (e.g. pnk
                // width was pathological / 0) — keep the bytes inline.
                final int pnkLookup = PathNodeKeyRegion.pathNodeKeyForSlot(
                    pathNodeKeyColumnBytes, slotBit);
                if (pnkLookup >= 0) {
                  final int pnkOff = OffsetTableTemplatePool.templateFieldOffset(
                      templatePool, templateOffsets, templateId, pnkFieldIdx);
                  int computed;
                  if (pnkFieldIdx + 1 < fc) {
                    computed = OffsetTableTemplatePool.templateFieldOffset(
                        templatePool, templateOffsets, templateId, pnkFieldIdx + 1) - pnkOff;
                  } else {
                    // Last-field case — ALL kinds with pnk actually have it at
                    // a non-terminal index in real data (by inspection of
                    // NodeFieldLayout.pathNodeKeyFieldIndexForKind), so this
                    // branch is dead. Compute via remaining-dataBytes for safety.
                    final int postStrippedDataBytes = (onDiskLen - 2);
                    final int reAddedHash = (hashElisionActive
                        && ((zeroHashBitmap[i >>> 3] >>> (i & 7)) & 1) == 1)
                        ? NodeFieldLayout.HASH_WIDTH : 0;
                    computed = OffsetTableTemplatePool.templateFieldWidth(
                        templatePool, templateOffsets, templateId, pnkFieldIdx,
                        postStrippedDataBytes + reAddedHash + pkWidth);
                  }
                  if (computed > 0 && computed <= 10) {
                    pnkWidth = computed;
                    inMemLen += pnkWidth;
                  }
                }
              }
            }
            // Value-width re-injection: for elided fused-primitive slots when
            // value-elision is active, the on-disk record is missing the value
            // payload bytes. The reader reads the on-disk (type, width) pair
            // from the value-elision section and adds the width back to
            // inMemLen so the slot's heap layout matches the unelided original.
            //
            // The on-disk per-slot byte 0 carries:
            //   NUMBER  -> typeByte (2 or 3)
            //   STRING  -> 0 (placeholder)
            //   BOOLEAN -> 0 (placeholder)
            // The reader dispatches on compactDir kindId — typeByte is only
            // consulted by the inject pass for NUMBER (to pick INTEGER vs LONG
            // varint width).
            int valueWidth = 0;
            if (valueElisionActive
                && (kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_NUMBER_KIND_ID
                    || kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_STRING_KIND_ID
                    || kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_BOOLEAN_KIND_ID)) {
              // Each elided primitive slot consumes 2 bytes from valueElidedTypes
              // (type, width) in slot-ascending order.
              if (valueElidedReadCursor + 1 >= valueElidedTypes.length) {
                throw new SirixIOException(
                    "value-elision section truncated at slot " + i);
              }
              valueWidth = valueElidedTypes[valueElidedReadCursor + 1] & 0xFF;
              // BOOLEAN width is exactly 1; STRING is 1 (compressed flag) +
              // varint(length) + length bytes; NUMBER is up to 11 (1 type +
              // up to 10 varint bytes for a long). The writer pre-scan caps at
              // 0xFF so width is always representable in the on-disk byte.
              if (valueWidth <= 0) {
                throw new SirixIOException(
                    "invalid value-elision width at slot " + i + ": " + valueWidth);
              }
              inMemLen += valueWidth;
              // Compute the in-data offset of the value field. All three
              // primitive-fused kinds (48/49/50) put their value at field
              // index 8 (NodeFieldLayout.OBJNAMEDNUM_PAYLOAD ==
              // OBJNAMEDSTR_PAYLOAD == OBJNAMEDBOOL_VALUE == 8).
              valueOffs[i] = (short) OffsetTableTemplatePool.templateFieldOffset(
                  templatePool, templateOffsets, templateId, NodeFieldLayout.OBJNAMEDNUM_PAYLOAD);
              valueWidths[i] = (byte) valueWidth;
              // Cursor advances by 2 (type + width) regardless.
              valueElidedReadCursor += 2;
            } else if (valueElisionActive) {
              // Slot's kind has no fused-primitive payload (e.g. fused-NULL,
              // structural-fused, plain leaf): zero out so inject treats no-op.
              valueWidths[i] = 0;
            }
            // Lever 4: name-key width re-injection. For fused OBJECT_NAMED_*
            // (48-51) slots the on-disk record has its [signed-varint nameKey]
            // field stripped. The reader reads the per-slot 1-byte width from
            // the packed-widths section and adds it back to inMemLen so the
            // slot's heap layout matches the unelided original.
            int nameKeyWidthLocal = 0;
            if (nameKeyElisionActive
                && KeyValueLeafPage.isFusedObjectNamedKindId(kindId)) {
              if (nameKeyElidedReadCursor >= nameKeyElidedWidthsPacked.length) {
                throw new SirixIOException(
                    "name-key elision section truncated at slot " + i);
              }
              nameKeyWidthLocal = nameKeyElidedWidthsPacked[nameKeyElidedReadCursor] & 0xFF;
              nameKeyElidedReadCursor++;
              if (nameKeyWidthLocal < 1 || nameKeyWidthLocal > 5) {
                throw new SirixIOException(
                    "invalid name-key elision width at slot " + i + ": " + nameKeyWidthLocal);
              }
              inMemLen += nameKeyWidthLocal;
              // The nameKey field index is kind-specific: 3 for primitives
              // (kindIds 48-51) and 5 for the Phase 1-reserved structurals
              // (52-53). NodeFieldLayout.nameKeyFieldIndexForKind handles
              // both ranges; for the primitive subset that this branch hits
              // it always returns 3.
              final int nameKeyFieldIdx = NodeFieldLayout.nameKeyFieldIndexForKind(kindId);
              nameKeyOffs[i] = (short) OffsetTableTemplatePool.templateFieldOffset(
                  templatePool, templateOffsets, templateId, nameKeyFieldIdx);
              nameKeyWidths[i] = (byte) nameKeyWidthLocal;
            } else if (nameKeyElisionActive) {
              // Slot's kind is not a fused OBJECT_NAMED_*: no nameKey strip,
              // no inject. Zero out so the inject pass treats it as no-op.
              nameKeyWidths[i] = 0;
            }
            inMemDataLengths[i] = inMemLen;
            // Stash widths for the expansion loop.
            if (parentKeyColumnActive && parentKeyWidths != null) {
              parentKeyWidths[i] = (byte) pkWidth;
            }
            if (pathNodeKeyColumnActive && pathNodeKeyWidths != null) {
              pathNodeKeyWidths[i] = (byte) pnkWidth;
            }
            running += inMemLen;
          }
          inMemHeapSize = running;

          // Expose the blob-heap offset so the record-expansion loop below can
          // consume from the correct position.
          BLOB_HEAP_OFFSET_HOLDER.set(blobPos);
          BLOB_STAGING_HOLDER.set(blobStaging);
        }
      } else {
        templateCount = 0;
        templatePool = null;
        templateOffsets = null;
        slotTemplateIds = null;
        inMemDataLengths = null;
        zeroHashBitmap = null;
        hashElisionActive = false;
        parentKeyValues = null;
        parentKeyWidths = null;
        parentKeyColumnActive = false;
        pathNodeKeyColumnBytes = null;
        pathNodeKeyWidths = null;
        pathNodeKeyColumnActive = false;
        valueElisionActive = false;
        valueElidedTypes = null;
        valueOffs = null;
        valueWidths = null;
        nameKeyElisionActive = false;
        nameKeyOffs = null;
        nameKeyWidths = null;
        inMemHeapSize = onDiskHeapSize;
        templatePoolBytes = 0;
        source.readInts(compactDir, 0, populatedCount);
      }

      // 4. Allocate slotted page MemorySegment — size to actual (in-memory) heap content.
      // The allocator rounds up to its next power-of-two size class (4/8/16/32/
      // 64/128/256 KiB), so we don't need to pre-round. Dropping the legacy
      // INITIAL_PAGE_SIZE floor lets pages with small heaps (e.g. path-summary
      // pages, sparsely-populated data pages) fall into smaller size classes —
      // 32 KiB instead of 64 KiB — doubling effective cache capacity for those
      // pages. Growth via growSlottedPage handles any later writes that exceed
      // the initial class. At 100M records the working set shrinks from ~68 GB
      // to ~35-40 GB at 64 KiB → 32 KiB splits, dramatically reducing LZ4
      // decompress calls on cache-miss paths (was 21% CPU in the v3 profile).
      final int allocSize = PageLayout.HEAP_START + inMemHeapSize;
      final MemorySegment slottedPage = memorySegmentAllocator.allocate(allocSize);

      // 5. Copy header + bitmap into page (first 160 bytes)
      MemorySegment.copy(headerBitmapSeg, 0, slottedPage, 0, PageLayout.DISK_HEADER_BITMAP_SIZE);

      // 6. Zero-fill preservation bitmap region (runtime-only, never on disk).
      // Kept — isSlotPreserved() reads this region by slot index regardless of
      // whether the bit is set, so stale bytes would read as true.
      slottedPage.asSlice(PageLayout.PRESERVATION_BITMAP_OFF, PageLayout.PRESERVATION_BITMAP_SIZE).fill((byte) 0);

      // 7. Directory region: skip zero-fill. Every populated slot gets its
      // dir entry written in step 10 below (packed setDirEntry). Non-populated
      // slots' dir entries are never read — all readers gate on
      // isSlotPopulated (bitmap check) before touching the directory.
      // Saves ~8 KB memset per cache-miss page; at 30% miss × 1M pages × 27
      // query runs that's ~65 GB of memset eliminated.
      // (unsafe_setmemory was ~1.7% CPU before this.)

      if (!offsetTableDedup) {
        // 8a. V0 path (no-dedup branch not taken during normal writes): read
        // heap data straight into the page at HEAP_START.
        if (source instanceof MemorySegmentBytesIn msSource) {
          MemorySegment.copy(msSource.getSource(), source.position(), slottedPage,
              PageLayout.HEAP_START, onDiskHeapSize);
          source.skip(onDiskHeapSize);
        } else {
          final byte[] heapData = new byte[onDiskHeapSize];
          source.read(heapData);
          MemorySegment.copy(heapData, 0, slottedPage, ValueLayout.JAVA_BYTE,
              PageLayout.HEAP_START, heapData.length);
        }
      } else if (templateCount == 0) {
        // 8a'. Inline path (dedup failed) — the heap bytes live inside the
        // decompressed blob that the templateCount==0 branch above staged.
        // Copy them into the slotted page's heap region.
        final MemorySegment stagingSeg = BLOB_STAGING_HOLDER.get();
        final long heapBase = BLOB_HEAP_OFFSET_HOLDER.get();
        BLOB_STAGING_HOLDER.set(null);
        BLOB_HEAP_OFFSET_HOLDER.set(0L);
        MemorySegment.copy(stagingSeg, heapBase, slottedPage,
            PageLayout.HEAP_START, onDiskHeapSize);
      }

      // 9. No tail zero-fill: bytes past heapEnd are never read. Slot access
      // is bounded by the directory (heap offsets < heapSize); header and
      // bitmap live at fixed addresses in [0, HEAP_START). Skipping the
      // fill saves ~60 KiB memset per page (large scans: 1M pages × ~60 KiB
      // = 60 GB of memset per iteration, ~4% of CPU in unsafe_setmemory).
      // If the page later grows via growSlottedPage, the new allocation is
      // copied in full and subsequent writes go through bump-allocation
      // from heapEnd, overwriting stale bytes before any read sees them.

      // 10. Rebuild full directory via prefix sums from compact dir entries
      int entryIdx = 0;
      int heapOffset = 0;
      for (int w = 0; w < PageLayout.BITMAP_WORDS; w++) {
        long word = PageLayout.getBitmapWord(slottedPage, w);
        while (word != 0) {
          final int bit = Long.numberOfTrailingZeros(word);
          final int slot = (w << 6) | bit;
          if (entryIdx >= populatedCount) {
            throw new SirixIOException(
                "Bitmap has more set bits than compact directory entries: entryIdx="
                    + entryIdx + ", populatedCount=" + populatedCount);
          }
          final int packed = compactDir[entryIdx];
          final int onDiskLen = packed >>> 8;
          final int nodeKindId = packed & 0xFF;
          final int dataLength;
          if (offsetTableDedup && templateCount > 0) {
            dataLength = inMemDataLengths[entryIdx];
          } else {
            dataLength = onDiskLen;
          }
          PageLayout.setDirEntry(slottedPage, slot, heapOffset, dataLength, nodeKindId);
          heapOffset += dataLength;
          entryIdx++;
          word &= word - 1; // clear lowest set bit
        }
      }

      // 8b. Dedup path: expand each record's (kindId + data) from the staged
      // heap section of the previously-decompressed blob into the in-memory
      // heap region, injecting the offset table from the template pool. The
      // blob was already decompressed above — we only consume from it here.
      // Structural re-injection in up to three places, sorted by in-memory offset:
      //   - parentKey column: re-encode delta-varint(parentKey, nodeKey) at
      //     data-region offset 0 (parentKey is always field 0).
      //   - pathNodeKey column: re-encode delta-varint(pnk, nodeKey) at
      //     data-region offset pnkOff (kind-specific interior).
      //   - hash elision: re-inject 8 zero bytes at the hash offset.
      // Each operation produces bytes whose widths were recorded pre-strip,
      // so the in-memory dataLength exactly matches inMemDataLengths[i].
      //
      // The reinject strategy interleaves copies of on-disk bytes with the
      // three insertion ranges, walking both cursors in parallel.
      if (offsetTableDedup && templateCount > 0) {
        final MemorySegment stagingSeg = BLOB_STAGING_HOLDER.get();
        final long heapBase = BLOB_HEAP_OFFSET_HOLDER.get();
        // Release the thread-local holders so subsequent pages on this thread
        // start from a clean slate. v1StagingScratch still retains the buffer
        // so this is a pure reference clear.
        BLOB_STAGING_HOLDER.set(null);
        BLOB_HEAP_OFFSET_HOLDER.set(0L);

        final long pageKeyBase = recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT;
        long onDiskPos = 0;
        int entryIdx2 = 0;
        for (int w = 0; w < PageLayout.BITMAP_WORDS; w++) {
          long word = PageLayout.getBitmapWord(slottedPage, w);
          while (word != 0) {
            final int bit = Long.numberOfTrailingZeros(word);
            final int slot = (w << 6) | bit;
            final int packed = compactDir[entryIdx2];
            final int onDiskLen = packed >>> 8;
            final int kindId = packed & 0xFF;
            final int templateId = slotTemplateIds[entryIdx2] & 0xFF;
            final int fc = OffsetTableTemplatePool.templateFieldCount(templatePool, templateOffsets, templateId);
            // Record on-disk layout: [kindId(1)][templateId(1)][data(D)]
            // where D = onDiskLen - 2. Expand to [kindId(1)][offsetTable(fc)][data(D')]
            // where D' = D + pkWidth + pnkWidth + (hashStripped ? 8 : 0).
            final int slotHeapOffset = PageLayout.getDirHeapOffset(slottedPage, slot);
            final long recordBase = PageLayout.HEAP_START + slotHeapOffset;
            // Copy kindId byte from staging heap section.
            slottedPage.set(ValueLayout.JAVA_BYTE, recordBase,
                stagingSeg.get(ValueLayout.JAVA_BYTE, heapBase + onDiskPos));
            // Expand offset table from the template pool.
            OffsetTableTemplatePool.expandTemplateTo(templatePool, templateOffsets,
                templateId, slottedPage, recordBase + 1);
            final int dataBytes = onDiskLen - 2;
            if (dataBytes < 0) {
              throw new SirixIOException(
                  "record too short: onDiskLen=" + onDiskLen + " slot=" + slot);
            }
            final boolean hashStripped = hashElisionActive
                && ((zeroHashBitmap[entryIdx2 >>> 3] >>> (entryIdx2 & 7)) & 1) == 1;
            final int hashOffInData = hashStripped
                ? OffsetTableTemplatePool.templateFieldOffset(templatePool, templateOffsets,
                    templateId, NodeFieldLayout.hashFieldIndexForKind(kindId))
                : -1;
            final int pkWidth = (parentKeyColumnActive && parentKeyWidths != null)
                ? (parentKeyWidths[entryIdx2] & 0xFF) : 0;
            final int pnkWidth = (pathNodeKeyColumnActive && pathNodeKeyWidths != null)
                ? (pathNodeKeyWidths[entryIdx2] & 0xFF) : 0;
            final int pnkOffInData;
            if (pnkWidth > 0) {
              pnkOffInData = OffsetTableTemplatePool.templateFieldOffset(templatePool,
                  templateOffsets, templateId,
                  NodeFieldLayout.pathNodeKeyFieldIndexForKind(kindId));
            } else {
              pnkOffInData = -1;
            }
            // Value-elision inject: for fused-NUMBER (kind 49) when value-elision
            // is active, the on-disk record has its [type:1][varint] payload stripped.
            // We placeholder-fill it here with zeros; the actual decoded value is
            // injected in a second pass after the heap is fully expanded (because
            // we need the offset table + nameKey/pathNodeKey to compute the slotRank).
            final int valueWidth = (valueElisionActive && valueWidths != null)
                ? (valueWidths[entryIdx2] & 0xFF) : 0;
            final int valueOffInData = valueWidth > 0 ? (valueOffs[entryIdx2] & 0xFFFF) : -1;
            // Lever 4: nameKey-elision inject. For fused OBJECT_NAMED_* slots we
            // recover the int nameKey via ObjectKeyNameKeyRegion.nameKeyForSlot
            // (called inline below — the region payload is loaded as part of
            // the regionTable AFTER the heap parse, but we cannot delay this
            // inject because the nameKey value sits between hash (offset 7) and
            // value (offset 8) in the in-memory layout). Therefore the writer
            // ensures nameKey-elision activates only when ObjectKeyNameKeyRegion
            // is part of the regionTable; the reader fetches the payload via
            // a second-pass inject AFTER regionTable.read consumes it. To keep
            // the first-pass deterministic, we placeholder-fill these bytes
            // with zero now and re-encode in {@link #injectNameKeyElidedRecords}
            // once the region payload is in hand.
            final int nameKeyWidthLocal = (nameKeyElisionActive && nameKeyWidths != null)
                ? (nameKeyWidths[entryIdx2] & 0xFF) : 0;
            final int nameKeyOffInData = nameKeyWidthLocal > 0
                ? (nameKeyOffs[entryIdx2] & 0xFFFF) : -1;
            // Build up to 5 insertion ranges (in-memory-offset, width), sorted
            // by in-memory-offset ascending. In-memory offset math:
            //   parentKey: at offset 0, width pkWidth (always first when active)
            //   pnk:       at offset pnkOffInData, width pnkWidth
            //   hash:      at offset hashOffInData, width HASH_WIDTH
            //   value:     at offset valueOffInData, width valueWidth
            //   nameKey:   at offset nameKeyOffInData, width nameKeyWidthLocal
            // A 5-entry bubble sort costs at most 10 compares — branch-predictable
            // and register-resident in the hot path.
            int i0Off = 0, i0Width = 0, i0Kind = 0; // 0=pk, 1=pnk, 2=hash, 3=value, 4=nameKey
            int i1Off = 0, i1Width = 0, i1Kind = 0;
            int i2Off = 0, i2Width = 0, i2Kind = 0;
            int i3Off = 0, i3Width = 0, i3Kind = 0;
            int i4Off = 0, i4Width = 0, i4Kind = 0;
            int iCount = 0;
            if (pkWidth > 0) {
              i0Off = 0; i0Width = pkWidth; i0Kind = 0; iCount = 1;
            }
            if (pnkWidth > 0) {
              if (iCount == 0) {
                i0Off = pnkOffInData; i0Width = pnkWidth; i0Kind = 1; iCount = 1;
              } else {
                i1Off = pnkOffInData; i1Width = pnkWidth; i1Kind = 1; iCount = 2;
              }
            }
            if (hashStripped) {
              if (iCount == 0) {
                i0Off = hashOffInData; i0Width = NodeFieldLayout.HASH_WIDTH; i0Kind = 2; iCount = 1;
              } else if (iCount == 1) {
                i1Off = hashOffInData; i1Width = NodeFieldLayout.HASH_WIDTH; i1Kind = 2; iCount = 2;
              } else {
                i2Off = hashOffInData; i2Width = NodeFieldLayout.HASH_WIDTH; i2Kind = 2; iCount = 3;
              }
            }
            if (valueWidth > 0) {
              if (iCount == 0) {
                i0Off = valueOffInData; i0Width = valueWidth; i0Kind = 3; iCount = 1;
              } else if (iCount == 1) {
                i1Off = valueOffInData; i1Width = valueWidth; i1Kind = 3; iCount = 2;
              } else if (iCount == 2) {
                i2Off = valueOffInData; i2Width = valueWidth; i2Kind = 3; iCount = 3;
              } else {
                i3Off = valueOffInData; i3Width = valueWidth; i3Kind = 3; iCount = 4;
              }
            }
            if (nameKeyWidthLocal > 0) {
              if (iCount == 0) {
                i0Off = nameKeyOffInData; i0Width = nameKeyWidthLocal; i0Kind = 4; iCount = 1;
              } else if (iCount == 1) {
                i1Off = nameKeyOffInData; i1Width = nameKeyWidthLocal; i1Kind = 4; iCount = 2;
              } else if (iCount == 2) {
                i2Off = nameKeyOffInData; i2Width = nameKeyWidthLocal; i2Kind = 4; iCount = 3;
              } else if (iCount == 3) {
                i3Off = nameKeyOffInData; i3Width = nameKeyWidthLocal; i3Kind = 4; iCount = 4;
              } else {
                i4Off = nameKeyOffInData; i4Width = nameKeyWidthLocal; i4Kind = 4; iCount = 5;
              }
            }
            if (iCount >= 2 && i0Off > i1Off) {
              int tOff = i0Off, tW = i0Width, tK = i0Kind;
              i0Off = i1Off; i0Width = i1Width; i0Kind = i1Kind;
              i1Off = tOff; i1Width = tW; i1Kind = tK;
            }
            if (iCount >= 3) {
              if (i1Off > i2Off) {
                int tOff = i1Off, tW = i1Width, tK = i1Kind;
                i1Off = i2Off; i1Width = i2Width; i1Kind = i2Kind;
                i2Off = tOff; i2Width = tW; i2Kind = tK;
              }
              if (i0Off > i1Off) {
                int tOff = i0Off, tW = i0Width, tK = i0Kind;
                i0Off = i1Off; i0Width = i1Width; i0Kind = i1Kind;
                i1Off = tOff; i1Width = tW; i1Kind = tK;
              }
            }
            if (iCount >= 4) {
              if (i2Off > i3Off) {
                int tOff = i2Off, tW = i2Width, tK = i2Kind;
                i2Off = i3Off; i2Width = i3Width; i2Kind = i3Kind;
                i3Off = tOff; i3Width = tW; i3Kind = tK;
              }
              if (i1Off > i2Off) {
                int tOff = i1Off, tW = i1Width, tK = i1Kind;
                i1Off = i2Off; i1Width = i2Width; i1Kind = i2Kind;
                i2Off = tOff; i2Width = tW; i2Kind = tK;
              }
              if (i0Off > i1Off) {
                int tOff = i0Off, tW = i0Width, tK = i0Kind;
                i0Off = i1Off; i0Width = i1Width; i0Kind = i1Kind;
                i1Off = tOff; i1Width = tW; i1Kind = tK;
              }
            }
            if (iCount == 5) {
              if (i3Off > i4Off) {
                int tOff = i3Off, tW = i3Width, tK = i3Kind;
                i3Off = i4Off; i3Width = i4Width; i3Kind = i4Kind;
                i4Off = tOff; i4Width = tW; i4Kind = tK;
              }
              if (i2Off > i3Off) {
                int tOff = i2Off, tW = i2Width, tK = i2Kind;
                i2Off = i3Off; i2Width = i3Width; i2Kind = i3Kind;
                i3Off = tOff; i3Width = tW; i3Kind = tK;
              }
              if (i1Off > i2Off) {
                int tOff = i1Off, tW = i1Width, tK = i1Kind;
                i1Off = i2Off; i1Width = i2Width; i1Kind = i2Kind;
                i2Off = tOff; i2Width = tW; i2Kind = tK;
              }
              if (i0Off > i1Off) {
                int tOff = i0Off, tW = i0Width, tK = i0Kind;
                i0Off = i1Off; i0Width = i1Width; i0Kind = i1Kind;
                i1Off = tOff; i1Width = tW; i1Kind = tK;
              }
            }

            // Walk in-memory offsets; between insertions, copy on-disk bytes.
            long writePos = recordBase + 1 + fc;
            long readPos = heapBase + onDiskPos + 2; // skip kindId + templateId bytes
            int inMemCursor = 0;
            final long nodeKey = pageKeyBase + slot;
            for (int ri = 0; ri < iCount; ri++) {
              final int rOff;
              final int rWidth;
              final int rKind;
              if (ri == 0) { rOff = i0Off; rWidth = i0Width; rKind = i0Kind; }
              else if (ri == 1) { rOff = i1Off; rWidth = i1Width; rKind = i1Kind; }
              else if (ri == 2) { rOff = i2Off; rWidth = i2Width; rKind = i2Kind; }
              else if (ri == 3) { rOff = i3Off; rWidth = i3Width; rKind = i3Kind; }
              else { rOff = i4Off; rWidth = i4Width; rKind = i4Kind; }
              // Copy on-disk bytes from inMemCursor → rOff (in-memory) = (rOff - inMemCursor) bytes.
              final int gap = rOff - inMemCursor;
              if (gap < 0) {
                throw new SirixIOException("overlapping insertions at slot " + slot
                    + " range " + ri + " offset " + rOff + " cursor " + inMemCursor);
              }
              if (gap > 0) {
                MemorySegment.copy(stagingSeg, readPos, slottedPage, writePos, gap);
                writePos += gap;
                readPos += gap;
                inMemCursor += gap;
              }
              // Inject rWidth bytes at rKind (0=pk, 1=pnk, 2=hash, 3=value, 4=nameKey).
              if (rKind == 0) {
                final long pk = parentKeyValues[entryIdx2];
                final int actualWidth = DeltaVarIntCodec.writeDeltaToSegment(slottedPage,
                    writePos, pk, nodeKey);
                if (actualWidth != rWidth) {
                  throw new SirixIOException(
                      "parentKey width mismatch at slot " + slot + ": expected=" + rWidth
                          + " actual=" + actualWidth + " value=" + pk + " nodeKey=" + nodeKey);
                }
              } else if (rKind == 1) {
                final int pnkValue = PathNodeKeyRegion.pathNodeKeyForSlot(
                    pathNodeKeyColumnBytes, slot);
                if (pnkValue < 0) {
                  throw new SirixIOException(
                      "pathNodeKey lookup failed for slot " + slot);
                }
                final int actualWidth = DeltaVarIntCodec.writeDeltaToSegment(slottedPage,
                    writePos, (long) pnkValue, nodeKey);
                if (actualWidth != rWidth) {
                  throw new SirixIOException(
                      "pathNodeKey width mismatch at slot " + slot + ": expected=" + rWidth
                          + " actual=" + actualWidth + " value=" + pnkValue + " nodeKey=" + nodeKey);
                }
              } else if (rKind == 2) {
                // Hash: write 8 zero bytes.
                slottedPage.set(LE.LONG, writePos, 0L);
              } else if (rKind == 3) {
                // Value: zero-fill placeholder; the second-pass injectValueElidedBytes
                // pass populates [type:1][varint] from the NumberRegion + tag/slotRank.
                // We zero-fill to keep the heap deterministic for the codec layer.
                for (int z = 0; z < rWidth; z++) {
                  slottedPage.set(ValueLayout.JAVA_BYTE, writePos + z, (byte) 0);
                }
              } else {
                // nameKey (rKind == 4): zero-fill placeholder. The second-pass
                // injectNameKeyElidedRecords (called after regionTable.read())
                // resolves the int nameKey via
                // ObjectKeyNameKeyRegion.nameKeyForSlot and re-encodes the
                // signed-varint into the heap at this offset+width.
                for (int z = 0; z < rWidth; z++) {
                  slottedPage.set(ValueLayout.JAVA_BYTE, writePos + z, (byte) 0);
                }
              }
              writePos += rWidth;
              inMemCursor += rWidth;
            }
            // Copy trailing on-disk bytes from cursor to end of in-memory data region.
            final int inMemDataLen = inMemDataLengths[entryIdx2] - 1 - fc;
            final int tail = inMemDataLen - inMemCursor;
            if (tail > 0) {
              MemorySegment.copy(stagingSeg, readPos, slottedPage, writePos, tail);
              writePos += tail;
              readPos += tail;
            } else if (tail < 0) {
              throw new SirixIOException(
                  "tail < 0 at slot " + slot + ": tail=" + tail
                      + " inMemDataLen=" + inMemDataLen + " inMemCursor=" + inMemCursor);
            }
            onDiskPos += onDiskLen;
            entryIdx2++;
            word &= word - 1;
          }
        }
        if (onDiskPos != onDiskHeapSize) {
          throw new SirixIOException(
              "heap-size mismatch: consumed=" + onDiskPos + " expected=" + onDiskHeapSize);
        }
      }

      final int heapSize = inMemHeapSize; // semantic alias for the remainder of this method

      // 11. Set heapEnd and heapUsed (both = heapSize since deserialized heap is contiguous/defragmented)
      PageLayout.setHeapEnd(slottedPage, heapSize);
      PageLayout.setHeapUsed(slottedPage, heapSize);

      final boolean areDeweyIDsStored = resourceConfig.areDeweyIDsStored;
      final RecordSerializer recordPersister = resourceConfig.recordPersister;

      // PAX region table appended after the heap. Empty on writes produced by
      // the Phase-1 scaffold (4 bytes: int regionCount=0); later tasks populate it.
      final RegionTable regionTable = RegionTable.read(source);

      // Lever 4: NAME-KEY elision second-pass injection. Runs FIRST (before
      // value-elision) because Lever 3's lookupTagForSlot reads the slot's
      // nameKey field (when the region's tagKind is TAG_KIND_NAME), which
      // requires the nameKey varint to already be present on the heap. We
      // walk the bitmap, look up each fused OBJECT_NAMED_* slot's int nameKey
      // via ObjectKeyNameKeyRegion.nameKeyForSlot, and re-encode the signed
      // varint into the heap at the same offset+width the writer recorded.
      // Width round-trip is verified vs the on-disk byte (deterministic).
      if (nameKeyElisionActive && regionTable != null) {
        injectNameKeyElidedRecords(slottedPage, populatedCount,
            nameKeyOffs, nameKeyWidths, regionTable);
      }

      // Lever 3: VALUE elision second-pass injection. After the heap is fully
      // expanded with placeholder zeros at each elided slot's value field, we
      // walk the bitmap, look up each fused-NUMBER slot's tag (nameKey or
      // pathNodeKey based on the region's tagKind), compute its slotRank, and
      // pull the original long value from the NumberRegion. We then re-encode
      // the [type:1][varint] payload into the heap at the same offset+width
      // the writer would have written. The width was preserved on disk so we
      // can validate equality with computeSignedEncodedWidth.
      if (valueElisionActive && regionTable != null) {
        injectValueElidedRecords(slottedPage, recordPageKey, populatedCount,
            valueElidedTypes, valueOffs, valueWidths, regionTable);
      }

      // Read overlong entries
      final var overlongEntriesBitmap = SerializationType.deserializeBitSet(source);
      final int overlongEntrySize = source.readInt();
      final Map<Long, PageReference> references = new LinkedHashMap<>(overlongEntrySize);
      var setBit = -1;
      for (int index = 0; index < overlongEntrySize; index++) {
        setBit = overlongEntriesBitmap.nextSetBit(setBit + 1);
        assert setBit >= 0;
        final long key = (recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + setBit;
        final PageReference reference = new PageReference();
        reference.setKey(source.readLong());
        references.put(key, reference);
      }

      // Read FSST symbol table
      byte[] fsstSymbolTable = null;
      final int fsstSymbolTableLength = source.readInt();
      if (fsstSymbolTableLength > 0) {
        fsstSymbolTable = new byte[fsstSymbolTableLength];
        source.read(fsstSymbolTable);
      }

      // Create page with dummy slotMemory; slotted page overrides all slot operations
      final MemorySegment dummySlotMemory = memorySegmentAllocator.allocate(1);
      final KeyValueLeafPage page = new KeyValueLeafPage(
          recordPageKey, revision, indexType, resourceConfig,
          areDeweyIDsStored, recordPersister, references,
          dummySlotMemory, null, -1);

      page.setSlottedPage(slottedPage);

      if (fsstSymbolTable != null) {
        page.setFsstSymbolTable(fsstSymbolTable);
      }

      if (regionTable != null) {
        page.setRegionTable(regionTable);
      }

      return page;
    }

    @Override
    public void serializePage(final ResourceConfiguration resourceConfig, final BytesOut<?> sink, final Page page,
        final SerializationType type) {
      final KeyValueLeafPage keyValueLeafPage = (KeyValueLeafPage) page;

      // Check for zero-copy compressed segment first
      final MemorySegment cachedSegment = keyValueLeafPage.getCompressedSegment();
      if (cachedSegment != null) {
        sink.writeSegment(cachedSegment, 0, cachedSegment.byteSize());
        return;
      }

      // Legacy byte[] cache fallback
      final var bytes = keyValueLeafPage.getBytes();
      if (bytes != null) {
        sink.write(bytes.toByteArray());
        return;
      }

      // Ensure slotted page exists — ALL pages use slotted page format V0
      keyValueLeafPage.ensureSlottedPage();

      sink.writeByte(KEYVALUELEAFPAGE.id);
      writeVersionAndFlags(sink);

      final Map<Long, PageReference> references = keyValueLeafPage.getReferencesMap();

      // Build FSST symbol table and compress strings BEFORE addReferences() serializes them
      keyValueLeafPage.buildFsstSymbolTable(resourceConfig);
      keyValueLeafPage.compressStringValues();

      // addReferences: serializes records to slotted page heap via processEntries,
      // copies preserved slots from completePageRef for DIFFERENTIAL/INCREMENTAL versioning
      keyValueLeafPage.addReferences(resourceConfig);

      // Write metadata
      Utils.putVarLong(sink, keyValueLeafPage.getPageKey());
      sink.writeInt(keyValueLeafPage.getRevision());
      sink.writeByte(keyValueLeafPage.getIndexType().getID());

      // Write compact on-disk format: header+bitmap, compact dir, heap (no 8KB slot directory)
      final MemorySegment slottedPage = keyValueLeafPage.getSlottedPage();

      // Debug: verify cached header fields match segment values
      assert keyValueLeafPage.getCachedPopulatedCount() >= 0 : "negative populatedCount";
      keyValueLeafPage.assertNoDrift();

      // [DIAG] per-section byte counting when -Dsirix.pageSectionDiag=true
      final boolean sectionDiag = PAGE_SECTION_DIAG;
      final long diagStart = sectionDiag ? sink.writePosition() : 0L;

      // 1. Write header (32B) + bitmap (128B) — 160 bytes (on-disk format, never changes)
      sink.writeSegment(slottedPage, 0, PageLayout.DISK_HEADER_BITMAP_SIZE);

      final long afterHeaderBitmap = sectionDiag ? sink.writePosition() : 0L;

      // 2. Single-pass bitmap scan: collect per-slot (kindId, heapOffset, dataLength)
      // into thread-local scratch. Defers compact-dir + heap emission to the
      // encoding-branch below so template dedup can rewrite lengths without a re-walk.
      final int populatedCount = keyValueLeafPage.getCachedPopulatedCount();
      final int[] scratch = SERIALIZE_SCRATCH.get();
      final int[] slotKindIds = SLOT_KINDID_SCRATCH.get();
      final int[] slotHeapOffs = SLOT_HEAPOFF_SCRATCH.get();
      final int[] slotDataLens = SLOT_DATALEN_SCRATCH.get();
      final short[] slotBits = SLOT_BIT_SCRATCH.get();
      int idx = 0;
      int slotIdx = 0;

      for (int w = 0; w < PageLayout.BITMAP_WORDS; w++) {
        long word = PageLayout.getBitmapWord(slottedPage, w);
        while (word != 0) {
          final int bit = Long.numberOfTrailingZeros(word);
          final int slot = (w << 6) | bit;
          final int dataLength = PageLayout.getDirDataLength(slottedPage, slot);
          final int nodeKindId = PageLayout.getDirNodeKindId(slottedPage, slot);
          final int heapOffset = PageLayout.getDirHeapOffset(slottedPage, slot);

          scratch[idx++] = heapOffset;
          scratch[idx++] = dataLength;
          slotKindIds[slotIdx] = nodeKindId;
          slotHeapOffs[slotIdx] = heapOffset;
          slotDataLens[slotIdx] = dataLength;
          slotBits[slotIdx] = (short) slot;
          slotIdx++;
          word &= word - 1; // clear lowest set bit
        }
      }

      // Encoding. Always attempts offset-table dedup + compressed heap; falls
      // back to the plain-heap marker (templateCount=0) when dedup aborts
      // (e.g. > 255 unique templates, or raw slab bytes without any offset
      // table structure).
      //
      // Wire layout:
      //   compactDir | onDiskHeapSize | templateCount(byte)
      //       | if templateCount > 0: templatePoolBytes(int) | pool | slotIds
      //       |                       compressedLen(int) | codec(byte) | compressed bytes
      //       | if templateCount == 0: heapBytes (inline, uncompressed)
      writeEncodedBody(sink, slottedPage, populatedCount, slotKindIds, slotHeapOffs,
          slotDataLens, slotBits);

      final long afterEncodedBody = sectionDiag ? sink.writePosition() : 0L;

      // PAX region table after the heap. Writer populates the number region
      // (fused OBJECT_NAMED_NUMBER slot values + inline nameKeys) so the
      // vectorized scan path can filter by logical field in a tight loop,
      // skipping both moveTo and varint decode.
      final RegionTable regionTable = buildRegionTable(keyValueLeafPage, slottedPage, resourceConfig);
      if (regionTable == null) {
        sink.writeInt(0);
      } else {
        keyValueLeafPage.setRegionTable(regionTable);
        regionTable.write(sink);
      }

      final long afterRegionTable = sectionDiag ? sink.writePosition() : 0L;

      // Write overlong entries
      writeOverlongEntries(sink, references);

      final long afterOverlong = sectionDiag ? sink.writePosition() : 0L;

      // Write FSST symbol table
      writeFsstSymbolTable(sink, keyValueLeafPage);

      final long afterFsst = sectionDiag ? sink.writePosition() : 0L;

      if (sectionDiag) {
        PageSectionDiag.record(
            afterHeaderBitmap - diagStart,
            afterEncodedBody - afterHeaderBitmap,
            afterRegionTable - afterEncodedBody,
            afterOverlong - afterRegionTable,
            afterFsst - afterOverlong);
      }

      // Compress the serialized data — but NOT when the page still carries unresolved overflow
      // references (#1076). Their disk keys are only assigned when the OverflowPages are written
      // during the recursive commit, which runs AFTER the parallel pre-serialization pass;
      // caching now would freeze NULL keys into the page bytes and the records would be
      // unreadable after reopen. Skipping the cache makes the page serialize again at write
      // time with the real keys (buildFsstSymbolTable/compressStringValues/addReferences are
      // idempotent for the second pass).
      boolean hasUnresolvedOverflowReferences = false;
      for (final PageReference overflowReference : references.values()) {
        if (overflowReference.getKey() == Constants.NULL_ID_LONG) {
          hasUnresolvedOverflowReferences = true;
          break;
        }
      }
      if (!hasUnresolvedOverflowReferences) {
        compressAndCache(resourceConfig, sink, keyValueLeafPage);
      }

      // Release node object references — all data is now in the slotted page + compressed cache
      keyValueLeafPage.clearRecordsForGC();
    }

    private static void writeOverlongEntries(final BytesOut<?> sink, final Map<Long, PageReference> references) {
      var overlongEntriesBitmap = new BitSet(Constants.NDP_NODE_COUNT);
      final var overlongEntriesSortedByKey = references.entrySet().stream()
          .sorted(Map.Entry.comparingByKey()).toList();

      for (final Map.Entry<Long, PageReference> entry : overlongEntriesSortedByKey) {
        final var pageOffset = StorageEngineReader.recordPageOffset(entry.getKey());
        overlongEntriesBitmap.set(pageOffset);
      }
      SerializationType.serializeBitSet(sink, overlongEntriesBitmap);
      sink.writeInt(overlongEntriesSortedByKey.size());
      for (final var entry : overlongEntriesSortedByKey) {
        sink.writeLong(entry.getValue().getKey());
      }
    }

    /**
     * Emit the compact-dir + heap bytes with offset-table template dedup +
     * compressed heap. Gracefully falls back to the plain inline-heap path
     * (emitted with a zero-byte {@code templateCount} marker) when dedup
     * doesn't pay (e.g. every record has a unique offset table or records
     * are raw slab bytes without offset-table structure).
     *
     * <p>Wire layout (the deserializer's two branches in {@code KEYVALUELEAFPAGE.deserializePage}
     * are the authoritative reader):
     * <pre>
     *   byte                templateCount         // 0 = dedup disabled (inline fallback)
     *   if templateCount &gt; 0 (dedup path):
     *     byte              structuralFlags       // bit0 hashElision, bit1 parentKeyColumn,
     *                                             // bit2 pathNodeKeyColumn, bit3 valueElision,
     *                                             // bit4 nameKeyElision
     *     int               templatePoolBytes
     *     int               compressedLen
     *     byte              codec                 // 0 ZeroRun, 1 LZ4, 2 ByteRun, 3 SirixLZ77
     *     byte[compressedLen] blob — decompresses to, in order:
     *       int[populatedCount] compactDir        // BE byte layout; dataLength = ON-DISK length
     *       byte[templatePoolBytes] templatePool
     *       byte[populatedCount]    slotTemplateIds
     *       if hashElision:      byte[ceil(N/8)] zeroHashBitmap
     *       if parentKeyColumn:  int len + byte[len]   (StructuralKeyColumnCodec)
     *       if pathNodeKeyColumn:int len + byte[len]
     *       if valueElision:     int len + section
     *       if nameKeyElision:   int len + section
     *       byte[onDiskHeapSize] heap
     *   if templateCount == 0 (inline path):
     *     int               compressedLen
     *     byte              codec
     *     byte[compressedLen] blob — decompresses to compactDir + heap
     * </pre>
     * The smallest-of-codecs bake-off covers the whole blob (compactDir included), not just the
     * heap.
     *
     * @param sink destination byte sink
     * @param slottedPage the slotted-page memory (in-memory format, full offset tables inline)
     * @param populatedCount number of populated slots
     * @param slotKindIds per-slot nodeKindId (length populatedCount)
     * @param slotHeapOffs per-slot in-memory heap offsets (length populatedCount)
     * @param slotDataLens per-slot in-memory dataLengths (length populatedCount)
     */
    private static void writeEncodedBody(final BytesOut<?> sink, final MemorySegment slottedPage,
        final int populatedCount, final int[] slotKindIds, final int[] slotHeapOffs,
        final int[] slotDataLens, final short[] slotBits) {
      final boolean finerDiag = PAGE_SECTION_DIAG;
      final long diagS0 = finerDiag ? sink.writePosition() : 0L;
      if (populatedCount > 0) {
        final byte[] templatePool = TEMPLATE_POOL_SCRATCH.get();
        final byte[] slotTemplateIds = SLOT_TEMPLATE_IDS_SCRATCH.get();
        final it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap templateMap = TEMPLATE_MAP.get();
        final OffsetTableTemplatePool.BuildResult br = OffsetTableTemplatePool.build(slottedPage,
            populatedCount, slotKindIds, slotHeapOffs, templatePool, slotTemplateIds, templateMap);
        if (br.isDedupEnabled()) {
          final long pageKeyBase = PageLayout.getRecordPageKey(slottedPage)
              << Constants.NDP_NODE_COUNT_EXPONENT;
          // Pre-scan to compute:
          //   1. hash-elision bitmap + per-slot hash offset
          //   2. parentKey column values + per-slot parentKey width
          //
          // HFT note: we DO NOT read the hash field's 8 bytes as a single
          // JAVA_LONG_UNALIGNED here — records on the heap are byte-aligned
          // at the start but the hash's absolute offset depends on the
          // variable-width fields before it, so unaligned is required.
          final byte[] zeroHashBitmap = SLOT_ZERO_HASH_BITMAP_SCRATCH.get();
          final short[] slotHashOffs = SLOT_HASH_OFFSET_SCRATCH.get();
          final long[] slotParentKeys = SLOT_PARENT_KEY_SCRATCH.get();
          final byte[] slotParentKeyWidths = SLOT_PARENT_KEY_WIDTH_SCRATCH.get();
          final short[] slotParentKeyOffs = SLOT_PARENT_KEY_OFF_SCRATCH.get();
          final long[] slotPnkValues = SLOT_PATH_NODE_KEY_SCRATCH.get();
          final byte[] slotPnkWidths = SLOT_PATH_NODE_KEY_WIDTH_SCRATCH.get();
          final short[] slotPnkOffs = SLOT_PATH_NODE_KEY_OFF_SCRATCH.get();
          // Lever 3: value-elision per-slot scratches. Filled for every fused-NUMBER
          // slot during this pre-scan; only consumed when the writer activates
          // value-elision (all fused-NUMBER slots eligible AND net savings > 0).
          final byte[] slotValueElided = SLOT_VALUE_ELIDED_SCRATCH.get();
          final short[] slotValueOffs = SLOT_VALUE_OFF_SCRATCH.get();
          final byte[] slotValueWidths = SLOT_VALUE_WIDTH_SCRATCH.get();
          final long[] slotValueLongs = SLOT_VALUE_LONG_SCRATCH.get();
          // Lever 4: per-slot nameKey-elision scratches. Filled for every fused
          // OBJECT_NAMED_* slot (kindIds 48-51) during the pre-scan; consumed
          // when the writer activates nameKey elision (region built AND
          // net savings > overhead).
          final byte[] slotNameKeyElided = SLOT_NAME_KEY_ELIDED_SCRATCH.get();
          final short[] slotNameKeyOffs = SLOT_NAME_KEY_OFF_SCRATCH.get();
          final byte[] slotNameKeyWidths = SLOT_NAME_KEY_WIDTH_SCRATCH.get();
          // Lever 4: track distinct nameKey count so we can match the
          // ObjectKeyNameKeyRegion.encode() upper-bound (255 unique). Beyond
          // that, the encoder returns null (no region built) and we MUST NOT
          // elide on the heap. Linear scan over the small dict is cheap given
          // dict size is bounded by 255.
          final int[] uniqueNameKeysScratch = NAME_KEY_UNIQUE_DICT_SCRATCH.get();
          int uniqueNameKeyCount = 0;
          final int hashBitmapBytes = HASH_ELISION_ENABLED
              ? ((populatedCount + 7) >>> 3) : 0;
          int zeroHashCount = 0;
          int parentKeySlotsWithField = 0;
          int pnkSlotsWithField = 0;
          int fusedNumberSlotCount = 0;
          int fusedStringSlotCount = 0;
          int fusedBooleanSlotCount = 0;
          int valueElidableSlotCount = 0;
          int valueElidableTotalBytes = 0;
          // Lever 4: counts ALL fused OBJECT_NAMED_* primitives on the page
          // (kindIds 48-51, NOT just 48-50 like the value-elision triplet).
          // Used by the activation guard to enforce all-or-nothing elision —
          // partial elision would corrupt the slot-ascending packed-widths
          // section.
          int totalFusedNamedSlotCount = 0;
          int nameKeyElidableSlotCount = 0;
          int nameKeyElidableTotalBytes = 0;
          if (HASH_ELISION_ENABLED) {
            // Clear bitmap header bytes — reusing thread-local scratch across
            // pages means stale bits from a larger prior page may otherwise
            // corrupt this page's bitmap.
            for (int b = 0; b < hashBitmapBytes; b++) {
              zeroHashBitmap[b] = 0;
            }
          }
          if (VALUE_ELISION_ENABLED) {
            // Clear stale per-slot flags from previous pages. Only the first
            // populatedCount entries will be inspected later, so we only clear
            // up to that count. The other scratches (offs/widths/longs) are
            // overwritten unconditionally per slot below so they don't need
            // a separate clear pass.
            for (int b = 0; b < populatedCount; b++) {
              slotValueElided[b] = 0;
            }
          }
          if (NAME_KEY_ELISION_ENABLED) {
            // Lever 4: clear stale per-slot nameKey-elision flags from previous
            // pages. Same rationale as the value-elision clear above — the
            // offs/widths arrays are overwritten unconditionally below.
            for (int b = 0; b < populatedCount; b++) {
              slotNameKeyElided[b] = 0;
            }
          }
          // ---- Lever 4: cheap-reject pre-pass --------------------------------
          // The full Lever 4 pre-scan (signed-varint decode + linear dict scan
          // for the 255-unique cap + per-slot scratch population) costs ~5
          // cycles/slot on average. For pages whose avg nameKey-varint width
          // is <2 bytes (typical for narrow schemas with <=5 distinct field
          // names per page — the scaleBench {id,age,dept,city,active} case
          // is a clean example) the page-wide activation guard
          //   nameKeyElidableTotalBytes > nameKeyElidableSlotCount + 4
          // can NEVER hold, regardless of dict size. In that regime we pay
          // the full pre-scan cost without ever activating Lever 4 — a 20%
          // shred-time regression on the bench.
          //
          // Cheap-reject: walk fused OBJECT_NAMED_* slots, read ONLY the two
          // offset-table bytes that bound the nameKey field (no varint decode,
          // no dict scan), and sum widths. If sum can't clear the activation
          // threshold, set {@code nameKeyElisionCheapReject} and short-circuit
          // ALL Lever 4 work below: no decode, no dict tracking, no scratch
          // population, no per-slot flag set, and the activation guard is
          // forced false.
          //
          // HFT contract: zero alloc, primitive-only, final locals, no virtual
          // dispatch. KindIds 48-51 are the ONLY kinds for which
          // {@link KeyValueLeafPage#isFusedObjectNamedKindId} returns true, and
          // ALL four place their nameKey at field index 3 (verified in
          // NodeFieldLayout — OBJNAMEDBOOL/NUM/STR/NULL_NAME_KEY all = 3) with
          // field count >= 8 (so {@code nameKeyFieldIdx + 1 < fc} is statically
          // true). We therefore hardcode {@code nameKeyFieldIdx = 3} and skip
          // both the {@link NodeFieldLayout#nameKeyFieldIndexForKind} switch
          // and the {@link NodeFieldLayout#fieldCountForKind} bounds check —
          // saving two static-switch dispatches per fused-named slot.
          //
          // Soundness: a slot with out-of-range width (corrupt offset table)
          // is silently dropped from the cheap sum. That can only LOWER the
          // sum vs. the slow path's view (which would also drop it), so it
          // never causes a false cheap-reject — the slow path would have
          // failed activation on the same input via the all-or-nothing
          // {@code nameKeyElidableSlotCount == totalFusedNamedSlotCount}
          // check.
          int cheapNamedCount = 0;
          int cheapWidthSum = 0;
          if (NAME_KEY_ELISION_ENABLED) {
            for (int i = 0; i < populatedCount; i++) {
              final int kindId = slotKindIds[i];
              if (!KeyValueLeafPage.isFusedObjectNamedKindId(kindId)) {
                continue;
              }
              final long recordBase = PageLayout.HEAP_START + slotHeapOffs[i];
              // nameKey field is index 3 for all four fused-named primitives.
              // Read the two adjacent offset-table bytes ([3] and [4]) — the
              // delta is the on-heap nameKey-varint width.
              final int nameKeyOff = slottedPage.get(ValueLayout.JAVA_BYTE,
                  recordBase + 1 + 3) & 0xFF;
              final int nextOff = slottedPage.get(ValueLayout.JAVA_BYTE,
                  recordBase + 1 + 4) & 0xFF;
              final int w = nextOff - nameKeyOff;
              if (w >= 1 && w <= 5) {
                cheapWidthSum += w;
                cheapNamedCount++;
              }
            }
          }
          // Activation requires {@code sum > count + 4}; a tie or smaller sum
          // cannot net-pay for the 4-byte length-prefix int + 1-byte/slot
          // packed-widths section overhead. When {@code cheapNamedCount == 0}
          // there are no fused-named slots and Lever 4 has nothing to elide.
          // Either case → cheap-reject and skip ALL Lever 4 work below.
          final boolean nameKeyElisionCheapReject = !NAME_KEY_ELISION_ENABLED
              || cheapNamedCount == 0
              || cheapWidthSum <= cheapNamedCount + 4;
          for (int i = 0; i < populatedCount; i++) {
            final int kindId = slotKindIds[i];
            final int fc = NodeFieldLayout.fieldCountForKind(kindId);
            final long recordBase = PageLayout.HEAP_START + slotHeapOffs[i];
            // --- hash elision scan ---
            if (HASH_ELISION_ENABLED) {
              final int hashFieldIdx = NodeFieldLayout.hashFieldIndexForKind(kindId);
              if (hashFieldIdx < 0) {
                slotHashOffs[i] = -1;
              } else {
                final int hashOffInData = slottedPage.get(ValueLayout.JAVA_BYTE,
                    recordBase + 1 + hashFieldIdx) & 0xFF;
                slotHashOffs[i] = (short) hashOffInData;
                final long hashAbsOff = recordBase + 1 + fc + hashOffInData;
                final long h = slottedPage.get(LE.LONG, hashAbsOff);
                if (h == 0L) {
                  zeroHashBitmap[i >>> 3] |= (byte) (1 << (i & 7));
                  zeroHashCount++;
                }
              }
            }
            // --- parentKey column scan ---
            if (PARENT_KEY_COLUMN_ENABLED) {
              final int pkFieldIdx = NodeFieldLayout.parentKeyFieldIndexForKind(kindId);
              if (pkFieldIdx < 0) {
                slotParentKeys[i] = Fixed.NULL_NODE_KEY.getStandardProperty();
                slotParentKeyWidths[i] = 0;
                slotParentKeyOffs[i] = 0;
              } else {
                // parentKey is always field index 0 (see NodeFieldLayout —
                // all 17 kinds that have a parentKey place it at index 0).
                // Its on-disk offset within the data region is therefore
                // always 0 (the offset-table byte is 0). Width is the delta
                // to the next field offset, or dataBytes when fc == 1.
                final int pkOff = slottedPage.get(ValueLayout.JAVA_BYTE,
                    recordBase + 1 + pkFieldIdx) & 0xFF;
                final int nextOff = (pkFieldIdx + 1 < fc)
                    ? (slottedPage.get(ValueLayout.JAVA_BYTE,
                        recordBase + 1 + pkFieldIdx + 1) & 0xFF)
                    : (slotDataLens[i] - 1 - fc);
                final int pkWidth = nextOff - pkOff;
                if (pkWidth <= 0 || pkWidth > 10) {
                  // Pathological — back out to NULL sentinel which the reader
                  // will interpret as "no parentKey" for this slot. Column is
                  // still active for the rest of the page.
                  slotParentKeys[i] = Fixed.NULL_NODE_KEY.getStandardProperty();
                  slotParentKeyWidths[i] = 0;
                  slotParentKeyOffs[i] = 0;
                } else {
                  final long nodeKey = pageKeyBase + (slotBits[i] & 0xFFFF);
                  final long pk = DeltaVarIntCodec.decodeDeltaFromSegment(slottedPage,
                      recordBase + 1 + fc + pkOff, nodeKey);
                  slotParentKeys[i] = pk;
                  slotParentKeyWidths[i] = (byte) pkWidth;
                  slotParentKeyOffs[i] = (short) pkOff;
                  parentKeySlotsWithField++;
                }
              }
            }
            // --- pathNodeKey column scan ---
            // pathNodeKey lives at a kind-specific interior field offset (unlike
            // parentKey which is always field 0). We read the offset-table entry
            // to find the in-data-region offset, derive the width from the next
            // field's offset (or from dataBytes when it's the last field), then
            // decode the value so the dict encoder can see all values up-front.
            if (PATH_NODE_KEY_COLUMN_ENABLED) {
              final int pnkFieldIdx = NodeFieldLayout.pathNodeKeyFieldIndexForKind(kindId);
              if (pnkFieldIdx < 0) {
                slotPnkValues[i] = Fixed.NULL_NODE_KEY.getStandardProperty();
                slotPnkWidths[i] = 0;
                slotPnkOffs[i] = 0;
              } else {
                final int pnkOff = slottedPage.get(ValueLayout.JAVA_BYTE,
                    recordBase + 1 + pnkFieldIdx) & 0xFF;
                final int nextOff = (pnkFieldIdx + 1 < fc)
                    ? (slottedPage.get(ValueLayout.JAVA_BYTE,
                        recordBase + 1 + pnkFieldIdx + 1) & 0xFF)
                    : (slotDataLens[i] - 1 - fc);
                final int pnkWidth = nextOff - pnkOff;
                if (pnkWidth <= 0 || pnkWidth > 10) {
                  // Pathological — back out to NULL sentinel which the reader
                  // will interpret as "no pathNodeKey" for this slot. Column is
                  // still active for the rest of the page.
                  slotPnkValues[i] = Fixed.NULL_NODE_KEY.getStandardProperty();
                  slotPnkWidths[i] = 0;
                  slotPnkOffs[i] = 0;
                } else {
                  final long nodeKey = pageKeyBase + (slotBits[i] & 0xFFFF);
                  final long pnk = DeltaVarIntCodec.decodeDeltaFromSegment(slottedPage,
                      recordBase + 1 + fc + pnkOff, nodeKey);
                  slotPnkValues[i] = pnk;
                  slotPnkWidths[i] = (byte) pnkWidth;
                  slotPnkOffs[i] = (short) pnkOff;
                  pnkSlotsWithField++;
                }
              }
            }
            // --- Lever 3: VALUE elision pre-scan (fused-NUMBER, STRING, BOOLEAN) ---
            // Only counts as "elidable" when the slot is one of the fused primitive
            // kinds AND its payload can be reconstituted bit-for-bit at read time
            // from the corresponding PAX region. We compute valueOff (offset-table[8])
            // and valueWidth (recordOnlyLen - 1 - fc - valueOff) from the in-memory
            // record so the strip pass below can operate without re-deriving them.
            // We use getRecordOnlyLength so the DeweyID trailer + bytes (when
            // areDeweyIDsStored is true) don't bloat valueWidth.
            //
            // The OBJNAMEDNUM_PAYLOAD / OBJNAMEDSTR_PAYLOAD / OBJNAMEDBOOL_VALUE
            // field indices are all 8 (verified in NodeFieldLayout) — sharing the
            // OBJNAMEDNUM_PAYLOAD constant keeps the offset read in a single line.
            if (VALUE_ELISION_ENABLED
                && (kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_NUMBER_KIND_ID
                    || kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_STRING_KIND_ID
                    || kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_BOOLEAN_KIND_ID)) {
              final int valueOff = slottedPage.get(ValueLayout.JAVA_BYTE,
                  recordBase + 1 + NodeFieldLayout.OBJNAMEDNUM_PAYLOAD) & 0xFF;
              final int recordOnlyLen = PageLayout.getRecordOnlyLength(slottedPage, slotBits[i] & 0xFFFF);
              final int dataBytes = recordOnlyLen - 1 - fc;
              final int valueWidth = dataBytes - valueOff;
              if (kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_NUMBER_KIND_ID) {
                fusedNumberSlotCount++;
                if (valueWidth > 0 && valueWidth <= 11) {
                  // Read the type byte; INTEGER=2 / LONG=3 are the only kinds we
                  // can elide (Float/Double/BigDecimal can't be reduced to a long
                  // and stay inline).
                  final byte typeByte = slottedPage.get(ValueLayout.JAVA_BYTE,
                      recordBase + 1 + fc + valueOff);
                  if (typeByte == NUMBER_TYPE_INTEGER || typeByte == NUMBER_TYPE_LONG) {
                    // Decode the long value; handles both INTEGER and LONG variants.
                    final long longVal = (typeByte == NUMBER_TYPE_INTEGER)
                        ? DeltaVarIntCodec.decodeSignedFromSegment(slottedPage,
                            recordBase + 1 + fc + valueOff + 1)
                        : DeltaVarIntCodec.decodeSignedLongFromSegment(slottedPage,
                            recordBase + 1 + fc + valueOff + 1);
                    // Defensive: skip Long.MIN_VALUE (the sentinel buildRegionTable
                    // uses to tag "not long-decodable"). Storing it would create a
                    // mismatch between elision-pass count and region count. This
                    // only triggers for the pathological LONG=Long.MIN_VALUE case;
                    // INTEGER cannot equal Long.MIN_VALUE since it's outside int range.
                    if (longVal != Long.MIN_VALUE) {
                      slotValueElided[i] = (byte) (typeByte & 0x7F); // store type in low bits, never 0
                      slotValueOffs[i] = (short) valueOff;
                      slotValueWidths[i] = (byte) valueWidth;
                      slotValueLongs[i] = longVal;
                      valueElidableSlotCount++;
                      valueElidableTotalBytes += valueWidth;
                    }
                  }
                }
              } else if (kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_STRING_KIND_ID) {
                fusedStringSlotCount++;
                // Heap layout: [isCompressed:1][length:varint][rawBytes].
                // Only elide uncompressed payloads — compressed bytes can't be
                // reconstructed from the StringRegion's plain UTF-8 dictionary.
                // Fused-string nodes are never compressed by current production
                // paths (FSST applies to STRING_VALUE kind 25 only), so skipping
                // compressed payloads is purely defensive.
                if (valueWidth > 0 && valueWidth <= 0xFF) {
                  final byte compressedFlag = slottedPage.get(ValueLayout.JAVA_BYTE,
                      recordBase + 1 + fc + valueOff);
                  if (compressedFlag == 0) {
                    // Validate the length encodes a non-empty payload (writer
                    // matches the strLen seen on heap because the StringRegion
                    // dictionary stores the same UTF-8 bytes verbatim — width
                    // round-trip is deterministic).
                    final long lenAbsOff = recordBase + 1 + fc + valueOff + 1;
                    final int strLen = DeltaVarIntCodec.decodeSignedFromSegment(slottedPage, lenAbsOff);
                    if (strLen > 0) {
                      slotValueElided[i] = STRING_ELIDE_MARKER;
                      slotValueOffs[i] = (short) valueOff;
                      slotValueWidths[i] = (byte) valueWidth;
                      valueElidableSlotCount++;
                      valueElidableTotalBytes += valueWidth;
                    }
                  }
                }
              } else {
                // FUSED_OBJECT_NAMED_BOOLEAN — width is always 1 (a single bool byte).
                fusedBooleanSlotCount++;
                if (valueWidth == 1) {
                  slotValueElided[i] = BOOLEAN_ELIDE_MARKER;
                  slotValueOffs[i] = (short) valueOff;
                  slotValueWidths[i] = (byte) 1;
                  valueElidableSlotCount++;
                  valueElidableTotalBytes += 1;
                }
              }
            }
            // --- Lever 4: NAME-KEY elision pre-scan (fused OBJECT_NAMED_*, kindIds 48-51) ---
            // For every fused-OBJECT_NAMED_* slot we record its nameKey field's
            // in-data-region offset and width. The actual elision activation
            // decision is made page-globally below (region built AND net savings
            // > overhead). The nameKey is encoded as a signed-varint inline; on
            // disk we keep only its width (1 byte) per elided slot. We also
            // count distinct nameKeys: ObjectKeyNameKeyRegion.encode caps unique
            // values at 255, so beyond that the region returns null and we must
            // refuse elision (heap copies become the only source of truth).
            if (KeyValueLeafPage.isFusedObjectNamedKindId(kindId)) {
              totalFusedNamedSlotCount++;
            }
            if (NAME_KEY_ELISION_ENABLED
                && !nameKeyElisionCheapReject
                && KeyValueLeafPage.isFusedObjectNamedKindId(kindId)
                && uniqueNameKeyCount < 256) {
              final int nameKeyFieldIdx = NodeFieldLayout.nameKeyFieldIndexForKind(kindId);
              if (nameKeyFieldIdx >= 0 && nameKeyFieldIdx + 1 < fc) {
                final int nameKeyOff = slottedPage.get(ValueLayout.JAVA_BYTE,
                    recordBase + 1 + nameKeyFieldIdx) & 0xFF;
                final int nextOff = slottedPage.get(ValueLayout.JAVA_BYTE,
                    recordBase + 1 + nameKeyFieldIdx + 1) & 0xFF;
                final int nameKeyWidth = nextOff - nameKeyOff;
                // signed-varint width is 1..5 for any 32-bit nameKey value.
                if (nameKeyWidth >= 1 && nameKeyWidth <= 5) {
                  // Track distinct nameKey count via linear scan over the
                  // tiny dict (typical record-shaped page has <10 distinct).
                  // Decode the inline signed-varint directly from the heap —
                  // avoids a virtual call into KeyValueLeafPage.getFused… and
                  // saves the per-slot offset-table re-read.
                  final int nk = DeltaVarIntCodec.decodeSignedFromSegment(
                      slottedPage, recordBase + 1 + fc + nameKeyOff);
                  boolean foundInDict = false;
                  for (int u = 0; u < uniqueNameKeyCount; u++) {
                    if (uniqueNameKeysScratch[u] == nk) { foundInDict = true; break; }
                  }
                  if (!foundInDict && uniqueNameKeyCount < 255) {
                    uniqueNameKeysScratch[uniqueNameKeyCount++] = nk;
                  } else if (!foundInDict) {
                    // Hit the encoder's 255-cap. Mark cap reached so subsequent
                    // slots also abort, and flip Lever 4 off page-globally.
                    uniqueNameKeyCount = 256;
                  }
                  if (uniqueNameKeyCount < 256) {
                    slotNameKeyElided[i] = (byte) 1;
                    slotNameKeyOffs[i] = (short) nameKeyOff;
                    slotNameKeyWidths[i] = (byte) nameKeyWidth;
                    nameKeyElidableSlotCount++;
                    nameKeyElidableTotalBytes += nameKeyWidth;
                  }
                }
              }
            }
          }
          final boolean hashElisionActive = HASH_ELISION_ENABLED && zeroHashCount > 0;
          if (finerDiag && hashElisionActive) {
            PageSectionDiag.recordHashElision((long) zeroHashCount * NodeFieldLayout.HASH_WIDTH);
          }

          // parentKey column is active iff at least one slot has a parentKey
          // field AND the column encoding pays off (i.e. is strictly smaller
          // than the raw varint bytes it replaces). Otherwise we skip the
          // column and keep parentKey inline.
          byte[] parentKeyColumnBytes = null;
          int parentKeyColumnLen = 0;
          int parentKeyTotalStrippedBytes = 0;
          if (PARENT_KEY_COLUMN_ENABLED && parentKeySlotsWithField > 0) {
            for (int i = 0; i < populatedCount; i++) {
              parentKeyTotalStrippedBytes += slotParentKeyWidths[i] & 0xFF;
            }
            // Compute encoded column size first to decide if column is worth it.
            // The column encodes N values (including sentinel for slots without
            // parentKey), so its length is a function of all values, not just
            // those with parentKey. Column must be strictly smaller than
            // parentKeyTotalStrippedBytes plus the overhead of the length-prefix
            // int (4 bytes) and flag bit (0).
            final int encodedLen = StructuralKeyColumnCodec.encodedSize(slotParentKeys,
                populatedCount);
            if (finerDiag) {
              PageSectionDiag.recordParentKeyColumnCandidate(
                  parentKeyTotalStrippedBytes, encodedLen);
            }
            if (encodedLen + 4 < parentKeyTotalStrippedBytes) {
              byte[] scratch = PARENT_KEY_COLUMN_SCRATCH.get();
              if (scratch.length < encodedLen) {
                scratch = new byte[Math.max(encodedLen, scratch.length * 2)];
                PARENT_KEY_COLUMN_SCRATCH.set(scratch);
              }
              StructuralKeyColumnCodec.encodeByteArray(scratch, 0, slotParentKeys,
                  populatedCount);
              parentKeyColumnBytes = scratch;
              parentKeyColumnLen = encodedLen;
              if (finerDiag) {
                PageSectionDiag.recordParentKeyColumn(
                    parentKeyTotalStrippedBytes - encodedLen - 4);
              }
            }
          }
          final boolean parentKeyColumnActive = parentKeyColumnBytes != null;

          // pathNodeKey column is active iff at least one slot has a pathNodeKey
          // field AND the dict encoding pays off vs. raw delta-varints. Unlike
          // parentKey (a wide pile of independent node keys), pathNodeKey typically
          // has 3-10 distinct values across 1000+ slots in record-oriented JSON
          // workloads — perfect for dict encoding.
          byte[] pathNodeKeyColumnBytes = null;
          int pathNodeKeyColumnLen = 0;
          int pnkTotalStrippedBytes = 0;
          if (PATH_NODE_KEY_COLUMN_ENABLED && pnkSlotsWithField > 0) {
            // Compact the pnk values/slots into tight arrays for encode input.
            // Width sum also becomes the "raw varint" cost baseline.
            final int[] pnkCompactValues = SLOT_PATH_NODE_KEY_VALUES_COMPACT_SCRATCH.get();
            final int[] pnkCompactSlots = SLOT_PATH_NODE_KEY_SLOTS_COMPACT_SCRATCH.get();
            int c = 0;
            for (int i = 0; i < populatedCount; i++) {
              final int w = slotPnkWidths[i] & 0xFF;
              if (w == 0) continue;
              pnkTotalStrippedBytes += w;
              // Truncate pnk long → int; pathNodeKeys are always non-negative
              // path-summary node keys and fit in 32 bits for realistic datasets
              // (> 2 billion distinct paths would be needed to overflow).
              pnkCompactValues[c] = (int) slotPnkValues[i];
              pnkCompactSlots[c] = slotBits[i] & 0xFFFF;
              c++;
            }
            final int[] dictScratch = PNK_ENCODE_DICT_SCRATCH.get();
            final int encodedLen = PathNodeKeyRegion.encodedSize(pnkCompactValues, c,
                dictScratch);
            if (encodedLen > 0 && encodedLen + 4 < pnkTotalStrippedBytes) {
              byte[] scratch = PATH_NODE_KEY_COLUMN_SCRATCH.get();
              if (scratch.length < encodedLen) {
                scratch = new byte[Math.max(encodedLen, scratch.length * 2)];
                PATH_NODE_KEY_COLUMN_SCRATCH.set(scratch);
              }
              byte[] dictIdsScratch = PNK_ENCODE_DICT_IDS_SCRATCH.get();
              if (dictIdsScratch.length < c) {
                dictIdsScratch = new byte[Math.max(c, dictIdsScratch.length * 2)];
                PNK_ENCODE_DICT_IDS_SCRATCH.set(dictIdsScratch);
              }
              final int written = PathNodeKeyRegion.encode(pnkCompactValues,
                  pnkCompactSlots, c, scratch, dictScratch, dictIdsScratch,
                  COLUMN_ENCODE_BITMAP_SCRATCH.get());
              if (written > 0) {
                pathNodeKeyColumnBytes = scratch;
                pathNodeKeyColumnLen = written;
              }
            }
          }
          final boolean pathNodeKeyColumnActive = pathNodeKeyColumnBytes != null;

          // Lever 3: VALUE elision is active iff every fused-NUMBER, fused-STRING,
          // and fused-BOOLEAN slot on the page is elidable AND the net savings
          // strictly outweigh the per-elided-slot 2-byte (type + width) overhead
          // plus the section length-prefix int. The reader must see ALL such
          // slots' values come from the region (we don't support a partial
          // per-slot bitmap here — that would add metadata that wipes the small
          // per-record savings).
          //
          // For the page to activate elision, it must contain at least one of
          // these fused primitive kinds AND every such slot must have been
          // marked elidable in the pre-scan above. Mixed kinds are allowed:
          // a page with N numbers + M booleans can elide all N+M values.
          //
          // Defensive cap: BooleanRegion.encode rejects pages with > 256
          // distinct parent tags by returning null, which would corrupt elision.
          // Bound activation to fusedBooleanSlotCount <= 256 — when the page has
          // at most 256 fused-boolean slots, the distinct-tag dict cannot exceed
          // 256 either (regardless of whether tag-by-name or tag-by-path is
          // selected). StringRegion has no such hard cap; per-tag dict size is
          // bit-packed so it self-adapts.
          final int totalFusedPrimitiveSlots =
              fusedNumberSlotCount + fusedStringSlotCount + fusedBooleanSlotCount;
          final boolean regionDictsSafe = fusedBooleanSlotCount <= 256;
          final boolean valueElisionActive = VALUE_ELISION_ENABLED
              && totalFusedPrimitiveSlots > 0
              && valueElidableSlotCount == totalFusedPrimitiveSlots
              && valueElidableTotalBytes > (valueElidableSlotCount * 2) + 4
              && regionDictsSafe;

          // Lever 4: name-key elision is active iff EVERY fused OBJECT_NAMED_*
          // (kindIds 48-51) slot on the page was marked elidable AND the page-wide
          // net byte savings strictly exceed the per-elided-slot 1-byte width
          // overhead plus the 4-byte length-prefix int. The reader recovers the
          // nameKey via ObjectKeyNameKeyRegion.nameKeyForSlot(payload, slot) — the
          // region is already built unconditionally for any page that emits
          // fused OBJECT_KEY-role slots (load-bearing for SIMD scans). All-or-
          // nothing per-page activation enforces section integrity: the
          // packed-widths array on disk is sized for {@code totalFusedNamedSlotCount}
          // entries in slot-ascending order, so partial elision would shift the
          // reader's cursor and corrupt subsequent slot reads.
          // {@code !nameKeyElisionCheapReject} short-circuits the predicate
          // without consulting the (cheap-rejected) zero counters — defensive
          // guard against future refactors that might pre-populate the
          // counters above the cheap-reject path.
          final boolean nameKeyElisionActive = NAME_KEY_ELISION_ENABLED
              && !nameKeyElisionCheapReject
              && nameKeyElidableSlotCount > 0
              && nameKeyElidableSlotCount == totalFusedNamedSlotCount
              && nameKeyElidableTotalBytes > nameKeyElidableSlotCount + 4;

          // Compute on-disk heap size: for each record, replace its FIELD_COUNT bytes of
          // offset table with a single templateId byte. In-memory = 1 (kindId) + FC + D;
          // on-disk = 1 (kindId) + 1 (templateId) + D = in-memory - (FC - 1).
          // When hash elision is active, also strip 8 bytes per zero-hash slot.
          // When parentKey column is active, strip the slot's parentKeyWidth bytes.
          // When pathNodeKey column is active, strip the slot's pnkWidth bytes.
          // When value elision is active, strip the slot's value field bytes
          // (1 type byte + delta-varint payload).
          // When name-key elision is active, strip the slot's nameKey varint bytes.
          int onDiskHeapSize = 0;
          for (int i = 0; i < populatedCount; i++) {
            final int fc = NodeFieldLayout.fieldCountForKind(slotKindIds[i]);
            int onDiskLen = slotDataLens[i] - (fc - 1);
            if (onDiskLen < 2) {
              // A record that's shorter than kindId+templateId means the in-memory layout
              // is inconsistent. Fall back to inline to avoid corrupting disk bytes.
              writeInlineBody(sink, slottedPage, populatedCount, slotKindIds, slotHeapOffs,
                  slotDataLens);
              return;
            }
            if (hashElisionActive && ((zeroHashBitmap[i >>> 3] >>> (i & 7)) & 1) == 1) {
              onDiskLen -= NodeFieldLayout.HASH_WIDTH;
            }
            if (parentKeyColumnActive) {
              onDiskLen -= slotParentKeyWidths[i] & 0xFF;
            }
            if (pathNodeKeyColumnActive) {
              onDiskLen -= slotPnkWidths[i] & 0xFF;
            }
            if (valueElisionActive && slotValueElided[i] != 0) {
              onDiskLen -= slotValueWidths[i] & 0xFF;
            }
            if (nameKeyElisionActive && slotNameKeyElided[i] != 0) {
              onDiskLen -= slotNameKeyWidths[i] & 0xFF;
            }
            onDiskHeapSize += onDiskLen;
          }

          // Emit populated count so the reader can size the compactDir + slotIds
          // arrays before decompression. All subsequent per-slot data lives inside
          // the staged (compressed) blob.
          sink.writeInt(populatedCount);
          // Emit on-disk heap size (uncompressed — needed to size decode buffer).
          sink.writeInt(onDiskHeapSize);
          // Emit template count (uncompressed — needed to sentinel the dedup path).
          sink.writeByte((byte) br.templateCount);
          // Structural flags: bit 0 = hash elision, bit 1 = parentKey column,
          // bit 2 = pathNodeKey column, bit 3 = value elision, bit 4 = name-key elision.
          int structuralFlags = 0;
          if (hashElisionActive) structuralFlags |= STRUCT_FLAG_HASH_ELISION;
          if (parentKeyColumnActive) structuralFlags |= STRUCT_FLAG_PARENT_KEY_COLUMN;
          if (pathNodeKeyColumnActive) structuralFlags |= STRUCT_FLAG_PATH_NODE_KEY_COLUMN;
          if (valueElisionActive) structuralFlags |= STRUCT_FLAG_VALUE_ELISION;
          if (nameKeyElisionActive) structuralFlags |= STRUCT_FLAG_NAME_KEY_ELISION;
          sink.writeByte((byte) structuralFlags);
          sink.writeInt(br.templatesByteLength);

          // Stage ALL structural metadata + heap into one contiguous buffer so the
          // zero-run RLE encoder sees cross-section patterns (repeated compact-dir
          // entries across pages + repeated offset-table templates compress together
          // with record bodies). This matches what the outer full-page LZ4 used to
          // catch and lets us drop that outer pass entirely.
          //
          // Staging layout:
          //   compactDir bytes (4 × populatedCount)
          //   templatePool bytes (br.templatesByteLength)
          //   slotTemplateIds (populatedCount bytes)
          //   [if hashElisionActive] zeroHashBitmap (hashBitmapBytes)
          //   [if parentKeyColumnActive] int columnLen + column bytes
          //   [if pathNodeKeyColumnActive] int columnLen + column bytes
          //   [if valueElisionActive] int valueTypesLen + valueTypeBytes
          //   [if nameKeyElisionActive] int elidedCount + per-slot 1-byte widths
          //   heap bytes (onDiskHeapSize, possibly reduced per slot)
          //
          // HFT-grade: staging buffer is thread-local and grows in powers of two
          // until steady-state; after warm-up every shred is allocation-free.
          final int compactDirBytes = 4 * populatedCount;
          final int stagedHashBitmapBytes = hashElisionActive ? hashBitmapBytes : 0;
          final int stagedParentKeyColBytes = parentKeyColumnActive
              ? (4 + parentKeyColumnLen) : 0;
          final int stagedPathNodeKeyColBytes = pathNodeKeyColumnActive
              ? (4 + pathNodeKeyColumnLen) : 0;
          // Per elided slot: 1 byte type + 1 byte width. Plus 4-byte length prefix.
          final int stagedValueElisionBytes = valueElisionActive
              ? (4 + (valueElidableSlotCount * 2)) : 0;
          // Lever 4: per elided slot 1 byte width + 4-byte length prefix.
          final int stagedNameKeyElisionBytes = nameKeyElisionActive
              ? (4 + nameKeyElidableSlotCount) : 0;
          final int structuralBytes = compactDirBytes + br.templatesByteLength + populatedCount
              + stagedHashBitmapBytes + stagedParentKeyColBytes + stagedPathNodeKeyColBytes
              + stagedValueElisionBytes + stagedNameKeyElisionBytes;
          final int totalStagingBytes = structuralBytes + onDiskHeapSize;
          final MemorySegment staging = v1StagingScratch(totalStagingBytes);
          long stagePos = 0;

          // compactDir — on-disk lengths, accounting for stripped hash + parentKey + pnk + value + nameKey.
          for (int i = 0; i < populatedCount; i++) {
            final int fc = NodeFieldLayout.fieldCountForKind(slotKindIds[i]);
            int onDiskLen = slotDataLens[i] - (fc - 1);
            if (hashElisionActive && ((zeroHashBitmap[i >>> 3] >>> (i & 7)) & 1) == 1) {
              onDiskLen -= NodeFieldLayout.HASH_WIDTH;
            }
            if (parentKeyColumnActive) {
              onDiskLen -= slotParentKeyWidths[i] & 0xFF;
            }
            if (pathNodeKeyColumnActive) {
              onDiskLen -= slotPnkWidths[i] & 0xFF;
            }
            if (valueElisionActive && slotValueElided[i] != 0) {
              onDiskLen -= slotValueWidths[i] & 0xFF;
            }
            if (nameKeyElisionActive && slotNameKeyElided[i] != 0) {
              onDiskLen -= slotNameKeyWidths[i] & 0xFF;
            }
            final int packed = PageLayout.packCompactDirEntry(onDiskLen, slotKindIds[i]);
            // Big-endian serialization — matches sink.writeInt() semantics so a
            // self-consistent reader can be written either way; we use the
            // MemorySegment JAVA_INT_UNALIGNED variant later but emit big-endian
            // here for parity with the prior wire format.
            staging.set(ValueLayout.JAVA_BYTE, stagePos,     (byte) ((packed >>> 24) & 0xFF));
            staging.set(ValueLayout.JAVA_BYTE, stagePos + 1, (byte) ((packed >>> 16) & 0xFF));
            staging.set(ValueLayout.JAVA_BYTE, stagePos + 2, (byte) ((packed >>> 8) & 0xFF));
            staging.set(ValueLayout.JAVA_BYTE, stagePos + 3, (byte) (packed & 0xFF));
            stagePos += 4;
          }
          // template pool
          if (br.templatesByteLength > 0) {
            MemorySegment.copy(templatePool, 0, staging, ValueLayout.JAVA_BYTE,
                stagePos, br.templatesByteLength);
            stagePos += br.templatesByteLength;
          }
          // slotTemplateIds
          if (populatedCount > 0) {
            MemorySegment.copy(slotTemplateIds, 0, staging, ValueLayout.JAVA_BYTE,
                stagePos, populatedCount);
            stagePos += populatedCount;
          }
          // zeroHashBitmap (only when hash elision active)
          if (hashElisionActive) {
            MemorySegment.copy(zeroHashBitmap, 0, staging, ValueLayout.JAVA_BYTE,
                stagePos, hashBitmapBytes);
            stagePos += hashBitmapBytes;
          }
          // parentKey column (only when active): int length prefix + bytes
          if (parentKeyColumnActive) {
            staging.set(ValueLayout.JAVA_BYTE, stagePos,     (byte) ((parentKeyColumnLen >>> 24) & 0xFF));
            staging.set(ValueLayout.JAVA_BYTE, stagePos + 1, (byte) ((parentKeyColumnLen >>> 16) & 0xFF));
            staging.set(ValueLayout.JAVA_BYTE, stagePos + 2, (byte) ((parentKeyColumnLen >>> 8) & 0xFF));
            staging.set(ValueLayout.JAVA_BYTE, stagePos + 3, (byte) (parentKeyColumnLen & 0xFF));
            stagePos += 4;
            MemorySegment.copy(parentKeyColumnBytes, 0, staging, ValueLayout.JAVA_BYTE,
                stagePos, parentKeyColumnLen);
            stagePos += parentKeyColumnLen;
          }
          // pathNodeKey column (only when active): int length prefix + bytes
          if (pathNodeKeyColumnActive) {
            staging.set(ValueLayout.JAVA_BYTE, stagePos,     (byte) ((pathNodeKeyColumnLen >>> 24) & 0xFF));
            staging.set(ValueLayout.JAVA_BYTE, stagePos + 1, (byte) ((pathNodeKeyColumnLen >>> 16) & 0xFF));
            staging.set(ValueLayout.JAVA_BYTE, stagePos + 2, (byte) ((pathNodeKeyColumnLen >>> 8) & 0xFF));
            staging.set(ValueLayout.JAVA_BYTE, stagePos + 3, (byte) (pathNodeKeyColumnLen & 0xFF));
            stagePos += 4;
            MemorySegment.copy(pathNodeKeyColumnBytes, 0, staging, ValueLayout.JAVA_BYTE,
                stagePos, pathNodeKeyColumnLen);
            stagePos += pathNodeKeyColumnLen;
          }
          // value elision section (only when active): int elidedCount + per-slot
          // (type, width) pairs. Layout: 2 bytes/elided slot in slot-ascending
          // order (skipping non-elided slots). The reader walks the bitmap in
          // parallel and consumes 2 bytes per elided slot.
          //   byte 0: kind-specific type discriminator
          //     - NUMBER:  NUMBER_TYPE_INTEGER (2) or NUMBER_TYPE_LONG (3)
          //     - STRING:  0 (placeholder; reader dispatches via compactDir kindId)
          //     - BOOLEAN: 0 (placeholder)
          //   byte 1: original heap width (1 type byte + payload bytes)
          if (valueElisionActive) {
            staging.set(ValueLayout.JAVA_BYTE, stagePos,     (byte) ((valueElidableSlotCount >>> 24) & 0xFF));
            staging.set(ValueLayout.JAVA_BYTE, stagePos + 1, (byte) ((valueElidableSlotCount >>> 16) & 0xFF));
            staging.set(ValueLayout.JAVA_BYTE, stagePos + 2, (byte) ((valueElidableSlotCount >>> 8) & 0xFF));
            staging.set(ValueLayout.JAVA_BYTE, stagePos + 3, (byte) (valueElidableSlotCount & 0xFF));
            stagePos += 4;
            for (int i = 0; i < populatedCount; i++) {
              final byte mark = slotValueElided[i];
              if (mark != 0) {
                // Number: write the actual type byte (2 or 3). String/Boolean:
                // write 0 — the reader inferring kind from compactDir kindId.
                final byte diskType = (mark == STRING_ELIDE_MARKER || mark == BOOLEAN_ELIDE_MARKER)
                    ? (byte) 0 : mark;
                staging.set(ValueLayout.JAVA_BYTE, stagePos,     diskType);
                staging.set(ValueLayout.JAVA_BYTE, stagePos + 1, slotValueWidths[i]);
                stagePos += 2;
              }
            }
          }
          // Lever 4: name-key elision section (only when active): int elidedCount +
          // per-slot 1-byte width pairs in slot-ascending order. The reader walks
          // the bitmap in parallel and consumes 1 byte per fused-OBJECT_NAMED_*
          // slot. nameKey value itself is recovered via
          // ObjectKeyNameKeyRegion.nameKeyForSlot(payload, slotBit).
          if (nameKeyElisionActive) {
            staging.set(ValueLayout.JAVA_BYTE, stagePos,     (byte) ((nameKeyElidableSlotCount >>> 24) & 0xFF));
            staging.set(ValueLayout.JAVA_BYTE, stagePos + 1, (byte) ((nameKeyElidableSlotCount >>> 16) & 0xFF));
            staging.set(ValueLayout.JAVA_BYTE, stagePos + 2, (byte) ((nameKeyElidableSlotCount >>> 8) & 0xFF));
            staging.set(ValueLayout.JAVA_BYTE, stagePos + 3, (byte) (nameKeyElidableSlotCount & 0xFF));
            stagePos += 4;
            for (int i = 0; i < populatedCount; i++) {
              if (slotNameKeyElided[i] != 0) {
                staging.set(ValueLayout.JAVA_BYTE, stagePos, slotNameKeyWidths[i]);
                stagePos++;
              }
            }
          }
          // heap (records with templateId replacing offset table; hash, parentKey,
          // pathNodeKey, value, and nameKey optionally stripped). Up to 5 skip
          // ranges per slot, sorted ascending by offset, so the general case
          // collapses to "copy gaps, skip ranges" in one pass.
          //
          // HFT-grade: no allocation. We inline the five (from, to) pairs into stack
          // locals and do at most ten compares for a 5-element bubble sort.
          for (int i = 0; i < populatedCount; i++) {
            final int fc = NodeFieldLayout.fieldCountForKind(slotKindIds[i]);
            final long recordBase = PageLayout.HEAP_START + slotHeapOffs[i];
            staging.set(ValueLayout.JAVA_BYTE, stagePos,
                slottedPage.get(ValueLayout.JAVA_BYTE, recordBase));
            stagePos++;
            staging.set(ValueLayout.JAVA_BYTE, stagePos, slotTemplateIds[i]);
            stagePos++;
            final int dataBytes = slotDataLens[i] - 1 - fc;
            final boolean stripHash = hashElisionActive
                && ((zeroHashBitmap[i >>> 3] >>> (i & 7)) & 1) == 1;
            final int stripPkWidth = parentKeyColumnActive
                ? (slotParentKeyWidths[i] & 0xFF) : 0;
            final int stripPnkWidth = pathNodeKeyColumnActive
                ? (slotPnkWidths[i] & 0xFF) : 0;
            final boolean stripValue = valueElisionActive && slotValueElided[i] != 0;
            final int stripValueWidth = stripValue ? (slotValueWidths[i] & 0xFF) : 0;
            final boolean stripNameKey = nameKeyElisionActive && slotNameKeyElided[i] != 0;
            final int stripNameKeyWidth = stripNameKey ? (slotNameKeyWidths[i] & 0xFF) : 0;
            // Build up to 5 skip ranges (from, to) relative to data-region start.
            //   parentKey: offset 0, width stripPkWidth (always at the front when active)
            //   pnk:       offset slotPnkOffs[i], width stripPnkWidth
            //   hash:      offset slotHashOffs[i], width HASH_WIDTH
            //   value:     offset slotValueOffs[i], width stripValueWidth
            //   nameKey:   offset slotNameKeyOffs[i], width stripNameKeyWidth
            int r0From = 0, r0To = 0;
            int r1From = 0, r1To = 0;
            int r2From = 0, r2To = 0;
            int r3From = 0, r3To = 0;
            int r4From = 0, r4To = 0;
            int rCount = 0;
            if (stripPkWidth > 0) {
              r0From = 0;
              r0To = stripPkWidth;
              rCount = 1;
            }
            if (stripPnkWidth > 0) {
              final int pnkFrom = slotPnkOffs[i] & 0xFFFF;
              final int pnkTo = pnkFrom + stripPnkWidth;
              if (rCount == 0) {
                r0From = pnkFrom; r0To = pnkTo; rCount = 1;
              } else {
                r1From = pnkFrom; r1To = pnkTo; rCount = 2;
              }
            }
            if (stripHash) {
              final int hFrom = slotHashOffs[i] & 0xFFFF;
              final int hTo = hFrom + NodeFieldLayout.HASH_WIDTH;
              if (rCount == 0) {
                r0From = hFrom; r0To = hTo; rCount = 1;
              } else if (rCount == 1) {
                r1From = hFrom; r1To = hTo; rCount = 2;
              } else {
                r2From = hFrom; r2To = hTo; rCount = 3;
              }
            }
            if (stripValue) {
              final int vFrom = slotValueOffs[i] & 0xFFFF;
              final int vTo = vFrom + stripValueWidth;
              if (rCount == 0) {
                r0From = vFrom; r0To = vTo; rCount = 1;
              } else if (rCount == 1) {
                r1From = vFrom; r1To = vTo; rCount = 2;
              } else if (rCount == 2) {
                r2From = vFrom; r2To = vTo; rCount = 3;
              } else {
                r3From = vFrom; r3To = vTo; rCount = 4;
              }
            }
            if (stripNameKey) {
              final int nkFrom = slotNameKeyOffs[i] & 0xFFFF;
              final int nkTo = nkFrom + stripNameKeyWidth;
              if (rCount == 0) {
                r0From = nkFrom; r0To = nkTo; rCount = 1;
              } else if (rCount == 1) {
                r1From = nkFrom; r1To = nkTo; rCount = 2;
              } else if (rCount == 2) {
                r2From = nkFrom; r2To = nkTo; rCount = 3;
              } else if (rCount == 3) {
                r3From = nkFrom; r3To = nkTo; rCount = 4;
              } else {
                r4From = nkFrom; r4To = nkTo; rCount = 5;
              }
            }
            // Bubble-sort up to 5 ranges by from-offset ascending. 10 compares
            // worst case — still cheap, branch-predictable, and register-resident.
            if (rCount >= 2 && r0From > r1From) {
              int tf = r0From, tt = r0To;
              r0From = r1From; r0To = r1To;
              r1From = tf; r1To = tt;
            }
            if (rCount >= 3) {
              if (r1From > r2From) {
                int tf = r1From, tt = r1To;
                r1From = r2From; r1To = r2To;
                r2From = tf; r2To = tt;
              }
              if (r0From > r1From) {
                int tf = r0From, tt = r0To;
                r0From = r1From; r0To = r1To;
                r1From = tf; r1To = tt;
              }
            }
            if (rCount >= 4) {
              if (r2From > r3From) {
                int tf = r2From, tt = r2To;
                r2From = r3From; r2To = r3To;
                r3From = tf; r3To = tt;
              }
              if (r1From > r2From) {
                int tf = r1From, tt = r1To;
                r1From = r2From; r1To = r2To;
                r2From = tf; r2To = tt;
              }
              if (r0From > r1From) {
                int tf = r0From, tt = r0To;
                r0From = r1From; r0To = r1To;
                r1From = tf; r1To = tt;
              }
            }
            if (rCount == 5) {
              if (r3From > r4From) {
                int tf = r3From, tt = r3To;
                r3From = r4From; r3To = r4To;
                r4From = tf; r4To = tt;
              }
              if (r2From > r3From) {
                int tf = r2From, tt = r2To;
                r2From = r3From; r2To = r3To;
                r3From = tf; r3To = tt;
              }
              if (r1From > r2From) {
                int tf = r1From, tt = r1To;
                r1From = r2From; r1To = r2To;
                r2From = tf; r2To = tt;
              }
              if (r0From > r1From) {
                int tf = r0From, tt = r0To;
                r0From = r1From; r0To = r1To;
                r1From = tf; r1To = tt;
              }
            }
            // Walk ranges: copy gaps, skip ranges.
            int cursor = 0;
            if (rCount >= 1) {
              if (r0From > cursor) {
                MemorySegment.copy(slottedPage, recordBase + 1 + fc + cursor,
                    staging, stagePos, r0From - cursor);
                stagePos += r0From - cursor;
              }
              cursor = r0To;
            }
            if (rCount >= 2) {
              if (r1From > cursor) {
                MemorySegment.copy(slottedPage, recordBase + 1 + fc + cursor,
                    staging, stagePos, r1From - cursor);
                stagePos += r1From - cursor;
              }
              cursor = r1To;
            }
            if (rCount >= 3) {
              if (r2From > cursor) {
                MemorySegment.copy(slottedPage, recordBase + 1 + fc + cursor,
                    staging, stagePos, r2From - cursor);
                stagePos += r2From - cursor;
              }
              cursor = r2To;
            }
            if (rCount >= 4) {
              if (r3From > cursor) {
                MemorySegment.copy(slottedPage, recordBase + 1 + fc + cursor,
                    staging, stagePos, r3From - cursor);
                stagePos += r3From - cursor;
              }
              cursor = r3To;
            }
            if (rCount == 5) {
              if (r4From > cursor) {
                MemorySegment.copy(slottedPage, recordBase + 1 + fc + cursor,
                    staging, stagePos, r4From - cursor);
                stagePos += r4From - cursor;
              }
              cursor = r4To;
            }
            // Final tail from cursor to dataBytes.
            if (dataBytes > cursor) {
              MemorySegment.copy(slottedPage, recordBase + 1 + fc + cursor,
                  staging, stagePos, dataBytes - cursor);
              stagePos += dataBytes - cursor;
            }
          }

          // Compress the combined staging blob with LZ4 (HC when configured) or
          // ZeroRunByteCodec fallback. Emit: int compressedLen, 1 byte codec,
          // compressed bytes. Reader decompresses once and parses the 4-section
          // blob in order.
          final FFILz4Compressor lz4 = HEAP_LZ4_DISABLED ? null : V1_HEAP_LZ4.get();
          if (lz4 != null) {
            final int bound = lz4.compressBound(totalStagingBytes);
            final MemorySegment lz4Out = v1Lz4OutScratch(bound);
            final MemorySegment stagingView = staging.asSlice(0, totalStagingBytes);
            final MemorySegment lz4OutView = lz4Out.asSlice(0, bound);
            final int compressedLen;
            if (lz4.getCompressionMode() == FFILz4Compressor.CompressionMode.HIGH_COMPRESSION) {
              compressedLen = lz4.compressSegmentHC(stagingView, lz4OutView, HEAP_LZ4_HC_LEVEL);
            } else {
              compressedLen = lz4.compressSegment(stagingView, lz4OutView);
            }
            if (compressedLen < 0) {
              throw new SirixIOException("body LZ4 compress failed: rc=" + compressedLen);
            }
            sink.writeInt(compressedLen);
            sink.writeByte((byte) 1); // codec: 1 = LZ4, 0 = ZeroRunByteCodec, 2 = ByteRunCodec
            sink.writeSegment(lz4Out, 0, compressedLen);
          } else {
            // Smallest-of-codecs bake-off with sticky-winner election —
            // shared with the inline path, see emitSmallestBody.
            emitSmallestBody(sink, staging, totalStagingBytes);
          }
          if (finerDiag) {
            final long diagS3 = sink.writePosition();
            PageSectionDiag.recordEncodedBody(
                compactDirBytes,                      // compactDir PRE-compression
                br.templatesByteLength + populatedCount, // templatePool+slotIds PRE-compression
                diagS3 - diagS0 - 9 /* populatedCount + heapSize + templateCount headers */);
          }
          return;
        }
      }
      // Fallback path (also used when dedup aborts).
      writeInlineBody(sink, slottedPage, populatedCount, slotKindIds, slotHeapOffs,
          slotDataLens);
      if (finerDiag) {
        final long diagS3 = sink.writePosition();
        PageSectionDiag.recordEncodedBody(0L, 0L, diagS3 - diagS0);
      }
    }

    /**
     * Inline (un-deduped) heap emission. Used when the page has too many
     * distinct offset-tables to qualify for template dedup, or when the
     * structural invariants that dedup relies on aren't satisfied (e.g.
     * onDiskLen &lt; 2 after strip).
     *
     * <p>Wire layout: same first 13-byte prefix as the dedup path
     * (populatedCount, onDiskHeapSize, templateCount=0, templatePoolBytes=0)
     * so the reader's header parse works identically. After that the body is
     * a single blob containing compactDir + records, encoded by whichever of
     * {@link ZeroRunByteCodec}, {@link ByteRunCodec}, {@link SirixLZ77Codec}
     * produces the smallest output. Emit: int compressedLen + 1 byte codec
     * + compressed bytes.
     *
     * <p>This is the HOT path for pages where the template pool blew past
     * {@link OffsetTableTemplatePool#MAX_TEMPLATES} (e.g. ELEMENT-heavy
     * mixed-DFS pages) — which represents roughly half of all pages on a
     * typical Chicago-like scale dataset. Adding compression here closes
     * the largest remaining gap vs LZ4 HC's whole-page compression.
     */
    private static void writeInlineBody(final BytesOut<?> sink, final MemorySegment slottedPage,
        final int populatedCount, final int[] slotKindIds, final int[] slotHeapOffs,
        final int[] slotDataLens) {
      int totalHeapSize = 0;
      for (int i = 0; i < populatedCount; i++) {
        totalHeapSize += slotDataLens[i];
      }
      // Matching header prefix so the reader can always consume the same 13 bytes.
      sink.writeInt(populatedCount);
      sink.writeInt(totalHeapSize);
      sink.writeByte((byte) 0);       // templateCount = 0 (no dedup)
      sink.writeInt(0);               // templatePoolBytes = 0

      // Stage compactDir + heap into a single blob and compress it.
      final int compactDirBytes = 4 * populatedCount;
      final int totalBlobBytes = compactDirBytes + totalHeapSize;

      // Stage into the dedup-path's staging scratch (native-backed so the
      // codecs can read via MemorySegment). Grow on demand.
      final MemorySegment staging = v1StagingScratch(totalBlobBytes);

      // Write compactDir entries (4 bytes each, big-endian).
      long stagePos = 0;
      for (int i = 0; i < populatedCount; i++) {
        final int packed = PageLayout.packCompactDirEntry(slotDataLens[i], slotKindIds[i]);
        staging.set(ValueLayout.JAVA_BYTE, stagePos,     (byte) ((packed >>> 24) & 0xFF));
        staging.set(ValueLayout.JAVA_BYTE, stagePos + 1, (byte) ((packed >>> 16) & 0xFF));
        staging.set(ValueLayout.JAVA_BYTE, stagePos + 2, (byte) ((packed >>> 8) & 0xFF));
        staging.set(ValueLayout.JAVA_BYTE, stagePos + 3, (byte) (packed & 0xFF));
        stagePos += 4;
      }
      // Append heap bytes.
      for (int i = 0; i < populatedCount; i++) {
        MemorySegment.copy(slottedPage, PageLayout.HEAP_START + slotHeapOffs[i],
            staging, stagePos, slotDataLens[i]);
        stagePos += slotDataLens[i];
      }

      // Smallest-of-codecs bake-off with sticky-winner election — shared with the
      // dedup path, see emitSmallestBody.
      emitSmallestBody(sink, staging, totalBlobBytes);
    }

    /**
     * Build the PAX {@link RegionTable} for {@code page} by walking the
     * populated-slot bitmap once, collecting each fused OBJECT_NAMED_NUMBER slot's
     * value (long) and its parent OBJECT_KEY's nameKey, and encoding them via
     * {@link NumberRegion}.
     *
     * <p>Values whose payload type is not int/long (BigDecimal, double, float)
     * are skipped — the slow-path {@code getNumberValueLongFromSlot} returns
     * {@link Long#MIN_VALUE} as a sentinel, and the caller still sees the
     * correct answer via the inline-slot fallback path.
     *
     * <p>Values whose parent OBJECT_KEY is not on the same page are tagged
     * with {@code -1} — the scan operator's fallback branch handles them.
     *
     * <p>Returns {@code null} when the page has no numeric values (common for
     * path-summary and index pages).
     */
    private static RegionTable buildRegionTable(final KeyValueLeafPage page,
        final MemorySegment slottedPage, final ResourceConfiguration resourceConfig) {
      final long[] valBuf = NUMBER_VALUE_SCRATCH.get();
      final int[] parBuf = NUMBER_PARENT_SCRATCH.get();
      final int[] numberPathBuf = NUMBER_PATH_SCRATCH.get();
      final int[] okNameKeys = OBJECT_KEY_NAMEKEY_SCRATCH.get();
      final int[] okSlots = OBJECT_KEY_SLOT_SCRATCH.get();
      int count = 0;
      int okCount = 0;
      final long pageKeyBase = page.getPageKey() << Constants.NDP_NODE_COUNT_EXPONENT;

      // Path-tagged region emission is gated by the resource config's path-summary
      // flag — without it the pathNodeKey column is absent, so nameKey is the
      // only tag we can derive.
      final boolean withPathSummary = resourceConfig != null && resourceConfig.withPathSummary;
      // Both flags flip to false permanently on the first parent-outside-page /
      // missing-pathNodeKey observation, forcing fallback to TAG_KIND_NAME.
      boolean numberAllPathNodeKeysValid = withPathSummary;
      boolean stringAllPathNodeKeysValid = withPathSummary;

      // Reuse a per-thread StringRegion.Encoder so the common path (every
      // KVL page in a large ingest) allocates nothing for the encoder itself.
      // It's reset() lazily — only if we actually encounter a string slot,
      // keeping pages without fused OBJECT_NAMED_STRING slots at zero touch.
      //
      // We may need TWO encoders: {@code stringEncName} for the legacy
      // nameKey-tagged fallback, and {@code stringEncPath} for the SIMD-safe
      // pathNodeKey-tagged variant. We populate both in lockstep until we
      // observe the first invalid pathNodeKey on the page; after that
      // {@code stringEncPath} is no longer touched and we keep going with
      // nameKey only. The final pick is a single-if branch.
      StringRegion.Encoder stringEncName = null;
      StringRegion.Encoder stringEncPath = null;
      int stringCount = 0;

      // BooleanRegion collection — mirrors NumberRegion's tagged-by-name OR
      // tagged-by-path layout. We populate two parallel int[] tag buffers and
      // pick one at finish time based on path-summary validity.
      final boolean[] boolValBuf = BOOLEAN_VALUE_SCRATCH.get();
      final int[] boolNameTags = BOOLEAN_TAG_SCRATCH.get();
      final int[] boolPathTags = BOOLEAN_PATH_SCRATCH.get();
      int boolCount = 0;
      boolean booleanAllPathNodeKeysValid = withPathSummary;

      for (int w = 0; w < PageLayout.BITMAP_WORDS; w++) {
        long word = PageLayout.getBitmapWord(slottedPage, w);
        final int baseSlot = w << 6;
        while (word != 0) {
          final int bit = Long.numberOfTrailingZeros(word);
          final int slot = baseSlot + bit;
          final int kindId = PageLayout.getDirNodeKindId(slottedPage, slot);
          if (KeyValueLeafPage.isFusedObjectNamedKindId(kindId)) {
            // Fused OBJECT_NAMED_* plays the OBJECT_KEY role; add to the nameKey region so the
            // SIMD scan in ObjectKeyNameKeyRegion.findMatchingSlots sees fused slots natively,
            // then feed NUMBER/STRING/BOOLEAN regions from the inline value (no parent indirection).
            okNameKeys[okCount] = page.getFusedObjectNamedNameKeyFromSlot(slot);
            okSlots[okCount] = slot;
            okCount++;
            if (kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_NUMBER_KIND_ID) {
              final long value = page.getFusedObjectNamedNumberValueLongFromSlot(slot);
              if (value != Long.MIN_VALUE) {
                final int fusedNameKey = okNameKeys[okCount - 1];
                int fusedPathNodeKeyInt = -1;
                if (numberAllPathNodeKeysValid) {
                  final long fusedNodeKey = pageKeyBase + slot;
                  final long pnk = page.getObjectKeyPathNodeKeyFromSlot(slot, fusedNodeKey);
                  if (pnk > 0L && pnk <= (long) Integer.MAX_VALUE) {
                    fusedPathNodeKeyInt = (int) pnk;
                  } else {
                    numberAllPathNodeKeysValid = false;
                  }
                }
                valBuf[count] = value;
                parBuf[count] = fusedNameKey;
                numberPathBuf[count] = fusedPathNodeKeyInt;
                count++;
              }
            } else if (kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_STRING_KIND_ID) {
              final byte[] value = page.readFusedObjectNamedStringBytes(slot);
              if (value != null) {
                final int fusedNameKey = okNameKeys[okCount - 1];
                int fusedPathNodeKeyInt = -1;
                if (stringAllPathNodeKeysValid) {
                  final long fusedNodeKey = pageKeyBase + slot;
                  final long pnk = page.getObjectKeyPathNodeKeyFromSlot(slot, fusedNodeKey);
                  if (pnk > 0L && pnk <= (long) Integer.MAX_VALUE) {
                    fusedPathNodeKeyInt = (int) pnk;
                  } else {
                    stringAllPathNodeKeysValid = false;
                  }
                }
                if (stringEncName == null) {
                  stringEncName = STRING_REGION_ENCODER.get();
                  stringEncName.reset();
                  if (withPathSummary) {
                    stringEncPath = STRING_REGION_ENCODER_PATH.get();
                    stringEncPath.reset();
                  }
                }
                stringEncName.addValue(fusedNameKey, value);
                if (stringEncPath != null && stringAllPathNodeKeysValid) {
                  stringEncPath.addValue(fusedPathNodeKeyInt, value);
                }
                stringCount++;
              }
            } else if (kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_BOOLEAN_KIND_ID) {
              final boolean value = page.getFusedObjectNamedBooleanValueFromSlot(slot);
              final int fusedNameKey = okNameKeys[okCount - 1];
              int fusedPathNodeKeyInt = -1;
              if (booleanAllPathNodeKeysValid) {
                final long fusedNodeKey = pageKeyBase + slot;
                final long pnk = page.getObjectKeyPathNodeKeyFromSlot(slot, fusedNodeKey);
                if (pnk > 0L && pnk <= (long) Integer.MAX_VALUE) {
                  fusedPathNodeKeyInt = (int) pnk;
                } else {
                  booleanAllPathNodeKeysValid = false;
                }
              }
              boolValBuf[boolCount] = value;
              boolNameTags[boolCount] = fusedNameKey;
              boolPathTags[boolCount] = fusedPathNodeKeyInt;
              boolCount++;
            }
          }
          word &= word - 1;
        }
      }

      if (count == 0 && okCount == 0 && stringCount == 0 && boolCount == 0) {
        return null;
      }
      final RegionTable table = new RegionTable();
      if (count > 0) {
        final byte numberTagKind = numberAllPathNodeKeysValid
            ? NumberRegion.TAG_KIND_PATH_NODE
            : NumberRegion.TAG_KIND_NAME;
        final int[] numberTagBuf = numberAllPathNodeKeysValid ? numberPathBuf : parBuf;
        final byte[] payload = NumberRegion.encode(valBuf, numberTagBuf, count, numberTagKind);
        table.set(RegionTable.KIND_NUMBER, payload);
      }
      if (okCount > 0) {
        final byte[] nameKeyPayload = ObjectKeyNameKeyRegion.encode(okNameKeys, okSlots, okCount);
        if (nameKeyPayload != null) {
          table.set(RegionTable.KIND_OBJECT_KEY_NAMEKEY, nameKeyPayload);
        }
      }
      if (stringCount > 0) {
        final byte[] stringPayload;
        if (stringAllPathNodeKeysValid && stringEncPath != null) {
          stringPayload = stringEncPath.finish(StringRegion.TAG_KIND_PATH_NODE);
        } else {
          stringPayload = stringEncName.finish(StringRegion.TAG_KIND_NAME);
        }
        if (stringPayload != null && stringPayload.length > 0) {
          table.set(RegionTable.KIND_STRING, stringPayload);
        }
      }
      if (boolCount > 0) {
        final byte boolTagKind = booleanAllPathNodeKeysValid
            ? BooleanRegion.TAG_KIND_PATH_NODE
            : BooleanRegion.TAG_KIND_NAME;
        final int[] boolTags = booleanAllPathNodeKeysValid ? boolPathTags : boolNameTags;
        final byte[] boolPayload = BooleanRegion.encode(
            boolValBuf, boolTags, boolCount, boolTagKind);
        if (boolPayload != null && boolPayload.length > 0) {
          table.set(RegionTable.KIND_BOOLEAN, boolPayload);
        }
      }
      return table.isEmpty() ? null : table;
    }

    /**
     * Lever 3 inject pass: walks the bitmap, looks up each elided fused-primitive
     * slot's value from the corresponding PAX region (NumberRegion / StringRegion
     * / BooleanRegion) keyed by {@code (tag, slotRank)}, and writes the original
     * payload bytes into the heap at the recorded value field offset+width.
     * Called after the heap is fully expanded with placeholder zeros at the
     * elided positions.
     *
     * <p>Per-kind dispatch:
     * <ul>
     *   <li>NUMBER (49): {@link NumberRegion#decodeValueAt} → {@code [type:1][delta-varint long]}</li>
     *   <li>STRING (50): {@link StringRegion#decodeStringOffset}/{@code decodeStringLength} → {@code [0:1][writeSignedToSegment(length)][rawBytes]}</li>
     *   <li>BOOLEAN (48): {@link BooleanRegion#decodeAt} → {@code [bool:1]}</li>
     * </ul>
     *
     * <p>Tag determination is per-kind, identical to the writer's pre-scan and
     * the {@link #buildRegionTable} collection: each region's {@code tagKind}
     * (NAME or PATH_NODE) selects whether to use the slot's nameKey (field 3)
     * or pathNodeKey (field 4) as the lookup tag. Per-tag {@code slotRank}
     * counters mirror the writer's emission order — slots walked in
     * bitmap-ascending order match the {@link KeyValueLeafPage} region
     * builders' walk order, ensuring ranks align bit-for-bit.
     *
     * <p>HFT contract: zero alloc. All headers + rank counters are thread-local
     * scratches, cleared on entry. No per-slot intermediate {@code byte[]} —
     * STRING bytes copied directly via {@link MemorySegment#copy} from payload
     * to heap.
     */
    private static void injectValueElidedRecords(final MemorySegment slottedPage,
        final long recordPageKey, final int populatedCount,
        final byte[] valueElidedTypes, final short[] valueOffs,
        final byte[] valueWidths, final RegionTable regionTable) {
      // Per-kind region payloads + headers — parsed lazily on first encounter
      // of each kind so pages with only one kind don't pay the parse cost for
      // the others. {@code numberHeader} is set once we hit a NUMBER slot, etc.
      final byte[] numberPayload = regionTable.payload(RegionTable.KIND_NUMBER);
      final byte[] stringPayload = regionTable.payload(RegionTable.KIND_STRING);
      final byte[] booleanPayload = regionTable.payload(RegionTable.KIND_BOOLEAN);

      NumberRegion.Header numberHeader = null;
      // Non-null only when the number region is delta-encoded: all values are
      // bulk-decoded once here (O(n)) so per-slot access is O(1) instead of the
      // O(index) delta prefix-sum that would make this loop O(n²).
      long[] numberValues = null;
      StringRegion.Header stringHeader = null;
      BooleanRegion.Header booleanHeader = null;

      // Per-kind tag→rank counters (each thread-local, cleared on entry).
      Int2IntOpenHashMap numberRanks = null;
      Int2IntOpenHashMap stringRanks = null;
      Int2IntOpenHashMap booleanRanks = null;

      final long pageKeyBase = recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT;
      // Walk slots in bitmap-ascending order — same as the writer's pre-scan
      // and buildRegionTable's iteration order, ensuring rank consistency.
      int entryIdx = 0;
      int typeIdx = 0;
      for (int w = 0; w < PageLayout.BITMAP_WORDS; w++) {
        long word = PageLayout.getBitmapWord(slottedPage, w);
        while (word != 0) {
          final int bit = Long.numberOfTrailingZeros(word);
          final int slot = (w << 6) | bit;
          final int valueWidth = valueWidths[entryIdx] & 0xFF;
          if (valueWidth > 0) {
            // Read the on-disk type byte (used only for NUMBER subtype dispatch);
            // the kind itself is read from the slotted page directly.
            final byte typeByte = valueElidedTypes[typeIdx * 2];
            typeIdx++;

            final int slotHeapOffset = PageLayout.getDirHeapOffset(slottedPage, slot);
            final long recordBase = PageLayout.HEAP_START + slotHeapOffset;
            final int kindIdRead = slottedPage.get(ValueLayout.JAVA_BYTE, recordBase) & 0xFF;
            final int fcRead = NodeFieldLayout.fieldCountForKind(kindIdRead);
            final int valueOff = valueOffs[entryIdx] & 0xFFFF;
            final long valueAbsOff = recordBase + 1 + fcRead + valueOff;

            if (kindIdRead == KeyValueLeafPage.FUSED_OBJECT_NAMED_NUMBER_KIND_ID) {
              if (numberPayload == null || numberPayload.length == 0) {
                throw new SirixIOException(
                    "value-elision: NUMBER region missing for elided slot " + slot);
              }
              if (numberHeader == null) {
                numberHeader = NUMBER_HEADER_SCRATCH.get();
                numberHeader.parseInto(numberPayload);
                numberRanks = VALUE_RANK_COUNTER.get();
                numberRanks.clear();
                numberRanks.defaultReturnValue(0);
                if (NumberRegion.isDelta(numberHeader.encodingKind)) {
                  long[] scratch = NUMBER_VALUES_SCRATCH.get();
                  if (scratch.length < numberHeader.count) {
                    scratch = new long[numberHeader.count];
                    NUMBER_VALUES_SCRATCH.set(scratch);
                  }
                  NumberRegion.decodeAllValues(numberPayload, numberHeader, scratch);
                  numberValues = scratch;
                }
              }
              final int tag = lookupTagForSlot(slottedPage, slot, recordBase, fcRead,
                  numberHeader.tagKind == NumberRegion.TAG_KIND_PATH_NODE, pageKeyBase);
              final int rank = numberRanks.get(tag);
              numberRanks.put(tag, rank + 1);
              final int tagId = NumberRegion.lookupTag(numberHeader, tag);
              if (tagId < 0) {
                throw new SirixIOException(
                    "value-elision: tag " + tag + " not found in NumberRegion at slot " + slot);
              }
              final int absIdx = numberHeader.tagStart[tagId] + rank;
              if (absIdx >= numberHeader.count) {
                throw new SirixIOException(
                    "value-elision: NUMBER slot rank out of bounds at slot " + slot
                        + ": absIdx=" + absIdx + " count=" + numberHeader.count);
              }
              final long longVal = numberValues != null
                  ? numberValues[absIdx]
                  : NumberRegion.decodeValueAt(numberPayload, numberHeader, absIdx);
              slottedPage.set(ValueLayout.JAVA_BYTE, valueAbsOff, typeByte);
              final int actualWidth;
              if (typeByte == NUMBER_TYPE_INTEGER) {
                actualWidth = 1 + DeltaVarIntCodec.writeSignedToSegment(slottedPage,
                    valueAbsOff + 1, (int) longVal);
              } else {
                // NUMBER_TYPE_LONG (3)
                actualWidth = 1 + DeltaVarIntCodec.writeSignedLongToSegment(slottedPage,
                    valueAbsOff + 1, longVal);
              }
              if (actualWidth != valueWidth) {
                throw new SirixIOException(
                    "value-elision: NUMBER width mismatch at slot " + slot
                        + ": expected=" + valueWidth + " actual=" + actualWidth
                        + " type=" + typeByte + " value=" + longVal);
              }
            } else if (kindIdRead == KeyValueLeafPage.FUSED_OBJECT_NAMED_STRING_KIND_ID) {
              if (stringPayload == null || stringPayload.length == 0) {
                throw new SirixIOException(
                    "value-elision: STRING region missing for elided slot " + slot);
              }
              if (stringHeader == null) {
                stringHeader = STRING_HEADER_SCRATCH.get();
                stringHeader.parseInto(stringPayload);
                stringRanks = STRING_RANK_COUNTER.get();
                stringRanks.clear();
                stringRanks.defaultReturnValue(0);
              }
              final int tag = lookupTagForSlot(slottedPage, slot, recordBase, fcRead,
                  stringHeader.tagKind == StringRegion.TAG_KIND_PATH_NODE, pageKeyBase);
              final int rank = stringRanks.get(tag);
              stringRanks.put(tag, rank + 1);
              final int tagId = StringRegion.lookupTag(stringHeader, tag);
              if (tagId < 0) {
                throw new SirixIOException(
                    "value-elision: tag " + tag + " not found in StringRegion at slot " + slot);
              }
              final int absIdx = stringHeader.tagStart[tagId] + rank;
              if (absIdx >= stringHeader.count) {
                throw new SirixIOException(
                    "value-elision: STRING slot rank out of bounds at slot " + slot
                        + ": absIdx=" + absIdx + " count=" + stringHeader.count);
              }
              final int dictId = StringRegion.decodeDictIdAt(stringPayload, stringHeader, absIdx);
              final int strOff = StringRegion.decodeStringOffset(stringPayload, stringHeader, tagId, dictId);
              final int strLen = StringRegion.decodeStringLength(stringPayload, stringHeader, tagId, dictId);
              // Reconstruct heap layout: [isCompressed=0:1][length:varint][rawBytes].
              slottedPage.set(ValueLayout.JAVA_BYTE, valueAbsOff, (byte) 0);
              final int lenWidth = DeltaVarIntCodec.writeSignedToSegment(slottedPage,
                  valueAbsOff + 1, strLen);
              MemorySegment.copy(stringPayload, strOff, slottedPage,
                  ValueLayout.JAVA_BYTE, valueAbsOff + 1 + lenWidth, strLen);
              final int actualWidth = 1 + lenWidth + strLen;
              if (actualWidth != valueWidth) {
                throw new SirixIOException(
                    "value-elision: STRING width mismatch at slot " + slot
                        + ": expected=" + valueWidth + " actual=" + actualWidth
                        + " strLen=" + strLen + " lenWidth=" + lenWidth);
              }
            } else if (kindIdRead == KeyValueLeafPage.FUSED_OBJECT_NAMED_BOOLEAN_KIND_ID) {
              if (booleanPayload == null || booleanPayload.length == 0) {
                throw new SirixIOException(
                    "value-elision: BOOLEAN region missing for elided slot " + slot);
              }
              if (booleanHeader == null) {
                booleanHeader = BOOLEAN_HEADER_SCRATCH.get();
                booleanHeader.parseInto(booleanPayload);
                booleanRanks = BOOLEAN_RANK_COUNTER.get();
                booleanRanks.clear();
                booleanRanks.defaultReturnValue(0);
              }
              final int tag = lookupTagForSlot(slottedPage, slot, recordBase, fcRead,
                  booleanHeader.tagKind == BooleanRegion.TAG_KIND_PATH_NODE,
                  pageKeyBase);
              final int rank = booleanRanks.get(tag);
              booleanRanks.put(tag, rank + 1);
              final int tagId = BooleanRegion.lookupTag(booleanHeader, tag);
              if (tagId < 0) {
                throw new SirixIOException(
                    "value-elision: tag " + tag + " not found in BooleanRegion at slot " + slot);
              }
              final int absIdx = booleanHeader.tagStart[tagId] + rank;
              if (absIdx >= booleanHeader.count) {
                throw new SirixIOException(
                    "value-elision: BOOLEAN slot rank out of bounds at slot " + slot
                        + ": absIdx=" + absIdx + " count=" + booleanHeader.count);
              }
              final boolean boolVal = BooleanRegion.decodeAt(
                  booleanPayload, booleanHeader, absIdx);
              slottedPage.set(ValueLayout.JAVA_BYTE, valueAbsOff, (byte) (boolVal ? 1 : 0));
              if (valueWidth != 1) {
                throw new SirixIOException(
                    "value-elision: BOOLEAN width must be 1 at slot " + slot
                        + ": got=" + valueWidth);
              }
            } else {
              throw new SirixIOException(
                  "value-elision: unexpected kindId " + kindIdRead + " at slot " + slot);
            }
          }
          entryIdx++;
          word &= word - 1;
        }
      }
    }

    /**
     * Resolve the per-slot tag for value-elision injection. When the region's
     * {@code tagKind} is {@link NumberRegion#TAG_KIND_PATH_NODE} the tag is the
     * slot's pathNodeKey (field 4, decoded as a delta-varint with the slot's
     * own nodeKey as the base). Otherwise it's the slot's nameKey (field 3,
     * decoded as a signed varint). All three primitive-fused kinds (48/49/50)
     * place name/path keys at the same field indices.
     *
     * <p>HFT contract: pure offset-table read + varint decode, no allocations.
     */
    private static int lookupTagForSlot(final MemorySegment slottedPage, final int slot,
        final long recordBase, final int fcRead, final boolean pathNode,
        final long pageKeyBase) {
      if (pathNode) {
        // PATH_NODE_KEY at field index 4 — same constant for all primitive-fused kinds.
        final int pnkOff = slottedPage.get(ValueLayout.JAVA_BYTE,
            recordBase + 1 + NodeFieldLayout.OBJNAMEDNUM_PATH_NODE_KEY) & 0xFF;
        final long fusedNodeKey = pageKeyBase + slot;
        final long pnkLong = DeltaVarIntCodec.decodeDeltaFromSegment(slottedPage,
            recordBase + 1 + fcRead + pnkOff, fusedNodeKey);
        return (int) pnkLong;
      }
      // NAME_KEY at field index 3 — same constant for all primitive-fused kinds.
      final int nameKeyOff = slottedPage.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJNAMEDNUM_NAME_KEY) & 0xFF;
      return DeltaVarIntCodec.decodeSignedFromSegment(slottedPage,
          recordBase + 1 + fcRead + nameKeyOff);
    }

    /**
     * Lever 4 inject pass: walks the bitmap, looks up each elided fused
     * {@code OBJECT_NAMED_*} (kindIds 48-51) slot's nameKey via {@link
     * ObjectKeyNameKeyRegion#nameKeyForSlot(byte[], int)}, and re-encodes
     * the {@code [signed-varint nameKey]} into the heap at the recorded
     * {@code nameKeyOffs[entryIdx]} offset and {@code nameKeyWidths[entryIdx]}
     * width. Called BEFORE the value-elision inject because that pass reads
     * the slot's nameKey field when {@link NumberRegion#TAG_KIND_NAME} (or
     * the corresponding STRING/BOOLEAN) is in effect.
     *
     * <p>Width round-trip is verified — {@link DeltaVarIntCodec#computeSignedEncodedWidth}
     * is deterministic per {@code int} value, so the width recorded by the
     * writer must exactly match what we re-encode here. Mismatch implies
     * a corrupt page or region.
     *
     * <p>HFT contract: zero alloc on hot path. Region payload is held by the
     * regionTable and dispatched per slot via a single primitive-int return.
     */
    private static void injectNameKeyElidedRecords(final MemorySegment slottedPage,
        final int populatedCount, final short[] nameKeyOffs,
        final byte[] nameKeyWidths, final RegionTable regionTable) {
      final byte[] nameKeyPayload = regionTable.payload(RegionTable.KIND_OBJECT_KEY_NAMEKEY);
      if (nameKeyPayload == null || nameKeyPayload.length == 0) {
        throw new SirixIOException(
            "name-key elision: ObjectKeyNameKeyRegion missing for elided page");
      }
      // Walk slots in bitmap-ascending order — the writer's pre-scan + the
      // strip-pass + the region builder all use the same order, so entryIdx
      // aligns one-to-one with the populated slot enumeration.
      int entryIdx = 0;
      for (int w = 0; w < PageLayout.BITMAP_WORDS; w++) {
        long word = PageLayout.getBitmapWord(slottedPage, w);
        while (word != 0) {
          final int bit = Long.numberOfTrailingZeros(word);
          final int slot = (w << 6) | bit;
          final int recordedWidth = nameKeyWidths[entryIdx] & 0xFF;
          if (recordedWidth > 0) {
            final int slotHeapOffset = PageLayout.getDirHeapOffset(slottedPage, slot);
            final long recordBase = PageLayout.HEAP_START + slotHeapOffset;
            final int kindIdRead = slottedPage.get(ValueLayout.JAVA_BYTE, recordBase) & 0xFF;
            final int fcRead = NodeFieldLayout.fieldCountForKind(kindIdRead);
            // Note: nameKeys come from String.hashCode() in NamePage and may be
            // negative; we cannot use a {@code < 0} sentinel from the lookup. The
            // writer guarantees the slot is in the region's bitmap, so the lookup
            // always succeeds for elided slots — width round-trip below catches
            // any inconsistency.
            final int nameKey = ObjectKeyNameKeyRegion.nameKeyForSlot(nameKeyPayload, slot);
            final int nameKeyOff = nameKeyOffs[entryIdx] & 0xFFFF;
            final long writePos = recordBase + 1 + fcRead + nameKeyOff;
            final int actualWidth = DeltaVarIntCodec.writeSignedToSegment(
                slottedPage, writePos, nameKey);
            if (actualWidth != recordedWidth) {
              throw new SirixIOException(
                  "name-key elision: width mismatch at slot " + slot
                      + ": expected=" + recordedWidth + " actual=" + actualWidth
                      + " nameKey=" + nameKey + " kindId=" + kindIdRead);
            }
          }
          entryIdx++;
          word &= word - 1;
        }
      }
    }

    private static void writeFsstSymbolTable(final BytesOut<?> sink, final KeyValueLeafPage page) {
      final byte[] fsstSymbolTable = page.getFsstSymbolTable();
      if (fsstSymbolTable != null && fsstSymbolTable.length > 0) {
        sink.writeInt(fsstSymbolTable.length);
        sink.write(fsstSymbolTable);
      } else {
        sink.writeInt(0);
      }
    }

    private static void compressAndCache(final ResourceConfiguration resourceConfig, final BytesOut<?> sink,
        final KeyValueLeafPage keyValueLeafPage) {
      final BytesIn<?> uncompressedBytes = sink.bytesForRead();
      final ByteHandlerPipeline pipeline = resourceConfig.byteHandlePipeline;

      if (pipeline.supportsMemorySegments() && uncompressedBytes instanceof MemorySegmentBytesIn segmentIn) {
        final MemorySegment uncompressed = segmentIn.getSource().asSlice(0, sink.writePosition());
        final MemorySegment compressed = pipeline.compress(uncompressed);
        keyValueLeafPage.setCompressedSegment(compressed);
      } else {
        final byte[] uncompressedArray = uncompressedBytes.toByteArray();
        final byte[] compressedPage = compressViaStream(pipeline, uncompressedArray);
        keyValueLeafPage.setBytes(Bytes.wrapForWrite(compressedPage));
      }
    }

    /**
     * When the outer byte-handler pipeline is empty and the inner heap LZ4
     * still runs, we leave a lot of structural overhead (compact dir, region
     * table bytes, PAX dictionaries) uncompressed on disk — that was what the
     * old outer LZ4 used to mop up. As an interim middle-ground between
     * "pure structural" and "outer LZ4", we re-use the inner heap LZ4 to
     * wrap the remaining un-compressed sink bytes for the
     * {@code -Dsirix.compression=none} path.
     *
     * <p>Experimental: the write path never shipped this; we are measuring.
     * It is still structural in the sense that the LZ4 pass happens inside
     * the page serializer, not as a pipelined ByteHandler, so the no-outer-
     * pipeline contract is preserved and the cold-path decompress CPU bill
     * is the same single LZ4 pass we already pay.
     */
  },

  /**
   * {@link NamePage}.
   */
  NAMEPAGE((byte) 2, NamePage.class) {
    @Override
    public Page deserializePage(final ResourceConfiguration resourceConfig, final BytesIn<?> source,
        final SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      final BinaryEncodingVersion binaryVersion = readVersionAndFlags(source);

      switch (binaryVersion) {
        case V0 -> {
          Page delegate = PageUtils.createDelegate(source, type);

          final Int2LongMap maxNodeKeys = PageKind.deserializeMaxNodeKeys(source);
          final Int2LongMap maxHotPageKeys = PageKind.deserializeMaxNodeKeys(source);
          final int numberOfArrays = source.readInt();
          final Int2IntMap currentMaxLevelsOfIndirectPages =
              PageKind.deserializeCurrentMaxLevelsOfIndirectPages(source);

          final NamePage namePage =
              new NamePage(delegate, maxNodeKeys, maxHotPageKeys, currentMaxLevelsOfIndirectPages, numberOfArrays);
          // Approach B: per-dictionary live entry node-keys (Roaring) for O(live) reconstruction.
          final int liveEntryNodeKeySize = source.readInt();
          for (int i = 0; i < liveEntryNodeKeySize; i++) {
            final int sizeInBytes = source.readInt();
            final byte[] buf = new byte[sizeInBytes];
            source.read(buf, 0, sizeInBytes);
            final Roaring64Bitmap bitmap = new Roaring64Bitmap();
            try {
              bitmap.deserialize(ByteBuffer.wrap(buf));
            } catch (final IOException e) {
              throw new IllegalStateException("NamePage live-key bitmap deserialization failed", e);
            }
            namePage.putLiveEntryNodeKeys(i, bitmap);
          }
          return namePage;
        }
        default -> throw new IllegalStateException();
      }
    }

    @Override
    public void serializePage(final ResourceConfiguration resourceConfig, final BytesOut<?> sink, final Page page,
        final SerializationType type) {
      NamePage namePage = (NamePage) page;
      sink.writeByte(NAMEPAGE.id);
      writeVersionAndFlags(sink);
      Page delegate = namePage.delegate();

      PageKind.writeDelegateType(delegate, sink);
      PageKind.serializeDelegate(sink, delegate, type);

      final int maxNodeKeySize = namePage.getMaxNodeKeySize();
      sink.writeInt(maxNodeKeySize);
      for (int i = 0; i < maxNodeKeySize; i++) {
        final long keys = namePage.getMaxNodeKey(i);
        sink.writeLong(keys);
      }

      final int maxHotPageKeysSize = namePage.getMaxHotPageKeySize();
      sink.writeInt(maxHotPageKeysSize);
      for (int i = 0; i < maxHotPageKeysSize; i++) {
        sink.writeLong(namePage.getMaxHotPageKey(i));
      }

      sink.writeInt(namePage.getNumberOfArrays());

      final int currentMaxLevelOfIndirectPagesSize = namePage.getCurrentMaxLevelOfIndirectPagesSize();
      sink.writeInt(currentMaxLevelOfIndirectPagesSize);
      for (int i = 0; i < currentMaxLevelOfIndirectPagesSize; i++) {
        sink.writeByte((byte) namePage.getCurrentMaxLevelOfIndirectPages(i));
      }

      // Approach B: per-dictionary live entry node-keys (Roaring) for O(live) reconstruction.
      final int liveEntryNodeKeySize = namePage.getMaxNodeKeySize();
      sink.writeInt(liveEntryNodeKeySize);
      for (int i = 0; i < liveEntryNodeKeySize; i++) {
        final Roaring64Bitmap bitmap = namePage.getLiveEntryNodeKeysToSerialize(i);
        final int sizeInBytes = (int) bitmap.serializedSizeInBytes();
        final byte[] buf = new byte[sizeInBytes];
        try {
          bitmap.serialize(ByteBuffer.wrap(buf));
        } catch (final IOException e) {
          throw new IllegalStateException("NamePage live-key bitmap serialization failed", e);
        }
        sink.writeInt(sizeInBytes);
        sink.write(buf);
      }
    }
  },

  /**
   * {@link UberPage}.
   */
  UBERPAGE((byte) 3, UberPage.class) {
    @Override
    public Page deserializePage(final ResourceConfiguration resourceConfig, final BytesIn<?> source,
        final SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      final BinaryEncodingVersion binaryVersion = readVersionAndFlags(source);

      switch (binaryVersion) {
        case V0 -> {
          final int revisionCount = source.readInt();

          return new UberPage(revisionCount);
        }
        default -> throw new IllegalStateException();
      }
    }

    @Override
    public void serializePage(final ResourceConfiguration resourceConfig, final BytesOut<?> sink, final Page page,
        final SerializationType type) {
      UberPage uberPage = (UberPage) page;

      sink.writeByte(UBERPAGE.id);
      writeVersionAndFlags(sink);
      sink.writeInt(uberPage.getRevisionCount());
      uberPage.setBootstrap(false);
    }
  },

  /**
   * {@link IndirectPage}.
   */
  INDIRECTPAGE((byte) 4, IndirectPage.class) {
    @Override
    public Page deserializePage(final ResourceConfiguration resourceConfiguration, final BytesIn<?> source,
        final SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      final BinaryEncodingVersion binaryVersion = readVersionAndFlags(source);

      switch (binaryVersion) {
        case V0 -> {
          Page delegate = PageUtils.createDelegate(source, type);
          return new IndirectPage(delegate);
        }
        default -> throw new IllegalStateException();
      }
    }

    @Override
    public void serializePage(final ResourceConfiguration resourceConfig, final BytesOut<?> sink, final Page page,
        final SerializationType type) {
      IndirectPage indirectPage = (IndirectPage) page;
      Page delegate = indirectPage.delegate();
      sink.writeByte(INDIRECTPAGE.id);
      writeVersionAndFlags(sink);

      PageKind.writeDelegateType(delegate, sink);

      PageKind.serializeDelegate(sink, delegate, type);
    }
  },

  /**
   * {@link RevisionRootPage}.
   */
  REVISIONROOTPAGE((byte) 5, RevisionRootPage.class) {
    @Override
    public Page deserializePage(final ResourceConfiguration resourceConfiguration, final BytesIn<?> source,
        final SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      final BinaryEncodingVersion binaryVersion = readVersionAndFlags(source);

      switch (binaryVersion) {
        case V0 -> {
          Page delegate = new BitmapReferencesPage(RevisionRootPage.REVISION_ROOT_PAGE_REFERENCE_COUNT, source, type);
          final int revision = source.readInt();
          final long maxNodeKeyInDocumentIndex = source.readLong();
          final long maxNodeKeyInChangedNodesIndex = source.readLong();
          final long maxNodeKeyInRecordToRevisionsIndex = source.readLong();
          final long revisionTimestamp = source.readLong();
          String commitMessage = null;
          User user = null;
          if (source.readBoolean()) {
            final byte[] commitMessageBytes = new byte[source.readInt()];
            source.read(commitMessageBytes);
            commitMessage = new String(commitMessageBytes, Constants.DEFAULT_ENCODING);
          }
          final int currentMaxLevelOfDocumentIndexIndirectPages = source.readByte() & 0xFF;
          final int currentMaxLevelOfChangedNodesIndirectPages = source.readByte() & 0xFF;
          final int currentMaxLevelOfRecordToRevisionsIndirectPages = source.readByte() & 0xFF;

          if (source.readBoolean()) {
            //noinspection DataFlowIssue
            user = new User(source.readUtf8(), UUID.fromString(source.readUtf8()));
          }

          return new RevisionRootPage(delegate,
                                      revision,
                                      maxNodeKeyInDocumentIndex,
                                      maxNodeKeyInChangedNodesIndex,
                                      maxNodeKeyInRecordToRevisionsIndex,
                                      revisionTimestamp,
                                      commitMessage,
                                      currentMaxLevelOfDocumentIndexIndirectPages,
                                      currentMaxLevelOfChangedNodesIndirectPages,
                                      currentMaxLevelOfRecordToRevisionsIndirectPages,
                                      user);
        }
        default -> throw new IllegalStateException();
      }
    }

    @Override
    public void serializePage(final ResourceConfiguration resourceConfig, final BytesOut<?> sink, final Page page,
        final SerializationType type) {
      RevisionRootPage revisionRootPage = (RevisionRootPage) page;
      sink.writeByte(REVISIONROOTPAGE.id);
      writeVersionAndFlags(sink);

      Page delegate = revisionRootPage.delegate();
      PageKind.serializeDelegate(sink, delegate, type);

      //initial variables from RevisionRootPage, to serialize
      final Instant commitTimestamp = revisionRootPage.getCommitTimestamp();
      final int revision = revisionRootPage.getRevision();
      final long maxNodeKeyInDocumentIndex = revisionRootPage.getMaxNodeKeyInDocumentIndex();
      final long maxNodeKeyInChangedNodesIndex = revisionRootPage.getMaxNodeKeyInChangedNodesIndex();
      final long maxNodeKeyInRecordToRevisionsIndex = revisionRootPage.getMaxNodeKeyInRecordToRevisionsIndex();
      final String commitMessage = revisionRootPage.getCommitMessage();
      final int currentMaxLevelOfDocumentIndexIndirectPages =
          revisionRootPage.getCurrentMaxLevelOfDocumentIndexIndirectPages();
      final int currentMaxLevelOfChangedNodesIndirectPages =
          revisionRootPage.getCurrentMaxLevelOfChangedNodesIndexIndirectPages();
      final int currentMaxLevelOfRecordToRevisionsIndirectPages =
          revisionRootPage.getCurrentMaxLevelOfRecordToRevisionsIndexIndirectPages();
      final long revisionTimestamp =
          commitTimestamp == null ? Instant.now().toEpochMilli() : commitTimestamp.toEpochMilli();
      revisionRootPage.setRevisionTimestamp(revisionTimestamp);

      sink.writeInt(revision);
      sink.writeLong(maxNodeKeyInDocumentIndex);
      sink.writeLong(maxNodeKeyInChangedNodesIndex);
      sink.writeLong(maxNodeKeyInRecordToRevisionsIndex);
      sink.writeLong(revisionTimestamp);
      sink.writeBoolean(commitMessage != null);

      if (commitMessage != null) {
        final byte[] commitMessageBytes = commitMessage.getBytes(Constants.DEFAULT_ENCODING);
        sink.writeInt(commitMessageBytes.length);
        sink.write(commitMessageBytes);
      }

      sink.writeByte((byte) currentMaxLevelOfDocumentIndexIndirectPages);
      sink.writeByte((byte) currentMaxLevelOfChangedNodesIndirectPages);
      sink.writeByte((byte) currentMaxLevelOfRecordToRevisionsIndirectPages);

      final Optional<User> user = revisionRootPage.getUser();
      final boolean hasUser = user.isPresent();
      sink.writeBoolean(hasUser);

      if (hasUser) {
        var currUser = user.get();
        sink.writeUtf8(currUser.getName());
        sink.writeUtf8(currUser.getId().toString());
      }
    }
  },

  /**
   * {@link PathSummaryPage}.
   */
  PATHSUMMARYPAGE((byte) 6, PathSummaryPage.class) {
    @Override
    public Page deserializePage(final ResourceConfiguration resourceConfig, final BytesIn<?> source,
        final SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      final BinaryEncodingVersion binaryVersion = readVersionAndFlags(source);

      switch (binaryVersion) {
        case V0 -> {
          Page delegate = PageUtils.createDelegate(source, type);

          final int maxNodeKeysSize = source.readInt();
          Int2LongMap maxNodeKeys = new Int2LongOpenHashMap(maxNodeKeysSize);
          for (int i = 0; i < maxNodeKeysSize; i++) {
            maxNodeKeys.put(i, source.readLong());
          }

          final int currentMaxLevelOfIndirectPagesSize = source.readInt();
          Int2IntMap currentMaxLevelsOfIndirectPages = new Int2IntOpenHashMap(currentMaxLevelOfIndirectPagesSize);
          for (int i = 0; i < currentMaxLevelOfIndirectPagesSize; i++) {
            currentMaxLevelsOfIndirectPages.put(i, source.readByte() & 0xFF);
          }
          return new PathSummaryPage(delegate, maxNodeKeys, currentMaxLevelsOfIndirectPages);
        }
        default -> throw new IllegalStateException();
      }
    }

    @Override
    public void serializePage(final ResourceConfiguration resourceConfig, final BytesOut<?> sink, final Page page,
        final SerializationType type) {
      PathSummaryPage pathSummaryPage = (PathSummaryPage) page;
      sink.writeByte(PATHSUMMARYPAGE.id);
      writeVersionAndFlags(sink);

      Page delegate = pathSummaryPage.delegate();
      // Shared helper instead of a hand-rolled (byte) 0 — a non-ReferencesPage4 delegate would
      // have been silently mislabeled.
      PageKind.writeDelegateType(delegate, sink);
      PageKind.serializeDelegate(sink, delegate, type);

      final int maxNodeKeySize = pathSummaryPage.getMaxNodeKeySize();
      sink.writeInt(maxNodeKeySize);
      for (int i = 0; i < maxNodeKeySize; i++) {
        sink.writeLong(pathSummaryPage.getMaxNodeKey(i));
      }

      final int currentMaxLevelOfIndirectPagesSize = pathSummaryPage.getCurrentMaxLevelOfIndirectPagesSize();
      sink.writeInt(currentMaxLevelOfIndirectPagesSize);
      for (int i = 0; i < currentMaxLevelOfIndirectPagesSize; i++) {
        sink.writeByte((byte) pathSummaryPage.getCurrentMaxLevelOfIndirectPages(i));
      }
    }
  },

  /**
   * {@link CASPage}.
   */
  CASPAGE((byte) 8, CASPage.class) {
    public Page deserializePage(final ResourceConfiguration resourceConfig, final BytesIn<?> source,
        final SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      final BinaryEncodingVersion binaryVersion = readVersionAndFlags(source);

      switch (binaryVersion) {
        case V0 -> {
          Page delegate = PageUtils.createDelegate(source, type);

          final Int2LongMap maxNodeKeys = PageKind.deserializeMaxNodeKeys(source);
          final Int2LongMap maxHotPageKeys = PageKind.deserializeMaxNodeKeys(source);
          final Int2IntMap currentMaxLevelsOfIndirectPages =
              PageKind.deserializeCurrentMaxLevelsOfIndirectPages(source);

          return new CASPage(delegate, maxNodeKeys, maxHotPageKeys, currentMaxLevelsOfIndirectPages);
        }
        default -> throw new IllegalStateException();
      }
    }

    @Override
    public void serializePage(final ResourceConfiguration resourceConfig, final BytesOut<?> sink, final Page page,
        final SerializationType type) {
      CASPage casPage = (CASPage) page;
      Page delegate = casPage.delegate();
      sink.writeByte(CASPAGE.id);
      writeVersionAndFlags(sink);

      PageKind.writeDelegateType(delegate, sink);
      PageKind.serializeDelegate(sink, delegate, type);

      final int maxNodeKeySize = casPage.getMaxNodeKeySize();
      sink.writeInt(maxNodeKeySize);
      for (int i = 0; i < maxNodeKeySize; i++) {
        sink.writeLong(casPage.getMaxNodeKey(i));
      }

      final int maxHotPageKeysSize = casPage.getMaxHotPageKeySize();
      sink.writeInt(maxHotPageKeysSize);
      for (int i = 0; i < maxHotPageKeysSize; i++) {
        sink.writeLong(casPage.getMaxHotPageKey(i));
      }

      final int currentMaxLevelOfIndirectPagesSize = casPage.getCurrentMaxLevelOfIndirectPagesSize();
      sink.writeInt(currentMaxLevelOfIndirectPagesSize);
      for (int i = 0; i < currentMaxLevelOfIndirectPagesSize; i++) {
        sink.writeByte((byte) casPage.getCurrentMaxLevelOfIndirectPages(i));
      }
    }
  },

  /**
   * {@link OverflowPage}.
   */
  OVERFLOWPAGE((byte) 9, OverflowPage.class) {
    @Override
    public Page deserializePage(final ResourceConfiguration resourceConfiguration, final BytesIn<?> source,
        final SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      final BinaryEncodingVersion binaryVersion = readVersionAndFlags(source);

      switch (binaryVersion) {
        case V0 -> {
          final int length = source.readInt();
          if (length < 0 || length > OverflowPage.MAX_PAGE_BYTES) {
            throw new IllegalStateException("Corrupt OverflowPage length " + length
                + " (max " + OverflowPage.MAX_PAGE_BYTES + ")");
          }
          final byte[] data = new byte[length];
          source.read(data);

          // Store as byte array to avoid memory leaks from Arena.global()
          return new OverflowPage(data);
        }
        default -> throw new IllegalStateException();
      }
    }

    @Override
    public void serializePage(final ResourceConfiguration resourceConfig, final BytesOut<?> sink, final Page page,
        SerializationType type) {
      OverflowPage overflowPage = (OverflowPage) page;
      sink.writeByte(OVERFLOWPAGE.id);
      writeVersionAndFlags(sink);
      
      // Write byte array directly
      byte[] data = overflowPage.getDataBytes();
      sink.writeInt(data.length);
      sink.write(data);
    }
  },

  /**
   * {@link PathPage}.
   */
  PATHPAGE((byte) 10, PathPage.class) {
    @Override
    public Page deserializePage(ResourceConfiguration resourceConfiguration, BytesIn<?> source,
        SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      final BinaryEncodingVersion binaryVersion = readVersionAndFlags(source);
      switch (binaryVersion) {
        case V0 -> {
          final Page delegate = PageUtils.createDelegate(source, type);

          final Int2LongMap maxNodeKeys = PageKind.deserializeMaxNodeKeys(source);
          final Int2LongMap maxHotPageKeys = PageKind.deserializeMaxNodeKeys(source);
          final Int2IntMap currentMaxLevelsOfIndirectPages =
              PageKind.deserializeCurrentMaxLevelsOfIndirectPages(source);

          return new PathPage(delegate, maxNodeKeys, maxHotPageKeys, currentMaxLevelsOfIndirectPages);
        }
        default -> throw new IllegalStateException();
      }
    }

    @Override
    public void serializePage(final ResourceConfiguration resourceConfig, BytesOut<?> sink, Page page,
        SerializationType type) {
      PathPage pathPage = (PathPage) page;
      Page delegate = pathPage.delegate();
      sink.writeByte(PATHPAGE.id);
      writeVersionAndFlags(sink);

      PageKind.writeDelegateType(delegate, sink);
      PageKind.serializeDelegate(sink, delegate, type);

      final int maxNodeKeysSize = pathPage.getMaxNodeKeySize();
      sink.writeInt(maxNodeKeysSize);
      for (int i = 0; i < maxNodeKeysSize; i++) {
        sink.writeLong(pathPage.getMaxNodeKey(i));
      }

      final int maxHotPageKeysSize = pathPage.getMaxHotPageKeySize();
      sink.writeInt(maxHotPageKeysSize);
      for (int i = 0; i < maxHotPageKeysSize; i++) {
        sink.writeLong(pathPage.getMaxHotPageKey(i));
      }

      final int currentMaxLevelOfIndirectPagesSize = pathPage.getCurrentMaxLevelOfIndirectPagesSize();
      sink.writeInt(currentMaxLevelOfIndirectPagesSize);
      for (int i = 0; i < currentMaxLevelOfIndirectPagesSize; i++) {
        sink.writeByte((byte) pathPage.getCurrentMaxLevelOfIndirectPages(i));
      }
    }
  },

  /**
   * {@link PathPage}.
   */
  DEWEYIDPAGE((byte) 11, DeweyIDPage.class) {
    @Override
    public Page deserializePage(ResourceConfiguration resourceConfiguration, BytesIn<?> source,
        SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      final BinaryEncodingVersion binaryVersion = readVersionAndFlags(source);

      switch (binaryVersion) {
        case V0 -> {
          Page delegate = PageUtils.createDelegate(source, type);
          final long maxNodeKey = source.readLong();
          final int currentMaxLevelOfIndirectPages = source.readByte() & 0xFF;
          return new DeweyIDPage(delegate, maxNodeKey, currentMaxLevelOfIndirectPages);
        }
        default -> throw new IllegalStateException();
      }
    }

    @Override
    public void serializePage(ResourceConfiguration resourceConfig, BytesOut<?> sink, Page page,
        SerializationType type) {
      DeweyIDPage deweyIDPage = (DeweyIDPage) page;
      Page delegate = deweyIDPage.delegate();
      sink.writeByte(DEWEYIDPAGE.id);
      writeVersionAndFlags(sink);

      PageKind.writeDelegateType(delegate, sink);

      PageKind.serializeDelegate(sink, delegate, type);
      sink.writeLong(deweyIDPage.getMaxNodeKey());
      sink.writeByte((byte) deweyIDPage.getCurrentMaxLevelOfIndirectPages());
    }
  },

  /**
   * {@link HOTLeafPage} - HOT trie leaf page for cache-friendly secondary indexes.
   */
  HOT_LEAF_PAGE((byte) 12, HOTLeafPage.class) {
    @Override
    public Page deserializePage(ResourceConfiguration resourceConfiguration, BytesIn<?> source,
        SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      final byte envelopeFlags = readVersionAndFlagsAllowing(source, HOTLeafPage.FLAG_SEGMENT_REFS);

      // Read header
      final long recordPageKey = Utils.getVarLong(source);
      final int revision = source.readInt();
      final IndexType indexType = IndexType.getType(source.readByte());

      // Read common prefix (V2 format with prefix compression)
      final int commonPrefixLen = Short.toUnsignedInt(source.readShort());
      final byte[] commonPrefix;
      if (commonPrefixLen > 0) {
        commonPrefix = new byte[commonPrefixLen];
        source.read(commonPrefix);
      } else {
        commonPrefix = new byte[0];
      }

      final int rawEntryCount = source.readInt();
      final boolean completeDump = (rawEntryCount & 0x80000000) != 0;
      final int entryCount = rawEntryCount & 0x7FFFFFFF;
      final int usedSlotMemorySize = source.readInt();

      // Read slot offsets (allocate MAX_ENTRIES to allow insertions after deserialization)
      final int[] slotOffsets = new int[HOTLeafPage.MAX_ENTRIES];
      for (int i = 0; i < entryCount; i++) {
        slotOffsets[i] = source.readInt();
      }

      // Read slot memory (zero-copy when possible)
      MemorySegmentAllocator allocator = Allocators.getInstance();

      final MemorySegment slotMemory;
      final Runnable releaser;

      final boolean canZeroCopy = decompressionResult != null && source instanceof MemorySegmentBytesIn;
      if (canZeroCopy) {
        final MemorySegment sourceSegment = ((MemorySegmentBytesIn) source).getSource();
        slotMemory = sourceSegment.asSlice(source.position(), usedSlotMemorySize);
        source.skip(usedSlotMemorySize);
        releaser = decompressionResult.transferOwnership();
      } else {
        slotMemory = allocator.allocate(HOTLeafPage.DEFAULT_SIZE);
        if (source instanceof MemorySegmentBytesIn msSource) {
          MemorySegment.copy(msSource.getSource(), source.position(), slotMemory, 0, usedSlotMemorySize);
          source.skip(usedSlotMemorySize);
        } else {
          byte[] slotData = new byte[usedSlotMemorySize];
          source.read(slotData);
          MemorySegment.copy(slotData, 0, slotMemory, java.lang.foreign.ValueLayout.JAVA_BYTE, 0, usedSlotMemorySize);
        }
        final MemorySegment segmentToRelease = slotMemory;
        releaser = () -> allocator.release(segmentToRelease);
      }

      final HOTLeafPage page = new HOTLeafPage(recordPageKey, revision, indexType, slotMemory,
          releaser, slotOffsets, entryCount, usedSlotMemorySize, commonPrefix, commonPrefixLen);
      page.setCompleteDump(completeDump);
      if ((envelopeFlags & HOTLeafPage.FLAG_SEGMENT_REFS) != 0) {
        deserializeSegmentRefs(source, page);
      }
      return page;
    }

    @Override
    public void serializePage(ResourceConfiguration resourceConfig, BytesOut<?> sink,
        Page page, SerializationType type) {
      final HOTLeafPage hotLeaf = (HOTLeafPage) page;
      final VersioningType versioningType = resourceConfig.versioningType;
      final boolean sparseEmit = versioningType != VersioningType.FULL
          && hotLeaf.getCompletePageRef() != null
          && hotLeaf.hasDirty();

      // Segment-reference side map (projection segment pages, see
      // docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §2.3/§2.4). EVERY fragment —
      // sparse or full — serializes the COMPLETE map: the writer-side page
      // always holds the authoritative current map (copy() carries it across
      // CoW; puts/removes mutate it), so the newest fragment is authoritative
      // and the fragment merge never unions older fragments' refs (which
      // would resurrect removed segments).
      final boolean hasSegmentRefs = hotLeaf.segmentRefCount() > 0;

      sink.writeByte(HOT_LEAF_PAGE.id);
      writeVersionAndFlags(sink, hasSegmentRefs ? HOTLeafPage.FLAG_SEGMENT_REFS : 0);

      // Write header
      Utils.putVarLong(sink, hotLeaf.getPageKey());
      sink.writeInt(hotLeaf.getRevision());
      sink.writeByte(hotLeaf.getIndexType().getID());

      // Write common prefix
      final byte[] prefix = hotLeaf.getCommonPrefix();
      final int prefixLen = hotLeaf.getCommonPrefixLen();
      sink.writeShort((short) prefixLen);
      if (prefixLen > 0) {
        sink.write(prefix, 0, prefixLen);
      }

      if (sparseEmit) {
        final int dirtyCount = hotLeaf.getDirtyEntryCount();
        final int dirtyUsed = hotLeaf.getDirtyEntriesUsedSize();
        final int encodedDirtyCount = hotLeaf.isCompleteDump()
            ? dirtyCount | 0x80000000 : dirtyCount;
        sink.writeInt(encodedDirtyCount);
        sink.writeInt(dirtyUsed);

        if (dirtyCount == 0) {
          if (hasSegmentRefs) {
            serializeSegmentRefs(sink, hotLeaf);
          }
          return;
        }

        final byte[] packed = new byte[dirtyUsed];
        final int[] packedOffsets = new int[dirtyCount];
        final int written = hotLeaf.packDirtyEntries(packed, packedOffsets);
        assert written == dirtyUsed : "packed size mismatch: " + written + " vs " + dirtyUsed;
        for (int i = 0; i < dirtyCount; i++) {
          sink.writeInt(packedOffsets[i]);
        }
        sink.write(packed);
        if (hasSegmentRefs) {
          serializeSegmentRefs(sink, hotLeaf);
        }
        return;
      }

      final int encodedFullCount = hotLeaf.isCompleteDump()
          ? hotLeaf.getEntryCount() | 0x80000000 : hotLeaf.getEntryCount();
      sink.writeInt(encodedFullCount);
      sink.writeInt(hotLeaf.getUsedSlotsSize());

      // Write slot offsets
      int entryCount = hotLeaf.getEntryCount();
      for (int i = 0; i < entryCount; i++) {
        sink.writeInt(hotLeaf.getSlotOffset(i));
      }

      // Write slot memory (bulk copy)
      MemorySegment slots = hotLeaf.slots();
      int usedSize = hotLeaf.getUsedSlotsSize();
      byte[] slotData = new byte[usedSize];
      MemorySegment.copy(slots, java.lang.foreign.ValueLayout.JAVA_BYTE, 0, slotData, 0, usedSize);
      sink.write(slotData);
      if (hasSegmentRefs) {
        serializeSegmentRefs(sink, hotLeaf);
      }
    }
  },

  /**
   * {@link HOTIndirectPage} - HOT trie interior node with compound structure.
   */
  HOT_INDIRECT_PAGE((byte) 13, HOTIndirectPage.class) {
    @Override
    public Page deserializePage(ResourceConfiguration resourceConfiguration, BytesIn<?> source,
        SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      final BinaryEncodingVersion binaryVersion = readVersionAndFlags(source);
      
      // Read header
      final long pageKey = Utils.getVarLong(source);
      final int revision = source.readInt();
      final int height = source.readByte() & 0xFF;
      final byte nodeTypeId = source.readByte();
      final byte layoutTypeId = source.readByte();
      final int numChildren = source.readInt();
      
      final HOTIndirectPage.NodeType nodeType = HOTIndirectPage.NodeType.fromID(nodeTypeId);
      final HOTIndirectPage.LayoutType layoutType = HOTIndirectPage.LayoutType.fromID(layoutTypeId);

      // Read layout-specific discriminative bit data
      final int initialBytePos;
      final long bitMask;
      final short mostSignificantBitIndex;
      final byte[] extractionPositions;
      final long[] extractionMasks;
      final int numExtractionBytes;
      int totalDiscBits;

      if (layoutType == HOTIndirectPage.LayoutType.MULTI_MASK) {
        // MultiMask: read extraction positions and masks
        initialBytePos = 0;
        bitMask = 0;
        mostSignificantBitIndex = source.readShort();
        numExtractionBytes = source.readShort() & 0xFFFF;
        extractionPositions = new byte[numExtractionBytes];
        source.read(extractionPositions);
        final int numChunks = (numExtractionBytes + 7) / 8;
        extractionMasks = new long[numChunks];
        for (int i = 0; i < numChunks; i++) {
          extractionMasks[i] = source.readLong();
        }
        totalDiscBits = 0;
        for (final long mask : extractionMasks) {
          totalDiscBits += Long.bitCount(mask);
        }
      } else {
        // SingleMask: read initialBytePos + bitMask
        initialBytePos = Short.toUnsignedInt(source.readShort());
        bitMask = source.readLong();
        mostSignificantBitIndex = source.readShort();
        extractionPositions = null;
        extractionMasks = null;
        numExtractionBytes = 0;
        totalDiscBits = Long.bitCount(bitMask);
      }

      // Read partial keys (width determined by total number of discriminative bits)
      final int partialKeyWidth = HOTIndirectPage.determinePartialKeyWidthFromBitCount(totalDiscBits);
      final int[] partialKeys = new int[numChildren];
      if (partialKeyWidth <= 1) {
        for (int i = 0; i < numChildren; i++) {
          partialKeys[i] = source.readByte() & 0xFF;
        }
      } else if (partialKeyWidth <= 2) {
        for (int i = 0; i < numChildren; i++) {
          partialKeys[i] = source.readShort() & 0xFFFF;
        }
      } else {
        for (int i = 0; i < numChildren; i++) {
          partialKeys[i] = source.readInt();
        }
      }

      // Read child references with embedded pageFragments — mirrors
      // SerializationType.readPageFragments so HOT leaf fragment chains survive parent round-trip.
      // Database/resource ids on PageFragmentKeyImpl are placeholders and patched by
      // Reader.fixupPageReferenceIds on the parent's references after this returns.
      final PageReference[] children = new PageReference[numChildren];
      for (int i = 0; i < numChildren; i++) {
        final PageReference ref = new PageReference();
        final long childKey = source.readLong();
        ref.setKey(childKey);
        final int fragmentCount = source.readByte() & 0xff;
        for (int f = 0; f < fragmentCount; f++) {
          final int fragRevision = source.readInt();
          final long fragKey = source.readLong();
          ref.addPageFragment(new PageFragmentKeyImpl(fragRevision, fragKey, 0L, 0L));
        }
        children[i] = ref;
      }

      // Create appropriate node type and layout
      final HOTIndirectPage created;
      if (layoutType == HOTIndirectPage.LayoutType.MULTI_MASK) {
        created = switch (nodeType) {
          case SPAN_NODE -> HOTIndirectPage.createSpanNodeMultiMask(pageKey, revision,
              extractionPositions, extractionMasks, numExtractionBytes,
              partialKeys, children, height, mostSignificantBitIndex);
          case MULTI_NODE -> HOTIndirectPage.createMultiNodeMultiMask(pageKey, revision,
              extractionPositions, extractionMasks, numExtractionBytes,
              partialKeys, children, height, mostSignificantBitIndex);
        };
      } else {
        created = switch (nodeType) {
          case SPAN_NODE -> HOTIndirectPage.createSpanNode(pageKey, revision, initialBytePos, bitMask,
              partialKeys, children, height);
          case MULTI_NODE -> {
            // Read and discard legacy childIndex payload to keep binary compatibility.
            byte[] childIndexArray = new byte[256];
            source.read(childIndexArray);
            yield HOTIndirectPage.createMultiNode(pageKey, revision, initialBytePos, bitMask, partialKeys,
                children, height);
          }
        };
      }

      return created;
    }

    @Override
    public void serializePage(ResourceConfiguration resourceConfig, BytesOut<?> sink,
        Page page, SerializationType type) {
      HOTIndirectPage hotIndirect = (HOTIndirectPage) page;
      sink.writeByte(HOT_INDIRECT_PAGE.id);
      writeVersionAndFlags(sink);
      
      // Write header
      Utils.putVarLong(sink, hotIndirect.getPageKey());
      sink.writeInt(hotIndirect.getRevision());
      sink.writeByte((byte) hotIndirect.getHeight());
      sink.writeByte(hotIndirect.getNodeType().getID());
      sink.writeByte(hotIndirect.getLayoutType().getID());
      sink.writeInt(hotIndirect.getNumChildren());
      
      // Write layout-specific discriminative bit data
      if (hotIndirect.getLayoutType() == HOTIndirectPage.LayoutType.MULTI_MASK) {
        // MultiMask: write extraction positions and masks
        sink.writeShort(hotIndirect.getMostSignificantBitIndex());
        final int numExtractionBytes = hotIndirect.getNumExtractionBytes();
        sink.writeShort((short) numExtractionBytes);
        final byte[] extractionPositions = hotIndirect.getExtractionPositions();
        if (extractionPositions != null) {
          sink.write(extractionPositions);
        }
        final long[] extractionMasks = hotIndirect.getExtractionMasks();
        if (extractionMasks != null) {
          for (final long mask : extractionMasks) {
            sink.writeLong(mask);
          }
        }
      } else {
        // SingleMask: write initialBytePos + bitMask
        sink.writeShort((short) hotIndirect.getInitialBytePos());
        sink.writeLong(hotIndirect.getBitMask());
        sink.writeShort(hotIndirect.getMostSignificantBitIndex());
      }

      // Write partial keys (width determined by total number of discriminative bits)
      final int[] partialKeysData = hotIndirect.getPartialKeys();
      final int pkWidth = HOTIndirectPage.determinePartialKeyWidthFromBitCount(hotIndirect.getTotalDiscBits());
      if (pkWidth <= 1) {
        for (final int pk : partialKeysData) {
          sink.writeByte((byte) pk);
        }
      } else if (pkWidth <= 2) {
        for (final int pk : partialKeysData) {
          sink.writeShort((short) pk);
        }
      } else {
        for (final int pk : partialKeysData) {
          sink.writeInt(pk);
        }
      }

      // Write child references — embed pageFragments so the leaf fragment chain
      // (built by VersioningType.bumpHOTPageFragmentChain at CoW time) survives
      // round-trip through the parent indirect page on disk.
      final int numChildren = hotIndirect.getNumChildren();
      for (int i = 0; i < numChildren; i++) {
        final PageReference ref = hotIndirect.getChildReference(i);
        if (ref == null) {
          sink.writeLong(Constants.NULL_ID_LONG);
          sink.writeByte((byte) 0);
          continue;
        }
        sink.writeLong(ref.getKey());
        final var fragments = ref.getPageFragments();
        final int fragmentCount = fragments.size();
        if (fragmentCount > 255) {
          // One byte on the wire — a silent (byte) wrap would mis-frame everything after.
          throw new IllegalStateException("Too many page fragments to serialize: " + fragmentCount + " (max 255)");
        }
        sink.writeByte((byte) fragmentCount);
        for (int f = 0; f < fragmentCount; f++) {
          final var fragKey = fragments.get(f);
          sink.writeInt(fragKey.revision());
          sink.writeLong(fragKey.key());
        }
      }

      // For SingleMask MultiNode, write the 256-byte child index array
      if (hotIndirect.getLayoutType() != HOTIndirectPage.LayoutType.MULTI_MASK
          && hotIndirect.getNodeType() == HOTIndirectPage.NodeType.MULTI_NODE) {
        final byte[] childIdx = hotIndirect.getChildIndex();
        if (childIdx != null) {
          sink.write(childIdx);
        } else {
          // Write zeroes as fallback
          sink.write(new byte[256]);
        }
      }

    }
  },

  /**
   * {@link BitmapChunkPage} - Versioned bitmap chunk for NodeReferences in HOT indexes.
   */
  BITMAP_CHUNK_PAGE((byte) 14, BitmapChunkPage.class) {
    @Override
    public Page deserializePage(ResourceConfiguration resourceConfiguration, BytesIn<?> source,
        SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      readVersionAndFlags(source);

      // Read page key (stored before calling deserialize)
      final long pageKey = Utils.getVarLong(source);
      
      try {
        // Create a DataInputStream wrapper for BitmapChunkPage.deserialize.
        // toByteArray() returns bytes from current position to end, so offset within the result is 0.
        final byte[] remaining = source.toByteArray();
        final java.io.DataInputStream dis = new java.io.DataInputStream(
            new java.io.ByteArrayInputStream(remaining, 0, remaining.length));
        return BitmapChunkPage.deserialize(dis, pageKey);
      } catch (java.io.IOException e) {
        throw new UncheckedIOException("Failed to deserialize BitmapChunkPage", e);
      }
    }

    @Override
    public void serializePage(ResourceConfiguration resourceConfig, BytesOut<?> sink,
        Page page, SerializationType type) {
      BitmapChunkPage chunkPage = (BitmapChunkPage) page;
      sink.writeByte(BITMAP_CHUNK_PAGE.id);
      writeVersionAndFlags(sink);
      
      // Write page key
      Utils.putVarLong(sink, chunkPage.getPageKey());
      
      try {
        // Serialize to byte array first, then write
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        chunkPage.serialize(new DataOutputStream(baos));
        byte[] data = baos.toByteArray();
        sink.write(data);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to serialize BitmapChunkPage", e);
      }
    }
  },

  /**
   * {@link VectorPage}.
   */
  VECTORPAGE((byte) 15, VectorPage.class) {
    @Override
    public Page deserializePage(final ResourceConfiguration resourceConfig, final BytesIn<?> source,
        final SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      final BinaryEncodingVersion binaryVersion = readVersionAndFlags(source);

      switch (binaryVersion) {
        case V0 -> {
          final Page delegate = PageUtils.createDelegate(source, type);

          final Int2LongMap maxNodeKeys = PageKind.deserializeMaxNodeKeys(source);
          final Int2IntMap currentMaxLevelsOfIndirectPages =
              PageKind.deserializeCurrentMaxLevelsOfIndirectPages(source);

          return new VectorPage(delegate, maxNodeKeys, currentMaxLevelsOfIndirectPages);
        }
        default -> throw new IllegalStateException("Unknown binary encoding version: " + binaryVersion);
      }
    }

    @Override
    public void serializePage(final ResourceConfiguration resourceConfig, final BytesOut<?> sink, final Page page,
        final SerializationType type) {
      final VectorPage vectorPage = (VectorPage) page;
      final Page delegate = vectorPage.delegate();
      sink.writeByte(VECTORPAGE.id);
      writeVersionAndFlags(sink);

      PageKind.writeDelegateType(delegate, sink);
      PageKind.serializeDelegate(sink, delegate, type);

      final int maxNodeKeySize = vectorPage.getMaxNodeKeySize();
      sink.writeInt(maxNodeKeySize);
      for (int i = 0; i < maxNodeKeySize; i++) {
        sink.writeLong(vectorPage.getMaxNodeKey(i));
      }

      final int currentMaxLevelOfIndirectPagesSize = vectorPage.getCurrentMaxLevelOfIndirectPagesSize();
      sink.writeInt(currentMaxLevelOfIndirectPagesSize);
      for (int i = 0; i < currentMaxLevelOfIndirectPagesSize; i++) {
        sink.writeByte((byte) vectorPage.getCurrentMaxLevelOfIndirectPages(i));
      }
    }
  },

  /**
   * {@link ProjectionIndexPage}.
   */
  PROJECTIONPAGE((byte) 16, ProjectionIndexPage.class) {
    @Override
    public Page deserializePage(final ResourceConfiguration resourceConfig, final BytesIn<?> source,
        final SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      final BinaryEncodingVersion binaryVersion = readVersionAndFlags(source);

      switch (binaryVersion) {
        case V0 -> {
          final Page delegate = PageUtils.createDelegate(source, type);

          final Int2LongMap maxNodeKeys = PageKind.deserializeMaxNodeKeys(source);
          final Int2LongMap maxHotPageKeys = PageKind.deserializeMaxNodeKeys(source);
          final Int2IntMap currentMaxLevelsOfIndirectPages =
              PageKind.deserializeCurrentMaxLevelsOfIndirectPages(source);

          return new ProjectionIndexPage(delegate, maxNodeKeys, maxHotPageKeys, currentMaxLevelsOfIndirectPages);
        }
        default -> throw new IllegalStateException("Unknown binary encoding version: " + binaryVersion);
      }
    }

    @Override
    public void serializePage(final ResourceConfiguration resourceConfig, final BytesOut<?> sink, final Page page,
        final SerializationType type) {
      final ProjectionIndexPage projectionPage = (ProjectionIndexPage) page;
      final Page delegate = projectionPage.delegate();
      sink.writeByte(PROJECTIONPAGE.id);
      writeVersionAndFlags(sink);

      PageKind.writeDelegateType(delegate, sink);
      PageKind.serializeDelegate(sink, delegate, type);

      final int maxNodeKeySize = projectionPage.getMaxNodeKeySize();
      sink.writeInt(maxNodeKeySize);
      for (int i = 0; i < maxNodeKeySize; i++) {
        sink.writeLong(projectionPage.getMaxNodeKey(i));
      }

      final int maxHotPageKeysSize = projectionPage.getMaxHotPageKeySize();
      sink.writeInt(maxHotPageKeysSize);
      for (int i = 0; i < maxHotPageKeysSize; i++) {
        sink.writeLong(projectionPage.getMaxHotPageKey(i));
      }

      final int currentMaxLevelOfIndirectPagesSize = projectionPage.getCurrentMaxLevelOfIndirectPagesSize();
      sink.writeInt(currentMaxLevelOfIndirectPagesSize);
      for (int i = 0; i < currentMaxLevelOfIndirectPagesSize; i++) {
        sink.writeByte((byte) projectionPage.getCurrentMaxLevelOfIndirectPages(i));
      }
    }
  },

  /**
   * {@link ValidTimeIndexPage}.
   */
  VALIDTIMEPAGE((byte) 17, ValidTimeIndexPage.class) {
    @Override
    public Page deserializePage(final ResourceConfiguration resourceConfig, final BytesIn<?> source,
        final SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      final BinaryEncodingVersion binaryVersion = readVersionAndFlags(source);

      switch (binaryVersion) {
        case V0 -> {
          final Page delegate = PageUtils.createDelegate(source, type);

          final Int2LongMap maxNodeKeys = PageKind.deserializeMaxNodeKeys(source);
          final Int2LongMap maxHotPageKeys = PageKind.deserializeMaxNodeKeys(source);
          final Int2IntMap currentMaxLevelsOfIndirectPages =
              PageKind.deserializeCurrentMaxLevelsOfIndirectPages(source);

          return new ValidTimeIndexPage(delegate, maxNodeKeys, maxHotPageKeys, currentMaxLevelsOfIndirectPages);
        }
        default -> throw new IllegalStateException("Unknown binary encoding version: " + binaryVersion);
      }
    }

    @Override
    public void serializePage(final ResourceConfiguration resourceConfig, final BytesOut<?> sink, final Page page,
        final SerializationType type) {
      final ValidTimeIndexPage validTimePage = (ValidTimeIndexPage) page;
      final Page delegate = validTimePage.delegate();
      sink.writeByte(VALIDTIMEPAGE.id);
      writeVersionAndFlags(sink);

      PageKind.writeDelegateType(delegate, sink);
      PageKind.serializeDelegate(sink, delegate, type);

      final int maxNodeKeySize = validTimePage.getMaxNodeKeySize();
      sink.writeInt(maxNodeKeySize);
      for (int i = 0; i < maxNodeKeySize; i++) {
        sink.writeLong(validTimePage.getMaxNodeKey(i));
      }

      final int maxHotPageKeysSize = validTimePage.getMaxHotPageKeySize();
      sink.writeInt(maxHotPageKeysSize);
      for (int i = 0; i < maxHotPageKeysSize; i++) {
        sink.writeLong(validTimePage.getMaxHotPageKey(i));
      }

      final int currentMaxLevelOfIndirectPagesSize = validTimePage.getCurrentMaxLevelOfIndirectPagesSize();
      sink.writeInt(currentMaxLevelOfIndirectPagesSize);
      for (int i = 0; i < currentMaxLevelOfIndirectPagesSize; i++) {
        sink.writeByte((byte) validTimePage.getCurrentMaxLevelOfIndirectPages(i));
      }
    }
  };

  private static void writeDelegateType(Page delegate, BytesOut<?> sink) {
    switch (delegate) {
      case ReferencesPage4 ignored -> sink.writeByte((byte) 0);
      case BitmapReferencesPage ignored -> sink.writeByte((byte) 1);
      case FullReferencesPage ignored -> sink.writeByte((byte) 2);
      default -> throw new IllegalStateException("Unexpected value: " + delegate);
    }
  }

  private static void serializeDelegate(BytesOut<?> sink, Page delegate, SerializationType type) {
    switch (delegate) {
      case ReferencesPage4 page -> type.serializeReferencesPage4(sink, page.getReferences(), page.getOffsets());
      case BitmapReferencesPage page ->
          type.serializeBitmapReferencesPage(sink, page.getReferences(), page.getBitmap());
      case FullReferencesPage ignored ->
          type.serializeFullReferencesPage(sink, ((FullReferencesPage) delegate).getReferencesArray());
      default -> throw new IllegalStateException("Unexpected value: " + delegate);
    }
  }

  private static Int2LongMap deserializeMaxNodeKeys(final BytesIn<?> source) {
    final int maxNodeKeysSize = source.readInt();
    final Int2LongMap maxNodeKeys = new Int2LongOpenHashMap((int) Math.ceil(maxNodeKeysSize / 0.75));

    for (int i = 0; i < maxNodeKeysSize; i++) {
      maxNodeKeys.put(i, source.readLong());
    }
    return maxNodeKeys;
  }

  private static Int2IntMap deserializeCurrentMaxLevelsOfIndirectPages(final BytesIn<?> source) {
    final int currentMaxLevelOfIndirectPagesSize = source.readInt();
    final Int2IntMap currentMaxLevelsOfIndirectPages =
        new Int2IntOpenHashMap((int) Math.ceil(currentMaxLevelOfIndirectPagesSize / 0.75));

    for (int i = 0; i < currentMaxLevelOfIndirectPagesSize; i++) {
      currentMaxLevelsOfIndirectPages.put(i, source.readByte() & 0xFF);
    }
    return currentMaxLevelsOfIndirectPages;
  }

  /**
   * Writes the shared page envelope after the kind byte: {@code [binaryVersion u8][flags u8]}.
   * The flags byte is reserved extension space for every page kind (all bits zero in V0) —
   * without it, any additive change to a non-KVLP page required a global version bump.
   */
  static void writeVersionAndFlags(final BytesOut<?> sink) {
    sink.writeByte(BinaryEncodingVersion.V0.byteVersion());
    sink.writeByte((byte) 0);
  }

  /**
   * Reads and validates the shared page envelope: the version byte (throws on unknown) and the
   * reserved flags byte (must be zero in V0 — a nonzero value means a newer writer used an
   * extension this build does not understand, so misparsing is not an option).
   */
  static BinaryEncodingVersion readVersionAndFlags(final BytesIn<?> source) {
    final BinaryEncodingVersion version = BinaryEncodingVersion.fromByte(source.readByte());
    final byte flags = source.readByte();
    if (flags != 0) {
      throw new IllegalStateException("Unknown page envelope flags 0x" + Integer.toHexString(flags & 0xFF)
          + " — page written by a newer version");
    }
    return version;
  }

  /**
   * Serializes a HOT leaf's segment-reference side map as a trailing section:
   * {@code varint count + count × (compositeKey u64, diskOffsetKey u64)}. Entries are emitted in
   * ascending compositeKey order so identical maps serialize to identical bytes. Every reference
   * must be resolved (disk key assigned) by the time the owning leaf serializes — the commit
   * descent writes segment pages before the leaf (OverflowPage discipline); an unresolved
   * reference here means a segment page bypassed the commit branch and would persist as a
   * dangling {@code -1}, so fail loudly instead.
   */
  private static void serializeSegmentRefs(final BytesOut<?> sink, final HOTLeafPage hotLeaf) {
    final long[] keys = hotLeaf.segmentRefKeysSorted();
    Utils.putVarLong(sink, keys.length);
    for (final long compositeKey : keys) {
      final PageReference ref = hotLeaf.getPageReference(compositeKey);
      if (ref == null || ref.getKey() == Constants.NULL_ID_LONG) {
        throw new IllegalStateException("Unresolved projection segment reference at compositeKey=" + compositeKey
            + " (leaf pageKey=" + hotLeaf.getPageKey() + ") during HOT leaf serialization — segment pages must be"
            + " written (key assigned) by the commit descent before the owning leaf serializes.");
      }
      sink.writeLong(compositeKey);
      sink.writeLong(ref.getKey());
    }
  }

  /** Inverse of {@link #serializeSegmentRefs}: rebuilds the side map with key-only references. */
  private static void deserializeSegmentRefs(final BytesIn<?> source, final HOTLeafPage page) {
    final long count = Utils.getVarLong(source);
    for (long i = 0; i < count; i++) {
      final long compositeKey = source.readLong();
      final long diskKey = source.readLong();
      final PageReference ref = new PageReference();
      ref.setKey(diskKey);
      page.setPageReference(compositeKey, ref);
    }
  }

  /**
   * Flags-carrying variant of {@link #writeVersionAndFlags(BytesOut)} for page kinds that use
   * envelope flag bits as additive format extensions (today: {@link #HOT_LEAF_PAGE}'s
   * segment-reference section).
   */
  static void writeVersionAndFlags(final BytesOut<?> sink, final byte flags) {
    sink.writeByte(BinaryEncodingVersion.V0.byteVersion());
    sink.writeByte(flags);
  }

  /**
   * Reads the shared page envelope for a kind that understands specific flag bits. Validates the
   * version byte (throws on unknown) and rejects any flag bit outside {@code allowedMask} — an
   * unknown bit means a newer writer used an extension this build does not understand, so
   * misparsing is not an option.
   *
   * @return the flags byte (all bits within {@code allowedMask})
   */
  static byte readVersionAndFlagsAllowing(final BytesIn<?> source, final byte allowedMask) {
    final BinaryEncodingVersion version = BinaryEncodingVersion.fromByte(source.readByte());
    if (version != BinaryEncodingVersion.V0) {
      throw new IllegalStateException("Unknown binary encoding version: " + version);
    }
    final byte flags = source.readByte();
    if ((flags & ~allowedMask) != 0) {
      throw new IllegalStateException("Unknown page envelope flags 0x" + Integer.toHexString(flags & 0xFF)
          + " (allowed mask 0x" + Integer.toHexString(allowedMask & 0xFF) + ") — page written by a newer version");
    }
    return flags;
  }

  /**
   * Mapping of keys -> page
   */
  private static final Map<Byte, PageKind> INSTANCEFORID = new HashMap<>();

  /**
   * Mapping of class -> page.
   */
  private static final Map<Class<? extends Page>, PageKind> INSTANCEFORCLASS = new HashMap<>();

  /**
   * Per-thread scratch array for single-pass serializePage.
   * Layout: [heapOffset0, dataLength0, heapOffset1, dataLength1, ...].
   * Max 1024 slots x 2 ints = 8 KB per thread.
   */
  private static final ThreadLocal<int[]> SERIALIZE_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT * 2]);

  /** Per-thread reusable buffer for number-region value collection at seal time. */
  private static final ThreadLocal<long[]> NUMBER_VALUE_SCRATCH =
      ThreadLocal.withInitial(() -> new long[PageLayout.SLOT_COUNT]);

  /** Per-thread reusable buffer for number-region parent-nameKey collection at seal time. */
  private static final ThreadLocal<int[]> NUMBER_PARENT_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread reusable buffer for number-region parent-pathNodeKey (int-truncated)
   * collection at seal time. Populated in parallel with {@link #NUMBER_PARENT_SCRATCH};
   * the final encoder call picks one buffer based on the resolved tagKind.
   */
  private static final ThreadLocal<int[]> NUMBER_PATH_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  /** Per-thread reusable buffer for OBJECT_KEY nameKey values at seal time. */
  private static final ThreadLocal<int[]> OBJECT_KEY_NAMEKEY_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  /** Per-thread reusable buffer for OBJECT_KEY slot indices at seal time. */
  private static final ThreadLocal<int[]> OBJECT_KEY_SLOT_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread reusable {@link StringRegion.Encoder}. The encoder's internal
   * fastutil maps and per-tag arrays are retained across pages; only per-page
   * counts and value-byte references are cleared via {@link
   * StringRegion.Encoder#reset()}. Writer threads therefore pay at most one
   * encoder allocation in their lifetime instead of one per committed page.
   */
  private static final ThreadLocal<StringRegion.Encoder> STRING_REGION_ENCODER =
      ThreadLocal.withInitial(StringRegion.Encoder::new);

  /**
   * Second per-thread {@link StringRegion.Encoder} used for the path-tagged
   * variant when the resource runs with path-summary enabled. Populated in
   * lockstep with {@link #STRING_REGION_ENCODER} until an invalid pathNodeKey
   * is observed; the final {@code finish()} chooses between the two based on
   * the resolved tagKind.
   */
  private static final ThreadLocal<StringRegion.Encoder> STRING_REGION_ENCODER_PATH =
      ThreadLocal.withInitial(StringRegion.Encoder::new);

  // ==================== Offset-table dedup scratch ====================

  /**
   * Per-thread kindId array (one entry per populated slot). Filled during the
   * page serialize walk and consumed by {@link OffsetTableTemplatePool#build}.
   */
  private static final ThreadLocal<int[]> SLOT_KINDID_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread heapOffset array (one entry per populated slot). Avoids a
   * re-walk of the bitmap when consulting offset-table bytes on the slotted
   * page heap.
   */
  private static final ThreadLocal<int[]> SLOT_HEAPOFF_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread dataLength array (one entry per populated slot). Stored to
   * avoid re-reading {@link PageLayout#getDirDataLength} after the
   * offset-table dedup walk.
   */
  private static final ThreadLocal<int[]> SLOT_DATALEN_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread bitmap-slot-index array (one short per populated slot).
   * Records the actual bitmap bit position {@code (word<<6)|bit} for each
   * populated slot in bitmap-walk order. Used by the parentKey column
   * pre-scan to compute {@code nodeKey = pageKeyBase + slot}.
   */
  private static final ThreadLocal<short[]> SLOT_BIT_SCRATCH =
      ThreadLocal.withInitial(() -> new short[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread zero-hash bitmap scratch (one bit per populated slot — byte
   * size {@code ceil(SLOT_COUNT / 8) = 128}). Bit set = record's hash field
   * is all-zero and gets stripped from the staging blob. Only populated when
   * {@link #HASH_ELISION_ENABLED} and the page carries records with a hash
   * field.
   */
  private static final ThreadLocal<byte[]> SLOT_ZERO_HASH_BITMAP_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[(PageLayout.SLOT_COUNT + 7) >>> 3]);

  /**
   * Per-thread per-slot hash offset scratch (one short per populated slot).
   * For slots whose kind has a hash field, stores the byte offset of the
   * hash bytes within the record's data region (as read from the offset
   * table). {@code -1} for slots without a hash field.
   */
  private static final ThreadLocal<short[]> SLOT_HASH_OFFSET_SCRATCH =
      ThreadLocal.withInitial(() -> new short[PageLayout.SLOT_COUNT]);

  /**
   * Enable / disable hash-elision structural encoding. When enabled, pages
   * carrying records whose 8-byte hash field is all-zero strip those bytes
   * from the staging blob and reinject them on read. Saves ~8 bytes/slot
   * for {@code HashType.NONE} databases — the common analytical-workload
   * setting — and compounds the zero-run RLE savings because the leading
   * zeros of adjacent structural fields are now contiguous in the blob.
   *
   * <p>Gated by {@code -Dsirix.hashElision.disable=true} for A/B
   * measurement; default ON.
   */
  private static final boolean HASH_ELISION_ENABLED =
      !Boolean.getBoolean("sirix.hashElision.disable");

  /**
   * Per-thread parentKey column-value scratch (one long per populated slot).
   * Filled during the structural-key column pre-scan and consumed by
   * {@link StructuralKeyColumnCodec}. {@code Fixed#NULL_NODE_KEY} when the
   * slot's kind has no parentKey field, or when the page-wide column is
   * inactive.
   */
  private static final ThreadLocal<long[]> SLOT_PARENT_KEY_SCRATCH =
      ThreadLocal.withInitial(() -> new long[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread parentKey on-disk width scratch (one byte per slot).
   * Bytes 0..15; 0 means "no parentKey field" (kind has no parent). Derived
   * from the offset-table entry delta during the column pre-scan.
   */
  private static final ThreadLocal<byte[]> SLOT_PARENT_KEY_WIDTH_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread parentKey in-data-region offset scratch (one short per slot).
   * Copied from the record's offset table by the column pre-scan so the
   * staging / reconstruct loops don't re-derive it.
   */
  private static final ThreadLocal<short[]> SLOT_PARENT_KEY_OFF_SCRATCH =
      ThreadLocal.withInitial(() -> new short[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread scratch byte buffer for the {@link StructuralKeyColumnCodec}
   * output bytes. Sized for the worst case of a fully-independent column
   * (11 bytes header + N × 10 bytes varint). Grows on demand.
   */
  private static final ThreadLocal<byte[]> PARENT_KEY_COLUMN_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT * 11]);

  /**
   * Per-thread pathNodeKey column-value scratch (one long per populated slot).
   * Filled during the pathNodeKey pre-scan pass and consumed by
   * {@link PathNodeKeyRegion}. Slots whose kind lacks a pathNodeKey field hold
   * {@link Fixed#NULL_NODE_KEY} and are skipped at encode time.
   */
  private static final ThreadLocal<long[]> SLOT_PATH_NODE_KEY_SCRATCH =
      ThreadLocal.withInitial(() -> new long[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread pathNodeKey on-disk width scratch (one byte per slot). Bytes
   * 0..10; 0 means "no pathNodeKey field" (kind has no pathNodeKey). Derived
   * from the offset-table delta during the column pre-scan.
   */
  private static final ThreadLocal<byte[]> SLOT_PATH_NODE_KEY_WIDTH_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread pathNodeKey in-data-region offset scratch (one short per slot).
   * Copied from the record's offset table by the column pre-scan so the
   * staging / reconstruct loops don't re-derive it. Unlike parentKey (always
   * at offset 0), pathNodeKey lives at a kind-specific interior offset.
   */
  private static final ThreadLocal<short[]> SLOT_PATH_NODE_KEY_OFF_SCRATCH =
      ThreadLocal.withInitial(() -> new short[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread compact buffer for the pathNodeKey-bearing slot int-values fed to
   * {@link PathNodeKeyRegion#encodedSize} / {@link PathNodeKeyRegion#encode}.
   * Compacted form — length = slots with pathNodeKey, bitmap order.
   */
  private static final ThreadLocal<int[]> SLOT_PATH_NODE_KEY_VALUES_COMPACT_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread compact slot-index array parallel to {@link
   * #SLOT_PATH_NODE_KEY_VALUES_COMPACT_SCRATCH}. Holds the slot index (0..1023)
   * of each entry so {@link PathNodeKeyRegion#encode} can populate its bitmap.
   */
  private static final ThreadLocal<int[]> SLOT_PATH_NODE_KEY_SLOTS_COMPACT_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread scratch byte buffer for the {@link PathNodeKeyRegion} encoded
   * column bytes. Grows on demand. Worst-case size bound is
   * {@code 1 + 256*4 + 2 + 128 + SLOT_COUNT}.
   */
  private static final ThreadLocal<byte[]> PATH_NODE_KEY_COLUMN_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[1 + 256 * 4 + 2 + 128 + PageLayout.SLOT_COUNT]);

  /**
   * Per-thread 256-int dictionary scratch shared by {@link
   * PathNodeKeyRegion#encodedSize} and {@link PathNodeKeyRegion#encode}.
   * Replaces the former per-page {@code new int[256]} alloc.
   */
  private static final ThreadLocal<int[]> PNK_ENCODE_DICT_SCRATCH =
      ThreadLocal.withInitial(() -> new int[256]);

  /**
   * Per-thread dict-id scratch for {@link PathNodeKeyRegion#encode} — one byte
   * per populated slot. Replaces the former per-page {@code new byte[count]} alloc.
   */
  private static final ThreadLocal<byte[]> PNK_ENCODE_DICT_IDS_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread 16-long bitmap scratch for {@link PathNodeKeyRegion#encode}. The encoder zero-fills
   * on entry so it is safe for Lever-3 region-lookup additions to reuse in future.
   */
  private static final ThreadLocal<long[]> COLUMN_ENCODE_BITMAP_SCRATCH =
      ThreadLocal.withInitial(() -> new long[16]);

  // ============================================================
  // Lever 3: VALUE elision scratches (writer + reader, fused-NUMBER only)
  // ============================================================

  /**
   * Per-thread per-slot value-strip flag (1 byte/slot). Bit 0 set = the slot is
   * a fused {@code OBJECT_NAMED_NUMBER} (kindId 49) whose payload bytes are
   * being elided on this page. Other bits reserved.
   */
  private static final ThreadLocal<byte[]> SLOT_VALUE_ELIDED_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread per-slot value field offset (one short/slot). For fused-NUMBER
   * slots holds the in-data-region offset of the {@code [type:1][varint]}
   * payload, derived from offset-table[OBJNAMEDNUM_PAYLOAD]. Zero for slots
   * that are not value-elided participants.
   */
  private static final ThreadLocal<short[]> SLOT_VALUE_OFF_SCRATCH =
      ThreadLocal.withInitial(() -> new short[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread per-slot value field width (one byte/slot, 0..15 valid range).
   * For fused-NUMBER slots holds the byte count of the {@code [type:1][varint]}
   * payload field. Zero for slots that are not value-elided participants.
   */
  private static final ThreadLocal<byte[]> SLOT_VALUE_WIDTH_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread per-elided-slot type byte ({@code NUMBER_TYPE_INTEGER == 2} or
   * {@code NUMBER_TYPE_LONG == 3}). Packed in slot-ascending order, length =
   * number of elided slots on the page. Stored on disk so the reader can
   * re-encode the original heap bytes byte-for-byte (the varint width depends
   * on whether the value is decoded as int or long).
   */
  private static final ThreadLocal<byte[]> SLOT_VALUE_TYPE_PACKED_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread per-slot decoded long value scratch. Filled by the writer's
   * pre-scan from {@link KeyValueLeafPage#getFusedObjectNamedNumberValueLongFromSlot}
   * for fused-NUMBER slots; consumed by the reader on the inject path.
   */
  private static final ThreadLocal<long[]> SLOT_VALUE_LONG_SCRATCH =
      ThreadLocal.withInitial(() -> new long[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread re-encode buffer for the value bytes (1 type byte + up to 10
   * varint bytes = 11 max). Sized at 16 to give headroom and stay within a
   * single cache-line.
   */
  private static final ThreadLocal<byte[]> VALUE_REENCODE_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[16]);

  /**
   * Per-thread scratch holding the (NumberRegion, slotRank) pair for each
   * fused-NUMBER slot on a value-elided page. Length = populatedCount; entries
   * for non-elided slots are unused. Computed by the reader's pre-expand walk
   * and consumed by the per-record value-injection step. Uses a single int per
   * slot — the rank itself fits in low 16 bits; tag id stored in upper 16 bits.
   * Reduces alloc to one int[] instead of two short[].
   */
  private static final ThreadLocal<int[]> SLOT_VALUE_RANK_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread scratch holding the per-slot decoded value type byte for the
   * reader. One byte per fused-NUMBER slot in slot-ascending order; populated
   * from the on-disk value-elision section.
   */
  private static final ThreadLocal<byte[]> SLOT_VALUE_TYPE_READ_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread {@link NumberRegion.Header} reused on the reader hot path. The
   * header parses inline into existing arrays where possible so we don't pay
   * a fresh allocation per page.
   */
  private static final ThreadLocal<NumberRegion.Header> NUMBER_HEADER_SCRATCH =
      ThreadLocal.withInitial(NumberRegion.Header::new);

  /**
   * Per-thread scratch holding all number values decoded once for a
   * delta-encoded ({@link NumberRegion#ENC_DELTA_ZM}) region. Delta random
   * access is O(index), so the per-slot rehydration loop bulk-decodes the whole
   * region up front (O(n)) and indexes this array instead of paying O(n²).
   * Grows on demand; other encodings never touch it.
   */
  private static final ThreadLocal<long[]> NUMBER_VALUES_SCRATCH =
      ThreadLocal.withInitial(() -> new long[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread Long2IntOpenHashMap-equivalent scratch for tag → slotRank-counter
   * computation during the reader's pre-expand walk. Keyed by tag (parent
   * nameKey or pathNodeKey, depending on tagKind); value is the running count
   * of fused-NUMBER slots seen so far for that tag. Cleared at the start of
   * each pre-expand walk.
   */
  private static final ThreadLocal<it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap> VALUE_RANK_COUNTER =
      ThreadLocal.withInitial(() -> {
        final var m = new it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap(16);
        m.defaultReturnValue(0);
        return m;
      });

  /** Type byte for {@code Integer} payloads (matches {@link io.sirix.node.NodeKind#serializeNumber}). */
  private static final byte NUMBER_TYPE_INTEGER = 2;

  /** Type byte for {@code Long} payloads. */
  private static final byte NUMBER_TYPE_LONG = 3;

  /**
   * Lever 3 elision marker stored in {@link #SLOT_VALUE_ELIDED_SCRATCH} for
   * fused {@code OBJECT_NAMED_STRING} (kindId 50) slots. Distinguishes the
   * STRING-elided code path from NUMBER (which stores 2 / 3) and BOOLEAN
   * (which stores {@link #STRING_ELIDE_MARKER + 1}). Value chosen outside the
   * NUMBER_TYPE_* range so a single equality check disambiguates per-slot kind
   * in the writer pre-scan, while staying non-zero so the strip pass's
   * {@code slotValueElided[i] != 0} guard still fires.
   */
  private static final byte STRING_ELIDE_MARKER = 0x70;

  /** Lever 3 elision marker for fused {@code OBJECT_NAMED_BOOLEAN} (kindId 48) slots. */
  private static final byte BOOLEAN_ELIDE_MARKER = 0x71;

  /**
   * Per-thread reusable {@link BooleanRegion.Header} for
   * the reader's value-elision inject pass. One header per worker thread,
   * reused across pages — its internal {@code int[]} arrays are sized lazily
   * by {@link BooleanRegion.Header#parseInto}.
   */
  private static final ThreadLocal<BooleanRegion.Header> BOOLEAN_HEADER_SCRATCH =
      ThreadLocal.withInitial(BooleanRegion.Header::new);

  /**
   * Per-thread reusable {@link StringRegion.Header} for the reader's
   * value-elision inject pass. One per worker thread.
   */
  private static final ThreadLocal<StringRegion.Header> STRING_HEADER_SCRATCH =
      ThreadLocal.withInitial(StringRegion.Header::new);

  /**
   * Per-thread per-tag rank counter for the BooleanRegion inject pass.
   * Cleared at the start of each per-page injection run.
   */
  private static final ThreadLocal<Int2IntOpenHashMap> BOOLEAN_RANK_COUNTER =
      ThreadLocal.withInitial(() -> {
        final var m = new Int2IntOpenHashMap(16);
        m.defaultReturnValue(0);
        return m;
      });

  /**
   * Per-thread per-tag rank counter for the StringRegion inject pass.
   * Cleared at the start of each per-page injection run.
   */
  private static final ThreadLocal<Int2IntOpenHashMap> STRING_RANK_COUNTER =
      ThreadLocal.withInitial(() -> {
        final var m = new Int2IntOpenHashMap(16);
        m.defaultReturnValue(0);
        return m;
      });

  /** Per-thread {@code boolean[]} scratch for {@link #buildRegionTable} boolean collection. */
  private static final ThreadLocal<boolean[]> BOOLEAN_VALUE_SCRATCH =
      ThreadLocal.withInitial(() -> new boolean[PageLayout.SLOT_COUNT]);

  /** Per-thread {@code int[]} scratch for {@link #buildRegionTable} boolean tag collection. */
  private static final ThreadLocal<int[]> BOOLEAN_TAG_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  /** Per-thread {@code int[]} scratch for {@link #buildRegionTable} boolean path-tag collection. */
  private static final ThreadLocal<int[]> BOOLEAN_PATH_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  // White-box column elision flags. Each call site applies the pick-smaller guard
  // ({@code encodedLen + 4 < rawStrippedBytes}) so the writer falls back to inline
  // bytes whenever the column would bloat the page.

  /** parentKey column. Gate off with {@code -Dsirix.parentKeyColumn.disable=true}. */
  private static final boolean PARENT_KEY_COLUMN_ENABLED =
      !Boolean.getBoolean("sirix.parentKeyColumn.disable");

  /** pathNodeKey dict column. Gate off with {@code -Dsirix.pathNodeKeyColumn.disable=true}. */
  private static final boolean PATH_NODE_KEY_COLUMN_ENABLED =
      !Boolean.getBoolean("sirix.pathNodeKeyColumn.disable");

  /**
   * Lever 3: VALUE elision via PAX region lookup. When enabled and a fused
   * {@code OBJECT_NAMED_NUMBER} (kindId 49) slot's payload is decode-able as a
   * long (Integer or Long type — i.e. {@link KeyValueLeafPage#getFusedObjectNamedNumberValueLongFromSlot}
   * does NOT return {@link Long#MIN_VALUE}), the per-record payload bytes
   * ([type:1][delta-varint long]) are stripped from the staged blob and the
   * value is resolved at read time from the {@link NumberRegion} payload via
   * {@code (tag, slotRank)} lookup, then re-encoded inline into the in-memory
   * heap. The PAX region copy is load-bearing for SIMD scans and is kept; we
   * only drop the heap copy.
   *
   * <p>Activation is per-page: the writer activates elision only when EVERY
   * fused-NUMBER slot on the page has a long-decodable value AND the page-wide
   * net savings are positive (2 bytes type+width overhead per elided slot).
   *
   * <p>The on-disk per-elided-slot metadata is 2 bytes:
   * <ul>
   *   <li>1 byte type ({@code NUMBER_TYPE_INTEGER}=2 or {@code NUMBER_TYPE_LONG}=3)</li>
   *   <li>1 byte original heap width ({@code 1 + varint bytes})</li>
   * </ul>
   * Storing the width avoids the need to parse the {@link NumberRegion} BEFORE
   * the slottedPage is allocated (chicken-and-egg: tag lookup needs decoded
   * fields which need the heap which needs to be sized which needs the value
   * width...). The reader uses width to size the heap; type+region-lookup are
   * used at injection time after the heap is in place and the offset table is
   * expanded.
   *
   * <p>Default ON; gate off with {@code -Dsirix.valueElision.regionLookup.disable=true}
   * for A/B size measurement. The activation heuristic per-page already
   * guarantees the column is only emitted when total saved bytes strictly
   * exceed the (2 bytes/slot + 4-byte length prefix) overhead.
   */
  private static final boolean VALUE_ELISION_ENABLED =
      !Boolean.getBoolean("sirix.valueElision.regionLookup.disable");

  // ============================================================
  // Lever 4: NAME-KEY elision (writer + reader, fused OBJECT_NAMED_*)
  // ============================================================

  /**
   * Lever 4: nameKey elision via {@link ObjectKeyNameKeyRegion} lookup. When
   * enabled and the page emits at least one fused {@code OBJECT_NAMED_*}
   * (kindIds 48-51) record, the per-record inline {@code [signed-varint
   * nameKey]} field is stripped from the on-disk staged blob. The reader
   * recovers the field's int value at read-time via {@link
   * ObjectKeyNameKeyRegion#nameKeyForSlot(byte[], int)} — a direct slot-indexed
   * lookup against the same region payload that powers SIMD nameKey scans
   * (load-bearing for query routing). The PAX region copy is therefore the
   * single source of truth; the heap copy is pure duplication and Lever 4
   * drops it.
   *
   * <p>Activation is per-page: the writer only activates elision when the
   * region is actually built ({@code okCount > 0}) AND the total nameKey
   * bytes stripped strictly exceed the per-elided-slot 1-byte width overhead
   * plus the 4-byte length-prefix int. The activation heuristic guarantees
   * a strict size win.
   *
   * <p>Default ON; gate off with {@code -Dsirix.nameKeyElision.disable=true}
   * for A/B size measurement.
   */
  private static final boolean NAME_KEY_ELISION_ENABLED =
      !Boolean.getBoolean("sirix.nameKeyElision.disable");

  /**
   * Per-thread per-slot nameKey elision flag. Bit 0 set = the slot's nameKey
   * varint is being stripped on this page. Cleared on entry to writer pre-scan.
   */
  private static final ThreadLocal<byte[]> SLOT_NAME_KEY_ELIDED_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread per-slot nameKey field offset (one short/slot). Holds the
   * in-data-region offset of the {@code [signed-varint nameKey]} field, derived
   * from offset-table[3] (primitives) or [5] (structurals). Zero for slots
   * that are not nameKey-elided participants.
   */
  private static final ThreadLocal<short[]> SLOT_NAME_KEY_OFF_SCRATCH =
      ThreadLocal.withInitial(() -> new short[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread per-slot nameKey field width (one byte/slot, 1..5 valid range).
   * Encodes {@code DeltaVarIntCodec.computeSignedEncodedWidth(nameKey)}. Zero
   * for slots that are not nameKey-elided participants.
   */
  private static final ThreadLocal<byte[]> SLOT_NAME_KEY_WIDTH_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread reader-side packed-widths buffer for the name-key elision
   * section. One byte per fused-OBJECT_NAMED_* slot in slot-ascending order;
   * length = elidedCount on the wire.
   */
  private static final ThreadLocal<byte[]> SLOT_NAME_KEY_WIDTH_PACKED_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread small int-dict for tracking distinct nameKeys during the
   * Lever-4 writer pre-scan. Sized 256 to match
   * {@link ObjectKeyNameKeyRegion#encode}'s 255-unique-cap; the dict is built
   * via linear-search insert (typical fused-OBJECT_NAMED_* page has 5-10
   * unique nameKeys, so the scan is short and branch-predictable).
   */
  private static final ThreadLocal<int[]> NAME_KEY_UNIQUE_DICT_SCRATCH =
      ThreadLocal.withInitial(() -> new int[256]);

  /**
   * Structural-flags byte bit positions for the compressed-blob header.
   * Kept as constants so writer + reader agree on bit layout.
   */
  private static final int STRUCT_FLAG_HASH_ELISION = 0x01;

  /** Flag bit: page contains a parentKey column (see {@link #PARENT_KEY_COLUMN_ENABLED}). */
  private static final int STRUCT_FLAG_PARENT_KEY_COLUMN = 0x02;

  /** Flag bit: page contains a pathNodeKey column (see {@link #PATH_NODE_KEY_COLUMN_ENABLED}). */
  private static final int STRUCT_FLAG_PATH_NODE_KEY_COLUMN = 0x04;

  /**
   * Flag bit: page has VALUE elision active for fused {@code OBJECT_NAMED_NUMBER}
   * slots (see {@link #VALUE_ELISION_ENABLED}). When set, the on-disk record
   * bodies for fused-NUMBER slots have their {@code [type:1][delta-varint]}
   * payload bytes stripped, with the type-byte stored separately in the blob
   * (1 byte per elided slot, in slot-ascending order). The reader resolves the
   * long value via {@link NumberRegion#decodeValueAt} using the per-tag rank.
   */
  private static final int STRUCT_FLAG_VALUE_ELISION = 0x08;

  /**
   * Flag bit: page has NAME-KEY elision active for fused {@code OBJECT_NAMED_*}
   * (kindIds 48-51) slots (see {@link #NAME_KEY_ELISION_ENABLED}). When set, the
   * on-disk record bodies for fused slots have their inline {@code [signed-varint
   * nameKey]} field stripped; the reader recovers the value via
   * {@link ObjectKeyNameKeyRegion#nameKeyForSlot(byte[], int)} keyed by slot.
   * Per-elided-slot disk overhead: 1 byte (original heap width). The 4-byte
   * length-prefix is shared across all elided slots in slot-ascending order.
   */
  private static final int STRUCT_FLAG_NAME_KEY_ELISION = 0x10;

  /**
   * Enable / disable the pick-smaller per-page codec choice between
   * {@link ZeroRunByteCodec} and {@link ByteRunCodec}. Default ON — the
   * reader always accepts both codec bytes, and the writer picks whichever
   * produces fewer bytes for the current page. Gate off with
   * {@code -Dsirix.byteRunCodec.disable=true} to force V1 zero-only RLE
   * for A/B comparison.
   */
  private static final boolean BYTE_RUN_CODEC_ENABLED =
      !Boolean.getBoolean("sirix.byteRunCodec.disable");

  /**
   * Enable / disable the {@link SirixLZ77Codec} (codec id 3) participation
   * in the pick-smallest codec selection. Default ON. Gate off with
   * {@code -Dsirix.lz77Codec.disable=true} to fall back to the RLE-only
   * codec choice for A/B comparison against the LZ4 baseline.
   *
   * <p>The LZ77 codec catches intra-page 4-byte+ back-references (e.g.
   * repeating record-header prefixes like {@code 0x01 <tid> 0x00 0x00}
   * across consecutive same-kind slots). It typically wins on heaps with
   * more than a few dozen slots.
   */
  private static final boolean LZ77_CODEC_ENABLED =
      !Boolean.getBoolean("sirix.lz77Codec.disable");

  /**
   * Probe cadence of the sticky-winner codec election
   * ({@code -Dsirix.codecBakeoff.probeInterval}, default 16): every Nth page per
   * serialization thread runs the full bake-off and re-elects the winner; the pages in
   * between encode with the elected codec only. {@code 1} probes every page — the
   * exhaustive pick-smallest behavior, required for byte-identical golden files (see
   * {@link #emitSmallestBody}).
   */
  private static final int STICKY_PROBE_INTERVAL =
      Integer.getInteger("sirix.codecBakeoff.probeInterval", 16);

  /**
   * Pages at the start of each serialization thread that always probe, so the first
   * election rests on real evidence rather than the zero-initialized default.
   */
  private static final int STICKY_WARMUP_PAGES = 8;

  /**
   * Sticky-winner election state per serialization thread:
   * {@code {winnerCodecId, warmupPagesSeen, pagesSinceProbe}}. The winner id matches the
   * wire codec byte (0 = {@link ZeroRunByteCodec}, 2 = {@link ByteRunCodec},
   * 3 = {@link SirixLZ77Codec}).
   */
  private static final ThreadLocal<int[]> STICKY_CODEC =
      ThreadLocal.withInitial(() -> new int[3]);

  /**
   * Reset the current thread's sticky-codec election so its next
   * {@link #STICKY_WARMUP_PAGES} page bodies run the full bake-off (exhaustive
   * pick-smallest). Golden-byte tests MUST call this before serializing: the election
   * makes stored bytes a function of per-thread serialization history (see
   * {@link #emitSmallestBody}), so a page serialized after unrelated work on the same
   * thread may be encoded with the elected codec instead of the smallest one — which is
   * exactly the run-to-run variance a golden comparison has to neutralize.
   */
  public static void resetStickyCodecElectionForCurrentThread() {
    final int[] sticky = STICKY_CODEC.get();
    sticky[0] = 0;
    sticky[1] = 0;
    sticky[2] = 0;
  }

  /** Per-thread scratch for {@link ByteRunCodec} output. */
  private static final ThreadLocal<byte[]> V1_HEAP_V2_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[128 * 1024]);

  /** Per-thread scratch for {@link SirixLZ77Codec} output. */
  private static final ThreadLocal<byte[]> V1_HEAP_V3_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[128 * 1024]);

  /**
   * Per-thread template-pool bytes scratch. Worst case: SLOT_COUNT templates
   * × (2 header bytes + 15 max field bytes) = ~17 KB.
   */
  private static final ThreadLocal<byte[]> TEMPLATE_POOL_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT * (2 + 16)]);

  /**
   * Per-thread per-slot templateId scratch (1 byte per slot).
   */
  private static final ThreadLocal<byte[]> SLOT_TEMPLATE_IDS_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread offsets array for parsed template pool (length templateCount+1).
   */
  private static final ThreadLocal<int[]> TEMPLATE_OFFSETS_SCRATCH =
      ThreadLocal.withInitial(() -> new int[OffsetTableTemplatePool.MAX_TEMPLATES + 1]);

  /**
   * Per-thread fastutil map for packed-key → templateId lookup during dedup.
   * Default return value is -1 (sentinel for "not present").
   */
  private static final ThreadLocal<it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap> TEMPLATE_MAP =
      ThreadLocal.withInitial(() -> {
        final var m = new it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap(256);
        m.defaultReturnValue(-1);
        return m;
      });

  /**
   * Per-thread staging buffer for the dedup-transformed record heap before
   * {@link ZeroRunByteCodec} compression. Sized for a typical 32-KiB page; grows
   * on demand for larger pages. Steady-state zero-alloc once the buffer has
   * reached peak size.
   */
  private static final ThreadLocal<byte[]> V1_HEAP_STAGING_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[64 * 1024]);

  /**
   * Per-thread output buffer for the RLE encoder. Sized for RLE's worst-case
   * expansion on a typical page (see {@link ZeroRunByteCodec#maxEncodedSize}).
   */
  private static final ThreadLocal<byte[]> V1_HEAP_RLE_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[128 * 1024]);

  /**
   * Encode {@code staging[0..totalBytes)} with the run-length/LZ77 body codecs and emit
   * {@code int compressedLen + 1 codec byte + payload} to {@code sink}. Both
   * KEYVALUELEAFPAGE body writers (template-dedup and inline) share this tail.
   *
   * <p><b>Sticky-winner election.</b> Encoding every page with all three codecs costs
   * roughly 3&times; the winner's encode time, while the winning codec is extremely
   * stable within a workload (a record heap keeps its byte-redundancy profile across
   * millions of consecutive pages). So only <i>probe</i> pages — the first
   * {@link #STICKY_WARMUP_PAGES} pages of a serialization thread, then every
   * {@link #STICKY_PROBE_INTERVAL}-th page — run the full bake-off and (re-)elect the
   * winner; the pages in between encode with the elected codec alone. The emitted codec
   * byte keeps the format self-describing, so readers never see the difference.
   *
   * <p><b>Determinism caveat.</b> With {@code probeInterval > 1} the codec picked for a
   * page depends on per-thread serialization history, so stored bytes are no longer a
   * pure function of page content (sizes can differ by a few bytes run to run; content
   * round-trips identically). Golden-file byte comparisons must pin
   * {@code -Dsirix.codecBakeoff.probeInterval=1}, which restores the exhaustive
   * pick-smallest behavior exactly.
   *
   * <p>Codec rationale: the two RLE codecs catch single-byte runs (zero-run and
   * constant-byte-run respectively); the LZ77 variant catches 4-byte+ back-references
   * within a 64&nbsp;KB window — the dominant remaining redundancy after structural
   * encoders have eliminated per-record offset-table bytes. On Chicago-like record heaps
   * LZ77 typically wins because record-header bytes repeat verbatim across slots.
   */
  private static void emitSmallestBody(final BytesOut<?> sink, final MemorySegment staging,
      final int totalBytes) {
    final int[] sticky = STICKY_CODEC.get();
    final boolean warmup = sticky[1] < STICKY_WARMUP_PAGES;
    if (warmup) {
      sticky[1]++;
    }
    final boolean probe = STICKY_PROBE_INTERVAL <= 1
        || warmup
        || sticky[2] >= STICKY_PROBE_INTERVAL - 1;
    if (!probe) {
      sticky[2]++;
      emitWithCodec(sticky[0], sink, staging, totalBytes);
      return;
    }
    sticky[2] = 0;

    final int maxV0 = ZeroRunByteCodec.maxEncodedSize(totalBytes);
    final int maxV2 = ByteRunCodec.maxEncodedSize(totalBytes);
    final int maxV3 = SirixLZ77Codec.maxEncodedSize(totalBytes);

    // V1 scratch (shared, largest-ever sized). Used for V0 (zero-run).
    final byte[] rleBuf = V1_HEAP_RLE_SCRATCH.get();
    final int maxRleSize = Math.max(maxV0, Math.max(maxV2, maxV3));
    if (rleBuf.length < maxRleSize) {
      V1_HEAP_RLE_SCRATCH.set(new byte[maxRleSize]);
    }
    final byte[] rle = V1_HEAP_RLE_SCRATCH.get();

    // Dedicated per-thread scratches for V2 and V3 so we can compare all three
    // without copy.
    byte[] v2Buf = V1_HEAP_V2_SCRATCH.get();
    if (v2Buf.length < maxV2) {
      v2Buf = new byte[Math.max(maxV2, v2Buf.length * 2)];
      V1_HEAP_V2_SCRATCH.set(v2Buf);
    }
    byte[] v3Buf = V1_HEAP_V3_SCRATCH.get();
    if (v3Buf.length < maxV3) {
      v3Buf = new byte[Math.max(maxV3, v3Buf.length * 2)];
      V1_HEAP_V3_SCRATCH.set(v3Buf);
    }

    final int v0Len = ZeroRunByteCodec.encode(staging, 0L, totalBytes, rle, 0);
    final int v2Len = BYTE_RUN_CODEC_ENABLED
        ? ByteRunCodec.encode(staging, 0L, totalBytes, v2Buf, 0)
        : Integer.MAX_VALUE;
    final int v3Len = LZ77_CODEC_ENABLED
        ? SirixLZ77Codec.encode(staging, 0L, totalBytes, v3Buf, 0)
        : Integer.MAX_VALUE;

    final int bestLen = Math.min(v0Len, Math.min(v2Len, v3Len));
    // Tie order mirrors the emission branches below (LZ77 > ByteRun > ZeroRun), so a
    // probe page emits exactly what the exhaustive pick would have. Disabled codecs
    // report Integer.MAX_VALUE and can never be elected.
    sticky[0] = bestLen == v3Len ? 3 : bestLen == v2Len ? 2 : 0;
    if (bestLen == v3Len) {
      sink.writeInt(v3Len);
      sink.writeByte((byte) 3); // codec: 3 = SirixLZ77Codec
      sink.write(v3Buf, 0, v3Len);
      if (PAGE_SECTION_DIAG) {
        PageSectionDiag.recordCodecLz77(v3Len);
      }
    } else if (bestLen == v2Len) {
      sink.writeInt(v2Len);
      sink.writeByte((byte) 2); // codec: 2 = ByteRunCodec
      sink.write(v2Buf, 0, v2Len);
      if (PAGE_SECTION_DIAG) {
        PageSectionDiag.recordCodecByteRun(v2Len);
      }
    } else {
      sink.writeInt(v0Len);
      sink.writeByte((byte) 0); // codec: 0 = ZeroRunByteCodec
      sink.write(rle, 0, v0Len);
      if (PAGE_SECTION_DIAG) {
        PageSectionDiag.recordCodecZeroRun(v0Len);
      }
    }
  }

  /**
   * Non-probe page of the sticky-winner election: encode with the elected codec only
   * and emit {@code int compressedLen + 1 codec byte + payload}.
   */
  private static void emitWithCodec(final int codec, final BytesOut<?> sink,
      final MemorySegment staging, final int totalBytes) {
    switch (codec) {
      case 3 -> {
        final int maxV3 = SirixLZ77Codec.maxEncodedSize(totalBytes);
        byte[] v3Buf = V1_HEAP_V3_SCRATCH.get();
        if (v3Buf.length < maxV3) {
          v3Buf = new byte[Math.max(maxV3, v3Buf.length * 2)];
          V1_HEAP_V3_SCRATCH.set(v3Buf);
        }
        final int v3Len = SirixLZ77Codec.encode(staging, 0L, totalBytes, v3Buf, 0);
        sink.writeInt(v3Len);
        sink.writeByte((byte) 3); // codec: 3 = SirixLZ77Codec
        sink.write(v3Buf, 0, v3Len);
        if (PAGE_SECTION_DIAG) {
          PageSectionDiag.recordCodecLz77(v3Len);
        }
      }
      case 2 -> {
        final int maxV2 = ByteRunCodec.maxEncodedSize(totalBytes);
        byte[] v2Buf = V1_HEAP_V2_SCRATCH.get();
        if (v2Buf.length < maxV2) {
          v2Buf = new byte[Math.max(maxV2, v2Buf.length * 2)];
          V1_HEAP_V2_SCRATCH.set(v2Buf);
        }
        final int v2Len = ByteRunCodec.encode(staging, 0L, totalBytes, v2Buf, 0);
        sink.writeInt(v2Len);
        sink.writeByte((byte) 2); // codec: 2 = ByteRunCodec
        sink.write(v2Buf, 0, v2Len);
        if (PAGE_SECTION_DIAG) {
          PageSectionDiag.recordCodecByteRun(v2Len);
        }
      }
      default -> {
        final int maxV0 = ZeroRunByteCodec.maxEncodedSize(totalBytes);
        byte[] rle = V1_HEAP_RLE_SCRATCH.get();
        if (rle.length < maxV0) {
          rle = new byte[Math.max(maxV0, rle.length * 2)];
          V1_HEAP_RLE_SCRATCH.set(rle);
        }
        final int v0Len = ZeroRunByteCodec.encode(staging, 0L, totalBytes, rle, 0);
        sink.writeInt(v0Len);
        sink.writeByte((byte) 0); // codec: 0 = ZeroRunByteCodec
        sink.write(rle, 0, v0Len);
        if (PAGE_SECTION_DIAG) {
          PageSectionDiag.recordCodecZeroRun(v0Len);
        }
      }
    }
  }

  /**
   * Per-thread LZ4 compressor reused for the page heap compression. HIGH_COMPRESSION
   * mode: under {@code -Dsirix.compression=none} the outer byte-pipeline LZ4 is
   * disabled, so the only compressor operating on the KVL heap is this inner
   * one — we need its ratio to be as close as possible to the double-LZ4 path's
   * effective compression. LZ4_HC is 5-15× slower to encode than FAST but matches
   * zlib ratio on typical DB content; shred throughput has been measured as
   * still adequate for the baseline bench. Toggle via
   * {@code -Dsirix.heapLz4.mode=fast} to restore the old FAST behaviour.
   *
   * <p>Lazily created per thread to avoid static-init ordering issues with the
   * FFI linker.
   */
  private static final ThreadLocal<FFILz4Compressor> V1_HEAP_LZ4 =
      ThreadLocal.withInitial(() -> FFILz4Compressor.isNativeAvailable()
          ? new FFILz4Compressor(resolveHeapLz4Mode())
          : null);

  /**
   * LZ4HC compression level (1-12; 9 is the liblz4 default, 12 is the max).
   * Configurable via {@code -Dsirix.heapLz4.hcLevel=<N>}. Higher is slower but
   * squeezes a few more bytes out; 12 is 2-3× slower than 9 for roughly 1-2%
   * better ratio on typical Sirix heap content.
   */
  private static final int HEAP_LZ4_HC_LEVEL =
      Math.min(12, Math.max(1, Integer.getInteger("sirix.heapLz4.hcLevel", 9)));

  /**
   * Pure-structural encoder mode. Default is {@code true} — the heap body is
   * compressed using only the structural {@link ZeroRunByteCodec} fallback
   * and the schema-aware PAX / template-pool / hash-elision encoders. LZ4 is
   * disabled on the write path entirely so the whole compression budget has
   * to come from the structural stack. Read path still accepts LZ4-encoded
   * pages (codec byte == 1) for backwards compatibility with any databases
   * produced during the prototyping phase.
   *
   * <p>Set {@code -Dsirix.heapLz4.enable=true} to re-enable LZ4 HC (costs
   * ~9 s of 36 s cold-100M CPU; used only for A/B-measuring the structural
   * stack against the LZ4 baseline).
   */
  /**
   * Inner heap-LZ4 participates in the per-page pick-smallest codec choice.
   * Default OFF — measurement on the 100M bench showed heap-LZ4 regressed
   * storage by ~400 MB when combined with the white-box columns
   * (hash elision + parentKey column + pathNodeKey column), because LZ4's
   * pattern matcher competes with the column encoders for the same bytes and
   * its output framing dominates over the already-tight column encoding. The
   * pure-white-box stack (LZ77 + ByteRun + ZeroRun codecs on staging blob,
   * column extractors for hash/parent/pathNodeKey) beats heap-LZ4 on this
   * workload.
   *
   * <p>Gate on via {@code -Dsirix.heapLz4.enable=true} for workloads with
   * heterogeneous page shapes where the column extractors don't pay off.
   */
  private static final boolean HEAP_LZ4_DISABLED =
      !Boolean.getBoolean("sirix.heapLz4.enable");

  /**
   * When {@code -Dsirix.pageSectionDiag=true} is set, the page serializer
   * records byte counts per section (header+bitmap, encoded body, region
   * table, overlong, FSST) via {@link PageSectionDiag}. Emits a cumulative
   * breakdown on JVM shutdown. Pure diagnostic; off by default.
   */
  private static final boolean PAGE_SECTION_DIAG =
      Boolean.getBoolean("sirix.pageSectionDiag");

  private static FFILz4Compressor.CompressionMode resolveHeapLz4Mode() {
    final String prop = System.getProperty("sirix.heapLz4.mode", "hc").toLowerCase();
    return switch (prop) {
      case "fast" -> FFILz4Compressor.CompressionMode.FAST;
      case "hc", "high", "high_compression", "highcompression" ->
          FFILz4Compressor.CompressionMode.HIGH_COMPRESSION;
      default -> throw new IllegalArgumentException(
          "Unknown sirix.heapLz4.mode='" + prop + "' (expected: fast, hc)");
    };
  }

  /**
   * Per-thread compressed-output scratch for V1 heap LZ4. Sized for the LZ4
   * worst case (slightly larger than input).
   */
  private static final ThreadLocal<MemorySegment> V1_HEAP_LZ4_OUT =
      ThreadLocal.withInitial(() -> {
        // Start at 128 KiB; {@link #lz4OutScratch} grows it as needed.
        return java.lang.foreign.Arena.ofAuto().allocate(128 * 1024);
      });

  /**
   * Per-thread staging MemorySegment (native-backed) for the pre-LZ4 heap.
   * Native-backed so LZ4's FFI call works without JNI round-trips.
   */
  private static final ThreadLocal<MemorySegment> V1_HEAP_LZ4_STAGING =
      ThreadLocal.withInitial(() -> java.lang.foreign.Arena.ofAuto().allocate(128 * 1024));

  /**
   * Per-thread pointer to the decompressed body blob and its heap-section offset.
   * Bridges the compactDir/templatePool parse pass and the per-record expansion
   * pass so we don't re-decompress or walk the blob twice.
   */
  private static final ThreadLocal<MemorySegment> BLOB_STAGING_HOLDER = new ThreadLocal<>();
  private static final ThreadLocal<Long> BLOB_HEAP_OFFSET_HOLDER =
      ThreadLocal.withInitial(() -> 0L);

  /**
   * Grow (or re-allocate) the per-thread V1 staging segment to at least
   * {@code needed} bytes. Uses {@link java.lang.foreign.Arena#ofAuto} so
   * the previous segment's memory is GC'd when no longer referenced.
   */
  private static MemorySegment v1StagingScratch(final int needed) {
    MemorySegment s = V1_HEAP_LZ4_STAGING.get();
    if (s.byteSize() < needed) {
      s = java.lang.foreign.Arena.ofAuto().allocate(Math.max((long) needed, s.byteSize() * 2L));
      V1_HEAP_LZ4_STAGING.set(s);
    }
    return s;
  }

  /** Grow (or re-allocate) the per-thread LZ4 output segment. */
  private static MemorySegment v1Lz4OutScratch(final int needed) {
    MemorySegment s = V1_HEAP_LZ4_OUT.get();
    if (s.byteSize() < needed) {
      s = java.lang.foreign.Arena.ofAuto().allocate(Math.max((long) needed, s.byteSize() * 2L));
      V1_HEAP_LZ4_OUT.set(s);
    }
    return s;
  }

  static {
    for (final PageKind page : values()) {
      INSTANCEFORID.put(page.id, page);
      INSTANCEFORCLASS.put(page.clazz, page);
    }
  }

  /**
   * Unique ID.
   */
  private final byte id;

  /**
   * Class.
   */
  private final Class<? extends Page> clazz;

  /**
   * Constructor.
   *
   * @param id    unique identifier
   * @param clazz class
   */
  PageKind(final byte id, final Class<? extends Page> clazz) {
    this.id = id;
    this.clazz = clazz;
  }

  /**
   * Get the unique page ID.
   *
   * @return unique page ID
   */
  public byte getID() {
    return id;
  }

  /**
   * Compress the serialized page using the configured {@link ByteHandlerPipeline} and write the
   * compressed bytes back to the provided sink. Uses the MemorySegment path when available to
   * avoid intermediate byte[] allocations.
   */
  private static byte[] compress(ResourceConfiguration resourceConfig,
                                 BytesIn<?> uncompressedBytes,
                                 byte[] uncompressedArray,
                                 long uncompressedLength) {
    final ByteHandlerPipeline pipeline = resourceConfig.byteHandlePipeline;

    if (pipeline.supportsMemorySegments() && uncompressedBytes instanceof MemorySegmentBytesIn segmentIn) {
      MemorySegment uncompressedSegment = segmentIn.getSource().asSlice(0, uncompressedLength);
      MemorySegment compressedSegment = pipeline.compress(uncompressedSegment);
      return segmentToByteArray(compressedSegment);
    }

    final byte[] compressedBytes = compressViaStream(pipeline, uncompressedArray);
    return compressedBytes;
  }

  private static byte[] compressViaStream(ByteHandlerPipeline pipeline, byte[] uncompressedArray) {
    try (final ByteArrayOutputStream output = new ByteArrayOutputStream(uncompressedArray.length);
         final DataOutputStream dataOutput = new DataOutputStream(pipeline.serialize(output))) {
      dataOutput.write(uncompressedArray);
      dataOutput.flush();
      return output.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static byte[] segmentToByteArray(MemorySegment segment) {
    return segment.toArray(ValueLayout.JAVA_BYTE);
  }

  /**
   * Serialize page.
   *
   * @param ResourceConfiguration the read only page transaction
   * @param sink                  {@link BytesOut<?>} instance
   * @param page                  {@link Page} implementation
   */
  public abstract void serializePage(final ResourceConfiguration ResourceConfiguration, final BytesOut<?> sink,
      final Page page, final SerializationType type);

  /**
   * Deserialize page.
   *
   * @param resourceConfiguration the resource configuration
   * @param source                {@link BytesIn} instance
   * @return page instance implementing the {@link Page} interface
   */
  public Page deserializePage(final ResourceConfiguration resourceConfiguration, final BytesIn<?> source,
      final SerializationType type) {
    return deserializePage(resourceConfiguration, source, type, null);
  }

  /**
   * Deserialize page with optional DecompressionResult for zero-copy support.
   * 
   * <p>When decompressionResult is provided, KeyValueLeafPages can take ownership
   * of the decompression buffer and use it directly as slotMemory.
   *
   * @param resourceConfiguration the resource configuration
   * @param source                {@link BytesIn} instance
   * @param type                  serialization type
   * @param decompressionResult   optional decompression result for zero-copy (may be null)
   * @return page instance implementing the {@link Page} interface
   */
  public abstract Page deserializePage(final ResourceConfiguration resourceConfiguration, final BytesIn<?> source,
      final SerializationType type, final ByteHandler.DecompressionResult decompressionResult);

  /**
   * Public method to get the related page based on the identifier.
   *
   * @param id the identifier for the page
   * @return the related page
   */
  public static PageKind getKind(final byte id) {
    final PageKind page = INSTANCEFORID.get(id);
    if (page == null) {
      throw new IllegalStateException("Unknown PageKind id: " + id + " (0x" + Integer.toHexString(id & 0xFF) + ")");
    }
    return page;
  }

  /**
   * Public method to get the related page based on the class.
   *
   * @param clazz the class for the page
   * @return the related page
   */
  public static PageKind getKind(final Class<? extends Page> clazz) {
    final PageKind page = INSTANCEFORCLASS.get(clazz);
    if (page == null) {
      throw new IllegalStateException();
    }
    return page;
  }
}
