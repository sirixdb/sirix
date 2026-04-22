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
import io.sirix.page.pax.NumberRegion;
import io.sirix.page.pax.ObjectKeyNameKeyRegion;
import io.sirix.page.pax.RegionTable;
import io.sirix.page.pax.StringRegion;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
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
import java.time.Instant;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
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
      final BinaryEncodingVersion binaryVersion = BinaryEncodingVersion.fromByte(source.readByte());

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
          final int maxBlobBytes = compactDirBytes + templatePoolBytes + populatedCount
              + hashBitmapBytes + maxParentKeyColBytes + onDiskHeapSize;

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
            if (lz4 == null) {
              throw new SirixIOException("body encoded with LZ4 but FFI LZ4 not available");
            }
            final MemorySegment blobView = blobStaging.asSlice(0, maxBlobBytes);
            actualBlobBytes = lz4.decompressSegment(
                compressedIn.asSlice(0, compressedLen), blobView, compressedLen);
            if (actualBlobBytes < 0) {
              throw new SirixIOException("body LZ4 decompress returned " + actualBlobBytes);
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

          // Compute in-memory lengths:
          //   onDiskLen + (fc - 1) + (hashStripped ? 8 : 0) + parentKeyWidth(slot)
          // parentKeyWidth is derived from the template for slots whose kind
          // has a parentKey field; we compute it once per slot and stash it
          // for the record-expansion loop below.
          inMemDataLengths = SLOT_DATALEN_SCRATCH.get();
          int running = 0;
          for (int i = 0; i < populatedCount; i++) {
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
            inMemDataLengths[i] = inMemLen;
            // Stash pkWidth for the expansion loop via parentKeyWidths[i].
            if (parentKeyColumnActive && parentKeyWidths != null) {
              parentKeyWidths[i] = (byte) pkWidth;
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
      // Structural re-injection in two places:
      //   - parentKey column: re-encode delta-varint(parentKey, nodeKey) at
      //     data-region offset 0 (parentKey is always field 0).
      //   - hash elision: re-inject 8 zero bytes at the hash offset.
      // Both operations produce bytes whose widths were recorded pre-strip,
      // so the in-memory dataLength exactly matches inMemDataLengths[i].
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
            // where D' = D + pkWidth + (hashStripped ? 8 : 0).
            final int slotHeapOffset = PageLayout.getDirHeapOffset(slottedPage, slot);
            final long recordBase = PageLayout.HEAP_START + slotHeapOffset;
            // Copy kindId byte from staging heap section.
            slottedPage.set(ValueLayout.JAVA_BYTE, recordBase,
                stagingSeg.get(ValueLayout.JAVA_BYTE, heapBase + onDiskPos));
            // Expand offset table from the template pool.
            OffsetTableTemplatePool.expandTemplateTo(templatePool, templateOffsets,
                templateId, slottedPage, recordBase + 1);
            // Copy record data: skip the leading 2 bytes (kindId + templateId) in staging.
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
            // Re-inject parentKey as delta-varint when the column is active.
            final int pkWidth = (parentKeyColumnActive && parentKeyWidths != null)
                ? (parentKeyWidths[entryIdx2] & 0xFF) : 0;
            long writePos = recordBase + 1 + fc;
            if (pkWidth > 0) {
              final long nodeKey = pageKeyBase + slot;
              final long pk = parentKeyValues[entryIdx2];
              final int actualWidth = DeltaVarIntCodec.writeDeltaToSegment(slottedPage,
                  writePos, pk, nodeKey);
              if (actualWidth != pkWidth) {
                throw new SirixIOException(
                    "parentKey width mismatch at slot " + slot + ": expected=" + pkWidth
                        + " actual=" + actualWidth + " value=" + pk + " nodeKey=" + nodeKey);
              }
              writePos += pkWidth;
            }
            // Copy the remaining bytes, splitting around the hash field when
            // hash elision is active.
            if (hashStripped) {
              // Data region layout (in-memory) is:
              //   [pkBytes: pkWidth] (just written) | [pre-hash: hashOffInData - pkWidth]
              //   | [hash: 8 zero bytes] | [post-hash: dataBytes - (hashOffInData - pkWidth)]
              //
              // On-disk remaining bytes start at onDiskPos + 2 + 0 (pk stripped)
              // and sum to dataBytes. They split at (hashOffInData - pkWidth).
              final int preHashInOnDisk = hashOffInData - pkWidth;
              if (preHashInOnDisk > 0) {
                MemorySegment.copy(stagingSeg, heapBase + onDiskPos + 2, slottedPage,
                    writePos, preHashInOnDisk);
                writePos += preHashInOnDisk;
              } else if (preHashInOnDisk < 0) {
                throw new SirixIOException(
                    "hashOff=" + hashOffInData + " < pkWidth=" + pkWidth + " slot=" + slot);
              }
              // Eight zero bytes at hash position.
              slottedPage.set(ValueLayout.JAVA_LONG_UNALIGNED, writePos, 0L);
              writePos += NodeFieldLayout.HASH_WIDTH;
              final int tailOnDisk = dataBytes - preHashInOnDisk;
              if (tailOnDisk > 0) {
                MemorySegment.copy(stagingSeg, heapBase + onDiskPos + 2 + preHashInOnDisk,
                    slottedPage, writePos, tailOnDisk);
                writePos += tailOnDisk;
              }
            } else if (dataBytes > 0) {
              // Only parentKey was stripped (or neither). Copy bytes after pk.
              MemorySegment.copy(stagingSeg, heapBase + onDiskPos + 2, slottedPage,
                  writePos, dataBytes);
              writePos += dataBytes;
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
      sink.writeByte(BinaryEncodingVersion.V0.byteVersion());

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
      // (OBJECT_NUMBER_VALUE slot values + parent OBJECT_KEY nameKeys) so the
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

      // Compress the serialized data
      compressAndCache(resourceConfig, sink, keyValueLeafPage);

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
     * <p>Wire layout:
     * <pre>
     *   int[populatedCount] compactDir            // dataLength = ON-DISK length
     *   int                 onDiskHeapSize        // == Σ on-disk lengths
     *   byte                templateCount         // 0 = dedup disabled (fall back)
     *   if templateCount &gt; 0:
     *     byte              structuralFlags       // bit 0 = hash elision, bit 1 = parentKey column
     *     int               templatePoolBytes
     *     byte[templatePoolBytes] templatePool
     *     byte[populatedCount] slotTemplateIds
     *     if hashElision:
     *       byte[ceil(N/8)] zeroHashBitmap        // bit i = slot i's hash was stripped
     *     if parentKeyColumn:
     *       int             parentKeyColumnLen
     *       byte[parentKeyColumnLen] parentKeyColumn  // StructuralKeyColumnCodec
     *     int               compressedLen
     *     byte              codec                 // 0 = ZeroRunByteCodec, 1 = LZ4, 2 = ByteRunCodec, 3 = SirixLZ77Codec
     *     byte[compressedLen] compressedHeap
     *   if templateCount == 0:
     *     byte[onDiskHeapSize] heapBytes          // inline, uncompressed
     * </pre>
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
          final int hashBitmapBytes = HASH_ELISION_ENABLED
              ? ((populatedCount + 7) >>> 3) : 0;
          int zeroHashCount = 0;
          int parentKeySlotsWithField = 0;
          if (HASH_ELISION_ENABLED) {
            // Clear bitmap header bytes — reusing thread-local scratch across
            // pages means stale bits from a larger prior page may otherwise
            // corrupt this page's bitmap.
            for (int b = 0; b < hashBitmapBytes; b++) {
              zeroHashBitmap[b] = 0;
            }
          }
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
                final long h = slottedPage.get(ValueLayout.JAVA_LONG_UNALIGNED, hashAbsOff);
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

          // Compute on-disk heap size: for each record, replace its FIELD_COUNT bytes of
          // offset table with a single templateId byte. In-memory = 1 (kindId) + FC + D;
          // on-disk = 1 (kindId) + 1 (templateId) + D = in-memory - (FC - 1).
          // When hash elision is active, also strip 8 bytes per zero-hash slot.
          // When parentKey column is active, strip the slot's parentKeyWidth bytes.
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
          // Structural flags: bit 0 = hash elision, bit 1 = parentKey column.
          int structuralFlags = 0;
          if (hashElisionActive) structuralFlags |= STRUCT_FLAG_HASH_ELISION;
          if (parentKeyColumnActive) structuralFlags |= STRUCT_FLAG_PARENT_KEY_COLUMN;
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
          //   heap bytes (onDiskHeapSize, possibly reduced per slot)
          //
          // HFT-grade: staging buffer is thread-local and grows in powers of two
          // until steady-state; after warm-up every shred is allocation-free.
          final int compactDirBytes = 4 * populatedCount;
          final int stagedHashBitmapBytes = hashElisionActive ? hashBitmapBytes : 0;
          final int stagedParentKeyColBytes = parentKeyColumnActive
              ? (4 + parentKeyColumnLen) : 0;
          final int structuralBytes = compactDirBytes + br.templatesByteLength + populatedCount
              + stagedHashBitmapBytes + stagedParentKeyColBytes;
          final int totalStagingBytes = structuralBytes + onDiskHeapSize;
          final MemorySegment staging = v1StagingScratch(totalStagingBytes);
          long stagePos = 0;

          // compactDir — on-disk lengths, accounting for stripped hash + parentKey.
          for (int i = 0; i < populatedCount; i++) {
            final int fc = NodeFieldLayout.fieldCountForKind(slotKindIds[i]);
            int onDiskLen = slotDataLens[i] - (fc - 1);
            if (hashElisionActive && ((zeroHashBitmap[i >>> 3] >>> (i & 7)) & 1) == 1) {
              onDiskLen -= NodeFieldLayout.HASH_WIDTH;
            }
            if (parentKeyColumnActive) {
              onDiskLen -= slotParentKeyWidths[i] & 0xFF;
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
          // heap (records with templateId replacing offset table, hash optionally stripped)
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
            // parentKey is always at data offset 0 when present, so stripping
            // it just advances the "from" cursor past parentKeyWidth bytes.
            int readCursor = stripPkWidth;
            if (stripHash) {
              // Copy [readCursor, hashOff) then [hashOff + 8, dataBytes).
              final int hashOff = slotHashOffs[i] & 0xFFFF;
              final int preLen = hashOff - readCursor;
              if (preLen > 0) {
                MemorySegment.copy(slottedPage, recordBase + 1 + fc + readCursor,
                    staging, stagePos, preLen);
                stagePos += preLen;
              }
              final int tailStart = hashOff + NodeFieldLayout.HASH_WIDTH;
              final int tailBytes = dataBytes - tailStart;
              if (tailBytes > 0) {
                MemorySegment.copy(slottedPage, recordBase + 1 + fc + tailStart,
                    staging, stagePos, tailBytes);
                stagePos += tailBytes;
              }
            } else {
              final int len = dataBytes - readCursor;
              if (len > 0) {
                MemorySegment.copy(slottedPage, recordBase + 1 + fc + readCursor,
                    staging, stagePos, len);
                stagePos += len;
              }
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
            // Three-way codec bake-off: ZeroRunByteCodec (0),
            // ByteRunCodec (2), SirixLZ77Codec (3). Each codec
            // encodes the staging blob into its own scratch buffer;
            // we emit the smallest. The codec byte in the output header
            // tells the reader which decoder to dispatch to.
            //
            // Rationale: the two RLE codecs catch single-byte runs
            // (zero-run and constant-byte-run respectively). The LZ77
            // variant catches 4-byte+ back-references within a 64 KB
            // window — the dominant remaining redundancy after
            // structural encoders have eliminated per-record offset-table
            // bytes. On Chicago-like record heaps LZ77 typically wins
            // because record-header bytes repeat verbatim across slots.
            final int maxV0 = ZeroRunByteCodec.maxEncodedSize(totalStagingBytes);
            final int maxV2 = ByteRunCodec.maxEncodedSize(totalStagingBytes);
            final int maxV3 = SirixLZ77Codec.maxEncodedSize(totalStagingBytes);

            // V1 scratch (shared, largest-ever sized). Used for V0 (zero-run).
            final byte[] rleBuf = V1_HEAP_RLE_SCRATCH.get();
            final int maxRleSize = Math.max(maxV0, Math.max(maxV2, maxV3));
            if (rleBuf.length < maxRleSize) {
              V1_HEAP_RLE_SCRATCH.set(new byte[maxRleSize]);
            }
            final byte[] rle = V1_HEAP_RLE_SCRATCH.get();

            // Dedicated per-thread scratches for V2 and V3 so we can
            // compare all three without copy.
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

            final int v0Len = ZeroRunByteCodec.encode(staging, 0L, totalStagingBytes, rle, 0);
            final int v2Len = BYTE_RUN_CODEC_ENABLED
                ? ByteRunCodec.encode(staging, 0L, totalStagingBytes, v2Buf, 0)
                : Integer.MAX_VALUE;
            final int v3Len = LZ77_CODEC_ENABLED
                ? SirixLZ77Codec.encode(staging, 0L, totalStagingBytes, v3Buf, 0)
                : Integer.MAX_VALUE;

            final int bestLen = Math.min(v0Len, Math.min(v2Len, v3Len));
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

      // Three-way codec bake-off — same as the dedup path.
      final int maxV0 = ZeroRunByteCodec.maxEncodedSize(totalBlobBytes);
      final int maxV2 = ByteRunCodec.maxEncodedSize(totalBlobBytes);
      final int maxV3 = SirixLZ77Codec.maxEncodedSize(totalBlobBytes);
      final byte[] rleBuf = V1_HEAP_RLE_SCRATCH.get();
      final int maxRleSize = Math.max(maxV0, Math.max(maxV2, maxV3));
      if (rleBuf.length < maxRleSize) {
        V1_HEAP_RLE_SCRATCH.set(new byte[maxRleSize]);
      }
      final byte[] rle = V1_HEAP_RLE_SCRATCH.get();

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

      final int v0Len = ZeroRunByteCodec.encode(staging, 0L, totalBlobBytes, rle, 0);
      final int v2Len = BYTE_RUN_CODEC_ENABLED
          ? ByteRunCodec.encode(staging, 0L, totalBlobBytes, v2Buf, 0)
          : Integer.MAX_VALUE;
      final int v3Len = LZ77_CODEC_ENABLED
          ? SirixLZ77Codec.encode(staging, 0L, totalBlobBytes, v3Buf, 0)
          : Integer.MAX_VALUE;

      final int bestLen = Math.min(v0Len, Math.min(v2Len, v3Len));
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
     * Build the PAX {@link RegionTable} for {@code page} by walking the
     * populated-slot bitmap once, collecting each OBJECT_NUMBER_VALUE slot's
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
      final int numberKindId = KeyValueLeafPage.objectNumberValueKindId();
      final int stringKindId = KeyValueLeafPage.objectStringValueKindId();
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
      // keeping pages without OBJECT_STRING_VALUE slots at zero touch.
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

      for (int w = 0; w < PageLayout.BITMAP_WORDS; w++) {
        long word = PageLayout.getBitmapWord(slottedPage, w);
        final int baseSlot = w << 6;
        while (word != 0) {
          final int bit = Long.numberOfTrailingZeros(word);
          final int slot = baseSlot + bit;
          final int kindId = PageLayout.getDirNodeKindId(slottedPage, slot);
          if (KeyValueLeafPage.isObjectKeyKindId(kindId)) {
            okNameKeys[okCount] = page.getObjectKeyNameKeyFromSlot(slot);
            okSlots[okCount] = slot;
            okCount++;
          } else if (kindId == numberKindId) {
            final long value = page.getNumberValueLongFromSlot(slot);
            if (value != Long.MIN_VALUE) {
              final long valueNodeKey = pageKeyBase + slot;
              final long parentKey = page.getObjectNumberValueParentKeyFromSlot(slot, valueNodeKey);
              int parentNameKey = -1;
              int parentPathNodeKeyInt = -1;
              if ((parentKey >>> Constants.NDP_NODE_COUNT_EXPONENT) == page.getPageKey()) {
                final int parentSlot = (int) (parentKey & (PageLayout.SLOT_COUNT - 1));
                if (KeyValueLeafPage.isObjectKeyKindId(PageLayout.getDirNodeKindId(slottedPage, parentSlot))) {
                  parentNameKey = page.getObjectKeyNameKeyFromSlot(parentSlot);
                  if (numberAllPathNodeKeysValid) {
                    final long pnk = page.getObjectKeyPathNodeKeyFromSlot(parentSlot, parentKey);
                    if (pnk > 0L && pnk <= (long) Integer.MAX_VALUE) {
                      parentPathNodeKeyInt = (int) pnk;
                    } else {
                      numberAllPathNodeKeysValid = false;
                    }
                  }
                } else {
                  numberAllPathNodeKeysValid = false;
                }
              } else {
                numberAllPathNodeKeysValid = false;
              }
              valBuf[count] = value;
              parBuf[count] = parentNameKey;
              numberPathBuf[count] = parentPathNodeKeyInt;
              count++;
            }
          } else if (kindId == stringKindId) {
            final byte[] value = page.readObjectStringValueBytesForRegionBuildPkg(slot);
            if (value != null) {
              final long valueNodeKey = pageKeyBase + slot;
              final long parentKey = page.getObjectStringValueParentKeyFromSlot(slot, valueNodeKey);
              int parentNameKey = -1;
              int parentPathNodeKeyInt = -1;
              if ((parentKey >>> Constants.NDP_NODE_COUNT_EXPONENT) == page.getPageKey()) {
                final int parentSlot = (int) (parentKey & (PageLayout.SLOT_COUNT - 1));
                if (KeyValueLeafPage.isObjectKeyKindId(PageLayout.getDirNodeKindId(slottedPage, parentSlot))) {
                  parentNameKey = page.getObjectKeyNameKeyFromSlot(parentSlot);
                  if (stringAllPathNodeKeysValid) {
                    final long pnk = page.getObjectKeyPathNodeKeyFromSlot(parentSlot, parentKey);
                    if (pnk > 0L && pnk <= (long) Integer.MAX_VALUE) {
                      parentPathNodeKeyInt = (int) pnk;
                    } else {
                      stringAllPathNodeKeysValid = false;
                    }
                  }
                } else {
                  stringAllPathNodeKeysValid = false;
                }
              } else {
                stringAllPathNodeKeysValid = false;
              }
              if (stringEncName == null) {
                stringEncName = STRING_REGION_ENCODER.get();
                stringEncName.reset();
                if (withPathSummary) {
                  stringEncPath = STRING_REGION_ENCODER_PATH.get();
                  stringEncPath.reset();
                }
              }
              stringEncName.addValue(parentNameKey, value);
              if (stringEncPath != null && stringAllPathNodeKeysValid) {
                stringEncPath.addValue(parentPathNodeKeyInt, value);
              }
              stringCount++;
            }
          }
          word &= word - 1;
        }
      }

      if (count == 0 && okCount == 0 && stringCount == 0) {
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
      return table.isEmpty() ? null : table;
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
      final BinaryEncodingVersion binaryVersion = BinaryEncodingVersion.fromByte(source.readByte());

      switch (binaryVersion) {
        case V0 -> {
          Page delegate = PageUtils.createDelegate(source, type);

          final Int2LongMap maxNodeKeys = PageKind.deserializeMaxNodeKeys(source);
          final Int2LongMap maxHotPageKeys = PageKind.deserializeMaxNodeKeys(source);
          final int numberOfArrays = source.readInt();
          final Int2IntMap currentMaxLevelsOfIndirectPages =
              PageKind.deserializeCurrentMaxLevelsOfIndirectPages(source);

          return new NamePage(delegate, maxNodeKeys, maxHotPageKeys, currentMaxLevelsOfIndirectPages, numberOfArrays);
        }
        default -> throw new IllegalStateException();
      }
    }

    @Override
    public void serializePage(final ResourceConfiguration resourceConfig, final BytesOut<?> sink, final Page page,
        final SerializationType type) {
      NamePage namePage = (NamePage) page;
      sink.writeByte(NAMEPAGE.id);
      sink.writeByte(BinaryEncodingVersion.V0.byteVersion());
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
    }
  },

  /**
   * {@link UberPage}.
   */
  UBERPAGE((byte) 3, UberPage.class) {
    @Override
    public Page deserializePage(final ResourceConfiguration resourceConfig, final BytesIn<?> source,
        final SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      final BinaryEncodingVersion binaryVersion = BinaryEncodingVersion.fromByte(source.readByte());

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
      sink.writeByte(BinaryEncodingVersion.V0.byteVersion());
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
      final BinaryEncodingVersion binaryVersion = BinaryEncodingVersion.fromByte(source.readByte());

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
      sink.writeByte(BinaryEncodingVersion.V0.byteVersion());

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
      final BinaryEncodingVersion binaryVersion = BinaryEncodingVersion.fromByte(source.readByte());

      switch (binaryVersion) {
        case V0 -> {
          Page delegate = new BitmapReferencesPage(10, source, type);
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
      sink.writeByte(BinaryEncodingVersion.V0.byteVersion());

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
      final BinaryEncodingVersion binaryVersion = BinaryEncodingVersion.fromByte(source.readByte());

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
      sink.writeByte(BinaryEncodingVersion.V0.byteVersion());

      sink.writeByte((byte) 0);

      Page delegate = pathSummaryPage.delegate();
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
      final BinaryEncodingVersion binaryVersion = BinaryEncodingVersion.fromByte(source.readByte());

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
      sink.writeByte(BinaryEncodingVersion.V0.byteVersion());

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
      final BinaryEncodingVersion binaryVersion = BinaryEncodingVersion.fromByte(source.readByte());

      switch (binaryVersion) {
        case V0 -> {
          final byte[] data = new byte[source.readInt()];
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
      sink.writeByte(BinaryEncodingVersion.V0.byteVersion());
      
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
      final BinaryEncodingVersion binaryVersion = BinaryEncodingVersion.fromByte(source.readByte());
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
      sink.writeByte(BinaryEncodingVersion.V0.byteVersion());

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
      final BinaryEncodingVersion binaryVersion = BinaryEncodingVersion.fromByte(source.readByte());

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
      sink.writeByte(BinaryEncodingVersion.V0.byteVersion());

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
      final BinaryEncodingVersion binaryVersion = BinaryEncodingVersion.fromByte(source.readByte());

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

      final int entryCount = source.readInt();
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

      return new HOTLeafPage(recordPageKey, revision, indexType, slotMemory, releaser,
                             slotOffsets, entryCount, usedSlotMemorySize, commonPrefix, commonPrefixLen);
    }

    @Override
    public void serializePage(ResourceConfiguration resourceConfig, BytesOut<?> sink,
        Page page, SerializationType type) {
      HOTLeafPage hotLeaf = (HOTLeafPage) page;
      sink.writeByte(HOT_LEAF_PAGE.id);
      sink.writeByte(BinaryEncodingVersion.V0.byteVersion());

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

      sink.writeInt(hotLeaf.getEntryCount());
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
    }
  },

  /**
   * {@link HOTIndirectPage} - HOT trie interior node with compound structure.
   */
  HOT_INDIRECT_PAGE((byte) 13, HOTIndirectPage.class) {
    @Override
    public Page deserializePage(ResourceConfiguration resourceConfiguration, BytesIn<?> source,
        SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      final BinaryEncodingVersion binaryVersion = BinaryEncodingVersion.fromByte(source.readByte());
      
      // Read header
      final long pageKey = Utils.getVarLong(source);
      final int revision = source.readInt();
      final int height = source.readByte() & 0xFF;
      final byte nodeTypeId = source.readByte();
      final byte layoutTypeId = source.readByte();
      final int numChildren = source.readInt();
      
      final HOTIndirectPage.NodeType nodeType = HOTIndirectPage.NodeType.values()[nodeTypeId];
      final HOTIndirectPage.LayoutType layoutType = HOTIndirectPage.LayoutType.values()[layoutTypeId];

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

      // Read child references (simple key-only format)
      final PageReference[] children = new PageReference[numChildren];
      for (int i = 0; i < numChildren; i++) {
        PageReference ref = new PageReference();
        long childKey = source.readLong();
        ref.setKey(childKey);
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
      sink.writeByte(BinaryEncodingVersion.V0.byteVersion());
      
      // Write header
      Utils.putVarLong(sink, hotIndirect.getPageKey());
      sink.writeInt(hotIndirect.getRevision());
      sink.writeByte((byte) hotIndirect.getHeight());
      sink.writeByte((byte) hotIndirect.getNodeType().ordinal());
      sink.writeByte((byte) hotIndirect.getLayoutType().ordinal());
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

      // Write child references
      for (int i = 0; i < hotIndirect.getNumChildren(); i++) {
        final PageReference ref = hotIndirect.getChildReference(i);
        final long key = ref != null ? ref.getKey() : Constants.NULL_ID_LONG;
        sink.writeLong(key);
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
      // Skip binary version byte for now
      source.readByte();
      
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
      sink.writeByte(BinaryEncodingVersion.V0.byteVersion());
      
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
      final BinaryEncodingVersion binaryVersion = BinaryEncodingVersion.fromByte(source.readByte());

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
      sink.writeByte(BinaryEncodingVersion.V0.byteVersion());

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
      final BinaryEncodingVersion binaryVersion = BinaryEncodingVersion.fromByte(source.readByte());

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
      sink.writeByte(BinaryEncodingVersion.V0.byteVersion());

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
   * Enable / disable parent-key column extraction. When enabled, the page-
   * wide {@code parentKey} varints are factored into a
   * {@link StructuralKeyColumnCodec}-encoded column and stripped from the
   * in-heap record bytes. In DFS order, JSON shreds interleave kinds within
   * each top-level record (OBJECT → OBJECT_KEY_PAX → value → OBJECT_KEY_PAX
   * → value → ...), so the parent varies every slot — the column's
   * "same-as-predictor" bitmap degenerates and the encoded bytes are larger
   * than the raw varints (see {@code PageSectionDiag} "parentKeyColumn
   * candidates" counters: avgRaw/page ≈ 1.28 KB, avgEncoded/page ≈ 1.67 KB
   * on the 500K scale-bench workload). The code path is gated off by
   * default and the activation test further ensures we never emit a bigger
   * column than the raw data.
   *
   * <p>Re-enable via {@code -Dsirix.parentKeyColumn.enable=true} for
   * experiments on workloads with deeper, less-interleaved trees (e.g.
   * XML with long attribute-sibling chains).
   */
  private static final boolean PARENT_KEY_COLUMN_ENABLED =
      Boolean.getBoolean("sirix.parentKeyColumn.enable");

  /**
   * Structural-flags byte bit positions for the compressed-blob header.
   * Kept as constants so writer + reader agree on bit layout.
   */
  private static final int STRUCT_FLAG_HASH_ELISION = 0x01;

  /** Flag bit: page contains a parentKey column (see {@link #PARENT_KEY_COLUMN_ENABLED}). */
  private static final int STRUCT_FLAG_PARENT_KEY_COLUMN = 0x02;

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
