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
import io.sirix.io.HashAlgorithm;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.bytepipe.FFILz4Compressor;
import io.sirix.io.bytepipe.JavaLz4BlockDecoder;
import io.sirix.exception.SirixIOException;

import java.lang.foreign.Arena;
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
import io.sirix.page.pax.DoubleRegion;
import io.sirix.page.pax.NumberRegion;
import io.sirix.page.pax.NumberZoneMapRegion;
import io.sirix.page.pax.ObjectKeyNameKeyRegion;
import io.sirix.page.pax.PathNodeKeyRegion;
import io.sirix.page.pax.RecordOrdinalRegion;
import io.sirix.page.pax.RegionTable;
import io.sirix.page.pax.StringDictSketch;
import io.sirix.page.pax.GlobalStringDictionaries;
import io.sirix.page.pax.ResolvedGlobalStrings;
import io.sirix.page.pax.StringRegion;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import io.sirix.settings.RegionCompressionType;
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
import org.jspecify.annotations.Nullable;
import java.util.Arrays;
import java.util.BitSet;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.LongAdder;

/**
 * All Page types.
 */
@SuppressWarnings("SwitchStatementWithTooFewBranches")
public enum PageKind {
  /**
   * {@link KeyValueLeafPage}.
   */
  KEYVALUELEAFPAGE((byte) 1, KeyValueLeafPage.class) {

    /** Page-envelope flag: an overflow reference has a projection-visible logical slot sidecar. */
    private static final byte FLAG_OVERFLOW_SLOT_SIDECAR = 0x02;

    /** Every envelope bit this record-page reader understands. */
    private static final byte ALLOWED_FLAGS = ChunkedBodyConfig.FLAG_CHUNKED_BODY | FLAG_OVERFLOW_SLOT_SIDECAR;

    /**
     * Thread-local scratch for the compact directory read during slotted-page deserialization.
     * Allocating a fresh int[populatedCount] per page at ~1M pages per scan × 20 threads showed up as
     * 30% of all allocation samples (async-profiler alloc mode). Capacity is NDP_NODE_COUNT so it
     * covers the worst case and never needs to grow.
     */
    private final ThreadLocal<int[]> compactDirScratch =
        ThreadLocal.withInitial(() -> new int[Constants.NDP_NODE_COUNT]);

    /**
     * Thread-local 160-byte scratch for reading the on-disk header + bitmap section. Avoids a fresh new
     * byte[160] on every page deserialize; at 1M pages × 20 threads × N iters that was ~10% of byte[]
     * allocation samples.
     */
    private final ThreadLocal<byte[]> headerBitmapScratch =
        ThreadLocal.withInitial(() -> new byte[PageLayout.DISK_HEADER_BITMAP_SIZE]);

    /** One reusable maximum-size logical-slot image for the rare sidecar decode path. */
    private final ThreadLocal<byte[]> overflowSlotScratch =
        ThreadLocal.withInitial(() -> new byte[PageConstants.MAX_RECORD_SIZE]);

    @Override
    public Page deserializePage(final ResourceConfiguration resourceConfig, final BytesIn<?> source,
        final SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      // Envelope flag bit 0x01 is the body discriminator: set = chunk-framed body, clear = monolith.
      // Any other bit still fails the fence every page kind applies to unknown extensions.
      final byte flags = readVersionAndFlagsAllowing(source, ALLOWED_FLAGS);
      return deserializeSlottedPage(resourceConfig, source, (flags & ChunkedBodyConfig.FLAG_CHUNKED_BODY) != 0,
          (flags & FLAG_OVERFLOW_SLOT_SIDECAR) != 0, false);
    }

    @Override
    public Page deserializePageLazily(final ResourceConfiguration resourceConfig, final BytesIn<?> source,
        final SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      final byte flags = readVersionAndFlagsAllowing(source, ALLOWED_FLAGS);
      final boolean chunkedBody = (flags & ChunkedBodyConfig.FLAG_CHUNKED_BODY) != 0;
      return deserializeSlottedPage(resourceConfig, source, chunkedBody,
          (flags & FLAG_OVERFLOW_SLOT_SIDECAR) != 0, chunkedBody);
    }

    @Override
    public long probeRegionTableOffset(final BytesIn<?> source, final long[] out, final long @Nullable [] bitmapOut) {
      final byte flags = readVersionAndFlagsAllowing(source, ALLOWED_FLAGS);
      final boolean chunkedBody = (flags & ChunkedBodyConfig.FLAG_CHUNKED_BODY) != 0;
      out[3] = KeyValueLeafPage.NO_FSST_SYMBOL_TABLE_ID;
      out[4] = 0L;
      out[0] = Utils.getVarLong(source); // recordPageKey
      out[1] = source.readInt(); // revision
      IndexType.getType(source.readByte());
      // Read the header even when the caller does not need its bitmap: its positive completeness
      // certificate says whether bounded column reads may trust that no value lives in overflow.
      final byte[] headerBitmapBytes = headerBitmapScratch.get();
      source.read(headerBitmapBytes, 0, PageLayout.DISK_HEADER_BITMAP_SIZE);
      final MemorySegment headerBitmapSeg = MemorySegment.ofArray(headerBitmapBytes);
      out[4] = PageLayout.hasCompleteColumnCoverage(headerBitmapSeg)
          ? 1L
          : 0L;
      if (bitmapOut != null) {
        // Skipping this bitmap made every chunk-read fragment claim that it defined no slots and
        // silently disabled the versioned merge.
        for (int w = 0; w < PageLayout.BITMAP_WORDS; w++) {
          bitmapOut[w] = PageLayout.getBitmapWord(headerBitmapSeg, w);
        }
      }
      out[2] = source.readInt(); // populatedCount
      source.readInt(); // onDiskHeapSize
      final int templateCount = source.readByte() & 0xFF;
      if (templateCount > 0) {
        skipStructuralFlags(source);
      }
      source.readInt(); // templatePoolBytes
      if (chunkedBody) {
        // A chunked body says where the tail begins in one field, so the probe steps over META
        // frame, chunk table and every chunk payload without parsing any of them. The FSST
        // dictionary id is hoisted into the prefix precisely so this read can surface it: on a
        // monolith page it lives behind the region table, which is why a bounded read of an FSST
        // resource has to decline.
        out[3] = Utils.getVarLong(source); // fsstDictId, 0 = none
        final int bodyTotalLen = source.readInt();
        if (bodyTotalLen < 0) {
          throw new SirixIOException("implausible chunked body length " + bodyTotalLen);
        }
        return source.position() + bodyTotalLen;
      }
      final int compressedLen = source.readInt();
      source.readByte(); // body codec
      if (compressedLen < 0) {
        throw new SirixIOException("implausible body length " + compressedLen);
      }
      return source.position() + compressedLen;
    }

    @Override
    public RegionsOnlyPage deserializeRegionTableAt(final ResourceConfiguration resourceConfig, final BytesIn<?> source,
        final long pageKey, final int revision, final int populatedCount, final long fsstSymbolTableId,
        final int regionKindMask, final int regionDeferMask, final long @Nullable [] slotBitmap,
        final boolean hasCompleteColumnCoverage) {
      final RegionTable regionTable = RegionTable.readStoppingWhenSatisfied(source, regionKindMask, regionDeferMask);
      try {
        return new RegionsOnlyPage(pageKey, revision, populatedCount, fsstSymbolTableId, regionTable, slotBitmap,
            hasCompleteColumnCoverage);
      } catch (final RuntimeException | Error failure) {
        try {
          regionTable.close();
        } catch (final RuntimeException | Error cleanupFailure) {
          if (cleanupFailure != failure) {
            failure.addSuppressed(cleanupFailure);
          }
        }
        throw failure;
      }
    }

    @Override
    public RegionsOnlyPage deserializeRegionsOnlyPage(final ResourceConfiguration resourceConfig,
        final BytesIn<?> source, final int regionKindMask, final int regionDeferMask) {
      final byte flags = readVersionAndFlagsAllowing(source, ALLOWED_FLAGS);
      final boolean chunkedBody = (flags & ChunkedBodyConfig.FLAG_CHUNKED_BODY) != 0;
      final boolean hasOverflowSlotSidecar = (flags & FLAG_OVERFLOW_SLOT_SIDECAR) != 0;
      final long recordPageKey = Utils.getVarLong(source);
      final int revision = source.readInt();
      // indexType: read (not skipped) so the byte is consumed exactly like the full parse does.
      IndexType.getType(source.readByte());

      // The regions carry their own slot ids, so a single-fragment read needs nothing from here —
      // but a FRAGMENT's populated-slot bitmap decides which fragment owns a slot during versioned
      // reconstruction, and it costs one 160-byte read we are passing over anyway.
      final byte[] headerBitmapBytes = headerBitmapScratch.get();
      source.read(headerBitmapBytes);
      final MemorySegment headerBitmapSeg = MemorySegment.ofArray(headerBitmapBytes);
      final long[] slotBitmap = new long[PageLayout.BITMAP_WORDS];
      for (int w = 0; w < PageLayout.BITMAP_WORDS; w++) {
        slotBitmap[w] = PageLayout.getBitmapWord(headerBitmapSeg, w);
      }

      // Fixed prefix of the encoded body. Both body shapes — offset-table dedup and the
      // degenerate templateCount == 0 fallback — end in {compressedLen, codec, blob}, which is
      // all we need to step over the row heap without decompressing a single byte of it.
      final int populatedCount = source.readInt();
      source.readInt(); // onDiskHeapSize
      final int templateCount = source.readByte() & 0xFF;
      if (templateCount > 0) {
        skipStructuralFlags(source);
      }
      source.readInt(); // templatePoolBytes
      long fsstSymbolTableId = KeyValueLeafPage.NO_FSST_SYMBOL_TABLE_ID;
      final int bodyBytes;
      if (chunkedBody) {
        // Prefix-hoisted dictionary id (D7): a chunked page carries no tail marker, so this read is
        // the only place the id appears — and the one that makes a bounded read of an FSST resource
        // possible at all.
        fsstSymbolTableId = Utils.getVarLong(source);
        bodyBytes = source.readInt(); // bodyTotalLen: META frame + chunk table + every payload
      } else {
        bodyBytes = source.readInt(); // compressedLen
        source.readByte(); // body codec
      }
      if (bodyBytes < 0 || bodyBytes > source.remaining()) {
        throw new SirixIOException("implausible body length " + bodyBytes + " on page " + recordPageKey);
      }
      source.skip(bodyBytes);

      final long beforeRegions = source.position();
      final RegionTable regionTable = RegionTable.read(source, regionKindMask, regionDeferMask);
      try {
        // Tail: overlong-entry references, then — on a monolith page only — the FSST symbol-table
        // reference. A string predicate encodes its literal against that table so it can compare the
        // dictionary's STORED bytes; the overlong section has to be stepped over to reach it.
        final BitSet overlongEntries = SerializationType.deserializeBitSet(source);
        final int overlongEntrySize = source.readInt();
        if (overlongEntrySize < 0 || overlongEntries.cardinality() != overlongEntrySize) {
          throw new SirixIOException("overlong-entry bitmap/count mismatch on page " + recordPageKey + ": bitmap="
              + overlongEntries.cardinality() + ", count=" + overlongEntrySize);
        }
        for (int i = 0; i < overlongEntrySize; i++) {
          source.readLong();
        }
        final int sideSlotCount = hasOverflowSlotSidecar
            ? skipOverflowSlotSidecar(source, recordPageKey, overlongEntries, slotBitmap)
            : 0;
        if (!chunkedBody) {
          final int fsstMarker = source.readInt();
          if (fsstMarker == FSST_SYMBOL_TABLE_REFERENCE_MARKER) {
            fsstSymbolTableId = Utils.getVarLong(source);
          }
        }
        if (REGION_READ_DIAG) {
          // What share of a page a column-only read actually needs. The body is read off the disk
          // and thrown away unexamined, so this ratio is the headroom left in the I/O itself.
          REGION_BODY_BYTES_SKIPPED.add(bodyBytes);
          REGION_TABLE_BYTES_READ.add(source.position() - beforeRegions);
          REGION_PAGES_DECODED.increment();
        }
        return new RegionsOnlyPage(recordPageKey, revision, populatedCount + sideSlotCount, fsstSymbolTableId,
            regionTable, slotBitmap, overlongEntries.isEmpty() && overlongEntrySize == 0);
      } catch (final RuntimeException | Error failure) {
        try {
          regionTable.close();
        } catch (final RuntimeException | Error cleanupFailure) {
          if (cleanupFailure != failure) {
            failure.addSuppressed(cleanupFailure);
          }
        }
        throw failure;
      }
    }

    /** Validate the fixed header before it can index any page-sized scratch array. */
    private static void validatePopulatedCount(final long pageKey, final MemorySegment headerBitmap,
        final int populatedCount) {
      if (populatedCount < 0 || populatedCount > PageLayout.SLOT_COUNT) {
        throw new SirixIOException("page " + pageKey + " declares " + populatedCount
            + " populated slots; a record page has " + PageLayout.SLOT_COUNT);
      }
      int bitmapCount = 0;
      for (int word = 0; word < PageLayout.BITMAP_WORDS; word++) {
        bitmapCount += Long.bitCount(PageLayout.getBitmapWord(headerBitmap, word));
      }
      if (bitmapCount != populatedCount) {
        throw new SirixIOException("page " + pageKey + " populated-count/bitmap mismatch: header="
            + populatedCount + " bitmap=" + bitmapCount);
      }
    }

    /** Bound a wire heap by the per-entry inline ceiling before sizing retained native scratch. */
    private static void validateOnDiskHeapSize(final long pageKey, final int populatedCount,
        final int onDiskHeapSize) {
      final long maximum = (long) populatedCount * PageConstants.MAX_RECORD_SIZE;
      if (onDiskHeapSize < 0 || onDiskHeapSize > maximum) {
        throw new SirixIOException("page " + pageKey + " has invalid on-disk heap size " + onDiskHeapSize
            + " for " + populatedCount + " entries (maximum " + maximum + ")");
      }
    }

    /** Checked int sizing for page-local wire sections. */
    private static int checkedPageBodySize(final long pageKey, final String section, final long size) {
      if (size < 0 || size > Integer.MAX_VALUE) {
        throw new SirixIOException("page " + pageKey + " has invalid " + section + " size " + size);
      }
      return (int) size;
    }

    /** Reject a wire length before it can grow a retained scratch buffer or advance the source. */
    private static void validateEncodedLength(final long pageKey, final String section, final int encodedLength,
        final BytesIn<?> source) {
      if (encodedLength < 0 || encodedLength > source.remaining()) {
        throw new SirixIOException("page " + pageKey + " has invalid " + section + " length " + encodedLength
            + "; " + source.remaining() + " source bytes remain");
      }
    }

    /**
     * Prove that compact lengths describe exactly the heap and, for chunked pages, exactly each
     * chunk. This runs before the full page allocation and before any directory offset is installed.
     */
    private static void validateCompactDirectory(final long pageKey, final int[] compactDir,
        final int populatedCount, final int onDiskHeapSize, final boolean deduplicated,
        final boolean chunkedBody) {
      long heapBytes = 0;
      for (int entry = 0; entry < populatedCount; entry++) {
        final int length = PageLayout.unpackDataLength(compactDir[entry]);
        if (deduplicated && length < 2) {
          throw new SirixIOException("page " + pageKey + " deduplicated entry " + entry
              + " is shorter than kindId+templateId: " + length);
        }
        heapBytes += length;
      }
      if (heapBytes != onDiskHeapSize) {
        throw new SirixIOException("page " + pageKey + " compact directory covers " + heapBytes
            + " heap bytes, header declares " + onDiskHeapSize);
      }
      if (!chunkedBody) {
        return;
      }

      final ChunkTable table = READ_CHUNK_TABLE.get();
      int coveredEntries = 0;
      for (int chunk = 0; chunk < table.count; chunk++) {
        final int first = table.firstEntry[chunk];
        final int count = table.entryCount[chunk];
        if (first != coveredEntries || count <= 0 || first > populatedCount - count) {
          throw new SirixIOException("page " + pageKey + " has invalid compact-directory range for chunk "
              + chunk + ": first=" + first + " count=" + count);
        }
        long chunkBytes = 0;
        final int end = first + count;
        for (int entry = first; entry < end; entry++) {
          chunkBytes += PageLayout.unpackDataLength(compactDir[entry]);
        }
        if (chunkBytes != table.rawLen[chunk]) {
          throw new SirixIOException("page " + pageKey + " compact directory covers " + chunkBytes
              + " bytes in chunk " + chunk + ", table declares " + table.rawLen[chunk]);
        }
        coveredEntries = end;
      }
      if (coveredEntries != populatedCount) {
        throw new SirixIOException("page " + pageKey + " chunked compact directory covers " + coveredEntries
            + " entries, expected " + populatedCount);
      }
    }

    private Page deserializeSlottedPage(final ResourceConfiguration resourceConfig, final BytesIn<?> source,
        final boolean chunkedBody, final boolean hasOverflowSlotSidecar, final boolean lazyChunks) {
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
      validatePopulatedCount(recordPageKey, headerBitmapSeg, populatedCount);

      // 2. Read the prefix of uncompressed fixed-size headers:
      // - populatedCount (int) — matches the compactDir array length
      // - onDiskHeapSize (int) — needed to size the decompress output
      // - templateCount (byte) — > 0 when offset-table dedup is active
      // - templatePoolBytes (int) — size of the template pool inside the
      // compressed blob (0 when templateCount == 0)
      //
      // The remaining data (compactDir + templatePool + slotTemplateIds + heap)
      // is delivered as a single LZ4/ZeroRunByteCodec-compressed blob. This gives
      // the codec cross-section visibility (the old outer-page LZ4 was already
      // doing this for us; folding that pass into the inner encoder lets us
      // drop the outer pipeline entirely for `-Dsirix.compression=none` parity).
      final int populatedCountHeader = source.readInt();
      if (populatedCountHeader != populatedCount) {
        throw new SirixIOException(
            "populatedCount header mismatch: bitmap=" + populatedCount + " prefix=" + populatedCountHeader);
      }
      final int onDiskHeapSize = source.readInt();
      validateOnDiskHeapSize(recordPageKey, populatedCount, onDiskHeapSize);

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
      final long[] rightSibKeyValues;
      final byte[] rightSibKeyWidths;
      final boolean rightSibColumnActive;
      final long[] leftSibKeyValues;
      final byte[] leftSibKeyWidths;
      final boolean leftSibColumnActive;
      final byte[] pathNodeKeyColumnBytes; // raw PathNodeKeyRegion payload (bitmap-indexed)
      final int pathNodeKeyColumnLen; // valid bytes of the payload, which is read into a larger scratch
      final byte[] pathNodeKeyWidths; // per-slot varint width after reinject
      final boolean pathNodeKeyColumnActive;
      final boolean valueElisionActive;
      /** Whether the two elision sections carry {@link ElisionDeriver}'s derived form. */
      final boolean derivedElisionSections;
      // Not final: the derived form settles the count only once the regions have been read and the
      // per-entry metadata re-derived from them, which happens further below.
      int valueElidedCount;
      final short[] valueElidedSlots; // per-elided-entry slot id, ascending
      final byte[] valueElidedTypes; // per-elided-entry type byte
      final int[] valueElidedWidths; // per-elided-entry original heap width
      final int[] valueElidedAbsIdx; // per-elided-entry absolute region index
      final short[] valueOffs; // per-slot value offset (in-data offset for fused-NUMBER)
      final short[] valueWidths; // per-slot value width (post-inject width on heap)
      // Lever 4: nameKey-elision per-slot scratches.
      final boolean nameKeyElisionActive;
      final short[] nameKeyOffs; // per-slot nameKey field offset
      final byte[] nameKeyWidths; // per-slot nameKey width on the in-memory heap
      final int[] compactDir = compactDirScratch.get();
      // Hoisted out of the deduplicated-body branch: both are consumed after the regions have been
      // read, which happens once that branch has finished parsing the page's metadata sections.
      byte[] nameKeyElidedWidthsPacked = null;
      ElisionDeriver elisionDeriver = null;
      // One carrier for both passes over this page: the length derivation binds the page's sections
      // onto it, and the expansion pass below reads them back.
      final SlottedPageDecodeState decodeState = SLOTTED_PAGE_DECODE_STATE.get();
      int inMemHeapSize;
      // A chunked page carries its FSST dictionary id in the body prefix instead of the tail marker,
      // so the value is read here and consumed where the tail would otherwise have supplied it.
      long chunkedFsstDictId = KeyValueLeafPage.NO_FSST_SYMBOL_TABLE_ID;
      if (offsetTableDedup) {
        templateCount = source.readByte() & 0xFF;

        if (templateCount == 0) {
          // Degenerate / fallback: no dedup was possible. The writer staged
          // compactDir + heap into a single blob and compressed it; we now
          // decompress and parse the compactDir, leaving the heap bytes to be
          // materialized into the slotted-page allocation further below.
          templatePoolBytes = source.readInt();
          if (templatePoolBytes != 0) {
            throw new SirixIOException("page " + recordPageKey + " has no templates but declares a "
                + templatePoolBytes + "-byte template pool");
          }
          templatePool = null;
          templateOffsets = null;
          slotTemplateIds = null;
          inMemDataLengths = null;
          zeroHashBitmap = null;
          hashElisionActive = false;
          parentKeyValues = null;
          parentKeyWidths = null;
          parentKeyColumnActive = false;
          rightSibKeyValues = null;
          rightSibKeyWidths = null;
          rightSibColumnActive = false;
          leftSibKeyValues = null;
          leftSibKeyWidths = null;
          leftSibColumnActive = false;
          pathNodeKeyColumnBytes = null;
          pathNodeKeyColumnLen = 0;
          pathNodeKeyWidths = null;
          pathNodeKeyColumnActive = false;
          valueElisionActive = false;
          derivedElisionSections = false;
          valueElidedCount = 0;
          valueElidedSlots = null;
          valueElidedTypes = null;
          valueElidedWidths = null;
          valueElidedAbsIdx = null;
          valueOffs = null;
          valueWidths = null;
          nameKeyElisionActive = false;
          nameKeyOffs = null;
          nameKeyWidths = null;
          inMemHeapSize = onDiskHeapSize;

          final int compactDirBytes = PageLayout.COMPACT_DIR_ENTRY_SIZE * populatedCount;
          final int totalBlobBytes = checkedPageBodySize(recordPageKey, "inline decoded body",
              (long) compactDirBytes + onDiskHeapSize);
          final MemorySegment blobStaging;
          final int decodedBlobBytes;
          if (chunkedBody) {
            // Degenerate twin of the chunked body: META is the compact dir alone and the chunks
            // hold the verbatim inline records. One framing, two semantics — mirroring the
            // dedup/inline split the monolith body already has.
            chunkedFsstDictId = Utils.getVarLong(source);
            final int bodyTotalLen = source.readInt();
            blobStaging = readChunkedBody(source, recordPageKey, populatedCount, onDiskHeapSize, bodyTotalLen,
                compactDirBytes, compactDirBytes, lazyChunks);
            decodedBlobBytes = totalBlobBytes;
            final int metaRawLen = READ_CHUNK_TABLE.get().metaRawLen;
            if (metaRawLen != compactDirBytes) {
              throw new SirixIOException("page " + recordPageKey + " has a degenerate META section of " + metaRawLen
                  + " bytes, expected the " + compactDirBytes + "-byte compact dir");
            }
          } else {
            final int compressedLen = source.readInt();
            final byte codec = source.readByte();
            validateEncodedLength(recordPageKey, "inline encoded body", compressedLen, source);
            if (codec != 0 && codec != 2 && codec != 3) {
              throw new SirixIOException("unknown inline-body codec: " + codec);
            }

            blobStaging = v1StagingScratch(totalBlobBytes);
            if (codec == 0) {
              final byte[] rleBuf = V1_HEAP_RLE_SCRATCH.get();
              final byte[] rle = rleBuf.length >= compressedLen
                  ? rleBuf
                  : new byte[compressedLen];
              if (rle != rleBuf) {
                V1_HEAP_RLE_SCRATCH.set(rle);
              }
              source.read(rle, 0, compressedLen);
              decodedBlobBytes = ZeroRunByteCodec.decode(rle, 0, compressedLen, blobStaging, 0L);
            } else if (codec == 2) {
              final byte[] rleBuf = V1_HEAP_RLE_SCRATCH.get();
              final byte[] rle = rleBuf.length >= compressedLen
                  ? rleBuf
                  : new byte[compressedLen];
              if (rle != rleBuf) {
                V1_HEAP_RLE_SCRATCH.set(rle);
              }
              source.read(rle, 0, compressedLen);
              decodedBlobBytes = ByteRunCodec.decode(rle, 0, compressedLen, blobStaging, 0L);
            } else {
              final byte[] rleBuf = V1_HEAP_RLE_SCRATCH.get();
              final byte[] rle = rleBuf.length >= compressedLen
                  ? rleBuf
                  : new byte[compressedLen];
              if (rle != rleBuf) {
                V1_HEAP_RLE_SCRATCH.set(rle);
              }
              source.read(rle, 0, compressedLen);
              decodedBlobBytes = SirixLZ77Codec.decode(rle, 0, compressedLen, blobStaging, 0L);
            }
          }

          if (decodedBlobBytes != totalBlobBytes) {
            throw new SirixIOException("page " + recordPageKey + " inline body decoded to " + decodedBlobBytes
                + " bytes, expected " + totalBlobBytes);
          }

          // Parse compactDir from the first section of the blob.
          for (int i = 0; i < populatedCount; i++) {
            compactDir[i] =
                PageLayout.readCompactDirEntry(blobStaging, (long) i * PageLayout.COMPACT_DIR_ENTRY_SIZE);
          }
          validateCompactDirectory(recordPageKey, compactDir, populatedCount, onDiskHeapSize, false, chunkedBody);
          // Stash the staging blob and the heap offset (compactDirBytes) so
          // the record-expansion loop below consumes from the correct spot.
          BLOB_HEAP_OFFSET_HOLDER.set((long) compactDirBytes);
          BLOB_STAGING_HOLDER.set(blobStaging);
        } else {
          // Dedup path. Read the structural-flags byte + the compressed blob
          // header (length + codec) and decompress into the staging buffer.
          // The blob holds, in order:
          // compactDir bytes (2 × populatedCount)
          // templatePool bytes (templatePoolBytes)
          // slotTemplateIds (populatedCount bytes)
          // [if hashElisionActive] zeroHashBitmap (ceil(populatedCount/8) bytes)
          // [if parentKeyColumnActive] int columnLen + column bytes
          // heap bytes (onDiskHeapSize)
          final int structuralFlags = source.readByte() & 0xFF;
          // A second flags byte only exists when one of the levers that did not fit the first is on,
          // so a page using none of them is byte-identical to what a pre-change writer produced.
          final int extendedFlags = (structuralFlags & STRUCT_FLAG_EXTENDED) != 0
              ? source.readByte() & 0xFF
              : 0;
          if ((extendedFlags & ~(EXT_FLAG_RIGHT_SIB_COLUMN | EXT_FLAG_LEFT_SIB_COLUMN)) != 0) {
            throw new SirixIOException("page " + recordPageKey + " declares unknown extended structural flags 0x"
                + Integer.toHexString(extendedFlags));
          }
          if ((structuralFlags & STRUCT_FLAG_EXTENDED) != 0 && extendedFlags == 0) {
            throw new SirixIOException("page " + recordPageKey
                + " announces an extended structural-flags byte that turns nothing on");
          }
          rightSibColumnActive = (extendedFlags & EXT_FLAG_RIGHT_SIB_COLUMN) != 0;
          leftSibColumnActive = (extendedFlags & EXT_FLAG_LEFT_SIB_COLUMN) != 0;
          hashElisionActive = (structuralFlags & STRUCT_FLAG_HASH_ELISION) != 0;
          parentKeyColumnActive = (structuralFlags & STRUCT_FLAG_PARENT_KEY_COLUMN) != 0;
          pathNodeKeyColumnActive = (structuralFlags & STRUCT_FLAG_PATH_NODE_KEY_COLUMN) != 0;
          valueElisionActive = (structuralFlags & STRUCT_FLAG_VALUE_ELISION) != 0;
          nameKeyElisionActive = (structuralFlags & STRUCT_FLAG_NAME_KEY_ELISION) != 0;
          // Which form the two elision sections take is the page's own statement, not this process's
          // configuration: a resource written before the derived form, or with its kill switch on,
          // stays readable beside one written after.
          derivedElisionSections = (structuralFlags & STRUCT_FLAG_DERIVED_ELISION) != 0;
          if (derivedElisionSections && !valueElisionActive && !nameKeyElisionActive) {
            throw new SirixIOException("page " + recordPageKey
                + " declares derived elision sections but carries neither elision section");
          }
          if (derivedElisionSections) {
            elisionDeriver = READER_ELISION_DERIVER.get();
          }
          templatePoolBytes = source.readInt();
          if (templateCount > populatedCount) {
            throw new SirixIOException("page " + recordPageKey + " declares " + templateCount
                + " templates for " + populatedCount + " entries");
          }
          if (templatePoolBytes < 0 || templatePoolBytes > MAX_TEMPLATE_POOL_BYTES) {
            throw new SirixIOException("page " + recordPageKey + " has invalid template-pool size "
                + templatePoolBytes + " (maximum " + MAX_TEMPLATE_POOL_BYTES + ")");
          }

          final int compactDirBytes = PageLayout.COMPACT_DIR_ENTRY_SIZE * populatedCount;
          final int hashBitmapBytes = hashElisionActive
              ? ((populatedCount + 7) >>> 3)
              : 0;
          // Upper bounds for the optional sections. The monolith body needs them to size its
          // staging buffer before the real section lengths are known; both bodies then use them to
          // bound the length prefixes they parse out of the decoded sections.
          final int maxParentKeyColBytes = parentKeyColumnActive
              ? 4 + populatedCount * 11
              : 0;
          final int maxRightSibColBytes = rightSibColumnActive
              ? 4 + populatedCount * 11
              : 0;
          final int maxLeftSibColBytes = leftSibColumnActive
              ? 4 + populatedCount * 11
              : 0;
          // pathNodeKey column size upper bound: 4 (len) + 1 + 256*4 + 2 + 128 + slotCount.
          final int maxPathNodeKeyColBytes = pathNodeKeyColumnActive
              ? 4 + 1 + 256 * 4 + 2 + 128 + populatedCount
              : 0;
          // valueElision section size upper bound: for the per-slot form, 4 (len) + up to 7 bytes/elided
          // slot (gap varint <= 2, type, width varint <= 2, region absolute-index varint <= 2); for the
          // derived form, see ElisionDeriver.
          final int maxValueElisionBytes = !valueElisionActive
              ? 0
              : (derivedElisionSections
                  ? ElisionDeriver.maxValueSectionBytes(populatedCount)
                  : 4 + (populatedCount * 7));
          // nameKeyElision section size upper bound: 4 (len) + 1 byte/elided slot, or the derived form's.
          final int maxNameKeyElisionBytes = !nameKeyElisionActive
              ? 0
              : (derivedElisionSections
                  ? ElisionDeriver.maxNameKeySectionBytes(populatedCount)
                  : 4 + populatedCount);
          final int minimumMetadataBytes = checkedPageBodySize(recordPageKey, "minimum deduplicated META",
              (long) compactDirBytes + templatePoolBytes + populatedCount + hashBitmapBytes
                  + (parentKeyColumnActive ? Integer.BYTES : 0)
                  + (rightSibColumnActive ? Integer.BYTES : 0)
                  + (leftSibColumnActive ? Integer.BYTES : 0)
                  + (pathNodeKeyColumnActive ? Integer.BYTES : 0)
                  + (valueElisionActive ? (derivedElisionSections ? 1 : Integer.BYTES) : 0)
                  + (nameKeyElisionActive ? (derivedElisionSections ? 1 : Integer.BYTES) : 0));
          final int maximumMetadataBytes = checkedPageBodySize(recordPageKey, "maximum deduplicated META",
              (long) compactDirBytes + templatePoolBytes + populatedCount + hashBitmapBytes + maxParentKeyColBytes
                  + maxRightSibColBytes + maxLeftSibColBytes + maxPathNodeKeyColBytes + maxValueElisionBytes
                  + maxNameKeyElisionBytes);
          final MemorySegment blobStaging;
          final int metadataLength;
          if (chunkedBody) {
            // Chunked body: the META frame states its own decoded length, so the staging buffer is
            // sized exactly rather than by the worst-case dance the monolith arm does below. The
            // FSST dictionary id rides the prefix here instead of the tail marker (D7).
            chunkedFsstDictId = Utils.getVarLong(source);
            final int bodyTotalLen = source.readInt();
            blobStaging = readChunkedBody(source, recordPageKey, populatedCount, onDiskHeapSize, bodyTotalLen,
                minimumMetadataBytes, maximumMetadataBytes, lazyChunks);
            metadataLength = READ_CHUNK_TABLE.get().metaRawLen;
          } else {
            // parentKey column length is inside the blob, but we need to know
            // the blob size upfront to decompress. Read the column-length int
            // (after the fixed-size sections) by pre-reading the first 4 bytes
            // of the column chunk after decompression — we don't need a separate
            // header because the column length is stored inside the blob
            // immediately after zeroHashBitmap.
            final int compressedLen = source.readInt();
            final byte codec = source.readByte();
            validateEncodedLength(recordPageKey, "deduplicated encoded body", compressedLen, source);
            if (codec != 0 && codec != 1 && codec != 2 && codec != 3) {
              throw new SirixIOException("unknown body codec: " + codec);
            }

            // Two-phase decompress: we don't know the parentKey column bytes
            // until we've decoded enough of the blob to parse its 4-byte length
            // prefix. Since the blob is always decompressed in full, we size
            // the staging buffer pessimistically and trust totalBlobBytes to
            // match after the fact.
            //
            // Total blob bytes we'll verify:
            // structural = compactDir + templatePool + slotTemplateIds + hashBitmap + (4 + colLen)
            // blob = structural + onDiskHeapSize
            //
            // Because colLen is inside the compressed blob, we size upper bound
            // via uncompressedSize embedded in the codec's frame header.
            // ZeroRunByteCodec uses an explicit uncompressedSize varint, and
            // LZ4 gives us the exact uncompressed length via decompress's
            // return value. We therefore allocate using onDiskHeapSize +
            // maxStructural where maxStructural includes a worst-case
            // parentKey column of populatedCount × 10 bytes.
            final int maxBlobBytes = checkedPageBodySize(recordPageKey, "maximum deduplicated decoded body",
                (long) maximumMetadataBytes + onDiskHeapSize);

            blobStaging = v1StagingScratch(maxBlobBytes);
            final int actualBlobBytes;
            if (codec == 1) {
              final MemorySegment compressedIn = v1Lz4OutScratch(compressedLen);
              final byte[] tmp = V1_HEAP_RLE_SCRATCH.get();
              final byte[] tmpBuf = tmp.length >= compressedLen
                  ? tmp
                  : new byte[compressedLen];
              if (tmpBuf != tmp) {
                V1_HEAP_RLE_SCRATCH.set(tmpBuf);
              }
              source.read(tmpBuf, 0, compressedLen);
              MemorySegment.copy(tmpBuf, 0, compressedIn, ValueLayout.JAVA_BYTE, 0L, compressedLen);
              final FFILz4Compressor lz4 = V1_HEAP_LZ4.get();
              final MemorySegment blobView = blobStaging.asSlice(0, maxBlobBytes);
              if (lz4 == null) {
                // Pure-Java fallback: LZ4-bodied pages stay readable without liblz4.
                actualBlobBytes =
                    JavaLz4BlockDecoder.decompressSafe(compressedIn, 0L, compressedLen, blobView, 0L, maxBlobBytes);
              } else {
                actualBlobBytes =
                    lz4.decompressSegment(compressedIn.asSlice(0, compressedLen), blobView, compressedLen);
                if (actualBlobBytes < 0) {
                  throw new SirixIOException("body LZ4 decompress returned " + actualBlobBytes);
                }
              }
            } else if (codec == 0) {
              final byte[] rleBuf = V1_HEAP_RLE_SCRATCH.get();
              final byte[] rle = rleBuf.length >= compressedLen
                  ? rleBuf
                  : new byte[compressedLen];
              if (rle != rleBuf) {
                V1_HEAP_RLE_SCRATCH.set(rle);
              }
              source.read(rle, 0, compressedLen);
              actualBlobBytes = ZeroRunByteCodec.decode(rle, 0, compressedLen, blobStaging, 0L);
            } else if (codec == 2) {
              final byte[] rleBuf = V1_HEAP_RLE_SCRATCH.get();
              final byte[] rle = rleBuf.length >= compressedLen
                  ? rleBuf
                  : new byte[compressedLen];
              if (rle != rleBuf) {
                V1_HEAP_RLE_SCRATCH.set(rle);
              }
              source.read(rle, 0, compressedLen);
              actualBlobBytes = ByteRunCodec.decode(rle, 0, compressedLen, blobStaging, 0L);
            } else {
              final byte[] rleBuf = V1_HEAP_RLE_SCRATCH.get();
              final byte[] rle = rleBuf.length >= compressedLen
                  ? rleBuf
                  : new byte[compressedLen];
              if (rle != rleBuf) {
                V1_HEAP_RLE_SCRATCH.set(rle);
              }
              source.read(rle, 0, compressedLen);
              actualBlobBytes = SirixLZ77Codec.decode(rle, 0, compressedLen, blobStaging, 0L);
            }
            if (actualBlobBytes < onDiskHeapSize) {
              throw new SirixIOException("page " + recordPageKey + " body decoded to " + actualBlobBytes
                  + " bytes, shorter than its " + onDiskHeapSize + "-byte heap");
            }
            metadataLength = actualBlobBytes - onDiskHeapSize;
            if (metadataLength < minimumMetadataBytes || metadataLength > maximumMetadataBytes) {
              throw new SirixIOException("page " + recordPageKey + " has a " + metadataLength
                  + "-byte deduplicated META section, expected [" + minimumMetadataBytes + ','
                  + maximumMetadataBytes + "]");
            }
          }

          // Parse compactDir from the decompressed blob (big-endian unsigned shorts).
          final MemorySegment metadataStaging = blobStaging.asSlice(0, metadataLength);
          long blobPos = 0;
          for (int i = 0; i < populatedCount; i++) {
            compactDir[i] = PageLayout.readCompactDirEntry(metadataStaging, blobPos);
            blobPos += PageLayout.COMPACT_DIR_ENTRY_SIZE;
          }
          validateCompactDirectory(recordPageKey, compactDir, populatedCount, onDiskHeapSize, true, chunkedBody);
          // Parse template pool from the blob.
          templatePool = TEMPLATE_POOL_SCRATCH.get();
          if (templatePool.length < templatePoolBytes) {
            throw new SirixIOException("template pool too large: " + templatePoolBytes);
          }
          MemorySegment.copy(metadataStaging, ValueLayout.JAVA_BYTE, blobPos, templatePool, 0, templatePoolBytes);
          blobPos += templatePoolBytes;

          templateOffsets = TEMPLATE_OFFSETS_SCRATCH.get();
          OffsetTableTemplatePool.parseTemplateOffsets(templatePool, templatePoolBytes, templateCount, templateOffsets);

          slotTemplateIds = SLOT_TEMPLATE_IDS_SCRATCH.get();
          if (slotTemplateIds.length < populatedCount) {
            throw new SirixIOException("slot template ids buffer too small: " + populatedCount);
          }
          MemorySegment.copy(metadataStaging, ValueLayout.JAVA_BYTE, blobPos, slotTemplateIds, 0, populatedCount);
          blobPos += populatedCount;

          // Node key of every populated slot, in bitmap-ascending order. The parentKey column
          // needs these as predictor context before it can be decoded, and the per-slot loop
          // below then reads them instead of carrying its own bitmap cursor.
          final long[] slotNodeKeys = SLOT_NODE_KEY_SCRATCH.get();
          final long pageKeyBase = recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT;
          {
            int bmWordIdx = 0;
            long bmBits = 0L;
            for (int i = 0; i < populatedCount; i++) {
              while (bmBits == 0) {
                if (bmWordIdx >= PageLayout.BITMAP_WORDS) {
                  throw new SirixIOException("bitmap exhausted at entry " + i + " / " + populatedCount);
                }
                bmBits = PageLayout.getBitmapWord(headerBitmapSeg, bmWordIdx++);
              }
              slotNodeKeys[i] = pageKeyBase + (((bmWordIdx - 1) << 6) | Long.numberOfTrailingZeros(bmBits));
              bmBits &= bmBits - 1;
            }
          }

          // Read zero-hash bitmap when hash elision is active.
          if (hashElisionActive) {
            zeroHashBitmap = SLOT_ZERO_HASH_BITMAP_SCRATCH.get();
            MemorySegment.copy(metadataStaging, ValueLayout.JAVA_BYTE, blobPos, zeroHashBitmap, 0, hashBitmapBytes);
            blobPos += hashBitmapBytes;
          } else {
            zeroHashBitmap = null;
          }

          // Read parentKey column when active. Column bytes are stored
          // behind a 4-byte length prefix so we can bound the slice.
          if (parentKeyColumnActive) {
            final int cb0 = metadataStaging.get(ValueLayout.JAVA_BYTE, blobPos) & 0xFF;
            final int cb1 = metadataStaging.get(ValueLayout.JAVA_BYTE, blobPos + 1) & 0xFF;
            final int cb2 = metadataStaging.get(ValueLayout.JAVA_BYTE, blobPos + 2) & 0xFF;
            final int cb3 = metadataStaging.get(ValueLayout.JAVA_BYTE, blobPos + 3) & 0xFF;
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
            MemorySegment.copy(metadataStaging, ValueLayout.JAVA_BYTE, blobPos, scratch, 0, colLen);
            blobPos += colLen;
            parentKeyValues = SLOT_PARENT_KEY_SCRATCH.get();
            // Bulk decode: decodeSlot restarts the override walk from slot 0 each call, so
            // decoding a 1024-slot column one slot at a time is quadratic on every page read.
            final int decoded = StructuralKeyColumnCodec.decodeAll(scratch, 0, parentKeyValues, slotNodeKeys);
            if (decoded != populatedCount) {
              throw new SirixIOException("parentKey column covers " + decoded + " slots, page has " + populatedCount);
            }
            parentKeyWidths = SLOT_PARENT_KEY_WIDTH_SCRATCH.get();
          } else {
            parentKeyValues = null;
            parentKeyWidths = null;
          }

          // The two sibling columns, in the order the writer staged them and behind the same length
          // prefix. Both decode in bulk for the same reason the parentKey column does: decodeSlot
          // restarts its override walk at slot 0, so a per-slot decode would be quadratic on the page.
          if (rightSibColumnActive) {
            rightSibKeyValues = SLOT_RIGHT_SIB_READ_SCRATCH.get();
            rightSibKeyWidths = SLOT_RIGHT_SIB_READ_WIDTH_SCRATCH.get();
            blobPos = readStructuralKeyColumn(metadataStaging, blobPos, maxRightSibColBytes, populatedCount,
                slotNodeKeys, rightSibKeyValues, "right-sibling");
          } else {
            rightSibKeyValues = null;
            rightSibKeyWidths = null;
          }
          if (leftSibColumnActive) {
            leftSibKeyValues = SLOT_LEFT_SIB_READ_SCRATCH.get();
            leftSibKeyWidths = SLOT_LEFT_SIB_READ_WIDTH_SCRATCH.get();
            blobPos = readStructuralKeyColumn(metadataStaging, blobPos, maxLeftSibColBytes, populatedCount,
                slotNodeKeys, leftSibKeyValues, "left-sibling");
          } else {
            leftSibKeyValues = null;
            leftSibKeyWidths = null;
          }

          // Read pathNodeKey column when active. Layout: int length prefix + payload.
          if (pathNodeKeyColumnActive) {
            final int pb0 = metadataStaging.get(ValueLayout.JAVA_BYTE, blobPos) & 0xFF;
            final int pb1 = metadataStaging.get(ValueLayout.JAVA_BYTE, blobPos + 1) & 0xFF;
            final int pb2 = metadataStaging.get(ValueLayout.JAVA_BYTE, blobPos + 2) & 0xFF;
            final int pb3 = metadataStaging.get(ValueLayout.JAVA_BYTE, blobPos + 3) & 0xFF;
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
            MemorySegment.copy(metadataStaging, ValueLayout.JAVA_BYTE, blobPos, pnkScratch, 0, pnkColLen);
            blobPos += pnkColLen;
            // A compacted column is expanded back to the random-access layout once, here, so every
            // reader below — length derivation, record expansion, the elision tag lookup — keeps the
            // single popcount it has always done per slot.
            final int expandedLen = PathNodeKeyRegion.expandedSize(pnkScratch, pnkColLen);
            if (expandedLen > 0) {
              byte[] expandedScratch = PATH_NODE_KEY_EXPANDED_SCRATCH.get();
              if (expandedScratch.length < expandedLen) {
                expandedScratch = new byte[expandedLen];
                PATH_NODE_KEY_EXPANDED_SCRATCH.set(expandedScratch);
              }
              PathNodeKeyRegion.expand(pnkScratch, pnkColLen, expandedScratch);
              pathNodeKeyColumnBytes = expandedScratch;
              pathNodeKeyColumnLen = expandedLen;
            } else {
              pathNodeKeyColumnBytes = pnkScratch;
              pathNodeKeyColumnLen = pnkColLen;
            }
            pathNodeKeyWidths = SLOT_PATH_NODE_KEY_WIDTH_SCRATCH.get();
          } else {
            pathNodeKeyColumnBytes = null;
            pathNodeKeyColumnLen = 0;
            pathNodeKeyWidths = null;
          }

          // Read the value-elision section when active, in whichever of its two forms the page's
          // structural flags declare. The per-slot form is int elidedCount + elidedCount × (slot-gap
          // varint, type byte, width varint, region absIdx varint) in slot-ascending order; the derived
          // form is a flag byte, an elided-slot bitmap and the exceptions to what can be re-derived
          // from the page's own regions and columns. Either way the section names slots EXPLICITLY —
          // the expand pass never assumes every fused-primitive slot was elided, and the inject pass
          // never re-derives a rank that a single non-elided slot would desynchronise.
          if (valueElisionActive) {
            valueElidedSlots = VALUE_ELIDED_SLOT_READ_SCRATCH.get();
            valueElidedTypes = SLOT_VALUE_TYPE_READ_SCRATCH.get();
            valueElidedWidths = VALUE_ELIDED_WIDTH_READ_SCRATCH.get();
            valueElidedAbsIdx = VALUE_ELIDED_ABS_IDX_READ_SCRATCH.get();
            if (derivedElisionSections) {
              // Only membership and the exception lists live in the section. Everything per-slot —
              // region index, type byte, heap width — is derived from the page's own regions and
              // columns once the region table has been read, a few statements further down.
              blobPos = elisionDeriver.parseValueSection(metadataStaging, blobPos, populatedCount, metadataLength);
              valueElidedCount = 0;
            } else {
            final int vb0 = metadataStaging.get(ValueLayout.JAVA_BYTE, blobPos) & 0xFF;
            final int vb1 = metadataStaging.get(ValueLayout.JAVA_BYTE, blobPos + 1) & 0xFF;
            final int vb2 = metadataStaging.get(ValueLayout.JAVA_BYTE, blobPos + 2) & 0xFF;
            final int vb3 = metadataStaging.get(ValueLayout.JAVA_BYTE, blobPos + 3) & 0xFF;
            valueElidedCount = (vb0 << 24) | (vb1 << 16) | (vb2 << 8) | vb3;
            blobPos += 4;
            if (valueElidedCount < 0 || valueElidedCount > populatedCount) {
              throw new SirixIOException("invalid value-elision count: " + valueElidedCount);
            }
            int prevSlot = -1;
            for (int e = 0; e < valueElidedCount; e++) {
              final int gap = DeltaVarIntCodec.decodeSignedFromSegment(metadataStaging, blobPos);
              blobPos += DeltaVarIntCodec.computeSignedEncodedWidth(gap);
              final int slot = prevSlot + gap;
              if (gap <= 0 || slot >= PageLayout.SLOT_COUNT) {
                throw new SirixIOException(
                    "value-elision entry " + e + " has slot " + slot + " (gap " + gap + "), outside the page");
              }
              prevSlot = slot;
              valueElidedSlots[e] = (short) slot;
              valueElidedTypes[e] = metadataStaging.get(ValueLayout.JAVA_BYTE, blobPos);
              blobPos += 1;
              final int width = DeltaVarIntCodec.decodeSignedFromSegment(metadataStaging, blobPos);
              blobPos += DeltaVarIntCodec.computeSignedEncodedWidth(width);
              valueElidedWidths[e] = width;
              final int absIdx = DeltaVarIntCodec.decodeSignedFromSegment(metadataStaging, blobPos);
              blobPos += DeltaVarIntCodec.computeSignedEncodedWidth(absIdx);
              if (absIdx < 0) {
                throw new SirixIOException("value-elision entry " + e + " has negative region index " + absIdx);
              }
              valueElidedAbsIdx[e] = absIdx;
            }
            }
            valueOffs = SLOT_VALUE_OFF_SCRATCH.get();
            valueWidths = SLOT_VALUE_WIDTH_SCRATCH.get();
          } else {
            valueElidedCount = 0;
            valueElidedSlots = null;
            valueElidedTypes = null;
            valueElidedWidths = null;
            valueElidedAbsIdx = null;
            valueOffs = null;
            valueWidths = null;
          }

          // Lever 4: read name-key elision section when active. Layout:
          // int elidedCount + elidedCount × (1 byte width). In slot-ascending order.
          // We expand into nameKeyOffs/nameKeyWidths during the per-slot pre-walk
          // below so the strip-pass and re-inject pass have direct slot indexing.
          //
          // The elided-slot widths buffer is read into the per-thread
          // SLOT_NAME_KEY_WIDTH_PACKED_SCRATCH (length = elidedCount). The
          // pre-walk maps these to per-slot widths via a slot-ascending cursor.
          if (nameKeyElisionActive) {
            byte[] widthScratch = SLOT_NAME_KEY_WIDTH_PACKED_SCRATCH.get();
            if (derivedElisionSections) {
              // Widths are the canonical varint widths of the region's own name keys; only the slots
              // whose stripped width was not that are named here.
              blobPos = elisionDeriver.parseNameKeySection(metadataStaging, blobPos, populatedCount, metadataLength);
              if (widthScratch.length < populatedCount) {
                widthScratch = new byte[populatedCount];
                SLOT_NAME_KEY_WIDTH_PACKED_SCRATCH.set(widthScratch);
              }
            } else {
            final int nb0 = metadataStaging.get(ValueLayout.JAVA_BYTE, blobPos) & 0xFF;
            final int nb1 = metadataStaging.get(ValueLayout.JAVA_BYTE, blobPos + 1) & 0xFF;
            final int nb2 = metadataStaging.get(ValueLayout.JAVA_BYTE, blobPos + 2) & 0xFF;
            final int nb3 = metadataStaging.get(ValueLayout.JAVA_BYTE, blobPos + 3) & 0xFF;
            final int elidedCount = (nb0 << 24) | (nb1 << 16) | (nb2 << 8) | nb3;
            blobPos += 4;
            if (elidedCount < 0 || elidedCount > populatedCount) {
              throw new SirixIOException("invalid name-key elision count: " + elidedCount);
            }
            if (widthScratch.length < elidedCount) {
              widthScratch = new byte[Math.max(elidedCount, widthScratch.length * 2)];
              SLOT_NAME_KEY_WIDTH_PACKED_SCRATCH.set(widthScratch);
            }
            MemorySegment.copy(metadataStaging, ValueLayout.JAVA_BYTE, blobPos, widthScratch, 0, elidedCount);
            blobPos += elidedCount;
            }
            nameKeyElidedWidthsPacked = widthScratch;
            nameKeyOffs = SLOT_NAME_KEY_OFF_SCRATCH.get();
            nameKeyWidths = SLOT_NAME_KEY_WIDTH_SCRATCH.get();
          } else {
            nameKeyElidedWidthsPacked = null;
            nameKeyOffs = null;
            nameKeyWidths = null;
          }

          // Compute in-memory lengths: on-disk length + every byte the writer stripped — the
          // offset table it replaced with a template id, an all-zero hash, a parentKey or
          // pathNodeKey varint that moved into its column, an elided value, an elided name key.
          // The derivation is shared with anything that has to lay out the heap before decoding
          // records, so bind this page's sections once and let the deriver walk the entries.
          // Where the sections end is where the heap begins. Both framed and monolithic bodies prove
          // this boundary before any heap offset is installed; otherwise retained scratch tail bytes
          // could make a truncated META section look valid.
          if (blobPos != metadataLength) {
            throw new SirixIOException("page " + recordPageKey + " parsed " + blobPos
                + " bytes of META sections, decoded body declares " + metadataLength);
          }

          inMemDataLengths = SLOT_DATALEN_SCRATCH.get();
          final SlottedPageDecodeState deriver = decodeState;
          deriver.bindTemplates(compactDir, slotTemplateIds, templatePool, templateOffsets, inMemDataLengths);
          deriver.bindHashElision(hashElisionActive, zeroHashBitmap);
          deriver.bindParentKeyColumn(parentKeyColumnActive, parentKeyValues, parentKeyWidths);
          deriver.bindSiblingKeyColumns(rightSibColumnActive, rightSibKeyValues, rightSibKeyWidths, leftSibColumnActive,
              leftSibKeyValues, leftSibKeyWidths);
          deriver.bindPathNodeKeyColumn(pathNodeKeyColumnActive, pathNodeKeyColumnBytes, pathNodeKeyWidths);
          deriver.bindValueElision(valueElisionActive, valueElidedCount, valueElidedSlots, valueElidedWidths, valueOffs,
              valueWidths);
          deriver.bindNameKeyElision(nameKeyElisionActive, nameKeyElidedWidthsPacked, nameKeyOffs, nameKeyWidths);
          // The lengths themselves are derived once the regions are in hand, a few statements below:
          // an elided slot's heap width is the width of the value its region holds.
          inMemHeapSize = 0;

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
        rightSibKeyValues = null;
        rightSibKeyWidths = null;
        rightSibColumnActive = false;
        leftSibKeyValues = null;
        leftSibKeyWidths = null;
        leftSibColumnActive = false;
        pathNodeKeyColumnBytes = null;
        pathNodeKeyColumnLen = 0;
        pathNodeKeyWidths = null;
        pathNodeKeyColumnActive = false;
        valueElisionActive = false;
        derivedElisionSections = false;
        valueElidedCount = 0;
        valueElidedSlots = null;
        valueElidedTypes = null;
        valueElidedWidths = null;
        valueElidedAbsIdx = null;
        valueOffs = null;
        valueWidths = null;
        nameKeyElisionActive = false;
        nameKeyOffs = null;
        nameKeyWidths = null;
        inMemHeapSize = onDiskHeapSize;
        templatePoolBytes = 0;
        for (int i = 0; i < populatedCount; i++) {
          compactDir[i] = PageLayout.readCompactDirEntry(source);
        }
      }

      // The page's PAX regions sit immediately behind its body on the wire, and the derived elision
      // sections are functions OF those regions: the heap width an elided slot gets back is the width
      // of the value the region holds. A directory cannot be laid out before those widths are known,
      // so a page whose body has already been consumed in full reads its regions here — the same wire
      // order, an earlier moment — and only then derives its lengths and expands its records. The V0
      // no-dedup path takes its heap straight out of `source` below, so its table is still read after
      // that. From here on the region table and the slotted frame are owned by this block until the
      // page takes them over.
      MemorySegment slottedPage = null;
      RegionTable regionTable = null;
      boolean regionTableRead = false;
      KeyValueLeafPage pageForCleanup = null;
      boolean regionTableTransferred = false;
      try {
        if (offsetTableDedup) {
          regionTable = RegionTable.read(source);
          regionTableRead = true;
        }
        if (offsetTableDedup && templateCount > 0) {
          if (derivedElisionSections) {
            // One derivation, run by the writer to size and verify and by the reader to reconstruct.
            elisionDeriver.bind(regionTable, pathNodeKeyColumnActive
                ? pathNodeKeyColumnBytes
                : null);
            if (valueElisionActive) {
              valueElidedCount = elisionDeriver.deriveValueMetadata(headerBitmapSeg, populatedCount, compactDir,
                  valueElidedSlots, valueElidedTypes, valueElidedWidths, valueElidedAbsIdx);
              decodeState.bindValueElision(true, valueElidedCount, valueElidedSlots, valueElidedWidths, valueOffs,
                  valueWidths);
            }
            if (nameKeyElisionActive) {
              final int derivedWidths = elisionDeriver.deriveNameKeyWidths(headerBitmapSeg, populatedCount, compactDir,
                  nameKeyElidedWidthsPacked);
              if (derivedWidths > nameKeyElidedWidthsPacked.length) {
                throw new SirixIOException("page " + recordPageKey + " derives " + derivedWidths
                    + " name-key widths into a " + nameKeyElidedWidthsPacked.length + "-entry buffer");
              }
            }
          }
          inMemHeapSize = decodeState.deriveAll(headerBitmapSeg, populatedCount);
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
        final int allocSize = checkedPageBodySize(recordPageKey, "in-memory slotted page",
            (long) PageLayout.HEAP_START + inMemHeapSize);
        slottedPage = memorySegmentAllocator.allocate(allocSize);

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
            MemorySegment.copy(msSource.getSource(), source.position(), slottedPage, PageLayout.HEAP_START,
                onDiskHeapSize);
            source.skip(onDiskHeapSize);
          } else {
            final byte[] heapData = new byte[onDiskHeapSize];
            source.read(heapData);
            MemorySegment.copy(heapData, 0, slottedPage, ValueLayout.JAVA_BYTE, PageLayout.HEAP_START, heapData.length);
          }
        } else if (templateCount == 0) {
          // 8a'. Inline path (dedup failed) — the heap bytes live inside the
          // decompressed blob that the templateCount==0 branch above staged.
          // Copy them into the slotted page's heap region.
          final MemorySegment stagingSeg = BLOB_STAGING_HOLDER.get();
          final long heapBase = BLOB_HEAP_OFFSET_HOLDER.get();
          BLOB_STAGING_HOLDER.set(null);
          BLOB_HEAP_OFFSET_HOLDER.set(0L);
          // Lazily, the staging holds the META section and nothing behind it: the records are still
          // sitting in their chunks, and each chunk copies its own run of them in when first read.
          if (!lazyChunks) {
            MemorySegment.copy(stagingSeg, heapBase, slottedPage, PageLayout.HEAP_START, onDiskHeapSize);
          }
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
              throw new SirixIOException("Bitmap has more set bits than compact directory entries: entryIdx=" + entryIdx
                  + ", populatedCount=" + populatedCount);
            }
            final int packed = compactDir[entryIdx];
            final int onDiskLen = PageLayout.unpackDataLength(packed);
            final int nodeKindId = PageLayout.unpackNodeKindId(packed);
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
        if (entryIdx != populatedCount || heapOffset != inMemHeapSize) {
          throw new SirixIOException("page " + recordPageKey + " rebuilt " + entryIdx + " directory entries and "
              + heapOffset + " heap bytes; expected " + populatedCount + " entries and " + inMemHeapSize + " bytes");
        }

        // 8b. Dedup path: expand each record's (kindId + data) from the staged
        // heap section of the previously-decompressed blob into the in-memory
        // heap region, injecting the offset table from the template pool. The
        // blob was already decompressed above — we only consume from it here.
        // Structural re-injection in up to three places, sorted by in-memory offset:
        // - parentKey column: re-encode delta-varint(parentKey, nodeKey) at
        // data-region offset 0 (parentKey is always field 0).
        // - pathNodeKey column: re-encode delta-varint(pnk, nodeKey) at
        // data-region offset pnkOff (kind-specific interior).
        // - hash elision: re-inject 8 zero bytes at the hash offset.
        // Each operation produces bytes whose widths were recorded pre-strip,
        // so the in-memory dataLength exactly matches inMemDataLengths[i].
        //
        // The reinject strategy interleaves copies of on-disk bytes with the
        // three insertion ranges, walking both cursors in parallel.
        if (offsetTableDedup && templateCount > 0 && !lazyChunks) {
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
              onDiskPos += decodeState.expandEntryInto(slottedPage, entryIdx2, slot, stagingSeg, heapBase + onDiskPos,
                  pageKeyBase);
              entryIdx2++;
              word &= word - 1;
            }
          }
          if (onDiskPos != onDiskHeapSize) {
            throw new SirixIOException("heap-size mismatch: consumed=" + onDiskPos + " expected=" + onDiskHeapSize);
          }
        }

        // 8c. Lazy bookkeeping. The directory is complete, so each chunk's run of slots and the heap
        // range it owns are already known — that is the whole point of putting the sections in their
        // own frame. Recorded now, while the bitmap walk is cheap and the answers are needed by a
        // reader that may arrive on any thread.
        final short[] slotOfEntry;
        final int[] chunkFirstSlot;
        final int[] chunkLastSlot;
        final int[] chunkHeapFrom;
        final int[] chunkHeapTo;
        if (lazyChunks) {
          BLOB_STAGING_HOLDER.set(null);
          BLOB_HEAP_OFFSET_HOLDER.set(0L);
          final ChunkTable table = READ_CHUNK_TABLE.get();
          slotOfEntry = new short[populatedCount];
          int entryIdx3 = 0;
          for (int w = 0; w < PageLayout.BITMAP_WORDS; w++) {
            long word = PageLayout.getBitmapWord(slottedPage, w);
            while (word != 0) {
              slotOfEntry[entryIdx3++] = (short) ((w << 6) | Long.numberOfTrailingZeros(word));
              word &= word - 1;
            }
          }
          chunkFirstSlot = new int[table.count];
          chunkLastSlot = new int[table.count];
          chunkHeapFrom = new int[table.count];
          chunkHeapTo = new int[table.count];
          for (int c = 0; c < table.count; c++) {
            final int firstOfChunk = slotOfEntry[table.firstEntry[c]] & 0xFFFF;
            final int lastOfChunk = slotOfEntry[table.firstEntry[c] + table.entryCount[c] - 1] & 0xFFFF;
            chunkFirstSlot[c] = firstOfChunk;
            chunkLastSlot[c] = lastOfChunk;
            chunkHeapFrom[c] = PageLayout.getDirHeapOffset(slottedPage, firstOfChunk);
            chunkHeapTo[c] = PageLayout.getDirHeapOffset(slottedPage, lastOfChunk)
                + PageLayout.getDirDataLength(slottedPage, lastOfChunk);
          }
        } else {
          slotOfEntry = null;
          chunkFirstSlot = null;
          chunkLastSlot = null;
          chunkHeapFrom = null;
          chunkHeapTo = null;
        }

        final int heapSize = inMemHeapSize; // semantic alias for the remainder of this method

        // 11. Set heapEnd and heapUsed (both = heapSize since deserialized heap is contiguous/defragmented)
        PageLayout.setHeapEnd(slottedPage, heapSize);
        PageLayout.setHeapUsed(slottedPage, heapSize);

        final boolean areDeweyIDsStored = resourceConfig.areDeweyIDsStored;
        final RecordSerializer recordPersister = resourceConfig.recordPersister;

        // PAX region table appended after the heap. Empty on writes produced by
        // the Phase-1 scaffold (4 bytes: int regionCount=0); later tasks populate it. Already read
        // above for every body this reader consumed in full; the V0 no-dedup path, whose heap came
        // straight out of `source` a few statements ago, reads it here.
        if (!regionTableRead) {
          regionTable = RegionTable.read(source);
        }

        // Lever 4: NAME-KEY elision second-pass injection. Still runs before
        // value-elision for orderliness, though the ordering is no longer load-bearing:
        // value injection decodes by absolute region index and reads nothing from the
        // slot's nameKey field. We
        // walk the bitmap, look up each fused OBJECT_NAMED_* slot's int nameKey
        // via ObjectKeyNameKeyRegion.nameKeyForSlot, and re-encode the signed
        // varint into the heap at the same offset+width the writer recorded.
        // Width round-trip is verified vs the on-disk byte (deterministic).
        if (nameKeyElisionActive && regionTable != null && !lazyChunks) {
          injectNameKeyElidedRecords(slottedPage, populatedCount, nameKeyOffs, nameKeyWidths, regionTable, 0,
              populatedCount);
        }

        // Lever 3: VALUE elision second-pass injection. After the heap is fully
        // expanded with placeholder zeros at each elided slot's value field, we
        // walk the bitmap, look up each fused-NUMBER slot's tag (nameKey or
        // pathNodeKey based on the region's tagKind), compute its slotRank, and
        // pull the original long value from the NumberRegion. We then re-encode
        // the [type:1][varint] payload into the heap at the same offset+width
        // the writer would have written. The width was preserved on disk so we
        // can validate equality with computeSignedEncodedWidth.
        if (valueElisionActive && regionTable != null && !lazyChunks) {
          // DECLINE UP FRONT, not partway through. Eager expansion runs inside deserializePage,
          // where the KeyValueLeafPage does not exist yet, so there is nothing that can hold a
          // dictionary -- and unlike the lazy path there is no later attach to defer a slot to:
          // eager expansion IS the whole expansion. Meeting a global tag mid-loop would leave the
          // page half-injected before failing, so the region is checked before a single slot is
          // touched and the message names the page rather than a slot.
          refuseGlobalTagsOnEagerPath(regionTable, recordPageKey);
          // null, not NONE, and deliberately: the line above has already proved this region carries
          // no global tag, so nothing here can ask for a resolved value. Handing it an empty table
          // would make a later regression -- a global tag reaching this path -- fail at a slot with
          // a lookup miss instead of at the page with the refusal that names the real cause.
          injectValueElidedRecords(slottedPage, valueElidedCount, valueElidedSlots, valueElidedTypes, valueElidedWidths,
              valueElidedAbsIdx, regionTable, 0, PageLayout.SLOT_COUNT - 1, null);
        }

        // Read overlong entries
        final var overlongEntriesBitmap = SerializationType.deserializeBitSet(source);
        final int overlongEntrySize = source.readInt();
        if (overlongEntrySize < 0 || overlongEntriesBitmap.cardinality() != overlongEntrySize) {
          throw new SirixIOException("overlong-entry bitmap/count mismatch on page " + recordPageKey + ": bitmap="
              + overlongEntriesBitmap.cardinality() + ", count=" + overlongEntrySize);
        }
        final Map<Long, PageReference> references = new LinkedHashMap<>(overlongEntrySize);
        var setBit = -1;
        for (int index = 0; index < overlongEntrySize; index++) {
          setBit = overlongEntriesBitmap.nextSetBit(setBit + 1);
          final long key = (recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + setBit;
          final PageReference reference = new PageReference();
          reference.setKey(source.readLong());
          references.put(key, reference);
        }

        // Create the page before decoding the optional sidecar so its packed native storage owns
        // the bytes directly; no transient byte[][] payload survives deserialization. From here on,
        // the page is also the sole owner of the slotted frame on every failure path.
        final MemorySegment dummySlotMemory = memorySegmentAllocator.allocate(1);
        final KeyValueLeafPage page = new KeyValueLeafPage(recordPageKey, revision, indexType, resourceConfig,
            areDeweyIDsStored, recordPersister, references, dummySlotMemory, null, -1);
        pageForCleanup = page;
        page.setSlottedPage(slottedPage);

        if (hasOverflowSlotSidecar) {
          readOverflowSlotSidecar(source, page, overlongEntriesBitmap);
        }

        // Read the FSST symbol-table reference — see writeFsstSymbolTable for the two cases.
        // Anything else, including the positive length an embedded table would have carried, is a
        // corrupt or foreign page and is rejected rather than guessed at. A chunked page has no tail
        // marker at all: its id was read from the body prefix, where a bounded read can reach it.
        long fsstSymbolTableId = KeyValueLeafPage.NO_FSST_SYMBOL_TABLE_ID;
        if (chunkedBody) {
          if (chunkedFsstDictId < 0) {
            throw new SirixIOException("page " + recordPageKey + " references FSST symbol table id " + chunkedFsstDictId
                + ", which is not a valid id — ids start at 1 and 0 means the page has no table");
          }
          fsstSymbolTableId = chunkedFsstDictId;
        } else {
          final int fsstSymbolTableMarker = source.readInt();
          if (fsstSymbolTableMarker == FSST_SYMBOL_TABLE_REFERENCE_MARKER) {
            fsstSymbolTableId = Utils.getVarLong(source);
            if (fsstSymbolTableId <= 0) {
              throw new SirixIOException(
                  "page " + recordPageKey + " references FSST symbol table id " + fsstSymbolTableId
                      + ", which is not a valid" + " id — ids start at 1 and 0 means the page has no table");
            }
          } else if (fsstSymbolTableMarker != 0) {
            throw new SirixIOException(
                "page " + recordPageKey + " has an unrecognised FSST symbol-table marker " + fsstSymbolTableMarker);
          }
        }

        if (fsstSymbolTableId != KeyValueLeafPage.NO_FSST_SYMBOL_TABLE_ID) {
          // Resolved lazily, on the first string this page is asked to decode: deserialization has
          // no storage-engine reader to walk the dictionary trie with, and a page whose strings are
          // never read should not pay for a lookup it does not need.
          page.setFsstSymbolTableId(fsstSymbolTableId);
        }

        if (regionTable != null) {
          page.setRegionTable(regionTable);
          regionTableTransferred = true;
        }

        // Read the trie-lane flag ONCE, here, and remember it on the page. The reader tests it on
        // the return of every record-page lookup to decide whether the page still owes a resolution
        // pass, and that is a per-record path -- re-parsing a string-region header there to learn
        // that the answer is "no" would tax every scan in the system for a lane almost no page uses.
        //
        // ORDER: before attachLazyChunks, which is where the page becomes expandable. A page that
        // could expand while still claiming to have no global tags would sail past the pre-pass that
        // must refuse it, and the values it produced would be the placeholder zeros rather than an
        // error. Both facts the flag needs are in scope here and nowhere later.
        page.setHasGlobalStringTags(valueElisionActive && stringRegionHasGlobalTags(regionTable));

        if (lazyChunks) {
          // Last, because the state it hands the page has to be able to reach the regions: an
          // expansion re-injects the values the writer moved out of the records, and those live in the
          // region table this page was just given.
          attachLazyChunks(page, slottedPage, recordPageKey, populatedCount, templateCount, templatePoolBytes,
              compactDir, slotTemplateIds, templatePool, templateOffsets, inMemDataLengths, hashElisionActive,
              zeroHashBitmap, parentKeyColumnActive, parentKeyValues, parentKeyWidths, rightSibColumnActive,
              rightSibKeyValues, rightSibKeyWidths, leftSibColumnActive, leftSibKeyValues, leftSibKeyWidths,
              pathNodeKeyColumnActive, pathNodeKeyColumnBytes, pathNodeKeyColumnLen, pathNodeKeyWidths,
              valueElisionActive, valueElidedCount,
              valueElidedSlots, valueElidedTypes, valueElidedWidths, valueElidedAbsIdx, valueOffs, valueWidths,
              nameKeyElisionActive, nameKeyOffs, nameKeyWidths, slotOfEntry, chunkFirstSlot, chunkLastSlot,
              chunkHeapFrom, chunkHeapTo);
        }

        return page;
      } catch (final RuntimeException | Error failure) {
        if (pageForCleanup != null) {
          try {
            pageForCleanup.close();
          } catch (final RuntimeException | Error cleanupFailure) {
            if (cleanupFailure != failure) {
              failure.addSuppressed(cleanupFailure);
            }
          }
        }
        if (!regionTableTransferred && regionTable != null) {
          try {
            regionTable.close();
          } catch (final RuntimeException | Error cleanupFailure) {
            if (cleanupFailure != failure) {
              failure.addSuppressed(cleanupFailure);
            }
          }
        }
        // The slotted frame is allocated before the appended region table is decoded. Corrupt
        // region/tail bytes can therefore fail before a KeyValueLeafPage exists to own and close
        // the frame. Once a page exists, setSlottedPage is its first operation and page.close()
        // above is the sole owner-side release.
        if (pageForCleanup == null && slottedPage != null) {
          try {
            memorySegmentAllocator.release(slottedPage);
          } catch (final RuntimeException | Error cleanupFailure) {
            if (cleanupFailure != failure) {
              failure.addSuppressed(cleanupFailure);
            }
          }
        }
        throw failure;
      }
    }

    /**
     * Hand the page everything an expansion will need, and nothing that belongs to this thread.
     *
     * <p>
     * Every array below is a per-thread scratch the next page decoded on this thread overwrites, so
     * each is copied at exactly the width this page uses. That copying is the price of laziness: a
     * chunk is expanded by whichever thread first reads a slot in it, arbitrarily long after the thread
     * that parsed the sections moved on. Only the levers the page actually activated are copied — most
     * pages carry two or three.
     */
    private static void attachLazyChunks(final KeyValueLeafPage page, final MemorySegment slottedPage,
        final long recordPageKey, final int populatedCount, final int templateCount, final int templatePoolBytes,
        final int[] compactDir, final byte[] slotTemplateIds, final byte[] templatePool, final int[] templateOffsets,
        final int[] inMemDataLengths, final boolean hashElisionActive, final byte[] zeroHashBitmap,
        final boolean parentKeyColumnActive, final long[] parentKeyValues, final byte[] parentKeyWidths,
        final boolean rightSibColumnActive, final long[] rightSibKeyValues, final byte[] rightSibKeyWidths,
        final boolean leftSibColumnActive, final long[] leftSibKeyValues, final byte[] leftSibKeyWidths,
        final boolean pathNodeKeyColumnActive, final byte[] pathNodeKeyColumnBytes, final int pathNodeKeyColumnLen,
        final byte[] pathNodeKeyWidths, final boolean valueElisionActive, final int valueElidedCount,
        final short[] valueElidedSlots, final byte[] valueElidedTypes, final int[] valueElidedWidths,
        final int[] valueElidedAbsIdx, final short[] valueOffs, final short[] valueWidths,
        final boolean nameKeyElisionActive, final short[] nameKeyOffs, final byte[] nameKeyWidths,
        final short[] slotOfEntry, final int[] chunkFirstSlot, final int[] chunkLastSlot, final int[] chunkHeapFrom,
        final int[] chunkHeapTo) {
      final SlottedPageDecodeState state;
      final LazyChunkedBody.RangeInjector injector;
      if (templateCount == 0) {
        // Degenerate body: the chunks hold the records verbatim, so expansion is a copy and there is
        // no derived state to carry.
        state = null;
        injector = null;
      } else {
        state = new SlottedPageDecodeState();
        state.bindTemplates(Arrays.copyOf(compactDir, populatedCount), Arrays.copyOf(slotTemplateIds, populatedCount),
            Arrays.copyOf(templatePool, templatePoolBytes), Arrays.copyOf(templateOffsets, templateCount + 1),
            Arrays.copyOf(inMemDataLengths, populatedCount));
        state.bindHashElision(hashElisionActive, hashElisionActive
            ? Arrays.copyOf(zeroHashBitmap, (populatedCount + 7) >>> 3)
            : null);
        state.bindParentKeyColumn(parentKeyColumnActive, parentKeyColumnActive
            ? Arrays.copyOf(parentKeyValues, populatedCount)
            : null,
            parentKeyColumnActive
                ? Arrays.copyOf(parentKeyWidths, populatedCount)
                : null);
        state.bindSiblingKeyColumns(rightSibColumnActive, rightSibColumnActive
            ? Arrays.copyOf(rightSibKeyValues, populatedCount)
            : null,
            rightSibColumnActive
                ? Arrays.copyOf(rightSibKeyWidths, populatedCount)
                : null,
            leftSibColumnActive, leftSibColumnActive
                ? Arrays.copyOf(leftSibKeyValues, populatedCount)
                : null,
            leftSibColumnActive
                ? Arrays.copyOf(leftSibKeyWidths, populatedCount)
                : null);
        state.bindPathNodeKeyColumn(pathNodeKeyColumnActive, pathNodeKeyColumnActive
            ? Arrays.copyOf(pathNodeKeyColumnBytes, pathNodeKeyColumnLen)
            : null,
            pathNodeKeyColumnActive
                ? Arrays.copyOf(pathNodeKeyWidths, populatedCount)
                : null);
        final short[] elidedSlots = valueElisionActive
            ? Arrays.copyOf(valueElidedSlots, valueElidedCount)
            : null;
        final int[] elidedWidths = valueElisionActive
            ? Arrays.copyOf(valueElidedWidths, valueElidedCount)
            : null;
        final byte[] elidedTypes = valueElisionActive
            ? Arrays.copyOf(valueElidedTypes, valueElidedCount)
            : null;
        final int[] elidedAbsIdx = valueElisionActive
            ? Arrays.copyOf(valueElidedAbsIdx, valueElidedCount)
            : null;
        state.bindValueElision(valueElisionActive, valueElidedCount, elidedSlots, elidedWidths, valueElisionActive
            ? Arrays.copyOf(valueOffs, populatedCount)
            : null,
            valueElisionActive
                ? Arrays.copyOf(valueWidths, populatedCount)
                : null);
        final short[] nkOffs = nameKeyElisionActive
            ? Arrays.copyOf(nameKeyOffs, populatedCount)
            : null;
        final byte[] nkWidths = nameKeyElisionActive
            ? Arrays.copyOf(nameKeyWidths, populatedCount)
            : null;
        // The packed widths section feeds the length derivation, which is done: expansion never
        // reads it.
        state.bindNameKeyElision(nameKeyElisionActive, null, nkOffs, nkWidths);
        if (nameKeyElisionActive || valueElisionActive) {
          injector = (seg, regions, fromEntry, toEntry, fromSlot, toSlot) -> {
            if (regions == null) {
              return;
            }
            // PRE-PASS, before the first heap write of this chunk. A page whose global tags nobody
            // resolved is refused whole and left exactly as it was; a mid-loop throw would leave the
            // heap half-injected, with the un-reached slots still holding the placeholder zeros
            // expansion starts from and nothing on the page recording where the loop stopped. Gated
            // on a field the deserializer set, so a page without the lane pays one boolean read.
            if (page.hasGlobalStringTags()) {
              refuseUnresolvedGlobalTags(regions, page.getPageKey(), page.resolvedGlobalStrings());
            }
            if (nameKeyElisionActive) {
              injectNameKeyElidedRecords(seg, populatedCount, nkOffs, nkWidths, regions, fromEntry, toEntry);
            }
            if (valueElisionActive) {
              // The resolved table is READ HERE, inside the lambda, and must stay here. This lambda
              // is built during deserialization, when the page has none -- the reader resolves
              // afterwards, exactly as it does the FSST symbol table. Hoisting this call to the
              // construction site would capture null on every page, and on a pooled or reused page
              // it would capture a stale one: the same reused-state hazard that once put a guard in
              // the wrong encoder. What it reads is BYTES; nothing transaction-scoped is captured.
              injectValueElidedRecords(seg, valueElidedCount, elidedSlots, elidedTypes, elidedWidths, elidedAbsIdx,
                  regions, fromSlot, toSlot, page.resolvedGlobalStrings());
            }
          };
        } else {
          injector = null;
        }
      }
      final LazyChunkedBody lazyBody =
          new LazyChunkedBody(recordPageKey, recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT, READ_CHUNK_TABLE.get(),
              chunkFirstSlot, chunkLastSlot, chunkHeapFrom, chunkHeapTo, slotOfEntry, state, injector);
      if (ChunkedBodyConfig.poisonEnabled()) {
        lazyBody.poison(slottedPage);
      }
      page.setLazyChunkedBody(lazyBody);
      ChunkedBodyConfig.recordLazyLoad();
    }

    @Override
    public void serializePage(final ResourceConfiguration resourceConfig, final BytesOut<?> sink, final Page page,
        final SerializationType type) {
      serializeKeyValueLeafPage(resourceConfig, sink, page, type, null);
    }

    @Override
    public void serializeDisposablePage(final ResourceConfiguration resourceConfig, final BytesOut<?> sink,
        final Page page, final SerializationType type) {
      // The table is populated, consumed and closed on this serializer worker. It is deliberately
      // absent from the page handed to the append thread; only the completed encoded cache crosses
      // that boundary.
      try (RegionTable writerTable = RegionTable.newConfinedWriterTable()) {
        serializeKeyValueLeafPage(resourceConfig, sink, page, type, writerTable);
      }
    }

    private void serializeKeyValueLeafPage(final ResourceConfiguration resourceConfig, final BytesOut<?> sink,
        final Page page, final SerializationType type, final @Nullable RegionTable disposableWriterTable) {
      final KeyValueLeafPage keyValueLeafPage = (KeyValueLeafPage) page;

      // Check for zero-copy compressed segment first
      final MemorySegment cachedSegment = keyValueLeafPage.getCompressedSegment();
      if (cachedSegment != null) {
        sink.writeSegment(cachedSegment, 0, cachedSegment.byteSize());
        if (PAGE_SECTION_DIAG) {
          PageSectionDiag.recordCacheServe(keyValueLeafPage.getIndexType().getID());
        }
        return;
      }

      // Legacy byte[] cache fallback
      final var bytes = keyValueLeafPage.getBytes();
      if (bytes != null) {
        sink.write(bytes.toByteArray());
        if (PAGE_SECTION_DIAG) {
          PageSectionDiag.recordCacheServe(keyValueLeafPage.getIndexType().getID());
        }
        return;
      }

      // [DIAG] Past this point the page is FULLY encoded: staging, template dedup, the region build
      // and the codec pass all run. The U6 ledger times exactly that work and attributes it to the
      // call path, so an encode whose bytes are later discarded is visible as work, not as bytes.
      final long encodeStartNanos = PAGE_SECTION_DIAG
          ? System.nanoTime()
          : 0L;
      final long encodeStartPosition = PAGE_SECTION_DIAG
          ? sink.writePosition()
          : 0L;

      // Ensure slotted page exists — ALL pages use slotted page format V0
      keyValueLeafPage.ensureSlottedPage();

      // Which body format this page gets is decided once here. The envelope itself is emitted after
      // addReferences(), because that sealing step may discover a dense-frame spill and create the
      // optional logical-slot sidecar flag.
      final boolean chunkedBody = ChunkedBodyConfig.enabled();

      final Map<Long, PageReference> references = keyValueLeafPage.getReferencesMap();

      // Compress strings BEFORE addReferences() serializes them. The symbol table is no longer
      // built here — NodeStorageEngineWriter.buildRevisionFsstSymbolTable builds one per commit
      // from strings pooled across all pages and hands it to each page before serialization. A
      // page without a table (FSST off, or the commit's strings too few/too incompressible to pay
      // for one) serializes its strings raw; compressStringValues no-ops without a table.
      keyValueLeafPage.compressStringValues();

      // addReferences: serializes records to slotted page heap via processEntries,
      // copies preserved slots from completePageRef for DIFFERENTIAL/INCREMENTAL versioning
      keyValueLeafPage.addReferences(resourceConfig);

      byte envelopeFlags = chunkedBody
          ? ChunkedBodyConfig.FLAG_CHUNKED_BODY
          : (byte) 0;
      if (keyValueLeafPage.getSideSlotCount() != 0) {
        envelopeFlags |= FLAG_OVERFLOW_SLOT_SIDECAR;
      }
      sink.writeByte(KEYVALUELEAFPAGE.id);
      writeVersionAndFlags(sink, envelopeFlags);

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
      final long diagStart = sectionDiag
          ? sink.writePosition()
          : 0L;

      // 1. Write header (32B) + bitmap (128B) — 160 bytes (on-disk format, never changes)
      final byte pageFlags = PageLayout.getFlags(slottedPage);
      PageLayout.setFlags(slottedPage, references.isEmpty()
          ? (byte) (pageFlags | PageLayout.FLAG_COMPLETE_COLUMN_COVERAGE)
          : (byte) (pageFlags & ~PageLayout.FLAG_COMPLETE_COLUMN_COVERAGE));
      sink.writeSegment(slottedPage, 0, PageLayout.DISK_HEADER_BITMAP_SIZE);

      final long afterHeaderBitmap = sectionDiag
          ? sink.writePosition()
          : 0L;

      // 2. Single-pass bitmap scan: collect per-slot (kindId, fieldCount, heapOffset, dataLength)
      // into thread-local scratch. Defers compact-dir + heap emission to the
      // encoding-branch below so template dedup can rewrite lengths without a re-walk. The same
      // compact descriptors also drive region collection and the later encoder passes; neither has
      // to revisit the bitmap or re-read directory kinds through foreign memory.
      final int populatedCount = keyValueLeafPage.getCachedPopulatedCount();
      final int[] scratch = SERIALIZE_SCRATCH.get();
      final int[] slotKindIds = SLOT_KINDID_SCRATCH.get();
      final byte[] slotFieldCounts = SLOT_FIELD_COUNT_SCRATCH.get();
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
          slotFieldCounts[slotIdx] = (byte) NodeFieldLayout.fieldCountForKind(nodeKindId);
          slotHeapOffs[slotIdx] = heapOffset;
          slotDataLens[slotIdx] = dataLength;
          slotBits[slotIdx] = (short) slot;
          slotIdx++;
          word &= word - 1; // clear lowest set bit
        }
      }

      // PAX regions are built BEFORE the heap encode, even though they are written after it:
      // the region build assigns each contributing fused-primitive slot its absolute index in
      // its region, and the heap encoder elides a slot's value exactly when it holds such an
      // index. Building second would leave the encoder deciding elision by re-deriving the
      // region's membership rules — the desync that kept elision all-or-nothing. The build only
      // reads the slotted page, so the ordering swap changes no bytes on its own.
      final int[] slotRegionAbsIdx = SLOT_REGION_ABS_IDX_SCRATCH.get();
      Arrays.fill(slotRegionAbsIdx, -1);
      final RegionTable regionTable = buildRegionTable(keyValueLeafPage, slottedPage, resourceConfig, populatedCount,
          slotKindIds, slotBits, slotRegionAbsIdx, disposableWriterTable);

      // Encoding. Always attempts offset-table dedup + compressed heap; falls
      // back to the plain-heap marker (templateCount=0) when dedup aborts
      // (e.g. > 255 unique templates, or raw slab bytes without any offset
      // table structure).
      //
      // Wire layout:
      // compactDir | onDiskHeapSize | templateCount(byte)
      // | if templateCount > 0: templatePoolBytes(int) | pool | slotIds
      // | compressedLen(int) | codec(byte) | compressed bytes
      // | if templateCount == 0: heapBytes (inline, uncompressed)
      try {
        final boolean nameKeyRegionPresent =
            regionTable != null && regionTable.payload(RegionTable.KIND_OBJECT_KEY_NAMEKEY) != null;
        writeEncodedBody(sink, slottedPage, populatedCount, slotKindIds, slotFieldCounts, slotHeapOffs, slotDataLens,
            slotBits, slotRegionAbsIdx, keyValueLeafPage.areDeweyIDsStored(), chunkedBody,
            keyValueLeafPage.getFsstSymbolTableId(), nameKeyRegionPresent, keyValueLeafPage.getIndexType().getID(),
            regionTable);
      } catch (final RuntimeException | Error failure) {
        // A normal table is not page-owned until the install below. If body encoding fails first,
        // return every pooled region frame immediately; the disposable variant is owned by its
        // caller's try-with-resources.
        if (regionTable != null && disposableWriterTable == null) {
          try {
            regionTable.close();
          } catch (final RuntimeException | Error cleanupFailure) {
            if (cleanupFailure != failure) {
              failure.addSuppressed(cleanupFailure);
            }
          }
        }
        throw failure;
      }

      final long afterEncodedBody = sectionDiag
          ? sink.writePosition()
          : 0L;

      // PAX region table after the heap on the wire, unchanged.
      if (regionTable == null) {
        sink.writeInt(0);
      } else {
        if (disposableWriterTable == null) {
          keyValueLeafPage.setRegionTable(regionTable);
        }
        if (sectionDiag) {
          for (byte kind = 0; kind < RegionTable.KIND_COUNT; kind++) {
            final MemorySegment payload = regionTable.payload(kind);
            if (payload != null) {
              PageSectionDiag.recordRegion(kind, (int) payload.byteSize());
            }
          }
        }
        regionTable.write(sink, resourceConfig.regionCompressionType == RegionCompressionType.LZ77);
      }

      final long afterRegionTable = sectionDiag
          ? sink.writePosition()
          : 0L;

      // Write overlong entries
      writeOverlongEntries(sink, references);

      if (keyValueLeafPage.getSideSlotCount() != 0) {
        writeOverflowSlotSidecar(sink, keyValueLeafPage);
      }

      final long afterOverlong = sectionDiag
          ? sink.writePosition()
          : 0L;

      // Write FSST symbol table
      writeFsstSymbolTable(sink, keyValueLeafPage, chunkedBody);

      final long afterFsst = sectionDiag
          ? sink.writePosition()
          : 0L;

      if (sectionDiag) {
        PageSectionDiag.record(afterHeaderBitmap - diagStart, afterEncodedBody - afterHeaderBitmap,
            afterRegionTable - afterEncodedBody, afterOverlong - afterRegionTable, afterFsst - afterOverlong);
        PageSectionDiag.recordRecords(populatedCount);
      }

      // Compress the serialized data — but NOT when the page still carries unresolved overflow
      // references (#1076). Their disk keys are only assigned when the OverflowPages are written
      // during the recursive commit, which runs AFTER the parallel pre-serialization pass;
      // caching now would freeze NULL keys into the page bytes and the records would be
      // unreadable after reopen. Skipping the cache makes the page serialize again at write
      // time with the real keys (compressStringValues/addReferences are idempotent for the
      // second pass; the revision's symbol table was handed to the page before the first).
      boolean hasUnresolvedOverflowReferences = false;
      for (final PageReference overflowReference : references.values()) {
        if (overflowReference.getKey() == Constants.NULL_ID_LONG) {
          hasUnresolvedOverflowReferences = true;
          break;
        }
      }
      if (!hasUnresolvedOverflowReferences && !skipsEmptyPipelineIdentityCache(resourceConfig, sink)) {
        compressAndCache(resourceConfig, sink, keyValueLeafPage);
      }

      // Release node object references while the logical slotted frame is still intact. An async
      // disposable-copy caller may deliberately own the empty-pipeline identity copy; it copies the
      // sink into that frame only after this method (and therefore every flyweight unbind) returns.
      keyValueLeafPage.clearRecordsForGC();

      if (PAGE_SECTION_DIAG) {
        PageSectionDiag.recordFullEncode(keyValueLeafPage.getIndexType().getID(), disposableWriterTable == null
            ? PageSectionDiag.SER_PATH_WRITE
            : PageSectionDiag.SER_PATH_SNAPSHOT, System.nanoTime() - encodeStartNanos,
            sink.writePosition() - encodeStartPosition, keyValueLeafPage.getPageKey());
      }
    }

    private static boolean skipsEmptyPipelineIdentityCache(final ResourceConfiguration resourceConfig,
        final BytesOut<?> sink) {
      return resourceConfig.byteHandlePipeline.isEmpty() && !sink.retainsEmptyPipelineIdentityCache();
    }

    private static void writeOverlongEntries(final BytesOut<?> sink, final Map<Long, PageReference> references) {
      var overlongEntriesBitmap = new BitSet(Constants.NDP_NODE_COUNT);
      final var overlongEntriesSortedByKey = references.entrySet().stream().sorted(Map.Entry.comparingByKey()).toList();

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
     * Persist the rare projection-visible companions which could not fit in the main slotted frame.
     * The side bitmap supplies slot ids, so entries stay compact and deterministic in slot order.
     */
    private static void writeOverflowSlotSidecar(final BytesOut<?> sink, final KeyValueLeafPage page) {
      final int declaredCount = page.getSideSlotCount();
      if (declaredCount <= 0) {
        throw new IllegalStateException("overflow-slot sidecar flag requires at least one entry");
      }

      final BitSet bitmap = new BitSet(Constants.NDP_NODE_COUNT);
      long payloadLength = 0L;
      int observedCount = 0;
      for (int slot = 0; slot < Constants.NDP_NODE_COUNT; slot++) {
        if (!page.hasSideSlot(slot)) {
          continue;
        }
        if (page.getSlot(slot) != null) {
          throw new IllegalStateException("slot " + slot + " has both inline and sidecar carriers on page "
              + page.getPageKey());
        }
        final long recordKey = (page.getPageKey() << Constants.NDP_NODE_COUNT_EXPONENT) + slot;
        if (page.getPageReference(recordKey) == null) {
          throw new IllegalStateException("sidecar slot " + slot + " has no overflow reference on page "
              + page.getPageKey());
        }
        final MemorySegment image = page.getSideSlotImage(slot);
        if (image == null) {
          throw new IllegalStateException("sidecar slot " + slot + " has no image on page " + page.getPageKey());
        }
        final long imageLength = image.byteSize();
        if (imageLength <= 0 || imageLength > PageConstants.MAX_RECORD_SIZE) {
          throw new IllegalStateException("sidecar slot " + slot + " has invalid image length " + imageLength);
        }
        final int kindId = page.getSideSlotNodeKindId(slot);
        if (kindId < 0 || kindId > 0xFF) {
          throw new IllegalStateException("sidecar slot " + slot + " has invalid node kind " + kindId);
        }
        if (kindId != 0 && (image.get(ValueLayout.JAVA_BYTE, 0L) & 0xFF) != kindId) {
          throw new IllegalStateException("sidecar slot " + slot + " kind/image mismatch on page "
              + page.getPageKey());
        }
        bitmap.set(slot);
        payloadLength = Math.addExact(payloadLength, 1L + Integer.BYTES + imageLength);
        observedCount++;
      }
      if (observedCount != declaredCount) {
        throw new IllegalStateException("sidecar count drift on page " + page.getPageKey() + ": declared="
            + declaredCount + ", observed=" + observedCount);
      }
      if (payloadLength > Integer.MAX_VALUE) {
        throw new IllegalStateException("sidecar payload is too large: " + payloadLength);
      }

      SerializationType.serializeBitSet(sink, bitmap);
      sink.writeInt(observedCount);
      sink.writeInt((int) payloadLength);
      for (int slot = bitmap.nextSetBit(0); slot >= 0; slot = bitmap.nextSetBit(slot + 1)) {
        final MemorySegment image = page.getSideSlotImage(slot);
        final int imageLength = Math.toIntExact(image.byteSize());
        sink.writeByte((byte) page.getSideSlotNodeKindId(slot));
        sink.writeInt(imageLength);
        sink.writeSegment(image, 0L, imageLength);
      }
    }

    /** Decode sidecar entries directly into page-owned packed native storage. */
    private void readOverflowSlotSidecar(final BytesIn<?> source, final KeyValueLeafPage page,
        final BitSet overlongEntries) {
      final BitSet sideSlots = SerializationType.deserializeBitSet(source);
      final int sideSlotCount = source.readInt();
      final int payloadLength = source.readInt();
      validateOverflowSlotSidecarHeader(page.getPageKey(), sideSlots, sideSlotCount, payloadLength,
          overlongEntries, source.remaining());

      final long payloadStart = source.position();
      final byte[] scratch = overflowSlotScratch.get();
      for (int slot = sideSlots.nextSetBit(0); slot >= 0; slot = sideSlots.nextSetBit(slot + 1)) {
        if (PageLayout.isSlotPopulated(page.getSlottedPage(), slot)) {
          throw new SirixIOException("page " + page.getPageKey() + " slot " + slot
              + " has both inline and overflow-sidecar carriers");
        }
        final int kindId = source.readByte() & 0xFF;
        final int imageLength = source.readInt();
        validateOverflowSlotImageHeader(page.getPageKey(), slot, kindId, imageLength,
            payloadLength - (source.position() - payloadStart));
        source.read(scratch, 0, imageLength);
        if (kindId != 0 && (scratch[0] & 0xFF) != kindId) {
          throw new SirixIOException("page " + page.getPageKey() + " sidecar slot " + slot
              + " kind/image mismatch: declared=" + kindId + ", image=" + (scratch[0] & 0xFF));
        }
        final MemorySegment image = MemorySegment.ofArray(scratch).asSlice(0L, imageLength);
        final long prepared = page.prepareSideSlot(kindId, image, imageLength);
        page.publishSideSlot(slot, prepared);
      }
      final long consumed = source.position() - payloadStart;
      if (consumed != payloadLength) {
        throw new SirixIOException("page " + page.getPageKey() + " sidecar payload length mismatch: declared="
            + payloadLength + ", consumed=" + consumed);
      }
    }

    /**
     * Validate and skip a sidecar during a column-only read, while folding its logical definitions
     * into the fragment bitmap so version reconstruction cannot resurrect an older inline value.
     */
    private int skipOverflowSlotSidecar(final BytesIn<?> source, final long pageKey, final BitSet overlongEntries,
        final long[] logicalSlotBitmap) {
      final BitSet sideSlots = SerializationType.deserializeBitSet(source);
      final int sideSlotCount = source.readInt();
      final int payloadLength = source.readInt();
      validateOverflowSlotSidecarHeader(pageKey, sideSlots, sideSlotCount, payloadLength, overlongEntries,
          source.remaining());

      final long payloadStart = source.position();
      for (int slot = sideSlots.nextSetBit(0); slot >= 0; slot = sideSlots.nextSetBit(slot + 1)) {
        final long mask = 1L << (slot & 63);
        final int word = slot >>> 6;
        if ((logicalSlotBitmap[word] & mask) != 0L) {
          throw new SirixIOException("page " + pageKey + " slot " + slot
              + " has both inline and overflow-sidecar carriers");
        }
        final int kindId = source.readByte() & 0xFF;
        final int imageLength = source.readInt();
        validateOverflowSlotImageHeader(pageKey, slot, kindId, imageLength,
            payloadLength - (source.position() - payloadStart));
        if (kindId == 0) {
          source.skip(imageLength);
        } else {
          final int imageKind = source.readByte() & 0xFF;
          if (imageKind != kindId) {
            throw new SirixIOException("page " + pageKey + " sidecar slot " + slot
                + " kind/image mismatch: declared=" + kindId + ", image=" + imageKind);
          }
          source.skip(imageLength - 1L);
        }
        logicalSlotBitmap[word] |= mask;
      }
      final long consumed = source.position() - payloadStart;
      if (consumed != payloadLength) {
        throw new SirixIOException("page " + pageKey + " sidecar payload length mismatch: declared="
            + payloadLength + ", consumed=" + consumed);
      }
      return sideSlotCount;
    }

    private static void validateOverflowSlotSidecarHeader(final long pageKey, final BitSet sideSlots,
        final int sideSlotCount, final int payloadLength, final BitSet overlongEntries, final long remainingBytes) {
      if (sideSlotCount <= 0 || sideSlots.cardinality() != sideSlotCount) {
        throw new SirixIOException("overflow-sidecar bitmap/count mismatch on page " + pageKey + ": bitmap="
            + sideSlots.cardinality() + ", count=" + sideSlotCount);
      }
      final BitSet missingReferences = (BitSet) sideSlots.clone();
      missingReferences.andNot(overlongEntries);
      if (!missingReferences.isEmpty()) {
        throw new SirixIOException("page " + pageKey + " sidecar slots have no overflow references: "
            + missingReferences);
      }
      if (payloadLength < sideSlotCount * (1 + Integer.BYTES) || payloadLength > remainingBytes) {
        throw new SirixIOException("page " + pageKey + " has invalid overflow-sidecar payload length "
            + payloadLength + " (remaining=" + remainingBytes + ')');
      }
    }

    private static void validateOverflowSlotImageHeader(final long pageKey, final int slot, final int kindId,
        final int imageLength, final long payloadBytesRemaining) {
      if (kindId != 0 && !KeyValueLeafPage.isFusedAnyObjectNamedKindId(kindId)) {
        throw new SirixIOException("page " + pageKey + " sidecar slot " + slot
            + " has unsupported node kind " + kindId);
      }
      if (imageLength <= 0 || imageLength > PageConstants.MAX_RECORD_SIZE
          || imageLength > payloadBytesRemaining) {
        throw new SirixIOException("page " + pageKey + " sidecar slot " + slot
            + " has invalid image length " + imageLength + " (payload remaining=" + payloadBytesRemaining + ')');
      }
    }


    /**
     * Read the offset-table bytes at {@code index} and {@code index + 1} as one unaligned little-endian
     * load: the byte at {@code index} in bits 0-7, its successor in bits 8-15.
     *
     * <p>
     * Profiling a 2 GB ingest put {@code writeEncodedBody} and its callees at roughly a third of
     * application CPU, with a tenth in the method itself — its per-slot loops read the offset table a
     * byte at a time, and each foreign-memory access carries its own session, bounds and alignment
     * checks. Wherever two <em>adjacent</em> entries are wanted, and a field width is always the delta
     * between neighbouring entries, one two-byte load does what two one-byte loads did.
     *
     * <p>
     * Only valid where both bytes are offset-table entries; callers that may be reading the last entry
     * keep the single-byte read, since the successor is then the data length rather than an entry.
     *
     * @param page the slotted page
     * @param index absolute offset of the first of the two entries
     * @return the two entries packed little-endian into the low 16 bits
     */
    private static int offsetTablePair(final MemorySegment page, final long index) {
      return page.get(LE.SHORT, index) & 0xFFFF;
    }

    /**
     * Collect one structural-key field of one record into the parallel arrays its column is built from.
     *
     * <p>
     * Participation is decided by the record's OFFSET TABLE — the field exists, is not the record's
     * last, and the width it implies is sane — never by the decoded value. The reader re-derives the
     * same predicate from the template, which IS that offset table, so the two cannot come apart the
     * day a node legitimately holds {@code NULL_NODE_KEY} in a field it does have.
     *
     * @param fieldIdx the field's index for this record's kind, or -1 when the kind has no such field
     * @param slotNodeKey the record's node key, the base the stored delta is against
     * @param entryIdx the record's rank in populated-bitmap order
     * @param values receives the decoded key, or {@code NULL_NODE_KEY} for a non-participant
     * @param widths receives the stripped width, or 0 for a non-participant
     * @param offs receives the field's offset within the data region
     * @return the stripped width, 0 when the slot does not take part
     */
    private static int collectStructuralKey(final MemorySegment slottedPage, final long recordBase, final int fc,
        final int fieldIdx, final long slotNodeKey, final int entryIdx, final long[] values, final byte[] widths,
        final short[] offs) {
      if (fieldIdx >= 0 && fieldIdx + 1 < fc) {
        // No kind places a structural key last, so the width is always the delta to the next field's
        // offset. Requiring a non-terminal field is what lets the reader re-derive participation from
        // the template alone.
        final int pair = offsetTablePair(slottedPage, recordBase + 1 + fieldIdx);
        final int off = pair & 0xFF;
        final int width = ((pair >>> 8) & 0xFF) - off;
        if (width > 0 && width <= 10) {
          values[entryIdx] =
              DeltaVarIntCodec.decodeDeltaFromSegment(slottedPage, recordBase + 1 + fc + off, slotNodeKey);
          widths[entryIdx] = (byte) width;
          offs[entryIdx] = (short) off;
          return width;
        }
      }
      // Pathological or absent — leave the bytes inline for this slot. The column stays active for the
      // rest of the page and the reader reaches the same verdict from the same offset-table bytes.
      values[entryIdx] = Fixed.NULL_NODE_KEY.getStandardProperty();
      widths[entryIdx] = 0;
      offs[entryIdx] = 0;
      return 0;
    }

    /**
     * Emit the compact-dir + heap bytes with offset-table template dedup + compressed heap. Gracefully
     * falls back to the plain inline-heap path (emitted with a zero-byte {@code templateCount} marker)
     * when dedup doesn't pay (e.g. every record has a unique offset table or records are raw slab bytes
     * without offset-table structure).
     *
     * <p>
     * Wire layout (the deserializer's two branches in {@code KEYVALUELEAFPAGE.deserializePage} are the
     * authoritative reader):
     * 
     * <pre>
     *   byte                templateCount         // 0 = dedup disabled (inline fallback)
     *   if templateCount &gt; 0 (dedup path):
     *     byte              structuralFlags       // bit0 hashElision, bit1 parentKeyColumn,
     *                                             // bit2 pathNodeKeyColumn, bit3 valueElision,
     *                                             // bit4 nameKeyElision, bit5 derivedElision
     *     int               templatePoolBytes
     *     int               compressedLen
     *     byte              codec                 // 0 ZeroRun, 1 LZ4, 2 ByteRun, 3 SirixLZ77
     *     byte[compressedLen] blob — decompresses to, in order:
     *       ushort[populatedCount] compactDir     // BE: 10-bit length + 6-bit node kind
     *       byte[templatePoolBytes] templatePool
     *       byte[populatedCount]    slotTemplateIds
     *       if hashElision:      byte[ceil(N/8)] zeroHashBitmap
     *       if parentKeyColumn:  int len + byte[len]   (StructuralKeyColumnCodec)
     *       if pathNodeKeyColumn:int len + byte[len]
     *       if valueElision:     section (per-slot tuples, or {@link ElisionDeriver}'s derived form
     *                                      when bit5 is set)
     *       if nameKeyElision:   section (per-slot widths, or the derived form when bit5 is set)
     *       byte[onDiskHeapSize] heap
     *   if templateCount == 0 (inline path):
     *     int               compressedLen
     *     byte              codec
     *     byte[compressedLen] blob — decompresses to compactDir + heap
     * </pre>
     * 
     * The smallest-of-codecs bake-off covers the whole blob (compactDir included), not just the heap.
     *
     * @param sink destination byte sink
     * @param slottedPage the slotted-page memory (in-memory format, full offset tables inline)
     * @param populatedCount number of populated slots
     * @param slotKindIds per-slot nodeKindId (length populatedCount)
     * @param slotFieldCounts per-slot offset-table field count, parallel to {@code slotKindIds}
     * @param slotHeapOffs per-slot in-memory heap offsets (length populatedCount)
     * @param slotDataLens per-slot in-memory dataLengths (length populatedCount)
     * @param deweyIdsStored whether record lengths include Dewey-ID bytes and their trailer
     * @param chunkedBody whether the body is chunk-framed ({@link ChunkedBodyConfig}); the staged
     *        sections are identical either way, only the framing around them differs
     * @param fsstDictId the page's FSST dictionary id, hoisted into a chunked body's prefix
     * @param nameKeyRegionPresent whether the page's region table actually carries the name-key region;
     *        name-key elision may only strip what that region can put back
     * @param indexTypeId {@link io.sirix.index.IndexType#getID()} of the page's index type. Read only
     *        by the section diagnostic, which splits the value-elision activation rate by index type —
     *        the activation rate alone cannot tell "this page holds nothing elidable" from "elision
     *        did not pay here", and the two lead to different levers.
     * @param regionTable the page's PAX regions, already built; the derived elision sections re-derive
     *        their per-slot metadata out of it, so the writer needs it to verify that derivation
     *        against what it actually holds. {@code null} when the page has no regions, which is also
     *        when nothing is elidable
     */
    private static void writeEncodedBody(final BytesOut<?> sink, final MemorySegment slottedPage,
        final int populatedCount, final int[] slotKindIds, final byte[] slotFieldCounts, final int[] slotHeapOffs,
        final int[] slotDataLens, final short[] slotBits, final int[] slotRegionAbsIdx, final boolean deweyIdsStored,
        final boolean chunkedBody, final long fsstDictId, final boolean nameKeyRegionPresent, final byte indexTypeId,
        final RegionTable regionTable) {
      final boolean finerDiag = PAGE_SECTION_DIAG;
      final long diagS0 = finerDiag
          ? sink.writePosition()
          : 0L;
      if (populatedCount > 0) {
        final byte[] templatePool = TEMPLATE_POOL_SCRATCH.get();
        final byte[] slotTemplateIds = SLOT_TEMPLATE_IDS_SCRATCH.get();
        final it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap templateMap = TEMPLATE_MAP.get();
        final OffsetTableTemplatePool.BuildResult br = OffsetTableTemplatePool.build(slottedPage, populatedCount,
            slotKindIds, slotHeapOffs, templatePool, slotTemplateIds, templateMap);
        if (br.isDedupEnabled()) {
          final long pageKeyBase = PageLayout.getRecordPageKey(slottedPage) << Constants.NDP_NODE_COUNT_EXPONENT;
          // Pre-scan to compute:
          // 1. hash-elision bitmap + per-slot hash offset
          // 2. parentKey column values + per-slot parentKey width
          //
          // HFT note: we DO NOT read the hash field's 8 bytes as a single
          // JAVA_LONG_UNALIGNED here — records on the heap are byte-aligned
          // at the start but the hash's absolute offset depends on the
          // variable-width fields before it, so unaligned is required.
          final byte[] zeroHashBitmap = SLOT_ZERO_HASH_BITMAP_SCRATCH.get();
          final short[] slotHashOffs = SLOT_HASH_OFFSET_SCRATCH.get();
          final long[] slotNodeKeys = SLOT_NODE_KEY_SCRATCH.get();
          final long[] slotParentKeys = SLOT_PARENT_KEY_SCRATCH.get();
          final byte[] slotParentKeyWidths = SLOT_PARENT_KEY_WIDTH_SCRATCH.get();
          final short[] slotParentKeyOffs = SLOT_PARENT_KEY_OFF_SCRATCH.get();
          final long[] slotRightSibKeys = SLOT_RIGHT_SIB_KEY_SCRATCH.get();
          final byte[] slotRightSibWidths = SLOT_RIGHT_SIB_WIDTH_SCRATCH.get();
          final short[] slotRightSibOffs = SLOT_RIGHT_SIB_OFF_SCRATCH.get();
          final long[] slotLeftSibKeys = SLOT_LEFT_SIB_KEY_SCRATCH.get();
          final byte[] slotLeftSibWidths = SLOT_LEFT_SIB_WIDTH_SCRATCH.get();
          final short[] slotLeftSibOffs = SLOT_LEFT_SIB_OFF_SCRATCH.get();
          final byte[] slotPnkWidths = SLOT_PATH_NODE_KEY_WIDTH_SCRATCH.get();
          final short[] slotPnkOffs = SLOT_PATH_NODE_KEY_OFF_SCRATCH.get();
          final int[] pnkCompactValues = PATH_NODE_KEY_COLUMN_ENABLED
              ? SLOT_PATH_NODE_KEY_VALUES_COMPACT_SCRATCH.get()
              : null;
          final int[] pnkCompactSlots = PATH_NODE_KEY_COLUMN_ENABLED
              ? SLOT_PATH_NODE_KEY_SLOTS_COMPACT_SCRATCH.get()
              : null;
          // Lever 3: value-elision per-slot scratches. Filled for every fused-NUMBER
          // slot during this pre-scan; only consumed when the writer activates
          // value-elision (all fused-NUMBER slots eligible AND net savings > 0).
          final byte[] slotValueElided = SLOT_VALUE_ELIDED_SCRATCH.get();
          // The wire type byte, kept apart from the internal marker: the derived section compares it
          // against what it re-derives, and the legacy section writes it verbatim.
          final byte[] slotValueDiskTypes = SLOT_VALUE_DISK_TYPE_SCRATCH.get();
          final short[] slotValueOffs = SLOT_VALUE_OFF_SCRATCH.get();
          final short[] slotValueWidths = SLOT_VALUE_WIDTH_SCRATCH.get();
          final long[] slotValueLongs = SLOT_VALUE_LONG_SCRATCH.get();
          // Diagnostic-only mirror of the payload width, written for EVERY fused-primitive slot; the
          // production array above holds a width only for the slots elision actually covers. Null
          // unless the section diagnostic is on, so the disabled path acquires no ThreadLocal.
          final short[] slotDiagValueWidths = finerDiag
              ? SLOT_DIAG_VALUE_WIDTH_SCRATCH.get()
              : null;
          // Lever 4: the region is the authority for whether name-key elision is safe. It has
          // already seen every primitive AND structural fused name and refuses the page if its
          // 255-entry dictionary cannot represent them. Do not rebuild that dictionary from the
          // primitive slots here: besides being only a proxy for the real decision, the duplicate
          // varint decode was the hottest line in the retained serializer profile. Pages without
          // the region (including XML pages) do not touch these thread-local arrays at all.
          final boolean nameKeyElisionCandidate = NAME_KEY_ELISION_ENABLED && nameKeyRegionPresent;
          final byte[] slotNameKeyElided = nameKeyElisionCandidate
              ? SLOT_NAME_KEY_ELIDED_SCRATCH.get()
              : null;
          final short[] slotNameKeyOffs = nameKeyElisionCandidate
              ? SLOT_NAME_KEY_OFF_SCRATCH.get()
              : null;
          final byte[] slotNameKeyWidths = nameKeyElisionCandidate
              ? SLOT_NAME_KEY_WIDTH_SCRATCH.get()
              : null;
          final int hashBitmapBytes = HASH_ELISION_ENABLED
              ? ((populatedCount + 7) >>> 3)
              : 0;
          int zeroHashCount = 0;
          int parentKeySlotsWithField = 0;
          int parentKeyTotalStrippedBytes = 0;
          int rightSibSlotsWithField = 0;
          int rightSibTotalStrippedBytes = 0;
          int leftSibSlotsWithField = 0;
          int leftSibTotalStrippedBytes = 0;
          int pnkSlotsWithField = 0;
          int pnkCompactCount = 0;
          int pnkTotalStrippedBytes = 0;
          int fusedNumberSlotCount = 0;
          int fusedStringSlotCount = 0;
          int fusedBooleanSlotCount = 0;
          int valueElidableSlotCount = 0;
          int valueElidableTotalBytes = 0;
          int valueElisionWireBytes = 0;
          int previousValueElidedSlot = -1;
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
            Arrays.fill(slotValueElided, 0, populatedCount, (byte) 0);
          }
          if (nameKeyElisionCandidate) {
            // Lever 4: clear stale per-slot nameKey-elision flags from previous
            // pages. Same rationale as the value-elision clear above — the
            // offs/widths arrays are overwritten unconditionally below.
            Arrays.fill(slotNameKeyElided, 0, populatedCount, (byte) 0);
          }
          // The directory data length collected by the bitmap pass is already the record-only
          // length when Dewey IDs are disabled. The page object supplies the page-wide flag, so the
          // dominant case avoids rereading both the header flag and the directory entry for every
          // fused primitive.
          for (int i = 0; i < populatedCount; i++) {
            final int kindId = slotKindIds[i];
            final int fc = slotFieldCounts[i];
            final long recordBase = PageLayout.HEAP_START + slotHeapOffs[i];
            // The structural-key column needs this slot's node key both to decode the
            // delta-varint it is replacing and as predictor context for the column codec.
            final long slotNodeKey = pageKeyBase + (slotBits[i] & 0xFFFF);
            slotNodeKeys[i] = slotNodeKey;
            // --- hash elision scan ---
            if (HASH_ELISION_ENABLED) {
              final int hashFieldIdx = NodeFieldLayout.hashFieldIndexForKind(kindId);
              if (hashFieldIdx < 0) {
                slotHashOffs[i] = -1;
              } else {
                final int hashOffInData = slottedPage.get(ValueLayout.JAVA_BYTE, recordBase + 1 + hashFieldIdx) & 0xFF;
                slotHashOffs[i] = (short) hashOffInData;
                final long hashAbsOff = recordBase + 1 + fc + hashOffInData;
                final long h = slottedPage.get(LE.LONG, hashAbsOff);
                if (h == 0L) {
                  zeroHashBitmap[i >>> 3] |= (byte) (1 << (i & 7));
                  zeroHashCount++;
                }
              }
            }
            // --- structural-key column scans (parentKey, right sibling, left sibling) ---
            // Three columns, one predicate. In DFS order a right sibling is usually the very next slot
            // and a left sibling the previous one, so both compress to a couple of bits per slot under
            // the same codec the parentKey column already uses.
            if (PARENT_KEY_COLUMN_ENABLED) {
              final int pkWidth = collectStructuralKey(slottedPage, recordBase, fc,
                  NodeFieldLayout.parentKeyFieldIndexForKind(kindId), slotNodeKey, i, slotParentKeys,
                  slotParentKeyWidths, slotParentKeyOffs);
              if (pkWidth > 0) {
                parentKeySlotsWithField++;
                parentKeyTotalStrippedBytes += pkWidth;
              }
            }
            if (SIBLING_KEY_COLUMNS_ENABLED) {
              final int rightWidth = collectStructuralKey(slottedPage, recordBase, fc,
                  NodeFieldLayout.rightSiblingKeyFieldIndexForKind(kindId), slotNodeKey, i, slotRightSibKeys,
                  slotRightSibWidths, slotRightSibOffs);
              if (rightWidth > 0) {
                rightSibSlotsWithField++;
                rightSibTotalStrippedBytes += rightWidth;
              }
              final int leftWidth = collectStructuralKey(slottedPage, recordBase, fc,
                  NodeFieldLayout.leftSiblingKeyFieldIndexForKind(kindId), slotNodeKey, i, slotLeftSibKeys,
                  slotLeftSibWidths, slotLeftSibOffs);
              if (leftWidth > 0) {
                leftSibSlotsWithField++;
                leftSibTotalStrippedBytes += leftWidth;
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
                slotPnkWidths[i] = 0;
                slotPnkOffs[i] = 0;
              } else {
                final int pnkOff;
                final int nextOff;
                if (pnkFieldIdx + 1 < fc) {
                  final int pnkPair = offsetTablePair(slottedPage, recordBase + 1 + pnkFieldIdx);
                  pnkOff = pnkPair & 0xFF;
                  nextOff = (pnkPair >>> 8) & 0xFF;
                } else {
                  pnkOff = slottedPage.get(ValueLayout.JAVA_BYTE, recordBase + 1 + pnkFieldIdx) & 0xFF;
                  nextOff = slotDataLens[i] - 1 - fc;
                }
                final int pnkWidth = nextOff - pnkOff;
                final long decodedPnk = pnkWidth > 0 && pnkWidth <= 10
                    ? DeltaVarIntCodec.decodeDeltaFromSegment(slottedPage, recordBase + 1 + fc + pnkOff, slotNodeKey)
                    : 0L;
                // A non-positive pathNodeKey — the "no path summary" sentinel — may not enter the
                // column: the reader's only way back is
                // PathNodeKeyRegion#pathNodeKeyForSlot, whose -1 means "this slot has no entry", so a
                // stored -1 would read as absent and the writer's stripped varint would never be put
                // back. Keeping those slots inline is what makes the two sides' participation rules
                // the same predicate rather than two that happen to agree.
                if (pnkWidth <= 0 || pnkWidth > 10 || decodedPnk <= 0L) {
                  // Pathological — keep the width at zero, which the reader interprets as
                  // "no pathNodeKey" for this slot. The column may still activate for the rest of
                  // the page.
                  slotPnkWidths[i] = 0;
                  slotPnkOffs[i] = 0;
                } else {
                  final long pnk = decodedPnk;
                  slotPnkWidths[i] = (byte) pnkWidth;
                  slotPnkOffs[i] = (short) pnkOff;
                  pnkSlotsWithField++;
                  pnkTotalStrippedBytes += pnkWidth;
                  // Preserve the old compact-column order exactly: the main scan is already in
                  // ascending bitmap order, so appending here produces the same value/slot arrays
                  // the removed follow-up scan did.
                  pnkCompactValues[pnkCompactCount] = (int) pnk;
                  pnkCompactSlots[pnkCompactCount] = slotBits[i] & 0xFFFF;
                  pnkCompactCount++;
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
            if (VALUE_ELISION_ENABLED && (kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_NUMBER_KIND_ID
                || kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_STRING_KIND_ID
                || kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_BOOLEAN_KIND_ID)) {
              final int valueOff =
                  slottedPage.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDNUM_PAYLOAD) & 0xFF;
              final int recordOnlyLen = deweyIdsStored
                  ? PageLayout.getRecordOnlyLength(slottedPage, slotBits[i] & 0xFFFF)
                  : slotDataLens[i];
              final int dataBytes = recordOnlyLen - 1 - fc;
              final int valueWidth = dataBytes - valueOff;
              if (slotDiagValueWidths != null) {
                slotDiagValueWidths[i] = valueWidth > 0 && valueWidth <= Short.MAX_VALUE
                    ? (short) valueWidth
                    : 0;
              }
              if (kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_NUMBER_KIND_ID) {
                fusedNumberSlotCount++;
                if (valueWidth > 0 && valueWidth <= 11) {
                  // Read the type byte; INTEGER=2 / LONG=3 are the only kinds we
                  // can elide (Float/Double/BigDecimal can't be reduced to a long
                  // and stay inline).
                  final byte typeByte = slottedPage.get(ValueLayout.JAVA_BYTE, recordBase + 1 + fc + valueOff);
                  if (typeByte == NUMBER_TYPE_INTEGER || typeByte == NUMBER_TYPE_LONG) {
                    // Decode the long value; handles both INTEGER and LONG variants.
                    final long longVal = (typeByte == NUMBER_TYPE_INTEGER)
                        ? DeltaVarIntCodec.decodeSignedFromSegment(slottedPage, recordBase + 1 + fc + valueOff + 1)
                        : DeltaVarIntCodec.decodeSignedLongFromSegment(slottedPage, recordBase + 1 + fc + valueOff + 1);
                    // Defensive: skip Long.MIN_VALUE (the sentinel buildRegionTable
                    // uses to tag "not long-decodable"). Storing it would create a
                    // mismatch between elision-pass count and region count. This
                    // only triggers for the pathological LONG=Long.MIN_VALUE case;
                    // INTEGER cannot equal Long.MIN_VALUE since it's outside int range.
                    if (longVal != Long.MIN_VALUE && slotRegionAbsIdx[slotBits[i] & 0xFFFF] >= 0) {
                      slotValueElided[i] = (byte) (typeByte & 0x7F); // store type in low bits, never 0
                      slotValueOffs[i] = (short) valueOff;
                      slotValueWidths[i] = (short) valueWidth;
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
                if (valueWidth > 0 && valueWidth <= 0xFFFF) {
                  // Both stored forms elide: the region dictionary mirrors the heap's stored
                  // bytes verbatim (raw or FSST-encoded), so re-injection is a straight copy
                  // either way. The flag must survive the trip — it rides the wire entry's type
                  // byte, chosen here via the marker.
                  final byte compressedFlag = slottedPage.get(ValueLayout.JAVA_BYTE, recordBase + 1 + fc + valueOff);
                  final long lenAbsOff = recordBase + 1 + fc + valueOff + 1;
                  final int strLen = DeltaVarIntCodec.decodeSignedFromSegment(slottedPage, lenAbsOff);
                  if (strLen > 0 && (compressedFlag == 0 || compressedFlag == 1)
                      && slotRegionAbsIdx[slotBits[i] & 0xFFFF] >= 0) {
                    slotValueElided[i] = compressedFlag == 1
                        ? STRING_ELIDE_COMPRESSED_MARKER
                        : STRING_ELIDE_MARKER;
                    slotValueOffs[i] = (short) valueOff;
                    slotValueWidths[i] = (short) valueWidth;
                    valueElidableSlotCount++;
                    valueElidableTotalBytes += valueWidth;
                  }
                }
              } else {
                // FUSED_OBJECT_NAMED_BOOLEAN — width is always 1 (a single bool byte). The
                // region-index requirement also subsumes the old 256-distinct-tag cap: when
                // BooleanRegion.encode rejects a page, no boolean slot holds an index.
                fusedBooleanSlotCount++;
                if (valueWidth == 1 && slotRegionAbsIdx[slotBits[i] & 0xFFFF] >= 0) {
                  slotValueElided[i] = BOOLEAN_ELIDE_MARKER;
                  slotValueOffs[i] = (short) valueOff;
                  slotValueWidths[i] = (short) 1;
                  valueElidableSlotCount++;
                  valueElidableTotalBytes += 1;
                }
              }
              final byte elideMarker = slotValueElided[i];
              if (elideMarker != 0) {
                final int slot = slotBits[i] & 0xFFFF;
                // NUMBER carries its 2/3 subtype; STRING carries its stored-form flag (0 raw, 1 FSST)
                // so injection restores the exact heap byte; BOOLEAN carries 0.
                slotValueDiskTypes[i] = elideMarker == STRING_ELIDE_COMPRESSED_MARKER
                    ? (byte) 1
                    : (elideMarker == STRING_ELIDE_MARKER || elideMarker == BOOLEAN_ELIDE_MARKER
                        ? (byte) 0
                        : elideMarker);
                valueElisionWireBytes += DeltaVarIntCodec.computeSignedEncodedWidth(slot - previousValueElidedSlot) + 1
                    + DeltaVarIntCodec.computeSignedEncodedWidth(slotValueWidths[i] & 0xFFFF)
                    + DeltaVarIntCodec.computeSignedEncodedWidth(slotRegionAbsIdx[slot]);
                previousValueElidedSlot = slot;
              }
            }
            // --- Lever 4: NAME-KEY elision pre-scan (fused OBJECT_NAMED_*, kindIds 48-51) ---
            // The actual region-presence decision was made by buildRegionTable over every fused
            // object-key role. Once present, only this slot's offset and width are needed. All four
            // primitive-fused layouts put nameKey at field 3, so one adjacent-pair load replaces
            // both the old two-byte reads and the duplicate signed-varint decode/dictionary scan.
            if (nameKeyElisionCandidate && KeyValueLeafPage.isFusedObjectNamedKindId(kindId)) {
              totalFusedNamedSlotCount++;
              final int offsetPair =
                  offsetTablePair(slottedPage, recordBase + 1 + NodeFieldLayout.FUSED_PRIMITIVE_NAME_KEY_FIELD);
              final int nameKeyOff = offsetPair & 0xFF;
              final int nameKeyWidth = ((offsetPair >>> 8) & 0xFF) - nameKeyOff;
              // signed-varint width is 1..5 for any 32-bit nameKey value. A malformed width keeps
              // the page on the inline path via the all-or-nothing count check below.
              if (nameKeyWidth >= 1 && nameKeyWidth <= 5) {
                slotNameKeyElided[i] = (byte) 1;
                slotNameKeyOffs[i] = (short) nameKeyOff;
                slotNameKeyWidths[i] = (byte) nameKeyWidth;
                nameKeyElidableSlotCount++;
                nameKeyElidableTotalBytes += nameKeyWidth;
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
          if (PARENT_KEY_COLUMN_ENABLED && parentKeySlotsWithField > 0) {
            // Encode straight into the scratch and judge from the length it returns. The
            // column encodes N values (including a sentinel for slots without a parentKey), so
            // its length is a function of all values, not just those with the field. It must
            // come out strictly smaller than the varints it displaces plus its own 4-byte
            // length prefix; otherwise the bytes stay inline and the scratch is simply not
            // used. Encoding speculatively costs one pass over the column and saves the
            // separate sizing pass that would have had to re-pick the same format.
            byte[] scratch = PARENT_KEY_COLUMN_SCRATCH.get();
            final int maxLen = StructuralKeyColumnCodec.maxEncodedSize(populatedCount);
            if (scratch.length < maxLen) {
              scratch = new byte[maxLen];
              PARENT_KEY_COLUMN_SCRATCH.set(scratch);
            }
            final int encodedLen =
                StructuralKeyColumnCodec.encodeByteArray(scratch, 0, slotParentKeys, populatedCount, slotNodeKeys);
            if (finerDiag) {
              PageSectionDiag.recordParentKeyColumnCandidate(parentKeyTotalStrippedBytes, encodedLen);
            }
            if (encodedLen + 4 < parentKeyTotalStrippedBytes) {
              parentKeyColumnBytes = scratch;
              parentKeyColumnLen = encodedLen;
              if (finerDiag) {
                PageSectionDiag.recordParentKeyColumn(parentKeyTotalStrippedBytes - encodedLen - 4);
              }
            }
          }
          final boolean parentKeyColumnActive = parentKeyColumnBytes != null;

          // The two sibling columns, judged exactly as the parentKey column is: encode into the
          // scratch, keep it only when the bytes it takes — plus its own length prefix — come out
          // under the varints it displaces.
          byte[] rightSibColumnBytes = null;
          int rightSibColumnLen = 0;
          if (SIBLING_KEY_COLUMNS_ENABLED && rightSibSlotsWithField > 0) {
            final byte[] rightScratch = structuralColumnScratch(RIGHT_SIB_COLUMN_SCRATCH, populatedCount);
            final int encodedLen =
                StructuralKeyColumnCodec.encodeByteArray(rightScratch, 0, slotRightSibKeys, populatedCount,
                    slotNodeKeys);
            if (finerDiag) {
              PageSectionDiag.recordRightSibKeyColumnCandidate(rightSibTotalStrippedBytes, encodedLen);
            }
            if (encodedLen + 4 < rightSibTotalStrippedBytes) {
              rightSibColumnBytes = rightScratch;
              rightSibColumnLen = encodedLen;
              if (finerDiag) {
                PageSectionDiag.recordRightSibKeyColumn(rightSibTotalStrippedBytes - encodedLen - 4);
              }
            }
          }
          final boolean rightSibColumnActive = rightSibColumnBytes != null;

          byte[] leftSibColumnBytes = null;
          int leftSibColumnLen = 0;
          if (SIBLING_KEY_COLUMNS_ENABLED && leftSibSlotsWithField > 0) {
            final byte[] leftScratch = structuralColumnScratch(LEFT_SIB_COLUMN_SCRATCH, populatedCount);
            final int encodedLen =
                StructuralKeyColumnCodec.encodeByteArray(leftScratch, 0, slotLeftSibKeys, populatedCount,
                    slotNodeKeys);
            if (finerDiag) {
              PageSectionDiag.recordLeftSibKeyColumnCandidate(leftSibTotalStrippedBytes, encodedLen);
            }
            if (encodedLen + 4 < leftSibTotalStrippedBytes) {
              leftSibColumnBytes = leftScratch;
              leftSibColumnLen = encodedLen;
              if (finerDiag) {
                PageSectionDiag.recordLeftSibKeyColumn(leftSibTotalStrippedBytes - encodedLen - 4);
              }
            }
          }
          final boolean leftSibColumnActive = leftSibColumnBytes != null;

          // pathNodeKey column is active iff at least one slot has a pathNodeKey
          // field AND the dict encoding pays off vs. raw delta-varints. Unlike
          // parentKey (a wide pile of independent node keys), pathNodeKey typically
          // has 3-10 distinct values across 1000+ slots in record-oriented JSON
          // workloads — perfect for dict encoding.
          byte[] pathNodeKeyColumnBytes = null;
          int pathNodeKeyColumnLen = 0;
          // The bytes the writer LOOKS UP through, always in the legacy random-access layout even when
          // the column goes to the wire compacted — the derivation reads a tag per elided slot and must
          // not pay a decode for it.
          byte[] pathNodeKeyLookupBytes = null;
          if (PATH_NODE_KEY_COLUMN_ENABLED && pnkSlotsWithField > 0) {
            final int[] dictScratch = PNK_ENCODE_DICT_SCRATCH.get();
            // Encode once, then judge the exact returned length. encodedSize used to build the
            // same dictionary immediately before encode rebuilt it; the output and dict-id
            // scratches are already sized for the largest possible record page.
            final byte[] scratch = PATH_NODE_KEY_COLUMN_SCRATCH.get();
            final byte[] dictIdsScratch = PNK_ENCODE_DICT_IDS_SCRATCH.get();
            final int written = PathNodeKeyRegion.encode(pnkCompactValues, pnkCompactSlots, pnkCompactCount, scratch,
                dictScratch, dictIdsScratch, COLUMN_ENCODE_BITMAP_SCRATCH.get());
            // The legacy layout spends four bytes on every dictionary key and one on every slot's id.
            // Both are laid out for random access rather than for size, and both are re-derivable: the
            // keys from a frame of reference, the id lane from its own runs. Measured, not assumed —
            // the compact form is kept only when it comes out smaller for THIS page.
            byte[] emittedBytes = scratch;
            int emittedLen = written;
            if (written > 0 && PATH_NODE_KEY_COLUMN_COMPACT && DERIVED_ELISION_SECTIONS) {
              byte[] compactScratch = PATH_NODE_KEY_COMPACT_SCRATCH.get();
              if (compactScratch.length < written) {
                compactScratch = new byte[written];
                PATH_NODE_KEY_COMPACT_SCRATCH.set(compactScratch);
              }
              final int compactLen = PathNodeKeyRegion.compact(scratch, written, compactScratch);
              if (compactLen > 0) {
                emittedBytes = compactScratch;
                emittedLen = compactLen;
              }
            }
            if (emittedLen > 0 && emittedLen + 4 < pnkTotalStrippedBytes) {
              pathNodeKeyColumnBytes = emittedBytes;
              pathNodeKeyColumnLen = emittedLen;
              pathNodeKeyLookupBytes = scratch;
            }
          }
          final boolean pathNodeKeyColumnActive = pathNodeKeyColumnBytes != null;

          // Lever 3: VALUE elision is per slot, not per page — a page with N numbers, M strings and a
          // float elides the N + M whose value a region can put back and leaves the float inline. It
          // activates whenever the bytes those slots hand to the regions beat what naming them costs.
          //
          // The two forms cost very different things. The per-slot tuples cost four to five bytes each
          // (slot-gap varint, type byte, width varint, region-index varint), which the pre-scan summed
          // as it walked. The derived form costs a flag byte, one BIT per populated slot, and an entry
          // only where the derivation is wrong — a size that is not a per-slot accumulation at all and
          // that depends on the pathNodeKey column's activation, since that column is the tag source a
          // PATH_NODE-tagged region's index derivation reads. So it is planned here, after that
          // decision, by running the READER's own derivation over the page about to be written and
          // recording an exception wherever the two disagree: the size comes out exact, and so does the
          // round trip.
          final ElisionDeriver elisionDeriver = DERIVED_ELISION_SECTIONS
              ? WRITER_ELISION_DERIVER.get()
              : null;
          if (elisionDeriver != null) {
            elisionDeriver.bind(regionTable, pathNodeKeyColumnActive
                ? pathNodeKeyLookupBytes
                : null);
          }
          final boolean derivedValueSection = elisionDeriver != null && VALUE_ELISION_ENABLED
              && valueElidableSlotCount > 0;
          final int valueElisionSectionBytes = derivedValueSection
              ? elisionDeriver.planValueSection(populatedCount, slotKindIds, slotBits, slotValueElided,
                  slotValueDiskTypes, slotValueWidths, slotRegionAbsIdx, valueElidableSlotCount,
                  fusedNumberSlotCount + fusedStringSlotCount + fusedBooleanSlotCount)
              : 4 + valueElisionWireBytes;
          final boolean valueElisionActive = VALUE_ELISION_ENABLED && valueElidableSlotCount > 0
              && valueElidableTotalBytes > valueElisionSectionBytes;

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
          // The region is the only place a stripped name key can come back from, so its presence is
          // the condition — not a locally rebuilt proxy. The region encoder sees primitive and
          // structural fused names together and enforces its own 255-value ceiling before this point.
          // The name-key widths are derived from the very region the injection pass reads back, so a
          // page whose region did not survive its encoder cannot use the derived form — and, since
          // that injection already refuses an absent region, must not elide name keys at all.
          final boolean derivedNameKeySection = elisionDeriver != null && elisionDeriver.hasNameKeyRegion();
          final int nameKeyElisionSectionBytes =
              derivedNameKeySection && nameKeyElisionCandidate && nameKeyElidableSlotCount > 0
                  ? elisionDeriver.planNameKeySection(populatedCount, slotBits, slotNameKeyElided, slotNameKeyWidths)
                  : 4 + nameKeyElidableSlotCount;
          final boolean nameKeyElisionActive = nameKeyElisionCandidate && nameKeyElidableSlotCount > 0
              && nameKeyElidableSlotCount == totalFusedNamedSlotCount
              && nameKeyElidableTotalBytes > nameKeyElisionSectionBytes
              && (elisionDeriver == null || derivedNameKeySection);
          // One page-level bit governs both sections: a page writes them in the derived form or in the
          // old tuple form, never one of each, so a reader parses both under the same rule.
          final boolean derivedElisionSections = elisionDeriver != null;
          if (finerDiag) {
            if (valueElisionActive) {
              PageSectionDiag.recordValueElision(valueElidableTotalBytes - (long) valueElisionSectionBytes);
            }
            if (nameKeyElisionActive) {
              PageSectionDiag.recordNameKeyElision(nameKeyElidableTotalBytes - (long) nameKeyElisionSectionBytes);
            }
          }

          // Compute on-disk heap size: for each record, replace its FIELD_COUNT bytes of
          // offset table with a single templateId byte. In-memory = 1 (kindId) + FC + D;
          // on-disk = 1 (kindId) + 1 (templateId) + D = in-memory - (FC - 1).
          // When hash elision is active, also strip 8 bytes per zero-hash slot.
          // When parentKey column is active, strip the slot's parentKeyWidth bytes.
          // When pathNodeKey column is active, strip the slot's pnkWidth bytes.
          // When value elision is active, strip the slot's value field bytes
          // (1 type byte + delta-varint payload).
          // When name-key elision is active, strip the slot's nameKey varint bytes.
          // The per-slot on-disk length is needed twice — to size the heap here, and again to
          // pack each compact-dir entry below. Computing it once and stashing it saves a second
          // full pass whose per-slot work is a kind switch plus five conditional subtractions.
          final int[] slotOnDiskLens = SLOT_ON_DISK_LEN_SCRATCH.get();
          int onDiskHeapSize = 0;
          for (int i = 0; i < populatedCount; i++) {
            final int fc = slotFieldCounts[i];
            int onDiskLen = slotDataLens[i] - (fc - 1);
            if (onDiskLen < 2) {
              // A record that's shorter than kindId+templateId means the in-memory layout
              // is inconsistent. Fall back to inline to avoid corrupting disk bytes.
              writeInlineBody(sink, slottedPage, populatedCount, slotKindIds, slotHeapOffs, slotDataLens, chunkedBody,
                  fsstDictId);
              if (finerDiag) {
                PageSectionDiag.recordInlineBodyPage(indexTypeId, PageSectionDiag.INLINE_REASON_SHORT_RECORD);
                PageSectionDiag.recordEncodedBody(0L, 0L, sink.writePosition() - diagS0);
              }
              return;
            }
            if (hashElisionActive && ((zeroHashBitmap[i >>> 3] >>> (i & 7)) & 1) == 1) {
              onDiskLen -= NodeFieldLayout.HASH_WIDTH;
            }
            if (parentKeyColumnActive) {
              onDiskLen -= slotParentKeyWidths[i] & 0xFF;
            }
            if (rightSibColumnActive) {
              onDiskLen -= slotRightSibWidths[i] & 0xFF;
            }
            if (leftSibColumnActive) {
              onDiskLen -= slotLeftSibWidths[i] & 0xFF;
            }
            if (pathNodeKeyColumnActive) {
              onDiskLen -= slotPnkWidths[i] & 0xFF;
            }
            if (valueElisionActive && slotValueElided[i] != 0) {
              onDiskLen -= slotValueWidths[i] & 0xFFFF;
            }
            if (nameKeyElisionActive && slotNameKeyElided[i] != 0) {
              onDiskLen -= slotNameKeyWidths[i] & 0xFF;
            }
            slotOnDiskLens[i] = onDiskLen;
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
          // bit 2 = pathNodeKey column, bit 3 = value elision, bit 4 = name-key elision,
          // bit 5 = the two elision sections carry their derived form.
          int structuralFlags = 0;
          if (hashElisionActive)
            structuralFlags |= STRUCT_FLAG_HASH_ELISION;
          if (parentKeyColumnActive)
            structuralFlags |= STRUCT_FLAG_PARENT_KEY_COLUMN;
          if (pathNodeKeyColumnActive)
            structuralFlags |= STRUCT_FLAG_PATH_NODE_KEY_COLUMN;
          if (valueElisionActive)
            structuralFlags |= STRUCT_FLAG_VALUE_ELISION;
          if (nameKeyElisionActive)
            structuralFlags |= STRUCT_FLAG_NAME_KEY_ELISION;
          if (derivedElisionSections && (valueElisionActive || nameKeyElisionActive))
            structuralFlags |= STRUCT_FLAG_DERIVED_ELISION;
          int extendedFlags = 0;
          if (rightSibColumnActive)
            extendedFlags |= EXT_FLAG_RIGHT_SIB_COLUMN;
          if (leftSibColumnActive)
            extendedFlags |= EXT_FLAG_LEFT_SIB_COLUMN;
          if (extendedFlags != 0)
            structuralFlags |= STRUCT_FLAG_EXTENDED;
          sink.writeByte((byte) structuralFlags);
          if (extendedFlags != 0) {
            sink.writeByte((byte) extendedFlags);
          }
          sink.writeInt(br.templatesByteLength);

          // Stage ALL structural metadata + heap into one contiguous buffer so the
          // zero-run RLE encoder sees cross-section patterns (repeated compact-dir
          // entries across pages + repeated offset-table templates compress together
          // with record bodies). This matches what the outer full-page LZ4 used to
          // catch and lets us drop that outer pass entirely.
          //
          // Staging layout:
          // compactDir bytes (2 × populatedCount)
          // templatePool bytes (br.templatesByteLength)
          // slotTemplateIds (populatedCount bytes)
          // [if hashElisionActive] zeroHashBitmap (hashBitmapBytes)
          // [if parentKeyColumnActive] int columnLen + column bytes
          // [if pathNodeKeyColumnActive] int columnLen + column bytes
          // [if valueElisionActive] int valueTypesLen + valueTypeBytes
          // [if nameKeyElisionActive] int elidedCount + per-slot 1-byte widths
          // heap bytes (onDiskHeapSize, possibly reduced per slot)
          //
          // HFT-grade: staging buffer is thread-local and grows in powers of two
          // until steady-state; after warm-up every shred is allocation-free.
          final int compactDirBytes = PageLayout.COMPACT_DIR_ENTRY_SIZE * populatedCount;
          final int stagedHashBitmapBytes = hashElisionActive
              ? hashBitmapBytes
              : 0;
          final int stagedParentKeyColBytes = parentKeyColumnActive
              ? (4 + parentKeyColumnLen)
              : 0;
          final int stagedRightSibColBytes = rightSibColumnActive
              ? (4 + rightSibColumnLen)
              : 0;
          final int stagedLeftSibColBytes = leftSibColumnActive
              ? (4 + leftSibColumnLen)
              : 0;
          final int stagedPathNodeKeyColBytes = pathNodeKeyColumnActive
              ? (4 + pathNodeKeyColumnLen)
              : 0;
          // Exactly what the section will occupy: the derived form's flag byte, bitmap and exception
          // lists as the plan measured them, or the per-slot tuples pre-summed by the pre-scan.
          final int stagedValueElisionBytes = valueElisionActive
              ? valueElisionSectionBytes
              : 0;
          final int stagedNameKeyElisionBytes = nameKeyElisionActive
              ? nameKeyElisionSectionBytes
              : 0;
          final int structuralBytes = compactDirBytes + br.templatesByteLength + populatedCount + stagedHashBitmapBytes
              + stagedParentKeyColBytes + stagedRightSibColBytes + stagedLeftSibColBytes + stagedPathNodeKeyColBytes
              + stagedValueElisionBytes + stagedNameKeyElisionBytes;
          final int totalStagingBytes = structuralBytes + onDiskHeapSize;
          if (finerDiag) {
            // The staged sizes are exactly what the writer is about to emit, so the diagnostic reports
            // the metadata each lever COSTS beside the bytes it saves. The heap fold runs over the
            // same per-slot arrays the sizing loop just filled, so it needs no second walk of the page.
            recordHeapCompositionDiag(populatedCount, slotKindIds, slotOnDiskLens, slotDiagValueWidths,
                slotValueElided, valueElisionActive);
            PageSectionDiag.recordStagedElisionMetadata(stagedValueElisionBytes, stagedNameKeyElisionBytes,
                stagedHashBitmapBytes, stagedParentKeyColBytes, stagedPathNodeKeyColBytes);
            PageSectionDiag.recordEncodedBodyOutcome(indexTypeId, valueElisionActive,
                fusedNumberSlotCount + fusedStringSlotCount + fusedBooleanSlotCount > 0);
          }
          final MemorySegment staging = v1StagingScratch(totalStagingBytes);
          final BodySections sections = BODY_SECTIONS.get();
          sections.begin(staging);
          sections.appendCompactDir(slotOnDiskLens, slotKindIds, populatedCount);
          if (br.templatesByteLength > 0) {
            sections.appendTemplatePool(templatePool, br.templatesByteLength);
          }
          if (populatedCount > 0) {
            sections.appendSlotTemplateIds(slotTemplateIds, populatedCount);
          }
          if (hashElisionActive) {
            sections.appendZeroHashBitmap(zeroHashBitmap, hashBitmapBytes);
          }
          if (parentKeyColumnActive) {
            sections.appendParentKeyColumn(parentKeyColumnBytes, parentKeyColumnLen);
          }
          if (rightSibColumnActive) {
            sections.appendRightSibKeyColumn(rightSibColumnBytes, rightSibColumnLen);
          }
          if (leftSibColumnActive) {
            sections.appendLeftSibKeyColumn(leftSibColumnBytes, leftSibColumnLen);
          }
          if (pathNodeKeyColumnActive) {
            sections.appendPathNodeKeyColumn(pathNodeKeyColumnBytes, pathNodeKeyColumnLen);
          }
          if (valueElisionActive) {
            if (derivedElisionSections) {
              sections.appendDerivedValueElision(elisionDeriver, populatedCount);
            } else {
              sections.appendValueElision(valueElidableSlotCount, populatedCount, slotValueElided, slotBits,
                  slotValueWidths, slotRegionAbsIdx);
            }
          }
          if (nameKeyElisionActive) {
            if (derivedElisionSections) {
              sections.appendDerivedNameKeyElision(elisionDeriver);
            } else {
              sections.appendNameKeyElision(nameKeyElidableSlotCount, populatedCount, slotNameKeyElided,
                  slotNameKeyWidths);
            }
          }
          long stagePos = sections.beginHeap();
          stagePos = stageEncodedHeap(slottedPage, staging, stagePos, populatedCount, slotFieldCounts, slotHeapOffs,
              slotDataLens, slotTemplateIds, hashElisionActive, zeroHashBitmap, slotHashOffs, parentKeyColumnActive,
              slotParentKeyWidths, rightSibColumnActive, slotRightSibWidths, slotRightSibOffs, leftSibColumnActive,
              slotLeftSibWidths, slotLeftSibOffs, pathNodeKeyColumnActive, slotPnkWidths, slotPnkOffs,
              valueElisionActive, slotValueElided, slotValueWidths, slotValueOffs, nameKeyElisionActive,
              slotNameKeyElided, slotNameKeyWidths, slotNameKeyOffs);
          sections.endHeap(stagePos);
          assert sections.totalLength() == totalStagingBytes
              && sections.metaLength() + sections.heapLength() == totalStagingBytes
              : "staged " + sections.totalLength() + " body bytes, sized for " + totalStagingBytes;

          if (chunkedBody) {
            // Same staged bytes, framed apart: the metadata sections become one META frame and the
            // heap is split at entry boundaries, each frame compressed and checksummed on its own.
            emitChunkedFrames(sink, staging, (int) sections.metaLength(), sections.heapLength(), populatedCount,
                slotOnDiskLens, fsstDictId);
            CHUNKED_DEDUP_BODIES_WRITTEN.increment();
            if (finerDiag) {
              final long diagS3 = sink.writePosition();
              PageSectionDiag.recordEncodedBody(compactDirBytes, br.templatesByteLength + populatedCount,
                  diagS3 - diagS0 - 9 /* populatedCount + heapSize + templateCount headers */,
                  sections.metaLength() + sections.heapLength());
            }
            return;
          }

          // Compress the combined staging blob with LZ4 (HC when configured) or
          // ZeroRunByteCodec fallback. Emit: int compressedLen, 1 byte codec,
          // compressed bytes. Reader decompresses once and parses the 4-section
          // blob in order.
          final FFILz4Compressor lz4 = HEAP_LZ4_DISABLED
              ? null
              : V1_HEAP_LZ4.get();
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
            if (finerDiag) {
              recordPostCodecAttribution(staging, sections, compressedLen, populatedCount, slotKindIds,
                  slotOnDiskLens, slotTemplateIds, 1);
            }
          } else {
            // Smallest-of-codecs bake-off with sticky-winner election —
            // shared with the inline path, see emitSmallestBody.
            final long beforeBody = finerDiag
                ? sink.writePosition()
                : 0L;
            emitSmallestBody(sink, staging, totalStagingBytes);
            if (finerDiag) {
              // Post-codec attribution, charged under the codec this page actually emitted — which is
              // not always the elected one, since the pages between probes write the smaller of
              // zero-run and LZ77 whatever the election says.
              recordPostCodecAttribution(staging, sections, sink.writePosition() - beforeBody - 5, populatedCount,
                  slotKindIds, slotOnDiskLens, slotTemplateIds, STICKY_CODEC.get()[STICKY_LAST_EMITTED]);
            }
          }
          if (finerDiag) {
            final long diagS3 = sink.writePosition();
            PageSectionDiag.recordEncodedBody(compactDirBytes, // compactDir PRE-compression
                br.templatesByteLength + populatedCount, // templatePool+slotIds PRE-compression
                diagS3 - diagS0 - 9 /* populatedCount + heapSize + templateCount headers */, totalStagingBytes);
          }
          return;
        }
      }
      // Fallback path (also used when dedup aborts).
      writeInlineBody(sink, slottedPage, populatedCount, slotKindIds, slotHeapOffs, slotDataLens, chunkedBody,
          fsstDictId);
      if (finerDiag) {
        final long diagS3 = sink.writePosition();
        PageSectionDiag.recordEncodedBody(0L, 0L, diagS3 - diagS0);
        PageSectionDiag.recordInlineBodyPage(indexTypeId, populatedCount == 0
            ? PageSectionDiag.INLINE_REASON_EMPTY_PAGE
            : PageSectionDiag.INLINE_REASON_TEMPLATE_DEDUP_ABORTED);
      }
    }

    /**
     * DIAGNOSTIC ONLY ({@code -Dsirix.pageSectionDiag=true}): fold one page's staged heap into
     * per-node-kind slots, then emit one {@link PageSectionDiag} call per kind present.
     *
     * <p>
     * The plan's first trie lever needs to know which record kinds spend the body's bytes and how many
     * payload bytes stay inline because value elision did not reach the slot — a page that lost its
     * string region to an overflow descriptor keeps every fused string's bytes in the heap, and that
     * is invisible in a per-page total. Folding per kind first keeps a 1,024-slot page at one
     * {@link java.util.concurrent.atomic.LongAdder} touch per kind rather than one per slot.
     *
     * <p>
     * Zero allocation: the fold rides a thread-local {@code long[]} and is cleared as it is emitted,
     * so it is left zeroed for the next page without a full-array wipe.
     */
    /**
     * Attribute the body's post-codec bytes to the sections that produced them.
     *
     * <p>
     * The body is staged as one contiguous buffer and compressed as one blob, so what any single
     * section costs on disk is not observable from the wire — only the total is. This compresses each
     * staged section ON ITS OWN with the codec the page actually used, which is an upper bound for its
     * share: the whole body is always at least as compressible as the sum of its parts, because the
     * codec also sees repetition ACROSS sections. The gap between the sum and the real body is exactly
     * that cross-section gain, and it is reported rather than hidden.
     *
     * <p>
     * Why it matters: a lever judged on staged bytes can move a kilobyte per page and nothing on disk,
     * because the bytes it removed were the ones the codec was already collapsing. Only an
     * attribution like this can tell the two apart.
     *
     * <p>
     * Diagnostic only, behind {@code -Dsirix.pageSectionDiag=true}, and it re-compresses the body's
     * bytes a further two to three times — never enable it on a measured ingest.
     *
     * <p>
     * Not reached on a chunk-framed body, which compresses its META frame and each chunk separately
     * already — the split it would report is the one it is written with.
     *
     * @param actualBodyBytes what the whole body compressed to, without its length and codec header
     * @param codec the codec the page emitted, so every section is charged what the page really paid
     */
    private static void recordPostCodecAttribution(final MemorySegment staging, final BodySections sections,
        final long actualBodyBytes, final int populatedCount, final int[] slotKindIds, final int[] slotOnDiskLens,
        final byte[] slotTemplateIds, final int codec) {
      long offset = 0;
      long encodedSum = 0;
      for (int section = 0; section <= PageSectionDiag.SECTION_HEAP; section++) {
        final int length = sections.sectionLength(section);
        if (length == 0) {
          continue;
        }
        final int encoded = encodedSizeWithCodec(codec, staging, offset, length);
        PageSectionDiag.recordPostCodecSection(section, length, encoded);
        encodedSum += encoded;
        offset += length;
      }
      PageSectionDiag.recordPostCodecBody(encodedSum, actualBodyBytes);

      // The heap again, split three ways by record kind. The three lanes are PARTS of the heap lane
      // above and are deliberately left out of the sum: a record's bytes must be counted once.
      final long heapStart = sections.heapStart();
      gatherAndRecordHeapClass(staging, heapStart, populatedCount, slotKindIds, slotOnDiskLens, codec,
          HEAP_CLASS_FUSED, PageSectionDiag.SECTION_HEAP_FUSED);
      gatherAndRecordHeapClass(staging, heapStart, populatedCount, slotKindIds, slotOnDiskLens, codec,
          HEAP_CLASS_STRUCTURAL, PageSectionDiag.SECTION_HEAP_STRUCTURAL);
      gatherAndRecordHeapClass(staging, heapStart, populatedCount, slotKindIds, slotOnDiskLens, codec,
          HEAP_CLASS_OTHER, PageSectionDiag.SECTION_HEAP_OTHER);

      // How much of the compact directory a template-implied one could drop (T1-b): an entry is
      // predictable when its kind AND its on-disk length repeat the previous entry of the same
      // template. The kind alone is already implied by the template; the length is the open question.
      final int[] lastKind = DIAG_TEMPLATE_LAST_KIND.get();
      final int[] lastLength = DIAG_TEMPLATE_LAST_LENGTH.get();
      Arrays.fill(lastKind, -1);
      Arrays.fill(lastLength, -1);
      int predictable = 0;
      for (int i = 0; i < populatedCount; i++) {
        final int templateId = slotTemplateIds[i] & 0xFF;
        if (lastKind[templateId] == slotKindIds[i] && lastLength[templateId] == slotOnDiskLens[i]) {
          predictable++;
        }
        lastKind[templateId] = slotKindIds[i];
        lastLength[templateId] = slotOnDiskLens[i];
      }
      PageSectionDiag.recordCompactDirPredictability(populatedCount, predictable);
    }

    /**
     * Gather one class of records out of the staged heap and record what the page's codec makes of
     * them alone.
     *
     * <p>
     * A class's records are not contiguous, so they are copied into a scratch first. That is the price
     * of asking "what do the fused records cost" of a codec that only ever saw them interleaved.
     */
    private static void gatherAndRecordHeapClass(final MemorySegment staging, final long heapStart,
        final int populatedCount, final int[] slotKindIds, final int[] slotOnDiskLens, final int codec,
        final int heapClass, final int section) {
      final MemorySegment gather = diagGatherScratch(sumOnDiskLengths(populatedCount, slotOnDiskLens));
      long gathered = 0;
      long heapOffset = heapStart;
      for (int i = 0; i < populatedCount; i++) {
        final int length = slotOnDiskLens[i];
        if (heapClassOf(slotKindIds[i]) == heapClass) {
          MemorySegment.copy(staging, heapOffset, gather, gathered, length);
          gathered += length;
        }
        heapOffset += length;
      }
      if (gathered == 0) {
        return;
      }
      PageSectionDiag.recordPostCodecSection(section, gathered,
          encodedSizeWithCodec(codec, gather, 0L, Math.toIntExact(gathered)));
    }

    private static int sumOnDiskLengths(final int populatedCount, final int[] slotOnDiskLens) {
      int total = 0;
      for (int i = 0; i < populatedCount; i++) {
        total += slotOnDiskLens[i];
      }
      return total;
    }

    /** Which of the three attribution classes a record kind belongs to. */
    private static int heapClassOf(final int kindId) {
      if (KeyValueLeafPage.isFusedObjectNamedKindId(kindId)) {
        return HEAP_CLASS_FUSED;
      }
      return kindId == OBJECT_KIND_ID || kindId == ARRAY_KIND_ID
          ? HEAP_CLASS_STRUCTURAL
          : HEAP_CLASS_OTHER;
    }

    /** What {@code codec} compresses {@code [off, off + length)} to, without emitting it. */
    private static int encodedSizeWithCodec(final int codec, final MemorySegment src, final long off,
        final int length) {
      if (codec == 1) {
        // LZ4 works segment to segment, so this arm needs its own output segment rather than the byte
        // array the RLE and LZ77 codecs write into.
        final FFILz4Compressor lz4 = V1_HEAP_LZ4.get();
        if (lz4 == null) {
          return length;
        }
        final int bound = lz4.compressBound(length);
        MemorySegment out = DIAG_LZ4_OUT_SCRATCH.get();
        if (out.byteSize() < bound) {
          out = Arena.ofAuto().allocate(Math.max((long) bound, out.byteSize() * 2L));
          DIAG_LZ4_OUT_SCRATCH.set(out);
        }
        final int encoded = lz4.getCompressionMode() == FFILz4Compressor.CompressionMode.HIGH_COMPRESSION
            ? lz4.compressSegmentHC(src.asSlice(off, length), out.asSlice(0, bound), HEAP_LZ4_HC_LEVEL)
            : lz4.compressSegment(src.asSlice(off, length), out.asSlice(0, bound));
        return encoded < 0
            ? length
            : encoded;
      }
      final int bound = switch (codec) {
        case 0 -> ZeroRunByteCodec.maxEncodedSize(length);
        case 2 -> ByteRunCodec.maxEncodedSize(length);
        default -> SirixLZ77Codec.maxEncodedSize(length);
      };
      byte[] out = DIAG_CODEC_OUT_SCRATCH.get();
      if (out.length < bound) {
        out = new byte[Math.max(bound, out.length * 2)];
        DIAG_CODEC_OUT_SCRATCH.set(out);
      }
      return switch (codec) {
        case 0 -> ZeroRunByteCodec.encode(src, off, length, out, 0);
        case 2 -> ByteRunCodec.encode(src, off, length, out, 0);
        default -> SirixLZ77Codec.encode(src, off, length, out, 0);
      };
    }

    private static void recordHeapCompositionDiag(final int populatedCount, final int[] slotKindIds,
        final int[] slotOnDiskLens, final short[] slotDiagValueWidths, final byte[] slotValueElided,
        final boolean valueElisionActive) {
      final long[] fold = DIAG_HEAP_BY_KIND_SCRATCH.get();
      for (int i = 0; i < populatedCount; i++) {
        final int base = (slotKindIds[i] & 0xFF) << 2;
        fold[base]++;
        fold[base + 1] += slotOnDiskLens[i];
        // Only the fused primitives carry a payload elision can reach, and only for them is the
        // diagnostic width array written on this page — reading it for any other kind would report a
        // width left behind by an earlier page.
        if (VALUE_ELISION_ENABLED && isValueElisionCandidateKindId(slotKindIds[i])) {
          final int width = slotDiagValueWidths[i] & 0xFFFF;
          if (width > 0 && !(valueElisionActive && slotValueElided[i] != 0)) {
            fold[base + 2] += width;
            fold[base + 3]++;
          }
        }
      }
      for (int i = 0; i < populatedCount; i++) {
        final int kindId = slotKindIds[i] & 0xFF;
        final int base = kindId << 2;
        if (fold[base] != 0) {
          PageSectionDiag.recordHeapKind(kindId, fold[base], fold[base + 1], fold[base + 2], fold[base + 3]);
          fold[base] = 0;
          fold[base + 1] = 0;
          fold[base + 2] = 0;
          fold[base + 3] = 0;
        }
      }
    }

    /** The three fused primitive kinds whose payload the value-elision pre-scan measures. */
    private static boolean isValueElisionCandidateKindId(final int kindId) {
      return kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_NUMBER_KIND_ID
          || kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_STRING_KIND_ID
          || kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_BOOLEAN_KIND_ID;
    }

    /**
     * Stages deduplicated heap records without allocating. Kept outside {@link #writeEncodedBody} as a
     * deliberate compilation boundary: the complete page pre-scan and this range-copy loop are both
     * hot, but together they exceed the compiler's practical optimization budget.
     */
    private static long stageEncodedHeap(final MemorySegment slottedPage, final MemorySegment staging, long stagePos,
        final int populatedCount, final byte[] slotFieldCounts, final int[] slotHeapOffs, final int[] slotDataLens,
        final byte[] slotTemplateIds, final boolean hashElisionActive, final byte[] zeroHashBitmap,
        final short[] slotHashOffs, final boolean parentKeyColumnActive, final byte[] slotParentKeyWidths,
        final boolean rightSibColumnActive, final byte[] slotRightSibWidths, final short[] slotRightSibOffs,
        final boolean leftSibColumnActive, final byte[] slotLeftSibWidths, final short[] slotLeftSibOffs,
        final boolean pathNodeKeyColumnActive, final byte[] slotPnkWidths, final short[] slotPnkOffs,
        final boolean valueElisionActive, final byte[] slotValueElided, final short[] slotValueWidths,
        final short[] slotValueOffs, final boolean nameKeyElisionActive, final byte[] slotNameKeyElided,
        final byte[] slotNameKeyWidths, final short[] slotNameKeyOffs) {
      // heap (records with templateId replacing the offset table; hash, parentKey, pathNodeKey, value
      // and nameKey optionally stripped). The stripped ranges are collected per slot and walked in
      // ascending offset order, so the general case collapses to "copy the gaps, skip the ranges" in
      // one pass.
      //
      // HFT-grade: no allocation. The two range arrays are per-thread scratch taken once for the page,
      // never per slot, and the insertion that keeps them ordered runs over a handful of entries.
      final int[] rangeFrom = STRIP_RANGE_FROM_SCRATCH.get();
      final int[] rangeTo = STRIP_RANGE_TO_SCRATCH.get();
      for (int i = 0; i < populatedCount; i++) {
        final int fc = slotFieldCounts[i];
        final long recordBase = PageLayout.HEAP_START + slotHeapOffs[i];
        staging.set(ValueLayout.JAVA_BYTE, stagePos, slottedPage.get(ValueLayout.JAVA_BYTE, recordBase));
        stagePos++;
        staging.set(ValueLayout.JAVA_BYTE, stagePos, slotTemplateIds[i]);
        stagePos++;
        final int dataBytes = slotDataLens[i] - 1 - fc;
        final boolean stripHash = hashElisionActive && ((zeroHashBitmap[i >>> 3] >>> (i & 7)) & 1) == 1;
        final int stripPkWidth = parentKeyColumnActive
            ? (slotParentKeyWidths[i] & 0xFF)
            : 0;
        final int stripRightSibWidth = rightSibColumnActive
            ? (slotRightSibWidths[i] & 0xFF)
            : 0;
        final int stripLeftSibWidth = leftSibColumnActive
            ? (slotLeftSibWidths[i] & 0xFF)
            : 0;
        final int stripPnkWidth = pathNodeKeyColumnActive
            ? (slotPnkWidths[i] & 0xFF)
            : 0;
        final boolean stripValue = valueElisionActive && slotValueElided[i] != 0;
        final int stripValueWidth = stripValue
            ? (slotValueWidths[i] & 0xFFFF)
            : 0;
        final boolean stripNameKey = nameKeyElisionActive && slotNameKeyElided[i] != 0;
        final int stripNameKeyWidth = stripNameKey
            ? (slotNameKeyWidths[i] & 0xFF)
            : 0;
        // Collect the stripped ranges, kept sorted by start offset as they are inserted, then walk
        // them once: copy the gaps, skip the ranges. Which field sits where inside the data region is
        // kind-specific, so the order the strips are collected in is not the order they appear in —
        // hence the insertion rather than a hand-ordered list.
        int rCount = 0;
        if (stripPkWidth > 0) {
          rCount = insertStripRange(rangeFrom, rangeTo, rCount, 0, stripPkWidth);
        }
        if (stripRightSibWidth > 0) {
          rCount = insertStripRange(rangeFrom, rangeTo, rCount, slotRightSibOffs[i] & 0xFFFF, stripRightSibWidth);
        }
        if (stripLeftSibWidth > 0) {
          rCount = insertStripRange(rangeFrom, rangeTo, rCount, slotLeftSibOffs[i] & 0xFFFF, stripLeftSibWidth);
        }
        if (stripPnkWidth > 0) {
          rCount = insertStripRange(rangeFrom, rangeTo, rCount, slotPnkOffs[i] & 0xFFFF, stripPnkWidth);
        }
        if (stripHash) {
          rCount = insertStripRange(rangeFrom, rangeTo, rCount, slotHashOffs[i] & 0xFFFF,
              NodeFieldLayout.HASH_WIDTH);
        }
        if (stripValue) {
          rCount = insertStripRange(rangeFrom, rangeTo, rCount, slotValueOffs[i] & 0xFFFF, stripValueWidth);
        }
        if (stripNameKey) {
          rCount = insertStripRange(rangeFrom, rangeTo, rCount, slotNameKeyOffs[i] & 0xFFFF, stripNameKeyWidth);
        }
        // Walk ranges: copy gaps, skip ranges.
        int cursor = 0;
        for (int r = 0; r < rCount; r++) {
          final int from = rangeFrom[r];
          if (from > cursor) {
            MemorySegment.copy(slottedPage, recordBase + 1 + fc + cursor, staging, stagePos, from - cursor);
            stagePos += from - cursor;
          }
          cursor = rangeTo[r];
        }
        // Final tail from cursor to dataBytes.
        if (dataBytes > cursor) {
          MemorySegment.copy(slottedPage, recordBase + 1 + fc + cursor, staging, stagePos, dataBytes - cursor);
          stagePos += dataBytes - cursor;
        }
      }
      return stagePos;
    }

    /**
     * Insert one stripped range into an offset-ordered list, and report the new length.
     *
     * <p>
     * At most {@link #STRIP_RANGE_CAPACITY} ranges exist for a record and they arrive nearly sorted, so
     * an insertion is a compare and, occasionally, one shift. The alternative — collecting them in
     * field-index order and asserting monotonicity — would tie the caller to a per-kind field ordering
     * that {@code NodeFieldLayout} deliberately does not promise.
     *
     * @param from the range start offsets, ordered ascending
     * @param to the matching range end offsets
     * @param count how many ranges the list already holds
     * @param rangeStart the new range's start offset within the record's data region
     * @param width the new range's width in bytes
     * @return {@code count + 1}
     */
    private static int insertStripRange(final int[] from, final int[] to, final int count, final int rangeStart,
        final int width) {
      if (count == from.length) {
        throw new SirixIOException("more than " + from.length
            + " stripped ranges on one record — raise STRIP_RANGE_CAPACITY alongside the lever that added one");
      }
      int at = count;
      while (at > 0 && from[at - 1] > rangeStart) {
        from[at] = from[at - 1];
        to[at] = to[at - 1];
        at--;
      }
      from[at] = rangeStart;
      to[at] = rangeStart + width;
      return count + 1;
    }

    /**
     * Inline (un-deduped) heap emission. Used when the page has too many distinct offset-tables to
     * qualify for template dedup, or when the structural invariants that dedup relies on aren't
     * satisfied (e.g. onDiskLen &lt; 2 after strip).
     *
     * <p>
     * Wire layout: same first 13-byte prefix as the dedup path (populatedCount, onDiskHeapSize,
     * templateCount=0, templatePoolBytes=0) so the reader's header parse works identically. After that
     * the body is a single blob containing compactDir + records, encoded by whichever of
     * {@link ZeroRunByteCodec}, {@link ByteRunCodec}, {@link SirixLZ77Codec} produces the smallest
     * output. Emit: int compressedLen + 1 byte codec + compressed bytes.
     *
     * <p>
     * This is the HOT path for pages where the template pool blew past
     * {@link OffsetTableTemplatePool#MAX_TEMPLATES} (e.g. ELEMENT-heavy mixed-DFS pages) — which
     * represents roughly half of all pages on a typical Chicago-like scale dataset. Adding compression
     * here closes the largest remaining gap vs LZ4 HC's whole-page compression.
     */
    private static void writeInlineBody(final BytesOut<?> sink, final MemorySegment slottedPage,
        final int populatedCount, final int[] slotKindIds, final int[] slotHeapOffs, final int[] slotDataLens,
        final boolean chunkedBody, final long fsstDictId) {
      int totalHeapSize = 0;
      for (int i = 0; i < populatedCount; i++) {
        totalHeapSize += slotDataLens[i];
      }
      // Matching header prefix so the reader can always consume the same 13 bytes.
      sink.writeInt(populatedCount);
      sink.writeInt(totalHeapSize);
      sink.writeByte((byte) 0); // templateCount = 0 (no dedup)
      sink.writeInt(0); // templatePoolBytes = 0

      // Stage compactDir + heap into a single blob and compress it.
      final int compactDirBytes = PageLayout.COMPACT_DIR_ENTRY_SIZE * populatedCount;
      final int totalBlobBytes = compactDirBytes + totalHeapSize;

      // Stage into the dedup-path's staging scratch (native-backed so the
      // codecs can read via MemorySegment). Grow on demand.
      final MemorySegment staging = v1StagingScratch(totalBlobBytes);

      // Write compactDir entries (2 bytes each, big-endian).
      long stagePos = 0;
      for (int i = 0; i < populatedCount; i++) {
        PageLayout.writeCompactDirEntry(staging, stagePos, slotDataLens[i], slotKindIds[i]);
        stagePos += PageLayout.COMPACT_DIR_ENTRY_SIZE;
      }
      // Append heap bytes.
      for (int i = 0; i < populatedCount; i++) {
        MemorySegment.copy(slottedPage, PageLayout.HEAP_START + slotHeapOffs[i], staging, stagePos, slotDataLens[i]);
        stagePos += slotDataLens[i];
      }

      if (chunkedBody) {
        // Degenerate chunked twin: META is the compact dir alone, the chunks are the verbatim
        // inline records. The reader's degenerate branch checks exactly that shape.
        emitChunkedFrames(sink, staging, compactDirBytes, totalHeapSize, populatedCount, slotDataLens, fsstDictId);
        return;
      }

      // Smallest-of-codecs bake-off with sticky-winner election — shared with the
      // dedup path, see emitSmallestBody.
      emitSmallestBody(sink, staging, totalBlobBytes);
    }

    /**
     * Emit a chunk-framed body over the staged sections: the two prefix fields only a chunked body
     * carries, the META frame, the chunk table, then every payload.
     *
     * <p>
     * The staged bytes are the monolith body's bytes — same sections, same order. What changes is the
     * framing: metadata becomes one independently compressed META frame, and the heap is cut at entry
     * boundaries into chunks that each compress and checksum on their own, so a reader that wants one
     * record can decode one chunk instead of the page.
     *
     * @param staging the staged body, metadata in {@code [0, metaLen)} and heap behind it
     * @param metaLen bytes of page-global metadata
     * @param heapLen bytes of record heap, equal to the sum of {@code entryOnDiskLens}
     * @param entryOnDiskLens per-entry on-disk record length, in populated-bitmap rank order
     * @param fsstDictId the page's FSST dictionary id, hoisted here out of the tail (D7)
     */
    private static void emitChunkedFrames(final BytesOut<?> sink, final MemorySegment staging, final int metaLen,
        final int heapLen, final int populatedCount, final int[] entryOnDiskLens, final long fsstDictId) {
      final ChunkTable table = WRITE_CHUNK_TABLE.get();
      planChunks(entryOnDiskLens, populatedCount, heapLen, table);
      final int chunkCount = table.count;

      // One election per page, on the monolith body's probe cadence: a probe page bakes off every
      // codec per frame and keeps the smallest — which is what makes probeInterval=1 byte-pure — and
      // elects the winner of its largest frame, the one whose ratio actually decides the page size.
      // Pages in between encode every frame with that codec outright.
      final int[] sticky = STICKY_CODEC.get();
      final boolean warmup = sticky[1] < STICKY_WARMUP_PAGES;
      if (warmup) {
        sticky[1]++;
      }
      final boolean probe = STICKY_PROBE_INTERVAL <= 1 || warmup || sticky[2] >= STICKY_PROBE_INTERVAL - 1;
      if (probe) {
        sticky[2] = 0;
      } else {
        sticky[2]++;
      }
      final int elected = sticky[0];
      int electedFrom = -1;
      int nextElected = elected;

      // One growth check for the whole page: every frame's worst case is arithmetic, so the output
      // buffer is sized once and never moves under the encode loop.
      long worstCase = maxFrameBytes(metaLen);
      for (int c = 0; c < chunkCount; c++) {
        worstCase += maxFrameBytes(table.rawLen[c]);
      }
      final byte[] out = chunkedOutScratch(worstCase, populatedCount);

      final int[] frame = FRAME_ENCODE_SCRATCH.get();
      final int metaOff = 0;
      encodeChunkedFrame(staging, 0L, metaLen, out, metaOff, probe, elected, frame);
      final int metaEncLen = frame[0];
      final int metaCodec = frame[1];
      final long metaHash = metaEncLen == 0
          ? 0L
          : HashAlgorithm.XXH3.computeHashLong(out, metaOff, metaEncLen);
      if (probe && metaLen > electedFrom) {
        electedFrom = metaLen;
        nextElected = frame[2];
      }

      int outLen = metaOff + metaEncLen;
      long srcOff = metaLen;
      long encSum = 0;
      for (int c = 0; c < chunkCount; c++) {
        final int rawLen = table.rawLen[c];
        encodeChunkedFrame(staging, srcOff, rawLen, out, outLen, probe, elected, frame);
        table.encLen[c] = frame[0];
        table.codec[c] = frame[1];
        table.payloadOff[c] = outLen;
        // An empty frame is described, never hashed — mirroring the META frame, and the only shape
        // a chunk of zero-length records can take.
        table.hash[c] = frame[0] == 0
            ? 0L
            : HashAlgorithm.XXH3.computeHashLong(out, outLen, frame[0]);
        if (probe && rawLen > electedFrom) {
          electedFrom = rawLen;
          nextElected = frame[2];
        }
        outLen += frame[0];
        encSum += frame[0];
        srcOff += rawLen;
      }
      if (probe) {
        sticky[0] = nextElected;
      }

      // Everything the body occupies, counted from the byte after this field to the region table.
      // Computed, never backpatched: all frame lengths are known before the first byte is written.
      final long bodyTotalLen = (long) ChunkedBodyConfig.META_FRAME_HEADER_BYTES + 1
          + (long) chunkCount * ChunkedBodyConfig.CHUNK_TABLE_ROW_BYTES + metaEncLen + encSum;
      if (bodyTotalLen > Integer.MAX_VALUE) {
        // The field is an int, and a truncated one would send every reader into the middle of the
        // body looking for the region table. A page caps at 256 KiB, so this is unreachable — which
        // is exactly why it should say so rather than wrap.
        throw new SirixIOException("chunked body of " + bodyTotalLen + " bytes does not fit its length field");
      }
      Utils.putVarLong(sink, fsstDictId);
      sink.writeInt((int) bodyTotalLen);
      sink.writeInt(metaLen);
      sink.writeInt(metaEncLen);
      sink.writeByte((byte) metaCodec);
      sink.writeLong(metaHash);
      sink.writeByte((byte) chunkCount);
      for (int c = 0; c < chunkCount; c++) {
        sink.writeShort((short) table.firstEntry[c]);
        sink.writeShort((short) table.entryCount[c]);
        sink.writeInt(table.rawLen[c]);
        sink.writeInt(table.encLen[c]);
        sink.writeByte((byte) table.codec[c]);
        sink.writeLong(table.hash[c]);
      }
      if (metaEncLen > 0) {
        sink.write(out, metaOff, metaEncLen);
      }
      for (int c = 0; c < chunkCount; c++) {
        sink.write(out, table.payloadOff[c], table.encLen[c]);
      }
      CHUNKED_BODIES_WRITTEN.increment();
    }

    /**
     * Cut the page's entries into chunks: walk them in rank order, adding on-disk lengths, and close a
     * chunk once it has reached the target. A record at least as large as the target lands in a chunk
     * of its own, which is why the table's length fields are ints.
     *
     * <p>
     * Boundaries live in ENTRY space, not slot space: the compact dir is a run of lengths with no slot
     * ids in it, so under a holey bitmap only the populated-rank order is well defined.
     *
     * <p>
     * The chunk count is a single byte. Rather than truncate a page that would need more, the planner
     * doubles the target and replans — reachable only for pages near the 256 KiB capacity whose records
     * are all tiny.
     */
    private static void planChunks(final int[] entryOnDiskLens, final int populatedCount, final int heapLen,
        final ChunkTable table) {
      int target = ChunkedBodyConfig.targetChunkBytes();
      for (;;) {
        int count = 0;
        int acc = 0;
        int start = 0;
        boolean overflow = false;
        for (int i = 0; i < populatedCount; i++) {
          acc += entryOnDiskLens[i];
          if (acc >= target) {
            if (count == ChunkedBodyConfig.MAX_CHUNKS) {
              overflow = true;
              break;
            }
            table.firstEntry[count] = start;
            table.entryCount[count] = i - start + 1;
            table.rawLen[count] = acc;
            count++;
            start = i + 1;
            acc = 0;
          }
        }
        if (!overflow && start < populatedCount) {
          if (count == ChunkedBodyConfig.MAX_CHUNKS) {
            overflow = true;
          } else {
            table.firstEntry[count] = start;
            table.entryCount[count] = populatedCount - start;
            table.rawLen[count] = acc;
            count++;
          }
        }
        if (!overflow) {
          long planned = 0;
          for (int c = 0; c < count; c++) {
            planned += table.rawLen[c];
          }
          if (planned != heapLen) {
            throw new SirixIOException("chunk plan covers " + planned + " heap bytes, the staged heap is " + heapLen);
          }
          table.count = count;
          return;
        }
        if (target > (1 << 28)) {
          throw new SirixIOException("cannot fit " + populatedCount + " entries into " + ChunkedBodyConfig.MAX_CHUNKS
              + " chunks at a " + target + "-byte target");
        }
        target <<= 1;
      }
    }

    /**
     * Encode one frame into {@code out} at {@code outOff}.
     *
     * <p>
     * A frame whose winning codec does not actually shrink it is stored verbatim instead: the codec
     * byte says so, and the reader copies rather than decodes.
     *
     * @param frame receives {@code [encLen, wire codec, bake-off winner]} — the winner is the codec the
     *        page may elect, which is never the STORED pseudo-codec
     */
    private static void encodeChunkedFrame(final MemorySegment src, final long srcOff, final int rawLen,
        final byte[] out, final int outOff, final boolean probe, final int elected, final int[] frame) {
      if (rawLen == 0) {
        frame[0] = 0;
        frame[1] = ChunkedBodyConfig.CODEC_STORED;
        frame[2] = elected;
        return;
      }
      final int winnerCodec;
      final int winnerLen;
      final byte[] winnerBuf;
      if (probe) {
        final byte[] zeroRunBuf = zeroRunScratch(rawLen);
        final int v0Len = ZeroRunByteCodec.encode(src, srcOff, rawLen, zeroRunBuf, 0);
        final byte[] byteRunBuf = byteRunScratch(rawLen);
        final int v2Len = BYTE_RUN_CODEC_ENABLED
            ? ByteRunCodec.encode(src, srcOff, rawLen, byteRunBuf, 0)
            : Integer.MAX_VALUE;
        final byte[] lz77Buf = lz77Scratch(rawLen);
        final int v3Len = LZ77_CODEC_ENABLED
            ? SirixLZ77Codec.encode(src, srcOff, rawLen, lz77Buf, 0)
            : Integer.MAX_VALUE;
        final int bestLen = Math.min(v0Len, Math.min(v2Len, v3Len));
        // Tie order mirrors emitSmallestBody: LZ77 > ByteRun > ZeroRun.
        if (bestLen == v3Len) {
          winnerCodec = 3;
          winnerLen = v3Len;
          winnerBuf = lz77Buf;
        } else if (bestLen == v2Len) {
          winnerCodec = 2;
          winnerLen = v2Len;
          winnerBuf = byteRunBuf;
        } else {
          winnerCodec = 0;
          winnerLen = v0Len;
          winnerBuf = zeroRunBuf;
        }
      } else if (!CODEC_BAKEOFF_STICKY_ONLY) {
        // Same rule as the monolith body between probes: zero-run and LZ77 always compete, byte-run
        // only while it holds the election. The scratches are per codec, so all three may be live.
        final byte[] zeroRunBuf = zeroRunScratch(rawLen);
        final int v0Len = ZeroRunByteCodec.encode(src, srcOff, rawLen, zeroRunBuf, 0);
        final byte[] byteRunBuf = elected == 2 && BYTE_RUN_CODEC_ENABLED
            ? byteRunScratch(rawLen)
            : null;
        final int v2Len = byteRunBuf != null
            ? ByteRunCodec.encode(src, srcOff, rawLen, byteRunBuf, 0)
            : Integer.MAX_VALUE;
        final byte[] lz77Buf = LZ77_CODEC_ENABLED
            ? lz77Scratch(rawLen)
            : null;
        final int v3Len = lz77Buf != null
            ? SirixLZ77Codec.encode(src, srcOff, rawLen, lz77Buf, 0)
            : Integer.MAX_VALUE;
        final int bestLen = Math.min(v0Len, Math.min(v2Len, v3Len));
        if (bestLen == v3Len) {
          winnerCodec = 3;
          winnerLen = v3Len;
          winnerBuf = lz77Buf;
        } else if (bestLen == v2Len) {
          winnerCodec = 2;
          winnerLen = v2Len;
          winnerBuf = byteRunBuf;
        } else {
          winnerCodec = 0;
          winnerLen = v0Len;
          winnerBuf = zeroRunBuf;
        }
      } else if (elected == 3) {
        winnerBuf = lz77Scratch(rawLen);
        winnerLen = SirixLZ77Codec.encode(src, srcOff, rawLen, winnerBuf, 0);
        winnerCodec = 3;
      } else if (elected == 2) {
        winnerBuf = byteRunScratch(rawLen);
        winnerLen = ByteRunCodec.encode(src, srcOff, rawLen, winnerBuf, 0);
        winnerCodec = 2;
      } else {
        winnerBuf = zeroRunScratch(rawLen);
        winnerLen = ZeroRunByteCodec.encode(src, srcOff, rawLen, winnerBuf, 0);
        winnerCodec = 0;
      }
      frame[2] = winnerCodec;
      if (winnerLen >= rawLen) {
        MemorySegment.copy(src, ValueLayout.JAVA_BYTE, srcOff, out, outOff, rawLen);
        frame[0] = rawLen;
        frame[1] = ChunkedBodyConfig.CODEC_STORED;
      } else {
        System.arraycopy(winnerBuf, 0, out, outOff, winnerLen);
        frame[0] = winnerLen;
        frame[1] = winnerCodec;
      }
    }

    /** Output bytes a frame of {@code rawLen} can occupy, whichever codec wins — STORED included. */
    private static int maxFrameBytes(final int rawLen) {
      if (rawLen == 0) {
        return 0;
      }
      int max = Math.max(rawLen, ZeroRunByteCodec.maxEncodedSize(rawLen));
      max = Math.max(max, ByteRunCodec.maxEncodedSize(rawLen));
      return Math.max(max, SirixLZ77Codec.maxEncodedSize(rawLen));
    }

    /**
     * Read a chunk-framed body into the staging buffer, laid out exactly as the monolith blob is:
     * metadata sections first, record heap behind them. Nothing downstream — section parse, record
     * expansion, heap copy — learns that the bytes arrived framed.
     *
     * <p>
     * Every field the table states is cross-checked against what the page's own header already says,
     * because a chunk table that disagrees with the bitmap would hand a record's bytes to the wrong
     * slot rather than fail: the entry ranges must partition the populated entries contiguously, the
     * raw lengths must sum to the heap, and the frames must occupy exactly the declared body.
     *
     * @param bodyTotalLen the prefix's account of the body size, checked against the frames
     * @param lazy stop after META: the chunk payloads are read but left encoded, in the table's
     *        {@code pendingWire} slots, for {@link LazyChunkedBody} to decode when a reader first asks
     *        for a slot inside one
     * @return the staging segment holding {@code META || heap}, or META alone when {@code lazy}; the
     *         META length lands in the thread's {@link ChunkTable}
     */
    private static MemorySegment readChunkedBody(final BytesIn<?> source, final long recordPageKey,
        final int populatedCount, final int onDiskHeapSize, final int bodyTotalLen, final int minimumMetaRawLen,
        final int maximumMetaRawLen, final boolean lazy) {
      final long bodyStart = source.position();
      validateEncodedLength(recordPageKey, "chunked body", bodyTotalLen, source);
      final ChunkTable table = READ_CHUNK_TABLE.get();
      final int metaRawLen = source.readInt();
      final int metaEncLen = source.readInt();
      final int metaCodec = source.readByte() & 0xFF;
      final long metaHash = source.readLong();
      if (metaRawLen < minimumMetaRawLen || metaRawLen > maximumMetaRawLen || metaEncLen < 0) {
        throw new SirixIOException("page " + recordPageKey + " has invalid META frame lengths: rawLen=" + metaRawLen
            + " expected=[" + minimumMetaRawLen + ',' + maximumMetaRawLen + "] encLen=" + metaEncLen);
      }
      table.metaRawLen = metaRawLen;
      final int chunkCount = source.readByte() & 0xFF;
      if (chunkCount > ChunkedBodyConfig.MAX_CHUNKS) {
        throw new SirixIOException("page " + recordPageKey + " declares " + chunkCount + " chunks, more than the "
            + ChunkedBodyConfig.MAX_CHUNKS + " a chunk table can hold");
      }
      table.count = chunkCount;
      int entrySum = 0;
      long rawSum = 0;
      long encSum = 0;
      for (int c = 0; c < chunkCount; c++) {
        final int firstEntry = source.readShort() & 0xFFFF;
        final int entryCount = source.readShort() & 0xFFFF;
        final int rawLen = source.readInt();
        final int encLen = source.readInt();
        final int codec = source.readByte() & 0xFF;
        final long hash = source.readLong();
        if (firstEntry != entrySum) {
          throw new SirixIOException("page " + recordPageKey + " chunk " + c + " starts at entry " + firstEntry
              + ", expected " + entrySum + " — chunk entry ranges must be contiguous and ascending");
        }
        // Every chunk covers at least one entry and every record carries at least its kind byte, so
        // a chunk with nothing in it is a corrupt table, not a page shape the writer can produce.
        if (entryCount <= 0 || rawLen < 0 || encLen < 0 || (rawLen == 0) != (encLen == 0)) {
          throw new SirixIOException("page " + recordPageKey + " chunk " + c + " has invalid lengths: entries="
              + entryCount + " rawLen=" + rawLen + " encLen=" + encLen);
        }
        entrySum += entryCount;
        rawSum += rawLen;
        encSum += encLen;
        table.firstEntry[c] = firstEntry;
        table.entryCount[c] = entryCount;
        table.rawLen[c] = rawLen;
        table.encLen[c] = encLen;
        table.codec[c] = codec;
        table.hash[c] = hash;
      }
      if (entrySum != populatedCount) {
        throw new SirixIOException("page " + recordPageKey + " chunk table covers " + entrySum + " entries, the page"
            + " has " + populatedCount);
      }
      if (rawSum != onDiskHeapSize) {
        throw new SirixIOException("page " + recordPageKey + " chunk table covers " + rawSum + " heap bytes, the"
            + " header says " + onDiskHeapSize);
      }
      final long framed = source.position() - bodyStart + metaEncLen + encSum;
      if (framed != bodyTotalLen) {
        throw new SirixIOException("page " + recordPageKey + " frames occupy " + framed + " bytes, the prefix declares"
            + " a body of " + bodyTotalLen);
      }

      final int decodedBodySize = checkedPageBodySize(recordPageKey, "chunked decoded body",
          (long) metaRawLen + (lazy ? 0L : onDiskHeapSize));
      final MemorySegment staging = v1StagingScratch(decodedBodySize);
      readChunkedFrame(source, metaEncLen, metaCodec, metaRawLen, metaHash, staging, 0L, recordPageKey, -1);
      if (lazy) {
        for (int c = 0; c < chunkCount; c++) {
          table.pendingWire[c] = readPendingChunk(source, table.encLen[c]);
        }
      } else {
        long dstOff = metaRawLen;
        for (int c = 0; c < chunkCount; c++) {
          readChunkedFrame(source, table.encLen[c], table.codec[c], table.rawLen[c], table.hash[c], staging, dstOff,
              recordPageKey, c);
          dstOff += table.rawLen[c];
        }
      }
      CHUNKED_BODIES_READ.increment();
      return staging;
    }

    /**
     * Copy one chunk's encoded bytes into a page-owned buffer, without decoding them.
     *
     * <p>
     * Over-sized by the native decoder's input tail slack. Sized exactly, the buffer fails
     * {@link SirixLZ77Codec}'s {@code off + len + NATIVE_INPUT_TAIL_SLACK <= input.length} precondition
     * and every lazily expanded chunk silently takes the pure-Java decoder — which is precisely what
     * happened to the deferred region payloads before they carried the same slack.
     */
    private static byte[] readPendingChunk(final BytesIn<?> source, final int encLen) {
      final byte[] wire = new byte[encLen + SirixLZ77Codec.NATIVE_INPUT_TAIL_SLACK];
      if (encLen > 0) {
        source.read(wire, 0, encLen);
      }
      return wire;
    }

    /**
     * Read one frame's stored bytes and decode them into {@code dst}, verifying the checksum the table
     * carries first.
     *
     * <p>
     * The verify-and-decode itself lives in {@link LazyChunkedBody#decodeFrame}, which is also what a
     * chunk expanded on demand goes through — one implementation, so a frame cannot mean one thing at
     * load time and another later.
     *
     * @param chunkIndex the chunk's index, or {@code -1} for the META frame
     */
    private static void readChunkedFrame(final BytesIn<?> source, final int encLen, final int codec, final int rawLen,
        final long expectedHash, final MemorySegment dst, final long dstOff, final long recordPageKey,
        final int chunkIndex) {
      if (encLen == 0) {
        if (rawLen != 0) {
          throw new SirixIOException("page " + recordPageKey + " " + (chunkIndex < 0
              ? "META frame"
              : "chunk " + chunkIndex) + " has no stored bytes but claims to decode to " + rawLen);
        }
        return;
      }
      byte[] buf = V1_HEAP_RLE_SCRATCH.get();
      // The native decoder reads past the frame, so the scratch is grown with room to spare rather
      // than to the frame's exact size — sized exactly it would fall back to the Java decoder.
      if (buf.length < encLen + SirixLZ77Codec.NATIVE_INPUT_TAIL_SLACK) {
        buf = new byte[Math.max(encLen + SirixLZ77Codec.NATIVE_INPUT_TAIL_SLACK, buf.length * 2)];
        V1_HEAP_RLE_SCRATCH.set(buf);
      }
      source.read(buf, 0, encLen);
      LazyChunkedBody.decodeFrame(buf, encLen, codec, rawLen, expectedHash, dst, dstOff, recordPageKey, chunkIndex);
    }

    /**
     * Build the PAX {@link RegionTable} for {@code page} by walking the compact slot descriptors that
     * the serializer's single bitmap pass already collected, collecting each fused OBJECT_NAMED_NUMBER
     * slot's value (long) and its parent OBJECT_KEY's nameKey, and encoding them via
     * {@link NumberRegion}.
     *
     * <p>
     * Values whose payload type is not int/long (BigDecimal, double, float) are skipped — the slow-path
     * {@code getNumberValueLongFromSlot} returns {@link Long#MIN_VALUE} as a sentinel, and the caller
     * still sees the correct answer via the inline-slot fallback path.
     *
     * <p>
     * Values whose parent OBJECT_KEY is not on the same page are tagged with {@code -1} — the scan
     * operator's fallback branch handles them.
     *
     * <p>
     * Returns {@code null} when the page has no numeric values (common for path-summary and index
     * pages).
     */
    /**
     * Build the PAX region table and, when {@code slotRegionAbsIdx} is non-null, record for every
     * contributing fused-primitive slot the <em>absolute index</em> its value occupies in its region's
     * value sequence.
     *
     * <p>
     * That index is what makes per-slot value elision safe. The writer elides exactly the slots this
     * method indexed, the index travels on the wire, and the reader decodes by it directly — so there
     * is no second predicate anywhere that has to agree with this method about which slots contribute,
     * and no rank-walk whose count can drift from the region's. (The previous all-or-nothing design
     * existed precisely because ranks were re-derived on both sides; one non-contributing slot on a
     * page desynchronised them, which is why elision fired on 0.1% of pages and every page stored its
     * fused strings twice.)
     *
     * <p>
     * Indices are assigned by replaying the contributor lists against the <em>winning</em> payload's
     * tag layout, because the name-tagged and path-tagged encoders group values differently and only
     * one of them is kept.
     */
    private static RegionTable buildRegionTable(final KeyValueLeafPage page, final MemorySegment slottedPage,
        final ResourceConfiguration resourceConfig, final int populatedCount, final int[] slotKindIds,
        final short[] slotBits, final int[] slotRegionAbsIdx, final @Nullable RegionTable disposableWriterTable) {
      // A sidecar is deliberately a pathological dense/fragmented-page fallback. Until every PAX
      // region builder consumes a single merged logical-slot iterator, emitting regions from only
      // the inline bitmap would make the page look complete while silently omitting side values.
      // Refuse the column cache for this rare page and let the side-aware row scan remain the source
      // of truth. Ordinary pages pay one predictable null/count branch and keep the existing path.
      if (page.getSideSlotCount() != 0) {
        return null;
      }
      final long[] valBuf = NUMBER_VALUE_SCRATCH.get();
      final int[] parBuf = NUMBER_PARENT_SCRATCH.get();
      final int[] numberPathBuf = NUMBER_PATH_SCRATCH.get();
      final int[] okNameKeys = OBJECT_KEY_NAMEKEY_SCRATCH.get();
      final int[] okSlots = OBJECT_KEY_SLOT_SCRATCH.get();
      final long[] okParentKeys = OBJECT_KEY_PARENT_KEY_SCRATCH.get();
      final int[] numberSlots = NUMBER_REGION_SLOT_SCRATCH.get();
      final int[] stringSlots = STRING_REGION_SLOT_SCRATCH.get();
      final int[] stringNameTags = STRING_REGION_NAME_TAG_SCRATCH.get();
      final int[] stringPathTags = STRING_REGION_PATH_TAG_SCRATCH.get();
      final int[] boolSlots = BOOLEAN_REGION_SLOT_SCRATCH.get();
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
      // Acquired lazily on the first fused string so pages without strings never touch the
      // ThreadLocal. The array is reused for every value on this page and retained across pages;
      // StringRegion copies only dictionary misses before the next slot overwrites it.
      byte[] stringValueScratch = null;
      int stringCount = 0;
      // A fused overflow descriptor exposes field metadata but not a value. Publishing a TAG from
      // only its remaining inline strings would make every tagCount under it — and every exact
      // negative probe — lie, so the descriptor's tag leaves the region
      // ({@link StringRegion.Encoder#suppressTag}) and its slots keep their values inline. Under the
      // kill switch the whole page's region goes instead, which is what this flag then tracks.
      boolean stringRegionComplete = true;
      // Whether any tag left the region. Gates the dictionary sketch: the sketch proves absence for
      // the PAGE, and a suppressed tag's values ARE on the page while being absent from the
      // dictionary the sketch is built from.
      boolean anyStringTagSuppressed = false;
      // Diagnostic-only tallies for the section report: how many overflow descriptors this page
      // holds (one is enough to suppress the region) and the stored bytes of the strings that then
      // stay inline in the record heap. Two stack locals whose only consumer is the diagnostic below,
      // so the disabled build drops both the reads and the accumulation with the dead branch.
      int stringOverflowDescriptorCount = 0;
      long stringStagedBytes = 0L;
      // Array-element strings are STAGED, not added as they are met: they are published only if
      // EVERY element on the page resolved its enclosing array, because a tag holding most of a
      // path's values is worse than no tag — every reader treats tagCount as the complete count of
      // that path's values on the page. Staged after the named strings, which is harmless: they
      // land under their OWN tag (the enclosing array's), so within-tag order stays slot order.
      byte[][] elemValues = null;
      int[] elemNameTags = null;
      int[] elemPathTags = null;
      int[] elemSlots = null;
      int elemCount = 0;
      boolean elemUsable = KeyValueLeafPage.ARRAY_ELEMENT_STRINGS_IN_REGION;
      int orphanCount = 0; // leading elements whose array opens on the previous page

      // BooleanRegion collection — mirrors NumberRegion's tagged-by-name OR
      // tagged-by-path layout. We populate two parallel int[] tag buffers and
      // pick one at finish time based on path-summary validity.
      final double[] dblValBuf = DOUBLE_VALUE_SCRATCH.get();
      final long[] dblDecUnscaled = DOUBLE_DECIMAL_UNSCALED_SCRATCH.get();
      final int[] dblDecScales = DOUBLE_DECIMAL_SCALE_SCRATCH.get();
      final int[] dblDecOut = DOUBLE_DECIMAL_OUT_SCRATCH.get();
      final int[] dblNameTags = DOUBLE_NAME_TAG_SCRATCH.get();
      final int[] dblPathTags = DOUBLE_PATH_TAG_SCRATCH.get();
      final int[] dblOrdinals = DOUBLE_ORDINAL_SCRATCH.get();
      // Field ordinal per nameKey, counted across BOTH numeric types in slot order — the position
      // list a versioned merge uses to split one anchor-slot liveness bitmap into per-column
      // masks. Counting only numeric slots equals counting all of the field's slots exactly when
      // the completeness oracle passes, which is the only time the positions are consulted.
      final Int2IntOpenHashMap fieldOrdinal = DOUBLE_FIELD_ORDINAL_SCRATCH.get();
      fieldOrdinal.clear();
      fieldOrdinal.defaultReturnValue(0);
      int dblCount = 0;
      boolean doubleAllPathNodeKeysValid = withPathSummary;
      final boolean[] boolValBuf = BOOLEAN_VALUE_SCRATCH.get();
      final int[] boolNameTags = BOOLEAN_TAG_SCRATCH.get();
      final int[] boolPathTags = BOOLEAN_PATH_SCRATCH.get();
      int boolCount = 0;
      boolean booleanAllPathNodeKeysValid = withPathSummary;

      // slotBits/slotKindIds are in the exact ascending bitmap order used by the former walk. Reusing
      // them removes one directory-kind foreign-memory read per populated slot plus the 16 bitmap-word
      // reads, without changing any region order or contributor predicate.
      for (int i = 0; i < populatedCount; i++) {
        final int slot = slotBits[i] & 0xFFFF;
        final int kindId = slotKindIds[i];
        if (elemUsable && KeyValueLeafPage.isElementPurityKindId(kindId)
            && !page.elementStagingStaysPure(slot, kindId, pageKeyBase)) {
          elemUsable = false; // a non-string element the certificate cannot model: no staging
        }
        if (KeyValueLeafPage.isFusedObjectNamedKindId(kindId)) {
          // Fused OBJECT_NAMED_* plays the OBJECT_KEY role; add to the nameKey region so the
          // SIMD scan in ObjectKeyNameKeyRegion.findMatchingSlots sees fused slots natively,
          // then feed NUMBER/STRING/BOOLEAN regions from the inline value (no parent indirection).
          okNameKeys[okCount] = page.getFusedObjectNamedNameKeyFromSlot(slot);
          okSlots[okCount] = slot;
          // Enclosing object's node key, raw. Read here because the writer already has the
          // record in hand; a scan resolving it later would be reconstructing the very record
          // the columns exist to avoid touching. Classification — on-page parent, the spanning
          // record's skip prefix, or a refusal — is RecordOrdinalRegion.encode's own contract.
          okParentKeys[okCount] = page.getObjectKeyParentKeyFromSlot(slot, pageKeyBase + slot);
          okCount++;
          if (kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_NUMBER_KIND_ID) {
            final int numericOrdinal = DoubleRegion.nextFieldOrdinal(fieldOrdinal, okNameKeys[okCount - 1]);
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
              numberSlots[count] = slot;
              count++;
            } else {
              // The long region declined the value: it is Double/Float-typed (or a Big* the
              // column-izer skips entirely). Route the floating-point ones to their own column
              // so a mixed field can still be answered as longKernel + doubleKernel instead of
              // sending the whole page back to the records over a handful of values. The
              // value itself STAYS in the record heap — this column is a pure accelerator and
              // takes no part in per-slot value elision.
              int dblScale = KeyValueLeafPage.DECIMAL_SCALE_UNAVAILABLE;
              long dblUnscaled = 0L;
              final double dblValue;
              if (page.isFusedObjectNamedNumberDecimalSlot(slot)) {
                // A decimal, whatever its double image happens to be — the STORED TYPE picks the
                // column, so prices like 19.99 are no longer dropped here and 1000.25 cannot slip
                // into the double domain beside them. Carried EXACTLY as its own unscaled integer:
                // the column stores such a tag at e = scale, f = 0, and the scan converts its
                // threshold into the same domain, so the kernel's integer comparison IS the
                // decimal comparison.
                dblUnscaled = page.getFusedObjectNamedNumberValueDecimalFromSlot(slot, dblDecOut);
                dblScale = dblDecOut[0];
                // Zone-map bound only, and every bound over a decimal tag is widened outward
                // before use, so this division's ulp cannot prune a matching page.
                dblValue = dblScale == KeyValueLeafPage.DECIMAL_SCALE_UNAVAILABLE
                    ? Double.NaN
                    : dblUnscaled / DoubleRegion.exp10(dblScale);
              } else {
                dblValue = page.getFusedObjectNamedNumberValueDoubleFromSlot(slot);
              }
              if (!Double.isNaN(dblValue)) {
                final int fusedNameKey = okNameKeys[okCount - 1];
                int fusedPathNodeKeyInt = -1;
                if (doubleAllPathNodeKeysValid) {
                  final long fusedNodeKey = pageKeyBase + slot;
                  final long pnk = page.getObjectKeyPathNodeKeyFromSlot(slot, fusedNodeKey);
                  if (pnk > 0L && pnk <= (long) Integer.MAX_VALUE) {
                    fusedPathNodeKeyInt = (int) pnk;
                  } else {
                    doubleAllPathNodeKeysValid = false;
                  }
                }
                dblValBuf[dblCount] = dblValue;
                dblDecUnscaled[dblCount] = dblUnscaled;
                dblDecScales[dblCount] = dblScale;
                dblNameTags[dblCount] = fusedNameKey;
                dblPathTags[dblCount] = fusedPathNodeKeyInt;
                dblOrdinals[dblCount] = numericOrdinal;
                dblCount++;
              }
            }
          } else if (kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_STRING_KIND_ID) {
            // STORED bytes, verbatim — FSST-encoded when the compress pass rewrote the slot.
            // The dictionary must mirror the heap bit-for-bit so value elision stays a pure
            // copy in both directions; decoding belongs to whoever materialises the value.
            if (stringValueScratch == null) {
              stringValueScratch = STRING_REGION_VALUE_SCRATCH.get();
            }
            int valueLength = page.copyFusedObjectNamedStringStoredBytes(slot, stringValueScratch);
            while (valueLength > stringValueScratch.length) {
              stringValueScratch = growStringRegionValueScratch(stringValueScratch, valueLength);
              valueLength = page.copyFusedObjectNamedStringStoredBytes(slot, stringValueScratch);
            }
            final boolean valueCompressed = valueLength >= 0 && page.isFusedObjectNamedStringValueCompressed(slot);
            if (valueLength >= 0) {
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
                stringEncPath = resetStringRegionPathCandidate(withPathSummary, page.globalStringDictionaries());
              }
              // The name-key dictionary owns the sole store range. The bridge gives the
              // alternative path-key dictionary that exact private range without exposing it or
              // copying the same distinct value twice.
              final StringRegion.Encoder alternateStringEncoder = stringEncPath != null && stringAllPathNodeKeysValid
                  ? stringEncPath
                  : null;
              stringEncName.addValueCopiedAndShareWith(fusedNameKey, stringValueScratch, 0, valueLength,
                  valueCompressed, alternateStringEncoder, fusedPathNodeKeyInt);
              stringSlots[stringCount] = slot;
              stringNameTags[stringCount] = fusedNameKey;
              stringPathTags[stringCount] = fusedPathNodeKeyInt;
              stringCount++;
              stringStagedBytes += valueLength;
            } else if (page.isFusedObjectNamedStringOverflowDescriptor(slot)) {
              stringOverflowDescriptorCount++;
              if (STRING_REGION_PER_TAG_COMPLETENESS) {
                final int fusedNameKey = okNameKeys[okCount - 1];
                // The descriptor's own path key decides the path-tagged candidate exactly as a
                // value's does: a suppression this page could not express in the path key space
                // would leave a path-tagged region claiming a completeness it does not have.
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
                  stringEncPath = resetStringRegionPathCandidate(withPathSummary, page.globalStringDictionaries());
                }
                stringEncName.suppressTag(fusedNameKey);
                if (stringEncPath != null && stringAllPathNodeKeysValid) {
                  stringEncPath.suppressTag(fusedPathNodeKeyInt);
                }
                anyStringTagSuppressed = true;
              } else {
                stringRegionComplete = false;
              }
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
            boolSlots[boolCount] = slot;
            boolCount++;
          }
        } else if (elemUsable && kindId == KeyValueLeafPage.STRING_VALUE_KIND_ID_PUBLIC) {
          // An ARRAY ELEMENT. It carries no path node key of its own — every one reads back as
          // -1 — which is why a path-tagged string column never held them. Its enclosing array
          // does have one, and that is the tag a query naming `$m.genres[]` looks under.
          final byte[] elementValue = page.readStringValueBytes(slot);
          final long parentKey = page.getSlotParentKey(slot);
          final long parentSlotLong = parentKey - pageKeyBase;
          if (elementValue == null) {
            elemUsable = false; // an undecodable value; the set would be silently short
          } else if (parentSlotLong < 0 || parentSlotLong >= Constants.NDP_NODE_COUNT) {
            // An element whose array opens on the PREVIOUS page. Slot order is node-key order,
            // so those form a LEADING RUN at the head of the page and nowhere else — the same
            // shape RecordOrdinalRegion records as its skip prefix rather than refusing.
            //
            // They used to be dropped, and that made the page holding the array unable to settle
            // its own last record: profiled cold, deciding those records through the records was
            // 69 % of an array-membership query, one full page rebuild each. They cannot be
            // tagged by their array's path — it is off-page and unnameable from here — so they go
            // under the reserved orphan tag, which is all the owning page needs, since only its
            // last array can spill and every orphan here therefore belongs to it.
            if (elemCount > orphanCount) {
              // An orphan PAST the leading run — i.e. after an element this page's own array
              // owns. Slot order makes that impossible in a well-formed page, so it is a shape
              // this does not model.
              elemUsable = false;
            } else {
              if (elemValues == null) {
                elemValues = new byte[64][];
                elemNameTags = new int[64];
                elemPathTags = new int[64];
                elemSlots = new int[64];
              } else if (elemCount == elemValues.length) {
                elemValues = Arrays.copyOf(elemValues, elemCount << 1);
                elemNameTags = Arrays.copyOf(elemNameTags, elemCount << 1);
                elemPathTags = Arrays.copyOf(elemPathTags, elemCount << 1);
                elemSlots = Arrays.copyOf(elemSlots, elemCount << 1);
              }
              elemNameTags[elemCount] = StringRegion.TAG_ORPHAN_ELEMENTS;
              elemPathTags[elemCount] = StringRegion.TAG_ORPHAN_ELEMENTS;
              elemValues[elemCount] = elementValue;
              elemSlots[elemCount] = slot;
              elemCount++;
              orphanCount++;
            }
          } else {
            final int parentSlot = (int) parentSlotLong;
            final int parentKind = PageLayout.getDirNodeKindId(slottedPage, parentSlot);
            if (!KeyValueLeafPage.isFusedStructuralKindId(parentKind)
                && !KeyValueLeafPage.isFusedObjectNamedKindId(parentKind)) {
              elemUsable = false;
            } else {
              if (elemValues == null) {
                elemValues = new byte[64][];
                elemNameTags = new int[64];
                elemPathTags = new int[64];
                elemSlots = new int[64];
              } else if (elemCount == elemValues.length) {
                elemValues = Arrays.copyOf(elemValues, elemCount << 1);
                elemNameTags = Arrays.copyOf(elemNameTags, elemCount << 1);
                elemPathTags = Arrays.copyOf(elemPathTags, elemCount << 1);
                elemSlots = Arrays.copyOf(elemSlots, elemCount << 1);
              }
              elemNameTags[elemCount] = KeyValueLeafPage.isFusedStructuralKindId(parentKind)
                  ? page.getFusedStructuralNameKeyFromSlot(parentSlot)
                  : page.getFusedObjectNamedNameKeyFromSlot(parentSlot);
              final long parentPnk = page.getObjectKeyPathNodeKeyFromSlot(parentSlot, pageKeyBase + parentSlot);
              elemPathTags[elemCount] = parentPnk > 0L && parentPnk <= (long) Integer.MAX_VALUE
                  ? (int) parentPnk
                  : -1;
              elemValues[elemCount] = elementValue;
              elemSlots[elemCount] = slot;
              elemCount++;
            }
          }
        } else if (KeyValueLeafPage.isFusedStructuralKindId(kindId)) {
          // OBJECT- and ARRAY-valued fields play the OBJECT_KEY role too — they carry a field
          // name — but their VALUE is a sub-tree, so they join the name and parent columns only
          // and feed no value region. Leaving them out made them invisible to every anchored
          // scan: getObjectKeySlotsForNameKey("genres") answered EMPTY, so a predicate over an
          // array visited no records at all, and the record-ordinal linkage built from this same
          // column had no entry to attribute an array element to.
          //
          // The name is read through the STRUCTURAL accessor: these are a 12-field layout with
          // NAME_KEY at index 5 against the primitive-fused 9-field layout with it at index 3,
          // which is why widening isFusedObjectNamedKindId would decode them wrongly rather than
          // include them.
          okNameKeys[okCount] = page.getFusedStructuralNameKeyFromSlot(slot);
          okSlots[okCount] = slot;
          okParentKeys[okCount] = page.getObjectKeyParentKeyFromSlot(slot, pageKeyBase + slot);
          okCount++;
        }
      }

      if (count == 0 && okCount == 0 && stringCount == 0 && boolCount == 0) {
        return null;
      }
      final RegionTable table = disposableWriterTable == null
          ? new RegionTable()
          : disposableWriterTable;
      try {
        byte[] regionEncodeScratch = null;
        if (count > 0) {
          final byte numberTagKind = numberAllPathNodeKeysValid
              ? NumberRegion.TAG_KIND_PATH_NODE
              : NumberRegion.TAG_KIND_NAME;
          final int[] numberTagBuf = numberAllPathNodeKeysValid
              ? numberPathBuf
              : parBuf;
          final NumberRegion.Encoder numberEncoder = NUMBER_REGION_ENCODER.get();
          // This writer publishes the zone map immediately below, so the value region may leave its
          // per-tag directory there instead of carrying a second copy of it.
          int numberPayloadLength = numberEncoder.encodeInto(valBuf, numberTagBuf, count, numberTagKind, true);
          table.set(RegionTable.KIND_NUMBER, numberEncoder.output(), numberPayloadLength);
          final NumberRegion.Header header;
          if (numberPayloadLength > 0) {
            header = WRITER_NUMBER_HEADER_SCRATCH.get();
            // From the encoder, not from a parse of what it just wrote: the directory is what the
            // zone map is about to publish, and re-reading it out of the payload was only ever
            // possible because the payload repeated it.
            numberEncoder.directoryInto(header);
            // Lift the per-tag zone maps into their own independently readable region. Written here rather
            // than derived on read because the point is for a scan to see them WITHOUT touching the
            // number payload — see NumberZoneMapRegion.
            regionEncodeScratch = REGION_ENCODE_SCRATCH.get();
            final int zoneMapLength = NumberZoneMapRegion.encodeInto(header, regionEncodeScratch);
            if (zoneMapLength != NumberZoneMapRegion.ENCODE_FAILED) {
              table.set(RegionTable.KIND_NUMBER_ZONEMAP, regionEncodeScratch, zoneMapLength);
            } else {
              // No summary to hold the directory: re-encode the value region self-contained rather
              // than write one nothing can decode. Unreachable for a region with tags, and cheap
              // enough that proving it unreachable is not worth depending on.
              numberPayloadLength = numberEncoder.encodeInto(valBuf, numberTagBuf, count, numberTagKind, false);
              table.set(RegionTable.KIND_NUMBER, numberEncoder.output(), numberPayloadLength);
              table.set(RegionTable.KIND_NUMBER_ZONEMAP, null, 0);
              numberEncoder.directoryInto(header);
            }
          } else {
            header = null;
          }
          if (slotRegionAbsIdx != null && header != null) {
            final Int2IntOpenHashMap ranks = WRITER_REGION_RANK_SCRATCH.get();
            ranks.clear();
            ranks.defaultReturnValue(0);
            for (int e = 0; e < count; e++) {
              final int tagId = NumberRegion.lookupTag(header, numberTagBuf[e]);
              if (tagId < 0) {
                throw new SirixIOException("region index assignment: NUMBER tag " + numberTagBuf[e]
                    + " missing from the payload just encoded");
              }
              final int rank = ranks.get(numberTagBuf[e]);
              ranks.put(numberTagBuf[e], rank + 1);
              slotRegionAbsIdx[numberSlots[e]] = header.tagStart[tagId] + rank;
            }
          }
        }
        if (dblCount > 0) {
          // The double column's tagKind is chosen INDEPENDENTLY of the long region's: each column's
          // reader probes its own dictionary in its own key space, so one falling back to nameKey
          // tagging does not poison the other.
          final byte dblTagKind = doubleAllPathNodeKeysValid
              ? NumberRegion.TAG_KIND_PATH_NODE
              : NumberRegion.TAG_KIND_NAME;
          final byte[] dblPayload = DoubleRegion.encode(dblValBuf, dblDecUnscaled, dblDecScales,
              doubleAllPathNodeKeysValid
                  ? dblPathTags
                  : dblNameTags,
              dblOrdinals, dblCount, dblTagKind);
          if (dblPayload != null) {
            table.set(RegionTable.KIND_DOUBLE, dblPayload);
          }
        }
        if (okCount > 0) {
          if (regionEncodeScratch == null) {
            regionEncodeScratch = REGION_ENCODE_SCRATCH.get();
          }
          final int nameKeyPayloadLength =
              ObjectKeyNameKeyRegion.encodeInto(okNameKeys, okSlots, okCount, regionEncodeScratch);
          if (nameKeyPayloadLength != ObjectKeyNameKeyRegion.ENCODE_FAILED) {
            table.set(RegionTable.KIND_OBJECT_KEY_NAMEKEY, regionEncodeScratch, nameKeyPayloadLength);
            // Record linkage, in the same bitmap order as the nameKey column just written. Gated on
            // that column existing: the ordinals are indexed by position within it, so they are
            // meaningless — and unreadable — without it.
            final int ordinalsLength =
                RecordOrdinalRegion.encodeInto(okParentKeys, pageKeyBase, okCount, regionEncodeScratch);
            if (ordinalsLength != RecordOrdinalRegion.ENCODE_FAILED) {
              table.set(RegionTable.KIND_RECORD_ORDINAL, regionEncodeScratch, ordinalsLength);
            }
          }
        }
        // Publish the staged array elements, all or nothing (see the staging declaration).
        if (Boolean.getBoolean("sirix.diag.elemStage")) {
          System.err.println("[elem-stage] complete=" + stringRegionComplete + " usable=" + elemUsable
              + " count=" + elemCount);
        }
        if (stringRegionComplete && elemUsable && elemCount > 0) {
          if (stringEncName == null) {
            stringEncName = STRING_REGION_ENCODER.get();
            stringEncName.reset();
            stringEncPath = resetStringRegionPathCandidate(withPathSummary, page.globalStringDictionaries());
          }
          for (int e = 0; e < elemCount; e++) {
            // The orphan tag is deliberately negative and is NOT an unresolved path: treating it as
            // one would push the whole page onto name tagging, which is exactly the tagging that
            // cannot tell an array's elements from its siblings.
            if (elemPathTags[e] < 0 && elemPathTags[e] != StringRegion.TAG_ORPHAN_ELEMENTS) {
              stringAllPathNodeKeysValid = false;
            }
          }
          for (int e = 0; e < elemCount; e++) {
            final StringRegion.Encoder alternateStringEncoder = stringEncPath != null && stringAllPathNodeKeysValid
                ? stringEncPath
                : null;
            final byte[] elementValue = elemValues[e];
            stringEncName.addValueCopiedAndShareWith(elemNameTags[e], elementValue, 0, elementValue.length, false,
                alternateStringEncoder, elemPathTags[e]);
            stringSlots[stringCount] = elemSlots[e];
            stringNameTags[stringCount] = elemNameTags[e];
            stringPathTags[stringCount] = elemPathTags[e];
            stringCount++;
            stringStagedBytes += elementValue.length;
          }
        }
        boolean stringRegionWritten = false;
        if (stringRegionComplete && stringCount > 0) {
          final boolean pathTagged = stringAllPathNodeKeysValid && stringEncPath != null;
          final StringRegion.Encoder stringEncoder = pathTagged
              ? stringEncPath
              : stringEncName;
          final byte stringTagKind = pathTagged
              ? StringRegion.TAG_KIND_PATH_NODE
              : StringRegion.TAG_KIND_NAME;
          final int stringPayloadLength = stringEncoder.encodeInto(stringTagKind, elemUsable);
          if (stringPayloadLength > 0) {
            final byte[] stringPayload = stringEncoder.output();
            table.set(RegionTable.KIND_STRING, stringPayload, stringPayloadLength);
            stringRegionWritten = true;
            // Membership sketch over the dictionary. Written next to the column it summarises so a
            // string equality can rule the page out for a few hundred bytes instead of decompressing
            // the dictionary — see StringDictSketch. Entries are hashed as STORED, so FSST-encoded
            // pages get a sketch too and the probe side encodes its literal to match. Returns null
            // only when the page has no dictionary entries.
            //
            // NOT written when a tag was suppressed. A sketch negative is read as EXACT and for the
            // whole PAGE, not per tag, so a suppressed tag's strings — present on the page, absent
            // from this dictionary — would let the page rule itself out of a literal it holds.
            if (!anyStringTagSuppressed) {
              final StringRegion.Header sketchHeader = WRITER_STRING_HEADER_SCRATCH.get();
              sketchHeader.parseInto(table.payload(RegionTable.KIND_STRING));
              final byte[] sketch =
                  StringDictSketch.encodeFromStringRegion(stringPayload, stringPayloadLength, sketchHeader);
              if (sketch != null) {
                table.set(RegionTable.KIND_STRING_DICT_SKETCH, sketch);
              }
            }
            if (slotRegionAbsIdx != null) {
              final int[] winningTags = pathTagged
                  ? stringPathTags
                  : stringNameTags;
              final StringRegion.Header header = WRITER_STRING_HEADER_SCRATCH.get();
              header.parseInto(table.payload(RegionTable.KIND_STRING));
              final Int2IntOpenHashMap ranks = WRITER_REGION_RANK_SCRATCH.get();
              ranks.clear();
              ranks.defaultReturnValue(0);
              for (int e = 0; e < stringCount; e++) {
                final int tagId = StringRegion.lookupTag(header, winningTags[e]);
                if (tagId < 0) {
                  if (StringRegion.isTagSuppressed(header, winningTags[e])) {
                    // Deliberately absent: the slot keeps its value in the record heap, its region
                    // index stays -1, and the value-elision pre-scan therefore skips it.
                    continue;
                  }
                  throw new SirixIOException("region index assignment: STRING tag " + winningTags[e]
                      + " missing from the payload just encoded");
                }
                final int rank = ranks.get(winningTags[e]);
                ranks.put(winningTags[e], rank + 1);
                slotRegionAbsIdx[stringSlots[e]] = header.tagStart[tagId] + rank;
              }
            }
          }
        }
        if (PAGE_SECTION_DIAG) {
          // U2: how a page's string region ended up. Recorded AFTER the publish decision, because
          // with per-tag completeness a page holding overflow descriptors normally still writes its
          // region — the counter has to name the outcome, not the input. It counts a suppression only
          // when the page ended up with no region AND a descriptor is why: then every staged value
          // stayed inline in the record heap, which is exactly what the bytes below measure.
          PageSectionDiag.recordStringRegionOutcome(!stringRegionWritten && stringOverflowDescriptorCount > 0,
              stringOverflowDescriptorCount, stringCount, stringStagedBytes);
        }
        if (boolCount > 0) {
          final byte boolTagKind = booleanAllPathNodeKeysValid
              ? BooleanRegion.TAG_KIND_PATH_NODE
              : BooleanRegion.TAG_KIND_NAME;
          final int[] boolTags = booleanAllPathNodeKeysValid
              ? boolPathTags
              : boolNameTags;
          final byte[] boolPayload = BooleanRegion.encode(boolValBuf, boolTags, boolCount, boolTagKind);
          if (boolPayload != null && boolPayload.length > 0) {
            table.set(RegionTable.KIND_BOOLEAN, boolPayload);
            if (slotRegionAbsIdx != null) {
              final BooleanRegion.Header header = WRITER_BOOLEAN_HEADER_SCRATCH.get();
              header.parseInto(table.payload(RegionTable.KIND_BOOLEAN));
              final Int2IntOpenHashMap ranks = WRITER_REGION_RANK_SCRATCH.get();
              ranks.clear();
              ranks.defaultReturnValue(0);
              for (int e = 0; e < boolCount; e++) {
                final int tagId = BooleanRegion.lookupTag(header, boolTags[e]);
                if (tagId < 0) {
                  throw new SirixIOException(
                      "region index assignment: BOOLEAN tag " + boolTags[e] + " missing from the payload just encoded");
                }
                final int rank = ranks.get(boolTags[e]);
                ranks.put(boolTags[e], rank + 1);
                slotRegionAbsIdx[boolSlots[e]] = header.tagStart[tagId] + rank;
              }
            }
          }
        }
        if (table.isEmpty()) {
          if (disposableWriterTable == null) {
            table.close();
          }
          return null;
        }
        return table;
      } catch (final RuntimeException | Error failure) {
        // A normal table is born here and has no page owner until serializeKeyValueLeafPage installs
        // it. Region encoders may fail after earlier kinds already acquired native frames, so that
        // pre-publication failure must release the table rather than strand allocator slots. The
        // disposable table remains owned by its caller's try-with-resources.
        if (disposableWriterTable == null) {
          try {
            table.close();
          } catch (final RuntimeException | Error cleanupFailure) {
            if (cleanupFailure != failure) {
              failure.addSuppressed(cleanupFailure);
            }
          }
        }
        throw failure;
      }
    }

    /**
     * Read one {@link StructuralKeyColumnCodec} column out of the decoded metadata and decode it in
     * bulk into {@code values}.
     *
     * @param metadataStaging the decoded META bytes
     * @param blobPos offset of the column's four-byte length prefix
     * @param maxColumnBytes the bound the page's header implies for this column
     * @param populatedCount entries the column must cover
     * @param slotNodeKeys per-entry node keys, the predictor the codec encodes against
     * @param values receives one decoded key per entry
     * @param name the column's name, for the failure message
     * @return the offset one past the column's last byte
     */
    private static long readStructuralKeyColumn(final MemorySegment metadataStaging, final long blobPos,
        final int maxColumnBytes, final int populatedCount, final long[] slotNodeKeys, final long[] values,
        final String name) {
      final int b0 = metadataStaging.get(ValueLayout.JAVA_BYTE, blobPos) & 0xFF;
      final int b1 = metadataStaging.get(ValueLayout.JAVA_BYTE, blobPos + 1) & 0xFF;
      final int b2 = metadataStaging.get(ValueLayout.JAVA_BYTE, blobPos + 2) & 0xFF;
      final int b3 = metadataStaging.get(ValueLayout.JAVA_BYTE, blobPos + 3) & 0xFF;
      final int columnLength = (b0 << 24) | (b1 << 16) | (b2 << 8) | b3;
      if (columnLength < 0 || columnLength > maxColumnBytes - 4) {
        throw new SirixIOException("invalid " + name + " column length: " + columnLength);
      }
      byte[] scratch = STRUCTURAL_COLUMN_READ_SCRATCH.get();
      if (scratch.length < columnLength) {
        scratch = new byte[Math.max(columnLength, scratch.length * 2)];
        STRUCTURAL_COLUMN_READ_SCRATCH.set(scratch);
      }
      MemorySegment.copy(metadataStaging, ValueLayout.JAVA_BYTE, blobPos + 4, scratch, 0, columnLength);
      final int decoded = StructuralKeyColumnCodec.decodeAll(scratch, 0, values, slotNodeKeys);
      if (decoded != populatedCount) {
        throw new SirixIOException(
            "the " + name + " column covers " + decoded + " slots, the page has " + populatedCount);
      }
      return blobPos + 4 + columnLength;
    }

    /**
     * The tag whose half-open value range {@code [tagStart, tagStart + tagCount)} contains
     * {@code absIdx}, or -1. Linear over the tag dictionary, which is a handful of entries.
     */
    private static int tagIdForAbsoluteIndex(final int[] tagStart, final int[] tagCount, final int dictSize,
        final int absIdx) {
      for (int t = 0; t < dictSize; t++) {
        if (absIdx >= tagStart[t] && absIdx < tagStart[t] + tagCount[t]) {
          return t;
        }
      }
      return -1;
    }

    /**
     * Lever 3 inject pass: for each entry the value-elision section names, decode the value from its
     * PAX region at the entry's absolute region index and write the original payload bytes back into
     * the heap at the recorded offset+width.
     *
     * <p>
     * Per-kind dispatch:
     * <ul>
     * <li>NUMBER (49): {@link NumberRegion#decodeValueAt} → {@code [type:1][varint long]}</li>
     * <li>STRING (50): {@link StringRegion#decodeStringOffset}/{@code decodeStringLength} →
     * {@code [0:1][varint length][rawBytes]}</li>
     * <li>BOOLEAN (48): {@link BooleanRegion#decodeAt} → {@code [bool:1]}</li>
     * </ul>
     *
     * <p>
     * There is deliberately nothing here that walks the bitmap, counts ranks, or looks up tags from the
     * heap. The absolute index was assigned by the region build itself on the write side and travelled
     * with the entry, so the only agreement this pass depends on is "the region's value sequence is
     * what the writer indexed" — which is a tautology. The previous rank-walking design required
     * writer, region builder and reader to re-derive the same membership predicate independently, and
     * one slot's disagreement corrupted every subsequent slot's value on the page.
     *
     * <p>
     * STRING entries need the owning tag to locate the per-tag dictionary; it is found by scanning
     * {@code tagStart}/{@code tagCount} for the range containing the index — a handful of comparisons
     * against a small dictionary, once per elided string.
     *
     * <p>
     * HFT contract: zero alloc. Headers are thread-local scratches; STRING bytes are copied directly
     * from payload to heap via {@link MemorySegment#copy}.
     */
    /**
     * Re-inject the values the writer elided into PAX regions, for the slots in
     * {@code [fromSlot, toSlot]}.
     *
     * <p>
     * The range exists so a caller can inject only what it has expanded. Entries are ordered by slot,
     * so a contiguous run of entries is a contiguous run of slots and the two ways of naming a chunk's
     * records agree; a whole-page caller passes the whole slot space.
     */
    /**
     * Re-inject a value whose tag stores GLOBAL IDS, by resolving the id through the dictionary.
     *
     * <p>
     * The tag handed to the resolver is the tag's VALUE — the path node key from {@code parentDict}
     * — not its local index on this page. The projection's anchors are keyed by path node key, and a
     * local index means nothing outside the page it was parsed from.
     * </p>
     *
     * <p>
     * A null answer is a hard failure and not an empty value. The resolver returns null when the
     * page's anchor is refused — a dictionary that shrank under a reused key, or one this tag does
     * not resolve against — and substituting anything there converts an unreadable page into a
     * wrong one, which is the failure this whole format is arranged to prevent.
     * </p>
     *
     * <p>
     * The width check below is a free round-trip witness: the elided slot recorded the ORIGINAL
     * value's width, so a resolver that returns a different value fails here rather than reaching
     * the user. It is not complete — a same-length wrong value passes — but it costs nothing.
     * </p>
     */
    private static void injectGlobalString(final MemorySegment slottedPage,
        final StringRegion.Header stringHeader, final int tagId, final int dictId, final int slot,
        final long valueAbsOff, final int valueWidth, final byte storedFlag,
        final @Nullable ResolvedGlobalStrings resolved) {
      if (storedFlag != 0) {
        // The encoder refuses to convert a tag with any FSST-encoded entry, because a stored form
        // is not a value and cannot be looked up. A global tag carrying the compressed flag is
        // therefore impossible unless something upstream is wrong.
        throw new SirixIOException("value-elision: global STRING slot " + slot + " carries compressed flag "
            + storedFlag + ", which the encoder never produces");
      }
      if (resolved == null) {
        // Belt to the pre-pass's braces. refuseUnresolvedGlobalTags has already run over the whole
        // region by the time any slot is touched, so reaching this means a NEW expansion route
        // skipped it -- exactly the class of change that a per-slot check catches and a pre-pass
        // alone would not.
        throw new SirixIOException("value-elision: slot " + slot + " holds a global-dictionary tag but this page "
            + "carries no resolved value table, and the pre-pass that must refuse such a page did not run on this "
            + "expansion route");
      }
      // A plain array index. The resolution walked the dictionary once, for the whole page, at a
      // site that holds a reader; expansion runs under synchronized(page) and must not descend a
      // trie from there. See ResolvedGlobalStrings for why the page holds bytes and not a resolver.
      final byte[] value = resolved.value(tagId, stringHeader.parentDict[tagId], dictId);
      slottedPage.set(ValueLayout.JAVA_BYTE, valueAbsOff, (byte) 0);
      final int lenWidth = DeltaVarIntCodec.writeSignedToSegment(slottedPage, valueAbsOff + 1, value.length);
      MemorySegment.copy(value, 0, slottedPage, ValueLayout.JAVA_BYTE, valueAbsOff + 1 + lenWidth, value.length);
      final int actualWidth = 1 + lenWidth + value.length;
      if (actualWidth != valueWidth) {
        throw new SirixIOException("value-elision: global STRING width mismatch at slot " + slot + ": expected="
            + valueWidth + " actual=" + actualWidth + " for tag " + stringHeader.parentDict[tagId] + " global id "
            + resolved.globalIdAt(tagId, dictId) + " against dictionary " + stringHeader.tagDictionaryKey[tagId]
            + " -- the dictionary returned a different value than the one this page elided");
      }
    }

    /**
     * Refuse a page whose string region carries a global tag when it is being expanded EAGERLY.
     *
     * <p>
     * The trie lane requires lazy chunks. That is not a policy choice: a global tag's value lives
     * behind the NamePage sub-trie and is reachable only through a reader, deserialization has none
     * and cannot be given one without recursing into page decodes, and the page object that would
     * carry a resolver does not exist yet. So the value cannot be produced here by any means.
     * </p>
     *
     * <p>
     * Refusing beats the alternative, which is leaving those slots elided and handing back records
     * whose values are absent -- a record with no value is not a record with an empty value, and
     * that substitution is the failure the whole anchor design exists to prevent.
     * </p>
     */
    private static void refuseGlobalTagsOnEagerPath(final RegionTable regionTable, final long recordPageKey) {
      final MemorySegment stringPayload = regionTable.payload(RegionTable.KIND_STRING);
      if (stringPayload == null || stringPayload.byteSize() == 0) {
        return;
      }
      final StringRegion.Header header = STRING_HEADER_SCRATCH.get();
      header.parseInto(stringPayload);
      for (int t = 0; t < header.parentDictSize; t++) {
        if (header.tagGlobal[t]) {
          throw new SirixIOException("record page " + recordPageKey + " carries a global-dictionary tag ("
              + header.parentDict[t] + ") but is being expanded EAGERLY, where no dictionary is reachable. "
              + "Pages using the trie lane must be read through deserializePageLazily.");
        }
      }
    }

    /**
     * Whether a string region marks any tag as storing global dictionary ids.
     *
     * <p>
     * Read once at deserialization and remembered on the page, so the reader's "does this page owe a
     * resolution" test is a field compare rather than a header parse on a per-record path.
     * </p>
     */
    static boolean stringRegionHasGlobalTags(final @Nullable RegionTable regionTable) {
      if (regionTable == null) {
        return false;
      }
      final MemorySegment stringPayload = regionTable.payload(RegionTable.KIND_STRING);
      if (stringPayload == null || stringPayload.byteSize() == 0) {
        return false;
      }
      final StringRegion.Header header = STRING_HEADER_SCRATCH.get();
      header.parseInto(stringPayload);
      for (int t = 0; t < header.parentDictSize; t++) {
        if (header.tagGlobal[t]) {
          return true;
        }
      }
      return false;
    }

    /**
     * Refuse a page whose global tags were never resolved -- BEFORE any slot is touched.
     *
     * <p>
     * A pre-pass rather than a check at the slot that meets the problem, and the difference is not
     * tidiness. Expansion writes values into the heap as it goes, so a throw partway through leaves
     * the page half-injected: some slots hold their values, the rest hold the placeholder zeros the
     * expansion started from, and nothing on the page says which is which. A caller that catches the
     * exception and retries -- or one that reads a slot the loop had already passed -- then reads a
     * page that looks whole and is not. Refusing before the first write means the page is exactly as
     * it was.
     * </p>
     *
     * <p>
     * It also names the PAGE. A slot number identifies nothing a reader can act on; the page key and
     * the tag are what say which column and which install went missing.
     * </p>
     */
    static void refuseUnresolvedGlobalTags(final @Nullable RegionTable regionTable, final long recordPageKey,
        final @Nullable ResolvedGlobalStrings resolved) {
      if (regionTable == null) {
        return;
      }
      final MemorySegment stringPayload = regionTable.payload(RegionTable.KIND_STRING);
      if (stringPayload == null || stringPayload.byteSize() == 0) {
        return;
      }
      final StringRegion.Header stringHeader = STRING_HEADER_SCRATCH.get();
      stringHeader.parseInto(stringPayload);
      for (int t = 0; t < stringHeader.parentDictSize; t++) {
        if (!stringHeader.tagGlobal[t]) {
          continue;
        }
        if (resolved == null) {
          throw new SirixIOException("record page " + recordPageKey + " carries a global-dictionary tag ("
              + stringHeader.parentDict[t] + ", dictionary " + stringHeader.tagDictionaryKey[t] + " at "
              + stringHeader.tagDictionaryEntryCount[t] + " entries) but no reader resolved its values before "
              + "expansion. The trie lane requires a reader-held resolution pass; expansion itself cannot walk the "
              + "dictionary, because it runs under the page monitor with no reader on the stack.");
        }
        // Probe entry 0 of every global tag. It proves the tag is IN the table under the value the
        // header carries -- which is what a wrong index or a re-encoded region would break -- and it
        // costs one lookup per tag rather than one per value. A tag is resolved whole or not at all
        // (ResolvedGlobalStrings.Builder refuses a hole), so entry 0 standing means the rest do.
        if (stringHeader.tagStringDictSize[t] > 0) {
          try {
            resolved.value(t, stringHeader.parentDict[t], 0);
          } catch (final RuntimeException mismatch) {
            throw new SirixIOException("record page " + recordPageKey + " carries global-dictionary tag "
                + stringHeader.parentDict[t] + ", which its resolved value table does not cover", mismatch);
          }
        }
      }
    }

    private static void injectValueElidedRecords(final MemorySegment slottedPage, final int valueElidedCount,
        final short[] valueElidedSlots, final byte[] valueElidedTypes, final int[] valueElidedWidths,
        final int[] valueElidedAbsIdx, final RegionTable regionTable, final int fromSlot, final int toSlot,
        final @Nullable ResolvedGlobalStrings resolved) {
      final MemorySegment numberPayload = regionTable.payload(RegionTable.KIND_NUMBER);
      final MemorySegment stringPayload = regionTable.payload(RegionTable.KIND_STRING);
      final MemorySegment booleanPayload = regionTable.payload(RegionTable.KIND_BOOLEAN);

      NumberRegion.Header numberHeader = null;
      // Non-null only when the number region is delta-encoded: all values are bulk-decoded once
      // (O(n)) so per-entry access is O(1) instead of the O(index) delta prefix-sum that would
      // make this loop O(n²).
      long[] numberValues = null;
      StringRegion.Header stringHeader = null;
      BooleanRegion.Header booleanHeader = null;

      for (int e = 0; e < valueElidedCount; e++) {
        final int slot = valueElidedSlots[e] & 0xFFFF;
        if (slot < fromSlot || slot > toSlot) {
          continue;
        }
        final int valueWidth = valueElidedWidths[e];
        final int absIdx = valueElidedAbsIdx[e];
        final int slotHeapOffset = PageLayout.getDirHeapOffset(slottedPage, slot);
        final long recordBase = PageLayout.HEAP_START + slotHeapOffset;
        final int kindIdRead = slottedPage.get(ValueLayout.JAVA_BYTE, recordBase) & 0xFF;
        final int fcRead = NodeFieldLayout.fieldCountForKind(kindIdRead);
        final int valueOff =
            slottedPage.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDNUM_PAYLOAD) & 0xFF;
        final long valueAbsOff = recordBase + 1 + fcRead + valueOff;

        if (kindIdRead == KeyValueLeafPage.FUSED_OBJECT_NAMED_NUMBER_KIND_ID) {
          if (numberPayload == null || numberPayload.byteSize() == 0) {
            throw new SirixIOException("value-elision: NUMBER region missing for elided slot " + slot);
          }
          if (numberHeader == null) {
            numberHeader = NUMBER_HEADER_SCRATCH.get();
            // The per-tag directory may live in the zone map. A page is read whole here — the region
            // table came from RegionTable.read with every kind — so the pair is always in hand.
            numberHeader.parseInto(numberPayload, regionTable.payload(RegionTable.KIND_NUMBER_ZONEMAP));
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
          if (absIdx >= numberHeader.count) {
            throw new SirixIOException("value-elision: NUMBER index out of bounds at slot " + slot + ": absIdx="
                + absIdx + " count=" + numberHeader.count);
          }
          final byte typeByte = valueElidedTypes[e];
          final long longVal = numberValues != null
              ? numberValues[absIdx]
              : NumberRegion.decodeValueAt(numberPayload, numberHeader, absIdx);
          slottedPage.set(ValueLayout.JAVA_BYTE, valueAbsOff, typeByte);
          final int actualWidth;
          if (typeByte == NUMBER_TYPE_INTEGER) {
            actualWidth = 1 + DeltaVarIntCodec.writeSignedToSegment(slottedPage, valueAbsOff + 1, (int) longVal);
          } else {
            // NUMBER_TYPE_LONG (3)
            actualWidth = 1 + DeltaVarIntCodec.writeSignedLongToSegment(slottedPage, valueAbsOff + 1, longVal);
          }
          if (actualWidth != valueWidth) {
            throw new SirixIOException("value-elision: NUMBER width mismatch at slot " + slot + ": expected="
                + valueWidth + " actual=" + actualWidth + " type=" + typeByte + " value=" + longVal);
          }
        } else if (kindIdRead == KeyValueLeafPage.FUSED_OBJECT_NAMED_STRING_KIND_ID) {
          if (stringPayload == null || stringPayload.byteSize() == 0) {
            throw new SirixIOException("value-elision: STRING region missing for elided slot " + slot);
          }
          if (stringHeader == null) {
            stringHeader = STRING_HEADER_SCRATCH.get();
            stringHeader.parseInto(stringPayload);
          }
          if (absIdx >= stringHeader.count) {
            throw new SirixIOException("value-elision: STRING index out of bounds at slot " + slot + ": absIdx="
                + absIdx + " count=" + stringHeader.count);
          }
          final int tagId =
              tagIdForAbsoluteIndex(stringHeader.tagStart, stringHeader.tagCount, stringHeader.parentDictSize, absIdx);
          if (tagId < 0) {
            throw new SirixIOException(
                "value-elision: no STRING tag range contains index " + absIdx + " at slot " + slot);
          }
          final int dictId = StringRegion.decodeDictIdAt(stringPayload, stringHeader, absIdx);
          if (stringHeader.tagGlobal[tagId]) {
            injectGlobalString(slottedPage, stringHeader, tagId, dictId, slot, valueAbsOff, valueWidth,
                valueElidedTypes[e], resolved);
            continue;
          }
          final int strOff = StringRegion.decodeStringOffset(stringPayload, stringHeader, tagId, dictId);
          final int strLen = StringRegion.decodeStringLength(stringPayload, stringHeader, tagId, dictId);
          // Reconstruct heap layout: [isCompressed:1][length:varint][storedBytes]. The type byte
          // carries the original compressed flag (0 raw, 1 FSST); the dictionary bytes are the
          // stored form either way, so this is a verbatim copy with no decode — deliberately,
          // since no symbol table is reachable at deserialize time.
          final byte storedFlag = valueElidedTypes[e];
          if (storedFlag != 0 && storedFlag != 1) {
            // Anything else written here would silently read as "raw" and hand back garbage
            // bytes as the value; a corrupt entry must fail at the page, not at the user.
            throw new SirixIOException("value-elision: STRING slot " + slot + " carries flag byte " + storedFlag
                + ", expected 0 (raw) or 1 (FSST)");
          }
          slottedPage.set(ValueLayout.JAVA_BYTE, valueAbsOff, storedFlag);
          final int lenWidth = DeltaVarIntCodec.writeSignedToSegment(slottedPage, valueAbsOff + 1, strLen);
          // Segment-to-segment: stringPayload is the region payload, which is natively backed, so
          // the array-source overload this used to bind to no longer applies.
          MemorySegment.copy(stringPayload, strOff, slottedPage, valueAbsOff + 1 + lenWidth, strLen);
          final int actualWidth = 1 + lenWidth + strLen;
          if (actualWidth != valueWidth) {
            throw new SirixIOException("value-elision: STRING width mismatch at slot " + slot + ": expected="
                + valueWidth + " actual=" + actualWidth + " strLen=" + strLen);
          }
        } else if (kindIdRead == KeyValueLeafPage.FUSED_OBJECT_NAMED_BOOLEAN_KIND_ID) {
          if (booleanPayload == null || booleanPayload.byteSize() == 0) {
            throw new SirixIOException("value-elision: BOOLEAN region missing for elided slot " + slot);
          }
          if (booleanHeader == null) {
            booleanHeader = BOOLEAN_HEADER_SCRATCH.get();
            booleanHeader.parseInto(booleanPayload);
          }
          if (absIdx >= booleanHeader.count) {
            throw new SirixIOException("value-elision: BOOLEAN index out of bounds at slot " + slot + ": absIdx="
                + absIdx + " count=" + booleanHeader.count);
          }
          final boolean value = BooleanRegion.decodeAt(booleanPayload, booleanHeader, absIdx);
          slottedPage.set(ValueLayout.JAVA_BYTE, valueAbsOff, (byte) (value
              ? 1
              : 0));
          if (valueWidth != 1) {
            throw new SirixIOException(
                "value-elision: BOOLEAN width mismatch at slot " + slot + ": expected=" + valueWidth);
          }
        } else {
          throw new SirixIOException(
              "value-elision names slot " + slot + ", whose kind " + kindIdRead + " has no fused-primitive payload");
        }
      }
    }

    /**
     * Lever 4 inject pass: walks the bitmap, looks up each elided fused {@code OBJECT_NAMED_*} (kindIds
     * 48-51) slot's nameKey via {@link ObjectKeyNameKeyRegion#nameKeyForSlot(byte[], int)}, and
     * re-encodes the {@code [signed-varint nameKey]} into the heap at the recorded
     * {@code nameKeyOffs[entryIdx]} offset and {@code nameKeyWidths[entryIdx]} width. Called BEFORE the
     * value-elision inject because that pass reads the slot's nameKey field when
     * {@link NumberRegion#TAG_KIND_NAME} (or the corresponding STRING/BOOLEAN) is in effect.
     *
     * <p>
     * Width round-trip is verified — {@link DeltaVarIntCodec#computeSignedEncodedWidth} is
     * deterministic per {@code int} value, so the width recorded by the writer must exactly match what
     * we re-encode here. Mismatch implies a corrupt page or region.
     *
     * <p>
     * HFT contract: zero alloc on hot path. Region payload is held by the regionTable and dispatched
     * per slot via a single primitive-int return.
     */
    /**
     * Re-inject the name keys the writer elided into the name-key region, for the entries in
     * {@code [fromEntry, toEntry)}.
     *
     * <p>
     * The width and offset arrays are entry-indexed, so the walk still counts every populated slot; it
     * just does the work for the entries the caller asks about. A whole-page caller passes the whole
     * entry range.
     */
    private static void injectNameKeyElidedRecords(final MemorySegment slottedPage, final int populatedCount,
        final short[] nameKeyOffs, final byte[] nameKeyWidths, final RegionTable regionTable, final int fromEntry,
        final int toEntry) {
      final MemorySegment nameKeyPayload = regionTable.payload(RegionTable.KIND_OBJECT_KEY_NAMEKEY);
      if (nameKeyPayload == null || nameKeyPayload.byteSize() == 0) {
        throw new SirixIOException("name-key elision: ObjectKeyNameKeyRegion missing for elided page");
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
          if (recordedWidth > 0 && entryIdx >= fromEntry && entryIdx < toEntry) {
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
            final int actualWidth = DeltaVarIntCodec.writeSignedToSegment(slottedPage, writePos, nameKey);
            if (actualWidth != recordedWidth) {
              throw new SirixIOException("name-key elision: width mismatch at slot " + slot + ": expected="
                  + recordedWidth + " actual=" + actualWidth + " nameKey=" + nameKey + " kindId=" + kindIdRead);
            }
          }
          entryIdx++;
          word &= word - 1;
        }
      }
    }

    /**
     * Emit the page's FSST symbol-table reference.
     *
     * <p>
     * Two cases, discriminated by the leading int: {@code 0} — the page has no symbol table and its
     * strings are stored raw; {@link #FSST_SYMBOL_TABLE_REFERENCE_MARKER} — a {@code varLong}
     * dictionary id follows, naming a table stored as a record under
     * {@link NamePage#fsstSymbolTableOffset}. Tables are never embedded in page bytes: a table runs to
     * a couple of kilobytes and is identical across every page of a revision, so embedding charged that
     * much per page — several megabytes across a large resource — on top of re-parsing it per page on
     * read. The id costs two or three bytes.
     *
     * <p>
     * A page holding table bytes without an id is refused outright. Its compressed slots would be
     * written with no on-disk trace of which symbols they were encoded against — readable in this
     * process, garbage after a reopen — so the write fails at the moment the state exists rather than
     * the read failing silently later.
     */
    private static void writeFsstSymbolTable(final BytesOut<?> sink, final KeyValueLeafPage page,
        final boolean chunkedBody) {
      final long symbolTableId = page.getFsstSymbolTableId();
      if (symbolTableId != KeyValueLeafPage.NO_FSST_SYMBOL_TABLE_ID) {
        // A chunked page already carries the id in its body prefix; emitting it again here would
        // put a second, unreferenced copy behind the region table.
        if (!chunkedBody) {
          sink.writeInt(FSST_SYMBOL_TABLE_REFERENCE_MARKER);
          Utils.putVarLong(sink, symbolTableId);
        }
        return;
      }
      final byte[] fsstSymbolTable = page.getFsstSymbolTable();
      if (fsstSymbolTable != null && fsstSymbolTable.length > 0) {
        throw new SirixIOException("page " + page.getPageKey() + " holds a symbol table without a"
            + " dictionary id; serializing it would strand its compressed strings with no way to"
            + " name the symbols they were encoded against");
      }
      if (!chunkedBody) {
        sink.writeInt(0);
      }
    }

    private static void compressAndCache(final ResourceConfiguration resourceConfig, final BytesOut<?> sink,
        final KeyValueLeafPage keyValueLeafPage) {
      final int byteHandlerInputLength = Math.toIntExact(sink.writePosition());
      final BytesIn<?> uncompressedBytes = sink.bytesForRead();
      final ByteHandlerPipeline pipeline = resourceConfig.byteHandlePipeline;

      if (pipeline.supportsMemorySegments() && uncompressedBytes instanceof MemorySegmentBytesIn segmentIn) {
        final MemorySegment uncompressed = segmentIn.getSource().asSlice(0, byteHandlerInputLength);
        final MemorySegment compressed = pipeline.compress(uncompressed);
        keyValueLeafPage.setCompressedSegment(compressed, byteHandlerInputLength);
      } else {
        final byte[] uncompressedArray = uncompressedBytes.toByteArray();
        final byte[] compressedPage = compressViaStream(pipeline, uncompressedArray);
        keyValueLeafPage.setBytes(Bytes.wrapForWrite(compressedPage), byteHandlerInputLength);
      }
    }

    /**
     * When the outer byte-handler pipeline is empty and the inner heap LZ4 still runs, we leave a lot
     * of structural overhead (compact dir, region table bytes, PAX dictionaries) uncompressed on disk —
     * that was what the old outer LZ4 used to mop up. As an interim middle-ground between "pure
     * structural" and "outer LZ4", we re-use the inner heap LZ4 to wrap the remaining un-compressed
     * sink bytes for the {@code -Dsirix.compression=none} path.
     *
     * <p>
     * Experimental: the write path never shipped this; we are measuring. It is still structural in the
     * sense that the LZ4 pass happens inside the page serializer, not as a pipelined ByteHandler, so
     * the no-outer- pipeline contract is preserved and the cold-path decompress CPU bill is the same
     * single LZ4 pass we already pay.
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
          final Int2LongMap maxHotPageKeys = PageKind.deserializeMaxHotPageKeys(source);
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

      // Positional: a count, then one value per offset from 0 upwards. That only round-trips if
      // the offsets in use are gapless, which getDictionaryOffsetCount() checks rather than
      // assumes — a gap would otherwise write the wrong count, fabricate a zero for the missing
      // offset and drop the highest one, silently emptying a dictionary on reload.
      final int maxNodeKeySize = namePage.getDictionaryOffsetCount();
      sink.writeInt(maxNodeKeySize);
      for (int i = 0; i < maxNodeKeySize; i++) {
        final long keys = namePage.getMaxNodeKey(i);
        sink.writeLong(keys);
      }

      PageKind.serializeMaxHotPageKeys(sink, namePage.maxHotPageKeysForSerialization());

      sink.writeInt(namePage.getNumberOfArrays());

      final int currentMaxLevelOfIndirectPagesSize = namePage.getCurrentMaxLevelOfIndirectPagesSize();
      sink.writeInt(currentMaxLevelOfIndirectPagesSize);
      for (int i = 0; i < currentMaxLevelOfIndirectPagesSize; i++) {
        sink.writeByte((byte) namePage.getCurrentMaxLevelOfIndirectPages(i));
      }

      // Approach B: per-dictionary live entry node-keys (Roaring) for O(live) reconstruction.
      final int liveEntryNodeKeySize = namePage.getDictionaryOffsetCount();
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
            // noinspection DataFlowIssue
            user = new User(source.readUtf8(), UUID.fromString(source.readUtf8()));
          }

          return new RevisionRootPage(delegate, revision, maxNodeKeyInDocumentIndex, maxNodeKeyInChangedNodesIndex,
              maxNodeKeyInRecordToRevisionsIndex, revisionTimestamp, commitMessage,
              currentMaxLevelOfDocumentIndexIndirectPages, currentMaxLevelOfChangedNodesIndirectPages,
              currentMaxLevelOfRecordToRevisionsIndirectPages, user);
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

      // initial variables from RevisionRootPage, to serialize
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
      final long revisionTimestamp = commitTimestamp == null
          ? Instant.now().toEpochMilli()
          : commitTimestamp.toEpochMilli();
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

          final Int2LongMap maxHotPageKeys = PageKind.deserializeMaxHotPageKeys(source);

          return new CASPage(delegate, maxHotPageKeys);
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

      PageKind.serializeMaxHotPageKeys(sink, casPage.maxHotPageKeysForSerialization());
    }
  },

  /**
   * {@link OverflowPage}.
   */
  OVERFLOWPAGE((byte) 9, OverflowPage.class) {
    @Override
    public Page deserializePage(final ResourceConfiguration resourceConfiguration, final BytesIn<?> source,
        final SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      final byte flags = readVersionAndFlagsAllowing(source, FLAG_OVERFLOW_PAYLOAD_COMPRESSED);
      final int length = source.readInt();
      if (length < 0) {
        throw new IllegalStateException("Corrupt OverflowPage length " + length);
      }
      if ((flags & FLAG_OVERFLOW_PAYLOAD_COMPRESSED) != 0) {
        return deserializeCompressedOverflowPayload(source, length);
      }
      // Corruption guard bounded by what the source actually holds, NOT by a fixed ceiling: an
      // overflow page legitimately carries an arbitrarily large node record, so any absolute cap
      // would reject valid committed data. A garbled length, by contrast, cannot be covered by
      // the remaining bytes — so this catches it before the allocation without ever rejecting an
      // intact page.
      final long remaining = source.remaining();
      if (length > remaining) {
        throw new IllegalStateException(
            "Corrupt OverflowPage length " + length + " (only " + remaining + " bytes remain in the source)");
      }
      final byte[] data = new byte[length];
      source.read(data);

      // Store as byte array to avoid memory leaks from Arena.global()
      return new OverflowPage(data);
    }

    @Override
    public void serializePage(final ResourceConfiguration resourceConfig, final BytesOut<?> sink, final Page page,
        SerializationType type) {
      OverflowPage overflowPage = (OverflowPage) page;
      final int dataLength = overflowPage.dataLength();

      // Elect a codec BEFORE anything is written: the envelope flag has to say which layout follows,
      // and whether compression pays is only known once it has been tried.
      final OverflowPayloadCodecResult elected = OVERFLOW_ENCODE_RESULT.get();
      final boolean compressed = OVERFLOW_PAYLOAD_COMPRESSION_ENABLED && dataLength >= OVERFLOW_COMPRESSION_MIN_BYTES
          && electOverflowPayloadCodec(overflowPage.payloadSegmentForSerializer(),
              overflowPage.payloadOffsetForSerializer(), dataLength, elected);

      sink.writeByte(OVERFLOWPAGE.id);
      if (!compressed) {
        // Byte-for-byte the layout this page has always had, flags included, so the kill switch and
        // every pre-existing resource stay readable and comparable.
        writeVersionAndFlags(sink);
        // A staged projection page is a bounded view into the writer's fixed native reservoir. Write
        // that view directly: materialising it as byte[] here would recreate the promoted-garbage
        // slope the native staging path exists to remove. Ordinary overflow records remain heap
        // backed and take the unchanged byte[] branch inside writeDataTo().
        sink.writeInt(dataLength);
        overflowPage.writeDataTo(sink);
        return;
      }
      writeVersionAndFlags(sink, FLAG_OVERFLOW_PAYLOAD_COMPRESSED);
      sink.writeInt(dataLength);
      sink.writeInt(elected.storedLength);
      sink.writeByte((byte) elected.codec);
      sink.write(elected.stored, 0, elected.storedLength);
    }
  },

  /**
   * {@link PathPage}.
   */
  PATHPAGE((byte) 10, PathPage.class) {
    @Override
    public Page deserializePage(ResourceConfiguration resourceConfiguration, BytesIn<?> source, SerializationType type,
        final ByteHandler.DecompressionResult decompressionResult) {
      final BinaryEncodingVersion binaryVersion = readVersionAndFlags(source);
      switch (binaryVersion) {
        case V0 -> {
          final Page delegate = PageUtils.createDelegate(source, type);

          final Int2LongMap maxHotPageKeys = PageKind.deserializeMaxHotPageKeys(source);

          return new PathPage(delegate, maxHotPageKeys);
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

      PageKind.serializeMaxHotPageKeys(sink, pathPage.maxHotPageKeysForSerialization());
    }
  },

  /**
   * {@link PathPage}.
   */
  DEWEYIDPAGE((byte) 11, DeweyIDPage.class) {
    @Override
    public Page deserializePage(ResourceConfiguration resourceConfiguration, BytesIn<?> source, SerializationType type,
        final ByteHandler.DecompressionResult decompressionResult) {
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
    public Page deserializePage(ResourceConfiguration resourceConfiguration, BytesIn<?> source, SerializationType type,
        final ByteHandler.DecompressionResult decompressionResult) {
      final byte envelopeFlags = readVersionAndFlagsAllowing(source, HOTLeafPage.FLAG_OVERFLOW_PAGE_REFS);

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

      // Read slot memory (zero-copy when possible). The ownership variables deliberately cover the
      // entire acquisition-to-return interval: the optional side-reference trailer is parsed only
      // after the leaf exists, and corrupt/truncated trailer bytes must close that unpublished leaf
      // instead of stranding either its transferred decompression frame or its copying-path frame.
      final MemorySegmentAllocator allocator = Allocators.getInstance();
      final boolean canZeroCopy = decompressionResult != null && source instanceof MemorySegmentBytesIn;
      Runnable acquiredFrameOwner = null;
      HOTLeafPage page = null;
      try {
        final MemorySegment slotMemory;
        // Base of the allocation backing slotMemory, when slotMemory is only a slice of it. The
        // allocator's live-slot map is keyed by the address it handed out, so the page has to be
        // told the base or its optimistic read stamps silently bind to "not slot-backed" and stop
        // validating anything. Null on the copying path, where slotMemory IS the allocation.
        final MemorySegment stampBase;
        if (canZeroCopy) {
          final MemorySegment sourceSegment = ((MemorySegmentBytesIn) source).getSource();
          slotMemory = sourceSegment.asSlice(source.position(), usedSlotMemorySize);
          stampBase = sourceSegment;
          source.skip(usedSlotMemorySize);
          acquiredFrameOwner = Objects.requireNonNull(decompressionResult.transferOwnership(),
              "HOT-leaf decompression-frame ownership was already transferred");
        } else {
          stampBase = null;
          // Construct the owner before entering the allocator. No heap allocation is then needed
          // between a successful native-frame acquisition and publication into HOTLeafPage.
          final HOTLeafAllocatedFrameOwner allocatedFrameOwner = new HOTLeafAllocatedFrameOwner(allocator);
          acquiredFrameOwner = allocatedFrameOwner;
          slotMemory = Objects.requireNonNull(allocator.allocate(HOTLeafPage.DEFAULT_SIZE));
          allocatedFrameOwner.bind(slotMemory);
          if (source instanceof MemorySegmentBytesIn msSource) {
            MemorySegment.copy(msSource.getSource(), source.position(), slotMemory, 0, usedSlotMemorySize);
            source.skip(usedSlotMemorySize);
          } else {
            final byte[] slotData = new byte[usedSlotMemorySize];
            source.read(slotData);
            MemorySegment.copy(slotData, 0, slotMemory, java.lang.foreign.ValueLayout.JAVA_BYTE, 0,
                usedSlotMemorySize);
          }
        }

        page = new HOTLeafPage(recordPageKey, revision, indexType, slotMemory, acquiredFrameOwner, slotOffsets,
            entryCount, usedSlotMemorySize, commonPrefix, commonPrefixLen);
        // Before the page is published: without this the zero-copy leaf's stamp binds to the slice's
        // address, which is not an allocator key, and every validateStamp degrades to a closed-flag
        // check for the page's whole lifetime.
        page.setStampBaseSegment(stampBase);
        page.setCompleteDump(completeDump);
        if ((envelopeFlags & HOTLeafPage.FLAG_OVERFLOW_PAGE_REFS) != 0) {
          deserializeSegmentRefs(source, page);
        }
        return page;
      } catch (final RuntimeException | Error failure) {
        try {
          if (page != null) {
            // The constructor has returned: the page is the sole frame owner, including any side
            // references already decoded before a later trailer read failed.
            page.close();
          } else if (acquiredFrameOwner != null) {
            // Construction did not publish ownership. Release the acquired/transferred frame here;
            // DecompressionResult.close() is a no-op after transfer, so this remains exactly-once.
            acquiredFrameOwner.run();
          }
        } catch (final RuntimeException | Error cleanupFailure) {
          HOTLeafPage.addSuppressedSafely(failure, cleanupFailure);
        }
        throw failure;
      }
    }

    @Override
    public void serializePage(ResourceConfiguration resourceConfig, BytesOut<?> sink, Page page,
        SerializationType type) {
      final HOTLeafPage hotLeaf = (HOTLeafPage) page;
      final VersioningType versioningType = resourceConfig.versioningType;
      final boolean sparseEmit =
          versioningType != VersioningType.FULL && hotLeaf.getCompletePageRef() != null && hotLeaf.hasDirty();

      // Segment-reference side map (projection segment pages, see
      // docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §2.3/§2.4). EVERY fragment —
      // sparse or full — serializes the COMPLETE map: the writer-side page
      // always holds the authoritative current map (copy() carries it across
      // CoW; puts/removes mutate it), so the newest fragment is authoritative
      // and the fragment merge never unions older fragments' refs (which
      // would resurrect removed segments).
      final boolean hasSegmentRefs = hotLeaf.segmentRefCount() > 0;

      sink.writeByte(HOT_LEAF_PAGE.id);
      writeVersionAndFlags(sink, hasSegmentRefs
          ? HOTLeafPage.FLAG_OVERFLOW_PAGE_REFS
          : 0);

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
            ? dirtyCount | 0x80000000
            : dirtyCount;
        sink.writeInt(encodedDirtyCount);
        sink.writeInt(dirtyUsed);

        if (dirtyCount == 0) {
          if (hasSegmentRefs) {
            serializeSegmentRefs(sink, hotLeaf);
          }
          return;
        }

        serializeSparseHOTLeafEntries(sink, hotLeaf, dirtyCount, dirtyUsed);
        if (hasSegmentRefs) {
          serializeSegmentRefs(sink, hotLeaf);
        }
        return;
      }

      final int encodedFullCount = hotLeaf.isCompleteDump()
          ? hotLeaf.getEntryCount() | 0x80000000
          : hotLeaf.getEntryCount();
      sink.writeInt(encodedFullCount);
      sink.writeInt(hotLeaf.getUsedSlotsSize());

      // Write slot offsets
      int entryCount = hotLeaf.getEntryCount();
      for (int i = 0; i < entryCount; i++) {
        sink.writeInt(hotLeaf.getSlotOffset(i));
      }

      // Copy the exact live prefix straight into the sink. The warm pinned-spill path writes into a
      // pooled MemorySegment-backed BytesOut, so materialising this payload as a byte[] would create
      // one full-page allocation per spill. writeSegment copies synchronously and transfers no
      // ownership: the HOT leaf remains the sole owner of its slot memory.
      final MemorySegment slots = hotLeaf.slots();
      final int usedSize = hotLeaf.getUsedSlotsSize();
      sink.writeSegment(slots, 0L, usedSize);
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
    public Page deserializePage(ResourceConfiguration resourceConfiguration, BytesIn<?> source, SerializationType type,
        final ByteHandler.DecompressionResult decompressionResult) {
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
          case SPAN_NODE -> HOTIndirectPage.createSpanNodeMultiMask(pageKey, revision, extractionPositions,
              extractionMasks, numExtractionBytes, partialKeys, children, height, mostSignificantBitIndex);
          case MULTI_NODE -> HOTIndirectPage.createMultiNodeMultiMask(pageKey, revision, extractionPositions,
              extractionMasks, numExtractionBytes, partialKeys, children, height, mostSignificantBitIndex);
        };
      } else {
        created = switch (nodeType) {
          case SPAN_NODE ->
            HOTIndirectPage.createSpanNode(pageKey, revision, initialBytePos, bitMask, partialKeys, children, height);
          case MULTI_NODE -> HOTIndirectPage.createMultiNode(pageKey, revision, initialBytePos, bitMask, partialKeys,
              children, height);
        };
      }

      return created;
    }

    @Override
    public void serializePage(ResourceConfiguration resourceConfig, BytesOut<?> sink, Page page,
        SerializationType type) {
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
        hotIndirect.writeExtractionPositions(sink);
        hotIndirect.writeExtractionMasks(sink);
      } else {
        // SingleMask: write initialBytePos + bitMask
        sink.writeShort((short) hotIndirect.getInitialBytePos());
        sink.writeLong(hotIndirect.getBitMask());
        sink.writeShort(hotIndirect.getMostSignificantBitIndex());
      }

      // Write partial keys (width determined by total number of discriminative bits)
      hotIndirect.writePartialKeys(sink);

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

    }
  },

  // Page-kind id 14 is permanently reserved. It belonged to the unreleased, unreachable bitmap
  // chunk secondary-index format; retaining the hole prevents accidental ID reuse.

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

          final Int2LongMap maxHotPageKeys = PageKind.deserializeMaxHotPageKeys(source);

          return new ProjectionIndexPage(delegate, maxHotPageKeys);
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

      PageKind.serializeMaxHotPageKeys(sink, projectionPage.maxHotPageKeysForSerialization());
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

          final Int2LongMap maxHotPageKeys = PageKind.deserializeMaxHotPageKeys(source);

          return new ValidTimeIndexPage(delegate, maxHotPageKeys);
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

      PageKind.serializeMaxHotPageKeys(sink, validTimePage.maxHotPageKeysForSerialization());
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
      case ReferencesPage4 page -> page.serializeReferences(sink, type);
      case BitmapReferencesPage page -> page.serializeReferences(sink, type);
      case FullReferencesPage page -> page.serializeReferences(sink, type);
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

  /**
   * Deserialize the sparse per-index HOT page-key high-water marks.
   *
   * <p>Unlike keyed-trie node counters, secondary-index identifiers are physical reference offsets
   * and need not start at zero (NAME indexes deliberately start after NamePage's dictionary slots).
   * The wire therefore carries each index number explicitly. Strictly increasing identifiers keep
   * the representation canonical and turn duplicates or reordered/corrupt metadata into a hard
   * failure instead of silently reusing a page key.</p>
   */
  private static Int2LongMap deserializeMaxHotPageKeys(final BytesIn<?> source) {
    final int size = source.readInt();
    if (size < 0 || size > Constants.INP_REFERENCE_COUNT) {
      throw new IllegalStateException("Corrupt HOT page-key entry count: " + size);
    }

    final Int2LongMap maxHotPageKeys = new Int2LongOpenHashMap(size);
    int previousIndex = -1;
    for (int entry = 0; entry < size; entry++) {
      final int index = source.readInt();
      final long maxPageKey = source.readLong();
      if (index <= previousIndex || index >= Constants.INP_REFERENCE_COUNT) {
        throw new IllegalStateException("Corrupt HOT page-key index " + index
            + " at entry " + entry + " (previous index " + previousIndex + ')');
      }
      if (maxPageKey < 0) {
        throw new IllegalStateException("Corrupt negative HOT page-key high-water mark " + maxPageKey
            + " for index " + index);
      }
      maxHotPageKeys.put(index, maxPageKey);
      previousIndex = index;
    }
    return maxHotPageKeys;
  }

  /**
   * Per-thread index-number scratch for deterministic HOT metadata serialization. A full reference
   * namespace costs 4 KiB once per serializer thread and avoids allocating an array on every commit.
   */
  private static final ThreadLocal<int[]> HOT_PAGE_KEY_INDEX_SCRATCH =
      ThreadLocal.withInitial(() -> new int[Constants.INP_REFERENCE_COUNT]);

  /** Serialize sparse HOT page-key metadata as deterministic {@code (index, high-water)} pairs. */
  private static void serializeMaxHotPageKeys(final BytesOut<?> sink, final Int2LongMap maxHotPageKeys) {
    final int size = maxHotPageKeys.size();
    if (size > Constants.INP_REFERENCE_COUNT) {
      throw new IllegalStateException("Too many HOT page-key entries to serialize: " + size);
    }

    final int[] indexes = HOT_PAGE_KEY_INDEX_SCRATCH.get();
    int entry = 0;
    for (final int index : maxHotPageKeys.keySet()) {
      indexes[entry++] = index;
    }
    if (entry != size) {
      throw new IllegalStateException("HOT page-key metadata changed during serialization: expected "
          + size + " entries but observed " + entry);
    }
    Arrays.sort(indexes, 0, size);
    sink.writeInt(size);
    for (int position = 0; position < size; position++) {
      final int index = indexes[position];
      final long maxPageKey = maxHotPageKeys.get(index);
      if (index < 0 || index >= Constants.INP_REFERENCE_COUNT) {
        throw new IllegalStateException("HOT page-key index outside reference space: " + index);
      }
      if (maxPageKey < 0) {
        throw new IllegalStateException("Negative HOT page-key high-water mark " + maxPageKey
            + " for index " + index);
      }
      sink.writeInt(index);
      sink.writeLong(maxPageKey);
    }
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
   * Accounting for the column-only read path, off unless {@code -Dsirix.page.regionReadDiag=true}.
   * Answers one question the wall clock cannot: of the bytes a column-only page read pulls off disk,
   * how many does it actually look at?
   */
  private static final boolean REGION_READ_DIAG = Boolean.getBoolean("sirix.page.regionReadDiag");

  private static final LongAdder REGION_BODY_BYTES_SKIPPED = new LongAdder();
  private static final LongAdder REGION_TABLE_BYTES_READ = new LongAdder();
  private static final LongAdder REGION_PAGES_DECODED = new LongAdder();

  /** Bytes of record body skipped by column-only reads since the last reset. */
  public static long regionReadBodyBytesSkipped() {
    return REGION_BODY_BYTES_SKIPPED.sum();
  }

  /** Bytes of region table materialized by column-only reads since the last reset. */
  public static long regionReadTableBytesRead() {
    return REGION_TABLE_BYTES_READ.sum();
  }

  /** Pages decoded column-only since the last reset. */
  public static long regionReadPagesDecoded() {
    return REGION_PAGES_DECODED.sum();
  }

  public static void resetRegionReadDiag() {
    REGION_BODY_BYTES_SKIPPED.reset();
    REGION_TABLE_BYTES_READ.reset();
    REGION_PAGES_DECODED.reset();
  }

  /**
   * Writes the shared page envelope after the kind byte: {@code [binaryVersion u8][flags u8]}. The
   * flags byte is reserved extension space for every page kind (all bits zero in V0) — without it,
   * any additive change to a non-KVLP page required a global version bump.
   */
  static void writeVersionAndFlags(final BytesOut<?> sink) {
    sink.writeByte(BinaryEncodingVersion.V0.byteVersion());
    sink.writeByte((byte) 0);
  }

  /**
   * Reads and validates the shared page envelope: the version byte (throws on unknown) and the
   * reserved flags byte (must be zero in V0 — a nonzero value means a newer writer used an extension
   * this build does not understand, so misparsing is not an option).
   */
  static BinaryEncodingVersion readVersionAndFlags(final BytesIn<?> source) {
    final BinaryEncodingVersion version = BinaryEncodingVersion.fromByte(source.readByte());
    final byte flags = source.readByte();
    if (flags != 0) {
      throw new IllegalStateException(
          "Unknown page envelope flags 0x" + Integer.toHexString(flags & 0xFF) + " — page written by a newer version");
    }
    return version;
  }

  /**
   * A leaf has at most {@link HOTLeafPage#MAX_ENTRIES} owners and each owner has a 16-bit sub-id
   * namespace. This is the format-derived hard ceiling for the reusable serializer scratch, not an
   * arbitrary operational limit.
   */
  private static final int MAX_HOT_LEAF_SIDE_REFERENCES =
      HOTLeafPage.MAX_ENTRIES * (HOTLeafPage.MAX_OVERFLOW_PAGE_REF_SUB_ID + 1);

  /**
   * Exactly-once owner for the copying HOT-leaf deserialization path.
   *
   * <p>The object is allocated before the native frame. Binding and release then allocate no Java
   * heap state, so an allocation failure between frame acquisition and page construction cannot
   * strand the frame. {@link HOTLeafPage} retains this same owner after successful construction.</p>
   */
  private static final class HOTLeafAllocatedFrameOwner implements Runnable {
    private final MemorySegmentAllocator allocator;
    private @Nullable MemorySegment segment;
    private boolean released;

    private HOTLeafAllocatedFrameOwner(final MemorySegmentAllocator allocator) {
      this.allocator = Objects.requireNonNull(allocator);
    }

    private void bind(final MemorySegment segment) {
      if (this.segment != null || released) {
        throw new IllegalStateException("HOT-leaf deserialization frame owner is already bound");
      }
      this.segment = Objects.requireNonNull(segment);
    }

    @Override
    public synchronized void run() {
      if (released) {
        return;
      }
      released = true;
      final MemorySegment ownedSegment = segment;
      segment = null;
      if (ownedSegment != null) {
        allocator.release(ownedSegment);
      }
    }
  }

  /**
   * Owner-confined primitive scratch for deterministic side-map emission. It starts at one key per
   * possible leaf slot and grows geometrically to the format ceiling; an allocation is paid only when
   * a serializer thread observes a new high-water mark, never on recurring warm spills.
   */
  private static final ThreadLocal<long[]> HOT_LEAF_SIDE_REFERENCE_KEYS =
      ThreadLocal.withInitial(() -> new long[HOTLeafPage.MAX_ENTRIES]);

  /** Tiny introsort partitions are faster as a branch-light in-place insertion pass. */
  private static final int HOT_LEAF_SIDE_REFERENCE_INSERTION_SORT_THRESHOLD = 24;

  /**
   * Emit sparse slot offsets and payload in two bitmap passes. The old path first copied both into a
   * per-spill {@code int[]} and {@code byte[]}; writing offsets in pass one and exact slot segment
   * ranges in pass two retains the same packed order without either allocation.
   */
  private static void serializeSparseHOTLeafEntries(final BytesOut<?> sink, final HOTLeafPage hotLeaf,
      final int expectedEntryCount, final int expectedPayloadBytes) {
    final int entryCount = hotLeaf.getEntryCount();
    final int usedSlotBytes = hotLeaf.getUsedSlotsSize();
    final MemorySegment slots = hotLeaf.slots();
    if (expectedEntryCount <= 0 || expectedEntryCount > entryCount || entryCount > HOTLeafPage.MAX_ENTRIES
        || expectedPayloadBytes <= 0 || expectedPayloadBytes > usedSlotBytes || usedSlotBytes > slots.byteSize()) {
      throw new IllegalStateException("Invalid sparse HOT-leaf bounds: dirtyCount=" + expectedEntryCount
          + ", entryCount=" + entryCount + ", dirtyBytes=" + expectedPayloadBytes + ", usedSlotBytes=" + usedSlotBytes
          + ", slotCapacity=" + slots.byteSize());
    }

    int emittedEntries = 0;
    int packedOffset = 0;
    final int bitmapWordCount = (HOTLeafPage.MAX_ENTRIES + Long.SIZE - 1) / Long.SIZE;
    for (int wordIndex = 0; wordIndex < bitmapWordCount; wordIndex++) {
      long word = hotLeaf.getDirtyBitmapWord(wordIndex);
      while (word != 0L) {
        final int entryIndex = (wordIndex << 6) | Long.numberOfTrailingZeros(word);
        if (entryIndex < entryCount) {
          final int slotOffset = hotLeaf.getSlotOffset(entryIndex);
          final int slotSize = hotLeaf.getSlotSize(entryIndex);
          if (emittedEntries >= expectedEntryCount || slotOffset < 0 || slotSize <= 0
              || slotOffset > usedSlotBytes - slotSize) {
            throw new IllegalStateException("Invalid dirty HOT-leaf slot " + entryIndex + ": offset=" + slotOffset
                + ", size=" + slotSize + ", usedSlotBytes=" + usedSlotBytes);
          }
          sink.writeInt(packedOffset);
          packedOffset += slotSize;
          emittedEntries++;
        }
        word &= word - 1L;
      }
    }
    validateSparseHOTLeafPass(expectedEntryCount, expectedPayloadBytes, emittedEntries, packedOffset, "offset");

    emittedEntries = 0;
    int emittedBytes = 0;
    for (int wordIndex = 0; wordIndex < bitmapWordCount; wordIndex++) {
      long word = hotLeaf.getDirtyBitmapWord(wordIndex);
      while (word != 0L) {
        final int entryIndex = (wordIndex << 6) | Long.numberOfTrailingZeros(word);
        if (entryIndex < entryCount) {
          final int slotSize = hotLeaf.getSlotSize(entryIndex);
          sink.writeSegment(slots, hotLeaf.getSlotOffset(entryIndex), slotSize);
          emittedBytes += slotSize;
          emittedEntries++;
        }
        word &= word - 1L;
      }
    }
    validateSparseHOTLeafPass(expectedEntryCount, expectedPayloadBytes, emittedEntries, emittedBytes, "payload");
  }

  private static void validateSparseHOTLeafPass(final int expectedEntryCount, final int expectedPayloadBytes,
      final int actualEntryCount, final int actualPayloadBytes, final String pass) {
    if (actualEntryCount != expectedEntryCount || actualPayloadBytes != expectedPayloadBytes) {
      throw new IllegalStateException("Sparse HOT-leaf " + pass + " pass changed beneath serialization: entries="
          + actualEntryCount + '/' + expectedEntryCount + ", bytes=" + actualPayloadBytes + '/' + expectedPayloadBytes);
    }
  }

  /** Return reusable primitive scratch with an explicit format-derived capacity bound. */
  private static long[] hotLeafSideReferenceKeyScratch(final int required) {
    if (required < 0 || required > MAX_HOT_LEAF_SIDE_REFERENCES) {
      throw new IllegalStateException(
          "HOT-leaf side-reference count " + required + " exceeds format limit " + MAX_HOT_LEAF_SIDE_REFERENCES);
    }
    long[] scratch = HOT_LEAF_SIDE_REFERENCE_KEYS.get();
    if (scratch.length >= required) {
      return scratch;
    }

    int capacity = scratch.length;
    while (capacity < required) {
      capacity = capacity <= MAX_HOT_LEAF_SIDE_REFERENCES / 2
          ? capacity << 1
          : MAX_HOT_LEAF_SIDE_REFERENCES;
    }
    scratch = new long[capacity];
    HOT_LEAF_SIDE_REFERENCE_KEYS.set(scratch);
    return scratch;
  }

  /**
   * Sort exactly the populated prefix using signed-long order, matching the historical
   * {@link Arrays#sort(long[], int, int)} wire order without its large-input work array. The normal
   * path is a median-of-three quicksort; a depth limit switches adversarial inputs to heapsort, and
   * recursing only into the smaller partition keeps stack usage logarithmic.
   */
  static void sortHotLeafSideReferenceKeyPrefix(final long[] keys, final int keyCount) {
    if (keyCount < 2) {
      return;
    }
    final int depthLimit = 2 * (Integer.SIZE - 1 - Integer.numberOfLeadingZeros(keyCount));
    introsortHotLeafSideReferenceKeys(keys, 0, keyCount, depthLimit);
  }

  private static void introsortHotLeafSideReferenceKeys(final long[] keys, int from, int to, int depthLimit) {
    while (to - from > HOT_LEAF_SIDE_REFERENCE_INSERTION_SORT_THRESHOLD) {
      if (depthLimit == 0) {
        heapSortHotLeafSideReferenceKeys(keys, from, to);
        return;
      }
      depthLimit--;

      final int middle = from + ((to - from) >>> 1);
      final long pivot = medianSignedLong(keys[from], keys[middle], keys[to - 1]);
      int left = from;
      int right = to - 1;
      while (left <= right) {
        while (keys[left] < pivot) {
          left++;
        }
        while (keys[right] > pivot) {
          right--;
        }
        if (left <= right) {
          final long value = keys[left];
          keys[left++] = keys[right];
          keys[right--] = value;
        }
      }

      // Finish the smaller partition recursively and loop over the larger one. Besides eliminating
      // one call per partition, this caps stack depth even before the heapsort guard is reached.
      if (right - from < to - left) {
        introsortHotLeafSideReferenceKeys(keys, from, right + 1, depthLimit);
        from = left;
      } else {
        introsortHotLeafSideReferenceKeys(keys, left, to, depthLimit);
        to = right + 1;
      }
    }
    insertionSortHotLeafSideReferenceKeys(keys, from, to);
  }

  private static long medianSignedLong(final long first, final long middle, final long last) {
    if (first < middle) {
      return middle < last
          ? middle
          : first < last
              ? last
              : first;
    }
    return first < last
        ? first
        : middle < last
            ? last
            : middle;
  }

  private static void insertionSortHotLeafSideReferenceKeys(final long[] keys, final int from, final int to) {
    for (int index = from + 1; index < to; index++) {
      final long value = keys[index];
      int insertionIndex = index;
      while (insertionIndex > from && value < keys[insertionIndex - 1]) {
        keys[insertionIndex] = keys[insertionIndex - 1];
        insertionIndex--;
      }
      keys[insertionIndex] = value;
    }
  }

  private static void heapSortHotLeafSideReferenceKeys(final long[] keys, final int from, final int to) {
    final int length = to - from;
    for (int root = (length >>> 1) - 1; root >= 0; root--) {
      siftDownHotLeafSideReferenceKeys(keys, from, root, length);
    }
    for (int end = length - 1; end > 0; end--) {
      final long maximum = keys[from];
      keys[from] = keys[from + end];
      keys[from + end] = maximum;
      siftDownHotLeafSideReferenceKeys(keys, from, 0, end);
    }
  }

  private static void siftDownHotLeafSideReferenceKeys(final long[] keys, final int offset, int root,
      final int length) {
    final long value = keys[offset + root];
    final int firstLeaf = length >>> 1;
    while (root < firstLeaf) {
      int child = (root << 1) + 1;
      long childValue = keys[offset + child];
      final int rightChild = child + 1;
      if (rightChild < length && childValue < keys[offset + rightChild]) {
        child = rightChild;
        childValue = keys[offset + child];
      }
      if (value >= childValue) {
        break;
      }
      keys[offset + root] = childValue;
      root = child;
    }
    keys[offset + root] = value;
  }

  /**
   * Serializes a HOT leaf's segment-reference side map as a trailing section:
   * {@code varint count + count × (compositeKey u64, diskOffsetKey u64)}. Entries are emitted in
   * ascending compositeKey order so identical maps serialize to identical bytes. Every reference must
   * be resolved (disk key assigned) by the time the owning leaf serializes — the commit descent
   * writes segment pages before the leaf (OverflowPage discipline); an unresolved reference here
   * means a segment page bypassed the commit branch and would persist as a dangling {@code -1}, so
   * fail loudly instead.
   */
  private static void serializeSegmentRefs(final BytesOut<?> sink, final HOTLeafPage hotLeaf) {
    final int keyCount = hotLeaf.segmentRefCount();
    final long[] keys = hotLeafSideReferenceKeyScratch(keyCount);
    final int copiedKeyCount = hotLeaf.copyOverflowPageRefKeysInto(keys);
    if (copiedKeyCount != keyCount) {
      throw new IllegalStateException("HOT-leaf side-reference map changed beneath serialization: copied="
          + copiedKeyCount + ", expected=" + keyCount);
    }
    sortHotLeafSideReferenceKeyPrefix(keys, keyCount);
    Utils.putVarLong(sink, keyCount);
    for (int keyIndex = 0; keyIndex < keyCount; keyIndex++) {
      final long compositeKey = keys[keyIndex];
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
    if (count < 0L || count > MAX_HOT_LEAF_SIDE_REFERENCES) {
      throw new IllegalStateException(
          "HOT-leaf side-reference count " + count + " exceeds format limit " + MAX_HOT_LEAF_SIDE_REFERENCES);
    }
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
   * unknown bit means a newer writer used an extension this build does not understand, so misparsing
   * is not an option.
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

  /** Permanently unassigned persisted page-kind ID; never reuse it for another page layout. */
  private static final byte RESERVED_PAGE_KIND_ID_14 = 14;

  /**
   * Mapping of class -> page.
   */
  private static final Map<Class<? extends Page>, PageKind> INSTANCEFORCLASS = new HashMap<>();

  /**
   * Per-thread scratch array for single-pass serializePage. Layout: [heapOffset0, dataLength0,
   * heapOffset1, dataLength1, ...]. Max 1024 slots x 2 ints = 8 KB per thread.
   */
  private static final ThreadLocal<int[]> SERIALIZE_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT * 2]);

  /** Per-thread reusable buffer for number-region value collection at seal time. */
  /**
   * Per-slot absolute region index for the value-elision handshake between {@code
   * buildRegionTable} (which assigns them) and {@code writeEncodedBody} (which elides only slots
   * holding one). Indexed by slot bit; -1 means the slot contributed nothing to its region.
   */
  /** Read-side per-entry scratches for the value-elision section. */
  private static final ThreadLocal<short[]> VALUE_ELIDED_SLOT_READ_SCRATCH =
      ThreadLocal.withInitial(() -> new short[PageLayout.SLOT_COUNT]);

  private static final ThreadLocal<int[]> VALUE_ELIDED_WIDTH_READ_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  private static final ThreadLocal<int[]> VALUE_ELIDED_ABS_IDX_READ_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  private static final ThreadLocal<int[]> SLOT_REGION_ABS_IDX_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  /** Contributor slot lists recorded by {@code buildRegionTable}, one per region kind. */
  private static final ThreadLocal<int[]> NUMBER_REGION_SLOT_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  private static final ThreadLocal<int[]> STRING_REGION_SLOT_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  private static final ThreadLocal<int[]> STRING_REGION_NAME_TAG_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  private static final ThreadLocal<int[]> STRING_REGION_PATH_TAG_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  private static final ThreadLocal<int[]> BOOLEAN_REGION_SLOT_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  /**
   * Writer-side header scratches for the index-assignment replay. Deliberately separate from the
   * reader-side {@code *_HEADER_SCRATCH} instances: a commit can load pages mid-serialization, and
   * sharing a mutable header between the two directions would be an aliasing bug waiting for the
   * first interleaving.
   */
  private static final ThreadLocal<NumberRegion.Header> WRITER_NUMBER_HEADER_SCRATCH =
      ThreadLocal.withInitial(NumberRegion.Header::new);

  private static final ThreadLocal<StringRegion.Header> WRITER_STRING_HEADER_SCRATCH =
      ThreadLocal.withInitial(StringRegion.Header::new);

  private static final ThreadLocal<BooleanRegion.Header> WRITER_BOOLEAN_HEADER_SCRATCH =
      ThreadLocal.withInitial(BooleanRegion.Header::new);

  private static final ThreadLocal<Int2IntOpenHashMap> WRITER_REGION_RANK_SCRATCH =
      ThreadLocal.withInitial(() -> new Int2IntOpenHashMap(16));

  private static final ThreadLocal<long[]> NUMBER_VALUE_SCRATCH =
      ThreadLocal.withInitial(() -> new long[PageLayout.SLOT_COUNT]);

  /** Exact unscaled value per collected double-column slot; paired with the scale below. */
  private static final ThreadLocal<long[]> DOUBLE_DECIMAL_UNSCALED_SCRATCH =
      ThreadLocal.withInitial(() -> new long[PageLayout.SLOT_COUNT]);

  /** Exact scale per collected slot, or DECIMAL_SCALE_UNAVAILABLE for a plain double. */
  private static final ThreadLocal<int[]> DOUBLE_DECIMAL_SCALE_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  /** One-element out-parameter for the decimal decoder, so the seal-time loop allocates nothing. */
  private static final ThreadLocal<int[]> DOUBLE_DECIMAL_OUT_SCRATCH = ThreadLocal.withInitial(() -> new int[1]);

  /** Per-thread reusable buffer for number-region parent-nameKey collection at seal time. */
  private static final ThreadLocal<int[]> NUMBER_PARENT_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread reusable buffer for number-region parent-pathNodeKey (int-truncated) collection at
   * seal time. Populated in parallel with {@link #NUMBER_PARENT_SCRATCH}; the final encoder call
   * picks one buffer based on the resolved tagKind.
   */
  private static final ThreadLocal<int[]> NUMBER_PATH_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  /** Reusable number-region encoder for the page serializer's one-thread-at-a-time seal path. */
  private static final ThreadLocal<NumberRegion.Encoder> NUMBER_REGION_ENCODER =
      ThreadLocal.withInitial(() -> new NumberRegion.Encoder(PageLayout.SLOT_COUNT));

  /** Per-thread buffers for double-typed number values the long region declines. */
  private static final ThreadLocal<double[]> DOUBLE_VALUE_SCRATCH =
      ThreadLocal.withInitial(() -> new double[PageLayout.SLOT_COUNT]);

  private static final ThreadLocal<int[]> DOUBLE_NAME_TAG_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  private static final ThreadLocal<int[]> DOUBLE_PATH_TAG_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  private static final ThreadLocal<int[]> DOUBLE_ORDINAL_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  private static final ThreadLocal<Int2IntOpenHashMap> DOUBLE_FIELD_ORDINAL_SCRATCH =
      ThreadLocal.withInitial(() -> new Int2IntOpenHashMap(16));

  /** Per-thread reusable buffer for OBJECT_KEY nameKey values at seal time. */
  private static final ThreadLocal<int[]> OBJECT_KEY_NAMEKEY_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  /** Per-thread reusable buffer for OBJECT_KEY slot indices at seal time. */
  private static final ThreadLocal<int[]> OBJECT_KEY_SLOT_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread buffer for the enclosing object's node key per OBJECT_KEY slot, feeding
   * {@link RecordOrdinalRegion#encode} raw. The encoder classifies each key itself — on-page parent,
   * the spanning record's skip prefix, or a shape that refuses the region — so the writer and the
   * reconstruction rebuild cannot drift apart on that contract.
   */
  private static final ThreadLocal<long[]> OBJECT_KEY_PARENT_KEY_SCRATCH =
      ThreadLocal.withInitial(() -> new long[PageLayout.SLOT_COUNT]);

  /**
   * Shared sequential output for the small page regions. {@link RegionTable#set(byte, byte[], int)}
   * copies each valid prefix into page-owned native storage before the next encoder reuses it.
   */
  private static final ThreadLocal<byte[]> REGION_ENCODE_SCRATCH = ThreadLocal.withInitial(() -> new byte[Math.max(
      NumberZoneMapRegion.encodedSize(PageLayout.SLOT_COUNT),
      Math.max(ObjectKeyNameKeyRegion.maxEncodedSize(PageLayout.SLOT_COUNT), RecordOrdinalRegion.maxEncodedSize()))]);

  /**
   * Per-thread reusable {@link StringRegion.Encoder}. The encoder's internal fastutil maps and
   * per-tag arrays/value-store capacity are retained across pages; only per-page counts, live shared
   * store references, and logical store length are cleared via {@link StringRegion.Encoder#reset()}.
   * Writer threads therefore pay at most one encoder allocation in their lifetime instead of one per
   * committed page.
   */
  private static final ThreadLocal<StringRegion.Encoder> STRING_REGION_ENCODER =
      ThreadLocal.withInitial(StringRegion.Encoder::new);

  /**
   * Second per-thread {@link StringRegion.Encoder} used for the path-tagged variant when the resource
   * runs with path-summary enabled. Populated in lockstep with {@link #STRING_REGION_ENCODER} until
   * an invalid pathNodeKey is observed; the final {@code finish()} chooses between the two based on
   * the resolved tagKind.
   */
  private static final ThreadLocal<StringRegion.Encoder> STRING_REGION_ENCODER_PATH =
      ThreadLocal.withInitial(StringRegion.Encoder::new);

  /**
   * Release the previous page's path candidate whenever the name candidate starts a page, even when
   * the current resource has no path summary. Writer threads can move between resources: a path
   * candidate from the preceding resource may still borrow ranges from the name encoder's value
   * store, and leaving it untouched would defer every subsequent name-store reset.
   *
   * @return the reset candidate when path tagging is enabled for this page, otherwise {@code null}
   */
  static StringRegion.@Nullable Encoder resetStringRegionPathCandidate(final boolean enabled,
      final @Nullable GlobalStringDictionaries dictionaries) {
    final StringRegion.Encoder pathCandidate = STRING_REGION_ENCODER_PATH.get();
    pathCandidate.reset();
    // THE encoder that writes bytes to disk, and therefore the one the trie lane has to ride. The
    // page also builds a string region on the DERIVE path
    // (KeyValueLeafPage#collectAndEncodeStringRegion, for region-only reads), which has its own
    // freshly constructed encoders; wiring only that one converted nothing on any page that was
    // actually serialized.
    //
    // Set unconditionally, including to null, and AFTER reset(): this encoder is a THREAD-LOCAL
    // reused across pages, and reset() does not clear the resolver. A page with no dictionaries
    // following one that had them would otherwise inherit the previous page's — the same reused-state
    // hazard that once put a guard in the wrong encoder.
    pathCandidate.setDictionaries(dictionaries);
    return enabled
        ? pathCandidate
        : null;
  }

  /**
   * Reusable destination for copying fused stored-string payloads out of native page memory. Its
   * contents are borrowed only until both candidate StringRegion encoders return synchronously.
   */
  private static final ThreadLocal<byte[]> STRING_REGION_VALUE_SCRATCH = ThreadLocal.withInitial(() -> new byte[1024]);

  /** Grow the current thread's scratch geometrically without copying dead contents. */
  private static byte[] growStringRegionValueScratch(final byte[] current, final int required) {
    if (required <= current.length) {
      return current;
    }
    final long doubled = Math.max(1L, (long) current.length << 1);
    final int capacity = (int) Math.min((long) Integer.MAX_VALUE, Math.max((long) required, doubled));
    final byte[] grown = new byte[capacity];
    STRING_REGION_VALUE_SCRATCH.set(grown);
    return grown;
  }

  // ==================== Offset-table dedup scratch ====================

  /**
   * Per-thread kindId array (one entry per populated slot). Filled during the page serialize walk and
   * consumed by {@link OffsetTableTemplatePool#build}.
   */
  private static final ThreadLocal<int[]> SLOT_KINDID_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread field-count array parallel to {@link #SLOT_KINDID_SCRATCH}. The bitmap scan derives it
   * once from the cached kind id; the encoder's pre-scan, sizing pass and staging pass then consume
   * the byte directly instead of repeating the same kind-table lookup.
   */
  private static final ThreadLocal<byte[]> SLOT_FIELD_COUNT_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread heapOffset array (one entry per populated slot). Avoids a re-walk of the bitmap when
   * consulting offset-table bytes on the slotted page heap.
   */
  private static final ThreadLocal<int[]> SLOT_HEAPOFF_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread dataLength array (one entry per populated slot). Stored to avoid re-reading
   * {@link PageLayout#getDirDataLength} after the offset-table dedup walk.
   */
  private static final ThreadLocal<int[]> SLOT_DATALEN_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread bitmap-slot-index array (one short per populated slot). Records the actual bitmap bit
   * position {@code (word<<6)|bit} for each populated slot in bitmap-walk order. Used by the
   * parentKey column pre-scan to compute {@code nodeKey = pageKeyBase + slot}.
   */
  private static final ThreadLocal<short[]> SLOT_BIT_SCRATCH =
      ThreadLocal.withInitial(() -> new short[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread zero-hash bitmap scratch (one bit per populated slot — byte size
   * {@code ceil(SLOT_COUNT / 8) = 128}). Bit set = record's hash field is all-zero and gets stripped
   * from the staging blob. Only populated when {@link #HASH_ELISION_ENABLED} and the page carries
   * records with a hash field.
   */
  private static final ThreadLocal<byte[]> SLOT_ZERO_HASH_BITMAP_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[(PageLayout.SLOT_COUNT + 7) >>> 3]);

  /**
   * Per-thread per-slot hash offset scratch (one short per populated slot). For slots whose kind has
   * a hash field, stores the byte offset of the hash bytes within the record's data region (as read
   * from the offset table). {@code -1} for slots without a hash field.
   */
  private static final ThreadLocal<short[]> SLOT_HASH_OFFSET_SCRATCH =
      ThreadLocal.withInitial(() -> new short[PageLayout.SLOT_COUNT]);

  /**
   * Enable / disable hash-elision structural encoding. When enabled, pages carrying records whose
   * 8-byte hash field is all-zero strip those bytes from the staging blob and reinject them on read.
   * Saves ~8 bytes/slot for {@code HashType.NONE} databases — the common analytical-workload setting
   * — and compounds the zero-run RLE savings because the leading zeros of adjacent structural fields
   * are now contiguous in the blob.
   *
   * <p>
   * Gated by {@code -Dsirix.hashElision.disable=true} for A/B measurement; default ON.
   */
  private static final boolean HASH_ELISION_ENABLED = !Boolean.getBoolean("sirix.hashElision.disable");

  /**
   * Per-thread parentKey column-value scratch (one long per populated slot). Filled during the
   * structural-key column pre-scan and consumed by {@link StructuralKeyColumnCodec}.
   * {@code Fixed#NULL_NODE_KEY} when the slot's kind has no parentKey field, or when the page-wide
   * column is inactive.
   */
  private static final ThreadLocal<long[]> SLOT_PARENT_KEY_SCRATCH =
      ThreadLocal.withInitial(() -> new long[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread parentKey on-disk width scratch (one byte per slot). Bytes 0..15; 0 means "no
   * parentKey field" (kind has no parent). Derived from the offset-table entry delta during the
   * column pre-scan.
   */
  private static final ThreadLocal<byte[]> SLOT_PARENT_KEY_WIDTH_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread parentKey in-data-region offset scratch (one short per slot). Copied from the record's
   * offset table by the column pre-scan so the staging / reconstruct loops don't re-derive it.
   */
  private static final ThreadLocal<short[]> SLOT_PARENT_KEY_OFF_SCRATCH =
      ThreadLocal.withInitial(() -> new short[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread scratch byte buffer for the {@link StructuralKeyColumnCodec} output bytes. Sized for
   * the worst case of a fully-independent column (11 bytes header + N × 10 bytes varint). Grows on
   * demand.
   */
  private static final ThreadLocal<byte[]> PARENT_KEY_COLUMN_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT * 11]);

  /**
   * Per-thread node key of each populated slot, in bitmap-ascending order.
   *
   * <p>
   * {@link StructuralKeyColumnCodec}'s node-key-predicted format needs a slot's node key as predictor
   * context on both the write and the read side, and the delta-varint re-encode needs it again.
   * Deriving it once per page beats recomputing {@code pageKeyBase + slotBit} in every column's
   * pre-scan.
   */
  private static final ThreadLocal<long[]> SLOT_NODE_KEY_SCRATCH =
      ThreadLocal.withInitial(() -> new long[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread on-disk record length for each populated slot, as the heap-sizing pass computed it.
   * Consumed by the compact-dir pass, which used to re-derive the identical value.
   */
  private static final ThreadLocal<int[]> SLOT_ON_DISK_LEN_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);


  /**
   * Per-thread pathNodeKey column-value scratch (one long per populated slot). Filled during the
   * pathNodeKey pre-scan pass and consumed by {@link PathNodeKeyRegion}. Slots whose kind lacks a
   * pathNodeKey field hold {@link Fixed#NULL_NODE_KEY} and are skipped at encode time.
   */
  private static final ThreadLocal<long[]> SLOT_PATH_NODE_KEY_SCRATCH =
      ThreadLocal.withInitial(() -> new long[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread pathNodeKey on-disk width scratch (one byte per slot). Bytes 0..10; 0 means "no
   * pathNodeKey field" (kind has no pathNodeKey). Derived from the offset-table delta during the
   * column pre-scan.
   */
  private static final ThreadLocal<byte[]> SLOT_PATH_NODE_KEY_WIDTH_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread pathNodeKey in-data-region offset scratch (one short per slot). Copied from the
   * record's offset table by the column pre-scan so the staging / reconstruct loops don't re-derive
   * it. Unlike parentKey (always at offset 0), pathNodeKey lives at a kind-specific interior offset.
   */
  private static final ThreadLocal<short[]> SLOT_PATH_NODE_KEY_OFF_SCRATCH =
      ThreadLocal.withInitial(() -> new short[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread compact buffer for the pathNodeKey-bearing slot int-values fed to
   * {@link PathNodeKeyRegion#encode}. Compacted form — length = slots with pathNodeKey, bitmap order.
   */
  private static final ThreadLocal<int[]> SLOT_PATH_NODE_KEY_VALUES_COMPACT_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread compact slot-index array parallel to
   * {@link #SLOT_PATH_NODE_KEY_VALUES_COMPACT_SCRATCH}. Holds the slot index (0..1023) of each entry
   * so {@link PathNodeKeyRegion#encode} can populate its bitmap.
   */
  private static final ThreadLocal<int[]> SLOT_PATH_NODE_KEY_SLOTS_COMPACT_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread scratch byte buffer for the {@link PathNodeKeyRegion} encoded column bytes. Grows on
   * demand on the read path. Its initial capacity has one spare dictionary entry beyond the encoder's
   * 255-entry ceiling, so every writer payload fits without a sizing pass or growth.
   */
  private static final ThreadLocal<byte[]> PATH_NODE_KEY_COLUMN_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[1 + 256 * 4 + 2 + 128 + PageLayout.SLOT_COUNT]);

  /**
   * Per-thread 256-int dictionary scratch for {@link PathNodeKeyRegion#encode}. Replaces the former
   * per-page {@code new int[256]} allocation.
   */
  private static final ThreadLocal<int[]> PNK_ENCODE_DICT_SCRATCH = ThreadLocal.withInitial(() -> new int[256]);

  /**
   * Per-thread dict-id scratch for {@link PathNodeKeyRegion#encode} — one byte per populated slot.
   * Replaces the former per-page {@code new byte[count]} alloc.
   */
  private static final ThreadLocal<byte[]> PNK_ENCODE_DICT_IDS_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread 16-long bitmap scratch for {@link PathNodeKeyRegion#encode}. The encoder zero-fills on
   * entry so it is safe for Lever-3 region-lookup additions to reuse in future.
   */
  private static final ThreadLocal<long[]> COLUMN_ENCODE_BITMAP_SCRATCH = ThreadLocal.withInitial(() -> new long[16]);

  // ============================================================
  // Lever 3: VALUE elision scratches (writer + reader, fused-NUMBER only)
  // ============================================================

  /**
   * Per-thread per-slot value-strip flag (1 byte/slot). Bit 0 set = the slot is a fused
   * {@code OBJECT_NAMED_NUMBER} (kindId 49) whose payload bytes are being elided on this page. Other
   * bits reserved.
   */
  private static final ThreadLocal<byte[]> SLOT_VALUE_ELIDED_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread per-slot value field offset (one short/slot). For fused-NUMBER slots holds the
   * in-data-region offset of the {@code [type:1][varint]} payload, derived from
   * offset-table[OBJNAMEDNUM_PAYLOAD]. Zero for slots that are not value-elided participants.
   */
  private static final ThreadLocal<short[]> SLOT_VALUE_OFF_SCRATCH =
      ThreadLocal.withInitial(() -> new short[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread per-slot value field width (one byte/slot, 0..15 valid range). For fused-NUMBER slots
   * holds the byte count of the {@code [type:1][varint]} payload field. Zero for slots that are not
   * value-elided participants.
   */
  private static final ThreadLocal<short[]> SLOT_VALUE_WIDTH_SCRATCH =
      ThreadLocal.withInitial(() -> new short[PageLayout.SLOT_COUNT]);

  /**
   * DIAGNOSTIC ONLY, acquired only when {@code -Dsirix.pageSectionDiag=true}: the payload width of
   * every fused-primitive slot, whether or not the slot turned out to be elidable.
   * {@link #SLOT_VALUE_WIDTH_SCRATCH} holds a width only for slots the writer marked elidable, so it
   * cannot answer the question the plan asks — how many payload bytes stay INLINE in the heap because
   * elision did not cover the slot. Kept apart from the production scratch so no diagnostic can ever
   * change what the writer strips.
   */
  private static final ThreadLocal<short[]> SLOT_DIAG_VALUE_WIDTH_SCRATCH =
      ThreadLocal.withInitial(() -> new short[PageLayout.SLOT_COUNT]);

  /**
   * DIAGNOSTIC ONLY: per-node-kind fold of one page's staged heap, so the per-kind counters cost one
   * {@link PageSectionDiag} call per kind present instead of one per slot. Four lanes per kind —
   * slots, staged bytes, inline payload bytes, inline payload slots.
   */
  private static final ThreadLocal<long[]> DIAG_HEAP_BY_KIND_SCRATCH =
      ThreadLocal.withInitial(() -> new long[4 * 256]);

  /**
   * Per-thread per-elided-slot type byte ({@code NUMBER_TYPE_INTEGER == 2} or
   * {@code NUMBER_TYPE_LONG == 3}). Packed in slot-ascending order, length = number of elided slots
   * on the page. Stored on disk so the reader can re-encode the original heap bytes byte-for-byte
   * (the varint width depends on whether the value is decoded as int or long).
   */
  private static final ThreadLocal<byte[]> SLOT_VALUE_TYPE_PACKED_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread per-slot decoded long value scratch. Filled by the writer's pre-scan from
   * {@link KeyValueLeafPage#getFusedObjectNamedNumberValueLongFromSlot} for fused-NUMBER slots;
   * consumed by the reader on the inject path.
   */
  private static final ThreadLocal<long[]> SLOT_VALUE_LONG_SCRATCH =
      ThreadLocal.withInitial(() -> new long[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread re-encode buffer for the value bytes (1 type byte + up to 10 varint bytes = 11 max).
   * Sized at 16 to give headroom and stay within a single cache-line.
   */
  private static final ThreadLocal<byte[]> VALUE_REENCODE_SCRATCH = ThreadLocal.withInitial(() -> new byte[16]);

  /**
   * Per-thread scratch holding the (NumberRegion, slotRank) pair for each fused-NUMBER slot on a
   * value-elided page. Length = populatedCount; entries for non-elided slots are unused. Computed by
   * the reader's pre-expand walk and consumed by the per-record value-injection step. Uses a single
   * int per slot — the rank itself fits in low 16 bits; tag id stored in upper 16 bits. Reduces alloc
   * to one int[] instead of two short[].
   */
  private static final ThreadLocal<int[]> SLOT_VALUE_RANK_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread scratch holding the per-slot decoded value type byte for the reader. One byte per
   * fused-NUMBER slot in slot-ascending order; populated from the on-disk value-elision section.
   */
  private static final ThreadLocal<byte[]> SLOT_VALUE_TYPE_READ_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread {@link NumberRegion.Header} reused on the reader hot path. The header parses inline
   * into existing arrays where possible so we don't pay a fresh allocation per page.
   */
  private static final ThreadLocal<NumberRegion.Header> NUMBER_HEADER_SCRATCH =
      ThreadLocal.withInitial(NumberRegion.Header::new);

  /**
   * Per-thread scratch holding all number values decoded once for a delta-encoded
   * ({@link NumberRegion#ENC_DELTA_ZM}) region. Delta random access is O(index), so the per-slot
   * rehydration loop bulk-decodes the whole region up front (O(n)) and indexes this array instead of
   * paying O(n²). Grows on demand; other encodings never touch it.
   */
  private static final ThreadLocal<long[]> NUMBER_VALUES_SCRATCH =
      ThreadLocal.withInitial(() -> new long[PageLayout.SLOT_COUNT]);

  /**
   * Type byte for {@code Integer} payloads (matches {@link io.sirix.node.NodeKind#serializeNumber}).
   */
  private static final byte NUMBER_TYPE_INTEGER = 2;

  /** Type byte for {@code Long} payloads. */
  private static final byte NUMBER_TYPE_LONG = 3;

  /**
   * Lever 3 elision marker stored in {@link #SLOT_VALUE_ELIDED_SCRATCH} for fused
   * {@code OBJECT_NAMED_STRING} (kindId 50) slots. Distinguishes the STRING-elided code path from
   * NUMBER (which stores 2 / 3) and BOOLEAN (which stores {@link #STRING_ELIDE_MARKER + 1}). Value
   * chosen outside the NUMBER_TYPE_* range so a single equality check disambiguates per-slot kind in
   * the writer pre-scan, while staying non-zero so the strip pass's {@code slotValueElided[i] != 0}
   * guard still fires.
   */
  private static final byte STRING_ELIDE_MARKER = 0x70;

  /** Lever 3 elision marker for fused {@code OBJECT_NAMED_BOOLEAN} (kindId 48) slots. */
  private static final byte BOOLEAN_ELIDE_MARKER = 0x71;

  /**
   * Lever 3 elision marker for fused {@code OBJECT_NAMED_STRING} slots whose stored payload is
   * FSST-encoded. Distinct from {@link #STRING_ELIDE_MARKER} because the wire entry's type byte must
   * restore the heap's compressed-flag byte exactly on injection.
   */
  private static final byte STRING_ELIDE_COMPRESSED_MARKER = 0x72;

  /**
   * Per-thread reusable {@link BooleanRegion.Header} for the reader's value-elision inject pass. One
   * header per worker thread, reused across pages — its internal {@code int[]} arrays are sized
   * lazily by {@link BooleanRegion.Header#parseInto}.
   */
  private static final ThreadLocal<BooleanRegion.Header> BOOLEAN_HEADER_SCRATCH =
      ThreadLocal.withInitial(BooleanRegion.Header::new);

  /**
   * Per-thread reusable {@link StringRegion.Header} for the reader's value-elision inject pass. One
   * per worker thread.
   */
  private static final ThreadLocal<StringRegion.Header> STRING_HEADER_SCRATCH =
      ThreadLocal.withInitial(StringRegion.Header::new);

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
  private static final boolean PARENT_KEY_COLUMN_ENABLED = !Boolean.getBoolean("sirix.parentKeyColumn.disable");

  /** pathNodeKey dict column. Gate off with {@code -Dsirix.pathNodeKeyColumn.disable=true}. */
  private static final boolean PATH_NODE_KEY_COLUMN_ENABLED = !Boolean.getBoolean("sirix.pathNodeKeyColumn.disable");

  /**
   * Lever 3: VALUE elision via PAX region lookup. When enabled and a fused
   * {@code OBJECT_NAMED_NUMBER} (kindId 49) slot's payload is decode-able as a long (Integer or Long
   * type — i.e. {@link KeyValueLeafPage#getFusedObjectNamedNumberValueLongFromSlot} does NOT return
   * {@link Long#MIN_VALUE}), the per-record payload bytes ([type:1][delta-varint long]) are stripped
   * from the staged blob and the value is resolved at read time from the {@link NumberRegion} payload
   * via {@code (tag, slotRank)} lookup, then re-encoded inline into the in-memory heap. The PAX
   * region copy is load-bearing for SIMD scans and is kept; we only drop the heap copy.
   *
   * <p>
   * Activation is per-page: the writer activates elision only when EVERY fused-NUMBER slot on the
   * page has a long-decodable value AND the page-wide net savings are positive (2 bytes type+width
   * overhead per elided slot).
   *
   * <p>
   * The on-disk per-elided-slot metadata is 2 bytes:
   * <ul>
   * <li>1 byte type ({@code NUMBER_TYPE_INTEGER}=2 or {@code NUMBER_TYPE_LONG}=3)</li>
   * <li>1 byte original heap width ({@code 1 + varint bytes})</li>
   * </ul>
   * Storing the width avoids the need to parse the {@link NumberRegion} BEFORE the slottedPage is
   * allocated (chicken-and-egg: tag lookup needs decoded fields which need the heap which needs to be
   * sized which needs the value width...). The reader uses width to size the heap; type+region-lookup
   * are used at injection time after the heap is in place and the offset table is expanded.
   *
   * <p>
   * Default ON; gate off with {@code -Dsirix.valueElision.regionLookup.disable=true} for A/B size
   * measurement. The activation heuristic per-page already guarantees the column is only emitted when
   * total saved bytes strictly exceed the (2 bytes/slot + 4-byte length prefix) overhead.
   */
  private static final boolean VALUE_ELISION_ENABLED = !Boolean.getBoolean("sirix.valueElision.regionLookup.disable");

  /**
   * Whether string-region completeness is decided PER TAG rather than for the whole page.
   *
   * <p>
   * A fused string past the inline record cap becomes an overflow descriptor. Deciding completeness
   * for the page meant one oversized Title evicted every other field's strings from the column: no
   * dictionary, no sketch, no value elision, every string back inline in the record heap. Measured on
   * a 1M-row ClickBench load, 11.8 % of document leaves lost their string region that way and 3.75 M
   * values (53.7 MB) stayed in the heap because of it. Per tag, only the oversized field's own tag
   * leaves the region — see {@link StringRegion#isTagSuppressed}.
   *
   * <p>
   * Not final, and not derived at class-init only: a test proving the byte-identity of the old
   * behaviour has to flip it after this class is loaded, the same contract
   * {@code KeyValueLeafPage.ARRAY_ELEMENT_STRINGS_IN_REGION} documents. Read once per page in the
   * region build, never per slot.
   *
   * <p>
   * Kill switch: {@code -Dsirix.page.stringRegion.perTagCompleteness=false} restores the
   * all-or-nothing rule byte for byte.
   */
  public static boolean STRING_REGION_PER_TAG_COMPLETENESS =
      !"false".equals(System.getProperty("sirix.page.stringRegion.perTagCompleteness"));

  // ============================================================
  // Lever 4: NAME-KEY elision (writer + reader, fused OBJECT_NAMED_*)
  // ============================================================

  /**
   * Lever 4: nameKey elision via {@link ObjectKeyNameKeyRegion} lookup. When enabled and the page
   * emits at least one fused {@code OBJECT_NAMED_*} (kindIds 48-51) record, the per-record inline
   * {@code [signed-varint
   * nameKey]} field is stripped from the on-disk staged blob. The reader recovers the field's int
   * value at read-time via {@link ObjectKeyNameKeyRegion#nameKeyForSlot(byte[], int)} — a direct
   * slot-indexed lookup against the same region payload that powers SIMD nameKey scans (load-bearing
   * for query routing). The PAX region copy is therefore the single source of truth; the heap copy is
   * pure duplication and Lever 4 drops it.
   *
   * <p>
   * Activation is per-page: the writer only activates elision when the region is actually built
   * ({@code okCount > 0}) AND the total nameKey bytes stripped strictly exceed the per-elided-slot
   * 1-byte width overhead plus the 4-byte length-prefix int. The activation heuristic guarantees a
   * strict size win.
   *
   * <p>
   * Default ON; gate off with {@code -Dsirix.nameKeyElision.disable=true} for A/B size measurement.
   */
  private static final boolean NAME_KEY_ELISION_ENABLED = !Boolean.getBoolean("sirix.nameKeyElision.disable");

  /**
   * Per-thread per-slot nameKey elision flag. Bit 0 set = the slot's nameKey varint is being stripped
   * on this page. Cleared on entry to writer pre-scan.
   */
  private static final ThreadLocal<byte[]> SLOT_NAME_KEY_ELIDED_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread per-slot nameKey field offset (one short/slot). Holds the in-data-region offset of the
   * {@code [signed-varint nameKey]} field, derived from offset-table[3] (primitives) or [5]
   * (structurals). Zero for slots that are not nameKey-elided participants.
   */
  private static final ThreadLocal<short[]> SLOT_NAME_KEY_OFF_SCRATCH =
      ThreadLocal.withInitial(() -> new short[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread per-slot nameKey field width (one byte/slot, 1..5 valid range). Encodes
   * {@code DeltaVarIntCodec.computeSignedEncodedWidth(nameKey)}. Zero for slots that are not
   * nameKey-elided participants.
   */
  private static final ThreadLocal<byte[]> SLOT_NAME_KEY_WIDTH_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT]);

  /**
   * Per-thread reader-side packed-widths buffer for the name-key elision section. One byte per
   * fused-OBJECT_NAMED_* slot in slot-ascending order; length = elidedCount on the wire.
   */
  private static final ThreadLocal<byte[]> SLOT_NAME_KEY_WIDTH_PACKED_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT]);

  /**
   * Structural-flags byte bit positions for the compressed-blob header. Kept as constants so writer +
   * reader agree on bit layout.
   */
  private static final int STRUCT_FLAG_HASH_ELISION = 0x01;

  /** Flag bit: page contains a parentKey column (see {@link #PARENT_KEY_COLUMN_ENABLED}). */
  private static final int STRUCT_FLAG_PARENT_KEY_COLUMN = 0x02;

  /** Flag bit: page contains a pathNodeKey column (see {@link #PATH_NODE_KEY_COLUMN_ENABLED}). */
  private static final int STRUCT_FLAG_PATH_NODE_KEY_COLUMN = 0x04;

  /**
   * Flag bit: page has VALUE elision active for fused {@code OBJECT_NAMED_NUMBER} slots (see
   * {@link #VALUE_ELISION_ENABLED}). When set, the on-disk record bodies for fused-NUMBER slots have
   * their {@code [type:1][delta-varint]} payload bytes stripped, with the type-byte stored separately
   * in the blob (1 byte per elided slot, in slot-ascending order). The reader resolves the long value
   * via {@link NumberRegion#decodeValueAt} using the per-tag rank.
   */
  private static final int STRUCT_FLAG_VALUE_ELISION = 0x08;

  /**
   * Flag bit: page has NAME-KEY elision active for fused {@code OBJECT_NAMED_*} (kindIds 48-51) slots
   * (see {@link #NAME_KEY_ELISION_ENABLED}). When set, the on-disk record bodies for fused slots have
   * their inline {@code [signed-varint
   * nameKey]} field stripped; the reader recovers the value via
   * {@link ObjectKeyNameKeyRegion#nameKeyForSlot(byte[], int)} keyed by slot. Per-elided-slot disk
   * overhead: 1 byte (original heap width). The 4-byte length-prefix is shared across all elided
   * slots in slot-ascending order.
   */
  private static final int STRUCT_FLAG_NAME_KEY_ELISION = 0x10;

  /**
   * Flag bit: the page's value- and name-key-elision sections carry {@link ElisionDeriver}'s derived
   * form — an elided-slot bitmap (or the "every candidate" flag) plus sparse exception lists — instead
   * of the per-slot tuples that spelled out slot gap, type, width and region index.
   *
   * <p>
   * Set per page by the writer, so a resource can hold both forms and a reader dispatches on the bit
   * rather than on when the page was written. Clear on every page the kill switch
   * {@link #DERIVED_ELISION_SECTIONS} produced.
   */
  private static final int STRUCT_FLAG_DERIVED_ELISION = 0x20;

  /**
   * Flag bit: a second structural-flags byte follows, carrying the levers that did not fit the first.
   *
   * <p>
   * Set only when one of those levers is actually on, so a page that uses none of them writes the one
   * byte it always wrote. That is what lets the kill switches restore the pre-change encoding exactly.
   */
  private static final int STRUCT_FLAG_EXTENDED = 0x80;

  /** Extended flag bit: the page carries a right-sibling-key column. */
  private static final int EXT_FLAG_RIGHT_SIB_COLUMN = 0x01;

  /** Extended flag bit: the page carries a left-sibling-key column. */
  private static final int EXT_FLAG_LEFT_SIB_COLUMN = 0x02;

  /**
   * Whether the elision sections are written in their derived form.
   *
   * <p>
   * Kill switch: {@code -Dsirix.page.body.derivedElision=false} restores the per-slot tuples byte for
   * byte. The reader accepts both forms regardless of this flag — it dispatches on
   * {@link #STRUCT_FLAG_DERIVED_ELISION} — so a resource written with the switch off stays readable
   * with it on, and the other way round.
   *
   * <p>
   * Not final, and not read once at class init: a test proving the byte-identity of the old encoding
   * has to flip it after this class is loaded, the same contract
   * {@link #STRING_REGION_PER_TAG_COMPLETENESS} documents. Read once per page in the body encoder,
   * never per slot.
   */
  public static boolean DERIVED_ELISION_SECTIONS =
      !"false".equals(System.getProperty("sirix.page.body.derivedElision"));

  /**
   * Per-thread {@link ElisionDeriver} for the writer's plan-and-verify pass. Deliberately separate
   * from the reader's instance: a commit can load pages mid-serialization, and one derivation state
   * shared between the two directions would be an aliasing bug waiting for the first interleaving.
   */
  private static final ThreadLocal<ElisionDeriver> WRITER_ELISION_DERIVER =
      ThreadLocal.withInitial(ElisionDeriver::new);

  /**
   * Whether the pathNodeKey column goes to the wire in its compact form — a frame-of-reference
   * dictionary and, when it pays for itself, a run-length-encoded id lane.
   *
   * <p>
   * Kill switch: {@code -Dsirix.page.pathNodeKeyColumn.compact=false} keeps the random-access layout
   * on disk. Gated on {@link #DERIVED_ELISION_SECTIONS} as well, so the one switch that restores the
   * pre-change encoding restores this too. Not final, for the same reason the others are not.
   */
  public static boolean PATH_NODE_KEY_COLUMN_COMPACT =
      !"false".equals(System.getProperty("sirix.page.pathNodeKeyColumn.compact"));

  /** Writer-side destination for the compact pathNodeKey column; grown on demand, reused per thread. */
  private static final ThreadLocal<byte[]> PATH_NODE_KEY_COMPACT_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT + 1_160]);

  /** Reader-side destination for a compact pathNodeKey column expanded back to random access. */
  private static final ThreadLocal<byte[]> PATH_NODE_KEY_EXPANDED_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT + 1_160]);

  /**
   * How many stripped ranges one record can carry: parentKey, pathNodeKey, hash, value, name key —
   * with room for the structural columns that follow. Sized as a constant so the writer's and reader's
   * range arrays cannot disagree about it.
   */
  private static final int STRIP_RANGE_CAPACITY = 8;

  /** Range-start scratch for {@code stageEncodedHeap}; taken once per page, never per slot. */
  private static final ThreadLocal<int[]> STRIP_RANGE_FROM_SCRATCH =
      ThreadLocal.withInitial(() -> new int[STRIP_RANGE_CAPACITY]);

  /** Range-end scratch for {@code stageEncodedHeap}. */
  private static final ThreadLocal<int[]> STRIP_RANGE_TO_SCRATCH =
      ThreadLocal.withInitial(() -> new int[STRIP_RANGE_CAPACITY]);

  /**
   * Whether the right- and left-sibling keys move into their own columns. <b>Off by default, on the
   * measurement below.</b>
   *
   * <p>
   * In document order a right sibling is usually the very next node key and a left sibling the
   * previous one, so both collapse to two bits per slot under the codec the parentKey column already
   * uses: on a 105-field record load the columns take 275 B per page where the varints they replace
   * take 1,019, and the staged heap falls from 6.97 to 5.97 B per record.
   *
   * <p>
   * <b>And almost none of that reaches the wire.</b> Those varints are the same two bytes in every
   * record, so the body codec was already collapsing them for nothing, while the bit-packed column is
   * close to incompressible. Over 92 pages of that load the encoded body goes 34,348 → 36,115 B
   * (+4.3 % of the body, ~+14 B per page once the column codec's run-length lane is in play; +19 B
   * before it) — and the same figure appears on a high-entropy variant of the same shape, so it is the
   * column's own bytes rather than the fixture's regularity. On a synthetic page whose sibling keys
   * are exactly ±1 it comes out about 21 bytes ahead instead. The sign is data-dependent and the
   * magnitude is nil either way: 930 raw bytes per page become 14 on the wire.
   *
   * <p>
   * A raw-byte win that does not survive the codec is the trap this campaign already walked into once
   * with the elision tuples, so the lever ships proven and dormant rather than on. Turn it on with
   * {@code -Dsirix.page.body.structuralColumns=true} to re-measure: on data whose sibling keys are NOT
   * a constant delta — heavy update traffic, moved subtrees — the arithmetic can come out the other
   * way, and the switch is how to find out. Not final, so a byte-identity test can flip it after class
   * load.
   */
  public static boolean SIBLING_KEY_COLUMNS_ENABLED =
      Boolean.parseBoolean(System.getProperty("sirix.page.body.structuralColumns", "false"));

  /** Per-thread per-entry decoded right-sibling keys, the right-sibling column's input. */
  private static final ThreadLocal<long[]> SLOT_RIGHT_SIB_KEY_SCRATCH =
      ThreadLocal.withInitial(() -> new long[PageLayout.SLOT_COUNT]);

  /** Per-thread per-entry right-sibling varint widths, zero where the slot keeps the bytes inline. */
  private static final ThreadLocal<byte[]> SLOT_RIGHT_SIB_WIDTH_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT]);

  /** Per-thread per-entry right-sibling field offsets within the record's data region. */
  private static final ThreadLocal<short[]> SLOT_RIGHT_SIB_OFF_SCRATCH =
      ThreadLocal.withInitial(() -> new short[PageLayout.SLOT_COUNT]);

  /** Per-thread per-entry decoded left-sibling keys, the left-sibling column's input. */
  private static final ThreadLocal<long[]> SLOT_LEFT_SIB_KEY_SCRATCH =
      ThreadLocal.withInitial(() -> new long[PageLayout.SLOT_COUNT]);

  /** Per-thread per-entry left-sibling varint widths, zero where the slot keeps the bytes inline. */
  private static final ThreadLocal<byte[]> SLOT_LEFT_SIB_WIDTH_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT]);

  /** Per-thread per-entry left-sibling field offsets within the record's data region. */
  private static final ThreadLocal<short[]> SLOT_LEFT_SIB_OFF_SCRATCH =
      ThreadLocal.withInitial(() -> new short[PageLayout.SLOT_COUNT]);

  /** Writer-side encoded bytes of the right-sibling column. */
  private static final ThreadLocal<byte[]> RIGHT_SIB_COLUMN_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[StructuralKeyColumnCodec.maxEncodedSize(PageLayout.SLOT_COUNT)]);

  /** Writer-side encoded bytes of the left-sibling column. */
  private static final ThreadLocal<byte[]> LEFT_SIB_COLUMN_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[StructuralKeyColumnCodec.maxEncodedSize(PageLayout.SLOT_COUNT)]);

  /** Reader-side decoded right-sibling keys, one per entry. */
  private static final ThreadLocal<long[]> SLOT_RIGHT_SIB_READ_SCRATCH =
      ThreadLocal.withInitial(() -> new long[PageLayout.SLOT_COUNT]);

  /** Reader-side decoded left-sibling keys, one per entry. */
  private static final ThreadLocal<long[]> SLOT_LEFT_SIB_READ_SCRATCH =
      ThreadLocal.withInitial(() -> new long[PageLayout.SLOT_COUNT]);

  /** Reader-side per-entry right-sibling widths the expansion reinjects. */
  private static final ThreadLocal<byte[]> SLOT_RIGHT_SIB_READ_WIDTH_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT]);

  /** Reader-side staging for one {@link StructuralKeyColumnCodec} column's encoded bytes. */
  private static final ThreadLocal<byte[]> STRUCTURAL_COLUMN_READ_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[StructuralKeyColumnCodec.maxEncodedSize(PageLayout.SLOT_COUNT)]);

  /** Reader-side per-entry left-sibling widths the expansion reinjects. */
  private static final ThreadLocal<byte[]> SLOT_LEFT_SIB_READ_WIDTH_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT]);

  /**
   * A structural column's encode destination, grown to what the codec can need for this page.
   *
   * @param scratch the thread-local holding it
   * @param populatedCount entries the column covers
   * @return a buffer of at least {@code StructuralKeyColumnCodec.maxEncodedSize(populatedCount)} bytes
   */
  private static byte[] structuralColumnScratch(final ThreadLocal<byte[]> scratch, final int populatedCount) {
    byte[] buffer = scratch.get();
    final int needed = StructuralKeyColumnCodec.maxEncodedSize(populatedCount);
    if (buffer.length < needed) {
      buffer = new byte[needed];
      scratch.set(buffer);
    }
    return buffer;
  }

  /** {@code expandEntryInto} range discriminator: the slot's parentKey varint. */
  private static final int INJECT_PARENT_KEY = 0;

  /** {@code expandEntryInto} range discriminator: the slot's pathNodeKey varint. */
  private static final int INJECT_PATH_NODE_KEY = 1;

  /** {@code expandEntryInto} range discriminator: the slot's all-zero hash. */
  private static final int INJECT_HASH = 2;

  /** {@code expandEntryInto} range discriminator: the slot's elided payload. */
  private static final int INJECT_VALUE = 3;

  /** {@code expandEntryInto} range discriminator: the slot's elided name key. */
  private static final int INJECT_NAME_KEY = 4;

  /** {@code expandEntryInto} range discriminator: the slot's right-sibling varint. */
  private static final int INJECT_RIGHT_SIB_KEY = 5;

  /** {@code expandEntryInto} range discriminator: the slot's left-sibling varint. */
  private static final int INJECT_LEFT_SIB_KEY = 6;

  /**
   * Step over a deduplicated body's structural-flags byte — and the extended byte behind it, when the
   * first one says there is one.
   *
   * <p>
   * Three readers parse this header independently: the full deserialization, the region-table probe
   * and the regions-only decode. Sharing the step is what keeps a new flag from silently desynching
   * two of them, which is exactly what an extra byte nobody skipped would do.
   *
   * @param source positioned at the structural-flags byte
   */
  private static void skipStructuralFlags(final BytesIn<?> source) {
    if ((source.readByte() & STRUCT_FLAG_EXTENDED) != 0) {
      source.readByte();
    }
  }

  /** Post-codec attribution class: a fused {@code OBJECT_NAMED_*} record. */
  private static final int HEAP_CLASS_FUSED = 0;

  /** Post-codec attribution class: a structural {@code OBJECT} or {@code ARRAY} record. */
  private static final int HEAP_CLASS_STRUCTURAL = 1;

  /** Post-codec attribution class: everything else. */
  private static final int HEAP_CLASS_OTHER = 2;

  /** Node kind id of {@code OBJECT}, for the attribution's structural class. */
  private static final int OBJECT_KIND_ID = 24;

  /** Node kind id of {@code ARRAY}. */
  private static final int ARRAY_KIND_ID = 25;

  /** Diagnostic-only codec output, sized for whatever section the attribution is measuring. */
  private static final ThreadLocal<byte[]> DIAG_CODEC_OUT_SCRATCH = ThreadLocal.withInitial(() -> new byte[1]);

  /** Diagnostic-only LZ4 output, for attributing a body the LZ4 arm wrote. */
  private static final ThreadLocal<MemorySegment> DIAG_LZ4_OUT_SCRATCH =
      ThreadLocal.withInitial(() -> Arena.ofAuto().allocate(1));

  /** Diagnostic-only gather buffer for one class of the staged heap's records. */
  private static final ThreadLocal<MemorySegment> DIAG_GATHER_SCRATCH =
      ThreadLocal.withInitial(() -> Arena.ofAuto().allocate(1));

  /** Per-template last record kind, for the compact directory's predictability count. */
  private static final ThreadLocal<int[]> DIAG_TEMPLATE_LAST_KIND = ThreadLocal.withInitial(() -> new int[256]);

  /** Per-template last on-disk record length. */
  private static final ThreadLocal<int[]> DIAG_TEMPLATE_LAST_LENGTH = ThreadLocal.withInitial(() -> new int[256]);

  /** Grow (or re-allocate) the per-thread diagnostic gather segment. */
  private static MemorySegment diagGatherScratch(final int needed) {
    MemorySegment segment = DIAG_GATHER_SCRATCH.get();
    if (segment.byteSize() < needed) {
      segment = Arena.ofAuto().allocate(Math.max((long) needed, segment.byteSize() * 2L));
      DIAG_GATHER_SCRATCH.set(segment);
    }
    return segment;
  }

  /** Per-thread {@link ElisionDeriver} for the reader's metadata reconstruction. */
  private static final ThreadLocal<ElisionDeriver> READER_ELISION_DERIVER =
      ThreadLocal.withInitial(ElisionDeriver::new);

  /**
   * Test seam: the writer's deriver as the last page serialized on this thread left it.
   *
   * <p>
   * A witness that the exception lists are empty — that the derivation was exact on the fixture rather
   * than merely round-tripping through its own escape hatch — has to read them, and they are the
   * writer's state, not the page's bytes.
   *
   * @return the calling thread's writer-side deriver
   */
  static ElisionDeriver writerElisionDeriverForTesting() {
    return WRITER_ELISION_DERIVER.get();
  }

  /**
   * Per-thread per-entry scratch holding the wire type byte of every elided fused-primitive slot —
   * NUMBER's 2/3 subtype, a string's stored-form flag, 0 for a boolean. Written by the writer pre-scan
   * beside {@link #SLOT_VALUE_ELIDED_SCRATCH}, whose markers are an internal encoding the derived
   * section never sees.
   */
  private static final ThreadLocal<byte[]> SLOT_VALUE_DISK_TYPE_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[PageLayout.SLOT_COUNT]);

  /**
   * Enable / disable the pick-smaller per-page codec choice between {@link ZeroRunByteCodec} and
   * {@link ByteRunCodec}. Default ON — the reader always accepts both codec bytes, and the writer
   * picks whichever produces fewer bytes for the current page. Gate off with
   * {@code -Dsirix.byteRunCodec.disable=true} to force V1 zero-only RLE for A/B comparison.
   */
  private static final boolean BYTE_RUN_CODEC_ENABLED = !Boolean.getBoolean("sirix.byteRunCodec.disable");

  /**
   * Enable / disable the {@link SirixLZ77Codec} (codec id 3) participation in the pick-smallest codec
   * selection. Default ON. Gate off with {@code -Dsirix.lz77Codec.disable=true} to fall back to the
   * RLE-only codec choice for A/B comparison against the LZ4 baseline.
   *
   * <p>
   * The LZ77 codec catches intra-page 4-byte+ back-references (e.g. repeating record-header prefixes
   * like {@code 0x01 <tid> 0x00 0x00} across consecutive same-kind slots). It typically wins on heaps
   * with more than a few dozen slots.
   */
  private static final boolean LZ77_CODEC_ENABLED = !Boolean.getBoolean("sirix.lz77Codec.disable");

  /**
   * Probe cadence of the sticky-winner codec election ({@code -Dsirix.codecBakeoff.probeInterval},
   * default 16): every Nth page per serialization thread runs the full bake-off and re-elects the
   * winner; the pages in between compare zero-run and LZ77 and add byte-run only while it holds the
   * election (or, under {@link #CODEC_BAKEOFF_STICKY_ONLY}, encode with the elected codec only).
   * {@code 1} probes every page —
   * the exhaustive pick-smallest behavior, required for byte-identical golden files (see
   * {@link #emitSmallestBody}).
   */
  private static final int STICKY_PROBE_INTERVAL = Integer.getInteger("sirix.codecBakeoff.probeInterval", 16);

  /**
   * Pages at the start of each serialization thread that always probe, so the first election rests on
   * real evidence rather than the zero-initialized default.
   */
  private static final int STICKY_WARMUP_PAGES = 8;

  /**
   * Sticky-winner election state per serialization thread:
   * {@code {winnerCodecId, warmupPagesSeen, pagesSinceProbe}}. The winner id matches the wire codec
   * byte (0 = {@link ZeroRunByteCodec}, 2 = {@link ByteRunCodec}, 3 = {@link SirixLZ77Codec}).
   */
  private static final ThreadLocal<int[]> STICKY_CODEC = ThreadLocal.withInitial(() -> new int[4]);

  /**
   * Index into {@link #STICKY_CODEC} holding the codec the last body emitted on this thread actually
   * used — which is not always the elected one, since the pages between probes write the smaller of
   * zero-run and LZ77 whatever the election says. Read only by the post-codec attribution, so it can
   * charge each section under the codec the page really paid.
   */
  private static final int STICKY_LAST_EMITTED = 3;

  /**
   * Whether the pages between probes encode with the elected codec ALONE ({@code true}) instead of
   * always comparing zero-run and LZ77 ({@code false}, the default).
   *
   * <p>
   * Measured on a 1M-row ClickBench load (2026-08-30): the elected codec alone wrote the leaf class at
   * 1,093.3 MB, a bake-off on every page at 1,037.7 MB (−5.1 %), for +1 s on a 30 s load. Always
   * comparing the two codecs that decide page size recovers that at a fraction of the cost: zero-run
   * is near memcpy speed and LZ77 already runs on every page that elected it.
   *
   * <p>
   * Kill switch: {@code -Dsirix.codecBakeoff.stickyOnly=true}. Not final: the witness flips it after
   * this class is loaded, the same contract {@link #STRING_REGION_PER_TAG_COMPLETENESS} documents.
   * Read once per page body, never per slot.
   */
  static boolean CODEC_BAKEOFF_STICKY_ONLY = Boolean.getBoolean("sirix.codecBakeoff.stickyOnly");

  /**
   * Test seam: make {@code codec} the current thread's elected body codec with the warm-up spent and
   * the probe cadence reset, so the next page body is a between-probes page under that election.
   */
  static void electBodyCodecForTesting(final int codec) {
    final int[] sticky = STICKY_CODEC.get();
    sticky[0] = codec;
    sticky[1] = STICKY_WARMUP_PAGES;
    sticky[2] = 0;
  }

  /**
   * Reset the current thread's sticky-codec election so its next {@link #STICKY_WARMUP_PAGES} page
   * bodies run the full bake-off (exhaustive pick-smallest). Golden-byte tests MUST call this before
   * serializing: the election makes stored bytes a function of per-thread serialization history (see
   * {@link #emitSmallestBody}), so a page serialized after unrelated work on the same thread may be
   * encoded with the elected codec instead of the smallest one — which is exactly the run-to-run
   * variance a golden comparison has to neutralize.
   */
  public static void resetStickyCodecElectionForCurrentThread() {
    final int[] sticky = STICKY_CODEC.get();
    sticky[0] = 0;
    sticky[1] = 0;
    sticky[2] = 0;
  }

  /** Per-thread scratch for {@link ByteRunCodec} output. */
  private static final ThreadLocal<byte[]> V1_HEAP_V2_SCRATCH = ThreadLocal.withInitial(() -> new byte[128 * 1024]);

  /** Per-thread scratch for {@link SirixLZ77Codec} output. */
  private static final ThreadLocal<byte[]> V1_HEAP_V3_SCRATCH = ThreadLocal.withInitial(() -> new byte[128 * 1024]);

  /**
   * The chunk table of one page: a row per chunk, plus the META frame's decoded length. Reused across
   * pages, one instance per thread and direction, so framing a page allocates nothing.
   */
  static final class ChunkTable {
    /** Entry rank the chunk starts at, in populated-bitmap order. */
    final int[] firstEntry = new int[ChunkedBodyConfig.MAX_CHUNKS];
    /** Entries the chunk covers; the ranges partition the page's entries. */
    final int[] entryCount = new int[ChunkedBodyConfig.MAX_CHUNKS];
    /** Heap bytes the chunk decodes to. An int because one chunk can aggregate beyond 64 KiB. */
    final int[] rawLen = new int[ChunkedBodyConfig.MAX_CHUNKS];
    /** Bytes the chunk occupies on the wire. */
    final int[] encLen = new int[ChunkedBodyConfig.MAX_CHUNKS];
    /** Wire codec id, {@link ChunkedBodyConfig#CODEC_STORED} when the chunk is stored verbatim. */
    final int[] codec = new int[ChunkedBodyConfig.MAX_CHUNKS];
    /** XXH3-64 over the chunk's stored bytes. */
    final long[] hash = new long[ChunkedBodyConfig.MAX_CHUNKS];
    /** Write side only: where the chunk's encoded bytes sit in the frame-output scratch. */
    final int[] payloadOff = new int[ChunkedBodyConfig.MAX_CHUNKS];
    /**
     * Lazy read side only: the chunk's encoded bytes, read but not decoded. Handed to the page's
     * {@link LazyChunkedBody}, which clears these slots — this table is a per-thread scratch and must
     * not keep another page's buffers alive.
     */
    final byte[][] pendingWire = new byte[ChunkedBodyConfig.MAX_CHUNKS][];
    /** Chunks in the table. */
    int count;
    /** Decoded length of the page's META section run. */
    int metaRawLen;
  }

  /**
   * Record pages written with a chunk-framed body, and read back from one, since the last
   * {@link #resetChunkedBodyStats()}.
   *
   * <p>
   * Counted unconditionally rather than behind a diagnostic flag: the format is selected by a system
   * property, so "did the writer actually use it" is not otherwise observable — and a test that
   * cannot tell passes just as happily when the flag never reached the writer.
   */
  private static final LongAdder CHUNKED_BODIES_WRITTEN = new LongAdder();

  private static final LongAdder CHUNKED_BODIES_READ = new LongAdder();

  /**
   * The subset of {@link #CHUNKED_BODIES_WRITTEN} whose body took the deduped shape — template pool,
   * column and elision sections — rather than the degenerate one. Kept apart because the two shapes
   * frame very different META content, and a test that only ever produced the degenerate shape would
   * look like coverage it is not.
   */
  private static final LongAdder CHUNKED_DEDUP_BODIES_WRITTEN = new LongAdder();

  /** Record-page bodies written chunk-framed since the last {@link #resetChunkedBodyStats()}. */
  public static long chunkedBodiesWritten() {
    return CHUNKED_BODIES_WRITTEN.sum();
  }

  /** Chunk-framed bodies whose sections came from offset-table dedup, not the degenerate fallback. */
  public static long chunkedDedupBodiesWritten() {
    return CHUNKED_DEDUP_BODIES_WRITTEN.sum();
  }

  /**
   * Record-page bodies decoded from chunk framing since the last {@link #resetChunkedBodyStats()}.
   */
  public static long chunkedBodiesRead() {
    return CHUNKED_BODIES_READ.sum();
  }

  /** Reset both chunked-body counters. */
  public static void resetChunkedBodyStats() {
    CHUNKED_BODIES_WRITTEN.reset();
    CHUNKED_BODIES_READ.reset();
    CHUNKED_DEDUP_BODIES_WRITTEN.reset();
  }

  /** Chunk table being built by this thread's serializer. */
  private static final ThreadLocal<ChunkTable> WRITE_CHUNK_TABLE = ThreadLocal.withInitial(ChunkTable::new);

  /** Chunk table being parsed by this thread's deserializer. Kept apart from the write side. */
  private static final ThreadLocal<ChunkTable> READ_CHUNK_TABLE = ThreadLocal.withInitial(ChunkTable::new);

  /**
   * Per-frame encode result: {@code [encLen, wire codec, bake-off winner]}. The winner is what the
   * page may elect and is never the STORED pseudo-codec, which describes a frame rather than a way of
   * encoding one.
   */
  private static final ThreadLocal<int[]> FRAME_ENCODE_SCRATCH = ThreadLocal.withInitial(() -> new int[3]);

  /**
   * Per-thread buffer holding every encoded frame of the page being serialized. The frames are
   * written to the sink only after the chunk table that describes them, so they have to survive each
   * other — which the shared per-codec scratches, overwritten by the next frame, cannot do.
   */
  private static final ThreadLocal<byte[]> CHUNKED_FRAME_OUT_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[64 * 1024]);

  /** Per-thread {@link ZeroRunByteCodec} output, grown to fit a frame of {@code rawLen}. */
  private static byte[] zeroRunScratch(final int rawLen) {
    final int needed = ZeroRunByteCodec.maxEncodedSize(rawLen);
    byte[] buf = V1_HEAP_RLE_SCRATCH.get();
    if (buf.length < needed) {
      buf = new byte[Math.max(needed, buf.length * 2)];
      V1_HEAP_RLE_SCRATCH.set(buf);
    }
    return buf;
  }

  /** Per-thread {@link ByteRunCodec} output, grown to fit a frame of {@code rawLen}. */
  private static byte[] byteRunScratch(final int rawLen) {
    final int needed = ByteRunCodec.maxEncodedSize(rawLen);
    byte[] buf = V1_HEAP_V2_SCRATCH.get();
    if (buf.length < needed) {
      buf = new byte[Math.max(needed, buf.length * 2)];
      V1_HEAP_V2_SCRATCH.set(buf);
    }
    return buf;
  }

  /** Per-thread {@link SirixLZ77Codec} output, grown to fit a frame of {@code rawLen}. */
  private static byte[] lz77Scratch(final int rawLen) {
    final int needed = SirixLZ77Codec.maxEncodedSize(rawLen);
    byte[] buf = V1_HEAP_V3_SCRATCH.get();
    if (buf.length < needed) {
      buf = new byte[Math.max(needed, buf.length * 2)];
      V1_HEAP_V3_SCRATCH.set(buf);
    }
    return buf;
  }

  /** Per-thread frame-output buffer, grown once per page to the worst case of all its frames. */
  private static byte[] chunkedOutScratch(final long needed, final int populatedCount) {
    if (needed > Integer.MAX_VALUE) {
      throw new SirixIOException(
          "a page of " + populatedCount + " entries would need " + needed + " bytes of frame output");
    }
    byte[] buf = CHUNKED_FRAME_OUT_SCRATCH.get();
    if (buf.length < needed) {
      buf = new byte[(int) Math.max(needed, Math.min((long) buf.length * 2, Integer.MAX_VALUE))];
      CHUNKED_FRAME_OUT_SCRATCH.set(buf);
    }
    return buf;
  }

  /**
   * Per-thread template-pool bytes scratch. Worst case: SLOT_COUNT templates × (2 header bytes + 15
   * max field bytes) = ~17 KB.
   */
  private static final int MAX_TEMPLATE_POOL_BYTES = PageLayout.SLOT_COUNT * (2 + 16);

  private static final ThreadLocal<byte[]> TEMPLATE_POOL_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[MAX_TEMPLATE_POOL_BYTES]);

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
   * Per-thread fastutil map for packed-key → templateId lookup during dedup. Default return value is
   * -1 (sentinel for "not present").
   */
  private static final ThreadLocal<it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap> TEMPLATE_MAP =
      ThreadLocal.withInitial(() -> {
        final var m = new it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap(256);
        m.defaultReturnValue(-1);
        return m;
      });

  /**
   * Per-thread staging buffer for the dedup-transformed record heap before {@link ZeroRunByteCodec}
   * compression. Sized for a typical 32-KiB page; grows on demand for larger pages. Steady-state
   * zero-alloc once the buffer has reached peak size.
   */
  private static final ThreadLocal<byte[]> V1_HEAP_STAGING_SCRATCH = ThreadLocal.withInitial(() -> new byte[64 * 1024]);

  /**
   * Per-thread output buffer for the RLE encoder. Sized for RLE's worst-case expansion on a typical
   * page (see {@link ZeroRunByteCodec#maxEncodedSize}).
   */
  private static final ThreadLocal<byte[]> V1_HEAP_RLE_SCRATCH = ThreadLocal.withInitial(() -> new byte[128 * 1024]);

  /**
   * Encode {@code staging[0..totalBytes)} with the run-length/LZ77 body codecs and emit
   * {@code int compressedLen + 1 codec byte + payload} to {@code sink}. Both KEYVALUELEAFPAGE body
   * writers (template-dedup and inline) share this tail.
   *
   * <p>
   * <b>Sticky-winner election.</b> Only <i>probe</i> pages — the first {@link #STICKY_WARMUP_PAGES}
   * pages of a serialization thread, then every {@link #STICKY_PROBE_INTERVAL}-th page — run all
   * three codecs and (re-)elect the winner. The pages in between still encode zero-run and LZ77 and
   * write the smaller of the two; the election only decides whether byte-run is encoded as well. The
   * elected codec ALONE ({@link #CODEC_BAKEOFF_STICKY_ONLY}) rested on the winner being stable
   * within a workload, and it is not: index pages (NAME, PATH_SUMMARY) share the serialization
   * threads with record pages, their probes elect zero-run, and the record pages that follow were
   * written up to 3&times; their LZ77 size — 5.1 % of leaf bytes on a 1M-row ClickBench load. The
   * emitted codec byte keeps the format self-describing, so readers never see the difference.
   *
   * <p>
   * <b>Determinism caveat.</b> With {@code probeInterval > 1} the codec picked for a page depends on
   * per-thread serialization history, so stored bytes are no longer a pure function of page content
   * (sizes can differ by a few bytes run to run; content round-trips identically). Golden-file byte
   * comparisons must pin {@code -Dsirix.codecBakeoff.probeInterval=1}, which restores the exhaustive
   * pick-smallest behavior exactly.
   *
   * <p>
   * Codec rationale: the two RLE codecs catch single-byte runs (zero-run and constant-byte-run
   * respectively); the LZ77 variant catches 4-byte+ back-references within a 64&nbsp;KB window — the
   * dominant remaining redundancy after structural encoders have eliminated per-record offset-table
   * bytes. On Chicago-like record heaps LZ77 typically wins because record-header bytes repeat
   * verbatim across slots.
   */
  private static void emitSmallestBody(final BytesOut<?> sink, final MemorySegment staging, final int totalBytes) {
    final int[] sticky = STICKY_CODEC.get();
    final boolean warmup = sticky[1] < STICKY_WARMUP_PAGES;
    if (warmup) {
      sticky[1]++;
    }
    final boolean probe = STICKY_PROBE_INTERVAL <= 1 || warmup || sticky[2] >= STICKY_PROBE_INTERVAL - 1;
    if (probe) {
      sticky[2] = 0;
    } else {
      sticky[2]++;
      if (CODEC_BAKEOFF_STICKY_ONLY) {
        emitWithCodec(sticky[0], sink, staging, totalBytes);
        return;
      }
    }
    // Between probes the election decides only whether byte-run is worth encoding: zero-run runs at
    // memcpy speed and LZ77 is what nearly every record page elects anyway, so those two are always
    // encoded and compared. See CODEC_BAKEOFF_STICKY_ONLY for what a stale election used to cost.
    final boolean tryByteRun = BYTE_RUN_CODEC_ENABLED && (probe || sticky[0] == 2);

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
    final int v2Len = tryByteRun
        ? ByteRunCodec.encode(staging, 0L, totalBytes, v2Buf, 0)
        : Integer.MAX_VALUE;
    final int v3Len = LZ77_CODEC_ENABLED
        ? SirixLZ77Codec.encode(staging, 0L, totalBytes, v3Buf, 0)
        : Integer.MAX_VALUE;

    final int bestLen = Math.min(v0Len, Math.min(v2Len, v3Len));
    // Tie order mirrors the emission branches below (LZ77 > ByteRun > ZeroRun), so a
    // probe page emits exactly what the exhaustive pick would have. Disabled codecs
    // report Integer.MAX_VALUE and can never be elected. Only a probe (re-)elects: a
    // page in between compared what it encoded and wrote the smallest, which is all the
    // election is for.
    if (probe) {
      sticky[0] = bestLen == v3Len
          ? 3
          : bestLen == v2Len
              ? 2
              : 0;
    }
    if (bestLen == v3Len) {
      sink.writeInt(v3Len);
      sink.writeByte((byte) 3); // codec: 3 = SirixLZ77Codec
      sink.write(v3Buf, 0, v3Len);
      sticky[STICKY_LAST_EMITTED] = 3;
      if (PAGE_SECTION_DIAG) {
        PageSectionDiag.recordCodecLz77(v3Len);
      }
    } else if (bestLen == v2Len) {
      sink.writeInt(v2Len);
      sink.writeByte((byte) 2); // codec: 2 = ByteRunCodec
      sink.write(v2Buf, 0, v2Len);
      sticky[STICKY_LAST_EMITTED] = 2;
      if (PAGE_SECTION_DIAG) {
        PageSectionDiag.recordCodecByteRun(v2Len);
      }
    } else {
      sink.writeInt(v0Len);
      sink.writeByte((byte) 0); // codec: 0 = ZeroRunByteCodec
      sink.write(rle, 0, v0Len);
      sticky[STICKY_LAST_EMITTED] = 0;
      if (PAGE_SECTION_DIAG) {
        PageSectionDiag.recordCodecZeroRun(v0Len);
      }
    }
  }

  /**
   * Envelope flag: this {@link OverflowPage}'s payload is codec-compressed.
   *
   * <p>
   * Chosen over a bare added field so that the OFF state is byte-for-byte the layout this page kind
   * has always had: a resource written before this change carries flags {@code 0} and still reads,
   * and {@code -Dsirix.page.overflow.compress=false} reproduces today's bytes exactly. A resource
   * written WITH the flag cannot be read by an older build — {@code readVersionAndFlags} refuses an
   * unknown flag bit loudly rather than misparsing, which is the trade this bit exists to make.
   * </p>
   */
  static final byte FLAG_OVERFLOW_PAYLOAD_COMPRESSED = 0x01;

  /**
   * Whether an {@link OverflowPage} payload is offered to the codec bake-off on write.
   *
   * <p>
   * Overflow payloads were the last raw bytes in the file: leaf bodies compress internally, PAX
   * regions compress internally, and the resource's default byte handler is {@code none}, so this
   * class — the projection's column segments, the value-dictionary value blocks and every record
   * above {@link io.sirix.settings.Constants#MAX_RECORD_SIZE} — reached the disk verbatim. Measured
   * at 100M it is 17.45 GB, a quarter of the database.
   * </p>
   *
   * <p>
   * <b>Opt-IN, and measured that way.</b> Compressing them costs 4.69 GB of that class at 100M
   * (ratio 0.731) and makes COLD queries faster — the 43-query sum went 555.7 s to 447.4 s, because a
   * scan reads fewer bytes — but it makes a REPEATED scan slower, and that is what decided the
   * default. The OS page cache holds the payload compressed, so every pass decodes again, where an
   * uncompressed page was free on the second read: q16 went 20.68 s to 38.15 s hot and q17 20.49 s to
   * 38.12 s, their hot time collapsing onto their cold time, while their cold times were unchanged or
   * better. Two queries 85 % slower is a slowdown whatever the totals say, and the totals were a wash
   * (hot 483.5 s against 480.1 s).
   * </p>
   *
   * <p>
   * A cache of decoded payloads is the obvious answer and it is the wrong one at this scale: the
   * class is 12.76 GB compressed at 100M and the query envelope is an 8 GB heap, so what would have
   * to be cached cannot be. What could earn the default back is either a decoder fast enough that a
   * per-pass decode disappears into the I/O it saves, or compressing only the payload classes that
   * are not scan-hot — the value-dictionary blocks and overlong records rather than the projection's
   * column segments — which needs the writer to know which it is holding, and buys correspondingly
   * less. Where it already pays with no caveat is a cold or I/O-bound workload: the 43-query cold sum
   * fell 555.7 s to 447.4 s. Turn it on with {@code -Dsirix.page.overflow.compress=true}; a resource
   * written with it cannot be read by a build that predates the flag bit.
   * </p>
   */
  private static final boolean OVERFLOW_PAYLOAD_COMPRESSION_ENABLED =
      Boolean.parseBoolean(System.getProperty("sirix.page.overflow.compress", "false"));

  /**
   * Below this the three encode passes cost more than the bytes they could save, and the 5-byte
   * compressed framing can only make the page bigger.
   */
  private static final int OVERFLOW_COMPRESSION_MIN_BYTES = 64;

  /** Per-thread election result, so a page-sized decision allocates nothing. */
  private static final class OverflowPayloadCodecResult {
    private byte[] stored;
    private int storedLength;
    private int codec;
  }

  private static final ThreadLocal<OverflowPayloadCodecResult> OVERFLOW_ENCODE_RESULT =
      ThreadLocal.withInitial(OverflowPayloadCodecResult::new);

  /** Per-thread staging for a compressed payload being read back. */
  private static final ThreadLocal<byte[]> OVERFLOW_DECODE_SCRATCH = ThreadLocal.withInitial(() -> new byte[1 << 16]);

  /**
   * Per-thread NATIVE landing area for an LZ77 frame being read back.
   *
   * <p>
   * {@link SirixLZ77Codec#decode} dispatches to the C decoder only when its output is native-backed
   * with tail slack; a heap output silently takes the Java decoder, measured here at 3.0 GB/s against
   * 16.9 GB/s native on a 32 KB frame — 5.6&times;. An {@link OverflowPage} must own a heap array (its
   * constructor's contract, so nothing retains a reservoir view), so the frame is decoded natively and
   * then copied out. The copy runs at memcpy speed and is bought back many times over.
   * </p>
   *
   * <p>
   * {@link Arena#ofAuto()} rather than a confined or shared arena: the segment is reachable only from
   * this thread-local, so it is freed when the thread and the local die, with no close() to get wrong
   * on a serialization thread that outlives any one page.
   * </p>
   */
  private static final ThreadLocal<MemorySegment> OVERFLOW_DECODE_NATIVE =
      ThreadLocal.withInitial(() -> Arena.ofAuto().allocate(1 << 16));

  /** Below this the native detour's extra copy costs more than the faster decoder saves. */
  private static final int OVERFLOW_NATIVE_DECODE_MIN_BYTES = 1 << 10;

  /**
   * Exhaustive pick-smallest over the same three codecs the leaf body uses, plus STORED.
   *
   * <p>
   * Deliberately NOT the sticky election of {@link #emitSmallestBody}: that election is per
   * serialization thread and shared across page kinds, and it has already been measured to poison
   * record pages once a different kind probes on the same thread. An overflow page is large — the
   * whole class exists for payloads above 1 KB — so the encode cost is amortised over many KB and
   * an exhaustive comparison is affordable where it would not be for a small page.
   * </p>
   *
   * <p>
   * STORED is what makes this safe on incompressible input: an FSST-compressed string region or a
   * bit-packed id lane can come back LARGER from every codec, and a storage lever that can enlarge a
   * page has no business shipping. Nothing is written unless it is strictly smaller than the raw
   * payload.
   * </p>
   *
   * @return {@code true} when a codec beat STORED, with {@code result} populated
   */
  private static boolean electOverflowPayloadCodec(final MemorySegment payload, final long payloadOffset,
      final int totalBytes, final OverflowPayloadCodecResult result) {
    final int maxV0 = ZeroRunByteCodec.maxEncodedSize(totalBytes);
    final int maxV2 = ByteRunCodec.maxEncodedSize(totalBytes);
    final int maxV3 = SirixLZ77Codec.maxEncodedSize(totalBytes);

    byte[] rle = V1_HEAP_RLE_SCRATCH.get();
    final int maxRleSize = Math.max(maxV0, Math.max(maxV2, maxV3));
    if (rle.length < maxRleSize) {
      rle = new byte[maxRleSize];
      V1_HEAP_RLE_SCRATCH.set(rle);
    }
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

    final int v0Len = ZeroRunByteCodec.encode(payload, payloadOffset, totalBytes, rle, 0);
    final int v2Len = BYTE_RUN_CODEC_ENABLED
        ? ByteRunCodec.encode(payload, payloadOffset, totalBytes, v2Buf, 0)
        : Integer.MAX_VALUE;
    final int v3Len = LZ77_CODEC_ENABLED
        ? SirixLZ77Codec.encode(payload, payloadOffset, totalBytes, v3Buf, 0)
        : Integer.MAX_VALUE;

    final int bestLen = Math.min(v0Len, Math.min(v2Len, v3Len));
    // The 5 bytes of compressed framing (stored length + codec byte) cost more than the 1 byte the
    // stored form pays, so a codec must beat the raw payload by more than the difference to be worth
    // electing. Tie order mirrors emitSmallestBody: LZ77 > ByteRun > ZeroRun.
    if (bestLen >= totalBytes - (Integer.BYTES + 1)) {
      return false;
    }
    if (bestLen == v3Len) {
      result.stored = v3Buf;
      result.codec = 3;
    } else if (bestLen == v2Len) {
      result.stored = v2Buf;
      result.codec = 2;
    } else {
      result.stored = rle;
      result.codec = 0;
    }
    result.storedLength = bestLen;
    return true;
  }

  /**
   * Reads a compressed {@link OverflowPage} payload back. {@code decodedLength} is the length the
   * writer recorded; the decoder must reproduce exactly that many bytes or the record is corrupt.
   */
  private static Page deserializeCompressedOverflowPayload(final BytesIn<?> source, final int decodedLength) {
    final int storedLength = source.readInt();
    final byte codec = source.readByte();
    final long remaining = source.remaining();
    if (storedLength < 0 || storedLength > remaining) {
      throw new IllegalStateException("Corrupt compressed OverflowPage payload length " + storedLength + " (only "
          + remaining + " bytes remain in the source)");
    }
    // The LZ77 decoder's native fast path reads past the frame, so its input buffer carries the tail
    // slack the codec documents; the RLE decoders ignore it.
    final int required = storedLength + SirixLZ77Codec.NATIVE_INPUT_TAIL_SLACK;
    byte[] in = OVERFLOW_DECODE_SCRATCH.get();
    if (in.length < required) {
      in = new byte[Math.max(required, in.length * 2)];
      OVERFLOW_DECODE_SCRATCH.set(in);
    }
    source.read(in, 0, storedLength);

    final byte[] data = new byte[decodedLength];
    final int produced;
    if (codec == 3 && decodedLength >= OVERFLOW_NATIVE_DECODE_MIN_BYTES && SirixLZ77NativeDecoder.isAvailable()) {
      MemorySegment landing = OVERFLOW_DECODE_NATIVE.get();
      // NATIVE_OUTPUT_TAIL_SLACK, not the INPUT constant: the dispatch tests the OUTPUT against 64
      // bytes of slack and the input against 16. This read INPUT until it was measured, which left
      // the landing 48 bytes short and the native path silently declined — the detour was inert.
      final long needed = (long) decodedLength + SirixLZ77Codec.NATIVE_OUTPUT_TAIL_SLACK;
      if (landing.byteSize() < needed) {
        landing = Arena.ofAuto().allocate(Math.max(needed, landing.byteSize() * 2));
        OVERFLOW_DECODE_NATIVE.set(landing);
      }
      produced = SirixLZ77Codec.decode(in, 0, storedLength, landing, 0L);
      if (produced == decodedLength) {
        MemorySegment.copy(landing, ValueLayout.JAVA_BYTE, 0L, data, 0, decodedLength);
      }
    } else {
      final MemorySegment out = MemorySegment.ofArray(data);
      produced = switch (codec) {
        case 0 -> ZeroRunByteCodec.decode(in, 0, storedLength, out, 0L);
        case 2 -> ByteRunCodec.decode(in, 0, storedLength, out, 0L);
        case 3 -> SirixLZ77Codec.decode(in, 0, storedLength, out, 0L);
        default -> throw new IllegalStateException("Unknown OverflowPage payload codec " + codec);
      };
    }
    if (produced != decodedLength) {
      throw new IllegalStateException(
          "Corrupt OverflowPage payload: codec " + codec + " produced " + produced + " bytes, expected "
              + decodedLength);
    }
    return new OverflowPage(data);
  }

  /**
   * Non-probe page of the sticky-winner election: encode with the elected codec only and emit
   * {@code int compressedLen + 1 codec byte + payload}.
   */
  private static void emitWithCodec(final int codec, final BytesOut<?> sink, final MemorySegment staging,
      final int totalBytes) {
    STICKY_CODEC.get()[STICKY_LAST_EMITTED] = codec;
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
   * Per-thread LZ4 compressor reused for the page heap compression. HIGH_COMPRESSION mode: under
   * {@code -Dsirix.compression=none} the outer byte-pipeline LZ4 is disabled, so the only compressor
   * operating on the KVL heap is this inner one — we need its ratio to be as close as possible to the
   * double-LZ4 path's effective compression. LZ4_HC is 5-15× slower to encode than FAST but matches
   * zlib ratio on typical DB content; shred throughput has been measured as still adequate for the
   * baseline bench. Toggle via {@code -Dsirix.heapLz4.mode=fast} to restore the old FAST behaviour.
   *
   * <p>
   * Lazily created per thread to avoid static-init ordering issues with the FFI linker.
   */
  private static final ThreadLocal<FFILz4Compressor> V1_HEAP_LZ4 =
      ThreadLocal.withInitial(() -> FFILz4Compressor.isNativeAvailable()
          ? new FFILz4Compressor(resolveHeapLz4Mode())
          : null);

  /**
   * LZ4HC compression level (1-12; 9 is the liblz4 default, 12 is the max). Configurable via
   * {@code -Dsirix.heapLz4.hcLevel=<N>}. Higher is slower but squeezes a few more bytes out; 12 is
   * 2-3× slower than 9 for roughly 1-2% better ratio on typical Sirix heap content.
   */
  private static final int HEAP_LZ4_HC_LEVEL =
      Math.min(12, Math.max(1, Integer.getInteger("sirix.heapLz4.hcLevel", 9)));

  /**
   * Pure-structural encoder mode. Default is {@code true} — the heap body is compressed using only
   * the structural {@link ZeroRunByteCodec} fallback and the schema-aware PAX / template-pool /
   * hash-elision encoders. LZ4 is disabled on the write path entirely so the whole compression budget
   * has to come from the structural stack. Read path still accepts LZ4-encoded pages (codec byte ==
   * 1) for backwards compatibility with any databases produced during the prototyping phase.
   *
   * <p>
   * Set {@code -Dsirix.heapLz4.enable=true} to re-enable LZ4 HC (costs ~9 s of 36 s cold-100M CPU;
   * used only for A/B-measuring the structural stack against the LZ4 baseline).
   */
  /**
   * Inner heap-LZ4 participates in the per-page pick-smallest codec choice. Default OFF — measurement
   * on the 100M bench showed heap-LZ4 regressed storage by ~400 MB when combined with the white-box
   * columns (hash elision + parentKey column + pathNodeKey column), because LZ4's pattern matcher
   * competes with the column encoders for the same bytes and its output framing dominates over the
   * already-tight column encoding. The pure-white-box stack (LZ77 + ByteRun + ZeroRun codecs on
   * staging blob, column extractors for hash/parent/pathNodeKey) beats heap-LZ4 on this workload.
   *
   * <p>
   * Gate on via {@code -Dsirix.heapLz4.enable=true} for workloads with heterogeneous page shapes
   * where the column extractors don't pay off.
   */
  private static final boolean HEAP_LZ4_DISABLED = !Boolean.getBoolean("sirix.heapLz4.enable");

  /**
   * When {@code -Dsirix.pageSectionDiag=true} is set, the page serializer records byte counts per
   * section (header+bitmap, encoded body, region table, overlong, FSST) via {@link PageSectionDiag}.
   * Emits a cumulative breakdown on JVM shutdown. Pure diagnostic; off by default.
   */
  private static final boolean PAGE_SECTION_DIAG = Boolean.getBoolean("sirix.pageSectionDiag");

  /**
   * Test seam: whether the section diagnostic is active in this JVM. The gate is a static final read
   * at class initialisation so the branch folds away when it is off, which also means a suite
   * asserting on the counters cannot turn it on for itself — it has to ASSERT it is on and let the
   * build provide it, because with the gate off every counter reads zero and a zero from a disabled
   * instrument is indistinguishable from a zero from a healthy one.
   */
  static boolean sectionDiagEnabled() {
    return PAGE_SECTION_DIAG;
  }

  /**
   * Marks a page's FSST section as holding a dictionary reference; {@code 0} means the page has none.
   * Every other value — notably the positive length an embedded table once carried here — is rejected
   * at read time.
   */
  private static final int FSST_SYMBOL_TABLE_REFERENCE_MARKER = -1;

  private static FFILz4Compressor.CompressionMode resolveHeapLz4Mode() {
    final String prop = System.getProperty("sirix.heapLz4.mode", "hc").toLowerCase();
    return switch (prop) {
      case "fast" -> FFILz4Compressor.CompressionMode.FAST;
      case "hc", "high", "high_compression", "highcompression" -> FFILz4Compressor.CompressionMode.HIGH_COMPRESSION;
      default -> throw new IllegalArgumentException("Unknown sirix.heapLz4.mode='" + prop + "' (expected: fast, hc)");
    };
  }

  /**
   * Per-thread compressed-output scratch for V1 heap LZ4. Sized for the LZ4 worst case (slightly
   * larger than input).
   */
  private static final ThreadLocal<MemorySegment> V1_HEAP_LZ4_OUT = ThreadLocal.withInitial(() -> {
    // Start at 128 KiB; {@link #lz4OutScratch} grows it as needed.
    return java.lang.foreign.Arena.ofAuto().allocate(128 * 1024);
  });

  /**
   * Per-thread staging MemorySegment (native-backed) for the pre-LZ4 heap. Native-backed so LZ4's FFI
   * call works without JNI round-trips.
   */
  private static final ThreadLocal<MemorySegment> V1_HEAP_LZ4_STAGING =
      ThreadLocal.withInitial(() -> java.lang.foreign.Arena.ofAuto().allocate(128 * 1024));

  /**
   * Per-thread pointer to the decompressed body blob and its heap-section offset. Bridges the
   * compactDir/templatePool parse pass and the per-record expansion pass so we don't re-decompress or
   * walk the blob twice.
   */
  private static final ThreadLocal<MemorySegment> BLOB_STAGING_HOLDER = new ThreadLocal<>();
  private static final ThreadLocal<Long> BLOB_HEAP_OFFSET_HOLDER = ThreadLocal.withInitial(() -> 0L);

  /**
   * Grow (or re-allocate) the per-thread V1 staging segment to at least {@code needed} bytes. Uses
   * {@link java.lang.foreign.Arena#ofAuto} so the previous segment's memory is GC'd when no longer
   * referenced.
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
      if (page.id == RESERVED_PAGE_KIND_ID_14) {
        throw new ExceptionInInitializerError("PageKind ID 14 is permanently reserved");
      }
      final PageKind duplicateId = INSTANCEFORID.put(page.id, page);
      if (duplicateId != null) {
        throw new ExceptionInInitializerError(
            "Duplicate PageKind ID " + (page.id & 0xFF) + " for " + duplicateId + " and " + page);
      }
      final PageKind duplicateClass = INSTANCEFORCLASS.put(page.clazz, page);
      if (duplicateClass != null) {
        throw new ExceptionInInitializerError(
            "Duplicate PageKind class " + page.clazz.getName() + " for " + duplicateClass + " and " + page);
      }
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
   * @param id unique identifier
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
   * compressed bytes back to the provided sink. Uses the MemorySegment path when available to avoid
   * intermediate byte[] allocations.
   */
  private static byte[] compress(ResourceConfiguration resourceConfig, BytesIn<?> uncompressedBytes,
      byte[] uncompressedArray, long uncompressedLength) {
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
   * @param sink {@link BytesOut<?>} instance
   * @param page {@link Page} implementation
   */
  public abstract void serializePage(final ResourceConfiguration ResourceConfiguration, final BytesOut<?> sink,
      final Page page, final SerializationType type);

  /**
   * Serialize a caller-owned disposable page without publishing serializer-only native state onto the
   * page that crosses an asynchronous hand-off.
   *
   * <p>
   * Only {@link #KEYVALUELEAFPAGE} has such state (its writer-built {@link RegionTable}); every other
   * kind rejects this specialized entry point so a future caller cannot silently assume an ownership
   * contract that kind does not implement.
   * </p>
   */
  public void serializeDisposablePage(final ResourceConfiguration resourceConfiguration, final BytesOut<?> sink,
      final Page page, final SerializationType type) {
    throw new UnsupportedOperationException(name() + " has no disposable serialization path");
  }

  /**
   * Deserialize page.
   *
   * @param resourceConfiguration the resource configuration
   * @param source {@link BytesIn} instance
   * @return page instance implementing the {@link Page} interface
   */
  public Page deserializePage(final ResourceConfiguration resourceConfiguration, final BytesIn<?> source,
      final SerializationType type) {
    return deserializePage(resourceConfiguration, source, type, null);
  }

  /**
   * Deserialize page with optional DecompressionResult for zero-copy support.
   * 
   * <p>
   * When decompressionResult is provided, KeyValueLeafPages can take ownership of the decompression
   * buffer and use it directly as slotMemory.
   *
   * @param resourceConfiguration the resource configuration
   * @param source {@link BytesIn} instance
   * @param type serialization type
   * @param decompressionResult optional decompression result for zero-copy (may be null)
   * @return page instance implementing the {@link Page} interface
   */
  public abstract Page deserializePage(final ResourceConfiguration resourceConfiguration, final BytesIn<?> source,
      final SerializationType type, final ByteHandler.DecompressionResult decompressionResult);

  /**
   * Deserialize a page without expanding the records a caller has not asked for yet.
   *
   * <p>
   * The page comes back complete in every respect a reader can observe — header, slot bitmap,
   * directory, regions, references — but the record heap is filled in one chunk at a time, the first
   * time a slot inside that chunk is read. Worth asking for when the load is triggered by a point
   * lookup, where the page is opened to answer for one slot and the other thousand are decoded for
   * nothing; not worth it for a scan, which reads every slot and would pay the gate for no saving.
   *
   * <p>
   * Only a chunk-framed {@link #KEYVALUELEAFPAGE} can answer lazily. Every other kind, and every page
   * whose body is not chunk-framed, decodes eagerly and returns a page with nothing outstanding — the
   * caller cannot tell the difference except by how long the call took.
   *
   * @param resourceConfiguration the resource configuration
   * @param source {@link BytesIn} instance
   * @param type serialization type
   * @param decompressionResult optional decompression result for zero-copy (may be null)
   * @return page instance implementing the {@link Page} interface
   */
  public Page deserializePageLazily(final ResourceConfiguration resourceConfiguration, final BytesIn<?> source,
      final SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
    return deserializePage(resourceConfiguration, source, type, decompressionResult);
  }

  /**
   * Decode only the PAX regions of a record page, skipping its record heap entirely.
   *
   * <p>
   * Supported by {@link #KEYVALUELEAFPAGE} alone — it is the only kind that carries a
   * {@link RegionTable}. Every other kind returns {@code null}, which callers read as "no column-only
   * path here, use the full page".
   *
   * @param resourceConfiguration the resource configuration
   * @param source {@link BytesIn} positioned at the page envelope (kind byte already consumed)
   * @param regionKindMask bitmask of the region kinds to read; see {@link RegionTable#maskOf(byte)}
   * @param regionDeferMask subset of {@code regionKindMask} to leave compressed until the caller
   *        actually asks for it
   * @return the decoded regions, or {@code null} when this page kind has none
   */
  public RegionsOnlyPage deserializeRegionsOnlyPage(final ResourceConfiguration resourceConfiguration,
      final BytesIn<?> source, final int regionKindMask, final int regionDeferMask) {
    return null;
  }

  /**
   * Parse the fixed header of a record page and report where its region table begins.
   *
   * <p>
   * The point of a separate probe: the region table sits behind a variable-length body, so its offset
   * cannot be known without reading the header — but once known, a scan can fetch the table alone
   * instead of the whole page. A few hundred bytes of header decide the range of the second read.
   *
   * @param source positioned at the page envelope (kind byte already consumed)
   * @param out receives {@code [recordPageKey, revision, populatedCount, fsstDictId,
   *        hasCompleteColumnCoverage]}. The dictionary id is only knowable this early on a chunked page,
   *        which hoists it into the body prefix; a monolith page keeps it in the tail and reports
   *        {@code 0} here
   * @param bitmapOut receives the page's {@link PageLayout#BITMAP_WORDS} slot-bitmap words; pass
   *        {@code null} to skip the bitmap. A caller that may have to merge page fragments MUST pass
   *        one: the bitmap is what says which slots a fragment defines, and a fragment without it is
   *        refused by the merge, silently sending the whole page back to the record path.
   * @return byte offset of the region table, relative to the same origin as {@code source}
   */
  public long probeRegionTableOffset(final BytesIn<?> source, final long[] out, final long @Nullable [] bitmapOut) {
    throw new UnsupportedOperationException("no region table on " + this);
  }

  /**
   * Decode a region table from a source positioned at its first byte, with the page identity the
   * caller already learned from {@link #probeRegionTableOffset}.
   *
   * @param slotBitmap the page's slot bitmap as filled by the probe, or {@code null} when the caller
   *        did not ask for it — the resulting page then reports
   *        {@link RegionsOnlyPage#hasSlotBitmap()} false and cannot take part in a fragment merge
   */
  public RegionsOnlyPage deserializeRegionTableAt(final ResourceConfiguration resourceConfiguration,
      final BytesIn<?> source, final long pageKey, final int revision, final int populatedCount,
      final long fsstSymbolTableId, final int regionKindMask, final int regionDeferMask,
      final long @Nullable [] slotBitmap, final boolean hasCompleteColumnCoverage) {
    throw new UnsupportedOperationException("no region table on " + this);
  }

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

  /**
   * Shared per-entry in-memory length derivation; see {@link SlottedPageDecodeState}. Bound per page,
   * reused across pages so the derivation itself never allocates.
   */
  private static final ThreadLocal<SlottedPageDecodeState> SLOTTED_PAGE_DECODE_STATE =
      ThreadLocal.withInitial(SlottedPageDecodeState::new);

  /**
   * Body-section staging carrier; see {@link BodySections}. One per writer thread, rebound per page.
   */
  private static final ThreadLocal<BodySections> BODY_SECTIONS = ThreadLocal.withInitial(BodySections::new);

  /**
   * Derives an entry's in-memory data length: the space its record occupies on the page heap once
   * every byte the writer stripped has been added back.
   *
   * <p>
   * The widths come from page metadata alone — the template's field count and offset-table deltas,
   * the hash-elision bit, the pathNodeKey column's per-slot participation bit, and the two elision
   * sections. No input is read from the record heap, which is what lets a page's directory be laid
   * out before a single record byte has been decoded.
   *
   * <p>
   * Bind a page's sections once with the {@code bind*} methods — one per format lever — then either
   * call {@link #deriveAll} to walk every entry, or {@link #inMemLengthOf} per entry when the caller
   * drives its own walk. The carrier is thread-local and reused across pages; the per-entry method
   * allocates nothing.
   */
  static final class SlottedPageDecodeState {
    /** Packed (on-disk length, kindId) per entry, in populated-bitmap rank order. */
    private int[] compactDir;
    /** Template id per entry. */
    private byte[] slotTemplateIds;
    /** The page's template pool, addressed through {@link #templateOffsets}. */
    private byte[] templatePool;
    private int[] templateOffsets;
    /** Receives each entry's in-memory data length. */
    private int[] inMemDataLengths;

    private boolean hashElisionActive;
    /** Entry-indexed: bit set when the writer dropped an all-zero hash. */
    private byte[] zeroHashBitmap;

    private boolean parentKeyColumnActive;
    /** Decoded parentKey per entry, re-encoded as a delta against the slot's node key on expansion. */
    private long[] parentKeyValues;
    /** Receives the parentKey varint width the expansion pass has to reinject. */
    private byte[] parentKeyWidths;

    private boolean rightSibColumnActive;
    /** Decoded right-sibling key per entry, re-encoded as a delta against the node key on expansion. */
    private long[] rightSibKeyValues;
    /** Receives the right-sibling varint width the expansion pass has to reinject. */
    private byte[] rightSibKeyWidths;

    private boolean leftSibColumnActive;
    private long[] leftSibKeyValues;
    private byte[] leftSibKeyWidths;

    private boolean pathNodeKeyColumnActive;
    /** Raw {@link PathNodeKeyRegion} payload, slot-bitmap indexed. */
    private byte[] pathNodeKeyColumnBytes;
    private byte[] pathNodeKeyWidths;

    private boolean valueElisionActive;
    private int valueElidedCount;
    private short[] valueElidedSlots;
    private int[] valueElidedWidths;
    private short[] valueOffs;
    private short[] valueWidths;

    private boolean nameKeyElisionActive;
    private byte[] nameKeyElidedWidthsPacked;
    private short[] nameKeyOffs;
    private byte[] nameKeyWidths;

    /**
     * Reinjection-range scratch, one set per decode-state instance and therefore per thread. Held here
     * rather than in a {@link ThreadLocal} because the carrier already is one, and because a chunk
     * expanded lazily on another thread gets that thread's carrier.
     */
    private final int[] injectRangeOffsets = new int[STRIP_RANGE_CAPACITY];
    private final int[] injectRangeWidths = new int[STRIP_RANGE_CAPACITY];
    private final int[] injectRangeKinds = new int[STRIP_RANGE_CAPACITY];

    /** Cursor into the value-elision section; advances per named slot, in slot-ascending order. */
    private int valueElidedReadCursor;
    /** Cursor into the nameKey-elision packed-widths section; advances per fused-named slot. */
    private int nameKeyElidedReadCursor;

    void bindTemplates(final int[] compactDir, final byte[] slotTemplateIds, final byte[] templatePool,
        final int[] templateOffsets, final int[] inMemDataLengths) {
      this.compactDir = compactDir;
      this.slotTemplateIds = slotTemplateIds;
      this.templatePool = templatePool;
      this.templateOffsets = templateOffsets;
      this.inMemDataLengths = inMemDataLengths;
    }

    void bindHashElision(final boolean hashElisionActive, final byte[] zeroHashBitmap) {
      this.hashElisionActive = hashElisionActive;
      this.zeroHashBitmap = zeroHashBitmap;
    }

    void bindParentKeyColumn(final boolean parentKeyColumnActive, final long[] parentKeyValues,
        final byte[] parentKeyWidths) {
      this.parentKeyColumnActive = parentKeyColumnActive;
      this.parentKeyValues = parentKeyValues;
      this.parentKeyWidths = parentKeyWidths;
    }

    void bindSiblingKeyColumns(final boolean rightSibColumnActive, final long[] rightSibKeyValues,
        final byte[] rightSibKeyWidths, final boolean leftSibColumnActive, final long[] leftSibKeyValues,
        final byte[] leftSibKeyWidths) {
      this.rightSibColumnActive = rightSibColumnActive;
      this.rightSibKeyValues = rightSibKeyValues;
      this.rightSibKeyWidths = rightSibKeyWidths;
      this.leftSibColumnActive = leftSibColumnActive;
      this.leftSibKeyValues = leftSibKeyValues;
      this.leftSibKeyWidths = leftSibKeyWidths;
    }

    void bindPathNodeKeyColumn(final boolean pathNodeKeyColumnActive, final byte[] pathNodeKeyColumnBytes,
        final byte[] pathNodeKeyWidths) {
      this.pathNodeKeyColumnActive = pathNodeKeyColumnActive;
      this.pathNodeKeyColumnBytes = pathNodeKeyColumnBytes;
      this.pathNodeKeyWidths = pathNodeKeyWidths;
    }

    void bindValueElision(final boolean valueElisionActive, final int valueElidedCount, final short[] valueElidedSlots,
        final int[] valueElidedWidths, final short[] valueOffs, final short[] valueWidths) {
      this.valueElisionActive = valueElisionActive;
      this.valueElidedCount = valueElidedCount;
      this.valueElidedSlots = valueElidedSlots;
      this.valueElidedWidths = valueElidedWidths;
      this.valueOffs = valueOffs;
      this.valueWidths = valueWidths;
    }

    void bindNameKeyElision(final boolean nameKeyElisionActive, final byte[] nameKeyElidedWidthsPacked,
        final short[] nameKeyOffs, final byte[] nameKeyWidths) {
      this.nameKeyElisionActive = nameKeyElisionActive;
      this.nameKeyElidedWidthsPacked = nameKeyElidedWidthsPacked;
      this.nameKeyOffs = nameKeyOffs;
      this.nameKeyWidths = nameKeyWidths;
    }

    /**
     * Derive every entry's length, stamping {@code inMemDataLengths} and the per-slot width and offset
     * scratches the record-expansion pass consumes.
     *
     * <p>
     * The pathNodeKey column is bitmap-indexed by slot (0..1023), so the page bitmap is walked in
     * parallel with the entry loop to map entry index → slot bit. Both elision sections are
     * slot-ascending, so their cursors advance with the same walk.
     *
     * @param headerBitmapSeg the page's 160-byte header + slot bitmap
     * @param populatedCount number of populated entries
     * @return the total in-memory heap size
     */
    int deriveAll(final MemorySegment headerBitmapSeg, final int populatedCount) {
      long running = 0L;
      final long maximum = (long) populatedCount * PageConstants.MAX_RECORD_SIZE;
      int bmIdx = 0;
      long bmWord = 0L;
      valueElidedReadCursor = 0;
      nameKeyElidedReadCursor = 0;
      for (int i = 0; i < populatedCount; i++) {
        while (bmWord == 0) {
          bmWord = PageLayout.getBitmapWord(headerBitmapSeg, bmIdx++);
          if (bmIdx > PageLayout.BITMAP_WORDS) {
            throw new SirixIOException("bitmap exhausted at entry " + i + " / " + populatedCount);
          }
        }
        final int slotBit = ((bmIdx - 1) << 6) | Long.numberOfTrailingZeros(bmWord);
        bmWord &= bmWord - 1;
        running += inMemLengthOf(i, slotBit);
        if (running > maximum) {
          throw new SirixIOException("reconstructed heap exceeds the inline-page bound: " + running
              + " bytes for " + populatedCount + " entries (maximum " + maximum + ")");
        }
      }
      return Math.toIntExact(running);
    }

    /**
     * Derive one entry's in-memory data length and stamp its per-slot scratches.
     *
     * <p>
     * Entries must be visited in ascending order: the value- and nameKey-elision sections are read
     * through cursors that only move forward.
     *
     * @param entryIdx the entry's rank in populated-bitmap order
     * @param slotBit the entry's slot id on the page
     * @return the entry's in-memory data length
     */
    int inMemLengthOf(final int entryIdx, final int slotBit) {
      final int onDiskLen = PageLayout.unpackDataLength(compactDir[entryIdx]);
      final int kindId = PageLayout.unpackNodeKindId(compactDir[entryIdx]);
      final int templateId = slotTemplateIds[entryIdx] & 0xFF;
      final int fc = OffsetTableTemplatePool.templateFieldCount(templatePool, templateOffsets, templateId);
      if (OffsetTableTemplatePool.templateKindId(templatePool, templateOffsets, templateId) != kindId) {
        throw new SirixIOException("V1 kindId mismatch at slot " + entryIdx + ": compactDir=" + kindId + " template="
            + OffsetTableTemplatePool.templateKindId(templatePool, templateOffsets, templateId));
      }
      int inMemLen = onDiskLen + (fc - 1);
      if (hashElisionActive && ((zeroHashBitmap[entryIdx >>> 3] >>> (entryIdx & 7)) & 1) == 1) {
        inMemLen += NodeFieldLayout.HASH_WIDTH;
      }
      // parentKey width reconstruction. The writer strips a slot's parentKey varint
      // whenever the kind has the field, the field is non-terminal, and the width the
      // record's offset table implies is sane. The template IS that offset table, so
      // re-deriving the same predicate from it keeps writer and reader in lockstep.
      // Deciding on the decoded value instead would come apart the day a node
      // legitimately holds NULL_NODE_KEY in a field it does have: the writer would strip
      // those bytes and the reader would never put them back.
      int pkWidth = 0;
      if (parentKeyColumnActive) {
        pkWidth = structuralKeyWidth(templateId, fc, NodeFieldLayout.parentKeyFieldIndexForKind(kindId));
        inMemLen += pkWidth;
      }
      // The two sibling columns, under the same rule and read out of the same template.
      int rightSibWidth = 0;
      if (rightSibColumnActive) {
        rightSibWidth = structuralKeyWidth(templateId, fc, NodeFieldLayout.rightSiblingKeyFieldIndexForKind(kindId));
        inMemLen += rightSibWidth;
      }
      int leftSibWidth = 0;
      if (leftSibColumnActive) {
        leftSibWidth = structuralKeyWidth(templateId, fc, NodeFieldLayout.leftSiblingKeyFieldIndexForKind(kindId));
        inMemLen += leftSibWidth;
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
          final int pnkLookup = PathNodeKeyRegion.pathNodeKeyForSlot(pathNodeKeyColumnBytes, slotBit);
          if (pnkLookup >= 0) {
            final int pnkOff =
                OffsetTableTemplatePool.templateFieldOffset(templatePool, templateOffsets, templateId, pnkFieldIdx);
            int computed;
            if (pnkFieldIdx + 1 < fc) {
              computed = OffsetTableTemplatePool.templateFieldOffset(templatePool, templateOffsets, templateId,
                  pnkFieldIdx + 1) - pnkOff;
            } else {
              // Last-field case — ALL kinds with pnk actually have it at
              // a non-terminal index in real data (by inspection of
              // NodeFieldLayout.pathNodeKeyFieldIndexForKind), so this
              // branch is dead. Compute via remaining-dataBytes for safety.
              final int postStrippedDataBytes = (onDiskLen - 2);
              final int reAddedHash =
                  (hashElisionActive && ((zeroHashBitmap[entryIdx >>> 3] >>> (entryIdx & 7)) & 1) == 1)
                      ? NodeFieldLayout.HASH_WIDTH
                      : 0;
              computed = OffsetTableTemplatePool.templateFieldWidth(templatePool, templateOffsets, templateId,
                  pnkFieldIdx, postStrippedDataBytes + reAddedHash + pkWidth);
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
      // NUMBER -> typeByte (2 or 3)
      // STRING -> 0 (placeholder)
      // BOOLEAN -> 0 (placeholder)
      // The reader dispatches on compactDir kindId — typeByte is only
      // consulted by the inject pass for NUMBER (to pick INTEGER vs LONG
      // varint width).
      int valueWidth = 0;
      if (valueElisionActive && valueElidedReadCursor < valueElidedCount
          && (valueElidedSlots[valueElidedReadCursor] & 0xFFFF) == slotBit) {
        // This slot is named by the elision section. Per-slot elision means a fused
        // primitive slot may equally well NOT be named — its payload is then simply
        // inline — so matching is by explicit slot id, never by kind.
        if (!(kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_NUMBER_KIND_ID
            || kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_STRING_KIND_ID
            || kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_BOOLEAN_KIND_ID)) {
          throw new SirixIOException(
              "value-elision names slot " + slotBit + ", whose kind " + kindId + " has no fused-primitive payload");
        }
        valueWidth = valueElidedWidths[valueElidedReadCursor];
        // BOOLEAN width is exactly 1; STRING is 1 (compressed flag) +
        // varint(length) + length bytes; NUMBER is up to 11 (1 type +
        // up to 10 varint bytes for a long). The writer pre-scan caps at
        // 0xFF so width is always representable in the on-disk byte.
        if (valueWidth <= 0) {
          throw new SirixIOException("invalid value-elision width at slot " + entryIdx + ": " + valueWidth);
        }
        inMemLen += valueWidth;
        // Compute the in-data offset of the value field. All three
        // primitive-fused kinds (48/49/50) put their value at field
        // index 8 (NodeFieldLayout.OBJNAMEDNUM_PAYLOAD ==
        // OBJNAMEDSTR_PAYLOAD == OBJNAMEDBOOL_VALUE == 8).
        valueOffs[entryIdx] = (short) OffsetTableTemplatePool.templateFieldOffset(templatePool, templateOffsets,
            templateId, NodeFieldLayout.OBJNAMEDNUM_PAYLOAD);
        valueWidths[entryIdx] = (short) valueWidth;
        valueElidedReadCursor++;
      } else if (valueElisionActive) {
        // Not named by the section — inline payload (or no payload at all).
        valueWidths[entryIdx] = 0;
      }
      // Lever 4: name-key width re-injection. For fused OBJECT_NAMED_*
      // (48-51) slots the on-disk record has its [signed-varint nameKey]
      // field stripped. The reader reads the per-slot 1-byte width from
      // the packed-widths section and adds it back to inMemLen so the
      // slot's heap layout matches the unelided original.
      int nameKeyWidthLocal = 0;
      if (nameKeyElisionActive && KeyValueLeafPage.isFusedObjectNamedKindId(kindId)) {
        if (nameKeyElidedReadCursor >= nameKeyElidedWidthsPacked.length) {
          throw new SirixIOException("name-key elision section truncated at slot " + entryIdx);
        }
        nameKeyWidthLocal = nameKeyElidedWidthsPacked[nameKeyElidedReadCursor] & 0xFF;
        nameKeyElidedReadCursor++;
        if (nameKeyWidthLocal < 1 || nameKeyWidthLocal > 5) {
          throw new SirixIOException("invalid name-key elision width at slot " + entryIdx + ": " + nameKeyWidthLocal);
        }
        inMemLen += nameKeyWidthLocal;
        // The nameKey field index is kind-specific: 3 for primitives
        // (kindIds 48-51) and 5 for the Phase 1-reserved structurals
        // (52-53). NodeFieldLayout.nameKeyFieldIndexForKind handles
        // both ranges; for the primitive subset that this branch hits
        // it always returns 3.
        final int nameKeyFieldIdx = NodeFieldLayout.nameKeyFieldIndexForKind(kindId);
        nameKeyOffs[entryIdx] = (short) OffsetTableTemplatePool.templateFieldOffset(templatePool, templateOffsets,
            templateId, nameKeyFieldIdx);
        nameKeyWidths[entryIdx] = (byte) nameKeyWidthLocal;
      } else if (nameKeyElisionActive) {
        // Slot's kind is not a fused OBJECT_NAMED_*: no nameKey strip,
        // no inject. Zero out so the inject pass treats it as no-op.
        nameKeyWidths[entryIdx] = 0;
      }
      final boolean validLength = kindId == 0
          ? inMemLen >= 0 && inMemLen <= PageLayout.MAX_COMPACT_DIR_DATA_LENGTH
          : inMemLen > 0 && inMemLen <= PageLayout.MAX_COMPACT_DIR_DATA_LENGTH;
      if (!validLength || inMemLen > PageConstants.MAX_RECORD_SIZE) {
        throw new SirixIOException("reconstructed slot " + slotBit + " kind " + kindId
            + " has invalid inline length " + inMemLen + " (maximum "
            + Math.min(PageLayout.MAX_COMPACT_DIR_DATA_LENGTH, PageConstants.MAX_RECORD_SIZE) + ")");
      }
      inMemDataLengths[entryIdx] = inMemLen;
      // Stash widths for the expansion loop.
      if (parentKeyColumnActive && parentKeyWidths != null) {
        parentKeyWidths[entryIdx] = (byte) pkWidth;
      }
      if (rightSibColumnActive && rightSibKeyWidths != null) {
        rightSibKeyWidths[entryIdx] = (byte) rightSibWidth;
      }
      if (leftSibColumnActive && leftSibKeyWidths != null) {
        leftSibKeyWidths[entryIdx] = (byte) leftSibWidth;
      }
      if (pathNodeKeyColumnActive && pathNodeKeyWidths != null) {
        pathNodeKeyWidths[entryIdx] = (byte) pnkWidth;
      }
      return inMemLen;
    }

    /**
     * The varint width the writer stripped for one structural-key field, or 0 when the slot kept its
     * bytes inline.
     *
     * <p>
     * Read out of the TEMPLATE — which is the record's offset table — under exactly the predicate the
     * writer's {@code collectStructuralKey} applied to that same table. Deciding on the decoded value
     * instead would come apart the day a node legitimately holds {@code NULL_NODE_KEY} in a field it
     * does have.
     */
    private int structuralKeyWidth(final int templateId, final int fc, final int fieldIdx) {
      if (fieldIdx < 0 || fieldIdx + 1 >= fc) {
        return 0;
      }
      final int computed =
          OffsetTableTemplatePool.templateFieldOffset(templatePool, templateOffsets, templateId, fieldIdx + 1)
              - OffsetTableTemplatePool.templateFieldOffset(templatePool, templateOffsets, templateId, fieldIdx);
      return computed > 0 && computed <= 10
          ? computed
          : 0;
    }

    /**
     * Insert one reinjection range into an offset-ordered list, and report the new length. The mirror
     * of the writer's {@code insertStripRange}, carrying which field each range belongs to.
     *
     * @param offs the range offsets, ordered ascending
     * @param widths the matching widths
     * @param kinds the matching {@code INJECT_*} discriminators
     * @param count how many ranges the list already holds
     * @param off the new range's offset within the record's in-memory data region
     * @param width the new range's width in bytes
     * @param kind which field the range reinjects
     * @return {@code count + 1}
     */
    private static int insertInjectRange(final int[] offs, final int[] widths, final int[] kinds, final int count,
        final int off, final int width, final int kind) {
      if (count == offs.length) {
        throw new SirixIOException("more than " + offs.length
            + " reinjection ranges on one record — raise STRIP_RANGE_CAPACITY alongside the lever that added one");
      }
      int at = count;
      while (at > 0 && offs[at - 1] > off) {
        offs[at] = offs[at - 1];
        widths[at] = widths[at - 1];
        kinds[at] = kinds[at - 1];
        at--;
      }
      offs[at] = off;
      widths[at] = width;
      kinds[at] = kind;
      return count + 1;
    }

    /**
     * Expand one on-disk record into its slot on the page heap.
     *
     * <p>
     * The inverse of what the writer stripped: the template id becomes the record's offset table again,
     * and the parentKey, pathNodeKey, hash, value and name-key bytes go back at the offsets the
     * template says they occupy, interleaved with copies of the bytes that were never stripped. Values
     * elided into PAX regions are placeholder-filled here and injected by the second pass, once the
     * region table has been read.
     *
     * <p>
     * Nothing here depends on where the previous record ended: the destination comes from the slot's
     * directory entry and the source is passed in. That is what lets a caller expand one chunk's
     * entries — or a single record — instead of walking the page from its first byte.
     *
     * @param entryIdx the entry's rank in populated-bitmap order
     * @param slot the entry's slot, whose directory entry says where the record goes
     * @param src bytes holding the on-disk record: the whole decoded body, or one chunk of it
     * @param srcRecordOff offset of this record's first byte within {@code src}
     * @return the record's on-disk length, so a sequential caller can find the next one
     */
    int expandEntryInto(final MemorySegment slottedPage, final int entryIdx, final int slot, final MemorySegment src,
        final long srcRecordOff, final long pageKeyBase) {
      final int packed = compactDir[entryIdx];
      final int onDiskLen = PageLayout.unpackDataLength(packed);
      final int kindId = PageLayout.unpackNodeKindId(packed);
      final int templateId = slotTemplateIds[entryIdx] & 0xFF;
      final int fc = OffsetTableTemplatePool.templateFieldCount(templatePool, templateOffsets, templateId);
      // Record on-disk layout: [kindId(1)][templateId(1)][data(D)]
      // where D = onDiskLen - 2. Expand to [kindId(1)][offsetTable(fc)][data(D')]
      // where D' = D + pkWidth + pnkWidth + (hashStripped ? 8 : 0).
      final int slotHeapOffset = PageLayout.getDirHeapOffset(slottedPage, slot);
      final long recordBase = PageLayout.HEAP_START + slotHeapOffset;
      // Copy kindId byte from staging heap section.
      slottedPage.set(ValueLayout.JAVA_BYTE, recordBase, src.get(ValueLayout.JAVA_BYTE, srcRecordOff));
      // Expand offset table from the template pool.
      OffsetTableTemplatePool.expandTemplateTo(templatePool, templateOffsets, templateId, slottedPage, recordBase + 1);
      final int dataBytes = onDiskLen - 2;
      if (dataBytes < 0) {
        throw new SirixIOException("record too short: onDiskLen=" + onDiskLen + " slot=" + slot);
      }
      final boolean hashStripped = hashElisionActive && ((zeroHashBitmap[entryIdx >>> 3] >>> (entryIdx & 7)) & 1) == 1;
      final int hashOffInData = hashStripped
          ? OffsetTableTemplatePool.templateFieldOffset(templatePool, templateOffsets, templateId,
              NodeFieldLayout.hashFieldIndexForKind(kindId))
          : -1;
      final int pkWidth = (parentKeyColumnActive && parentKeyWidths != null)
          ? (parentKeyWidths[entryIdx] & 0xFF)
          : 0;
      final int rightSibWidth = (rightSibColumnActive && rightSibKeyWidths != null)
          ? (rightSibKeyWidths[entryIdx] & 0xFF)
          : 0;
      final int rightSibOffInData = rightSibWidth > 0
          ? OffsetTableTemplatePool.templateFieldOffset(templatePool, templateOffsets, templateId,
              NodeFieldLayout.rightSiblingKeyFieldIndexForKind(kindId))
          : -1;
      final int leftSibWidth = (leftSibColumnActive && leftSibKeyWidths != null)
          ? (leftSibKeyWidths[entryIdx] & 0xFF)
          : 0;
      final int leftSibOffInData = leftSibWidth > 0
          ? OffsetTableTemplatePool.templateFieldOffset(templatePool, templateOffsets, templateId,
              NodeFieldLayout.leftSiblingKeyFieldIndexForKind(kindId))
          : -1;
      final int pnkWidth = (pathNodeKeyColumnActive && pathNodeKeyWidths != null)
          ? (pathNodeKeyWidths[entryIdx] & 0xFF)
          : 0;
      final int pnkOffInData;
      if (pnkWidth > 0) {
        pnkOffInData = OffsetTableTemplatePool.templateFieldOffset(templatePool, templateOffsets, templateId,
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
          ? (valueWidths[entryIdx] & 0xFFFF)
          : 0;
      final int valueOffInData = valueWidth > 0
          ? (valueOffs[entryIdx] & 0xFFFF)
          : -1;
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
          ? (nameKeyWidths[entryIdx] & 0xFF)
          : 0;
      final int nameKeyOffInData = nameKeyWidthLocal > 0
          ? (nameKeyOffs[entryIdx] & 0xFFFF)
          : -1;
      // Collect the reinjection ranges (in-memory offset, width, which field), kept sorted by offset
      // as they are inserted. Which field sits where inside the data region is kind-specific, so the
      // order they are collected in is not the order they appear in.
      //   parentKey: offset 0, width pkWidth (always first when active)
      //   pnk:       offset pnkOffInData, width pnkWidth
      //   hash:      offset hashOffInData, width HASH_WIDTH
      //   value:     offset valueOffInData, width valueWidth
      //   nameKey:   offset nameKeyOffInData, width nameKeyWidthLocal
      final int[] injectOffs = injectRangeOffsets;
      final int[] injectWidths = injectRangeWidths;
      final int[] injectKinds = injectRangeKinds;
      int iCount = 0;
      if (pkWidth > 0) {
        iCount = insertInjectRange(injectOffs, injectWidths, injectKinds, iCount, 0, pkWidth, INJECT_PARENT_KEY);
      }
      if (rightSibWidth > 0) {
        iCount = insertInjectRange(injectOffs, injectWidths, injectKinds, iCount, rightSibOffInData, rightSibWidth,
            INJECT_RIGHT_SIB_KEY);
      }
      if (leftSibWidth > 0) {
        iCount = insertInjectRange(injectOffs, injectWidths, injectKinds, iCount, leftSibOffInData, leftSibWidth,
            INJECT_LEFT_SIB_KEY);
      }
      if (pnkWidth > 0) {
        iCount = insertInjectRange(injectOffs, injectWidths, injectKinds, iCount, pnkOffInData, pnkWidth,
            INJECT_PATH_NODE_KEY);
      }
      if (hashStripped) {
        iCount = insertInjectRange(injectOffs, injectWidths, injectKinds, iCount, hashOffInData,
            NodeFieldLayout.HASH_WIDTH, INJECT_HASH);
      }
      if (valueWidth > 0) {
        iCount = insertInjectRange(injectOffs, injectWidths, injectKinds, iCount, valueOffInData, valueWidth,
            INJECT_VALUE);
      }
      if (nameKeyWidthLocal > 0) {
        iCount = insertInjectRange(injectOffs, injectWidths, injectKinds, iCount, nameKeyOffInData, nameKeyWidthLocal,
            INJECT_NAME_KEY);
      }

      // Walk in-memory offsets; between insertions, copy on-disk bytes.
      long writePos = recordBase + 1 + fc;
      long readPos = srcRecordOff + 2; // skip kindId + templateId bytes
      int inMemCursor = 0;
      final long nodeKey = pageKeyBase + slot;
      for (int ri = 0; ri < iCount; ri++) {
        final int rOff = injectOffs[ri];
        final int rWidth = injectWidths[ri];
        final int rKind = injectKinds[ri];
        // Copy on-disk bytes from inMemCursor → rOff (in-memory) = (rOff - inMemCursor) bytes.
        final int gap = rOff - inMemCursor;
        if (gap < 0) {
          throw new SirixIOException(
              "overlapping insertions at slot " + slot + " range " + ri + " offset " + rOff + " cursor " + inMemCursor);
        }
        if (gap > 0) {
          MemorySegment.copy(src, readPos, slottedPage, writePos, gap);
          writePos += gap;
          readPos += gap;
          inMemCursor += gap;
        }
        // Inject rWidth bytes for the field this range names.
        if (rKind == INJECT_PARENT_KEY) {
          final long pk = parentKeyValues[entryIdx];
          final int actualWidth = DeltaVarIntCodec.writeDeltaToSegment(slottedPage, writePos, pk, nodeKey);
          if (actualWidth != rWidth) {
            throw new SirixIOException("parentKey width mismatch at slot " + slot + ": expected=" + rWidth + " actual="
                + actualWidth + " value=" + pk + " nodeKey=" + nodeKey);
          }
        } else if (rKind == INJECT_RIGHT_SIB_KEY) {
          final long rightSib = rightSibKeyValues[entryIdx];
          final int actualWidth = DeltaVarIntCodec.writeDeltaToSegment(slottedPage, writePos, rightSib, nodeKey);
          if (actualWidth != rWidth) {
            throw new SirixIOException("right-sibling width mismatch at slot " + slot + ": expected=" + rWidth
                + " actual=" + actualWidth + " value=" + rightSib + " nodeKey=" + nodeKey);
          }
        } else if (rKind == INJECT_LEFT_SIB_KEY) {
          final long leftSib = leftSibKeyValues[entryIdx];
          final int actualWidth = DeltaVarIntCodec.writeDeltaToSegment(slottedPage, writePos, leftSib, nodeKey);
          if (actualWidth != rWidth) {
            throw new SirixIOException("left-sibling width mismatch at slot " + slot + ": expected=" + rWidth
                + " actual=" + actualWidth + " value=" + leftSib + " nodeKey=" + nodeKey);
          }
        } else if (rKind == INJECT_PATH_NODE_KEY) {
          final int pnkValue = PathNodeKeyRegion.pathNodeKeyForSlot(pathNodeKeyColumnBytes, slot);
          if (pnkValue < 0) {
            throw new SirixIOException("pathNodeKey lookup failed for slot " + slot);
          }
          final int actualWidth = DeltaVarIntCodec.writeDeltaToSegment(slottedPage, writePos, (long) pnkValue, nodeKey);
          if (actualWidth != rWidth) {
            throw new SirixIOException("pathNodeKey width mismatch at slot " + slot + ": expected=" + rWidth
                + " actual=" + actualWidth + " value=" + pnkValue + " nodeKey=" + nodeKey);
          }
        } else if (rKind == INJECT_HASH) {
          // Hash: write 8 zero bytes.
          slottedPage.set(LE.LONG, writePos, 0L);
        } else if (rKind == INJECT_VALUE) {
          // Value: zero-fill placeholder; the second-pass injectValueElidedBytes
          // pass populates [type:1][varint] from the NumberRegion + tag/slotRank.
          // We zero-fill to keep the heap deterministic for the codec layer.
          for (int z = 0; z < rWidth; z++) {
            slottedPage.set(ValueLayout.JAVA_BYTE, writePos + z, (byte) 0);
          }
        } else {
          // nameKey (INJECT_NAME_KEY): zero-fill placeholder. The second-pass
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
      final int inMemDataLen = inMemDataLengths[entryIdx] - 1 - fc;
      final int tail = inMemDataLen - inMemCursor;
      if (tail > 0) {
        MemorySegment.copy(src, readPos, slottedPage, writePos, tail);
        writePos += tail;
        readPos += tail;
      } else if (tail < 0) {
        throw new SirixIOException("tail < 0 at slot " + slot + ": tail=" + tail + " inMemDataLen=" + inMemDataLen
            + " inMemCursor=" + inMemCursor);
      }
      return onDiskLen;
    }
  }

  /**
   * The sections of a record page's body as they are staged, in wire order, with the length of each
   * one.
   *
   * <p>
   * Everything ahead of the heap — compact dir, template pool, per-slot template ids, and the four
   * column/elision sections — is page-global metadata the reader needs before it can touch a single
   * record; the heap that follows is the records themselves. The two are staged into one contiguous
   * buffer and compressed together, exactly as before, but the carrier records where the boundary
   * falls so a caller can frame metadata and records apart without re-deriving the layout.
   *
   * <p>
   * Thread-local and reused: {@link #begin} rebinds it to a staging buffer and clears the lengths.
   */
  static final class BodySections {
    private MemorySegment staging;
    /** Write cursor into {@link #staging}; also the number of bytes staged so far. */
    private long pos;
    /** Staging offset of the first heap byte, i.e. the end of the metadata sections. */
    private long heapStart;
    private int compactDirLen;
    private int templatePoolLen;
    private int slotTemplateIdsLen;
    private int zeroHashBitmapLen;
    private int parentKeyColumnLen;
    private int rightSibColumnLen;
    private int leftSibColumnLen;
    private int pathNodeKeyColumnLen;
    private int valueElisionLen;
    private int nameKeyElisionLen;
    private int heapLen;

    /**
     * Bind a staging buffer and drop the previous page's lengths.
     *
     * @param staging the staging buffer, sized for the whole body
     */
    void begin(final MemorySegment staging) {
      this.staging = staging;
      pos = 0;
      heapStart = 0;
      compactDirLen = 0;
      templatePoolLen = 0;
      slotTemplateIdsLen = 0;
      zeroHashBitmapLen = 0;
      parentKeyColumnLen = 0;
      rightSibColumnLen = 0;
      leftSibColumnLen = 0;
      pathNodeKeyColumnLen = 0;
      valueElisionLen = 0;
      nameKeyElisionLen = 0;
      heapLen = 0;
    }

    void appendCompactDir(final int[] slotOnDiskLens, final int[] slotKindIds, final int populatedCount) {
      final long start = pos;
      // compactDir — on-disk lengths, accounting for stripped hash + parentKey + pnk + value + nameKey.
      for (int i = 0; i < populatedCount; i++) {
        // Big-endian on the wire. One unaligned short store avoids per-byte segment checks and keeps
        // the serializer's compact-directory loop allocation-free.
        PageLayout.writeCompactDirEntry(staging, pos, slotOnDiskLens[i], slotKindIds[i]);
        pos += PageLayout.COMPACT_DIR_ENTRY_SIZE;
      }
      compactDirLen = (int) (pos - start);
    }

    void appendTemplatePool(final byte[] templatePool, final int len) {
      MemorySegment.copy(templatePool, 0, staging, ValueLayout.JAVA_BYTE, pos, len);
      pos += len;
      templatePoolLen = len;
    }

    void appendSlotTemplateIds(final byte[] slotTemplateIds, final int populatedCount) {
      MemorySegment.copy(slotTemplateIds, 0, staging, ValueLayout.JAVA_BYTE, pos, populatedCount);
      pos += populatedCount;
      slotTemplateIdsLen = populatedCount;
    }

    /** Entry-indexed bitmap of the slots whose all-zero hash the writer dropped. */
    void appendZeroHashBitmap(final byte[] zeroHashBitmap, final int len) {
      MemorySegment.copy(zeroHashBitmap, 0, staging, ValueLayout.JAVA_BYTE, pos, len);
      pos += len;
      zeroHashBitmapLen = len;
    }

    /** parentKey column: int length prefix + {@link StructuralKeyColumnCodec} bytes. */
    void appendParentKeyColumn(final byte[] column, final int len) {
      putIntBE(len);
      MemorySegment.copy(column, 0, staging, ValueLayout.JAVA_BYTE, pos, len);
      pos += len;
      parentKeyColumnLen = 4 + len;
    }

    /** Right-sibling column: int length prefix + {@link StructuralKeyColumnCodec} bytes. */
    void appendRightSibKeyColumn(final byte[] column, final int len) {
      putIntBE(len);
      MemorySegment.copy(column, 0, staging, ValueLayout.JAVA_BYTE, pos, len);
      pos += len;
      rightSibColumnLen = 4 + len;
    }

    /** Left-sibling column: int length prefix + {@link StructuralKeyColumnCodec} bytes. */
    void appendLeftSibKeyColumn(final byte[] column, final int len) {
      putIntBE(len);
      MemorySegment.copy(column, 0, staging, ValueLayout.JAVA_BYTE, pos, len);
      pos += len;
      leftSibColumnLen = 4 + len;
    }

    /** pathNodeKey column: int length prefix + {@link PathNodeKeyRegion} bytes. */
    void appendPathNodeKeyColumn(final byte[] column, final int len) {
      putIntBE(len);
      MemorySegment.copy(column, 0, staging, ValueLayout.JAVA_BYTE, pos, len);
      pos += len;
      pathNodeKeyColumnLen = 4 + len;
    }

    /**
     * Value-elision section: int elidedCount, then one entry per elided slot in slot-ascending order.
     *
     * <p>
     * Each entry is (slot gap varint, type byte, original heap width varint, region absolute-index
     * varint). The type byte carries NUMBER's 2/3 subtype, a string's compressed flag, or 0 for a
     * boolean, whose kind the reader takes from the compact dir.
     *
     * @param elidedCount number of slots named by the section
     */
    void appendValueElision(final int elidedCount, final int populatedCount, final byte[] slotValueElided,
        final short[] slotBits, final short[] slotValueWidths, final int[] slotRegionAbsIdx) {
      final long start = pos;
      putIntBE(elidedCount);
      int prevElidedSlot = -1;
      for (int i = 0; i < populatedCount; i++) {
        final byte mark = slotValueElided[i];
        if (mark != 0) {
          final int slot = slotBits[i] & 0xFFFF;
          // Slot id as a gap from the previous elided slot, then the type byte (NUMBER's
          // 2/3; 0 for STRING/BOOLEAN, whose kind the reader takes from the compact dir),
          // the original heap width, and the value's absolute index in its region — the
          // reader decodes by that index directly, so no rank bookkeeping exists to drift.
          pos += DeltaVarIntCodec.writeSignedToSegment(staging, pos, slot - prevElidedSlot);
          prevElidedSlot = slot;
          // NUMBER carries its 2/3 subtype; STRING carries its compressed flag (0 raw,
          // 1 FSST) so injection restores the exact heap byte; BOOLEAN carries 0.
          final byte diskType;
          if (mark == STRING_ELIDE_MARKER || mark == BOOLEAN_ELIDE_MARKER) {
            diskType = 0;
          } else if (mark == STRING_ELIDE_COMPRESSED_MARKER) {
            diskType = 1;
          } else {
            diskType = mark;
          }
          staging.set(ValueLayout.JAVA_BYTE, pos, diskType);
          pos += 1;
          pos += DeltaVarIntCodec.writeSignedToSegment(staging, pos, slotValueWidths[i] & 0xFFFF);
          pos += DeltaVarIntCodec.writeSignedToSegment(staging, pos, slotRegionAbsIdx[slot]);
        }
      }
      valueElisionLen = (int) (pos - start);
    }

    /**
     * Derived value-elision section: a flag byte, an elided-slot bitmap unless every candidate slot is
     * elided, and one sparse exception list per derived field. See {@link ElisionDeriver}.
     *
     * @param deriver the writer's deriver, holding the plan {@code planValueSection} produced
     * @param populatedCount number of populated entries, which sizes the bitmap
     */
    void appendDerivedValueElision(final ElisionDeriver deriver, final int populatedCount) {
      valueElisionLen = deriver.encodeValueSection(staging, pos, populatedCount);
      pos += valueElisionLen;
    }

    /**
     * Derived name-key-elision section: a flag byte and, where the canonical varint width of the
     * region's nameKey is not what the writer stripped, one exception per deviating slot.
     *
     * @param deriver the writer's deriver, holding the plan {@code planNameKeySection} produced
     */
    void appendDerivedNameKeyElision(final ElisionDeriver deriver) {
      nameKeyElisionLen = deriver.encodeNameKeySection(staging, pos);
      pos += nameKeyElisionLen;
    }

    /**
     * Name-key elision section: int elidedCount, then one width byte per elided slot in slot-ascending
     * order. The nameKey value itself is recovered from the page's {@link ObjectKeyNameKeyRegion}.
     *
     * @param elidedCount number of elided fused OBJECT_NAMED_* slots
     */
    void appendNameKeyElision(final int elidedCount, final int populatedCount, final byte[] slotNameKeyElided,
        final byte[] slotNameKeyWidths) {
      final long start = pos;
      putIntBE(elidedCount);
      for (int i = 0; i < populatedCount; i++) {
        if (slotNameKeyElided[i] != 0) {
          staging.set(ValueLayout.JAVA_BYTE, pos, slotNameKeyWidths[i]);
          pos++;
        }
      }
      nameKeyElisionLen = (int) (pos - start);
    }

    /**
     * Open the heap section. The caller stages the records itself — it walks the same slots five more
     * times with skip ranges — and reports back through {@link #endHeap}.
     *
     * @return the staging offset the first heap byte goes to
     */
    long beginHeap() {
      heapStart = pos;
      return pos;
    }

    /**
     * Close the heap section.
     *
     * @param heapEnd the caller's write cursor once the last record has been staged
     */
    void endHeap(final long heapEnd) {
      heapLen = (int) (heapEnd - heapStart);
      pos = heapEnd;
    }

    /** Bytes of page-global metadata staged ahead of the heap. */
    long metaLength() {
      return (long) compactDirLen + templatePoolLen + slotTemplateIdsLen + zeroHashBitmapLen + parentKeyColumnLen
          + rightSibColumnLen + leftSibColumnLen + pathNodeKeyColumnLen + valueElisionLen + nameKeyElisionLen;
    }

    /** Bytes of record heap staged behind the metadata. */
    int heapLength() {
      return heapLen;
    }

    /** Staging offset of the first heap byte, i.e. the end of the metadata sections. */
    long heapStart() {
      return heapStart;
    }

    /**
     * The staged length of one section, by {@link PageSectionDiag}'s section id.
     *
     * <p>
     * Read only by the post-codec attribution, which needs to slice the staging buffer the way it was
     * filled. Sections are appended in this order, so a caller walks them with a running offset.
     *
     * @param section a {@code PageSectionDiag.SECTION_*} id naming one of the staged sections
     * @return its length in bytes, 0 when the page did not write it
     */
    int sectionLength(final int section) {
      return switch (section) {
        case PageSectionDiag.SECTION_COMPACT_DIR -> compactDirLen;
        case PageSectionDiag.SECTION_TEMPLATES -> templatePoolLen + slotTemplateIdsLen;
        case PageSectionDiag.SECTION_ZERO_HASH_BITMAP -> zeroHashBitmapLen;
        case PageSectionDiag.SECTION_PARENT_KEY_COLUMN -> parentKeyColumnLen;
        case PageSectionDiag.SECTION_RIGHT_SIB_COLUMN -> rightSibColumnLen;
        case PageSectionDiag.SECTION_LEFT_SIB_COLUMN -> leftSibColumnLen;
        case PageSectionDiag.SECTION_PATH_NODE_KEY_COLUMN -> pathNodeKeyColumnLen;
        case PageSectionDiag.SECTION_VALUE_ELISION -> valueElisionLen;
        case PageSectionDiag.SECTION_NAME_KEY_ELISION -> nameKeyElisionLen;
        case PageSectionDiag.SECTION_HEAP -> heapLen;
        default -> 0;
      };
    }

    /** Total bytes staged. */
    long totalLength() {
      return pos;
    }

    /** Big-endian int, matching {@code sink.writeInt} semantics for the section length prefixes. */
    private void putIntBE(final int value) {
      staging.set(ValueLayout.JAVA_BYTE, pos, (byte) ((value >>> 24) & 0xFF));
      staging.set(ValueLayout.JAVA_BYTE, pos + 1, (byte) ((value >>> 16) & 0xFF));
      staging.set(ValueLayout.JAVA_BYTE, pos + 2, (byte) ((value >>> 8) & 0xFF));
      staging.set(ValueLayout.JAVA_BYTE, pos + 3, (byte) (value & 0xFF));
      pos += 4;
    }
  }
}
