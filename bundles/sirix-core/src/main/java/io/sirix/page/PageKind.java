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
import io.sirix.page.pax.NumberRegion;
import io.sirix.page.pax.ObjectKeyNameKeyRegion;
import io.sirix.page.pax.RegionTable;
import io.sirix.page.pax.StringRegion;
import io.sirix.settings.Constants;
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
      final long recordPageKey = Utils.getVarLong(source);
      final int revision = source.readInt();
      final IndexType indexType = IndexType.getType(source.readByte());

      final MemorySegmentAllocator memorySegmentAllocator = Allocators.getInstance();

      // 1. Read header (32B) + bitmap (128B) — 160 bytes (on-disk format, never changes)
      final byte[] headerBitmapBytes = headerBitmapScratch.get();
      source.read(headerBitmapBytes);
      final MemorySegment headerBitmapSeg = MemorySegment.ofArray(headerBitmapBytes);
      final int populatedCount = PageLayout.getPopulatedCount(headerBitmapSeg);

      // 2. Read compact dir entries into thread-local scratch (reused across
      // pages to avoid the per-page int[populatedCount] allocation). Bulk
      // readInts collapses populatedCount separate VarHandle-dispatched
      // readInt calls into a single native MemorySegment.copy when the source
      // is segment-backed (the common path at cold-cache scan time).
      final int[] compactDir = compactDirScratch.get();
      source.readInts(compactDir, 0, populatedCount);

      // 3. Read heap size
      final int heapSize = source.readInt();

      // 4. Allocate slotted page MemorySegment — size to actual heap content.
      // The allocator rounds up to its next power-of-two size class (4/8/16/32/
      // 64/128/256 KiB), so we don't need to pre-round. Dropping the legacy
      // INITIAL_PAGE_SIZE floor lets pages with small heaps (e.g. path-summary
      // pages, sparsely-populated data pages) fall into smaller size classes —
      // 32 KiB instead of 64 KiB — doubling effective cache capacity for those
      // pages. Growth via growSlottedPage handles any later writes that exceed
      // the initial class. At 100M records the working set shrinks from ~68 GB
      // to ~35-40 GB at 64 KiB → 32 KiB splits, dramatically reducing LZ4
      // decompress calls on cache-miss paths (was 21% CPU in the v3 profile).
      final int allocSize = PageLayout.HEAP_START + heapSize;
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

      // 8. Read heap data into page at HEAP_START
      if (source instanceof MemorySegmentBytesIn msSource) {
        MemorySegment.copy(msSource.getSource(), source.position(), slottedPage, PageLayout.HEAP_START, heapSize);
        source.skip(heapSize);
      } else {
        final byte[] heapData = new byte[heapSize];
        source.read(heapData);
        MemorySegment.copy(heapData, 0, slottedPage, ValueLayout.JAVA_BYTE, PageLayout.HEAP_START, heapData.length);
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
          final int packed = compactDir[entryIdx++];
          final int dataLength = packed >>> 8;
          final int nodeKindId = packed & 0xFF;
          PageLayout.setDirEntry(slottedPage, slot, heapOffset, dataLength, nodeKindId);
          heapOffset += dataLength;
          word &= word - 1; // clear lowest set bit
        }
      }

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
      sink.writeByte(resourceConfig.getBinaryEncodingVersion().byteVersion());

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

      // 1. Write header (32B) + bitmap (128B) — 160 bytes (on-disk format, never changes)
      sink.writeSegment(slottedPage, 0, PageLayout.DISK_HEADER_BITMAP_SIZE);

      // 2. Single-pass bitmap scan: write compact dir entries, collect heap info into scratch
      final int populatedCount = keyValueLeafPage.getCachedPopulatedCount();
      final int[] scratch = SERIALIZE_SCRATCH.get();
      int totalHeapSize = 0;
      int idx = 0;

      for (int w = 0; w < PageLayout.BITMAP_WORDS; w++) {
        long word = PageLayout.getBitmapWord(slottedPage, w);
        while (word != 0) {
          final int bit = Long.numberOfTrailingZeros(word);
          final int slot = (w << 6) | bit;
          final int dataLength = PageLayout.getDirDataLength(slottedPage, slot);
          final int nodeKindId = PageLayout.getDirNodeKindId(slottedPage, slot);
          final int heapOffset = PageLayout.getDirHeapOffset(slottedPage, slot);

          sink.writeInt(PageLayout.packCompactDirEntry(dataLength, nodeKindId));
          scratch[idx++] = heapOffset;
          scratch[idx++] = dataLength;
          totalHeapSize += dataLength;
          word &= word - 1; // clear lowest set bit
        }
      }

      // 3. Write heap size
      sink.writeInt(totalHeapSize);

      // 4. Write heap data from scratch array (no second bitmap scan)
      for (int i = 0; i < idx; i += 2) {
        sink.writeSegment(slottedPage, PageLayout.HEAP_START + scratch[i], scratch[i + 1]);
      }

      // PAX region table after the heap. Writer populates the number region
      // (OBJECT_NUMBER_VALUE slot values + parent OBJECT_KEY nameKeys) so the
      // vectorized scan path can filter by logical field in a tight loop,
      // skipping both moveTo and varint decode.
      final RegionTable regionTable = buildRegionTable(keyValueLeafPage, slottedPage);
      if (regionTable == null) {
        sink.writeInt(0);
      } else {
        keyValueLeafPage.setRegionTable(regionTable);
        regionTable.write(sink);
      }

      // Write overlong entries
      writeOverlongEntries(sink, references);

      // Write FSST symbol table
      writeFsstSymbolTable(sink, keyValueLeafPage);

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
        final MemorySegment slottedPage) {
      final long[] valBuf = NUMBER_VALUE_SCRATCH.get();
      final int[] parBuf = NUMBER_PARENT_SCRATCH.get();
      final int[] okNameKeys = OBJECT_KEY_NAMEKEY_SCRATCH.get();
      final int[] okSlots = OBJECT_KEY_SLOT_SCRATCH.get();
      int count = 0;
      int okCount = 0;
      final int numberKindId = KeyValueLeafPage.objectNumberValueKindId();
      final int stringKindId = KeyValueLeafPage.objectStringValueKindId();
      final long pageKeyBase = page.getPageKey() << Constants.NDP_NODE_COUNT_EXPONENT;

      // Reuse a per-thread StringRegion.Encoder so the common path (every
      // KVL page in a large ingest) allocates nothing for the encoder itself.
      // It's reset() lazily — only if we actually encounter a string slot,
      // keeping pages without OBJECT_STRING_VALUE slots at zero touch.
      StringRegion.Encoder stringEnc = null;
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
              if ((parentKey >>> Constants.NDP_NODE_COUNT_EXPONENT) == page.getPageKey()) {
                final int parentSlot = (int) (parentKey & (PageLayout.SLOT_COUNT - 1));
                if (KeyValueLeafPage.isObjectKeyKindId(PageLayout.getDirNodeKindId(slottedPage, parentSlot))) {
                  parentNameKey = page.getObjectKeyNameKeyFromSlot(parentSlot);
                }
              }
              valBuf[count] = value;
              parBuf[count] = parentNameKey;
              count++;
            }
          } else if (kindId == stringKindId) {
            final byte[] value = page.readObjectStringValueBytesForRegionBuildPkg(slot);
            if (value != null) {
              final long valueNodeKey = pageKeyBase + slot;
              final long parentKey = page.getObjectStringValueParentKeyFromSlot(slot, valueNodeKey);
              int parentNameKey = -1;
              if ((parentKey >>> Constants.NDP_NODE_COUNT_EXPONENT) == page.getPageKey()) {
                final int parentSlot = (int) (parentKey & (PageLayout.SLOT_COUNT - 1));
                if (KeyValueLeafPage.isObjectKeyKindId(PageLayout.getDirNodeKindId(slottedPage, parentSlot))) {
                  parentNameKey = page.getObjectKeyNameKeyFromSlot(parentSlot);
                }
              }
              if (stringEnc == null) {
                stringEnc = STRING_REGION_ENCODER.get();
                stringEnc.reset();
              }
              stringEnc.addValue(parentNameKey, value);
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
        final byte[] payload = NumberRegion.encode(valBuf, parBuf, count);
        table.set(RegionTable.KIND_NUMBER, payload);
      }
      if (okCount > 0) {
        final byte[] nameKeyPayload = ObjectKeyNameKeyRegion.encode(okNameKeys, okSlots, okCount);
        if (nameKeyPayload != null) {
          table.set(RegionTable.KIND_OBJECT_KEY_NAMEKEY, nameKeyPayload);
        }
      }
      if (stringCount > 0) {
        final byte[] stringPayload = stringEnc.finish();
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
      sink.writeByte(resourceConfig.getBinaryEncodingVersion().byteVersion());
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
      sink.writeByte(resourceConfig.getBinaryEncodingVersion().byteVersion());
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
      sink.writeByte(resourceConfig.getBinaryEncodingVersion().byteVersion());

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
          Page delegate = new BitmapReferencesPage(9, source, type);
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
      sink.writeByte(resourceConfig.getBinaryEncodingVersion().byteVersion());

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
      sink.writeByte(resourceConfig.getBinaryEncodingVersion().byteVersion());

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
      sink.writeByte(resourceConfig.getBinaryEncodingVersion().byteVersion());

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
      sink.writeByte(resourceConfig.getBinaryEncodingVersion().byteVersion());
      
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
      sink.writeByte(resourceConfig.getBinaryEncodingVersion().byteVersion());

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
      sink.writeByte(resourceConfig.getBinaryEncodingVersion().byteVersion());

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
      sink.writeByte(resourceConfig.getBinaryEncodingVersion().byteVersion());

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
      if (layoutType == HOTIndirectPage.LayoutType.MULTI_MASK) {
        return switch (nodeType) {
          case SPAN_NODE -> HOTIndirectPage.createSpanNodeMultiMask(pageKey, revision,
              extractionPositions, extractionMasks, numExtractionBytes,
              partialKeys, children, height, mostSignificantBitIndex);
          case MULTI_NODE -> HOTIndirectPage.createMultiNodeMultiMask(pageKey, revision,
              extractionPositions, extractionMasks, numExtractionBytes,
              partialKeys, children, height, mostSignificantBitIndex);
        };
      } else {
        return switch (nodeType) {
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
    }

    @Override
    public void serializePage(ResourceConfiguration resourceConfig, BytesOut<?> sink,
        Page page, SerializationType type) {
      HOTIndirectPage hotIndirect = (HOTIndirectPage) page;
      sink.writeByte(HOT_INDIRECT_PAGE.id);
      sink.writeByte(resourceConfig.getBinaryEncodingVersion().byteVersion());
      
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
      sink.writeByte(resourceConfig.getBinaryEncodingVersion().byteVersion());
      
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
      sink.writeByte(resourceConfig.getBinaryEncodingVersion().byteVersion());

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
