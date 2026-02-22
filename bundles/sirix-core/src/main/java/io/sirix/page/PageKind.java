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
import io.sirix.cache.LinuxMemorySegmentAllocator;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.cache.WindowsMemorySegmentAllocator;
import io.sirix.index.IndexType;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.io.bytepipe.ByteHandlerPipeline;

import java.lang.foreign.MemorySegment;
import io.sirix.node.Utils;
import io.sirix.node.Bytes;
import io.sirix.node.interfaces.RecordSerializer;
import io.sirix.page.delegates.BitmapReferencesPage;
import io.sirix.page.delegates.FullReferencesPage;
import io.sirix.page.delegates.ReferencesPage4;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;
import io.sirix.utils.OS;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import io.sirix.node.BytesOut;
import io.sirix.node.BytesIn;
import io.sirix.node.MemorySegmentBytesIn;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.*;

/**
 * All Page types.
 */
@SuppressWarnings("SwitchStatementWithTooFewBranches")
public enum PageKind {
  /**
   * {@link KeyValueLeafPage}.
   */
  KEYVALUELEAFPAGE((byte) 1, KeyValueLeafPage.class) {
    @Override
    @NonNull
    public Page deserializePage(final ResourceConfiguration resourceConfig, final BytesIn<?> source,
        final SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      final BinaryEncodingVersion binaryVersion = BinaryEncodingVersion.fromByte(source.readByte());

      switch (binaryVersion) {
        case V0 -> {
          // ========== UNIFIED PAGE FORMAT ==========
          // Read metadata written outside the unified page segment
          final long recordPageKey = Utils.getVarLong(source);
          final int revision = source.readInt();
          final IndexType indexType = IndexType.getType(source.readByte());

          // Read unified page data size (HEAP_START + heapEnd)
          final int unifiedDataSize = source.readInt();

          MemorySegmentAllocator memorySegmentAllocator =
              OS.isWindows() ? WindowsMemorySegmentAllocator.getInstance() : LinuxMemorySegmentAllocator.getInstance();

          // Allocate unified page (at least INITIAL_PAGE_SIZE for future mutations on modified pages)
          final int allocSize = Math.max(unifiedDataSize, PageLayout.INITIAL_PAGE_SIZE);
          final MemorySegment unifiedPage = memorySegmentAllocator.allocate(allocSize);

          // Copy unified page data from source (header + bitmap + directory + heap)
          if (source instanceof MemorySegmentBytesIn msSource) {
            MemorySegment.copy(msSource.getSource(), source.position(), unifiedPage, 0, unifiedDataSize);
            source.skip(unifiedDataSize);
          } else {
            final byte[] pageData = new byte[unifiedDataSize];
            source.read(pageData);
            MemorySegment.copy(pageData, 0, unifiedPage,
                java.lang.foreign.ValueLayout.JAVA_BYTE, 0, unifiedDataSize);
          }

          // Zero remaining bytes if page is larger than data (for clean directory/bitmap reads)
          if (allocSize > unifiedDataSize) {
            unifiedPage.asSlice(unifiedDataSize, allocSize - unifiedDataSize).fill((byte) 0);
          }

          // DeweyIDs are stored inline in the heap (after record data, with 2-byte length trailer)
          // when FLAG_DEWEY_IDS_STORED is set — no separate deserialization needed.
          final boolean areDeweyIDsStored = resourceConfig.areDeweyIDsStored;
          final RecordSerializer recordPersister = resourceConfig.recordPersister;

          // Read overlong entries bitmap
          final var overlongEntriesBitmap = SerializationType.deserializeBitSet(source);

          // Read overlong entries
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

          // Create page using the deserialization constructor with dummy slotMemory.
          // The unified page overrides all slot + DeweyID operations (DeweyIDs inline in heap).
          final MemorySegment dummySlotMemory = memorySegmentAllocator.allocate(1);
          final KeyValueLeafPage page = new KeyValueLeafPage(
              recordPageKey, revision, indexType, resourceConfig,
              areDeweyIDsStored, recordPersister, references,
              dummySlotMemory, null, -1, -1);

          // Set the unified page — all slot operations now route through it
          page.setUnifiedPage(unifiedPage);

          // Set FSST symbol table if present
          if (fsstSymbolTable != null) {
            page.setFsstSymbolTable(fsstSymbolTable);
          }

          return page;
        }
        default -> throw new IllegalStateException();
      }
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

      sink.writeByte(KEYVALUELEAFPAGE.id);
      sink.writeByte(resourceConfig.getBinaryEncodingVersion().byteVersion());

      final long recordPageKey = keyValueLeafPage.getPageKey();
      final RecordSerializer recordPersister = resourceConfig.recordPersister;
      final Map<Long, PageReference> references = keyValueLeafPage.getReferencesMap();

      // Build FSST symbol table and compress strings BEFORE addReferences() serializes them
      keyValueLeafPage.buildFsstSymbolTable(resourceConfig);
      keyValueLeafPage.compressStringValues();

      // addReferences: serializes non-flyweight records to unified page heap via processEntries,
      // copies preserved slots from completePageRef for DIFFERENTIAL/INCREMENTAL versioning
      keyValueLeafPage.addReferences(resourceConfig);

      // ========== UNIFIED PAGE FORMAT ==========
      // Write metadata (for format identification and indexing)
      Utils.putVarLong(sink, recordPageKey);
      sink.writeInt(keyValueLeafPage.getRevision());
      sink.writeByte(keyValueLeafPage.getIndexType().getID());

      // Write unified page: header(32) + bitmap(128) + directory(8192) + heap(heapEnd)
      final MemorySegment unifiedPage = keyValueLeafPage.getUnifiedPage();
      final int heapEnd = PageLayout.getHeapEnd(unifiedPage);
      final int unifiedDataSize = PageLayout.HEAP_START + heapEnd;

      sink.writeInt(unifiedDataSize);
      sink.writeSegment(unifiedPage, 0, unifiedDataSize);

      // DeweyIDs are stored inline in the heap (after record data, with 2-byte length trailer)
      // when FLAG_DEWEY_IDS_STORED is set — no separate section needed.

      // Write overlong entries
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

      // Write FSST symbol table
      final byte[] fsstSymbolTable = keyValueLeafPage.getFsstSymbolTable();
      if (fsstSymbolTable != null && fsstSymbolTable.length > 0) {
        sink.writeInt(fsstSymbolTable.length);
        sink.write(fsstSymbolTable);
      } else {
        sink.writeInt(0);
      }

      // Compress the serialized data
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

      // Release node object references — all data is now in the unified page + compressed cache
      keyValueLeafPage.clearRecordsForGC();
    }
  },

  /**
   * {@link NamePage}.
   */
  NAMEPAGE((byte) 2, NamePage.class) {
    @Override
    @NonNull
    public Page deserializePage(final ResourceConfiguration resourceConfig, final BytesIn<?> source,
        final SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      final BinaryEncodingVersion binaryVersion = BinaryEncodingVersion.fromByte(source.readByte());

      switch (binaryVersion) {
        case V0 -> {
          Page delegate = PageUtils.createDelegate(source, type);

          final Int2LongMap maxNodeKeys = PageKind.deserializeMaxNodeKeys(source);
          final int numberOfArrays = source.readInt();
          final Int2IntMap currentMaxLevelsOfIndirectPages =
              PageKind.deserializeCurrentMaxLevelsOfIndirectPages(source);

          return new NamePage(delegate, maxNodeKeys, currentMaxLevelsOfIndirectPages, numberOfArrays);
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
    @NonNull
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
    @NonNull
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
    @NonNull
    public Page deserializePage(final ResourceConfiguration resourceConfiguration, final BytesIn<?> source,
        final SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      final BinaryEncodingVersion binaryVersion = BinaryEncodingVersion.fromByte(source.readByte());

      switch (binaryVersion) {
        case V0 -> {
          Page delegate = new BitmapReferencesPage(8, source, type);
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
    @NonNull
    public Page deserializePage(final ResourceConfiguration resourceConfig, final BytesIn<?> source,
        final @NonNull SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
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
        final @NonNull SerializationType type) {
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
    @NonNull
    public Page deserializePage(final ResourceConfiguration resourceConfig, final BytesIn<?> source,
        final SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      final BinaryEncodingVersion binaryVersion = BinaryEncodingVersion.fromByte(source.readByte());

      switch (binaryVersion) {
        case V0 -> {
          Page delegate = PageUtils.createDelegate(source, type);

          final Int2LongMap maxNodeKeys = PageKind.deserializeMaxNodeKeys(source);
          final Int2IntMap currentMaxLevelsOfIndirectPages =
              PageKind.deserializeCurrentMaxLevelsOfIndirectPages(source);

          return new CASPage(delegate, maxNodeKeys, currentMaxLevelsOfIndirectPages);
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
    @NonNull
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
        @NonNull SerializationType type) {
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
    public Page deserializePage(@NonNull ResourceConfiguration resourceConfiguration, BytesIn<?> source,
        @NonNull SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      final BinaryEncodingVersion binaryVersion = BinaryEncodingVersion.fromByte(source.readByte());
      switch (binaryVersion) {
        case V0 -> {
          final Page delegate = PageUtils.createDelegate(source, type);

          final Int2LongMap maxNodeKeys = PageKind.deserializeMaxNodeKeys(source);
          final Int2IntMap currentMaxLevelsOfIndirectPages =
              PageKind.deserializeCurrentMaxLevelsOfIndirectPages(source);

          return new PathPage(delegate, maxNodeKeys, currentMaxLevelsOfIndirectPages);
        }
        default -> throw new IllegalStateException();
      }
    }

    @Override
    public void serializePage(final ResourceConfiguration resourceConfig, BytesOut<?> sink, @NonNull Page page,
        @NonNull SerializationType type) {
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
    public Page deserializePage(@NonNull ResourceConfiguration resourceConfiguration, BytesIn<?> source,
        @NonNull SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
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
    public void serializePage(@NonNull ResourceConfiguration resourceConfig, BytesOut<?> sink, @NonNull Page page,
        @NonNull SerializationType type) {
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
    public Page deserializePage(@NonNull ResourceConfiguration resourceConfiguration, BytesIn<?> source,
        @NonNull SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      final BinaryEncodingVersion binaryVersion = BinaryEncodingVersion.fromByte(source.readByte());
      
      // Read header
      final long recordPageKey = Utils.getVarLong(source);
      final int revision = source.readInt();
      final IndexType indexType = IndexType.getType(source.readByte());
      final int entryCount = source.readInt();
      final int usedSlotMemorySize = source.readInt();
      
      // Read slot offsets (allocate MAX_ENTRIES to allow insertions after deserialization)
      final int[] slotOffsets = new int[HOTLeafPage.MAX_ENTRIES];
      for (int i = 0; i < entryCount; i++) {
        slotOffsets[i] = source.readInt();
      }
      
      // Read slot memory (zero-copy when possible)
      MemorySegmentAllocator allocator = OS.isWindows() 
          ? WindowsMemorySegmentAllocator.getInstance() 
          : LinuxMemorySegmentAllocator.getInstance();
      
      final MemorySegment slotMemory;
      final Runnable releaser;
      
      // Note: For zero-copy we use just the needed size, but for regular allocation we use DEFAULT_SIZE
      // to allow insertions after deserialization.
      final boolean canZeroCopy = decompressionResult != null && source instanceof MemorySegmentBytesIn;
      if (canZeroCopy) {
        // Zero-copy mode: use slice of source memory (read-only after this)
        final MemorySegment sourceSegment = ((MemorySegmentBytesIn) source).getSource();
        slotMemory = sourceSegment.asSlice(source.position(), usedSlotMemorySize);
        source.skip(usedSlotMemorySize);
        releaser = decompressionResult.transferOwnership();
      } else {
        // Regular allocation: use DEFAULT_SIZE to allow insertions
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
                             slotOffsets, entryCount, usedSlotMemorySize);
    }

    @Override
    public void serializePage(@NonNull ResourceConfiguration resourceConfig, @NonNull BytesOut<?> sink,
        @NonNull Page page, @NonNull SerializationType type) {
      HOTLeafPage hotLeaf = (HOTLeafPage) page;
      sink.writeByte(HOT_LEAF_PAGE.id);
      sink.writeByte(resourceConfig.getBinaryEncodingVersion().byteVersion());
      
      // Write header
      Utils.putVarLong(sink, hotLeaf.getPageKey());
      sink.writeInt(hotLeaf.getRevision());
      sink.writeByte(hotLeaf.getIndexType().getID());
      sink.writeInt(hotLeaf.getEntryCount());
      sink.writeInt(hotLeaf.getUsedSlotsSize());
      
      // Write slot offsets
      int entryCount = hotLeaf.getEntryCount();
      for (int i = 0; i < entryCount; i++) {
        MemorySegment slot = hotLeaf.getSlot(i);
        if (slot != null) {
          // Calculate offset from slot position
          sink.writeInt((int) (slot.address() - hotLeaf.slots().address()));
        } else {
          sink.writeInt(0);
        }
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
    public Page deserializePage(@NonNull ResourceConfiguration resourceConfiguration, BytesIn<?> source,
        @NonNull SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      final BinaryEncodingVersion binaryVersion = BinaryEncodingVersion.fromByte(source.readByte());
      
      // Read header
      final long pageKey = Utils.getVarLong(source);
      final int revision = source.readInt();
      final int height = source.readByte() & 0xFF;
      final byte nodeTypeId = source.readByte();
      final byte layoutTypeId = source.readByte();
      final int numChildren = source.readInt();
      
      final HOTIndirectPage.NodeType nodeType = HOTIndirectPage.NodeType.values()[nodeTypeId];
      
      // Read discriminative bits based on layout type
      final byte initialBytePos = source.readByte();
      final long bitMask = source.readLong();
      
      // Read partial keys
      final byte[] partialKeys = new byte[numChildren];
      source.read(partialKeys);
      
      // Read child references (simple key-only format)
      final PageReference[] children = new PageReference[numChildren];
      for (int i = 0; i < numChildren; i++) {
        PageReference ref = new PageReference();
        long childKey = source.readLong();
        ref.setKey(childKey);
        children[i] = ref;
      }
      
      // Create appropriate node type
      return switch (nodeType) {
        case BI_NODE -> {
          // Reconstruct discriminativeBitPos from serialized form
          // The mapping during creation was: bitInWord = 7 - (discriminativeBitPos % 8)
          // and bitMask = 1L << bitInWord, with byteWithinWindow = 0
          // So to reverse: bitWithinByte = 7 - Long.numberOfTrailingZeros(bitMask)
          // and discriminativeBitPos = initialBytePos * 8 + bitWithinByte
          int bitInWord = Long.numberOfTrailingZeros(bitMask);
          int bitWithinByte = 7 - bitInWord;
          int discriminativeBitPos = (initialBytePos & 0xFF) * 8 + bitWithinByte;
          yield HOTIndirectPage.createBiNode(pageKey, revision, discriminativeBitPos, children[0], children[1]);
        }
        case SPAN_NODE -> HOTIndirectPage.createSpanNode(pageKey, revision, 
            initialBytePos, bitMask, partialKeys, children);
        case MULTI_NODE -> {
          byte[] childIndexArray = new byte[256];
          source.read(childIndexArray);
          yield HOTIndirectPage.createMultiNode(pageKey, revision, initialBytePos, childIndexArray, children);
        }
      };
    }

    @Override
    public void serializePage(@NonNull ResourceConfiguration resourceConfig, @NonNull BytesOut<?> sink,
        @NonNull Page page, @NonNull SerializationType type) {
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
      
      // Write discriminative bits properly based on layout type
      sink.writeByte((byte) hotIndirect.getInitialBytePos());
      sink.writeLong(hotIndirect.getBitMask());
      
      // Write partial keys
      byte[] partialKeysData = hotIndirect.getPartialKeys();
      sink.write(partialKeysData);
      
      // Write child references
      for (int i = 0; i < hotIndirect.getNumChildren(); i++) {
        PageReference ref = hotIndirect.getChildReference(i);
        long key = ref != null ? ref.getKey() : Constants.NULL_ID_LONG;
        sink.writeLong(key);
      }
      
      // For MultiNode, write the 256-byte child index array
      if (hotIndirect.getNodeType() == HOTIndirectPage.NodeType.MULTI_NODE) {
        byte[] childIdx = hotIndirect.getChildIndex();
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
    public Page deserializePage(@NonNull ResourceConfiguration resourceConfiguration, BytesIn<?> source,
        @NonNull SerializationType type, final ByteHandler.DecompressionResult decompressionResult) {
      // Skip binary version byte for now
      source.readByte();
      
      // Read page key (stored before calling deserialize)
      final long pageKey = Utils.getVarLong(source);
      
      try {
        // Create a DataInputStream wrapper for BitmapChunkPage.deserialize
        byte[] remaining = source.toByteArray();
        java.io.DataInputStream dis = new java.io.DataInputStream(
            new java.io.ByteArrayInputStream(remaining, (int) source.position(), 
                remaining.length - (int) source.position()));
        return BitmapChunkPage.deserialize(dis, pageKey);
      } catch (java.io.IOException e) {
        throw new UncheckedIOException("Failed to deserialize BitmapChunkPage", e);
      }
    }

    @Override
    public void serializePage(@NonNull ResourceConfiguration resourceConfig, @NonNull BytesOut<?> sink,
        @NonNull Page page, @NonNull SerializationType type) {
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
    return segment.toArray(java.lang.foreign.ValueLayout.JAVA_BYTE);
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
      throw new IllegalStateException();
    }
    return page;
  }

  /**
   * Public method to get the related page based on the class.
   *
   * @param clazz the class for the page
   * @return the related page
   */
  public static @NonNull PageKind getKind(final Class<? extends Page> clazz) {
    final PageKind page = INSTANCEFORCLASS.get(clazz);
    if (page == null) {
      throw new IllegalStateException();
    }
    return page;
  }
}
