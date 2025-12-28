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
import io.sirix.io.Reader;
import io.sirix.node.Utils;
import io.sirix.node.Bytes;
import io.sirix.node.interfaces.DeweyIdSerializer;
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
          final long recordPageKey = Utils.getVarLong(source);
          final int revision = source.readInt();
          final IndexType indexType = IndexType.getType(source.readByte());
          final int lastSlotIndex = source.readInt();

          // Read compressed slot offsets (delta + bit-packed)
          final int[] slotOffsets = SlotOffsetCodec.decode(source);

          // Read slotMemory size
          final int slotMemorySize = source.readInt();

          // ZERO-COPY: Slice decompression buffer directly as slotMemory
          // This eliminates per-slot MemorySegment.copy() calls (major performance win)
          final boolean canZeroCopy = decompressionResult != null && source instanceof MemorySegmentBytesIn;
          final MemorySegment slotMemory;
          final MemorySegment backingBuffer;
          final Runnable backingBufferReleaser;

          MemorySegmentAllocator memorySegmentAllocator =
              OS.isWindows() ? WindowsMemorySegmentAllocator.getInstance() : LinuxMemorySegmentAllocator.getInstance();
          if (canZeroCopy) {
            // Zero-copy path: slice decompression buffer directly
            final MemorySegment sourceSegment = ((MemorySegmentBytesIn) source).getSource();
            slotMemory = sourceSegment.asSlice(source.position(), slotMemorySize);
            source.skip(slotMemorySize);

            // Transfer buffer ownership to page
            backingBufferReleaser = decompressionResult.transferOwnership();
            backingBuffer = decompressionResult.backingBuffer();
          } else {
            // Fallback: allocate and copy (for non-MemorySegment sources or no decompressionResult)
            MemorySegmentAllocator allocator = memorySegmentAllocator;
            slotMemory = allocator.allocate(slotMemorySize);
            
            // Copy slot data
            if (source instanceof MemorySegmentBytesIn msSource) {
              MemorySegment.copy(msSource.getSource(), source.position(), slotMemory, 0, slotMemorySize);
              source.skip(slotMemorySize);
            } else {
              byte[] slotData = new byte[slotMemorySize];
              source.read(slotData);
              MemorySegment.copy(slotData, 0, slotMemory, java.lang.foreign.ValueLayout.JAVA_BYTE, 0, slotMemorySize);
            }
            backingBuffer = null;
            backingBufferReleaser = null;
          }

          // Read dewey ID data if stored
          final boolean areDeweyIDsStored = resourceConfig.areDeweyIDsStored;
          final RecordSerializer recordPersister = resourceConfig.recordPersister;
          final int[] deweyIdOffsets;
          final MemorySegment deweyIdMemory;
          final int lastDeweyIdIndex;

          if (areDeweyIDsStored && recordPersister instanceof DeweyIdSerializer) {
            lastDeweyIdIndex = source.readInt();

            // Read compressed dewey ID offsets (delta + bit-packed)
            deweyIdOffsets = SlotOffsetCodec.decode(source);

            // Read deweyIdMemory size and data
            final int deweyIdMemorySize = source.readInt();
            
            if (canZeroCopy && deweyIdMemorySize > 1) {
              // Zero-copy for dewey IDs too (part of same backing buffer)
              final MemorySegment sourceSegment = ((MemorySegmentBytesIn) source).getSource();
              deweyIdMemory = sourceSegment.asSlice(source.position(), deweyIdMemorySize);
              source.skip(deweyIdMemorySize);
            } else if (deweyIdMemorySize > 1) {
              // Allocate and copy
              MemorySegmentAllocator allocator = memorySegmentAllocator;
              deweyIdMemory = allocator.allocate(deweyIdMemorySize);
              
              if (source instanceof MemorySegmentBytesIn msSource) {
                MemorySegment.copy(msSource.getSource(), source.position(), deweyIdMemory, 0, deweyIdMemorySize);
                source.skip(deweyIdMemorySize);
              } else {
                byte[] deweyData = new byte[deweyIdMemorySize];
                source.read(deweyData);
                MemorySegment.copy(deweyData, 0, deweyIdMemory, java.lang.foreign.ValueLayout.JAVA_BYTE, 0, deweyIdMemorySize);
              }
            } else {
              deweyIdMemory = null;
              source.skip(1); // Skip placeholder byte
            }
          } else {
            deweyIdOffsets = null;
            deweyIdMemory = null;
            lastDeweyIdIndex = -1;
          }

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

          // Read FSST symbol table for string compression
          byte[] fsstSymbolTable = null;
          final int fsstSymbolTableLength = source.readInt();
          if (fsstSymbolTableLength > 0) {
            fsstSymbolTable = new byte[fsstSymbolTableLength];
            source.read(fsstSymbolTable);
          }

          // Read columnar string storage if present
          // Format: [hasColumnar:1][size:4][offsets:bit-packed][data:N]
          MemorySegment stringValueMemory = null;
          int[] stringValueOffsets = null;
          int lastStringValueIndex = -1;
          int stringValueMemorySize = 0;
          
          byte hasColumnar = source.readByte();
          if (hasColumnar == 1) {
            stringValueMemorySize = source.readInt();
            stringValueOffsets = SlotOffsetCodec.decode(source);
            
            // Find last string value index from offsets
            for (int i = stringValueOffsets.length - 1; i >= 0; i--) {
              if (stringValueOffsets[i] >= 0) {
                lastStringValueIndex = i;
                break;
              }
            }
            
            // Read columnar data - zero-copy if possible
            if (canZeroCopy && stringValueMemorySize > 0) {
              final MemorySegment sourceSegment = ((MemorySegmentBytesIn) source).getSource();
              stringValueMemory = sourceSegment.asSlice(source.position(), stringValueMemorySize);
              source.skip(stringValueMemorySize);
            } else if (stringValueMemorySize > 0) {
              stringValueMemory = memorySegmentAllocator.allocate(stringValueMemorySize);
              if (source instanceof MemorySegmentBytesIn msSource) {
                MemorySegment.copy(msSource.getSource(), source.position(), 
                    stringValueMemory, 0, stringValueMemorySize);
                source.skip(stringValueMemorySize);
              } else {
                byte[] stringData = new byte[stringValueMemorySize];
                source.read(stringData);
                MemorySegment.copy(stringData, 0, stringValueMemory, 
                    java.lang.foreign.ValueLayout.JAVA_BYTE, 0, stringValueMemorySize);
              }
            }
          }

          // Create page - use the zero-copy constructor for both paths
          // (it properly handles slotOffsets; backingBuffer/releaser can be null for non-zero-copy)
          KeyValueLeafPage page = new KeyValueLeafPage(
              recordPageKey,
              revision,
              indexType,
              resourceConfig,
              slotOffsets,
              slotMemory,
              lastSlotIndex,
              deweyIdOffsets,
              deweyIdMemory,
              lastDeweyIdIndex,
              references,
              backingBuffer,
              backingBufferReleaser
          );

          // Set FSST symbol table if present and propagate to any deserialized nodes
          if (fsstSymbolTable != null) {
            page.setFsstSymbolTable(fsstSymbolTable);
            // Propagate symbol table to nodes that are already in the records array
            // Lazily-deserialized nodes will get the table when accessed via getRecord()
            page.propagateFsstSymbolTableToNodes();
          }

          // Set columnar string storage if present
          if (stringValueMemory != null && stringValueOffsets != null) {
            page.setStringValueData(stringValueMemory, stringValueOffsets, 
                lastStringValueIndex, stringValueMemorySize);
          }

          return page;
        }
        default -> throw new IllegalStateException();
      }
    }

    @Override
    public void serializePage(final ResourceConfiguration resourceConfig, final BytesOut<?> sink, final Page page,
        final SerializationType type) {
      KeyValueLeafPage keyValueLeafPage = (KeyValueLeafPage) page;

      final var bytes = keyValueLeafPage.getBytes();

      if (bytes != null) {
        sink.write(bytes.toByteArray());
        return;
      }

      sink.writeByte(KEYVALUELEAFPAGE.id);
      sink.writeByte(resourceConfig.getBinaryEncodingVersion().byteVersion());

      //Variables from keyValueLeafPage
      final long recordPageKey = keyValueLeafPage.getPageKey();
      final IndexType indexType = keyValueLeafPage.getIndexType();
      final RecordSerializer recordPersister = resourceConfig.recordPersister;
      final Map<Long, PageReference> references = keyValueLeafPage.getReferencesMap();

      // Build FSST symbol table and compress strings BEFORE addReferences() serializes them
      keyValueLeafPage.buildFsstSymbolTable(resourceConfig);
      keyValueLeafPage.compressStringValues();

      // Add references to overflow pages if necessary.
      keyValueLeafPage.addReferences(resourceConfig);

      // Write page key.
      Utils.putVarLong(sink, recordPageKey);
      // Write revision number.
      sink.writeInt(keyValueLeafPage.getRevision());
      // Write index type.
      sink.writeByte(indexType.getID());
      // Write last slot index.
      sink.writeInt(keyValueLeafPage.getLastSlotIndex());

      // Write compressed slot offsets (delta + bit-packed) - ~75% smaller than raw int[1024]
      final int[] slotOffsets = keyValueLeafPage.getSlotOffsets();
      SlotOffsetCodec.encode(sink, slotOffsets, keyValueLeafPage.getLastSlotIndex());

      // Write slotMemory region - BULK COPY
      int slotMemoryUsedSize = keyValueLeafPage.getUsedSlotsSize();
      if (slotMemoryUsedSize == 0) {
        slotMemoryUsedSize = 1;
      }
      sink.writeInt(slotMemoryUsedSize);
      
      // Bulk copy slotMemory - direct segment-to-segment copy (zero allocation for MemorySegmentBytesOut/PooledBytesOut)
      final MemorySegment slotMem = keyValueLeafPage.getSlotMemory();
      sink.writeSegment(slotMem, 0, slotMemoryUsedSize);

      // Write dewey ID data if stored
      if (resourceConfig.areDeweyIDsStored && recordPersister instanceof DeweyIdSerializer) {
        // Write last dewey ID index
        sink.writeInt(keyValueLeafPage.getLastDeweyIdIndex());
        
        // Write compressed dewey ID offsets (delta + bit-packed)
        final int[] deweyIdOffsets = keyValueLeafPage.getDeweyIdOffsets();
        SlotOffsetCodec.encode(sink, deweyIdOffsets, keyValueLeafPage.getLastDeweyIdIndex());
        
        // Write deweyIdMemory region - BULK COPY
        int deweyIdMemoryUsedSize = keyValueLeafPage.getUsedDeweyIdSize();
        if (deweyIdMemoryUsedSize == 0) {
          deweyIdMemoryUsedSize = 1;
        }
        sink.writeInt(deweyIdMemoryUsedSize);
        
        final MemorySegment deweyMem = keyValueLeafPage.getDeweyIdMemory();
        if (deweyMem != null) {
          // Direct segment-to-segment copy (zero allocation for MemorySegmentBytesOut/PooledBytesOut)
          sink.writeSegment(deweyMem, 0, deweyIdMemoryUsedSize);
        } else {
          // Write a single byte placeholder if no dewey ID memory
          sink.writeByte((byte) 0);
        }
      }

      // Write overlong entries bitmap (entries bitmap is not needed - slot presence determined by slotOffsets)
      var overlongEntriesBitmap = new BitSet(Constants.NDP_NODE_COUNT);
      final var overlongEntriesSortedByKey = references.entrySet().stream().sorted(Map.Entry.comparingByKey()).toList();

      for (final Map.Entry<Long, PageReference> entry : overlongEntriesSortedByKey) {
        final var pageOffset = StorageEngineReader.recordPageOffset(entry.getKey());
        overlongEntriesBitmap.set(pageOffset);
      }
      SerializationType.serializeBitSet(sink, overlongEntriesBitmap);

      // Write overlong entries.
      sink.writeInt(overlongEntriesSortedByKey.size());
      for (final var entry : overlongEntriesSortedByKey) {
        // Write key in persistent storage.
        sink.writeLong(entry.getValue().getKey());
      }

      // Write FSST symbol table for string compression
      final byte[] fsstSymbolTable = keyValueLeafPage.getFsstSymbolTable();
      if (fsstSymbolTable != null && fsstSymbolTable.length > 0) {
        sink.writeInt(fsstSymbolTable.length);
        sink.write(fsstSymbolTable);
      } else {
        sink.writeInt(0); // No symbol table
      }

      // Write columnar string storage if present
      // Format: [hasColumnar:1][size:4][offsets:bit-packed][data:N]
      if (keyValueLeafPage.hasColumnarStringStorage()) {
        sink.writeByte((byte) 1); // Has columnar data
        int columnarSize = keyValueLeafPage.getStringValueMemoryFreeSpaceStart();
        sink.writeInt(columnarSize);
        
        // Write bit-packed string value offsets
        SlotOffsetCodec.encode(sink, keyValueLeafPage.getStringValueOffsets(), 
            keyValueLeafPage.getLastStringValueIndex());
        
        // Write columnar string data - direct segment copy (zero allocation for MemorySegmentBytesOut/PooledBytesOut)
        MemorySegment stringMem = keyValueLeafPage.getStringValueMemory();
        sink.writeSegment(stringMem, 0, columnarSize);
      } else {
        sink.writeByte((byte) 0); // No columnar data
      }

      keyValueLeafPage.setHashCode(Reader.hashFunction.hashBytes(sink.bytesForRead().toByteArray()).asBytes());

      final BytesIn<?> uncompressedBytes = sink.bytesForRead();
      final byte[] uncompressedArray = uncompressedBytes.toByteArray();

      final byte[] compressedPage =
          compress(resourceConfig, uncompressedBytes, uncompressedArray, sink.writePosition());

      // Cache compressed form for writers, but leave the sink unmodified (uncompressed)
      // so in-memory round-trips that bypass the ByteHandler still work.
      keyValueLeafPage.setBytes(Bytes.wrapForWrite(compressedPage));
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
