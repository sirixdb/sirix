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
import io.sirix.api.PageReadOnlyTrx;
import io.sirix.api.PageTrx;
import io.sirix.index.IndexType;
import io.sirix.node.Utils;
import io.sirix.node.interfaces.DeweyIdSerializer;
import io.sirix.node.interfaces.RecordSerializer;
import io.sirix.page.delegates.BitmapReferencesPage;
import io.sirix.page.delegates.FullReferencesPage;
import io.sirix.page.delegates.ReferencesPage4;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
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
    public Page deserializePage(final PageReadOnlyTrx pageReadOnlyTrx, final BytesIn<?> source,
        final SerializationType type) {
      final BinaryEncodingVersion binaryVersion = BinaryEncodingVersion.fromByte(source.readByte());

      switch (binaryVersion) {
        case V0 -> {
          final long recordPageKey = Utils.getVarLong(source);
          final int revision = source.readInt();
          final IndexType indexType = IndexType.getType(source.readByte());
          final ResourceConfiguration resourceConfig = pageReadOnlyTrx.getResourceSession().getResourceConfig();
          final boolean areDeweyIDsStored = resourceConfig.areDeweyIDsStored;
          final RecordSerializer recordPersister = resourceConfig.recordPersister;
          final byte[][] slots = new byte[Constants.NDP_NODE_COUNT][];
          final byte[][] deweyIds = new byte[Constants.NDP_NODE_COUNT][];

          if (resourceConfig.areDeweyIDsStored && recordPersister instanceof DeweyIdSerializer serializer) {
            final var deweyIdsBitmap = SerializationType.deserializeBitSet(source);
            final int deweyIdsSize = source.readInt();
            boolean hasDeweyIds = deweyIdsSize != 0;

            if (hasDeweyIds) {
              var setBit = -1;
              byte[] deweyId = null;

              for (int index = 0; index < deweyIdsSize; index++) {
                setBit = deweyIdsBitmap.nextSetBit(setBit + 1);

                assert setBit >= 0;

                if (recordPageKey == 0 && setBit == 0) {
                  continue; // No document root.
                }

                deweyId = serializer.deserializeDeweyID(source, deweyId, resourceConfig);
                deweyIds[setBit] = deweyId;
              }
            }
          }

          final var entriesBitmap = SerializationType.deserializeBitSet(source);
          final var overlongEntriesBitmap = SerializationType.deserializeBitSet(source);
          final int normalEntrySize = source.readInt();
          var setBit = -1;

          for (int index = 0; index < normalEntrySize; index++) {
            setBit = entriesBitmap.nextSetBit(setBit + 1);
            assert setBit >= 0;

            final long key = (recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + setBit;
            final int dataSize = source.readInt();
            assert dataSize > 0;

            final byte[] data = new byte[dataSize];
            source.read(data);
            final var offset = PageReadOnlyTrx.recordPageOffset(key);

            slots[offset] = data;
          }

          final int overlongEntrySize = source.readInt();
          final Map<Long, PageReference> references = new LinkedHashMap<>(overlongEntrySize);
          setBit = -1;

          for (int index = 0; index < overlongEntrySize; index++) {
            setBit = overlongEntriesBitmap.nextSetBit(setBit + 1);
            assert setBit >= 0;
            //recordPageKey * Constants.NDP_NODE_COUNT + setBit;
            final long key = (recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + setBit;
            final PageReference reference = new PageReference();

            reference.setKey(source.readLong());
            references.put(key, reference);
          }

          return new KeyValueLeafPage(recordPageKey,
                                      revision,
                                      indexType,
                                      resourceConfig,
                                      areDeweyIDsStored,
                                      recordPersister,
                                      slots,
                                      deweyIds,
                                      references);
        }
        default -> throw new IllegalStateException();
      }
    }

    @Override
    public void serializePage(final PageTrx pageTrx, final BytesOut<?> sink, final Page page,
        final SerializationType type) {
      KeyValueLeafPage keyValueLeafPage = (KeyValueLeafPage) page;

      try {
        keyValueLeafPage.getLock().acquire();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      try {
        final var bytes = keyValueLeafPage.getBytes();

        if (bytes != null) {
          sink.write(bytes.bytesForWrite());
          return;
        }

        final var tmpSink = Bytes.elasticHeapByteBuffer(60_000);

        tmpSink.writeByte(KEYVALUELEAFPAGE.id);
        tmpSink.writeByte(pageTrx.getResourceSession().getResourceConfig().getBinaryEncodingVersion().byteVersion());

        //Variables from keyValueLeafPage
        final long recordPageKey = keyValueLeafPage.getPageKey();
        final IndexType indexType = keyValueLeafPage.getIndexType();
        final ResourceConfiguration resourceConfig = keyValueLeafPage.getResourceConfig();
        final RecordSerializer recordPersister = resourceConfig.recordPersister;
        final byte[][] deweyIds = keyValueLeafPage.getDeweyIds();
        final byte[][] slots = keyValueLeafPage.getSlots();
        final Map<Long, PageReference> references = keyValueLeafPage.getReferencesMap();

        // Add references to overflow pages if necessary.
        keyValueLeafPage.addReferences(pageTrx);
        // Write page key.
        Utils.putVarLong(tmpSink, recordPageKey);
        // Write revision number.
        tmpSink.writeInt(pageTrx.getRevisionNumber());
        // Write index type.
        tmpSink.writeByte(indexType.getID());

        // Write dewey IDs.
        if (resourceConfig.areDeweyIDsStored && recordPersister instanceof DeweyIdSerializer persistence) {
          var deweyIdsBitmap = new BitSet(Constants.NDP_NODE_COUNT);
          for (int i = 0; i < deweyIds.length; i++) {
            if (deweyIds[i] != null) {
              deweyIdsBitmap.set(i);
            }
          }
          SerializationType.serializeBitSet(tmpSink, deweyIdsBitmap);
          tmpSink.writeInt(deweyIdsBitmap.cardinality());

          boolean first = true;
          byte[] previousDeweyId = null;

          for (byte[] deweyId : deweyIds) {
            if (deweyId != null) {
              if (first) {
                first = false;
                persistence.serializeDeweyID(tmpSink, deweyId, null, resourceConfig);
              } else {
                persistence.serializeDeweyID(tmpSink, previousDeweyId, deweyId, resourceConfig);
              }
              previousDeweyId = deweyId;
            }
          }
        }

        var entriesBitmap = new BitSet(Constants.NDP_NODE_COUNT);

        for (int i = 0; i < slots.length; i++) {
          if (slots[i] != null) {
            entriesBitmap.set(i);
          }
        }
        SerializationType.serializeBitSet(tmpSink, entriesBitmap);

        var overlongEntriesBitmap = new BitSet(Constants.NDP_NODE_COUNT);
        final var overlongEntriesSortedByKey =
            references.entrySet().stream().sorted(Map.Entry.comparingByKey()).toList();

        for (final Map.Entry<Long, PageReference> entry : overlongEntriesSortedByKey) {
          final var pageOffset = PageReadOnlyTrx.recordPageOffset(entry.getKey());
          overlongEntriesBitmap.set(pageOffset);
        }
        SerializationType.serializeBitSet(tmpSink, overlongEntriesBitmap);

        // Write normal entries.
        tmpSink.writeInt(entriesBitmap.cardinality());
        for (final byte[] data : slots) {
          if (data != null) {
            final int length = data.length;
            tmpSink.writeInt(length);
            tmpSink.write(data);
          }
        }

        // Write overlong entries.
        tmpSink.writeInt(overlongEntriesSortedByKey.size());
        for (final var entry : overlongEntriesSortedByKey) {
          // Write key in persistent storage.
          tmpSink.writeLong(entry.getValue().getKey());
        }

        keyValueLeafPage.setHashCode(pageTrx.getReader().hashFunction.hashBytes(tmpSink.bytesForRead().toByteArray())
                                                                     .asBytes());

        final var byteArray = tmpSink.bytesForRead().toByteArray();

        final byte[] serializedPage;

        try (final ByteArrayOutputStream output = new ByteArrayOutputStream(byteArray.length)) {
          try (final DataOutputStream dataOutput = new DataOutputStream(pageTrx.getResourceSession()
                                                                               .getResourceConfig().byteHandlePipeline.serialize(
                  output))) {
            dataOutput.write(byteArray);
            dataOutput.flush();
          }
          serializedPage = output.toByteArray();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }

        keyValueLeafPage.setBytes(Bytes.wrapForRead(serializedPage));

        sink.write(serializedPage);
      } finally {
        keyValueLeafPage.getLock().release();
      }
    }

    @Override
    public @NonNull Page getInstance(final Page nodePage, final PageTrx pageTrx) {
      assert nodePage instanceof KeyValueLeafPage;
      final KeyValueLeafPage page = (KeyValueLeafPage) nodePage;
      return new KeyValueLeafPage(page.getPageKey(), page.getIndexType(), pageTrx);
    }
  },

  /**
   * {@link NamePage}.
   */
  NAMEPAGE((byte) 2, NamePage.class) {
    @Override
    @NonNull
    public Page deserializePage(final PageReadOnlyTrx pageReadOnlyTrx, final BytesIn<?> source,
        final SerializationType type) {
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
    public void serializePage(final PageTrx pageTrx, final BytesOut<?> sink, final Page page,
        final SerializationType type) {
      NamePage namePage = (NamePage) page;
      sink.writeByte(NAMEPAGE.id);
      sink.writeByte(pageTrx.getResourceSession().getResourceConfig().getBinaryEncodingVersion().byteVersion());
      Page delegate = namePage.delegate();

      PageKind.writeDelegateType(delegate, sink);
      PageKind.serializeDelegate(pageTrx.getRevisionNumber(), sink, delegate, type);

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

    @Override
    public @NonNull Page getInstance(final Page page, final PageTrx pageTrx) {
      return new NamePage();
    }
  },

  /**
   * {@link UberPage}.
   */
  UBERPAGE((byte) 3, UberPage.class) {
    @Override
    @NonNull
    public Page deserializePage(final PageReadOnlyTrx pageReadOnlyTrx, final BytesIn<?> source,
        final SerializationType type) {
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
    public void serializePage(final PageTrx pagepageTrx, final BytesOut<?> sink, final Page page,
        final SerializationType type) {
      UberPage uberPage = (UberPage) page;

      sink.writeByte(UBERPAGE.id);
      sink.writeByte(pagepageTrx.getResourceSession().getResourceConfig().getBinaryEncodingVersion().byteVersion());
      sink.writeInt(uberPage.getRevisionCount());
      uberPage.setBootstrap(false);
    }

    @Override
    public @NonNull Page getInstance(final Page page, final PageTrx pageTrx) {
      return new UberPage();
    }
  },

  /**
   * {@link IndirectPage}.
   */
  INDIRECTPAGE((byte) 4, IndirectPage.class) {
    @Override
    @NonNull
    public Page deserializePage(final PageReadOnlyTrx pageReadOnlyTrx, final BytesIn<?> source,
        final SerializationType type) {
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
    public void serializePage(final PageTrx pageTrx, final BytesOut<?> sink, final Page page,
        final SerializationType type) {
      IndirectPage indirectPage = (IndirectPage) page;

      try {
        indirectPage.getLock().acquire();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      try {
        Page delegate = indirectPage.delegate();
        sink.writeByte(INDIRECTPAGE.id);
        sink.writeByte(pageTrx.getResourceSession().getResourceConfig().getBinaryEncodingVersion().byteVersion());

        PageKind.writeDelegateType(delegate, sink);

        PageKind.serializeDelegate(pageTrx.getRevisionNumber(), sink, delegate, type);
      } finally {
        indirectPage.getLock().release();
      }
    }

    @Override
    public @NonNull Page getInstance(final Page page, final PageTrx pageTrx) {
      return new IndirectPage();
    }
  },

  /**
   * {@link RevisionRootPage}.
   */
  REVISIONROOTPAGE((byte) 5, RevisionRootPage.class) {
    @Override
    @NonNull
    public Page deserializePage(final PageReadOnlyTrx pageReadOnlyTrx, final BytesIn<?> source,
        final SerializationType type) {
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
    public void serializePage(final PageTrx pageTrx, final BytesOut<?> sink, final Page page,
        final SerializationType type) {
      RevisionRootPage revisionRootPage = (RevisionRootPage) page;
      sink.writeByte(REVISIONROOTPAGE.id);
      sink.writeByte(pageTrx.getResourceSession().getResourceConfig().getBinaryEncodingVersion().byteVersion());

      Page delegate = revisionRootPage.delegate();
      PageKind.serializeDelegate(pageTrx.getRevisionNumber(), sink, delegate, type);

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

    @Override
    public @NonNull Page getInstance(final Page page, final PageTrx pageTrx) {
      return new RevisionRootPage();
    }
  },

  /**
   * {@link PathSummaryPage}.
   */
  PATHSUMMARYPAGE((byte) 6, PathSummaryPage.class) {
    @Override
    @NonNull
    public Page deserializePage(final PageReadOnlyTrx pageReadOnlyTrx, final BytesIn<?> source,
        final @NonNull SerializationType type) {
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
    public void serializePage(final PageTrx pageTrx, final BytesOut<?> sink, final Page page,
        final @NonNull SerializationType type) {
      PathSummaryPage pathSummaryPage = (PathSummaryPage) page;
      sink.writeByte(PATHSUMMARYPAGE.id);
      sink.writeByte(pageTrx.getResourceSession().getResourceConfig().getBinaryEncodingVersion().byteVersion());

      sink.writeByte((byte) 0);

      Page delegate = pathSummaryPage.delegate();
      PageKind.serializeDelegate(pageTrx.getRevisionNumber(), sink, delegate, type);

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

    @Override
    public @NonNull Page getInstance(final Page page, final PageTrx pageTrx) {
      return new PathSummaryPage();
    }
  },

  /**
   * {@link CASPage}.
   */
  CASPAGE((byte) 8, CASPage.class) {
    @NonNull
    public Page deserializePage(final PageReadOnlyTrx pageReadOnlyTrx, final BytesIn<?> source,
        final SerializationType type) {
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
    public void serializePage(final PageTrx pageTrx, final BytesOut<?> sink, final Page page,
        final SerializationType type) {
      CASPage casPage = (CASPage) page;
      Page delegate = casPage.delegate();
      sink.writeByte(CASPAGE.id);
      sink.writeByte(pageTrx.getResourceSession().getResourceConfig().getBinaryEncodingVersion().byteVersion());

      PageKind.writeDelegateType(delegate, sink);
      PageKind.serializeDelegate(pageTrx.getRevisionNumber(), sink, delegate, type);

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

    @Override
    public @NonNull Page getInstance(final Page page, final PageTrx pageTrx) {
      return new CASPage();
    }
  },

  /**
   * {@link OverflowPage}.
   */
  OVERFLOWPAGE((byte) 9, OverflowPage.class) {
    @Override
    @NonNull
    public Page deserializePage(final PageReadOnlyTrx pageReadOnlyTrx, final BytesIn<?> source,
        final SerializationType type) {
      final BinaryEncodingVersion binaryVersion = BinaryEncodingVersion.fromByte(source.readByte());

      switch (binaryVersion) {
        case V0 -> {
          final byte[] data = new byte[source.readInt()];
          source.read(data);

          return new OverflowPage(data);
        }
        default -> throw new IllegalStateException();
      }
    }

    @Override
    public void serializePage(final PageTrx pageTrx, final BytesOut<?> sink, final Page page,
        @NonNull SerializationType type) {
      OverflowPage overflowPage = (OverflowPage) page;
      sink.writeByte(OVERFLOWPAGE.id);
      sink.writeByte(pageTrx.getResourceSession().getResourceConfig().getBinaryEncodingVersion().byteVersion());
      sink.writeInt(overflowPage.getData().length);
      sink.write(overflowPage.getData());
    }

    @Override
    public @NonNull Page getInstance(final Page page, final PageTrx pageTrx) {
      return new OverflowPage();
    }
  },

  /**
   * {@link PathPage}.
   */
  PATHPAGE((byte) 10, PathPage.class) {
    @Override
    public Page deserializePage(@NonNull PageReadOnlyTrx pageReadOnlyTrx, BytesIn<?> source,
        @NonNull SerializationType type) {
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
    public void serializePage(final PageTrx pageTrx, BytesOut<?> sink, @NonNull Page page,
        @NonNull SerializationType type) {
      PathPage pathPage = (PathPage) page;
      Page delegate = pathPage.delegate();
      sink.writeByte(PATHPAGE.id);
      sink.writeByte(pageTrx.getResourceSession().getResourceConfig().getBinaryEncodingVersion().byteVersion());

      PageKind.writeDelegateType(delegate, sink);
      PageKind.serializeDelegate(pageTrx.getRevisionNumber(), sink, delegate, type);

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

    @Override
    public @NonNull Page getInstance(Page page, @NonNull PageTrx pageTrx) {
      return new PathPage();
    }
  },

  /**
   * {@link PathPage}.
   */
  DEWEYIDPAGE((byte) 11, DeweyIDPage.class) {
    @Override
    public Page deserializePage(@NonNull PageReadOnlyTrx pageReadOnlyTrx, BytesIn<?> source,
        @NonNull SerializationType type) {
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
    public void serializePage(@NonNull PageTrx pageTrx, BytesOut<?> sink, @NonNull Page page,
        @NonNull SerializationType type) {
      DeweyIDPage deweyIDPage = (DeweyIDPage) page;
      Page delegate = deweyIDPage.delegate();
      sink.writeByte(DEWEYIDPAGE.id);
      sink.writeByte(pageTrx.getResourceSession().getResourceConfig().getBinaryEncodingVersion().byteVersion());

      PageKind.writeDelegateType(delegate, sink);

      PageKind.serializeDelegate(pageTrx.getRevisionNumber(), sink, delegate, type);
      sink.writeLong(deweyIDPage.getMaxNodeKey());
      sink.writeByte((byte) deweyIDPage.getCurrentMaxLevelOfIndirectPages());
    }

    @Override
    public @NonNull Page getInstance(Page page, @NonNull PageTrx pageTrx) {
      return new DeweyIDPage();
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

  private static void serializeDelegate(int revision, BytesOut<?> sink, Page delegate, SerializationType type) {
    switch (delegate) {
      case ReferencesPage4 page ->
          type.serializeReferencesPage4(revision, sink, page.getReferences(), page.getOffsets());
      case BitmapReferencesPage page ->
          type.serializeBitmapReferencesPage(revision, sink, page.getReferences(), page.getBitmap());
      case FullReferencesPage ignored ->
          type.serializeFullReferencesPage(revision, sink, ((FullReferencesPage) delegate).getReferencesArray());
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
   * Serialize page.
   *
   * @param pageTrx the page transaction
   * @param sink    {@link Bytes<ByteBuffer>} instance
   * @param page    {@link Page} implementation
   */
  public abstract void serializePage(final PageTrx pageTrx, final BytesOut<?> sink, final Page page,
      final SerializationType type);

  /**
   * Deserialize page.
   *
   * @param pageReadOnlyTrx the read only page transaction
   * @param source          {@link Bytes<ByteBuffer>} instance
   * @return page instance implementing the {@link Page} interface
   */
  public abstract Page deserializePage(final PageReadOnlyTrx pageReadOnlyTrx, final BytesIn<?> source,
      final SerializationType type);

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

  /**
   * New page instance.
   *
   * @param page    instance of class which implements {@link Page}
   * @param pageTrx instance of class which implements {@link PageTrx}
   * @return new page instance
   */
  public abstract @NonNull Page getInstance(final Page page, final PageTrx pageTrx);
}
