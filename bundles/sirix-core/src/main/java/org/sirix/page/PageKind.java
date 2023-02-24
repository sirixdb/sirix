/*
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.sirix.page;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import net.openhft.chronicle.bytes.Bytes;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.access.ResourceConfiguration;

import org.sirix.api.PageReadOnlyTrx;
import org.sirix.index.IndexType;
import org.sirix.node.interfaces.RecordSerializer;
import org.sirix.node.interfaces.DeweyIdSerializer;
import org.sirix.page.delegates.BitmapReferencesPage;
import org.sirix.page.delegates.FullReferencesPage;
import org.sirix.page.delegates.ReferencesPage4;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;
import org.sirix.access.User;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;

import static org.sirix.node.Utils.getVarLong;
import static org.sirix.node.Utils.putVarLong;

/**
 * All Page types.
 */
public enum PageKind {
  /**
   * {@link KeyValueLeafPage}.
   */
  RECORDPAGE((byte) 1, KeyValueLeafPage.class) {
    @Override
    @NonNull Page deserializePage(final PageReadOnlyTrx pageReadTrx, final Bytes<?> source,
        final SerializationType type) {

      final long recordPageKey = getVarLong(source);
      final int revision = source.readInt();
      final IndexType indexType = IndexType.getType(source.readByte());
      final ResourceConfiguration resourceConfig = pageReadTrx.getResourceSession().getResourceConfig();
      final boolean areDeweyIDsStored = resourceConfig.areDeweyIDsStored;
      final RecordSerializer recordPersister = resourceConfig.recordPersister;
      final byte[][] slots = new byte[Constants.NDP_NODE_COUNT][];
      final byte[][] deweyIds = new byte[Constants.NDP_NODE_COUNT][];
      final int overlongEntrySize = source.readInt();
      final Map<Long, PageReference> references = new LinkedHashMap<>(overlongEntrySize);

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
      return new KeyValueLeafPage(recordPageKey, revision, indexType, resourceConfig, areDeweyIDsStored,
                                  recordPersister,slots, deweyIds,references);

    }

    @Override
    void serializePage(final PageReadOnlyTrx pageReadOnlyTrx, final Bytes<ByteBuffer> sink, final Page page,
        final SerializationType type) {

      KeyValueLeafPage keyValueLeafPage = (KeyValueLeafPage) page;
      sink.writeByte(RECORDPAGE.id);

      //Variables from keyValueLeafPage
      final Bytes<ByteBuffer> bytes = keyValueLeafPage.getBytes();
      final long recordPageKey = keyValueLeafPage.getPageKey();
      final IndexType indexType = keyValueLeafPage.getIndexType();
      final ResourceConfiguration resourceConfig = keyValueLeafPage.getResourceConfig();
      final RecordSerializer recordPersister = resourceConfig.recordPersister;
      final byte[][] deweyIds = keyValueLeafPage.getDeweyIds();
      final byte[][] slots = keyValueLeafPage.getSlots();
      final Map<Long, PageReference> references = keyValueLeafPage.getReferencesMap();

      if (bytes != null) {
        sink.write(bytes);
        return;
      }
      // Add references to overflow pages if necessary.
      keyValueLeafPage.addReferences(pageReadOnlyTrx);
      // Write page key.
      putVarLong(sink, recordPageKey);
      // Write revision number.
      sink.writeInt(pageReadOnlyTrx.getRevisionNumber());
      // Write index type.
      sink.writeByte(indexType.getID());

      // Write dewey IDs.
      if (resourceConfig.areDeweyIDsStored && recordPersister instanceof DeweyIdSerializer persistence) {
        var deweyIdsBitmap = new BitSet(Constants.NDP_NODE_COUNT);
        for (int i = 0; i < deweyIds.length; i++) {
          if (deweyIds[i] != null) {
            deweyIdsBitmap.set(i);
          }
        }
        SerializationType.serializeBitSet(sink, deweyIdsBitmap);
        sink.writeInt(deweyIdsBitmap.cardinality());

        boolean first = true;
        byte[] previousDeweyId = null;
        for (byte[] deweyId : deweyIds) {
          if (deweyId != null) {
            if (first) {
              first = false;
              persistence.serializeDeweyID(sink, deweyId, null, resourceConfig);
            } else {
              persistence.serializeDeweyID(sink, previousDeweyId, deweyId, resourceConfig);
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
      SerializationType.serializeBitSet(sink, entriesBitmap);

      var overlongEntriesBitmap = new BitSet(Constants.NDP_NODE_COUNT);
      final var overlongEntriesSortedByKey = references.entrySet().stream().sorted(Map.Entry.comparingByKey()).toList();
      for (final Map.Entry<Long, PageReference> entry : overlongEntriesSortedByKey) {
        final var pageOffset = PageReadOnlyTrx.recordPageOffset(entry.getKey());
        overlongEntriesBitmap.set(pageOffset);
      }
      SerializationType.serializeBitSet(sink, overlongEntriesBitmap);

      // Write normal entries.
      sink.writeInt(entriesBitmap.cardinality());
      for (final byte[] data : slots) {
        if (data != null) {
          final int length = data.length;
          sink.writeInt(length);
          sink.write(data);
        }
      }

      // Write overlong entries.
      sink.writeInt(overlongEntriesSortedByKey.size());
      for (final var entry : overlongEntriesSortedByKey) {
        // Write key in persistent storage.
        sink.writeLong(entry.getValue().getKey());
      }

      keyValueLeafPage.setHashCode(pageReadOnlyTrx.getReader().hashFunction.hashBytes(sink.toByteArray()).asBytes());
      keyValueLeafPage.setBytes(sink);
    }

    @Override
    public @NonNull Page getInstance(final Page nodePage, final PageReadOnlyTrx pageReadTrx) {
      assert nodePage instanceof KeyValueLeafPage;
      final KeyValueLeafPage page = (KeyValueLeafPage) nodePage;
      return new KeyValueLeafPage(page.getPageKey(), page.getIndexType(), pageReadTrx);
    }
  },

  /**
   * {@link NamePage}.
   */
  NAMEPAGE((byte) 2, NamePage.class) {
    @Override
    @NonNull Page deserializePage(final PageReadOnlyTrx pageReadTrx, final Bytes<?> source,
        final SerializationType type) {

      Page delegate = PageUtils.createDelegate(source, type);

      final int maxNodeKeysSize = source.readInt();
      final Int2LongMap maxNodeKeys = new Int2LongOpenHashMap((int) Math.ceil(maxNodeKeysSize / 0.75));
      for (int i = 0; i < maxNodeKeysSize; i++) {
        maxNodeKeys.put(i, source.readLong());
      }

      final int numberOfArrays = source.readInt();
      final int currentMaxLevelOfIndirectPagesSize = source.readInt();
      final Int2IntMap currentMaxLevelsOfIndirectPages = new Int2IntOpenHashMap((int) Math.ceil(currentMaxLevelOfIndirectPagesSize / 0.75));
      for (int i = 0; i < currentMaxLevelOfIndirectPagesSize; i++) {
        currentMaxLevelsOfIndirectPages.put(i, source.readByte() & 0xFF);
      }
      return new NamePage(delegate, maxNodeKeys, currentMaxLevelsOfIndirectPages, numberOfArrays);

    }

    @Override
    void serializePage(final PageReadOnlyTrx pageReadTrx, final Bytes<ByteBuffer> sink, final Page page,
        final SerializationType type) {
      NamePage namePage = (NamePage) page;
      sink.writeByte(NAMEPAGE.id);
      Page delegate = namePage.delegate();

      if (delegate instanceof ReferencesPage4) {
        sink.writeByte((byte) 0);
      } else if (delegate instanceof BitmapReferencesPage) {
        sink.writeByte((byte) 1);
      }

      final int maxNodeKeySize = namePage.getMaxNodeKeySize();
      sink.writeInt(maxNodeKeySize);
      for (int i = 0; i < maxNodeKeySize; i++) {
        final long keys = namePage.getMaxNodeKey(i);
        sink.writeLong(keys);
      }

      sink.writeInt(namePage.getNumberOfArrays());
      final int currentMaxLevelOfIndirectPagesSize = namePage.getCurrentMaxLevelOfIndirectPagesSize();
      sink.writeInt(currentMaxLevelOfIndirectPagesSize);
      for (int i = 0; i < currentMaxLevelOfIndirectPagesSize; i++){
        sink.writeByte((byte) namePage.getCurrentMaxLevelOfIndirectPages(i));
      }
    }

    @Override
    public @NonNull Page getInstance(final Page page, final PageReadOnlyTrx pageReadTrx) {
      return new NamePage();
    }
  },

  /**
   * {@link UberPage}.
   */
  UBERPAGE((byte) 3, UberPage.class) {
    @Override
    @NonNull Page deserializePage(final PageReadOnlyTrx pageReadTrx, final Bytes<?> source,
        final SerializationType type) {

      final int revisionCount = source.readInt();
      final boolean isBootstrap = false;

      return new UberPage(revisionCount, isBootstrap);
    }

    @Override
    void serializePage(final PageReadOnlyTrx pageReadTrx, final Bytes<ByteBuffer> sink, final Page page,
        final SerializationType type) {

      UberPage uberPage = (UberPage) page;

      sink.writeByte(UBERPAGE.id);
      sink.writeInt(uberPage.getRevisionCount());
      uberPage.setBootstrap(false);
    }

    @Override
    public @NonNull Page getInstance(final Page page, final PageReadOnlyTrx pageReadTrx) {
      return new UberPage();
    }
  },

  /**
   * {@link IndirectPage}.
   */
  INDIRECTPAGE((byte) 4, IndirectPage.class) {
    @Override
    @NonNull Page deserializePage(final PageReadOnlyTrx pageReadTrx, final Bytes<?> source,
        final SerializationType type) {
      Page delegate = PageUtils.createDelegate(source, type);
      return new IndirectPage(delegate);
    }

    @Override
    void serializePage(final PageReadOnlyTrx pageReadTrx, final Bytes<ByteBuffer> sink, final Page page,
        final SerializationType type) {
      IndirectPage indirectPage = (IndirectPage) page;
      Page delegate = indirectPage.delegate();
      sink.writeByte(INDIRECTPAGE.id);

      if (delegate instanceof ReferencesPage4) {
        sink.writeByte((byte) 0);
      } else if (delegate instanceof BitmapReferencesPage) {
        sink.writeByte((byte) 1);
      } else if (delegate instanceof FullReferencesPage) {
        sink.writeByte((byte) 2);
      }

    }

    @Override
    public @NonNull Page getInstance(final Page page, final PageReadOnlyTrx pageReadTrx) {
      return new IndirectPage();
    }
  },

  /**
   * {@link RevisionRootPage}.
   */
  REVISIONROOTPAGE((byte) 5, RevisionRootPage.class) {
    @Override
    @NonNull Page deserializePage(final PageReadOnlyTrx pageReadTrx, final Bytes<?> source,
        final SerializationType type) {

      Page delegate = new BitmapReferencesPage(8, source, type);
      final int revision = source.readInt();
      final Long maxNodeKeyInDocumentIndex = source.readLong();
      final Long maxNodeKeyInChangedNodesIndex = source.readLong();
      final Long maxNodeKeyInRecordToRevisionsIndex = source.readLong();
      final Long revisionTimestamp = source.readLong();
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
        user = new User(source.readUtf8(), UUID.fromString(source.readUtf8()));}

      return new RevisionRootPage(delegate, revision, maxNodeKeyInDocumentIndex,
                                  maxNodeKeyInChangedNodesIndex, maxNodeKeyInRecordToRevisionsIndex,
                                revisionTimestamp, commitMessage,currentMaxLevelOfDocumentIndexIndirectPages,
                                 currentMaxLevelOfChangedNodesIndirectPages,
                                currentMaxLevelOfRecordToRevisionsIndirectPages, user);
    }

    @Override
    void serializePage(final PageReadOnlyTrx pageReadTrx, final Bytes<ByteBuffer> sink, final Page page,
        final SerializationType type) {

      RevisionRootPage revisionRootPage = (RevisionRootPage) page;
      sink.writeByte(REVISIONROOTPAGE.id);

      //initial variables from RevisionRootPage, to serialize
      final long revisionTimestamp;
      final Instant commitTimestamp = revisionRootPage.getCommitTimestamp();
      final int revision = revisionRootPage.getRevision();
      final long maxNodeKeyInDocumentIndex = revisionRootPage.getMaxNodeKeyInDocumentIndex();
      final long maxNodeKeyInChangedNodesIndex = revisionRootPage.getMaxNodeKeyInChangedNodesIndex();
      final long maxNodeKeyInRecordToRevisionsIndex = revisionRootPage.getMaxNodeKeyInRecordToRevisionsIndex();
      final String commitMessage = revisionRootPage.getCommitMessage();
      final int currentMaxLevelOfDocumentIndexIndirectPages = revisionRootPage.getCurrentMaxLevelOfDocumentIndexIndirectPages();
      final int currentMaxLevelOfChangedNodesIndirectPages = revisionRootPage.getCurrentMaxLevelOfChangedNodesIndexIndirectPages();
      final int currentMaxLevelOfRecordToRevisionsIndirectPages = revisionRootPage.getCurrentMaxLevelOfRecordToRevisionsIndexIndirectPages();

      revisionTimestamp = commitTimestamp == null ? Instant.now().toEpochMilli() : commitTimestamp.toEpochMilli();

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
      Optional<User> user = revisionRootPage.getUser();
      final boolean hasUser =  user != null;
      sink.writeBoolean(hasUser);
      if (hasUser) {
        sink.writeUtf8(revisionRootPage.getUser().get().getName());
        sink.writeUtf8(revisionRootPage.getUser().get().getId().toString());
      }
    }

    @Override
    public @NonNull Page getInstance(final Page page, final PageReadOnlyTrx pageReadTrx) {
      return new RevisionRootPage();
    }
  },

  /**
   * {@link PathSummaryPage}.
   */
  PATHSUMMARYPAGE((byte) 6, PathSummaryPage.class) {
    @Override
    @NonNull Page deserializePage(final PageReadOnlyTrx pageReadTrx, final Bytes<?> source,
        final @NonNull SerializationType type) {
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

    @Override
    void serializePage(final PageReadOnlyTrx pageReadTrx, final Bytes<ByteBuffer> sink, final Page page,
        final @NonNull SerializationType type) {

      PathSummaryPage pathSummaryPage = (PathSummaryPage) page;
      sink.writeByte(PATHSUMMARYPAGE.id);

      sink.writeByte((byte) 0);

      final int  maxNodeKeySize =  pathSummaryPage.getMaxNodeKeySize();
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
    public @NonNull Page getInstance(final Page page, final PageReadOnlyTrx pageReadTrx) {
      return new PathSummaryPage();
    }
  },

  /**
   * {@link CASPage}.
   */
  CASPAGE((byte) 8, CASPage.class) {
    @NonNull Page deserializePage(final PageReadOnlyTrx pageReadTrx, final Bytes<?> source,
                                  final SerializationType type) {

      //source, bytes to read
      //type, kind of data
      Page delegate = PageUtils.createDelegate(source, type);

      final int  maxNodeKeySize = source.readInt();
      Int2LongMap maxNodeKeys = new Int2LongOpenHashMap((int) Math.ceil(maxNodeKeySize / 0.75));

      for (int i = 0; i < maxNodeKeySize; i++) {
        maxNodeKeys.put(i, source.readLong());
      }

      final int currentMaxLevelOfIndirectPagesSize = source.readInt();
      Int2IntMap currentMaxLevelsOfIndirectPages = new Int2IntOpenHashMap((int) Math.ceil(currentMaxLevelOfIndirectPagesSize / 0.75));

      for (int i = 0; i < currentMaxLevelOfIndirectPagesSize; i++) {
        currentMaxLevelsOfIndirectPages.put(i, source.readByte() & 0xFF);
      }

      return new CASPage(delegate, maxNodeKeys, currentMaxLevelsOfIndirectPages);
    }

    @Override
    void serializePage(final PageReadOnlyTrx pageReadTrx, final Bytes<ByteBuffer> sink, final Page page,
                       final SerializationType type) {

      CASPage casPage = (CASPage) page;
      Page delegate = casPage.delegate();
      sink.writeByte(CASPAGE.id);

      if (delegate instanceof ReferencesPage4) {
        sink.writeByte((byte) 0);
      } else if (delegate instanceof BitmapReferencesPage) {
        sink.writeByte((byte) 1);
      }

      final int  maxNodeKeySize =  casPage.getMaxNodeKeySize();
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
    public @NonNull Page getInstance(final Page page, final PageReadOnlyTrx pageReadTrx) {
      return new CASPage();
    }
  },

  /**
   * {@link OverflowPage}.
   */
  OVERFLOWPAGE((byte) 9, OverflowPage.class) {
    @Override
    @NonNull Page deserializePage(final PageReadOnlyTrx pageReadTrx, final Bytes<?> source,
        final SerializationType type) {

      final byte[] data = new byte[source.readInt()];
      source.read(data);

      return new OverflowPage(data);
    }

    @Override
    void serializePage(final PageReadOnlyTrx pageReadTrx, final Bytes<ByteBuffer> sink, final Page page,
        @NonNull SerializationType type) {
      OverflowPage overflowPage = (OverflowPage) page;
      sink.writeByte(OVERFLOWPAGE.id);
      sink.writeInt(overflowPage.getData().length);
      sink.write(overflowPage.getData());
    }

    @Override
    public @NonNull Page getInstance(final Page page, final PageReadOnlyTrx pageReadTrx) {
      return new OverflowPage();
    }
  },

  /**
   * {@link PathPage}.
   */
  PATHPAGE((byte) 10, PathPage.class) {

    @Override
    Page deserializePage(@NonNull PageReadOnlyTrx pageReadTrx, Bytes<?> source,
        @NonNull SerializationType type) {

      final Page delegate = PageUtils.createDelegate(source, type);

      final int maxNodeKeysSize = source.readInt();
      final Int2LongMap maxNodeKeys = new Int2LongOpenHashMap((int) Math.ceil(maxNodeKeysSize / 0.75));
      for (int i = 0; i < maxNodeKeysSize; i++) {
        maxNodeKeys.put(i, source.readLong());
      }

      final int currentMaxLevelOfIndirectPagesSize = source.readInt();
      final Int2IntMap currentMaxLevelsOfIndirectPages = new Int2IntOpenHashMap((int) Math.ceil(currentMaxLevelOfIndirectPagesSize / 0.75));
      for (int i = 0; i < currentMaxLevelOfIndirectPagesSize; i++) {
        currentMaxLevelsOfIndirectPages.put(i, source.readByte() & 0xFF);
      }
      return new PathPage(delegate, maxNodeKeys, currentMaxLevelsOfIndirectPages);
    }
    @Override
    void serializePage(final PageReadOnlyTrx pageReadTrx, Bytes<ByteBuffer> sink, @NonNull Page page,
                       @NonNull SerializationType type) {
      PathPage pathPage = (PathPage) page;
      Page delegate = pathPage.delegate();
      sink.writeByte(PATHPAGE.id);

      if (delegate instanceof ReferencesPage4) {
        sink.writeByte((byte) 0);
      } else if (delegate instanceof BitmapReferencesPage) {
        sink.writeByte((byte) 1);
      }
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
    public @NonNull Page getInstance(Page page, @NonNull PageReadOnlyTrx pageReadTrx) {
      return new PathPage();
    }
  },

  /**
   * {@link PathPage}.
   */
  DEWEYIDPAGE((byte) 11, DeweyIDPage.class) {

    @Override
    Page deserializePage(@NonNull PageReadOnlyTrx pageReadTrx, Bytes<?> source,
                         @NonNull SerializationType type) {

      Page delegate = PageUtils.createDelegate(source, type);
      final long maxNodeKey = source.readLong();
      final int currentMaxLevelOfIndirectPages = source.readByte() & 0xFF;
      return new DeweyIDPage(delegate, maxNodeKey, currentMaxLevelOfIndirectPages);
    }
    @Override
    void serializePage(@NonNull PageReadOnlyTrx pageReadTrx, Bytes<ByteBuffer> sink, @NonNull Page page,
        @NonNull SerializationType type) {
      DeweyIDPage deweyIDPage = (DeweyIDPage) page;
      Page delegate = deweyIDPage.delegate();
      sink.writeByte(DEWEYIDPAGE.id);

      if (delegate instanceof ReferencesPage4) {
        sink.writeByte((byte) 0);
      } else if (delegate instanceof BitmapReferencesPage) {
        sink.writeByte((byte) 1);
      }
      sink.writeLong(deweyIDPage.getMaxNodeKey());
      sink.writeByte((byte) deweyIDPage.getCurrentMaxLevelOfIndirectPages());
    }

    @Override
    public @NonNull Page getInstance(Page page, @NonNull PageReadOnlyTrx pageReadTrx) {
      return new DeweyIDPage();
    }
  };

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
   * @param pageReadOnlyTrx the read only page transaction
   * @param sink            {@link Bytes<ByteBuffer>} instance
   * @param page            {@link Page} implementation
   */
  abstract void serializePage(final PageReadOnlyTrx pageReadOnlyTrx, final Bytes<ByteBuffer> sink, final Page page,
      final SerializationType type);

  /**
   * Deserialize page.
   *
   * @param pageReadTrx the read only page transaction
   * @param source      {@link Bytes<ByteBuffer>} instance
   * @return page instance implementing the {@link Page} interface
   */
  abstract Page deserializePage(final PageReadOnlyTrx pageReadTrx, final Bytes<?> source,
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
   * @param page        instance of class which implements {@link Page}
   * @param pageReadTrx instance of class which implements {@link PageReadOnlyTrx}
   * @return new page instance
   */
  public abstract @NonNull Page getInstance(final Page page, final PageReadOnlyTrx pageReadTrx);
}
