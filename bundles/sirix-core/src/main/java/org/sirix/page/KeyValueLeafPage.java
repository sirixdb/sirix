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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesIn;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.PageTrx;
import org.sirix.index.IndexType;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.node.interfaces.DeweyIdSerializer;
import org.sirix.node.interfaces.RecordSerializer;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.settings.Constants;
import org.sirix.utils.ArrayIterator;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import static org.sirix.node.Utils.getVarLong;
import static org.sirix.node.Utils.putVarLong;

/**
 * <p>
 * An UnorderedKeyValuePage stores a set of records, commonly nodes in an unordered data structure.
 * </p>
 * <p>
 * The page currently is not thread safe (might have to be for concurrent write-transactions)!
 * </p>
 */
public final class KeyValueLeafPage implements KeyValuePage<DataRecord> {

  /**
   * The current revision.
   */
  private final int revision;

  /**
   * Determines if DeweyIDs are stored or not.
   */
  private final boolean areDeweyIDsStored;

  /**
   * Determines if references to {@link OverflowPage}s have been added or not.
   */
  private boolean addedReferences;

  /**
   * References to overflow pages.
   */
  private final Map<Long, PageReference> references;

  /**
   * Key of record page. This is the base key of all contained nodes.
   */
  private final long recordPageKey;

  /**
   * The record-ID mapped to the records.
   */
  private final DataRecord[] records;

  /**
   * Slots which have to be serialized.
   */
  private final byte[][] slots;

  /**
   * DeweyIDs.
   */
  private final byte[][] deweyIds;

  /**
   * The index type.
   */
  private final IndexType indexType;

  /**
   * Persistenter.
   */
  private final RecordSerializer recordPersister;

  /**
   * The resource configuration.
   */
  private final ResourceConfiguration resourceConfig;

  private volatile Bytes<ByteBuffer> bytes;

  private volatile byte[] hashCode;

  private int hash;

  /**
   * Copy constructor.
   *
   * @param pageToClone the page to clone
   */
  @SuppressWarnings("CopyConstructorMissesField")
  public KeyValueLeafPage(final KeyValueLeafPage pageToClone) {
    addedReferences = false;
    references = pageToClone.references;
    recordPageKey = pageToClone.recordPageKey;
    records = Arrays.copyOf(pageToClone.records, pageToClone.records.length);
    slots = Arrays.copyOf(pageToClone.slots, pageToClone.slots.length);
    deweyIds = Arrays.copyOf(pageToClone.deweyIds, pageToClone.deweyIds.length);
    indexType = pageToClone.indexType;
    recordPersister = pageToClone.recordPersister;
    resourceConfig = pageToClone.resourceConfig;
    revision = pageToClone.revision;
    areDeweyIDsStored = pageToClone.areDeweyIDsStored;
  }

  /**
   * Constructor which initializes a new {@link KeyValueLeafPage}.
   *
   * @param recordPageKey   base key assigned to this node page
   * @param indexType       the index type
   * @param pageReadOnlyTrx the page reading transaction
   */
  public KeyValueLeafPage(final @NonNegative long recordPageKey, final IndexType indexType,
      final PageReadOnlyTrx pageReadOnlyTrx) {
    // Assertions instead of requireNonNull(...) checks as it's part of the
    // internal flow.
    assert pageReadOnlyTrx != null : "The page reading trx must not be null!";

    references = new ConcurrentHashMap<>();
    this.recordPageKey = recordPageKey;
    records = new DataRecord[Constants.NDP_NODE_COUNT];
    slots = new byte[Constants.NDP_NODE_COUNT][];
    this.indexType = indexType;
    resourceConfig = pageReadOnlyTrx.getResourceSession().getResourceConfig();
    recordPersister = resourceConfig.recordPersister;
    deweyIds = new byte[Constants.NDP_NODE_COUNT][];
    this.revision = pageReadOnlyTrx.getRevisionNumber();
    areDeweyIDsStored = resourceConfig.areDeweyIDsStored;
  }

  /**
   * Constructor which reads the {@link KeyValueLeafPage} from the storage.
   *
   * @param in              input bytes to read page from
   * @param pageReadOnlyTrx {@link PageReadOnlyTrx} implementation
   */
  KeyValueLeafPage(final BytesIn<?> in, final PageReadOnlyTrx pageReadOnlyTrx) {
    recordPageKey = getVarLong(in);
    revision = in.readInt();
    indexType = IndexType.getType(in.readByte());
    resourceConfig = pageReadOnlyTrx.getResourceSession().getResourceConfig();
    areDeweyIDsStored = resourceConfig.areDeweyIDsStored;
    recordPersister = resourceConfig.recordPersister;
    slots = new byte[Constants.NDP_NODE_COUNT][];
    deweyIds = new byte[Constants.NDP_NODE_COUNT][];
    records = new DataRecord[Constants.NDP_NODE_COUNT];

    if (resourceConfig.areDeweyIDsStored && recordPersister instanceof DeweyIdSerializer serializer) {
      final var deweyIdsBitmap = SerializationType.deserializeBitSet(in);
      final int deweyIdsSize = in.readInt();
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

          deweyId = serializer.deserializeDeweyID(in, deweyId, resourceConfig);
          deweyIds[setBit] = deweyId;
        }
      }
    }

    final var entriesBitmap = SerializationType.deserializeBitSet(in);
    final var overlongEntriesBitmap = SerializationType.deserializeBitSet(in);

    final int normalEntrySize = in.readInt();
    var setBit = -1;
    for (int index = 0; index < normalEntrySize; index++) {
      setBit = entriesBitmap.nextSetBit(setBit + 1);
      assert setBit >= 0;
      final long key = (recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + setBit;
      final int dataSize = in.readInt();
      assert dataSize > 0;
      final byte[] data = new byte[dataSize];
      in.read(data);
      final var offset = PageReadOnlyTrx.recordPageOffset(key);
      slots[offset] = data;
    }

    final int overlongEntrySize = in.readInt();
    references = new LinkedHashMap<>(overlongEntrySize);
    setBit = -1;
    for (int index = 0; index < overlongEntrySize; index++) {
      setBit = overlongEntriesBitmap.nextSetBit(setBit + 1);
      assert setBit >= 0;
      //recordPageKey * Constants.NDP_NODE_COUNT + setBit;
      final long key = (recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + setBit;
      final PageReference reference = new PageReference();
      reference.setKey(in.readLong());
      references.put(key, reference);
    }
  }

  @Override
  public long getPageKey() {
    return recordPageKey;
  }

  @Override
  public DataRecord getRecord(long key) {
    int offset = PageReadOnlyTrx.recordPageOffset(key);
    return records[offset];
  }

  @Override
  public byte[] getSlot(int slotNumber) {
    return slots[slotNumber];
  }

  @Override
  public void setRecord(@NonNull final DataRecord record) {
    addedReferences = false;
    final var offset = PageReadOnlyTrx.recordPageOffset(record.getNodeKey());
    synchronized (records) {
      records[offset] = record;
    }
  }

  @Override
  public <C extends KeyValuePage<DataRecord>> C copy() {
    return (C) new KeyValueLeafPage(this);
  }

  @Override
  public DataRecord[] records() {
    return records;
  }

  public byte[] getHashCode() {
    return hashCode;
  }

  @Override
  public void serialize(final PageReadOnlyTrx pageReadOnlyTrx, final Bytes<ByteBuffer> out,
      final SerializationType type) {
    if (bytes != null) {
      out.write(bytes);
      return;
    }
    // Add references to overflow pages if necessary.
    addReferences(pageReadOnlyTrx);
    // Write page key.
    putVarLong(out, recordPageKey);
    // Write revision number.
    out.writeInt(pageReadOnlyTrx.getRevisionNumber());
    // Write index type.
    out.writeByte(indexType.getID());

    // Write dewey IDs.
    if (resourceConfig.areDeweyIDsStored && recordPersister instanceof DeweyIdSerializer persistence) {
      var deweyIdsBitmap = new BitSet(Constants.NDP_NODE_COUNT);
      for (int i = 0; i < deweyIds.length; i++) {
        if (deweyIds[i] != null) {
          deweyIdsBitmap.set(i);
        }
      }
      SerializationType.serializeBitSet(out, deweyIdsBitmap);
      out.writeInt(deweyIdsBitmap.cardinality());

      boolean first = true;
      byte[] previousDeweyId = null;
      for (byte[] deweyId : deweyIds) {
        if (deweyId != null) {
          if (first) {
            first = false;
            persistence.serializeDeweyID(out, deweyId, null, resourceConfig);
          } else {
            persistence.serializeDeweyID(out, previousDeweyId, deweyId, resourceConfig);
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
    SerializationType.serializeBitSet(out, entriesBitmap);

    var overlongEntriesBitmap = new BitSet(Constants.NDP_NODE_COUNT);
    final var overlongEntriesSortedByKey = references.entrySet().stream().sorted(Entry.comparingByKey()).toList();
    for (final Map.Entry<Long, PageReference> entry : overlongEntriesSortedByKey) {
      final var pageOffset = PageReadOnlyTrx.recordPageOffset(entry.getKey());
      overlongEntriesBitmap.set(pageOffset);
    }
    SerializationType.serializeBitSet(out, overlongEntriesBitmap);

    // Write normal entries.
    out.writeInt(entriesBitmap.cardinality());
    for (final byte[] data : slots) {
      if (data != null) {
        final int length = data.length;
        out.writeInt(length);
        out.write(data);
      }
    }

    // Write overlong entries.
    out.writeInt(overlongEntriesSortedByKey.size());
    for (final var entry : overlongEntriesSortedByKey) {
      // Write key in persistent storage.
      out.writeLong(entry.getValue().getKey());
    }

    hashCode = pageReadOnlyTrx.getReader().hashFunction.hashBytes(out.toByteArray()).asBytes();
    bytes = out;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public <I extends Iterable<DataRecord>> I values() {
    //noinspection unchecked
    return (I) new ArrayIterator(records, records.length);
  }

  @Override
  public byte[][] slots() {
    return slots;
  }

  @Override
  public synchronized void setSlot(byte[] recordData, int offset) {
    slots[offset] = recordData;
  }

  @Override
  public byte[] getDeweyId(int offset) {
    return deweyIds[offset];
  }

  @Override
  public void setDeweyId(byte[] deweyId, int offset) {
    deweyIds[offset] = deweyId;
  }

  @Override
  public byte[][] deweyIds() {
    return deweyIds;
  }

  @Override
  public String toString() {
    final MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this).add("pagekey", recordPageKey);
    for (final DataRecord record : records) {
      helper.add("record", record);
    }
    for (final PageReference reference : references.values()) {
      helper.add("reference", reference);
    }
    return helper.toString();
  }

  @Override
  public int hashCode() {
    if (hash == 0) {
      hash = Objects.hashCode(recordPageKey, revision);
    }
    return hash;
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof KeyValueLeafPage other) {
      return recordPageKey == other.recordPageKey && revision == other.revision;
    }
    return false;
  }

  @Override
  public List<PageReference> getReferences() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commit(@NonNull PageTrx pageWriteTrx) {
    addReferences(pageWriteTrx);

    for (final PageReference reference : references.values()) {
      if (!(reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
          && reference.getLogKey() == Constants.NULL_ID_LONG)) {
        pageWriteTrx.commit(reference);
      }
    }
  }

  // Add references to OverflowPages.
  private void addReferences(final PageReadOnlyTrx pageReadOnlyTrx) {
    if (!addedReferences) {
      if (areDeweyIDsStored && recordPersister instanceof DeweyIdSerializer) {
        processEntries(pageReadOnlyTrx, records);
        for (int i = 0; i < records.length; i++) {
          final DataRecord record = records[i];
          if (record != null && record.getDeweyID() != null && record.getNodeKey() != 0) {
            deweyIds[i] = record.getDeweyID().toBytes();
          }
        }
      } else {
        processEntries(pageReadOnlyTrx, records);
      }

      addedReferences = true;
    }
  }

  private void processEntries(final PageReadOnlyTrx pageReadOnlyTrx, final DataRecord[] records) {
    var out = Bytes.elasticByteBuffer(30);
    for (final DataRecord record : records) {
      if (record == null) {
        continue;
      }
      final var recordID = record.getNodeKey();
      final var offset = PageReadOnlyTrx.recordPageOffset(recordID);
      //if (slots[offset] == null) {
      // Must be either a normal record or one which requires an overflow page.
      recordPersister.serialize(out, record, pageReadOnlyTrx);
      final var data = out.toByteArray();
      out.clear();
      if (data.length > PageConstants.MAX_RECORD_SIZE) {
        final var reference = new PageReference();
        reference.setPage(new OverflowPage(data));
        references.put(recordID, reference);
      } else {
        slots[offset] = data;
      }
      //}
    }
  }

  @Override
  public PageReference getOrCreateReference(int offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean setOrCreateReference(int offset, PageReference pageReference) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <C extends KeyValuePage<DataRecord>> C newInstance(final long recordPageKey,
      @NonNull final IndexType indexType, @NonNull final PageReadOnlyTrx pageReadTrx) {
    //noinspection unchecked
    return (C) new KeyValueLeafPage(recordPageKey, indexType, pageReadTrx);
  }

  @Override
  public IndexType getIndexType() {
    return indexType;
  }

  @Override
  public int size() {
    return getNumberOfNonNullEntries(records, slots) + references.size();
  }

  @Override
  public void setPageReference(final long key, @NonNull final PageReference reference) {
    references.put(key, reference);
  }

  @Override
  public Set<Entry<Long, PageReference>> referenceEntrySet() {
    return references.entrySet();
  }

  @Override
  public PageReference getPageReference(final long key) {
    return references.get(key);
  }

  @Override
  public int getRevision() {
    return revision;
  }

  @Override
  public void close() {
  }

  @Override
  public KeyValuePage<DataRecord> clearPage() {
    if (bytes != null) {
      bytes.clear();
      bytes = null;
    }
    hashCode = null;
    Arrays.fill(records, null);
    Arrays.fill(slots, null);
    Arrays.fill(deweyIds, null);
    references.clear();
    return this;
  }

  public static int getNumberOfNonNullEntries(DataRecord[] entries, byte[][] slots) {
    int count = 0;
    for (int i = 0; i < entries.length; i++) {
      if (entries[i] != null || slots[i] != null) {
        ++count;
      }
    }
    return count;
  }
}
