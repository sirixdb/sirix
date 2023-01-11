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
import com.google.common.collect.Iterators;
import it.unimi.dsi.fastutil.objects.Object2LongLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import net.openhft.chronicle.bytes.Bytes;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.PageTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.index.IndexType;
import org.sirix.io.BytesUtils;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.node.interfaces.NodePersistenter;
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
public final class UnorderedKeyValuePage implements KeyValuePage<DataRecord> {

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
   * Dewey IDs to node key mapping.
   */
  private final Object2LongMap<SirixDeweyID> deweyIDs;

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

  private int recordsStored;

  private int hash;

  /**
   * Copy constructor.
   *
   * @param pageToClone     the page to clone
   */
  public UnorderedKeyValuePage(final UnorderedKeyValuePage pageToClone) {
    addedReferences = pageToClone.addedReferences;
    references = pageToClone.references;
    recordPageKey = pageToClone.recordPageKey;
    records = pageToClone.records;
    slots = pageToClone.slots;
    deweyIDs = pageToClone.deweyIDs;
    indexType = pageToClone.indexType;
    recordPersister = pageToClone.recordPersister;
    resourceConfig = pageToClone.resourceConfig;
    revision = pageToClone.revision;
    recordsStored = pageToClone.recordsStored;
    areDeweyIDsStored = pageToClone.areDeweyIDsStored;
  }

  /**
   * Constructor which initializes a new {@link UnorderedKeyValuePage}.
   *
   * @param recordPageKey   base key assigned to this node page
   * @param indexType       the index type
   * @param pageReadOnlyTrx the page reading transaction
   */
  public UnorderedKeyValuePage(final @NonNegative long recordPageKey, final IndexType indexType,
      final PageReadOnlyTrx pageReadOnlyTrx) {
    // Assertions instead of checkNotNull(...) checks as it's part of the
    // internal flow.
    assert pageReadOnlyTrx != null : "The page reading trx must not be null!";

    references = new ConcurrentHashMap<>();
    this.recordPageKey = recordPageKey;
    records = new DataRecord[Constants.NDP_NODE_COUNT];
    slots = new byte[Constants.NDP_NODE_COUNT][];
    this.indexType = indexType;
    resourceConfig = pageReadOnlyTrx.getResourceSession().getResourceConfig();
    recordPersister = resourceConfig.recordPersister;
    deweyIDs = new Object2LongLinkedOpenHashMap<>((int) Math.ceil(Constants.NDP_NODE_COUNT / 0.75));
    this.revision = pageReadOnlyTrx.getRevisionNumber();
    recordsStored = 0;
    areDeweyIDsStored = resourceConfig.areDeweyIDsStored;
  }

  /**
   * Constructor which reads the {@link UnorderedKeyValuePage} from the storage.
   *
   * @param in              input bytes to read page from
   * @param pageReadOnlyTrx {@link PageReadOnlyTrx} implementation
   */
  UnorderedKeyValuePage(final Bytes<ByteBuffer> in, final PageReadOnlyTrx pageReadOnlyTrx) {
    recordPageKey = getVarLong(in);
    revision = in.readInt();
    resourceConfig = pageReadOnlyTrx.getResourceSession().getResourceConfig();
    areDeweyIDsStored = resourceConfig.areDeweyIDsStored;
    recordPersister = resourceConfig.recordPersister;
    slots = new byte[Constants.NDP_NODE_COUNT][];

    if (resourceConfig.areDeweyIDsStored && recordPersister instanceof NodePersistenter persistenter) {
      final int deweyIDSize = in.readInt();
      deweyIDs = new Object2LongLinkedOpenHashMap<>((int) Math.ceil(Constants.NDP_NODE_COUNT / 0.75));
      records = new DataRecord[Constants.NDP_NODE_COUNT];
      byte[] optionalDeweyId = null;
      var byteBufferBytes = Bytes.elasticByteBuffer();

      for (int index = 0; index < deweyIDSize; index++) {
        final byte[] deweyID = persistenter.deserializeDeweyID(in, optionalDeweyId, resourceConfig);

        optionalDeweyId = deweyID;

        if (deweyID != null) {
          deserializeRecordAndPutIntoMap(in, deweyID, byteBufferBytes, pageReadOnlyTrx);
        }
      }

      byteBufferBytes.clear();
    } else {
      deweyIDs = new Object2LongLinkedOpenHashMap<>((int) Math.ceil(Constants.NDP_NODE_COUNT / 0.75));
      records = new DataRecord[Constants.NDP_NODE_COUNT];
    }

    final var entriesBitmap = SerializationType.deserializeBitSet(in);
    final var overlongEntriesBitmap = SerializationType.deserializeBitSet(in);

    final int normalEntrySize = in.readInt();
    var setBit = -1;
    var byteBufferBytes = Bytes.elasticByteBuffer(50);
    for (int index = 0; index < normalEntrySize; index++) {
      setBit = entriesBitmap.nextSetBit(setBit + 1);
      assert setBit >= 0;
      final long key = (recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + setBit;
      final int dataSize = in.readInt();
      in.read(byteBufferBytes, dataSize);
      final DataRecord record = recordPersister.deserialize(byteBufferBytes, key, null, pageReadOnlyTrx);
      byteBufferBytes.clear();
      final var offset = PageReadOnlyTrx.recordPageOffset(key);
      records[offset] = record;
      recordsStored++;
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
    indexType = IndexType.getType(in.readByte());
  }

  private void deserializeRecordAndPutIntoMap(Bytes<ByteBuffer> in, byte[] deweyId, Bytes<ByteBuffer> byteBufferBytes,
      PageReadOnlyTrx pageReadOnlyTrx) {
    final long key = getVarLong(in);
    final int dataSize = in.readInt();
    final byte[] data = new byte[dataSize];
    in.read(data);
    BytesUtils.doWrite(byteBufferBytes, data);
    final DataRecord record = recordPersister.deserialize(byteBufferBytes, key, deweyId, pageReadOnlyTrx);
    byteBufferBytes.clear();
    final var offset = PageReadOnlyTrx.recordPageOffset(key);
    if (records[offset] == null) {
      recordsStored++;
    }
    records[offset] = record;
  }

  @Override
  public long getPageKey() {
    return recordPageKey;
  }

  @Override
  public DataRecord getValue(final @Nullable PageReadOnlyTrx pageReadOnlyTrx, final long key) {
    final var offset = PageReadOnlyTrx.recordPageOffset(key);
    DataRecord record = records[offset];
    if (record == null && pageReadOnlyTrx != null) {
      byte[] data;
      try {
        final PageReference reference = references.get(key);
        if (reference != null && reference.getKey() != Constants.NULL_ID_LONG) {
          data = ((OverflowPage) pageReadOnlyTrx.getReader().read(reference, pageReadOnlyTrx)).getData();
        } else {
          return null;
        }
      } catch (final SirixIOException e) {
        return null;
      }
      var byteBufferBytes = Bytes.elasticByteBuffer(data.length);
      BytesUtils.doWrite(byteBufferBytes, data);
      record = recordPersister.deserialize(byteBufferBytes, key, null, null);
      byteBufferBytes.clear();
      records[offset] = record;
    }
    return record;
  }

  @Override
  public void setRecord(@NonNull final DataRecord record) {
    addedReferences = false;
    final var offset = PageReadOnlyTrx.recordPageOffset(record.getNodeKey());
    if (records[offset] == null) {
      recordsStored++;
    }
    records[offset] = record;
    hash = 0;
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
    // Write dewey IDs.
    if (resourceConfig.areDeweyIDsStored && recordPersister instanceof NodePersistenter persistence) {
      // Write dewey IDs.
      out.writeInt(deweyIDs.size());
      final var iter = Iterators.peekingIterator(deweyIDs.keySet().iterator());
      SirixDeweyID id = null;
      if (iter.hasNext()) {
        id = iter.next();
        persistence.serializeDeweyID(out, id.toBytes(), null, resourceConfig);
        serializeDeweyRecord(id, out);
      }
      while (iter.hasNext()) {
        final var nextDeweyID = iter.next();
        persistence.serializeDeweyID(out, id.toBytes(), nextDeweyID.toBytes(), resourceConfig);
        serializeDeweyRecord(nextDeweyID, out);
        id = nextDeweyID;
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
    overlongEntriesBitmap = null;

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

    out.writeByte(indexType.getID());
    hashCode = pageReadOnlyTrx.getReader().hashFunction.hashBytes(out.toByteArray()).asBytes();
    bytes = out;
  }

  private void serializeDeweyRecord(SirixDeweyID id, Bytes<ByteBuffer> out) {
    final long recordKey = deweyIDs.getLong(id);
    putVarLong(out, recordKey);
    final var offset = PageReadOnlyTrx.recordPageOffset(recordKey);
    final byte[] data = slots[offset];
    final int length = data.length;
    out.writeInt(length);
    out.write(data);
    slots[offset] = null;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public <I extends Iterable<DataRecord>> I values() {
    //noinspection unchecked
    return (I) new ArrayIterator(records, records.length);
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
      hash = Objects.hashCode(recordPageKey, records, references);
    }
    return hash;
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof UnorderedKeyValuePage other) {
      return recordPageKey == other.recordPageKey && Arrays.equals(records, other.records) && Objects.equal(references,
                                                                                                            other.references);
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
      if (areDeweyIDsStored && recordPersister instanceof NodePersistenter) {
        processEntries(pageReadOnlyTrx, records);
        for (final var record : records) {
          if (record != null && record.getDeweyID() != null && record.getNodeKey() != 0) {
            deweyIDs.put(record.getDeweyID(), record.getNodeKey());
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
      if (slots[offset] == null) {
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
      }
    }
    out = null;
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
    return (C) new UnorderedKeyValuePage(recordPageKey, indexType, pageReadTrx);
  }

  @Override
  public IndexType getIndexType() {
    return indexType;
  }

  @Override
  public int size() {
    return recordsStored + references.size();
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
    deweyIDs.clear();
    references.clear();
    return this;
  }
}
