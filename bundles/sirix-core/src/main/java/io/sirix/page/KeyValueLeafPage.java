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
package io.sirix.page;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.DeweyIdSerializer;
import io.sirix.node.interfaces.RecordSerializer;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesOut;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import io.sirix.api.PageReadOnlyTrx;
import io.sirix.api.PageTrx;
import io.sirix.page.interfaces.KeyValuePage;
import io.sirix.settings.Constants;
import io.sirix.utils.ArrayIterator;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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

  private volatile BytesOut<?> bytes;

  private volatile byte[] hashCode;

  private int hash;

  /**
   * Copy constructor.
   *
   * @param pageToClone the page to clone
   */
  @SuppressWarnings("CopyConstructorMissesField")
  public KeyValueLeafPage(final KeyValueLeafPage pageToClone) {
    this.addedReferences = false;
    this.references = pageToClone.references;
    this.recordPageKey = pageToClone.recordPageKey;
    this.records = Arrays.copyOf(pageToClone.records, pageToClone.records.length);
    this.slots = Arrays.copyOf(pageToClone.slots, pageToClone.slots.length);
    this.deweyIds = Arrays.copyOf(pageToClone.deweyIds, pageToClone.deweyIds.length);
    this.indexType = pageToClone.indexType;
    this.recordPersister = pageToClone.recordPersister;
    this.resourceConfig = pageToClone.resourceConfig;
    this.revision = pageToClone.revision;
    this.areDeweyIDsStored = pageToClone.areDeweyIDsStored;
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

    this.references = new ConcurrentHashMap<>();
    this.recordPageKey = recordPageKey;
    this.records = new DataRecord[Constants.NDP_NODE_COUNT];
    this.slots = new byte[Constants.NDP_NODE_COUNT][];
    this.indexType = indexType;
    this.resourceConfig = pageReadOnlyTrx.getResourceSession().getResourceConfig();
    this.recordPersister = resourceConfig.recordPersister;
    this.deweyIds = new byte[Constants.NDP_NODE_COUNT][];
    this.revision = pageReadOnlyTrx.getRevisionNumber();
    this.areDeweyIDsStored = resourceConfig.areDeweyIDsStored;
  }

  /**
   * Constructor which reads deserialized data to the {@link KeyValueLeafPage} from the storage.
   *
   * @param recordPageKey     This is the base key of all contained nodes.
   * @param revision          The current revision.
   * @param indexType         The index type.
   * @param resourceConfig    The resource configuration.
   * @param areDeweyIDsStored Determines if DeweyIDs are stored or not.
   * @param recordPersister   Persistenter.
   * @param slots             Slots which were serialized.
   * @param deweyIds          DeweyIDs.
   * @param references        References to overflow pages.
   */
  KeyValueLeafPage(final long recordPageKey, final int revision, final IndexType indexType,
      final ResourceConfiguration resourceConfig, final boolean areDeweyIDsStored,
      final RecordSerializer recordPersister, final byte[][] slots, final byte[][] deweyIds,
      final Map<Long, PageReference> references) {

    this.recordPageKey = recordPageKey;
    this.revision = revision;
    this.indexType = indexType;
    this.resourceConfig = resourceConfig;
    this.areDeweyIDsStored = areDeweyIDsStored;
    this.recordPersister = recordPersister;
    this.slots = slots;
    this.deweyIds = deweyIds;
    this.references = references;
    this.records = new DataRecord[Constants.NDP_NODE_COUNT];

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

  /**
   * Get bytes to serialize
   *
   * @return bytes
   */
  public BytesOut<?> getBytes() {
    return bytes;
  }

  /**
   * Set bytes after serialized
   *
   * @param bytes bytes
   */
  public void setBytes(BytesOut<?> bytes) {
    this.bytes = bytes;
  }

  public byte[][] getSlots() {
    return slots;
  }

  public byte[][] getDeweyIds() {
    return deweyIds;
  }

  public ResourceConfiguration getResourceConfig() {
    return resourceConfig;
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

  public void setHashCode(byte[] hashCode) {
    this.hashCode = hashCode;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public <I extends Iterable<DataRecord>> I values() {
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

  public Map<Long, PageReference> getReferencesMap() {
    return references;
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
  public void addReferences(final PageReadOnlyTrx pageReadOnlyTrx) {
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
