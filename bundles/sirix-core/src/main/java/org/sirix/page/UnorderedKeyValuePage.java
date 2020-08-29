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
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.PageTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.index.IndexType;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.node.interfaces.NodePersistenter;
import org.sirix.node.interfaces.RecordPersister;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.settings.Constants;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.util.*;
import java.util.Map.Entry;

import static java.util.stream.Collectors.toList;
import static org.sirix.node.Utils.getVarLong;
import static org.sirix.node.Utils.putVarLong;

/**
 * <p>
 * An UnorderedKeyValuePage stores a set of records, commonly nodes in an unordered datastructure.
 * </p>
 * <p>
 * The page currently is not thread safe (might have to be for concurrent write-transactions)!
 * </p>
 */
public final class UnorderedKeyValuePage implements KeyValuePage<Long, DataRecord> {

  /**
   * The current revision.
   */
  private final int revision;

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
   * Records (must be a {@link LinkedHashMap} to provide consistent iteration order).
   */
  private final LinkedHashMap<Long, DataRecord> records;

  /**
   * Slots which have to be serialized.
   */
  private final Map<Long, byte[]> slots;

  /**
   * Dewey IDs which have to be serialized.
   */
  private final Map<SirixDeweyID, Long> deweyIDs;

  /**
   * Sirix {@link PageReadOnlyTrx}.
   */
  private final PageReadOnlyTrx pageReadOnlyTrx;

  /**
   * The index type.
   */
  private final IndexType indexType;

  /**
   * Persistenter.
   */
  private final RecordPersister recordPersister;

  /**
   * The resource configuration.
   */
  private final ResourceConfiguration resourceConfig;

  /**
   * Copy constructor.
   *
   * @param pageReadOnlyTrx the page read-only trx
   * @param pageToClone     the page to clone
   */
  public UnorderedKeyValuePage(final PageReadOnlyTrx pageReadOnlyTrx, final UnorderedKeyValuePage pageToClone) {
    addedReferences = pageToClone.addedReferences;
    references = pageToClone.references;
    recordPageKey = pageToClone.recordPageKey;
    records = pageToClone.records;
    slots = pageToClone.slots;
    deweyIDs = pageToClone.deweyIDs;
    this.pageReadOnlyTrx = pageReadOnlyTrx;
    indexType = pageToClone.indexType;
    recordPersister = pageToClone.recordPersister;
    resourceConfig = pageToClone.resourceConfig;
    revision = pageToClone.revision;
  }

  /**
   * Constructor which initializes a new {@link UnorderedKeyValuePage}.
   *
   * @param recordPageKey   base key assigned to this node page
   * @param indexType       the index type
   * @param pageReadOnlyTrx the page reading transaction
   */
  public UnorderedKeyValuePage(final @Nonnegative long recordPageKey, final IndexType indexType,
      final PageReadOnlyTrx pageReadOnlyTrx) {
    // Assertions instead of checkNotNull(...) checks as it's part of the
    // internal flow.
    assert recordPageKey >= 0 : "recordPageKey must not be negative!";
    assert pageReadOnlyTrx != null : "The page reading trx must not be null!";

    references = new LinkedHashMap<>();
    this.recordPageKey = recordPageKey;
    records = new LinkedHashMap<>();
    slots = new LinkedHashMap<>();
    this.pageReadOnlyTrx = pageReadOnlyTrx;
    this.indexType = indexType;
    resourceConfig = pageReadOnlyTrx.getResourceManager().getResourceConfig();
    recordPersister = resourceConfig.recordPersister;

    if (this.pageReadOnlyTrx.getResourceManager().getResourceConfig().areDeweyIDsStored
        && recordPersister instanceof NodePersistenter) {
      deweyIDs = new LinkedHashMap<>();
    } else {
      deweyIDs = Collections.emptyMap();
    }

    this.revision = pageReadOnlyTrx.getRevisionNumber();
  }

  /**
   * Constructor which reads the {@link UnorderedKeyValuePage} from the storage.
   *
   * @param in          input bytes to read page from
   * @param pageReadTrx {@link PageReadOnlyTrx} implementation
   */
  protected UnorderedKeyValuePage(final DataInput in, final PageReadOnlyTrx pageReadTrx) throws IOException {
    recordPageKey = getVarLong(in);
    revision = in.readInt();
    resourceConfig = pageReadTrx.getResourceManager().getResourceConfig();
    recordPersister = resourceConfig.recordPersister;
    this.pageReadOnlyTrx = pageReadTrx;
    slots = new LinkedHashMap<>();

    if (resourceConfig.areDeweyIDsStored && recordPersister instanceof NodePersistenter persistenter) {
      deweyIDs = new LinkedHashMap<>();
      final int deweyIDSize = in.readInt();

      records = new LinkedHashMap<>(deweyIDSize);
      SirixDeweyID optionalDeweyId = null;

      for (int index = 0; index < deweyIDSize; index++) {
        optionalDeweyId = persistenter.deserializeDeweyID(in, optionalDeweyId, resourceConfig);

        if (optionalDeweyId != null) {
          deserializeRecordAndPutIntoMap(in, optionalDeweyId);
        }
      }
    } else {
      deweyIDs = Collections.emptyMap();
      records = new LinkedHashMap<>();
    }

    final var entriesBitmap = SerializationType.deserializeBitSet(in);
    final var overlongEntriesBitmap = SerializationType.deserializeBitSet(in);

    final int normalEntrySize = in.readInt();
    var setBit = -1;
    for (int index = 0; index < normalEntrySize; index++) {
      setBit = entriesBitmap.nextSetBit(setBit + 1);
      assert setBit >= 0;
      final long key = recordPageKey * Constants.NDP_NODE_COUNT + setBit;
      final int dataSize = in.readInt();
      final byte[] data = new byte[dataSize];
      in.readFully(data);
      final DataRecord record = recordPersister.deserialize(new DataInputStream(new ByteArrayInputStream(data)),
                                                            key,
                                                            null,
                                                            this.pageReadOnlyTrx);
      records.put(key, record);
    }

    final int overlongEntrySize = in.readInt();
    references = new LinkedHashMap<>(overlongEntrySize);
    setBit = -1;
    for (int index = 0; index < overlongEntrySize; index++) {
      setBit = overlongEntriesBitmap.nextSetBit(setBit + 1);
      assert setBit >= 0;
      final long key = recordPageKey * Constants.NDP_NODE_COUNT + setBit;
      final PageReference reference = new PageReference();
      reference.setKey(in.readLong());
      references.put(key, reference);
    }
    assert pageReadTrx != null : "pageReadTrx must not be null!";
    indexType = IndexType.getType(in.readByte());
  }

  private void deserializeRecordAndPutIntoMap(DataInput in, SirixDeweyID deweyId) {
    try {
      final long key = getVarLong(in);
      final int dataSize = in.readInt();
      final byte[] data = new byte[dataSize];
      in.readFully(data);
      final DataRecord record = recordPersister.deserialize(new DataInputStream(new ByteArrayInputStream(data)),
                                                            key,
                                                            deweyId,
                                                            pageReadOnlyTrx);
      records.put(key, record);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public long getPageKey() {
    return recordPageKey;
  }

  @Override
  public DataRecord getValue(final Long key) {
    assert key != null : "key must not be null!";
    DataRecord record = records.get(key);
    if (record == null) {
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
      final InputStream in = new ByteArrayInputStream(data);
      try {
        record = recordPersister.deserialize(new DataInputStream(in), key, null, null);
      } catch (final IOException e) {
        return null;
      }
      records.put(key, record);
    }
    return record;
  }

  @Override
  public void setRecord(final Long key, @Nonnull final DataRecord value) {
    assert value != null : "record must not be null!";
    addedReferences = false;
    records.put(key, value);
  }

  @Override
  public void serialize(final DataOutput out, final SerializationType type) throws IOException {
    if (!addedReferences) {
      addReferences();
    }
    // Write page key.
    putVarLong(out, recordPageKey);
    // Write revision number.
    out.writeInt(revision);
    // Write dewey IDs.
    if (resourceConfig.areDeweyIDsStored && recordPersister instanceof NodePersistenter persistence) {
      out.writeInt(deweyIDs.size());
      final List<SirixDeweyID> ids = new ArrayList<>(deweyIDs.keySet());
      ids.sort(Comparator.comparingInt((SirixDeweyID sirixDeweyID) -> sirixDeweyID.toBytes().length));
      final var iter = Iterators.peekingIterator(ids.iterator());
      SirixDeweyID id = null;
      if (iter.hasNext()) {
        id = iter.next();
        persistence.serializeDeweyID(out, id, null, resourceConfig);
        serializeDeweyRecord(id, out);
      }
      while (iter.hasNext()) {
        final var nextDeweyID = iter.next();
        persistence.serializeDeweyID(out, id, nextDeweyID, resourceConfig);
        serializeDeweyRecord(nextDeweyID, out);
        id = nextDeweyID;
      }
    }

    final var entriesBitmap = new BitSet(Constants.NDP_NODE_COUNT);
    final var entriesSortedByKey = slots.entrySet().stream().sorted(Entry.comparingByKey()).collect(toList());
    for (final Entry<Long, byte[]> entry : entriesSortedByKey) {
      final var pageOffset = pageReadOnlyTrx.recordPageOffset(entry.getKey());
      entriesBitmap.set(pageOffset);
    }
    SerializationType.serializeBitSet(out, entriesBitmap);

    final var overlongEntriesBitmap = new BitSet(Constants.NDP_NODE_COUNT);
    final var overlongEntriesSortedByKey =
        references.entrySet().stream().sorted(Entry.comparingByKey()).collect(toList());
    for (final Map.Entry<Long, PageReference> entry : overlongEntriesSortedByKey) {
      final var pageOffset = pageReadOnlyTrx.recordPageOffset(entry.getKey());
      overlongEntriesBitmap.set(pageOffset);
    }
    SerializationType.serializeBitSet(out, overlongEntriesBitmap);

    // Write normal entries.
    out.writeInt(entriesSortedByKey.size());
    for (final var entry : entriesSortedByKey) {
      final byte[] data = entry.getValue();
      final int length = data.length;
      out.writeInt(length);
      out.write(data);
    }

    // Write overlong entries.
    out.writeInt(overlongEntriesSortedByKey.size());
    for (final var entry : overlongEntriesSortedByKey) {
      // Write key in persistent storage.
      out.writeLong(entry.getValue().getKey());
    }

    out.writeByte(indexType.getID());
  }

  private void serializeDeweyRecord(SirixDeweyID id, DataOutput out) throws IOException {
    final long recordKey = deweyIDs.get(id);
    putVarLong(out, recordKey);
    final byte[] data = slots.get(recordKey);
    final int length = data.length;
    out.writeInt(length);
    out.write(data);
    slots.remove(recordKey);
  }

  @Override
  public String toString() {
    final MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this).add("pagekey", recordPageKey);
    for (final DataRecord record : records.values()) {
      helper.add("record", record);
    }
    for (final PageReference reference : references.values()) {
      helper.add("reference", reference);
    }
    return helper.toString();
  }

  @Override
  public Set<Entry<Long, DataRecord>> entrySet() {
    return records.entrySet();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(recordPageKey, records, references);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof UnorderedKeyValuePage other) {
      return recordPageKey == other.recordPageKey && Objects.equal(records, other.records) && Objects.equal(references,
                                                                                                            other.references);
    }
    return false;
  }

  @Override
  public List<PageReference> getReferences() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commit(@Nonnull PageTrx pageWriteTrx) {
    if (!addedReferences) {
      try {
        addReferences();
      } catch (final IOException e) {
        throw new SirixIOException(e);
      }
    }

    for (final PageReference reference : references.values()) {
      if (!(reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
          && reference.getLogKey() == Constants.NULL_ID_LONG)) {
        pageWriteTrx.commit(reference);
      }
    }
  }

  // Add references to OverflowPages.
  private void addReferences() throws IOException {
    final var storeDeweyIDs = pageReadOnlyTrx.getResourceManager().getResourceConfig().areDeweyIDsStored;

    final var entries = sort();
    for (final var entry : entries) {
      final var record = entry.getValue();
      final var recordID = record.getNodeKey();
      if (slots.get(recordID) == null) {
        // Must be either a normal record or one which requires an
        // Overflow page.
        final byte[] data;
        try (final var output = new ByteArrayOutputStream(); final var out = new DataOutputStream(output)) {
          recordPersister.serialize(out, record, pageReadOnlyTrx);
          data = output.toByteArray();
        }

        if (data.length > PageConstants.MAX_RECORD_SIZE) {
          final var reference = new PageReference();
          reference.setPage(new OverflowPage(data));
          references.put(recordID, reference);
        } else {
          if (storeDeweyIDs && recordPersister instanceof NodePersistenter && record.getDeweyID() != null
              && record.getNodeKey() != 0) {
            deweyIDs.put(record.getDeweyID(), record.getNodeKey());
          }
          slots.put(recordID, data);
        }
      }
    }

    addedReferences = true;
  }

  private List<Entry<Long, DataRecord>> sort() {
    // Sort entries which have deweyIDs according to their byte-length.
    final List<Map.Entry<Long, DataRecord>> entries = new ArrayList<>(records.entrySet());
    final boolean storeDeweyIDs = pageReadOnlyTrx.getResourceManager().getResourceConfig().areDeweyIDsStored;
    if (storeDeweyIDs && recordPersister instanceof NodePersistenter) {
      entries.sort((a, b) -> {
        if (a.getValue() instanceof ImmutableNode && b.getValue() instanceof ImmutableNode) {
          final SirixDeweyID first = a.getValue().getDeweyID();
          final SirixDeweyID second = b.getValue().getDeweyID();

          // Document node has no DeweyID.
          if (first == null && second != null)
            return 1;

          if (second == null && first != null)
            return -1;

          if (first == null)
            return 0;

          return first.compareTo(second);
        }

        return -1;
      });
    }

    return entries;
  }

  @Override
  public Collection<DataRecord> values() {
    return records.values();
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
  public PageReadOnlyTrx getPageReadOnlyTrx() {
    return pageReadOnlyTrx;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <C extends KeyValuePage<Long, DataRecord>> C newInstance(final long recordPageKey,
      @Nonnull final IndexType indexType, @Nonnull final PageReadOnlyTrx pageReadTrx) {
    return (C) new UnorderedKeyValuePage(recordPageKey, indexType, pageReadTrx);
  }

  @Override
  public IndexType getIndexType() {
    return indexType;
  }

  @Override
  public int size() {
    return records.size() + references.size();
  }

  @Override
  public void setPageReference(final Long key, @Nonnull final PageReference reference) {
    assert key != null;
    references.put(key, reference);
  }

  @Override
  public Set<Entry<Long, PageReference>> referenceEntrySet() {
    return references.entrySet();
  }

  @Override
  public PageReference getPageReference(final Long key) {
    assert key != null;
    return references.get(key);
  }

  @Override
  public int getRevision() {
    return revision;
  }

}