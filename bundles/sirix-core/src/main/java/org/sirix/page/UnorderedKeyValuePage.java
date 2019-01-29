/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
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

import static org.sirix.node.Utils.getVarLong;
import static org.sirix.node.Utils.putVarLong;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnegative;
import javax.annotation.Nullable;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.PageTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.node.Kind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.NodePersistenter;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.RecordPersister;
import org.sirix.node.interfaces.immutable.ImmutableXdmNode;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.settings.Constants;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

/**
 * <h1>UnorderedKeyValuePage</h1>
 *
 * <p>
 * An UnorderedKeyValuePage stores a set of records, commonly nodes in an unordered datastructure.
 * </p>
 * <p>
 * The page currently is not thread safe (might have to be for concurrent write-transactions)!
 * </p>
 */
public final class UnorderedKeyValuePage implements KeyValuePage<Long, Record> {

  private boolean mAddedReferences;

  /** References to overflow pages. */
  private final Map<Long, PageReference> mReferences;

  /** Key of record page. This is the base key of all contained nodes. */
  private final long mRecordPageKey;

  /** Records (must be a {@link LinkedHashMap} to provide consistent iteration order). */
  private final LinkedHashMap<Long, Record> mRecords;

  /** Slots which have to be serialized. */
  private final Map<Long, byte[]> mSlots;

  /** Dewey IDs which have to be serialized. */
  private final Map<SirixDeweyID, Long> mDeweyIDs;

  /** Sirix {@link PageReadOnlyTrx}. */
  private final PageReadOnlyTrx mPageReadTrx;

  /** The kind of page (in which subtree it resides). */
  private final PageKind mPageKind;

  /** Persistenter. */
  private final RecordPersister mRecordPersister;

  /** Reference key to the previous page if any. */
  private long mPreviousPageRefKey;

  /** The resource configuration. */
  private final ResourceConfiguration mResourceConfig;

  /**
   * Constructor which initializes a new {@link UnorderedKeyValuePage}.
   *
   * @param recordPageKey base key assigned to this node page
   * @param pageKind the kind of subtree page (NODEPAGE, PATHSUMMARYPAGE, TEXTVALUEPAGE,
   *        ATTRIBUTEVALUEPAGE)
   * @param pageReadTrx the page reading transaction
   */
  public UnorderedKeyValuePage(final @Nonnegative long recordPageKey, final PageKind pageKind,
      final long previousPageRefKey, final PageReadOnlyTrx pageReadTrx) {
    // Assertions instead of checkNotNull(...) checks as it's part of the
    // internal flow.
    assert recordPageKey >= 0 : "recordPageKey must not be negative!";
    assert pageReadTrx != null : "The page reading trx must not be null!";

    mReferences = new LinkedHashMap<>();
    mRecordPageKey = recordPageKey;
    mRecords = new LinkedHashMap<>();
    mSlots = new LinkedHashMap<>();
    mPageReadTrx = pageReadTrx;
    mPageKind = pageKind;
    mResourceConfig = pageReadTrx.getResourceManager().getResourceConfig();
    mRecordPersister = mResourceConfig.recordPersister;
    mPreviousPageRefKey = previousPageRefKey;

    if (mPageReadTrx.getResourceManager().getResourceConfig().areDeweyIDsStored
        && mRecordPersister instanceof NodePersistenter) {
      mDeweyIDs = new LinkedHashMap<>();
    } else {
      mDeweyIDs = Collections.emptyMap();
    }
  }

  /**
   * Constructor which reads the {@link UnorderedKeyValuePage} from the storage.
   *
   * @param in input bytes to read page from
   * @param pageReadTrx {@link PageReadOnlyTrx} implementation
   */
  protected UnorderedKeyValuePage(final DataInput in, final PageReadOnlyTrx pageReadTrx) throws IOException {
    mRecordPageKey = getVarLong(in);
    mResourceConfig = pageReadTrx.getResourceManager().getResourceConfig();
    mRecordPersister = mResourceConfig.recordPersister;
    mPageReadTrx = pageReadTrx;
    mSlots = new LinkedHashMap<>();

    if (mResourceConfig.areDeweyIDsStored && mRecordPersister instanceof NodePersistenter) {
      mDeweyIDs = new LinkedHashMap<>();
      final NodePersistenter persistenter = (NodePersistenter) mRecordPersister;
      final int deweyIDSize = in.readInt();

      mRecords = new LinkedHashMap<>(deweyIDSize);
      Optional<SirixDeweyID> id = Optional.empty();

      for (int index = 0; index < deweyIDSize; index++) {
        id = persistenter.deserializeDeweyID(in, id.orElse(null), mResourceConfig);

        id.ifPresent(deweyId -> {
          try {
            final long key = getVarLong(in);
            final int dataSize = in.readInt();
            final byte[] data = new byte[dataSize];
            in.readFully(data);
            final Record record = mRecordPersister.deserialize(new DataInputStream(new ByteArrayInputStream(data)), key,
                deweyId, mPageReadTrx);
            mRecords.put(key, record);
          } catch (final IOException e) {
            throw new SirixIOException(e);
          }
        });
      }
    } else {
      mDeweyIDs = Collections.emptyMap();
      mRecords = new LinkedHashMap<>();
    }

    final int normalEntrySize = in.readInt();
    for (int index = 0; index < normalEntrySize; index++) {
      final long key = getVarLong(in);
      final int dataSize = in.readInt();
      final byte[] data = new byte[dataSize];
      in.readFully(data);
      final Record record =
          mRecordPersister.deserialize(new DataInputStream(new ByteArrayInputStream(data)), key, null, mPageReadTrx);
      mRecords.put(key, record);
    }
    final int overlongEntrySize = in.readInt();
    mReferences = new LinkedHashMap<>(overlongEntrySize);
    for (int index = 0; index < overlongEntrySize; index++) {
      final long key = in.readLong();
      final PageReference reference = new PageReference();
      reference.setKey(in.readLong());
      mReferences.put(key, reference);
    }
    assert pageReadTrx != null : "pageReadTrx must not be null!";
    final boolean hasPreviousReference = in.readBoolean();
    if (hasPreviousReference) {
      mPreviousPageRefKey = in.readLong();
    } else {
      mPreviousPageRefKey = Constants.NULL_ID_LONG;
    }
    mPageKind = PageKind.getKind(in.readByte());
  }

  @Override
  public long getPageKey() {
    return mRecordPageKey;
  }

  @Override
  public Record getValue(final Long key) {
    assert key != null : "key must not be null!";
    Record record = mRecords.get(key);
    if (record == null) {
      byte[] data = null;
      try {
        final PageReference reference = mReferences.get(key);
        if (reference != null && reference.getKey() != Constants.NULL_ID_LONG) {
          data = ((OverflowPage) mPageReadTrx.getReader().read(reference, mPageReadTrx)).getData();
        } else {
          return null;
        }
      } catch (final SirixIOException e) {
        return null;
      }
      final InputStream in = new ByteArrayInputStream(data);
      try {
        record = mRecordPersister.deserialize(new DataInputStream(in), key, null, null);
      } catch (final IOException e) {
        return null;
      }
      mRecords.put(key, record);
    }
    return record;
  }

  @Override
  public void setEntry(final Long key, final Record value) {
    assert value != null : "record must not be null!";
    mAddedReferences = false;
    mRecords.put(key, value);
  }

  @Override
  public void serialize(final DataOutput out, final SerializationType type) throws IOException {
    if (!mAddedReferences) {
      addReferences();
    }
    // Write page key.
    putVarLong(out, mRecordPageKey);
    // Write dewey IDs.
    if (mResourceConfig.areDeweyIDsStored && mRecordPersister instanceof NodePersistenter) {
      final NodePersistenter persistenter = (NodePersistenter) mRecordPersister;
      out.writeInt(mDeweyIDs.size());
      final List<SirixDeweyID> ids = new ArrayList<>(mDeweyIDs.keySet());
      ids.sort((SirixDeweyID first, SirixDeweyID second) -> Integer.valueOf(first.toBytes().length)
                                                                   .compareTo(second.toBytes().length));
      final PeekingIterator<SirixDeweyID> iter = Iterators.peekingIterator(ids.iterator());
      SirixDeweyID id = null;
      if (iter.hasNext()) {
        id = iter.next();
        persistenter.serializeDeweyID(out, Kind.ELEMENT, id, null, mResourceConfig);
        serializeDeweyRecord(id, out);
      }
      while (iter.hasNext()) {
        final SirixDeweyID nextDeweyID = iter.next();
        persistenter.serializeDeweyID(out, Kind.ELEMENT, id, nextDeweyID, mResourceConfig);
        serializeDeweyRecord(nextDeweyID, out);
        id = nextDeweyID;
      }
    }
    // Write normal entries.
    out.writeInt(mSlots.size());
    for (final Entry<Long, byte[]> entry : mSlots.entrySet()) {
      putVarLong(out, entry.getKey());
      final byte[] data = entry.getValue();
      final int length = data.length;
      out.writeInt(length);
      out.write(data);
    }
    // Write overlong entries.
    out.writeInt(mReferences.size());
    for (final Map.Entry<Long, PageReference> entry : mReferences.entrySet()) {
      // Write record ID.
      out.writeLong(entry.getKey());
      // Write key in persistent storage.
      out.writeLong(entry.getValue().getKey());
    }
    // Write previous reference if it has any reference.
    final boolean hasPreviousReference = mPreviousPageRefKey != Constants.NULL_ID_LONG;
    out.writeBoolean(hasPreviousReference);
    if (hasPreviousReference) {
      out.writeLong(mPreviousPageRefKey);
    }
    out.writeByte(mPageKind.getID());
  }

  private void serializeDeweyRecord(SirixDeweyID id, DataOutput out) throws IOException {
    final long recordKey = mDeweyIDs.get(id);
    putVarLong(out, recordKey);
    final byte[] data = mSlots.get(recordKey);
    final int length = data.length;
    out.writeInt(length);
    out.write(data);
    mSlots.remove(recordKey);
  }

  @Override
  public String toString() {
    final MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this).add("pagekey", mRecordPageKey);
    for (final Record record : mRecords.values()) {
      helper.add("record", record);
    }
    for (final PageReference reference : mReferences.values()) {
      helper.add("reference", reference);
    }
    return helper.toString();
  }

  @Override
  public Set<Entry<Long, Record>> entrySet() {
    return mRecords.entrySet();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mRecordPageKey, mRecords, mReferences);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof UnorderedKeyValuePage) {
      final UnorderedKeyValuePage other = (UnorderedKeyValuePage) obj;
      return mRecordPageKey == other.mRecordPageKey && Objects.equal(mRecords, other.mRecords)
          && Objects.equal(mReferences, other.mReferences);
    }
    return false;
  }

  @Override
  public List<PageReference> getReferences() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <K extends Comparable<? super K>, V extends Record, S extends KeyValuePage<K, V>> void commit(
      PageTrx<K, V, S> pageWriteTrx) {
    if (!mAddedReferences) {
      try {
        addReferences();
      } catch (final IOException e) {
        throw new SirixIOException(e);
      }
    }

    for (final PageReference reference : mReferences.values()) {
      if (!(reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
          && reference.getLogKey() == Constants.NULL_ID_LONG)) {
        pageWriteTrx.commit(reference);
      }
    }
  }

  // Add references to OverflowPages.
  private void addReferences() throws IOException {
    final boolean storeDeweyIDs = mPageReadTrx.getResourceManager().getResourceConfig().areDeweyIDsStored;

    final List<Entry<Long, Record>> entries = sort();
    final Iterator<Entry<Long, Record>> it = entries.iterator();
    while (it.hasNext()) {
      final Entry<Long, Record> entry = it.next();
      final Record record = entry.getValue();
      final long recordID = record.getNodeKey();
      if (mSlots.get(recordID) == null) {
        // Must be either a normal record or one which requires an
        // Overflow page.
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        final DataOutput out = new DataOutputStream(output);
        mRecordPersister.serialize(out, record, mPageReadTrx);
        final byte[] data = output.toByteArray();
        if (data.length > PageConstants.MAX_RECORD_SIZE) {
          final PageReference reference = new PageReference();
          reference.setPage(new OverflowPage(data));
          mReferences.put(recordID, reference);
        } else {
          if (storeDeweyIDs && mRecordPersister instanceof NodePersistenter && record instanceof ImmutableXdmNode
              && ((ImmutableXdmNode) record).getDeweyID().isPresent() && record.getNodeKey() != 0)
            mDeweyIDs.put(((ImmutableXdmNode) record).getDeweyID().get(), record.getNodeKey());
          mSlots.put(recordID, data);
        }
      }
    }

    mAddedReferences = true;
  }

  private List<Entry<Long, Record>> sort() {
    // Sort entries which have deweyIDs according to their byte-length.
    final List<Map.Entry<Long, Record>> entries = new ArrayList<>(mRecords.entrySet());
    final boolean storeDeweyIDs = mPageReadTrx.getResourceManager().getResourceConfig().areDeweyIDsStored;
    if (storeDeweyIDs && mRecordPersister instanceof NodePersistenter) {
      entries.sort((a, b) -> {
        if (a.getValue() instanceof ImmutableXdmNode && b.getValue() instanceof ImmutableXdmNode) {
          final Optional<SirixDeweyID> first = ((ImmutableXdmNode) a.getValue()).getDeweyID();
          final Optional<SirixDeweyID> second = ((ImmutableXdmNode) b.getValue()).getDeweyID();

          // Document node has no DeweyID.
          if (!first.isPresent() && second.isPresent())
            return 1;

          if (!second.isPresent() && first.isPresent())
            return -1;

          if (!first.isPresent() && !second.isPresent())
            return 0;

          return first.get().toBytes().length == second.get().toBytes().length
              ? 0
              : first.get().toBytes().length > second.get().toBytes().length
                  ? 1
                  : -1;
        }

        return -1;
      });
    }

    return entries;
  }

  @Override
  public Collection<Record> values() {
    return mRecords.values();
  }

  @Override
  public PageReference getReference(int offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setReference(int offset, PageReference pageReference) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PageReadOnlyTrx getPageReadTrx() {
    return mPageReadTrx;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <C extends KeyValuePage<Long, Record>> C newInstance(final long recordPageKey, final PageKind pageKind,
      final long previousPageRefKey, final PageReadOnlyTrx pageReadTrx) {
    return (C) new UnorderedKeyValuePage(recordPageKey, pageKind, previousPageRefKey, pageReadTrx);
  }

  @Override
  public PageKind getPageKind() {
    return mPageKind;
  }

  @Override
  public int size() {
    return mRecords.size() + mReferences.size();
  }

  @Override
  public void setPageReference(final Long key, final PageReference reference) {
    assert key != null;
    mReferences.put(key, reference);
  }

  @Override
  public Set<Entry<Long, PageReference>> referenceEntrySet() {
    return mReferences.entrySet();
  }

  @Override
  public PageReference getPageReference(final Long key) {
    assert key != null;
    return mReferences.get(key);
  }

  @Override
  public long getPreviousReferenceKey() {
    return mPreviousPageRefKey;
  }

}
