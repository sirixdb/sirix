/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
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
package org.sirix.page;

import static org.sirix.node.Utils.getVarLong;
import static org.sirix.node.Utils.putVarLong;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import javax.annotation.Nonnegative;
import javax.annotation.Nullable;

import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.RecordPersistenter;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;

import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

/**
 * <h1>UnorderedKeyValuePage</h1>
 * 
 * <p>
 * An UnorderedKeyValuePage stores a set of records, commonly nodes in an
 * unordered datastructure.
 * </p>
 * <p>
 * The page currently is not thread safe (might have to be for concurrent
 * write-transactions)!
 * </p>
 */
public final class UnorderedKeyValuePage implements KeyValuePage<Long, Record> {

	private boolean mAddedReferences;

	private final Map<Long, PageReference> mReferences;

	/** Key of record page. This is the base key of all contained nodes. */
	private final long mRecordPageKey;

	/**
	 * Records (must be a {@link LinkedHashMap} to provide consistent iteration
	 * order).
	 */
	private final LinkedHashMap<Long, Record> mRecords;

	private final Map<Long, byte[]> mSlots;

	/** Determine if node page has been modified. */
	private boolean mIsDirty;

	/** Sirix {@link PageReadTrx}. */
	private final PageReadTrx mPageReadTrx;

	/** The kind of page (in which subtree it resides). */
	private final PageKind mPageKind;

	/** Persistenter. */
	private final RecordPersistenter mPersistenter;

	private Optional<PageReference> mPreviousPageReference;

	/**
	 * Constructor which initializes a new {@link UnorderedKeyValuePage}.
	 * 
	 * @param recordPageKey
	 *          base key assigned to this node page
	 * @param pageKind
	 *          the kind of subtree page (NODEPAGE, PATHSUMMARYPAGE,
	 *          TEXTVALUEPAGE, ATTRIBUTEVALUEPAGE)
	 * @param pageReadTrx
	 *          the page reading transaction
	 */
	public UnorderedKeyValuePage(final @Nonnegative long recordPageKey,
			final PageKind pageKind,
			final Optional<PageReference> previousPageRef,
			final PageReadTrx pageReadTrx) {
		// Assertions instead of checkNotNull(...) checks as it's part of the
		// internal flow.
		assert recordPageKey >= 0 : "recordPageKey must not be negative!";
		assert pageReadTrx != null : "pageReadTrx must not be null!";
		mReferences = new HashMap<>();
		mRecordPageKey = recordPageKey;
		mRecords = new LinkedHashMap<>();
		mSlots = new HashMap<>();
		mIsDirty = true;
		mPageReadTrx = pageReadTrx;
		mPageKind = pageKind;
		mPersistenter = pageReadTrx.getSession().getResourceConfig().mPersistenter;
		mPreviousPageReference = previousPageRef;
	}

	/**
	 * Constructor which reads the {@link UnorderedKeyValuePage} from the storage.
	 * 
	 * @param in
	 *          input bytes to read page from
	 * @param pageReadTrx
	 *          {@link PageReadTrx} implementation
	 */
	protected UnorderedKeyValuePage(final ByteArrayDataInput in,
			final PageReadTrx pageReadTrx) {
		mRecordPageKey = getVarLong(in);
		mPersistenter = pageReadTrx.getSession().getResourceConfig().mPersistenter;
		mPageReadTrx = pageReadTrx;
		mSlots = new HashMap<>();
		final int normalEntrySize = in.readInt();
		mRecords = new LinkedHashMap<>(normalEntrySize);
		for (int index = 0; index < normalEntrySize; index++) {
			final long key = getVarLong(in);
			final int dataSize = in.readInt();
			final byte[] data = new byte[dataSize];
			in.readFully(data);
			final Record record = mPersistenter.deserialize(ByteStreams.newDataInput(data), key,
					mPageReadTrx);
			mRecords.put(key, record);
		}
		final int overlongEntrySize = in.readInt();
		mReferences = new HashMap<>(overlongEntrySize);
		for (int index = 0; index < overlongEntrySize; index++) {
			final long key = in.readLong();
			final PageReference reference = new PageReference();
			reference.setKey(in.readLong());
			mReferences.put(key, reference);
		}
		assert pageReadTrx != null : "pageReadTrx must not be null!";
		final boolean hasPreviousReference = in.readBoolean();
		if (hasPreviousReference) {
			final PageReference previousPageReference = new PageReference();
			previousPageReference.setKey(in.readLong());
			mPreviousPageReference = Optional.of(previousPageReference);
		} else {
			mPreviousPageReference = Optional.absent();
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
				if (reference != null && reference.getKey() != Constants.NULL_ID) {
					data = ((OverflowPage) mPageReadTrx.getReader().read(
							reference.getKey(), mPageReadTrx)).getData();
				} else {
					return null;
				}
			} catch (final SirixIOException e) {
				return null;
			}
			record = mPersistenter.deserialize(ByteStreams.newDataInput(data), key,
					mPageReadTrx);
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
	public void serialize(final ByteArrayDataOutput out) {
		if (!mAddedReferences) {
			addReferences();
		}
		putVarLong(out, mRecordPageKey);
		// Write normal entries.
		out.writeInt(mRecords.size());
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
		final boolean hasPreviousReference = mPreviousPageReference.isPresent();
		out.writeBoolean(hasPreviousReference);
		if (hasPreviousReference) {
			out.writeLong(mPreviousPageReference.get().getKey());
		}
		out.writeByte(mPageKind.getID());
	}

	@Override
	public String toString() {
		final ToStringHelper helper = Objects.toStringHelper(this).add("pagekey",
				mRecordPageKey);
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
			return mRecordPageKey == other.mRecordPageKey
					&& Objects.equal(mRecords, other.mRecords)
					&& Objects.equal(mReferences, other.mReferences);
		}
		return false;
	}

	@Override
	public PageReference[] getReferences() {
		throw new UnsupportedOperationException();
	}

	@Override
	public <K extends Comparable<? super K>, V extends Record, S extends KeyValuePage<K, V>> void commit(
			PageWriteTrx<K, V, S> pageWriteTrx) throws SirixException {
		if (!mAddedReferences) {
			addReferences();
		}

		for (final PageReference reference : mReferences.values()) {
			if (!(reference.getLogKey() == null && reference.getPage() == null && reference
					.getKey() == Constants.NULL_ID)) {
				pageWriteTrx.commit(reference);
			}
		}
	}

	// Add references to OverflowPages.
	private void addReferences() {
		final PeekingIterator<Entry<Long, Record>> it = Iterators
				.peekingIterator(mRecords.entrySet().iterator());
		while (it.hasNext()) {
			final Entry<Long, Record> entry = it.next();
			final Record record = entry.getValue();
			final long recordID = record.getNodeKey();
			if (mSlots.get(recordID) == null) {
				final ByteArrayDataOutput output = ByteStreams.newDataOutput();
				Entry<Long, Record> nextEntry = null;
				try {
					nextEntry = it.peek();
				} catch (final NoSuchElementException e) {
				}
				final Record nextRecord = nextEntry == null ? null : nextEntry
						.getValue();
				mPersistenter.serialize(output, record, nextRecord, mPageReadTrx);
				final byte[] data = output.toByteArray();
				if (data.length > PageConstants.MAX_RECORD_SIZE) {
					final PageReference reference = new PageReference();
					reference.setPage(new OverflowPage(data));
					mReferences.put(recordID, reference);
				} else {
					mSlots.put(recordID, data);
				}
			}
		}
		mAddedReferences = true;
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
	public boolean isDirty() {
		return mIsDirty;
	}

	@Override
	public Page setDirty(final boolean dirty) {
		mIsDirty = dirty;
		return this;
	}

	@Override
	public PageReadTrx getPageReadTrx() {
		return mPageReadTrx;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <C extends KeyValuePage<Long, Record>> C newInstance(
			final long recordPageKey, final PageKind pageKind,
			final Optional<PageReference> previousPageRef,
			final PageReadTrx pageReadTrx) {
		return (C) new UnorderedKeyValuePage(recordPageKey, pageKind,
				previousPageRef, pageReadTrx);
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
	public void setPageReference(final Long key,
			final PageReference reference) {
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
	public Optional<PageReference> getPreviousReference() {
		return mPreviousPageReference;
	}

}
