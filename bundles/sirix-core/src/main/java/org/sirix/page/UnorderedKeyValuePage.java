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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.RecordPersistenter;
import org.sirix.page.delegates.PageDelegate;
import org.sirix.page.interfaces.Page;
import org.sirix.page.interfaces.KeyValuePage;

import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;

/**
 * <h1>UnorderedKeyValuePage</h1>
 * 
 * <p>
 * An UnorderedKeyValuePage stores a set of records, commonly nodes in an
 * unordered datastructure.
 * </p>
 */
public final class UnorderedKeyValuePage implements KeyValuePage<Long, Record> {

	/** Key of record page. This is the base key of all contained nodes. */
	private final long mRecordPageKey;

	/** Records. */
	private final Map<Long, Record> mRecords;

	/** {@link PageDelegate} reference. */
	private final int mRevision;

	/** Determine if node page has been modified. */
	private boolean mIsDirty;

	/** {@link PageReadTrx} instance. */
	private final PageReadTrx mPageReadTrx;

	/**
	 * Create record page.
	 * 
	 * @param recordPageKey
	 *          base key assigned to this node page
	 * @param revision
	 *          revision the page belongs to
	 */
	public UnorderedKeyValuePage(final @Nonnegative long recordPageKey,
			final @Nonnegative int revision, final @Nonnull PageReadTrx pageReadTrx) {
		// Assertions instead of checkNotNull(...) checks as it's part of the
		// internal flow.
		assert recordPageKey >= 0 : "recordPageKey must not be negative!";
		assert revision >= 0 : "revision must not be negative!";
		assert pageReadTrx != null : "pageReadTrx must not be null!";
		mRevision = revision;
		mRecordPageKey = recordPageKey;
		mRecords = new HashMap<>();
		mIsDirty = true;
		mPageReadTrx = pageReadTrx;
	}

	/**
	 * Read node page.
	 * 
	 * @param in
	 *          input bytes to read page from
	 * @param pageReadTrx
	 *          {@link 
	 */
	protected UnorderedKeyValuePage(final @Nonnull ByteArrayDataInput in,
			final @Nonnull PageReadTrx pageReadTrx) {
		mRevision = in.readInt();
		mRecordPageKey = in.readLong();
		final int size = in.readInt();
		mRecords = new HashMap<>(size);
		final RecordPersistenter persistenter = pageReadTrx.getSession()
				.getResourceConfig().mPersistenter;
		for (int offset = 0; offset < size; offset++) {
			final Record node = persistenter.deserialize(in, pageReadTrx);
			mRecords.put(node.getNodeKey(), node);
//			if (node.getKind() == Kind.AVL) {
//				offset += 2;
//			}
//			offset++;
		}
		assert pageReadTrx != null : "pageReadTrx must not be null!";
		mPageReadTrx = pageReadTrx;
	}

	@Override
	public long getRecordPageKey() {
		return mRecordPageKey;
	}

	@Override
	public Record getRecord(final @Nonnegative Long key) {
		assert key != null : "key must not be null!";
		assert key >= 0 : "key must not be negative!";
		return mRecords.get(key);
	}

	@Override
	public void setRecord(final @Nonnull Record record) {
		assert record != null : "record must not be null!";
		mRecords.put(record.getNodeKey(), record);
	}

	@Override
	public void serialize(final @Nonnull ByteArrayDataOutput out) {
		out.writeInt(mRevision);
		out.writeLong(mRecordPageKey);
		int size = 0;
		for (final Record node : mRecords.values()) {
			if (node.getKind() != Kind.ATTRIBUTE_VALUE
					&& node.getKind() != Kind.TEXT_VALUE
					&& node.getKind() != Kind.TEXT_REFERENCES) {
				size++;
			}
		}
		out.writeInt(size);
		final RecordPersistenter persistenter = mPageReadTrx.getSession()
				.getResourceConfig().mPersistenter;
		for (final Record node : mRecords.values()) {
			if (node.getKind() != Kind.ATTRIBUTE_VALUE
					&& node.getKind() != Kind.TEXT_VALUE
					&& node.getKind() != Kind.TEXT_REFERENCES) {
				persistenter.serialize(out, node, mPageReadTrx);
			}
		}
	}

	@Override
	public String toString() {
		final ToStringHelper helper = Objects.toStringHelper(this)
				.add("revision", mRevision).add("pagekey", mRecordPageKey)
				.add("nodes", mRecords.toString());
		for (final Record node : mRecords.values()) {
			helper.add("node", node);
		}
		return helper.toString();
	}

	@Override
	public Set<Entry<Long, Record>> entrySet() {
		return mRecords.entrySet();
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mRecordPageKey, mRecords);
	}

	@Override
	public boolean equals(final @Nullable Object obj) {
		if (obj instanceof UnorderedKeyValuePage) {
			final UnorderedKeyValuePage other = (UnorderedKeyValuePage) obj;
			return Objects.equal(mRecordPageKey, other.mRecordPageKey)
					&& Objects.equal(mRecords, other.mRecords);
		}
		return false;
	}

	@Override
	public int getRevision() {
		return mRevision;
	}

	@Override
	public PageReference[] getReferences() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void commit(final @Nonnull PageWriteTrx pPageWriteTrx)
			throws SirixException {
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
	public Page setDirty(final boolean pDirty) {
		mIsDirty = pDirty;
		return this;
	}

	@Override
	public PageReadTrx getPageReadTrx() {
		return mPageReadTrx;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <C extends KeyValuePage<Long, Record>> C newInstance(
			final long recordPageKey, final @Nonnegative int revision,
			final @Nonnull PageReadTrx pageReadTrx) {
		return (C) new UnorderedKeyValuePage(recordPageKey, revision, pageReadTrx);
	}
}
