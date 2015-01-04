package org.sirix.index.bplustree;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import javax.annotation.Nonnegative;
import javax.annotation.Nullable;

import org.sirix.api.PageReadTrx;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.RecordPersistenter;
import org.sirix.page.AbstractForwardingPage;
import org.sirix.page.PageKind;
import org.sirix.page.PageReference;
import org.sirix.page.delegates.PageDelegate;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;

import com.google.common.io.ByteArrayDataInput;

/**
 * Inner node key/value page.
 * 
 * @author Johannes Lichtenberger
 * 
 * @param <K>
 *          the key
 * @param <V>
 *          the value
 */
public class BPlusInnerNodePage<K extends Comparable<? super K> & Record, V extends Record>
		extends AbstractForwardingPage implements KeyValuePage<K, V> {

	/** Key of record page. This is the base key of all contained nodes. */
	private final long mRecordPageKey;

	/** Key/Value records. */
	private final Map<K, V> mRecords;

	/** Determine if node page has been modified. */
	private boolean mIsDirty;

	/** Sirix {@link PageReadTrx}. */
	private final PageReadTrx mPageReadTrx;

	/** Optional left page reference (leaf page). */
	private Optional<PageReference> mLeftPage;

	/** Optional right page reference (inner node page). */
	private Optional<PageReference> mRightPage;

	private final PageDelegate mDelegate;

	private final PageKind mPageKind;

	/** Determines the node kind. */
	public enum Kind {
		/** Leaf node. */
		LEAF,

		/** Inner node. */
		INNERNODE
	}

	/**
	 * Create record page.
	 * 
	 * @param recordPageKey
	 *          base key assigned to this node page
	 * @param pageReadTrx
	 *          Sirix page reading transaction
	 * @param kind
	 *          determines if it's a leaf or inner node page
	 */
	public BPlusInnerNodePage(final @Nonnegative long recordPageKey,
			final PageKind pageKind, final Optional<PageReference> previousPageRef,
			final PageReadTrx pageReadTrx) {
		// Assertions instead of checkNotNull(...) checks as it's part of the
		// internal flow.
		assert recordPageKey >= 0 : "recordPageKey must not be negative!";
		assert pageKind != null;
		assert pageReadTrx != null : "pageReadTrx must not be null!";
		mRecordPageKey = recordPageKey;
		mRecords = new TreeMap<>();
		mIsDirty = true;
		mPageReadTrx = pageReadTrx;
		mDelegate = new PageDelegate(Constants.INP_REFERENCE_COUNT);
		mPageKind = pageKind;
	}

	/**
	 * Read node page.
	 * 
	 * @param in
	 *          input bytes to read page from
	 * @param pageReadTrx
	 *          {@link 
	 */
	protected BPlusInnerNodePage(final ByteArrayDataInput in,
			final PageReadTrx pageReadTrx) {
		mDelegate = null;
		// mDelegate = new PageDelegate(Constants.INP_REFERENCE_COUNT, in);
		mRecordPageKey = in.readLong();
		final int size = in.readInt();
		mRecords = new TreeMap<>();
		final RecordPersistenter persistenter = pageReadTrx.getSession()
				.getResourceConfig().mPersistenter;
		for (int offset = 0; offset < size; offset++) {
			// Must be the key which has been serialized.
			// @SuppressWarnings("unchecked")
			// final K key = (K) persistenter.deserialize(in, pageReadTrx);
			// Inner nodes do not have values.
			// @SuppressWarnings("unchecked")
			final V value = (V) new VoidValue();
			// mRecords.put(key, value);
		}
		assert pageReadTrx != null : "pageReadTrx must not be null!";
		mPageReadTrx = pageReadTrx;
		mPageKind = PageKind.getKind(in.readByte());
	}

	public void setLeftPage(final Optional<PageReference> leftPage) {
		mLeftPage = leftPage;
	}

	public void setRightPage(final Optional<PageReference> rightPage) {
		mLeftPage = rightPage;
	}

	@Override
	public void serialize(final DataOutput out) throws IOException {
		super.serialize(out);
		out.writeLong(mRecordPageKey);
		out.writeInt(mRecords.size());
		serializePointer(mLeftPage, out);
		serializePointer(mRightPage, out);
		final RecordPersistenter persistenter = mPageReadTrx.getSession()
				.getResourceConfig().mPersistenter;
		// for (final K record : mRecords.keySet()) {
		// persistenter.serialize(out, record, mPageReadTrx);
		// }
		out.writeByte(mPageKind.getID());
	}

	private void serializePointer(final Optional<PageReference> page,
			final DataOutput out) throws IOException {
		if (page.isPresent()) {
			out.writeBoolean(page.get().getKey() == org.sirix.settings.Constants.NULL_ID ? false
					: true);
		} else {
			out.writeBoolean(false);
		}
	}

	@Override
	public Page setDirty(final boolean dirty) {
		mIsDirty = dirty;
		return this;
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		return mRecords.entrySet();
	}

	@Override
	public Collection<V> values() {
		return mRecords.values();
	}

	@Override
	public long getPageKey() {
		return mRecordPageKey;
	}

	@Override
	public V getValue(final K key) {
		return mRecords.get(key);
	}

	@Override
	public void setEntry(final K key, final @Nullable V value) {
		mRecords.put(key, value);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <C extends KeyValuePage<K, V>> C newInstance(
			final @Nonnegative long recordPageKey, final PageKind pageKind,
			final Optional<PageReference> previousPageRef,
			final PageReadTrx pageReadTrx) {
		return (C) new BPlusInnerNodePage<K, V>(recordPageKey, pageKind,
				previousPageRef, pageReadTrx);
	}

	@Override
	public PageReadTrx getPageReadTrx() {
		return mPageReadTrx;
	}

	@Override
	protected Page delegate() {
		return mDelegate;
	}

	@Override
	public PageKind getPageKind() {
		return mPageKind;
	}

	@Override
	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Set<Entry<K, PageReference>> referenceEntrySet() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setPageReference(K key, PageReference reference) {
		// TODO Auto-generated method stub

	}

	@Override
	public PageReference getPageReference(K key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Optional<PageReference> getPreviousReference() {
		// TODO Auto-generated method stub
		return null;
	}
}
