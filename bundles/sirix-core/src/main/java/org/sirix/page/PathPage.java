package org.sirix.page;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.node.interfaces.Record;
import org.sirix.page.delegates.PageDelegate;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;

import com.google.common.base.MoreObjects;

/**
 * Page to hold references to a value summary.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class PathPage extends AbstractForwardingPage {

	/** {@link PageDelegate} instance. */
	private final PageDelegate mDelegate;

	/** Maximum node keys. */
	private final Map<Integer, Long> mMaxNodeKeys;

	/**
	 * Constructor.
	 */
	public PathPage() {
		mDelegate = new PageDelegate(PageConstants.MAX_INDEX_NR);
		mMaxNodeKeys = new HashMap<>();
	}

	/**
	 * Get indirect page reference.
	 *
	 * @param offset the offset of the indirect page, that is the index number
	 * @return indirect page reference
	 */
	public PageReference getIndirectPageReference(int offset) {
		return getReference(offset);
	}

	/**
	 * Read meta page.
	 *
	 * @param in input bytes to read from
	 * @throws IOException if the page couldn't be deserialized
	 */
	protected PathPage(final DataInput in) throws IOException {
		mDelegate = new PageDelegate(PageConstants.MAX_INDEX_NR, in);
		final int size = in.readInt();
		mMaxNodeKeys = new HashMap<>(size);
		for (int i = 0; i < size; i++) {
			mMaxNodeKeys.put(i, in.readLong());
		}
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this).add("mDelegate", mDelegate).toString();
	}

	@Override
	protected Page delegate() {
		return mDelegate;
	}

	@Override
	public Page setDirty(final boolean pDirty) {
		mDelegate.setDirty(pDirty);
		return this;
	}

	/**
	 * Initialize path index tree.
	 *
	 * @param pageReadTrx {@link PageReadTrx} instance
	 * @param index the index number
	 */
	public <K extends Comparable<? super K>, V extends Record, S extends KeyValuePage<K, V>> void createPathIndexTree(
			final PageWriteTrx<K, V, S> pageWriteTrx, final int index) {
		final PageReference reference = getReference(index);
		if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID
				&& reference.getLogKey() == Constants.NULL_ID) {
			PageUtils.createTree(reference, PageKind.PATHPAGE, index, pageWriteTrx);
			if (mMaxNodeKeys.get(index) == null) {
				mMaxNodeKeys.put(index, 0l);
			} else {
				mMaxNodeKeys.put(index, mMaxNodeKeys.get(index).longValue() + 1);
			}
		}
	}

	@Override
	public void serialize(final DataOutput out) throws IOException {
		super.serialize(out);
		final int size = mMaxNodeKeys.size();
		out.writeInt(size);
		for (int i = 0; i < size; i++) {
			out.writeLong(mMaxNodeKeys.get(i));
		}
	}

	/**
	 * Get the maximum node key of the specified index by its index number.
	 *
	 * @param indexNo the index number
	 * @return the maximum node key stored
	 */
	public long getMaxNodeKey(final int indexNo) {
		return mMaxNodeKeys.get(indexNo);
	}

	public long incrementAndGetMaxNodeKey(final int indexNo) {
		final long newMaxNodeKey = mMaxNodeKeys.get(indexNo).longValue() + 1;
		mMaxNodeKeys.put(indexNo, newMaxNodeKey);
		return newMaxNodeKey;
	}
}
