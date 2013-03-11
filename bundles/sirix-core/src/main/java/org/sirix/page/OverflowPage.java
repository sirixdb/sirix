package org.sirix.page;

import javax.annotation.Nonnull;

import org.sirix.api.PageWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.node.interfaces.Record;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;

/**
 * OverflowPage used to store records which are longer than a predefined
 * threshold.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public final class OverflowPage implements Page {

	/** Determines if page has been created/modified or not. */
	private boolean mIsDirty;

	/** Data to be stored. */
	private final byte[] mData;

	public OverflowPage() {
		mData = new byte[0];
	}
	
	/**
	 * Constructor.
	 * 
	 * @param data
	 *          data to be stored
	 */
	public OverflowPage(final @Nonnull byte[] data) {
		assert data != null;
		mIsDirty = true;
		mData = data;
	}
	
	public OverflowPage(final @Nonnull ByteArrayDataInput in) {
		mData = new byte[in.readInt()];
		in.readFully(mData);
	}

	@Override
	public PageReference[] getReferences() {
		throw new UnsupportedOperationException();
	}

	@Override
	public <K extends Comparable<? super K>, V extends Record, S extends KeyValuePage<K, V>> void commit(
			PageWriteTrx<K, V, S> pageWriteTrx) throws SirixException {
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
	public void serialize(final @Nonnull ByteArrayDataOutput out) {
		out.writeInt(mData.length);
		out.write(mData);
	}

	public byte[] getData() {
		return mData;
	}
}
