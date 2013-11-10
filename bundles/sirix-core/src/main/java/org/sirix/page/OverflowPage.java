package org.sirix.page;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

import org.sirix.api.PageWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.node.interfaces.Record;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;

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
	public OverflowPage(final byte[] data) {
		assert data != null;
		mIsDirty = true;
		mData = data;
	}
	
	public OverflowPage(final DataInputStream in) throws IOException {
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
	public void serialize(final DataOutput out) throws IOException {
		out.writeInt(mData.length);
		out.write(mData);
	}

	public byte[] getData() {
		return mData;
	}
}
