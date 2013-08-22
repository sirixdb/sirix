package org.sirix.page;

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
import java.io.DataInputStream;
import java.io.DataOutputStream;
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

import com.google.common.base.Objects;

/**
 * Page to hold references to a content and value summary.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class CASPage extends AbstractForwardingPage {

	/** {@link PageDelegate} instance. */
	private final PageDelegate mDelegate;
	
	/** Maximum node keys. */
	private final Map<Integer, Long> mMaxNodeKeys;

	/**
	 * Constructor.
	 */
	public CASPage() {
		mDelegate = new PageDelegate(PageConstants.MAX_INDEX_NR);
		mMaxNodeKeys = new HashMap<>();
	}

	/**
	 * Get indirect page reference.
	 * 
	 * @param index
	 *          the offset of the indirect page, that is the index number
	 * @return indirect page reference
	 */
	public PageReference getIndirectPageReference(int index) {
		return getReference(index);
	}

	/**
	 * Read meta page.
	 * 
	 * @param in
	 *          input bytes to read from
	 */
	protected CASPage(final DataInputStream in) throws IOException {
		mDelegate = new PageDelegate(PageConstants.MAX_INDEX_NR, in);
		final int size = in.readInt();
		mMaxNodeKeys = new HashMap<>(size);
		for (int i = 0; i < size; i ++) {
			mMaxNodeKeys.put(i, in.readLong());
		}
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("mDelegate", mDelegate).toString();
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
	 * Initialize CAS index tree.
	 * 
	 * @param pageReadTrx
	 *          {@link PageReadTrx} instance
	 * @param index
	 *          the index number
	 */
	public <K extends Comparable<? super K>, V extends Record, S extends KeyValuePage<K, V>> void createCASIndexTree(
			final PageWriteTrx<K, V, S> pageWriteTrx, final int index) {
		final PageReference reference = getReference(index);
		if (reference.getPage() == null && reference.getLogKey() == null
				&& reference.getKey() == Constants.NULL_ID) {
			PageUtils.createTree(reference, PageKind.CASPAGE, index, pageWriteTrx);
			if (mMaxNodeKeys.get(index) == null) {
				mMaxNodeKeys.put(index, 0l);
			} else {
				mMaxNodeKeys.put(index, mMaxNodeKeys.get(index).longValue() + 1);
			}
		}
	}
	
	@Override
	public void serialize(DataOutputStream out) throws IOException {
		super.serialize(out);
		final int size = mMaxNodeKeys.size();
		out.writeInt(size);
		for (int i = 0; i < size; i ++) {
			out.writeLong(mMaxNodeKeys.get(i));
		}
	}
	
	/**
	 * Get the maximum node key of the specified index by its index number.
	 * 
	 * @param indexNo
	 *          the index number
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
