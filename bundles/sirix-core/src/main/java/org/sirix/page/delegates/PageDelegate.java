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

package org.sirix.page.delegates;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import javax.annotation.Nonnegative;

import org.sirix.api.PageWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.node.interfaces.Record;
import org.sirix.page.PageReference;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;

import com.google.common.base.Objects;

/**
 * <h1>PageDelegate</h1>
 * 
 * <p>
 * Class to provide basic reference handling functionality.
 * </p>
 */
public class PageDelegate implements Page {

	/** Page references. */
	private PageReference[] mReferences;

	/** Determines if page is new or changed. */
	private boolean mIsDirty;

	/**
	 * Constructor to initialize instance.
	 * 
	 * @param referenceCount
	 *          number of references of page
	 * @param revision
	 *          revision number
	 */
	public PageDelegate(final @Nonnegative int referenceCount) {
		checkArgument(referenceCount >= 0);
		mReferences = new PageReference[referenceCount];
		mIsDirty = true;
		for (int i = 0; i < referenceCount; i++) {
			mReferences[i] = new PageReference();
		}
	}

	/**
	 * Constructor to initialize instance.
	 * 
	 * @param referenceCount
	 *          number of references of page
	 * @param in
	 *          input stream to read from
	 * @throws IOException
	 *           if the delegate couldn't be deserialized
	 */
	public PageDelegate(final @Nonnegative int referenceCount,
			final DataInputStream in) throws IOException {
		checkArgument(referenceCount >= 0);
		mReferences = new PageReference[referenceCount];
		mIsDirty = false;
		for (int offset = 0; offset < mReferences.length; offset++) {
			mReferences[offset] = new PageReference();
			mReferences[offset].setKey(in.readLong());
		}
	}

	/**
	 * Constructor to initialize instance.
	 * 
	 * @param commitedPage
	 *          commited page
	 * @param revision
	 *          revision number
	 */
	public PageDelegate(final Page commitedPage) {
		mReferences = commitedPage.getReferences();
		mIsDirty = true;
	}

	/**
	 * Get page reference of given offset.
	 * 
	 * @param offset
	 *          offset of page reference
	 * @return {@link PageReference} at given offset
	 */
	public final PageReference getReference(final @Nonnegative int offset) {
		if (mReferences[offset] == null) {
			mReferences[offset] = new PageReference();
		}
		return mReferences[offset];
	}

	/**
	 * Recursively call commit on all referenced pages.
	 * 
	 * @param pState
	 *          IWriteTransaction state
	 * @throws SirixException
	 *           if a write-error occured
	 */
	@Override
	public final <K extends Comparable<? super K>, V extends Record, S extends KeyValuePage<K, V>> void commit(
			final PageWriteTrx<K, V, S> pageWriteTrx) throws SirixException {
		for (final PageReference reference : mReferences) {
			if (!(reference.getLogKey() == null && reference.getPage() == null && reference
					.getKey() == Constants.NULL_ID)) {
				pageWriteTrx.commit(reference);
			}
		}
	}

	/**
	 * Serialize page references into output.
	 * 
	 * @param out
	 *          output stream
	 */
	@Override
	public void serialize(final DataOutputStream out) throws IOException {
		for (final PageReference reference : mReferences) {
			out.writeLong(reference.getKey());
		}
	}

	/**
	 * Get all references.
	 * 
	 * @return copied references
	 */
	@Override
	public final PageReference[] getReferences() {
		// final PageReference[] copiedRefs = new PageReference[mReferences.length];
		// System.arraycopy(mReferences, 0, copiedRefs, 0, mReferences.length);
		// return copiedRefs;
		return mReferences;
	}

	@Override
	public String toString() {
		final Objects.ToStringHelper helper = Objects.toStringHelper(this);
		for (final PageReference ref : mReferences) {
			helper.add("reference", ref);
		}
		return helper.toString();
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

}
