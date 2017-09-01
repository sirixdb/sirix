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

package org.sirix.page.delegates;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import javax.annotation.Nonnegative;

import org.sirix.api.PageWriteTrx;
import org.sirix.node.interfaces.Record;
import org.sirix.page.PageReference;
import org.sirix.page.SerializationType;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;

import com.google.common.base.MoreObjects;

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

	/**
	 * Constructor to initialize instance.
	 *
	 * @param referenceCount number of references of page
	 * @param revision revision number
	 */
	public PageDelegate(final @Nonnegative int referenceCount) {
		checkArgument(referenceCount >= 0);
		mReferences = new PageReference[referenceCount];
		for (int i = 0; i < referenceCount; i++) {
			mReferences[i] = new PageReference();
		}
	}

	/**
	 * Constructor to initialize instance.
	 *
	 * @param referenceCount number of references of page
	 * @param in input stream to read from
	 * @throws IOException if the delegate couldn't be deserialized
	 */
	public PageDelegate(final @Nonnegative int referenceCount, final DataInput in,
			final SerializationType type) throws IOException {
		checkArgument(referenceCount >= 0);
		mReferences = new PageReference[referenceCount];

		for (int offset = 0, length = mReferences.length; offset < length; offset++) {
			mReferences[offset] = new PageReference();

			switch (type) {
				case COMMIT:
					final boolean hasKey = in.readBoolean();
					if (hasKey) {
						final long key = in.readLong();
						mReferences[offset].setKey(key);
					}
					break;
				case TRANSACTION_INTENT_LOG:
					final boolean hasLogKey = in.readBoolean();
					if (hasLogKey) {
						final int key = in.readInt();
						mReferences[offset].setLogKey(key);
					}
					break;
			}
		}
	}

	/**
	 * Constructor to initialize instance.
	 *
	 * @param commitedPage commited page
	 * @param revision revision number
	 */
	public PageDelegate(final Page commitedPage) {
		mReferences = Arrays.copyOf(commitedPage.getReferences(), commitedPage.getReferences().length);

		for (int offset = 0, length = mReferences.length; offset < length; offset++) {
			mReferences[offset] = new PageReference();
			mReferences[offset].setKey(commitedPage.getReferences()[offset].getKey());
		}
	}

	/**
	 * Get page reference of given offset.
	 *
	 * @param offset offset of page reference
	 * @return {@link PageReference} at given offset
	 */
	@Override
	public final PageReference getReference(final @Nonnegative int offset) {
		if (mReferences[offset] == null) {
			mReferences[offset] = new PageReference();
		}
		return mReferences[offset];
	}

	/**
	 * Recursively call commit on all referenced pages.
	 *
	 * @param pageWriteTransaction the page write transaction
	 */
	@Override
	public final <K extends Comparable<? super K>, V extends Record, S extends KeyValuePage<K, V>> void commit(
			final PageWriteTrx<K, V, S> pageWriteTrx) {
		for (final PageReference reference : mReferences) {
			if (!(reference.getLogKey() == Constants.NULL_ID_INT
					&& reference.getPersistentLogKey() == Constants.NULL_ID_LONG)) {
				pageWriteTrx.commit(reference);
			}
		}
	}

	/**
	 * Serialize page references into output.
	 *
	 * @param out output stream
	 */
	@Override
	public void serialize(final DataOutput out, final SerializationType type) throws IOException {
		for (final PageReference reference : mReferences) {
			switch (type) {
				case COMMIT:
					out.writeBoolean(reference.getKey() != Constants.NULL_ID_LONG);
					if (reference.getKey() != Constants.NULL_ID_LONG)
						out.writeLong(reference.getKey());
					break;
				case TRANSACTION_INTENT_LOG:
					out.writeBoolean(reference.getLogKey() != Constants.NULL_ID_INT);
					if (reference.getLogKey() != Constants.NULL_ID_INT)
						out.writeInt(reference.getLogKey());
					break;
			}
		}
	}

	/**
	 * Get all references.
	 *
	 * @return copied references
	 */
	@Override
	public final PageReference[] getReferences() {
		return mReferences;
	}

	@Override
	public String toString() {
		final MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
		for (final PageReference ref : mReferences) {
			helper.add("reference", ref);
		}
		return helper.toString();
	}
}
