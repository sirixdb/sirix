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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.index.name.Names;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.Record;
import org.sirix.page.delegates.PageDelegate;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;

import com.google.common.base.MoreObjects;

/**
 * <h1>NamePage</h1>
 *
 * <p>
 * Name page holds all names and their keys for a revision.
 * </p>
 */
public final class NamePage extends AbstractForwardingPage {

	/** Attribute names. */
	private final Names mAttributes;

	/** Element names. */
	private final Names mElements;

	/** Namespace URIs. */
	private final Names mNamespaces;

	/** Processing instruction names. */
	private final Names mPIs;

	/** {@link PageDelegate} instance. */
	private final PageDelegate mDelegate;

	/** Maximum node keys. */
	private final Map<Integer, Long> mMaxNodeKeys;

	/**
	 * Create name page.
	 */
	public NamePage() {
		mDelegate = new PageDelegate(PageConstants.MAX_INDEX_NR);
		mMaxNodeKeys = new HashMap<>();
		mAttributes = Names.getInstance();
		mElements = Names.getInstance();
		mNamespaces = Names.getInstance();
		mPIs = Names.getInstance();
	}

	/**
	 * Read name page.
	 *
	 * @param in input bytes to read from
	 */
	protected NamePage(final DataInput in, final SerializationType type) throws IOException {
		mDelegate = new PageDelegate(PageConstants.MAX_INDEX_NR, in, type);
		final int size = in.readInt();
		mMaxNodeKeys = new HashMap<>(size);
		for (int i = 0; i < size; i++) {
			mMaxNodeKeys.put(i, in.readLong());
		}
		mElements = Names.clone(in);
		mNamespaces = Names.clone(in);
		mAttributes = Names.clone(in);
		mPIs = Names.clone(in);
	}

	/**
	 * Get raw name belonging to name key.
	 *
	 * @param key name key identifying name
	 * @return raw name of name key
	 */
	public byte[] getRawName(final int key, final Kind nodeKind) {
		byte[] rawName = new byte[] {};
		switch (nodeKind) {
			case ELEMENT:
				rawName = mElements.getRawName(key);
				break;
			case NAMESPACE:
				rawName = mNamespaces.getRawName(key);
				break;
			case ATTRIBUTE:
				rawName = mAttributes.getRawName(key);
				break;
			case PROCESSING_INSTRUCTION:
				rawName = mPIs.getRawName(key);
				break;
			default:
				throw new IllegalStateException("No other node types supported!");
		}
		return rawName;
	}

	/**
	 * Get raw name belonging to name key.
	 *
	 * @param key name key identifying name
	 * @return raw name of name key, or {@code null} if not present
	 */
	public String getName(final int key, @Nonnull final Kind nodeKind) {
		String name;
		switch (nodeKind) {
			case ELEMENT:
				name = mElements.getName(key);
				break;
			case NAMESPACE:
				name = mNamespaces.getName(key);
				break;
			case ATTRIBUTE:
				name = mAttributes.getName(key);
				break;
			case PROCESSING_INSTRUCTION:
				name = mPIs.getName(key);
				break;
			default:
				throw new IllegalStateException("No other node types supported!");
		}
		return name;
	}

	/**
	 * Get number of nodes with the given name key.
	 *
	 * @param key name key identifying name
	 * @return number of nodes with the given name key
	 */
	public int getCount(final int key, @Nonnull final Kind nodeKind) {
		int count;
		switch (nodeKind) {
			case ELEMENT:
				count = mElements.getCount(key);
				break;
			case NAMESPACE:
				count = mNamespaces.getCount(key);
				break;
			case ATTRIBUTE:
				count = mAttributes.getCount(key);
				break;
			case PROCESSING_INSTRUCTION:
				count = mPIs.getCount(key);
				break;
			default:
				throw new IllegalStateException("No other node types supported!");
		}
		return count;
	}

	/**
	 * Create name key given a name.
	 *
	 * @param key key for given name
	 * @param name name to create key for
	 * @param nodeKind kind of node
	 */
	public void setName(final int key, final String name, final Kind nodeKind) {
		switch (nodeKind) {
			case ELEMENT:
				mElements.setName(key, name);
				break;
			case NAMESPACE:
				mNamespaces.setName(key, name);
				break;
			case ATTRIBUTE:
				mAttributes.setName(key, name);
				break;
			case PROCESSING_INSTRUCTION:
				mPIs.setName(key, name);
				break;
			default:
				throw new IllegalStateException("No other node types supported!");
		}
	}

	@Override
	public void serialize(final DataOutput out, final SerializationType type) throws IOException {
		super.serialize(out, type);
		final int size = mMaxNodeKeys.size();
		out.writeInt(size);
		for (int i = 0; i < size; i++) {
			final long keys = mMaxNodeKeys.get(i);
			out.writeLong(keys);
		}
		mElements.serialize(out);
		mNamespaces.serialize(out);
		mAttributes.serialize(out);
		mPIs.serialize(out);
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this).add("elements", mElements)
				.add("attributes", mAttributes).add("URIs", mNamespaces).add("PIs", mPIs).toString();
	}

	/**
	 * Remove an attribute-name.
	 *
	 * @param key the key to remove
	 */
	public void removeName(final int key, final Kind nodeKind) {
		switch (nodeKind) {
			case ELEMENT:
				mElements.removeName(key);
				break;
			case NAMESPACE:
				mNamespaces.removeName(key);
				break;
			case ATTRIBUTE:
				mAttributes.removeName(key);
				break;
			case PROCESSING_INSTRUCTION:
				mPIs.removeName(key);
				break;
			default:
				throw new IllegalStateException("No other node types supported!");
		}
	}

	/**
	 * Initialize name index tree.
	 *
	 * @param pageReadTrx {@link PageReadTrx} instance
	 * @param index the index number
	 */
	public <K extends Comparable<? super K>, V extends Record, S extends KeyValuePage<K, V>> void createNameIndexTree(
			final PageWriteTrx<K, V, S> pageWriteTrx, final int index) {
		final PageReference reference = getReference(index);
		if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
				&& reference.getLogKey() == Constants.NULL_ID_INT
				&& reference.getPersistentLogKey() == Constants.NULL_ID_LONG) {
			PageUtils.createTree(reference, PageKind.NAMEPAGE, index, pageWriteTrx);
			if (mMaxNodeKeys.get(index) == null) {
				mMaxNodeKeys.put(index, 0l);
			} else {
				mMaxNodeKeys.put(index, mMaxNodeKeys.get(index).longValue() + 1);
			}
		}
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

	@Override
	protected Page delegate() {
		return mDelegate;
	}
}
