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

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.base.Objects;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.PageWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.index.name.Names;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.Record;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;

/**
 * <h1>NamePage</h1>
 * 
 * <p>
 * Name page holds all names and their keys for a revision.
 * </p>
 */
public final class NamePage implements Page {

	/** Attribute names. */
	private final Names mAttributes;

	/** Element names. */
	private final Names mElements;

	/** Namespace URIs. */
	private final Names mNamespaces;

	/** Processing instruction names. */
	private final Names mPIs;

	/** Revision number. */
	private final int mRevision;

	/** Determine if name page has been modified. */
	private boolean mIsDirty;

	/**
	 * Create name page.
	 * 
	 * @param revision
	 *          revision number
	 */
	public NamePage(final @Nonnegative int revision) {
		checkArgument(revision >= 0, "pRevision must be >= 0!");
		mRevision = revision;
		mAttributes = Names.getInstance();
		mElements = Names.getInstance();
		mNamespaces = Names.getInstance();
		mPIs = Names.getInstance();
		mIsDirty = true;
	}

	/**
	 * Read name page.
	 * 
	 * @param in
	 *          input bytes to read from
	 */
	protected NamePage(final @Nonnull ByteArrayDataInput in) {
		mRevision = in.readInt();
		mElements = Names.clone(in);
		mNamespaces = Names.clone(in);
		mAttributes = Names.clone(in);
		mPIs = Names.clone(in);
	}

	/**
	 * Get raw name belonging to name key.
	 * 
	 * @param key
	 *          name key identifying name
	 * @return raw name of name key
	 */
	public byte[] getRawName(final int key, final @Nonnull Kind nodeKind) {
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
	 * @param key
	 *          name key identifying name
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
	 * @param key
	 *          name key identifying name
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
	 * @param key
	 *          key for given name
	 * @param name
	 *          name to create key for
	 * @param pNodeKind
	 *          kind of node
	 */
	public void setName(final int key, final @Nonnull String name,
			final @Nonnull Kind pNodeKind) {
		switch (pNodeKind) {
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
	public void serialize(final @Nonnull ByteArrayDataOutput out) {
		out.writeInt(mRevision);
		mElements.serialize(out);
		mNamespaces.serialize(out);
		mAttributes.serialize(out);
		mPIs.serialize(out);
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("revision", mRevision)
				.add("elements", mElements).add("attributes", mAttributes)
				.add("URIs", mNamespaces).add("PIs", mPIs).toString();
	}

	/**
	 * Remove an attribute-name.
	 * 
	 * @param key
	 *          the key to remove
	 */
	public void removeName(final int key, final @Nonnull Kind nodeKind) {
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

	@Override
	public int getRevision() {
		return mRevision;
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
	public Page setDirty(final boolean pDirty) {
		mIsDirty = pDirty;
		return this;
	}
}
