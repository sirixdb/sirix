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

import com.google.common.base.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;

/**
 * <h1>PageReference</h1>
 * 
 * <p>
 * Page reference pointing to a page. This might be on stable storage pointing
 * to the start byte in a file, including the length in bytes, and the checksum
 * of the serialized page. Or it might be an immediate reference to an in-memory
 * instance of the deserialized page.
 * </p>
 */
public final class PageReference {

	/** Page kind. */
	private PageKind mPageKind;

	/** In-memory deserialized page instance. */
	private Page mPage;

	/** Corresponding mKey of the related node page. */
	private long mNodePageKey = -1;

	/** Key in persistent storage. */
	private long mKey = Constants.NULL_ID;;

	/**
	 * Default constructor setting up an uninitialized page reference.
	 */
	public PageReference() {
	}

	/**
	 * Copy constructor.
	 * 
	 * @param reference
	 *          {@link PageReference} to copy
	 */
	public PageReference(final @Nonnull PageReference reference) {
		mPageKind = reference.mPageKind;
		mPage = reference.mPage;
		mNodePageKey = reference.mNodePageKey;
		mKey = reference.mKey;
	}

	/**
	 * Get in-memory instance of deserialized page.
	 * 
	 * @return in-memory instance of deserialized page
	 */
	public Page getPage() {
		return mPage;
	}

	/**
	 * Set in-memory instance of deserialized page.
	 * 
	 * @param page
	 *          deserialized page
	 */
	public void setPage(final @Nullable Page page) {
		mPage = page;
	}

	/**
	 * Get start byte offset in file.
	 * 
	 * @return start offset in file
	 */
	public long getKey() {
		return mKey;
	}

	/**
	 * Set start byte offset in file.
	 * 
	 * @param key
	 *          key of this reference set by the persistent storage
	 */
	public void setKey(final long key) {
		mKey = key;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("nodePageKey", mNodePageKey)
				.add("key", mKey).add("page", mPage).toString();
	}

	/**
	 * Set nodepage key.
	 * 
	 * @param nodePageKey
	 *          the nodePageKey to set
	 */
	public void setNodePageKey(final long nodePageKey) {
		mNodePageKey = nodePageKey;
	}

	/**
	 * Get nodepage key.
	 * 
	 * @return the nodePageKey
	 */
	public long getNodePageKey() {
		return mNodePageKey;
	}

	/**
	 * Set the kind of page to which is referenced.
	 * 
	 * @param pageKind
	 *          the page kind
	 */
	public void setPageKind(final @Nonnull PageKind pageKind) {
		assert pageKind != null;
		mPageKind = pageKind;
	}

	/**
	 * Get the kind of page to which a reference is provided.
	 * 
	 * @return the page kind
	 */
	public PageKind getPageKind() {
		return mPageKind;
	}

}
