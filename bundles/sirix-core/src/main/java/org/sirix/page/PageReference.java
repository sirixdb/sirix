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

import javax.annotation.Nullable;

import org.sirix.cache.IndirectPageLogKey;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;

import com.google.common.base.Objects;

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

	private IndirectPageLogKey mLogKey;

	/** In-memory deserialized page instance. */
	private Page mPage;

	/** Corresponding mKey of the related key/value page. */
	private long mKeyValuePageKey = -1;

	/** Key in persistent storage. */
	private long mKey = Constants.NULL_ID;

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
	public PageReference(final PageReference reference) {
		mLogKey = reference.mLogKey;
		mPage = reference.mPage;
		mKeyValuePageKey = reference.mKeyValuePageKey;
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
		return Objects.toStringHelper(this).add("keyValuePage", mKeyValuePageKey)
				.add("key", mKey).add("page", mPage).toString();
	}

	/**
	 * Set keyValuePageKey key.
	 * 
	 * @param keyValuePageKey
	 *          the keyValuePageKey to set
	 */
	public void setKeyValuePageKey(final long keyValuePageKey) {
		mKeyValuePageKey = keyValuePageKey;
	}

	/**
	 * Get nodepage key.
	 * 
	 * @return the nodePageKey
	 */
	public long getKeyValuePageKey() {
		return mKeyValuePageKey;
	}

	/**
	 * Set a {@link IndirectPageLogKey}.
	 * 
	 * @param logKey
	 *          the {@link IndirectPageLogKey}
	 */
	public void setLogKey(final IndirectPageLogKey logKey) {
		mLogKey = logKey;
	}

	/**
	 * Get a {@link IndirectPageLogKey}
	 * 
	 * @return the {@link IndirectPageLogKey}
	 */
	public IndirectPageLogKey getLogKey() {
		return mLogKey;
	}
}
