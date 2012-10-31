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

package org.sirix.cache;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.page.PagePersistenter;
import org.sirix.page.UnorderedRecordPage;
import org.sirix.page.interfaces.RecordPage;

import com.google.common.base.Objects;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.sleepycat.bind.tuple.TupleOutput;

/**
 * <h1>PageContainer</h1>
 * 
 * <p>
 * This class acts as a container for revisioned {@link RecordPage}s. Each
 * {@link RecordPage} is stored in a versioned manner. If modifications
 * occur, the versioned {@link RecordPage}s are dereferenced and
 * reconstructed. Afterwards, this container is used to store a complete
 * {@link RecordPage} as well as one for upcoming modifications.
 * </p>
 * 
 * <p>
 * Both {@link RecordPage}s can differ since the complete one is mainly used
 * for read access and the modifying one for write access (and therefore mostly
 * lazy dereferenced).
 * </p>
 * 
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class RecordPageContainer<T extends RecordPage<?>> {

	/** {@link UnorderedRecordPage} reference, which references the complete node page. */
	private final T mComplete;

	/** {@link UnorderedRecordPage} reference, which references the modified node page. */
	private final T mModified;

	/** Empty instance. */
	public static final RecordPageContainer<? extends RecordPage<?>> EMPTY_INSTANCE = new RecordPageContainer<>();

	/** Private constructor for empty instance. */
	private RecordPageContainer() {
		mComplete = null;
		mModified = null;
	}

	/**
	 * Constructor with complete page and lazy instantiated modifying page.
	 * 
	 * @param complete
	 *          to be used as a base for this container
	 * @param resourceConfig
	 *          {@link ResourceConfiguration} instance
	 */
	@SuppressWarnings("unchecked")
	public RecordPageContainer(final @Nonnull T complete) {
		this(complete, (T) complete.newInstance(complete.getRecordPageKey(),
				complete.getRevision(), complete.getPageReadTrx()));
	}

	/**
	 * Constructor with both, complete and modifying page.
	 * 
	 * @param complete
	 *          to be used as a base for this container
	 * @param modifying
	 *          to be used as a base for this container
	 */
	public RecordPageContainer(final @Nonnull T complete,
			final @Nonnull T modifying) {
		assert complete != null;
		assert modifying != null;
		mComplete = complete;
		mModified = modifying;
	}

	/**
	 * Getting the complete page.
	 * 
	 * @return the complete page
	 */
	public T getComplete() {
		return mComplete;
	}

	/**
	 * Getting the modified page.
	 * 
	 * @return the modified page
	 */
	public T getModified() {
		return mModified;
	}

	/**
	 * Serializing the container to the cache.
	 * 
	 * @param out
	 *          for serialization
	 */
	public void serialize(final @Nonnull TupleOutput out) {
		final ByteArrayDataOutput sink = ByteStreams.newDataOutput();
		PagePersistenter.serializePage(sink, mComplete);
		PagePersistenter.serializePage(sink, mModified);
		out.write(sink.toByteArray());
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mComplete, mModified);
	}

	@Override
	public boolean equals(final @Nullable Object obj) {
		if (obj instanceof RecordPageContainer) {
			final RecordPageContainer<?> other = (RecordPageContainer<?>) obj;
			return Objects.equal(mComplete, other.mComplete)
					&& Objects.equal(mModified, other.mModified);
		}
		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("complete page", mComplete)
				.add("modified page", mModified).toString();
	}
}
