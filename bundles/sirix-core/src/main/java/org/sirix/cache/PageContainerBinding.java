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

import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.api.PageReadTrx;
import org.sirix.page.PagePersistenter;
import org.sirix.page.interfaces.KeyValuePage;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteStreams;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

/**
 * Binding for {@link RecordPageContainer} reference.
 */
public class PageContainerBinding<T extends KeyValuePage<?, ?>> extends
		TupleBinding<RecordPageContainer<T>> {

	/** {@link ResourceConfiguration} instance. */
	private final PageReadTrx mPageReadTrx;

	/**
	 * Constructor.
	 * 
	 * @param pageReadTrx
	 *          {@link PageReadTrx} instance
	 */
	public PageContainerBinding(final @Nonnull PageReadTrx pageReadTrx) {
		assert pageReadTrx != null : "pageReadTrx must not be null!";
		mPageReadTrx = pageReadTrx;
	}


	@Override
	public RecordPageContainer<T> entryToObject(
			final @Nullable TupleInput input) {
		if (input == null) {
			@SuppressWarnings("unchecked")
			final RecordPageContainer<T> emptyInstance = (RecordPageContainer<T>) RecordPageContainer.EMPTY_INSTANCE;
			return emptyInstance;
		}
		final ByteArrayDataInput source = ByteStreams.newDataInput(input
				.getBufferBytes());
		@SuppressWarnings("unchecked")
		final T current = (T) PagePersistenter
				.deserializePage(source, mPageReadTrx);
		@SuppressWarnings("unchecked")
		final T modified = (T) PagePersistenter
				.deserializePage(source, mPageReadTrx);
		return new RecordPageContainer<T>(current, modified);
	}

	@Override
	public void objectToEntry(
			final @Nullable RecordPageContainer<T> pageContainer,
			final @Nullable TupleOutput output) {
		if (pageContainer != null && output != null) {
			pageContainer.serialize(output);
		}
	}
}
