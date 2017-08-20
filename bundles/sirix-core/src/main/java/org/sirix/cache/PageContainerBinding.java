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

package org.sirix.cache;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import javax.annotation.Nullable;

import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.api.PageReadTrx;
import org.sirix.page.PagePersistenter;
import org.sirix.page.interfaces.Page;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

/**
 * Binding for {@link PageContainer} reference.
 */
public final class PageContainerBinding extends TupleBinding<PageContainer> {

	/** Logger. */
	private static final LogWrapper LOGGER =
			new LogWrapper(LoggerFactory.getLogger(PageContainerBinding.class));

	/** {@link ResourceConfiguration} instance. */
	private final PageReadTrx mPageReadTrx;

	/**
	 * Constructor.
	 *
	 * @param pageReadTrx {@link PageReadTrx} instance
	 */
	public PageContainerBinding(final PageReadTrx pageReadTrx) {
		assert pageReadTrx != null : "pageReadTrx must not be null!";
		mPageReadTrx = pageReadTrx;
	}

	@Override
	public PageContainer entryToObject(final @Nullable TupleInput input) {
		if (input == null) {
			final PageContainer emptyInstance = PageContainer.EMPTY_INSTANCE;
			return emptyInstance;
		}
		final DataInputStream source =
				new DataInputStream(new ByteArrayInputStream(input.getBufferBytes()));
		try {
			final Page current = PagePersistenter.deserializePage(source, mPageReadTrx);
			final Page modified = PagePersistenter.deserializePage(source, mPageReadTrx);
			return new PageContainer(current, modified);
		} catch (final IOException e) {
			LOGGER.error(e.getMessage(), e);
			return PageContainer.EMPTY_INSTANCE;
		}
	}

	@Override
	public void objectToEntry(final @Nullable PageContainer pageContainer,
			final @Nullable TupleOutput output) {
		if (pageContainer != null && output != null) {
			pageContainer.serialize(output);
		}
	}
}
