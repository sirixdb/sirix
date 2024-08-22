/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package io.sirix.page.interfaces;

import io.sirix.api.PageTrx;
import io.sirix.page.PageReference;
import io.sirix.settings.Constants;
import org.checkerframework.checker.index.qual.NonNegative;

import java.io.Closeable;
import java.util.List;

/**
 * Page interface all pages have to implement.
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 */
public interface Page extends Closeable {

	default Page clearPage() {
		return this;
	}

	/**
	 * Get all page references.
	 *
	 * @return all page references
	 */
	List<PageReference> getReferences();

	/**
	 * Commit page.
	 *
	 * @param pageWriteTrx
	 *            {@link PageTrx} implementation
	 */
	default void commit(PageTrx pageWriteTrx) {
		final var references = getReferences();
		// final var log = pageWriteTrx.getLog();
		// final List<CompletableFuture<Void>> futures = new
		// ArrayList<>(references.size());
		// for (final PageReference reference : references) {
		// if (reference != null && (reference.getLogKey() != Constants.NULL_ID_INT
		// || reference.getPersistentLogKey() != Constants.NULL_ID_LONG)) {
		// final PageContainer container = log.get(reference, pageWriteTrx);
		//
		// assert container != null;
		//
		// final Page page = container.getModified();
		//
		// assert page != null;
		//
		// if (page instanceof UnorderedKeyValuePage unorderedKeyValuePage) {
		// final var byteBufferBytes = Bytes.elasticByteBuffer(10_000);
		// futures.add(CompletableFuture.runAsync(() ->
		// unorderedKeyValuePage.serialize(byteBufferBytes,
		// SerializationType.DATA)));
		// }
		// }
		// }
		//
		// CompletableFuture.allOf(futures.toArray(new
		// CompletableFuture[futures.size()])).join();

		for (final PageReference reference : references) {
			if (reference.getLogKey() != Constants.NULL_ID_INT) {
				pageWriteTrx.commit(reference);
			}
		}
	}

	/**
	 * Get the {@link PageReference} at the specified offset
	 *
	 * @param offset
	 *            the offset
	 * @return the {@link PageReference} at the specified offset
	 */
	PageReference getOrCreateReference(@NonNegative int offset);

	/**
	 * Set the reference at the specified offset
	 *
	 * @param offset
	 *            the offset
	 * @param pageReference
	 *            the page reference
	 * @return {@code true}, if the page is already full, {@code false} otherwise
	 */
	boolean setOrCreateReference(int offset, PageReference pageReference);

	@Override
	default void close() {
		// Nothing to do.
	}
}
