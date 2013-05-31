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
package org.sirix.page.interfaces;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.PageWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.node.interfaces.Record;
import org.sirix.page.PageReference;

import com.google.common.io.ByteArrayDataOutput;

/**
 * Page interface all pages have to implement.
 * 
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 * 
 */
public interface Page {

	/**
	 * Serialize a page
	 * 
	 * @param out
	 *          {@link ByteArrayDataOutput} reference to serialize to
	 */
	void serialize(final ByteArrayDataOutput out);

	/**
	 * Get all page references.
	 * 
	 * @return all page references
	 */
	PageReference[] getReferences();

	/**
	 * Commit page.
	 * 
	 * @param pageWriteTrx
	 *          {@link PageWriteTrx} implementation
	 * @throws SirixException
	 *           if something went wrong
	 */
	<K extends Comparable<? super K>, V extends Record, S extends KeyValuePage<K, V>> void commit(
			@Nonnull PageWriteTrx<K, V, S> pageWriteTrx) throws SirixException;

	/**
	 * Get the {@link PageReference} at the specified offset
	 * 
	 * @param offset
	 *          the offset
	 * @return the {@link PageReference} at the specified offset
	 */
	PageReference getReference(@Nonnegative int offset);

	/**
	 * Determines if a page is dirty meaning if it has been changed.
	 * 
	 * @return {@code true} if it has been changed, {@code false} otherwise
	 */
	boolean isDirty();

	/**
	 * Set dirty flag (if page has been modified).
	 * 
	 * @param dirty
	 *          dirty or not
	 * @return the page instance
	 */
	Page setDirty(boolean dirty);

}
