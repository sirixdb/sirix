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

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.collect.ForwardingObject;
import com.google.common.io.ByteArrayDataOutput;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.IPageWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.page.interfaces.IPage;

/**
 * Forwarding the implementation of all methods in the {@link IPage} interface
 * to a delegate.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public abstract class AbsForwardingPage extends ForwardingObject implements
		IPage {

	/** Constructor for use by subclasses. */
	protected AbsForwardingPage() {
	}

	@Override
	protected abstract IPage delegate();

	@Override
	public void commit(final @Nonnull IPageWriteTrx pPageWriteTrx)
			throws SirixException {
		delegate().commit(checkNotNull(pPageWriteTrx));
	}

	@Override
	public PageReference[] getReferences() {
		return delegate().getReferences();
	}

	@Override
	public int getRevision() {
		return delegate().getRevision();
	}

	@Override
	public PageReference getReference(final @Nonnegative int pOffset) {
		return delegate().getReference(pOffset);
	}

	@Override
	public void serialize(final @Nonnull ByteArrayDataOutput pOut) {
		delegate().serialize(checkNotNull(pOut));
	}

}
