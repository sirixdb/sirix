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

package org.sirix.axis;

import javax.annotation.Nonnegative;

import org.sirix.api.XdmNodeReadTrx;
import org.sirix.node.Kind;
import org.sirix.settings.Fixed;

/**
 * <h1>AncestorAxis</h1>
 * 
 * <p>
 * Iterate over all descendants of kind ELEMENT or TEXT starting at a given node. Self is not
 * included.
 * </p>
 */
public final class AncestorAxis extends AbstractAxis {

	/**
	 * First touch of node.
	 */
	private boolean mFirst;

	/**
	 * Constructor initializing internal state.
	 * 
	 * @param paramRtx exclusive (immutable) trx to iterate with
	 */
	public AncestorAxis(final XdmNodeReadTrx rtx) {
		super(rtx);
	}

	/**
	 * Constructor initializing internal state.
	 * 
	 * @param rtx exclusive (immutable) trx to iterate with
	 * @param includeSelf Is self included?
	 */
	public AncestorAxis(final XdmNodeReadTrx rtx, final IncludeSelf includeSelf) {
		super(rtx, includeSelf);
	}

	@Override
	public void reset(final @Nonnegative long nodeKey) {
		super.reset(nodeKey);
		mFirst = true;
	}

	@Override
	protected long nextKey() {
		// Self
		if (mFirst && isSelfIncluded() == IncludeSelf.YES) {
			mFirst = false;
			return getTrx().getNodeKey();
		}

		if (getTrx().getKind() != Kind.DOCUMENT && getTrx().hasParent()
				&& getTrx().getParentKey() != Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
			return getTrx().getParentKey();
		}

		return done();
	}
}
