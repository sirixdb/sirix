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

package org.sirix.axis;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;

import org.sirix.api.Axis;
import org.sirix.api.NodeReadTrx;

/**
 * <h1>NonStructuralWrapperAxis</h1>
 * 
 * <p>
 * Adds non-structural nodes to an axis.
 * </p>
 */
public final class NonStructuralWrapperAxis extends AbsAxis {

	/** Parent axis. */
	private final Axis mParentAxis;

	/** Namespace index. */
	private int mNspIndex;

	/** Attribute index. */
	private int mAttIndex;

	/** First run. */
	private boolean mFirst;

	/**
	 * Constructor initializing internal state.
	 * 
	 * @param pParentAxis
	 *          inner nested axis
	 * @param pChildAxis
	 *          outer nested axis
	 */
	public NonStructuralWrapperAxis(@Nonnull final Axis pParentAxis) {
		super(pParentAxis.getTrx());
		mParentAxis = checkNotNull(pParentAxis);
	}

	@Override
	public void reset(final long pNodeKey) {
		super.reset(pNodeKey);
		if (mParentAxis != null) {
			mParentAxis.reset(pNodeKey);
		}
	}

	@Override
	protected long nextKey() {
		final NodeReadTrx trx = mParentAxis.getTrx();
		if (trx.isNamespace()) {
			trx.moveToParent();
		}
		if (trx.isElement() && mNspIndex < trx.getNamespaceCount()) {
			trx.moveToNamespace(mNspIndex++);
			return trx.getNodeKey();
		}
		if (trx.isAttribute()) {
			trx.moveToParent();
		}
		if (trx.isElement() && mAttIndex < trx.getAttributeCount()) {
			trx.moveToAttribute(mAttIndex++);
			return trx.getNodeKey();
		}

		if (mParentAxis.hasNext()) {
			long key = mParentAxis.next();
			if (!trx.isElement()) {
				mNspIndex = 0;
				mAttIndex = 0;
			}
			return key;
		}
		return done();
	}
}
