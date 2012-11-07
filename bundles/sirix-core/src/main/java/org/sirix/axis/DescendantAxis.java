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

import java.util.ArrayDeque;
import java.util.Deque;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.NodeReadTrx;
import org.sirix.settings.Fixed;

/**
 * <h1>DescendantAxis</h1>
 * 
 * <p>
 * Iterate over all structural descendants starting at a given node. Self is not
 * included.
 * </p>
 */
public final class DescendantAxis extends AbstractAxis {

	/** Stack for remembering next nodeKey in document order. */
	private Deque<Long> mRightSiblingKeyStack;

	/** Determines if it's the first call to hasNext(). */
	private boolean mFirst;
	
	/**
	 * Constructor initializing internal state.
	 * 
	 * @param rtx
	 *          exclusive (immutable) trx to iterate with
	 */
	public DescendantAxis(final @Nonnull NodeReadTrx rtx) {
		super(rtx);
	}

	/**
	 * Constructor initializing internal state.
	 * 
	 * @param rtx
	 *          Exclusive (immutable) trx to iterate with.
	 * @param includeSelf
	 *          Is self included?
	 */
	public DescendantAxis(final @Nonnull NodeReadTrx rtx,
			final @Nonnull IncludeSelf includeSelf) {
		super(rtx, includeSelf);
	}

	@Override
	public void reset(final long pNodeKey) {
		super.reset(pNodeKey);
		mFirst = true;
		mRightSiblingKeyStack = new ArrayDeque<>();
	}

	@Override
	protected long nextKey() {
		long key = Fixed.NULL_NODE_KEY.getStandardProperty();

		// Determines if first call to hasNext().
		if (mFirst) {
			mFirst = false;

			if (isSelfIncluded() == IncludeSelf.YES) {
				key = getTrx().getNodeKey();
			} else {
				key = getTrx().getFirstChildKey();
			}

			return key;
		}

		// Always follow first child if there is one.
		if (getTrx().hasFirstChild()) {
			key = getTrx().getFirstChildKey();
			if (getTrx().hasRightSibling()) {
				mRightSiblingKeyStack.push(getTrx().getRightSiblingKey());
			}
			return key;
		}

		// Then follow right sibling if there is one.
		if (getTrx().hasRightSibling()) {
			final long currKey = getTrx().getNodeKey();
			key = getTrx().getRightSiblingKey();
			return hasNextNode(key, currKey);
		}

		// Then follow right sibling on stack.
		if (mRightSiblingKeyStack.size() > 0) {
			final long currKey = getTrx().getNodeKey();
			key = mRightSiblingKeyStack.pop();
			return hasNextNode(key, currKey);
		}

		return done();
	}

	/**
	 * Determines if the subtree-traversal is finished.
	 * 
	 * @param pKey
	 *          next key
	 * @param pCurrKey
	 *          current node key
	 * @return {@code false} if finished, {@code true} if not
	 */
	private long hasNextNode(@Nonnegative long pKey,
			final @Nonnegative long pCurrKey) {
		getTrx().moveTo(pKey);
		if (getTrx().getLeftSiblingKey() == getStartKey()) {
			return done();
		} else {
			getTrx().moveTo(pCurrKey);
			return pKey;
		}
	}
}
