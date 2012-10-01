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

import java.util.ArrayDeque;
import java.util.Deque;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.INodeCursor;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.visitor.EVisitResult;
import org.sirix.api.visitor.IVisitor;
import org.sirix.node.interfaces.IStructNode;
import org.sirix.settings.EFixed;

import com.google.common.base.Optional;

/**
 * <h1>VisitorDescendantAxis</h1>
 * 
 * <p>
 * Iterate over all descendants of kind ELEMENT or TEXT starting at a given
 * node. Self is not optionally included. Furthermore an {@link IVisitor} can be
 * used. Note that it is faster to use the standard {@link DescendantAxis} if no
 * visitor is specified.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 */
public final class VisitorDescendantAxis extends AbsAxis {

	/** Stack for remembering next nodeKey in document order. */
	private Deque<Long> mRightSiblingKeyStack;

	/** Optional visitor. */
	private Optional<? extends IVisitor> mVisitor = Optional.absent();

	private boolean mFirst;

	/** The builder. */
	public static class Builder {

		/** Optional visitor. */
		private Optional<? extends IVisitor> mVisitor = Optional.absent();

		/** Sirix {@link INodeCursor}. */
		private final INodeCursor mRtx;

		/** Determines if current node should be included or not. */
		private EIncludeSelf mIncludeSelf = EIncludeSelf.NO;

		/**
		 * Constructor.
		 * 
		 * @param pRtx
		 *          Sirix {@link INodeCursor}
		 */
		public Builder(final INodeCursor pRtx) {
			mRtx = checkNotNull(pRtx);
		}

		/**
		 * Set include self option.
		 * 
		 * @param pIncludeSelf
		 *          include self
		 * @return this builder instance
		 */
		public Builder includeSelf() {
			mIncludeSelf = EIncludeSelf.YES;
			return this;
		}

		/**
		 * Set visitor.
		 * 
		 * @param pVisitor
		 *          the visitor
		 * @return this builder instance
		 */
		public Builder visitor(final Optional<? extends IVisitor> pVisitor) {
			mVisitor = checkNotNull(pVisitor);
			return this;
		}

		/**
		 * Build a new instance.
		 * 
		 * @return new {@link DescendantAxis} instance
		 */
		public VisitorDescendantAxis build() {
			return new VisitorDescendantAxis(this);
		}
	}

	/**
	 * Private constructor.
	 * 
	 * @param pBuilder
	 *          the builder to construct a new instance
	 */
	private VisitorDescendantAxis(@Nonnull final Builder pBuilder) {
		super(pBuilder.mRtx, pBuilder.mIncludeSelf);
		mVisitor = pBuilder.mVisitor;
	}

	@Override
	public void reset(final long pNodeKey) {
		super.reset(pNodeKey);
		mFirst = true;
		mRightSiblingKeyStack = new ArrayDeque<>();
	}

	@Override
	public boolean hasNext() {
		if (!isHasNext()) {
			return false;
		}
		if (isNext()) {
			return true;
		}
		resetToLastKey();

		// Visitor.
		Optional<EVisitResult> result = Optional.absent();
		if (mVisitor.isPresent()) {
			result = Optional.fromNullable(getTrx().acceptVisitor(
					mVisitor.get()));
		}

		// If visitor is present and the return value is EVisitResult.TERMINATE than
		// return false.
		if (result.isPresent() && result.get() == EVisitResult.TERMINATE) {
			resetToStartKey();
			return false;
		}

		final INodeReadTrx rtx = getTrx();
		
		// Determines if first call to hasNext().
		if (mFirst) {
			mFirst = false;

			if (isSelfIncluded() == EIncludeSelf.YES) {
				mKey = rtx.getNodeKey();
			} else {
				mKey = rtx.getFirstChildKey();
			}

			if (mKey == EFixed.NULL_NODE_KEY.getStandardProperty()) {
				resetToStartKey();
				return false;
			}
			return true;
		}

		// If visitor is present and the the righ sibling stack must be adapted.
		if (result.isPresent() && result.get() == EVisitResult.SKIPSUBTREEPOPSTACK) {
			mRightSiblingKeyStack.pop();
		}

		// If visitor is present and result is not
		// EVisitResult.SKIPSUBTREE/EVisitResult.SKIPSUBTREEPOPSTACK or visitor is
		// not present.
		if ((result.isPresent() && result.get() != EVisitResult.SKIPSUBTREE && result
				.get() != EVisitResult.SKIPSUBTREEPOPSTACK) || !result.isPresent()) {
			// Always follow first child if there is one.
			if (rtx.hasFirstChild()) {
				mKey = rtx.getFirstChildKey();
				final long rightSiblNodeKey = rtx.getRightSiblingKey();
				if (rtx.hasRightSibling()
						&& (mRightSiblingKeyStack.isEmpty() || (!mRightSiblingKeyStack
								.isEmpty() && mRightSiblingKeyStack.peek() != rightSiblNodeKey))) {
					mRightSiblingKeyStack.push(rightSiblNodeKey);
				}
				return true;
			}
		}

		// If visitor is present and result is not EVisitResult.SKIPSIBLINGS or
		// visitor is not present.
		if ((result.isPresent() && result.get() != EVisitResult.SKIPSIBLINGS)
				|| !result.isPresent()) {
			// Then follow right sibling if there is one.
			if (rtx.hasRightSibling()) {
				mKey = rtx.getRightSiblingKey();
				return hasNextNode(rtx.getNodeKey());
			}
		}

		// Then follow right sibling on stack.
		if (mRightSiblingKeyStack.size() > 0) {
			mKey = mRightSiblingKeyStack.pop();
			return hasNextNode(rtx.getNodeKey());
		}

		// Then end.
		resetToStartKey();
		return false;
	}

	/**
	 * Determines if next node is not a right sibling of the current node.
	 * 
	 * @param pCurrKey
	 *          node key of current node
	 */
	private boolean hasNextNode(final @Nonnegative long pCurrKey) {
		// Fail if the subtree is finished.
		final INodeReadTrx rtx = getTrx();
		rtx.moveTo(mKey);
		if (rtx.getLeftSiblingKey() == getStartKey()) {
			resetToStartKey();
			return false;
		} else {
			rtx.moveTo(pCurrKey);
			return true;
		}
	}
}
