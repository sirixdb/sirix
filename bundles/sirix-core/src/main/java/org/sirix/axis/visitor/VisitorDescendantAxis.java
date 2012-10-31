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

package org.sirix.axis.visitor;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayDeque;
import java.util.Deque;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.VisitResultType;
import org.sirix.api.visitor.Visitor;
import org.sirix.axis.AbstractAxis;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.settings.Fixed;

import com.google.common.base.Optional;

/**
 * <h1>VisitorDescendantAxis</h1>
 * 
 * <p>
 * Iterate over all descendants of kind ELEMENT or TEXT starting at a given
 * node. Self is not optionally included. Furthermore an {@link Visitor} can be
 * used. Note that it is faster to use the standard {@link DescendantAxis} if no
 * visitor is specified.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 */
public final class VisitorDescendantAxis extends AbstractAxis {

	/** Stack for remembering next nodeKey in document order. */
	private Deque<Long> mRightSiblingKeyStack;

	/** Optional visitor. */
	private Optional<? extends Visitor> mVisitor = Optional.absent();

	/** Determines if it is the first call. */
	private boolean mFirst;

	/**
	 * Get a new builder instance.
	 * 
	 * @param rtx
	 *          the {@link NodeReadTrx} to iterate with
	 * @return {@link Builder} instance
	 */
	public static Builder builder(final @Nonnull NodeReadTrx rtx) {
		return new Builder(rtx);
	}

	/** The builder. */
	public static class Builder {

		/** Optional visitor. */
		private Optional<? extends Visitor> mVisitor = Optional.absent();

		/** Sirix {@link NodeReadTrx}. */
		private final NodeReadTrx mRtx;

		/** Determines if current node should be included or not. */
		private IncludeSelf mIncludeSelf = IncludeSelf.NO;

		/**
		 * Constructor.
		 * 
		 * @param rtx
		 *          Sirix {@link NodeCursor}
		 */
		public Builder(final @Nonnull NodeReadTrx rtx) {
			mRtx = checkNotNull(rtx);
		}

		/**
		 * Set include self option.
		 * 
		 * @param pIncludeSelf
		 *          include self
		 * @return this builder instance
		 */
		public Builder includeSelf() {
			mIncludeSelf = IncludeSelf.YES;
			return this;
		}

		/**
		 * Set visitor.
		 * 
		 * @param visitor
		 *          the visitor
		 * @return this builder instance
		 */
		public Builder visitor(final Optional<? extends Visitor> visitor) {
			mVisitor = checkNotNull(visitor);
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
	 * @param builder
	 *          the builder to construct a new instance
	 */
	private VisitorDescendantAxis(final @Nonnull Builder builder) {
		super(builder.mRtx, builder.mIncludeSelf);
		mVisitor = builder.mVisitor;
	}

	@Override
	public void reset(final long nodeKey) {
		super.reset(nodeKey);
		mFirst = true;
		mRightSiblingKeyStack = new ArrayDeque<>();
	}

	@Override
	protected long nextKey() {
		// Visitor.
		Optional<VisitResult> result = Optional.absent();
		if (mVisitor.isPresent()) {
			result = Optional.fromNullable(getTrx().acceptVisitor(mVisitor.get()));
		}

		// If visitor is present and the return value is EVisitResult.TERMINATE than
		// return false.
		if (result.isPresent() && result.get() == VisitResultType.TERMINATE) {
			return Fixed.NULL_NODE_KEY.getStandardProperty();
		}

		final NodeReadTrx rtx = getTrx();

		// Determines if first call to hasNext().
		if (mFirst) {
			mFirst = false;
			return isSelfIncluded() == IncludeSelf.YES ? rtx.getNodeKey() : rtx
					.getFirstChildKey();
		}

		// If visitor is present and the the righ sibling stack must be adapted.
		if (result.isPresent()
				&& result.get() == LocalVisitResult.SKIPSUBTREEPOPSTACK) {
			mRightSiblingKeyStack.pop();
		}

		// If visitor is present and result is not
		// EVisitResult.SKIPSUBTREE/EVisitResult.SKIPSUBTREEPOPSTACK or visitor is
		// not present.
		if ((result.isPresent() && result.get() != VisitResultType.SKIPSUBTREE && result
				.get() != LocalVisitResult.SKIPSUBTREEPOPSTACK) || !result.isPresent()) {
			// Always follow first child if there is one.
			if (rtx.hasFirstChild()) {
				final long key = rtx.getFirstChildKey();
				final long rightSiblNodeKey = rtx.getRightSiblingKey();
				if (rtx.hasRightSibling()
						&& (mRightSiblingKeyStack.isEmpty() || (!mRightSiblingKeyStack
								.isEmpty() && mRightSiblingKeyStack.peek() != rightSiblNodeKey))) {
					mRightSiblingKeyStack.push(rightSiblNodeKey);
				}
				return key;
			}
		}

		// If visitor is present and result is not EVisitResult.SKIPSIBLINGS or
		// visitor is not present.
		if ((result.isPresent() && result.get() != VisitResultType.SKIPSIBLINGS)
				|| !result.isPresent()) {
			// Then follow right sibling if there is one.
			if (rtx.hasRightSibling()) {
				final long nextKey = rtx.getRightSiblingKey();
				return hasNextNode(nextKey, rtx.getNodeKey());
			}
		}

		// Then follow right sibling on stack.
		if (mRightSiblingKeyStack.size() > 0) {
			final long nextKey = mRightSiblingKeyStack.pop();
			return hasNextNode(nextKey, rtx.getNodeKey());
		}

		return Fixed.NULL_NODE_KEY.getStandardProperty();
	}

	/**
	 * Determines if next node is not a right sibling of the current node.
	 * 
	 * @param currKey
	 *          node key of current node
	 */
	private long hasNextNode(final @Nonnegative long nextKey,
			final @Nonnegative long currKey) {
		// Fail if the subtree is finished.
		final NodeReadTrx rtx = getTrx();
		rtx.moveTo(nextKey);
		if (rtx.getLeftSiblingKey() == getStartKey()) {
			return Fixed.NULL_NODE_KEY.getStandardProperty();
		} else {
			rtx.moveTo(currKey);
			return nextKey;
		}
	}
}
