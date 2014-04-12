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
package org.sirix.node.delegates;

import javax.annotation.Nonnegative;
import javax.annotation.Nullable;

import org.sirix.api.visitor.VisitResultType;
import org.sirix.api.visitor.Visitor;
import org.sirix.node.AbstractForwardingNode;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.StructNode;
import org.sirix.settings.Fixed;

import com.google.common.base.Objects;

/**
 * Delegate method for all nodes building up the structure. That means that all
 * nodes representing trees in Sirix are represented by an instance of the
 * interface {@link StructNode} namely containing the position of all related
 * siblings, the first-child and all nodes defined by the {@link NodeDelegate}
 * as well.
 * 
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class StructNodeDelegate extends AbstractForwardingNode implements
		StructNode {

	/** Pointer to the first child of the current node. */
	private long mFirstChild;

	/** Pointer to the right sibling of the current node. */
	private long mRightSibling;

	/** Pointer to the left sibling of the current node. */
	private long mLeftSibling;

	/** Number of children. */
	private long mChildCount;

	/** Number of descendants. */
	private long mDescendantCount;

	/** Delegate for common node information. */
	private final NodeDelegate mDelegate;

	/**
	 * Constructor.
	 * 
	 * @param del
	 *          {@link NodeDelegate} instance
	 * @param firstChild
	 *          first child key
	 * @param rightSib
	 *          right sibling key
	 * @param leftSib
	 *          left sibling key
	 * @param childCount
	 *          number of children of the node
	 * @param descendantCount
	 *          number of descendants of the node
	 * @param pSiblingPos
	 *          sibling position
	 */
	public StructNodeDelegate(final NodeDelegate del, final long firstChild,
			final long rightSib, final long leftSib,
			final @Nonnegative long childCount,
			final @Nonnegative long descendantCount) {
		assert childCount >= 0 : "childCount must be >= 0!";
		assert descendantCount >= 0 : "descendantCount must be >= 0!";
		assert del != null : "del must not be null!";
		mDelegate = del;
		mFirstChild = firstChild;
		mRightSibling = rightSib;
		mLeftSibling = leftSib;
		mChildCount = childCount;
		mDescendantCount = descendantCount;
	}

	@Override
	public Kind getKind() {
		return mDelegate.getKind();
	}

	@Override
	public boolean hasFirstChild() {
		return mFirstChild != Fixed.NULL_NODE_KEY.getStandardProperty();
	}

	@Override
	public boolean hasLeftSibling() {
		return mLeftSibling != Fixed.NULL_NODE_KEY.getStandardProperty();
	}

	@Override
	public boolean hasRightSibling() {
		return mRightSibling != Fixed.NULL_NODE_KEY.getStandardProperty();
	}

	@Override
	public long getChildCount() {
		return mChildCount;
	}

	@Override
	public long getFirstChildKey() {
		return mFirstChild;
	}

	@Override
	public long getLeftSiblingKey() {
		return mLeftSibling;
	}

	@Override
	public long getRightSiblingKey() {
		return mRightSibling;
	}

	@Override
	public void setRightSiblingKey(final long key) {
		mRightSibling = key;
	}

	@Override
	public void setLeftSiblingKey(final long key) {
		mLeftSibling = key;
	}

	@Override
	public void setFirstChildKey(final long key) {
		mFirstChild = key;
	}

	@Override
	public void decrementChildCount() {
		mChildCount--;
	}

	@Override
	public void incrementChildCount() {
		mChildCount++;
	}

	@Override
	public VisitResultType acceptVisitor(final Visitor pVisitor) {
		return mDelegate.acceptVisitor(pVisitor);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mChildCount, mDelegate, mFirstChild, mLeftSibling,
				mRightSibling, mDescendantCount);
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj instanceof StructNodeDelegate) {
			final StructNodeDelegate other = (StructNodeDelegate) obj;
			return Objects.equal(mChildCount, other.mChildCount)
					&& Objects.equal(mDelegate, other.mDelegate)
					&& Objects.equal(mFirstChild, other.mFirstChild)
					&& Objects.equal(mLeftSibling, other.mLeftSibling)
					&& Objects.equal(mRightSibling, other.mRightSibling)
					&& Objects.equal(mDescendantCount, other.mDescendantCount);
		}
		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("first child", getFirstChildKey())
				.add("left sib", getLeftSiblingKey())
				.add("right sib", getRightSiblingKey())
				.add("child count", getChildCount())
				.add("descendant count", getDescendantCount()).toString();
	}

	@Override
	public long getDescendantCount() {
		return mDescendantCount;
	}

	@Override
	public void decrementDescendantCount() {
		mDescendantCount--;
	}

	@Override
	public void incrementDescendantCount() {
		mDescendantCount++;
	}

	@Override
	public void setDescendantCount(final @Nonnegative long descendantCount) {
		assert descendantCount >= 0 : "pDescendantCount must be >= 0!";
		mDescendantCount = descendantCount;
	}

	@Override
	public boolean isSameItem(@Nullable final Node other) {
		return mDelegate.isSameItem(other);
	}

	@Override
	protected NodeDelegate delegate() {
		return mDelegate;
	}
}
