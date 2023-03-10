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

package org.sirix.gui.view.sunburst;

import static com.google.common.base.Preconditions.checkArgument;

import org.sirix.diff.DiffTuple;

/**
 * Item container to simplify {@link Moved} enum.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class Item {

	/** Builder instance. */
	// public static final Builder BUILDER = new Builder();

	/** Start angle. */
	public transient float mAngle;

	/** End angle. */
	public transient float mExtension;

	/** Index to the parent item. */
	public transient int mIndexToParent;

	/** Descendant count. */
	public transient int mDescendantCount;

	/** Parent descendant count. */
	public transient int mParentDescendantCount;

	/** Modification count on a node (counts subtree modifications). */
	public transient int mModificationCount;

	/** Modification count on the parent node (counts subtree modifications). */
	public transient int mParentModificationCount;

	/** Determines if 1 must be subtracted. */
	public final boolean mSubtract;

	/** Kind of diff of the current node. */
	public final DiffTuple mDiff;

	/** Next depth of item. */
	public final int mNextDepth;

	/** Current depth of item. */
	public final int mDepth;

	/** Original depth of item. */
	public final int mOrigDepth;

	/** Builder to simplify item constructor. */
	public static final class Builder {

		/** Start angle. */
		transient float mAngle;

		/** End angle. */
		transient float mExtension;

		/** Child count per depth. */
		transient int mDescendantCount = -1;

		/** Parent descendant count. */
		transient int mParentDescendantCount;

		/** Index to the parent item. */
		transient int mIndexToParent;

		/** Modification count on a node (counts subtree modifications). */
		transient int mModificationCount;

		/** Modification count on the parent node (counts subtree modifications). */
		transient int mParentModificationCount;

		/** Determines if 1 must be subtracted. */
		transient boolean mSubtract;

		/** Kind of diff of the current node. */
		transient DiffTuple mDiff;

		/** Depth of the item. */
		transient int mDepth;

		/** Next depth. */
		transient int mNextDepth;

		/** Original depth. */
		transient int mOrigDepth;

		/**
		 * Set all fields.
		 * 
		 * @param pAngle
		 *          start angle
		 * @param pExtension
		 *          end angle
		 * @param pIndexToParent
		 *          index to the parent item
		 * @param pDepth
		 *          depth of item
		 * @param pOrigDepth
		 *          original depth of item
		 * @return this builder
		 */
		public Builder(final float pAngle, final float pExtension,
				final int pIndexToParent, final int pDepth, final int pOrigDepth) {
			checkArgument(pAngle >= 0f, "pAngle must be >= 0!");
			checkArgument(pExtension >= 0f, "pExtension must be >= 0!");
			checkArgument(pIndexToParent >= -1, "pIndexToParent must be >= 0!");
			checkArgument(pDepth >= 0, "pDepth must be >= 0!");
			checkArgument(pOrigDepth >= 0, "pOrigDepth must be >= 0!");
			mAngle = pAngle;
			mExtension = pExtension;
			mIndexToParent = pIndexToParent;
			mDepth = pDepth;
			mOrigDepth = pOrigDepth;
		}

		/**
		 * Set subtract.
		 * 
		 * @param pSubtract
		 *          determines if one must be subtracted
		 * @return this builder
		 */
		public Builder setSubtract(final boolean pSubtract) {
			mSubtract = pSubtract;
			return this;
		}

		/**
		 * Set descendant count.
		 * 
		 * @param pDescendantCount
		 *          descendant count
		 * @return this builder
		 */
		public Builder setDescendantCount(final int pDescendantCount) {
			checkArgument(pDescendantCount >= 1);
			mDescendantCount = pDescendantCount;
			return this;
		}

		/**
		 * Set parent descendant count.
		 * 
		 * @param pParentDescCount
		 *          parent descendant count
		 * @return this builder
		 */
		public Builder setParentDescendantCount(final int pParentDescCount) {
			checkArgument(pParentDescCount >= 0);
			mParentDescendantCount = pParentDescCount;
			return this;
		}

		/**
		 * Set modification count.
		 * 
		 * @param pModificationCount
		 *          modification count
		 * @return this builder
		 */
		public Builder setModificationCount(final int pModificationCount) {
			checkArgument(pModificationCount >= 1);
			mModificationCount = pModificationCount;
			return this;
		}

		/**
		 * Set parent modification count.
		 * 
		 * @param pParentModificationCount
		 *          parent modification count
		 * @return this builder
		 */
		public Builder setParentModificationCount(final int pParentModificationCount) {
			checkArgument(pParentModificationCount >= 1);
			mParentModificationCount = pParentModificationCount;
			return this;
		}

		/**
		 * Set kind of diff.
		 * 
		 * @param pDiff
		 *          {@link DiffTuple} -- kind of diff
		 * @return this builder
		 */
		public Builder setDiff(final DiffTuple pDiff) {
			mDiff = requireNonNull(pDiff);
			return this;
		}

		/**
		 * Set next depth.
		 * 
		 * @param pNextDepth
		 *          next depth
		 * @return this builder
		 */
		public Builder setNextDepth(final int pNextDepth) {
			checkArgument(pNextDepth > 0, "pNextDepth must be >= 0!");
			mNextDepth = pNextDepth;
			return this;
		}

		/**
		 * Setup item.
		 * 
		 * @return {@link Item} instance
		 */
		public Item build() {
			assert mDescendantCount != -1;
			return new Item(this);
		}
	}

	/**
	 * Constructor.
	 * 
	 * @param pBuilder
	 *          the {@link Builder} reference
	 */
	private Item(final Builder pBuilder) {
		mAngle = pBuilder.mAngle;
		mExtension = pBuilder.mExtension;
		mIndexToParent = pBuilder.mIndexToParent;
		mModificationCount = pBuilder.mModificationCount;
		mParentModificationCount = pBuilder.mParentModificationCount;
		mDescendantCount = pBuilder.mDescendantCount;
		mParentDescendantCount = pBuilder.mParentDescendantCount;
		mSubtract = pBuilder.mSubtract;
		mDiff = pBuilder.mDiff;
		mNextDepth = pBuilder.mNextDepth;
		mDepth = pBuilder.mDepth;
		mOrigDepth = pBuilder.mOrigDepth;
	}
}
