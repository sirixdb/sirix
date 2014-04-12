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

package org.sirix.gui.view.sunburst.axis;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.annotation.Nonnegative;

import org.sirix.api.NodeReadTrx;
import org.sirix.axis.IncludeSelf;
import org.sirix.diff.DiffDepth;
import org.sirix.diff.DiffFactory.DiffType;
import org.sirix.diff.DiffTuple;
import org.sirix.exception.SirixException;
import org.sirix.gui.view.model.interfaces.Model;
import org.sirix.gui.view.model.interfaces.TraverseModel;
import org.sirix.gui.view.sunburst.Item;
import org.sirix.gui.view.sunburst.Moved;
import org.sirix.gui.view.sunburst.Pruning;
import org.sirix.gui.view.sunburst.model.Modification;
import org.sirix.gui.view.sunburst.model.Modifications;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import processing.core.PConstants;

import com.google.common.base.Optional;

/**
 * Special sunburst compare descendant axis.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class DiffSunburstAxis extends AbstractSunburstAxis {

	/** {@link LogWrapper}. */
	private static final LogWrapper LOGWRAPPER = new LogWrapper(
			LoggerFactory.getLogger(DiffSunburstAxis.class));

	/** Extension stack. */
	private transient Deque<Float> mExtensionStack;

	/** Children per depth. */
	private transient Deque<Integer> mDescendantsStack;

	/** Angle stack. */
	private transient Deque<Float> mAngleStack;

	/** Parent stack. */
	private transient Deque<Integer> mParentStack;

	/** Diff stack. */
	private transient Deque<Integer> mDiffStack;

	/** Determines movement of transaction. */
	private transient Moved mMoved;

	/** Index to parent node. */
	private transient int mIndexToParent;

	/** Parent extension. */
	private transient float mParExtension;

	/** Extension. */
	private transient float mExtension;

	/** Depth in the tree starting at 0. */
	private transient int mDepth;

	/** Start angle. */
	private transient float mAngle;

	/** Current item index. */
	private transient int mIndex;

	/** Current item. */
	private transient Item mItem;

	/**
	 * Model which implements the method createSunburstItem(...) defined by
	 * {@link Model}.
	 */
	private final TraverseModel mModel;

	/** {@link Map} of {@link DiffTuple}s. */
	private final Map<Integer, DiffTuple> mDiffs;

	/** Modification count. */
	private transient int mModificationCount;

	/** Parent modification count. */
	private transient int mParentModificationCount;

	/** Descendant count. */
	private transient int mDescendantCount;

	/** Parent descendant count. */
	private transient int mParentDescendantCount;

	/** {@link NodeReadTrx} on new revision. */
	private transient NodeReadTrx mNewRtx;

	/** {@link NodeReadTrx} on old revision. */
	private transient NodeReadTrx mOldRtx;

	/** Maximum depth in old revision. */
	private transient int mMaxDepth;

	/** Current diff. */
	private transient DiffType mDiff;

	/** {@link DiffTuple} instance. */
	private transient DiffTuple mDiffCont;

	/** Initial depth. */
	private transient int mInitDepth;

	/** Counts the pruned nodes. */
	private transient int mPrunedNodes;

	/** {@link BlockingQueue} for modifications. */
	private final BlockingQueue<Future<Modification>> mModificationsQueue;

	/** Determines if pruning is enabled or disabled. */
	private final Pruning mPrune;

	/** Size of data. */
	private final int mSize;

	/** Determines if axis has next node. */
	private transient boolean mHasNext;

	/**
	 * Temporary key for right sibling of same nodes, right before the depth is
	 * adjusted to the maxdepth + 2.
	 */
	private transient long mTempKey;

	/** Original item depth. */
	private int mOrigDepth;

	/**
	 * Constructor initializing internal state.
	 * 
	 * @param pIncludeSelf
	 *          determines if self is included
	 * @param pCallableModel
	 *          model which implements {@link TraverseModel} interface and
	 *          "observes" axis changes
	 * @param pNewRtx
	 *          {@link NodeReadTrx} on new revision
	 * @param pOldRtx
	 *          {@link NodeReadTrx} on old revision
	 * @param pDiffs
	 *          {@link List} of {@link DiffType}s
	 * @param pMaxDepth
	 *          maximum depth in old revision
	 * @param pInitDepth
	 *          initial depth
	 */
	public DiffSunburstAxis(final IncludeSelf pIncludeSelf,
			final TraverseModel pCallableModel, final NodeReadTrx pNewRtx,
			final NodeReadTrx pOldRtx, final Map<Integer, DiffTuple> pDiffs,
			final int pMaxDepth, final int pInitDepth, final Pruning pPrune) {
		super(pNewRtx, pIncludeSelf);
		mModel = pCallableModel;
		mDiffs = pDiffs;
		mSize = mDiffs.size();
		mNewRtx = pNewRtx;
		mOldRtx = pOldRtx;
		mModificationsQueue = mModel.getModificationQueue();
		mPrune = pPrune;
		if (mPrune != Pruning.ITEMSIZE) {
			try {
				mModel.descendants(Optional.<NodeReadTrx> absent());
			} catch (final InterruptedException | ExecutionException e) {
				LOGWRAPPER.error(e.getMessage(), e);
			}
		}

		mParentDescendantCount = 0;
		mParentModificationCount = 1;
		mDescendantCount = mParentDescendantCount;
		mModificationCount = mParentModificationCount;
		mMaxDepth = pMaxDepth;
		mInitDepth = pInitDepth;
	}

	@Override
	public void reset(final long mNodeKey) {
		super.reset(mNodeKey);
		mExtensionStack = new ArrayDeque<>();
		mDescendantsStack = new ArrayDeque<>();
		mAngleStack = new ArrayDeque<>();
		mParentStack = new ArrayDeque<>();
		mDiffStack = new ArrayDeque<>();
		mAngle = 0F;
		mDepth = 0;
		mDescendantCount = 0;
		mParentDescendantCount = 0;
		mMoved = Moved.STARTRIGHTSIBL;
		mIndexToParent = -1;
		mParExtension = PConstants.TWO_PI;
		mExtension = PConstants.TWO_PI;
		mIndex = -1;
		mHasNext = true;
	}

	@Override
	public boolean hasNext() {
		if (getNext()) {
			return true;
		}

		resetToLastKey();

		// Fail if there is no node anymore.
		if (!mHasNext) {
			return false;
		}

		// Setup everything.
		mDiffCont = mDiffs.get(mIndex + mPrunedNodes + 1);
		mDiff = mDiffCont.getDiff();

		final boolean isOldTransaction = (mDiff == DiffType.DELETED
				|| mDiff == DiffType.MOVEDFROM || mDiff == DiffType.REPLACEDOLD);
		final long nodeKey = isOldTransaction ? mDiffCont.getOldNodeKey()
				: mDiffCont.getNewNodeKey();
		final DiffDepth depthCont = mDiffCont.getDepth();
		mOrigDepth = isOldTransaction ? depthCont.getOldDepth() : depthCont
				.getNewDepth();

		setTransaction(isOldTransaction ? mOldRtx : mNewRtx);

		// Move to next key.
		getTransaction().moveTo(nodeKey);

		if (mDiff == DiffType.UPDATED) {
			mOldRtx.moveTo(mDiffCont.getOldNodeKey());
		}

		if (mDiff == DiffType.REPLACEDOLD) {
			mNewRtx.moveTo(mDiffCont.getNewNodeKey());
		}

		if (mIndex + mPrunedNodes + 2 < mSize) {
			final DiffTuple nextDiffCont = mDiffs.get(mIndex + mPrunedNodes + 2);
			final DiffType nextDiff = nextDiffCont.getDiff();
			final boolean nextIsOldTransaction = (nextDiff == DiffType.DELETED
					|| nextDiff == DiffType.MOVEDFROM || nextDiff == DiffType.REPLACEDOLD);
			final DiffDepth nextDepthCont = nextDiffCont.getDepth();
			final int nextDepth = nextIsOldTransaction ? nextDepthCont.getOldDepth()
					: nextDepthCont.getNewDepth();

			// Always follow first child if there is one.
			if (nextDepth > mOrigDepth) {
				return processFirstChild(mOrigDepth, nextDepth);
			}

			// Then follow right sibling if there is one.
			if (mOrigDepth == nextDepth) {
				return processRightSibling(nextDepth);
			}

			// Then follow right sibling on stack.
			if (nextDepth < mOrigDepth) {
				return processNextFollowing(mOrigDepth, nextDepth);
			}
		}

		// Then end.
		processLastItem(1);
		mHasNext = false;
		return true;
	}

	/**
	 * Process first child.
	 * 
	 * @param pDepth
	 *          depth of diff
	 * @param pNextDepth
	 *          depth of next diff
	 * @return {@code true} if axis has more nodes, {@code false} otherwise
	 */
	private boolean processFirstChild(@Nonnegative final int pDepth,
			@Nonnegative final int pNextDepth) {
		assert pDepth >= 0;
		assert pNextDepth >= 0;
		processLastItem(pNextDepth);

		if (mModel.getIsPruned()) {
			if (mDiff != DiffType.SAMEHASH) {
				mPrunedNodes += mDescendantCount - 1;
			}

			final boolean isOldTransaction = (mDiff == DiffType.DELETED
					|| mDiff == DiffType.MOVEDFROM || mDiff == DiffType.REPLACEDOLD);
			final DiffDepth depthCont = mDiffCont.getDepth();
			final int depth = isOldTransaction ? depthCont.getOldDepth() : depthCont
					.getNewDepth();

			if (mIndex + mPrunedNodes + 1 < mSize) {
				final DiffTuple nextDiffCont = mDiffs.get(mIndex + mPrunedNodes + 1);
				final DiffType nextDiff = nextDiffCont.getDiff();
				final boolean nextIsOldTransaction = (nextDiff == DiffType.DELETED
						|| nextDiff == DiffType.MOVEDFROM || nextDiff == DiffType.REPLACEDOLD);
				final DiffDepth nextDepthCont = nextDiffCont.getDepth();
				final int nextDepth = nextIsOldTransaction ? nextDepthCont
						.getOldDepth() : nextDepthCont.getNewDepth();

				if (depth == nextDepth) {
					mAngle += mExtension;
					mMoved = Moved.STARTRIGHTSIBL;

					determineIfHasNext();
					return true;
				} else {
					processStacks(depth, nextDepth);
					mMoved = Moved.ANCHESTSIBL;
					determineIfHasNext();
					return true;
				}
			}
		} else {
			mDiffStack.push(mModificationCount);
			mAngleStack.push(mAngle);
			mExtensionStack.push(mExtension);
			mParentStack.push(mIndex);
			mDescendantsStack.push(mDescendantCount);

			mDepth++;
			mMoved = Moved.CHILD;
		}

		determineIfHasNext();
		return true;
	}

	/**
	 * Process right sibling.
	 * 
	 * @param pNextDepth
	 *          depth of next diff
	 * @return {@code true} if axis has more nodes, {@code false} otherwise
	 */
	private boolean processRightSibling(@Nonnegative final int pNextDepth) {
		processLastItem(pNextDepth);

		mAngle += mExtension;
		mMoved = Moved.STARTRIGHTSIBL;

		determineIfHasNext();
		return true;
	}

	/**
	 * Process next following node.
	 * 
	 * @param pDepth
	 *          current depth
	 * @param pNextDepth
	 *          next depth
	 * @return {@code true} if axis has more nodes, {@code false} otherwise
	 */
	private boolean processNextFollowing(@Nonnegative final int pDepth,
			@Nonnegative final int pNextDepth) {
		assert pDepth >= 0;
		assert pNextDepth >= 0;

		processLastItem(pNextDepth);
		processStacks(pDepth, pNextDepth);
		mMoved = Moved.ANCHESTSIBL;
		determineIfHasNext();
		return true;
	}

	/**
	 * Process the stacks in case of a next following node is determined.
	 * 
	 * @param pDepth
	 *          the current depth
	 * @param pNextDepth
	 *          the next depth
	 */
	private void processStacks(@Nonnegative final int pDepth,
			@Nonnegative final int pNextDepth) {
		assert pDepth >= 0;
		assert pNextDepth >= 0;
		int depth = pDepth;
		int nextDepth = pNextDepth;
		boolean first = true;
		while (depth > nextDepth) {
			if (first) {
				// Do not pop from stack if it's a leaf node.
				first = false;
			} else {
				mDiffStack.pop();
				mAngleStack.pop();
				mExtensionStack.pop();
				mParentStack.pop();
				mDescendantsStack.pop();
			}
			depth--;
			mDepth--;
		}
	}

	/**
	 * Process and create the item which has been determined by the last
	 * {@code hasNext()} call.
	 * 
	 * @param pNextDepth
	 *          depth of next diff
	 */
	private void processLastItem(@Nonnegative final int pNextDepth) {
		assert pNextDepth >= 0;
		processMove(pNextDepth);
		calculateDepth();
		mExtension = mModel.createSunburstItem(mItem, mDepth, mIndex);
	}

	/**
	 * Determine if next call to {@code hasNext()} will yield {@code true} or
	 * {@code false}.
	 */
	private void determineIfHasNext() {
		if (mIndex + 1 + mPrunedNodes >= mSize) {
			mHasNext = false;
		}
	}

	/**
	 * Calculates new depth for modifications and back to "normal" depth.
	 */
	private void calculateDepth() {
		if (mDiff != DiffType.SAME && mDiff != DiffType.SAMEHASH
				&& mDepth <= mMaxDepth + 2) {
			mDepth = mMaxDepth + 2;
			setTempKey();
		} else if ((mDiff == DiffType.SAME || mDiff == DiffType.SAMEHASH)
				&& mDiffCont.getNewNodeKey() == mTempKey) {
			mDepth = mDiffCont.getDepth().getNewDepth() - mInitDepth;
		}
	}

	/**
	 * Set temporal key which is the next following node of an {@code inserted},
	 * {@code deleted}, {@code updated} or {@code same} node.
	 */
	private void setTempKey() {
		final int index = mIndex + mPrunedNodes + mDescendantCount;
		final boolean isOldTransaction = (mDiff == DiffType.DELETED
				|| mDiff == DiffType.MOVEDFROM || mDiff == DiffType.REPLACEDOLD);
		final DiffDepth depthCont = mDiffCont.getDepth();
		final int depth = isOldTransaction ? depthCont.getOldDepth() : depthCont
				.getNewDepth();

		if (index < mSize) {
			DiffTuple nextDiffCont = mDiffs.get(index);
			DiffType nextDiff = nextDiffCont.getDiff();
			boolean nextIsOldTransaction = (nextDiff == DiffType.DELETED
					|| nextDiff == DiffType.MOVEDFROM || nextDiff == DiffType.REPLACEDOLD);
			DiffDepth nextDepthCont = nextDiffCont.getDepth();
			int nextDepth = nextIsOldTransaction ? nextDepthCont.getOldDepth()
					: nextDepthCont.getNewDepth();

			assert nextDepth <= depth;
			mTempKey = nextIsOldTransaction ? nextDiffCont.getOldNodeKey()
					: nextDiffCont.getNewNodeKey();
		}
	}

	/** Process movement. */
	private void processMove(final int pNextDepth) {
		final Optional<Modification> optionalMod = getModification();
		final Modification modification = optionalMod.isPresent() == true ? optionalMod
				.get() : Modifications.emptyModification();
		assert modification != Modifications.emptyModification() : "modification shouldn't be empty!";
		mDescendantCount = modification.getDescendants();
		mItem = new Item.Builder(mAngle, mParExtension, mIndexToParent, mDepth,
				mOrigDepth)
				.setDescendantCount(mDescendantCount)
				.setParentDescendantCount(mParentDescendantCount)
				.setModificationCount(
						modification.getModifications() + mDescendantCount)
				.setParentModificationCount(mParentModificationCount)
				.setSubtract(modification.isSubtract()).setDiff(mDiffCont)
				.setNextDepth(pNextDepth).build();
		mMoved.processCompareMove(getTransaction(), mItem, mAngleStack,
				mExtensionStack, mDescendantsStack, mParentStack, mDiffStack);
		mModificationCount = mItem.mModificationCount;
		mParentModificationCount = mItem.mParentModificationCount;
		mDescendantCount = mItem.mDescendantCount;
		mParentDescendantCount = mItem.mParentDescendantCount;
		mAngle = mItem.mAngle;
		mParExtension = mItem.mExtension;
		mIndexToParent = mItem.mIndexToParent;
		mIndex++;
	}

	/**
	 * Get next modification.
	 * 
	 * @return {@link Optional} reference which includes the {@link Modifcation}
	 *         reference
	 */
	private Optional<Modification> getModification() {
		Modification modification = null;
		try {
			if (mPrune == Pruning.ITEMSIZE) {
				modification = Modifications.getInstance(mIndex + 1 + mPrunedNodes,
						mDiffs).countDiffs();
			} else {
				modification = mModificationsQueue.take().get();
			}
		} catch (final InterruptedException | ExecutionException | SirixException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}
		return Optional.fromNullable(modification);
	}

	@Override
	public void decrementIndex() {
		mIndex--;
		mPrunedNodes++;
	}

	@Override
	public int getDescendantCount() {
		return mDescendantCount;
	}

	@Override
	public int getPrunedNodes() {
		return mPrunedNodes;
	}

	@Override
	public int getModificationCount() {
		return mModificationCount;
	}
}
