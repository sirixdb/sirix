///**
// * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
// * All rights reserved.
// * 
// * Redistribution and use in source and binary forms, with or without
// * modification, are permitted provided that the following conditions are met:
// * * Redistributions of source code must retain the above copyright
// * notice, this list of conditions and the following disclaimer.
// * * Redistributions in binary form must reproduce the above copyright
// * notice, this list of conditions and the following disclaimer in the
// * documentation and/or other materials provided with the distribution.
// * * Neither the name of the University of Konstanz nor the
// * names of its contributors may be used to endorse or promote products
// * derived from this software without specific prior written permission.
// * 
// * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
// * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
// */
//
//package org.treetank.gui.view.sunburst.axis;
//
//import com.google.common.base.Optional;
//
//import java.util.ArrayDeque;
//import java.util.Deque;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.Future;
//
//import org.slf4j.LoggerFactory;
//import org.treetank.api.INodeReadTrx;
//import org.treetank.api.ISession;
//import org.treetank.axis.EIncludeSelf;
//import org.treetank.diff.DiffTuple;
//import org.treetank.diff.DiffFactory.EDiff;
//import org.treetank.exception.AbsTTException;
//import org.treetank.gui.view.model.interfaces.IModel;
//import org.treetank.gui.view.model.interfaces.ITraverseModel;
//import org.treetank.gui.view.sunburst.EMoved;
//import org.treetank.gui.view.sunburst.EPruning;
//import org.treetank.gui.view.sunburst.Item;
//import org.treetank.gui.view.sunburst.model.Modification;
//import org.treetank.gui.view.sunburst.model.Modifications;
//import org.treetank.node.ENode;
//import org.treetank.node.interfaces.IStructNode;
//import org.treetank.settings.EFixed;
//import org.treetank.utils.LogWrapper;
//import processing.core.PConstants;
//
///**
// * Special sunburst compare descendant axis.
// * 
// * @author Johannes Lichtenberger, University of Konstanz
// * 
// */
//public final class SunburstCompareDescendantAxis extends AbsSunburstAxis {
//
//  /** {@link LogWrapper}. */
//  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory
//    .getLogger(SunburstCompareDescendantAxis.class));
//
//  /** Extension stack. */
//  private transient Deque<Float> mExtensionStack;
//
//  /** Children per depth. */
//  private transient Deque<Integer> mDescendantsStack;
//
//  /** Angle stack. */
//  private transient Deque<Float> mAngleStack;
//
//  /** Parent stack. */
//  private transient Deque<Integer> mParentStack;
//
//  /** Diff stack. */
//  private transient Deque<Integer> mDiffStack;
//
//  /** Determines movement of transaction. */
//  private transient EMoved mMoved;
//
//  /** Index to parent node. */
//  private transient int mIndexToParent;
//
//  /** Parent extension. */
//  private transient float mParExtension;
//
//  /** Extension. */
//  private transient float mExtension;
//
//  /** Depth in the tree starting at 0. */
//  private transient int mDepth;
//
//  /** Start angle. */
//  private transient float mAngle;
//
//  /** Current item index. */
//  private transient int mIndex;
//
//  /** Current item. */
//  private transient Item mItem;
//
//  /** Stack for remembering next nodeKey in document order. */
//  private transient Deque<Long> mRightSiblingKeyStack;
//
//  /** Temporal stack for remembering next nodeKey in document order. */
//  private transient Deque<Long> mTempRightSiblingKeyStack;
//
//  /** The nodeKey of the next node to visit. */
//  private transient long mNextKey;
//
//  /** The nodeKey of the next node to visit. */
//  private transient long mTempNextKey;
//
//  /** The nodeKey of the right sibling of the INSERTED, DELETED or UPDATED node. */
//  private transient long mTempKey;
//
//  /** Model which implements the method createSunburstItem(...) defined by {@link IModel}. */
//  private final ITraverseModel mModel;
//
//  /** {@link Map} of {@link DiffTuple}s. */
//  private final Map<Integer, DiffTuple> mDiffs;
//
//  /** Modification count. */
//  private transient int mModificationCount;
//
//  /** Parent modification count. */
//  private transient int mParentModificationCount;
//
//  /** Descendant count. */
//  private transient int mDescendantCount;
//
//  /** Parent descendant count. */
//  private transient int mParentDescendantCount;
//
//  /** {@link INodeReadTrx} on new revision. */
//  private transient INodeReadTrx mNewRtx;
//
//  /** {@link INodeReadTrx} on old revision. */
//  private transient INodeReadTrx mOldRtx;
//
//  /** Maximum depth in old revision. */
//  private transient int mMaxDepth;
//
//  /** Current diff. */
//  private transient EDiff mDiff;
//
//  /** Last diff. */
//  private transient EDiff mLastDiff;
//
//  /** {@link DiffTuple} instance. */
//  private transient DiffTuple mDiffCont;
//
//  /** Last {@link DiffTuple}. */
//  private transient DiffTuple mLastDiffCont;
//
//  /** Initial depth. */
//  private transient int mInitDepth;
//
//  /** Counts the pruned nodes. */
//  private transient int mPrunedNodes;
//
//  /** {@link BlockingQueue} for modifications. */
//  private final BlockingQueue<Future<Modification>> mModificationsQueue;
//
//  /** Determines if pruning is enabled or disabled. */
//  private final EPruning mPrune;
//
//  /**
//   * Constructor initializing internal state.
//   * 
//   * @param paramIncludeSelf
//   *          determines if self is included
//   * @param paramCallableModel
//   *          model which implements {@link ITraverseModel} interface and "observes" axis changes
//   * @param paramNewRtx
//   *          {@link INodeReadTrx} on new revision
//   * @param paramOldRtx
//   *          {@link INodeReadTrx} on old revision
//   * @param paramDiffs
//   *          {@link List} of {@link EDiff}s
//   * @param paramMaxDepth
//   *          maximum depth in old revision
//   * @param paramInitDepth
//   *          initial depth
//   */
//  public SunburstCompareDescendantAxis(final EIncludeSelf paramIncludeSelf,
//    final ITraverseModel paramCallableModel, final INodeReadTrx paramNewRtx, final INodeReadTrx paramOldRtx,
//    final Map<Integer, DiffTuple> paramDiffs, final int paramMaxDepth, final int paramInitDepth,
//    final EPruning paramPrune, final ISession paramSession) {
//    super(paramNewRtx, paramIncludeSelf);
//    mModel = paramCallableModel;
//    mDiffs = paramDiffs;
//    mNewRtx = paramNewRtx;
//    mOldRtx = paramOldRtx;
//    mModificationsQueue = mModel.getModificationQueue();
//    mPrune = paramPrune;
//    if (mPrune == EPruning.NO || mPrune == EPruning.DIFF) {
//      try {
//        mModel.getDescendants(getTransaction());
//      } catch (final InterruptedException | ExecutionException e) {
//        LOGWRAPPER.error(e.getMessage(), e);
//      }
//    }
//
//    mParentDescendantCount = 0;
//    mParentModificationCount = 1;
//    mDescendantCount = mParentDescendantCount;
//    mModificationCount = mParentModificationCount;
//    mMaxDepth = paramMaxDepth;
//    mInitDepth = paramInitDepth;
//  }
//
//  @Override
//  public void reset(final long mNodeKey) {
//    super.reset(mNodeKey);
//    mRightSiblingKeyStack = new ArrayDeque<Long>();
//    if (isSelfIncluded() == EIncludeSelf.YES) {
//      mNextKey = getTransaction().getNode().getNodeKey();
//    } else {
//      mNextKey = getTransaction().getStructuralNode().getFirstChildKey();
//    }
//    mExtensionStack = new ArrayDeque<>();
//    mDescendantsStack = new ArrayDeque<>();
//    mAngleStack = new ArrayDeque<>();
//    mParentStack = new ArrayDeque<>();
//    mDiffStack = new ArrayDeque<>();
//    mAngle = 0F;
//    mDepth = 0;
//    mDescendantCount = 0;
//    mParentDescendantCount = 0;
//    mMoved = EMoved.STARTRIGHTSIBL;
//    mIndexToParent = -1;
//    mParExtension = PConstants.TWO_PI;
//    mExtension = PConstants.TWO_PI;
//    mIndex = -1;
//    mLastDiff = EDiff.SAME;
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public boolean hasNext() {
//    if (getNext()) {
//      return true;
//    }
//    resetToLastKey();
//
//    // Check for deletions.
//    if (mIndex + 1 + mPrunedNodes < mDiffs.size() && mNextKey != 0) {
//      mDiffCont = mDiffs.get(mIndex + 1 + mPrunedNodes);
//      mDiff = mDiffCont.getDiff();
//      final boolean firstDelete = processDeletions();
//      if (firstDelete) {
//        return true;
//      }
//    }
//
//    // Fail if there is no node anymore.
//    if (mNextKey == (Long)EFixed.NULL_NODE_KEY.getStandardProperty()) {
//      resetToStartKey();
//      return false;
//    }
//
//    // Move to next key.
//    getTransaction().moveTo(mNextKey);
//
//    // Fail if the subtree is finished.
//    if (getTransaction().getStructuralNode().getLeftSiblingKey() == getStartKey()) {
//      resetToStartKey();
//      return false;
//    }
//
//    // The logical operator might be faster.
//    if (mDiff == EDiff.UPDATED | mDiff == EDiff.REPLACEDOLD | mDiff == EDiff.REPLACEDNEW) {
//      // For EDiff.UPDATE or EDiff.REPLACED the transaction needs to be on the right node.
//      mOldRtx.moveTo(mDiffCont.getOldNodeKey());
//    }
//
//    /*
//     * Always follow first child if there is one. If not check if a DELETE rooted at the current
//     * node follows or if current diff is an UPDATE and DELETES follow because then current values
//     * have to be pushed onto the stacks and the movement has to be EMoved.CHILD as well.
//     */
//    if (getTransaction().getStructuralNode().hasFirstChild()
//      || (mIndex + 2 + mPrunedNodes < mDiffs.size()
//        && ((mDiffs.get(mIndex + 2 + mPrunedNodes).getDiff() == EDiff.DELETED | mDiffs.get(
//          mIndex + 2 + mPrunedNodes).getDiff() == EDiff.MOVEDFROM)
//          && mOldRtx.moveTo(mDiffs.get(mIndex + 2 + mPrunedNodes).getOldNodeKey()) && mOldRtx
//          .getStructuralNode().getParentKey() == getTransaction().getNode().getNodeKey()) || (mDiff == EDiff.UPDATED
//        && mIndex + 2 + mPrunedNodes < mDiffs.size()
//        && (mDiffs.get(mIndex + 2 + mPrunedNodes).getDiff() == EDiff.DELETED | mDiffs.get(
//          mIndex + 2 + mPrunedNodes).getDiff() == EDiff.MOVEDFROM) && mDiffs.get(mIndex + 2 + mPrunedNodes)
//        .getDepth().getOldDepth() == mDiffCont.getDepth().getNewDepth() + 1))) {
//      if (getTransaction().getStructuralNode().hasFirstChild()) {
//        mNextKey = getTransaction().getStructuralNode().getFirstChildKey();
//      } else if (getTransaction().getStructuralNode().hasRightSibling()) {
//        mNextKey = getTransaction().getStructuralNode().getRightSiblingKey();
//      } else if (!mRightSiblingKeyStack.isEmpty()) {
//        mNextKey = mRightSiblingKeyStack.pop();
//      } else {
//        mNextKey = (Long)EFixed.NULL_NODE_KEY.getStandardProperty();
//      }
//
//      if (getTransaction().getStructuralNode().hasFirstChild()
//        && getTransaction().getStructuralNode().hasRightSibling() && mDepth != 0) {
//        mRightSiblingKeyStack.push(getTransaction().getStructuralNode().getRightSiblingKey());
//      }
//      if (getTransaction().getNode().getKind() != ENode.ROOT_KIND) {
//        processMove();
//        calculateDepth();
//        mExtension = mModel.createSunburstItem(mItem, mDepth, mIndex);
//
//        if (mModel.getIsPruned()) {
//          if (mDiff != EDiff.SAMEHASH) {
//            mPrunedNodes += mDescendantCount - 1;
//          }
//
//          processPruned();
//        } else {
//          mDiffStack.push(mModificationCount);
//          mAngleStack.push(mAngle);
//          mExtensionStack.push(mExtension);
//          mParentStack.push(mIndex);
//          mDescendantsStack.push(mDescendantCount);
//
//          mDepth++;
//          mMoved = EMoved.CHILD;
//        }
//      }
//
//      mLastDiff = mDiff;
//      mLastDiffCont = mDiffCont;
//      if (mIndex + 1 + mPrunedNodes >= mDiffs.size()) {
//        mNextKey = (Long)EFixed.NULL_NODE_KEY.getStandardProperty();
//      }
//      return true;
//    }
//
//    // Move back to next node key because the transaction might have moved away.
//    getTransaction().moveTo(mNextKey);
//
//    // Then follow right sibling if there is one.
//    if (getTransaction().getStructuralNode().hasRightSibling()) {
//      mNextKey = getTransaction().getStructuralNode().getRightSiblingKey();
//
//      processMove();
//      calculateDepth();
//      mExtension = mModel.createSunburstItem(mItem, mDepth, mIndex);
//
//      processRightSibling();
//
//      mLastDiff = mDiff;
//      mLastDiffCont = mDiffCont;
//      if (mIndex + 1 + mPrunedNodes >= mDiffs.size()) {
//        mNextKey = (Long)EFixed.NULL_NODE_KEY.getStandardProperty();
//      }
//      return true;
//    }
//
//    // Then follow right sibling on stack.
//    if (!mRightSiblingKeyStack.isEmpty()) {
//      // TODO: Not quiet sure why I've needed the pop()-operation!
//      if (mLastDiff == EDiff.DELETED && mDiff != EDiff.DELETED && mMoved == EMoved.CHILD) {
//        mRightSiblingKeyStack.pop();
//      }
//
//      if (!mRightSiblingKeyStack.isEmpty()) {
//        mNextKey = mRightSiblingKeyStack.pop();
//      }
//
//      processMove();
//      calculateDepth();
//      mExtension = mModel.createSunburstItem(mItem, mDepth, mIndex);
//
//      processAnchSibl();
//
//      mLastDiff = mDiff;
//      mLastDiffCont = mDiffCont;
//      if (mIndex + 1 + mPrunedNodes >= mDiffs.size()) {
//        mNextKey = (Long)EFixed.NULL_NODE_KEY.getStandardProperty();
//      }
//      return true;
//    }
//
//    /*
//     * Then end (might be the end of DELETES or the end of all other kinds, such that other nodes
//     * still might follow.
//     */
//    processMove();
//    calculateDepth();
//    mExtension = mModel.createSunburstItem(mItem, mDepth, mIndex);
//
//    processEndOfTraversal();
//
//    mLastDiff = mDiff;
//    mLastDiffCont = mDiffCont;
//    return true;
//  }
//
//  /**
//   * Process the end of a traversal, if no more same nodes or no more deletes/movedto nodes are encountered
//   * but the opposite item types might still follow.
//   */
//  private void processEndOfTraversal() {
//    if (mIndex + 1 + mPrunedNodes < mDiffs.size()) {
//      final DiffTuple container = mDiffs.get(mIndex + 1 + mPrunedNodes);
//      int currDepth;
//      if (mDiff == EDiff.DELETED || mDiff == EDiff.MOVEDFROM) {
//        currDepth = mDiffCont.getDepth().getOldDepth();
//      } else {
//        currDepth = mDiffCont.getDepth().getNewDepth();
//      }
//      int newDepth;
//      if (container.getDiff() == EDiff.DELETED || container.getDiff() == EDiff.MOVEDFROM) {
//        newDepth = container.getDepth().getOldDepth();
//      } else {
//        newDepth = container.getDepth().getNewDepth();
//      }
//      detectMovement(currDepth, newDepth);
//    } else {
//      mNextKey = (Long)EFixed.NULL_NODE_KEY.getStandardProperty();
//    }
//  }
//
//  /**
//   * Detect movement.
//   * 
//   * @param pNewDepth
//   *          the depth of the next diff element
//   */
//  private void detectMovement(final int pCurrDepth, final int pNewDepth) {
//    assert pCurrDepth >= 0;
//    assert pNewDepth >= 0;
//    if (pCurrDepth > pNewDepth) {
//      mMoved = EMoved.ANCHESTSIBL;
//    } else if (pCurrDepth == pNewDepth) {
//      processRightSibling();
//    } else {
//      assert pCurrDepth < pNewDepth;
//      mMoved = EMoved.CHILD;
//    }
//  }
//
//  /** Process a pruned node. */
//  private void processPruned() {
//    if (getTransaction().getStructuralNode().hasRightSibling()) {
//      // Next node will be right sibling node.
//      if (getTransaction().getStructuralNode().hasFirstChild() && mDepth != 0) {
//        mRightSiblingKeyStack.pop();
//      }
//
//      mNextKey = getTransaction().getStructuralNode().getRightSiblingKey();
//      mAngle += mExtension;
//      mMoved = EMoved.STARTRIGHTSIBL;
//    } else {
//      // Next will be following node (first one in the following axis).
//      if (mLastDiff == EDiff.DELETED && mDiff != EDiff.DELETED && mMoved == EMoved.CHILD) {
//        mRightSiblingKeyStack.pop();
//      }
//      if (!mRightSiblingKeyStack.isEmpty()) {
//        mNextKey = mRightSiblingKeyStack.pop();
//        moveToNextFollowing();
//      } else {
//        processEndOfTraversal();
//      }
//    }
//  }
//
//  /** Process right sibling. */
//  private void processRightSibling() {
//    mAngle += mExtension;
//    mMoved = EMoved.STARTRIGHTSIBL;
//  }
//
//  /** Process anchestor sibling. */
//  private void processAnchSibl() {
//    // Next node will be a right sibling of an anchestor node or the traversal ends.
//    moveToNextFollowing();
//  }
//
//  private void moveToNextFollowing() {
//    mMoved = EMoved.ANCHESTSIBL;
//    if (mDiff != EDiff.DELETED && mDiff != EDiff.MOVEDFROM && mIndex + 1 + mPrunedNodes < mDiffs.size()
//      && mDiffs.get(mIndex + 1 + mPrunedNodes).getDiff() != EDiff.DELETED
//      && mDiffs.get(mIndex + 1 + mPrunedNodes).getDiff() != EDiff.MOVEDFROM) {
//      /*
//       * Only move to next node if next diff is not a delete, because in deletes it moves itself
//       * to the next node. This has been done because deletes can occur some depths/levels above
//       * but between the next node and the current node.
//       */
//      moveToNext();
//    }
//  }
//
//  /**
//   * Process diff if current diff is of kind {@link EDiff#DELETED}.
//   * 
//   * @return {@code true}, if current diff is the first {@link EDiff#DELETED} after any other diff,
//   *         {@code false} otherwise
//   */
//  private boolean processDeletions() {
//    if ((mDiff == EDiff.DELETED || mDiff == EDiff.MOVEDFROM) && mLastDiff != EDiff.DELETED
//      && mLastDiff != EDiff.MOVEDFROM) {
//      // if (mLastDiff != EDiff.MOVEDCUT) {
//      mNewRtx = getTransaction();
//      mOldRtx.moveTo(mDiffCont.getOldNodeKey());
//      // }
//
//      moveToNextNode();
//      mOldRtx.moveTo(mDiffCont.getOldNodeKey());
//      setTransaction(mOldRtx);
//      // if (mLastDiff != EDiff.MOVEDCUT) {
//      mTempNextKey = mNextKey;
//      mTempRightSiblingKeyStack = mRightSiblingKeyStack;
//      // }
//      mRightSiblingKeyStack = new ArrayDeque<Long>();
//
//      processMove();
//      calculateDepth();
//
//      mExtension = mModel.createSunburstItem(mItem, mDepth, mIndex);
//
//      if (getTransaction().getStructuralNode().hasFirstChild()) {
//        mDiffStack.push(mModificationCount);
//        mAngleStack.push(mAngle);
//        mExtensionStack.push(mExtension);
//        mParentStack.push(mIndex);
//        mDescendantsStack.push(mDescendantCount);
//        mMoved = EMoved.CHILD;
//        mDepth++;
//      } else if (getTransaction().getStructuralNode().hasRightSibling()) {
//        mAngle += mExtension;
//        mMoved = EMoved.STARTRIGHTSIBL;
//      } else {
//        mMoved = EMoved.ANCHESTSIBL;
//      }
//
//      if (mIndex + 1 + mPrunedNodes >= mDiffs.size()) {
//        mNextKey = (Long)EFixed.NULL_NODE_KEY.getStandardProperty();
//      }
//
//      mLastDiff = mDiff;
//      mLastDiffCont = mDiffCont;
//      return true;
//    } else if (mDiff != EDiff.DELETED && mDiff != EDiff.MOVEDFROM
//      && (mLastDiff == EDiff.DELETED || mLastDiff == EDiff.MOVEDFROM)) {
//      mRightSiblingKeyStack = mTempRightSiblingKeyStack;
//      mNextKey = mTempNextKey;
//
//      if (mMoved == EMoved.ANCHESTSIBL) {
//        boolean first = true;
//        long oldDepth = mLastDiffCont.getDepth().getOldDepth();
//        while (!getTransaction().getStructuralNode().hasRightSibling()
//          && getTransaction().getStructuralNode().hasParent()
//          && getTransaction().getNode().getNodeKey() != mNextKey
//          && oldDepth - mInitDepth > mDiffCont.getDepth().getNewDepth() - mInitDepth) {
//          if (first) {
//            // Do not pop from stack if it's a leaf node.
//            first = false;
//          } else {
//            mDiffStack.pop();
//            mAngleStack.pop();
//            mExtensionStack.pop();
//            mParentStack.pop();
//            mDescendantsStack.pop();
//          }
//
//          getTransaction().moveToParent();
//          mDepth--;
//          oldDepth--;
//        }
//      }
//
//      setTransaction(mNewRtx);
//    } else if (mDiff == EDiff.DELETED || mDiff == EDiff.MOVEDFROM) {
//      // Subsequent deletes.
//      mNextKey = mDiffCont.getOldNodeKey();
//
//      if (mMoved != EMoved.ANCHESTSIBL) {
//        getTransaction().moveTo(mNextKey);
//      }
//
//      // Modify stacks.
//      moveToNext();
//    }
//    return false;
//  }
//
//  /** Simulate move to next following node (must be first of EDiff.MOVEDCUT and EDiff.DELETED). */
//  private void moveToNextNode() {
//    int tmpDepth = mDepth == 0 ? mInitDepth : mLastDiffCont.getDepth().getNewDepth();
//    if (mMoved == EMoved.ANCHESTSIBL) {
//      if (tmpDepth - mInitDepth > mDiffCont.getDepth().getOldDepth() - mInitDepth) {
//        // Must be done on the transaction which is bound to the new revision.
//        // final long currNodeKey = getTransaction().getItem().getKey();
//        boolean first = true;
//        // while (!getTransaction().getStructuralNode().hasRightSibling()
//        // && getTransaction().getStructuralNode().hasParent()
//        while (tmpDepth - mInitDepth > mDiffCont.getDepth().getOldDepth() - mInitDepth) {
//          if (first) {
//            // Do not pop from stack if it's a leaf node.
//            first = false;
//          } else {
//            mDiffStack.pop();
//            mAngleStack.pop();
//            mExtensionStack.pop();
//            mParentStack.pop();
//            mDescendantsStack.pop();
//          }
//
//          tmpDepth--;
//          mDepth--;
//          // getTransaction().moveToParent();
//        }
//        // getTransaction().moveTo(currNodeKey);
//      } else {
//        mMoved = EMoved.STARTRIGHTSIBL;
//        mAngle += mExtension;
//      }
//    }
//  }
//
//  /**
//   * Simulate move to next node in document order to adapt stacks.
//   */
//  private void moveToNext() {
//    final long currNodeKey = getTransaction().getNode().getNodeKey();
//    boolean first = true;
//    while (!getTransaction().getStructuralNode().hasRightSibling()
//      && getTransaction().getStructuralNode().hasParent()
//      && getTransaction().getNode().getNodeKey() != mNextKey
//      && getTransaction().getNode().getKind() != ENode.ROOT_KIND) {
//      getTransaction().moveToParent();
//      mMoved = EMoved.ANCHESTSIBL;
//      if (first) {
//        // Do not pop from stack if it's a leaf node.
//        first = false;
//      } else {
//        mDiffStack.pop();
//        mAngleStack.pop();
//        mExtensionStack.pop();
//        mParentStack.pop();
//        mDescendantsStack.pop();
//      }
//      mDepth--;
//    }
//    getTransaction().moveTo(currNodeKey);
//  }
//
//  /**
//   * Calculates new depth for modifications and back to "normal" depth.
//   */
//  private void calculateDepth() {
//    if (mDiff != EDiff.SAME && mDiff != EDiff.SAMEHASH && mDepth <= mMaxDepth + 2) {
//      mDepth = mMaxDepth + 2;
//
//      final long nodeKey = getTransaction().getNode().getNodeKey();
//      if (mDiff == EDiff.DELETED || mDiff == EDiff.MOVEDFROM) {
//        getTransaction().moveTo(mDiffCont.getOldNodeKey());
//        final IStructNode node = getTransaction().getStructuralNode();
//        setTempKey(node);
//      } else {
//        getTransaction().moveTo(mDiffCont.getNewNodeKey());
//        final IStructNode node = getTransaction().getStructuralNode();
//        setTempKey(node);
//      }
//      getTransaction().moveTo(nodeKey);
//    } else if ((mDiff == EDiff.SAME || mDiff == EDiff.SAMEHASH) && mDiffCont.getNewNodeKey() == mTempKey) {
//      mDepth = mDiffCont.getDepth().getNewDepth() - mInitDepth;
//    }
//  }
//
//  /**
//   * Set temporal key which is the next following node of an {@code inserted}, {@code deleted} or
//   * {@code updated} node.
//   * 
//   * @param paramNode
//   *          node from which to get the next following node from
//   */
//  private void setTempKey(final IStructNode paramNode) {
//    if (paramNode.hasRightSibling()) {
//      mTempKey = paramNode.getRightSiblingKey();
//    } else {
//      final long key = paramNode.getNodeKey();
//      while (!getTransaction().getStructuralNode().hasRightSibling()
//        && getTransaction().getNode().getKind() != ENode.ROOT_KIND) {
//        getTransaction().moveToParent();
//      }
//      mTempKey = getTransaction().getStructuralNode().getRightSiblingKey();
//      getTransaction().moveTo(key);
//    }
//  }
//
//  /** Process movement. */
//  private void processMove() {
//    final Optional<Modification> optionalMod = getModification();
//    final Modification modification =
//      optionalMod.isPresent() == true ? optionalMod.get() : Modifications.emptyModification();
//    assert modification != Modifications.emptyModification() : "modification shouldn't be empty!";
//    mDescendantCount = modification.getDescendants();
//    mItem =
//      new Item.Builder(mAngle, mParExtension, mIndexToParent, mDepth).setDescendantCount(mDescendantCount)
//        .setParentDescendantCount(mParentDescendantCount).setModificationCount(
//          modification.getModifications() + mDescendantCount).setParentModificationCount(
//          mParentModificationCount).setSubtract(modification.isSubtract()).setDiff(mDiffCont).setNextDepth(
//          mIndex + mPrunedNodes + 2).build();
//    mMoved.processCompareMove(getTransaction(), mItem, mAngleStack, mExtensionStack, mDescendantsStack,
//      mParentStack, mDiffStack);
//    mModificationCount = mItem.mModificationCount;
//    mParentModificationCount = mItem.mParentModificationCount;
//    mDescendantCount = mItem.mDescendantCount;
//    mParentDescendantCount = mItem.mParentDescendantCount;
//    mAngle = mItem.mAngle;
//    mParExtension = mItem.mExtension;
//    mIndexToParent = mItem.mIndexToParent;
//    mIndex++;
//  }
//
//  /**
//   * Get next modification.
//   * 
//   * @return {@link Optional} reference which includes the {@link Modifcation} reference
//   */
//  private Optional<Modification> getModification() {
//    Modification modification = null;
//    if (mPrune == EPruning.NO || mPrune == EPruning.DIFF) {
//      try {
//        modification = mModificationsQueue.take().get();
//      } catch (final InterruptedException | ExecutionException e) {
//        LOGWRAPPER.error(e.getMessage(), e);
//      }
//    } else {
//      try {
//        modification = Modifications.getInstance(mIndex + 1 + mPrunedNodes, mDiffs).countDiffs();
//      } catch (final AbsTTException e) {
//        LOGWRAPPER.error(e.getMessage(), e);
//      }
//    }
//    return Optional.fromNullable(modification);
//  }
//
//  @Override
//  public void decrementIndex() {
//    mIndex--;
//    mPrunedNodes++;
//  }
//
//  @Override
//  public int getDescendantCount() {
//    return mDescendantCount;
//  }
//
//  @Override
//  public int getPrunedNodes() {
//    return mPrunedNodes;
//  }
//
//  @Override
//  public int getModificationCount() {
//    return mModificationCount;
//  }
//}
