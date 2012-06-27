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

import static com.google.common.base.Preconditions.checkNotNull;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.LoggerFactory;
import org.sirix.api.INodeReadTrx;
import org.sirix.axis.AbsAxis;
import org.sirix.axis.EIncludeSelf;
import org.sirix.gui.view.model.interfaces.ITraverseModel;
import org.sirix.gui.view.sunburst.EMoved;
import org.sirix.gui.view.sunburst.EPruning;
import org.sirix.gui.view.sunburst.Item;
import org.sirix.settings.EFixed;
import org.sirix.utils.LogWrapper;
import processing.core.PConstants;

/**
 * Special Sunburst axis.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class SunburstDescendantAxis extends AbsAxis implements PropertyChangeListener {

  /** {@link LogWrapper}. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory
    .getLogger(SunburstDescendantAxis.class));

  /** Extension Deque. */
  private transient Deque<Float> mExtensionStack;

  /** Angle Deque. */
  private transient Deque<Float> mAngleStack;

  /** Parent Deque. */
  private transient Deque<Integer> mParentStack;

  /** Descendants Deque. */
  private transient Deque<Integer> mDescendantsStack;

  /** Determines movement of transaction. */
  private transient EMoved mMoved;

  /** Index to parent node. */
  private transient int mIndexToParent;

  /** Parent extension. */
  private transient float mExtension;

  /** Extension. */
  private transient float mChildExtension;

  /** Depth in the tree starting at 0. */
  private transient int mDepth;

  /** Parent descendant count. */
  private transient int mParDescendantCount;

  /** Descendant count. */
  private transient int mDescendantCount;

  /** Start angle. */
  private transient float mAngle;

  /** Current item indes. */
  private transient int mIndex;

  /** Current item. */
  private transient Item mItem;

  /** Deque for remembering next nodeKey in document order. */
  private transient Deque<Long> mRightSiblingKeyStack;

  /** The nodeKey of the next node to visit. */
  private transient long mNextKey;

  /** {@link ITraverseModel} which observes axis changes through a callback method. */
  private transient ITraverseModel mModel;

  /** {@link BlockingQueue} of {@link Future}s which hold the number of descendants. */
  private transient BlockingQueue<Future<Integer>> mDescendants;

  /** Determines if tree should be pruned or not. */
  private transient EPruning mPruning;

  /**
   * Constructor initializing internal state.
   * 
   * @param paramRtx
   *          exclusive (immutable) trx to iterate with
   * @param paramIncludeSelf
   *          determines if self is included
   * @param paramModel
   *          model which observes axis changes
   * @param paramTraverseModel
   *          model
   * @param paramPruning
   */
  public SunburstDescendantAxis(final INodeReadTrx paramRtx, final EIncludeSelf paramIncludeSelf,
    final ITraverseModel paramTraverseModel, final EPruning paramPruning) {
    super(paramRtx, paramIncludeSelf);
    assert paramRtx != null;
    assert paramTraverseModel != null;
    assert paramPruning != null;
    mPruning = paramPruning;
    mModel = paramTraverseModel;
    mModel.addPropertyChangeListener(this);
  }

  @Override
  public void reset(final long pNodeKey) {
    super.reset(pNodeKey);
    mRightSiblingKeyStack = new ArrayDeque<Long>();
    if (isSelfIncluded() == EIncludeSelf.YES) {
      mNextKey = getTransaction().getNode().getNodeKey();
    } else {
      mNextKey = getTransaction().getStructuralNode().getFirstChildKey();
    }
    mExtensionStack = new ArrayDeque<Float>();
    mAngleStack = new ArrayDeque<Float>();
    mParentStack = new ArrayDeque<Integer>();
    mDescendantsStack = new ArrayDeque<Integer>();
    mDescendants = new LinkedBlockingQueue<Future<Integer>>();
    mAngle = 0F;
    mDepth = 0;
    mMoved = EMoved.STARTRIGHTSIBL;
    mIndexToParent = -1;
    mExtension = PConstants.TWO_PI;
    mChildExtension = PConstants.TWO_PI;
    mIndex = -1;
    mDescendantCount = (int)getTransaction().getStructuralNode().getDescendantCount() + 1;
    mParDescendantCount = mDescendantCount;
  }

  @Override
  public boolean hasNext() {
    if (isNext()) {
      return true;
    }
    
    resetToLastKey();

    // Fail if there is no node anymore.
    if (mNextKey == EFixed.NULL_NODE_KEY.getStandardProperty()) {
      resetToStartKey();
      return false;
    }

    getTransaction().moveTo(mNextKey);

    // Fail if the subtree is finished.
    if (getTransaction().getStructuralNode().getLeftSiblingKey() == getStartKey()) {
      resetToStartKey();
      return false;
    }

    // Always follow first child if there is one.
    if (getTransaction().getStructuralNode().hasFirstChild()) {
      mDescendantCount = (int)(getTransaction().getStructuralNode().getDescendantCount() + 1);// mDescendants.take().get();
      if (mDescendantCount == ITraverseModel.DESCENDANTS_DONE) {
        resetToStartKey();
        return false;
      } else {
        processMove();
        mChildExtension = mModel.createSunburstItem(mItem, mDepth, mIndex);

        if (mPruning == EPruning.DEPTH && mDepth + 1 >= ITraverseModel.DEPTH_TO_PRUNE) {
          return processPruned();
        } else {
          mNextKey = getTransaction().getStructuralNode().getFirstChildKey();
          if (getTransaction().getStructuralNode().hasRightSibling()) {
            mRightSiblingKeyStack.push(getTransaction().getStructuralNode().getRightSiblingKey());
          }
          mAngleStack.push(mAngle);
          mExtensionStack.push(mChildExtension);
          mParentStack.push(mIndex);
          mDescendantsStack.push(mDescendantCount);
          mDepth++;
          mMoved = EMoved.CHILD;
        }
        return true;
      }
    }

    // Then follow right sibling if there is one.
    if (getTransaction().getStructuralNode().hasRightSibling()) {
      mDescendantCount = (int)(getTransaction().getStructuralNode().getDescendantCount() + 1);// mDescendants.take().get();
      if (mDescendantCount == ITraverseModel.DESCENDANTS_DONE) {
        resetToStartKey();
        return false;
      } else {
        processMove();
        mChildExtension = mModel.createSunburstItem(mItem, mDepth, mIndex);

        // Next node is a right sibling.
        nextRightSibling();
        return true;
      }
    }

    // Then follow right sibling on Deque.
    if (!mRightSiblingKeyStack.isEmpty()) {
      mDescendantCount = (int)(getTransaction().getStructuralNode().getDescendantCount() + 1);// mDescendants.take().get();
      if (mDescendantCount == ITraverseModel.DESCENDANTS_DONE) {
        resetToStartKey();
        return false;
      } else {
        processMove();
        mChildExtension = mModel.createSunburstItem(mItem, mDepth, mIndex);

        // Next node will be a right sibling of an anchestor node or the traversal ends.
        nextFollowing();
        return true;
      }
    }

    // Then end.
    mDescendantCount = (int)(getTransaction().getStructuralNode().getDescendantCount() + 1);// mDescendants.take().get();
    if (mDescendantCount == ITraverseModel.DESCENDANTS_DONE) {
      resetToStartKey();
      return false;
    } else {
      processMove();
      mChildExtension = mModel.createSunburstItem(mItem, mDepth, mIndex);
      mNextKey = (Long)EFixed.NULL_NODE_KEY.getStandardProperty();
      return true;
    }
  }

  /**
   * Process for next right sibling.
   */
  private void nextRightSibling() {
    mNextKey = getTransaction().getStructuralNode().getRightSiblingKey();
    mAngle += mChildExtension;
    mMoved = EMoved.STARTRIGHTSIBL;
  }

  /**
   * Process for next following node which is a right sibling of an anchestor nore.
   */
  private void nextFollowing() {
    assert !mRightSiblingKeyStack.isEmpty();
    mNextKey = mRightSiblingKeyStack.pop();
    mMoved = EMoved.ANCHESTSIBL;
    final long currNodeKey = getTransaction().getNode().getNodeKey();
    boolean first = true;
    while (!getTransaction().getStructuralNode().hasRightSibling()
      && getTransaction().getStructuralNode().hasParent()
      && getTransaction().getNode().getNodeKey() != mNextKey) {
      if (first) {
        // Do not pop from Deque if it's a leaf node.
        first = false;
      } else {
        mAngleStack.pop();
        mExtensionStack.pop();
        mParentStack.pop();
        mDescendantsStack.pop();
      }

      getTransaction().moveToParent();
      mDepth--;
    }
    getTransaction().moveTo(currNodeKey);
  }

  /**
   * Process pruned node.
   */
  private boolean processPruned() {
    if (getTransaction().getStructuralNode().hasRightSibling()) {
      nextRightSibling();
      return true;
    } else if (!mRightSiblingKeyStack.isEmpty()) {
      nextFollowing();
      return true;
    } else {
      resetToStartKey();
      return false;
    }
  }

  /**
   * Process movement.
   */
  private void processMove() {
    mItem =
      new Item.Builder(mAngle, mExtension, mIndexToParent, mDepth, mDepth).setParentDescendantCount(
        mParDescendantCount).setDescendantCount(mDescendantCount).build();
    mMoved
      .processMove(getTransaction(), mItem, mAngleStack, mExtensionStack, mParentStack, mDescendantsStack);
    mAngle = mItem.mAngle;
    mExtension = mItem.mExtension;
    mIndexToParent = mItem.mIndexToParent;
    mParDescendantCount = mItem.mParentDescendantCount;
    mDescendantCount = mItem.mDescendantCount;
    mIndex++;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void propertyChange(final PropertyChangeEvent paramEvent) {
    checkNotNull(paramEvent);

    if ("maxDescendantCount".equals(paramEvent.getPropertyName())) {
      mParDescendantCount = (Integer)paramEvent.getNewValue();
      mDescendantCount = mParDescendantCount;
    } else if ("descendants".equals(paramEvent.getPropertyName())) {
      try {
        mDescendants.put((Future<Integer>)paramEvent.getNewValue());
      } catch (final InterruptedException e) {
        LOGWRAPPER.error(e.getMessage(), e);
      }
    }
  }
}
