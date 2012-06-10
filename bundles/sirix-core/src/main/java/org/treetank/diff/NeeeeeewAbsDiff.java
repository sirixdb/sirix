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

package org.treetank.diff;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.treetank.access.NodeWriteTrx.EHashKind;
import org.treetank.api.IAxis;
import org.treetank.api.INodeReadTrx;
import org.treetank.axis.DescendantAxis;
import org.treetank.axis.EIncludeSelf;
import org.treetank.diff.DiffFactory.Builder;
import org.treetank.diff.DiffFactory.EDiff;
import org.treetank.diff.DiffFactory.EDiffOptimized;
import org.treetank.exception.AbsTTException;
import org.treetank.node.ENode;
import org.treetank.node.interfaces.IStructNode;

/**
 * Abstract diff class which implements common functionality.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
@Nonnull
abstract class NeeeeeewAbsDiff extends AbsDiffObservable {

  /** Determines transaction movement. */
  private enum EMove {
    /** To the following node (next node in the following-axis). */
    FOLLOWING,

    /** Next node in document order. */
    DOCUMENT_ORDER
  }

  /** Determines if a diff should be fired or not. */
  private enum EFireDiff {
    /** Yes, it should be fired. */
    TRUE,

    /** No, it shouldn't be fired. */
    FALSE
  }

  /** Determines the current revision. */
  private enum ERevision {
    /** Old revision. */
    OLD,

    /** New revision. */
    NEW;
  }

  /**
   * Kind of hash method.
   * 
   * @see EHashKind
   */
  private transient EHashKind mHashKind;

  /**
   * Kind of difference.
   * 
   * @see EDiff
   */
  private transient EDiff mDiff;

  /** Diff kind. */
  private transient EDiffOptimized mDiffKind;

  /** {@link DepthCounter} instance. */
  private transient DepthCounter mDepth;

  /** Root key. */
  private transient long mRootKey;

  /** Root key of old revision. */
  private transient long mOldRootKey;

  /**
   * Determines if {@link INodeReadTrx} on newer revision moved to the
   * node denoted by {@code mNewStartKey}.
   */
  private transient boolean mNewRtxMoved;

  /**
   * Determines if {@link INodeReadTrx} on older revision moved to the
   * node denoted by {@code mOldStartKey}.
   */
  private transient boolean mOldRtxMoved;

  /** Determines if the GUI uses the algorithm or not. */
  private transient boolean mIsGUI;

  /** Captures {@link DiffTuple}s for the detection of replaces. */
  private transient List<DiffTuple> mDiffTuples;

  /** Captures diff types of subtrees for the detection of replaces. */
  private transient List<EDiff> mDiffs;

  /** {@link INodeReadTrx} on new revision. */
  private final INodeReadTrx mNewRtx;

  /** {@link INodeReadTrx} on old revision. */
  private final INodeReadTrx mOldRtx;

  /**
   * Constructor.
   * 
   * @param pBuilder
   *          {@link Builder} reference
   * @throws AbsTTException
   *           if setting up transactions failes
   */
  NeeeeeewAbsDiff(final Builder pBuilder) throws AbsTTException {
    mDiffKind = checkNotNull(pBuilder).mKind;
    synchronized (pBuilder.mSession) {
      mNewRtx = pBuilder.mSession.beginNodeReadTrx(pBuilder.mNewRev);
      mOldRtx = pBuilder.mSession.beginNodeReadTrx(pBuilder.mOldRev);
      mHashKind = pBuilder.mHashKind;
    }
    mNewRtxMoved = mNewRtx.moveTo(pBuilder.mNewStartKey);
    mOldRtxMoved = mOldRtx.moveTo(pBuilder.mOldStartKey);
    if (mNewRtx.getNode().getKind() == ENode.ROOT_KIND) {
      mNewRtx.moveToFirstChild();
    }
    if (mOldRtx.getNode().getKind() == ENode.ROOT_KIND) {
      mOldRtx.moveToFirstChild();
    }
    mRootKey = pBuilder.mNewStartKey;
    mOldRootKey = pBuilder.mOldStartKey;

    synchronized (pBuilder.mObservers) {
      for (final IDiffObserver observer : pBuilder.mObservers) {
        addObserver(observer);
      }
    }
    mDiff = EDiff.SAME;
    mDiffKind = pBuilder.mKind;
    mDepth = new DepthCounter(pBuilder.mNewDepth, pBuilder.mOldDepth);
    mIsGUI = pBuilder.mIsGUI;
    mDiffTuples = new ArrayList<>();
    mDiffs = new ArrayList<>(2);
  }

  /**
   * Do the diff.
   * 
   * @throws AbsTTException
   *           if setting up transactions failes
   */
  void diffMovement() throws AbsTTException {
    assert mHashKind != null;
    assert mNewRtx != null;
    assert mOldRtx != null;
    assert mDiff != null;
    assert mDiffKind != null;

    if (!mNewRtxMoved) {
      fireDeletes();
      if (!mIsGUI || mDepth.getNewDepth() == 0) {
        fireInserts();
      }
      done();
      return;
    }
    if (!mOldRtxMoved) {
      fireInserts();
      if (!mIsGUI || mDepth.getOldDepth() == 0) {
        fireDeletes();
      }
      done();
      return;
    }

    // Check first node.
    if (mHashKind == EHashKind.None || mDiffKind == EDiffOptimized.NO) {
      mDiff = diff(mNewRtx, mOldRtx, mDepth, EFireDiff.TRUE);
    } else {
      mDiff = optimizedDiff(mNewRtx, mOldRtx, mDepth, EFireDiff.TRUE);
    }

    // Iterate over new revision (order of operators significant -- regarding the OR).
    if (mDiff != EDiff.SAMEHASH) {
      while ((mOldRtx.getNode().getKind() != ENode.ROOT_KIND && mDiff == EDiff.DELETED)
        || moveCursor(mNewRtx, ERevision.NEW, EMove.FOLLOWING)) {
        if (mDiff != EDiff.INSERTED) {
          moveCursor(mOldRtx, ERevision.OLD, EMove.FOLLOWING);
        }

        if (mNewRtx.getNode().getKind() != ENode.ROOT_KIND || mOldRtx.getNode().getKind() != ENode.ROOT_KIND) {
          if (mHashKind == EHashKind.None || mDiffKind == EDiffOptimized.NO) {
            mDiff = diff(mNewRtx, mOldRtx, mDepth, EFireDiff.TRUE);
          } else {
            mDiff = optimizedDiff(mNewRtx, mOldRtx, mDepth, EFireDiff.TRUE);
          }
        }
      }

      // Nodes deleted in old rev at the end of the tree.
      if (mOldRtx.getNode().getKind() != ENode.ROOT_KIND) {
        mRootKey = mOldRootKey;
        // First time it might be EDiff.INSERTED where the cursor doesn't move.
        if (mDiff == EDiff.INSERTED) {
          mDiff = EDiff.DELETED;
          fireDiff(mDiff, mNewRtx.getStructuralNode(), mOldRtx.getStructuralNode(), new DiffDepth(mDepth
            .getNewDepth(), mDepth.getOldDepth()));
        }
        boolean moved = true;
        if (mDiffKind == EDiffOptimized.HASHED && mDiff == EDiff.SAMEHASH) {
          moved = moveToFollowingNode(mOldRtx, ERevision.OLD);
          if (moved) {
            mDiff = EDiff.DELETED;
            fireDiff(mDiff, mNewRtx.getStructuralNode(), mOldRtx.getStructuralNode(), new DiffDepth(mDepth
              .getNewDepth(), mDepth.getOldDepth()));
          }
        }
        if (moved) {
          while (moveCursor(mOldRtx, ERevision.OLD, EMove.DOCUMENT_ORDER)) {
            mDiff = EDiff.DELETED;
            fireDiff(mDiff, mNewRtx.getStructuralNode(), mOldRtx.getStructuralNode(), new DiffDepth(mDepth
              .getNewDepth(), mDepth.getOldDepth()));
          }
        }
      }
    }

    diffDone();
  }

  /**
   * Done processing diffs. Fire remaining diffs and signal that the algorithm is done.
   * 
   * @throws AbsTTException
   *           if Treetank fails to close the transactions
   */
  private void diffDone() throws AbsTTException {
    fireTuples();
    mNewRtx.close();
    mOldRtx.close();
    done();
  }

  /**
   * Fire {@code EDiff.DELETEs} for the whole subtree.
   */
  private void fireDeletes() {
    fireDiff(EDiff.DELETED, mNewRtx.getStructuralNode(), mOldRtx.getStructuralNode(), new DiffDepth(mDepth
      .getNewDepth(), mDepth.getOldDepth()));
    while (moveCursor(mOldRtx, ERevision.OLD, EMove.DOCUMENT_ORDER)) {
      fireDiff(EDiff.DELETED, mNewRtx.getStructuralNode(), mOldRtx.getStructuralNode(), new DiffDepth(mDepth
        .getNewDepth(), mDepth.getOldDepth()));
    }
  }

  /**
   * Fire {@code EDiff.INSERTs} for the whole subtree.
   */
  private void fireInserts() {
    fireDiff(EDiff.INSERTED, mNewRtx.getStructuralNode(), mOldRtx.getStructuralNode(), new DiffDepth(mDepth
      .getNewDepth(), mDepth.getOldDepth()));
    while (moveCursor(mNewRtx, ERevision.NEW, EMove.DOCUMENT_ORDER)) {
      fireDiff(EDiff.INSERTED, mNewRtx.getStructuralNode(), mOldRtx.getStructuralNode(), new DiffDepth(mDepth
        .getNewDepth(), mDepth.getOldDepth()));
    }
  }

  /**
   * Move cursor one node forward in pre order.
   * 
   * @param pRtx
   *          the {@link INodeReadTrx} to use
   * @param pRevision
   *          the {@link ERevision} constant
   * @return {@code true}, if cursor moved, {@code false} otherwise, if no nodes follow in document order
   */
  private boolean moveCursor(final INodeReadTrx pRtx, final ERevision pRevision, final EMove pMove) {
    assert pRtx != null;
    assert pRevision != null;

    boolean moved = false;

    if (pRtx.getNode().getKind() != ENode.ROOT_KIND) {
      switch (mDiff) {
      case SAME:
      case SAMEHASH:
      case UPDATED:
        moved = moveToNext(pRtx, pRevision);
        break;
      case REPLACED:
        moved = moveToFollowingNode(pRtx, pRevision);
        break;
      case INSERTED:
      case DELETED:
        if (pMove == EMove.FOLLOWING && (mDiff == EDiff.INSERTED || mDiff == EDiff.DELETED)) {
          if (pRtx.getNode().getKind() == ENode.ROOT_KIND) {
            moved = false;
          } else {
            moved = true;
          }
        } else {
          moved = moveToNext(pRtx, pRevision);
        }
        break;
      default:
      }
    }

    return moved;
  }

  private boolean moveToNext(final INodeReadTrx pRtx, final ERevision pRevision) {
    boolean moved = false;
    final IStructNode node = pRtx.getStructuralNode();
    if (node.hasFirstChild()) {
      if (node.getKind() != ENode.ROOT_KIND && mDiffKind == EDiffOptimized.HASHED && mDiff == EDiff.SAMEHASH) {
        moved = pRtx.moveToRightSibling();

        if (!moved) {
          moved = moveToFollowingNode(pRtx, pRevision);
        }
      } else {
        moved = pRtx.moveToFirstChild();

        if (moved) {
          switch (pRevision) {
          case NEW:
            mDepth.incrementNewDepth();
            break;
          case OLD:
            mDepth.incrementOldDepth();
            break;
          }
        }
      }
    } else if (node.hasRightSibling()) {
      if (pRtx.getNode().getNodeKey() == mRootKey) {
        pRtx.moveToDocumentRoot();
      } else {
        moved = pRtx.moveToRightSibling();
      }
    } else {
      moved = moveToFollowingNode(pRtx, pRevision);
    }
    return moved;
  }

  /**
   * Move to next following node.
   * 
   * @param pRtx
   *          the {@link INodeReadTrx} to use
   * @param pRevision
   *          the {@link ERevision} constant
   * @return true, if cursor moved, false otherwise
   */
  private boolean moveToFollowingNode(final INodeReadTrx pRtx, final ERevision pRevision) {
    boolean moved = false;
    while (!pRtx.getStructuralNode().hasRightSibling() && pRtx.getStructuralNode().hasParent()
      && pRtx.getNode().getNodeKey() != mRootKey) {
      moved = pRtx.moveToParent();
      if (moved) {
        switch (pRevision) {
        case NEW:
          mDepth.decrementNewDepth();
          break;
        case OLD:
          mDepth.decrementOldDepth();
          break;
        }
      }
    }

    if (pRtx.getNode().getNodeKey() == mRootKey) {
      pRtx.moveToDocumentRoot();
    }

    moved = pRtx.moveToRightSibling();
    return moved;
  }

  /**
   * Diff of nodes.
   * 
   * @param pNewRtx
   *          {@link INodeReadTrx} on new revision
   * @param pOldRtx
   *          {@link INodeReadTrx} on old revision
   * @param pDepth
   *          {@link DepthCounter} container for current depths of both transaction cursors
   * @param paramFireDiff
   *          determines if a diff should be fired
   * @return kind of difference
   */
  EDiff diff(final INodeReadTrx pNewRtx, final INodeReadTrx pOldRtx, final DepthCounter pDepth,
    final EFireDiff paramFireDiff) {
    assert pNewRtx != null;
    assert pOldRtx != null;
    assert pDepth != null;

    EDiff diff = EDiff.SAME;

    // Check for modifications.
    switch (pNewRtx.getNode().getKind()) {
    case ROOT_KIND:
    case TEXT_KIND:
    case ELEMENT_KIND:
      if (checkNodes(pNewRtx, pOldRtx)) {
        if (checkReplace(pNewRtx, pOldRtx)) {
          diff = EDiff.REPLACED;
        } else {
          fireTuples();
          fireDiff(diff, pNewRtx.getStructuralNode(), pOldRtx.getStructuralNode(), new DiffDepth(pDepth
            .getNewDepth(), pDepth.getOldDepth()));
        }
      } else {
        diff = diffType(pNewRtx, pOldRtx, pDepth);
      }
      break;
    default:
      // Do nothing.
    }

    return diff;
  }

  /**
   * Optimized diff, which skips unnecessary comparsions.
   * 
   * @param pNewRtx
   *          {@link INodeReadTrx} on new revision
   * @param pOldRtx
   *          {@link INodeReadTrx} on old revision
   * @param pDepth
   *          {@link DepthCounter} container for current depths of both transaction cursors
   * @param paramFireDiff
   *          determines if a diff should be fired
   * @return kind of difference
   */
  EDiff optimizedDiff(final INodeReadTrx pNewRtx, final INodeReadTrx pOldRtx, final DepthCounter pDepth,
    final EFireDiff paramFireDiff) {
    assert pNewRtx != null;
    assert pOldRtx != null;
    assert pDepth != null;

    EDiff diff = EDiff.SAMEHASH;

    // Check for modifications.
    switch (pNewRtx.getNode().getKind()) {
    case ROOT_KIND:
    case TEXT_KIND:
    case ELEMENT_KIND:
      if (pNewRtx.getNode().getNodeKey() != pOldRtx.getNode().getNodeKey()
        || pNewRtx.getNode().getHash() != pOldRtx.getNode().getHash()) {
        // Check if nodes are the same (even if subtrees may vary).
        if (checkNodes(pNewRtx, pOldRtx)) {
          if (checkReplace(pNewRtx, pOldRtx)) {
            diff = EDiff.REPLACED;
          } else {
            diff = EDiff.SAME;
            fireTuples();
            fireDiff(diff, pNewRtx.getStructuralNode(), pOldRtx.getStructuralNode(), new DiffDepth(pDepth
              .getNewDepth(), pDepth.getOldDepth()));
          }
        } else {
          diff = diffType(pNewRtx, pOldRtx, pDepth);
        }
      } else {
        if (checkReplace(pNewRtx, pOldRtx)) {
          diff = EDiff.REPLACED;
        } else {
          fireTuples();
          fireDiff(diff, pNewRtx.getStructuralNode(), pOldRtx.getStructuralNode(), new DiffDepth(pDepth
            .getNewDepth(), pDepth.getOldDepth()));
        }
      }
      break;
    default:
      // Do nothing.
    }

    return diff;
  }

  /** Fire all tuples previously gathered. */
  private void fireTuples() {
    final long newKey = mNewRtx.getNode().getNodeKey();
    final long oldKey = mOldRtx.getNode().getNodeKey();
    for (final DiffTuple tuple : mDiffTuples) {
      mNewRtx.moveTo(tuple.getNewNodeKey());
      mOldRtx.moveTo(tuple.getOldNodeKey());
      fireDiff(tuple.getDiff(), mNewRtx.getStructuralNode(), mOldRtx.getStructuralNode(), tuple.getDepth());
    }
    mNewRtx.moveTo(newKey);
    mOldRtx.moveTo(oldKey);
    mDiffTuples = new ArrayList<>();
    mDiffs = new ArrayList<>();
  }

  /**
   * Main algorithm to compute diffs between two nodes.
   * 
   * @param pNewRtx
   *          {@link INodeReadTrx} on new revision
   * @param pOldRtx
   *          {@link INodeReadTrx} on old revision
   * @param pDepth
   *          {@link DepthCounter} container for current depths of both transaction cursors
   * @return kind of diff
   */
  private EDiff diffType(final INodeReadTrx pNewRtx, final INodeReadTrx pOldRtx,
    final DepthCounter pDepth) {
    assert pNewRtx != null;
    assert pOldRtx != null;
    assert pDepth != null;
    EDiff diff = EDiff.SAME;

    if (pDepth.getOldDepth() > pDepth.getNewDepth()) { // Check if node has been deleted.
      diff = EDiff.DELETED;
      cumulateDiffs(diff);
      if (checkReplace(pNewRtx, pOldRtx)) {
        diff = EDiff.REPLACED;
      }
    } else if (checkUpdate(pNewRtx, pOldRtx)) { // Check if node has been updated.
      diff = EDiff.UPDATED;
      fireDiff(diff, pNewRtx.getStructuralNode(), pOldRtx.getStructuralNode(), new DiffDepth(pDepth
        .getNewDepth(), pDepth.getOldDepth()));
    } else if (checkReplace(pNewRtx, pOldRtx)) { // Check if node has been replaced.
      diff = EDiff.REPLACED;
    } else {
      final long oldKey = pOldRtx.getNode().getNodeKey();
      final boolean movedOld = pOldRtx.moveTo(pNewRtx.getNode().getNodeKey());
      pOldRtx.moveTo(oldKey);
      
      final long newKey = pNewRtx.getNode().getNodeKey();
      final boolean movedNew = pNewRtx.moveTo(pOldRtx.getNode().getNodeKey());
      pNewRtx.moveTo(newKey);
      
      if (!movedOld) {
        diff = EDiff.INSERTED;
      } else if (!movedNew) {
        diff = EDiff.DELETED;
      } else {
        // Determine if one of the right sibling matches.
        EFoundEqualNode found = EFoundEqualNode.FALSE;

        while (pOldRtx.getStructuralNode().hasRightSibling() && pOldRtx.moveToRightSibling()
          && found == EFoundEqualNode.FALSE) {
          if (pNewRtx.getNode().getNodeKey() == pOldRtx.getNode().getNodeKey()) {
            found = EFoundEqualNode.TRUE;
            break;
          }
        }

        pOldRtx.moveTo(oldKey);
        diff = found.kindOfDiff();
      }

      cumulateDiffs(diff);
    }

    assert diff != EDiff.SAME;
    return diff;
  }

  /**
   * Emit diffs for {@code INSERTED} or {@code DELETED} nodes and traverse accordingly.
   * 
   * @param pDiff
   *          kind of diff
   */
  private void cumulateDiffs(final EDiff pDiff) {
    final ERevision revision = pDiff == EDiff.DELETED ? ERevision.OLD : ERevision.NEW;
    final int depth = pDiff == EDiff.DELETED ? mDepth.getOldDepth() : mDepth.getNewDepth();
    final INodeReadTrx rtx = pDiff == EDiff.DELETED ? mOldRtx : mNewRtx;

    mDiff = pDiff;
    mDiffs.add(pDiff);
    do {
      mDiffTuples.add(new DiffTuple(pDiff, mNewRtx.getStructuralNode().getNodeKey(), mOldRtx
        .getStructuralNode().getNodeKey(), new DiffDepth(mDepth.getNewDepth(), mDepth.getOldDepth())));
    } while (moveCursor(rtx, revision, EMove.DOCUMENT_ORDER)
      && ((pDiff == EDiff.INSERTED && mDepth.getNewDepth() > depth) || (pDiff == EDiff.DELETED && mDepth
        .getOldDepth() > depth)));
  }

  /**
   * Check {@link QName} of nodes.
   * 
   * @param pNewRtx
   *          {@link INodeReadTrx} on new revision
   * @param pOldRtx
   *          {@link INodeReadTrx} on old revision
   * @return true if nodes are "equal" according to their {@link QName}s, otherwise false
   */
  boolean checkName(final INodeReadTrx pNewRtx, final INodeReadTrx pOldRtx) {
    boolean found = false;
    if (pNewRtx.getNode().getKind() == pOldRtx.getNode().getKind()) {
      switch (pNewRtx.getNode().getKind()) {
      case ELEMENT_KIND:
        if (pNewRtx.getQNameOfCurrentNode().equals(pOldRtx.getQNameOfCurrentNode())) {
          found = true;
        }
        break;
      case TEXT_KIND:
        if (pNewRtx.getValueOfCurrentNode().equals(pOldRtx.getValueOfCurrentNode())) {
          found = true;
        }
        break;
      default:
      }
    }
    return found;
  }

  /**
   * Check if nodes are equal excluding subtrees.
   * 
   * @param pNewRtx
   *          {@link INodeReadTrx} on new revision
   * @param pOldRtx
   *          {@link INodeReadTrx} on old revision
   * @return true if nodes are "equal", otherwise false
   */
  abstract boolean checkNodes(final INodeReadTrx pNewRtx, final INodeReadTrx pOldRtx);

  /**
   * Check for a replace of a node.
   * 
   * @param pNewRtx
   *          first {@link INodeReadTrx} instance
   * @param pOldRtx
   *          second {@link INodeReadTrx} instance
   * @return true if node has been replaced, false otherwise
   */
  boolean checkReplace(final INodeReadTrx pNewRtx, final INodeReadTrx pOldRtx) {
    boolean replaced = false;

    if (mDiffs.size() == 2) {
      final EDiff first = mDiffs.get(0);
      final EDiff second = mDiffs.get(1);
      if ((first == EDiff.DELETED && second == EDiff.INSERTED)
        || (first == EDiff.INSERTED && second == EDiff.DELETED)) {
        final DiffTuple tuple = mDiffTuples.get(0);
        if (tuple.getDepth().getOldDepth() == tuple.getDepth().getNewDepth()) {
          replaced = true;
          mNewRtx.moveTo(tuple.getNewNodeKey());
          mOldRtx.moveTo(tuple.getOldNodeKey());
          final DiffDepth diffDepth = tuple.getDepth();
          mDepth = new DepthCounter(diffDepth.getNewDepth(), diffDepth.getOldDepth());
        }
        mDiffs = new ArrayList<>();
        mDiffTuples = new ArrayList<>();
      } else if (first == EDiff.INSERTED && second == EDiff.INSERTED) {
      } else {
        fireTuples();
      }
    } else if (mDiffs.size() == 4) {
      final EDiff first = mDiffs.get(0);
      final EDiff second = mDiffs.get(1);
      final EDiff third = mDiffs.get(2);
      final EDiff fourth = mDiffs.get(3);

      if (first == EDiff.INSERTED && second == EDiff.INSERTED && third == EDiff.DELETED
        && fourth == EDiff.DELETED) {
        final DiffTuple tuple = mDiffTuples.get(0);
        if (tuple.getDepth().getOldDepth() == tuple.getDepth().getNewDepth()) {
          replaced = true;
          mNewRtx.moveTo(tuple.getNewNodeKey());
          mOldRtx.moveTo(tuple.getOldNodeKey());
          final DiffDepth diffDepth = tuple.getDepth();
          mDepth = new DepthCounter(diffDepth.getNewDepth(), diffDepth.getOldDepth());
        }
        mDiffs = new ArrayList<>();
        mDiffTuples = new ArrayList<>();
      } else {
        fireTuples();
      }
    }

    if (replaced) {
      final long newNodeKey = pNewRtx.getNode().getNodeKey();
      final long oldNodeKey = pOldRtx.getNode().getNodeKey();
      final IAxis oldAxis = new DescendantAxis(pOldRtx, EIncludeSelf.YES);
      final IAxis newAxis = new DescendantAxis(pNewRtx, EIncludeSelf.YES);
      // if (pNewRtx.getStructuralNode().getDescendantCount() >= pOldRtx.getStructuralNode()
      // .getDescendantCount()) {
      // } else {
      while (oldAxis.hasNext()) {
        oldAxis.next();
        // boolean moved = false;
        // if (newAxis.hasNext()) {
        // newAxis.next();
        // moved = true;
        // }
        fireDiff(EDiff.REPLACEDOLD, pNewRtx.getStructuralNode(), pOldRtx.getStructuralNode(), new DiffDepth(
          mDepth.getNewDepth(), mDepth.getOldDepth()));
        adjustDepth(oldAxis.getTransaction(), oldNodeKey, ERevision.OLD);
        // if (moved) {
        // adjustDepth(newAxis.getTransaction(), newNodeKey, ERevision.NEW);
        // }
        // }
      }
      
      while (newAxis.hasNext()) {
        newAxis.next();
        // boolean moved = false;
        // if (oldAxis.hasNext()) {
        // oldAxis.next();
        // moved = true;
        // }
        fireDiff(EDiff.REPLACEDNEW, pNewRtx.getStructuralNode(), pOldRtx.getStructuralNode(), new DiffDepth(
          mDepth.getNewDepth(), mDepth.getOldDepth()));
        adjustDepth(newAxis.getTransaction(), newNodeKey, ERevision.NEW);
        // if (moved) {
        // adjustDepth(oldAxis.getTransaction(), oldNodeKey, ERevision.OLD);
        // }
      }
      pNewRtx.moveTo(newNodeKey);
      pOldRtx.moveTo(oldNodeKey);
      mDiff = EDiff.REPLACED;
    }
    return replaced;
  }

  /**
   * Adjust the depth.
   * 
   * @param pRtx
   *          the transaction to simulate moves
   * @param pStartNodeKey
   *          the start node key
   */
  private void adjustDepth(final INodeReadTrx pRtx, final long pStartNodeKey, final ERevision pRevision) {
    assert pRtx != null;
    assert pStartNodeKey >= 0;
    assert pRevision != null;
    final long nodeKey = pRtx.getNode().getNodeKey();
    if (pRtx.getStructuralNode().hasFirstChild()) {
      switch (pRevision) {
      case NEW:
        mDepth.incrementNewDepth();
        break;
      case OLD:
        mDepth.incrementOldDepth();
        break;
      }
    } else {
      while (!pRtx.getStructuralNode().hasRightSibling() && pRtx.getNode().hasParent()
        && pRtx.getNode().getNodeKey() != pStartNodeKey) {
        pRtx.moveToParent();
        switch (pRevision) {
        case NEW:
          mDepth.decrementNewDepth();
          break;
        case OLD:
          mDepth.decrementOldDepth();
          break;
        }
      }
    }
    pRtx.moveTo(nodeKey);
  }

  /**
   * Check for an update of a node.
   * 
   * @param pNewRtx
   *          first {@link INodeReadTrx} instance
   * @param pOldRtx
   *          second {@link INodeReadTrx} instance
   * @return kind of diff
   */
  boolean checkUpdate(final INodeReadTrx pNewRtx, final INodeReadTrx pOldRtx) {
    if (mOldRootKey == pOldRtx.getNode().getNodeKey() && mRootKey == pNewRtx.getNode().getNodeKey()) {
      return true;
    }
    return pNewRtx.getNode().getNodeKey() == pOldRtx.getNode().getNodeKey()
      && mDepth.getNewDepth() == mDepth.getOldDepth()
      && pNewRtx.getNode().getParentKey() == pOldRtx.getNode().getParentKey();
  }
}
