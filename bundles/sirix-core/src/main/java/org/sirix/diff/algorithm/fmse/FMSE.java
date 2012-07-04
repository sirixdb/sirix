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
package org.sirix.diff.algorithm.fmse;

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.base.Optional;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.namespace.QName;

import org.slf4j.LoggerFactory;
import org.sirix.access.PageWriteTrx;
import org.sirix.api.IAxis;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.INodeWriteTrx;
import org.sirix.api.visitor.IVisitor;
import org.sirix.axis.AbsAxis;
import org.sirix.axis.ChildAxis;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.EIncludeSelf;
import org.sirix.axis.LevelOrderAxis;
import org.sirix.axis.LevelOrderAxis.EIncludeNodes;
import org.sirix.axis.PostOrderAxis;
import org.sirix.axis.VisitorDescendantAxis;
import org.sirix.diff.algorithm.IImportDiff;
import org.sirix.exception.AbsTTException;
import org.sirix.exception.TTUsageException;
import org.sirix.node.ENode;
import org.sirix.node.ElementNode;
import org.sirix.node.TextNode;
import org.sirix.node.interfaces.INode;
import org.sirix.utils.LogWrapper;
import org.sirix.utils.Pair;

/**
 * Provides the fast match / edit script (fmes) tree to tree correction
 * algorithm as described in "Change detection in hierarchically structured
 * information" by S. Chawathe, A. Rajaraman, H. Garcia-Molina and J. Widom
 * Stanford University, 1996 ([CRGMW95]) <br>
 * FMES is used by the <a href="http://www.logilab.org/projects/xmldiff">python
 * script</a> xmldiff from Logilab. <br>
 * 
 * Based on the FMES version of Daniel Hottinger and Franziska Meyer.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class FMSE implements IImportDiff, AutoCloseable {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory
    .getLogger(FMSE.class));

  /** Determines if a reverse lookup has to be made. */
  enum EReverseMap {
    /** Yes, reverse lookup. */
    TRUE,

    /** No, normal lookup. */
    FALSE
  }

  /** Algorithm name. */
  private static final String NAME = "Fast Matching / Edit Script";

  /**
   * Matching Criterion 1. For the "good matching problem", the following
   * conditions must hold for leafs x and y:
   * <ul>
   * <li>label(x) == label(y)</li>
   * <li>compare(value(x), value(y)) <= FMESF</li>
   * </ul>
   * where FMESF is in the range [0,1] and compare() computes the cost of
   * updating a leaf node.
   */
  private static final double FMESF = 0.5;

  /**
   * Matching Criterion 2. For the "good matching problem", the following
   * conditions must hold inner nodes x and y:
   * <ul>
   * <li>label(x) == label(y)</li>
   * <li>|common(x,y)| / max(|x|, |y|) > FMESTHRESHOLD</li>
   * </ul>
   * where FMESTHRESHOLD is in the range [0.5, 1] and common(x,y) computes the
   * number of leafs that can be matched between x and y.
   */
  private static final double FMESTHRESHOLD = 0.5;

  /**
   * Used by emitInsert: when inserting a whole subtree - keep track that
   * nodes are not inserted multiple times.
   */
  private transient Map<Long, Boolean> mAlreadyInserted;

  /**
   * This is the matching M between nodes as described in the paper.
   */
  private transient Matching mFastMatching;

  /**
   * This is the total matching M' between nodes as described in the paper.
   */
  private transient Matching mTotalMatching;

  /**
   * Stores the in-order property for each node for old revision.
   */
  private transient Map<Long, Boolean> mInOrderOldRev;

  /**
   * Stores the in-order property for each node for new revision.
   */
  private transient Map<Long, Boolean> mInOrderNewRev;

  /**
   * Number of descendants in subtree of node on old revision.
   */
  private transient Map<Long, Long> mDescendantsOldRev;

  /**
   * Number of descendants in subtree of node on new revision.
   */
  private transient Map<Long, Long> mDescendantsNewRev;

  /** {@link IVisitor} implementation on old revision. */
  private transient IVisitor mOldRevVisitor;

  /** {@link IVisitor} implementation on new revision. */
  private transient IVisitor mNewRevVisitor;

  /** {@link IVisitor} implementation to collect label/nodes on old revision. */
  private transient LabelFMSEVisitor mLabelOldRevVisitor;

  /** {@link IVisitor} implementation to collect label/nodes on new revision. */
  private transient LabelFMSEVisitor mLabelNewRevVisitor;

  /** sirix {@link INodeWriteTrx}. */
  private transient INodeWriteTrx mWtx;

  /** sirix {@link INodeReadTrx}. */
  private transient INodeReadTrx mRtx;

  /** Start key of old revision. */
  private transient long mOldStartKey;

  /** Start key of new revision. */
  private transient long mNewStartKey;

  /** Max length for Levenshtein comparsion. */
  private static final int MAX_LENGTH = 50;

  /** {@code Levenshtein} reference, can be reused among instances. */
  private static final Levenshtein mLevenshtein = new Levenshtein();

  @Override
  public void diff(final INodeWriteTrx pWtx, final INodeReadTrx pRtx)
    throws AbsTTException {
    mWtx = checkNotNull(pWtx);
    mRtx = checkNotNull(pRtx);
    mOldStartKey = mWtx.getNode().getNodeKey();
    mNewStartKey = mRtx.getNode().getNodeKey();
    mDescendantsOldRev = new HashMap<>();
    mDescendantsNewRev = new HashMap<>();
    mInOrderOldRev = new HashMap<>();
    mInOrderNewRev = new HashMap<>();
    mAlreadyInserted = new HashMap<>();

    mOldRevVisitor = new FMSEVisitor(mWtx, mInOrderOldRev, mDescendantsOldRev);
    mNewRevVisitor = new FMSEVisitor(mRtx, mInOrderNewRev, mDescendantsNewRev);

    mLabelOldRevVisitor = new LabelFMSEVisitor(mWtx);
    mLabelNewRevVisitor = new LabelFMSEVisitor(mRtx);
    init(mWtx, mOldRevVisitor);
    init(mRtx, mNewRevVisitor);
    mFastMatching = fastMatch(mWtx, mRtx);
    mTotalMatching = new Matching(mFastMatching);
    firstFMESStep(mWtx, mRtx);
    try {
      secondFMESStep(mWtx, mRtx);
    } catch (final AbsTTException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  /**
   * First step of the edit script algorithm. Combines the update, insert,
   * align and move phases.
   * 
   * @param pWtx
   *          {@link INodeWriteTrx} implementation reference on old
   *          revisionso
   * @param pRtxn
   *          {@link INodeReadTrx} implementation reference o new
   *          revision
   */
  private void firstFMESStep(final INodeWriteTrx pWtx, final INodeReadTrx pRtx) {
    assert pWtx != null;
    assert pRtx != null;

    pWtx.moveTo(mOldStartKey);
    pRtx.moveTo(mNewStartKey);

    // 2. Iterate over new shreddered file
    for (final IAxis axis =
      new LevelOrderAxis(pRtx, EIncludeNodes.NONSTRUCTURAL, EIncludeSelf.YES); axis
      .hasNext();) {
      axis.next();
      final INode node = axis.getTransaction().getNode();
      final long nodeKey = node.getNodeKey();
      doFirstFSMEStep(pWtx, pRtx);
      axis.getTransaction().moveTo(nodeKey);
    }
  }

  /**
   * Do the actual first step of FSME.
   * 
   * @param pWtx
   *          {@link INodeWriteTrx} implementation reference on old
   *          revision
   * @param pRtx
   *          {@link INodeReadTrx} implementation reference on new
   *          revision
   * @throws AbsTTException
   *           if anything in sirix fails
   */
  private void
    doFirstFSMEStep(final INodeWriteTrx pWtx, final INodeReadTrx pRtx) {
    assert pWtx != null;
    assert pRtx != null;
    // 2(a) - Parent of x.
    final long key = pRtx.getNode().getNodeKey();
    final long x = pRtx.getNode().getNodeKey();
    pRtx.moveToParent();
    final long y = pRtx.getNode().getNodeKey();

    final Long z = mTotalMatching.reversePartner(y);
    Long w = mTotalMatching.reversePartner(x);

    pWtx.moveTo(mOldStartKey);
    // 2(b) - insert
    if (w == null) {
      // x has no partner.
      assert z != null;
      mInOrderNewRev.put(x, true);
      final int k = findPos(x, pWtx, pRtx);
      assert k > -1;
      w = emitInsert(x, z, k, pWtx, pRtx);
    } else if (x != pWtx.getNode().getNodeKey()) {
      // 2(c) not the root (x has a partner in M').
      if (pWtx.moveTo(w)
        && pRtx.moveTo(x)
        && pWtx.getNode().getKind() == pRtx.getNode().getKind()
        && (!nodeValuesEqual(w, x, pWtx, pRtx) || (pRtx.getNode().getKind() == ENode.ATTRIBUTE_KIND && !pRtx
          .getValueOfCurrentNode().equals(pWtx.getValueOfCurrentNode())))) {
        // Either QNames differ or the values in case of attribute nodes.
        emitUpdate(w, x, pWtx, pRtx);
      }
      pWtx.moveTo(w);
      pWtx.moveToParent();
      final long v = pWtx.getNode().getNodeKey();
      if (!mTotalMatching.contains(v, y) && pWtx.moveTo(w) && pRtx.moveTo(x)) {
        assert z != null;
        mInOrderNewRev.put(x, true);
        pRtx.moveTo(x);
        if (pRtx.getNode().getKind() == ENode.NAMESPACE_KIND
          || pRtx.getNode().getKind() == ENode.ATTRIBUTE_KIND) {
          pWtx.moveTo(w);
          try {
            mTotalMatching.remove(w);
            pWtx.remove();
          } catch (final AbsTTException e) {
            LOGWRAPPER.error(e.getMessage(), e);
          }
          w = emitInsert(x, z, -1, pWtx, pRtx);
        } else {
          final int k = findPos(x, pWtx, pRtx);
          assert k > -1;
          emitMove(w, z, k, pWtx, pRtx);
        }
      }
    }

    alignChildren(w, x, pWtx, pRtx);
    pRtx.moveTo(key);
  }

  /**
   * Second step of the edit script algorithm. This is the delete phase.
   * 
   * @param pWtx
   *          {@link INodeWriteTrx} implementation reference on old
   *          revision
   * @param pRtx
   *          {@link INodeReadTrx} implementation reference on new
   *          revision
   */
  private void
    secondFMESStep(final INodeWriteTrx pWtx, final INodeReadTrx pRtx)
      throws AbsTTException {
    assert pWtx != null;
    assert pRtx != null;
    pWtx.moveTo(mOldStartKey);
    for (final IAxis axis =
      new VisitorDescendantAxis.Builder(pWtx).includeSelf().visitor(
        Optional.<DeleteFMSEVisitor> of(new DeleteFMSEVisitor(pWtx,
          mTotalMatching, mOldStartKey))).build(); axis.hasNext();) {
      axis.next();
    }
  }

  /**
   * Alignes the children of a node x according the the children of node w.
   * 
   * @param pW
   *          node in the first document
   * @param pX
   *          node in the second document
   * @param pWtx
   *          {@link INodeWriteTrx} implementation reference on old
   *          revision
   * @param pRtx
   *          {@link INodeReadTrx} implementation reference on new
   *          revision
   */
  private void alignChildren(final long pW, final long pX,
    final INodeWriteTrx pWtx, final INodeReadTrx pRtx) {
    assert pW >= 0;
    assert pX >= 0;
    assert pWtx != null;
    assert pRtx != null;

    pWtx.moveTo(pW);
    pRtx.moveTo(pX);

    // Mark all children of w and all children of x "out of order".
    markOutOfOrder(pWtx, mInOrderOldRev);
    markOutOfOrder(pRtx, mInOrderNewRev);

    // 2
    final List<Long> first =
      commonChildren(pW, pX, pWtx, pRtx, EReverseMap.FALSE);
    final List<Long> second =
      commonChildren(pX, pW, pRtx, pWtx, EReverseMap.TRUE);
    // 3 && 4
    final List<Pair<Long, Long>> s =
      Util.longestCommonSubsequence(first, second, new IComparator<Long>() {
        @Override
        public boolean isEqual(final Long pX, final Long pY) {
          return mTotalMatching.contains(pX, pY);
        }
      });
    // 5
    final Map<Long, Long> seen = new HashMap<>();
    for (final Pair<Long, Long> p : s) {
      mInOrderOldRev.put(p.getFirst(), true);
      mInOrderNewRev.put(p.getSecond(), true);
      seen.put(p.getFirst(), p.getSecond());
    }
    // 6
    for (final long a : first) {
      pWtx.moveTo(a);
      final Long b = mTotalMatching.partner(a);
      // assert b != null;
      if (seen.get(a) == null && pWtx.moveTo(a) && b != null && pRtx.moveTo(b)) { // (a, b)
                                                                                  // \notIn S
        // if (seen.get(a) == null || (seen.get(a) != null && !seen.get(a).equals(b))
        // && mInOrderOldRev.get(a) != null && !mInOrderOldRev.get(a)) { // (a, b) \notIn S
        mInOrderOldRev.put(a, true);
        mInOrderNewRev.put(b, true);
        final int k = findPos(b, pWtx, pRtx);
        LOGWRAPPER.debug("Move in align children: " + k);
        emitMove(a, pW, k, pWtx, pRtx);
      }
    }
  }

  /**
   * Mark children out of order.
   * 
   * @param pRtx
   *          {@link INodeReadTrx} reference
   * @param pInOrder
   *          {@link Map} to put all children out of order
   */
  private void markOutOfOrder(final INodeReadTrx pRtx,
    final Map<Long, Boolean> pInOrder) {
    for (final AbsAxis axis = new ChildAxis(pRtx); axis.hasNext();) {
      axis.next();
      pInOrder.put(axis.getTransaction().getNode().getNodeKey(), false);
    }
  }

  /**
   * The sequence of children of n whose partners are children of o. This is
   * used by alignChildren().
   * 
   * @param pN
   *          parent node in a document tree
   * @param pO
   *          corresponding parent node in the other tree
   * @param pFirstRtx
   *          {@link INodeReadTrx} on pN node
   * @param pSecondRtx
   *          {@link INodeReadTrx} on pO node
   * @param pReverse
   *          determines if reverse partners need to be found
   * @return {@link List} of common child nodes
   */
  private List<Long> commonChildren(final long pN, final long pO,
    final INodeReadTrx pFirstRtx, final INodeReadTrx pSecondRtx,
    final EReverseMap pReverse) {
    assert pN >= 0;
    assert pO >= 0;
    assert pFirstRtx != null;
    assert pSecondRtx != null;
    assert pReverse != null;
    final List<Long> retVal = new LinkedList<>();
    pFirstRtx.moveTo(pN);
    if (pFirstRtx.getStructuralNode().hasFirstChild()) {
      pFirstRtx.moveToFirstChild();

      do {
        Long partner;
        if (pReverse == EReverseMap.TRUE) {
          partner =
            mTotalMatching.reversePartner(pFirstRtx.getNode().getNodeKey());
        } else {
          partner = mTotalMatching.partner(pFirstRtx.getNode().getNodeKey());
        }

        if (partner != null) {
          pSecondRtx.moveTo(partner);
          if (pSecondRtx.getNode().getParentKey() == pO) {
            retVal.add(pFirstRtx.getNode().getNodeKey());
          }
        }
      } while (pFirstRtx.getStructuralNode().hasRightSibling()
        && pFirstRtx.moveToRightSibling());
    }
    return retVal;
  }

  /**
   * Emits the move of node "child" to the pPos-th child of node "parent".
   * 
   * @param pChild
   *          child node to move
   * @param pParent
   *          node where to insert the moved subtree
   * @param pPos
   *          position among the childs to move to
   * @param pWtx
   *          {@link INodeWriteTrx} implementation reference on old
   *          revision
   * @param pRtx
   *          {@link INodeReadTrx} implementation reference on new
   *          revision
   */
  private long emitMove(final long pChild, final long pParent, int pPos,
    final INodeWriteTrx pWtx, final INodeReadTrx pRtx) {
    assert pChild >= 0;
    assert pParent >= 0;
    assert pWtx != null;
    assert pRtx != null;

    boolean moved = pWtx.moveTo(pChild);
    assert moved;

    if (pWtx.getNode().getKind() == ENode.ATTRIBUTE_KIND
      || pWtx.getNode().getKind() == ENode.NAMESPACE_KIND) {
      // Attribute- and namespace-nodes can't be moved.
      return -1;
    }

    assert pPos >= 0;
    moved = pWtx.moveTo(pParent);
    assert moved;

    try {
      if (pPos == 0) {
        assert pWtx.getStructuralNode().getKind() == ENode.ELEMENT_KIND
          || pWtx.getStructuralNode().getKind() == ENode.ROOT_KIND;
        if (pWtx.getStructuralNode().getFirstChildKey() == pChild) {
          LOGWRAPPER
            .error("Something went wrong: First child and child may never be the same!");
        } else {
          if (pWtx.moveTo(pChild)) {
            boolean isTextKind = false;
            if (pWtx.getNode().getKind() == ENode.TEXT_KIND) {
              isTextKind = true;
            }

            checkFromNodeForTextRemoval(pWtx, pChild);
            pWtx.moveTo(pParent);
            if (isTextKind
              && pWtx.getStructuralNode().getFirstChildKey() != pChild) {
              if (pWtx.getStructuralNode().hasFirstChild()) {
                pWtx.moveToFirstChild();
                if (pWtx.getStructuralNode().getKind() == ENode.TEXT_KIND) {
                  mTotalMatching.remove(pWtx.getNode().getNodeKey());
                  pWtx.remove();
                }
                pWtx.moveTo(pParent);
              }
            }

            if (pWtx.getNode().getKind() == ENode.ROOT_KIND) {
              pRtx.moveTo(pChild);
              pWtx.moveTo(pWtx.copySubtreeAsFirstChild(pRtx).getNode()
                .getNodeKey());
            } else {
              pWtx.moveTo(pWtx.moveSubtreeToFirstChild(pChild).getNode()
                .getNodeKey());
            }
          }
        }
      } else {
        assert pWtx.getStructuralNode().hasFirstChild();
        pWtx.moveToFirstChild();

        for (int i = 1; i < pPos; i++) {
          assert pWtx.getStructuralNode().hasRightSibling();
          pWtx.moveToRightSibling();
        }

        // Check if text nodes are getting collapsed and remove mappings accordingly.
        // ===========================================================================
        final long nodeKey = pWtx.getNode().getNodeKey();
        checkFromNodeForTextRemoval(pWtx, pChild);
        pWtx.moveTo(nodeKey);
        if (pWtx.getNode().getKind() == ENode.TEXT_KIND && pWtx.moveTo(pChild)
          && pWtx.getNode().getKind() == ENode.TEXT_KIND) {
          pWtx.moveTo(nodeKey);
          mTotalMatching.remove(pWtx.getNode().getNodeKey());
        }
        pWtx.moveTo(nodeKey);
        if (pWtx.moveToRightSibling()) {
          final long rightNodeKey = pWtx.getNode().getNodeKey();
          if (pWtx.getNode().getKind() == ENode.TEXT_KIND
            && pWtx.moveTo(pChild)
            && pWtx.getNode().getKind() == ENode.TEXT_KIND) {
            pWtx.moveTo(rightNodeKey);
            mTotalMatching.remove(pWtx.getNode().getNodeKey());
          }
          pWtx.moveToLeftSibling();
        }

        // Move.
        moved = pWtx.moveTo(nodeKey);
        assert moved;
        assert pWtx.getNode().getNodeKey() != pChild;

        pWtx.moveTo(pWtx.moveSubtreeToRightSibling(pChild).getNode()
          .getNodeKey());
      }
    } catch (final AbsTTException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }

    return pWtx.getNode().getNodeKey();
  }

  private void checkFromNodeForTextRemoval(final INodeWriteTrx pWtx,
    final long pChild) {
    boolean maybeRemoveLeftSibling =
      pWtx.getStructuralNode().getLeftSiblingKey() == pChild ? true : false;
    if (pWtx.moveTo(pChild)) {
      boolean isText = false;
      if (pWtx.getStructuralNode().hasLeftSibling()) {
        pWtx.moveToLeftSibling();
        if (pWtx.getNode().getKind() == ENode.TEXT_KIND) {
          isText = true;
        }
        pWtx.moveToRightSibling();
      }
      if (isText && pWtx.getStructuralNode().hasRightSibling()) {
        pWtx.moveToRightSibling();
        if (pWtx.getNode().getKind() == ENode.TEXT_KIND && isText) {
          if (maybeRemoveLeftSibling) {
            boolean moved = pWtx.moveToLeftSibling();
            assert moved;
            moved = pWtx.moveToLeftSibling();
            assert moved;
            mTotalMatching.remove(pWtx.getNode().getNodeKey());
          } else {
            mTotalMatching.remove(pWtx.getNode().getNodeKey());
          }
        }
        pWtx.moveToLeftSibling();
      }
    }
  }

  /**
   * Emit an update.
   * 
   * @param pFromNode
   *          the node to update
   * @param pToNode
   *          the new node
   * @param pWtxnull
   *          {@link INodeWriteTrx} implementation reference on old
   *          revision
   * @param pRtx
   *          {@link INodeReadTrx} implementation reference on new
   *          revision
   * @return updated {@link INode}
   */
  private long emitUpdate(final long pFromNode, final long pToNode,
    final INodeWriteTrx pWtx, final INodeReadTrx pRtx) {
    assert pFromNode >= 0;
    assert pToNode >= 0;
    assert pWtx != null;
    assert pRtx != null;

    pWtx.moveTo(pFromNode);
    pRtx.moveTo(pToNode);

    try {
      switch (pRtx.getNode().getKind()) {
      case ELEMENT_KIND:
      case ATTRIBUTE_KIND:
      case NAMESPACE_KIND:
        assert pRtx.getNode().getKind() == ENode.ELEMENT_KIND
          || pRtx.getNode().getKind() == ENode.ATTRIBUTE_KIND
          || pRtx.getNode().getKind() == ENode.NAMESPACE_KIND;
        pWtx.setQName(pRtx.getQNameOfCurrentNode());

        if (pWtx.getNode().getKind() == ENode.ATTRIBUTE_KIND) {
          pWtx.setValue(pRtx.getValueOfCurrentNode());
        }
        break;
      case TEXT_KIND:
        assert pWtx.getNode().getKind() == ENode.TEXT_KIND;
        pWtx.setValue(pRtx.getValueOfCurrentNode());
        break;
      default:
      }
    } catch (final AbsTTException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }

    return pWtx.getNode().getNodeKey();
  }

  /**
   * Emit an insert operation.
   * 
   * @param pParent
   *          parent of the current {@link INode} implementation reference
   *          to insert
   * @param pChild
   *          the current node to insert
   * @param pPos
   *          position of the insert
   * @param pWtx
   *          {@link INodeWriteTrx} implementation reference on old
   *          revision
   * @param pRtx
   *          {@link INodeReadTrx} implementation reference on new
   *          revision
   * @return inserted {@link INode} implementation reference
   * @throws AbsTTException
   *           if anything in sirix fails
   */
  private long emitInsert(final long pChild, final long pParent,
    final int pPos, final INodeWriteTrx pWtx, final INodeReadTrx pRtx) {
    assert pChild >= 0;
    assert pParent >= 0;
    assert pWtx != null;
    assert pRtx != null;

    // Determines if node has been already inserted (for subtrees).
    if (mAlreadyInserted.get(pChild) != null) {
      return pChild; // actually child'
    }

    pWtx.moveTo(pParent);
    pRtx.moveTo(pChild);

    try {
      switch (pRtx.getNode().getKind()) {
      case ATTRIBUTE_KIND:
        try {
          pWtx.insertAttribute(pRtx.getQNameOfCurrentNode(), pRtx
            .getValueOfCurrentNode());
        } catch (final TTUsageException e) {
          mTotalMatching.remove(pWtx.getNode().getNodeKey());
          pWtx.setValue(pRtx.getValueOfCurrentNode());
        }
        process(pWtx.getNode().getNodeKey(), pRtx.getNode().getNodeKey());
        break;
      case NAMESPACE_KIND:
        // Note that the insertion is right (localPart as prefix).
        try {
          pWtx
            .insertNamespace(new QName(pRtx.getQNameOfCurrentNode()
              .getNamespaceURI(), "", pRtx.getQNameOfCurrentNode()
              .getLocalPart()));
        } catch (final TTUsageException e) {
          mTotalMatching.remove(pWtx.getNode().getNodeKey());
        }
        process(pWtx.getNode().getNodeKey(), pRtx.getNode().getNodeKey());
        break;
      default:
        // In case of other node types.
        long oldKey = 0;
        if (pPos == 0) {
          switch (pRtx.getNode().getKind()) {
          case ELEMENT_KIND:
            oldKey = pWtx.copySubtreeAsFirstChild(pRtx).getNode().getNodeKey();
            break;
          case TEXT_KIND:
            // Remove first child text node if there is one and a new text node is inserted.
            if (pWtx.getStructuralNode().hasFirstChild()) {
              pWtx.moveToFirstChild();
              if (pWtx.getNode().getKind() == ENode.TEXT_KIND) {
                mTotalMatching.remove(pWtx.getNode().getNodeKey());
                pWtx.remove();
              }
              pWtx.moveTo(pParent);
            }

            oldKey =
              pWtx.insertTextAsFirstChild(pRtx.getValueOfCurrentNode())
                .getNode().getNodeKey();
            break;
          default:
            // Already inserted.
          }
        } else {
          assert pWtx.getStructuralNode().hasFirstChild();
          pWtx.moveToFirstChild();
          for (int i = 0; i < pPos - 1; i++) {
            assert pWtx.getStructuralNode().hasRightSibling();
            pWtx.moveToRightSibling();
          }

          // Remove right sibl. text node if a text node already exists.
          removeRightSiblingTextNode(pWtx);
          switch (pRtx.getNode().getKind()) {
          case ELEMENT_KIND:
            oldKey =
              pWtx.copySubtreeAsRightSibling(pRtx).getNode().getNodeKey();
            break;
          case TEXT_KIND:
            oldKey =
              pWtx.insertTextAsRightSibling(pRtx.getValueOfCurrentNode())
                .getNode().getNodeKey();
            break;
          default:
            // Already inserted.
            throw new IllegalStateException("Child should be already inserted!");
          }
        }

        // Mark all nodes in subtree as inserted.
        pWtx.moveTo(oldKey);
        pRtx.moveTo(pChild);
        for (final IAxis oldAxis = new DescendantAxis(pWtx, EIncludeSelf.YES), newAxis =
          new DescendantAxis(pRtx, EIncludeSelf.YES); oldAxis.hasNext()
          && newAxis.hasNext();) {
          oldAxis.next();
          newAxis.next();
          final INode node = newAxis.getTransaction().getNode();
          process(oldAxis.getTransaction().getNode().getNodeKey(), newAxis
            .getTransaction().getNode().getNodeKey());
          final long newNodeKey = node.getNodeKey();
          final long oldNodeKey =
            oldAxis.getTransaction().getNode().getNodeKey();
          if (node.getKind() == ENode.ELEMENT_KIND) {
            assert node.getKind() == oldAxis.getTransaction()
              .getStructuralNode().getKind();
            final ElementNode element = (ElementNode)node;
            final ElementNode oldElement =
              (ElementNode)oldAxis.getTransaction().getStructuralNode();
            if (element.getAttributeCount() > 0) {
              for (int i = 0, attCount = element.getAttributeCount(); i < attCount; i++) {
                pRtx.moveToAttribute(i);
                for (int j = 0, oldAttCount = oldElement.getAttributeCount(); i < oldAttCount; j++) {
                  pWtx.moveToAttribute(j);
                  if (pWtx.getQNameOfCurrentNode().equals(
                    pRtx.getQNameOfCurrentNode())) {
                    process(oldAxis.getTransaction().getNode().getNodeKey(),
                      newAxis.getTransaction().getNode().getNodeKey());
                    break;
                  }
                  oldAxis.getTransaction().moveTo(oldNodeKey);
                }
                newAxis.getTransaction().moveTo(newNodeKey);
              }
            }
            if (element.getNamespaceCount() > 0) {
              for (int i = 0, nspCount = element.getNamespaceCount(); i < nspCount; i++) {
                pRtx.moveToNamespace(i);
                for (int j = 0, oldNspCount = oldElement.getNamespaceCount(); j < oldNspCount; j++) {
                  pWtx.moveToNamespace(j);
                  if (pWtx.getQNameOfCurrentNode().getNamespaceURI().equals(
                    pRtx.getQNameOfCurrentNode().getNamespaceURI())
                    && pWtx.getQNameOfCurrentNode().getPrefix().equals(
                      pWtx.getQNameOfCurrentNode().getPrefix())) {
                    process(pWtx.getNode().getNodeKey(), pRtx.getNode()
                      .getNodeKey());
                    break;
                  }
                  oldAxis.getTransaction().moveTo(oldNodeKey);
                }
                newAxis.getTransaction().moveTo(newNodeKey);
              }
            }
          }

          newAxis.getTransaction().moveTo(newNodeKey);
        }
      }
    } catch (final AbsTTException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }

    return pWtx.getNode().getNodeKey();
  }

  /**
   * Remove right sibling text node from the storage as well as from the matching.
   * 
   * @param pWtx
   *          sirix {@link INodeWriteTrx}
   * @throws AbsTTException
   *           if removing of node in the storage fails
   */
  private void removeRightSiblingTextNode(final INodeWriteTrx pWtx)
    throws AbsTTException {
    assert pWtx != null;
    if (pWtx.getStructuralNode().hasRightSibling()) {
      final long nodeKey = pWtx.getNode().getNodeKey();
      pWtx.moveToRightSibling();
      if (pWtx.getNode().getKind() == ENode.TEXT_KIND) {
        mTotalMatching.remove(pWtx.getNode().getNodeKey());
        pWtx.remove();
      }
      pWtx.moveTo(nodeKey);
    }
  }

  /**
   * Process nodes and add update data structures.
   * 
   * @param pOldKey
   *          {@link INode} in old revision
   * @param pNewKey
   *          {@link INode} in new revision
   */
  private void process(final long pOldKey, final long pNewKey) {
    mAlreadyInserted.put(pNewKey, true);
    final Long partner = mTotalMatching.partner(pOldKey);
    if (partner != null) {
      mTotalMatching.remove(pOldKey);
    }
    final Long reversePartner = mTotalMatching.reversePartner(pNewKey);
    if (reversePartner != null) {
      mTotalMatching.remove(reversePartner);
    }
    assert !mTotalMatching.contains(pOldKey, pNewKey);
    mTotalMatching.add(pOldKey, pNewKey);
    mInOrderOldRev.put(pOldKey, true);
    mInOrderNewRev.put(pNewKey, true);
  }

  /**
   * The position of node x in the destination tree (tree2).
   * 
   * @param pX
   *          a node in the second (new) document
   * @param pWtx
   *          {@link INodeWriteTrx} implementation reference on old
   *          revision
   * @param pRtx
   *          {@link INodeReadTrx} implementation reference on new
   *          revision
   * @return it's position, with respect to already inserted/deleted nodes
   */
  private int findPos(final long pX, final INodeWriteTrx pWtx,
    final INodeReadTrx pRtx) {
    assert pX > 0;
    assert pWtx != null;
    assert pRtx != null;
    pRtx.moveTo(pX);

    if (pRtx.getNode().getKind() == ENode.ATTRIBUTE_KIND
      || pRtx.getNode().getKind() == ENode.NAMESPACE_KIND) {
      return 0;
    } else {
      final long nodeKey = pRtx.getNode().getNodeKey();
      // 1 - Let y = p(x) in T2.
      // if (pRtx.getItem().getKind() == ENode.ATTRIBUTE_KIND
      // || pRtx.getItem().getKind() == ENode.NAMESPACE_KIND) {
      // pRtx.moveToParent();
      // }
      pRtx.moveToParent();

      // 2 - If x is the leftmost child of y that is marked "in order", return 0.
      if (pRtx.getStructuralNode().hasFirstChild()) {
        pRtx.moveToFirstChild();

        final long v = pRtx.getNode().getNodeKey();
        if (mInOrderNewRev.get(v) != null && mInOrderNewRev.get(v) && v == pX) {
          return 0;
        }
      }

      // 3 - Find v \in T2 where v is the rightmost sibling of x
      // that is to the left of x and is marked "in order".
      pRtx.moveTo(nodeKey);
      // if (pRtx.getItem().getKind() == ENode.ATTRIBUTE_KIND
      // || pRtx.getItem().getKind() == ENode.NAMESPACE_KIND) {
      // pRtx.moveToParent();
      // }
      pRtx.moveToLeftSibling();
      INode v = pRtx.getNode();
      while (pRtx.getStructuralNode().hasLeftSibling()
        && (mInOrderNewRev.get(v.getNodeKey()) == null || (mInOrderNewRev.get(v
          .getNodeKey()) != null && !mInOrderNewRev.get(v.getNodeKey())))) {
        pRtx.moveToLeftSibling();
        v = pRtx.getNode();
      }

      // Step 2 states that in ``in order'' node exists, but this is not
      // true.
      if (mInOrderNewRev.get(v.getNodeKey()) == null) {
        // Assume it is the first node (undefined in the paper).
        return 0;
      }

      // 4 - Let u be the partner of v in T1
      Long u = mTotalMatching.reversePartner(v.getNodeKey());
      int i = -1;
      if (u != null) {
        boolean moved = pWtx.moveTo(u);
        assert moved;

        // Suppose u is the i-th child of its parent (counting from left to
        // right) that is marked "in order". Return i+1.
        final long toNodeKey = u;
        pWtx.moveToParent();
        pWtx.moveToFirstChild();
        do {
          u = pWtx.getNode().getNodeKey();
          // true for original u? only count nodes marked as inOrder?!
          // if (mInOrderOldRev.get(u) != null && mInOrderOldRev.get(u)) {
          i++;
          // }
        } while (u != toNodeKey && pWtx.getStructuralNode().hasRightSibling()
          && pWtx.moveToRightSibling());

        // return i + 1;
      }

      return i + 1;
    }
  }

  /**
   * The fast match algorithm. Try to resolve the "good matching problem".
   * 
   * @param pWtx
   *          {@link INodeWriteTrx} implementation reference on old
   *          revision
   * @param pRtx
   *          {@link INodeReadTrx} implementation reference on new
   *          revision
   * @return {@link Matching} reference with matched nodes
   * @throws AbsTTException
   *           if anything in sirix fails
   */
  private Matching fastMatch(final INodeWriteTrx pWtx, final INodeReadTrx pRtx) {
    assert pWtx != null;
    assert pRtx != null;

    // Chain all nodes with a given label l in tree T together.
    getLabels(pWtx, mLabelOldRevVisitor);
    getLabels(pRtx, mLabelNewRevVisitor);

    // Do the matching job on the leaf nodes.
    final Matching matching = new Matching(pWtx, pRtx);
    matching.reset();
    match(mLabelOldRevVisitor.getLeafLabels(), mLabelNewRevVisitor
      .getLeafLabels(), matching, new LeafEqual());

    // Remove roots ('/') from labels and append them to mapping.
    final Map<ENode, List<Long>> oldLabels = mLabelOldRevVisitor.getLabels();
    final Map<ENode, List<Long>> newLabels = mLabelNewRevVisitor.getLabels();
    oldLabels.remove(ENode.ROOT_KIND);
    newLabels.remove(ENode.ROOT_KIND);

    pWtx.moveTo(mOldStartKey);
    pRtx.moveTo(mNewStartKey);
    pWtx.moveToParent();
    pRtx.moveToParent();
    matching.add(pWtx.getNode().getNodeKey(), pRtx.getNode().getNodeKey());

    match(oldLabels, newLabels, matching, new InnerNodeEqual(matching));

    return matching;
  }

  /**
   * Actual matching.
   * 
   * @param pOldLabels
   *          nodes in tree1, sorted by node type (element, attribute, text,
   *          comment, ...)
   * @param pNewLabels
   *          nodes in tree2, sorted by node type (element, attribute, text,
   *          comment, ...)
   * @param pMatching
   *          {@link Matching} reference
   * @param pCmp
   *          functional class
   */
  private void match(final Map<ENode, List<Long>> pOldLabels,
    final Map<ENode, List<Long>> pNewLabels, final Matching pMatching,
    final IComparator<Long> pCmp) {
    final Set<ENode> labels = pOldLabels.keySet();
    labels.retainAll(pNewLabels.keySet()); // intersection

    // 2 - for each label do
    for (final ENode label : labels) {
      final List<Long> first = pOldLabels.get(label); // 2(a)
      final List<Long> second = pNewLabels.get(label); // 2(b)

      // 2(c)
      final List<Pair<Long, Long>> common =
        Util.longestCommonSubsequence(first, second, pCmp);
      // Used to remove the nodes in common from s1 and s2 in step 2(e).
      final Map<Long, Boolean> seen = new HashMap<>();

      // 2(d) - for each pair of nodes in the lcs: add to matching.
      for (final Pair<Long, Long> p : common) {
        pMatching.add(p.getFirst(), p.getSecond());
        seen.put(p.getFirst(), true);
        seen.put(p.getSecond(), true);
      }

      // 2(e) (prepare) - remove nodes in common from s1, s2.
      removeCommonNodes(first, seen);
      removeCommonNodes(second, seen);

      // 2(e) - For each unmatched node x \in s1.
      final Iterator<Long> firstIterator = first.iterator();
      while (firstIterator.hasNext()) {
        final Long firstItem = firstIterator.next();
        boolean firstIter = true;
        // If there is an unmatched node y \in s2.
        final Iterator<Long> secondIterator = second.iterator();
        while (secondIterator.hasNext()) {
          final Long secondItem = secondIterator.next();
          // Such that equal.
          if (pCmp.isEqual(firstItem, secondItem)) {
            // 2(e)A
            pMatching.add(firstItem, secondItem);

            // 2(e)B
            if (firstIter) {
              firstIter = false;
              firstIterator.remove();
            }
            secondIterator.remove();
            break;
          }
        }
      }
    }
  }

  /**
   * Remove nodes in common.
   * 
   * @param pList
   *          {@link List} of {@link INode}s
   * @param pSeen
   *          {@link Map} of {@link INode}s
   */
  private void removeCommonNodes(final List<Long> pList,
    final Map<Long, Boolean> pSeen) {
    assert pList != null;
    assert pSeen != null;

    final Iterator<Long> iterator = pList.iterator();
    while (iterator.hasNext()) {
      final Long item = iterator.next();
      if (pSeen.containsKey(item)) {
        iterator.remove();
      }
    }
  }

  /**
   * Initialize data structures.
   * 
   * @param pRtx
   *          {@link IRriteTransaction} implementation reference on old
   *          revision
   * @param pVisitor
   *          {@link IVisitor} reference
   * @throws AbsTTException
   *           if anything in sirix fails
   */
  private void init(final INodeReadTrx pRtx, final IVisitor pVisitor) {
    assert pVisitor != null;

    final long nodeKey = pRtx.getNode().getNodeKey();
    for (final IAxis axis = new PostOrderAxis(pRtx); axis.hasNext();) {
      axis.next();
      if (axis.getTransaction().getNode().getNodeKey() == nodeKey) {
        break;
      }
      axis.getTransaction().getNode().acceptVisitor(pVisitor);
    }
    pRtx.getNode().acceptVisitor(pVisitor);
  }

  /**
   * Creates a flat list of all nodes by doing an in-order-traversal. NOTE:
   * Since this is not a binary tree, we use post-order-traversal (wrong in
   * paper). For each node type (element, attribute, text, comment, ...) there
   * is a separate list.
   * 
   * @param pRtx
   *          {@link INodeReadTrx} reference
   * @param pVisitor
   *          {@link LabelFMSEVisitor} used to save node type/list
   */
  private void getLabels(final INodeReadTrx pRtx,
    final LabelFMSEVisitor pVisitor) {
    assert pRtx != null;
    assert pVisitor != null;

    final long nodeKey = pRtx.getNode().getNodeKey();
    for (final AbsAxis axis = new PostOrderAxis(pRtx); axis.hasNext();) {
      axis.next();
      if (axis.getTransaction().getNode().getNodeKey() == nodeKey) {
        break;
      }
      axis.getTransaction().getNode().acceptVisitor(pVisitor);
    }
    pRtx.getNode().acceptVisitor(pVisitor);
  }

  /**
   * Compares the values of two nodes. Values are the text content, if the
   * nodes do have child nodes or the name for inner nodes such as element or
   * attribute (an attribute has one child: the value).
   * 
   * @param pX
   *          first node
   * @param pY
   *          second node
   * @param pRtx
   *          {@link INodeReadTrx} implementation reference
   * @param pWtx
   *          {@link INodeWriteTrx} implementation reference
   * @return true iff the values of the nodes are equal
   */
  private boolean nodeValuesEqual(final long pX, final long pY,
    final INodeReadTrx pRtxOld, final INodeReadTrx pRtxNew) {
    assert pX >= 0;
    assert pY >= 0;
    assert pRtxOld != null;
    assert pRtxNew != null;

    final String a = getNodeValue(pX, pRtxOld);
    final String b = getNodeValue(pY, pRtxNew);

    return a == null ? b == null : a.equals(b);
  }

  /**
   * Get node value of current node which is the string representation of {@link QName}s in the form
   * {@code prefix:localName} or the value of {@link TextNode}s.
   * 
   * @param pNodeKey
   *          node from which to get the value
   * @param pRtx
   *          {@link INodeReadTrx} implementation reference
   * @return string value of current node
   */
  private String getNodeValue(final long pNodeKey, final INodeReadTrx pRtx) {
    assert pNodeKey >= 0;
    assert pRtx != null;
    pRtx.moveTo(pNodeKey);
    final StringBuilder retVal = new StringBuilder();
    switch (pRtx.getNode().getKind()) {
    case ELEMENT_KIND:
    case NAMESPACE_KIND:
    case ATTRIBUTE_KIND:
      retVal.append(PageWriteTrx.buildName(pRtx.getQNameOfCurrentNode()));
      break;
    case TEXT_KIND:
      retVal.append(pRtx.getValueOfCurrentNode());
      break;
    default:
      // Do nothing.
    }
    return retVal.toString();
  }

  /**
   * This functional class is used to compare leaf nodes. The comparison is
   * done by comparing the (characteristic) string for two nodes. If the
   * strings are sufficient similar, the nodes are considered to be equal.
   */
  private class LeafEqual implements IComparator<Long> {

    /** {@inheritDoc} */
    @Override
    public boolean isEqual(final Long pFirstNode, final Long pSecondNode) {
      assert pFirstNode != null;
      assert pSecondNode != null;

      mWtx.moveTo(pFirstNode);
      mRtx.moveTo(pSecondNode);

      final INode oldNode = mWtx.getNode();
      final INode newNode = mRtx.getNode();

      assert oldNode.getKind() == newNode.getKind();

      double ratio = 0;

      if (oldNode.getKind() == ENode.ATTRIBUTE_KIND
        || oldNode.getKind() == ENode.NAMESPACE_KIND) {
        if (mWtx.getQNameOfCurrentNode().equals(mRtx.getQNameOfCurrentNode())) {
          ratio = 1;
          if (mWtx.getNode().getKind() == ENode.ATTRIBUTE_KIND) {
            ratio =
              calculateRatio(mWtx.getValueOfCurrentNode(), mRtx
                .getValueOfCurrentNode());
          }

          // Also check QNames of the parents.
          if (ratio > FMESF) {
            mWtx.moveToParent();
            mRtx.moveToParent();
            // final QName oldQName = mWtx.getQNameOfCurrentNode();
            // final QName newQName = mRtx.getQNameOfCurrentNode();
            // if (oldQName.equals(newQName) && oldQName.getPrefix().equals(newQName.getPrefix())) {
            // ratio = checkAncestors(mWtx.getItem().getKey(), mRtx.getItem().getKey()) ? 1 : 0;
            // } else {
            ratio =
              calculateRatio(getNodeValue(pFirstNode, mWtx), getNodeValue(
                pSecondNode, mRtx));
            // if (ratio > FMESF) {
            // ratio = checkAncestors(mWtx.getItem().getKey(), mRtx.getItem().getKey()) ? 1 : 0;
            // }
            // }
          }
        }
      } else {
        if (nodeValuesEqual(pFirstNode, pSecondNode, mWtx, mRtx)) {
          ratio = 1;
        } else {
          ratio =
            calculateRatio(getNodeValue(pFirstNode, mWtx), getNodeValue(
              pSecondNode, mRtx));
        }

        if (ratio <= FMESF
          && checkAncestors(mWtx.getNode().getNodeKey(), mRtx.getNode()
            .getNodeKey())) {
          ratio = 1;
        }
      }

      return ratio > FMESF;
    }
  }

  /**
   * This functional class is used to compare inner nodes. FMES uses different
   * comparison criteria for leaf nodes and inner nodes. This class compares
   * two nodes by calculating the number of common children (i.e. children
   * contained in the matching) in relation to the total number of children.
   */
  private class InnerNodeEqual implements IComparator<Long> {

    /** {@link Matching} reference. */
    private final Matching mMatching;

    /**
     * Constructor.
     * 
     * @param pMatching
     *          {@link Matching} reference
     * @param pWtx
     *          {@link INodeWriteTrx} implementation reference on old
     *          revision
     * @param pRtx
     *          {@link INodeReadTrx} implementation reference on new
     *          revision
     */
    public InnerNodeEqual(final Matching pMatching) {
      assert pMatching != null;
      mMatching = pMatching;
    }

    @Override
    public boolean isEqual(final Long pFirstNode, final Long pSecondNode) {
      assert pFirstNode != null;
      assert pSecondNode != null;

      mWtx.moveTo(pFirstNode);
      mRtx.moveTo(pSecondNode);

      assert mWtx.getNode().getKind() == mRtx.getNode().getKind();

      boolean retVal = false;

      final ElementNode firstElement = (ElementNode)mWtx.getNode();
      final ElementNode secondElement = (ElementNode)mRtx.getNode();
      if ((firstElement.hasFirstChild() || firstElement.getAttributeCount() > 0 || firstElement
        .getNamespaceCount() > 0)
        && (secondElement.hasFirstChild()
          || secondElement.getAttributeCount() > 0 || secondElement
          .getNamespaceCount() > 0)) {
        final long common =
          mMatching.containedDescendants(pFirstNode, pSecondNode);
        final long maxFamilySize =
          Math.max(mDescendantsOldRev.get(pFirstNode), mDescendantsNewRev
            .get(pSecondNode));
        if (common == 0 && maxFamilySize == 1) {
          retVal =
            mWtx.getQNameOfCurrentNode().equals(mRtx.getQNameOfCurrentNode());
        } else {
          retVal = ((double)common / (double)maxFamilySize) >= FMESTHRESHOLD;
        }
      } else {
        final QName oldName = mWtx.getQNameOfCurrentNode();
        final QName newName = mRtx.getQNameOfCurrentNode();
        if (oldName.getNamespaceURI().equals(newName.getNamespaceURI())
          && calculateRatio(oldName.getLocalPart(), newName.getLocalPart()) > 0.7) {
          retVal =
            checkAncestors(mWtx.getNode().getNodeKey(), mRtx.getNode()
              .getNodeKey());
        }
      }

      return retVal;
    }
  }

  @Override
  public String getName() {
    return NAME;
  }

  /**
   * Calculate ratio between 0 and 1 between two String values for {@code text-nodes} denoted.
   * 
   * @param pFirstNode
   *          node key of first node
   * @param pSecondNode
   *          node key of second node
   * @return ratio between 0 and 1, whereas 1 is a complete match and 0 denotes that the Strings are
   *         completely different
   */
  private float calculateRatio(final String pOldValue, final String pNewValue) {
    assert pOldValue != null;
    assert pNewValue != null;
    float ratio;

    if (pOldValue.length() > MAX_LENGTH || pNewValue.length() > MAX_LENGTH) {
      ratio = Util.quickRatio(pOldValue, pNewValue);
    } else {
      ratio = mLevenshtein.getSimilarity(pOldValue, pNewValue);
    }

    return ratio;
  }

  /**
   * Check if ancestors are equal.
   * 
   * @param pOldKey
   *          start key in old revision
   * @param pNewKey
   *          start key in new revision
   * @return {@code true} if all ancestors up to the start keys of the FMSE-algorithm, {@code false} otherwise
   */
  private boolean checkAncestors(final long pOldKey, final long pNewKey) {
    assert pOldKey >= 0;
    assert pNewKey >= 0;
    mWtx.moveTo(pOldKey);
    mRtx.moveTo(pNewKey);
    boolean retVal = true;
    if (mWtx.getNode().hasParent() && mRtx.getNode().hasParent()) {
      do {
        mWtx.moveToParent();
        mRtx.moveToParent();
      } while (mWtx.getNode().getNodeKey() != mOldStartKey
        && mRtx.getNode().getNodeKey() != mNewStartKey
        && mWtx.getNode().hasParent()
        && mRtx.getNode().hasParent()
        && calculateRatio(getNodeValue(mWtx.getNode().getNodeKey(), mWtx),
          getNodeValue(mRtx.getNode().getNodeKey(), mRtx)) >= 0.7f);
      if ((mWtx.getNode().hasParent() && mWtx.getNode().getNodeKey() != mOldStartKey)
        || (mRtx.getNode().hasParent() && mRtx.getNode().getNodeKey() != mNewStartKey)) {
        retVal = false;
      } else {
        retVal = true;
      }
    } else {
      retVal = false;
    }
    return retVal;
  }

  @Override
  public void close() throws AbsTTException {
    mWtx.commit();
  }
}
