/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.sirix.diff.algorithm.fmse;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.xml.namespace.QName;
import org.brackit.xquery.atomic.QNm;
import org.sirix.access.Utils;
import org.sirix.api.Axis;
import org.sirix.api.XdmNodeReadTrx;
import org.sirix.api.XdmNodeWriteTrx;
import org.sirix.api.visitor.Visitor;
import org.sirix.axis.AbstractAxis;
import org.sirix.axis.ChildAxis;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.axis.LevelOrderAxis;
import org.sirix.axis.PostOrderAxis;
import org.sirix.axis.visitor.DeleteFMSEVisitor;
import org.sirix.axis.visitor.VisitorDescendantAxis;
import org.sirix.diff.algorithm.ImportDiff;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixUsageException;
import org.sirix.node.Kind;
import org.sirix.node.TextNode;
import org.sirix.node.interfaces.Node;
import org.sirix.utils.LogWrapper;
import org.sirix.utils.Pair;
import org.slf4j.LoggerFactory;
import com.google.common.base.Optional;

/**
 * Provides the fast match / edit script (fmes) tree to tree correction algorithm as described in
 * "Change detection in hierarchically structured information" by S. Chawathe, A. Rajaraman, H.
 * Garcia-Molina and J. Widom Stanford University, 1996 ([CRGMW95]) <br>
 * FMES is used by the <a href="http://www.logilab.org/projects/xmldiff">python script</a> xmldiff
 * from Logilab. <br>
 * 
 * Based on the FMES version of Daniel Hottinger and Franziska Meyer.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class FMSE implements ImportDiff, AutoCloseable {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(FMSE.class));

  /** Determines if a reverse lookup has to be made. */
  enum ReverseMap {
    /** Yes, reverse lookup. */
    TRUE,

    /** No, normal lookup. */
    FALSE
  }

  /** Algorithm name. */
  private static final String NAME = "Fast Matching / Edit Script";

  /**
   * Matching Criterion 1. For the "good matching problem", the following conditions must hold for
   * leafs x and y:
   * <ul>
   * <li>label(x) == label(y)</li>
   * <li>compare(value(x), value(y)) <= FMESF</li>
   * </ul>
   * where FMESF is in the range [0,1] and compare() computes the cost of updating a leaf node.
   */
  private static final double FMESF = 0.5;

  /**
   * Matching Criterion 2. For the "good matching problem", the following conditions must hold inner
   * nodes x and y:
   * <ul>
   * <li>label(x) == label(y)</li>
   * <li>|common(x,y)| / max(|x|, |y|) > FMESTHRESHOLD</li>
   * </ul>
   * where FMESTHRESHOLD is in the range [0.5, 1] and common(x,y) computes the number of leafs that
   * can be matched between x and y.
   */
  private static final double FMESTHRESHOLD = 0.5;

  /** Max length for Levenshtein comparsion. */
  private static final int MAX_LENGTH = 50;

  /** {@code Levenshtein} reference, can be reused among instances. */
  private static final Levenshtein mLevenshtein = new Levenshtein();

  /**
   * Used by emitInsert: when inserting a whole subtree - keep track that nodes are not inserted
   * multiple times.
   */
  private Map<Long, Boolean> mAlreadyInserted;

  /**
   * This is the matching M between nodes as described in the paper.
   */
  private Matching mFastMatching;

  /**
   * This is the total matching M' between nodes as described in the paper.
   */
  private transient Matching mTotalMatching;

  /**
   * Stores the in-order property for each node for old revision.
   */
  private Map<Long, Boolean> mInOrderOldRev;

  /**
   * Stores the in-order property for each node for new revision.
   */
  private Map<Long, Boolean> mInOrderNewRev;

  /**
   * Number of descendants in subtree of node on old revision.
   */
  private Map<Long, Long> mDescendantsOldRev;

  /**
   * Number of descendants in subtree of node on new revision.
   */
  private Map<Long, Long> mDescendantsNewRev;

  /** {@link Visitor} implementation on old revision. */
  private Visitor mOldRevVisitor;

  /** {@link Visitor} implementation on new revision. */
  private Visitor mNewRevVisitor;

  /** {@link Visitor} implementation to collect label/nodes on old revision. */
  private LabelFMSEVisitor mLabelOldRevVisitor;

  /** {@link Visitor} implementation to collect label/nodes on new revision. */
  private LabelFMSEVisitor mLabelNewRevVisitor;

  /** Sirix {@link XdmNodeWriteTrx}. */
  private XdmNodeWriteTrx mWtx;

  /** Sirix {@link XdmNodeReadTrx}. */
  private XdmNodeReadTrx mRtx;

  /** Start key of old revision. */
  private long mOldStartKey;

  /** Start key of new revision. */
  private long mNewStartKey;

  @Override
  public void diff(final XdmNodeWriteTrx wtx, final XdmNodeReadTrx rtx) throws SirixException {
    mWtx = checkNotNull(wtx);
    mRtx = checkNotNull(rtx);
    mOldStartKey = mWtx.getNodeKey();
    mNewStartKey = mRtx.getNodeKey();
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
    } catch (final SirixException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  /**
   * First step of the edit script algorithm. Combines the update, insert, align and move phases.
   * 
   * @param wtx {@link XdmNodeWriteTrx} implementation reference on old revisionso
   * @param pRtxn {@link XdmNodeReadTrx} implementation reference o new revision
   */
  private void firstFMESStep(final XdmNodeWriteTrx wtx, final XdmNodeReadTrx rtx) {
    assert wtx != null;
    assert rtx != null;

    wtx.moveTo(mOldStartKey);
    rtx.moveTo(mNewStartKey);

    // 2. Iterate over new shreddered file
    for (final Axis axis =
        new LevelOrderAxis.Builder(rtx).includeSelf().includeNonStructuralNodes().build(); axis
            .hasNext();) {
      axis.next();
      final long nodeKey = axis.getTrx().getNodeKey();
      doFirstFSMEStep(wtx, rtx);
      axis.getTrx().moveTo(nodeKey);
    }
  }

  /**
   * Do the actual first step of FSME.
   * 
   * @param wtx {@link XdmNodeWriteTrx} implementation reference on old revision
   * @param rtx {@link XdmNodeReadTrx} implementation reference on new revision
   * @throws SirixException if anything in sirix fails
   */
  private void doFirstFSMEStep(final XdmNodeWriteTrx wtx, final XdmNodeReadTrx rtx) {
    assert wtx != null;
    assert rtx != null;
    // 2(a) - Parent of x.
    final long key = rtx.getNodeKey();
    final long x = rtx.getNodeKey();
    rtx.moveToParent();
    final long y = rtx.getNodeKey();

    final Long z = mTotalMatching.reversePartner(y);
    Long w = mTotalMatching.reversePartner(x);

    wtx.moveTo(mOldStartKey);
    // 2(b) - insert
    if (w == null) {
      // x has no partner.
      assert z != null;
      mInOrderNewRev.put(x, true);
      final int k = findPos(x, wtx, rtx);
      assert k > -1;
      w = emitInsert(x, z, k, wtx, rtx);
    } else if (x != wtx.getNodeKey()) {
      // 2(c) not the root (x has a partner in M').
      if (wtx.moveTo(w).hasMoved() && rtx.moveTo(x).hasMoved() && wtx.getKind() == rtx.getKind()
          && (!nodeValuesEqual(w, x, wtx, rtx)
              || (rtx.getKind() == Kind.ATTRIBUTE && !rtx.getValue().equals(wtx.getValue())))) {
        // Either QNames differ or the values in case of attribute nodes.
        emitUpdate(w, x, wtx, rtx);
      }
      wtx.moveTo(w);
      wtx.moveToParent();
      final long v = wtx.getNodeKey();
      if (!mTotalMatching.contains(v, y) && wtx.moveTo(w).hasMoved() && rtx.moveTo(x).hasMoved()) {
        assert z != null;
        mInOrderNewRev.put(x, true);
        rtx.moveTo(x);
        if (rtx.getKind() == Kind.NAMESPACE || rtx.getKind() == Kind.ATTRIBUTE) {
          wtx.moveTo(w);
          try {
            mTotalMatching.remove(w);
            wtx.remove();
          } catch (final SirixException e) {
            LOGWRAPPER.error(e.getMessage(), e);
          }
          w = emitInsert(x, z, -1, wtx, rtx);
        } else {
          final int k = findPos(x, wtx, rtx);
          assert k > -1;
          emitMove(w, z, k, wtx, rtx);
        }
      }
    }

    alignChildren(w, x, wtx, rtx);
    rtx.moveTo(key);
  }

  /**
   * Second step of the edit script algorithm. This is the delete phase.
   * 
   * @param wtx {@link XdmNodeWriteTrx} implementation reference on old revision
   * @param rtx {@link XdmNodeReadTrx} implementation reference on new revision
   */
  private void secondFMESStep(final XdmNodeWriteTrx wtx, final XdmNodeReadTrx rtx)
      throws SirixException {
    assert wtx != null;
    assert rtx != null;
    wtx.moveTo(mOldStartKey);
    for (@SuppressWarnings("unused")
    final long nodeKey : VisitorDescendantAxis.newBuilder(wtx).includeSelf()
        .visitor(Optional
            .<DeleteFMSEVisitor>of(new DeleteFMSEVisitor(wtx, mTotalMatching, mOldStartKey)))
        .build());
  }

  /**
   * Alignes the children of a node x according the the children of node w.
   * 
   * @param w node in the first document
   * @param x node in the second document
   * @param wtx {@link XdmNodeWriteTrx} implementation reference on old revision
   * @param rtx {@link XdmNodeReadTrx} implementation reference on new revision
   */
  private void alignChildren(final long w, final long x, final XdmNodeWriteTrx wtx,
      final XdmNodeReadTrx rtx) {
    assert w >= 0;
    assert x >= 0;
    assert wtx != null;
    assert rtx != null;

    wtx.moveTo(w);
    rtx.moveTo(x);

    // Mark all children of w and all children of x "out of order".
    markOutOfOrder(wtx, mInOrderOldRev);
    markOutOfOrder(rtx, mInOrderNewRev);

    // 2
    final List<Long> first = commonChildren(w, x, wtx, rtx, ReverseMap.FALSE);
    final List<Long> second = commonChildren(x, w, rtx, wtx, ReverseMap.TRUE);
    // 3 && 4
    final List<Pair<Long, Long>> s =
        Util.longestCommonSubsequence(first, second, new Comparator<Long>() {
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
      wtx.moveTo(a);
      final Long b = mTotalMatching.partner(a);
      // assert b != null;
      if (seen.get(a) == null && wtx.moveTo(a).hasMoved() && b != null
          && rtx.moveTo(b).hasMoved()) { // (a,
        // b)
        // \notIn
        // S
        // if (seen.get(a) == null || (seen.get(a) != null &&
        // !seen.get(a).equals(b))
        // && mInOrderOldRev.get(a) != null && !mInOrderOldRev.get(a)) { // (a,
        // b) \notIn S
        mInOrderOldRev.put(a, true);
        mInOrderNewRev.put(b, true);
        final int k = findPos(b, wtx, rtx);
        LOGWRAPPER.debug("Move in align children: " + k);
        emitMove(a, w, k, wtx, rtx);
      }
    }
  }

  /**
   * Mark children out of order.
   * 
   * @param rtx {@link XdmNodeReadTrx} reference
   * @param inOrder {@link Map} to put all children out of order
   */
  private void markOutOfOrder(final XdmNodeReadTrx rtx, final Map<Long, Boolean> inOrder) {
    for (final AbstractAxis axis = new ChildAxis(rtx); axis.hasNext();) {
      axis.next();
      inOrder.put(axis.getTrx().getNodeKey(), false);
    }
  }

  /**
   * The sequence of children of n whose partners are children of o. This is used by
   * alignChildren().
   * 
   * @param n parent node in a document tree
   * @param o corresponding parent node in the other tree
   * @param firstRtx {@link XdmNodeReadTrx} on pN node
   * @param secondRtx {@link XdmNodeReadTrx} on pO node
   * @param reverse determines if reverse partners need to be found
   * @return {@link List} of common child nodes
   */
  private List<Long> commonChildren(final long n, final long o, final XdmNodeReadTrx firstRtx,
      final XdmNodeReadTrx secondRtx, final ReverseMap reverse) {
    assert n >= 0;
    assert o >= 0;
    assert firstRtx != null;
    assert secondRtx != null;
    assert reverse != null;
    final List<Long> retVal = new LinkedList<>();
    firstRtx.moveTo(n);
    if (firstRtx.hasFirstChild()) {
      firstRtx.moveToFirstChild();

      do {
        Long partner;
        if (reverse == ReverseMap.TRUE) {
          partner = mTotalMatching.reversePartner(firstRtx.getNodeKey());
        } else {
          partner = mTotalMatching.partner(firstRtx.getNodeKey());
        }

        if (partner != null) {
          secondRtx.moveTo(partner);
          if (secondRtx.getParentKey() == o) {
            retVal.add(firstRtx.getNodeKey());
          }
        }
      } while (firstRtx.hasRightSibling() && firstRtx.moveToRightSibling().hasMoved());
    }
    return retVal;
  }

  /**
   * Emits the move of node "child" to the pPos-th child of node "parent".
   * 
   * @param child child node to move
   * @param parent node where to insert the moved subtree
   * @param pos position among the childs to move to
   * @param wtx {@link XdmNodeWriteTrx} implementation reference on old revision
   * @param rtx {@link XdmNodeReadTrx} implementation reference on new revision
   */
  private long emitMove(final long child, final long parent, int pos, final XdmNodeWriteTrx wtx,
      final XdmNodeReadTrx rtx) {
    assert child >= 0;
    assert parent >= 0;
    assert wtx != null;
    assert rtx != null;

    boolean moved = wtx.moveTo(child).hasMoved();
    assert moved;

    if (wtx.getKind() == Kind.ATTRIBUTE || wtx.getKind() == Kind.NAMESPACE) {
      // Attribute- and namespace-nodes can't be moved.
      return -1;
    }

    assert pos >= 0;
    moved = wtx.moveTo(parent).hasMoved();
    assert moved;

    try {
      if (pos == 0) {
        assert wtx.getKind() == Kind.ELEMENT || wtx.getKind() == Kind.DOCUMENT;
        if (wtx.getFirstChildKey() == child) {
          LOGWRAPPER.error("Something went wrong: First child and child may never be the same!");
        } else {
          if (wtx.moveTo(child).hasMoved()) {
            boolean isTextKind = false;
            if (wtx.getKind() == Kind.TEXT) {
              isTextKind = true;
            }

            checkFromNodeForTextRemoval(wtx, child);
            wtx.moveTo(parent);
            if (isTextKind && wtx.getFirstChildKey() != child) {
              if (wtx.hasFirstChild()) {
                wtx.moveToFirstChild();
                if (wtx.getKind() == Kind.TEXT) {
                  mTotalMatching.remove(wtx.getNodeKey());
                  wtx.remove();
                }
                wtx.moveTo(parent);
              }
            }

            if (wtx.getKind() == Kind.DOCUMENT) {
              rtx.moveTo(child);
              wtx.moveTo(wtx.copySubtreeAsFirstChild(rtx).getNodeKey());
            } else {
              wtx.moveTo(wtx.moveSubtreeToFirstChild(child).getNodeKey());
            }
          }
        }
      } else {
        assert wtx.hasFirstChild();
        wtx.moveToFirstChild();

        for (int i = 1; i < pos; i++) {
          assert wtx.hasRightSibling();
          wtx.moveToRightSibling();
        }

        // Check if text nodes are getting collapsed and remove mappings
        // accordingly.
        // ===========================================================================
        final long nodeKey = wtx.getNodeKey();
        checkFromNodeForTextRemoval(wtx, child);
        wtx.moveTo(nodeKey);
        if (wtx.getKind() == Kind.TEXT && wtx.moveTo(child).hasMoved()
            && wtx.getKind() == Kind.TEXT) {
          wtx.moveTo(nodeKey);
          mTotalMatching.remove(wtx.getNodeKey());
        }
        wtx.moveTo(nodeKey);
        if (wtx.moveToRightSibling().hasMoved()) {
          final long rightNodeKey = wtx.getNodeKey();
          if (wtx.getKind() == Kind.TEXT && wtx.moveTo(child).hasMoved()
              && wtx.getKind() == Kind.TEXT) {
            wtx.moveTo(rightNodeKey);
            mTotalMatching.remove(wtx.getNodeKey());
          }
          wtx.moveToLeftSibling();
        }

        // Move.
        moved = wtx.moveTo(nodeKey).hasMoved();
        assert moved;
        assert wtx.getNodeKey() != child;

        wtx.moveTo(wtx.moveSubtreeToRightSibling(child).getNodeKey());
      }
    } catch (final SirixException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }

    return wtx.getNodeKey();
  }

  private void checkFromNodeForTextRemoval(final XdmNodeWriteTrx wtx, final long child) {
    boolean maybeRemoveLeftSibling = wtx.getLeftSiblingKey() == child ? true : false;
    if (wtx.moveTo(child).hasMoved()) {
      boolean isText = false;
      if (wtx.hasLeftSibling()) {
        wtx.moveToLeftSibling();
        if (wtx.getKind() == Kind.TEXT) {
          isText = true;
        }
        wtx.moveToRightSibling();
      }
      if (isText && wtx.hasRightSibling()) {
        wtx.moveToRightSibling();
        if (wtx.getKind() == Kind.TEXT && isText) {
          if (maybeRemoveLeftSibling) {
            boolean moved = wtx.moveToLeftSibling().hasMoved();
            assert moved;
            moved = wtx.moveToLeftSibling().hasMoved();
            assert moved;
            mTotalMatching.remove(wtx.getNodeKey());
          } else {
            mTotalMatching.remove(wtx.getNodeKey());
          }
        }
        wtx.moveToLeftSibling();
      }
    }
  }

  /**
   * Emit an update.
   * 
   * @param fromNode the node to update
   * @param toNode the new node
   * @param pWtxnull {@link XdmNodeWriteTrx} implementation reference on old revision
   * @param rtx {@link XdmNodeReadTrx} implementation reference on new revision
   * @return updated {@link Node}
   */
  private long emitUpdate(final long fromNode, final long toNode, final XdmNodeWriteTrx wtx,
      final XdmNodeReadTrx rtx) {
    assert fromNode >= 0;
    assert toNode >= 0;
    assert wtx != null;
    assert rtx != null;

    wtx.moveTo(fromNode);
    rtx.moveTo(toNode);

    try {
      switch (rtx.getKind()) {
        case ELEMENT:
        case ATTRIBUTE:
        case NAMESPACE:
        case PROCESSING_INSTRUCTION:
          assert rtx.getKind() == Kind.ELEMENT || rtx.getKind() == Kind.ATTRIBUTE
              || rtx.getKind() == Kind.NAMESPACE || rtx.getKind() == Kind.PROCESSING_INSTRUCTION;
          wtx.setName(rtx.getName());

          if (wtx.getKind() == Kind.ATTRIBUTE || wtx.getKind() == Kind.PROCESSING_INSTRUCTION) {
            wtx.setValue(rtx.getValue());
          }
          break;
        case TEXT:
        case COMMENT:
          assert wtx.getKind() == Kind.TEXT;
          wtx.setValue(rtx.getValue());
          break;
        default:
      }
    } catch (final SirixException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }

    return wtx.getNodeKey();
  }

  /**
   * Emit an insert operation.
   * 
   * @param parent parent of the current {@link Node} implementation reference to insert
   * @param child the current node to insert
   * @param pos position of the insert
   * @param wtx {@link XdmNodeWriteTrx} implementation reference on old revision
   * @param rtx {@link XdmNodeReadTrx} implementation reference on new revision
   * @return inserted {@link Node} implementation reference
   * @throws SirixException if anything in sirix fails
   */
  private long emitInsert(final long child, final long parent, final int pos,
      final XdmNodeWriteTrx wtx, final XdmNodeReadTrx rtx) {
    assert child >= 0;
    assert parent >= 0;
    assert wtx != null;
    assert rtx != null;

    // Determines if node has been already inserted (for subtrees).
    if (mAlreadyInserted.get(child) != null) {
      return child; // actually child'
    }

    wtx.moveTo(parent);
    rtx.moveTo(child);

    try {
      switch (rtx.getKind()) {
        case ATTRIBUTE:
          try {
            wtx.insertAttribute(rtx.getName(), rtx.getValue());
          } catch (final SirixUsageException e) {
            mTotalMatching.remove(wtx.getNodeKey());
            wtx.setValue(rtx.getValue());
          }
          process(wtx.getNodeKey(), rtx.getNodeKey());
          break;
        case NAMESPACE:
          // Note that the insertion is right (localPart as prefix).
          try {
            wtx.insertNamespace(
                new QNm(rtx.getName().getNamespaceURI(), rtx.getName().getLocalName(), ""));
          } catch (final SirixUsageException e) {
            mTotalMatching.remove(wtx.getNodeKey());
          }
          process(wtx.getNodeKey(), rtx.getNodeKey());
          break;
        default:
          // In case of other node types.
          long oldKey = 0;
          if (pos == 0) {
            switch (rtx.getKind()) {
              case ELEMENT:
                oldKey = wtx.copySubtreeAsFirstChild(rtx).getNodeKey();
                break;
              case TEXT:
                // Remove first child text node if there is one and a new text node
                // is inserted.
                if (wtx.hasFirstChild()) {
                  wtx.moveToFirstChild();
                  if (wtx.getKind() == Kind.TEXT) {
                    mTotalMatching.remove(wtx.getNodeKey());
                    wtx.remove();
                  }
                  wtx.moveTo(parent);
                }

                oldKey = wtx.insertTextAsFirstChild(rtx.getValue()).getNodeKey();
                break;
              default:
                // Already inserted.
            }
          } else {
            assert wtx.hasFirstChild();
            wtx.moveToFirstChild();
            for (int i = 0; i < pos - 1; i++) {
              assert wtx.hasRightSibling();
              wtx.moveToRightSibling();
            }

            // Remove right sibl. text node if a text node already exists.
            removeRightSiblingTextNode(wtx);
            switch (rtx.getKind()) {
              case ELEMENT:
                oldKey = wtx.copySubtreeAsRightSibling(rtx).getNodeKey();
                break;
              case TEXT:
                oldKey = wtx.insertTextAsRightSibling(rtx.getValue()).getNodeKey();
                break;
              default:
                // Already inserted.
                throw new IllegalStateException("Child should be already inserted!");
            }
          }

          // Mark all nodes in subtree as inserted.
          wtx.moveTo(oldKey);
          rtx.moveTo(child);
          for (final Axis oldAxis = new DescendantAxis(wtx, IncludeSelf.YES), newAxis =
              new DescendantAxis(rtx, IncludeSelf.YES); oldAxis.hasNext() && newAxis.hasNext();) {
            oldAxis.next();
            newAxis.next();
            final XdmNodeReadTrx oldRtx = oldAxis.getTrx();
            final XdmNodeReadTrx newRtx = newAxis.getTrx();
            process(oldRtx.getNodeKey(), newRtx.getNodeKey());
            final long newNodeKey = newRtx.getNodeKey();
            final long oldNodeKey = oldRtx.getNodeKey();
            if (newRtx.getKind() == Kind.ELEMENT) {
              assert newRtx.getKind() == oldRtx.getKind();
              if (newRtx.getAttributeCount() > 0) {
                for (int i = 0, attCount = newRtx.getAttributeCount(); i < attCount; i++) {
                  rtx.moveToAttribute(i);
                  for (int j = 0, oldAttCount = oldRtx.getAttributeCount(); i < oldAttCount; j++) {
                    wtx.moveToAttribute(j);
                    if (wtx.getName().equals(rtx.getName())) {
                      process(oldAxis.getTrx().getNodeKey(), newAxis.getTrx().getNodeKey());
                      break;
                    }
                    oldAxis.getTrx().moveTo(oldNodeKey);
                  }
                  newAxis.getTrx().moveTo(newNodeKey);
                }
              }
              if (newRtx.getNamespaceCount() > 0) {
                for (int i = 0, nspCount = newRtx.getNamespaceCount(); i < nspCount; i++) {
                  rtx.moveToNamespace(i);
                  for (int j = 0, oldNspCount = oldRtx.getNamespaceCount(); j < oldNspCount; j++) {
                    wtx.moveToNamespace(j);
                    if (wtx.getName().getNamespaceURI().equals(rtx.getName().getNamespaceURI())
                        && wtx.getName().getPrefix().equals(wtx.getName().getPrefix())) {
                      process(wtx.getNodeKey(), rtx.getNodeKey());
                      break;
                    }
                    oldAxis.getTrx().moveTo(oldNodeKey);
                  }
                  newAxis.getTrx().moveTo(newNodeKey);
                }
              }
            }

            newAxis.getTrx().moveTo(newNodeKey);
          }
      }
    } catch (final SirixException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }

    return wtx.getNodeKey();
  }

  /**
   * Remove right sibling text node from the storage as well as from the matching.
   * 
   * @param wtx sirix {@link XdmNodeWriteTrx}
   * @throws SirixException if removing of node in the storage fails
   */
  private void removeRightSiblingTextNode(final XdmNodeWriteTrx wtx) throws SirixException {
    assert wtx != null;
    if (wtx.hasRightSibling()) {
      final long nodeKey = wtx.getNodeKey();
      wtx.moveToRightSibling();
      if (wtx.getKind() == Kind.TEXT) {
        mTotalMatching.remove(wtx.getNodeKey());
        wtx.remove();
      }
      wtx.moveTo(nodeKey);
    }
  }

  /**
   * Process nodes and add update data structures.
   * 
   * @param oldKey {@link Node} in old revision
   * @param newKey {@link Node} in new revision
   */
  private void process(final long oldKey, final long newKey) {
    mAlreadyInserted.put(newKey, true);
    final Long partner = mTotalMatching.partner(oldKey);
    if (partner != null) {
      mTotalMatching.remove(oldKey);
    }
    final Long reversePartner = mTotalMatching.reversePartner(newKey);
    if (reversePartner != null) {
      mTotalMatching.remove(reversePartner);
    }
    assert !mTotalMatching.contains(oldKey, newKey);
    mTotalMatching.add(oldKey, newKey);
    mInOrderOldRev.put(oldKey, true);
    mInOrderNewRev.put(newKey, true);
  }

  /**
   * The position of node x in the destination tree (tree2).
   * 
   * @param x a node in the second (new) document
   * @param wtx {@link XdmNodeWriteTrx} implementation reference on old revision
   * @param rtx {@link XdmNodeReadTrx} implementation reference on new revision
   * @return it's position, with respect to already inserted/deleted nodes
   */
  private int findPos(final long x, final XdmNodeWriteTrx wtx, final XdmNodeReadTrx rtx) {
    assert x > 0;
    assert wtx != null;
    assert rtx != null;
    rtx.moveTo(x);

    if (rtx.getKind() == Kind.ATTRIBUTE || rtx.getKind() == Kind.NAMESPACE) {
      return 0;
    } else {
      final long nodeKey = rtx.getNodeKey();
      // 1 - Let y = p(x) in T2.
      // if (pRtx.getItem().getKind() == ENode.ATTRIBUTE_KIND
      // || pRtx.getItem().getKind() == ENode.NAMESPACE_KIND) {
      // pRtx.moveToParent();
      // }
      rtx.moveToParent();

      // 2 - If x is the leftmost child of y that is marked "in order", return
      // 0.
      if (rtx.hasFirstChild()) {
        rtx.moveToFirstChild();

        final long v = rtx.getNodeKey();
        if (mInOrderNewRev.get(v) != null && mInOrderNewRev.get(v) && v == x) {
          return 0;
        }
      }

      // 3 - Find v \in T2 where v is the rightmost sibling of x
      // that is to the left of x and is marked "in order".
      rtx.moveTo(nodeKey);
      // if (pRtx.getItem().getKind() == ENode.ATTRIBUTE_KIND
      // || pRtx.getItem().getKind() == ENode.NAMESPACE_KIND) {
      // pRtx.moveToParent();
      // }
      rtx.moveToLeftSibling();
      long v = rtx.getNodeKey();
      while (rtx.hasLeftSibling() && (mInOrderNewRev.get(v) == null
          || (mInOrderNewRev.get(v) != null && !mInOrderNewRev.get(v)))) {
        rtx.moveToLeftSibling();
        v = rtx.getNodeKey();
      }

      // Step 2 states that in ``in order'' node exists, but this is not
      // true.
      if (mInOrderNewRev.get(v) == null) {
        // Assume it is the first node (undefined in the paper).
        return 0;
      }

      // 4 - Let u be the partner of v in T1
      Long u = mTotalMatching.reversePartner(v);
      int i = -1;
      if (u != null) {
        boolean moved = wtx.moveTo(u).hasMoved();
        assert moved;

        // Suppose u is the i-th child of its parent (counting from left to
        // right) that is marked "in order". Return i+1.
        final long toNodeKey = u;
        wtx.moveToParent();
        wtx.moveToFirstChild();
        do {
          u = wtx.getNodeKey();
          i++;
        } while (u != toNodeKey && wtx.hasRightSibling() && wtx.moveToRightSibling().hasMoved());
      }

      return i + 1;
    }
  }

  /**
   * The fast match algorithm. Try to resolve the "good matching problem".
   * 
   * @param wtx {@link XdmNodeWriteTrx} implementation reference on old revision
   * @param rtx {@link XdmNodeReadTrx} implementation reference on new revision
   * @return {@link Matching} reference with matched nodes
   * @throws SirixException if anything in sirix fails
   */
  private Matching fastMatch(final XdmNodeWriteTrx wtx, final XdmNodeReadTrx rtx) {
    assert wtx != null;
    assert rtx != null;

    // Chain all nodes with a given label l in tree T together.
    getLabels(wtx, mLabelOldRevVisitor);
    getLabels(rtx, mLabelNewRevVisitor);

    // Do the matching job on the leaf nodes.
    final Matching matching = new Matching(wtx, rtx);
    matching.reset();
    match(mLabelOldRevVisitor.getLeafLabels(), mLabelNewRevVisitor.getLeafLabels(), matching,
        new LeafEqual());

    // Remove roots ('/') from labels and append them to mapping.
    final Map<Kind, List<Long>> oldLabels = mLabelOldRevVisitor.getLabels();
    final Map<Kind, List<Long>> newLabels = mLabelNewRevVisitor.getLabels();
    oldLabels.remove(Kind.DOCUMENT);
    newLabels.remove(Kind.DOCUMENT);

    wtx.moveTo(mOldStartKey);
    rtx.moveTo(mNewStartKey);
    wtx.moveToParent();
    rtx.moveToParent();
    matching.add(wtx.getNodeKey(), rtx.getNodeKey());

    match(oldLabels, newLabels, matching, new InnerNodeEqual(matching));

    return matching;
  }

  /**
   * Actual matching.
   * 
   * @param oldLabels nodes in tree1, sorted by node type (element, attribute, text, comment, ...)
   * @param newLabels nodes in tree2, sorted by node type (element, attribute, text, comment, ...)
   * @param matching {@link Matching} reference
   * @param cmp functional class
   */
  private void match(final Map<Kind, List<Long>> oldLabels, final Map<Kind, List<Long>> newLabels,
      final Matching matching, final Comparator<Long> cmp) {
    final Set<Kind> labels = oldLabels.keySet();
    labels.retainAll(newLabels.keySet()); // intersection

    // 2 - for each label do
    for (final Kind label : labels) {
      final List<Long> first = oldLabels.get(label); // 2(a)
      final List<Long> second = newLabels.get(label); // 2(b)

      // 2(c)
      final List<Pair<Long, Long>> common = Util.longestCommonSubsequence(first, second, cmp);
      // Used to remove the nodes in common from s1 and s2 in step 2(e).
      final Map<Long, Boolean> seen = new HashMap<>();

      // 2(d) - for each pair of nodes in the lcs: add to matching.
      for (final Pair<Long, Long> p : common) {
        matching.add(p.getFirst(), p.getSecond());
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
          if (cmp.isEqual(firstItem, secondItem)) {
            // 2(e)A
            matching.add(firstItem, secondItem);

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
   * @param list {@link List} of {@link Node}s
   * @param seen {@link Map} of {@link Node}s
   */
  private void removeCommonNodes(final List<Long> list, final Map<Long, Boolean> seen) {
    assert list != null;
    assert seen != null;

    final Iterator<Long> iterator = list.iterator();
    while (iterator.hasNext()) {
      final Long item = iterator.next();
      if (seen.containsKey(item)) {
        iterator.remove();
      }
    }
  }

  /**
   * Initialize data structures.
   * 
   * @param rtx {@link IRriteTransaction} implementation reference on old revision
   * @param visitor {@link Visitor} reference
   * @throws SirixException if anything in sirix fails
   */
  private void init(final XdmNodeReadTrx rtx, final Visitor visitor) {
    assert visitor != null;

    final long nodeKey = rtx.getNodeKey();
    for (final Axis axis = new PostOrderAxis(rtx); axis.hasNext();) {
      axis.next();
      if (axis.getTrx().getNodeKey() == nodeKey) {
        break;
      }
      axis.getTrx().acceptVisitor(visitor);
    }
    rtx.acceptVisitor(visitor);
  }

  /**
   * Creates a flat list of all nodes by doing an in-order-traversal. NOTE: Since this is not a
   * binary tree, we use post-order-traversal (wrong in paper). For each node type (element,
   * attribute, text, comment, ...) there is a separate list.
   * 
   * @param rtx {@link XdmNodeReadTrx} reference
   * @param visitor {@link LabelFMSEVisitor} used to save node type/list
   */
  private void getLabels(final XdmNodeReadTrx rtx, final LabelFMSEVisitor visitor) {
    assert rtx != null;
    assert visitor != null;

    final long nodeKey = rtx.getNodeKey();
    for (final AbstractAxis axis = new PostOrderAxis(rtx); axis.hasNext();) {
      axis.next();
      if (axis.getTrx().getNodeKey() == nodeKey) {
        break;
      }
      axis.getTrx().acceptVisitor(visitor);
    }
    rtx.acceptVisitor(visitor);
  }

  /**
   * Compares the values of two nodes. Values are the text content, if the nodes do have child nodes
   * or the name for inner nodes such as element or attribute (an attribute has one child: the
   * value).
   * 
   * @param x first node
   * @param y second node
   * @param pRtx {@link XdmNodeReadTrx} implementation reference
   * @param pWtx {@link XdmNodeWriteTrx} implementation reference
   * @return true iff the values of the nodes are equal
   */
  private boolean nodeValuesEqual(final long x, final long y, final XdmNodeReadTrx rtxOld,
      final XdmNodeReadTrx rtxNew) {
    assert x >= 0;
    assert y >= 0;
    assert rtxOld != null;
    assert rtxNew != null;

    final String a = getNodeValue(x, rtxOld);
    final String b = getNodeValue(y, rtxNew);

    return a == null ? b == null : a.equals(b);
  }

  /**
   * Get node value of current node which is the string representation of {@link QName}s in the form
   * {@code prefix:localName} or the value of {@link TextNode}s.
   * 
   * @param nodeKey node from which to get the value
   * @param rtx {@link XdmNodeReadTrx} implementation reference
   * @return string value of current node
   */
  private String getNodeValue(final long nodeKey, final XdmNodeReadTrx rtx) {
    assert nodeKey >= 0;
    assert rtx != null;
    rtx.moveTo(nodeKey);
    final StringBuilder retVal = new StringBuilder();
    switch (rtx.getKind()) {
      case ELEMENT:
      case NAMESPACE:
      case ATTRIBUTE:
        retVal.append(Utils.buildName(rtx.getName()));
        break;
      case TEXT:
      case COMMENT:
        retVal.append(rtx.getValue());
        break;
      case PROCESSING_INSTRUCTION:
        retVal.append(rtx.getName().getLocalName()).append(" ").append(rtx.getValue());
        break;
      default:
        // Do nothing.
    }
    return retVal.toString();
  }

  /**
   * This functional class is used to compare leaf nodes. The comparison is done by comparing the
   * (characteristic) string for two nodes. If the strings are sufficient similar, the nodes are
   * considered to be equal.
   */
  private final class LeafEqual implements Comparator<Long> {

    @Override
    public boolean isEqual(final Long firstNode, final Long secondNode) {
      assert firstNode != null;
      assert secondNode != null;

      // Old.
      mWtx.moveTo(firstNode);

      // New.
      mRtx.moveTo(secondNode);

      assert mWtx.getKind() == mRtx.getKind();
      double ratio = 0;

      if (mWtx.getKind() == Kind.ATTRIBUTE || mWtx.getKind() == Kind.NAMESPACE
          || mWtx.getKind() == Kind.PROCESSING_INSTRUCTION) {
        if (mWtx.getName().equals(mRtx.getName())) {
          ratio = 1;
          if (mWtx.getKind() == Kind.ATTRIBUTE || mWtx.getKind() == Kind.PROCESSING_INSTRUCTION) {
            ratio = calculateRatio(mWtx.getValue(), mRtx.getValue());
          }

          // Also check QNames of the parents.
          if (ratio > FMESF) {
            mWtx.moveToParent();
            mRtx.moveToParent();
            // final QName oldQName = mWtx.getQNameOfCurrentNode();
            // final QName newQName = mRtx.getQNameOfCurrentNode();
            // if (oldQName.equals(newQName) &&
            // oldQName.getPrefix().equals(newQName.getPrefix())) {
            // ratio = checkAncestors(mWtx.getItem().getKey(),
            // mRtx.getItem().getKey()) ? 1 : 0;
            // } else {
            ratio = calculateRatio(getNodeValue(firstNode, mWtx), getNodeValue(secondNode, mRtx));
            // if (ratio > FMESF) {
            // ratio = checkAncestors(mWtx.getItem().getKey(),
            // mRtx.getItem().getKey()) ? 1 : 0;
            // }
            // }
          }
        }
      } else {
        if (nodeValuesEqual(firstNode, secondNode, mWtx, mRtx)) {
          ratio = 1;
        } else {
          ratio = calculateRatio(getNodeValue(firstNode, mWtx), getNodeValue(secondNode, mRtx));
        }

        if (ratio <= FMESF && checkAncestors(mWtx.getNodeKey(), mRtx.getNodeKey())) {
          ratio = 1;
        }
      }

      return ratio > FMESF;
    }
  }

  /**
   * This functional class is used to compare inner nodes. FMES uses different comparison criteria
   * for leaf nodes and inner nodes. This class compares two nodes by calculating the number of
   * common children (i.e. children contained in the matching) in relation to the total number of
   * children.
   */
  private final class InnerNodeEqual implements Comparator<Long> {

    /** {@link Matching} reference. */
    private final Matching mMatching;

    /**
     * Constructor.
     * 
     * @param matching {@link Matching} reference
     * @param pWtx {@link XdmNodeWriteTrx} implementation reference on old revision
     * @param pRtx {@link XdmNodeReadTrx} implementation reference on new revision
     */
    public InnerNodeEqual(final Matching matching) {
      assert matching != null;
      mMatching = matching;
    }

    @Override
    public boolean isEqual(final Long firstNode, final Long secondNode) {
      assert firstNode != null;
      assert secondNode != null;

      mWtx.moveTo(firstNode);
      mRtx.moveTo(secondNode);

      assert mWtx.getKind() == mRtx.getKind();

      boolean retVal = false;

      if ((mWtx.hasFirstChild() || mWtx.getAttributeCount() > 0 || mWtx.getNamespaceCount() > 0)
          && (mRtx.hasFirstChild() || mRtx.getAttributeCount() > 0
              || mRtx.getNamespaceCount() > 0)) {
        final long common = mMatching.containedDescendants(firstNode, secondNode);
        final long maxFamilySize =
            Math.max(mDescendantsOldRev.get(firstNode), mDescendantsNewRev.get(secondNode));
        if (common == 0 && maxFamilySize == 1) {
          retVal = mWtx.getName().equals(mRtx.getName());
        } else {
          retVal = ((double) common / (double) maxFamilySize) >= FMESTHRESHOLD;
        }
      } else {
        final QNm oldName = mWtx.getName();
        final QNm newName = mRtx.getName();
        if (oldName.getNamespaceURI().equals(newName.getNamespaceURI())
            && calculateRatio(oldName.getLocalName(), newName.getLocalName()) > 0.7) {
          retVal = checkAncestors(mWtx.getNodeKey(), mRtx.getNodeKey());
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
   * @param pFirstNode node key of first node
   * @param pSecondNode node key of second node
   * @return ratio between 0 and 1, whereas 1 is a complete match and 0 denotes that the Strings are
   *         completely different
   */
  private float calculateRatio(final String oldValue, final String newValue) {
    assert oldValue != null;
    assert newValue != null;
    float ratio;

    if (oldValue.length() > MAX_LENGTH || newValue.length() > MAX_LENGTH) {
      ratio = Util.quickRatio(oldValue, newValue);
    } else {
      ratio = mLevenshtein.getSimilarity(oldValue, newValue);
    }

    return ratio;
  }

  /**
   * Check if ancestors are equal.
   * 
   * @param oldKey start key in old revision
   * @param newKey start key in new revision
   * @return {@code true} if all ancestors up to the start keys of the FMSE-algorithm, {@code false}
   *         otherwise
   */
  private boolean checkAncestors(final long oldKey, final long newKey) {
    assert oldKey >= 0;
    assert newKey >= 0;
    mWtx.moveTo(oldKey);
    mRtx.moveTo(newKey);
    boolean retVal = true;
    if (mWtx.hasParent() && mRtx.hasParent()) {
      do {
        mWtx.moveToParent();
        mRtx.moveToParent();
      } while (mWtx.getNodeKey() != mOldStartKey && mRtx.getNodeKey() != mNewStartKey
          && mWtx.hasParent() && mRtx.hasParent()
          && calculateRatio(getNodeValue(mWtx.getNodeKey(), mWtx),
              getNodeValue(mRtx.getNodeKey(), mRtx)) >= 0.7f);
      if ((mWtx.hasParent() && mWtx.getNodeKey() != mOldStartKey)
          || (mRtx.hasParent() && mRtx.getNodeKey() != mNewStartKey)) {
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
  public void close() throws SirixException {
    mWtx.commit();
  }
}
