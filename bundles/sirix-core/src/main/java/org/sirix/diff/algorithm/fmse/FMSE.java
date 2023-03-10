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

import org.brackit.xquery.atomic.QNm;
import org.sirix.api.Axis;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.axis.*;
import org.sirix.axis.visitor.DeleteFMSEVisitor;
import org.sirix.axis.visitor.VisitorDescendantAxis;
import org.sirix.diff.algorithm.ImportDiff;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixUsageException;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.Node;
import org.sirix.utils.LogWrapper;
import org.sirix.utils.Pair;
import org.slf4j.LoggerFactory;

import java.util.*;

import static java.util.Objects.requireNonNull;

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
  private static final LogWrapper logger = new LogWrapper(LoggerFactory.getLogger(FMSE.class));

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
   * Used by emitInsert: when inserting a whole subtree - keep track that nodes are not inserted
   * multiple times.
   */
  private Map<Long, Boolean> alreadyInserted;

  /**
   * This is the total matching M' between nodes as described in the paper.
   */
  private transient Matching totalMatching;

  /**
   * Stores the in-order property for each node for old revision.
   */
  private Map<Long, Boolean> inOrderOldRev;

  /**
   * Stores the in-order property for each node for new revision.
   */
  private Map<Long, Boolean> inOrderNewRev;

  /**
   * Number of descendants in subtree of node on old revision.
   */
  private Map<Long, Long> descendantsOldRev;

  /**
   * Number of descendants in subtree of node on new revision.
   */
  private Map<Long, Long> descendantsNewRev;

  /** {@link XmlNodeVisitor} implementation to collect label/nodes on old revision. */
  private LabelFMSEVisitor labelOldRevVisitor;

  /** {@link XmlNodeVisitor} implementation to collect label/nodes on new revision. */
  private LabelFMSEVisitor labelNewRevVisitor;

  /** Sirix {@link XmlNodeTrx}. */
  private XmlNodeTrx wtx;

  /** Sirix {@link XmlNodeReadOnlyTrx}. */
  private XmlNodeReadOnlyTrx rtx;

  /** Start key of old revision. */
  private long oldStartKey;

  /** Start key of new revision. */
  private long newStartKey;

  /** Unique identifier for elements. */
  private final QNm idName;

  /** Path summary reader for the old revision. */
  private PathSummaryReader oldPathSummary;

  /** Path summary reader for the new revision. */
  private PathSummaryReader newPathSummary;

  /** The node comparison factory to check leaf/inner nodes for a matching candidate. */
  private final NodeComparisonFactory nodeComparisonFactory;

  /**
   * Private constructor.
   *
   * @param idName unique identifier for elements
   * @param nodeComparisonFactory the node comparison factory to use
   */
  private FMSE(final QNm idName, final NodeComparisonFactory nodeComparisonFactory) {
    this.idName = idName;
    this.nodeComparisonFactory = requireNonNull(nodeComparisonFactory);
  }

  /**
   * Create a new instance with a unique identifier used to match element nodes.
   *
   * @param idName the unique identifier name
   * @param nodeComparisonFactory the node comparison factory to use
   * @return a new instance
   */
  public static FMSE createWithIdentifier(final QNm idName, final NodeComparisonFactory nodeComparisonFactory) {
    return new FMSE(idName, nodeComparisonFactory);
  }

  /**
   * Create a new instance.
   *
   * @param nodeComparisonFactory the node comparison factory to use
   * @return a new instance
   */
  public static FMSE createInstance(final NodeComparisonFactory nodeComparisonFactory) {
    return new FMSE(null, nodeComparisonFactory);
  }

  @Override
  public void diff(final XmlNodeTrx wtx, final XmlNodeReadOnlyTrx rtx) {
    this.wtx = requireNonNull(wtx);
    this.rtx = requireNonNull(rtx);
    oldStartKey = this.wtx.getNodeKey();
    newStartKey = this.rtx.getNodeKey();
    descendantsOldRev = new HashMap<>();
    descendantsNewRev = new HashMap<>();
    inOrderOldRev = new HashMap<>();
    inOrderNewRev = new HashMap<>();
    alreadyInserted = new HashMap<>();

    oldPathSummary = this.wtx.getPathSummary();
    newPathSummary = this.rtx.getResourceSession().openPathSummary(this.rtx.getRevisionNumber());

    final var oldRevVisitor = new FMSEVisitor(this.wtx, inOrderOldRev, descendantsOldRev);
    final var newRevVisitor = new FMSEVisitor(this.rtx, inOrderNewRev, descendantsNewRev);

    labelOldRevVisitor = new LabelFMSEVisitor(this.wtx);
    labelNewRevVisitor = new LabelFMSEVisitor(this.rtx);
    init(this.wtx, oldRevVisitor);
    init(this.rtx, newRevVisitor);

    final var fastMatching = fastMatch(this.wtx, this.rtx);
    totalMatching = new Matching(fastMatching);
    firstFMESStep(this.wtx, this.rtx);
    try {
      secondFMESStep(this.wtx, this.rtx);
    } catch (final SirixException e) {
      logger.error(e.getMessage(), e);
    }
  }

  /**
   * First step of the edit script algorithm. Combines the update, insert, align and move phases.
   *
   * @param wtx {@link XmlNodeTrx} implementation reference on old revisionso
   * @param rtx {@link XmlNodeReadOnlyTrx} implementation reference o new revision
   */
  private void firstFMESStep(final XmlNodeTrx wtx, final XmlNodeReadOnlyTrx rtx) {
    assert wtx != null;
    assert rtx != null;

    wtx.moveTo(oldStartKey);
    rtx.moveTo(newStartKey);

    // 2. Iterate over new shreddered file
    for (final Axis axis =
        new LevelOrderAxis.Builder(rtx).includeSelf().includeNonStructuralNodes().build(); axis.hasNext();) {
      axis.nextLong();
      final long nodeKey = axis.asXmlNodeReadTrx().getNodeKey();
      doFirstFSMEStep(wtx, rtx);
      axis.asXmlNodeReadTrx().moveTo(nodeKey);
    }
  }

  /**
   * Do the actual first step of FSME.
   *
   * @param wtx {@link XmlNodeTrx} implementation reference on old revision
   * @param rtx {@link XmlNodeReadOnlyTrx} implementation reference on new revision
   * @throws SirixException if anything in sirix fails
   */
  private void doFirstFSMEStep(final XmlNodeTrx wtx, final XmlNodeReadOnlyTrx rtx) {
    assert wtx != null;
    assert rtx != null;

    final FMSENodeComparisonUtils nodeComparisonUtils =
        new FMSENodeComparisonUtils(oldStartKey, newStartKey, this.wtx, this.rtx);

    // 2(a) - Parent of x.
    final long key = rtx.getNodeKey();
    final long x = rtx.getNodeKey();
    rtx.moveToParent();
    final long y = rtx.getNodeKey();

    final Long z = totalMatching.reversePartner(y);
    Long w = totalMatching.reversePartner(x);

    wtx.moveTo(oldStartKey);
    // 2(b) - insert
    if (w == null) {
      // x has no partner.
      assert z != null;
      inOrderNewRev.put(x, true);
      final int k = findPos(x, wtx, rtx);
      assert k > -1;
      w = emitInsert(x, z, k, wtx, rtx);
    } else if (x != wtx.getNodeKey()) {
      // 2(c) not the root (x has a partner in M').
      //noinspection SuspiciousNameCombination
      if (wtx.moveTo(w) && rtx.moveTo(x) && wtx.getKind() == rtx.getKind()
          && (!nodeComparisonUtils.nodeValuesEqual(w, x, wtx, rtx)
              || (rtx.isAttribute() && !rtx.getValue().equals(wtx.getValue())))) {
        // Either QNames differ or the values in case of attribute nodes.
        emitUpdate(w, x, wtx, rtx);
      }
      wtx.moveTo(w);
      wtx.moveToParent();
      final long v = wtx.getNodeKey();
      if (!totalMatching.contains(v, y) && wtx.moveTo(w) && rtx.moveTo(x)) {
        assert z != null;
        inOrderNewRev.put(x, true);
        rtx.moveTo(x);
        if (rtx.isNamespace() || rtx.isAttribute()) {
          wtx.moveTo(w);
          try {
            totalMatching.remove(w);
            wtx.remove();
          } catch (final SirixException e) {
            logger.error(e.getMessage(), e);
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
   * @param wtx {@link XmlNodeTrx} implementation reference on old revision
   * @param rtx {@link XmlNodeReadOnlyTrx} implementation reference on new revision
   */
  private void secondFMESStep(final XmlNodeTrx wtx, final NodeReadOnlyTrx rtx) throws SirixException {
    assert wtx != null;
    assert rtx != null;
    wtx.moveTo(oldStartKey);
    //noinspection StatementWithEmptyBody
    for (@SuppressWarnings("unused")
    final long nodeKey : VisitorDescendantAxis.newBuilder(wtx)
                                              .includeSelf()
                                              .visitor(new DeleteFMSEVisitor(wtx, totalMatching, oldStartKey))
                                              .build());
  }

  /**
   * Alignes the children of a node x according the the children of node w.
   *
   * @param w node in the first document
   * @param x node in the second document
   * @param wtx {@link XmlNodeTrx} implementation reference on old revision
   * @param rtx {@link XmlNodeReadOnlyTrx} implementation reference on new revision
   */
  private void alignChildren(final long w, final long x, final XmlNodeTrx wtx, final XmlNodeReadOnlyTrx rtx) {
    assert w >= 0;
    assert x >= 0;
    assert wtx != null;
    assert rtx != null;

    wtx.moveTo(w);
    rtx.moveTo(x);

    // Mark all children of w and all children of x "out of order".
    markOutOfOrder(wtx, inOrderOldRev);
    markOutOfOrder(rtx, inOrderNewRev);

    // 2
    final List<Long> first = commonChildren(w, x, wtx, rtx, ReverseMap.FALSE);
    final List<Long> second = commonChildren(x, w, rtx, wtx, ReverseMap.TRUE);
    // 3 && 4
    final List<Pair<Long, Long>> s =
        Util.longestCommonSubsequence(first, second, (pX, pY) -> totalMatching.contains(pX, pY));
    // 5
    final Map<Long, Long> seen = new HashMap<>();
    for (final Pair<Long, Long> p : s) {
      inOrderOldRev.put(p.getFirst(), true);
      inOrderNewRev.put(p.getSecond(), true);
      seen.put(p.getFirst(), p.getSecond());
    }
    // 6
    for (final long a : first) {
      wtx.moveTo(a);
      final Long b = totalMatching.partner(a);
      // assert b != null;
      if (seen.get(a) == null && wtx.moveTo(a) && b != null && rtx.moveTo(b)) { // (a,
        // b)
        // \notIn
        // S
        // if (seen.get(a) == null || (seen.get(a) != null &&
        // !seen.get(a).equals(b))
        // && mInOrderOldRev.get(a) != null && !mInOrderOldRev.get(a)) { // (a,
        // b) \notIn S
        inOrderOldRev.put(a, true);
        inOrderNewRev.put(b, true);
        final int k = findPos(b, wtx, rtx);
        logger.debug("Move in align children: " + k);
        emitMove(a, w, k, wtx, rtx);
      }
    }
  }

  /**
   * Mark children out of order.
   *
   * @param rtx {@link XmlNodeReadOnlyTrx} reference
   * @param inOrder {@link Map} to put all children out of order
   */
  private static void markOutOfOrder(final XmlNodeReadOnlyTrx rtx, final Map<Long, Boolean> inOrder) {
    for (final AbstractAxis axis = new ChildAxis(rtx); axis.hasNext();) {
      axis.nextLong();
      inOrder.put(axis.asXmlNodeReadTrx().getNodeKey(), false);
    }
  }

  /**
   * The sequence of children of n whose partners are children of o. This is used by alignChildren().
   *
   * @param n parent node in a document tree
   * @param o corresponding parent node in the other tree
   * @param firstRtx {@link XmlNodeReadOnlyTrx} on pN node
   * @param secondRtx {@link XmlNodeReadOnlyTrx} on pO node
   * @param reverse determines if reverse partners need to be found
   * @return {@link List} of common child nodes
   */
  private List<Long> commonChildren(final long n, final long o, final XmlNodeReadOnlyTrx firstRtx,
      final XmlNodeReadOnlyTrx secondRtx, final ReverseMap reverse) {
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
          partner = totalMatching.reversePartner(firstRtx.getNodeKey());
        } else {
          partner = totalMatching.partner(firstRtx.getNodeKey());
        }

        if (partner != null) {
          secondRtx.moveTo(partner);
          if (secondRtx.getParentKey() == o) {
            retVal.add(firstRtx.getNodeKey());
          }
        }
      } while (firstRtx.hasRightSibling() && firstRtx.moveToRightSibling());
    }
    return retVal;
  }

  /**
   * Emits the move of node "child" to the pos-th child of node "parent".
   *  @param child child node to move
   * @param parent node where to insert the moved subtree
   * @param pos position among the childs to move to
   * @param wtx {@link XmlNodeTrx} implementation reference on old revision
   * @param rtx {@link XmlNodeReadOnlyTrx} implementation reference on new revision
   */
  private void emitMove(final long child, final long parent, final int pos, final XmlNodeTrx wtx,
      final XmlNodeReadOnlyTrx rtx) {
    assert child >= 0;
    assert parent >= 0;
    assert wtx != null;
    assert rtx != null;

    boolean moved = wtx.moveTo(child);
    assert moved;

    if (wtx.getKind() == NodeKind.ATTRIBUTE || wtx.getKind() == NodeKind.NAMESPACE) {
      // Attribute- and namespace-nodes can't be moved.
      return;
    }

    assert pos >= 0;
    moved = wtx.moveTo(parent);
    assert moved;

    try {
      if (pos == 0) {
        assert wtx.getKind() == NodeKind.ELEMENT || wtx.getKind() == NodeKind.XML_DOCUMENT;
        if (wtx.getFirstChildKey() == child) {
          logger.error("Something went wrong: First child and child may never be the same!");
        } else {
          if (wtx.moveTo(child)) {
            boolean isTextKind = wtx.getKind() == NodeKind.TEXT;

            checkFromNodeForTextRemoval(wtx, child);
            wtx.moveTo(parent);
            if (isTextKind && wtx.getFirstChildKey() != child) {
              if (wtx.hasFirstChild()) {
                wtx.moveToFirstChild();
                if (wtx.getKind() == NodeKind.TEXT) {
                  totalMatching.remove(wtx.getNodeKey());
                  wtx.remove();
                }
                wtx.moveTo(parent);
              }
            }

            if (wtx.getKind() == NodeKind.XML_DOCUMENT) {
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
        if (wtx.getKind() == NodeKind.TEXT && wtx.moveTo(child) && wtx.getKind() == NodeKind.TEXT) {
          wtx.moveTo(nodeKey);
          totalMatching.remove(wtx.getNodeKey());
        }
        wtx.moveTo(nodeKey);
        if (wtx.moveToRightSibling()) {
          final long rightNodeKey = wtx.getNodeKey();
          if (wtx.getKind() == NodeKind.TEXT && wtx.moveTo(child) && wtx.getKind() == NodeKind.TEXT) {
            wtx.moveTo(rightNodeKey);
            totalMatching.remove(wtx.getNodeKey());
          }
          wtx.moveToLeftSibling();
        }

        // Move.
        moved = wtx.moveTo(nodeKey);
        assert moved;
        assert wtx.getNodeKey() != child;

        wtx.moveTo(wtx.moveSubtreeToRightSibling(child).getNodeKey());
      }
    } catch (final SirixException e) {
      logger.error(e.getMessage(), e);
    }
  }

  private void checkFromNodeForTextRemoval(final XmlNodeTrx wtx, final long child) {
    final boolean maybeRemoveLeftSibling = wtx.getLeftSiblingKey() == child;
    if (wtx.moveTo(child)) {
      boolean isText = false;
      if (wtx.hasLeftSibling()) {
        wtx.moveToLeftSibling();
        if (wtx.getKind() == NodeKind.TEXT) {
          isText = true;
        }
        wtx.moveToRightSibling();
      }
      if (isText && wtx.hasRightSibling()) {
        wtx.moveToRightSibling();
        if (wtx.getKind() == NodeKind.TEXT) {
          if (maybeRemoveLeftSibling) {
            boolean moved = wtx.moveToLeftSibling();
            assert moved;
            moved = wtx.moveToLeftSibling();
            assert moved;
          }
          totalMatching.remove(wtx.getNodeKey());
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
   * @param wtx {@link XmlNodeTrx} implementation reference on old revision
   * @param rtx {@link XmlNodeReadOnlyTrx} implementation reference on new revision
   */
  private static void emitUpdate(final long fromNode, final long toNode, final XmlNodeTrx wtx,
      final XmlNodeReadOnlyTrx rtx) {
    assert fromNode >= 0;
    assert toNode >= 0;
    assert wtx != null;
    assert rtx != null;

    wtx.moveTo(fromNode);
    rtx.moveTo(toNode);

    try {
      switch (rtx.getKind()) {
        case ELEMENT, ATTRIBUTE, NAMESPACE, PROCESSING_INSTRUCTION -> {
          assert rtx.getKind() == NodeKind.ELEMENT || rtx.getKind() == NodeKind.ATTRIBUTE
              || rtx.getKind() == NodeKind.NAMESPACE || rtx.getKind() == NodeKind.PROCESSING_INSTRUCTION;
          wtx.setName(rtx.getName());
          if (wtx.getKind() == NodeKind.ATTRIBUTE || wtx.getKind() == NodeKind.PROCESSING_INSTRUCTION) {
            wtx.setValue(rtx.getValue());
          }
        }
        case TEXT, COMMENT -> {
          assert wtx.getKind() == NodeKind.TEXT;
          wtx.setValue(rtx.getValue());
        }
        // $CASES-OMITTED$
        default -> {
        }
      }
    } catch (final SirixException e) {
      logger.error(e.getMessage(), e);
    }
  }

  /**
   * Emit an insert operation.
   *
   * @param child the current node to insert
   * @param parent parent of the current node to insert
   * @param pos position of the insert
   * @param wtx {@link XmlNodeTrx} implementation reference on old revision
   * @param rtx {@link XmlNodeReadOnlyTrx} implementation reference on new revision
   * @return inserted {@link Node} implementation reference
   * @throws SirixException if anything in sirix fails
   */
  private long emitInsert(final long child, final long parent, final int pos, final XmlNodeTrx wtx,
      final XmlNodeReadOnlyTrx rtx) {
    assert child >= 0;
    assert parent >= 0;
    assert wtx != null;
    assert rtx != null;

    // Determines if node has been already inserted (for subtrees).
    if (alreadyInserted.get(child) != null) {
      return child; // actually child'
    }

    wtx.moveTo(parent);
    rtx.moveTo(child);

    try {
      switch (rtx.getKind()) {
        case ATTRIBUTE -> {
          try {
            wtx.insertAttribute(rtx.getName(), rtx.getValue());
          } catch (final SirixUsageException e) {
            totalMatching.remove(wtx.getNodeKey());
            if (!rtx.getValue().isEmpty())
              wtx.setValue(rtx.getValue());
          }
          process(wtx.getNodeKey(), rtx.getNodeKey());
        }
        case NAMESPACE -> {
          // Note that the insertion is right (localPart as prefix).
          try {
            final QNm qName = rtx.getName();
            wtx.insertNamespace(new QNm(qName.getNamespaceURI(), qName.getPrefix(), qName.getLocalName()));
          } catch (final SirixUsageException e) {
            totalMatching.remove(wtx.getNodeKey());
          }
          process(wtx.getNodeKey(), rtx.getNodeKey());
        }
        // $CASES-OMITTED$
        default -> {
          // In case of other node types.
          long oldKey = 0;
          if (pos == 0) {
            switch (rtx.getKind()) {
              case ELEMENT -> oldKey = wtx.copySubtreeAsFirstChild(rtx).getNodeKey();
              case TEXT -> {
                // Remove first child text node if there is one and a new text node is inserted.
                if (wtx.hasFirstChild()) {
                  wtx.moveToFirstChild();
                  if (wtx.getKind() == NodeKind.TEXT) {
                    totalMatching.remove(wtx.getNodeKey());
                    wtx.remove();
                  }
                  wtx.moveTo(parent);
                }
                oldKey = wtx.insertTextAsFirstChild(rtx.getValue()).getNodeKey();
              }
              // $CASES-OMITTED$
              default -> {
                // Already inserted.
              }
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
            oldKey = switch (rtx.getKind()) {
              case ELEMENT -> wtx.copySubtreeAsRightSibling(rtx).getNodeKey();
              case TEXT -> wtx.insertTextAsRightSibling(rtx.getValue()).getNodeKey();
              // $CASES-OMITTED$
              default ->
                // Already inserted.
                  throw new IllegalStateException("Child should be already inserted!");
            };
          }

          // Mark all nodes in subtree as inserted.
          wtx.moveTo(oldKey);
          rtx.moveTo(child);
          for (final Axis oldAxis = new DescendantAxis(wtx, IncludeSelf.YES), newAxis =
               new DescendantAxis(rtx, IncludeSelf.YES); oldAxis.hasNext() && newAxis.hasNext(); ) {
            oldAxis.nextLong();
            newAxis.nextLong();
            final XmlNodeReadOnlyTrx oldRtx = oldAxis.asXmlNodeReadTrx();
            final XmlNodeReadOnlyTrx newRtx = newAxis.asXmlNodeReadTrx();
            process(oldRtx.getNodeKey(), newRtx.getNodeKey());
            final long newNodeKey = newRtx.getNodeKey();
            final long oldNodeKey = oldRtx.getNodeKey();
            if (newRtx.getKind() == NodeKind.ELEMENT) {
              assert newRtx.getKind() == oldRtx.getKind();
              if (newRtx.getAttributeCount() > 0) {
                for (int i = 0, attCount = newRtx.getAttributeCount(); i < attCount; i++) {
                  rtx.moveToAttribute(i);
                  for (int j = 0, oldAttCount = oldRtx.getAttributeCount(); j < oldAttCount; j++) {
                    wtx.moveToAttribute(j);
                    if (wtx.getName().equals(rtx.getName())) {
                      process(oldAxis.asXmlNodeReadTrx().getNodeKey(), newAxis.asXmlNodeReadTrx().getNodeKey());
                      break;
                    }
                    oldAxis.asXmlNodeReadTrx().moveTo(oldNodeKey);
                  }
                  newAxis.asXmlNodeReadTrx().moveTo(newNodeKey);
                }
              }
              if (newRtx.getNamespaceCount() > 0) {
                for (int i = 0, nspCount = newRtx.getNamespaceCount(); i < nspCount; i++) {
                  rtx.moveToNamespace(i);
                  for (int j = 0, oldNspCount = oldRtx.getNamespaceCount(); j < oldNspCount; j++) {
                    wtx.moveToNamespace(j);
                    if (wtx.getName().getNamespaceURI().equals(rtx.getName().getNamespaceURI()) && wtx.getName()
                                                                                                      .getPrefix()
                                                                                                      .equals(wtx.getName()
                                                                                                                 .getPrefix())) {
                      process(wtx.getNodeKey(), rtx.getNodeKey());
                      break;
                    }
                    oldAxis.asXmlNodeReadTrx().moveTo(oldNodeKey);
                  }
                  newAxis.asXmlNodeReadTrx().moveTo(newNodeKey);
                }
              }
            }

            newAxis.asXmlNodeReadTrx().moveTo(newNodeKey);
          }
        }
      }
    } catch (final SirixException e) {
      logger.error(e.getMessage(), e);
    }

    return wtx.getNodeKey();
  }

  /**
   * Remove right sibling text node from the storage as well as from the matching.
   *
   * @param wtx sirix {@link XmlNodeTrx}
   * @throws SirixException if removing of node in the storage fails
   */
  private void removeRightSiblingTextNode(final XmlNodeTrx wtx) throws SirixException {
    assert wtx != null;
    if (wtx.hasRightSibling()) {
      final long nodeKey = wtx.getNodeKey();
      wtx.moveToRightSibling();
      if (wtx.getKind() == NodeKind.TEXT) {
        totalMatching.remove(wtx.getNodeKey());
        wtx.remove();
      }
      wtx.moveTo(nodeKey);
    }
  }

  /**
   * Process nodes and update data structures.
   *
   * @param oldKey {@link Node} in old revision
   * @param newKey {@link Node} in new revision
   */
  private void process(final long oldKey, final long newKey) {
    alreadyInserted.put(newKey, true);
    final Long partner = totalMatching.partner(oldKey);
    if (partner != null) {
      totalMatching.remove(oldKey);
    }
    final Long reversePartner = totalMatching.reversePartner(newKey);
    if (reversePartner != null) {
      totalMatching.remove(reversePartner);
    }
    assert !totalMatching.contains(oldKey, newKey);
    totalMatching.add(oldKey, newKey);
    inOrderOldRev.put(oldKey, true);
    inOrderNewRev.put(newKey, true);
  }

  /**
   * The position of node x in the destination tree (tree2).
   *
   * @param x a node in the second (new) document
   * @param wtx {@link XmlNodeTrx} implementation reference on old revision
   * @param rtx {@link XmlNodeReadOnlyTrx} implementation reference on new revision
   * @return it's position, with respect to already inserted/deleted nodes
   */
  private int findPos(final long x, final XmlNodeTrx wtx, final XmlNodeReadOnlyTrx rtx) {
    assert x > 0;
    assert wtx != null;
    assert rtx != null;
    rtx.moveTo(x);

    if (rtx.getKind() == NodeKind.ATTRIBUTE || rtx.getKind() == NodeKind.NAMESPACE) {
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
        if (inOrderNewRev.get(v) != null && inOrderNewRev.get(v) && v == x) {
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
      while (rtx.hasLeftSibling()
          && (inOrderNewRev.get(v) == null || (inOrderNewRev.get(v) != null && !inOrderNewRev.get(v)))) {
        rtx.moveToLeftSibling();
        v = rtx.getNodeKey();
      }

      // Step 2 states that in ``in order'' node exists, but this is not
      // true.
      if (inOrderNewRev.get(v) == null) {
        // Assume it is the first node (undefined in the paper).
        return 0;
      }

      // 4 - Let u be the partner of v in T1
      Long u = totalMatching.reversePartner(v);
      int i = -1;
      if (u != null) {
        final boolean moved = wtx.moveTo(u);
        assert moved;

        // Suppose u is the i-th child of its parent (counting from left to
        // right) that is marked "in order". Return i+1.
        final long toNodeKey = u;
        wtx.moveToParent();
        wtx.moveToFirstChild();
        do {
          u = wtx.getNodeKey();
          i++;
        } while (u != toNodeKey && wtx.hasRightSibling() && wtx.moveToRightSibling());
      }

      return i + 1;
    }
  }

  /**
   * The fast match algorithm. Try to resolve the "good matching problem".
   *
   * @param wtx {@link XmlNodeTrx} implementation reference on old revision
   * @param rtx {@link XmlNodeReadOnlyTrx} implementation reference on new revision
   * @return {@link Matching} reference with matched nodes
   * @throws SirixException if anything in sirix fails
   */
  private Matching fastMatch(final XmlNodeTrx wtx, final XmlNodeReadOnlyTrx rtx) {
    assert wtx != null;
    assert rtx != null;

    final FMSENodeComparisonUtils nodeComparisonUtils =
        new FMSENodeComparisonUtils(oldStartKey, newStartKey, wtx, rtx);

    // Chain all nodes with a given label l in tree T together.
    getLabels(wtx, labelOldRevVisitor);
    getLabels(rtx, labelNewRevVisitor);

    // Do the matching job on the leaf nodes.
    final Matching matching = new Matching(wtx, rtx);
    matching.reset();
    match(labelOldRevVisitor.getLeafLabels(), labelNewRevVisitor.getLeafLabels(), matching,
          new LeafNodeComparator(idName, this.wtx, this.rtx, oldPathSummary, newPathSummary, nodeComparisonUtils));

    // Remove roots ('/') from labels and append them to mapping.
    final Map<NodeKind, List<Long>> oldLabels = labelOldRevVisitor.getLabels();
    final Map<NodeKind, List<Long>> newLabels = labelNewRevVisitor.getLabels();
    oldLabels.remove(NodeKind.XML_DOCUMENT);
    newLabels.remove(NodeKind.XML_DOCUMENT);

    wtx.moveTo(oldStartKey);
    rtx.moveTo(newStartKey);
    wtx.moveToParent();
    rtx.moveToParent();
    matching.add(wtx.getNodeKey(), rtx.getNodeKey());

    final NodeComparator<Long> innerNodeComparator =
        nodeComparisonFactory.createInnerNodeEqualityChecker(idName, matching, wtx, rtx,
                                                             new FMSENodeComparisonUtils(oldStartKey,
                                                                                          newStartKey, wtx, rtx),
                                                             descendantsOldRev, descendantsNewRev);

    match(oldLabels, newLabels, matching, innerNodeComparator);

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
  private static void match(final Map<NodeKind, List<Long>> oldLabels, final Map<NodeKind, List<Long>> newLabels,
      final Matching matching, final NodeComparator<Long> cmp) {
    final Set<NodeKind> labels = oldLabels.keySet();
    labels.retainAll(newLabels.keySet()); // intersection

    // 2 - for each label do
    for (final NodeKind label : labels) {
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
  private static void removeCommonNodes(final List<Long> list, final Map<Long, Boolean> seen) {
    assert list != null;
    assert seen != null;

    list.removeIf(seen::containsKey);
  }

  /**
   * Initialize data structures.
   *
   * @param rtx {@link XmlNodeReadOnlyTrx} reference on old revision
   * @param visitor {@link XmlNodeVisitor} reference
   * @throws SirixException if anything in sirix fails
   */
  private static void init(final XmlNodeReadOnlyTrx rtx, final XmlNodeVisitor visitor) {
    assert visitor != null;

    final long nodeKey = rtx.getNodeKey();
    for (final Axis axis = new PostOrderAxis(rtx); axis.hasNext();) {
      axis.nextLong();
      if (axis.asXmlNodeReadTrx().getNodeKey() == nodeKey) {
        break;
      }
      axis.asXmlNodeReadTrx().acceptVisitor(visitor);
    }
    rtx.acceptVisitor(visitor);
  }

  /**
   * Creates a flat list of all nodes by doing an in-order-traversal. NOTE: Since this is not a binary
   * tree, we use post-order-traversal (wrong in paper). For each node type (element, attribute, text,
   * comment, ...) there is a separate list.
   *
   * @param rtx {@link XmlNodeReadOnlyTrx} reference
   * @param visitor {@link LabelFMSEVisitor} used to save node type/list
   */
  private static void getLabels(final XmlNodeReadOnlyTrx rtx, final LabelFMSEVisitor visitor) {
    assert rtx != null;
    assert visitor != null;

    final long nodeKey = rtx.getNodeKey();
    for (final var axis = new PostOrderAxis(rtx); axis.hasNext();) {
      axis.nextLong();
      if (axis.asXmlNodeReadTrx().getNodeKey() == nodeKey) {
        break;
      }
      axis.asXmlNodeReadTrx().acceptVisitor(visitor);
    }
    rtx.acceptVisitor(visitor);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void close() {
    wtx.commit();
    oldPathSummary.close();
    newPathSummary.close();
  }
}
