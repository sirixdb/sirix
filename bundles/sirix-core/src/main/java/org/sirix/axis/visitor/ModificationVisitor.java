package org.sirix.axis.visitor;

import org.brackit.xquery.atomic.QNm;
import org.sirix.access.trx.node.xml.AbstractXmlNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.VisitResultType;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.axis.DescendantAxis;
import org.sirix.exception.SirixException;
import org.sirix.node.NodeKind;
import org.sirix.node.immutable.xml.ImmutableElement;
import org.sirix.node.immutable.xml.ImmutableText;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.service.xml.shredder.XmlShredder;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Visitor implementation for use with the {@link VisitorDescendantAxis} to modify nodes.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class ModificationVisitor extends AbstractXmlNodeVisitor {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER =
      new LogWrapper(LoggerFactory.getLogger(ModificationVisitor.class));

  /** Determines the modify rate. */
  private static final int MODIFY_EVERY = 1111;

  /** Sirix {@link XmlNodeTrx}. */
  private final XmlNodeTrx wtx;

  /** Random number generator. */
  private final Random random = new Random();

  /** Start key. */
  private final long startKey;

  /** Current node index, that is every node is indexed starting at 1. */
  private long nodeIndex;

  /**
   * Constructor.
   *
   * @param wtx sirix {@link XmlNodeTrx}
   * @param startKey start key
   */
  public ModificationVisitor(final XmlNodeTrx wtx, final long startKey) {
    this.wtx = checkNotNull(wtx);
    checkArgument(startKey >= 0, "start key must be >= 0!");
    this.startKey = startKey;
    nodeIndex = 1;
  }

  @Override
  public VisitResult visit(final ImmutableElement node) {
    return processNode(node);
  }

  /**
   * Process a node, that is decide if it has to be deleted and move accordingly.
   *
   * @param node the node to check
   * @return the appropriate {@link VisitResultType} value
   */
  private VisitResult processNode(final ImmutableNode node) {
    assert node != null;
    final VisitResult result = modify(node);
    if (node.getNodeKey() == startKey) {
      return VisitResultType.TERMINATE;
    }
    return result;
  }

  @Override
  public VisitResult visit(final ImmutableText node) {
    return processNode(node);
  }

  /**
   * Determines if a node must be modified. If yes, it is deleted and {@code true} is returned. If
   * it must not be deleted {@code false} is returned. The transaction is moved accordingly in case
   * of a remove-operation such that the {@link DescendantAxis} can move to the next node after a
   * delete occurred.
   *
   * @param node the node to check and possibly delete
   * @return {@code true} if node has been deleted, {@code false} otherwise
   */
  private VisitResult modify(final ImmutableNode node) {
    assert node != null;
    if (nodeIndex % MODIFY_EVERY == 0) {
      nodeIndex = 1;

      try {
        switch (random.nextInt(4)) {
          case 0:
            final QNm insert = new QNm("testInsert");
            final long key = wtx.getNodeKey();
            wtx.insertElementAsLeftSibling(insert);
            final boolean moved = wtx.moveTo(key);
            assert moved;
            return VisitResultType.CONTINUE;
          case 1:
            if (wtx.getKind() == NodeKind.TEXT) {
              wtx.setValue("testUpdate");
            } else if (wtx.getKind() == NodeKind.ELEMENT) {
              wtx.setName(new QNm("testUpdate"));
            }
            return VisitResultType.CONTINUE;
          case 2:
            return delete();
          case 3:
            wtx.replaceNode(XmlShredder.createStringReader("<foo/>"));
            return VisitResultType.CONTINUE;
          default:
            break;
        }
      } catch (final SirixException e) {
        LOGWRAPPER.error(e.getMessage(), e);
        return VisitResultType.TERMINATE;
      }
    } else {
      nodeIndex++;
      return VisitResultType.CONTINUE;
    }
    return VisitResultType.CONTINUE;
  }

  /** Delete a subtree and determine movement. */
  private VisitResult delete() throws SirixException {
    try {
      final long nodeKey = wtx.getNodeKey();
      boolean removeTextNode = wtx.getLeftSiblingKind() == NodeKind.TEXT && wtx.getRightSiblingKind() == NodeKind.TEXT;
      wtx.moveTo(nodeKey);

      // Case: Has no right and no left sibl. but the parent has a right sibl.
      if (!removeTextNode) {
        final boolean movedToParent = wtx.moveToParent();
        assert movedToParent;
        final long parentNodeKey = wtx.getNodeKey();
        if (wtx.getChildCount() == 1 && wtx.hasRightSibling()) {
          wtx.moveTo(nodeKey);
          wtx.remove();
          assert wtx.getNodeKey() == parentNodeKey;
          return LocalVisitResult.SKIPSUBTREEPOPSTACK;
        }
      }
      wtx.moveTo(nodeKey);

      // Case: Has left sibl. but no right sibl.
      if (!wtx.hasRightSibling() && wtx.hasLeftSibling()) {
        final long leftSiblKey = wtx.getLeftSiblingKey();
        wtx.remove();
        assert wtx.getNodeKey() == leftSiblKey;
        return VisitResultType.SKIPSUBTREE;
      }

      // Case: Has right sibl. and left sibl.
      if (wtx.hasRightSibling() && wtx.hasLeftSibling()) {
        final long rightSiblKey = wtx.getRightSiblingKey();
        wtx.moveToRightSibling();
        final long rightRightSiblKey = wtx.getRightSiblingKey();
        wtx.moveTo(nodeKey);
        wtx.remove();
        if (removeTextNode) {
          assert wtx.getKind() == NodeKind.TEXT;
          assert wtx.getRightSiblingKey() == rightRightSiblKey;
          return VisitResultType.CONTINUE;
        } else {
          final boolean moved = wtx.moveToLeftSibling();
          assert moved;
          assert wtx.getRightSiblingKey() == rightSiblKey;
          return VisitResultType.SKIPSUBTREE;
        }
      }

      // Case: Has right sibl. but no left sibl.
      if (wtx.hasRightSibling() && !wtx.hasLeftSibling()) {
        final long rightSiblKey = wtx.getRightSiblingKey();
        wtx.remove();
        wtx.moveToParent();
        assert wtx.getFirstChildKey() == rightSiblKey;
        return VisitResultType.CONTINUE;
      }

      // Case: Has no right and no left sibl.
      final long parentKey = wtx.getParentKey();
      wtx.remove();
      assert wtx.getNodeKey() == parentKey;
    } catch (final SirixException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
    return VisitResultType.CONTINUE;
  }
}
