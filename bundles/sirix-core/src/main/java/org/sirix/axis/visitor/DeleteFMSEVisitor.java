package org.sirix.axis.visitor;

import org.checkerframework.checker.index.qual.NonNegative;
import org.sirix.access.trx.node.xml.AbstractXmlNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.VisitResultType;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.axis.DescendantAxis;
import org.sirix.diff.algorithm.fmse.Matching;
import org.sirix.exception.SirixException;
import org.sirix.node.NodeKind;
import org.sirix.node.immutable.xml.ImmutableComment;
import org.sirix.node.immutable.xml.ImmutableElement;
import org.sirix.node.immutable.xml.ImmutablePI;
import org.sirix.node.immutable.xml.ImmutableText;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Visitor implementation for use with the {@link VisitorDescendantAxis} to delete unmatched nodes
 * in the FSME implementation in the second step.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class DeleteFMSEVisitor extends AbstractXmlNodeVisitor {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER =
      new LogWrapper(LoggerFactory.getLogger(DeleteFMSEVisitor.class));

  /** {@link Matching} reference. */
  private final Matching matching;

  /** sirix {@link XmlNodeTrx}. */
  private final XmlNodeTrx wtx;

  /** Start key. */
  private final long startKey;

  /**
   * Constructor. pStartKey
   * 
   * @param wtx sirix {@link XmlNodeTrx}
   * @param matching {@link Matching} reference
   * @param startKey start key
   */
  public DeleteFMSEVisitor(final XmlNodeTrx wtx, final Matching matching,
      @NonNegative final long startKey) {
    this.wtx = checkNotNull(wtx);
    this.matching = checkNotNull(matching);
    checkArgument(startKey >= 0, "start key must be >= 0!");
    this.startKey = startKey;
  }

  @Override
  public VisitResult visit(final ImmutableElement node) {
    final Long partner = matching.partner(node.getNodeKey());
    if (partner == null) {
      VisitResult retVal = delete(node);
      if (node.getNodeKey() == startKey) {
        retVal = VisitResultType.TERMINATE;
      }
      return retVal;
    } else {
      wtx.moveTo(node.getNodeKey());
      final long nodeKey = node.getNodeKey();
      final List<Long> keysToDelete =
          new ArrayList<>(wtx.getAttributeCount() + wtx.getNamespaceCount());
      for (int i = 0, attCount = wtx.getAttributeCount(); i < attCount; i++) {
        wtx.moveToAttribute(i);
        final long attNodeKey = wtx.getNodeKey();
        if (matching.partner(attNodeKey) == null) {
          keysToDelete.add(attNodeKey);
        }
        wtx.moveTo(nodeKey);
      }
      for (int i = 0, nspCount = wtx.getNamespaceCount(); i < nspCount; i++) {
        wtx.moveToNamespace(i);
        final long namespNodeKey = wtx.getNodeKey();
        if (matching.partner(namespNodeKey) == null) {
          keysToDelete.add(namespNodeKey);
        }
        wtx.moveTo(nodeKey);
      }

      for (final long keyToDelete : keysToDelete) {
        wtx.moveTo(keyToDelete);
        try {
          wtx.remove();
        } catch (final SirixException e) {
          LOGWRAPPER.error(e.getMessage(), e);
        }
      }

      wtx.moveTo(nodeKey);
      return VisitResultType.CONTINUE;
    }
  }

  @Override
  public VisitResult visit(final ImmutableText node) {
    return deleteLeaf(node);
  }

  @Override
  public VisitResult visit(final ImmutableComment node) {
    return deleteLeaf(node);
  }

  @Override
  public VisitResult visit(final ImmutablePI node) {
    return deleteLeaf(node);
  }

  /**
   * Delete a leaf node.
   * 
   * @param node the node to delete
   * @return the result of the deletion
   */
  private VisitResult deleteLeaf(final ImmutableNode node) {
    final Long partner = matching.partner(node.getNodeKey());
    if (partner == null) {
      VisitResult retVal = delete(node);
      if (node.getNodeKey() == startKey) {
        retVal = VisitResultType.TERMINATE;
      }
      return retVal;
    } else {
      return VisitResultType.CONTINUE;
    }
  }

  /**
   * Determines if a node must be deleted. If yes, it is deleted and {@code true} is returned. If it
   * must not be deleted {@code false} is returned. The transaction is moved accordingly in case of
   * a remove-operation such that the {@link DescendantAxis} can move to the next node after a
   * delete occurred.
   * 
   * @param node the node to check and possibly delete
   * @return {@code EVisitResult} how to move the transaction subsequently
   */
  private VisitResult delete(final ImmutableNode node) {
    try {
      wtx.moveTo(node.getNodeKey());
      final long nodeKey = wtx.getNodeKey();
      boolean removeTextNode = false;
      boolean resetValue = false;
      if (wtx.hasLeftSibling() && wtx.moveToLeftSibling()
          && wtx.getKind() == NodeKind.TEXT && wtx.moveToRightSibling()
          && wtx.hasRightSibling() && wtx.moveToRightSibling()
          && wtx.getKind() == NodeKind.TEXT) {
        final Long partner = matching.partner(wtx.getNodeKey());
        if (partner == null) {
          // Case: Right text node should be deleted (thus, the value must not
          // be appended to the left text node during deletion) => Reset value
          // afterwards.
          resetValue = true;
        }
        removeTextNode = true;
      }
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

        final String value;

        if (removeTextNode) {
          wtx.moveToLeftSibling();
          value = wtx.getValue();
        } else {
          value = "";
        }

        wtx.moveTo(nodeKey);
        wtx.remove();
        if (removeTextNode) {
          // Make sure to reset value.
          if (resetValue && !value.equals(wtx.getValue())) {
            wtx.setValue(value);
          }
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
