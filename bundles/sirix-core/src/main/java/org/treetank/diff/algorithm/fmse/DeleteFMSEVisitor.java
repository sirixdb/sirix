package org.treetank.diff.algorithm.fmse;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.LoggerFactory;
import org.treetank.access.AbsVisitorSupport;
import org.treetank.api.INodeWriteTrx;
import org.treetank.api.visitor.EVisitResult;
import org.treetank.axis.DescendantAxis;
import org.treetank.exception.AbsTTException;
import org.treetank.node.ENode;
import org.treetank.node.ElementNode;
import org.treetank.node.TextNode;
import org.treetank.node.interfaces.INode;
import org.treetank.node.interfaces.IStructNode;
import org.treetank.utils.LogWrapper;

/**
 * Visitor implementation for use with the {@link DescendantAxis} to delete
 * unmatched nodes in the FSME implementation in the second step.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class DeleteFMSEVisitor extends AbsVisitorSupport {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory
    .getLogger(DeleteFMSEVisitor.class));

  /** {@link Matching} reference. */
  private final Matching mMatching;

  /** Treetank {@link INodeWriteTrx}. */
  private final INodeWriteTrx mWtx;

  /** Start key. */
  private final long mStartKey;

  /**
   * Constructor.
   * pStartKey
   * 
   * @param pWtx
   *          Treetank {@link INodeWriteTrx}
   * @param pMatching
   *          {@link Matching} reference
   * @param pStartKey
   *          start key
   */
  public DeleteFMSEVisitor(final INodeWriteTrx pWtx, final Matching pMatching, final long pStartKey) {
    mWtx = checkNotNull(pWtx);
    mMatching = checkNotNull(pMatching);
    checkArgument(pStartKey >= 0, "start key must be >= 0!");
    mStartKey = pStartKey;
  }

  @Override
  public EVisitResult visit(final ElementNode pNode) {
    final Long partner = mMatching.partner(pNode.getNodeKey());
    if (partner == null) {
      EVisitResult retVal = delete(pNode);
      if (pNode.getNodeKey() == mStartKey) {
        retVal = EVisitResult.TERMINATE;
      }
      return retVal;
    } else {
      final long nodeKey = pNode.getNodeKey();
      final List<Long> keysToDelete =
        new ArrayList<Long>(pNode.getAttributeCount() + pNode.getNamespaceCount());
      for (int i = 0; i < pNode.getAttributeCount(); i++) {
        mWtx.moveToAttribute(i);
        final long attNodeKey = mWtx.getNode().getNodeKey();
        if (mMatching.partner(attNodeKey) == null) {
          keysToDelete.add(attNodeKey);
        }
        mWtx.moveTo(nodeKey);
      }
      for (int i = 0; i < pNode.getNamespaceCount(); i++) {
        mWtx.moveToNamespace(i);
        final long namespNodeKey = mWtx.getNode().getNodeKey();
        if (mMatching.partner(namespNodeKey) == null) {
          keysToDelete.add(namespNodeKey);
        }
        mWtx.moveTo(nodeKey);
      }

      for (final long keyToDelete : keysToDelete) {
        mWtx.moveTo(keyToDelete);
        try {
          mWtx.remove();
        } catch (final AbsTTException e) {
          LOGWRAPPER.error(e.getMessage(), e);
        }
      }

      mWtx.moveTo(nodeKey);
      return EVisitResult.CONTINUE;
    }
  }

  @Override
  public EVisitResult visit(final TextNode pNode) {
    final Long partner = mMatching.partner(pNode.getNodeKey());
    if (partner == null) {
      EVisitResult retVal = delete(pNode);
      if (pNode.getNodeKey() == mStartKey) {
        retVal = EVisitResult.TERMINATE;
      }
      return retVal;
    } else {
      return EVisitResult.CONTINUE;
    }
  }

  /**
   * Determines if a node must be deleted. If yes, it is deleted and {@code true} is returned. If it must
   * not be deleted {@code false} is returned. The transaction is moved accordingly in case of a
   * remove-operation such that the {@link DescendantAxis} can move to the next node after a delete
   * occurred.
   * 
   * @param pNode
   *          the node to check and possibly delete
   * @return {@code EVisitResult} how to move the transaction subsequently
   */
  private EVisitResult delete(final INode pNode) {
    try {
      mWtx.moveTo(pNode.getNodeKey());
      final long nodeKey = mWtx.getNode().getNodeKey();
      boolean removeTextNode = false;
      if (mWtx.getStructuralNode().hasLeftSibling() && mWtx.moveToLeftSibling()
        && mWtx.getNode().getKind() == ENode.TEXT_KIND && mWtx.moveToRightSibling()
        && mWtx.getStructuralNode().hasRightSibling() && mWtx.moveToRightSibling()
        && mWtx.getStructuralNode().getKind() == ENode.TEXT_KIND) {
        removeTextNode = true;
      }
      mWtx.moveTo(nodeKey);

      // Case: Has no right and no left sibl. but the parent has a right sibl.
      mWtx.moveToParent();
      final IStructNode node = mWtx.getStructuralNode();
      if (node.getChildCount() == 1 && node.hasRightSibling()) {
        mWtx.moveTo(nodeKey);
        mWtx.remove();
        return EVisitResult.SKIPSUBTREEPOPSTACK;
      }
      mWtx.moveTo(nodeKey);

      // Case: Has left sibl. but no right sibl.
      if (!mWtx.getStructuralNode().hasRightSibling() && mWtx.getStructuralNode().hasLeftSibling()) {
        mWtx.remove();
        return EVisitResult.CONTINUE;
      }

      // Case: Has right sibl. and left sibl.
      if (mWtx.getStructuralNode().hasRightSibling() && mWtx.getStructuralNode().hasLeftSibling()) {
        if (removeTextNode) {
          mWtx.moveToRightSibling();
          if (mWtx.getStructuralNode().hasRightSibling()) {
            mWtx.moveToLeftSibling();
            mWtx.remove();
            mWtx.moveToLeftSibling();
            return EVisitResult.SKIPSUBTREE;
          } else {
            mWtx.moveToLeftSibling();
            mWtx.remove();
            return EVisitResult.CONTINUE;
          }
        } else {
          mWtx.remove();
          mWtx.moveToLeftSibling();
          return EVisitResult.SKIPSUBTREE;
        }
      }

      // Case: Has right sibl. but no left sibl.
      if (mWtx.getStructuralNode().hasRightSibling() && !mWtx.getStructuralNode().hasLeftSibling()) {
        if (removeTextNode) {
          mWtx.moveToRightSibling();
          if (mWtx.getStructuralNode().hasRightSibling()) {
            mWtx.moveToLeftSibling();
            mWtx.remove();
            mWtx.moveToParent();
            return EVisitResult.CONTINUE;
          } else {
            mWtx.moveToLeftSibling();
            mWtx.remove();
            return EVisitResult.CONTINUE;
          }
        } else {
          mWtx.remove();
          mWtx.moveToParent();
          return EVisitResult.CONTINUE;
        }
      }

      // Case: Has no right and no left sibl.
      mWtx.remove();
    } catch (final AbsTTException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
    return EVisitResult.CONTINUE;
  }
}
