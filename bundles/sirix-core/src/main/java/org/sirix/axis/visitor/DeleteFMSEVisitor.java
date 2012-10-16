package org.sirix.axis.visitor;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.access.AbsVisitor;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.visitor.EVisitResult;
import org.sirix.api.visitor.IVisitResult;
import org.sirix.axis.DescendantAxis;
import org.sirix.diff.algorithm.fmse.Matching;
import org.sirix.exception.SirixException;
import org.sirix.node.Kind;
import org.sirix.node.immutable.ImmutableElement;
import org.sirix.node.immutable.ImmutableText;
import org.sirix.node.interfaces.Node;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

/**
 * Visitor implementation for use with the {@link VisitorDescendantAxis} to delete
 * unmatched nodes in the FSME implementation in the second step.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class DeleteFMSEVisitor extends AbsVisitor {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory
    .getLogger(DeleteFMSEVisitor.class));

  /** {@link Matching} reference. */
  private final Matching mMatching;

  /** sirix {@link NodeWriteTrx}. */
  private final NodeWriteTrx mWtx;

  /** Start key. */
  private final long mStartKey;

  /**
   * Constructor.
   * pStartKey
   * 
   * @param pWtx
   *          sirix {@link NodeWriteTrx}
   * @param pMatching
   *          {@link Matching} reference
   * @param pStartKey
   *          start key
   */
  public DeleteFMSEVisitor(@Nonnull final NodeWriteTrx pWtx,
    @Nonnull final Matching pMatching, @Nonnegative final long pStartKey) {
    mWtx = checkNotNull(pWtx);
    mMatching = checkNotNull(pMatching);
    checkArgument(pStartKey >= 0, "start key must be >= 0!");
    mStartKey = pStartKey;
  }

  @Override
  public IVisitResult visit(@Nonnull final ImmutableElement pNode) {
    final Long partner = mMatching.partner(pNode.getNodeKey());
    if (partner == null) {
      IVisitResult retVal = delete(pNode);
      if (pNode.getNodeKey() == mStartKey) {
        retVal = EVisitResult.TERMINATE;
      }
      return retVal;
    } else {
    	mWtx.moveTo(pNode.getNodeKey());
      final long nodeKey = pNode.getNodeKey();
      final List<Long> keysToDelete =
        new ArrayList<>(mWtx.getAttributeCount()
          + mWtx.getNamespaceCount());
      for (int i = 0, attCount = mWtx.getAttributeCount(); i < attCount; i++) {
        mWtx.moveToAttribute(i);
        final long attNodeKey = mWtx.getNodeKey();
        if (mMatching.partner(attNodeKey) == null) {
          keysToDelete.add(attNodeKey);
        }
        mWtx.moveTo(nodeKey);
      }
      for (int i = 0, nspCount = mWtx.getNamespaceCount(); i < nspCount; i++) {
        mWtx.moveToNamespace(i);
        final long namespNodeKey = mWtx.getNodeKey();
        if (mMatching.partner(namespNodeKey) == null) {
          keysToDelete.add(namespNodeKey);
        }
        mWtx.moveTo(nodeKey);
      }

      for (final long keyToDelete : keysToDelete) {
        mWtx.moveTo(keyToDelete);
        try {
          mWtx.remove();
        } catch (final SirixException e) {
          LOGWRAPPER.error(e.getMessage(), e);
        }
      }

      mWtx.moveTo(nodeKey);
      return EVisitResult.CONTINUE;
    }
  }

  @Override
  public IVisitResult visit(@Nonnull final ImmutableText pNode) {
    final Long partner = mMatching.partner(pNode.getNodeKey());
    if (partner == null) {
      IVisitResult retVal = delete(pNode);
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
  private IVisitResult delete(@Nonnull final Node pNode) {
    try {
      mWtx.moveTo(pNode.getNodeKey());
			final long nodeKey = mWtx.getNodeKey();
			boolean removeTextNode = false;
			if (mWtx.hasLeftSibling() && mWtx.moveToLeftSibling().hasMoved()
					&& mWtx.getKind() == Kind.TEXT
					&& mWtx.moveToRightSibling().hasMoved()
					&& mWtx.hasRightSibling()
					&& mWtx.moveToRightSibling().hasMoved()
					&& mWtx.getKind() == Kind.TEXT) {
				removeTextNode = true;
			}
			mWtx.moveTo(nodeKey);

			// Case: Has no right and no left sibl. but the parent has a right sibl.
			if (!removeTextNode) {
				final boolean movedToParent = mWtx.moveToParent().hasMoved();
				assert movedToParent;
				final long parentNodeKey = mWtx.getNodeKey();
				if (mWtx.getChildCount() == 1 && mWtx.hasRightSibling()) {
					mWtx.moveTo(nodeKey);
					mWtx.remove();
					assert mWtx.getNodeKey() == parentNodeKey;
					return LocalVisitResult.SKIPSUBTREEPOPSTACK;
				}
			}
			mWtx.moveTo(nodeKey);

			// Case: Has left sibl. but no right sibl.
			if (!mWtx.hasRightSibling()
					&& mWtx.hasLeftSibling()) {
				final long leftSiblKey = mWtx.getLeftSiblingKey();
				mWtx.remove();
				assert mWtx.getNodeKey() == leftSiblKey;
				return EVisitResult.SKIPSUBTREE;
			}

			// Case: Has right sibl. and left sibl.
			if (mWtx.hasRightSibling() && mWtx.hasLeftSibling()) {
				final long rightSiblKey = mWtx.getRightSiblingKey();
				final long rightRightSiblKey = mWtx.moveToRightSibling().get().getRightSiblingKey();
				mWtx.moveTo(nodeKey);
				mWtx.remove();
				if (removeTextNode) {
					assert mWtx.getKind() == Kind.TEXT;
					assert mWtx.getRightSiblingKey() == rightRightSiblKey;
					return EVisitResult.CONTINUE;
				} else {
					final boolean moved = mWtx.moveToLeftSibling().hasMoved();
					assert moved;
					assert mWtx.getRightSiblingKey() == rightSiblKey;
					return EVisitResult.SKIPSUBTREE;
				}
			}

			// Case: Has right sibl. but no left sibl.
			if (mWtx.hasRightSibling()
					&& !mWtx.hasLeftSibling()) {
				final long rightSiblKey = mWtx.getRightSiblingKey();
				mWtx.remove();
				mWtx.moveToParent();
				assert mWtx.getFirstChildKey() == rightSiblKey;
				return EVisitResult.CONTINUE;
			}

			// Case: Has no right and no left sibl.
			final long parentKey = mWtx.getParentKey();
			mWtx.remove();
			assert mWtx.getNodeKey() == parentKey;
		} catch (final SirixException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}
		return EVisitResult.CONTINUE;
  }
}
