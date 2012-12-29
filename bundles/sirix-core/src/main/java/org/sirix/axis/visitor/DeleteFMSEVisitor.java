package org.sirix.axis.visitor;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.access.AbstractVisitor;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.VisitResultType;
import org.sirix.axis.DescendantAxis;
import org.sirix.diff.algorithm.fmse.Matching;
import org.sirix.exception.SirixException;
import org.sirix.node.Kind;
import org.sirix.node.immutable.ImmutableComment;
import org.sirix.node.immutable.ImmutableElement;
import org.sirix.node.immutable.ImmutablePI;
import org.sirix.node.immutable.ImmutableText;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

/**
 * Visitor implementation for use with the {@link VisitorDescendantAxis} to
 * delete unmatched nodes in the FSME implementation in the second step.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class DeleteFMSEVisitor extends AbstractVisitor {

	/** {@link LogWrapper} reference. */
	private static final LogWrapper LOGWRAPPER = new LogWrapper(
			LoggerFactory.getLogger(DeleteFMSEVisitor.class));

	/** {@link Matching} reference. */
	private final Matching mMatching;

	/** sirix {@link NodeWriteTrx}. */
	private final NodeWriteTrx mWtx;

	/** Start key. */
	private final long mStartKey;

	/**
	 * Constructor. pStartKey
	 * 
	 * @param wtx
	 *          sirix {@link NodeWriteTrx}
	 * @param matching
	 *          {@link Matching} reference
	 * @param startKey
	 *          start key
	 */
	public DeleteFMSEVisitor(final @Nonnull NodeWriteTrx wtx,
			final @Nonnull Matching matching, @Nonnegative final long startKey) {
		mWtx = checkNotNull(wtx);
		mMatching = checkNotNull(matching);
		checkArgument(startKey >= 0, "start key must be >= 0!");
		mStartKey = startKey;
	}

	@Override
	public VisitResult visit(final @Nonnull ImmutableElement node) {
		final Long partner = mMatching.partner(node.getNodeKey());
		if (partner == null) {
			VisitResult retVal = delete(node);
			if (node.getNodeKey() == mStartKey) {
				retVal = VisitResultType.TERMINATE;
			}
			return retVal;
		} else {
			mWtx.moveTo(node.getNodeKey());
			final long nodeKey = node.getNodeKey();
			final List<Long> keysToDelete = new ArrayList<>(mWtx.getAttributeCount()
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
			return VisitResultType.CONTINUE;
		}
	}

	@Override
	public VisitResult visit(final @Nonnull ImmutableText node) {
		return deleteLeaf(node);
	}

	@Override
	public VisitResult visit(final @Nonnull ImmutableComment node) {
		return deleteLeaf(node);
	}

	@Override
	public VisitResult visit(final @Nonnull ImmutablePI node) {
		return deleteLeaf(node);
	}

	/**
	 * Delete a leaf node.
	 * 
	 * @param node
	 *          the node to delete
	 * @return the result of the deletion
	 */
	private VisitResult deleteLeaf(final @Nonnull ImmutableNode node) {
		final Long partner = mMatching.partner(node.getNodeKey());
		if (partner == null) {
			VisitResult retVal = delete(node);
			if (node.getNodeKey() == mStartKey) {
				retVal = VisitResultType.TERMINATE;
			}
			return retVal;
		} else {
			return VisitResultType.CONTINUE;
		}
	}

	/**
	 * Determines if a node must be deleted. If yes, it is deleted and
	 * {@code true} is returned. If it must not be deleted {@code false} is
	 * returned. The transaction is moved accordingly in case of a
	 * remove-operation such that the {@link DescendantAxis} can move to the next
	 * node after a delete occurred.
	 * 
	 * @param pNode
	 *          the node to check and possibly delete
	 * @return {@code EVisitResult} how to move the transaction subsequently
	 */
	private VisitResult delete(final @Nonnull ImmutableNode pNode) {
		try {
			mWtx.moveTo(pNode.getNodeKey());
			final long nodeKey = mWtx.getNodeKey();
			boolean removeTextNode = false;
			boolean resetValue = false;
			if (mWtx.hasLeftSibling() && mWtx.moveToLeftSibling().hasMoved()
					&& mWtx.getKind() == Kind.TEXT
					&& mWtx.moveToRightSibling().hasMoved() && mWtx.hasRightSibling()
					&& mWtx.moveToRightSibling().hasMoved()
					&& mWtx.getKind() == Kind.TEXT) {
				final Long partner = mMatching.partner(mWtx.getNodeKey());
				if (partner == null) {
					// Case: Right text node should be deleted (thus, the value must not
					// be appended to the left text node during deletion) => Reset value
					// afterwards.
					resetValue = true;
				}
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
			if (!mWtx.hasRightSibling() && mWtx.hasLeftSibling()) {
				final long leftSiblKey = mWtx.getLeftSiblingKey();
				mWtx.remove();
				assert mWtx.getNodeKey() == leftSiblKey;
				return VisitResultType.SKIPSUBTREE;
			}

			// Case: Has right sibl. and left sibl.
			if (mWtx.hasRightSibling() && mWtx.hasLeftSibling()) {
				final long rightSiblKey = mWtx.getRightSiblingKey();
				final long rightRightSiblKey = mWtx.moveToRightSibling().get()
						.getRightSiblingKey();
				mWtx.moveTo(nodeKey);
				final String value = removeTextNode ? mWtx.moveToLeftSibling().get()
						.getValue() : "";
				mWtx.moveTo(nodeKey);
				mWtx.remove();
				if (removeTextNode) {
					// Make sure to reset value.
					if (resetValue && !value.equals(mWtx.getValue())) {
						mWtx.setValue(value);
					}
					assert mWtx.getKind() == Kind.TEXT;
					assert mWtx.getRightSiblingKey() == rightRightSiblKey;
					return VisitResultType.CONTINUE;
				} else {
					final boolean moved = mWtx.moveToLeftSibling().hasMoved();
					assert moved;
					assert mWtx.getRightSiblingKey() == rightSiblKey;
					return VisitResultType.SKIPSUBTREE;
				}
			}

			// Case: Has right sibl. but no left sibl.
			if (mWtx.hasRightSibling() && !mWtx.hasLeftSibling()) {
				final long rightSiblKey = mWtx.getRightSiblingKey();
				mWtx.remove();
				mWtx.moveToParent();
				assert mWtx.getFirstChildKey() == rightSiblKey;
				return VisitResultType.CONTINUE;
			}

			// Case: Has no right and no left sibl.
			final long parentKey = mWtx.getParentKey();
			mWtx.remove();
			assert mWtx.getNodeKey() == parentKey;
		} catch (final SirixException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}
		return VisitResultType.CONTINUE;
	}
}
