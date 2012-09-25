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

package org.sirix.access;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;

import org.sirix.access.conf.ResourceConfiguration.EIndexes;
import org.sirix.api.IAxis;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.INodeWriteTrx;
import org.sirix.api.IPageReadTrx;
import org.sirix.api.IPageWriteTrx;
import org.sirix.api.IPostCommitHook;
import org.sirix.api.IPreCommitHook;
import org.sirix.axis.ChildAxis;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.EIncludeSelf;
import org.sirix.axis.FilterAxis;
import org.sirix.axis.LevelOrderAxis;
import org.sirix.axis.PostOrderAxis;
import org.sirix.axis.filter.NameFilter;
import org.sirix.axis.filter.PathKindFilter;
import org.sirix.axis.filter.PathLevelFilter;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixThreadedException;
import org.sirix.exception.SirixUsageException;
import org.sirix.index.path.PathNode;
import org.sirix.index.path.PathSummary;
import org.sirix.index.value.AVLTree;
import org.sirix.node.AttributeNode;
import org.sirix.node.DocumentRootNode;
import org.sirix.node.EKind;
import org.sirix.node.ElementNode;
import org.sirix.node.NamespaceNode;
import org.sirix.node.TextNode;
import org.sirix.node.TextReferences;
import org.sirix.node.TextValue;
import org.sirix.node.interfaces.INameNode;
import org.sirix.node.interfaces.INode;
import org.sirix.node.interfaces.IStructNode;
import org.sirix.node.interfaces.IValNode;
import org.sirix.page.EPage;
import org.sirix.page.NamePage;
import org.sirix.page.UberPage;
import org.sirix.service.xml.serialize.StAXSerializer;
import org.sirix.service.xml.shredder.EInsert;
import org.sirix.service.xml.shredder.EShredderCommit;
import org.sirix.service.xml.shredder.XMLShredder;
import org.sirix.settings.EFixed;
import org.sirix.settings.IConstants;
import org.sirix.utils.XMLToken;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * <h1>NodeWriteTrx</h1>
 * 
 * <p>
 * Single-threaded instance of only write-transaction per resource.
 * </p>
 * 
 * <p>
 * All methods throw {@link NullPointerException}s in case of null values for
 * reference peters.
 * </p>
 * 
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 */
final class NodeWriteTrx extends AbsForwardingNodeReadTrx implements
		INodeWriteTrx {

	/**
	 * Operation type to determine behavior of path summary updates during
	 * {@code setQName(QName)} and the move-operations.
	 */
	private enum EOPType {
		/**
		 * Move from and to is on the same level (before and after the move, the
		 * node has the same parent).
		 */
		MOVEDSAMELEVEL,

		/**
		 * Move from and to is not on the same level (before and after the move, the
		 * node has a different parent).
		 */
		MOVED,

		/** A new {@link QName} is set. */
		SETNAME,
	}

	/** MD5 hash-function. */
	private final HashFunction mHash = Hashing.md5();

	/** Prime for computing the hash. */
	private static final int PRIME = 77081;

	/** Maximum number of node modifications before auto commit. */
	private final int mMaxNodeCount;

	/** Modification counter. */
	private long mModificationCount;

	/** Hash kind of Structure. */
	private final EHashKind mHashKind;

	/** Scheduled executor service. */
	private final ScheduledExecutorService mPool = Executors
			.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());

	/** {@link NodeReadTrx} reference. */
	private final NodeReadTrx mNodeRtx;

	/** Determines if a bulk insert operation is done. */
	private boolean mBulkInsert;

	/** Collection holding pre-commit hooks. */
	private final List<IPreCommitHook> mPreCommitHooks = new ArrayList<>();

	/** Collection holding post-commit hooks. */
	private final List<IPostCommitHook> mPostCommitHooks = new ArrayList<>();

	/** {@link PathSummary} instance. */
	private PathSummary mPathSummary;

	/** {@link AVLTree} instance. */
	private AVLTree<TextValue, TextReferences> mAVLTree;

	/** Indexes structures used during updates. */
	private final Set<EIndexes> mIndexes;

	/** {@link NodeFactory} to be able to create nodes. */
	private NodeFactory mNodeFactory;

	/** Determines if a path subtree must be deleted or not. */
	private enum ERemove {
		/** Yes, it must be deleted. */
		YES,

		/** No, it must not be deleted. */
		NO
	}

	/**
	 * Constructor.
	 * 
	 * @param pTransactionID
	 *          ID of transaction
	 * @param pSession
	 *          the {@link session} instance this transaction is bound to
	 * @param pPageWriteTrx
	 *          {@link IPageWriteTrx} to interact with the page layer
	 * @param pMaxNodeCount
	 *          maximum number of node modifications before auto commit
	 * @param pTimeUnit
	 *          unit of the number of the next param {@code pMaxTime}
	 * @param pMaxTime
	 *          maximum number of seconds before auto commit
	 * @throws SirixIOException
	 *           if the reading of the props is failing
	 * @throws SirixUsageException
	 *           if {@code pMaxNodeCount < 0} or {@code pMaxTime < 0}
	 */
	NodeWriteTrx(@Nonnegative final long pTransactionID,
			final @Nonnull Session pSession,
			final @Nonnull IPageWriteTrx pPageWriteTrx,
			@Nonnegative final int pMaxNodeCount, final @Nonnull TimeUnit pTimeUnit,
			@Nonnegative final int pMaxTime) throws SirixIOException,
			SirixUsageException {

		// Do not accept negative values.
		if ((pMaxNodeCount < 0) || (pMaxTime < 0)) {
			throw new SirixUsageException("Negative arguments are not accepted.");
		}

		mNodeRtx = new NodeReadTrx(pSession, pTransactionID, pPageWriteTrx);

		// Only auto commit by node modifications if it is more then 0.
		mMaxNodeCount = pMaxNodeCount;
		mModificationCount = 0L;
		mIndexes = mNodeRtx.mSession.mResourceConfig.mIndexes;

		// Indexes.
		if (mIndexes.contains(EIndexes.PATH)) {
			mPathSummary = PathSummary.getInstance(pPageWriteTrx, pSession);
		}
		if (mIndexes.contains(EIndexes.VALUE)) {
			mAVLTree = AVLTree.<TextValue, TextReferences> getInstance(pPageWriteTrx);
		}

		// Node factory.
		mNodeFactory = new NodeFactory(pPageWriteTrx);

		if (pMaxTime > 0) {
			mPool.scheduleAtFixedRate(new Runnable() {
				@Override
				public void run() {
					try {
						commit();
					} catch (final SirixException e) {
						throw new IllegalStateException(e);
					}
				}
			}, pMaxTime, pMaxTime, pTimeUnit);
		}

		mHashKind = pSession.mResourceConfig.mHashKind;

		// // Redo last transaction if the system crashed.
		// if (!pPageWriteTrx.isCreated()) {
		// try {
		// commit();
		// } catch (final SirixException e) {
		// throw new IllegalStateException(e);
		// }
		// }
	}

	@Override
	public INodeWriteTrx moveSubtreeToFirstChild(@Nonnegative final long pFromKey)
			throws SirixException, IllegalArgumentException {
		if (pFromKey < 0 || pFromKey > getMaxNodeKey()) {
			throw new IllegalArgumentException("Argument must be a valid node key!");
		}
		if (pFromKey == getNode().getNodeKey()) {
			throw new IllegalArgumentException(
					"Can't move itself to right sibling of itself!");
		}

		@SuppressWarnings("unchecked")
		final Optional<? extends INode> node = (Optional<? extends INode>) getPageTransaction()
				.getNode(pFromKey, EPage.NODEPAGE);
		if (!node.isPresent()) {
			throw new IllegalStateException("Node to move must exist!");
		}

		final INode nodeToMove = node.get();

		if (nodeToMove instanceof IStructNode
				&& getNode().getKind() == EKind.ELEMENT) {
			// Safe to cast (because IStructNode is a subtype of INode).
			checkAncestors(nodeToMove);
			checkAccessAndCommit();

			final ElementNode nodeAnchor = (ElementNode) getNode();
			if (nodeAnchor.getFirstChildKey() != nodeToMove.getNodeKey()) {
				final IStructNode toMove = (IStructNode) nodeToMove;
				// Adapt hashes.
				adaptHashesForMove(toMove);

				// Adapt pointers and merge sibling text nodes.
				adaptForMove(toMove, nodeAnchor, EInsertPos.ASFIRSTCHILD);
				mNodeRtx.setCurrentNode(toMove);
				adaptHashesWithAdd();

				// Adapt path summary.
				if (mIndexes.contains(EIndexes.PATH) && toMove instanceof INameNode) {
					final INameNode moved = (INameNode) toMove;
					adaptPathForChangedNode(moved, getQNameOfCurrentNode(),
							moved.getNameKey(), moved.getURIKey(), EOPType.MOVED);
				}
			}

			return this;
		} else {
			throw new SirixUsageException(
					"Move is not allowed if moved node is not an ElementNode and the node isn't inserted at an element node!");
		}
	}

	@Override
	public INodeWriteTrx moveSubtreeToLeftSibling(@Nonnegative final long pFromKey)
			throws SirixException, IllegalArgumentException {
		if (getStructuralNode().hasLeftSibling()) {
			moveToLeftSibling();
			return moveSubtreeToRightSibling(pFromKey);
		} else {
			moveToParent();
			return moveSubtreeToFirstChild(pFromKey);
		}
	}

	@Override
	public INodeWriteTrx moveSubtreeToRightSibling(
			@Nonnegative final long pFromKey) throws SirixException {
		if (pFromKey < 0 || pFromKey > getMaxNodeKey()) {
			throw new IllegalArgumentException("Argument must be a valid node key!");
		}
		if (pFromKey == getNode().getNodeKey()) {
			throw new IllegalArgumentException(
					"Can't move itself to first child of itself!");
		}

		// Save: Every node in the "usual" node page is of type INode.
		@SuppressWarnings("unchecked")
		final Optional<? extends INode> node = (Optional<? extends INode>) getPageTransaction()
				.getNode(pFromKey, EPage.NODEPAGE);
		if (!node.isPresent()) {
			throw new IllegalStateException("Node to move must exist!");
		}

		final INode nodeToMove = node.get();

		if (nodeToMove instanceof IStructNode && getNode() instanceof IStructNode) {
			final IStructNode toMove = (IStructNode) nodeToMove;
			checkAncestors(toMove);
			checkAccessAndCommit();

			final IStructNode nodeAnchor = (IStructNode) getNode();
			if (nodeAnchor.getRightSiblingKey() != nodeToMove.getNodeKey()) {
				final long parentKey = toMove.getParentKey();

				// Adapt hashes.
				adaptHashesForMove(toMove);

				// Adapt pointers and merge sibling text nodes.
				adaptForMove(toMove, nodeAnchor, EInsertPos.ASRIGHTSIBLING);
				mNodeRtx.setCurrentNode(toMove);
				adaptHashesWithAdd();

				// Adapt path summary.
				if (mIndexes.contains(EIndexes.PATH) && toMove instanceof INameNode) {
					final INameNode moved = (INameNode) toMove;
					final EOPType type = moved.getParentKey() == parentKey ? EOPType.MOVEDSAMELEVEL
							: EOPType.MOVED;
					adaptPathForChangedNode(moved, getQNameOfCurrentNode(),
							moved.getNameKey(), moved.getURIKey(), type);
				}
			}
			return this;
		} else {
			throw new SirixUsageException(
					"Move is not allowed if moved node is not an ElementNode or TextNode and the node isn't inserted at an ElementNode or TextNode!");
		}
	}

	/**
	 * Adapt hashes for move operation ("remove" phase).
	 * 
	 * @param pNodeToMove
	 *          node which implements {@link IStructNode} and is moved
	 * @throws SirixIOException
	 *           if any I/O operation fails
	 */
	private void adaptHashesForMove(final @Nonnull IStructNode pNodeToMove)
			throws SirixIOException {
		assert pNodeToMove != null;
		mNodeRtx.setCurrentNode(pNodeToMove);
		adaptHashesWithRemove();
	}

	/**
	 * Adapting everything for move operations.
	 * 
	 * @param pFromNode
	 *          root {@link IStructNode} of the subtree to be moved
	 * @param pToNode
	 *          the {@link IStructNode} which is the anchor of the new subtree
	 * @param pInsert
	 *          determines if it has to be inserted as a first child or a right
	 *          sibling
	 * @throws SirixException
	 *           if removing a node fails after merging text nodes
	 */
	private void adaptForMove(final @Nonnull IStructNode pFromNode,
			final @Nonnull IStructNode pToNode, final @Nonnull EInsertPos pInsert)
			throws SirixException {
		assert pFromNode != null;
		assert pToNode != null;
		assert pInsert != null;

		// Modify nodes where the subtree has been moved from.
		// ==============================================================================
		final IStructNode parent = (IStructNode) getPageTransaction()
				.prepareNodeForModification(pFromNode.getParentKey(), EPage.NODEPAGE);
		switch (pInsert) {
		case ASRIGHTSIBLING:
			if (pFromNode.getParentKey() != pToNode.getParentKey()) {
				parent.decrementChildCount();
			}
			break;
		case ASFIRSTCHILD:
			if (pFromNode.getParentKey() != pToNode.getNodeKey()) {
				parent.decrementChildCount();
			}
			break;
		case ASNONSTRUCTURAL:
			// Do not decrement child count.
			break;
		}

		// Adapt first child key of former parent.
		if (parent.getFirstChildKey() == pFromNode.getNodeKey()) {
			parent.setFirstChildKey(pFromNode.getRightSiblingKey());
		}
		getPageTransaction().finishNodeModification(parent, EPage.NODEPAGE);

		// Adapt left sibling key of former right sibling.
		if (pFromNode.hasRightSibling()) {
			final IStructNode rightSibling = (IStructNode) getPageTransaction()
					.prepareNodeForModification(pFromNode.getRightSiblingKey(),
							EPage.NODEPAGE);
			rightSibling.setLeftSiblingKey(pFromNode.getLeftSiblingKey());
			getPageTransaction().finishNodeModification(rightSibling, EPage.NODEPAGE);
		}

		// Adapt right sibling key of former left sibling.
		if (pFromNode.hasLeftSibling()) {
			final IStructNode leftSibling = (IStructNode) getPageTransaction()
					.prepareNodeForModification(pFromNode.getLeftSiblingKey(),
							EPage.NODEPAGE);
			leftSibling.setRightSiblingKey(pFromNode.getRightSiblingKey());
			getPageTransaction().finishNodeModification(leftSibling, EPage.NODEPAGE);
		}

		// Merge text nodes.
		if (pFromNode.hasLeftSibling() && pFromNode.hasRightSibling()) {
			moveTo(pFromNode.getLeftSiblingKey());
			if (getNode() != null && getNode().getKind() == EKind.TEXT) {
				final StringBuilder builder = new StringBuilder(getValueOfCurrentNode());
				moveTo(pFromNode.getRightSiblingKey());
				if (getNode() != null && getNode().getKind() == EKind.TEXT) {
					builder.append(getValueOfCurrentNode());
					if (pFromNode.getRightSiblingKey() == pToNode.getNodeKey()) {
						moveTo(pFromNode.getLeftSiblingKey());
						if (getStructuralNode().hasLeftSibling()) {
							final IStructNode leftSibling = (IStructNode) getPageTransaction()
									.prepareNodeForModification(
											getStructuralNode().getLeftSiblingKey(), EPage.NODEPAGE);
							leftSibling.setRightSiblingKey(pFromNode.getRightSiblingKey());
							getPageTransaction().finishNodeModification(leftSibling,
									EPage.NODEPAGE);
						}
						final long leftSiblingKey = getStructuralNode().hasLeftSibling() == true ? getStructuralNode()
								.getLeftSiblingKey() : getNode().getNodeKey();
						moveTo(pFromNode.getRightSiblingKey());
						final IStructNode rightSibling = (IStructNode) getPageTransaction()
								.prepareNodeForModification(getNode().getNodeKey(),
										EPage.NODEPAGE);
						rightSibling.setLeftSiblingKey(leftSiblingKey);
						getPageTransaction().finishNodeModification(rightSibling,
								EPage.NODEPAGE);
						moveTo(pFromNode.getLeftSiblingKey());
						remove();
						moveTo(pFromNode.getRightSiblingKey());
					} else {
						if (getStructuralNode().hasRightSibling()) {
							final IStructNode rightSibling = (IStructNode) getPageTransaction()
									.prepareNodeForModification(
											getStructuralNode().getRightSiblingKey(), EPage.NODEPAGE);
							rightSibling.setLeftSiblingKey(pFromNode.getLeftSiblingKey());
							getPageTransaction().finishNodeModification(rightSibling,
									EPage.NODEPAGE);
						}
						final long rightSiblingKey = getStructuralNode().hasRightSibling() == true ? getStructuralNode()
								.getRightSiblingKey() : getNode().getNodeKey();
						moveTo(pFromNode.getLeftSiblingKey());
						final IStructNode leftSibling = (IStructNode) getPageTransaction()
								.prepareNodeForModification(getNode().getNodeKey(),
										EPage.NODEPAGE);
						leftSibling.setRightSiblingKey(rightSiblingKey);
						getPageTransaction().finishNodeModification(leftSibling,
								EPage.NODEPAGE);
						moveTo(pFromNode.getRightSiblingKey());
						remove();
						moveTo(pFromNode.getLeftSiblingKey());
					}
					setValue(builder.toString());
				}
			}
		}

		// Modify nodes where the subtree has been moved to.
		// ==============================================================================
		pInsert.processMove(pFromNode, pToNode, this);
	}

	/**
	 * Insert a path node as first child.
	 * 
	 * @param pQName
	 *          {@link QName} of the path node (not stored) twice
	 * @param pKind
	 *          kind of node to index
	 * @param pLevel
	 *          level in the path summary
	 * @return this {@link WriteTransaction} instance
	 * @throws SirixException
	 *           if an I/O error occurs
	 */
	private INodeWriteTrx insertPathAsFirstChild(final @Nonnull QName pQName,
			final EKind pKind, final int pLevel) throws SirixException {
		if (!XMLToken.isValidQName(checkNotNull(pQName))) {
			throw new IllegalArgumentException("The QName is not valid!");
		}

		checkAccessAndCommit();

		final long parentKey = mPathSummary.getNode().getNodeKey();
		final long leftSibKey = EFixed.NULL_NODE_KEY.getStandardProperty();
		final long rightSibKey = mPathSummary.getStructuralNode()
				.getFirstChildKey();
		final PathNode node = mNodeFactory.createPathNode(parentKey, leftSibKey,
				rightSibKey, 0, pQName, pKind, pLevel);

		mPathSummary.setCurrentNode(node);
		adaptForInsert(node, EInsertPos.ASFIRSTCHILD, EPage.PATHSUMMARYPAGE);
		mPathSummary.setCurrentNode(node);

		return this;
	}

	@Override
	public INodeWriteTrx insertElementAsFirstChild(final @Nonnull QName pQName)
			throws SirixException {
		if (!XMLToken.isValidQName(checkNotNull(pQName))) {
			throw new IllegalArgumentException("The QName is not valid!");
		}
		final EKind kind = mNodeRtx.getNode().getKind();
		if (kind == EKind.ELEMENT || kind == EKind.DOCUMENT_ROOT) {
			checkAccessAndCommit();

			final long parentKey = mNodeRtx.getNode().getNodeKey();
			final long leftSibKey = EFixed.NULL_NODE_KEY.getStandardProperty();
			final long rightSibKey = ((IStructNode) mNodeRtx.getNode())
					.getFirstChildKey();

			final long pathNodeKey = mIndexes.contains(EIndexes.PATH) ? getPathNodeKey(
					pQName, EKind.ELEMENT) : 0;

			final ElementNode node = mNodeFactory.createElementNode(parentKey,
					leftSibKey, rightSibKey, 0, pQName, pathNodeKey);

			mNodeRtx.setCurrentNode(node);
			adaptForInsert(node, EInsertPos.ASFIRSTCHILD, EPage.NODEPAGE);
			mNodeRtx.setCurrentNode(node);
			adaptHashesWithAdd();

			return this;
		} else {
			throw new SirixUsageException(
					"Insert is not allowed if current node is not an ElementNode!");
		}
	}

	private long getPathNodeKey(final @Nonnull QName pQName,
			final @Nonnull EKind pKind) throws SirixException {
		final EKind kind = mNodeRtx.getNode().getKind();
		int level = 0;
		if (kind == EKind.DOCUMENT_ROOT) {
			mPathSummary.moveTo(EFixed.DOCUMENT_NODE_KEY.getStandardProperty());
		} else {
			movePathSummary();
			level = mPathSummary.getPathNode().getLevel();
		}

		final long nodeKey = mPathSummary.getNode().getNodeKey();
		final IAxis axis = new FilterAxis(new ChildAxis(mPathSummary),
				new NameFilter(mPathSummary,
						pKind == EKind.NAMESPACE ? pQName.getPrefix()
								: Utils.buildName(pQName)), new PathKindFilter(mPathSummary,
						pKind));
		long retVal = nodeKey;
		if (axis.hasNext()) {
			axis.next();
			retVal = mPathSummary.getNode().getNodeKey();
			final PathNode pathNode = (PathNode) getPageTransaction()
					.prepareNodeForModification(retVal, EPage.PATHSUMMARYPAGE);
			pathNode.incrementReferenceCount();
			getPageTransaction().finishNodeModification(pathNode,
					EPage.PATHSUMMARYPAGE);
		} else {
			assert nodeKey == mPathSummary.getNode().getNodeKey();
			insertPathAsFirstChild(pQName, pKind, level + 1);
			retVal = mPathSummary.getNode().getNodeKey();
		}
		return retVal;
	}

	/**
	 * Move path summary cursor to the path node which is references by the
	 * current node.
	 */
	private void movePathSummary() {
		if (mNodeRtx.getNode() instanceof INameNode) {
			mPathSummary.moveTo(((INameNode) mNodeRtx.getNode()).getPathNodeKey());
		} else {
			throw new IllegalStateException();
		}
	}

	@Override
	public INodeWriteTrx insertElementAsLeftSibling(final @Nonnull QName pQName)
			throws SirixException {
		if (!XMLToken.isValidQName(checkNotNull(pQName))) {
			throw new IllegalArgumentException("The QName is not valid!");
		}
		if (getNode() instanceof IStructNode) {
			checkAccessAndCommit();

			final long key = getNode().getNodeKey();
			moveToParent();
			final long pathNodeKey = mIndexes.contains(EIndexes.PATH) ? getPathNodeKey(
					pQName, EKind.ELEMENT) : 0;
			moveTo(key);

			final long parentKey = getNode().getParentKey();
			final long leftSibKey = ((IStructNode) getNode()).getLeftSiblingKey();
			final long rightSibKey = getNode().getNodeKey();
			final ElementNode node = mNodeFactory.createElementNode(parentKey,
					leftSibKey, rightSibKey, 0, pQName, pathNodeKey);

			mNodeRtx.setCurrentNode(node);
			adaptForInsert(node, EInsertPos.ASLEFTSIBLING, EPage.NODEPAGE);
			mNodeRtx.setCurrentNode(node);
			adaptHashesWithAdd();

			return this;
		} else {
			throw new SirixUsageException(
					"Insert is not allowed if current node is not an StructuralNode (either Text or Element)!");
		}
	}

	@Override
	public INodeWriteTrx insertElementAsRightSibling(final @Nonnull QName pQName)
			throws SirixException {
		if (!XMLToken.isValidQName(checkNotNull(pQName))) {
			throw new IllegalArgumentException("The QName is not valid!");
		}
		if (getNode() instanceof IStructNode) {
			checkAccessAndCommit();

			final long key = getNode().getNodeKey();
			moveToParent();
			final long pathNodeKey = mIndexes.contains(EIndexes.PATH) ? getPathNodeKey(
					pQName, EKind.ELEMENT) : 0;
			moveTo(key);

			final long parentKey = getNode().getParentKey();
			final long leftSibKey = getNode().getNodeKey();
			final long rightSibKey = ((IStructNode) getNode()).getRightSiblingKey();
			final ElementNode node = mNodeFactory.createElementNode(parentKey,
					leftSibKey, rightSibKey, 0, pQName, pathNodeKey);

			mNodeRtx.setCurrentNode(node);
			adaptForInsert(node, EInsertPos.ASRIGHTSIBLING, EPage.NODEPAGE);
			mNodeRtx.setCurrentNode(node);
			adaptHashesWithAdd();

			return this;
		} else {
			throw new SirixUsageException(
					"Insert is not allowed if current node is not an StructuralNode (either Text or Element)!");
		}
	}

	@Override
	public INodeWriteTrx insertSubtree(final @Nonnull XMLEventReader pReader,
			final @Nonnull EInsert pInsert) throws SirixException {
		mBulkInsert = true;
		long nodeKey = getNode().getNodeKey();
		final XMLShredder shredder = new XMLShredder(this, pReader, pInsert,
				EShredderCommit.NOCOMMIT);
		shredder.call();
		moveTo(nodeKey);
		switch (pInsert) {
		case ASFIRSTCHILD:
			moveToFirstChild();
			break;
		case ASRIGHTSIBLING:
			moveToRightSibling();
			break;
		case ASLEFTSIBLING:
			moveToLeftSibling();
			break;
		}
		nodeKey = getNode().getNodeKey();
		postOrderTraversalHashes();
		final INode startNode = getNode();
		moveToParent();
		while (getNode().hasParent()) {
			moveToParent();
			addParentHash(startNode);
		}
		moveTo(nodeKey);
		mBulkInsert = false;
		return this;
	}

	@Override
	public INodeWriteTrx insertTextAsFirstChild(final @Nonnull String pValue)
			throws SirixException {
		checkNotNull(pValue);
		if (getNode() instanceof IStructNode
				&& getNode().getKind() != EKind.DOCUMENT_ROOT && !pValue.isEmpty()) {
			checkAccessAndCommit();

			final long parentKey = getNode().getNodeKey();
			final long leftSibKey = EFixed.NULL_NODE_KEY.getStandardProperty();
			final long rightSibKey = ((IStructNode) getNode()).getFirstChildKey();

			// Update value in case of adjacent text nodes.
			if (moveTo(rightSibKey)) {
				if (getNode().getKind() == EKind.TEXT) {
					setValue(new StringBuilder(pValue).append(getValueOfCurrentNode())
							.toString());
					adaptHashedWithUpdate(getNode().getHash());
					return this;
				}
				moveTo(parentKey);
			}

			// Insert new text node if no adjacent text nodes are found.
			final byte[] value = getBytes(pValue);
			final TextNode node = mNodeFactory.createTextNode(parentKey, leftSibKey,
					rightSibKey, value, mNodeRtx.mSession.mResourceConfig.mCompression);

			// Adapt local nodes and hashes.
			mNodeRtx.setCurrentNode(node);
			adaptForInsert(node, EInsertPos.ASFIRSTCHILD, EPage.NODEPAGE);
			mNodeRtx.setCurrentNode(node);
			adaptHashesWithAdd();

			// Index text value.
			indexText(value);

			return this;
		} else {
			throw new SirixUsageException(
					"Insert is not allowed if current node is not an ElementNode or TextNode!");
		}
	}

	/**
	 * Index text.
	 * 
	 * @param pValue
	 *          text value
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	private void indexText(final @Nonnull byte[] pValue) throws SirixIOException {
		if (mIndexes.contains(EIndexes.VALUE)) {
			final IStructNode parent = mNodeRtx.getParent().get();
			final long pathNodeKey = parent instanceof INameNode ? ((INameNode) parent)
					.getPathNodeKey() : 0;
			final IPageWriteTrx pageTrx = getPageTransaction();
			final TextValue textVal = (TextValue) pageTrx.createNode(new TextValue(
					pValue, pageTrx.getActualRevisionRootPage().getMaxValueNodeKey() + 1,
					pathNodeKey), EPage.VALUEPAGE);
			final Optional<TextReferences> textReferences = mAVLTree.get(textVal);
			if (textReferences.isPresent()) {
				final TextReferences references = textReferences.get();
				references.setNodeKey(mNodeRtx.getNode().getNodeKey());
				mAVLTree.index(textVal, references);
			} else {
				final TextReferences textRef = (TextReferences) pageTrx.createNode(
						new TextReferences(new HashSet<Long>(), pageTrx
								.getActualRevisionRootPage().getMaxValueNodeKey() + 1),
						EPage.VALUEPAGE);
				mAVLTree.index(textVal, textRef);
			}
		}
	}

	@Override
	public INodeWriteTrx insertTextAsLeftSibling(final @Nonnull String pValue)
			throws SirixException {
		checkNotNull(pValue);
		if (getNode() instanceof IStructNode
				&& getNode().getKind() != EKind.DOCUMENT_ROOT && !pValue.isEmpty()) {
			checkAccessAndCommit();

			final long parentKey = getNode().getParentKey();
			final long leftSibKey = ((IStructNode) getNode()).getLeftSiblingKey();
			final long rightSibKey = getNode().getNodeKey();

			// Update value in case of adjacent text nodes.
			final StringBuilder builder = new StringBuilder();
			if (getNode().getKind() == EKind.TEXT) {
				builder.append(pValue);
			}
			builder.append(getValueOfCurrentNode());

			if (!pValue.equals(builder.toString())) {
				setValue(builder.toString());
				return this;
			}
			if (moveTo(leftSibKey)) {
				final StringBuilder value = new StringBuilder();
				if (getNode().getKind() == EKind.TEXT) {
					value.append(getValueOfCurrentNode()).append(builder);
				}
				if (!pValue.equals(value.toString())) {
					setValue(value.toString());
					return this;
				}
			}

			// Insert new text node if no adjacent text nodes are found.
			moveTo(rightSibKey);
			final byte[] value = getBytes(builder.toString());
			final TextNode node = mNodeFactory.createTextNode(parentKey, leftSibKey,
					rightSibKey, value, mNodeRtx.mSession.mResourceConfig.mCompression);

			// Adapt local nodes and hashes.
			mNodeRtx.setCurrentNode(node);
			adaptForInsert(node, EInsertPos.ASLEFTSIBLING, EPage.NODEPAGE);
			mNodeRtx.setCurrentNode(node);
			adaptHashesWithAdd();

			// Index text value.
			// indexText(value);

			return this;
		} else {
			throw new SirixUsageException(
					"Insert is not allowed if current node is not an Element- or Text-node!");
		}
	}

	@Override
	public INodeWriteTrx insertTextAsRightSibling(final @Nonnull String pValue)
			throws SirixException {
		checkNotNull(pValue);
		if (getNode() instanceof IStructNode
				&& getNode().getKind() != EKind.DOCUMENT_ROOT && !pValue.isEmpty()) {
			checkAccessAndCommit();

			// If an empty value is specified the node needs to be removed (see XDM).
			if (pValue.isEmpty()) {
				remove();
				return this;
			}

			final long parentKey = getNode().getParentKey();
			final long leftSibKey = getNode().getNodeKey();
			final long rightSibKey = ((IStructNode) getNode()).getRightSiblingKey();

			// Update value in case of adjacent text nodes.
			final StringBuilder builder = new StringBuilder();
			if (getNode().getKind() == EKind.TEXT) {
				builder.append(getValueOfCurrentNode());
			}
			builder.append(pValue);
			if (!pValue.equals(builder.toString())) {
				setValue(builder.toString());
				return this;
			}
			if (moveTo(rightSibKey)) {
				if (getNode().getKind() == EKind.TEXT) {
					builder.append(getValueOfCurrentNode());
				}
				if (!pValue.equals(builder.toString())) {
					setValue(builder.toString());
					return this;
				}
			}

			// Insert new text node if no adjacent text nodes are found.
			moveTo(leftSibKey);
			final byte[] value = getBytes(builder.toString());
			final TextNode node = mNodeFactory.createTextNode(parentKey, leftSibKey,
					rightSibKey, value, mNodeRtx.mSession.mResourceConfig.mCompression);

			// Adapt local nodes and hashes.
			mNodeRtx.setCurrentNode(node);
			adaptForInsert(node, EInsertPos.ASRIGHTSIBLING, EPage.NODEPAGE);
			mNodeRtx.setCurrentNode(node);
			adaptHashesWithAdd();

			// Index text value.
			indexText(value);

			return this;
		} else {
			throw new SirixUsageException(
					"Insert is not allowed if current node is not an Element- or Text-node!");
		}
	}

	/**
	 * Get a byte-array from a value.
	 * 
	 * @param pValue
	 *          the value
	 * @return byte-array representation of {@code pValue}
	 */
	private byte[] getBytes(final String pValue) {
		return pValue.getBytes(IConstants.DEFAULT_ENCODING);
	}

	@Override
	public INodeWriteTrx insertAttribute(final @Nonnull QName pQName,
			final @Nonnull String pValue) throws SirixException {
		return insertAttribute(pQName, pValue, EMove.NONE);
	}

	@Override
	public INodeWriteTrx insertAttribute(final @Nonnull QName pQName,
			final @Nonnull String pValue, final @Nonnull EMove pMove)
			throws SirixException {
		checkNotNull(pValue);
		if (!XMLToken.isValidQName(checkNotNull(pQName))) {
			throw new IllegalArgumentException("The QName is not valid!");
		}
		if (getNode().getKind() == EKind.ELEMENT) {
			checkAccessAndCommit();

			/*
			 * Update value in case of the same attribute name is found but the
			 * attribute to insert has a different value (otherwise an exception is
			 * thrown because of a duplicate attribute which would otherwise be
			 * inserted!).
			 */
			final Optional<Long> attKey = ((ElementNode) getNode())
					.getAttributeKeyByName(pQName);
			if (attKey.isPresent()) {
				moveTo(attKey.get());
				final QName qName = getQNameOfCurrentNode();
				if (pQName.equals(qName)
						&& pQName.getPrefix().equals(qName.getPrefix())) {
					if (!getValueOfCurrentNode().equals(pValue)) {
						setValue(pValue);
					} else {
						throw new SirixUsageException("Duplicate attribute!");
					}
				}
				moveToParent();
			}

			final long pathNodeKey = mIndexes.contains(EIndexes.PATH) ? getPathNodeKey(
					pQName, EKind.ATTRIBUTE) : 0;
			final byte[] value = getBytes(pValue);
			final long elementKey = getNode().getNodeKey();
			final AttributeNode node = mNodeFactory.createAttributeNode(elementKey,
					pQName, value, pathNodeKey);

			final INode parentNode = (INode) getPageTransaction()
					.prepareNodeForModification(node.getParentKey(), EPage.NODEPAGE);
			((ElementNode) parentNode).insertAttribute(node.getNodeKey(),
					node.getNameKey());
			getPageTransaction().finishNodeModification(parentNode, EPage.NODEPAGE);

			mNodeRtx.setCurrentNode(node);
			adaptHashesWithAdd();
			if (pMove == EMove.TOPARENT) {
				moveToParent();
			}
			return this;
		} else {
			throw new SirixUsageException(
					"Insert is not allowed if current node is not an ElementNode!");
		}
	}

	@Override
	public INodeWriteTrx insertNamespace(final @Nonnull QName pQName)
			throws SirixException {
		return insertNamespace(pQName, EMove.NONE);
	}

	@Override
	public INodeWriteTrx insertNamespace(final @Nonnull QName pQName,
			final @Nonnull EMove pMove) throws SirixException {
		if (!XMLToken.isValidQName(checkNotNull(pQName))) {
			throw new IllegalArgumentException("The QName is not valid!");
		}
		if (getNode().getKind() == EKind.ELEMENT) {
			checkAccessAndCommit();

			for (int i = 0, namespCount = ((ElementNode) getNode())
					.getNamespaceCount(); i < namespCount; i++) {
				moveToNamespace(i);
				final QName qName = getQNameOfCurrentNode();
				if (pQName.getPrefix().equals(qName.getPrefix())) {
					throw new SirixUsageException("Duplicate namespace!");
				}
				moveToParent();
			}

			final long pathNodeKey = mIndexes.contains(EIndexes.PATH) ? getPathNodeKey(
					pQName, EKind.NAMESPACE) : 0;
			final int uriKey = getPageTransaction().createNameKey(
					pQName.getNamespaceURI(), EKind.NAMESPACE);
			final int prefixKey = getPageTransaction().createNameKey(
					pQName.getPrefix(), EKind.NAMESPACE);
			final long elementKey = getNode().getNodeKey();

			final NamespaceNode node = mNodeFactory.createNamespaceNode(elementKey,
					uriKey, prefixKey, pathNodeKey);

			final INode parentNode = (INode) getPageTransaction()
					.prepareNodeForModification(node.getParentKey(), EPage.NODEPAGE);
			((ElementNode) parentNode).insertNamespace(node.getNodeKey());
			getPageTransaction().finishNodeModification(parentNode, EPage.NODEPAGE);

			mNodeRtx.setCurrentNode(node);
			adaptHashesWithAdd();
			if (pMove == EMove.TOPARENT) {
				moveToParent();
			}
			return this;
		} else {
			throw new SirixUsageException(
					"Insert is not allowed if current node is not an ElementNode!");
		}
	}

	/**
	 * Check ancestors of current node.
	 * 
	 * @throws IllegalStateException
	 *           if one of the ancestors is the node/subtree rooted at the node to
	 *           move
	 */
	private void checkAncestors(final INode pItem) {
		assert pItem != null;
		final INode item = getNode();
		while (getNode().hasParent()) {
			moveToParent();
			if (getNode().getNodeKey() == pItem.getNodeKey()) {
				throw new IllegalStateException(
						"Moving one of the ancestor nodes is not permitted!");
			}
		}
		moveTo(item.getNodeKey());
	}

	@Override
	public void remove() throws SirixException {
		checkAccessAndCommit();
		if (getNode().getKind() == EKind.DOCUMENT_ROOT) {
			throw new SirixUsageException("Document root can not be removed.");
		} else if (getNode() instanceof IStructNode) {
			final IStructNode node = (IStructNode) mNodeRtx.getNode();

			// Remove subtree.
			for (final IAxis axis = new PostOrderAxis(this); axis.hasNext();) {
				axis.next();
				final IStructNode nodeToDelete = axis.getTransaction()
						.getStructuralNode();
				if (nodeToDelete.getKind() == EKind.ELEMENT) {
					final ElementNode element = (ElementNode) nodeToDelete;
					removeName();
					removeNonStructural(element);
				}
				getPageTransaction().removeNode(nodeToDelete, EPage.NODEPAGE);
			}

			// Adapt hashes and neighbour nodes as well as the name from the NamePage
			// mapping if it's not a text
			// node.
			mNodeRtx.setCurrentNode(node);
			adaptHashesWithRemove();
			adaptForRemove(node, EPage.NODEPAGE);
			mNodeRtx.setCurrentNode(node);

			// Remove the name of subtree-root.
			if (node.getKind() == EKind.ELEMENT) {
				removeName();
			}

			// Set current node (don't remove the moveTo(long) inside the if-clause
			// which is needed because
			// of text merges.
			if (node.hasRightSibling() && moveTo(node.getRightSiblingKey())) {
			} else if (node.hasLeftSibling()) {
				moveTo(node.getLeftSiblingKey());
			} else {
				moveTo(node.getParentKey());
			}
		} else if (getNode().getKind() == EKind.ATTRIBUTE) {
			final INode node = mNodeRtx.getNode();

			final ElementNode parent = (ElementNode) getPageTransaction()
					.prepareNodeForModification(node.getParentKey(), EPage.NODEPAGE);
			parent.removeAttribute(node.getNodeKey());
			getPageTransaction().finishNodeModification(parent, EPage.NODEPAGE);
			adaptHashesWithRemove();
			getPageTransaction().removeNode(node, EPage.NODEPAGE);
			removeName();
			moveToParent();
		} else if (getNode().getKind() == EKind.NAMESPACE) {
			final INode node = mNodeRtx.getNode();

			final ElementNode parent = (ElementNode) getPageTransaction()
					.prepareNodeForModification(node.getParentKey(), EPage.NODEPAGE);
			parent.removeNamespace(node.getNodeKey());
			getPageTransaction().finishNodeModification(parent, EPage.NODEPAGE);
			adaptHashesWithRemove();
			getPageTransaction().removeNode(node, EPage.NODEPAGE);
			removeName();
			moveToParent();
		}
	}

	/**
	 * Remove non structural nodes of an {@link ElementNode}, that is namespaces
	 * and attributes.
	 * 
	 * @param pElement
	 *          the element
	 * @throws SirixException
	 *           if anything goes wrong
	 */
	private void removeNonStructural(final @Nonnull ElementNode pElement)
			throws SirixException {
		moveTo(pElement.getNodeKey());
		final int attCount = pElement.getAttributeCount();
		for (int i = 0; i < attCount; i++) {
			moveToAttribute(i);
			removeName();
			getPageTransaction().removeNode(getNode(), EPage.NODEPAGE);
			moveToParent();
		}
		final int nspCount = pElement.getNamespaceCount();
		for (int i = 0; i < nspCount; i++) {
			moveToNamespace(i);
			removeName();
			getPageTransaction().removeNode(getNode(), EPage.NODEPAGE);
			moveToParent();
		}
	}

	/**
	 * Remove a name from the {@link NamePage} reference and the path summary if
	 * needed.
	 * 
	 * @throws SirixException
	 *           if Sirix fails
	 */
	private void removeName() throws SirixException {
		assert getNode() instanceof INameNode;
		final INameNode node = ((INameNode) getNode());
		final EKind nodeKind = node.getKind();
		final NamePage page = ((NamePage) getPageTransaction()
				.getActualRevisionRootPage().getNamePageReference().getPage());
		page.removeName(node.getNameKey(), nodeKind);
		page.removeName(node.getURIKey(), EKind.NAMESPACE);

		assert nodeKind != EKind.DOCUMENT_ROOT;
		if (mIndexes.contains(EIndexes.PATH)
				&& mPathSummary.moveTo(node.getPathNodeKey())) {
			PathNode pathNode = (PathNode) mPathSummary.getNode();
			if (pathNode.getReferences() == 1) {
				removePathSummaryNode(ERemove.YES);
			} else {
				assert page.getCount(node.getNameKey(), nodeKind) != 0;
				if (pathNode.getReferences() > 1) {
					pathNode = (PathNode) getPageTransaction()
							.prepareNodeForModification(pathNode.getNodeKey(),
									EPage.PATHSUMMARYPAGE);
					pathNode.decrementReferenceCount();
					getPageTransaction().finishNodeModification(pathNode,
							EPage.PATHSUMMARYPAGE);
				}
			}
		}
	}

	/**
	 * Remove a path summary node with the specified PCR.
	 * 
	 * @throws SirixException
	 *           if Sirix fails to remove the path node
	 */
	private void removePathSummaryNode(final @Nonnull ERemove pRemove)
			throws SirixException {
		// Remove all descendant nodes.
		if (pRemove == ERemove.YES) {
			for (final IAxis axis = new DescendantAxis(mPathSummary); axis.hasNext();) {
				axis.next();
				getPageTransaction().removeNode(mPathSummary.getNode(),
						EPage.PATHSUMMARYPAGE);
			}
		}

		final IStructNode node = mPathSummary.getStructuralNode();

		// Adapt left sibling node if there is one.
		if (node.hasLeftSibling()) {
			final IStructNode leftSibling = (IStructNode) getPageTransaction()
					.prepareNodeForModification(node.getLeftSiblingKey(),
							EPage.PATHSUMMARYPAGE);
			leftSibling.setRightSiblingKey(node.getRightSiblingKey());
			getPageTransaction().finishNodeModification(leftSibling,
					EPage.PATHSUMMARYPAGE);
		}

		// Adapt right sibling node if there is one.
		if (node.hasRightSibling()) {
			final IStructNode rightSibling = (IStructNode) getPageTransaction()
					.prepareNodeForModification(node.getRightSiblingKey(),
							EPage.PATHSUMMARYPAGE);
			rightSibling.setLeftSiblingKey(node.getLeftSiblingKey());
			getPageTransaction().finishNodeModification(rightSibling,
					EPage.PATHSUMMARYPAGE);
		}

		// Adapt parent. If node has no left sibling it is a first child.
		IStructNode parent = (IStructNode) getPageTransaction()
				.prepareNodeForModification(node.getParentKey(), EPage.PATHSUMMARYPAGE);
		if (!node.hasLeftSibling()) {
			parent.setFirstChildKey(node.getRightSiblingKey());
		}
		parent.decrementChildCount();
		getPageTransaction().finishNodeModification(parent, EPage.PATHSUMMARYPAGE);

		// Remove node.
		assert node.getKind() != EKind.DOCUMENT_ROOT;
		getPageTransaction().removeNode(node, EPage.PATHSUMMARYPAGE);
	}

	@Override
	public void setQName(final @Nonnull QName pQName) throws SirixException {
		checkNotNull(pQName);
		if (getNode() instanceof INameNode) {
			if (!getQNameOfCurrentNode().equals(pQName)) {
				checkAccessAndCommit();

				INameNode node = (INameNode) mNodeRtx.getNode();
				final long oldHash = node.hashCode();

				// Create new keys for mapping.
				final int nameKey = getPageTransaction().createNameKey(
						Utils.buildName(pQName), node.getKind());
				final int uriKey = getPageTransaction().createNameKey(
						pQName.getNamespaceURI(), EKind.NAMESPACE);

				// Adapt path summary.
				if (mIndexes.contains(EIndexes.PATH)) {
					adaptPathForChangedNode(node, pQName, nameKey, uriKey,
							EOPType.SETNAME);
				}

				// Remove old keys from mapping.
				final EKind nodeKind = node.getKind();
				final int oldNameKey = node.getNameKey();
				final int oldUriKey = node.getURIKey();
				final NamePage page = ((NamePage) getPageTransaction()
						.getActualRevisionRootPage().getNamePageReference().getPage());
				page.removeName(oldNameKey, nodeKind);
				page.removeName(oldUriKey, EKind.NAMESPACE);

				// Set new keys for current node.
				node = (INameNode) getPageTransaction().prepareNodeForModification(
						node.getNodeKey(), EPage.NODEPAGE);
				node.setNameKey(nameKey);
				node.setURIKey(uriKey);
				node.setPathNodeKey(mIndexes.contains(EIndexes.PATH) ? mPathSummary
						.getNode().getNodeKey() : 0);
				getPageTransaction().finishNodeModification(node, EPage.NODEPAGE);

				mNodeRtx.setCurrentNode(node);
				adaptHashedWithUpdate(oldHash);
			}
		} else {
			throw new SirixUsageException(
					"setQName is not allowed if current node is not an INameNode implementation!");
		}
	}

	/**
	 * Adapt path summary either for moves or {@code setQName(QName)}.
	 * 
	 * @param pNode
	 *          the node for which the path node needs to be adapted
	 * @param pQName
	 *          the new {@link QName} in case of a new one is set, the old
	 *          {@link QName} otherwise
	 * @param nameKey
	 *          nameKey of the new node
	 * @param uriKey
	 *          uriKey of the new node
	 * @throws SirixException
	 *           if a Sirix operation fails
	 * @throws NullPointerException
	 *           if {@code pNode} or {@code pQName} is null
	 */
	private void adaptPathForChangedNode(final @Nonnull INameNode pNode,
			final @Nonnull QName pQName, final int nameKey, final int uriKey,
			final @Nonnull EOPType pType) throws SirixException {
		// Possibly either reset a path node or decrement its reference counter
		// and search for the new path node or insert it.
		movePathSummary();
		final PathNode oldPathNode = (PathNode) mPathSummary.getNode();

		// Only one path node is referenced (after a setQName(QName) the
		// reference-counter would be 0).
		if (pType == EOPType.SETNAME
				&& ((PathNode) mPathSummary.getNode()).getReferences() == 1) {
			moveSummaryGetLevel(pNode);
			// Search for new path entry.
			final IAxis axis = new FilterAxis(new ChildAxis(mPathSummary),
					new NameFilter(mPathSummary, Utils.buildName(pQName)),
					new PathKindFilter(mPathSummary, pNode.getKind()));
			if (axis.hasNext()) {
				axis.next();

				// Found node.
				processFoundPathNode(oldPathNode.getNodeKey(), mPathSummary.getNode()
						.getNodeKey(), pNode.getNodeKey(), nameKey, uriKey, ERemove.YES,
						pType);
			} else {
				if (mPathSummary.getNode().getKind() != EKind.DOCUMENT_ROOT) {
					mPathSummary.moveTo(oldPathNode.getNodeKey());
					final PathNode pathNode = (PathNode) getPageTransaction()
							.prepareNodeForModification(mPathSummary.getNode().getNodeKey(),
									EPage.PATHSUMMARYPAGE);
					pathNode.setNameKey(nameKey);
					pathNode.setURIKey(uriKey);
					getPageTransaction().finishNodeModification(pathNode,
							EPage.PATHSUMMARYPAGE);
				}
			}
		} else {
			int level = moveSummaryGetLevel(pNode);
			// Search for new path entry.
			final IAxis axis = new FilterAxis(new ChildAxis(mPathSummary),
					new NameFilter(mPathSummary, Utils.buildName(pQName)),
					new PathKindFilter(mPathSummary, pNode.getKind()));
			if (pType == EOPType.MOVEDSAMELEVEL || axis.hasNext()) {
				if (pType != EOPType.MOVEDSAMELEVEL) {
					axis.next();
				}

				// Found node.
				processFoundPathNode(oldPathNode.getNodeKey(), mPathSummary.getNode()
						.getNodeKey(), pNode.getNodeKey(), nameKey, uriKey, ERemove.NO,
						pType);
			} else {
				long nodeKey = mPathSummary.getNode().getNodeKey();
				// Decrement reference count or remove path summary node.
				mNodeRtx.moveTo(pNode.getNodeKey());
				final Set<Long> nodesToDelete = new HashSet<>();
				// boolean first = true;
				for (final IAxis descendants = new DescendantAxis(mNodeRtx,
						EIncludeSelf.YES); descendants.hasNext();) {
					descendants.next();
					deleteOrDecrement(nodesToDelete);
					if (mNodeRtx.getNode().getKind() == EKind.ELEMENT) {
						final ElementNode element = (ElementNode) mNodeRtx.getNode();

						// Namespaces.
						for (int i = 0, nsps = element.getNamespaceCount(); i < nsps; i++) {
							mNodeRtx.moveToNamespace(i);
							deleteOrDecrement(nodesToDelete);
							mNodeRtx.moveToParent();
						}

						// Attributes.
						for (int i = 0, atts = element.getAttributeCount(); i < atts; i++) {
							mNodeRtx.moveToAttribute(i);
							deleteOrDecrement(nodesToDelete);
							mNodeRtx.moveToParent();
						}
					}
				}

				mPathSummary.moveTo(nodeKey);

				// Not found => create new path nodes for the whole subtree.
				boolean firstRun = true;
				for (final IAxis descendants = new DescendantAxis(this,
						EIncludeSelf.YES); descendants.hasNext();) {
					descendants.next();
					if (descendants.getTransaction().getNode().getKind() == EKind.ELEMENT) {
						final ElementNode element = (ElementNode) descendants
								.getTransaction().getNode();

						// Path Summary : New mapping.
						if (firstRun) {
							insertPathAsFirstChild(pQName, EKind.ELEMENT, ++level);
							nodeKey = mPathSummary.getNode().getNodeKey();
						} else {
							insertPathAsFirstChild(descendants.getTransaction()
									.getQNameOfCurrentNode(), EKind.ELEMENT, ++level);
						}
						resetPathNodeKey(getNode().getNodeKey());

						// Namespaces.
						for (int i = 0, nsps = element.getNamespaceCount(); i < nsps; i++) {
							moveToNamespace(i);
							// Path Summary : New mapping.
							insertPathAsFirstChild(descendants.getTransaction()
									.getQNameOfCurrentNode(), EKind.NAMESPACE, level + 1);
							resetPathNodeKey(getNode().getNodeKey());
							moveToParent();
							mPathSummary.moveToParent();
						}

						// Attributes.
						for (int i = 0, atts = element.getAttributeCount(); i < atts; i++) {
							moveToAttribute(i);
							// Path Summary : New mapping.
							insertPathAsFirstChild(descendants.getTransaction()
									.getQNameOfCurrentNode(), EKind.ATTRIBUTE, level + 1);
							resetPathNodeKey(getNode().getNodeKey());
							moveToParent();
							mPathSummary.moveToParent();
						}

						if (firstRun) {
							firstRun = false;
						} else {
							mPathSummary.moveToParent();
							level--;
						}
					}
				}

				for (final long key : nodesToDelete) {
					mPathSummary.moveTo(key);
					removePathSummaryNode(ERemove.NO);
				}

				mPathSummary.moveTo(nodeKey);
			}
		}

	}

	/**
	 * Schedule for deletion of decrement path reference counter.
	 * 
	 * @param pNodesToDelete
	 *          stores nodeKeys which should be deleted
	 * @throws SirixIOException
	 *           if an I/O error occurs while decrementing the reference counter
	 */
	private void deleteOrDecrement(final @Nonnull Set<Long> pNodesToDelete)
			throws SirixIOException {
		if (mNodeRtx.getNode() instanceof INameNode) {
			movePathSummary();
			if (mPathSummary.getPathNode().getReferences() == 1) {
				pNodesToDelete.add(mPathSummary.getNode().getNodeKey());
			} else {
				final PathNode pathNode = (PathNode) getPageTransaction()
						.prepareNodeForModification(mPathSummary.getNode().getNodeKey(),
								EPage.PATHSUMMARYPAGE);
				pathNode.decrementReferenceCount();
				getPageTransaction().finishNodeModification(pathNode,
						EPage.PATHSUMMARYPAGE);
			}
		}
	}

	/**
	 * Process a found path node.
	 * 
	 * @param pOldPathNodeKey
	 *          key of old path node
	 * 
	 * @param pOldNodeKey
	 *          key of old node
	 * 
	 * @throws SirixException
	 *           if Sirix fails to do so
	 */
	private void processFoundPathNode(final @Nonnegative long pOldPathNodeKey,
			final @Nonnegative long pNewPathNodeKey,
			final @Nonnegative long pOldNodeKey, final int pNameKey,
			final int pUriKey, final @Nonnull ERemove pRemove,
			final @Nonnull EOPType pType) throws SirixException {
		final PathSummary cloned = PathSummary.getInstance(getPageTransaction(),
				mNodeRtx.getSession());
		boolean moved = cloned.moveTo(pOldPathNodeKey);
		assert moved;

		// Set new reference count.
		if (pType != EOPType.MOVEDSAMELEVEL) {
			final PathNode currNode = (PathNode) getPageTransaction()
					.prepareNodeForModification(mPathSummary.getPathNode().getNodeKey(),
							EPage.PATHSUMMARYPAGE);
			currNode.setReferenceCount(currNode.getReferences()
					+ cloned.getPathNode().getReferences());
			currNode.setNameKey(pNameKey);
			currNode.setURIKey(pUriKey);
			getPageTransaction().finishNodeModification(currNode,
					EPage.PATHSUMMARYPAGE);
		}

		// For all old path nodes.
		mPathSummary.moveToFirstChild();
		final int oldLevel = cloned.getPathNode().getLevel();
		for (final IAxis oldDescendants = new DescendantAxis(cloned); oldDescendants
				.hasNext();) {
			oldDescendants.next();

			// Search for new path entry.
			final PathNode node = cloned.getPathNode();
			final IAxis axis = new FilterAxis(
					new LevelOrderAxis.Builder(mPathSummary)
							.filterLevel(node.getLevel() - oldLevel)
							.includeSelf(EIncludeSelf.YES).build(), new NameFilter(
							mPathSummary, Utils.buildName(cloned.getQNameOfCurrentNode())),
					new PathKindFilter(mPathSummary, node.getPathKind()),
					new PathLevelFilter(mPathSummary, node.getLevel()));
			if (axis.hasNext()) {
				axis.next();

				// Set new reference count.
				if (pType != EOPType.MOVEDSAMELEVEL) {
					final PathNode currNode = (PathNode) getPageTransaction()
							.prepareNodeForModification(
									mPathSummary.getPathNode().getNodeKey(),
									EPage.PATHSUMMARYPAGE);
					currNode.setReferenceCount(currNode.getReferences()
							+ cloned.getPathNode().getReferences());
					getPageTransaction().finishNodeModification(currNode,
							EPage.PATHSUMMARYPAGE);
				}
			} else {
				// Insert new node.
				PathNode currNode = cloned.getPathNode();
				insertPathAsFirstChild(cloned.getQNameOfCurrentNode(),
						currNode.getPathKind(), mPathSummary.getPathNode().getLevel() + 1);

				// Set new reference count.
				currNode = (PathNode) getPageTransaction().prepareNodeForModification(
						mPathSummary.getPathNode().getNodeKey(), EPage.PATHSUMMARYPAGE);
				currNode.setReferenceCount(cloned.getPathNode().getReferences());
				getPageTransaction().finishNodeModification(currNode,
						EPage.PATHSUMMARYPAGE);
			}
			mPathSummary.moveTo(pNewPathNodeKey);
		}

		// Set new path nodes.
		// ==========================================================
		mPathSummary.moveTo(pNewPathNodeKey);
		mNodeRtx.moveTo(pOldNodeKey);

		boolean first = true;
		for (final IAxis axis = new DescendantAxis(mNodeRtx, EIncludeSelf.YES); axis
				.hasNext();) {
			axis.next();

			if (first && pType == EOPType.SETNAME) {
				first = false;
			} else if (mNodeRtx.getNode() instanceof INameNode) {
				cloned.moveTo(((INameNode) mNodeRtx.getNode()).getPathNodeKey());
				resetPath(pNewPathNodeKey, cloned.getPathNode());

				if (mNodeRtx.getNode().getKind() == EKind.ELEMENT) {
					final ElementNode element = (ElementNode) mNodeRtx.getNode();

					for (int i = 0, nspCount = element.getNamespaceCount(); i < nspCount; i++) {
						mNodeRtx.moveToNamespace(i);
						cloned.moveTo(((INameNode) mNodeRtx.getNode()).getPathNodeKey());
						resetPath(pNewPathNodeKey, cloned.getPathNode());
						mNodeRtx.moveToParent();
					}
					for (int i = 0, attCount = element.getAttributeCount(); i < attCount; i++) {
						mNodeRtx.moveToAttribute(i);
						cloned.moveTo(((INameNode) mNodeRtx.getNode()).getPathNodeKey());
						resetPath(pNewPathNodeKey, cloned.getPathNode());
						mNodeRtx.moveToParent();
					}
				}
			}
		}

		// Remove old nodes.
		if (pRemove == ERemove.YES) {
			mPathSummary.moveTo(pOldPathNodeKey);
			removePathSummaryNode(pRemove);
		}
	}

	/**
	 * Reset the path node key of a node.
	 * 
	 * @param pNewPathNodeKey
	 *          path node key of new path node
	 * @param pOldPathNode
	 *          old path node
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	private void resetPath(final @Nonnegative long pNewPathNodeKey,
			final @Nonnull PathNode pOldPathNode) throws SirixIOException {
		// Search for new path entry.
		mPathSummary.moveTo(pNewPathNodeKey);
		final IAxis filterAxis = new FilterAxis(new LevelOrderAxis.Builder(
				mPathSummary).includeSelf(EIncludeSelf.YES).build(), new NameFilter(
				mPathSummary, Utils.buildName(mNodeRtx.getQNameOfCurrentNode())),
				new PathKindFilter(mPathSummary, mNodeRtx.getNode().getKind()),
				new PathLevelFilter(mPathSummary, pOldPathNode.getLevel()));
		if (filterAxis.hasNext()) {
			filterAxis.next();

			// Set new path node.
			final INameNode node = (INameNode) getPageTransaction()
					.prepareNodeForModification(mNodeRtx.getNode().getNodeKey(),
							EPage.NODEPAGE);
			node.setPathNodeKey(mPathSummary.getPathNode().getNodeKey());
			getPageTransaction().finishNodeModification(node, EPage.NODEPAGE);
		} else {
			throw new IllegalStateException();
		}
	}

	/**
	 * Move path summary to the associated {@code parent} {@link PathNode} and get
	 * the level of the node.
	 * 
	 * @param pNode
	 *          the node to lookup for it's {@link PathNode}
	 * @return level of the path node
	 */
	private int moveSummaryGetLevel(final @Nonnull INode pNode) {
		assert pNode != null;
		// Get parent path node and level.
		moveToParent();
		int level = 0;
		if (mNodeRtx.getNode().getKind() == EKind.DOCUMENT_ROOT) {
			mPathSummary.moveToDocumentRoot();
		} else {
			movePathSummary();
			level = mPathSummary.getPathNode().getLevel();
		}
		moveTo(pNode.getNodeKey());
		return level;
	}

	/**
	 * Reset a path node key.
	 * 
	 * @param pNodeKey
	 *          the nodeKey of the node to adapt
	 * @throws SirixException
	 *           if anything fails
	 */
	private void resetPathNodeKey(final @Nonnegative long pNodeKey)
			throws SirixException {
		final INameNode currNode = (INameNode) getPageTransaction()
				.prepareNodeForModification(pNodeKey, EPage.NODEPAGE);
		currNode.setPathNodeKey(mPathSummary.getNode().getNodeKey());
		getPageTransaction().finishNodeModification(currNode, EPage.NODEPAGE);
	}

	@Override
	public void setValue(final @Nonnull String pValue) throws SirixException {
		checkNotNull(pValue);
		if (getNode() instanceof IValNode) {
			checkAccessAndCommit();
			final long oldHash = mNodeRtx.getNode().hashCode();
			final byte[] byteVal = getBytes(pValue);

			final IValNode node = (IValNode) getPageTransaction()
					.prepareNodeForModification(mNodeRtx.getNode().getNodeKey(),
							EPage.NODEPAGE);
			node.setValue(byteVal);
			getPageTransaction().finishNodeModification(node, EPage.NODEPAGE);

			mNodeRtx.setCurrentNode(node);
			adaptHashedWithUpdate(oldHash);

			// Index new value.
			indexText(byteVal);
		} else {
			throw new SirixUsageException(
					"setValue(String) is not allowed if current node is not an IValNode implementation!");
		}
	}

	@Override
	public void revertTo(@Nonnegative final int pRevision) throws SirixException {
		mNodeRtx.assertNotClosed();
		mNodeRtx.mSession.assertAccess(pRevision);

		// Close current page transaction.
		final long trxID = getTransactionID();
		final int revNumber = getRevisionNumber();
		getPageTransaction().close();
		mNodeRtx.setPageReadTransaction(null);

		// Reset internal transaction state to new uber page.
		mNodeRtx.setPageReadTransaction(mNodeRtx.mSession
				.createPageWriteTransaction(trxID, pRevision, revNumber - 1));
		mNodeFactory = null;
		mNodeFactory = new NodeFactory(
				(IPageWriteTrx) mNodeRtx.getPageTransaction());

		// New path summary.
		reInstantiateIndexes();

		// Reset modification counter.
		mModificationCount = 0L;

		// Move to document root.
		moveToDocumentRoot();
	}

	@Override
	public void close() throws SirixException {
		if (!isClosed()) {
			// Make sure to commit all dirty data.
			if (mModificationCount > 0) {
				throw new SirixUsageException("Must commit/abort transaction first");
			}
			// Release all state immediately.
			mNodeRtx.mSession.closeWriteTransaction(getTransactionID());
			mNodeRtx.close();
			
			// mPathSummary.close();
			mPathSummary = null;

			// Shutdown pool.
			mPool.shutdown();
			try {
				mPool.awaitTermination(5, TimeUnit.SECONDS);
			} catch (final InterruptedException e) {
				throw new SirixThreadedException(e);
			}
		}
	}

	@Override
	public void abort() throws SirixException {
		mNodeRtx.assertNotClosed();

		// Reset modification counter.
		mModificationCount = 0L;

		getPageTransaction().close();

		// Close current page transaction.
		final long trxID = getTransactionID();
		final int revNumber = getPageTransaction().getUberPage().isBootstrap() ? 0
				: getRevisionNumber() - 1;

		reInstantiate(trxID, revNumber);
	}

	@Override
	public void commit() throws SirixException {
		mNodeRtx.assertNotClosed();

		// Assert that the DocumentNode has no more than one child node (the root
		// node).
		final long nodeKey = mNodeRtx.getNode().getNodeKey();
		moveToDocumentRoot();
		final DocumentRootNode document = (DocumentRootNode) mNodeRtx.getNode();
		if (document.getChildCount() > 1) {
			moveTo(nodeKey);
			throw new IllegalStateException(
					"DocumentRootNode may not have more than one child node!");
		}
		moveTo(nodeKey);

		final File commitFile = mNodeRtx.mSession.mCommitFile;
		try {
			commitFile.createNewFile();
		} catch (final IOException e) {
			throw new SirixIOException(e.getCause());
		}

		// Execute pre-commit hooks.
		for (final IPreCommitHook hook : mPreCommitHooks) {
			hook.preCommit(this);
		}

		// Reset modification counter.
		mModificationCount = 0L;

		final INodeWriteTrx trx = this;
//		mPool.submit(new Callable<Void>() {
//			@Override
//			public Void call() throws SirixException {
				// Commit uber page.
				final UberPage currUberPage = getPageTransaction().getUberPage();
				if (currUberPage.isBootstrap()) {
					currUberPage.setIsBulkInserted(mBulkInsert);
				}
				final UberPage uberPage = getPageTransaction().commit(
						EMultipleWriteTrx.NO);

				// Remember succesfully committed uber page in session.
				mNodeRtx.mSession.setLastCommittedUberPage(uberPage);

				// Close current page transaction.
				getPageTransaction().close();
				
				final long trxID = getTransactionID();
				final int revNumber = getRevisionNumber();

				reInstantiate(trxID, revNumber);

				// Execute post-commit hooks.
				for (final IPostCommitHook hook : mPostCommitHooks) {
					hook.postCommit(trx);
				}

				// Delete commit file which denotes that a commit must write the log in
				// the data file.
				commitFile.delete();
//				return null;
//			}
//		});
	}

	private void reInstantiate(final @Nonnegative long trxID,
			final @Nonnegative int revNumber) throws SirixException {
		// Reset page transaction to new uber page.
		mNodeRtx.setPageReadTransaction(null);
		mNodeRtx.setPageReadTransaction(mNodeRtx.mSession
				.createPageWriteTransaction(trxID, revNumber, revNumber));
		mNodeFactory = null;
		mNodeFactory = new NodeFactory(
				(IPageWriteTrx) mNodeRtx.getPageTransaction());

		mPathSummary = null;
		mAVLTree = null;

		reInstantiateIndexes();
	}

	private void reInstantiateIndexes() {
		// Get a new path summary instance.
		if (mIndexes.contains(EIndexes.PATH)) {
			mPathSummary = PathSummary.getInstance(mNodeRtx.getPageTransaction(),
					mNodeRtx.getSession());
		}

		// Get a new avl tree instance.
		if (mIndexes.contains(EIndexes.VALUE)) {
			mAVLTree = AVLTree.getInstance(getPageTransaction());
		}
	}

	/**
	 * Modifying hashes in a postorder-traversal.
	 * 
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	private void postOrderTraversalHashes() throws SirixIOException {
		for (final IAxis axis = new PostOrderAxis(this, EIncludeSelf.YES); axis
				.hasNext();) {
			axis.next();
			final IStructNode node = getStructuralNode();
			if (node.getKind() == EKind.ELEMENT) {
				final ElementNode element = (ElementNode) node;
				for (int i = 0, nspCount = element.getNamespaceCount(); i < nspCount; i++) {
					moveToNamespace(i);
					addHashAndDescendantCount();
					moveToParent();
				}
				for (int i = 0, attCount = element.getAttributeCount(); i < attCount; i++) {
					moveToAttribute(i);
					addHashAndDescendantCount();
					moveToParent();
				}
			}
			addHashAndDescendantCount();
		}
	}

	/**
	 * Add a hash.
	 * 
	 * @param pStartNode
	 *          start node
	 */
	private void addParentHash(final @Nonnull INode pStartNode)
			throws SirixIOException {
		switch (mHashKind) {
		case Rolling:
			long hashToAdd = mHash.hashLong(pStartNode.hashCode()).asLong();
			INode node = (INode) getPageTransaction().prepareNodeForModification(
					mNodeRtx.getNode().getNodeKey(), EPage.NODEPAGE);
			node.setHash(node.getHash() + hashToAdd * PRIME);
			if (pStartNode instanceof IStructNode) {
				((IStructNode) node).setDescendantCount(((IStructNode) node)
						.getDescendantCount()
						+ ((IStructNode) pStartNode).getDescendantCount() + 1);
			}
			getPageTransaction().finishNodeModification(node, EPage.NODEPAGE);
			break;
		case Postorder:
			break;
		}
	}

	/** Add a hash and the descendant count. */
	private void addHashAndDescendantCount() throws SirixIOException {
		switch (mHashKind) {
		case Rolling:
			// Setup.
			final INode startNode = getNode();
			final long oldDescendantCount = getStructuralNode().getDescendantCount();
			final long descendantCount = oldDescendantCount == 0 ? 1
					: oldDescendantCount + 1;

			// Set start node.
			long hashToAdd = mHash.hashLong(startNode.hashCode()).asLong();
			INode node = (INode) getPageTransaction().prepareNodeForModification(
					mNodeRtx.getNode().getNodeKey(), EPage.NODEPAGE);
			node.setHash(hashToAdd);
			getPageTransaction().finishNodeModification(node, EPage.NODEPAGE);

			// Set parent node.
			if (startNode.hasParent()) {
				moveToParent();
				node = (INode) getPageTransaction().prepareNodeForModification(
						mNodeRtx.getNode().getNodeKey(), EPage.NODEPAGE);
				node.setHash(node.getHash() + hashToAdd * PRIME);
				setAddDescendants(startNode, node, descendantCount);
				getPageTransaction().finishNodeModification(node, EPage.NODEPAGE);
			}

			mNodeRtx.setCurrentNode(startNode);
			break;
		case Postorder:
			postorderAdd();
			break;
		default:
		}
	}

	/**
	 * Checking write access and intermediate commit.
	 * 
	 * @throws SirixException
	 *           if anything weird happens
	 */
	private void checkAccessAndCommit() throws SirixException {
		mNodeRtx.assertNotClosed();
		mModificationCount++;
		intermediateCommitIfRequired();
	}

	// ////////////////////////////////////////////////////////////
	// insert operation
	// ////////////////////////////////////////////////////////////

	/**
	 * Adapting everything for insert operations.
	 * 
	 * @param pNewNode
	 *          pointer of the new node to be inserted
	 * @param pInsert
	 *          determines the position where to insert
	 * @throws SirixIOException
	 *           if anything weird happens
	 */
	private void adaptForInsert(final @Nonnull INode pNewNode,
			final @Nonnull EInsertPos pInsert, final @Nonnull EPage pPage)
			throws SirixIOException {
		assert pNewNode != null;
		assert pInsert != null;
		assert pPage != null;

		if (pNewNode instanceof IStructNode) {
			final IStructNode strucNode = (IStructNode) pNewNode;
			final IStructNode parent = (IStructNode) getPageTransaction()
					.prepareNodeForModification(pNewNode.getParentKey(), pPage);
			parent.incrementChildCount();
			if (pInsert == EInsertPos.ASFIRSTCHILD) {
				parent.setFirstChildKey(pNewNode.getNodeKey());
			}
			getPageTransaction().finishNodeModification(parent, pPage);

			if (strucNode.hasRightSibling()) {
				final IStructNode rightSiblingNode = (IStructNode) getPageTransaction()
						.prepareNodeForModification(strucNode.getRightSiblingKey(), pPage);
				rightSiblingNode.setLeftSiblingKey(pNewNode.getNodeKey());
				getPageTransaction().finishNodeModification(rightSiblingNode, pPage);
			}
			if (strucNode.hasLeftSibling()) {
				final IStructNode leftSiblingNode = (IStructNode) getPageTransaction()
						.prepareNodeForModification(strucNode.getLeftSiblingKey(), pPage);
				leftSiblingNode.setRightSiblingKey(pNewNode.getNodeKey());
				getPageTransaction().finishNodeModification(leftSiblingNode, pPage);
			}
		}
	}

	// ////////////////////////////////////////////////////////////
	// end of insert operation
	// ////////////////////////////////////////////////////////////

	// ////////////////////////////////////////////////////////////
	// remove operation
	// ////////////////////////////////////////////////////////////

	/**
	 * Adapting everything for remove operations.
	 * 
	 * @param pOldNode
	 *          pointer of the old node to be replaced
	 * @throws SirixException
	 *           if anything weird happens
	 */
	private void adaptForRemove(final @Nonnull IStructNode pOldNode,
			final @Nonnull EPage pPage) throws SirixException {
		assert pOldNode != null;

		// Concatenate neighbor text nodes if they exist (the right sibling is
		// deleted afterwards).
		boolean concatenated = false;
		if (pOldNode.hasLeftSibling() && pOldNode.hasRightSibling()
				&& moveTo(pOldNode.getRightSiblingKey())
				&& getNode().getKind() == EKind.TEXT
				&& moveTo(pOldNode.getLeftSiblingKey())
				&& getNode().getKind() == EKind.TEXT) {
			final StringBuilder builder = new StringBuilder(getValueOfCurrentNode());
			moveTo(pOldNode.getRightSiblingKey());
			builder.append(getValueOfCurrentNode());
			moveTo(pOldNode.getLeftSiblingKey());
			setValue(builder.toString());
			concatenated = true;
		}

		// Adapt left sibling node if there is one.
		if (pOldNode.hasLeftSibling()) {
			final IStructNode leftSibling = (IStructNode) getPageTransaction()
					.prepareNodeForModification(pOldNode.getLeftSiblingKey(), pPage);
			if (concatenated) {
				moveTo(pOldNode.getRightSiblingKey());
				leftSibling.setRightSiblingKey(((IStructNode) getNode())
						.getRightSiblingKey());
			} else {
				leftSibling.setRightSiblingKey(pOldNode.getRightSiblingKey());
			}
			getPageTransaction().finishNodeModification(leftSibling, pPage);
		}

		// Adapt right sibling node if there is one.
		if (pOldNode.hasRightSibling()) {
			IStructNode rightSibling;
			if (concatenated) {
				moveTo(pOldNode.getRightSiblingKey());
				moveTo(getStructuralNode().getRightSiblingKey());
				rightSibling = (IStructNode) getPageTransaction()
						.prepareNodeForModification(mNodeRtx.getNode().getNodeKey(), pPage);
				rightSibling.setLeftSiblingKey(pOldNode.getLeftSiblingKey());
			} else {
				rightSibling = (IStructNode) getPageTransaction()
						.prepareNodeForModification(pOldNode.getRightSiblingKey(), pPage);
				rightSibling.setLeftSiblingKey(pOldNode.getLeftSiblingKey());
			}
			getPageTransaction().finishNodeModification(rightSibling, pPage);
		}

		// Adapt parent, if node has now left sibling it is a first child.
		IStructNode parent = (IStructNode) getPageTransaction()
				.prepareNodeForModification(pOldNode.getParentKey(), pPage);
		if (!pOldNode.hasLeftSibling()) {
			parent.setFirstChildKey(pOldNode.getRightSiblingKey());
		}
		parent.decrementChildCount();
		if (concatenated) {
			parent.decrementDescendantCount();
			parent.decrementChildCount();
		}
		getPageTransaction().finishNodeModification(parent, pPage);
		if (concatenated) {
			// Adjust descendant count.
			moveTo(parent.getNodeKey());
			while (parent.hasParent()) {
				moveToParent();
				final IStructNode ancestor = (IStructNode) getPageTransaction()
						.prepareNodeForModification(mNodeRtx.getNode().getNodeKey(), pPage);
				ancestor.decrementDescendantCount();
				getPageTransaction().finishNodeModification(ancestor, pPage);
				parent = ancestor;
			}
		}

		// Remove right sibling text node if text nodes have been
		// concatenated/merged.
		if (concatenated) {
			moveTo(pOldNode.getRightSiblingKey());
			getPageTransaction().removeNode(mNodeRtx.getNode(), pPage);
		}

		// Remove non structural nodes of old node.
		if (pOldNode.getKind() == EKind.ELEMENT) {
			removeNonStructural((ElementNode) pOldNode);
		}

		// Remove old node.
		moveTo(pOldNode.getNodeKey());
		getPageTransaction().removeNode(pOldNode, pPage);
	}

	// ////////////////////////////////////////////////////////////
	// end of remove operation
	// ////////////////////////////////////////////////////////////

	/**
	 * Making an intermediate commit based on set attributes.
	 * 
	 * @throws SirixException
	 *           if commit fails
	 */
	private void intermediateCommitIfRequired() throws SirixException {
		mNodeRtx.assertNotClosed();
		if ((mMaxNodeCount > 0) && (mModificationCount > mMaxNodeCount)) {
			commit();
		}
	}

	/**
	 * Get the page transaction.
	 * 
	 * @return the page transaction.
	 */
	public IPageWriteTrx getPageTransaction() {
		return (IPageWriteTrx) mNodeRtx.getPageTransaction();
	}

	/**
	 * Adapting the structure with a hash for all ancestors only with insert.
	 * 
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	private void adaptHashesWithAdd() throws SirixIOException {
		if (!mBulkInsert) {
			switch (mHashKind) {
			case Rolling:
				rollingAdd();
				break;
			case Postorder:
				postorderAdd();
				break;
			default:
			}
		}
	}

	/**
	 * Adapting the structure with a hash for all ancestors only with remove.
	 * 
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	private void adaptHashesWithRemove() throws SirixIOException {
		if (!mBulkInsert) {
			switch (mHashKind) {
			case Rolling:
				rollingRemove();
				break;
			case Postorder:
				postorderRemove();
				break;
			default:
			}
		}
	}

	/**
	 * Adapting the structure with a hash for all ancestors only with update.
	 * 
	 * @param pOldHash
	 *          pOldHash to be removed
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	private void adaptHashedWithUpdate(final long pOldHash)
			throws SirixIOException {
		if (!mBulkInsert) {
			switch (mHashKind) {
			case Rolling:
				rollingUpdate(pOldHash);
				break;
			case Postorder:
				postorderAdd();
				break;
			default:
			}
		}
	}

	/**
	 * Removal operation for postorder hash computation.
	 * 
	 * @throws SirixIOException
	 *           if anything weird happens
	 */
	private void postorderRemove() throws SirixIOException {
		moveTo(getNode().getParentKey());
		postorderAdd();
	}

	/**
	 * Adapting the structure with a rolling hash for all ancestors only with
	 * insert.
	 * 
	 * @throws SirixIOException
	 *           if anything weird happened
	 */
	private void postorderAdd() throws SirixIOException {
		// start with hash to add
		final INode startNode = getNode();
		// long for adapting the hash of the parent
		long hashCodeForParent = 0;
		// adapting the parent if the current node is no structural one.
		if (!(startNode instanceof IStructNode)) {
			final INode node = (INode) getPageTransaction()
					.prepareNodeForModification(mNodeRtx.getNode().getNodeKey(),
							EPage.NODEPAGE);
			node.setHash(mHash.hashLong(mNodeRtx.getNode().hashCode()).asLong());
			getPageTransaction().finishNodeModification(node, EPage.NODEPAGE);
			moveTo(mNodeRtx.getNode().getParentKey());
		}
		// Cursor to root
		IStructNode cursorToRoot;
		do {
			cursorToRoot = (IStructNode) getPageTransaction()
					.prepareNodeForModification(mNodeRtx.getNode().getNodeKey(),
							EPage.NODEPAGE);
			hashCodeForParent = mNodeRtx.getNode().hashCode() + hashCodeForParent
					* PRIME;
			// Caring about attributes and namespaces if node is an element.
			if (cursorToRoot.getKind() == EKind.ELEMENT) {
				final ElementNode currentElement = (ElementNode) cursorToRoot;
				// setting the attributes and namespaces
				final int attCount = ((ElementNode) cursorToRoot).getAttributeCount();
				for (int i = 0; i < attCount; i++) {
					moveTo(currentElement.getAttributeKey(i));
					hashCodeForParent = mNodeRtx.getNode().hashCode() + hashCodeForParent
							* PRIME;
				}
				final int nspCount = ((ElementNode) cursorToRoot).getNamespaceCount();
				for (int i = 0; i < nspCount; i++) {
					moveTo(currentElement.getNamespaceKey(i));
					hashCodeForParent = mNodeRtx.getNode().hashCode() + hashCodeForParent
							* PRIME;
				}
				moveTo(cursorToRoot.getNodeKey());
			}

			// Caring about the children of a node
			if (moveTo(getStructuralNode().getFirstChildKey())) {
				do {
					hashCodeForParent = mNodeRtx.getNode().getHash() + hashCodeForParent
							* PRIME;
				} while (moveTo(getStructuralNode().getRightSiblingKey()));
				moveTo(getStructuralNode().getParentKey());
			}

			// setting hash and resetting hash
			cursorToRoot.setHash(hashCodeForParent);
			getPageTransaction().finishNodeModification(cursorToRoot, EPage.NODEPAGE);
			hashCodeForParent = 0;
		} while (moveTo(cursorToRoot.getParentKey()));

		mNodeRtx.setCurrentNode(startNode);
	}

	/**
	 * Adapting the structure with a rolling hash for all ancestors only with
	 * update.
	 * 
	 * @param pOldHash
	 *          pOldHash to be removed
	 * @throws SirixIOException
	 *           if anything weird happened
	 */
	private void rollingUpdate(final long pOldHash) throws SirixIOException {
		final INode newNode = getNode();
		final long hash = newNode.hashCode();
		final long newNodeHash = hash;
		long resultNew = hash;

		// go the path to the root
		do {
			final INode node = (INode) getPageTransaction()
					.prepareNodeForModification(mNodeRtx.getNode().getNodeKey(),
							EPage.NODEPAGE);
			if (node.getNodeKey() == newNode.getNodeKey()) {
				resultNew = node.getHash() - pOldHash;
				resultNew = resultNew + newNodeHash;
			} else {
				resultNew = node.getHash() - pOldHash * PRIME;
				resultNew = resultNew + newNodeHash * PRIME;
			}
			node.setHash(resultNew);
			getPageTransaction().finishNodeModification(node, EPage.NODEPAGE);
		} while (moveTo(mNodeRtx.getNode().getParentKey()));

		mNodeRtx.setCurrentNode(newNode);
	}

	/**
	 * Adapting the structure with a rolling hash for all ancestors only with
	 * remove.
	 * 
	 * @throws SirixIOException
	 *           if anything weird happened
	 */
	private void rollingRemove() throws SirixIOException {
		final INode startNode = getNode();
		long hashToRemove = startNode.getHash();
		long hashToAdd = 0;
		long newHash = 0;
		// go the path to the root
		do {
			final INode node = (INode) getPageTransaction()
					.prepareNodeForModification(mNodeRtx.getNode().getNodeKey(),
							EPage.NODEPAGE);
			if (node.getNodeKey() == startNode.getNodeKey()) {
				// the begin node is always null
				newHash = 0;
			} else if (node.getNodeKey() == startNode.getParentKey()) {
				// the parent node is just removed
				newHash = node.getHash() - hashToRemove * PRIME;
				hashToRemove = node.getHash();
				setRemoveDescendants(startNode);
			} else {
				// the ancestors are all touched regarding the modification
				newHash = node.getHash() - hashToRemove * PRIME;
				newHash = newHash + hashToAdd * PRIME;
				hashToRemove = node.getHash();
				setRemoveDescendants(startNode);
			}
			node.setHash(newHash);
			hashToAdd = newHash;
			getPageTransaction().finishNodeModification(node, EPage.NODEPAGE);
		} while (moveTo(mNodeRtx.getNode().getParentKey()));

		mNodeRtx.setCurrentNode(startNode);
	}

	/**
	 * Set new descendant count of ancestor after a remove-operation.
	 * 
	 * @param pStartNode
	 *          the node which has been removed
	 */
	private void setRemoveDescendants(final @Nonnull INode pStartNode) {
		assert pStartNode != null;
		if (pStartNode instanceof IStructNode) {
			final IStructNode node = ((IStructNode) getNode());
			node.setDescendantCount(node.getDescendantCount()
					- ((IStructNode) pStartNode).getDescendantCount() - 1);
		}
	}

	/**
	 * Adapting the structure with a rolling hash for all ancestors only with
	 * insert.
	 * 
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	private void rollingAdd() throws SirixIOException {
		// start with hash to add
		final INode startNode = mNodeRtx.getNode();
		final long oldDescendantCount = getStructuralNode().getDescendantCount();
		final long descendantCount = oldDescendantCount == 0 ? 1
				: oldDescendantCount + 1;
		long hashToAdd = startNode.getHash() == 0 ? mHash.hashLong(
				startNode.hashCode()).asLong() : startNode.getHash();
		long newHash = 0;
		long possibleOldHash = 0;
		// go the path to the root
		do {
			final INode node = (INode) getPageTransaction()
					.prepareNodeForModification(mNodeRtx.getNode().getNodeKey(),
							EPage.NODEPAGE);
			if (node.getNodeKey() == startNode.getNodeKey()) {
				// at the beginning, take the hashcode of the node only
				newHash = hashToAdd;
			} else if (node.getNodeKey() == startNode.getParentKey()) {
				// at the parent level, just add the node
				possibleOldHash = node.getHash();
				newHash = possibleOldHash + hashToAdd * PRIME;
				hashToAdd = newHash;
				setAddDescendants(startNode, node, descendantCount);
			} else {
				// at the rest, remove the existing old key for this element
				// and add the new one
				newHash = node.getHash() - possibleOldHash * PRIME;
				newHash = newHash + hashToAdd * PRIME;
				hashToAdd = newHash;
				possibleOldHash = node.getHash();
				setAddDescendants(startNode, node, descendantCount);
			}
			node.setHash(newHash);
			getPageTransaction().finishNodeModification(node, EPage.NODEPAGE);
		} while (moveTo(mNodeRtx.getNode().getParentKey()));
		mNodeRtx.setCurrentNode(startNode);
	}

	/**
	 * Set new descendant count of ancestor after an add-operation.
	 * 
	 * @param pStartNode
	 *          the node which has been removed
	 */
	private void setAddDescendants(final @Nonnull INode pStartNode,
			final @Nonnull INode pNodeToModifiy,
			@Nonnegative final long pDescendantCount) {
		assert pStartNode != null;
		assert pDescendantCount >= 0;
		assert pNodeToModifiy != null;
		if (pStartNode instanceof IStructNode) {
			final IStructNode node = (IStructNode) pNodeToModifiy;
			final long oldDescendantCount = node.getDescendantCount();
			node.setDescendantCount(oldDescendantCount + pDescendantCount);
		}
	}

	@Override
	public INodeWriteTrx copySubtreeAsFirstChild(final @Nonnull INodeReadTrx pRtx)
			throws SirixException {
		checkNotNull(pRtx);
		checkAccessAndCommit();
		final long nodeKey = getNode().getNodeKey();
		copy(pRtx, EInsert.ASFIRSTCHILD);
		moveTo(nodeKey);
		moveToFirstChild();
		return this;
	}

	@Override
	public INodeWriteTrx copySubtreeAsLeftSibling(final @Nonnull INodeReadTrx pRtx)
			throws SirixException {
		checkNotNull(pRtx);
		checkAccessAndCommit();
		final long nodeKey = getNode().getNodeKey();
		copy(pRtx, EInsert.ASLEFTSIBLING);
		moveTo(nodeKey);
		moveToFirstChild();
		return this;
	}

	@Override
	public INodeWriteTrx copySubtreeAsRightSibling(
			final @Nonnull INodeReadTrx pRtx) throws SirixException {
		checkNotNull(pRtx);
		checkAccessAndCommit();
		final long nodeKey = getNode().getNodeKey();
		copy(pRtx, EInsert.ASRIGHTSIBLING);
		moveTo(nodeKey);
		moveToRightSibling();
		return this;
	}

	/**
	 * Helper method for copy-operations.
	 * 
	 * @param pRtx
	 *          the source {@link INodeReadTrx}
	 * @param pInsert
	 *          the insertion strategy
	 * @throws SirixException
	 *           if anything fails in sirix
	 */
	private void copy(final @Nonnull INodeReadTrx pRtx,
			final @Nonnull EInsert pInsert) throws SirixException {
		assert pRtx != null;
		assert pInsert != null;
		final INodeReadTrx rtx = pRtx.getSession().beginNodeReadTrx(
				pRtx.getRevisionNumber());
		assert rtx.getRevisionNumber() == pRtx.getRevisionNumber();
		rtx.moveTo(pRtx.getNode().getNodeKey());
		assert rtx.getNode().getNodeKey() == pRtx.getNode().getNodeKey();
		if (rtx.getNode().getKind() == EKind.DOCUMENT_ROOT) {
			rtx.moveToFirstChild();
		}
		if (!(rtx.getNode() instanceof IStructNode)) {
			throw new IllegalStateException(
					"Node to insert must be a structural node (currently Text or Element)!");
		}

		if (rtx.getNode().getKind() == EKind.TEXT) {
			final String value = rtx.getValueOfCurrentNode();
			switch (pInsert) {
			case ASFIRSTCHILD:
				insertTextAsFirstChild(value);
				break;
			case ASLEFTSIBLING:
				insertTextAsLeftSibling(value);
				break;
			case ASRIGHTSIBLING:
				insertTextAsRightSibling(value);
				break;
			}
		} else {
			final XMLEventReader reader = new StAXSerializer(pRtx);
			new XMLShredder(this, reader, pInsert, EShredderCommit.NOCOMMIT).call();
		}
		// rtx.getNode().acceptVisitor(new InsertSubtreeVisitor(rtx, this,
		// pInsert));
		rtx.close();
	}

	@Override
	public INodeWriteTrx replaceNode(final @Nonnull String pXML)
			throws SirixException, IOException, XMLStreamException {
		checkNotNull(pXML);
		checkAccessAndCommit();
		final XMLEventReader reader = XMLShredder
				.createStringReader(checkNotNull(pXML));
		INode insertedRootNode = null;
		if (getNode() instanceof IStructNode) {
			final IStructNode currentNode = getStructuralNode();

			if (pXML.startsWith("<")) {
				while (reader.hasNext()) {
					XMLEvent event = reader.peek();

					if (event.isStartDocument()) {
						reader.nextEvent();
						continue;
					}

					switch (event.getEventType()) {
					case XMLStreamConstants.START_ELEMENT:
						EInsert pos = EInsert.ASFIRSTCHILD;
						if (currentNode.hasLeftSibling()) {
							moveToLeftSibling();
							pos = EInsert.ASRIGHTSIBLING;
						} else {
							moveToParent();
						}

						final XMLShredder shredder = new XMLShredder(this, reader, pos,
								EShredderCommit.NOCOMMIT);
						shredder.call();
						if (reader.hasNext()) {
							reader.nextEvent(); // End document.
						}

						insertedRootNode = mNodeRtx.getNode();
						moveTo(currentNode.getNodeKey());
						remove();
						moveTo(insertedRootNode.getNodeKey());
						break;
					}
				}
			} else {
				insertedRootNode = replaceWithTextNode(pXML);
			}
		}

		if (insertedRootNode != null) {
			moveTo(insertedRootNode.getNodeKey());
		}
		return this;
	}

	@Override
	public INodeWriteTrx replaceNode(final @Nonnull INodeReadTrx pRtx)
			throws SirixException {
		checkNotNull(pRtx);
		switch (pRtx.getNode().getKind()) {
		case ELEMENT:
		case TEXT:
			checkCurrentNode();
			replace(pRtx);
			break;
		case ATTRIBUTE:
			if (getNode().getKind() != EKind.ATTRIBUTE) {
				throw new IllegalStateException(
						"Current node must be an attribute node!");
			}
			insertAttribute(pRtx.getQNameOfCurrentNode(),
					pRtx.getValueOfCurrentNode());
			break;
		case NAMESPACE:
			if (mNodeRtx.getNode().getClass() != NamespaceNode.class) {
				throw new IllegalStateException(
						"Current node must be a namespace node!");
			}
			insertNamespace(pRtx.getQNameOfCurrentNode());
			break;
		default:
			throw new UnsupportedOperationException("Node type not supported!");
		}
		return this;
	}

	/**
	 * Check current node type (must be a structural node).
	 */
	private void checkCurrentNode() {
		if (!(getNode() instanceof IStructNode)) {
			throw new IllegalStateException("Current node must be a structural node!");
		}
	}

	/**
	 * Replace current node with a {@link TextNode}.
	 * 
	 * @param pValue
	 *          text value
	 * @return inserted node
	 * @throws SirixException
	 *           if anything fails
	 */
	private INode replaceWithTextNode(final @Nonnull String pValue)
			throws SirixException {
		assert pValue != null;
		final IStructNode currentNode = getStructuralNode();
		long key = currentNode.getNodeKey();
		if (currentNode.hasLeftSibling()) {
			moveToLeftSibling();
			key = insertTextAsRightSibling(pValue).getNode().getNodeKey();
		} else {
			moveToParent();
			key = insertTextAsFirstChild(pValue).getNode().getNodeKey();
			moveTo(key);
		}

		moveTo(currentNode.getNodeKey());
		remove();
		moveTo(key);
		return mNodeRtx.getNode();
	}

	/**
	 * Replace a node.
	 * 
	 * @param pRtx
	 *          the transaction which is located at the node to replace
	 * @return
	 * @throws SirixException
	 */
	private INode replace(final @Nonnull INodeReadTrx pRtx) throws SirixException {
		assert pRtx != null;
		final IStructNode currentNode = getStructuralNode();
		long key = currentNode.getNodeKey();
		if (currentNode.hasLeftSibling()) {
			moveToLeftSibling();
			key = copySubtreeAsRightSibling(pRtx).getNode().getNodeKey();
		} else {
			moveToParent();
			key = copySubtreeAsFirstChild(pRtx).getNode().getNodeKey();
			moveTo(key);
		}

		removeReplaced(currentNode, key);
		return mNodeRtx.getNode();
	}

	/**
	 * 
	 * @param pNode
	 * @param pKey
	 * @throws SirixException
	 */
	private void removeReplaced(final @Nonnull IStructNode pNode,
			@Nonnegative long pKey) throws SirixException {
		assert pNode != null;
		assert pKey >= 0;
		moveTo(pNode.getNodeKey());
		remove();
		moveTo(pKey);
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("readTrx", mNodeRtx.toString())
				.add("hashKind", mHashKind).toString();
	}

	@Override
	protected INodeReadTrx delegate() {
		return mNodeRtx;
	}

	@Override
	public void addPreCommitHook(final @Nonnull IPreCommitHook pHook) {
		mPreCommitHooks.add(checkNotNull(pHook));
	}

	@Override
	public void addPostCommitHook(final @Nonnull IPostCommitHook pHook) {
		mPostCommitHooks.add(checkNotNull(pHook));
	}

	@Override
	public boolean equals(final @Nullable Object pObj) {
		if (pObj instanceof NodeWriteTrx) {
			final NodeWriteTrx wtx = (NodeWriteTrx) pObj;
			return Objects.equal(mNodeRtx, wtx.mNodeRtx);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mNodeRtx);
	}

	@Override
	public PathSummary getPathSummary() {
		return mPathSummary;
	}

	@Override
	public AVLTree<TextValue, TextReferences> getAVLTree() {
		return mAVLTree;
	}
}
