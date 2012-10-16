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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
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
import org.sirix.api.Axis;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.api.PostCommitHook;
import org.sirix.api.PreCommitHook;
import org.sirix.axis.ChildAxis;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.axis.LevelOrderAxis;
import org.sirix.axis.PostOrderAxis;
import org.sirix.axis.filter.FilterAxis;
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
import org.sirix.node.CommentNode;
import org.sirix.node.Kind;
import org.sirix.node.ElementNode;
import org.sirix.node.NamespaceNode;
import org.sirix.node.PINode;
import org.sirix.node.TextNode;
import org.sirix.node.TextReferences;
import org.sirix.node.TextValue;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.ValNode;
import org.sirix.page.EPage;
import org.sirix.page.NamePage;
import org.sirix.page.UberPage;
import org.sirix.service.xml.serialize.StAXSerializer;
import org.sirix.service.xml.shredder.Insert;
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
 * Single-threaded instance of only write-transaction per resource, thus it is
 * not thread-safe.
 * </p>
 * 
 * <p>
 * If auto-commit is enabled, that is a scheduled commit(), all access to public
 * methods is synchronized, such that a commit() and another method doesn't
 * interfere, which could produce severe inconsistencies.
 * </p>
 * 
 * <p>
 * All methods throw {@link NullPointerException}s in case of null values for
 * reference parameters.
 * </p>
 * 
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 */
final class NodeWriteTrxImpl extends AbsForwardingNodeReadTrx implements
		NodeWriteTrx {

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
	private final HashKind mHashKind;

	/** Scheduled executor service. */
	private final ScheduledExecutorService mPool = Executors
			.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());

	/** {@link NodeReadTrxImpl} reference. */
	private final NodeReadTrxImpl mNodeRtx;

	/** Determines if a bulk insert operation is done. */
	private boolean mBulkInsert;

	/** Collection holding pre-commit hooks. */
	private final List<PreCommitHook> mPreCommitHooks = new ArrayList<>();

	/** Collection holding post-commit hooks. */
	private final List<PostCommitHook> mPostCommitHooks = new ArrayList<>();

	/** {@link PathSummary} instance. */
	private PathSummary mPathSummary;

	/** {@link AVLTree} instance. */
	private AVLTree<TextValue, TextReferences> mAVLTree;

	/** Indexes structures used during updates. */
	private final Set<EIndexes> mIndexes;

	/** {@link NodeFactoryImpl} to be able to create nodes. */
	private NodeFactoryImpl mNodeFactory;

	/** An optional lock for all methods, if an automatic commit is issued. */
	private final Optional<Semaphore> mLock;

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
	 *          {@link PageWriteTrx} to interact with the page layer
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
	NodeWriteTrxImpl(final @Nonnegative long pTransactionID,
			final @Nonnull SessionImpl pSession,
			final @Nonnull PageWriteTrx pPageWriteTrx,
			final @Nonnegative int pMaxNodeCount, final @Nonnull TimeUnit pTimeUnit,
			final @Nonnegative int pMaxTime) throws SirixIOException,
			SirixUsageException {

		// Do not accept negative values.
		if ((pMaxNodeCount < 0) || (pMaxTime < 0)) {
			throw new SirixUsageException("Negative arguments are not accepted.");
		}

		mNodeRtx = new NodeReadTrxImpl(pSession, pTransactionID, pPageWriteTrx);

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
		mNodeFactory = new NodeFactoryImpl(pPageWriteTrx);

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

		// Synchronize commit and other public methods if needed.
		if (pMaxTime > 0) {
			mLock = Optional.of(new Semaphore(1));
		} else {
			mLock = Optional.absent();
		}

		// // Redo last transaction if the system crashed.
		// if (!pPageWriteTrx.isCreated()) {
		// try {
		// commit();
		// } catch (final SirixException e) {
		// throw new IllegalStateException(e);
		// }
		// }
	}

	/** Acquire a lock if necessary. */
	private void acquireLock() {
		if (mLock.isPresent()) {
			mLock.get().acquireUninterruptibly();
		}
	}

	/** Release a lock if necessary. */
	private void unLock() {
		if (mLock.isPresent()) {
			mLock.get().release();
		}
	}

	@Override
	public NodeWriteTrx moveSubtreeToFirstChild(final @Nonnegative long pFromKey)
			throws SirixException, IllegalArgumentException {
		acquireLock();
		try {
			if (pFromKey < 0 || pFromKey > getMaxNodeKey()) {
				throw new IllegalArgumentException("Argument must be a valid node key!");
			}
			if (pFromKey == getCurrentNode().getNodeKey()) {
				throw new IllegalArgumentException(
						"Can't move itself to right sibling of itself!");
			}

			@SuppressWarnings("unchecked")
			final Optional<? extends Node> node = (Optional<? extends Node>) getPageTransaction()
					.getNode(pFromKey, EPage.NODEPAGE);
			if (!node.isPresent()) {
				throw new IllegalStateException("Node to move must exist!");
			}

			final Node nodeToMove = node.get();

			if (nodeToMove instanceof StructNode
					&& getCurrentNode().getKind() == Kind.ELEMENT) {
				// Safe to cast (because IStructNode is a subtype of INode).
				checkAncestors(nodeToMove);
				checkAccessAndCommit();

				final ElementNode nodeAnchor = (ElementNode) getCurrentNode();
				if (nodeAnchor.getFirstChildKey() != nodeToMove.getNodeKey()) {
					final StructNode toMove = (StructNode) nodeToMove;
					// Adapt hashes.
					adaptHashesForMove(toMove);

					// Adapt pointers and merge sibling text nodes.
					adaptForMove(toMove, nodeAnchor, InsertPos.ASFIRSTCHILD);
					mNodeRtx.setCurrentNode(toMove);
					adaptHashesWithAdd();

					// Adapt path summary.
					if (mIndexes.contains(EIndexes.PATH) && toMove instanceof NameNode) {
						final NameNode moved = (NameNode) toMove;
						adaptPathForChangedNode(moved, getName(), moved.getNameKey(),
								moved.getURIKey(), EOPType.MOVED);
					}
				}

				return this;
			} else {
				throw new SirixUsageException(
						"Move is not allowed if moved node is not an ElementNode and the node isn't inserted at an element node!");
			}
		} finally {
			unLock();
		}
	}

	@Override
	public NodeWriteTrx moveSubtreeToLeftSibling(final @Nonnegative long pFromKey)
			throws SirixException, IllegalArgumentException {
		acquireLock();
		try {
			if (mNodeRtx.getStructuralNode().hasLeftSibling()) {
				moveToLeftSibling();
				return moveSubtreeToRightSibling(pFromKey);
			} else {
				moveToParent();
				return moveSubtreeToFirstChild(pFromKey);
			}
		} finally {
			unLock();
		}
	}

	@Override
	public NodeWriteTrx moveSubtreeToRightSibling(
			final @Nonnegative long pFromKey) throws SirixException {
		acquireLock();
		try {
			if (pFromKey < 0 || pFromKey > getMaxNodeKey()) {
				throw new IllegalArgumentException("Argument must be a valid node key!");
			}
			if (pFromKey == getCurrentNode().getNodeKey()) {
				throw new IllegalArgumentException(
						"Can't move itself to first child of itself!");
			}

			// Save: Every node in the "usual" node page is of type INode.
			@SuppressWarnings("unchecked")
			final Optional<? extends Node> node = (Optional<? extends Node>) getPageTransaction()
					.getNode(pFromKey, EPage.NODEPAGE);
			if (!node.isPresent()) {
				throw new IllegalStateException("Node to move must exist!");
			}

			final Node nodeToMove = node.get();

			if (nodeToMove instanceof StructNode
					&& getCurrentNode() instanceof StructNode) {
				final StructNode toMove = (StructNode) nodeToMove;
				checkAncestors(toMove);
				checkAccessAndCommit();

				final StructNode nodeAnchor = (StructNode) getCurrentNode();
				if (nodeAnchor.getRightSiblingKey() != nodeToMove.getNodeKey()) {
					final long parentKey = toMove.getParentKey();

					// Adapt hashes.
					adaptHashesForMove(toMove);

					// Adapt pointers and merge sibling text nodes.
					adaptForMove(toMove, nodeAnchor, InsertPos.ASRIGHTSIBLING);
					mNodeRtx.setCurrentNode(toMove);
					adaptHashesWithAdd();

					// Adapt path summary.
					if (mIndexes.contains(EIndexes.PATH) && toMove instanceof NameNode) {
						final NameNode moved = (NameNode) toMove;
						final EOPType type = moved.getParentKey() == parentKey ? EOPType.MOVEDSAMELEVEL
								: EOPType.MOVED;
						adaptPathForChangedNode(moved, getName(), moved.getNameKey(),
								moved.getURIKey(), type);
					}
				}
				return this;
			} else {
				throw new SirixUsageException(
						"Move is not allowed if moved node is not an ElementNode or TextNode and the node isn't inserted at an ElementNode or TextNode!");
			}
		} finally {
			unLock();
		}
	}

	/**
	 * Adapt hashes for move operation ("remove" phase).
	 * 
	 * @param pNodeToMove
	 *          node which implements {@link StructNode} and is moved
	 * @throws SirixIOException
	 *           if any I/O operation fails
	 */
	private void adaptHashesForMove(final @Nonnull StructNode pNodeToMove)
			throws SirixIOException {
		assert pNodeToMove != null;
		mNodeRtx.setCurrentNode(pNodeToMove);
		adaptHashesWithRemove();
	}

	/**
	 * Adapting everything for move operations.
	 * 
	 * @param pFromNode
	 *          root {@link StructNode} of the subtree to be moved
	 * @param pToNode
	 *          the {@link StructNode} which is the anchor of the new subtree
	 * @param pInsert
	 *          determines if it has to be inserted as a first child or a right
	 *          sibling
	 * @throws SirixException
	 *           if removing a node fails after merging text nodes
	 */
	private void adaptForMove(final @Nonnull StructNode pFromNode,
			final @Nonnull StructNode pToNode, final @Nonnull InsertPos pInsert)
			throws SirixException {
		assert pFromNode != null;
		assert pToNode != null;
		assert pInsert != null;

		// Modify nodes where the subtree has been moved from.
		// ==============================================================================
		final StructNode parent = (StructNode) getPageTransaction()
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
		default:
		}

		// Adapt first child key of former parent.
		if (parent.getFirstChildKey() == pFromNode.getNodeKey()) {
			parent.setFirstChildKey(pFromNode.getRightSiblingKey());
		}
		getPageTransaction().finishNodeModification(parent.getNodeKey(),
				EPage.NODEPAGE);

		// Adapt left sibling key of former right sibling.
		if (pFromNode.hasRightSibling()) {
			final StructNode rightSibling = (StructNode) getPageTransaction()
					.prepareNodeForModification(pFromNode.getRightSiblingKey(),
							EPage.NODEPAGE);
			rightSibling.setLeftSiblingKey(pFromNode.getLeftSiblingKey());
			getPageTransaction().finishNodeModification(rightSibling.getNodeKey(),
					EPage.NODEPAGE);
		}

		// Adapt right sibling key of former left sibling.
		if (pFromNode.hasLeftSibling()) {
			final StructNode leftSibling = (StructNode) getPageTransaction()
					.prepareNodeForModification(pFromNode.getLeftSiblingKey(),
							EPage.NODEPAGE);
			leftSibling.setRightSiblingKey(pFromNode.getRightSiblingKey());
			getPageTransaction().finishNodeModification(leftSibling.getNodeKey(),
					EPage.NODEPAGE);
		}

		// Merge text nodes.
		if (pFromNode.hasLeftSibling() && pFromNode.hasRightSibling()) {
			moveTo(pFromNode.getLeftSiblingKey());
			if (getCurrentNode() != null && getCurrentNode().getKind() == Kind.TEXT) {
				final StringBuilder builder = new StringBuilder(getValue());
				moveTo(pFromNode.getRightSiblingKey());
				if (getCurrentNode() != null
						&& getCurrentNode().getKind() == Kind.TEXT) {
					builder.append(getValue());
					if (pFromNode.getRightSiblingKey() == pToNode.getNodeKey()) {
						moveTo(pFromNode.getLeftSiblingKey());
						if (mNodeRtx.getStructuralNode().hasLeftSibling()) {
							final StructNode leftSibling = (StructNode) getPageTransaction()
									.prepareNodeForModification(
											mNodeRtx.getStructuralNode().getLeftSiblingKey(),
											EPage.NODEPAGE);
							leftSibling.setRightSiblingKey(pFromNode.getRightSiblingKey());
							getPageTransaction().finishNodeModification(
									leftSibling.getNodeKey(), EPage.NODEPAGE);
						}
						final long leftSiblingKey = mNodeRtx.getStructuralNode()
								.hasLeftSibling() == true ? mNodeRtx.getStructuralNode()
								.getLeftSiblingKey() : getCurrentNode().getNodeKey();
						moveTo(pFromNode.getRightSiblingKey());
						final StructNode rightSibling = (StructNode) getPageTransaction()
								.prepareNodeForModification(getCurrentNode().getNodeKey(),
										EPage.NODEPAGE);
						rightSibling.setLeftSiblingKey(leftSiblingKey);
						getPageTransaction().finishNodeModification(
								rightSibling.getNodeKey(), EPage.NODEPAGE);
						moveTo(pFromNode.getLeftSiblingKey());
						remove();
						moveTo(pFromNode.getRightSiblingKey());
					} else {
						if (mNodeRtx.getStructuralNode().hasRightSibling()) {
							final StructNode rightSibling = (StructNode) getPageTransaction()
									.prepareNodeForModification(
											mNodeRtx.getStructuralNode().getRightSiblingKey(),
											EPage.NODEPAGE);
							rightSibling.setLeftSiblingKey(pFromNode.getLeftSiblingKey());
							getPageTransaction().finishNodeModification(
									rightSibling.getNodeKey(), EPage.NODEPAGE);
						}
						final long rightSiblingKey = mNodeRtx.getStructuralNode()
								.hasRightSibling() == true ? mNodeRtx.getStructuralNode()
								.getRightSiblingKey() : getCurrentNode().getNodeKey();
						moveTo(pFromNode.getLeftSiblingKey());
						final StructNode leftSibling = (StructNode) getPageTransaction()
								.prepareNodeForModification(getCurrentNode().getNodeKey(),
										EPage.NODEPAGE);
						leftSibling.setRightSiblingKey(rightSiblingKey);
						getPageTransaction().finishNodeModification(
								leftSibling.getNodeKey(), EPage.NODEPAGE);
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
	private NodeWriteTrx insertPathAsFirstChild(final @Nonnull QName pQName,
			final Kind pKind, final int pLevel) throws SirixException {
		if (!XMLToken.isValidQName(checkNotNull(pQName))) {
			throw new IllegalArgumentException("The QName is not valid!");
		}

		checkAccessAndCommit();

		final long parentKey = mPathSummary.getNodeKey();
		final long leftSibKey = EFixed.NULL_NODE_KEY.getStandardProperty();
		final long rightSibKey = mPathSummary.getFirstChildKey();
		final PathNode node = mNodeFactory.createPathNode(parentKey, leftSibKey,
				rightSibKey, 0, pQName, pKind, pLevel);

		mPathSummary.setCurrentNode(node);
		adaptForInsert(node, InsertPos.ASFIRSTCHILD, EPage.PATHSUMMARYPAGE);
		mPathSummary.setCurrentNode(node);

		return this;
	}

	@Override
	public NodeWriteTrx insertElementAsFirstChild(final @Nonnull QName pQName)
			throws SirixException {
		if (!XMLToken.isValidQName(checkNotNull(pQName))) {
			throw new IllegalArgumentException("The QName is not valid!");
		}
		acquireLock();
		try {
			final Kind kind = mNodeRtx.getCurrentNode().getKind();
			if (kind == Kind.ELEMENT || kind == Kind.DOCUMENT_ROOT) {
				checkAccessAndCommit();

				final long parentKey = mNodeRtx.getCurrentNode().getNodeKey();
				final long leftSibKey = EFixed.NULL_NODE_KEY.getStandardProperty();
				final long rightSibKey = ((StructNode) mNodeRtx.getCurrentNode())
						.getFirstChildKey();

				final long pathNodeKey = mIndexes.contains(EIndexes.PATH) ? getPathNodeKey(
						pQName, Kind.ELEMENT) : 0;

				final ElementNode node = mNodeFactory.createElementNode(parentKey,
						leftSibKey, rightSibKey, 0, pQName, pathNodeKey);

				mNodeRtx.setCurrentNode(node);
				adaptForInsert(node, InsertPos.ASFIRSTCHILD, EPage.NODEPAGE);
				mNodeRtx.setCurrentNode(node);
				adaptHashesWithAdd();

				return this;
			} else {
				throw new SirixUsageException(
						"Insert is not allowed if current node is not an ElementNode!");
			}
		} finally {
			unLock();
		}
	}

	private long getPathNodeKey(final @Nonnull QName pQName,
			final @Nonnull Kind pKind) throws SirixException {
		final Kind kind = mNodeRtx.getCurrentNode().getKind();
		int level = 0;
		if (kind == Kind.DOCUMENT_ROOT) {
			mPathSummary.moveTo(EFixed.DOCUMENT_NODE_KEY.getStandardProperty());
		} else {
			movePathSummary();
			level = mPathSummary.getLevel();
		}

		final long nodeKey = mPathSummary.getNodeKey();
		final Axis axis = new FilterAxis(new ChildAxis(mPathSummary),
				new NameFilter(mPathSummary,
						pKind == Kind.NAMESPACE ? pQName.getPrefix()
								: Utils.buildName(pQName)), new PathKindFilter(mPathSummary,
						pKind));
		long retVal = nodeKey;
		if (axis.hasNext()) {
			axis.next();
			retVal = mPathSummary.getNodeKey();
			final PathNode pathNode = (PathNode) getPageTransaction()
					.prepareNodeForModification(retVal, EPage.PATHSUMMARYPAGE);
			pathNode.incrementReferenceCount();
			getPageTransaction().finishNodeModification(pathNode.getNodeKey(),
					EPage.PATHSUMMARYPAGE);
		} else {
			assert nodeKey == mPathSummary.getNodeKey();
			insertPathAsFirstChild(pQName, pKind, level + 1);
			retVal = mPathSummary.getNodeKey();
		}
		return retVal;
	}

	/**
	 * Move path summary cursor to the path node which is references by the
	 * current node.
	 */
	private void movePathSummary() {
		if (mNodeRtx.getCurrentNode() instanceof NameNode) {
			mPathSummary.moveTo(((NameNode) mNodeRtx.getCurrentNode())
					.getPathNodeKey());
		} else {
			throw new IllegalStateException();
		}
	}

	@Override
	public NodeWriteTrx insertElementAsLeftSibling(final @Nonnull QName pQName)
			throws SirixException {
		if (!XMLToken.isValidQName(checkNotNull(pQName))) {
			throw new IllegalArgumentException("The QName is not valid!");
		}
		acquireLock();
		try {
			if (getCurrentNode() instanceof StructNode) {
				checkAccessAndCommit();

				final long key = getCurrentNode().getNodeKey();
				moveToParent();
				final long pathNodeKey = mIndexes.contains(EIndexes.PATH) ? getPathNodeKey(
						pQName, Kind.ELEMENT) : 0;
				moveTo(key);

				final long parentKey = getCurrentNode().getParentKey();
				final long leftSibKey = ((StructNode) getCurrentNode())
						.getLeftSiblingKey();
				final long rightSibKey = getCurrentNode().getNodeKey();
				final ElementNode node = mNodeFactory.createElementNode(parentKey,
						leftSibKey, rightSibKey, 0, pQName, pathNodeKey);

				mNodeRtx.setCurrentNode(node);
				adaptForInsert(node, InsertPos.ASLEFTSIBLING, EPage.NODEPAGE);
				mNodeRtx.setCurrentNode(node);
				adaptHashesWithAdd();

				return this;
			} else {
				throw new SirixUsageException(
						"Insert is not allowed if current node is not an StructuralNode (either Text or Element)!");
			}
		} finally {
			unLock();
		}
	}

	@Override
	public NodeWriteTrx insertElementAsRightSibling(final @Nonnull QName pQName)
			throws SirixException {
		if (!XMLToken.isValidQName(checkNotNull(pQName))) {
			throw new IllegalArgumentException("The QName is not valid!");
		}
		acquireLock();
		try {
			if (getCurrentNode() instanceof StructNode) {
				checkAccessAndCommit();

				final long key = getCurrentNode().getNodeKey();
				moveToParent();
				final long pathNodeKey = mIndexes.contains(EIndexes.PATH) ? getPathNodeKey(
						pQName, Kind.ELEMENT) : 0;
				moveTo(key);

				final long parentKey = getCurrentNode().getParentKey();
				final long leftSibKey = getCurrentNode().getNodeKey();
				final long rightSibKey = ((StructNode) getCurrentNode())
						.getRightSiblingKey();
				final ElementNode node = mNodeFactory.createElementNode(parentKey,
						leftSibKey, rightSibKey, 0, pQName, pathNodeKey);

				mNodeRtx.setCurrentNode(node);
				adaptForInsert(node, InsertPos.ASRIGHTSIBLING, EPage.NODEPAGE);
				mNodeRtx.setCurrentNode(node);
				adaptHashesWithAdd();

				return this;
			} else {
				throw new SirixUsageException(
						"Insert is not allowed if current node is not an StructuralNode (either Text or Element)!");
			}
		} finally {
			unLock();
		}
	}

	@Override
	public NodeWriteTrx insertSubtree(final @Nonnull XMLEventReader pReader,
			final @Nonnull Insert pInsert) throws SirixException {
		acquireLock();
		try {
			if (getCurrentNode() instanceof StructNode) {
				checkAccessAndCommit();
				mBulkInsert = true;
				long nodeKey = getCurrentNode().getNodeKey();
				final XMLShredder shredder = new XMLShredder.Builder(this, pReader,
						pInsert).commitAfterwards().build();
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
				nodeKey = getCurrentNode().getNodeKey();
				postOrderTraversalHashes();
				final Node startNode = getCurrentNode();
				moveToParent();
				while (getCurrentNode().hasParent()) {
					moveToParent();
					addParentHash(startNode);
				}
				moveTo(nodeKey);
				mBulkInsert = false;
			}
		} finally {
			unLock();
		}
		return this;
	}

	@Override
	public NodeWriteTrx insertPIAsLeftSibling(final @Nonnull String pTarget,
			final @Nonnull String pContent) throws SirixException {
		return pi(pTarget, pContent, Insert.ASLEFTSIBLING);
	}

	@Override
	public NodeWriteTrx insertPIAsRightSibling(final @Nonnull String pTarget,
			final @Nonnull String pContent) throws SirixException {
		return pi(pTarget, pContent, Insert.ASRIGHTSIBLING);
	}

	@Override
	public NodeWriteTrx insertPIAsFirstChild(final @Nonnull String pTarget,
			final @Nonnull String pContent) throws SirixException {
		return pi(pTarget, pContent, Insert.ASFIRSTCHILD);
	}

	/**
	 * Processing instruction.
	 * 
	 * @param pQName
	 *          {@link QName} of PI
	 * @param pValue
	 *          value of PI
	 * @param pInsert
	 *          insertion location
	 * @throws SirixException
	 *           if any unexpected error occurs
	 */
	private NodeWriteTrx pi(final @Nonnull String pTarget,
			final @Nonnull String pContent, final @Nonnull Insert pInsert)
			throws SirixException {
		final byte[] target = getBytes(pTarget);
		if (!XMLToken.isNCName(checkNotNull(target))) {
			throw new IllegalArgumentException("The target is not valid!");
		}
		if (pContent.contains("?>-")) {
			throw new SirixUsageException("The content must not contain '?>-'");
		}
		acquireLock();
		try {
			if (getCurrentNode() instanceof StructNode) {
				checkAccessAndCommit();

				// Insert new comment node.
				final byte[] content = getBytes(pContent);
				long parentKey = 0;
				long leftSibKey = 0;
				long rightSibKey = 0;
				InsertPos pos = InsertPos.ASFIRSTCHILD;
				switch (pInsert) {
				case ASFIRSTCHILD:
					parentKey = getCurrentNode().getNodeKey();
					leftSibKey = EFixed.NULL_NODE_KEY.getStandardProperty();
					rightSibKey = ((StructNode) getCurrentNode()).getFirstChildKey();
					break;
				case ASRIGHTSIBLING:
					parentKey = getCurrentNode().getParentKey();
					leftSibKey = getCurrentNode().getNodeKey();
					rightSibKey = ((StructNode) getCurrentNode()).getRightSiblingKey();
					pos = InsertPos.ASRIGHTSIBLING;
					break;
				case ASLEFTSIBLING:
					parentKey = getCurrentNode().getParentKey();
					leftSibKey = ((StructNode) getCurrentNode()).getLeftSiblingKey();
					rightSibKey = getCurrentNode().getNodeKey();
					pos = InsertPos.ASLEFTSIBLING;
					break;
				default:
					throw new IllegalStateException("Insert location not known!");
				}

				final QName targetName = new QName(pTarget);
				final long pathNodeKey = mIndexes.contains(EIndexes.PATH) ? getPathNodeKey(
						targetName, Kind.PROCESSING) : 0;
				final PINode node = mNodeFactory.createPINode(parentKey, leftSibKey,
						rightSibKey, targetName, content,
						mNodeRtx.mSession.mResourceConfig.mCompression, pathNodeKey);

				// Adapt local nodes and hashes.
				mNodeRtx.setCurrentNode(node);
				adaptForInsert(node, pos, EPage.NODEPAGE);
				mNodeRtx.setCurrentNode(node);
				adaptHashesWithAdd();

				return this;
			} else {
				throw new SirixUsageException("Current node must be a structural node!");
			}
		} finally {
			unLock();
		}
	}

	@Override
	public NodeWriteTrx insertCommentAsLeftSibling(final @Nonnull String pValue)
			throws SirixException {
		return comment(pValue, Insert.ASLEFTSIBLING);
	}

	@Override
	public NodeWriteTrx insertCommentAsRightSibling(final @Nonnull String pValue)
			throws SirixException {
		return comment(pValue, Insert.ASRIGHTSIBLING);
	}

	@Override
	public NodeWriteTrx insertCommentAsFirstChild(final @Nonnull String pValue)
			throws SirixException {
		return comment(pValue, Insert.ASFIRSTCHILD);
	}

	/**
	 * Comment node.
	 * 
	 * @param pValue
	 *          value of comment
	 * @param pInsert
	 *          insertion location
	 * @throws SirixException
	 *           if any unexpected error occurs
	 */
	private NodeWriteTrx comment(final @Nonnull String pValue,
			final @Nonnull Insert pInsert) throws SirixException {
		// Produces a NPE if pValue is null (what we want).
		if (pValue.contains("--")) {
			throw new SirixUsageException(
					"Character sequence \"--\" is not allowed in comment content!");
		}
		if (pValue.endsWith("-")) {
			throw new SirixUsageException("Comment content must not end with \"-\"!");
		}
		acquireLock();
		try {
			if (getCurrentNode() instanceof StructNode) {
				checkAccessAndCommit();

				// Insert new comment node.
				final byte[] value = getBytes(pValue);
				long parentKey = 0;
				long leftSibKey = 0;
				long rightSibKey = 0;
				InsertPos pos = InsertPos.ASFIRSTCHILD;
				switch (pInsert) {
				case ASFIRSTCHILD:
					parentKey = getCurrentNode().getNodeKey();
					leftSibKey = EFixed.NULL_NODE_KEY.getStandardProperty();
					rightSibKey = ((StructNode) getCurrentNode()).getFirstChildKey();
					break;
				case ASRIGHTSIBLING:
					parentKey = getCurrentNode().getParentKey();
					leftSibKey = getCurrentNode().getNodeKey();
					rightSibKey = ((StructNode) getCurrentNode()).getRightSiblingKey();
					pos = InsertPos.ASRIGHTSIBLING;
					break;
				case ASLEFTSIBLING:
					parentKey = getCurrentNode().getParentKey();
					leftSibKey = ((StructNode) getCurrentNode()).getLeftSiblingKey();
					rightSibKey = getCurrentNode().getNodeKey();
					pos = InsertPos.ASLEFTSIBLING;
					break;
				default:
					throw new IllegalStateException("Insert location not known!");
				}

				final CommentNode node = mNodeFactory.createCommentNode(parentKey,
						leftSibKey, rightSibKey, value,
						mNodeRtx.mSession.mResourceConfig.mCompression);

				// Adapt local nodes and hashes.
				mNodeRtx.setCurrentNode(node);
				adaptForInsert(node, pos, EPage.NODEPAGE);
				mNodeRtx.setCurrentNode(node);
				adaptHashesWithAdd();

				return this;
			} else {
				throw new SirixUsageException("Current node must be a structural node!");
			}
		} finally {
			unLock();
		}
	}

	@Override
	public NodeWriteTrx insertTextAsFirstChild(final @Nonnull String pValue)
			throws SirixException {
		checkNotNull(pValue);
		acquireLock();
		try {
			if (getCurrentNode() instanceof StructNode
					&& getCurrentNode().getKind() != Kind.DOCUMENT_ROOT
					&& !pValue.isEmpty()) {
				checkAccessAndCommit();

				final long parentKey = getCurrentNode().getNodeKey();
				final long leftSibKey = EFixed.NULL_NODE_KEY.getStandardProperty();
				final long rightSibKey = ((StructNode) getCurrentNode())
						.getFirstChildKey();

				// Update value in case of adjacent text nodes.
				if (hasNode(rightSibKey)) {
					moveTo(rightSibKey);
					if (getCurrentNode().getKind() == Kind.TEXT) {
						setValue(new StringBuilder(pValue).append(getValue()).toString());
						adaptHashedWithUpdate(getCurrentNode().getHash());
						return this;
					}
					moveTo(parentKey);
				}

				// Insert new text node if no adjacent text nodes are found.
				final byte[] value = getBytes(pValue);
				final TextNode node = mNodeFactory.createTextNode(parentKey,
						leftSibKey, rightSibKey, value,
						mNodeRtx.mSession.mResourceConfig.mCompression);

				// Adapt local nodes and hashes.
				mNodeRtx.setCurrentNode(node);
				adaptForInsert(node, InsertPos.ASFIRSTCHILD, EPage.NODEPAGE);
				mNodeRtx.setCurrentNode(node);
				adaptHashesWithAdd();

				// Index text value.
				indexText(value);

				return this;
			} else {
				throw new SirixUsageException(
						"Insert is not allowed if current node is not an ElementNode or TextNode!");
			}
		} finally {
			unLock();
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
			mNodeRtx.moveToParent();
			final long pathNodeKey = mNodeRtx.isNameNode() ? mNodeRtx
					.getPathNodeKey() : 0;
			final PageWriteTrx pageTrx = getPageTransaction();
			final TextValue textVal = (TextValue) pageTrx.createNode(new TextValue(
					pValue, pageTrx.getActualRevisionRootPage().getMaxValueNodeKey() + 1,
					pathNodeKey), EPage.VALUEPAGE);
			final Optional<TextReferences> textReferences = mAVLTree.get(textVal);
			if (textReferences.isPresent()) {
				final TextReferences references = textReferences.get();
				references.setNodeKey(mNodeRtx.getCurrentNode().getNodeKey());
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
	public NodeWriteTrx insertTextAsLeftSibling(final @Nonnull String pValue)
			throws SirixException {
		checkNotNull(pValue);
		acquireLock();
		try {
			if (getCurrentNode() instanceof StructNode
					&& getCurrentNode().getKind() != Kind.DOCUMENT_ROOT
					&& !pValue.isEmpty()) {
				checkAccessAndCommit();

				final long parentKey = getCurrentNode().getParentKey();
				final long leftSibKey = ((StructNode) getCurrentNode())
						.getLeftSiblingKey();
				final long rightSibKey = getCurrentNode().getNodeKey();

				// Update value in case of adjacent text nodes.
				final StringBuilder builder = new StringBuilder();
				if (getCurrentNode().getKind() == Kind.TEXT) {
					builder.append(pValue);
				}
				builder.append(getValue());

				if (!pValue.equals(builder.toString())) {
					setValue(builder.toString());
					return this;
				}
				if (hasNode(leftSibKey)) {
					moveTo(leftSibKey);
					final StringBuilder value = new StringBuilder();
					if (getCurrentNode().getKind() == Kind.TEXT) {
						value.append(getValue()).append(builder);
					}
					if (!pValue.equals(value.toString())) {
						setValue(value.toString());
						return this;
					}
				}

				// Insert new text node if no adjacent text nodes are found.
				moveTo(rightSibKey);
				final byte[] value = getBytes(builder.toString());
				final TextNode node = mNodeFactory.createTextNode(parentKey,
						leftSibKey, rightSibKey, value,
						mNodeRtx.mSession.mResourceConfig.mCompression);

				// Adapt local nodes and hashes.
				mNodeRtx.setCurrentNode(node);
				adaptForInsert(node, InsertPos.ASLEFTSIBLING, EPage.NODEPAGE);
				mNodeRtx.setCurrentNode(node);
				adaptHashesWithAdd();

				// Index text value.
				// indexText(value);

				return this;
			} else {
				throw new SirixUsageException(
						"Insert is not allowed if current node is not an Element- or Text-node!");
			}
		} finally {
			unLock();
		}
	}

	@Override
	public NodeWriteTrx insertTextAsRightSibling(final @Nonnull String pValue)
			throws SirixException {
		checkNotNull(pValue);
		acquireLock();
		try {
			if (getCurrentNode() instanceof StructNode
					&& getCurrentNode().getKind() != Kind.DOCUMENT_ROOT
					&& !pValue.isEmpty()) {
				checkAccessAndCommit();

				// If an empty value is specified the node needs to be removed (see
				// XDM).
				if (pValue.isEmpty()) {
					remove();
					return this;
				}

				final long parentKey = getCurrentNode().getParentKey();
				final long leftSibKey = getCurrentNode().getNodeKey();
				final long rightSibKey = ((StructNode) getCurrentNode())
						.getRightSiblingKey();

				// Update value in case of adjacent text nodes.
				final StringBuilder builder = new StringBuilder();
				if (getCurrentNode().getKind() == Kind.TEXT) {
					builder.append(getValue());
				}
				builder.append(pValue);
				if (!pValue.equals(builder.toString())) {
					setValue(builder.toString());
					return this;
				}
				if (hasNode(rightSibKey)) {
					moveTo(rightSibKey);
					if (getCurrentNode().getKind() == Kind.TEXT) {
						builder.append(getValue());
					}
					if (!pValue.equals(builder.toString())) {
						setValue(builder.toString());
						return this;
					}
				}

				// Insert new text node if no adjacent text nodes are found.
				moveTo(leftSibKey);
				final byte[] value = getBytes(builder.toString());
				final TextNode node = mNodeFactory.createTextNode(parentKey,
						leftSibKey, rightSibKey, value,
						mNodeRtx.mSession.mResourceConfig.mCompression);

				// Adapt local nodes and hashes.
				mNodeRtx.setCurrentNode(node);
				adaptForInsert(node, InsertPos.ASRIGHTSIBLING, EPage.NODEPAGE);
				mNodeRtx.setCurrentNode(node);
				adaptHashesWithAdd();

				// Index text value.
				indexText(value);

				return this;
			} else {
				throw new SirixUsageException(
						"Insert is not allowed if current node is not an Element- or Text-node!");
			}
		} finally {
			unLock();
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
	public NodeWriteTrx insertAttribute(final @Nonnull QName pQName,
			final @Nonnull String pValue) throws SirixException {
		return insertAttribute(pQName, pValue, Movement.NONE);
	}

	@Override
	public NodeWriteTrx insertAttribute(final @Nonnull QName pQName,
			final @Nonnull String pValue, final @Nonnull Movement pMove)
			throws SirixException {
		checkNotNull(pValue);
		if (!XMLToken.isValidQName(checkNotNull(pQName))) {
			throw new IllegalArgumentException("The QName is not valid!");
		}
		acquireLock();
		try {
			if (getCurrentNode().getKind() == Kind.ELEMENT) {
				checkAccessAndCommit();

				/*
				 * Update value in case of the same attribute name is found but the
				 * attribute to insert has a different value (otherwise an exception is
				 * thrown because of a duplicate attribute which would otherwise be
				 * inserted!).
				 */
				final Optional<Long> attKey = ((ElementNode) getCurrentNode())
						.getAttributeKeyByName(pQName);
				if (attKey.isPresent()) {
					moveTo(attKey.get());
					final QName qName = getName();
					if (pQName.equals(qName)
							&& pQName.getPrefix().equals(qName.getPrefix())) {
						if (!getValue().equals(pValue)) {
							setValue(pValue);
						} else {
							throw new SirixUsageException("Duplicate attribute!");
						}
					}
					moveToParent();
				}

				final long pathNodeKey = mIndexes.contains(EIndexes.PATH) ? getPathNodeKey(
						pQName, Kind.ATTRIBUTE) : 0;
				final byte[] value = getBytes(pValue);
				final long elementKey = getCurrentNode().getNodeKey();
				final AttributeNode node = mNodeFactory.createAttributeNode(elementKey,
						pQName, value, pathNodeKey);

				final Node parentNode = (Node) getPageTransaction()
						.prepareNodeForModification(node.getParentKey(), EPage.NODEPAGE);
				((ElementNode) parentNode).insertAttribute(node.getNodeKey(),
						node.getNameKey());
				getPageTransaction().finishNodeModification(parentNode.getNodeKey(),
						EPage.NODEPAGE);

				mNodeRtx.setCurrentNode(node);
				adaptHashesWithAdd();
				if (pMove == Movement.TOPARENT) {
					moveToParent();
				}
				return this;
			} else {
				throw new SirixUsageException(
						"Insert is not allowed if current node is not an ElementNode!");
			}
		} finally {
			unLock();
		}
	}

	@Override
	public NodeWriteTrx insertNamespace(final @Nonnull QName pQName)
			throws SirixException {
		return insertNamespace(pQName, Movement.NONE);
	}

	@Override
	public NodeWriteTrx insertNamespace(final @Nonnull QName pQName,
			final @Nonnull Movement pMove) throws SirixException {
		if (!XMLToken.isValidQName(checkNotNull(pQName))) {
			throw new IllegalArgumentException("The QName is not valid!");
		}
		acquireLock();
		try {
			if (getCurrentNode().getKind() == Kind.ELEMENT) {
				checkAccessAndCommit();

				for (int i = 0, namespCount = ((ElementNode) getCurrentNode())
						.getNamespaceCount(); i < namespCount; i++) {
					moveToNamespace(i);
					final QName qName = getName();
					if (pQName.getPrefix().equals(qName.getPrefix())) {
						throw new SirixUsageException("Duplicate namespace!");
					}
					moveToParent();
				}

				final long pathNodeKey = mIndexes.contains(EIndexes.PATH) ? getPathNodeKey(
						pQName, Kind.NAMESPACE) : 0;
				final int uriKey = getPageTransaction().createNameKey(
						pQName.getNamespaceURI(), Kind.NAMESPACE);
				final int prefixKey = getPageTransaction().createNameKey(
						pQName.getPrefix(), Kind.NAMESPACE);
				final long elementKey = getCurrentNode().getNodeKey();

				final NamespaceNode node = mNodeFactory.createNamespaceNode(elementKey,
						uriKey, prefixKey, pathNodeKey);

				final Node parentNode = (Node) getPageTransaction()
						.prepareNodeForModification(node.getParentKey(), EPage.NODEPAGE);
				((ElementNode) parentNode).insertNamespace(node.getNodeKey());
				getPageTransaction().finishNodeModification(parentNode.getNodeKey(),
						EPage.NODEPAGE);

				mNodeRtx.setCurrentNode(node);
				adaptHashesWithAdd();
				if (pMove == Movement.TOPARENT) {
					moveToParent();
				}
				return this;
			} else {
				throw new SirixUsageException(
						"Insert is not allowed if current node is not an ElementNode!");
			}
		} finally {
			unLock();
		}
	}

	/**
	 * Check ancestors of current node.
	 * 
	 * @throws IllegalStateException
	 *           if one of the ancestors is the node/subtree rooted at the node to
	 *           move
	 */
	private void checkAncestors(final Node pItem) {
		assert pItem != null;
		final Node item = getCurrentNode();
		while (getCurrentNode().hasParent()) {
			moveToParent();
			if (getCurrentNode().getNodeKey() == pItem.getNodeKey()) {
				throw new IllegalStateException(
						"Moving one of the ancestor nodes is not permitted!");
			}
		}
		moveTo(item.getNodeKey());
	}

	@Override
	public void remove() throws SirixException {
		checkAccessAndCommit();
		acquireLock();
		try {
			if (getCurrentNode().getKind() == Kind.DOCUMENT_ROOT) {
				throw new SirixUsageException("Document root can not be removed.");
			} else if (getCurrentNode() instanceof StructNode) {
				final StructNode node = (StructNode) mNodeRtx.getCurrentNode();

				// Remove subtree.
				for (final Axis axis = new PostOrderAxis(this); axis.hasNext();) {
					axis.next();

					// Remove name.
					removeName();

					// Remove namespaces and attributes.
					removeNonStructural();

					// Then remove node.
					getPageTransaction().removeNode(getCurrentNode().getNodeKey(),
							EPage.NODEPAGE);
				}

				// Adapt hashes and neighbour nodes as well as the name from the
				// NamePage mapping if it's not a text node.
				mNodeRtx.setCurrentNode(node);
				adaptHashesWithRemove();
				adaptForRemove(node, EPage.NODEPAGE);
				mNodeRtx.setCurrentNode(node);

				// Remove the name of subtree-root.
				if (node.getKind() == Kind.ELEMENT) {
					removeName();
				}

				// Set current node (don't remove the moveTo(long) inside the if-clause
				// which is needed because
				// of text merges.
				if (mNodeRtx.hasRightSibling()
						&& moveTo(node.getRightSiblingKey()).hasMoved()) {
				} else if (node.hasLeftSibling()) {
					moveTo(node.getLeftSiblingKey());
				} else {
					moveTo(node.getParentKey());
				}
			} else if (getCurrentNode().getKind() == Kind.ATTRIBUTE) {
				final Node node = mNodeRtx.getCurrentNode();

				final ElementNode parent = (ElementNode) getPageTransaction()
						.prepareNodeForModification(node.getParentKey(), EPage.NODEPAGE);
				parent.removeAttribute(node.getNodeKey());
				getPageTransaction().finishNodeModification(parent.getNodeKey(),
						EPage.NODEPAGE);
				adaptHashesWithRemove();
				getPageTransaction().removeNode(node.getNodeKey(), EPage.NODEPAGE);
				removeName();
				moveToParent();
			} else if (getCurrentNode().getKind() == Kind.NAMESPACE) {
				final Node node = mNodeRtx.getCurrentNode();

				final ElementNode parent = (ElementNode) getPageTransaction()
						.prepareNodeForModification(node.getParentKey(), EPage.NODEPAGE);
				parent.removeNamespace(node.getNodeKey());
				getPageTransaction().finishNodeModification(parent.getNodeKey(),
						EPage.NODEPAGE);
				adaptHashesWithRemove();
				getPageTransaction().removeNode(node.getNodeKey(), EPage.NODEPAGE);
				removeName();
				moveToParent();
			}
		} finally {
			unLock();
		}
	}

	/**
	 * Remove non structural nodes of an {@link ElementNode}, that is namespaces
	 * and attributes.
	 * 
	 * @throws SirixException
	 *           if anything goes wrong
	 */
	private void removeNonStructural() throws SirixException {
		if (mNodeRtx.getKind() == Kind.ELEMENT) {
			for (int i = 0, attCount = mNodeRtx.getAttributeCount(); i < attCount; i++) {
				moveToAttribute(i);
				removeName();
				getPageTransaction().removeNode(getCurrentNode().getNodeKey(),
						EPage.NODEPAGE);
				moveToParent();
			}
			final int nspCount = mNodeRtx.getNamespaceCount();
			for (int i = 0; i < nspCount; i++) {
				moveToNamespace(i);
				removeName();
				getPageTransaction().removeNode(getCurrentNode().getNodeKey(),
						EPage.NODEPAGE);
				moveToParent();
			}
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
		if (getCurrentNode() instanceof NameNode) {
			final NameNode node = ((NameNode) getCurrentNode());
			final Kind nodeKind = node.getKind();
			final NamePage page = ((NamePage) getPageTransaction()
					.getActualRevisionRootPage().getNamePageReference().getPage());
			page.removeName(node.getNameKey(), nodeKind);
			page.removeName(node.getURIKey(), Kind.NAMESPACE);

			assert nodeKind != Kind.DOCUMENT_ROOT;
			if (mIndexes.contains(EIndexes.PATH)
					&& mPathSummary.moveTo(node.getPathNodeKey()).hasMoved()) {
				if (mPathSummary.getReferences() == 1) {
					removePathSummaryNode(ERemove.YES);
				} else {
					assert page.getCount(node.getNameKey(), nodeKind) != 0;
					if (mPathSummary.getReferences() > 1) {
						final PathNode pathNode = (PathNode) getPageTransaction()
								.prepareNodeForModification(mPathSummary.getNodeKey(),
										EPage.PATHSUMMARYPAGE);
						pathNode.decrementReferenceCount();
						getPageTransaction().finishNodeModification(pathNode.getNodeKey(),
								EPage.PATHSUMMARYPAGE);
					}
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
			for (final Axis axis = new DescendantAxis(mPathSummary); axis.hasNext();) {
				axis.next();
				getPageTransaction().removeNode(mPathSummary.getNodeKey(),
						EPage.PATHSUMMARYPAGE);
			}
		}

		// Adapt left sibling node if there is one.
		if (mPathSummary.hasLeftSibling()) {
			final StructNode leftSibling = (StructNode) getPageTransaction()
					.prepareNodeForModification(mPathSummary.getLeftSiblingKey(),
							EPage.PATHSUMMARYPAGE);
			leftSibling.setRightSiblingKey(mPathSummary.getRightSiblingKey());
			getPageTransaction().finishNodeModification(leftSibling.getNodeKey(),
					EPage.PATHSUMMARYPAGE);
		}

		// Adapt right sibling node if there is one.
		if (mPathSummary.hasRightSibling()) {
			final StructNode rightSibling = (StructNode) getPageTransaction()
					.prepareNodeForModification(mPathSummary.getRightSiblingKey(),
							EPage.PATHSUMMARYPAGE);
			rightSibling.setLeftSiblingKey(mPathSummary.getLeftSiblingKey());
			getPageTransaction().finishNodeModification(rightSibling.getNodeKey(),
					EPage.PATHSUMMARYPAGE);
		}

		// Adapt parent. If node has no left sibling it is a first child.
		StructNode parent = (StructNode) getPageTransaction()
				.prepareNodeForModification(mPathSummary.getParentKey(),
						EPage.PATHSUMMARYPAGE);
		if (!mPathSummary.hasLeftSibling()) {
			parent.setFirstChildKey(mPathSummary.getRightSiblingKey());
		}
		parent.decrementChildCount();
		getPageTransaction().finishNodeModification(parent.getNodeKey(),
				EPage.PATHSUMMARYPAGE);

		// Remove node.
		getPageTransaction().removeNode(mPathSummary.getNodeKey(),
				EPage.PATHSUMMARYPAGE);
	}

	@Override
	public void setQName(final @Nonnull QName pQName) throws SirixException {
		checkNotNull(pQName);
		acquireLock();
		try {
			if (getCurrentNode() instanceof NameNode) {
				if (!getName().equals(pQName)) {
					checkAccessAndCommit();

					NameNode node = (NameNode) mNodeRtx.getCurrentNode();
					final long oldHash = node.hashCode();

					// Create new keys for mapping.
					final int nameKey = getPageTransaction().createNameKey(
							Utils.buildName(pQName), node.getKind());
					final int uriKey = getPageTransaction().createNameKey(
							pQName.getNamespaceURI(), Kind.NAMESPACE);

					// Adapt path summary.
					if (mIndexes.contains(EIndexes.PATH)) {
						adaptPathForChangedNode(node, pQName, nameKey, uriKey,
								EOPType.SETNAME);
					}

					// Remove old keys from mapping.
					final Kind nodeKind = node.getKind();
					final int oldNameKey = node.getNameKey();
					final int oldUriKey = node.getURIKey();
					final NamePage page = ((NamePage) getPageTransaction()
							.getActualRevisionRootPage().getNamePageReference().getPage());
					page.removeName(oldNameKey, nodeKind);
					page.removeName(oldUriKey, Kind.NAMESPACE);

					// Set new keys for current node.
					node = (NameNode) getPageTransaction().prepareNodeForModification(
							node.getNodeKey(), EPage.NODEPAGE);
					node.setNameKey(nameKey);
					node.setURIKey(uriKey);
					node.setPathNodeKey(mIndexes.contains(EIndexes.PATH) ? mPathSummary
							.getNodeKey() : 0);
					getPageTransaction().finishNodeModification(node.getNodeKey(),
							EPage.NODEPAGE);

					mNodeRtx.setCurrentNode(node);
					adaptHashedWithUpdate(oldHash);
				}
			} else {
				throw new SirixUsageException(
						"setQName is not allowed if current node is not an INameNode implementation!");
			}
		} finally {
			unLock();
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
	private void adaptPathForChangedNode(final @Nonnull NameNode pNode,
			final @Nonnull QName pQName, final int nameKey, final int uriKey,
			final @Nonnull EOPType pType) throws SirixException {
		// Possibly either reset a path node or decrement its reference counter
		// and search for the new path node or insert it.
		movePathSummary();

		final long oldPathNodeKey = mPathSummary.getNodeKey();

		// Only one path node is referenced (after a setQName(QName) the
		// reference-counter would be 0).
		if (pType == EOPType.SETNAME && mPathSummary.getReferences() == 1) {
			moveSummaryGetLevel(pNode);
			// Search for new path entry.
			final Axis axis = new FilterAxis(new ChildAxis(mPathSummary),
					new NameFilter(mPathSummary, Utils.buildName(pQName)),
					new PathKindFilter(mPathSummary, pNode.getKind()));
			if (axis.hasNext()) {
				axis.next();

				// Found node.
				processFoundPathNode(oldPathNodeKey, mPathSummary.getNodeKey(),
						pNode.getNodeKey(), nameKey, uriKey, ERemove.YES, pType);
			} else {
				if (mPathSummary.getKind() != Kind.DOCUMENT_ROOT) {
					mPathSummary.moveTo(oldPathNodeKey);
					final PathNode pathNode = (PathNode) getPageTransaction()
							.prepareNodeForModification(mPathSummary.getNodeKey(),
									EPage.PATHSUMMARYPAGE);
					pathNode.setNameKey(nameKey);
					pathNode.setURIKey(uriKey);
					getPageTransaction().finishNodeModification(pathNode.getNodeKey(),
							EPage.PATHSUMMARYPAGE);
				}
			}
		} else {
			int level = moveSummaryGetLevel(pNode);
			// Search for new path entry.
			final Axis axis = new FilterAxis(new ChildAxis(mPathSummary),
					new NameFilter(mPathSummary, Utils.buildName(pQName)),
					new PathKindFilter(mPathSummary, pNode.getKind()));
			if (pType == EOPType.MOVEDSAMELEVEL || axis.hasNext()) {
				if (pType != EOPType.MOVEDSAMELEVEL) {
					axis.next();
				}

				// Found node.
				processFoundPathNode(oldPathNodeKey, mPathSummary.getNodeKey(),
						pNode.getNodeKey(), nameKey, uriKey, ERemove.NO, pType);
			} else {
				long nodeKey = mPathSummary.getNodeKey();
				// Decrement reference count or remove path summary node.
				mNodeRtx.moveTo(pNode.getNodeKey());
				final Set<Long> nodesToDelete = new HashSet<>();
				// boolean first = true;
				for (final Axis descendants = new DescendantAxis(mNodeRtx,
						IncludeSelf.YES); descendants.hasNext();) {
					descendants.next();
					deleteOrDecrement(nodesToDelete);
					if (mNodeRtx.getCurrentNode().getKind() == Kind.ELEMENT) {
						final ElementNode element = (ElementNode) mNodeRtx.getCurrentNode();

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
				for (final Axis descendants = new DescendantAxis(this,
						IncludeSelf.YES); descendants.hasNext();) {
					descendants.next();
					if (this.getKind() == Kind.ELEMENT) {
						// Path Summary : New mapping.
						if (firstRun) {
							insertPathAsFirstChild(pQName, Kind.ELEMENT, ++level);
							nodeKey = mPathSummary.getNodeKey();
						} else {
							insertPathAsFirstChild(this.getName(), Kind.ELEMENT, ++level);
						}
						resetPathNodeKey(getCurrentNode().getNodeKey());

						// Namespaces.
						for (int i = 0, nsps = this.getNamespaceCount(); i < nsps; i++) {
							moveToNamespace(i);
							// Path Summary : New mapping.
							insertPathAsFirstChild(this.getName(), Kind.NAMESPACE,
									level + 1);
							resetPathNodeKey(getCurrentNode().getNodeKey());
							moveToParent();
							mPathSummary.moveToParent();
						}

						// Attributes.
						for (int i = 0, atts = this.getAttributeCount(); i < atts; i++) {
							moveToAttribute(i);
							// Path Summary : New mapping.
							insertPathAsFirstChild(this.getName(), Kind.ATTRIBUTE,
									level + 1);
							resetPathNodeKey(getCurrentNode().getNodeKey());
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
		if (mNodeRtx.getCurrentNode() instanceof NameNode) {
			movePathSummary();
			if (mPathSummary.getReferences() == 1) {
				pNodesToDelete.add(mPathSummary.getNodeKey());
			} else {
				final PathNode pathNode = (PathNode) getPageTransaction()
						.prepareNodeForModification(mPathSummary.getNodeKey(),
								EPage.PATHSUMMARYPAGE);
				pathNode.decrementReferenceCount();
				getPageTransaction().finishNodeModification(pathNode.getNodeKey(),
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
		boolean moved = cloned.moveTo(pOldPathNodeKey).hasMoved();
		assert moved;

		// Set new reference count.
		if (pType != EOPType.MOVEDSAMELEVEL) {
			final PathNode currNode = (PathNode) getPageTransaction()
					.prepareNodeForModification(mPathSummary.getNodeKey(),
							EPage.PATHSUMMARYPAGE);
			currNode.setReferenceCount(currNode.getReferences()
					+ cloned.getReferences());
			currNode.setNameKey(pNameKey);
			currNode.setURIKey(pUriKey);
			getPageTransaction().finishNodeModification(currNode.getNodeKey(),
					EPage.PATHSUMMARYPAGE);
		}

		// For all old path nodes.
		mPathSummary.moveToFirstChild();
		final int oldLevel = cloned.getLevel();
		for (final Axis oldDescendants = new DescendantAxis(cloned); oldDescendants
				.hasNext();) {
			oldDescendants.next();

			// Search for new path entry.
			final Axis axis = new FilterAxis(
					new LevelOrderAxis.Builder(mPathSummary)
							.filterLevel(cloned.getLevel() - oldLevel)
							.includeSelf(IncludeSelf.YES).build(), new NameFilter(
							mPathSummary, Utils.buildName(cloned.getName())),
					new PathKindFilter(mPathSummary, cloned.getPathKind()),
					new PathLevelFilter(mPathSummary, cloned.getLevel()));
			if (axis.hasNext()) {
				axis.next();

				// Set new reference count.
				if (pType != EOPType.MOVEDSAMELEVEL) {
					final PathNode currNode = (PathNode) getPageTransaction()
							.prepareNodeForModification(mPathSummary.getNodeKey(),
									EPage.PATHSUMMARYPAGE);
					currNode.setReferenceCount(currNode.getReferences()
							+ cloned.getReferences());
					getPageTransaction().finishNodeModification(currNode.getNodeKey(),
							EPage.PATHSUMMARYPAGE);
				}
			} else {
				// Insert new node.
				insertPathAsFirstChild(cloned.getName(), cloned.getPathKind(),
						mPathSummary.getLevel() + 1);

				// Set new reference count.
				final PathNode currNode = (PathNode) getPageTransaction()
						.prepareNodeForModification(mPathSummary.getNodeKey(),
								EPage.PATHSUMMARYPAGE);
				currNode.setReferenceCount(cloned.getReferences());
				getPageTransaction().finishNodeModification(currNode.getNodeKey(),
						EPage.PATHSUMMARYPAGE);
			}
			mPathSummary.moveTo(pNewPathNodeKey);
		}

		// Set new path nodes.
		// ==========================================================
		mPathSummary.moveTo(pNewPathNodeKey);
		mNodeRtx.moveTo(pOldNodeKey);

		boolean first = true;
		for (final Axis axis = new DescendantAxis(mNodeRtx, IncludeSelf.YES); axis
				.hasNext();) {
			axis.next();

			if (first && pType == EOPType.SETNAME) {
				first = false;
			} else if (mNodeRtx.getCurrentNode() instanceof NameNode) {
				cloned.moveTo(((NameNode) mNodeRtx.getCurrentNode()).getPathNodeKey());
				resetPath(pNewPathNodeKey, cloned.getLevel());

				if (mNodeRtx.getCurrentNode().getKind() == Kind.ELEMENT) {
					final ElementNode element = (ElementNode) mNodeRtx.getCurrentNode();

					for (int i = 0, nspCount = element.getNamespaceCount(); i < nspCount; i++) {
						mNodeRtx.moveToNamespace(i);
						cloned.moveTo(((NameNode) mNodeRtx.getCurrentNode())
								.getPathNodeKey());
						resetPath(pNewPathNodeKey, cloned.getLevel());
						mNodeRtx.moveToParent();
					}
					for (int i = 0, attCount = element.getAttributeCount(); i < attCount; i++) {
						mNodeRtx.moveToAttribute(i);
						cloned.moveTo(((NameNode) mNodeRtx.getCurrentNode())
								.getPathNodeKey());
						resetPath(pNewPathNodeKey, cloned.getLevel());
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
	 * @param pOldLevel
	 *          old level of node
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	private void resetPath(final @Nonnegative long pNewPathNodeKey,
			final @Nonnegative int pOldLevel) throws SirixIOException {
		// Search for new path entry.
		mPathSummary.moveTo(pNewPathNodeKey);
		final Axis filterAxis = new FilterAxis(new LevelOrderAxis.Builder(
				mPathSummary).includeSelf(IncludeSelf.YES).build(), new NameFilter(
				mPathSummary, Utils.buildName(mNodeRtx.getName())),
				new PathKindFilter(mPathSummary, mNodeRtx.getCurrentNode().getKind()),
				new PathLevelFilter(mPathSummary, pOldLevel));
		if (filterAxis.hasNext()) {
			filterAxis.next();

			// Set new path node.
			final NameNode node = (NameNode) getPageTransaction()
					.prepareNodeForModification(mNodeRtx.getCurrentNode().getNodeKey(),
							EPage.NODEPAGE);
			node.setPathNodeKey(mPathSummary.getNodeKey());
			getPageTransaction().finishNodeModification(node.getNodeKey(),
					EPage.NODEPAGE);
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
	private int moveSummaryGetLevel(final @Nonnull Node pNode) {
		assert pNode != null;
		// Get parent path node and level.
		moveToParent();
		int level = 0;
		if (mNodeRtx.getCurrentNode().getKind() == Kind.DOCUMENT_ROOT) {
			mPathSummary.moveToDocumentRoot();
		} else {
			movePathSummary();
			level = mPathSummary.getLevel();
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
		final NameNode currNode = (NameNode) getPageTransaction()
				.prepareNodeForModification(pNodeKey, EPage.NODEPAGE);
		currNode.setPathNodeKey(mPathSummary.getNodeKey());
		getPageTransaction().finishNodeModification(currNode.getNodeKey(),
				EPage.NODEPAGE);
	}

	@Override
	public void setValue(final @Nonnull String pValue) throws SirixException {
		checkNotNull(pValue);
		acquireLock();
		try {
			if (getCurrentNode() instanceof ValNode) {
				checkAccessAndCommit();
				final long oldHash = mNodeRtx.getCurrentNode().hashCode();
				final byte[] byteVal = getBytes(pValue);

				final ValNode node = (ValNode) getPageTransaction()
						.prepareNodeForModification(mNodeRtx.getCurrentNode().getNodeKey(),
								EPage.NODEPAGE);
				node.setValue(byteVal);
				getPageTransaction().finishNodeModification(node.getNodeKey(),
						EPage.NODEPAGE);

				mNodeRtx.setCurrentNode(node);
				adaptHashedWithUpdate(oldHash);

				// Index new value.
				indexText(byteVal);
			} else {
				throw new SirixUsageException(
						"setValue(String) is not allowed if current node is not an IValNode implementation!");
			}
		} finally {
			unLock();
		}
	}

	@Override
	public void revertTo(final @Nonnegative int pRevision) throws SirixException {
		acquireLock();
		try {
			mNodeRtx.assertNotClosed();
			mNodeRtx.mSession.assertAccess(pRevision);

			// Close current page transaction.
			final long trxID = getTransactionID();
			final int revNumber = getRevisionNumber();

			// Reset internal transaction state to new uber page.
			mNodeRtx.mSession.closeNodePageWriteTransaction(getTransactionID());
			final PageWriteTrx trx = mNodeRtx.mSession.createPageWriteTransaction(
					trxID, pRevision, revNumber - 1);
			mNodeRtx.setPageReadTransaction(null);
			mNodeRtx.setPageReadTransaction(trx);
			mNodeRtx.mSession.setNodePageWriteTransaction(getTransactionID(), trx);

			// Reset node factory.
			mNodeFactory = null;
			mNodeFactory = new NodeFactoryImpl(
					(PageWriteTrx) mNodeRtx.getPageTransaction());

			// New index instances.
			reInstantiateIndexes();

			// Reset modification counter.
			mModificationCount = 0L;

			// Move to document root.
			moveToDocumentRoot();
		} finally {
			unLock();
		}
	}

	@Override
	public void close() throws SirixException {
		acquireLock();
		try {
			if (!isClosed()) {
				// Make sure to commit all dirty data.
				if (mModificationCount > 0) {
					throw new SirixUsageException("Must commit/abort transaction first");
				}
				// Release all state immediately.
				mNodeRtx.mSession.closeWriteTransaction(getTransactionID());
				mNodeRtx.close();

				mPathSummary = null;
				mAVLTree = null;
				mNodeFactory = null;

				// Shutdown pool.
				mPool.shutdown();
				try {
					mPool.awaitTermination(100, TimeUnit.SECONDS);
				} catch (final InterruptedException e) {
					throw new SirixThreadedException(e);
				}
			}
		} finally {
			unLock();
		}
	}

	@Override
	public void abort() throws SirixException {
		acquireLock();
		try {
			mNodeRtx.assertNotClosed();

			// Reset modification counter.
			mModificationCount = 0L;

			// Close current page transaction.
			final long trxID = getTransactionID();
			final int revNumber = getPageTransaction().getUberPage().isBootstrap() ? 0
					: getRevisionNumber() - 1;

			mNodeRtx.getPageTransaction().clearCaches();
			mNodeRtx.mSession.closeNodePageWriteTransaction(getTransactionID());
			final PageWriteTrx trx = mNodeRtx.mSession.createPageWriteTransaction(
					trxID, revNumber, revNumber);
			mNodeRtx.setPageReadTransaction(null);
			mNodeRtx.setPageReadTransaction(trx);
			mNodeRtx.mSession.setNodePageWriteTransaction(getTransactionID(), trx);

			mNodeFactory = null;
			mNodeFactory = new NodeFactoryImpl(
					(PageWriteTrx) mNodeRtx.getPageTransaction());

			reInstantiateIndexes();
		} finally {
			unLock();
		}
	}

	@Override
	public void commit() throws SirixException {
		mNodeRtx.assertNotClosed();

		// // Assert that the DocumentNode has no more than one child node (the root
		// // node).
		// final long nodeKey = mNodeRtx.getCurrentNode().getNodeKey();
		// moveToDocumentRoot();
		// final DocumentRootNode document = (DocumentRootNode)
		// mNodeRtx.getCurrentNode();
		// if (document.getChildCount() > 1) {
		// moveTo(nodeKey);
		// throw new IllegalStateException(
		// "DocumentRootNode may not have more than one child node!");
		// }
		// moveTo(nodeKey);

		final File commitFile = mNodeRtx.mSession.mCommitFile;
		try {
			commitFile.createNewFile();
		} catch (final IOException e) {
			throw new SirixIOException(e.getCause());
		}

		// Execute pre-commit hooks.
		for (final PreCommitHook hook : mPreCommitHooks) {
			hook.preCommit(this);
		}

		// Reset modification counter.
		mModificationCount = 0L;

		final NodeWriteTrx trx = this;
		// mPool.submit(new Callable<Void>() {
		// @Override
		// public Void call() throws SirixException {
		// Commit uber page.

		// final Thread checkpointer = new Thread(new Runnable() {
		// @Override
		// public void run() {
		// try {
		final UberPage currUberPage = getPageTransaction().getUberPage();
		if (currUberPage.isBootstrap()) {
			currUberPage.setIsBulkInserted(mBulkInsert);
		}
		final UberPage uberPage = getPageTransaction().commit(MultipleWriteTrx.NO);

		// Optionally lock while assigning new instances.
		acquireLock();
		try {
			// Remember succesfully committed uber page in session.
			mNodeRtx.mSession.setLastCommittedUberPage(uberPage);

			final long trxID = getTransactionID();
			final int revNumber = getRevisionNumber();

			reInstantiate(trxID, revNumber);
		} finally {
			unLock();
		}

		// Execute post-commit hooks.
		for (final PostCommitHook hook : mPostCommitHooks) {
			hook.postCommit(trx);
		}

		// Delete commit file which denotes that a commit must write the log in
		// the data file.
		try {
			Files.delete(commitFile.toPath());
		} catch (final IOException e) {
			throw new SirixIOException(e.getCause());
		}
		// } catch (final SirixException e) {
		// e.printStackTrace();
		// }
		// }
		// });
		// checkpointer.setDaemon(true);
		// checkpointer.start();
		// return null;
		// }
		// });
	}

	/**
	 * Create new instances.
	 * 
	 * @param trxID
	 *          transaction ID
	 * @param revNumber
	 *          revision number
	 * @throws SirixException
	 *           if an I/O exception occurs
	 */
	private void reInstantiate(final @Nonnegative long trxID,
			final @Nonnegative int revNumber) throws SirixException {
		// Reset page transaction to new uber page.
		mNodeRtx.mSession.closeNodePageWriteTransaction(getTransactionID());
		final PageWriteTrx trx = mNodeRtx.mSession.createPageWriteTransaction(
				trxID, revNumber, revNumber);
		mNodeRtx.setPageReadTransaction(null);
		mNodeRtx.setPageReadTransaction(trx);
		mNodeRtx.mSession.setNodePageWriteTransaction(getTransactionID(), trx);

		mNodeFactory = null;
		mNodeFactory = new NodeFactoryImpl(
				(PageWriteTrx) mNodeRtx.getPageTransaction());

		reInstantiateIndexes();
	}

	/**
	 * Create new instances for indexes.
	 * 
	 * @param trxID
	 *          transaction ID
	 * @param revNumber
	 *          revision number
	 */
	private void reInstantiateIndexes() {
		// Get a new path summary instance.
		if (mIndexes.contains(EIndexes.PATH)) {
			mPathSummary = null;
			mPathSummary = PathSummary.getInstance(mNodeRtx.getPageTransaction(),
					mNodeRtx.getSession());
		}

		// Get a new avl tree instance.
		if (mIndexes.contains(EIndexes.VALUE)) {
			mAVLTree = null;
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
		for (final Axis axis = new PostOrderAxis(this, IncludeSelf.YES); axis
				.hasNext();) {
			axis.next();
			final StructNode node = mNodeRtx.getStructuralNode();
			if (node.getKind() == Kind.ELEMENT) {
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
	private void addParentHash(final @Nonnull Node pStartNode)
			throws SirixIOException {
		switch (mHashKind) {
		case Rolling:
			long hashToAdd = mHash.hashLong(pStartNode.hashCode()).asLong();
			Node node = (Node) getPageTransaction().prepareNodeForModification(
					mNodeRtx.getCurrentNode().getNodeKey(), EPage.NODEPAGE);
			node.setHash(node.getHash() + hashToAdd * PRIME);
			if (pStartNode instanceof StructNode) {
				((StructNode) node).setDescendantCount(((StructNode) node)
						.getDescendantCount()
						+ ((StructNode) pStartNode).getDescendantCount() + 1);
			}
			getPageTransaction().finishNodeModification(node.getNodeKey(),
					EPage.NODEPAGE);
			break;
		case Postorder:
			break;
		default:
		}
	}

	/** Add a hash and the descendant count. */
	private void addHashAndDescendantCount() throws SirixIOException {
		switch (mHashKind) {
		case Rolling:
			// Setup.
			final Node startNode = getCurrentNode();
			final long oldDescendantCount = mNodeRtx.getStructuralNode()
					.getDescendantCount();
			final long descendantCount = oldDescendantCount == 0 ? 1
					: oldDescendantCount + 1;

			// Set start node.
			long hashToAdd = mHash.hashLong(startNode.hashCode()).asLong();
			Node node = (Node) getPageTransaction().prepareNodeForModification(
					mNodeRtx.getCurrentNode().getNodeKey(), EPage.NODEPAGE);
			node.setHash(hashToAdd);
			getPageTransaction().finishNodeModification(node.getNodeKey(),
					EPage.NODEPAGE);

			// Set parent node.
			if (startNode.hasParent()) {
				moveToParent();
				node = (Node) getPageTransaction().prepareNodeForModification(
						mNodeRtx.getCurrentNode().getNodeKey(), EPage.NODEPAGE);
				node.setHash(node.getHash() + hashToAdd * PRIME);
				setAddDescendants(startNode, node, descendantCount);
				getPageTransaction().finishNodeModification(node.getNodeKey(),
						EPage.NODEPAGE);
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
	private void adaptForInsert(final @Nonnull Node pNewNode,
			final @Nonnull InsertPos pInsert, final @Nonnull EPage pPage)
			throws SirixIOException {
		assert pNewNode != null;
		assert pInsert != null;
		assert pPage != null;

		if (pNewNode instanceof StructNode) {
			final StructNode strucNode = (StructNode) pNewNode;
			final StructNode parent = (StructNode) getPageTransaction()
					.prepareNodeForModification(pNewNode.getParentKey(), pPage);
			parent.incrementChildCount();
			if (pInsert == InsertPos.ASFIRSTCHILD) {
				parent.setFirstChildKey(pNewNode.getNodeKey());
			}
			getPageTransaction().finishNodeModification(parent.getNodeKey(), pPage);

			if (strucNode.hasRightSibling()) {
				final StructNode rightSiblingNode = (StructNode) getPageTransaction()
						.prepareNodeForModification(strucNode.getRightSiblingKey(), pPage);
				rightSiblingNode.setLeftSiblingKey(pNewNode.getNodeKey());
				getPageTransaction().finishNodeModification(
						rightSiblingNode.getNodeKey(), pPage);
			}
			if (strucNode.hasLeftSibling()) {
				final StructNode leftSiblingNode = (StructNode) getPageTransaction()
						.prepareNodeForModification(strucNode.getLeftSiblingKey(), pPage);
				leftSiblingNode.setRightSiblingKey(pNewNode.getNodeKey());
				getPageTransaction().finishNodeModification(
						leftSiblingNode.getNodeKey(), pPage);
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
	private void adaptForRemove(final @Nonnull StructNode pOldNode,
			final @Nonnull EPage pPage) throws SirixException {
		assert pOldNode != null;

		// Concatenate neighbor text nodes if they exist (the right sibling is
		// deleted afterwards).
		boolean concatenated = false;
		if (pOldNode.hasLeftSibling() && pOldNode.hasRightSibling()
				&& moveTo(pOldNode.getRightSiblingKey()).hasMoved()
				&& getCurrentNode().getKind() == Kind.TEXT
				&& moveTo(pOldNode.getLeftSiblingKey()).hasMoved()
				&& getCurrentNode().getKind() == Kind.TEXT) {
			final StringBuilder builder = new StringBuilder(getValue());
			moveTo(pOldNode.getRightSiblingKey());
			builder.append(getValue());
			moveTo(pOldNode.getLeftSiblingKey());
			setValue(builder.toString());
			concatenated = true;
		}

		// Adapt left sibling node if there is one.
		if (pOldNode.hasLeftSibling()) {
			final StructNode leftSibling = (StructNode) getPageTransaction()
					.prepareNodeForModification(pOldNode.getLeftSiblingKey(), pPage);
			if (concatenated) {
				moveTo(pOldNode.getRightSiblingKey());
				leftSibling.setRightSiblingKey(((StructNode) getCurrentNode())
						.getRightSiblingKey());
			} else {
				leftSibling.setRightSiblingKey(pOldNode.getRightSiblingKey());
			}
			getPageTransaction().finishNodeModification(leftSibling.getNodeKey(),
					pPage);
		}

		// Adapt right sibling node if there is one.
		if (pOldNode.hasRightSibling()) {
			StructNode rightSibling;
			if (concatenated) {
				moveTo(pOldNode.getRightSiblingKey());
				moveTo(mNodeRtx.getStructuralNode().getRightSiblingKey());
				rightSibling = (StructNode) getPageTransaction()
						.prepareNodeForModification(mNodeRtx.getCurrentNode().getNodeKey(),
								pPage);
				rightSibling.setLeftSiblingKey(pOldNode.getLeftSiblingKey());
			} else {
				rightSibling = (StructNode) getPageTransaction()
						.prepareNodeForModification(pOldNode.getRightSiblingKey(), pPage);
				rightSibling.setLeftSiblingKey(pOldNode.getLeftSiblingKey());
			}
			getPageTransaction().finishNodeModification(rightSibling.getNodeKey(),
					pPage);
		}

		// Adapt parent, if node has now left sibling it is a first child.
		StructNode parent = (StructNode) getPageTransaction()
				.prepareNodeForModification(pOldNode.getParentKey(), pPage);
		if (!pOldNode.hasLeftSibling()) {
			parent.setFirstChildKey(pOldNode.getRightSiblingKey());
		}
		parent.decrementChildCount();
		if (concatenated) {
			parent.decrementDescendantCount();
			parent.decrementChildCount();
		}
		getPageTransaction().finishNodeModification(parent.getNodeKey(), pPage);
		if (concatenated) {
			// Adjust descendant count.
			moveTo(parent.getNodeKey());
			while (parent.hasParent()) {
				moveToParent();
				final StructNode ancestor = (StructNode) getPageTransaction()
						.prepareNodeForModification(mNodeRtx.getCurrentNode().getNodeKey(),
								pPage);
				ancestor.decrementDescendantCount();
				getPageTransaction().finishNodeModification(ancestor.getNodeKey(),
						pPage);
				parent = ancestor;
			}
		}

		// Remove right sibling text node if text nodes have been
		// concatenated/merged.
		if (concatenated) {
			moveTo(pOldNode.getRightSiblingKey());
			getPageTransaction().removeNode(mNodeRtx.getNodeKey(), pPage);
		}

		// Remove non structural nodes of old node.
		if (pOldNode.getKind() == Kind.ELEMENT) {
			moveTo(pOldNode.getNodeKey());
			removeNonStructural();
		}

		// Remove old node.
		moveTo(pOldNode.getNodeKey());
		getPageTransaction().removeNode(pOldNode.getNodeKey(), pPage);
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
	 * @return the page transaction
	 */
	public PageWriteTrx getPageTransaction() {
		return (PageWriteTrx) mNodeRtx.getPageTransaction();
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
		moveTo(getCurrentNode().getParentKey());
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
		final Node startNode = getCurrentNode();
		// long for adapting the hash of the parent
		long hashCodeForParent = 0;
		// adapting the parent if the current node is no structural one.
		if (!(startNode instanceof StructNode)) {
			final Node node = (Node) getPageTransaction()
					.prepareNodeForModification(mNodeRtx.getCurrentNode().getNodeKey(),
							EPage.NODEPAGE);
			node.setHash(mHash.hashLong(mNodeRtx.getCurrentNode().hashCode())
					.asLong());
			getPageTransaction().finishNodeModification(node.getNodeKey(),
					EPage.NODEPAGE);
			moveTo(mNodeRtx.getCurrentNode().getParentKey());
		}
		// Cursor to root
		StructNode cursorToRoot;
		do {
			cursorToRoot = (StructNode) getPageTransaction()
					.prepareNodeForModification(mNodeRtx.getCurrentNode().getNodeKey(),
							EPage.NODEPAGE);
			hashCodeForParent = mNodeRtx.getCurrentNode().hashCode()
					+ hashCodeForParent * PRIME;
			// Caring about attributes and namespaces if node is an element.
			if (cursorToRoot.getKind() == Kind.ELEMENT) {
				final ElementNode currentElement = (ElementNode) cursorToRoot;
				// setting the attributes and namespaces
				final int attCount = ((ElementNode) cursorToRoot).getAttributeCount();
				for (int i = 0; i < attCount; i++) {
					moveTo(currentElement.getAttributeKey(i));
					hashCodeForParent = mNodeRtx.getCurrentNode().hashCode()
							+ hashCodeForParent * PRIME;
				}
				final int nspCount = ((ElementNode) cursorToRoot).getNamespaceCount();
				for (int i = 0; i < nspCount; i++) {
					moveTo(currentElement.getNamespaceKey(i));
					hashCodeForParent = mNodeRtx.getCurrentNode().hashCode()
							+ hashCodeForParent * PRIME;
				}
				moveTo(cursorToRoot.getNodeKey());
			}

			// Caring about the children of a node
			if (moveTo(mNodeRtx.getStructuralNode().getFirstChildKey()).hasMoved()) {
				do {
					hashCodeForParent = mNodeRtx.getCurrentNode().getHash()
							+ hashCodeForParent * PRIME;
				} while (moveTo(mNodeRtx.getStructuralNode().getRightSiblingKey())
						.hasMoved());
				moveTo(mNodeRtx.getStructuralNode().getParentKey());
			}

			// setting hash and resetting hash
			cursorToRoot.setHash(hashCodeForParent);
			getPageTransaction().finishNodeModification(cursorToRoot.getNodeKey(),
					EPage.NODEPAGE);
			hashCodeForParent = 0;
		} while (moveTo(cursorToRoot.getParentKey()).hasMoved());

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
		final Node newNode = getCurrentNode();
		final long hash = newNode.hashCode();
		final long newNodeHash = hash;
		long resultNew = hash;

		// go the path to the root
		do {
			final Node node = (Node) getPageTransaction()
					.prepareNodeForModification(mNodeRtx.getCurrentNode().getNodeKey(),
							EPage.NODEPAGE);
			if (node.getNodeKey() == newNode.getNodeKey()) {
				resultNew = node.getHash() - pOldHash;
				resultNew = resultNew + newNodeHash;
			} else {
				resultNew = node.getHash() - pOldHash * PRIME;
				resultNew = resultNew + newNodeHash * PRIME;
			}
			node.setHash(resultNew);
			getPageTransaction().finishNodeModification(node.getNodeKey(),
					EPage.NODEPAGE);
		} while (moveTo(mNodeRtx.getCurrentNode().getParentKey()).hasMoved());

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
		final Node startNode = getCurrentNode();
		long hashToRemove = startNode.getHash();
		long hashToAdd = 0;
		long newHash = 0;
		// go the path to the root
		do {
			final Node node = (Node) getPageTransaction()
					.prepareNodeForModification(mNodeRtx.getCurrentNode().getNodeKey(),
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
			getPageTransaction().finishNodeModification(node.getNodeKey(),
					EPage.NODEPAGE);
		} while (moveTo(mNodeRtx.getCurrentNode().getParentKey()).hasMoved());

		mNodeRtx.setCurrentNode(startNode);
	}

	/**
	 * Set new descendant count of ancestor after a remove-operation.
	 * 
	 * @param pStartNode
	 *          the node which has been removed
	 */
	private void setRemoveDescendants(final @Nonnull Node pStartNode) {
		assert pStartNode != null;
		if (pStartNode instanceof StructNode) {
			final StructNode node = ((StructNode) getCurrentNode());
			node.setDescendantCount(node.getDescendantCount()
					- ((StructNode) pStartNode).getDescendantCount() - 1);
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
		final Node startNode = mNodeRtx.getCurrentNode();
		final long oldDescendantCount = mNodeRtx.getStructuralNode()
				.getDescendantCount();
		final long descendantCount = oldDescendantCount == 0 ? 1
				: oldDescendantCount + 1;
		long hashToAdd = startNode.getHash() == 0 ? mHash.hashLong(
				startNode.hashCode()).asLong() : startNode.getHash();
		long newHash = 0;
		long possibleOldHash = 0;
		// go the path to the root
		do {
			final Node node = (Node) getPageTransaction()
					.prepareNodeForModification(mNodeRtx.getCurrentNode().getNodeKey(),
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
			getPageTransaction().finishNodeModification(node.getNodeKey(),
					EPage.NODEPAGE);
		} while (moveTo(mNodeRtx.getCurrentNode().getParentKey()).hasMoved());
		mNodeRtx.setCurrentNode(startNode);
	}

	/**
	 * Set new descendant count of ancestor after an add-operation.
	 * 
	 * @param pStartNode
	 *          the node which has been removed
	 */
	private void setAddDescendants(final @Nonnull Node pStartNode,
			final @Nonnull Node pNodeToModifiy,
			final @Nonnegative long pDescendantCount) {
		assert pStartNode != null;
		assert pDescendantCount >= 0;
		assert pNodeToModifiy != null;
		if (pStartNode instanceof StructNode) {
			final StructNode node = (StructNode) pNodeToModifiy;
			final long oldDescendantCount = node.getDescendantCount();
			node.setDescendantCount(oldDescendantCount + pDescendantCount);
		}
	}

	@Override
	public NodeWriteTrx copySubtreeAsFirstChild(final @Nonnull NodeReadTrx pRtx)
			throws SirixException {
		checkNotNull(pRtx);
		acquireLock();
		try {
			checkAccessAndCommit();
			final long nodeKey = getCurrentNode().getNodeKey();
			copy(pRtx, Insert.ASFIRSTCHILD);
			moveTo(nodeKey);
			moveToFirstChild();
		} finally {
			unLock();
		}
		return this;
	}

	@Override
	public NodeWriteTrx copySubtreeAsLeftSibling(final @Nonnull NodeReadTrx pRtx)
			throws SirixException {
		checkNotNull(pRtx);
		acquireLock();
		try {
			checkAccessAndCommit();
			final long nodeKey = getCurrentNode().getNodeKey();
			copy(pRtx, Insert.ASLEFTSIBLING);
			moveTo(nodeKey);
			moveToFirstChild();
		} finally {
			unLock();
		}
		return this;
	}

	@Override
	public NodeWriteTrx copySubtreeAsRightSibling(
			final @Nonnull NodeReadTrx pRtx) throws SirixException {
		checkNotNull(pRtx);
		acquireLock();
		try {
			checkAccessAndCommit();
			final long nodeKey = getCurrentNode().getNodeKey();
			copy(pRtx, Insert.ASRIGHTSIBLING);
			moveTo(nodeKey);
			moveToRightSibling();
		} finally {
			unLock();
		}
		return this;
	}

	/**
	 * Helper method for copy-operations.
	 * 
	 * @param pRtx
	 *          the source {@link NodeReadTrx}
	 * @param pInsert
	 *          the insertion strategy
	 * @throws SirixException
	 *           if anything fails in sirix
	 */
	private void copy(final @Nonnull NodeReadTrx pRtx,
			final @Nonnull Insert pInsert) throws SirixException {
		assert pRtx != null;
		assert pInsert != null;
		final NodeReadTrx rtx = pRtx.getSession().beginNodeReadTrx(
				pRtx.getRevisionNumber());
		assert rtx.getRevisionNumber() == pRtx.getRevisionNumber();
		rtx.moveTo(pRtx.getNodeKey());
		assert rtx.getNodeKey() == pRtx.getNodeKey();
		if (rtx.getKind() == Kind.DOCUMENT_ROOT) {
			rtx.moveToFirstChild();
		}
		if (!(rtx.isStructuralNode())) {
			throw new IllegalStateException(
					"Node to insert must be a structural node (Text, PI, Comment, Document root or Element)!");
		}

		if (rtx.getKind() == Kind.TEXT) {
			final String value = rtx.getValue();
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
			new XMLShredder.Builder(this, reader, pInsert).build().call();
		}
		rtx.close();
	}

	@Override
	public NodeWriteTrx replaceNode(final @Nonnull String pXML)
			throws SirixException, IOException, XMLStreamException {
		checkNotNull(pXML);
		acquireLock();
		try {
			checkAccessAndCommit();
			final XMLEventReader reader = XMLShredder
					.createStringReader(checkNotNull(pXML));
			Node insertedRootNode = null;
			if (getCurrentNode() instanceof StructNode) {
				final StructNode currentNode = mNodeRtx.getStructuralNode();

				if (pXML.startsWith("<")) {
					while (reader.hasNext()) {
						XMLEvent event = reader.peek();

						if (event.isStartDocument()) {
							reader.nextEvent();
							continue;
						}

						switch (event.getEventType()) {
						case XMLStreamConstants.START_ELEMENT:
							Insert pos = Insert.ASFIRSTCHILD;
							if (currentNode.hasLeftSibling()) {
								moveToLeftSibling();
								pos = Insert.ASRIGHTSIBLING;
							} else {
								moveToParent();
							}

							final XMLShredder shredder = new XMLShredder.Builder(this,
									reader, pos).build();
							shredder.call();
							if (reader.hasNext()) {
								reader.nextEvent(); // End document.
							}

							insertedRootNode = mNodeRtx.getCurrentNode();
							moveTo(currentNode.getNodeKey());
							remove();
							moveTo(insertedRootNode.getNodeKey());
							break;
						}
					}
				} else {
					insertedRootNode = replaceWithTextNode(pXML);
				}

				if (insertedRootNode != null) {
					moveTo(insertedRootNode.getNodeKey());
				}
			} else {

			}

		} finally {
			unLock();
		}
		return this;
	}

	@Override
	public NodeWriteTrx replaceNode(final @Nonnull NodeReadTrx pRtx)
			throws SirixException {
		checkNotNull(pRtx);
		acquireLock();
		try {
			switch (pRtx.getKind()) {
			case ELEMENT:
			case TEXT:
				checkCurrentNode();
				replace(pRtx);
				break;
			case ATTRIBUTE:
				if (getCurrentNode().getKind() != Kind.ATTRIBUTE) {
					throw new IllegalStateException(
							"Current node must be an attribute node!");
				}
				insertAttribute(pRtx.getName(), pRtx.getValue());
				break;
			case NAMESPACE:
				if (mNodeRtx.getCurrentNode().getClass() != NamespaceNode.class) {
					throw new IllegalStateException(
							"Current node must be a namespace node!");
				}
				insertNamespace(pRtx.getName());
				break;
			default:
				throw new UnsupportedOperationException("Node type not supported!");
			}
		} finally {
			unLock();
		}
		return this;
	}

	/**
	 * Check current node type (must be a structural node).
	 */
	private void checkCurrentNode() {
		if (!(getCurrentNode() instanceof StructNode)) {
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
	private Node replaceWithTextNode(final @Nonnull String pValue)
			throws SirixException {
		assert pValue != null;
		final StructNode currentNode = mNodeRtx.getStructuralNode();
		long key = currentNode.getNodeKey();
		if (currentNode.hasLeftSibling()) {
			moveToLeftSibling();
			key = insertTextAsRightSibling(pValue).getNodeKey();
		} else {
			moveToParent();
			key = insertTextAsFirstChild(pValue).getNodeKey();
			moveTo(key);
		}

		moveTo(currentNode.getNodeKey());
		remove();
		moveTo(key);
		return mNodeRtx.getCurrentNode();
	}

	/**
	 * Replace a node.
	 * 
	 * @param pRtx
	 *          the transaction which is located at the node to replace
	 * @return
	 * @throws SirixException
	 */
	private Node replace(final @Nonnull NodeReadTrx pRtx) throws SirixException {
		assert pRtx != null;
		final StructNode currentNode = mNodeRtx.getStructuralNode();
		long key = currentNode.getNodeKey();
		if (currentNode.hasLeftSibling()) {
			moveToLeftSibling();
			key = copySubtreeAsRightSibling(pRtx).getNodeKey();
		} else {
			moveToParent();
			key = copySubtreeAsFirstChild(pRtx).getNodeKey();
			moveTo(key);
		}

		removeReplaced(currentNode, key);
		return mNodeRtx.getCurrentNode();
	}

	/**
	 * Get the current node.
	 * 
	 * @return {@link Node} implementation
	 */
	private Node getCurrentNode() {
		return mNodeRtx.getCurrentNode();
	}

	/**
	 * 
	 * @param pNode
	 * @param pKey
	 * @throws SirixException
	 */
	private void removeReplaced(final @Nonnull StructNode pNode,
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
	protected NodeReadTrx delegate() {
		return mNodeRtx;
	}

	@Override
	public void addPreCommitHook(final @Nonnull PreCommitHook pHook) {
		acquireLock();
		try {
			mPreCommitHooks.add(checkNotNull(pHook));
		} finally {
			unLock();
		}
	}

	@Override
	public void addPostCommitHook(final @Nonnull PostCommitHook pHook) {
		acquireLock();
		try {
			mPostCommitHooks.add(checkNotNull(pHook));
		} finally {
			unLock();
		}
	}

	@Override
	public boolean equals(final @Nullable Object pObj) {
		if (pObj instanceof NodeWriteTrxImpl) {
			final NodeWriteTrxImpl wtx = (NodeWriteTrxImpl) pObj;
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
		acquireLock();
		try {
			return mPathSummary;
		} finally {
			unLock();
		}
	}

	@Override
	public AVLTree<TextValue, TextReferences> getValueIndex() {
		acquireLock();
		try {
			return mAVLTree;
		} finally {
			unLock();
		}
	}
}
