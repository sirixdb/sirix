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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnegative;
import javax.annotation.Nullable;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;

import org.brackit.xquery.atomic.QNm;
import org.sirix.access.IndexController.ChangeType;
import org.sirix.access.SessionImpl.Abort;
import org.sirix.api.Axis;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.api.PostCommitHook;
import org.sirix.api.PreCommitHook;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.axis.LevelOrderAxis;
import org.sirix.axis.PostOrderAxis;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixThreadedException;
import org.sirix.exception.SirixUsageException;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.index.path.summary.PathSummaryWriter;
import org.sirix.index.path.summary.PathSummaryWriter.OPType;
import org.sirix.node.AttributeNode;
import org.sirix.node.CommentNode;
import org.sirix.node.ElementNode;
import org.sirix.node.Kind;
import org.sirix.node.NamespaceNode;
import org.sirix.node.PINode;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.TextNode;
import org.sirix.node.immutable.ImmutableAttribute;
import org.sirix.node.immutable.ImmutableNamespace;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.page.NamePage;
import org.sirix.page.PageKind;
import org.sirix.page.UberPage;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.service.xml.serialize.StAXSerializer;
import org.sirix.service.xml.shredder.Insert;
import org.sirix.service.xml.shredder.XMLShredder;
import org.sirix.settings.Constants;
import org.sirix.settings.Fixed;
import org.sirix.utils.XMLToken;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * <h1>NodeWriteTrxImpl</h1>
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
final class NodeWriteTrxImpl extends AbstractForwardingNodeReadTrx implements
		NodeWriteTrx {

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

	/** {@link PathSummaryWriter} instance. */
	private PathSummaryWriter mPathSummaryWriter;

	/** Determines if a path summary should be built and kept up-to-date or not. */
	private final boolean mBuildPathSummary;

	/** {@link NodeFactoryImpl} to be able to create nodes. */
	private NodeFactoryImpl mNodeFactory;

	/** An optional lock for all methods, if an automatic commit is issued. */
	private final Optional<Semaphore> mLock;

	/** Determines if dewey IDs should be stored or not. */
	private final boolean mDeweyIDsStored;

	/** Determines if text values should be compressed or not. */
	private final boolean mCompression;

	/** The {@link IndexController} used within the session this {@link NodeWriteTrx} is bound to. */
	private final IndexController mIndexController;

	/**
	 * Constructor.
	 * 
	 * @param transactionID
	 *          ID of transaction
	 * @param session
	 *          the {@link session} instance this transaction is bound to
	 * @param pageWriteTrx
	 *          {@link PageWriteTrx} to interact with the page layer
	 * @param maxNodeCount
	 *          maximum number of node modifications before auto commit
	 * @param timeUnit
	 *          unit of the number of the next param {@code pMaxTime}
	 * @param maxTime
	 *          maximum number of seconds before auto commit
	 * @throws SirixIOException
	 *           if the reading of the props is failing
	 * @throws SirixUsageException
	 *           if {@code pMaxNodeCount < 0} or {@code pMaxTime < 0}
	 */
	NodeWriteTrxImpl(
			final @Nonnegative long transactionID,
			final SessionImpl session,
			final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
			final @Nonnegative int maxNodeCount, final TimeUnit timeUnit,
			final @Nonnegative int maxTime) throws SirixIOException,
			SirixUsageException {

		// Do not accept negative values.
		if ((maxNodeCount < 0) || (maxTime < 0)) {
			throw new SirixUsageException(
					"Negative arguments for maxNodeCount and maxTime are not accepted.");
		}

		mNodeRtx = new NodeReadTrxImpl(session, transactionID, pageWriteTrx);
		mIndexController = session.getIndexController();
		mBuildPathSummary = session.mResourceConfig.mPathSummary;

		// Only auto commit by node modifications if it is more then 0.
		mMaxNodeCount = maxNodeCount;
		mModificationCount = 0L;

		// Node factory.
		mNodeFactory = new NodeFactoryImpl(pageWriteTrx);

		// Path summary.
		if (mBuildPathSummary) {
			mPathSummaryWriter = PathSummaryWriter.getInstance(pageWriteTrx, session,
					mNodeFactory, mNodeRtx);
		}

		if (maxTime > 0) {
			mPool.scheduleAtFixedRate(new Runnable() {
				@Override
				public void run() {
					try {
						commit();
					} catch (final SirixException e) {
						throw new IllegalStateException(e);
					}
				}
			}, maxTime, maxTime, timeUnit);
		}

		mHashKind = session.mResourceConfig.mHashKind;

		// Synchronize commit and other public methods if needed.
		if (maxTime > 0) {
			mLock = Optional.of(new Semaphore(1));
		} else {
			mLock = Optional.absent();
		}

		mDeweyIDsStored = mNodeRtx.mSession.mResourceConfig.mDeweyIDsStored;
		mCompression = mNodeRtx.mSession.mResourceConfig.mCompression;

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
	public NodeWriteTrx moveSubtreeToFirstChild(final @Nonnegative long fromKey)
			throws SirixException, IllegalArgumentException {
		acquireLock();
		try {
			if (fromKey < 0 || fromKey > getMaxNodeKey()) {
				throw new IllegalArgumentException("Argument must be a valid node key!");
			}
			if (fromKey == getCurrentNode().getNodeKey()) {
				throw new IllegalArgumentException(
						"Can't move itself to right sibling of itself!");
			}

			@SuppressWarnings("unchecked")
			final Optional<? extends Node> node = (Optional<? extends Node>) getPageTransaction()
					.getRecord(fromKey, PageKind.RECORDPAGE, -1);
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

				// Check that it's not already the first child.
				if (nodeAnchor.getFirstChildKey() != nodeToMove.getNodeKey()) {
					final StructNode toMove = (StructNode) nodeToMove;

					// Adapt index-structures (before move).
					adaptSubtreeForMove(toMove, ChangeType.DELETE);

					// Adapt hashes.
					adaptHashesForMove(toMove);

					// Adapt pointers and merge sibling text nodes.
					adaptForMove(toMove, nodeAnchor, InsertPos.ASFIRSTCHILD);
					mNodeRtx.moveTo(toMove.getNodeKey());
					adaptHashesWithAdd();

					// Adapt path summary.
					if (mBuildPathSummary && toMove instanceof NameNode) {
						final NameNode moved = (NameNode) toMove;
						mPathSummaryWriter.adaptPathForChangedNode(moved, getName(),
								moved.getURIKey(), moved.getPrefixKey(),
								moved.getLocalNameKey(), OPType.MOVED);
					}

					// Adapt index-structures (after move).
					adaptSubtreeForMove(toMove, ChangeType.INSERT);

					// Compute and assign new DeweyIDs.
					if (mDeweyIDsStored) {
						computeNewDeweyIDs();
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

	/**
	 * Adapt subtree regarding the index-structures.
	 * 
	 * @param toMove
	 *          node which is moved (either before the move, or after the move)
	 * @param type
	 *          the type of change (either deleted from the old position or
	 *          inserted into the new position)
	 * @throws SirixIOException
	 *           if an I/O exception occurs
	 */
	private void adaptSubtreeForMove(final Node node,
			final ChangeType type) throws SirixIOException {
		assert type != null;
		final long beforeNodeKey = getNode().getNodeKey();
		moveTo(node.getNodeKey());
		final Axis axis = new DescendantAxis(this);
		while (axis.hasNext()) {
			axis.next();
			for (int i = 0, attCount = getAttributeCount(); i < attCount; i++) {
				moveToAttribute(i);
				final ImmutableAttribute att = (ImmutableAttribute) getNode();
				mIndexController.notifyChange(type, att, att.getPathNodeKey());
				moveToParent();
			}
			for (int i = 0, nspCount = getNamespaceCount(); i < nspCount; i++) {
				moveToAttribute(i);
				final ImmutableNamespace nsp = (ImmutableNamespace) getNode();
				mIndexController.notifyChange(type, nsp, nsp.getPathNodeKey());
				moveToParent();
			}
			long pathNodeKey = -1;
			if (getNode() instanceof ValueNode
					&& getNode().getParentKey() != Fixed.DOCUMENT_NODE_KEY
							.getStandardProperty()) {
				final long nodeKey = getNode().getNodeKey();
				pathNodeKey = moveToParent().get().getNameNode().getPathNodeKey();
				moveTo(nodeKey);
			} else if (getNode() instanceof NameNode) {
				pathNodeKey = getNameNode().getPathNodeKey();
			}
			mIndexController.notifyChange(type, getNode(), pathNodeKey);
		}
		moveTo(beforeNodeKey);
	}

	/**
	 * Compute the new DeweyIDs.
	 * 
	 * @throws SirixException
	 *           if anything went wrong
	 */
	private void computeNewDeweyIDs() throws SirixException {
		SirixDeweyID id = null;
		if (hasLeftSibling() && hasRightSibling()) {
			id = SirixDeweyID.newBetween(getLeftSiblingDeweyID().get(),
					getRightSiblingDeweyID().get());
		} else if (hasLeftSibling()) {
			id = SirixDeweyID.newBetween(getLeftSiblingDeweyID().get(), null);
		} else if (hasRightSibling()) {
			id = SirixDeweyID.newBetween(null, getRightSiblingDeweyID().get());
		} else {
			id = mNodeRtx.getParentDeweyID().get().getNewChildID();
		}

		assert id != null;
		final long nodeKey = mNodeRtx.getCurrentNode().getNodeKey();

		final StructNode root = (StructNode) getPageTransaction()
				.prepareEntryForModification(nodeKey, PageKind.RECORDPAGE, -1,
						Optional.<UnorderedKeyValuePage> absent());
		root.setDeweyID(Optional.of(id));

		if (root.hasFirstChild()) {
			final Node firstChild = (Node) getPageTransaction()
					.prepareEntryForModification(root.getFirstChildKey(),
							PageKind.RECORDPAGE, -1, Optional.<UnorderedKeyValuePage> absent());
			firstChild.setDeweyID(Optional.of(id.getNewChildID()));

			int previousLevel = getDeweyID().get().getLevel();
			mNodeRtx.moveTo(firstChild.getNodeKey());
			int attributeNr = 0;
			int nspNr = 0;
			for (@SuppressWarnings("unused")
			final long key : LevelOrderAxis.newBuilder(this)
					.includeNonStructuralNodes().build()) {
				Optional<SirixDeweyID> deweyID = Optional.<SirixDeweyID> absent();
				if (isAttribute()) {
					final long attNodeKey = mNodeRtx.getNodeKey();
					if (attributeNr == 0) {
						deweyID = Optional.of(mNodeRtx.getParentDeweyID().get()
								.getNewAttributeID());
					} else {
						mNodeRtx.moveTo(attributeNr - 1);
						deweyID = Optional.of(SirixDeweyID.newBetween(mNodeRtx.getNode()
								.getDeweyID().get(), null));
					}
					mNodeRtx.moveTo(attNodeKey);
					attributeNr++;
				} else if (isNamespace()) {
					final long nspNodeKey = mNodeRtx.getNodeKey();
					if (nspNr == 0) {
						deweyID = Optional.of(mNodeRtx.getParentDeweyID().get()
								.getNewNamespaceID());
					} else {
						mNodeRtx.moveTo(nspNr - 1);
						deweyID = Optional.of(SirixDeweyID.newBetween(mNodeRtx.getNode()
								.getDeweyID().get(), null));
					}
					mNodeRtx.moveTo(nspNodeKey);
					nspNr++;
				} else {
					attributeNr = 0;
					nspNr = 0;
					if (previousLevel + 1 == getDeweyID().get().getLevel()) {
						if (mNodeRtx.hasLeftSibling()) {
							deweyID = Optional.of(SirixDeweyID.newBetween(
									getLeftSiblingDeweyID().get(), null));
						} else {
							deweyID = Optional.of(getParentDeweyID().get().getNewChildID());
						}
					} else {
						previousLevel++;
						deweyID = Optional.of(getParentDeweyID().get().getNewChildID());
					}
				}

				final Node node = (Node) getPageTransaction()
						.prepareEntryForModification(
								mNodeRtx.getCurrentNode().getNodeKey(), PageKind.RECORDPAGE, -1,
								Optional.<UnorderedKeyValuePage> absent());
				node.setDeweyID(deweyID);
			}

			mNodeRtx.moveTo(nodeKey);
		}
	}

	@Override
	public NodeWriteTrx moveSubtreeToLeftSibling(final @Nonnegative long fromKey)
			throws SirixException, IllegalArgumentException {
		acquireLock();
		try {
			if (mNodeRtx.getStructuralNode().hasLeftSibling()) {
				moveToLeftSibling();
				return moveSubtreeToRightSibling(fromKey);
			} else {
				moveToParent();
				return moveSubtreeToFirstChild(fromKey);
			}
		} finally {
			unLock();
		}
	}

	@Override
	public NodeWriteTrx moveSubtreeToRightSibling(final @Nonnegative long fromKey)
			throws SirixException {
		acquireLock();
		try {
			if (fromKey < 0 || fromKey > getMaxNodeKey()) {
				throw new IllegalArgumentException("Argument must be a valid node key!");
			}
			if (fromKey == getCurrentNode().getNodeKey()) {
				throw new IllegalArgumentException(
						"Can't move itself to first child of itself!");
			}

			// Save: Every node in the "usual" node page is of type Node.
			@SuppressWarnings("unchecked")
			final Optional<? extends Node> node = (Optional<? extends Node>) getPageTransaction()
					.getRecord(fromKey, PageKind.RECORDPAGE, -1);
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
					mNodeRtx.moveTo(toMove.getNodeKey());
					adaptHashesWithAdd();

					// Adapt path summary.
					if (mBuildPathSummary && toMove instanceof NameNode) {
						final NameNode moved = (NameNode) toMove;
						final OPType type = moved.getParentKey() == parentKey ? OPType.MOVEDSAMELEVEL
								: OPType.MOVED;
						mPathSummaryWriter.adaptPathForChangedNode(moved, getName(),
								moved.getURIKey(), moved.getPrefixKey(),
								moved.getLocalNameKey(), type);
					}

					// Recompute DeweyIDs if they are used.
					if (mDeweyIDsStored) {
						computeNewDeweyIDs();
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
	 * @param nodeToMove
	 *          node which implements {@link StructNode} and is moved
	 * @throws SirixIOException
	 *           if any I/O operation fails
	 */
	private void adaptHashesForMove(final StructNode nodeToMove)
			throws SirixIOException {
		assert nodeToMove != null;
		mNodeRtx.setCurrentNode(nodeToMove);
		adaptHashesWithRemove();
	}

	/**
	 * Adapting everything for move operations.
	 * 
	 * @param fromNode
	 *          root {@link StructNode} of the subtree to be moved
	 * @param toNode
	 *          the {@link StructNode} which is the anchor of the new subtree
	 * @param insertPos
	 *          determines if it has to be inserted as a first child or a right
	 *          sibling
	 * @throws SirixException
	 *           if removing a node fails after merging text nodes
	 */
	private void adaptForMove(final StructNode fromNode,
			final StructNode toNode, final InsertPos insertPos)
			throws SirixException {
		assert fromNode != null;
		assert toNode != null;
		assert insertPos != null;

		// Modify nodes where the subtree has been moved from.
		// ==============================================================================
		final StructNode parent = (StructNode) getPageTransaction()
				.prepareEntryForModification(fromNode.getParentKey(),
						PageKind.RECORDPAGE, -1, Optional.<UnorderedKeyValuePage> absent());
		switch (insertPos) {
		case ASRIGHTSIBLING:
			if (fromNode.getParentKey() != toNode.getParentKey()) {
				parent.decrementChildCount();
			}
			break;
		case ASFIRSTCHILD:
			if (fromNode.getParentKey() != toNode.getNodeKey()) {
				parent.decrementChildCount();
			}
			break;
		case ASNONSTRUCTURAL:
			// Do not decrement child count.
			break;
		default:
		}

		// Adapt first child key of former parent.
		if (parent.getFirstChildKey() == fromNode.getNodeKey()) {
			parent.setFirstChildKey(fromNode.getRightSiblingKey());
		}

		// Adapt left sibling key of former right sibling.
		if (fromNode.hasRightSibling()) {
			final StructNode rightSibling = (StructNode) getPageTransaction()
					.prepareEntryForModification(fromNode.getRightSiblingKey(),
							PageKind.RECORDPAGE, -1, Optional.<UnorderedKeyValuePage> absent());
			rightSibling.setLeftSiblingKey(fromNode.getLeftSiblingKey());
		}

		// Adapt right sibling key of former left sibling.
		if (fromNode.hasLeftSibling()) {
			final StructNode leftSibling = (StructNode) getPageTransaction()
					.prepareEntryForModification(fromNode.getLeftSiblingKey(),
							PageKind.RECORDPAGE, -1, Optional.<UnorderedKeyValuePage> absent());
			leftSibling.setRightSiblingKey(fromNode.getRightSiblingKey());
		}

		// Merge text nodes.
		if (fromNode.hasLeftSibling() && fromNode.hasRightSibling()) {
			moveTo(fromNode.getLeftSiblingKey());
			if (getCurrentNode() != null && getCurrentNode().getKind() == Kind.TEXT) {
				final StringBuilder builder = new StringBuilder(getValue());
				moveTo(fromNode.getRightSiblingKey());
				if (getCurrentNode() != null && getCurrentNode().getKind() == Kind.TEXT) {
					builder.append(getValue());
					if (fromNode.getRightSiblingKey() == toNode.getNodeKey()) {
						moveTo(fromNode.getLeftSiblingKey());
						if (mNodeRtx.getStructuralNode().hasLeftSibling()) {
							final StructNode leftSibling = (StructNode) getPageTransaction()
									.prepareEntryForModification(
											mNodeRtx.getStructuralNode().getLeftSiblingKey(),
											PageKind.RECORDPAGE, -1,
											Optional.<UnorderedKeyValuePage> absent());
							leftSibling.setRightSiblingKey(fromNode.getRightSiblingKey());
						}
						final long leftSiblingKey = mNodeRtx.getStructuralNode()
								.hasLeftSibling() == true ? mNodeRtx.getStructuralNode()
								.getLeftSiblingKey() : getCurrentNode().getNodeKey();
						moveTo(fromNode.getRightSiblingKey());
						final StructNode rightSibling = (StructNode) getPageTransaction()
								.prepareEntryForModification(getCurrentNode().getNodeKey(),
										PageKind.RECORDPAGE, -1,
										Optional.<UnorderedKeyValuePage> absent());
						rightSibling.setLeftSiblingKey(leftSiblingKey);
						moveTo(fromNode.getLeftSiblingKey());
						remove();
						moveTo(fromNode.getRightSiblingKey());
					} else {
						if (mNodeRtx.getStructuralNode().hasRightSibling()) {
							final StructNode rightSibling = (StructNode) getPageTransaction()
									.prepareEntryForModification(
											mNodeRtx.getStructuralNode().getRightSiblingKey(),
											PageKind.RECORDPAGE, -1,
											Optional.<UnorderedKeyValuePage> absent());
							rightSibling.setLeftSiblingKey(fromNode.getLeftSiblingKey());
						}
						final long rightSiblingKey = mNodeRtx.getStructuralNode()
								.hasRightSibling() == true ? mNodeRtx.getStructuralNode()
								.getRightSiblingKey() : getCurrentNode().getNodeKey();
						moveTo(fromNode.getLeftSiblingKey());
						final StructNode leftSibling = (StructNode) getPageTransaction()
								.prepareEntryForModification(getCurrentNode().getNodeKey(),
										PageKind.RECORDPAGE, -1,
										Optional.<UnorderedKeyValuePage> absent());
						leftSibling.setRightSiblingKey(rightSiblingKey);
						moveTo(fromNode.getRightSiblingKey());
						remove();
						moveTo(fromNode.getLeftSiblingKey());
					}
					setValue(builder.toString());
				}
			}
		}

		// Modify nodes where the subtree has been moved to.
		// ==============================================================================
		insertPos.processMove(fromNode, toNode, this);
	}

	@Override
	public NodeWriteTrx insertElementAsFirstChild(final QNm name)
			throws SirixException {
		if (!XMLToken.isValidQName(checkNotNull(name))) {
			throw new IllegalArgumentException("The QName is not valid!");
		}
		acquireLock();
		try {
			final Kind kind = mNodeRtx.getCurrentNode().getKind();
			if (kind == Kind.ELEMENT || kind == Kind.DOCUMENT) {
				checkAccessAndCommit();

				final long parentKey = mNodeRtx.getCurrentNode().getNodeKey();
				final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
				final long rightSibKey = ((StructNode) mNodeRtx.getCurrentNode())
						.getFirstChildKey();

				final long pathNodeKey = mBuildPathSummary ? mPathSummaryWriter
						.getPathNodeKey(name, Kind.ELEMENT) : 0;
				final Optional<SirixDeweyID> id = newFirstChildID();
				final ElementNode node = mNodeFactory.createElementNode(parentKey,
						leftSibKey, rightSibKey, 0, name, pathNodeKey, id);

				mNodeRtx.setCurrentNode(node);
				adaptForInsert(node, InsertPos.ASFIRSTCHILD, PageKind.RECORDPAGE);
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

	@Override
	public NodeWriteTrx insertElementAsLeftSibling(final QNm name)
			throws SirixException {
		if (!XMLToken.isValidQName(checkNotNull(name))) {
			throw new IllegalArgumentException("The QName is not valid!");
		}
		acquireLock();
		try {
			if (getCurrentNode() instanceof StructNode
					&& getCurrentNode().getKind() != Kind.DOCUMENT) {
				checkAccessAndCommit();

				final long key = getCurrentNode().getNodeKey();
				moveToParent();
				final long pathNodeKey = mBuildPathSummary ? mPathSummaryWriter
						.getPathNodeKey(name, Kind.ELEMENT) : 0;
				moveTo(key);

				final long parentKey = getCurrentNode().getParentKey();
				final long leftSibKey = ((StructNode) getCurrentNode())
						.getLeftSiblingKey();
				final long rightSibKey = getCurrentNode().getNodeKey();

				final Optional<SirixDeweyID> id = newLeftSiblingID();
				final ElementNode node = mNodeFactory.createElementNode(parentKey,
						leftSibKey, rightSibKey, 0, name, pathNodeKey, id);

				mNodeRtx.setCurrentNode(node);
				adaptForInsert(node, InsertPos.ASLEFTSIBLING, PageKind.RECORDPAGE);
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
	public NodeWriteTrx insertElementAsRightSibling(final QNm name)
			throws SirixException {
		if (!XMLToken.isValidQName(checkNotNull(name))) {
			throw new IllegalArgumentException("The QName is not valid!");
		}
		acquireLock();
		try {
			if (getCurrentNode() instanceof StructNode && !isDocumentRoot()) {
				checkAccessAndCommit();

				final long key = getCurrentNode().getNodeKey();
				moveToParent();
				final long pathNodeKey = mBuildPathSummary ? mPathSummaryWriter
						.getPathNodeKey(name, Kind.ELEMENT) : 0;
				moveTo(key);

				final long parentKey = getCurrentNode().getParentKey();
				final long leftSibKey = getCurrentNode().getNodeKey();
				final long rightSibKey = ((StructNode) getCurrentNode())
						.getRightSiblingKey();

				final Optional<SirixDeweyID> id = newRightSiblingID();
				final ElementNode node = mNodeFactory.createElementNode(parentKey,
						leftSibKey, rightSibKey, 0, name, pathNodeKey, id);

				mNodeRtx.setCurrentNode(node);
				adaptForInsert(node, InsertPos.ASRIGHTSIBLING, PageKind.RECORDPAGE);
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
	public NodeWriteTrx insertSubtreeAsFirstChild(
			final XMLEventReader reader) throws SirixException {
		return insertSubtree(reader, Insert.ASFIRSTCHILD);
	}

	@Override
	public NodeWriteTrx insertSubtreeAsRightSibling(
			final XMLEventReader reader) throws SirixException {
		return insertSubtree(reader, Insert.ASRIGHTSIBLING);
	}

	@Override
	public NodeWriteTrx insertSubtreeAsLeftSibling(
			final XMLEventReader reader) throws SirixException {
		return insertSubtree(reader, Insert.ASLEFTSIBLING);
	}

	private NodeWriteTrx insertSubtree(final XMLEventReader reader,
			final Insert insert) throws SirixException {
		checkNotNull(reader);
		assert insert != null;
		acquireLock();
		try {
			if (getCurrentNode() instanceof StructNode) {
				checkAccessAndCommit();
				mBulkInsert = true;
				long nodeKey = getCurrentNode().getNodeKey();
				final XMLShredder shredder = new XMLShredder.Builder(this, reader,
						insert).commitAfterwards().build();
				shredder.call();
				moveTo(nodeKey);
				switch (insert) {
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
				final ImmutableNode startNode = getCurrentNode();
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
	public NodeWriteTrx insertPIAsLeftSibling(final String target,
			final String content) throws SirixException {
		return pi(target, content, Insert.ASLEFTSIBLING);
	}

	@Override
	public NodeWriteTrx insertPIAsRightSibling(final String target,
			final String content) throws SirixException {
		return pi(target, content, Insert.ASRIGHTSIBLING);
	}

	@Override
	public NodeWriteTrx insertPIAsFirstChild(final String target,
			final String content) throws SirixException {
		return pi(target, content, Insert.ASFIRSTCHILD);
	}

	/**
	 * Processing instruction.
	 * 
	 * @param target
	 *          target PI
	 * @param content
	 *          content of PI
	 * @param insert
	 *          insertion location
	 * @throws SirixException
	 *           if any unexpected error occurs
	 */
	private NodeWriteTrx pi(final String target,
			final String content, final Insert insert)
			throws SirixException {
		final byte[] targetBytes = getBytes(target);
		if (!XMLToken.isNCName(checkNotNull(targetBytes))) {
			throw new IllegalArgumentException("The target is not valid!");
		}
		if (content.contains("?>-")) {
			throw new SirixUsageException("The content must not contain '?>-'");
		}
		acquireLock();
		try {
			if (getCurrentNode() instanceof StructNode) {
				checkAccessAndCommit();

				// Insert new processing instruction node.
				final byte[] processingContent = getBytes(content);
				long parentKey = 0;
				long leftSibKey = 0;
				long rightSibKey = 0;
				InsertPos pos = InsertPos.ASFIRSTCHILD;
				Optional<SirixDeweyID> id = Optional.<SirixDeweyID> absent();
				switch (insert) {
				case ASFIRSTCHILD:
					parentKey = getCurrentNode().getNodeKey();
					leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
					rightSibKey = ((StructNode) getCurrentNode()).getFirstChildKey();
					id = newFirstChildID();
					break;
				case ASRIGHTSIBLING:
					parentKey = getCurrentNode().getParentKey();
					leftSibKey = getCurrentNode().getNodeKey();
					rightSibKey = ((StructNode) getCurrentNode()).getRightSiblingKey();
					pos = InsertPos.ASRIGHTSIBLING;
					id = newRightSiblingID();
					break;
				case ASLEFTSIBLING:
					parentKey = getCurrentNode().getParentKey();
					leftSibKey = ((StructNode) getCurrentNode()).getLeftSiblingKey();
					rightSibKey = getCurrentNode().getNodeKey();
					pos = InsertPos.ASLEFTSIBLING;
					id = newLeftSiblingID();
					break;
				default:
					throw new IllegalStateException("Insert location not known!");
				}

				final QNm targetName = new QNm(target);
				final long pathNodeKey = mBuildPathSummary ? mPathSummaryWriter
						.getPathNodeKey(targetName, Kind.PROCESSING_INSTRUCTION) : 0;
				final PINode node = mNodeFactory.createPINode(parentKey, leftSibKey,
						rightSibKey, targetName, processingContent, mCompression,
						pathNodeKey, id);

				// Adapt local nodes and hashes.
				mNodeRtx.setCurrentNode(node);
				adaptForInsert(node, pos, PageKind.RECORDPAGE);
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
	public NodeWriteTrx insertCommentAsLeftSibling(final String value)
			throws SirixException {
		return comment(value, Insert.ASLEFTSIBLING);
	}

	@Override
	public NodeWriteTrx insertCommentAsRightSibling(final String value)
			throws SirixException {
		return comment(value, Insert.ASRIGHTSIBLING);
	}

	@Override
	public NodeWriteTrx insertCommentAsFirstChild(final String value)
			throws SirixException {
		return comment(value, Insert.ASFIRSTCHILD);
	}

	/**
	 * Comment node.
	 * 
	 * @param value
	 *          value of comment
	 * @param insert
	 *          insertion location
	 * @throws SirixException
	 *           if any unexpected error occurs
	 */
	private NodeWriteTrx comment(final String value,
			final Insert insert) throws SirixException {
		// Produces a NPE if value is null (what we want).
		if (value.contains("--")) {
			throw new SirixUsageException(
					"Character sequence \"--\" is not allowed in comment content!");
		}
		if (value.endsWith("-")) {
			throw new SirixUsageException("Comment content must not end with \"-\"!");
		}
		acquireLock();
		try {
			if (getCurrentNode() instanceof StructNode
					&& (getCurrentNode().getKind() != Kind.DOCUMENT || (getCurrentNode()
							.getKind() == Kind.DOCUMENT && insert == Insert.ASFIRSTCHILD))) {
				checkAccessAndCommit();

				// Insert new comment node.
				final byte[] commentValue = getBytes(value);
				long parentKey = 0;
				long leftSibKey = 0;
				long rightSibKey = 0;
				InsertPos pos = InsertPos.ASFIRSTCHILD;
				Optional<SirixDeweyID> id = Optional.<SirixDeweyID> absent();
				switch (insert) {
				case ASFIRSTCHILD:
					parentKey = getCurrentNode().getNodeKey();
					leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
					rightSibKey = ((StructNode) getCurrentNode()).getFirstChildKey();
					id = newFirstChildID();
					break;
				case ASRIGHTSIBLING:
					parentKey = getCurrentNode().getParentKey();
					leftSibKey = getCurrentNode().getNodeKey();
					rightSibKey = ((StructNode) getCurrentNode()).getRightSiblingKey();
					pos = InsertPos.ASRIGHTSIBLING;
					id = newRightSiblingID();
					break;
				case ASLEFTSIBLING:
					parentKey = getCurrentNode().getParentKey();
					leftSibKey = ((StructNode) getCurrentNode()).getLeftSiblingKey();
					rightSibKey = getCurrentNode().getNodeKey();
					pos = InsertPos.ASLEFTSIBLING;
					id = newLeftSiblingID();
					break;
				default:
					throw new IllegalStateException("Insert location not known!");
				}

				final CommentNode node = mNodeFactory.createCommentNode(parentKey,
						leftSibKey, rightSibKey, commentValue, mCompression, id);

				// Adapt local nodes and hashes.
				mNodeRtx.setCurrentNode(node);
				adaptForInsert(node, pos, PageKind.RECORDPAGE);
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
	public NodeWriteTrx insertTextAsFirstChild(final String value)
			throws SirixException {
		checkNotNull(value);
		acquireLock();
		try {
			if (getCurrentNode() instanceof StructNode && !value.isEmpty()) {
				checkAccessAndCommit();

				final long pathNodeKey = getCurrentNode().getNodeKey();
				final long parentKey = getCurrentNode().getNodeKey();
				final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
				final long rightSibKey = ((StructNode) getCurrentNode())
						.getFirstChildKey();

				// Update value in case of adjacent text nodes.
				if (hasNode(rightSibKey)) {
					moveTo(rightSibKey);
					if (getCurrentNode().getKind() == Kind.TEXT) {
						setValue(new StringBuilder(value).append(getValue()).toString());
						adaptHashedWithUpdate(getCurrentNode().getHash());
						return this;
					}
					moveTo(parentKey);
				}

				// Insert new text node if no adjacent text nodes are found.
				final byte[] textValue = getBytes(value);
				final Optional<SirixDeweyID> id = newFirstChildID();
				final TextNode node = mNodeFactory.createTextNode(parentKey,
						leftSibKey, rightSibKey, textValue, mCompression, id);

				// Adapt local nodes and hashes.
				mNodeRtx.setCurrentNode(node);
				adaptForInsert(node, InsertPos.ASFIRSTCHILD, PageKind.RECORDPAGE);
				mNodeRtx.setCurrentNode(node);
				adaptHashesWithAdd();

				// Index text value.
				mIndexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

				return this;
			} else {
				throw new SirixUsageException(
						"Insert is not allowed if current node is not an ElementNode or TextNode!");
			}
		} finally {
			unLock();
		}
	}

	@Override
	public NodeWriteTrx insertTextAsLeftSibling(final String value)
			throws SirixException {
		checkNotNull(value);
		acquireLock();
		try {
			if (getCurrentNode() instanceof StructNode
					&& getCurrentNode().getKind() != Kind.DOCUMENT && !value.isEmpty()) {
				checkAccessAndCommit();

				final long parentKey = getCurrentNode().getParentKey();
				final long leftSibKey = ((StructNode) getCurrentNode())
						.getLeftSiblingKey();
				final long rightSibKey = getCurrentNode().getNodeKey();

				// Update value in case of adjacent text nodes.
				final StringBuilder builder = new StringBuilder();
				if (getCurrentNode().getKind() == Kind.TEXT) {
					builder.append(value);
				}
				builder.append(getValue());

				if (!value.equals(builder.toString())) {
					setValue(builder.toString());
					return this;
				}
				if (hasNode(leftSibKey)) {
					moveTo(leftSibKey);
					final StringBuilder valueBuilder = new StringBuilder();
					if (getCurrentNode().getKind() == Kind.TEXT) {
						valueBuilder.append(getValue()).append(builder);
					}
					if (!value.equals(valueBuilder.toString())) {
						setValue(valueBuilder.toString());
						return this;
					}
				}

				// Insert new text node if no adjacent text nodes are found.
				moveTo(rightSibKey);
				final byte[] textValue = getBytes(builder.toString());
				final Optional<SirixDeweyID> id = newLeftSiblingID();
				final TextNode node = mNodeFactory.createTextNode(parentKey,
						leftSibKey, rightSibKey, textValue, mCompression, id);

				// Adapt local nodes and hashes.
				mNodeRtx.setCurrentNode(node);
				adaptForInsert(node, InsertPos.ASLEFTSIBLING, PageKind.RECORDPAGE);
				mNodeRtx.setCurrentNode(node);
				adaptHashesWithAdd();
				
				// Get the path node key.
				final long pathNodeKey = moveToParent().get().isElement() ? getNameNode().getPathNodeKey() : -1;
				mNodeRtx.setCurrentNode(node);
				
				// Index text value.
				mIndexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

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
	public NodeWriteTrx insertTextAsRightSibling(final String value)
			throws SirixException {
		checkNotNull(value);
		acquireLock();
		try {
			if (getCurrentNode() instanceof StructNode
					&& getCurrentNode().getKind() != Kind.DOCUMENT && !value.isEmpty()) {
				checkAccessAndCommit();

				final long parentKey = getCurrentNode().getParentKey();
				final long leftSibKey = getCurrentNode().getNodeKey();
				final long rightSibKey = ((StructNode) getCurrentNode())
						.getRightSiblingKey();

				// Update value in case of adjacent text nodes.
				final StringBuilder builder = new StringBuilder();
				if (getCurrentNode().getKind() == Kind.TEXT) {
					builder.append(getValue());
				}
				builder.append(value);
				if (!value.equals(builder.toString())) {
					setValue(builder.toString());
					return this;
				}
				if (hasNode(rightSibKey)) {
					moveTo(rightSibKey);
					if (getCurrentNode().getKind() == Kind.TEXT) {
						builder.append(getValue());
					}
					if (!value.equals(builder.toString())) {
						setValue(builder.toString());
						return this;
					}
				}

				// Insert new text node if no adjacent text nodes are found.
				moveTo(leftSibKey);
				final byte[] textValue = getBytes(builder.toString());
				final Optional<SirixDeweyID> id = newRightSiblingID();

				final TextNode node = mNodeFactory.createTextNode(parentKey,
						leftSibKey, rightSibKey, textValue, mCompression, id);

				// Adapt local nodes and hashes.
				mNodeRtx.setCurrentNode(node);
				adaptForInsert(node, InsertPos.ASRIGHTSIBLING, PageKind.RECORDPAGE);
				mNodeRtx.setCurrentNode(node);
				adaptHashesWithAdd();

				// Get the path node key.
				final long pathNodeKey = moveToParent().get().isElement() ? getNameNode().getPathNodeKey() : -1;
				mNodeRtx.setCurrentNode(node);
				
				// Index text value.
				mIndexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

				return this;
			} else {
				throw new SirixUsageException(
						"Insert is not allowed if current node is not an Element- or Text-node or value is empty!");
			}
		} finally {
			unLock();
		}
	}

	/**
	 * Get an optional namespace {@link SirixDeweyID} reference.
	 * 
	 * @return optional namespace {@link SirixDeweyID} reference
	 * @throws SirixException
	 *           if generating an ID fails
	 */
	private Optional<SirixDeweyID> newNamespaceID() throws SirixException {
		Optional<SirixDeweyID> id = Optional.<SirixDeweyID> absent();
		if (mDeweyIDsStored) {
			if (mNodeRtx.hasNamespaces()) {
				mNodeRtx.moveToNamespace(mNodeRtx.getNamespaceCount() - 1);
				id = Optional.of(SirixDeweyID.newBetween(mNodeRtx.getNode()
						.getDeweyID().get(), null));
				mNodeRtx.moveToParent();
			} else {
				id = Optional.of(mNodeRtx.getCurrentNode().getDeweyID().get()
						.getNewNamespaceID());
			}
		}
		return id;
	}

	/**
	 * Get an optional attribute {@link SirixDeweyID} reference.
	 * 
	 * @return optional attribute {@link SirixDeweyID} reference
	 * @throws SirixException
	 *           if generating an ID fails
	 */
	private Optional<SirixDeweyID> newAttributeID() throws SirixException {
		Optional<SirixDeweyID> id = Optional.<SirixDeweyID> absent();
		if (mDeweyIDsStored) {
			if (mNodeRtx.hasAttributes()) {
				mNodeRtx.moveToAttribute(mNodeRtx.getAttributeCount() - 1);
				id = Optional.of(SirixDeweyID.newBetween(mNodeRtx.getNode()
						.getDeweyID().get(), null));
				mNodeRtx.moveToParent();
			} else {
				id = Optional.of(mNodeRtx.getCurrentNode().getDeweyID().get()
						.getNewAttributeID());
			}
		}
		return id;
	}

	/**
	 * Get an optional first child {@link SirixDeweyID} reference.
	 * 
	 * @return optional first child {@link SirixDeweyID} reference
	 * @throws SirixException
	 *           if generating an ID fails
	 */
	private Optional<SirixDeweyID> newFirstChildID() throws SirixException {
		Optional<SirixDeweyID> id = Optional.<SirixDeweyID> absent();
		if (mDeweyIDsStored) {
			if (mNodeRtx.getStructuralNode().hasFirstChild()) {
				mNodeRtx.moveToFirstChild();
				id = Optional.of(SirixDeweyID.newBetween(null, mNodeRtx.getNode()
						.getDeweyID().get()));
			} else {
				id = Optional.of(mNodeRtx.getCurrentNode().getDeweyID().get()
						.getNewChildID());
			}
		}
		return id;
	}

	/**
	 * Get an optional left sibling {@link SirixDeweyID} reference.
	 * 
	 * @return optional left sibling {@link SirixDeweyID} reference
	 * @throws SirixException
	 *           if generating an ID fails
	 */
	private Optional<SirixDeweyID> newLeftSiblingID() throws SirixException {
		Optional<SirixDeweyID> id = Optional.<SirixDeweyID> absent();
		if (mDeweyIDsStored) {
			final SirixDeweyID currID = mNodeRtx.getCurrentNode().getDeweyID().get();
			if (mNodeRtx.hasLeftSibling()) {
				mNodeRtx.moveToLeftSibling();
				id = Optional.of(SirixDeweyID.newBetween(mNodeRtx.getCurrentNode()
						.getDeweyID().get(), currID));
				mNodeRtx.moveToRightSibling();
			} else {
				id = Optional.of(SirixDeweyID.newBetween(null, currID));
			}
		}
		return id;
	}

	/**
	 * Get an optional right sibling {@link SirixDeweyID} reference.
	 * 
	 * @return optional right sibling {@link SirixDeweyID} reference
	 * @throws SirixException
	 *           if generating an ID fails
	 */
	private Optional<SirixDeweyID> newRightSiblingID() throws SirixException {
		Optional<SirixDeweyID> id = Optional.<SirixDeweyID> absent();
		if (mDeweyIDsStored) {
			final SirixDeweyID currID = mNodeRtx.getCurrentNode().getDeweyID().get();
			if (mNodeRtx.hasRightSibling()) {
				mNodeRtx.moveToRightSibling();
				id = Optional.of(SirixDeweyID.newBetween(currID, mNodeRtx
						.getCurrentNode().getDeweyID().get()));
				mNodeRtx.moveToLeftSibling();
			} else {
				id = Optional.of(SirixDeweyID.newBetween(currID, null));
			}
		}
		return id;
	}

	/**
	 * Get a byte-array from a value.
	 * 
	 * @param pValue
	 *          the value
	 * @return byte-array representation of {@code pValue}
	 */
	private byte[] getBytes(final String pValue) {
		return pValue.getBytes(Constants.DEFAULT_ENCODING);
	}

	@Override
	public NodeWriteTrx insertAttribute(final QNm name,
			final String value) throws SirixException {
		return insertAttribute(name, value, Movement.NONE);
	}

	@Override
	public NodeWriteTrx insertAttribute(final QNm name,
			final String value, final Movement move)
			throws SirixException {
		checkNotNull(value);
		if (!XMLToken.isValidQName(checkNotNull(name))) {
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
				final ElementNode element = (ElementNode) getCurrentNode();
				final Optional<Long> attKey = element.getAttributeKeyByName(name);
				if (attKey.isPresent()) {
					moveTo(attKey.get());
					final QNm qName = getName();
					if (name.equals(qName)) {
						if (getValue().equals(value)) {
							throw new SirixUsageException("Duplicate attribute!");
						} else {
							setValue(value);
						}
					}
					moveToParent();
				}

				// Get the path node key.
				final long pathNodeKey = mNodeRtx.mSession.mResourceConfig.mPathSummary ? mPathSummaryWriter
						.getPathNodeKey(name, Kind.ATTRIBUTE) : 0;
				final byte[] attValue = getBytes(value);

				final Optional<SirixDeweyID> id = newAttributeID();
				final long elementKey = getCurrentNode().getNodeKey();
				final AttributeNode node = mNodeFactory.createAttributeNode(elementKey,
						name, attValue, pathNodeKey, id);

				final Node parentNode = (Node) getPageTransaction()
						.prepareEntryForModification(node.getParentKey(),
								PageKind.RECORDPAGE, -1,
								Optional.<UnorderedKeyValuePage> absent());
				((ElementNode) parentNode).insertAttribute(node.getNodeKey(),
						node.getPrefixKey() + node.getLocalNameKey());

				mNodeRtx.setCurrentNode(node);
				adaptHashesWithAdd();

				// Index text value.
				mIndexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

				if (move == Movement.TOPARENT) {
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
	public NodeWriteTrx insertNamespace(final QNm name)
			throws SirixException {
		return insertNamespace(name, Movement.NONE);
	}

	@Override
	public NodeWriteTrx insertNamespace(final QNm name,
			final Movement move) throws SirixException {
		if (!XMLToken.isValidQName(checkNotNull(name))) {
			throw new IllegalArgumentException("The QName is not valid!");
		}
		acquireLock();
		try {
			if (getCurrentNode().getKind() == Kind.ELEMENT) {
				checkAccessAndCommit();

				for (int i = 0, namespCount = ((ElementNode) getCurrentNode())
						.getNamespaceCount(); i < namespCount; i++) {
					moveToNamespace(i);
					final QNm qName = getName();
					if (name.getPrefix().equals(qName.getPrefix())) {
						throw new SirixUsageException("Duplicate namespace!");
					}
					moveToParent();
				}

				final long pathNodeKey = mBuildPathSummary ? mPathSummaryWriter
						.getPathNodeKey(name, Kind.NAMESPACE) : 0;
				final int uriKey = getPageTransaction().createNameKey(
						name.getNamespaceURI(), Kind.NAMESPACE);
				final int prefixKey = name.getPrefix() != null
						&& !name.getPrefix().isEmpty() ? getPageTransaction()
						.createNameKey(name.getPrefix(), Kind.NAMESPACE) : -1;
				final long elementKey = getCurrentNode().getNodeKey();

				final Optional<SirixDeweyID> id = newNamespaceID();
				final NamespaceNode node = mNodeFactory.createNamespaceNode(elementKey,
						uriKey, prefixKey, pathNodeKey, id);

				final Node parentNode = (Node) getPageTransaction()
						.prepareEntryForModification(node.getParentKey(),
								PageKind.RECORDPAGE, -1,
								Optional.<UnorderedKeyValuePage> absent());
				((ElementNode) parentNode).insertNamespace(node.getNodeKey());

				mNodeRtx.setCurrentNode(node);
				adaptHashesWithAdd();
				if (move == Movement.TOPARENT) {
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
	private void checkAncestors(final Node node) {
		assert node != null;
		final ImmutableNode item = getCurrentNode();
		while (getCurrentNode().hasParent()) {
			moveToParent();
			if (getCurrentNode().getNodeKey() == node.getNodeKey()) {
				throw new IllegalStateException(
						"Moving one of the ancestor nodes is not permitted!");
			}
		}
		moveTo(item.getNodeKey());
	}

	@Override
	public NodeWriteTrx remove() throws SirixException {
		checkAccessAndCommit();
		acquireLock();
		try {
			if (getCurrentNode().getKind() == Kind.DOCUMENT) {
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

					// Remove text value.
					removeValue();

					// Then remove node.
					getPageTransaction().removeEntry(getCurrentNode().getNodeKey(),
							PageKind.RECORDPAGE, -1, Optional.<UnorderedKeyValuePage> absent());
				}

				// Adapt hashes and neighbour nodes as well as the name from the
				// NamePage mapping if it's not a text node.
				mNodeRtx.setCurrentNode(node);
				adaptHashesWithRemove();
				adaptForRemove(node, PageKind.RECORDPAGE);
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
				final ImmutableNode node = mNodeRtx.getCurrentNode();

				final ElementNode parent = (ElementNode) getPageTransaction()
						.prepareEntryForModification(node.getParentKey(),
								PageKind.RECORDPAGE, -1,
								Optional.<UnorderedKeyValuePage> absent());
				parent.removeAttribute(node.getNodeKey());
				adaptHashesWithRemove();
				getPageTransaction().removeEntry(node.getNodeKey(), PageKind.RECORDPAGE,
						-1, Optional.<UnorderedKeyValuePage> absent());
				removeName();
				mIndexController.notifyChange(ChangeType.DELETE, getNode(), parent.getPathNodeKey());
				moveToParent();
			} else if (getCurrentNode().getKind() == Kind.NAMESPACE) {
				final ImmutableNode node = mNodeRtx.getCurrentNode();

				final ElementNode parent = (ElementNode) getPageTransaction()
						.prepareEntryForModification(node.getParentKey(),
								PageKind.RECORDPAGE, -1,
								Optional.<UnorderedKeyValuePage> absent());
				parent.removeNamespace(node.getNodeKey());
				adaptHashesWithRemove();
				getPageTransaction().removeEntry(node.getNodeKey(), PageKind.RECORDPAGE,
						-1, Optional.<UnorderedKeyValuePage> absent());
				removeName();
				moveToParent();
			}

			return this;
		} finally {
			unLock();
		}
	}

	private void removeValue() throws SirixIOException {
		if (getCurrentNode() instanceof ValueNode) {
			final long nodeKey = getNodeKey();
			final long pathNodeKey = moveToParent().hasMoved() ? getPathNodeKey() : -1;
			moveTo(nodeKey);
			mIndexController.notifyChange(ChangeType.DELETE, getNode(), pathNodeKey);
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
				removeValue();
				getPageTransaction().removeEntry(getCurrentNode().getNodeKey(),
						PageKind.RECORDPAGE, -1, Optional.<UnorderedKeyValuePage> absent());
				moveToParent();
			}
			final int nspCount = mNodeRtx.getNamespaceCount();
			for (int i = 0; i < nspCount; i++) {
				moveToNamespace(i);
				removeName();
				getPageTransaction().removeEntry(getCurrentNode().getNodeKey(),
						PageKind.RECORDPAGE, -1, Optional.<UnorderedKeyValuePage> absent());
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
			page.removeName(node.getPrefixKey(), nodeKind);
			page.removeName(node.getLocalNameKey(), nodeKind);
			page.removeName(node.getURIKey(), Kind.NAMESPACE);

			assert nodeKind != Kind.DOCUMENT;
			if (mBuildPathSummary) {
				mPathSummaryWriter.remove(node, nodeKind, page);
			}
		}
	}

	@Override
	public NodeWriteTrx setName(final QNm name) throws SirixException {
		checkNotNull(name);
		acquireLock();
		try {
			if (getCurrentNode() instanceof NameNode) {
				if (!getName().equals(name)) {
					checkAccessAndCommit();

					NameNode node = (NameNode) mNodeRtx.getCurrentNode();
					final long oldHash = node.hashCode();

					// Create new keys for mapping.
					final int prefixKey = name.getPrefix() != null
							&& !name.getPrefix().isEmpty() ? getPageTransaction()
							.createNameKey(name.getPrefix(), node.getKind()) : -1;
					final int localNameKey = name.getLocalName() != null
							&& !name.getLocalName().isEmpty() ? getPageTransaction()
							.createNameKey(name.getLocalName(), node.getKind()) : -1;
					final int uriKey = name.getNamespaceURI() != null
							&& !name.getNamespaceURI().isEmpty() ? getPageTransaction()
							.createNameKey(name.getNamespaceURI(), Kind.NAMESPACE) : -1;

					// Adapt path summary.
					if (mBuildPathSummary) {
						mPathSummaryWriter.adaptPathForChangedNode(node, name, uriKey,
								prefixKey, localNameKey, OPType.SETNAME);
					}

					// Remove old keys from mapping.
					final Kind nodeKind = node.getKind();
					final int oldPrefixKey = node.getPrefixKey();
					final int oldLocalNameKey = node.getLocalNameKey();
					final int oldUriKey = node.getURIKey();
					final NamePage page = ((NamePage) getPageTransaction()
							.getActualRevisionRootPage().getNamePageReference().getPage());
					page.removeName(oldPrefixKey, nodeKind);
					page.removeName(oldLocalNameKey, nodeKind);
					page.removeName(oldUriKey, Kind.NAMESPACE);

					// Set new keys for current node.
					node = (NameNode) getPageTransaction().prepareEntryForModification(
							node.getNodeKey(), PageKind.RECORDPAGE, -1,
							Optional.<UnorderedKeyValuePage> absent());
					node.setLocalNameKey(localNameKey);
					node.setURIKey(uriKey);
					node.setPrefixKey(prefixKey);
					node.setPathNodeKey(mBuildPathSummary ? mPathSummaryWriter
							.getNodeKey() : 0);

					mNodeRtx.setCurrentNode(node);
					adaptHashedWithUpdate(oldHash);
				}

				return this;
			} else {
				throw new SirixUsageException(
						"setName is not allowed if current node is not an INameNode implementation!");
			}
		} finally {
			unLock();
		}
	}

	@Override
	public NodeWriteTrx setValue(final String value)
			throws SirixException {
		checkNotNull(value);
		acquireLock();
		try {
			if (getCurrentNode() instanceof ValueNode) {
				checkAccessAndCommit();

				// If an empty value is specified the node needs to be removed (see
				// XDM).
				if (value.isEmpty()) {
					remove();
					return this;
				}
				
				// Remove old value from indexes.
				mIndexController.notifyChange(ChangeType.DELETE, getNode(), getPathNodeKey());

				final long oldHash = mNodeRtx.getCurrentNode().hashCode();
				final byte[] byteVal = getBytes(value);

				final ValueNode node = (ValueNode) getPageTransaction()
						.prepareEntryForModification(
								mNodeRtx.getCurrentNode().getNodeKey(), PageKind.RECORDPAGE, -1,
								Optional.<UnorderedKeyValuePage> absent());
				node.setValue(byteVal);

				mNodeRtx.setCurrentNode(node);
				adaptHashedWithUpdate(oldHash);

				// Index new value.
				mIndexController.notifyChange(ChangeType.INSERT, getNode(), getPathNodeKey());

				return this;
			} else {
				throw new SirixUsageException(
						"setValue(String) is not allowed if current node is not an IValNode implementation!");
			}
		} finally {
			unLock();
		}
	}

	@Override
	public void revertTo(final @Nonnegative int revision) throws SirixException {
		acquireLock();
		try {
			mNodeRtx.assertNotClosed();
			mNodeRtx.mSession.assertAccess(revision);

			// Close current page transaction.
			final long trxID = getTransactionID();
			final int revNumber = getRevisionNumber();

			// Reset internal transaction state to new uber page.
			mNodeRtx.mSession.closeNodePageWriteTransaction(getTransactionID());
			final PageWriteTrx<Long, Record, UnorderedKeyValuePage> trx = mNodeRtx.mSession
					.createPageWriteTransaction(trxID, revision, revNumber - 1, Abort.NO);
			mNodeRtx.setPageReadTransaction(null);
			mNodeRtx.setPageReadTransaction(trx);
			mNodeRtx.mSession.setNodePageWriteTransaction(getTransactionID(), trx);

			// Reset node factory.
			mNodeFactory = null;
			mNodeFactory = new NodeFactoryImpl(trx);

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

				final int revision = getRevisionNumber();

				// Release all state immediately.
				mNodeRtx.mSession.closeWriteTransaction(getTransactionID());
				mNodeRtx.close();
				removeCommitFileAndLogs(revision);

				mPathSummaryWriter = null;
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
			final int revision = getRevisionNumber();
			final int revNumber = getPageTransaction().getUberPage().isBootstrap() ? 0
					: revision - 1;

			mNodeRtx.getPageTransaction().clearCaches();
			mNodeRtx.getPageTransaction().closeCaches();
			mNodeRtx.mSession.closeNodePageWriteTransaction(getTransactionID());
			mNodeRtx.setPageReadTransaction(null);
			removeCommitFileAndLogs(revision);
			final PageWriteTrx<Long, Record, UnorderedKeyValuePage> trx = mNodeRtx.mSession
					.createPageWriteTransaction(trxID, revNumber, revNumber, Abort.YES);
			mNodeRtx.setPageReadTransaction(trx);
			mNodeRtx.mSession.setNodePageWriteTransaction(getTransactionID(), trx);

			mNodeFactory = null;
			mNodeFactory = new NodeFactoryImpl(trx);

			reInstantiateIndexes();
		} finally {
			unLock();
		}
	}

	/**
	 * Remove a commit file and all transaction logs.
	 * 
	 * @param revision
	 *          the revision from which to remove all transaction logs
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	private void removeCommitFileAndLogs(final @Nonnegative int revision)
			throws SirixIOException {
		// Delete commit file.
		mNodeRtx.mSession.commitFile(revision).delete();
	}

	@Override
	public void commit() throws SirixException {
		mNodeRtx.assertNotClosed();

		// Execute pre-commit hooks.
		for (final PreCommitHook hook : mPreCommitHooks) {
			hook.preCommit(this);
		}

		// Reset modification counter.
		mModificationCount = 0L;

		// Optionally lock while commiting and assigning new instances.
		acquireLock();
		try {
			final UberPage uberPage = getPageTransaction()
					.commit(MultipleWriteTrx.NO);

			// Remember succesfully committed uber page in session.
			mNodeRtx.mSession.setLastCommittedUberPage(uberPage);

			// Reinstantiate everything.
			reInstantiate(getTransactionID(), getRevisionNumber());
		} finally {
			unLock();
		}

		// Execute post-commit hooks.
		for (final PostCommitHook hook : mPostCommitHooks) {
			hook.postCommit(this);
		}
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
		final PageWriteTrx<Long, Record, UnorderedKeyValuePage> trx = mNodeRtx.mSession
				.createPageWriteTransaction(trxID, revNumber, revNumber, Abort.NO);
		mNodeRtx.setPageReadTransaction(null);
		mNodeRtx.setPageReadTransaction(trx);
		mNodeRtx.mSession.setNodePageWriteTransaction(getTransactionID(), trx);

		mNodeFactory = null;
		mNodeFactory = new NodeFactoryImpl(trx);

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
	@SuppressWarnings("unchecked")
	private void reInstantiateIndexes() {
		// Get a new path summary instance.
		if (mBuildPathSummary) {
			mPathSummaryWriter = null;
			mPathSummaryWriter = PathSummaryWriter.getInstance(
					(PageWriteTrx<Long, Record, UnorderedKeyValuePage>) mNodeRtx
							.getPageTransaction(), mNodeRtx.getSession(), mNodeFactory,
					mNodeRtx);
		}
		
		// Recreate index listeners.
		mIndexController.createIndexListeners(mIndexController.getIndexes().getIndexDefs(), this);
	}

	/**
	 * Modifying hashes in a postorder-traversal.
	 * 
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	private void postOrderTraversalHashes() throws SirixIOException {
		for (@SuppressWarnings("unused")
		final long nodeKey : new PostOrderAxis(this, IncludeSelf.YES)) {
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
	 * @param startNode
	 *          start node
	 */
	private void addParentHash(final ImmutableNode startNode)
			throws SirixIOException {
		switch (mHashKind) {
		case ROLLING:
			long hashToAdd = mHash.hashLong(startNode.hashCode()).asLong();
			Node node = (Node) getPageTransaction().prepareEntryForModification(
					mNodeRtx.getCurrentNode().getNodeKey(), PageKind.RECORDPAGE, -1,
					Optional.<UnorderedKeyValuePage> absent());
			node.setHash(node.getHash() + hashToAdd * PRIME);
			if (startNode instanceof StructNode) {
				((StructNode) node).setDescendantCount(((StructNode) node)
						.getDescendantCount()
						+ ((StructNode) startNode).getDescendantCount() + 1);
			}
			break;
		case POSTORDER:
			break;
		default:
		}
	}

	/** Add a hash and the descendant count. */
	private void addHashAndDescendantCount() throws SirixIOException {
		switch (mHashKind) {
		case ROLLING:
			// Setup.
			final ImmutableNode startNode = getCurrentNode();
			final long oldDescendantCount = mNodeRtx.getStructuralNode()
					.getDescendantCount();
			final long descendantCount = oldDescendantCount == 0 ? 1
					: oldDescendantCount + 1;

			// Set start node.
			long hashToAdd = mHash.hashLong(startNode.hashCode()).asLong();
			Node node = (Node) getPageTransaction().prepareEntryForModification(
					mNodeRtx.getCurrentNode().getNodeKey(), PageKind.RECORDPAGE, -1,
					Optional.<UnorderedKeyValuePage> absent());
			node.setHash(hashToAdd);

			// Set parent node.
			if (startNode.hasParent()) {
				moveToParent();
				node = (Node) getPageTransaction().prepareEntryForModification(
						mNodeRtx.getCurrentNode().getNodeKey(), PageKind.RECORDPAGE, -1,
						Optional.<UnorderedKeyValuePage> absent());
				node.setHash(node.getHash() + hashToAdd * PRIME);
				setAddDescendants(startNode, node, descendantCount);
			}

			mNodeRtx.setCurrentNode(startNode);
			break;
		case POSTORDER:
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
	 * @param newNode
	 *          pointer of the new node to be inserted
	 * @param insertPos
	 *          determines the position where to insert
	 * @param pageKind
	 *          kind of subtree root page
	 * @throws SirixIOException
	 *           if anything weird happens
	 */
	private void adaptForInsert(final Node newNode,
			final InsertPos insertPos, final PageKind pageKind)
			throws SirixIOException {
		assert newNode != null;
		assert insertPos != null;
		assert pageKind != null;

		if (newNode instanceof StructNode) {
			final StructNode strucNode = (StructNode) newNode;
			final StructNode parent = (StructNode) getPageTransaction()
					.prepareEntryForModification(newNode.getParentKey(), pageKind, -1,
							Optional.<UnorderedKeyValuePage> absent());
			parent.incrementChildCount();
			if (insertPos == InsertPos.ASFIRSTCHILD) {
				parent.setFirstChildKey(newNode.getNodeKey());
			}

			if (strucNode.hasRightSibling()) {
				final StructNode rightSiblingNode = (StructNode) getPageTransaction()
						.prepareEntryForModification(strucNode.getRightSiblingKey(),
								pageKind, -1, Optional.<UnorderedKeyValuePage> absent());
				rightSiblingNode.setLeftSiblingKey(newNode.getNodeKey());
			}
			if (strucNode.hasLeftSibling()) {
				final StructNode leftSiblingNode = (StructNode) getPageTransaction()
						.prepareEntryForModification(strucNode.getLeftSiblingKey(),
								pageKind, -1, Optional.<UnorderedKeyValuePage> absent());
				leftSiblingNode.setRightSiblingKey(newNode.getNodeKey());
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
	 * @param oldNode
	 *          pointer of the old node to be replaced
	 * @throws SirixException
	 *           if anything weird happens
	 */
	private void adaptForRemove(final StructNode oldNode,
			final PageKind page) throws SirixException {
		assert oldNode != null;

		// Concatenate neighbor text nodes if they exist (the right sibling is
		// deleted afterwards).
		boolean concatenated = false;
		if (oldNode.hasLeftSibling() && oldNode.hasRightSibling()
				&& moveTo(oldNode.getRightSiblingKey()).hasMoved()
				&& getCurrentNode().getKind() == Kind.TEXT
				&& moveTo(oldNode.getLeftSiblingKey()).hasMoved()
				&& getCurrentNode().getKind() == Kind.TEXT) {
			final StringBuilder builder = new StringBuilder(getValue());
			moveTo(oldNode.getRightSiblingKey());
			builder.append(getValue());
			moveTo(oldNode.getLeftSiblingKey());
			setValue(builder.toString());
			concatenated = true;
		}

		// Adapt left sibling node if there is one.
		if (oldNode.hasLeftSibling()) {
			final StructNode leftSibling = (StructNode) getPageTransaction()
					.prepareEntryForModification(oldNode.getLeftSiblingKey(), page, -1,
							Optional.<UnorderedKeyValuePage> absent());
			if (concatenated) {
				moveTo(oldNode.getRightSiblingKey());
				leftSibling.setRightSiblingKey(((StructNode) getCurrentNode())
						.getRightSiblingKey());
			} else {
				leftSibling.setRightSiblingKey(oldNode.getRightSiblingKey());
			}
		}

		// Adapt right sibling node if there is one.
		if (oldNode.hasRightSibling()) {
			StructNode rightSibling;
			if (concatenated) {
				moveTo(oldNode.getRightSiblingKey());
				moveTo(mNodeRtx.getStructuralNode().getRightSiblingKey());
				rightSibling = (StructNode) getPageTransaction()
						.prepareEntryForModification(
								mNodeRtx.getCurrentNode().getNodeKey(), page, -1,
								Optional.<UnorderedKeyValuePage> absent());
				rightSibling.setLeftSiblingKey(oldNode.getLeftSiblingKey());
			} else {
				rightSibling = (StructNode) getPageTransaction()
						.prepareEntryForModification(oldNode.getRightSiblingKey(), page,
								-1, Optional.<UnorderedKeyValuePage> absent());
				rightSibling.setLeftSiblingKey(oldNode.getLeftSiblingKey());
			}
		}

		// Adapt parent, if node has now left sibling it is a first child.
		StructNode parent = (StructNode) getPageTransaction()
				.prepareEntryForModification(oldNode.getParentKey(), page, -1,
						Optional.<UnorderedKeyValuePage> absent());
		if (!oldNode.hasLeftSibling()) {
			parent.setFirstChildKey(oldNode.getRightSiblingKey());
		}
		parent.decrementChildCount();
		if (concatenated) {
			parent.decrementDescendantCount();
			parent.decrementChildCount();
		}
		if (concatenated) {
			// Adjust descendant count.
			moveTo(parent.getNodeKey());
			while (parent.hasParent()) {
				moveToParent();
				final StructNode ancestor = (StructNode) getPageTransaction()
						.prepareEntryForModification(
								mNodeRtx.getCurrentNode().getNodeKey(), page, -1,
								Optional.<UnorderedKeyValuePage> absent());
				ancestor.decrementDescendantCount();
				parent = ancestor;
			}
		}

		// Remove right sibling text node if text nodes have been
		// concatenated/merged.
		if (concatenated) {
			moveTo(oldNode.getRightSiblingKey());
			getPageTransaction().removeEntry(mNodeRtx.getNodeKey(), page, -1,
					Optional.<UnorderedKeyValuePage> absent());
		}

		// Remove non structural nodes of old node.
		if (oldNode.getKind() == Kind.ELEMENT) {
			moveTo(oldNode.getNodeKey());
			removeNonStructural();
		}

		// Remove old node.
		moveTo(oldNode.getNodeKey());
		getPageTransaction().removeEntry(oldNode.getNodeKey(), page, -1,
				Optional.<UnorderedKeyValuePage> absent());
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
	public PageWriteTrx<Long, Record, UnorderedKeyValuePage> getPageTransaction() {
		@SuppressWarnings("unchecked")
		final PageWriteTrx<Long, Record, UnorderedKeyValuePage> trx = (PageWriteTrx<Long, Record, UnorderedKeyValuePage>) mNodeRtx
				.getPageTransaction();
		return trx;
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
			case ROLLING:
				rollingAdd();
				break;
			case POSTORDER:
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
			case ROLLING:
				rollingRemove();
				break;
			case POSTORDER:
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
			case ROLLING:
				rollingUpdate(pOldHash);
				break;
			case POSTORDER:
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
		final ImmutableNode startNode = getCurrentNode();
		// long for adapting the hash of the parent
		long hashCodeForParent = 0;
		// adapting the parent if the current node is no structural one.
		if (!(startNode instanceof StructNode)) {
			final Node node = (Node) getPageTransaction()
					.prepareEntryForModification(mNodeRtx.getCurrentNode().getNodeKey(),
							PageKind.RECORDPAGE, -1, Optional.<UnorderedKeyValuePage> absent());
			node.setHash(mHash.hashLong(mNodeRtx.getCurrentNode().hashCode())
					.asLong());
			moveTo(mNodeRtx.getCurrentNode().getParentKey());
		}
		// Cursor to root
		StructNode cursorToRoot;
		do {
			cursorToRoot = (StructNode) getPageTransaction()
					.prepareEntryForModification(mNodeRtx.getCurrentNode().getNodeKey(),
							PageKind.RECORDPAGE, -1, Optional.<UnorderedKeyValuePage> absent());
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
			hashCodeForParent = 0;
		} while (moveTo(cursorToRoot.getParentKey()).hasMoved());

		mNodeRtx.setCurrentNode(startNode);
	}

	/**
	 * Adapting the structure with a rolling hash for all ancestors only with
	 * update.
	 * 
	 * @param oldHash
	 *          pOldHash to be removed
	 * @throws SirixIOException
	 *           if anything weird happened
	 */
	private void rollingUpdate(final long oldHash) throws SirixIOException {
		final ImmutableNode newNode = getCurrentNode();
		final long hash = newNode.hashCode();
		final long newNodeHash = hash;
		long resultNew = hash;

		// go the path to the root
		do {
			final Node node = (Node) getPageTransaction()
					.prepareEntryForModification(mNodeRtx.getCurrentNode().getNodeKey(),
							PageKind.RECORDPAGE, -1, Optional.<UnorderedKeyValuePage> absent());
			if (node.getNodeKey() == newNode.getNodeKey()) {
				resultNew = node.getHash() - oldHash;
				resultNew = resultNew + newNodeHash;
			} else {
				resultNew = node.getHash() - oldHash * PRIME;
				resultNew = resultNew + newNodeHash * PRIME;
			}
			node.setHash(resultNew);
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
		final ImmutableNode startNode = getCurrentNode();
		long hashToRemove = startNode.getHash();
		long hashToAdd = 0;
		long newHash = 0;
		// go the path to the root
		do {
			final Node node = (Node) getPageTransaction()
					.prepareEntryForModification(mNodeRtx.getCurrentNode().getNodeKey(),
							PageKind.RECORDPAGE, -1, Optional.<UnorderedKeyValuePage> absent());
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
		} while (moveTo(mNodeRtx.getCurrentNode().getParentKey()).hasMoved());

		mNodeRtx.setCurrentNode(startNode);
	}

	/**
	 * Set new descendant count of ancestor after a remove-operation.
	 * 
	 * @param startNode
	 *          the node which has been removed
	 */
	private void setRemoveDescendants(final ImmutableNode startNode) {
		assert startNode != null;
		if (startNode instanceof StructNode) {
			final StructNode node = ((StructNode) getCurrentNode());
			node.setDescendantCount(node.getDescendantCount()
					- ((StructNode) startNode).getDescendantCount() - 1);
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
		final ImmutableNode startNode = mNodeRtx.getCurrentNode();
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
					.prepareEntryForModification(mNodeRtx.getCurrentNode().getNodeKey(),
							PageKind.RECORDPAGE, -1, Optional.<UnorderedKeyValuePage> absent());
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
		} while (moveTo(mNodeRtx.getCurrentNode().getParentKey()).hasMoved());
		mNodeRtx.setCurrentNode(startNode);
	}

	/**
	 * Set new descendant count of ancestor after an add-operation.
	 * 
	 * @param startNode
	 *          the node which has been removed
	 * @param nodeToModify
	 *          node to modify
	 * @param descendantCount
	 *          the descendantCount to add
	 */
	private void setAddDescendants(final ImmutableNode startNode,
			final Node nodeToModifiy, final @Nonnegative long descendantCount) {
		assert startNode != null;
		assert descendantCount >= 0;
		assert nodeToModifiy != null;
		if (startNode instanceof StructNode) {
			final StructNode node = (StructNode) nodeToModifiy;
			final long oldDescendantCount = node.getDescendantCount();
			node.setDescendantCount(oldDescendantCount + descendantCount);
		}
	}

	@Override
	public NodeWriteTrx copySubtreeAsFirstChild(final NodeReadTrx rtx)
			throws SirixException {
		checkNotNull(rtx);
		acquireLock();
		try {
			checkAccessAndCommit();
			final long nodeKey = getCurrentNode().getNodeKey();
			copy(rtx, Insert.ASFIRSTCHILD);
			moveTo(nodeKey);
			moveToFirstChild();
		} finally {
			unLock();
		}
		return this;
	}

	@Override
	public NodeWriteTrx copySubtreeAsLeftSibling(final NodeReadTrx rtx)
			throws SirixException {
		checkNotNull(rtx);
		acquireLock();
		try {
			checkAccessAndCommit();
			final long nodeKey = getCurrentNode().getNodeKey();
			copy(rtx, Insert.ASLEFTSIBLING);
			moveTo(nodeKey);
			moveToFirstChild();
		} finally {
			unLock();
		}
		return this;
	}

	@Override
	public NodeWriteTrx copySubtreeAsRightSibling(final NodeReadTrx rtx)
			throws SirixException {
		checkNotNull(rtx);
		acquireLock();
		try {
			checkAccessAndCommit();
			final long nodeKey = getCurrentNode().getNodeKey();
			copy(rtx, Insert.ASRIGHTSIBLING);
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
	 * @param rtx
	 *          the source {@link NodeReadTrx}
	 * @param insert
	 *          the insertion strategy
	 * @throws SirixException
	 *           if anything fails in sirix
	 */
	private void copy(final NodeReadTrx trx, final Insert insert)
			throws SirixException {
		assert trx != null;
		assert insert != null;
		final NodeReadTrx rtx = trx.getSession().beginNodeReadTrx(
				trx.getRevisionNumber());
		assert rtx.getRevisionNumber() == trx.getRevisionNumber();
		rtx.moveTo(trx.getNodeKey());
		assert rtx.getNodeKey() == trx.getNodeKey();
		if (rtx.getKind() == Kind.DOCUMENT) {
			rtx.moveToFirstChild();
		}
		if (!(rtx.isStructuralNode())) {
			throw new IllegalStateException(
					"Node to insert must be a structural node (Text, PI, Comment, Document root or Element)!");
		}

		final Kind kind = rtx.getKind();
		switch (kind) {
		case TEXT:
			final String textValue = rtx.getValue();
			switch (insert) {
			case ASFIRSTCHILD:
				insertTextAsFirstChild(textValue);
				break;
			case ASLEFTSIBLING:
				insertTextAsLeftSibling(textValue);
				break;
			case ASRIGHTSIBLING:
				insertTextAsRightSibling(textValue);
				break;
			default:
				throw new IllegalStateException();
			}
			break;
		case PROCESSING_INSTRUCTION:
			switch (insert) {
			case ASFIRSTCHILD:
				insertPIAsFirstChild(rtx.getName().getLocalName(), rtx.getValue());
				break;
			case ASLEFTSIBLING:
				insertPIAsLeftSibling(rtx.getName().getLocalName(), rtx.getValue());
				break;
			case ASRIGHTSIBLING:
				insertPIAsRightSibling(rtx.getName().getLocalName(), rtx.getValue());
				break;
			default:
				throw new IllegalStateException();
			}
			break;
		case COMMENT:
			final String commentValue = rtx.getValue();
			switch (insert) {
			case ASFIRSTCHILD:
				insertCommentAsFirstChild(commentValue);
				break;
			case ASLEFTSIBLING:
				insertCommentAsLeftSibling(commentValue);
				break;
			case ASRIGHTSIBLING:
				insertCommentAsRightSibling(commentValue);
				break;
			default:
				throw new IllegalStateException();
			}
			break;
		default:
			new XMLShredder.Builder(this, new StAXSerializer(rtx), insert).build()
					.call();
		}
		rtx.close();
	}

	@Override
	public NodeWriteTrx replaceNode(final String xml)
			throws SirixException, IOException, XMLStreamException {
		checkNotNull(xml);
		acquireLock();
		try {
			checkAccessAndCommit();
			final XMLEventReader reader = XMLShredder
					.createStringReader(checkNotNull(xml));
			ImmutableNode insertedRootNode = null;
			if (getCurrentNode() instanceof StructNode) {
				final StructNode currentNode = mNodeRtx.getStructuralNode();

				if (xml.startsWith("<")) {
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
					insertedRootNode = replaceWithTextNode(xml);
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
	public NodeWriteTrx replaceNode(final NodeReadTrx rtx)
			throws SirixException {
		checkNotNull(rtx);
		acquireLock();
		try {
			switch (rtx.getKind()) {
			case ELEMENT:
			case TEXT:
				checkCurrentNode();
				replace(rtx);
				break;
			case ATTRIBUTE:
				if (getCurrentNode().getKind() != Kind.ATTRIBUTE) {
					throw new IllegalStateException(
							"Current node must be an attribute node!");
				}
				insertAttribute(rtx.getName(), rtx.getValue());
				break;
			case NAMESPACE:
				if (mNodeRtx.getCurrentNode().getClass() != NamespaceNode.class) {
					throw new IllegalStateException(
							"Current node must be a namespace node!");
				}
				insertNamespace(rtx.getName());
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
	 * @param value
	 *          text value
	 * @return inserted node
	 * @throws SirixException
	 *           if anything fails
	 */
	private ImmutableNode replaceWithTextNode(final String value)
			throws SirixException {
		assert value != null;
		final StructNode currentNode = mNodeRtx.getStructuralNode();
		long key = currentNode.getNodeKey();
		if (currentNode.hasLeftSibling()) {
			moveToLeftSibling();
			key = insertTextAsRightSibling(value).getNodeKey();
		} else {
			moveToParent();
			key = insertTextAsFirstChild(value).getNodeKey();
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
	 * @param rtx
	 *          the transaction which is located at the node to replace
	 * @return
	 * @throws SirixException
	 */
	private ImmutableNode replace(final NodeReadTrx rtx)
			throws SirixException {
		assert rtx != null;
		final StructNode currentNode = mNodeRtx.getStructuralNode();
		long key = currentNode.getNodeKey();
		if (currentNode.hasLeftSibling()) {
			moveToLeftSibling();
			key = copySubtreeAsRightSibling(rtx).getNodeKey();
		} else {
			moveToParent();
			key = copySubtreeAsFirstChild(rtx).getNodeKey();
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
	private ImmutableNode getCurrentNode() {
		return mNodeRtx.getCurrentNode();
	}

	/**
	 * 
	 * @param node
	 * @param key
	 * @throws SirixException
	 */
	private void removeReplaced(final StructNode node,
			@Nonnegative long key) throws SirixException {
		assert node != null;
		assert key >= 0;
		moveTo(node.getNodeKey());
		remove();
		moveTo(key);
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
	public void addPreCommitHook(final PreCommitHook hook) {
		acquireLock();
		try {
			mPreCommitHooks.add(checkNotNull(hook));
		} finally {
			unLock();
		}
	}

	@Override
	public void addPostCommitHook(final PostCommitHook hook) {
		acquireLock();
		try {
			mPostCommitHooks.add(checkNotNull(hook));
		} finally {
			unLock();
		}
	}

	@Override
	public boolean equals(final @Nullable Object obj) {
		if (obj instanceof NodeWriteTrxImpl) {
			final NodeWriteTrxImpl wtx = (NodeWriteTrxImpl) obj;
			return Objects.equal(mNodeRtx, wtx.mNodeRtx);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mNodeRtx);
	}

	@Override
	public PathSummaryReader getPathSummary() {
		acquireLock();
		try {
			return mPathSummaryWriter.getPathSummary();
		} finally {
			unLock();
		}
	}
}
