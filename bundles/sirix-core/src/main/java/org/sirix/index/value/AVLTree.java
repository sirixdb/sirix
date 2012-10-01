package org.sirix.index.value;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.access.Move;
import org.sirix.access.Moved;
import org.sirix.api.INodeCursor;
import org.sirix.api.IPageWriteTrx;
import org.sirix.api.visitor.EVisitResult;
import org.sirix.api.visitor.IVisitor;
import org.sirix.exception.SirixIOException;
import org.sirix.node.DocumentRootNode;
import org.sirix.node.EKind;
import org.sirix.node.NullNode;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.interfaces.INode;
import org.sirix.node.interfaces.INodeBase;
import org.sirix.node.interfaces.IStructNode;
import org.sirix.page.EPage;
import org.sirix.page.RevisionRootPage;
import org.sirix.settings.EFixed;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

/**
 * Simple AVLTree (balanced binary search-tree -- based on BaseX(.org) version).
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 * @param <K>
 *          the key to search for or insert
 * @param <V>
 *          the value
 */
public class AVLTree<K extends Comparable<? super K>, V> implements INodeCursor {

	/** {@link LogWrapper} reference. */
	private static final LogWrapper LOGWRAPPER = new LogWrapper(
			LoggerFactory.getLogger(AVLTree.class));

	/** Tree root node. */
	private AVLNode<K, V> mRoot;

	/** Number of indexed tokens. */
	private int mSize;

	/** Determines if tree is closed or not. */
	private boolean mClosed;

	/** Strong reference to currently selected node. */
	private INode mCurrentNode;

	/** {@link IPageWriteTrx} for persistent storage. */
	private final IPageWriteTrx mPageWriteTrx;

	/**
	 * Private constructor.
	 * 
	 * @param pPageWriteTrx
	 *          {@link IPageWriteTrx} for persistent storage
	 */
	private AVLTree(final @Nonnull IPageWriteTrx pPageWriteTrx) {
		mPageWriteTrx = pPageWriteTrx;
		mClosed = false;

		try {
			Optional<? extends INodeBase> node = mPageWriteTrx.getNode(
					EFixed.DOCUMENT_NODE_KEY.getStandardProperty(), EPage.VALUEPAGE);
			if (node.isPresent()) {
				mCurrentNode = (INode) node.get();
			} else {
				throw new IllegalStateException(
						"Node couldn't be fetched from persistent storage!");
			}
		} catch (final SirixIOException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}
	}

	/**
	 * Get a new instance.
	 * 
	 * @param pPageWriteTrx
	 *          {@link IPageWriteTrx} for persistent storage
	 * @return new tree instance
	 */
	public static <KE extends Comparable<? super KE>, VA> AVLTree<KE, VA> getInstance(
			final @Nonnull IPageWriteTrx pPageWriteTrx) {
		return new AVLTree<KE, VA>(checkNotNull(pPageWriteTrx));
	}

	/**
	 * Checks if the specified token is already indexed; if yes, returns its
	 * reference. Otherwise, creates a new index entry and returns a reference of
	 * the indexed token.
	 * 
	 * @param token
	 *          token to be indexed
	 * @return indexed token reference
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	@SuppressWarnings("unchecked")
	public V index(final @Nonnull K pKey, final @Nonnull V pValue)
			throws SirixIOException {
		final RevisionRootPage root = mPageWriteTrx.getActualRevisionRootPage();
		if (mRoot == null) {
			// Index is empty.. create root node.
			mRoot = (AVLNode<K, V>) mPageWriteTrx.createNode(new AVLNode<>(pKey,
					pValue, new NodeDelegate(root.getMaxValueNodeKey() + 1,
							EFixed.NULL_NODE_KEY.getStandardProperty(), 0, 0)),
					EPage.VALUEPAGE);
			final DocumentRootNode document = (DocumentRootNode) mPageWriteTrx
					.prepareNodeForModification(
							EFixed.DOCUMENT_NODE_KEY.getStandardProperty(), EPage.VALUEPAGE);
			document.setFirstChildKey(mRoot.getNodeKey());
			mPageWriteTrx.finishNodeModification(document.getNodeKey(),
					EPage.VALUEPAGE);
			mSize++;
			return pValue;
		}

		AVLNode<K, V> node = mRoot;
		while (true) {
			final int c = pKey.compareTo(node.getKey());
			if (c == 0) {
				return node.getValue();
			}

			final boolean moved = c < 0 ? moveToLeftSibling().hasMoved()
					: moveToRightSibling().hasMoved();
			if (moved) {
				node = getAVLNode();
				continue;
			}

			final AVLNode<K, V> child = (AVLNode<K, V>) mPageWriteTrx.createNode(
					new AVLNode<>(pKey, pValue, new NodeDelegate(root
							.getMaxValueNodeKey(),
							EFixed.NULL_NODE_KEY.getStandardProperty(), 0, 0)),
					EPage.VALUEPAGE);
			node = (AVLNode<K, V>) mPageWriteTrx.prepareNodeForModification(
					node.getNodeKey(), EPage.VALUEPAGE);
			if (c < 0) {
				node.setLeftChildKey(child.getNodeKey());
				adjust(child);
			} else {
				node.setRightChildKey(child.getNodeKey());
				adjust(child);
			}
			mPageWriteTrx.finishNodeModification(node.getNodeKey(), EPage.VALUEPAGE);
			mSize++;
			return pValue;
		}
	}

	/**
	 * Get the {@link AVLNode}.
	 * 
	 * @return {@link AVLNode} instance
	 */
	private AVLNode<K, V> getAVLNode() {
		if (mCurrentNode.getKind() == EKind.AVL) {
			@SuppressWarnings("unchecked")
			final AVLNode<K, V> node = (AVLNode<K, V>) mCurrentNode;
			return node;
		}
		return null;
	}

	/**
	 * Finds the specified key in the index and returns its value.
	 * 
	 * @param pKey
	 *          key to be found
	 * @return {@link Optional} reference
	 */
	public Optional<V> get(final @Nonnull K pKey) {
		if (mRoot == null) {
			return Optional.absent();
		}
		AVLNode<K, V> node = mRoot;
		while (true) {
			int c = pKey.compareTo(node.getKey());
			if (c == 0) {
				return Optional.fromNullable(node.getValue());
			}
			final boolean moved = c < 0 ? moveToLeftSibling().hasMoved()
					: moveToRightSibling().hasMoved();
			if (moved) {
				node = getAVLNode();
			} else {
				break;
			}
		}
		return Optional.absent();
	}

	/**
	 * Returns the number of index entries.
	 * 
	 * @return number of index entries
	 */
	public int size() {
		return mSize;
	}

	/**
	 * Adjusts the tree balance.
	 * 
	 * @param pNode
	 *          node to be adjusted
	 * @throws SirixIOException
	 */
	private void adjust(@Nonnull AVLNode<K, V> pNode) throws SirixIOException {
		pNode.setChanged(true);

		while (pNode != null && pNode != mRoot && parent(pNode) != null
				&& parent(pNode).isChanged()) {
			if (parent(pNode) == left(parent(parent(pNode)))) {
				AVLNode<K, V> y = right(parent(parent(pNode)));
				if (y != null && y.isChanged()) {
					setChanged(parent(pNode), false);
					y.setChanged(false);
					setChanged(parent(parent(pNode)), true);
					pNode = parent(parent(pNode));
				} else {
					if (pNode == right(parent(pNode))) {
						pNode = parent(pNode);
						rotateLeft(pNode);
					}
					setChanged(parent(pNode), false);
					setChanged(parent(parent(pNode)), true);
					if (parent(parent(pNode)) != null)
						rotateRight(parent(parent(pNode)));
				}
			} else {
				AVLNode<K, V> y = left(parent(parent(pNode)));
				if (y != null && y.isChanged()) {
					setChanged(parent(pNode), false);
					setChanged(y, false);
					setChanged(parent(parent(pNode)), true);
					pNode = parent(parent(pNode));
				} else {
					if (pNode == left(parent(pNode))) {
						pNode = parent(pNode);
						rotateRight(pNode);
					}
					setChanged(parent(pNode), false);
					setChanged(parent(parent(pNode)), true);
					if (parent(parent(pNode)) != null)
						rotateLeft(parent(parent(pNode)));
				}
			}
		}
		mRoot.setChanged(false);
	}

	/**
	 * Set changed value.
	 * 
	 * @param pNode
	 *          node to adjust
	 * @param pChanged
	 *          changed value
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	private void setChanged(final @Nonnull AVLNode<K, V> pNode,
			final boolean pChanged) throws SirixIOException {
		@SuppressWarnings("unchecked")
		final AVLNode<K, V> node = (AVLNode<K, V>) mPageWriteTrx
				.prepareNodeForModification(pNode.getNodeKey(), EPage.VALUEPAGE);
		node.setChanged(pChanged);
		mPageWriteTrx.finishNodeModification(node.getNodeKey(), EPage.VALUEPAGE);
	}

	/**
	 * Returns the left child node.
	 * 
	 * @param node
	 * @return left child node
	 */
	private AVLNode<K, V> left(@Nullable AVLNode<K, V> pNode) {
		if (pNode == null)
			return null;
		return moveTo(pNode.getLeftChildKey()).hasMoved() ? getAVLNode() : null;
	}

	/**
	 * Returns the right child node.
	 * 
	 * @param node
	 * @return right child node
	 */
	private AVLNode<K, V> right(@Nullable AVLNode<K, V> pNode) {
		if (pNode == null)
			return null;
		return moveTo(pNode.getRightChildKey()).hasMoved() ? getAVLNode() : null;
	}

	/**
	 * Returns the parent node.
	 * 
	 * @param pNode
	 *          get the parent of the current node
	 * @return parent sub node
	 */
	private AVLNode<K, V> parent(@Nullable AVLNode<K, V> pNode) {
		if (pNode == null) {
			return null;
		}
		return moveTo(pNode.getParentKey()).hasMoved() ? getAVLNode() : null;
	}

	/**
	 * Left rotation.
	 * 
	 * @param node
	 *          node to be rotated
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	@SuppressWarnings({ "unchecked" })
	private void rotateLeft(@Nonnull AVLNode<K, V> pNode) throws SirixIOException {
		moveTo(pNode.getNodeKey());

		AVLNode<K, V> right = (AVLNode<K, V>) moveToLastChild().get().getAVLNode();

		pNode = (AVLNode<K, V>) mPageWriteTrx.prepareNodeForModification(
				pNode.getNodeKey(), EPage.VALUEPAGE);
		pNode.setRightChildKey(right.getLeftChildKey());
		mPageWriteTrx.finishNodeModification(pNode.getNodeKey(), EPage.VALUEPAGE);

		if (right.hasLeftChild()) {
			final AVLNode<K, V> rightLeftChild = (AVLNode<K, V>) mPageWriteTrx
					.prepareNodeForModification(right.getLeftChildKey(), EPage.VALUEPAGE);
			rightLeftChild.setParentKey(pNode.getNodeKey());
			mPageWriteTrx.finishNodeModification(rightLeftChild.getNodeKey(),
					EPage.VALUEPAGE);
		}

		right = (AVLNode<K, V>) mPageWriteTrx.prepareNodeForModification(
				right.getNodeKey(), EPage.VALUEPAGE);
		right.setParentKey(pNode.getParentKey());
		mPageWriteTrx.finishNodeModification(right.getNodeKey(), EPage.VALUEPAGE);

		if (pNode.getParentKey() == EFixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
			mRoot = right;
		} else if (moveTo(pNode.getParentKey()).hasMoved()
				&& getAVLNode().getLeftChildKey() == pNode.getNodeKey()) {
			final AVLNode<K, V> parent = (AVLNode<K, V>) mPageWriteTrx
					.prepareNodeForModification(mCurrentNode.getNodeKey(),
							EPage.VALUEPAGE);
			parent.setLeftChildKey(right.getNodeKey());
			mPageWriteTrx
					.finishNodeModification(parent.getNodeKey(), EPage.VALUEPAGE);
		} else {
			final AVLNode<K, V> parent = (AVLNode<K, V>) mPageWriteTrx
					.prepareNodeForModification(mCurrentNode.getNodeKey(),
							EPage.VALUEPAGE);
			parent.setRightChildKey(right.getNodeKey());
			mPageWriteTrx
					.finishNodeModification(parent.getNodeKey(), EPage.VALUEPAGE);
		}

		right = (AVLNode<K, V>) mPageWriteTrx.prepareNodeForModification(
				right.getNodeKey(), EPage.VALUEPAGE);
		right.setLeftChildKey(pNode.getNodeKey());
		mPageWriteTrx.finishNodeModification(right.getNodeKey(), EPage.VALUEPAGE);

		pNode = (AVLNode<K, V>) mPageWriteTrx.prepareNodeForModification(
				pNode.getNodeKey(), EPage.VALUEPAGE);
		pNode.setParentKey(right.getNodeKey());
		mPageWriteTrx.finishNodeModification(pNode.getNodeKey(), EPage.VALUEPAGE);
	}

	/**
	 * Right rotation.
	 * 
	 * @param node
	 *          node to be rotated
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	@SuppressWarnings({ "unchecked" })
	private void rotateRight(@Nonnull AVLNode<K, V> pNode)
			throws SirixIOException {
		moveTo(pNode.getNodeKey());

		AVLNode<K, V> leftChild = (AVLNode<K, V>) moveToFirstChild().get()
				.getAVLNode();
		pNode = (AVLNode<K, V>) mPageWriteTrx.prepareNodeForModification(
				pNode.getNodeKey(), EPage.VALUEPAGE);
		pNode.setLeftChildKey(leftChild.getRightChildKey());
		mPageWriteTrx.finishNodeModification(pNode.getNodeKey(), EPage.VALUEPAGE);

		if (leftChild.hasRightChild()) {
			final INode leftRightChild = (INode) mPageWriteTrx
					.prepareNodeForModification(leftChild.getRightChildKey(),
							EPage.VALUEPAGE);
			leftRightChild.setParentKey(pNode.getNodeKey());
			mPageWriteTrx.finishNodeModification(leftRightChild.getNodeKey(),
					EPage.VALUEPAGE);
		}

		leftChild = (AVLNode<K, V>) mPageWriteTrx.prepareNodeForModification(
				leftChild.getNodeKey(), EPage.VALUEPAGE);
		leftChild.setParentKey(pNode.getParentKey());
		mPageWriteTrx.finishNodeModification(leftChild.getNodeKey(),
				EPage.VALUEPAGE);

		if (pNode.getParentKey() == EFixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
			mRoot = leftChild;
		} else if (moveTo(pNode.getParentKey()).hasMoved()
				&& getAVLNode().getRightChildKey() == pNode.getNodeKey()) {
			final AVLNode<K, V> parent = (AVLNode<K, V>) mPageWriteTrx
					.prepareNodeForModification(mCurrentNode.getNodeKey(),
							EPage.VALUEPAGE);
			parent.setRightChildKey(leftChild.getNodeKey());
			mPageWriteTrx
					.finishNodeModification(parent.getNodeKey(), EPage.VALUEPAGE);
		} else {
			final AVLNode<K, V> parent = (AVLNode<K, V>) mPageWriteTrx
					.prepareNodeForModification(mCurrentNode.getNodeKey(),
							EPage.VALUEPAGE);
			parent.setLeftChildKey(leftChild.getNodeKey());
			mPageWriteTrx
					.finishNodeModification(parent.getNodeKey(), EPage.VALUEPAGE);
		}

		leftChild = (AVLNode<K, V>) mPageWriteTrx.prepareNodeForModification(
				leftChild.getNodeKey(), EPage.VALUEPAGE);
		leftChild.setRightChildKey(pNode.getNodeKey());
		mPageWriteTrx.finishNodeModification(leftChild.getNodeKey(),
				EPage.VALUEPAGE);

		pNode = (AVLNode<K, V>) mPageWriteTrx.prepareNodeForModification(
				pNode.getNodeKey(), EPage.VALUEPAGE);
		pNode.setParentKey(leftChild.getNodeKey());
		mPageWriteTrx.finishNodeModification(pNode.getNodeKey(), EPage.VALUEPAGE);
	}

	@Override
	public void close() throws Exception {
		mClosed = true;
	}

	/**
	 * Make sure that the path summary is not yet closed when calling this method.
	 */
	final void assertNotClosed() {
		if (mClosed) {
			throw new IllegalStateException("Path summary is already closed.");
		}
	}

	/**
	 * Set current node.
	 * 
	 * @param pNode
	 */
	public void setCurrentNode(final @Nonnull AVLNode<K, V> pNode) {
		mCurrentNode = checkNotNull(pNode);
	}

	private IStructNode getStructuralNode() {
		if (mCurrentNode instanceof IStructNode) {
			return (IStructNode) mCurrentNode;
		} else {
			return new NullNode(mCurrentNode);
		}
	}

	@Override
	public boolean hasNode(long pKey) {
		final long currKey = mCurrentNode.getNodeKey();
		final boolean moved = moveTo(pKey).hasMoved();
		final Move<AVLTree<K, V>> movedCursor = moveTo(currKey);
		assert movedCursor.hasMoved() == true : "Must be moveable back!";
		return moved;
	}

	@Override
	public boolean hasParent() {
		return mCurrentNode.hasParent();
	}

	@Override
	public boolean hasFirstChild() {
		return getStructuralNode().hasFirstChild();
	}

	@Override
	public boolean hasLastChild() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean hasLeftSibling() {
		return getStructuralNode().hasLeftSibling();
	}

	@Override
	public boolean hasRightSibling() {
		return getStructuralNode().hasRightSibling();
	}

	@Override
	public EVisitResult acceptVisitor(@Nonnull IVisitor pVisitor) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Move<AVLTree<K, V>> moveTo(final long pNodeKey) {
		assertNotClosed();

		// Remember old node and fetch new one.
		final INode oldNode = mCurrentNode;
		Optional<? extends INode> newNode;
		try {
			// Immediately return node from item list if node key negative.
			@SuppressWarnings("unchecked")
			final Optional<? extends INode> node = (Optional<? extends INode>) mPageWriteTrx
					.getNode(pNodeKey, EPage.PATHSUMMARYPAGE);
			newNode = node;
		} catch (final SirixIOException e) {
			newNode = Optional.absent();
		}

		if (newNode.isPresent()) {
			mCurrentNode = newNode.get();
			return Move.moved(this);
		} else {
			mCurrentNode = oldNode;
			return Move.notMoved();
		}
	}

	@Override
	public Move<AVLTree<K, V>> moveToDocumentRoot() {
		return moveTo(EFixed.DOCUMENT_NODE_KEY.getStandardProperty());
	}

	@Override
	public Move<AVLTree<K, V>> moveToParent() {
		return moveTo(mCurrentNode.getParentKey());
	}

	@Override
	public Move<AVLTree<K, V>> moveToFirstChild() {
		assertNotClosed();
		final IStructNode node = getStructuralNode();
		if (!node.hasFirstChild()) {
			return Moved.notMoved();
		}
		return moveTo(node.getFirstChildKey());
	}

	@Override
	public Move<AVLTree<K, V>> moveToLastChild() {
		if (mCurrentNode instanceof AVLNode) {
			final AVLNode<K, V> node = getAVLNode();
			if (!node.hasRightChild()) {
				return Moved.notMoved();
			}
			return moveTo(node.getRightChildKey());
		}
		return Move.notMoved();
	}

	@Override
	public Move<AVLTree<K, V>> moveToLeftSibling() {
		return Move.notMoved();
	}

	@Override
	public Move<AVLTree<K, V>> moveToRightSibling() {
		return Move.notMoved();
	}

	@Override
	public long getNodeKey() {
		return mCurrentNode.getNodeKey();
	}

	@Override
	public EKind getRightSiblingKind() {
		return EKind.UNKNOWN;
	}

	@Override
	public EKind getLeftSiblingKind() {
		return EKind.UNKNOWN;
	}

	@Override
	public EKind getFirstChildKind() {
		return EKind.AVL;
	}

	@Override
	public EKind getLastChildKind() {
		return EKind.AVL;
	}

	@Override
	public EKind getParentKind() {
		if (mCurrentNode.getKind() == EKind.DOCUMENT_ROOT) {
			return EKind.DOCUMENT_ROOT;
		}
		return EKind.AVL;
	}
	
	@Override
	public EKind getKind() {
		return EKind.AVL;
	}
}
