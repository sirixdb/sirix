package org.sirix.index.value;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayDeque;
import java.util.Deque;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.access.Move;
import org.sirix.access.Moved;
import org.sirix.api.NodeCursor;
import org.sirix.api.PageWriteTrx;
import org.sirix.api.visitor.VisitResultType;
import org.sirix.api.visitor.Visitor;
import org.sirix.exception.SirixIOException;
import org.sirix.index.SearchMode;
import org.sirix.node.DocumentRootNode;
import org.sirix.node.Kind;
import org.sirix.node.NullNode;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.ValueKind;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.page.PageKind;
import org.sirix.page.RevisionRootPage;
import org.sirix.settings.Fixed;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.AbstractIterator;

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
public final class AVLTree<K extends Comparable<? super K>, V>
		implements NodeCursor {

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
	private Node mCurrentNode;

	/** {@link PageWriteTrx} for persistent storage. */
	private final PageWriteTrx mPageWriteTrx;

	/** Page kind. */
	private final PageKind mPageKind;

	public enum MoveCursor {
		TO_DOCUMENT_ROOT,

		NO_MOVE
	}

	/**
	 * Private constructor.
	 * 
	 * @param pageWriteTrx
	 *          {@link PageWriteTrx} for persistent storage
	 * @param kind
	 *          kind of value (attribute/text)
	 */
	private AVLTree(final @Nonnull PageWriteTrx pageWriteTrx,
			final @Nonnull ValueKind kind) {
		mPageWriteTrx = checkNotNull(pageWriteTrx);
		switch (kind) {
		case ATTRIBUTE:
			mPageKind = PageKind.ATTRIBUTEVALUEPAGE;
			break;
		case TEXT:
			mPageKind = PageKind.TEXTVALUEPAGE;
			break;
		default:
			mPageKind = null;
			throw new IllegalStateException();
		}
		mClosed = false;

		try {
			Optional<? extends Record> node = mPageWriteTrx.getNode(
					Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), mPageKind);
			if (node.isPresent()) {
				mCurrentNode = (Node) node.get();
			} else {
				throw new IllegalStateException(
						"Node couldn't be fetched from persistent storage!");
			}
		} catch (final SirixIOException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}

		if (((DocumentRootNode) mCurrentNode).hasFirstChild()) {
			mRoot = moveToFirstChild().get().getAVLNode();
		}
	}

	/**
	 * Get a new instance.
	 * 
	 * @param pageWriteTrx
	 *          {@link PageWriteTrx} for persistent storage
	 * @param kind
	 *          kind of value (attribute/text)
	 * @return new tree instance
	 */
	public static <KE extends Comparable<? super KE>, VA> AVLTree<KE, VA> getInstance(
			final @Nonnull PageWriteTrx pageWriteTrx, final @Nonnull ValueKind kind) {
		return new AVLTree<KE, VA>(pageWriteTrx, kind);
	}

	/**
	 * Checks if the specified token is already indexed; if yes, returns its
	 * reference. Otherwise, creates a new index entry and returns a reference of
	 * the indexed token.
	 * 
	 * @param key
	 *          token to be indexed
	 * @param value
	 *          node key references
	 * @param move
	 *          determines if AVLNode cursor must be moved to document root/root
	 *          node or not
	 * @return indexed node key references
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	@SuppressWarnings("unchecked")
	public V index(final @Nonnull K key, final @Nonnull V value,
			final @Nonnull MoveCursor move) throws SirixIOException {
		if (move == MoveCursor.TO_DOCUMENT_ROOT) {
			moveToDocumentRoot();
		}
		final RevisionRootPage root = mPageWriteTrx.getActualRevisionRootPage();
		if (mRoot == null) {
			// Index is empty.. create root node.
			final long nodeKey = mPageKind == PageKind.TEXTVALUEPAGE ? root
					.getMaxTextValueNodeKey() + 1
					: root.getMaxAttributeValueNodeKey() + 1;
			mRoot = (AVLNode<K, V>) mPageWriteTrx.createNode(
					new AVLNode<>(key, value, new NodeDelegate(nodeKey,
							Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), 0, 0, Optional
									.<SirixDeweyID> absent())), mPageKind);
			final DocumentRootNode document = (DocumentRootNode) mPageWriteTrx
					.prepareNodeForModification(
							Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), mPageKind);
			document.setFirstChildKey(mRoot.getNodeKey());
			document.incrementChildCount();
			document.incrementDescendantCount();
			mPageWriteTrx.finishNodeModification(document.getNodeKey(), mPageKind);
			mSize++;
			return value;
		}

		if (move == MoveCursor.TO_DOCUMENT_ROOT || getAVLNode() == null) {
			moveTo(mRoot.getNodeKey());
		}
		AVLNode<K, V> node = move == MoveCursor.TO_DOCUMENT_ROOT ? mRoot
				: getAVLNode();
		while (true) {
			final int c = key.compareTo(node.getKey());
			if (c == 0) {
				if (!value.equals(node.getValue())) {
					final AVLNode<K, V> avlNode = (AVLNode<K, V>) mPageWriteTrx
							.prepareNodeForModification(node.getNodeKey(), mPageKind);
					avlNode.setValue(value);
					mPageWriteTrx.finishNodeModification(avlNode.getNodeKey(), mPageKind);
				}
				return node.getValue();
			}

			final boolean moved = c < 0 ? moveToFirstChild().hasMoved()
					: moveToLastChild().hasMoved();
			if (moved) {
				node = getAVLNode();
				continue;
			}

			final long nodeKey = mPageKind == PageKind.TEXTVALUEPAGE ? root
					.getMaxTextValueNodeKey() + 1
					: root.getMaxAttributeValueNodeKey() + 1;
			final AVLNode<K, V> child = (AVLNode<K, V>) mPageWriteTrx.createNode(
					new AVLNode<>(key, value, new NodeDelegate(nodeKey,
							Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0, Optional
									.<SirixDeweyID> absent())), mPageKind);
			node = (AVLNode<K, V>) mPageWriteTrx.prepareNodeForModification(
					node.getNodeKey(), mPageKind);
			if (c < 0) {
				node.setLeftChildKey(child.getNodeKey());
				adjust(child);
			} else {
				node.setRightChildKey(child.getNodeKey());
				adjust(child);
			}
			mPageWriteTrx.finishNodeModification(node.getNodeKey(), mPageKind);
			mSize++;
			return value;
		}
	}

	/**
	 * Get the {@link AVLNode}.
	 * 
	 * @return {@link AVLNode} instance
	 */
	private AVLNode<K, V> getAVLNode() {
		if (mCurrentNode.getKind() == Kind.AVL) {
			@SuppressWarnings("unchecked")
			final AVLNode<K, V> node = (AVLNode<K, V>) mCurrentNode;
			return node;
		}
		return null;
	}

	/**
	 * Finds the specified key in the index and returns its value.
	 * 
	 * @param key
	 *          key to be found
	 * @param mode
	 *          the search mode
	 * @return {@link Optional} reference (with the found value, or a reference
	 *         which indicates that the value hasn't been found)
	 */
	public Optional<V> get(final @Nonnull K key, final @Nonnull SearchMode mode) {
		if (mRoot == null) {
			return Optional.absent();
		}
		moveTo(mRoot.getNodeKey());
		AVLNode<K, V> node = mRoot;
		while (true) {
			final int c = mode.compare(key, node.getKey());
			if (c == 0) {
				return Optional.fromNullable(node.getValue());
			}
			final boolean moved = c < 0 ? moveToFirstChild().hasMoved()
					: moveToLastChild().hasMoved();
			if (moved) {
				node = getAVLNode();
			} else {
				break;
			}
		}
		return Optional.absent();
	}

	/**
	 * Iterator supporting different search modes.
	 * 
	 * @author Johannes Lichtenberger
	 * 
	 */
	public final class AVLIterator extends AbstractIterator<Optional<V>> {

		/** The key to search. */
		private final K mKey;

		/** Determines if it's the first call. */
		private boolean mFirst;

		/** All AVLNode keys which are part of the result sequence. */
		private final Deque<Long> mKeys;

		/** Search mode. */
		private final SearchMode mMode;

		/**
		 * Constructor.
		 * 
		 * @param key
		 *          the key to search for
		 * @param mode
		 *          the search mode
		 */
		public AVLIterator(final @Nonnull K key, final @Nonnull SearchMode mode) {
			mKey = checkNotNull(key);
			mFirst = true;
			mKeys = new ArrayDeque<>();
			mMode = checkNotNull(mode);
		}

		@Override
		protected Optional<V> computeNext() {
			if (!mFirst && mMode == SearchMode.EQUAL) {
				return endOfData();
			}
			if (!mFirst) {
				if (!mKeys.isEmpty()) {
					// Subsequent results.
					final AVLNode<K, V> node = moveTo(mKeys.pop()).get().getAVLNode();
					if (node.hasRightChild()) {
						final AVLNode<K, V> right = moveToLastChild().get().getAVLNode();
						mKeys.push(right.getNodeKey());
					}
					if (node.hasLeftChild()) {
						final AVLNode<K, V> left = moveToFirstChild().get().getAVLNode();
						mKeys.push(left.getNodeKey());
					}

					return Optional.of(node.getValue());
				}
				return endOfData();
			}

			// First search.
			final Optional<V> result = get(mKey, mMode);
			mFirst = false;
			if (result.isPresent()) {
				mKeys.push(getAVLNode().getNodeKey());
				return result;
			}
			return endOfData();
		}
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
	 * @param node
	 *          node to be adjusted
	 * @throws SirixIOException
	 */
	private void adjust(@Nonnull AVLNode<K, V> node) throws SirixIOException {
		node.setChanged(true);

		while (node != null && node != mRoot && parent(node) != null
				&& parent(node).isChanged()) {
			if (parent(node) == left(parent(parent(node)))) {
				AVLNode<K, V> y = right(parent(parent(node)));
				if (y != null && y.isChanged()) {
					setChanged(parent(node), false);
					y.setChanged(false);
					setChanged(parent(parent(node)), true);
					node = parent(parent(node));
				} else {
					if (node == right(parent(node))) {
						node = parent(node);
						rotateLeft(node);
					}
					setChanged(parent(node), false);
					setChanged(parent(parent(node)), true);
					if (parent(parent(node)) != null) rotateRight(parent(parent(node)));
				}
			} else {
				AVLNode<K, V> y = left(parent(parent(node)));
				if (y != null && y.isChanged()) {
					setChanged(parent(node), false);
					setChanged(y, false);
					setChanged(parent(parent(node)), true);
					node = parent(parent(node));
				} else {
					if (node == left(parent(node))) {
						node = parent(node);
						rotateRight(node);
					}
					setChanged(parent(node), false);
					setChanged(parent(parent(node)), true);
					if (parent(parent(node)) != null) rotateLeft(parent(parent(node)));
				}
			}
		}
		mRoot.setChanged(false);
	}

	/**
	 * Set changed value.
	 * 
	 * @param nodeToChange
	 *          node to adjust
	 * @param changed
	 *          changed value
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	private void setChanged(final @Nonnull AVLNode<K, V> nodeToChange,
			final boolean changed) throws SirixIOException {
		@SuppressWarnings("unchecked")
		final AVLNode<K, V> node = (AVLNode<K, V>) mPageWriteTrx
				.prepareNodeForModification(nodeToChange.getNodeKey(), mPageKind);
		node.setChanged(changed);
		mPageWriteTrx.finishNodeModification(node.getNodeKey(), mPageKind);
	}

	/**
	 * Returns the left child node.
	 * 
	 * @param node
	 *          node from which to move to and return the left sibling
	 * @return left child node
	 */
	private AVLNode<K, V> left(@Nullable AVLNode<K, V> node) {
		if (node == null
				|| node.getLeftChildKey() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
			return null;
		}
		return moveTo(node.getLeftChildKey()).hasMoved() ? getAVLNode() : null;
	}

	/**
	 * Returns the right child node.
	 * 
	 * @param node
	 *          node from which to move to and return the right sibling
	 * @return right child node
	 */
	private AVLNode<K, V> right(@Nullable AVLNode<K, V> node) {
		if (node == null
				|| node.getRightChildKey() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
			return null;
		}
		return moveTo(node.getRightChildKey()).hasMoved() ? getAVLNode() : null;
	}

	/**
	 * Returns the parent node.
	 * 
	 * @param node
	 *          node from which to move to and return the parent node
	 * @return parent node
	 */
	private AVLNode<K, V> parent(@Nullable AVLNode<K, V> node) {
		if (node == null
				|| node.getParentKey() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
			return null;
		}
		return moveTo(node.getParentKey()).hasMoved() ? getAVLNode() : null;
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
	private void rotateLeft(@Nonnull AVLNode<K, V> node) throws SirixIOException {
		moveTo(node.getNodeKey());

		AVLNode<K, V> right = (AVLNode<K, V>) moveToLastChild().get().getAVLNode();

		node = (AVLNode<K, V>) mPageWriteTrx.prepareNodeForModification(
				node.getNodeKey(), mPageKind);
		node.setRightChildKey(right.getLeftChildKey());
		mPageWriteTrx.finishNodeModification(node.getNodeKey(), mPageKind);

		if (right.hasLeftChild()) {
			final AVLNode<K, V> rightLeftChild = (AVLNode<K, V>) mPageWriteTrx
					.prepareNodeForModification(right.getLeftChildKey(), mPageKind);
			rightLeftChild.setParentKey(node.getNodeKey());
			mPageWriteTrx.finishNodeModification(rightLeftChild.getNodeKey(),
					mPageKind);
		}

		right = (AVLNode<K, V>) mPageWriteTrx.prepareNodeForModification(
				right.getNodeKey(), mPageKind);
		right.setParentKey(node.getParentKey());
		mPageWriteTrx.finishNodeModification(right.getNodeKey(), mPageKind);

		if (node.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
			mRoot = right;
		} else if (moveTo(node.getParentKey()).hasMoved()
				&& getAVLNode().getLeftChildKey() == node.getNodeKey()) {
			final AVLNode<K, V> parent = (AVLNode<K, V>) mPageWriteTrx
					.prepareNodeForModification(mCurrentNode.getNodeKey(), mPageKind);
			parent.setLeftChildKey(right.getNodeKey());
			mPageWriteTrx.finishNodeModification(parent.getNodeKey(), mPageKind);
		} else {
			final AVLNode<K, V> parent = (AVLNode<K, V>) mPageWriteTrx
					.prepareNodeForModification(mCurrentNode.getNodeKey(), mPageKind);
			parent.setRightChildKey(right.getNodeKey());
			mPageWriteTrx.finishNodeModification(parent.getNodeKey(), mPageKind);
		}

		right = (AVLNode<K, V>) mPageWriteTrx.prepareNodeForModification(
				right.getNodeKey(), mPageKind);
		right.setLeftChildKey(node.getNodeKey());
		mPageWriteTrx.finishNodeModification(right.getNodeKey(), mPageKind);

		node = (AVLNode<K, V>) mPageWriteTrx.prepareNodeForModification(
				node.getNodeKey(), mPageKind);
		node.setParentKey(right.getNodeKey());
		mPageWriteTrx.finishNodeModification(node.getNodeKey(), mPageKind);
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
	private void rotateRight(@Nonnull AVLNode<K, V> node) throws SirixIOException {
		moveTo(node.getNodeKey());

		AVLNode<K, V> leftChild = (AVLNode<K, V>) moveToFirstChild().get()
				.getAVLNode();
		node = (AVLNode<K, V>) mPageWriteTrx.prepareNodeForModification(
				node.getNodeKey(), mPageKind);
		node.setLeftChildKey(leftChild.getRightChildKey());
		mPageWriteTrx.finishNodeModification(node.getNodeKey(), mPageKind);

		if (leftChild.hasRightChild()) {
			final Node leftRightChild = (Node) mPageWriteTrx
					.prepareNodeForModification(leftChild.getRightChildKey(), mPageKind);
			leftRightChild.setParentKey(node.getNodeKey());
			mPageWriteTrx.finishNodeModification(leftRightChild.getNodeKey(),
					mPageKind);
		}

		leftChild = (AVLNode<K, V>) mPageWriteTrx.prepareNodeForModification(
				leftChild.getNodeKey(), mPageKind);
		leftChild.setParentKey(node.getParentKey());
		mPageWriteTrx.finishNodeModification(leftChild.getNodeKey(), mPageKind);

		if (node.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
			mRoot = leftChild;
		} else if (moveTo(node.getParentKey()).hasMoved()
				&& getAVLNode().getRightChildKey() == node.getNodeKey()) {
			final AVLNode<K, V> parent = (AVLNode<K, V>) mPageWriteTrx
					.prepareNodeForModification(mCurrentNode.getNodeKey(), mPageKind);
			parent.setRightChildKey(leftChild.getNodeKey());
			mPageWriteTrx.finishNodeModification(parent.getNodeKey(), mPageKind);
		} else {
			final AVLNode<K, V> parent = (AVLNode<K, V>) mPageWriteTrx
					.prepareNodeForModification(mCurrentNode.getNodeKey(), mPageKind);
			parent.setLeftChildKey(leftChild.getNodeKey());
			mPageWriteTrx.finishNodeModification(parent.getNodeKey(), mPageKind);
		}

		leftChild = (AVLNode<K, V>) mPageWriteTrx.prepareNodeForModification(
				leftChild.getNodeKey(), mPageKind);
		leftChild.setRightChildKey(node.getNodeKey());
		mPageWriteTrx.finishNodeModification(leftChild.getNodeKey(), mPageKind);

		node = (AVLNode<K, V>) mPageWriteTrx.prepareNodeForModification(
				node.getNodeKey(), mPageKind);
		node.setParentKey(leftChild.getNodeKey());
		mPageWriteTrx.finishNodeModification(node.getNodeKey(), mPageKind);
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
	 * @param node
	 */
	public void setCurrentNode(final @Nonnull AVLNode<K, V> node) {
		mCurrentNode = checkNotNull(node);
	}

	/**
	 * Get the structural node of the current node.
	 * 
	 * @return structural node
	 */
	private StructNode getStructuralNode() {
		if (mCurrentNode instanceof StructNode) {
			return (StructNode) mCurrentNode;
		} else {
			return new NullNode(mCurrentNode);
		}
	}

	@Override
	public boolean hasNode(final long key) {
		final long currKey = mCurrentNode.getNodeKey();
		final boolean moved = moveTo(key).hasMoved();
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
		if (mCurrentNode instanceof AVLNode) {
			return getAVLNode().hasRightChild();
		}
		return false;
	}

	@Override
	public boolean hasLeftSibling() {
		return getStructuralNode().hasLeftSibling();
	}

	@Override
	public boolean hasRightSibling() {
		assertNotClosed();
		return getStructuralNode().hasRightSibling();
	}

	@Override
	public VisitResultType acceptVisitor(@Nonnull Visitor visitor) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Move<AVLTree<K, V>> moveTo(final long nodeKey) {
		assertNotClosed();

		// Remember old node and fetch new one.
		final Node oldNode = mCurrentNode;
		Optional<? extends Node> newNode;
		try {
			// Immediately return node from item list if node key negative.
			@SuppressWarnings("unchecked")
			final Optional<? extends Node> node = (Optional<? extends Node>) mPageWriteTrx
					.getNode(nodeKey, mPageKind);
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
		return moveTo(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
	}

	@Override
	public Move<AVLTree<K, V>> moveToParent() {
		return moveTo(mCurrentNode.getParentKey());
	}

	@Override
	public Move<AVLTree<K, V>> moveToFirstChild() {
		assertNotClosed();
		if (mCurrentNode instanceof AVLNode) {
			final AVLNode<K, V> node = getAVLNode();
			if (!node.hasLeftChild()) {
				return Moved.notMoved();
			}
			return moveTo(node.getLeftChildKey());
		}
		return moveTo(((DocumentRootNode) mCurrentNode).getFirstChildKey());
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
	public Move<? extends NodeCursor> moveToPrevious() {
		return moveToParent();
	}

	@Override
	public Move<? extends NodeCursor> moveToNext() {
		return moveToLastChild();
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
	public Kind getRightSiblingKind() {
		return Kind.UNKNOWN;
	}

	@Override
	public Kind getLeftSiblingKind() {
		return Kind.UNKNOWN;
	}

	@Override
	public Kind getFirstChildKind() {
		return Kind.AVL;
	}

	@Override
	public Kind getLastChildKind() {
		return Kind.AVL;
	}

	@Override
	public Kind getParentKind() {
		if (mCurrentNode.getKind() == Kind.DOCUMENT) {
			return Kind.DOCUMENT;
		}
		return Kind.AVL;
	}

	@Override
	public Kind getKind() {
		return Kind.AVL;
	}

	@Override
	public ImmutableNode getNode() {
		return mCurrentNode;
	}
}
