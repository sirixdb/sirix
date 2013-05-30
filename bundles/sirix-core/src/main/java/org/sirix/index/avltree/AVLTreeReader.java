package org.sirix.index.avltree;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.PrintStream;
import java.util.ArrayDeque;
import java.util.Deque;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.access.Move;
import org.sirix.access.Moved;
import org.sirix.api.NodeCursor;
import org.sirix.api.PageReadTrx;
import org.sirix.api.visitor.VisitResultType;
import org.sirix.api.visitor.Visitor;
import org.sirix.exception.SirixIOException;
import org.sirix.index.IndexType;
import org.sirix.index.SearchMode;
import org.sirix.index.avltree.interfaces.References;
import org.sirix.node.DocumentRootNode;
import org.sirix.node.Kind;
import org.sirix.node.NullNode;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.page.PageKind;
import org.sirix.settings.Fixed;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.AbstractIterator;

/**
 * Simple AVLTreeReader (balanced binary search-tree -- based on BaseX(.org) version).
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 * @param <K>
 *          the key to search for or insert
 * @param <V>
 *          the value
 */
public final class AVLTreeReader<K extends Comparable<? super K>, V extends References>
		implements NodeCursor {

	/** {@link LogWrapper} reference. */
	private static final LogWrapper LOGWRAPPER = new LogWrapper(
			LoggerFactory.getLogger(AVLTreeReader.class));

	/** Determines if tree is closed or not. */
	private boolean mClosed;

	/** Strong reference to currently selected node. */
	private Node mCurrentNode;

	/** {@link PageReadTrx} for persistent storage. */
	final PageReadTrx mPageReadTrx;

	/** Page kind. */
	final PageKind mPageKind;

	/** Index number. */
	final int mIndex;

	/** Determines movement of the internal cursor. */
	public enum MoveCursor {
		/** Cursor should be moved document root. */
		TO_DOCUMENT_ROOT,

		/** Cursor should not be moved. */
		NO_MOVE
	}

	/**
	 * Private constructor.
	 * 
	 * @param pageReadTrx
	 *          {@link PageReadTrx} for persistent storage
	 * @param type
	 *          kind of index
	 * @param index
	 * 					the index number
	 */
	private AVLTreeReader(
			final @Nonnull PageReadTrx pageReadTrx,
			final @Nonnull IndexType type, final int index) {
		mPageReadTrx = checkNotNull(pageReadTrx);
		switch (type) {
		case PATH:
			mPageKind = PageKind.PATHPAGE;
			break;
		case CAS:
			mPageKind = PageKind.CASPAGE;
			break;
		case NAME:
			// FIXME
			throw new UnsupportedOperationException();
		default:
			mPageKind = null;
			throw new IllegalStateException();
		}
		mClosed = false;
		mIndex = index;

		try {
			Optional<? extends Record> node = mPageReadTrx.getRecord(
					Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), mPageKind, index);
			if (node.isPresent()) {
				mCurrentNode = (Node) node.get();
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
	 * @param pageReadTrx
	 *          {@link PageReadTrx} for persistent storage
	 * @param type
	 *          type of index
	 * @return new tree instance
	 */
	public static <K extends Comparable<? super K>, V extends References> AVLTreeReader<K, V> getInstance(
			final @Nonnull PageReadTrx pageReadTrx,
			final @Nonnull IndexType type, final @Nonnegative int index) {
		return new AVLTreeReader<K, V>(pageReadTrx, type, index);
	}

	/**
	 * Dump the AVLTree in preorder.
	 * 
	 * @param out
	 *          {@link PrintStream} to print to
	 */
	public void dump(final @Nonnull PrintStream out) {
		moveToDocumentRoot();
		if (((DocumentRootNode) getNode()).hasFirstChild()) {
			moveToFirstChild();
		} else {
			return;
		}

		internalDump(out);
	}

	// Internal function to dump data to a PrintStream instance.
	private void internalDump(final @Nonnull PrintStream out) {
		out.println(getAVLNode());
		final long nodeKey = getAVLNode().getNodeKey();
		if (getAVLNode().hasLeftChild()) {
			moveToFirstChild();
			internalDump(out);
		}
		moveTo(nodeKey);
		if (getAVLNode().hasRightChild()) {
			moveToLastChild();
			internalDump(out);
		}
	}

	/**
	 * Get the {@link AVLNode}.
	 * 
	 * @return {@link AVLNode} instance
	 */
	AVLNode<K, V> getAVLNode() {
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
		moveToDocumentRoot();
		if (!((DocumentRootNode) getNode()).hasFirstChild()) {
			return Optional.absent();
		}
		moveToFirstChild();
		AVLNode<K, V> node = getAVLNode();
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
	public long size() {
		return ((DocumentRootNode) moveToDocumentRoot().get().getNode())
				.getDescendantCount();
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
		final Move<AVLTreeReader<K, V>> movedCursor = moveTo(currKey);
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
	public Move<AVLTreeReader<K, V>> moveTo(final long nodeKey) {
		assertNotClosed();

		// Remember old node and fetch new one.
		final Node oldNode = mCurrentNode;
		Optional<? extends Node> newNode;
		try {
			// Immediately return node from item list if node key negative.
			@SuppressWarnings("unchecked")
			final Optional<? extends Node> node = (Optional<? extends Node>) mPageReadTrx
					.getRecord(nodeKey, mPageKind, mIndex);
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
	public Move<AVLTreeReader<K, V>> moveToDocumentRoot() {
		return moveTo(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
	}

	@Override
	public Move<AVLTreeReader<K, V>> moveToParent() {
		return moveTo(mCurrentNode.getParentKey());
	}

	@Override
	public Move<AVLTreeReader<K, V>> moveToFirstChild() {
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
	public Move<AVLTreeReader<K, V>> moveToLastChild() {
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
	public Move<AVLTreeReader<K, V>> moveToLeftSibling() {
		return Move.notMoved();
	}

	@Override
	public Move<AVLTreeReader<K, V>> moveToRightSibling() {
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
		if (hasParent()) {
			if (mCurrentNode.getParentKey() == Fixed.DOCUMENT_NODE_KEY
					.getStandardProperty()) {
				return Kind.DOCUMENT;
			} else {
				return Kind.AVL;
			}
		}
		return Kind.UNKNOWN;
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
