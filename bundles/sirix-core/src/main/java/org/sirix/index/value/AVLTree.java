package org.sirix.index.value;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.PrintStream;
import java.util.ArrayDeque;
import java.util.Deque;

import javax.annotation.Nonnegative;
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
import org.sirix.index.value.interfaces.References;
import org.sirix.node.DocumentRootNode;
import org.sirix.node.Kind;
import org.sirix.node.NullNode;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.page.PageKind;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.UnorderedKeyValuePage;
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
public final class AVLTree<K extends Comparable<? super K>, V extends References>
		implements NodeCursor {

	/** {@link LogWrapper} reference. */
	private static final LogWrapper LOGWRAPPER = new LogWrapper(
			LoggerFactory.getLogger(AVLTree.class));

	/** Determines if tree is closed or not. */
	private boolean mClosed;

	/** Strong reference to currently selected node. */
	private Node mCurrentNode;

	/** {@link PageWriteTrx} for persistent storage. */
	private final PageWriteTrx<Long, Record, UnorderedKeyValuePage> mPageWriteTrx;

	/** Page kind. */
	private final PageKind mPageKind;

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
	 * @param pageWriteTrx
	 *          {@link PageWriteTrx} for persistent storage
	 * @param kind
	 *          kind of value (attribute/text)
	 */
	private AVLTree(
			final @Nonnull PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
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
			Optional<? extends Record> node = mPageWriteTrx.getRecord(
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
	public static <K extends Comparable<? super K>, V extends References> AVLTree<K, V> getInstance(
			final @Nonnull PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
			final @Nonnull ValueKind kind) {
		return new AVLTree<K, V>(pageWriteTrx, kind);
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
		if (getAVLNode() == null
				&& ((DocumentRootNode) getNode()).getFirstChildKey() == Fixed.NULL_NODE_KEY
						.getStandardProperty()) {
			// Index is empty.. create root node.
			final long nodeKey = mPageKind == PageKind.TEXTVALUEPAGE ? root
					.getMaxTextValueNodeKey() + 1
					: root.getMaxAttributeValueNodeKey() + 1;
			final AVLNode<K, V> treeRoot = (AVLNode<K, V>) mPageWriteTrx.createEntry(
					nodeKey,
					new AVLNode<>(key, value, new NodeDelegate(nodeKey,
							Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), 0, 0, Optional
									.<SirixDeweyID> absent())), mPageKind, Optional
							.<UnorderedKeyValuePage> absent());
			final DocumentRootNode document = (DocumentRootNode) mPageWriteTrx
					.prepareEntryForModification(
							Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), mPageKind,
							Optional.<UnorderedKeyValuePage> absent());
			document.setFirstChildKey(treeRoot.getNodeKey());
			document.incrementChildCount();
			document.incrementDescendantCount();
			mPageWriteTrx.finishEntryModification(document.getNodeKey(), mPageKind);
			return value;
		}

		if (move == MoveCursor.TO_DOCUMENT_ROOT || getAVLNode() == null) {
			moveToDocumentRoot();
			moveToFirstChild();
		}
		AVLNode<K, V> node = getAVLNode();
		while (true) {
			final int c = key.compareTo(node.getKey());
			if (c == 0) {
				if (!value.equals(node.getValue())) {
					final AVLNode<K, V> avlNode = (AVLNode<K, V>) mPageWriteTrx
							.prepareEntryForModification(node.getNodeKey(), mPageKind,
									Optional.<UnorderedKeyValuePage> absent());
					avlNode.setValue(value);
					mPageWriteTrx
							.finishEntryModification(avlNode.getNodeKey(), mPageKind);
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
			final AVLNode<K, V> child = (AVLNode<K, V>) mPageWriteTrx.createEntry(
					nodeKey,
					new AVLNode<>(key, value, new NodeDelegate(nodeKey,
							node.getNodeKey(), 0, 0, Optional.<SirixDeweyID> absent())),
					mPageKind, Optional.<UnorderedKeyValuePage> absent());
			node = (AVLNode<K, V>) mPageWriteTrx.prepareEntryForModification(
					node.getNodeKey(), mPageKind,
					Optional.<UnorderedKeyValuePage> absent());
			if (c < 0) {
				node.setLeftChildKey(child.getNodeKey());
				mPageWriteTrx.finishEntryModification(node.getNodeKey(), mPageKind);
				adjust(child);
			} else {
				node.setRightChildKey(child.getNodeKey());
				mPageWriteTrx.finishEntryModification(node.getNodeKey(), mPageKind);
				adjust(child);
			}
			final DocumentRootNode document = (DocumentRootNode) mPageWriteTrx
					.prepareEntryForModification(
							Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), mPageKind,
							Optional.<UnorderedKeyValuePage> absent());
			document.incrementDescendantCount();
			mPageWriteTrx.finishEntryModification(document.getNodeKey(), mPageKind);
			return value;
		}
	}

	/**
	 * Remove a node key from the value, or remove the whole node, if no keys are
	 * stored anymore.
	 * 
	 * @param key
	 *          the key for which to search the value
	 * @param nodeKey
	 *          the nodeKey to remove from the value
	 * @throws SirixIOException
	 *           if an I/O error occured
	 */
	public void remove(final @Nonnull K key, final @Nonnegative long nodeKey)
			throws SirixIOException {
		final Optional<V> searchedValue = get(key, SearchMode.EQUAL);
		if (searchedValue.isPresent()) {
			final V value = searchedValue.get();
			value.removeNodeKey(nodeKey);

			@SuppressWarnings("unchecked")
			final AVLNode<K, V> node = (AVLNode<K, V>) mPageWriteTrx
					.prepareEntryForModification(mCurrentNode.getNodeKey(), mPageKind,
							Optional.<UnorderedKeyValuePage> absent());
			node.setValue(value);
			mPageWriteTrx.finishEntryModification(node.getNodeKey(), mPageKind);

			if (value.hasNodeKeys()) {
				return;
			}

			removeNode();
		}
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
	 * Remove a node in the AVLTree.
	 * 
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	private void removeNode() throws SirixIOException {
		AVLNode<K, V> toDelete = getAVLNode();
		// Case: No children.
		if (!toDelete.hasLeftChild() && !toDelete.hasRightChild()) {
			if (toDelete.getParentKey() == Fixed.DOCUMENT_NODE_KEY
					.getStandardProperty()) {
				// Root node removed.
				final DocumentRootNode document = (DocumentRootNode) mPageWriteTrx
						.prepareEntryForModification(
								Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), mPageKind,
								Optional.<UnorderedKeyValuePage> absent());
				document.setFirstChildKey(Fixed.NULL_NODE_KEY.getStandardProperty());
				mPageWriteTrx.finishEntryModification(
						Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), mPageKind);
			} else {
				moveToParent();
				if (getAVLNode().getLeftChildKey() == toDelete.getNodeKey()) {
					@SuppressWarnings("unchecked")
					final AVLNode<K, V> parent = (AVLNode<K, V>) mPageWriteTrx
							.prepareEntryForModification(getAVLNode().getNodeKey(),
									mPageKind, Optional.<UnorderedKeyValuePage> absent());
					parent.setLeftChildKey(Fixed.NULL_NODE_KEY.getStandardProperty());
					parent.setChanged(true);
					mPageWriteTrx.finishEntryModification(parent.getNodeKey(), mPageKind);
				} else {
					assert getAVLNode().getRightChildKey() == toDelete.getNodeKey();

					@SuppressWarnings("unchecked")
					final AVLNode<K, V> parent = (AVLNode<K, V>) mPageWriteTrx
							.prepareEntryForModification(getAVLNode().getNodeKey(),
									mPageKind, Optional.<UnorderedKeyValuePage> absent());
					parent.setRightChildKey(Fixed.NULL_NODE_KEY.getStandardProperty());
					parent.setChanged(true);
					mPageWriteTrx.finishEntryModification(parent.getNodeKey(), mPageKind);
				}
				moveTo(toDelete.getNodeKey());

				// Balance up to root.
				adjust(getAVLNode());
			}

			mPageWriteTrx.removeEntry(toDelete.getNodeKey(), mPageKind,
					Optional.<UnorderedKeyValuePage> absent());
			return;
		}
		// Case: 1 child.
		if (toDelete.hasLeftChild() && !toDelete.hasRightChild()) {
			if (toDelete.getParentKey() == Fixed.DOCUMENT_NODE_KEY
					.getStandardProperty()) {
				final DocumentRootNode document = (DocumentRootNode) mPageWriteTrx
						.prepareEntryForModification(
								Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), mPageKind,
								Optional.<UnorderedKeyValuePage> absent());
				document.setFirstChildKey(toDelete.getLeftChildKey());
				mPageWriteTrx.finishEntryModification(
						Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), mPageKind);
			} else {
				@SuppressWarnings("unchecked")
				final AVLNode<K, V> parent = (AVLNode<K, V>) mPageWriteTrx
						.prepareEntryForModification(toDelete.getParentKey(), mPageKind,
								Optional.<UnorderedKeyValuePage> absent());
				parent.setLeftChildKey(toDelete.getLeftChildKey());
				parent.setChanged(true);
				mPageWriteTrx.finishEntryModification(parent.getNodeKey(), mPageKind);
			}

			@SuppressWarnings("unchecked")
			final AVLNode<K, V> leftChild = (AVLNode<K, V>) mPageWriteTrx
					.prepareEntryForModification(toDelete.getLeftChildKey(), mPageKind,
							Optional.<UnorderedKeyValuePage> absent());
			leftChild.setParentKey(toDelete.getParentKey());
			mPageWriteTrx.finishEntryModification(leftChild.getNodeKey(), mPageKind);

			// Balance up to root.
			adjust(getAVLNode());

			// Remove deleted node.
			mPageWriteTrx.removeEntry(toDelete.getNodeKey(), mPageKind,
					Optional.<UnorderedKeyValuePage> absent());
			return;
		}
		// Case: 1 child.
		if (!toDelete.hasLeftChild() && toDelete.hasRightChild()) {
			if (toDelete.getParentKey() == Fixed.DOCUMENT_NODE_KEY
					.getStandardProperty()) {
				final DocumentRootNode document = (DocumentRootNode) mPageWriteTrx
						.prepareEntryForModification(
								Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), mPageKind,
								Optional.<UnorderedKeyValuePage> absent());
				document.setFirstChildKey(toDelete.getRightChildKey());
				mPageWriteTrx.finishEntryModification(
						Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), mPageKind);
			} else {
				@SuppressWarnings("unchecked")
				final AVLNode<K, V> parent = (AVLNode<K, V>) mPageWriteTrx
						.prepareEntryForModification(toDelete.getParentKey(), mPageKind,
								Optional.<UnorderedKeyValuePage> absent());
				parent.setRightChildKey(toDelete.getRightChildKey());
				parent.setChanged(true);
				mPageWriteTrx.finishEntryModification(parent.getNodeKey(), mPageKind);
			}

			@SuppressWarnings("unchecked")
			final AVLNode<K, V> rightChild = (AVLNode<K, V>) mPageWriteTrx
					.prepareEntryForModification(toDelete.getRightChildKey(), mPageKind,
							Optional.<UnorderedKeyValuePage> absent());
			rightChild.setParentKey(toDelete.getParentKey());
			mPageWriteTrx.finishEntryModification(rightChild.getNodeKey(), mPageKind);

			// Balance up to root.
			adjust(getAVLNode());

			// Remove deleted node.
			mPageWriteTrx.removeEntry(toDelete.getNodeKey(), mPageKind,
					Optional.<UnorderedKeyValuePage> absent());
			return;
		}
		assert toDelete.hasLeftChild() && toDelete.hasRightChild();
		// Case: 2 children.
		// Move to successor.
		moveToLastChild();
		while (getAVLNode().hasLeftChild()) {
			moveToFirstChild();
		}

		final AVLNode<K, V> successor = getAVLNode();
		final long nodeKey = successor.getNodeKey();

		@SuppressWarnings("unchecked")
		final AVLNode<K, V> leftChild = (AVLNode<K, V>) mPageWriteTrx
				.prepareEntryForModification(toDelete.getLeftChildKey(), mPageKind,
						Optional.<UnorderedKeyValuePage> absent());
		leftChild.setParentKey(successor.getNodeKey());
		mPageWriteTrx.finishEntryModification(leftChild.getNodeKey(), mPageKind);

		if (toDelete.getRightChildKey() != successor.getNodeKey()) {
			@SuppressWarnings("unchecked")
			final AVLNode<K, V> rightChild = (AVLNode<K, V>) mPageWriteTrx
					.prepareEntryForModification(toDelete.getRightChildKey(), mPageKind,
							Optional.<UnorderedKeyValuePage> absent());
			rightChild.setParentKey(successor.getNodeKey());
			mPageWriteTrx.finishEntryModification(rightChild.getNodeKey(), mPageKind);

			if (successor.hasRightChild()) {
				// The only subtree.
				@SuppressWarnings("unchecked")
				final AVLNode<K, V> child = (AVLNode<K, V>) mPageWriteTrx
						.prepareEntryForModification(successor.getRightChildKey(),
								mPageKind, Optional.<UnorderedKeyValuePage> absent());
				child.setParentKey(successor.getParentKey());
				mPageWriteTrx.finishEntryModification(child.getNodeKey(), mPageKind);

				@SuppressWarnings("unchecked")
				final AVLNode<K, V> parent = (AVLNode<K, V>) mPageWriteTrx
						.prepareEntryForModification(successor.getParentKey(), mPageKind,
								Optional.<UnorderedKeyValuePage> absent());
				parent.setLeftChildKey(successor.getRightChildKey());
				parent.setChanged(true);
				mPageWriteTrx.finishEntryModification(parent.getNodeKey(), mPageKind);
			} else {
				@SuppressWarnings("unchecked")
				final AVLNode<K, V> parent = (AVLNode<K, V>) mPageWriteTrx
						.prepareEntryForModification(successor.getParentKey(), mPageKind,
								Optional.<UnorderedKeyValuePage> absent());
				parent.setLeftChildKey(Fixed.NULL_NODE_KEY.getStandardProperty());
				parent.setChanged(true);
				mPageWriteTrx.finishEntryModification(parent.getNodeKey(), mPageKind);
			}
		}

		@SuppressWarnings("unchecked")
		final AVLNode<K, V> successorToModify = (AVLNode<K, V>) mPageWriteTrx
				.prepareEntryForModification(nodeKey, mPageKind,
						Optional.<UnorderedKeyValuePage> absent());
		assert toDelete.getLeftChildKey() != Fixed.NULL_NODE_KEY
				.getStandardProperty()
				&& toDelete.getRightChildKey() != Fixed.NULL_NODE_KEY
						.getStandardProperty();
		successorToModify.setLeftChildKey(toDelete.getLeftChildKey());
		if (successor.getNodeKey() != toDelete.getRightChildKey()) {
			successorToModify.setRightChildKey(toDelete.getRightChildKey());
		}
		successorToModify.setParentKey(toDelete.getParentKey());
		mPageWriteTrx.finishEntryModification(successorToModify.getNodeKey(),
				mPageKind);

		if (toDelete.getParentKey() == Fixed.DOCUMENT_NODE_KEY
				.getStandardProperty()) {
			final DocumentRootNode parent = (DocumentRootNode) mPageWriteTrx
					.prepareEntryForModification(toDelete.getParentKey(), mPageKind,
							Optional.<UnorderedKeyValuePage> absent());
			parent.setFirstChildKey(successor.getNodeKey());
			mPageWriteTrx.finishEntryModification(parent.getNodeKey(), mPageKind);
		} else {
			@SuppressWarnings("unchecked")
			final AVLNode<K, V> parent = (AVLNode<K, V>) mPageWriteTrx
					.prepareEntryForModification(toDelete.getParentKey(), mPageKind,
							Optional.<UnorderedKeyValuePage> absent());
			if (parent.getLeftChildKey() == toDelete.getNodeKey()) {
				parent.setLeftChildKey(successor.getNodeKey());
			} else {
				assert parent.getRightChildKey() == toDelete.getNodeKey();
				parent.setRightChildKey(successor.getNodeKey());
			}
			mPageWriteTrx.finishEntryModification(parent.getNodeKey(), mPageKind);
		}

		// Balance up to root.
		moveTo(nodeKey);
		adjust(getAVLNode());

		mPageWriteTrx.removeEntry(toDelete.getNodeKey(), mPageKind,
				Optional.<UnorderedKeyValuePage> absent());
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

	/**
	 * Adjusts the tree balance.
	 * 
	 * @param node
	 *          node to be adjusted
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	private void adjust(@Nonnull AVLNode<K, V> node) throws SirixIOException {
		setChanged(node, true);

		while (node != null
				&& node.getParentKey() != Fixed.DOCUMENT_NODE_KEY.getStandardProperty()
				&& parent(node) != null && parent(node).isChanged()) {
			if (parent(node).equals(left(parent(parent(node))))) {
				AVLNode<K, V> y = right(parent(parent(node)));
				if (y != null && y.isChanged()) {
					setChanged(parent(node), false);
					y.setChanged(false);
					setChanged(parent(parent(node)), true);
					node = parent(parent(node));
				} else {
					if (node.equals(right(parent(node)))) {
						node = parent(node);
						rotateLeft(node);
					}
					setChanged(parent(node), false);
					setChanged(parent(parent(node)), true);
					if (parent(parent(node)) != null) rotateRight(parent(parent(node)));
				}
			} else if (parent(node).equals(right(parent(parent(node))))) {
				AVLNode<K, V> y = left(parent(parent(node)));
				if (y != null && y.isChanged()) {
					setChanged(parent(node), false);
					setChanged(y, false);
					setChanged(parent(parent(node)), true);
					node = parent(parent(node));
				} else {
					if (node.equals(left(parent(node)))) {
						node = parent(node);
						rotateRight(node);
					}
					setChanged(parent(node), false);
					setChanged(parent(parent(node)), true);
					if (parent(parent(node)) != null) rotateLeft(parent(parent(node)));
				}
			} else {
				node = null;
			}
		}

		final long nodeKey = getNodeKey();
		moveToDocumentRoot();
		if (((DocumentRootNode) getNode()).hasFirstChild()) {
			moveToFirstChild();
			setChanged(getAVLNode(), false);
		}
		moveTo(nodeKey);
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
				.prepareEntryForModification(nodeToChange.getNodeKey(), mPageKind,
						Optional.<UnorderedKeyValuePage> absent());
		node.setChanged(changed);
		mPageWriteTrx.finishEntryModification(node.getNodeKey(), mPageKind);
	}

	/**
	 * Returns the left child node.
	 * 
	 * @param node
	 *          node from which to move to and return the left sibling
	 * @return left child node or {@code null}
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
	 * @return right child node or {@code null}
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
	 * @return parent node or {@code null}
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

		node = (AVLNode<K, V>) mPageWriteTrx
				.prepareEntryForModification(node.getNodeKey(), mPageKind,
						Optional.<UnorderedKeyValuePage> absent());
		node.setRightChildKey(right.getLeftChildKey());
		mPageWriteTrx.finishEntryModification(node.getNodeKey(), mPageKind);

		if (right.hasLeftChild()) {
			final AVLNode<K, V> rightLeftChild = (AVLNode<K, V>) mPageWriteTrx
					.prepareEntryForModification(right.getLeftChildKey(), mPageKind,
							Optional.<UnorderedKeyValuePage> absent());
			rightLeftChild.setParentKey(node.getNodeKey());
			mPageWriteTrx.finishEntryModification(rightLeftChild.getNodeKey(),
					mPageKind);
		}

		right = (AVLNode<K, V>) mPageWriteTrx.prepareEntryForModification(
				right.getNodeKey(), mPageKind,
				Optional.<UnorderedKeyValuePage> absent());
		right.setParentKey(node.getParentKey());
		mPageWriteTrx.finishEntryModification(right.getNodeKey(), mPageKind);

		if (node.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
			final DocumentRootNode parent = (DocumentRootNode) mPageWriteTrx
					.prepareEntryForModification(
							Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), mPageKind,
							Optional.<UnorderedKeyValuePage> absent());
			parent.setFirstChildKey(right.getNodeKey());
			mPageWriteTrx.finishEntryModification(parent.getNodeKey(), mPageKind);
		} else if (moveTo(node.getParentKey()).hasMoved()
				&& getAVLNode().getLeftChildKey() == node.getNodeKey()) {
			final AVLNode<K, V> parent = (AVLNode<K, V>) mPageWriteTrx
					.prepareEntryForModification(mCurrentNode.getNodeKey(), mPageKind,
							Optional.<UnorderedKeyValuePage> absent());
			parent.setLeftChildKey(right.getNodeKey());
			mPageWriteTrx.finishEntryModification(parent.getNodeKey(), mPageKind);
		} else {
			final AVLNode<K, V> parent = (AVLNode<K, V>) mPageWriteTrx
					.prepareEntryForModification(mCurrentNode.getNodeKey(), mPageKind,
							Optional.<UnorderedKeyValuePage> absent());
			parent.setRightChildKey(right.getNodeKey());
			mPageWriteTrx.finishEntryModification(parent.getNodeKey(), mPageKind);
		}

		right = (AVLNode<K, V>) mPageWriteTrx.prepareEntryForModification(
				right.getNodeKey(), mPageKind,
				Optional.<UnorderedKeyValuePage> absent());
		right.setLeftChildKey(node.getNodeKey());
		mPageWriteTrx.finishEntryModification(right.getNodeKey(), mPageKind);

		node = (AVLNode<K, V>) mPageWriteTrx
				.prepareEntryForModification(node.getNodeKey(), mPageKind,
						Optional.<UnorderedKeyValuePage> absent());
		node.setParentKey(right.getNodeKey());
		mPageWriteTrx.finishEntryModification(node.getNodeKey(), mPageKind);
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
		node = (AVLNode<K, V>) mPageWriteTrx
				.prepareEntryForModification(node.getNodeKey(), mPageKind,
						Optional.<UnorderedKeyValuePage> absent());
		node.setLeftChildKey(leftChild.getRightChildKey());
		mPageWriteTrx.finishEntryModification(node.getNodeKey(), mPageKind);

		if (leftChild.hasRightChild()) {
			final Node leftRightChild = (Node) mPageWriteTrx
					.prepareEntryForModification(leftChild.getRightChildKey(), mPageKind,
							Optional.<UnorderedKeyValuePage> absent());
			leftRightChild.setParentKey(node.getNodeKey());
			mPageWriteTrx.finishEntryModification(leftRightChild.getNodeKey(),
					mPageKind);
		}

		leftChild = (AVLNode<K, V>) mPageWriteTrx.prepareEntryForModification(
				leftChild.getNodeKey(), mPageKind,
				Optional.<UnorderedKeyValuePage> absent());
		leftChild.setParentKey(node.getParentKey());
		mPageWriteTrx.finishEntryModification(leftChild.getNodeKey(), mPageKind);

		if (node.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
			final DocumentRootNode parent = (DocumentRootNode) mPageWriteTrx
					.prepareEntryForModification(
							Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), mPageKind,
							Optional.<UnorderedKeyValuePage> absent());
			parent.setFirstChildKey(leftChild.getNodeKey());
			mPageWriteTrx.finishEntryModification(parent.getNodeKey(), mPageKind);
		} else if (moveTo(node.getParentKey()).hasMoved()
				&& getAVLNode().getRightChildKey() == node.getNodeKey()) {
			final AVLNode<K, V> parent = (AVLNode<K, V>) mPageWriteTrx
					.prepareEntryForModification(mCurrentNode.getNodeKey(), mPageKind,
							Optional.<UnorderedKeyValuePage> absent());
			parent.setRightChildKey(leftChild.getNodeKey());
			mPageWriteTrx.finishEntryModification(parent.getNodeKey(), mPageKind);
		} else {
			final AVLNode<K, V> parent = (AVLNode<K, V>) mPageWriteTrx
					.prepareEntryForModification(mCurrentNode.getNodeKey(), mPageKind,
							Optional.<UnorderedKeyValuePage> absent());
			parent.setLeftChildKey(leftChild.getNodeKey());
			mPageWriteTrx.finishEntryModification(parent.getNodeKey(), mPageKind);
		}

		leftChild = (AVLNode<K, V>) mPageWriteTrx.prepareEntryForModification(
				leftChild.getNodeKey(), mPageKind,
				Optional.<UnorderedKeyValuePage> absent());
		leftChild.setRightChildKey(node.getNodeKey());
		mPageWriteTrx.finishEntryModification(leftChild.getNodeKey(), mPageKind);

		node = (AVLNode<K, V>) mPageWriteTrx
				.prepareEntryForModification(node.getNodeKey(), mPageKind,
						Optional.<UnorderedKeyValuePage> absent());
		node.setParentKey(leftChild.getNodeKey());
		mPageWriteTrx.finishEntryModification(node.getNodeKey(), mPageKind);
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
					.getRecord(nodeKey, mPageKind);
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
