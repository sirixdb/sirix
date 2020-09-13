package org.sirix.index.art;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Base class for AdaptiveRadixTree Iterators
 * note: taken from TreeMap
 */
abstract class PrivateEntryIterator<K, V, T> implements Iterator<T> {
	private final AdaptiveRadixTree<K, V> m;
	private LeafNode<K,V> next;
	private LeafNode<K, V> lastReturned;
	private int expectedModCount;

	PrivateEntryIterator(AdaptiveRadixTree<K, V> m, LeafNode<K,V> first) {
		expectedModCount = m.getModCount();
		lastReturned = null;
		next = first;
		this.m = m;
	}

	public final boolean hasNext() {
		return next != null;
	}

	final LeafNode<K,V> nextEntry() {
		LeafNode<K,V> e = next;
		if (e == null)
			throw new NoSuchElementException();
		if (m.getModCount() != expectedModCount)
			throw new ConcurrentModificationException();
		next = AdaptiveRadixTree.successor(e);
		lastReturned = e;
		return e;
	}

	final LeafNode<K,V> prevEntry() {
		LeafNode<K,V> e = next;
		if (e == null)
			throw new NoSuchElementException();
		if (m.getModCount() != expectedModCount)
			throw new ConcurrentModificationException();
		next = AdaptiveRadixTree.predecessor(e);
		lastReturned = e;
		return e;
	}

	public void remove() {
		if (lastReturned == null)
			throw new IllegalStateException();
		if (m.getModCount() != expectedModCount)
			throw new ConcurrentModificationException();
		/*
			next already points to the next leaf node (that might be a sibling to this lastReturned).
			if next is the only sibling left, then the parent gets path compressed.
			BUT the reference that next holds to the sibling leaf node remains the same, just it's parent changes.
			Therefore at all times, next is a valid reference to be simply returned on the
			next call to next().
			Is there any scenario in which the next leaf pointer gets changed and iterator next
			points to a stale leaf?
			No.
			Infact the LeafNode ctor is only ever called in a put and that too for the newer leaf
			to be created/entered.
			So references to an existing LeafNode won't get stale.
		 */
		m.deleteEntry(lastReturned);
		expectedModCount = m.getModCount();
		lastReturned = null;
	}
}
