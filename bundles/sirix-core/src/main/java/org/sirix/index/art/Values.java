package org.sirix.index.art;

import java.util.AbstractCollection;
import java.util.Iterator;

// contains all stuff borrowed from TreeMap
// such methods/utilities should be taken out and made a library of their own
// so any implementation of NavigableMap can reuse it, while the implementation
// provides certain primitive methods (getEntry, successor, predecessor, etc)

class Values<K, V> extends AbstractCollection<V> {
	private final AdaptiveRadixTree<K, V> m;

	Values(AdaptiveRadixTree<K, V> m){
		this.m = m;
	}

	@Override
	public Iterator<V> iterator() {
		return m.valueIterator();
	}

	@Override
	public int size() {
		return m.size();
	}

	@Override
	public boolean contains(Object o) {
		return m.containsValue(o);
	}

	@Override
	public boolean remove(Object o) {
		for (LeafNode<K,V> e = m.getFirstEntry(); e != null; e = AdaptiveRadixTree.successor(e)) {
			if (AdaptiveRadixTree.valEquals(e.getValue(), o)) {
				m.deleteEntry(e);
				return true;
			}
		}
		return false;
	}

	@Override
	public void clear() {
		m.clear();
	}

	// TODO: implement Spliterator
}

