package org.sirix.index.art;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;

class EntrySet<K, V> extends AbstractSet<Map.Entry<K, V>> {
	private final AdaptiveRadixTree<K, V> tree;

	EntrySet(AdaptiveRadixTree<K, V> tree) {
		this.tree = tree;
	}

	@Override
	public Iterator<Map.Entry<K, V>> iterator() {
		return tree.entryIterator();
	}

	@Override
	public boolean contains(Object o) {
		if (!(o instanceof Map.Entry))
			return false;
		Map.Entry<?, ?> entry = (Map.Entry<?, ?>) o;
		Object value = entry.getValue();
		LeafNode<K, V> p = tree.getEntry(entry.getKey());
		return p != null && AdaptiveRadixTree.valEquals(p.getValue(), value);
	}

	@Override
	public boolean remove(Object o) {
		if (!(o instanceof Map.Entry))
			return false;
		Map.Entry<?, ?> entry = (Map.Entry<?, ?>) o;
		Object value = entry.getValue();
		LeafNode<K, V> p = tree.getEntry(entry.getKey());
		if (p != null && AdaptiveRadixTree.valEquals(p.getValue(), value)) {
			tree.deleteEntry(p);
			return true;
		}
		return false;
	}

	@Override
	public int size() {
		return tree.size();
	}

	@Override
	public void clear() {
		tree.clear();
	}

	// TODO: implement Spliterator
}
