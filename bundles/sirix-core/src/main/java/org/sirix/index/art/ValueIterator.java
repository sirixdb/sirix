package org.sirix.index.art;

final class ValueIterator<K, V> extends PrivateEntryIterator<K, V, V> {
	ValueIterator(AdaptiveRadixTree<K, V> m, LeafNode<K,V> first) {
		super(m, first);
	}
	@Override
	public V next() {
		return nextEntry().getValue();
	}
}
