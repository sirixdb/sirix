package org.sirix.index.art;

final class KeyIterator<K, V> extends PrivateEntryIterator<K, V, K> {
	KeyIterator(AdaptiveRadixTree<K, V> tree, LeafNode<K,V> first) {
		super(tree, first);
	}
	@Override
	public K next() {
		return nextEntry().getKey();
	}
}
