package org.sirix.index.art;

final class DescendingKeyIterator<K, V> extends PrivateEntryIterator<K, V, K> {
  DescendingKeyIterator(AdaptiveRadixTree<K, V> m, LeafNode<K, V> last) {
    super(m, last);
  }

  @Override
  public K next() {
    return prevEntry().getKey();
  }
}
