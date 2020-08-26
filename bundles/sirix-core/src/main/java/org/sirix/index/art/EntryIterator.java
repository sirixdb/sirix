package org.sirix.index.art;

import java.util.Map;

final class EntryIterator<K, V> extends PrivateEntryIterator<K, V, Map.Entry<K, V>> {
  EntryIterator(AdaptiveRadixTree<K, V> tree, LeafNode<K, V> first) {
    super(tree, first);
  }

  @Override
  public Map.Entry<K, V> next() {
    return nextEntry();
  }
}
