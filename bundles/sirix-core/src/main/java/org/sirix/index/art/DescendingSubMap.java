package org.sirix.index.art;

import java.util.*;

final class DescendingSubMap<K, V> extends NavigableSubMap<K, V> {

  DescendingSubMap(AdaptiveRadixTree<K, V> m, boolean fromStart, K lo, boolean loInclusive, boolean toEnd, K hi,
      boolean hiInclusive) {
    super(m, fromStart, lo, loInclusive, toEnd, hi, hiInclusive);
  }

  @Override
  public Comparator<? super K> comparator() {
    return m.comparator();
  }

  // create a new submap out of a submap.
  // the new bounds should be within the current submap's bounds
  @Override
  public NavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    if (!inRange(fromKey, fromInclusive))
      throw new IllegalArgumentException("fromKey out of range");
    if (!inRange(toKey, toInclusive))
      throw new IllegalArgumentException("toKey out of range");
    return new DescendingSubMap<>(m, false, toKey, toInclusive, false, fromKey, fromInclusive);
  }

  @Override
  public NavigableMap<K, V> headMap(K toKey, boolean inclusive) {
    if (!inRange(toKey, inclusive))
      throw new IllegalArgumentException("toKey out of range");
    return new DescendingSubMap<>(m, false, toKey, inclusive, toEnd, hi, hiInclusive);
  }

  @Override
  public NavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
    if (!inRange(fromKey, inclusive))
      throw new IllegalArgumentException("fromKey out of range");
    return new DescendingSubMap<>(m, fromStart, lo, loInclusive, false, fromKey, inclusive);
  }

  @Override
  public NavigableMap<K, V> descendingMap() {
    NavigableMap<K, V> mapView = descendingMapView;
    return (mapView != null)
        ? mapView
        : (descendingMapView = new AscendingSubMap<>(m, fromStart, lo, loInclusive, toEnd, hi, hiInclusive));
  }

  @Override
  Iterator<K> keyIterator() {
    return new DescendingSubMapKeyIterator(absHighest(), absLowFence());
  }

  @Override
  Spliterator<K> keySpliterator() {
    return new DescendingSubMapKeyIterator(absHighest(), absLowFence());
  }

  @Override
  Iterator<K> descendingKeyIterator() {
    return new SubMapKeyIterator(absLowest(), absHighFence());
  }

  final class DescendingEntrySetView extends EntrySetView {
    @Override
    public Iterator<Entry<K, V>> iterator() {
      return new DescendingSubMapEntryIterator(absHighest(), absLowFence());
    }
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    EntrySetView es = entrySetView;
    return (es != null) ? es : (entrySetView = new DescendingEntrySetView());
  }

  @Override
  LeafNode<K, V> subLowest() {
    return absHighest();
  }

  @Override
  LeafNode<K, V> subHighest() {
    return absLowest();
  }

  @Override
  LeafNode<K, V> subCeiling(K key) {
    return absFloor(key);
  }

  @Override
  LeafNode<K, V> subHigher(K key) {
    return absLower(key);
  }

  @Override
  LeafNode<K, V> subFloor(K key) {
    return absCeiling(key);
  }

  @Override
  LeafNode<K, V> subLower(K key) {
    return absHigher(key);
  }
}
