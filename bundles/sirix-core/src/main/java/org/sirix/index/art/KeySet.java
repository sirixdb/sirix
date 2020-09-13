package org.sirix.index.art;

import java.util.*;

// implementation simply relays/delegates calls to backing map's methods
final class KeySet<E> extends AbstractSet<E> implements NavigableSet<E> {
	private final NavigableMap<E, ?> map;

	KeySet(NavigableMap<E, ?> map) {
		this.map = map;
	}

	// this KeySet can only be created either on ART or on one of it's subMaps
	@Override
	@SuppressWarnings("unchecked")
	public Iterator<E> iterator() {
		if (map instanceof AdaptiveRadixTree)

			return ((AdaptiveRadixTree<E, ?>) map).keyIterator();
		else
			return ((NavigableSubMap<E, ?>) map).keyIterator();
	}

	// this KeySet can only be created either on ART or on one of it's subMaps
	@Override
	@SuppressWarnings("unchecked")
	public Iterator<E> descendingIterator() {
		if (map instanceof AdaptiveRadixTree)
			return ((AdaptiveRadixTree<E, ?>) map).descendingKeyIterator();
		else
			return ((NavigableSubMap<E, ?>) map).descendingKeyIterator();
	}

	@Override
	public int size() {
		return map.size();
	}

	@Override
	public boolean isEmpty() {
		return map.isEmpty();
	}

	@Override
	public boolean contains(Object o) {
		return map.containsKey(o);
	}

	@Override
	public void clear() {
		map.clear();
	}

	@Override
	public E lower(E e) {
		return map.lowerKey(e);
	}

	@Override
	public E floor(E e) {
		return map.floorKey(e);
	}

	@Override
	public E ceiling(E e) {
		return map.ceilingKey(e);
	}

	@Override
	public E higher(E e) {
		return map.higherKey(e);
	}

	@Override
	public E first() {
		return map.firstKey();
	}

	@Override
	public E last() {
		return map.lastKey();
	}

	@Override
	public Comparator<? super E> comparator() {
		return map.comparator();
	}

	@Override
	public E pollFirst() {
		Map.Entry<E, ?> e = map.pollFirstEntry();
		return (e == null) ? null : e.getKey();
	}

	@Override
	public E pollLast() {
		Map.Entry<E, ?> e = map.pollLastEntry();
		return (e == null) ? null : e.getKey();
	}

	@Override
	public boolean remove(Object o) {
		int oldSize = size();
		map.remove(o);
		return size() != oldSize;
	}

	@Override
	public NavigableSet<E> subSet(E fromElement, boolean fromInclusive,
			E toElement, boolean toInclusive) {
		return new KeySet<>(map.subMap(fromElement, fromInclusive,
		                               toElement, toInclusive));
	}

	@Override
	public NavigableSet<E> headSet(E toElement, boolean inclusive) {
		return new KeySet<>(map.headMap(toElement, inclusive));
	}

	@Override
	public NavigableSet<E> tailSet(E fromElement, boolean inclusive) {
		return new KeySet<>(map.tailMap(fromElement, inclusive));
	}

	@Override
	public SortedSet<E> subSet(E fromElement, E toElement) {
		return subSet(fromElement, true, toElement, false);
	}

	@Override
	public SortedSet<E> headSet(E toElement) {
		return headSet(toElement, false);
	}

	@Override
	public SortedSet<E> tailSet(E fromElement) {
		return tailSet(fromElement, true);
	}

	@Override
	public NavigableSet<E> descendingSet() {
		return new KeySet<>(map.descendingMap());
	}

	// TODO: implement Spliterator
}
