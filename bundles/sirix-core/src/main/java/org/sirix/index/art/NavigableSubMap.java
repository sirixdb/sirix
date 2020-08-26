package org.sirix.index.art;

import java.util.*;
import java.util.function.Consumer;

// A NavigableMap that adds range checking (if passed in key is within lower and upper bound)
// for all the map methods and then relays the call
// into the backing map
abstract class NavigableSubMap<K, V> extends AbstractMap<K, V>
		implements NavigableMap<K, V> {

	final AdaptiveRadixTree<K, V> m;

	/**
	 * Endpoints are represented as triples (fromStart, lo,
	 * loInclusive) and (toEnd, hi, hiInclusive). If fromStart is
	 * true, then the low (absolute) bound is the start of the
	 * backing map, and the other values are ignored. Otherwise,
	 * if loInclusive is true, lo is the inclusive bound, else lo
	 * is the exclusive bound. Similarly for the upper bound.
	 */

	final K lo, hi;
	final byte[] loBytes, hiBytes;
	final boolean fromStart, toEnd;
	final boolean loInclusive, hiInclusive;

	NavigableSubMap(AdaptiveRadixTree<K, V> m,
			boolean fromStart, K lo, boolean loInclusive,
			boolean toEnd, K hi, boolean hiInclusive) {
		// equivalent to type check in TreeMap
		this.loBytes = fromStart ? null : m.binaryComparable().get(lo);
		this.hiBytes = toEnd ? null : m.binaryComparable().get(hi);
		if (!fromStart && !toEnd) {
			if (m.compare(loBytes, 0, loBytes.length, hiBytes, 0, hiBytes.length) > 0)
				throw new IllegalArgumentException("fromKey > toKey");
		}
		this.m = m;
		this.fromStart = fromStart;
		this.lo = lo;
		this.loInclusive = loInclusive;
		this.toEnd = toEnd;
		this.hi = hi;
		this.hiInclusive = hiInclusive;
	}

	// internal utilities

	final boolean tooLow(K key) {
		if (!fromStart) {
			int c = m.compare(key, loBytes);
			// if c == 0 and if lower bound is exclusive
			// then this key is too low
			// else it is not, since it is as low as our lower bound
			if (c < 0 || (c == 0 && !loInclusive))
				return true;
		}
		// we don't have a lower bound
		return false;
	}

	final boolean tooHigh(K key) {
		if (!toEnd) {
			int c = m.compare(key, hiBytes);
			// if c == 0 and if upper bound is exclusive
			// then this key is too higher
			// else it is not, since it is as greater as our upper bound
			if (c > 0 || (c == 0 && !hiInclusive))
				return true;
		}
		// we don't have an upper bound
		return false;
	}

	final boolean inRange(K key) {
		return !tooLow(key) && !tooHigh(key);
	}

	final boolean inClosedRange(K key) {
		// if we don't have any upper nor lower bounds, then all keys are always in range.
		// if we have a lower bound, then this key ought to be higher than our lower bound (closed, hence including).
		// if we have an upper bound, then this key ought to be lower than our upper bound (closed, hence including).
		return (fromStart || m.compare(key, loBytes) >= 0)
				&& (toEnd || m.compare(key, hiBytes) <= 0);
	}

	final boolean inRange(K key, boolean inclusive) {
		return inclusive ? inRange(key) : inClosedRange(key);
	}


	/*
	 * Absolute versions of relation operations.
	 * Subclasses map to these using like-named "sub"
	 * versions that invert senses for descending maps
	 */

	final LeafNode<K, V> absLowest() {
		LeafNode<K, V> e =
				(fromStart ? m.getFirstEntry() :
						(loInclusive ? m.getCeilingEntry(loBytes) :
								m.getHigherEntry(loBytes)));
		return (e == null || tooHigh(e.getKey())) ? null : e;
	}

	final LeafNode<K, V> absHighest() {
		LeafNode<K, V> e =
				(toEnd ? m.getLastEntry() :
						(hiInclusive ? m.getFloorEntry(hiBytes) :
								m.getLowerEntry(hiBytes)));
		return (e == null || tooLow(e.getKey())) ? null : e;
	}

	final LeafNode<K, V> absCeiling(K key) {
		if (tooLow(key))
			return absLowest();
		LeafNode<K, V> e = m.getCeilingEntry(key);
		return (e == null || tooHigh(e.getKey())) ? null : e;
	}

	final LeafNode<K, V> absHigher(K key) {
		if (tooLow(key))
			return absLowest();
		LeafNode<K, V> e = m.getHigherEntry(key);
		return (e == null || tooHigh(e.getKey())) ? null : e;
	}

	final LeafNode<K, V> absFloor(K key) {
		if (tooHigh(key))
			return absHighest();
		LeafNode<K, V> e = m.getFloorEntry(key);
		return (e == null || tooLow(e.getKey())) ? null : e;
	}

	final LeafNode<K, V> absLower(K key) {
		if (tooHigh(key))
			return absHighest();
		LeafNode<K, V> e = m.getLowerEntry(key);
		return (e == null || tooLow(e.getKey())) ? null : e;
	}

	/** Returns the absolute high fence for ascending traversal */
	final LeafNode<K, V> absHighFence() {
		return (toEnd ? null : (hiInclusive ?
				m.getHigherEntry(hiBytes) :
				m.getCeilingEntry(hiBytes))); // then hi itself (but we want the entry, hence traversal is required)
	}

	/** Return the absolute low fence for descending traversal  */
	final LeafNode<K, V> absLowFence() {
		return (fromStart ? null : (loInclusive ?
				m.getLowerEntry(loBytes) :
				m.getFloorEntry(loBytes))); // then lo itself (but we want the entry, hence traversal is required)
	}

	// Abstract methods defined in ascending vs descending classes
	// These relay to the appropriate absolute versions

	abstract LeafNode<K, V> subLowest();

	abstract LeafNode<K, V> subHighest();

	abstract LeafNode<K, V> subCeiling(K key);

	abstract LeafNode<K, V> subHigher(K key);

	abstract LeafNode<K, V> subFloor(K key);

	abstract LeafNode<K, V> subLower(K key);


	/* Returns ascending iterator from the perspective of this submap */

	abstract Iterator<K> keyIterator();

	abstract Spliterator<K> keySpliterator();


	/* Returns descending iterator from the perspective of this submap*/

	abstract Iterator<K> descendingKeyIterator();

	// public methods
	@Override
	public boolean isEmpty() {
		return (fromStart && toEnd) ? m.isEmpty() : entrySet().isEmpty();
	}

	@Override
	public int size() {
		return (fromStart && toEnd) ? m.size() : entrySet().size();
	}

	@Override
	public final boolean containsKey(Object key) {
		return inRange((K) key) && m.containsKey(key);
	}

	@Override
	public final V put(K key, V value) {
		if (!inRange(key))
			throw new IllegalArgumentException("key out of range");
		return m.put(key, value);
	}

	@Override
	public final V get(Object key) {
		return !inRange((K) key) ? null : m.get(key);
	}

	@Override
	public final V remove(Object key) {
		return !inRange((K) key) ? null : m.remove(key);
	}

	@Override
	public final Entry<K, V> ceilingEntry(K key) {
		return AdaptiveRadixTree.exportEntry(subCeiling(key));
	}

	@Override
	public final K ceilingKey(K key) {
		return AdaptiveRadixTree.keyOrNull(subCeiling(key));
	}

	@Override
	public final Entry<K, V> higherEntry(K key) {
		return AdaptiveRadixTree.exportEntry(subHigher(key));
	}

	@Override
	public final K higherKey(K key) {
		return AdaptiveRadixTree.keyOrNull(subHigher(key));
	}

	@Override
	public final Entry<K, V> floorEntry(K key) {
		return AdaptiveRadixTree.exportEntry(subFloor(key));
	}

	@Override
	public final K floorKey(K key) {
		return AdaptiveRadixTree.keyOrNull(subFloor(key));
	}

	@Override
	public final Entry<K, V> lowerEntry(K key) {
		return AdaptiveRadixTree.exportEntry(subLower(key));
	}

	@Override
	public final K lowerKey(K key) {
		return AdaptiveRadixTree.keyOrNull(subLower(key));
	}

	@Override
	public final K firstKey() {
		return AdaptiveRadixTree.key(subLowest());
	}

	@Override
	public final K lastKey() {
		return AdaptiveRadixTree.key(subHighest());
	}

	@Override
	public final Entry<K, V> firstEntry() {
		return AdaptiveRadixTree.exportEntry(subLowest());
	}

	@Override
	public final Entry<K, V> lastEntry() {
		return AdaptiveRadixTree.exportEntry(subHighest());
	}

	@Override
	public final Entry<K, V> pollFirstEntry() {
		LeafNode<K, V> e = subLowest();
		Entry<K, V> result = AdaptiveRadixTree.exportEntry(e);
		if (e != null)
			m.deleteEntry(e);
		return result;
	}

	@Override
	public final Entry<K, V> pollLastEntry() {
		LeafNode<K, V> e = subHighest();
		Entry<K, V> result = AdaptiveRadixTree.exportEntry(e);
		if (e != null)
			m.deleteEntry(e);
		return result;
	}

	// Views
	transient NavigableMap<K, V> descendingMapView;
	transient EntrySetView entrySetView;
	transient KeySet<K> navigableKeySetView;

	@Override
	public final NavigableSet<K> navigableKeySet() {
		KeySet<K> nksv = navigableKeySetView;
		return (nksv != null) ? nksv :
				(navigableKeySetView = new KeySet<>(this));
	}

	@Override
	public final Set<K> keySet() {
		return navigableKeySet();
	}

	@Override
	public NavigableSet<K> descendingKeySet() {
		return descendingMap().navigableKeySet();
	}

	@Override
	public final SortedMap<K, V> subMap(K fromKey, K toKey) {
		return subMap(fromKey, true, toKey, false);
	}

	@Override
	public final SortedMap<K, V> headMap(K toKey) {
		return headMap(toKey, false);
	}

	@Override
	public final SortedMap<K, V> tailMap(K fromKey) {
		return tailMap(fromKey, true);
	}

	// View classes

	// entry set views for submaps
	abstract class EntrySetView extends AbstractSet<Entry<K, V>> {
		private transient int size = -1, sizeModCount;

		// if the submap does not define any upper and lower bounds
		// i.e. it is the same view as the original map (very unlikely)
		// then no need to explicitly calculate the size.
		@Override
		public int size() {
			if (fromStart && toEnd)
				return m.size();
			// if size == -1, it is the first time we're calculating the size
			// if sizeModCount != m.getModCount(), the map has had modification operations
			// so it's size must've changed, recalculate.
			if (size == -1 || sizeModCount != m.getModCount()) {
				sizeModCount = m.getModCount();
				size = 0;
				Iterator<?> i = iterator();
				while (i.hasNext()) {
					size++;
					i.next();
				}
			}
			return size;
		}

		@Override
		public boolean isEmpty() {
			LeafNode<K, V> n = absLowest();
			return n == null || tooHigh(n.getKey());
		}

		// efficient impl of contains than the default in AbstractSet
		@Override
		public boolean contains(Object o) {
			if (!(o instanceof Map.Entry))
				return false;
			Entry<?, ?> entry = (Entry<?, ?>) o;
			Object key = entry.getKey();
			if (!inRange((K) key))
				return false;
			LeafNode<?, ?> node = m.getEntry(key);
			return node != null &&
					AdaptiveRadixTree.valEquals(node.getValue(), entry.getValue());
		}

		// efficient impl of remove than the default in AbstractSet
		@Override
		public boolean remove(Object o) {
			if (!(o instanceof Map.Entry))
				return false;
			Entry<?, ?> entry = (Entry<?, ?>) o;
			Object key = entry.getKey();
			if (!inRange((K) key))
				return false;
			LeafNode<K, V> node = m.getEntry(key);
			if (node != null && AdaptiveRadixTree.valEquals(node.getValue(),
                                                                               entry.getValue())) {
				m.deleteEntry(node);
				return true;
			}
			return false;
		}
	}


	/* Dummy value serving as unmatchable fence key for unbounded SubMapIterators */
	private static final Object UNBOUNDED = new Object();

	/*
	 *  Iterators for SubMaps
	 *  that understand the submap's upper and lower bound while iterating.
	 *  Fence is one of the bounds depending on the kind of iterator (ascending, descending)
	 *  and first becomes the other one to start from.
	 */
	abstract class SubMapIterator<T> implements Iterator<T> {
		LeafNode<K, V> lastReturned;
		LeafNode<K, V> next;
		final Object fenceKey;
		int expectedModCount;

		SubMapIterator(LeafNode<K, V> first,
				LeafNode<K, V> fence) {
			expectedModCount = m.getModCount();
			lastReturned = null;
			next = first;
			fenceKey = fence == null ? UNBOUNDED : fence.getKey();
		}

		@Override
		public final boolean hasNext() {
			return next != null && next.getKey() != fenceKey;
		}

		final LeafNode<K, V> nextEntry() {
			LeafNode<K, V> e = next;
			if (e == null || e.getKey() == fenceKey)
				throw new NoSuchElementException();
			if (m.getModCount() != expectedModCount)
				throw new ConcurrentModificationException();
			next = AdaptiveRadixTree.successor(e);
			lastReturned = e;
			return e;
		}

		final LeafNode<K, V> prevEntry() {
			LeafNode<K, V> e = next;
			if (e == null || e.getKey() == fenceKey)
				throw new NoSuchElementException();
			if (m.getModCount() != expectedModCount)
				throw new ConcurrentModificationException();
			next = AdaptiveRadixTree.predecessor(e);
			lastReturned = e;
			return e;
		}

		@Override
		public void remove() {
			if (lastReturned == null)
				throw new IllegalStateException();
			if (m.getModCount() != expectedModCount)
				throw new ConcurrentModificationException();
			// deleted entries are replaced by their successors
			//	if (lastReturned.left != null && lastReturned.right != null)
			//		next = lastReturned;
			m.deleteEntry(lastReturned);
			lastReturned = null;
			expectedModCount = m.getModCount();
		}
	}

	final class SubMapEntryIterator extends SubMapIterator<Entry<K, V>> {
		SubMapEntryIterator(LeafNode<K, V> first,
				LeafNode<K, V> fence) {
			super(first, fence);
		}

		@Override
		public Entry<K, V> next() {
			return nextEntry();
		}
	}

	final class DescendingSubMapEntryIterator extends SubMapIterator<Entry<K, V>> {
		DescendingSubMapEntryIterator(LeafNode<K, V> last,
				LeafNode<K, V> fence) {
			super(last, fence);
		}

		@Override
		public Entry<K, V> next() {
			return prevEntry();
		}
	}

	// Implement minimal Spliterator as KeySpliterator backup
	final class SubMapKeyIterator extends SubMapIterator<K>
			implements Spliterator<K> {
		SubMapKeyIterator(LeafNode<K, V> first,
				LeafNode<K, V> fence) {
			super(first, fence);
		}

		@Override
		public K next() {
			return nextEntry().getKey();
		}

		@Override
		public Spliterator<K> trySplit() {
			return null;
		}

		@Override
		public void forEachRemaining(Consumer<? super K> action) {
			while (hasNext())
				action.accept(next());
		}

		@Override
		public boolean tryAdvance(Consumer<? super K> action) {
			if (hasNext()) {
				action.accept(next());
				return true;
			}
			return false;
		}

		// estimating size of submap would be expensive
		// since we'd have to traverse from lower bound to upper bound
		// for this submap
		@Override
		public long estimateSize() {
			return Long.MAX_VALUE;
		}

		@Override
		public int characteristics() {
			return Spliterator.DISTINCT | Spliterator.ORDERED |
					Spliterator.SORTED;
		}

		@Override
		public final Comparator<? super K> getComparator() {
			return NavigableSubMap.this.comparator();
		}
	}

	final class DescendingSubMapKeyIterator extends SubMapIterator<K>
			implements Spliterator<K> {
		DescendingSubMapKeyIterator(LeafNode<K, V> last,
				LeafNode<K, V> fence) {
			super(last, fence);
		}

		@Override
		public K next() {
			return prevEntry().getKey();
		}

		@Override
		public Spliterator<K> trySplit() {
			return null;
		}

		@Override
		public void forEachRemaining(Consumer<? super K> action) {
			while (hasNext())
				action.accept(next());
		}

		@Override
		public boolean tryAdvance(Consumer<? super K> action) {
			if (hasNext()) {
				action.accept(next());
				return true;
			}
			return false;
		}

		@Override
		public long estimateSize() {
			return Long.MAX_VALUE;
		}

		@Override
		public int characteristics() {
			return Spliterator.DISTINCT | Spliterator.ORDERED;
		}
	}
}


