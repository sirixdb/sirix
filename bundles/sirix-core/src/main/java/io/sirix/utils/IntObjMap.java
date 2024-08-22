package io.sirix.utils;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This is an efficient and memory-saving hash map for storing primitive
 * integers and objects. It extends the {@link IntSet} class.
 *
 * @param <E>
 *            generic value type
 * @author BaseX Team 2005-22, BSD License
 * @author Christian Gruen
 */
public final class IntObjMap<E> extends IntSet {
	/**
	 * Values.
	 */
	private Object[] values;

	/**
	 * Default constructor.
	 */
	public IntObjMap() {
		values = new Object[capacity()];
	}

	/**
	 * Default constructor.
	 */
	public IntObjMap(int capacity) {
		super(capacity);
		values = new Object[capacity()];
	}

	/**
	 * Indexes the specified key and stores the associated value. If the key already
	 * exists, the value is updated.
	 *
	 * @param key
	 *            key
	 * @param value
	 *            value
	 * @return old value
	 */
	@SuppressWarnings("unchecked")
	public E put(final int key, final E value) {
		// array bounds are checked before array is resized..
		final int i = put(key);
		final Object v = values[i];
		values[i] = value;
		return (E) v;
	}

	/**
	 * Returns the value for the specified key. Creates a new value if none exists.
	 *
	 * @param key
	 *            key
	 * @param func
	 *            function that create a new value
	 * @return value
	 */
	public E computeIfAbsent(final int key, final Supplier<? extends E> func) {
		E value = get(key);
		if (value == null) {
			value = func.get();
			put(key, value);
		}
		return value;
	}

	/**
	 * Returns the value for the specified key.
	 *
	 * @param key
	 *            key to be looked up
	 * @return value or {@code null} if the key was not found
	 */
	@SuppressWarnings("unchecked")
	public E get(final int key) {
		return (E) values[id(key)];
	}

	/**
	 * Returns a value iterator.
	 *
	 * @return iterator
	 */
	public Iterable<E> values() {
		return new ArrayIterator<>(values, 1, size);
	}

	public Collection<Integer> keys() {
		return Arrays.stream(keys).boxed().collect(Collectors.toList());
	}

	public int[] keyArray() {
		return keys;
	}

	@Override
	protected void rehash(final int newSize) {
		super.rehash(newSize);
		values = Array.copy(values, new Object[newSize]);
	}

	@Override
	public void clear() {
		super.clear();
		Arrays.fill(values, null);
	}

	@Override
	public String toString() {
		final List<Object> k = new ArrayList<>(keys.length);
		for (final int key : keys)
			k.add(key);
		return toString(k.toArray(), values);
	}

	public Set<Map.Entry<Integer, E>> entrySet() {
		final Set<Map.Entry<Integer, E>> set = new HashSet<>();
		for (int i = 1; i < size; i++) {
			set.add(new AbstractMap.SimpleEntry<>(keys[i], (E) values[i]));
		}
		return set;
	}
}
