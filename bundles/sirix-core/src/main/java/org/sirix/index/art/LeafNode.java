package org.sirix.index.art;

import java.util.Arrays;
import java.util.Map;

/*
    currently we use what the paper mentions as "Single-value" leaves
 */
class LeafNode<K, V> extends Node implements Map.Entry<K, V> {
	private V value;

	// we have to save the keyBytes, because leaves are lazy expanded at times
	private final byte[] keyBytes;
	private final K key;

	LeafNode(byte[] keyBytes, K key, V value) {
		this.value = value;
		// defensive copy
		this.keyBytes = Arrays.copyOf(keyBytes, keyBytes.length);
		this.key = key;
	}

	public V setValue(V value) {
		V oldValue = this.value;
		this.value = value;
		return oldValue;
	}

	public V getValue() {
		return value;
	}

	byte[] getKeyBytes() {
		return keyBytes;
	}

	public K getKey() {
		return key;
	}

	/**
	 Dev note: first() is implemented to detect end of the SortedMap.firstKey()
	 */
	@Override
	public Node first() {
		return null;
	}

	@Override
	public Node firstOrLeaf() {
		return null;
	}

	/**
	 Dev note: last() is implemented to detect end of the SortedMap.lastKey()
	 */
	@Override
	public Node last() {
		return null;
	}

	/**
	 * Compares this <code>Map.Entry</code> with another <code>Map.Entry</code>.
	 * <p>
	 * Implemented per API documentation of {@link Map.Entry#equals(Object)}
	 *
	 * @param obj  the object to compare to
	 * @return true if equal key and value
	 */
	@Override
	public boolean equals(final Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof Map.Entry)) {
			return false;
		}
		final Map.Entry<?, ?> other = (Map.Entry<?, ?>) obj;
		return
				(getKey() == null ? other.getKey() == null : getKey().equals(other.getKey())) &&
						(getValue() == null ? other.getValue() == null : getValue().equals(other.getValue()));
	}

	/**
	 * Gets a hashCode compatible with the equals method.
	 * <p>
	 * Implemented per API documentation of {@link Map.Entry#hashCode()}
	 *
	 * @return a suitable hash code
	 */
	@Override
	public int hashCode() {
		return (getKey() == null ? 0 : getKey().hashCode()) ^
				(getValue() == null ? 0 : getValue().hashCode());
	}

	@Override
	public String toString() {
		return key + "=" + value;
	}
}