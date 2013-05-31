package org.sirix.cache;

import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

public class EmptyCache<K, V> implements Cache<K, V> {

	public EmptyCache() {
	}

	@Override
	public void clear() {
	}

	@Override
	public V get(K key) {
		return null;
	}

	@Override
	public void put(K key, @Nonnull V value) {
	}

	@Override
	public ImmutableMap<K, V> getAll(Iterable<? extends K> keys) {
		return null;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> map) {
	}

	@Override
	public void toSecondCache() {
	}

	@Override
	public void remove(K key) {
	}

	@Override
	public void close() {
	}

}
