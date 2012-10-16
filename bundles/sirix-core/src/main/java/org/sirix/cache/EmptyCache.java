package org.sirix.cache;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;

public class EmptyCache<K, V> implements Cache<K, V> {
  
  public EmptyCache() {
  }
  
  @Override
  public void clear() {
  }

  @Override
  public V get(@Nonnull K key) {
    return null;
  }

  @Override
  public void put(@Nonnull K key, @Nonnull V value) {
  }

  @Override
  public ImmutableMap<K, V> getAll(@Nonnull Iterable<? extends K> keys) {
    return null;
  }

  @Override
  public void putAll(@Nonnull Map<K, V> map) {
  }

  @Override
  public void toSecondCache() {
  }
  
  @Override
  public void remove(@Nonnull K key) {
  }

	@Override
	public void close() {
	}

}
