package org.sirix.cache;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;

public class EmptyCache<K, V> implements ICache<K, V> {
  
  public EmptyCache() {
  }
  
  @Override
  public void clear() {
  }

  @Override
  public V get(@Nonnull K pKey) {
    return null;
  }

  @Override
  public void put(@Nonnull K pKey, @Nonnull V pValue) {
  }

  @Override
  public ImmutableMap<K, V> getAll(@Nonnull Iterable<? extends K> pKeys) {
    return null;
  }

  @Override
  public void putAll(@Nonnull Map<K, V> pMap) {
  }

  @Override
  public void toSecondCache() {
  }
  
  @Override
  public void remove(@Nonnull K pKey) {
  }

	@Override
	public void close() {
	}

}
