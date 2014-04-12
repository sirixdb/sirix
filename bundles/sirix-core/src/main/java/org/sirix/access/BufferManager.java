package org.sirix.access;

import org.sirix.cache.Cache;
import org.sirix.page.interfaces.Page;

public interface BufferManager<K, V> {
	Cache<K, V> getReadingCache(final Page pageKind, final long revision);
	
	Cache<K, V> getTransactionCache(final Page pageKind);
}
