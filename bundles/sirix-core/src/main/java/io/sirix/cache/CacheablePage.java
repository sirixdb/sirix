package io.sirix.cache;

import io.sirix.index.IndexType;

/**
 * Interface for pages that hold off-heap MemorySegments and can be managed by
 * {@link ShardedPageCache}. Both {@code KeyValueLeafPage} and {@code HOTLeafPage}
 * implement this to enable unified budget-aware caching with clock-sweep eviction.
 */
public interface CacheablePage {

  long getActualMemorySize();

  void markAccessed();

  boolean isHot();

  void clearHot();

  boolean acquireGuard();

  void releaseGuard();

  int getGuardCount();

  boolean isClosed();

  void close();

  void incrementVersion();

  long getPageKey();

  int getRevision();

  IndexType getIndexType();
}
