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

  /**
   * Mark this page as orphaned: it left its owning structure (cache mapping replaced,
   * truncate/TIL teardown) but concurrent holders may still guard it. {@link #close()} is
   * guard-aware — an orphaned page is torn down immediately when unguarded, otherwise by the
   * last {@link #releaseGuard()}.
   */
  void markOrphaned();

  void close();

  void incrementVersion();

  long getPageKey();

  int getRevision();

  IndexType getIndexType();
}
