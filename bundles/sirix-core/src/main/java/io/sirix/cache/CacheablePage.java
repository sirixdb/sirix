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

  /**
   * Discard this page: free its frame now if nobody holds a guard, or at the last
   * {@link #releaseGuard()} if someone does.
   *
   * <p><b>Use this rather than a bare {@link #close()} whenever a page is being dropped.</b>
   * {@code KeyValueLeafPage.close()} RETURNS EARLY on a guarded page <em>without</em> setting the
   * orphan bit, so it neither frees the frame nor arranges for anything to free it later — the frame
   * stays pinned for the process's lifetime. Setting the orphan bit first is what makes the holder's
   * last release complete the teardown. ({@code HOTLeafPage.close()} already orphans; this makes the
   * two types behave alike so callers need not know which they hold.)</p>
   *
   * <p>Only for a page the caller owns. One still owned by a shared cache must leave that cache
   * first, or it is freed under the readers the cache continues to hand it to. And never drain
   * guards to "make it free sooner" — that frees the frame under its actual holder.</p>
   */
  default void retire() {
    markOrphaned();
    close();
  }

  void incrementVersion();

  long getPageKey();

  int getRevision();

  IndexType getIndexType();
}
