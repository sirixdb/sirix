package io.sirix.cache;

import io.sirix.index.IndexType;
import io.sirix.page.PageReference;

/**
 * Interface for pages that hold off-heap MemorySegments and can be managed by
 * {@link ShardedPageCache}. Both {@code KeyValueLeafPage} and {@code HOTLeafPage} implement this to
 * enable unified budget-aware caching with clock-sweep eviction.
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
   * Mark this page as orphaned: it left its owning structure (cache mapping replaced, truncate/TIL
   * teardown) but concurrent holders may still guard it. {@link #close()} is guard-aware — an
   * orphaned page is torn down immediately when unguarded, otherwise by the last
   * {@link #releaseGuard()}.
   */
  void markOrphaned();

  void close();

  /**
   * Discard this page: free its frame now if nobody holds a guard, or at the last
   * {@link #releaseGuard()} if someone does.
   *
   * <p>
   * <b>Use this rather than a bare {@link #close()} whenever a page is being dropped.</b>
   * {@code KeyValueLeafPage.close()} RETURNS EARLY on a guarded page <em>without</em> setting the
   * orphan bit, so it neither frees the frame nor arranges for anything to free it later — the frame
   * stays pinned for the process's lifetime. Setting the orphan bit first is what makes the holder's
   * last release complete the teardown. ({@code HOTLeafPage.close()} already orphans; this makes the
   * two types behave alike so callers need not know which they hold.)
   * </p>
   *
   * <p>
   * Only for a page the caller owns. One still owned by a shared cache must leave that cache first,
   * or it is freed under the readers the cache continues to hand it to. And never drain guards to
   * "make it free sooner" — that frees the frame under its actual holder.
   * </p>
   */
  default void retire() {
    markOrphaned();
    close();
  }

  void incrementVersion();

  long getPageKey();

  /**
   * The {@link PageReference} this page is cached under, or {@code null} if no mapping owns it: it
   * was never cached, it has since been detached, or the implementation does not track the key.
   *
   * <p>
   * Exists so a caller holding only a page instance can ask "is this still owned by the cache?"
   * without scanning every entry. That question has no other cheap answer for a HOT leaf: its cache
   * key is derived from its ROOT REFERENCE's disk offset, which the page itself does not carry, so
   * the key cannot be reconstructed from the page the way a record page's can be from
   * {@link #getPageKey()}.
   *
   * <p>
   * <b>A POSITIVE identity match is authoritative; a non-null MISMATCH is not.</b> A page re-cached
   * under a new reference (copy-on-write moves the root reference's key) remembers only the latest
   * one, so a mismatch means "not under this key", NOT "not in the cache" — callers must fall back to
   * an exact check before treating the page as unowned and freeing it.
   *
   * <p>
   * <b>{@code null} is conclusive only for an implementation that really records the key.</b>
   * {@link ShardedPageCache} records it inside the same per-key lock that publishes the mapping and
   * forgets it on every mapping removal, so for such a page type no key means no mapping owns this
   * instance — which is what lets {@code ShardedPageCache.removePage} skip its exact scan (it asserts
   * that invariant rather than assuming it). A page type that keeps the no-op setter below always
   * reports {@code null} and so says nothing; telling the two apart is the cache's job, not the
   * caller's.
   *
   * @return the reference this page is cached under, or {@code null}
   */
  default PageReference lastCacheKey() {
    return null;
  }

  /**
   * Records the reference this page is being cached under, or forgets it. Called by the cache inside
   * the compute that publishes the mapping, and again with {@code null} when that mapping is removed;
   * not part of the page's identity and never persisted.
   *
   * <p>
   * An implementation that stores the key must publish it safely (a volatile field):
   * {@link #lastCacheKey()} is read outside the lock that records it.
   *
   * @param cacheKey the reference the page is being stored under, or {@code null} to forget the key
   */
  default void setLastCacheKey(final PageReference cacheKey) {
    // No-op by default — implementations that want the O(1) ownership probe store it.
  }

  int getRevision();

  IndexType getIndexType();
}
