/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.access.trx.page;

import io.sirix.api.StorageEngineReader;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.Semaphore;

/**
 * HOT trie reader for HOT (Height Optimized Trie) navigation.
 * 
 * <p>
 * This class provides read-only access to HOT indexes with proper guard management to prevent page
 * eviction during active use.
 * </p>
 * 
 * <p>
 * <b>Key Features:</b>
 * </p>
 * <ul>
 * <li>Guard acquisition for page lifetime management</li>
 * <li>Zero-copy value access via MemorySegment slices</li>
 * <li>SIMD-optimized child lookup via HOTIndirectPage</li>
 * <li>Pre-allocated traversal arrays for zero allocations</li>
 * </ul>
 * 
 * <p>
 * <b>Usage:</b>
 * </p>
 * 
 * <pre>{@code
 * try (HOTTrieReader reader = new HOTTrieReader(storageEngineReader)) {
 *   MemorySegment value = reader.get(rootRef, key);
 *   if (value != null) {
 *     // Use value...
 *   }
 * }
 * }</pre>
 * 
 * @author Johannes Lichtenberger
 * @see HOTLeafPage
 * @see HOTIndirectPage
 * @see HOTTrieWriter
 */
public final class HOTTrieReader implements AutoCloseable {

  /**
   * Maximum tree height for pre-allocated path traversal arrays.
   *
   * <p>With minimum fanout of 2 (BiNode): max height = log2(2^63) ~ 63.
   * With typical fanout of 16+ (SpanNode/MultiNode): height ~ 13.
   * We use 64 as a generous safety margin. Exceeding this limit indicates
   * a bug in split/merge logic or index corruption.</p>
   */
  private static final int MAX_TREE_HEIGHT = 64;

  /**
   * Cursor sibling-prefetch window. Tunable via {@code -Dsirix.hot.prefetch.window=N}.
   * Default 16 matches NVMe command-queue sweet spot; too-small starves the I/O
   * scheduler; too-large bloats the virtual-thread carrier pool without further
   * gain once the device is saturated.
   */
  private static final int PREFETCH_WINDOW =
      Integer.getInteger("sirix.hot.prefetch.window", 16);

  /**
   * Maximum concurrent in-flight prefetch virtual threads across the whole JVM.
   *
   * <p><b>iter#04 measurement finding (see
   * {@code profiling-output/iter04-prefetcher-analysis.md} and
   * {@code profiling-output/iteration-log.md} iter#04 section):</b> on the cold
   * 100 M brackit-scale bench, any non-zero cap is a net loss versus disabling
   * prefetching entirely. Concrete 5-round alternating A/B medians:</p>
   *
   * <table>
   *   <caption>Cold-wall medians by prefetch cap</caption>
   *   <tr><th>cap</th><th>median wall</th><th>hydrate median</th></tr>
   *   <tr><td>0 (disabled)</td><td>5.10 s</td><td>1,178 ms</td></tr>
   *   <tr><td>40 (= {@code 2 × cores})</td><td>5.59 s</td><td>1,362 ms</td></tr>
   *   <tr><td>1024 (unbounded)</td><td>5.68 s</td><td>1,395 ms</td></tr>
   * </table>
   *
   * <p>The lock-profile evidence: with prefetch unbounded,
   * {@code sun.nio.ch.NativeThreadSet} accumulates <b>6× more contention-time</b>
   * (38.2 billion ns vs 6.7 billion ns) because every concurrent
   * {@code FileChannel.read} acquires this lock. Hundreds of prefetch virtual
   * threads hammer it in parallel with the synchronous reader, starving the
   * sync path of NVMe command-queue slots and direct-buffer pool entries.</p>
   *
   * <p>Default: <b>0 (prefetching disabled)</b>. The Semaphore machinery is
   * retained as an opt-in rollback ({@code -Dsirix.hot.prefetch.parallelism=N>0})
   * in case a different workload (deeper tree, higher-latency storage) makes
   * prefetching net-positive.</p>
   *
   * <p>Tunable via {@code -Dsirix.hot.prefetch.parallelism=N}:</p>
   * <ul>
   * <li>{@code N == 0}: Prefetching disabled entirely (default).</li>
   * <li>{@code N > 0}: Semaphore cap = {@code N}. At most {@code N} concurrent
   *   prefetch virtual threads are in flight JVM-wide; additional requests are
   *   silently dropped (the synchronous reader loads on demand).</li>
   * </ul>
   *
   * <p>HFT-grade properties: {@code tryAcquire()} is lock-free on the fast path
   * (AQS CAS on the permit counter). No caller ever parks on this Semaphore —
   * {@link #prefetchPage} uses {@code tryAcquire → skip} so a full-to-capacity
   * prefetcher simply drops the hint rather than serializing the descent. With
   * the default of 0 permits, every {@code tryAcquire} returns {@code false}
   * on a single CAS with no allocation.</p>
   */
  private static final int PREFETCH_PARALLELISM_DEFAULT = 0;

  /** Current prefetch-parallelism cap. Volatile because the test hook
   *  {@link #setPrefetchParallelismForTest(int)} rebuilds the limiter on another
   *  thread and we want readers to see the latest reference. */
  private static volatile Semaphore PREFETCH_LIMIT = initialPrefetchLimit();

  private static Semaphore initialPrefetchLimit() {
    final int configured =
        Integer.getInteger("sirix.hot.prefetch.parallelism", PREFETCH_PARALLELISM_DEFAULT);
    // N == 0 → disable prefetching. Represent by a zero-permit Semaphore so the
    // tryAcquire branch always returns false without allocating or branching on
    // a second flag.
    return new Semaphore(Math.max(0, configured));
  }

  /** The storage engine reader. */
  private final StorageEngineReader storageEngineReader;

  // ===== Pre-allocated traversal path - ZERO allocations on hot path! =====
  private final PageReference[] pathRefs = new PageReference[MAX_TREE_HEIGHT];
  private final HOTIndirectPage[] pathNodes = new HOTIndirectPage[MAX_TREE_HEIGHT];
  private final int[] pathChildIndices = new int[MAX_TREE_HEIGHT];
  private int pathDepth = 0;

  // ===== Currently guarded leaf page =====
  private HOTLeafPage guardedLeaf = null;

  /**
   * Create a new HOTTrieReader.
   *
   * @param storageEngineReader the storage engine reader
   */
  public HOTTrieReader(StorageEngineReader storageEngineReader) {
    this.storageEngineReader = Objects.requireNonNull(storageEngineReader);
  }

  /**
   * Find value for exact key match. Returns null if not found - no Optional allocation!
   *
   * @param rootRef the root page reference
   * @param key the search key
   * @return the value as a MemorySegment slice, or null if not found
   */
  public @Nullable MemorySegment get(PageReference rootRef, byte[] key) {
    Objects.requireNonNull(rootRef);
    Objects.requireNonNull(key);

    // NOTE: a min/max-range leaf cache is UNSAFE for HOT. Leaves in HOT
    // partition by PEXT of disc bits, not by total key order — two
    // distinct leaves can have overlapping [firstKey, lastKey] ranges.
    // A key K whose value matches cached.first <= K <= cached.last may
    // belong to a different leaf. Caching by range produces false
    // negatives (returns null for keys that exist in a different leaf).
    // The correct fast path would key on the exact routing decisions
    // that land at the leaf; for now we always re-navigate, which is
    // still cheap for log_32-shallow HOT trees.

    // Release any previously guarded leaf.
    releaseGuardedLeaf();
    final HOTLeafPage leaf = navigateToLeaf(rootRef, key);
    if (leaf == null) return null;
    if (!leaf.acquireGuard()) return null;
    guardedLeaf = leaf;
    try {
      final int index = leaf.findEntry(key);
      if (index < 0) return null;
      return leaf.getValueSlice(index);
    } catch (Exception e) {
      releaseGuardedLeaf();
      throw e;
    }
  }

  /**
   * Check if a key exists in the trie.
   *
   * @param rootRef the root page reference
   * @param key the search key
   * @return true if key exists
   */
  public boolean containsKey(PageReference rootRef, byte[] key) {
    Objects.requireNonNull(rootRef);
    Objects.requireNonNull(key);

    // Release any previously guarded leaf
    releaseGuardedLeaf();

    HOTLeafPage leaf = navigateToLeaf(rootRef, key);
    if (leaf == null) {
      return false;
    }

    // Acquire guard temporarily. If the page was evicted between navigation
    // and here, acquireGuard returns false — treat as not found.
    if (!leaf.acquireGuard()) {
      return false;
    }
    try {
      int index = leaf.findEntry(key);
      return index >= 0;
    } finally {
      leaf.releaseGuard();
    }
  }

  /**
   * Create a range cursor for iterating over a key range.
   *
   * @param rootRef the root page reference
   * @param fromKey the start key (inclusive)
   * @param toKey the end key (inclusive)
   * @return the range cursor
   */
  public HOTRangeCursor range(PageReference rootRef, byte[] fromKey, byte[] toKey) {
    return new HOTRangeCursor(this, rootRef, fromKey, toKey);
  }

  /**
   * Navigate through HOT trie to reach the leaf containing the key. Uses pre-allocated path arrays -
   * ZERO allocations!
   * 
   * <p>
   * <b>Prefetching Strategy (Reference: thesis section 4.3.4):</b>
   * </p>
   * <p>
   * For optimal performance on modern CPUs with deep memory hierarchies:
   * </p>
   * <ul>
   * <li>Prefetch child's first cache line before navigating (hide memory latency)</li>
   * <li>HOT's compound nodes reduce tree height → fewer prefetch opportunities needed</li>
   * <li>Java's MemorySegment.prefetch() can be used with off-heap pages (JDK 21+)</li>
   * </ul>
   *
   * @param rootRef the root reference
   * @param key the search key
   * @return the leaf page, or null if not found
   */
  public @Nullable HOTLeafPage navigateToLeaf(PageReference rootRef, byte[] key) {
    pathDepth = 0;
    PageReference currentRef = rootRef;

    while (true) {
      Page page = loadPage(currentRef);
      if (page == null) {
        return null; // Empty trie
      }

      if (page instanceof HOTLeafPage leaf) {
        return leaf;
      }

      if (!(page instanceof HOTIndirectPage hotNode)) {
        return null; // Unexpected page type
      }

      // Find child reference using HOT node type-specific logic (uses PEXT/Long.compress)
      final int childIndex = hotNode.findChildIndex(key);
      if (childIndex < 0) {
        return null; // Key not found
      }

      final PageReference childRef = hotNode.getChildReference(childIndex);
      if (childRef == null) {
        return null;
      }

      // Async SSD prefetch: fire-and-forget load of the next sibling's page on a virtual thread.
      // Overlaps SSD I/O with the CPU work of descending into the current subtree.
      final int nextSibling = childIndex + 1;
      if (nextSibling < hotNode.getNumChildren()) {
        final PageReference siblingRef = hotNode.getChildReference(nextSibling);
        if (siblingRef != null && siblingRef.getPage() == null && siblingRef.getKey() >= 0) {
          prefetchPage(siblingRef);
        }
      }

      // Record path for parent-based range traversal
      pushPath(currentRef, hotNode, childIndex);

      currentRef = childRef;
    }
  }

  /**
   * Navigate to the leftmost leaf in the subtree. Used for range scan initialization.
   *
   * @param rootRef the root reference
   * @return the leftmost leaf, or null if empty
   */
  public @Nullable HOTLeafPage navigateToLeftmostLeaf(PageReference rootRef) {
    pathDepth = 0;
    PageReference currentRef = rootRef;

    while (true) {
      Page page = loadPage(currentRef);
      if (page == null) {
        return null;
      }

      if (page instanceof HOTLeafPage leaf) {
        return leaf;
      }

      if (!(page instanceof HOTIndirectPage hotNode)) {
        return null;
      }

      // Take the first (leftmost) child
      final int childIndex = 0;
      final PageReference childRef = hotNode.getChildReference(childIndex);
      if (childRef == null) {
        return null;
      }

      // Async SSD prefetch: fire-and-forget load of second child on a virtual thread.
      if (hotNode.getNumChildren() > 1) {
        final PageReference siblingRef = hotNode.getChildReference(1);
        if (siblingRef != null && siblingRef.getPage() == null && siblingRef.getKey() >= 0) {
          prefetchPage(siblingRef);
        }
      }

      pushPath(currentRef, hotNode, childIndex);
      currentRef = childRef;
    }
  }

  /**
   * Advance to the next leaf in sorted order using parent-based traversal. This is the COW-compatible
   * alternative to sibling pointers.
   *
   * @return the next leaf, or null if no more leaves
   */
  public @Nullable HOTLeafPage advanceToNextLeaf() {
    // Pop back up the tree until we find an unvisited sibling
    while (pathDepth > 0) {
      int parentIdx = pathDepth - 1;
      HOTIndirectPage parent = pathNodes[parentIdx];
      int currentChildIdx = pathChildIndices[parentIdx];
      int numChildren = parent.getNumChildren();

      // Check if there's a next sibling
      if (currentChildIdx + 1 < numChildren) {
        // Found next sibling - descend to its leftmost leaf
        final int nextChildIdx = currentChildIdx + 1;
        pathChildIndices[parentIdx] = nextChildIdx;

        final PageReference nextChildRef = parent.getChildReference(nextChildIdx);
        if (nextChildRef != null) {
          // Prefetch-batch: issue PREFETCH_WINDOW in-flight reads for the upcoming
          // siblings. Deepens NVMe/io_uring queue depth — on FFM-io_uring storage
          // these coalesce into a single submit; on FILE_CHANNEL each fires on
          // a separate virtual thread and kernel I/O scheduler interleaves them.
          prefetchSiblingWindow(parent, nextChildIdx + 1, numChildren);

          final HOTLeafPage result = descendToLeftmostLeaf(nextChildRef);
          if (result != null) {
            return result;
          }
          // If descend failed, continue to next sibling or pop up
        }
      }

      // No more siblings at this level, pop up
      pathDepth--;
    }

    // Exhausted the tree
    return null;
  }

  /**
   * Descend to the leftmost leaf from a given reference. Prefetches the next sibling
   * at each level for range scan readahead.
   */
  private @Nullable HOTLeafPage descendToLeftmostLeaf(PageReference ref) {
    PageReference currentRef = ref;

    while (true) {
      final Page page = loadPage(currentRef);
      if (page == null) {
        return null;
      }

      if (page instanceof HOTLeafPage leaf) {
        return leaf;
      }

      if (!(page instanceof HOTIndirectPage hotNode)) {
        return null;
      }

      final int childIndex = 0;
      final PageReference childRef = hotNode.getChildReference(childIndex);
      if (childRef == null) {
        return null;
      }

      // Prefetch-batch at descent: schedule PREFETCH_WINDOW in-flight reads for
      // the current inner node's first N children. Saturates queue depth on the
      // way down — the cursor will visit all of them in order anyway.
      prefetchSiblingWindow(hotNode, 1, hotNode.getNumChildren());

      pushPath(currentRef, hotNode, childIndex);
      currentRef = childRef;
    }
  }

  /**
   * Prefetch up to {@link #PREFETCH_WINDOW} consecutive child references of
   * {@code parent} starting at index {@code startIdx} (inclusive) up to
   * {@code numChildren} (exclusive). Each eligible reference (not already
   * swizzled, with a valid disk key) fires a fire-and-forget async load.
   *
   * <p>Why a window rather than a single sibling: the kernel I/O scheduler and
   * NVMe command queue benefit from queue depths in the 8-32 range. A single
   * one-ahead prefetch leaves the device idle between reads on cold cache. A
   * window of {@value #PREFETCH_WINDOW} matches typical NVMe QD sweet spots
   * and, on FFM-io_uring storage, the individual reads can be batched into a
   * single {@code io_uring_enter} submit on the underlying reader.
   */
  private void prefetchSiblingWindow(final HOTIndirectPage parent, final int startIdx,
      final int numChildren) {
    final int end = Math.min(startIdx + PREFETCH_WINDOW, numChildren);
    for (int i = startIdx; i < end; i++) {
      final PageReference ref = parent.getChildReference(i);
      if (ref != null && ref.getPage() == null && ref.getKey() >= 0) {
        prefetchPage(ref);
      }
    }
  }

  /**
   * Flyweight push for traversal path - no allocation!
   */
  private void pushPath(PageReference ref, HOTIndirectPage node, int childIdx) {
    if (pathDepth >= MAX_TREE_HEIGHT) {
      throw new IllegalStateException("HOT tree exceeds maximum height: " + MAX_TREE_HEIGHT);
    }
    pathRefs[pathDepth] = ref;
    pathNodes[pathDepth] = node;
    pathChildIndices[pathDepth] = childIdx;
    pathDepth++;
  }

  /**
   * Clear traversal path (allows GC but no allocation).
   */
  void clearPath() {
    for (int i = 0; i < pathDepth; i++) {
      pathRefs[i] = null;
      pathNodes[i] = null;
    }
    pathDepth = 0;
  }

  /**
   * Get the current traversal path depth.
   */
  public int getPathDepth() {
    return pathDepth;
  }

  /** Diagnostic accessor: indirect node at the given path depth. */
  public HOTIndirectPage pathNodeAt(int depth) {
    return pathNodes[depth];
  }

  /** Diagnostic accessor: child index taken at the given path depth. */
  public int pathChildAt(int depth) {
    return pathChildIndices[depth];
  }

  /** Diagnostic: clear the traversal path. Public wrapper around clearPath(). */
  public void clearPathPublic() {
    clearPath();
  }

  /**
   * Load a page from storage. Checks the page reference's in-memory page first (swizzle check),
   * then falls back to storage. After loading from storage, the page is swizzled onto the
   * reference so subsequent accesses avoid SSD I/O entirely.
   *
   * <p><b>SSD optimization:</b> Swizzling loaded pages onto their PageReference eliminates
   * redundant I/O for repeated traversals through the same internal nodes. Since HOT trees
   * have low height (typically 3-5 levels with compound nodes), keeping all internal nodes
   * swizzled is memory-efficient and avoids the dominant cost of random SSD reads.</p>
   */
  private @Nullable Page loadPage(PageReference ref) {
    // First check if page is already swizzled (in-memory from transaction log or prior load)
    final Page inMemory = ref.getPage();
    if (inMemory != null) {
      return inMemory;
    }

    // CRITICAL: Check BOTH storage key AND log key before giving up.
    // When a page is in the transaction log, key is set to NULL_ID_LONG (-1)
    // but logKey is set to the index in the log. We must call loadHOTPage
    // which checks the transaction log using the logKey.
    if (ref.getKey() < 0 && ref.getLogKey() < 0) {
      return null; // Page not in storage AND not in transaction log
    }

    // Load from storage via the storage engine reader
    // The storage engine will handle versioning/fragment combining AND transaction log lookup
    final Page loaded = storageEngineReader.loadHOTPage(ref);

    // Swizzle: pin the loaded page on the reference so future accesses skip I/O.
    // This is safe because PageReference.setPage is idempotent and HOT pages are
    // immutable once loaded (COW creates new pages for modifications).
    if (loaded != null) {
      ref.setPage(loaded);
    }

    return loaded;
  }

  /**
   * Asynchronously prefetch a page into swizzled state.
   *
   * <p><b>SSD optimization:</b> Fires the I/O on a virtual thread so the calling traversal
   * can continue descending without blocking on sibling I/O. When the virtual thread
   * completes, the result is swizzled onto the PageReference (volatile field). If the
   * traversal later reaches this reference, {@link #loadPage} finds it already swizzled
   * and avoids a redundant read.</p>
   *
   * <p>Race safety: if {@code loadPage} is called before the async task finishes,
   * it will not find a swizzled page and will load synchronously. The duplicate load
   * is benign — both paths produce the same immutable page and {@code setPage} on
   * the volatile field is idempotent.</p>
   *
   * <p><b>Concurrency cap:</b> Gated by {@link #PREFETCH_LIMIT}. When the Semaphore
   * is drained (prefetcher already saturating the kernel I/O queue), the call
   * returns immediately without starting a virtual thread — the hint is dropped
   * and the synchronous {@link #loadPage} path will load on demand. See
   * {@code iter04-prefetcher-analysis.md} for the formal-verification argument
   * that skipping prefetch never affects correctness of the returned
   * {@code MemorySegment} values.</p>
   */
  private void prefetchPage(PageReference ref) {
    final Semaphore limit = PREFETCH_LIMIT;
    if (!limit.tryAcquire()) {
      // Either (a) prefetching disabled (N=0 permits) or (b) cap reached.
      // Skip-on-contention is safe: the synchronous reader will load the page
      // on demand when it reaches this ref.
      return;
    }
    // IMPORTANT: release path is try/finally inside the virtual thread body so
    // any Throwable from loadHOTPage/setPage does not leak a permit.
    Thread.startVirtualThread(() -> {
      try {
        final Page loaded = storageEngineReader.loadHOTPage(ref);
        if (loaded != null) {
          ref.setPage(loaded);
        }
      } catch (Throwable t) {
        // Prefetch is a hint — swallow failures. The synchronous read path
        // will re-attempt the load and propagate the error to the caller if
        // it's a real IO error (not a benign race with concurrent eviction).
      } finally {
        limit.release();
      }
    });
  }

  /**
   * Test-only hook: rebuild the static {@link #PREFETCH_LIMIT} with a new
   * permit cap. Needed because the default cap is computed at class-load
   * time. Declared {@code public} only so cross-package tests
   * ({@code io.sirix.index.projection.ProjectionIndexHOTStorageTest}) can
   * sweep permit values without reflection.
   *
   * <p><b>Do not call from production code.</b> Use the JVM property
   * {@code -Dsirix.hot.prefetch.parallelism=N} instead.</p>
   *
   * <p>Caller must ensure no prefetch virtual threads are outstanding against
   * the previous {@code PREFETCH_LIMIT} (otherwise the release on the old
   * Semaphore is harmless — the old instance is GC'd once all tasks complete).</p>
   */
  public static void setPrefetchParallelismForTest(int permits) {
    if (permits < 0) {
      throw new IllegalArgumentException("permits must be >= 0");
    }
    PREFETCH_LIMIT = new Semaphore(permits);
  }

  /**
   * Test-only accessor for the current Semaphore's available-permit count.
   * Declared {@code public} for the same reason as
   * {@link #setPrefetchParallelismForTest(int)}. <b>Do not call from production
   * code.</b>
   */
  public static int getPrefetchAvailablePermitsForTest() {
    return PREFETCH_LIMIT.availablePermits();
  }

  /**
   * Release the currently guarded leaf page.
   */
  private void releaseGuardedLeaf() {
    if (guardedLeaf != null) {
      guardedLeaf.releaseGuard();
      guardedLeaf = null;
    }
  }

  /**
   * Get the storage engine reader.
   */
  StorageEngineReader getStorageEngineReader() {
    return storageEngineReader;
  }

  @Override
  public void close() {
    releaseGuardedLeaf();
    clearPath();
  }
}

