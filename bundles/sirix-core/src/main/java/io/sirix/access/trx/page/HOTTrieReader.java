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
import io.sirix.index.hot.DiscriminativeBitComputer;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.concurrent.Semaphore;

/**
 * HOT trie reader for HOT (Height Optimized Trie) navigation.
 * 
 * <p>
 * This class provides read-only access to HOT indexes with OPTIMISTIC stamp validation instead of
 * page pinning: leaves stay evictable at all times, every batch of leaf-content reads is confirmed
 * against the FrameSlotAllocator's per-slot seqlock version before its result escapes, and a failed
 * validation retries on a freshly reloaded copy of the same immutable content.
 * </p>
 *
 * <p>
 * <b>Key Features:</b>
 * </p>
 * <ul>
 * <li>Optimistic stamp validation for page lifetime safety — no pins, no guard churn</li>
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
 * @see io.sirix.index.hot.AbstractHOTIndexWriter
 */
public final class HOTTrieReader implements AutoCloseable {

  /**
   * Maximum tree height for pre-allocated path traversal arrays.
   *
   * <p>
   * With minimum fanout of 2 (BiNode): max height = log2(2^63) ~ 63. With typical fanout of 16+
   * (SpanNode/MultiNode): height ~ 13. We use 64 as a generous safety margin. Exceeding this limit
   * indicates a bug in split/merge logic or index corruption.
   * </p>
   */
  private static final int MAX_TREE_HEIGHT = 64;

  /**
   * Cursor sibling-prefetch window. Tunable via {@code -Dsirix.hot.prefetch.window=N}. Default 16
   * matches NVMe command-queue sweet spot; too-small starves the I/O scheduler; too-large bloats the
   * virtual-thread carrier pool without further gain once the device is saturated.
   */
  private static final int PREFETCH_WINDOW = Integer.getInteger("sirix.hot.prefetch.window", 16);

  /**
   * Maximum concurrent in-flight prefetch virtual threads across the whole JVM.
   *
   * <p>
   * <b>iter#04 measurement finding (see {@code profiling-output/iter04-prefetcher-analysis.md} and
   * {@code profiling-output/iteration-log.md} iter#04 section):</b> on the cold 100 M brackit-scale
   * bench, any non-zero cap is a net loss versus disabling prefetching entirely. Concrete 5-round
   * alternating A/B medians:
   * </p>
   *
   * <table>
   * <caption>Cold-wall medians by prefetch cap</caption>
   * <tr>
   * <th>cap</th>
   * <th>median wall</th>
   * <th>hydrate median</th>
   * </tr>
   * <tr>
   * <td>0 (disabled)</td>
   * <td>5.10 s</td>
   * <td>1,178 ms</td>
   * </tr>
   * <tr>
   * <td>40 (= {@code 2 × cores})</td>
   * <td>5.59 s</td>
   * <td>1,362 ms</td>
   * </tr>
   * <tr>
   * <td>1024 (unbounded)</td>
   * <td>5.68 s</td>
   * <td>1,395 ms</td>
   * </tr>
   * </table>
   *
   * <p>
   * The lock-profile evidence: with prefetch unbounded, {@code sun.nio.ch.NativeThreadSet}
   * accumulates <b>6× more contention-time</b> (38.2 billion ns vs 6.7 billion ns) because every
   * concurrent {@code FileChannel.read} acquires this lock. Hundreds of prefetch virtual threads
   * hammer it in parallel with the synchronous reader, starving the sync path of NVMe command-queue
   * slots and direct-buffer pool entries.
   * </p>
   *
   * <p>
   * Default: <b>0 (prefetching disabled)</b>. The Semaphore machinery is retained as an opt-in
   * rollback ({@code -Dsirix.hot.prefetch.parallelism=N>0}) in case a different workload (deeper
   * tree, higher-latency storage) makes prefetching net-positive.
   * </p>
   *
   * <p>
   * Tunable via {@code -Dsirix.hot.prefetch.parallelism=N}:
   * </p>
   * <ul>
   * <li>{@code N == 0}: Prefetching disabled entirely (default).</li>
   * <li>{@code N > 0}: Semaphore cap = {@code N}. At most {@code N} concurrent prefetch virtual
   * threads are in flight JVM-wide; additional requests are silently dropped (the synchronous reader
   * loads on demand).</li>
   * </ul>
   *
   * <p>
   * HFT-grade properties: {@code tryAcquire()} is lock-free on the fast path (AQS CAS on the permit
   * counter). No caller ever parks on this Semaphore — {@link #prefetchPage} uses
   * {@code tryAcquire → skip} so a full-to-capacity prefetcher simply drops the hint rather than
   * serializing the descent. With the default of 0 permits, every {@code tryAcquire} returns
   * {@code false} on a single CAS with no allocation.
   * </p>
   */
  private static final int PREFETCH_PARALLELISM_DEFAULT = 0;

  /**
   * Current prefetch-parallelism cap. Volatile because the test hook
   * {@link #setPrefetchParallelismForTest(int)} rebuilds the limiter on another thread and we want
   * readers to see the latest reference.
   */
  private static volatile Semaphore PREFETCH_LIMIT = initialPrefetchLimit();

  private static Semaphore initialPrefetchLimit() {
    final int configured = Integer.getInteger("sirix.hot.prefetch.parallelism", PREFETCH_PARALLELISM_DEFAULT);
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
  /**
   * Per-level snapshot of {@link HOTIndirectPage#getMostSignificantBitIndex} captured during the
   * PEXT-routed descent. Used by {@link #lowerOrUpperBound} (Binna §4.2 lower_or_upper_bound,
   * reference: {@code HOTSingleThreaded.hpp:347-415}) to walk the search-stack back up to the
   * branching depth where the searchKey actually diverges from the candidate leaf's key.
   */
  private final short[] pathMsbAtDepth = new short[MAX_TREE_HEIGHT];
  private int pathDepth = 0;

  // ===== Current leaf, protected by OPTIMISTIC STAMPS — never pinned =====
  // The reader holds no guard: leaves stay evictable at all times, and safety comes from the
  // FrameSlotAllocator's per-slot seqlock versions instead. loadPage snapshots the resolved
  // leaf's stamp; every read of leaf content is trusted only after validateCurrentLeaf()
  // confirms the stamp, and one validation covers every read since the snapshot. On a failed
  // validation the leaf is re-resolved through its PageReference — content per reference is
  // immutable, so every slot index computed before the failure stays valid after the reload.
  private HOTLeafPage currentLeaf = null;
  private PageReference currentLeafRef = null;
  private long currentLeafStamp = HOTLeafPage.STAMP_INVALID;

  /**
   * The leaf binding {@link #currentLeafStamp} was issued under, snapshotted immediately before it.
   *
   * <p>
   * A stamp is a per-SLOT sequence number and proves nothing without the slot it belongs to: two
   * slots' counters are unrelated and can hold equal values at once, so validating a stamp against
   * whatever slot the leaf is bound to NOW can return {@code true} by coincidence. Carrying the
   * binding here costs one {@code long} per reader and turns that coincidence into a rejected read.
   * {@link HOTLeafPage#STAMP_INVALID} is odd, so the initial value never validates either.
   * </p>
   */
  private long currentLeafBinding = HOTLeafPage.STAMP_INVALID;

  /**
   * Bound on how many times {@link #loadPage} reloads a leaf that keeps getting evicted between
   * resolve and stamp snapshot. Each retry reads a fresh copy from storage, so the race is
   * independent per attempt; exhausting this many implies pathological thrashing.
   */
  private static final int MAX_LOAD_RETRIES = 256;

  /**
   * Bound on validate-and-retry rounds for a single positioning decision, shared by every consumer of
   * the optimistic-stamp protocol. A retry re-reads a freshly reloaded copy of the same immutable
   * content, so each round races eviction independently; exhausting this many implies pathological
   * allocator thrashing, not a logic error.
   *
   * <p>
   * ONE declaration on purpose: this budget was previously restated under five different names across
   * five classes (plus a bare literal), so tuning it meant finding all six and missing one left a
   * single scan path on the old value.
   * </p>
   */
  public static final int MAX_STAMP_RETRIES = 64;

  /**
   * Create a new HOTTrieReader.
   *
   * @param storageEngineReader the storage engine reader
   */
  public HOTTrieReader(StorageEngineReader storageEngineReader) {
    this.storageEngineReader = Objects.requireNonNull(storageEngineReader);
    // Child-first-key memoization is sound ONLY on a read-only snapshot: everything below a
    // committed PageReference is immutable (swizzle/evict cycles reload identical content), so
    // a subtree's first key is a constant of the parent page object. A WRITE transaction can
    // re-point a child reference's page without touching the parent node, which no
    // parent-local invalidation can observe — so writer-backed readers never touch the cache.
    //
    // hasTrxIntentLog() rather than `instanceof StorageEngineWriter`, and the distinction is
    // load-bearing here for the same reason AbstractHOTIndexReader states it for the lookup cache:
    // StorageEngineWriter#getStorageEngineReader hands out a PLAIN NodeStorageEngineReader carrying
    // the writer's intent log, which is not a StorageEngineWriter yet resolves uncommitted pages
    // under an already-committed revision number. AbstractHOTIndexReader constructs this class with
    // exactly the reader it was handed, so the two memoizations sit one call apart and must agree
    // on what "read-only snapshot" means — the intent-log test is the property that actually
    // matters, and it is strictly the more conservative of the two.
    this.firstKeyCacheEnabled = !storageEngineReader.hasTrxIntentLog();
    // Span-hint capability, resolved once: a backend advertising a prefetch batch takes the
    // ZERO-THREAD advisory route (one batched WILLNEED/ring submit per sibling window) instead
    // of the virtual-thread read pool — the vthread route stays as the opt-in for backends
    // without the primitive (see the NativeThreadSet lock findings above PREFETCH_LIMIT).
    this.spanPrefetchCapable = storageEngineReader.recordPagePrefetchBatch() > 0;
  }

  /** See the constructor. */
  private final boolean spanPrefetchCapable;

  /** Scratch for the span-hint sibling window (transaction-confined, like this reader). */
  private final PageReference[] spanScratch = new PageReference[PREFETCH_WINDOW];

  /** See the constructor — memoized first-key probes are restricted to read-only snapshots. */
  private final boolean firstKeyCacheEnabled;

  /**
   * Find value for exact key match. Returns null if not found - no Optional allocation!
   *
   * <p>
   * The returned slice views UNPINNED slot memory: the found/not-found decision is stamp-validated
   * before this method returns, but a caller that reads the slice afterwards must confirm those reads
   * via {@link #validateCurrentLeaf()} (retrying through {@link #refreshCurrentLeaf()} on failure) —
   * or copy the bytes and validate the copy.
   * </p>
   *
   * @param rootRef the root page reference
   * @param key the search key
   * @return the value as a MemorySegment slice, or null if not found
   */
  public @Nullable MemorySegment get(PageReference rootRef, byte[] key) {
    Objects.requireNonNull(rootRef);
    Objects.requireNonNull(key);

    // Re-navigate through the canonical PEXT route on every lookup. The tree's routing and
    // disjoint-subtree invariants make that route authoritative, and its log_32 height keeps it
    // shallow without introducing mutable reader-side routing state.

    // navigateToLeaf resolves the leaf through loadPage, which snapshots its optimistic stamp
    // as this reader's current leaf. The leaf stays evictable the whole time: findEntry and the
    // slice computation may read torn bytes if the slot is reclaimed mid-read, so the batch is
    // validated once before the result escapes — and retried on a fresh copy if it fails.
    // Content per PageReference is immutable, so a retry re-derives the identical answer.
    for (int attempt = 0; attempt < MAX_STAMP_RETRIES; attempt++) {
      final HOTLeafPage leaf = navigateToLeaf(rootRef, key);
      final MemorySegment value;
      try {
        final int index = leaf.findEntry(key);
        value = index >= 0
            ? leaf.getValueSlice(index)
            : null;
      } catch (RuntimeException e) {
        if (validateCurrentLeaf()) {
          throw e; // stable bytes — genuine corruption, not a torn read
        }
        continue;
      }
      if (!validateCurrentLeaf()) {
        continue;
      }
      return value;
    }
    throw stampRetriesExhausted("get");
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

    // Same optimistic discipline as get(): compute on the evictable leaf, validate the stamp
    // before the boolean escapes, retry on a reloaded copy if the bytes were torn.
    for (int attempt = 0; attempt < MAX_STAMP_RETRIES; attempt++) {
      final HOTLeafPage leaf = navigateToLeaf(rootRef, key);
      final boolean found;
      try {
        found = leaf.findEntry(key) >= 0;
      } catch (RuntimeException e) {
        if (validateCurrentLeaf()) {
          throw e;
        }
        continue;
      }
      if (!validateCurrentLeaf()) {
        continue;
      }
      return found;
    }
    throw stampRetriesExhausted("containsKey");
  }

  /**
   * Create a range cursor for iterating over a key range.
   *
   * @param rootRef the root page reference
   * @param fromKey the start key (inclusive), or {@code null} to start at the leftmost leaf — which
   *        is NOT the same as an empty array: {@code null} takes the deterministic
   *        {@code navigateToLeftmostLeaf} descent, an empty array a PEXT-routed
   *        {@link #lowerBound(PageReference, byte[])} on an all-zero partial key
   * @param toKey the end key (inclusive), or {@code null} for an unbounded upper end
   * @return the range cursor
   */
  public HOTRangeCursor range(PageReference rootRef, byte @Nullable [] fromKey, byte @Nullable [] toKey) {
    return new HOTRangeCursor(this, rootRef, fromKey, toKey);
  }

  /**
   * Result of a lower-bound or upper-bound seek. The leaf is positioned via the reader's internal
   * path-stack; subsequent {@link #advanceToNextLeaf()} calls continue iteration in lex order (HOT
   * children are sorted by first-key, so leftmost-first sibling traversal is lex-monotonic). When the
   * seek lands past every key in the trie, {@link #leaf} is {@code null} and the caller should treat
   * the cursor as exhausted.
   */
  public static final class LowerBoundResult {
    /** Leaf containing the seeked entry, or {@code null} if the seek is past end-of-trie. */
    public final @Nullable HOTLeafPage leaf;
    /** Entry index within {@link #leaf} of the seeked entry. {@code -1} when {@code leaf == null}. */
    public final int indexInLeaf;

    private LowerBoundResult(@Nullable HOTLeafPage leaf, int indexInLeaf) {
      this.leaf = leaf;
      this.indexInLeaf = indexInLeaf;
    }

    /** Sentinel for "seek past end of trie". */
    private static final LowerBoundResult EXHAUSTED = new LowerBoundResult(null, -1);

    /**
     * Sentinel for "a stamp validation failed mid-seek" — the leaf whose content fed a positioning
     * decision was reclaimed while being read. Never escapes to callers: the public entry points
     * ({@code lowerBound}/{@code upperBound}) re-run the whole seek on freshly reloaded copies.
     */
    private static final LowerBoundResult RETRY = new LowerBoundResult(null, -2);
  }

  /**
   * Locate the first entry whose key is {@code >= searchKey}, in lex order.
   *
   * <p>
   * Reference: Robert Binna, <i>The Height Optimized Trie</i>, §4.2; reference impl
   * {@code HOTSingleThreaded::lower_or_upper_bound} ({@code HOTSingleThreaded.hpp:347-415}).
   * </p>
   *
   * <p>
   * Algorithm (5 phases, ports the C++ reference 1:1):
   * </p>
   * <ol>
   * <li><b>PEXT-routed descent with stack.</b> Use the existing
   * {@link #navigateToLeaf(PageReference, byte[])} machinery — captures
   * {@code (parentNode, childIdx, mostSignificantBitIndex)} at every level. Lands at a candidate leaf
   * chosen by partial-key match, which may not be the lex-correct leaf when {@code searchKey} doesn't
   * exist.</li>
   * <li><b>Mismatch-bit detection.</b> Compute the first absolute bit position where any entry in the
   * candidate leaf differs from {@code searchKey} (via
   * {@link DiscriminativeBitComputer#computeDifferingBit(byte[], byte[])} on the leaf's first key —
   * same partial-key prefix above the disc bit means same mismatch info). If keys are identical the
   * candidate IS the lower bound.</li>
   * <li><b>Walk stack up to branching depth.</b> Pop levels while the disc bit lies below the level's
   * most-significant disc bit — bits above the disc bit already matched perfectly, so those levels
   * routed correctly; bits at or above the disc bit determine where {@code searchKey} actually
   * branches.</li>
   * <li><b>Compute affected subtree at branching depth.</b> Find the contiguous run of siblings
   * sharing the matched entry's bit-prefix above the disc bit. HOT children are stored lex-sorted by
   * first-key, so disc-bit-prefix groups are contiguous in child-index order. Walk outward from the
   * matched index using {@link DiscriminativeBitComputer#computeDifferingBit} on first-keys.</li>
   * <li><b>Position at next entry.</b> If the searchKey's bit at the disc position is 1, lower-bound
   * is one past the affected subtree; if 0, it is the first index of the affected subtree. Descend
   * leftmost from there. If the next index falls past the branching node's last child, bubble up via
   * {@link #advanceToNextLeaf()}.</li>
   * </ol>
   *
   * @param rootRef root of the HOT subtree
   * @param searchKey the lex search key
   * @return position of the first entry {@code >= searchKey}, or {@link LowerBoundResult#EXHAUSTED}
   *         when no such entry exists
   */
  public LowerBoundResult lowerBound(PageReference rootRef, byte[] searchKey) {
    Objects.requireNonNull(rootRef, "rootRef");
    Objects.requireNonNull(searchKey, "searchKey");
    return retryingBound(rootRef, searchKey, searchKey.length, true);
  }

  /**
   * Locate the first entry whose key is at least {@code searchKey[0..searchKeyLen)}.
   *
   * <p>
   * This overload is the canonical seek for reusable serialization buffers. Tail bytes outside the
   * valid prefix never participate in PEXT routing, leaf insertion-point lookup, mismatch-bit
   * detection, or the final branch decision, so callers do not need to allocate an exactly-sized
   * array before every seek.
   * </p>
   *
   * @param rootRef root of the HOT subtree
   * @param searchKey buffer containing the lexicographic search key
   * @param searchKeyLen number of valid bytes in {@code searchKey}
   * @return position of the first entry {@code >= searchKey[0..searchKeyLen)}, or exhaustion
   */
  public LowerBoundResult lowerBound(final PageReference rootRef, final byte[] searchKey, final int searchKeyLen) {
    Objects.requireNonNull(rootRef, "rootRef");
    Objects.checkFromIndexSize(0, searchKeyLen, Objects.requireNonNull(searchKey, "searchKey").length);
    return retryingBound(rootRef, searchKey, searchKeyLen, true);
  }

  /**
   * Locate the first entry whose key is {@code > searchKey}, in lex order. See
   * {@link #lowerBound(PageReference, byte[])} for algorithm details.
   */
  public LowerBoundResult upperBound(PageReference rootRef, byte[] searchKey) {
    Objects.requireNonNull(rootRef, "rootRef");
    Objects.requireNonNull(searchKey, "searchKey");
    return retryingBound(rootRef, searchKey, searchKey.length, false);
  }

  /**
   * Locate the first entry whose key is greater than {@code searchKey[0..searchKeyLen)} without
   * trimming a reusable serialization buffer.
   *
   * @param rootRef root of the HOT subtree
   * @param searchKey buffer containing the lexicographic search key
   * @param searchKeyLen number of valid bytes in {@code searchKey}
   * @return position of the first entry {@code > searchKey[0..searchKeyLen)}, or exhaustion
   */
  public LowerBoundResult upperBound(final PageReference rootRef, final byte[] searchKey, final int searchKeyLen) {
    Objects.requireNonNull(rootRef, "rootRef");
    Objects.checkFromIndexSize(0, searchKeyLen, Objects.requireNonNull(searchKey, "searchKey").length);
    return retryingBound(rootRef, searchKey, searchKeyLen, false);
  }

  private LowerBoundResult retryingBound(final PageReference rootRef, final byte[] searchKey, final int searchKeyLen,
      final boolean isLowerBound) {
    for (int attempt = 0; attempt < MAX_STAMP_RETRIES; attempt++) {
      final LowerBoundResult result = lowerOrUpperBound(rootRef, searchKey, searchKeyLen, isLowerBound);
      if (result != LowerBoundResult.RETRY) {
        return result;
      }
    }
    throw stampRetriesExhausted(isLowerBound
        ? "lowerBound"
        : "upperBound");
  }

  /**
   * Bounded torn-read recovery: reload the current leaf through its {@link PageReference} so the
   * caller can re-read the SAME position on a fresh copy. Content per reference is immutable, so
   * every index the caller already computed stays valid.
   *
   * @param round how many consecutive torn rounds this is, including the current one
   * @param operation names the caller, for the exhaustion diagnostic
   */
  public void recoverTorn(final int round, final String operation) {
    if (round > MAX_STAMP_RETRIES) {
      throw stampRetriesExhausted(operation);
    }
    if (!refreshCurrentLeaf()) {
      throw new IllegalStateException(operation + ": evicted leaf could not be reloaded through its PageReference");
    }
  }

  /** A seek that could not observe stable leaf bytes in {@link #MAX_STAMP_RETRIES} rounds. */
  public static IllegalStateException stampRetriesExhausted(final String operation) {
    return new IllegalStateException("HOT: " + operation + " failed stamp validation on every one of "
        + MAX_STAMP_RETRIES + " attempts — sustained allocator thrashing");
  }

  /**
   * A non-null HOT root denotes a materialized trie, including the valid empty-root-leaf state.
   * Consequently, a missing page, child, or routing decision below that root is corruption rather
   * than key absence. Keep that distinction explicit so point lookups and ranges never turn a broken
   * structure into a plausible empty result.
   */
  private static IllegalStateException structuralCorruption(final String detail) {
    return new IllegalStateException("HOT structural corruption: " + detail);
  }

  private LowerBoundResult lowerOrUpperBound(final PageReference rootRef, final byte[] searchKey,
      final int searchKeyLen, final boolean isLowerBound) {
    // Phase 1: PEXT-routed descent with stack tracking.
    final HOTLeafPage candidateLeaf = navigateToLeafUnchecked(rootRef, searchKey, searchKeyLen);

    // Phase 2: try exact match in candidate leaf first (cheap fast path; matches the
    // C++ reference where exact match through PEXT is the common case). findEntry and the
    // entry count are one read batch against the unpinned leaf — validated ONCE below, before
    // any decision derived from them escapes.
    final int exact;
    final int candidateEntryCount;
    try {
      exact = candidateLeaf.findEntry(searchKey, searchKeyLen);
      candidateEntryCount = candidateLeaf.getEntryCount();
    } catch (RuntimeException e) {
      if (validateCurrentLeaf()) {
        throw e; // stable bytes — genuine corruption, not a torn read
      }
      return LowerBoundResult.RETRY;
    }
    if (!validateCurrentLeaf()) {
      return LowerBoundResult.RETRY;
    }
    if (exact >= 0) {
      if (isLowerBound) {
        return new LowerBoundResult(candidateLeaf, exact);
      }
      // upper_bound: step past the exact match.
      return advanceOneFrom(candidateLeaf, exact);
    }

    // Phase 2b: insertion-point-within-candidate fast path. Sirix's HOT leaves are
    // multi-entry — a candidate leaf can hold up to 512 keys. {@link HOTLeafPage#findEntry}
    // returns {@code -(insertionPoint+1)} when no exact match exists, where
    // {@code insertionPoint} is the lex-position where {@code searchKey} would land in the
    // sorted leaf. If {@code insertionPoint < entryCount}, the smallest leaf key strictly
    // greater than {@code searchKey} is {@code candidateLeaf[insertionPoint]} — that IS the
    // lower_bound result; no walk-up needed.
    //
    // Binna's reference (single-TID leaves) doesn't hit this case because every leaf has
    // exactly one entry; non-exact matches always live in a different leaf. With multi-entry
    // leaves the walk-up phase below becomes incorrect for queries whose insertion point is
    // strictly inside the candidate (e.g., chunked-bitmap range scans for prefixes whose
    // chunkIdx_be4 trailer differs from any stored composite).
    if (candidateEntryCount == 0) {
      // Shouldn't happen — empty leaves aren't part of a populated trie.
      return LowerBoundResult.EXHAUSTED;
    }
    final int insertionPoint = -(exact + 1);
    if (insertionPoint < candidateEntryCount) {
      // For lower_bound the answer is candidateLeaf[insertionPoint]; for upper_bound on a
      // non-exact match the answer is the same (no leaf entry equals searchKey).
      return new LowerBoundResult(candidateLeaf, insertionPoint);
    }

    final int discBit;
    try {
      // Keep the absent-key walk allocation-free: compare/msdbWith read the first slot's
      // common-prefix + off-heap suffix in place and honor searchKeyLen, including zero padding
      // beyond the logical end. The equality check preserves the defensive outcome below if
      // findEntry ever reports a miss for the first key.
      discBit = candidateLeaf.compareKeyWithBound(0, searchKey, searchKeyLen) == 0
          ? -1
          : candidateLeaf.msdbWith(0, searchKey, searchKeyLen);
    } catch (RuntimeException e) {
      if (validateCurrentLeaf()) {
        throw e;
      }
      return LowerBoundResult.RETRY;
    }
    if (!validateCurrentLeaf()) {
      return LowerBoundResult.RETRY;
    }
    if (discBit < 0) {
      // Candidate first-key equals searchKey but findEntry didn't match: defensive path.
      // Treat as exact match at index 0.
      if (isLowerBound) {
        return new LowerBoundResult(candidateLeaf, 0);
      }
      return advanceOneFrom(candidateLeaf, 0);
    }
    final boolean searchKeyBit = DiscriminativeBitComputer.isBitSet(searchKey, searchKeyLen, discBit);

    // Phase 3: walk stack up while discBit is BELOW the level's most-significant disc bit.
    // (Smaller absoluteBitIndex = "more significant" = earlier in key.) The C++ reference
    // condition is `significantBitIdx < mostSignificantBitIndexes[depth]`; we mirror it.
    int branchingDepth = pathDepth - 1; // start at parent of the leaf
    while (branchingDepth > 0 && discBit < pathMsbAtDepth[branchingDepth]) {
      branchingDepth--;
    }
    if (branchingDepth < 0) {
      // Path was empty (root is a leaf). Candidate leaf is the only leaf; insertion-point
      // wholly determines the result.
      if (insertionPoint < candidateEntryCount) {
        return new LowerBoundResult(candidateLeaf, insertionPoint);
      }
      return LowerBoundResult.EXHAUSTED;
    }

    // Phase 4: at the branching depth, compute the affected subtree's [firstIdx, lastIdx].
    // HOT writers sort children by first-key (lex), so the affected subtree (children that
    // share matched's bit value at {@code discBit} AND all higher-significance disc bits) is
    // a contiguous run in child-index order.
    //
    // <p>Critical invariant (Binna §4.2): a sibling is in the affected subtree IFF it shares
    // matched's bit value AT {@code discBit} as well as all bits ABOVE. Equivalently:
    // {@code computeDifferingBit(matched, sibling) > discBit}. The {@code >} (not {@code >=})
    // matters: a sibling whose first-key differs from matched at exactly {@code discBit} is
    // on the OPPOSITE side of the disc-bit split — Binna's "new entry's side" — so it is NOT
    // part of the affected subtree. Including it would over-expand the subtree across the
    // disc-bit boundary and corrupt the {@code nextChildIdx = lastIdx + 1} step (it would
    // skip the very subtree where searchKey lives when {@code searchKeyBit=1}).
    final HOTIndirectPage branchingNode = pathNodes[branchingDepth];
    final int matchedIdx = pathChildIndices[branchingDepth];
    final byte[] matchedFirstKey = getFirstKeyOfChild(branchingNode, matchedIdx);

    int firstIdx = matchedIdx;
    for (int i = matchedIdx - 1; i >= 0; i--) {
      final byte[] iFirst = getFirstKeyOfChild(branchingNode, i);
      final int diff = DiscriminativeBitComputer.computeDifferingBit(matchedFirstKey, iFirst);
      // diff < 0 ⇒ identical first-keys (extremely rare — same-prefix duplicates); in-subtree.
      // diff > discBit ⇒ keys agree at bit positions 0..discBit, so sibling is on matched's
      // side of the disc-bit split ⇒ in-subtree.
      // diff <= discBit ⇒ sibling differs at-or-above discBit ⇒ NOT in-subtree.
      if (diff < 0 || diff > discBit) {
        firstIdx = i;
      } else {
        break;
      }
    }
    int lastIdx = matchedIdx;
    final int numChildren = branchingNode.getNumChildren();
    for (int i = matchedIdx + 1; i < numChildren; i++) {
      final byte[] iFirst = getFirstKeyOfChild(branchingNode, i);
      final int diff = DiscriminativeBitComputer.computeDifferingBit(matchedFirstKey, iFirst);
      if (diff < 0 || diff > discBit) {
        lastIdx = i;
      } else {
        break;
      }
    }

    // Phase 5: position at the lower-bound child.
    // searchKeyBit == 1 → searchKey would land AFTER the affected subtree
    // searchKeyBit == 0 → searchKey would land at the FIRST entry of the subtree
    // For upper_bound on a non-exact-match, the answer is the same as lower_bound (the
    // first key strictly greater than searchKey), because no leaf entry equals searchKey.
    final int nextChildIdx = searchKeyBit
        ? (lastIdx + 1)
        : firstIdx;
    if (nextChildIdx >= numChildren) {
      // Past the last child of the branching node — bubble up. Reuse the existing
      // advanceToNextLeaf() machinery: position the path stack so the branching-level
      // child is the "current" (last) child, then ask for the next leaf.
      pathDepth = branchingDepth + 1;
      pathChildIndices[branchingDepth] = numChildren - 1;
      final HOTLeafPage next = advanceToNextLeaf();
      if (next == null) {
        return LowerBoundResult.EXHAUSTED;
      }
      return new LowerBoundResult(next, 0);
    }

    // Update path so subsequent advanceToNextLeaf() walks correctly: replace the branching
    // child with nextChildIdx, then descend leftmost into it.
    pathChildIndices[branchingDepth] = nextChildIdx;
    pathDepth = branchingDepth + 1;
    final PageReference nextRef = branchingNode.getChildReference(nextChildIdx);
    if (nextRef == null) {
      throw structuralCorruption("lower-bound child " + nextChildIdx + " has no reference");
    }
    final HOTLeafPage targetLeaf = descendToLeftmostLeaf(nextRef);
    return new LowerBoundResult(targetLeaf, 0);
  }

  /**
   * Step one entry forward from {@code (leaf, idx)}, advancing across leaves via the path stack when
   * the leaf is exhausted. Used for upper_bound stepping past an exact match.
   */
  private LowerBoundResult advanceOneFrom(HOTLeafPage leaf, int idx) {
    // The entry count is a leaf-content read on an unpinned page; validate before the
    // stay-or-advance decision escapes. leaf is the reader's current leaf on every call path.
    final int count;
    try {
      count = leaf.getEntryCount();
    } catch (RuntimeException e) {
      if (validateCurrentLeaf()) {
        throw e;
      }
      return LowerBoundResult.RETRY;
    }
    if (!validateCurrentLeaf()) {
      return LowerBoundResult.RETRY;
    }
    if (idx + 1 < count) {
      return new LowerBoundResult(leaf, idx + 1);
    }
    final HOTLeafPage next = advanceToNextLeaf();
    if (next == null) {
      return LowerBoundResult.EXHAUSTED;
    }
    return new LowerBoundResult(next, 0);
  }

  /**
   * Resolve the lex-smallest key under {@code parent}'s child {@code childIdx}, descending leftmost
   * when the child is itself an indirect page. Mirrors writer-side first-key resolution but reuses
   * this reader's {@link #loadPage} to swizzle on cold pages.
   *
   * <p>
   * Cost: at most one full leftmost descent of the subtree per call. For typical fanout-32 HOT trees,
   * the lower_bound walk-outward at the branching depth fires this O(run-length) times, with run
   * length bounded by {@code numChildren ≤ 32}.
   * </p>
   */
  private byte[] getFirstKeyOfChild(HOTIndirectPage parent, int childIdx) {
    if (firstKeyCacheEnabled) {
      final byte[] cached = parent.cachedChildFirstKey(childIdx);
      if (cached != null) {
        return cached;
      }
    }
    // The materialized first key is read off an unpinned leaf and — worse — memoized into the
    // parent, where a torn copy would poison every later probe. Validate the stamp BEFORE
    // caching or returning; a failed validation restarts the (cheap, leftmost) descent.
    for (int attempt = 0; attempt < MAX_STAMP_RETRIES; attempt++) {
      PageReference ref = parent.getChildReference(childIdx);
      if (ref == null) {
        throw structuralCorruption("child " + childIdx + " has no reference while resolving its first key");
      }
      int depth = 0;
      while (true) {
        if (depth++ >= MAX_TREE_HEIGHT) {
          throw structuralCorruption("subtree exceeds maximum height while resolving child " + childIdx);
        }
        final Page page = loadPage(ref);
        if (page == null) {
          throw structuralCorruption("child " + childIdx + " references an unresolved page");
        }
        if (page instanceof HOTLeafPage leaf) {
          final int entryCount;
          final byte @Nullable [] firstKey;
          try {
            entryCount = leaf.getEntryCount();
            firstKey = entryCount == 0
                ? null
                : leaf.getFirstKey();
          } catch (RuntimeException e) {
            if (validateCurrentLeaf()) {
              throw e;
            }
            break;
          }
          if (!validateCurrentLeaf()) {
            break;
          }
          if (entryCount == 0 || firstKey == null) {
            throw structuralCorruption("indirect child " + childIdx + " resolves to an empty leaf");
          }
          if (firstKeyCacheEnabled) {
            parent.cacheChildFirstKey(childIdx, firstKey);
          }
          return firstKey;
        }
        if (!(page instanceof HOTIndirectPage indirect)) {
          throw structuralCorruption("child " + childIdx + " resolves to " + page.getClass().getName());
        }
        if (indirect.getNumChildren() == 0) {
          throw structuralCorruption("child " + childIdx + " resolves through an empty indirect page");
        }
        ref = indirect.getChildReference(0);
        if (ref == null) {
          throw structuralCorruption("leftmost descendant of child " + childIdx + " has no reference");
        }
      }
    }
    throw stampRetriesExhausted("getFirstKeyOfChild");
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
   * @return the PEXT-routed leaf page; an empty trie is represented by an empty root leaf
   * @throws IllegalStateException if the materialized trie contains an unresolved or invalid route
   */
  public HOTLeafPage navigateToLeaf(PageReference rootRef, byte[] key) {
    Objects.requireNonNull(rootRef, "rootRef");
    Objects.requireNonNull(key, "key");
    return navigateToLeafUnchecked(rootRef, key, key.length);
  }

  private HOTLeafPage navigateToLeafUnchecked(final PageReference rootRef, final byte[] key, final int keyLen) {
    pathDepth = 0;
    PageReference currentRef = rootRef;

    while (true) {
      Page page = loadPage(currentRef);
      if (page == null) {
        throw structuralCorruption("PEXT route references an unresolved page");
      }

      if (page instanceof HOTLeafPage leaf) {
        return leaf;
      }

      if (!(page instanceof HOTIndirectPage hotNode)) {
        throw structuralCorruption("PEXT route reached " + page.getClass().getName());
      }

      // Find child reference using HOT node type-specific logic (uses PEXT/Long.compress)
      final int childIndex = hotNode.findChildIndex(key, keyLen);
      if (childIndex < 0) {
        throw structuralCorruption("indirect page produced no PEXT child");
      }

      final PageReference childRef = hotNode.getChildReference(childIndex);
      if (childRef == null) {
        throw structuralCorruption("PEXT child " + childIndex + " has no reference");
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

      // Record path for parent-based range traversal. Capture the per-level MSB so a
      // subsequent {@link #lowerOrUpperBound} call can walk back up to the branching depth
      // (Binna §4.2). Cheap: a single short read from the indirect page.
      pathMsbAtDepth[pathDepth] = hotNode.getMostSignificantBitIndex();
      pushPath(currentRef, hotNode, childIndex);

      currentRef = childRef;
    }
  }

  /**
   * Navigate to the leftmost leaf in the subtree. Used for range scan initialization.
   *
   * @param rootRef the root reference
   * @return the leftmost leaf; an empty trie is represented by an empty root leaf
   * @throws IllegalStateException if the materialized trie contains an unresolved or invalid child
   */
  public HOTLeafPage navigateToLeftmostLeaf(PageReference rootRef) {
    Objects.requireNonNull(rootRef);
    pathDepth = 0;
    PageReference currentRef = rootRef;

    while (true) {
      Page page = loadPage(currentRef);
      if (page == null) {
        throw structuralCorruption("leftmost route references an unresolved page");
      }

      if (page instanceof HOTLeafPage leaf) {
        return leaf;
      }

      if (!(page instanceof HOTIndirectPage hotNode)) {
        throw structuralCorruption("leftmost route reached " + page.getClass().getName());
      }

      // Take the first (leftmost) child
      if (hotNode.getNumChildren() == 0) {
        throw structuralCorruption("leftmost route reached an empty indirect page");
      }
      final int childIndex = 0;
      final PageReference childRef = hotNode.getChildReference(childIndex);
      if (childRef == null) {
        throw structuralCorruption("leftmost child has no reference");
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
      final int parentIdx = pathDepth - 1;
      final HOTIndirectPage parent = pathNodes[parentIdx];
      final int numChildren = parent.getNumChildren();

      for (int nextChildIdx = pathChildIndices[parentIdx] + 1; nextChildIdx < numChildren; nextChildIdx++) {
        pathChildIndices[parentIdx] = nextChildIdx;

        final PageReference nextChildRef = parent.getChildReference(nextChildIdx);
        if (nextChildRef == null) {
          throw structuralCorruption("range successor child " + nextChildIdx + " has no reference");
        }
        // Prefetch-batch: issue PREFETCH_WINDOW in-flight reads for the upcoming
        // siblings. Deepens NVMe/io_uring queue depth — on FFM-io_uring storage
        // these coalesce into a single submit; on FILE_CHANNEL each fires on
        // a separate virtual thread and kernel I/O scheduler interleaves them.
        prefetchSiblingWindow(parent, nextChildIdx + 1, numChildren);

        return descendToLeftmostLeaf(nextChildRef);
      }

      // No more siblings at this level, pop up
      pathDepth--;
    }

    // Exhausted the tree
    return null;
  }

  /**
   * Descend to the leftmost leaf from a given reference. Prefetches the next sibling at each level
   * for range scan readahead.
   */
  private HOTLeafPage descendToLeftmostLeaf(PageReference ref) {
    final Page page = loadPage(ref);
    if (page == null) {
      throw structuralCorruption("leftmost descendant references an unresolved page");
    }

    if (page instanceof HOTLeafPage leaf) {
      return leaf;
    }

    if (!(page instanceof HOTIndirectPage hotNode)) {
      throw structuralCorruption("leftmost descendant reached " + page.getClass().getName());
    }

    // Prefetch-batch at descent: schedule PREFETCH_WINDOW in-flight reads for
    // the current inner node's first N children. Saturates queue depth on the
    // way down — the cursor will visit all of them in order anyway.
    final int numChildren = hotNode.getNumChildren();
    if (numChildren == 0) {
      throw structuralCorruption("leftmost descent reached an empty indirect page");
    }
    prefetchSiblingWindow(hotNode, 1, numChildren);

    final PageReference childRef = hotNode.getChildReference(0);
    if (childRef == null) {
      throw structuralCorruption("leftmost descendant has no child reference");
    }
    pushPath(ref, hotNode, 0);
    return descendToLeftmostLeaf(childRef);
  }

  /**
   * Prefetch up to {@link #PREFETCH_WINDOW} consecutive child references of {@code parent} starting
   * at index {@code startIdx} (inclusive) up to {@code numChildren} (exclusive). Each eligible
   * reference (not already swizzled, with a valid disk key) fires a fire-and-forget async load.
   *
   * <p>
   * Why a window rather than a single sibling: the kernel I/O scheduler and NVMe command queue
   * benefit from queue depths in the 8-32 range. A single one-ahead prefetch leaves the device idle
   * between reads on cold cache. A window of {@value #PREFETCH_WINDOW} matches typical NVMe QD sweet
   * spots and, on FFM-io_uring storage, the individual reads can be batched into a single
   * {@code io_uring_enter} submit on the underlying reader.
   */
  private void prefetchSiblingWindow(final HOTIndirectPage parent, final int startIdx, final int numChildren) {
    final int end = Math.min(startIdx + PREFETCH_WINDOW, numChildren);
    if (spanPrefetchCapable) {
      // One batched span hint for the whole window: zero threads, zero locks, and the
      // backend coalesces (WILLNEED readahead on mmap; one ring submit on io_uring).
      int n = 0;
      for (int i = startIdx; i < end; i++) {
        final PageReference ref = parent.getChildReference(i);
        if (ref != null && ref.getPage() == null && ref.getKey() >= 0) {
          spanScratch[n++] = ref;
        }
      }
      if (n > 0) {
        storageEngineReader.prefetchPageSpans(spanScratch, n);
      }
      return;
    }
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
   * Load a page from storage. Checks the page reference's in-memory page first (swizzle check), then
   * falls back to storage. After loading from storage, the page is swizzled onto the reference so
   * subsequent accesses avoid SSD I/O entirely.
   *
   * <p>
   * <b>SSD optimization:</b> Swizzling loaded pages onto their PageReference eliminates redundant I/O
   * for repeated traversals through the same internal nodes. Since HOT trees have low height
   * (typically 3-5 levels with compound nodes), keeping all internal nodes swizzled is
   * memory-efficient and avoids the dominant cost of random SSD reads.
   * </p>
   */
  private @Nullable Page loadPage(PageReference ref) {
    // Resolving a HOT leaf and snapshotting its optimistic stamp are fused here so no caller can
    // observe a leaf without a stamp to validate against: a concurrent eviction would otherwise
    // let a reader mistake an evicted page for a missing key. On a lost race the leaf is simply
    // reloaded — eviction is transient, not absence.
    for (int attempt = 0; attempt < MAX_LOAD_RETRIES; attempt++) {
      // A swizzled page that is already closed means a concurrent eviction reclaimed its
      // off-heap slot; drop it and reload a fresh copy rather than hand back dead memory.
      Page page = ref.getPage();
      if (page == null || page.isClosed()) {
        // CRITICAL: check BOTH storage key AND log key before giving up. A page in the
        // transaction log has key == NULL_ID_LONG but a valid logKey.
        if (ref.getKey() < 0 && ref.getLogKey() < 0) {
          return null; // not in storage and not in the transaction log
        }
        // The storage engine handles versioning/fragment combining and the log lookup.
        page = storageEngineReader.loadHOTPage(ref);
        if (page == null) {
          return null;
        }
        // Swizzle: pin the loaded page so future descents skip I/O. HOT pages are immutable
        // once loaded (COW creates new pages for modifications), so setPage is idempotent.
        ref.setPage(page);
      }

      if (!(page instanceof HOTLeafPage leaf)) {
        return page; // an indirect page — eviction only de-swizzles it, never closes it
      }
      // NO PIN. Snapshot the leaf's optimistic stamp instead: an odd stamp means the leaf is
      // closed or its slot is mid-teardown — drop the swizzle and reload a fresh copy. Every
      // read of this leaf's content must later pass validateCurrentLeaf() before its result is
      // trusted; the leaf stays evictable the entire time.
      //
      // The binding comes FIRST and travels with the stamp: a stamp is a per-slot sequence, so a
      // rebind between the two reads must be detectable, and an ODD binding means a rebind is in
      // flight right now — nothing read under it could be proved, so reload instead of reading.
      final long binding = leaf.readStampBinding();
      final long stamp = leaf.readStamp();
      if ((binding & 1L) != 0L || (stamp & 1L) != 0L) {
        ref.setPage(null);
        continue;
      }
      // Second-chance signal for the ClockSweeper: a leaf under active read survives one
      // eviction cycle. Purely advisory — correctness never depends on it. Guarded because
      // markAccessed is a volatile store and a seek re-resolves the same leaf many times (every
      // first-key probe descends to one), so the unguarded form paid a store fence per probe on a
      // flag that is already set after the first.
      if (!leaf.isHot()) {
        leaf.markAccessed();
      }
      currentLeaf = leaf;
      currentLeafRef = ref;
      currentLeafBinding = binding;
      currentLeafStamp = stamp;
      return leaf;
    }
    throw new IllegalStateException("HOT: leaf at " + ref + " evicted on every one of " + MAX_LOAD_RETRIES
        + " load attempts — sustained allocator thrashing");
  }

  /**
   * Resolve {@code ref} to its page — the public form of the descent's page load, for consumers that
   * enumerate a subtree's references themselves instead of walking it with a cursor.
   *
   * <p>
   * A resolved {@link HOTLeafPage} becomes this reader's current leaf with its optimistic stamp
   * snapshotted, so the caller reads its content under the same discipline the cursor uses:
   * {@link #validateCurrentLeaf()} before any result escapes, {@link #recoverTorn} to re-read on a
   * freshly reloaded copy. An indirect page carries no stamp — it is never closed by eviction, only
   * de-swizzled.
   *
   * @return the page, or {@code null} when the reference resolves to nothing
   */
  public @Nullable Page resolvePage(final PageReference ref) {
    Objects.requireNonNull(ref);
    return loadPage(ref);
  }

  /**
   * Whether every read of {@link #currentLeaf}'s content since {@link #loadPage} resolved it saw
   * stable bytes. One call covers the whole batch of reads since the snapshot.
   */
  public boolean validateCurrentLeaf() {
    final HOTLeafPage leaf = currentLeaf;
    return leaf == null || leaf.validateStamp(currentLeafBinding, currentLeafStamp);
  }

  /** The most-recently-resolved leaf, or {@code null}. Reads of it require stamp validation. */
  public @Nullable HOTLeafPage currentLeafPage() {
    return currentLeaf;
  }

  /**
   * Re-resolve {@link #currentLeaf} through its {@link PageReference} after a failed validation.
   * Content per reference is immutable, so every entry index computed against the stale copy remains
   * valid against the fresh one — callers keep their position and redo only the reads.
   *
   * @return {@code true} when the leaf was re-resolved (fields updated), {@code false} when it is no
   *         longer resolvable
   */
  public boolean refreshCurrentLeaf() {
    final PageReference ref = currentLeafRef;
    if (ref == null) {
      return false;
    }
    final Page swizzled = ref.getPage();
    if (swizzled != null && swizzled.isClosed()) {
      ref.setPage(null);
    }
    return loadPage(ref) instanceof HOTLeafPage;
  }

  /**
   * Asynchronously prefetch a page into swizzled state.
   *
   * <p>
   * <b>SSD optimization:</b> Fires the I/O on a virtual thread so the calling traversal can continue
   * descending without blocking on sibling I/O. When the virtual thread completes, the result is
   * swizzled onto the PageReference (volatile field). If the traversal later reaches this reference,
   * {@link #loadPage} finds it already swizzled and avoids a redundant read.
   * </p>
   *
   * <p>
   * Race safety: if {@code loadPage} is called before the async task finishes, it will not find a
   * swizzled page and will load synchronously. The duplicate load is benign — both paths produce the
   * same immutable page and {@code setPage} on the volatile field is idempotent.
   * </p>
   *
   * <p>
   * <b>Concurrency cap:</b> Gated by {@link #PREFETCH_LIMIT}. When the Semaphore is drained
   * (prefetcher already saturating the kernel I/O queue), the call returns immediately without
   * starting a virtual thread — the hint is dropped and the synchronous {@link #loadPage} path will
   * load on demand. See {@code iter04-prefetcher-analysis.md} for the formal-verification argument
   * that skipping prefetch never affects correctness of the returned {@code MemorySegment} values.
   * </p>
   */
  private void prefetchPage(PageReference ref) {
    if (spanPrefetchCapable) {
      // Zero-thread advisory route: a single-ref span hint (madvise/ring submit) replaces the
      // virtual-thread read — same skip-safe contract, no permits, no thread churn.
      spanScratch[0] = ref;
      storageEngineReader.prefetchPageSpans(spanScratch, 1);
      return;
    }
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
   * Test-only hook: rebuild the static {@link #PREFETCH_LIMIT} with a new permit cap. Needed because
   * the default cap is computed at class-load time. Declared {@code public} only so cross-package
   * tests ({@code io.sirix.index.projection.ProjectionIndexHOTStorageTest}) can sweep permit values
   * without reflection.
   *
   * <p>
   * <b>Do not call from production code.</b> Use the JVM property
   * {@code -Dsirix.hot.prefetch.parallelism=N} instead.
   * </p>
   *
   * <p>
   * Caller must ensure no prefetch virtual threads are outstanding against the previous
   * {@code PREFETCH_LIMIT} (otherwise the release on the old Semaphore is harmless — the old instance
   * is GC'd once all tasks complete).
   * </p>
   */
  public static void setPrefetchParallelismForTest(int permits) {
    if (permits < 0) {
      throw new IllegalArgumentException("permits must be >= 0");
    }
    PREFETCH_LIMIT = new Semaphore(permits);
  }

  /**
   * Test-only accessor for the current Semaphore's available-permit count. Declared {@code public}
   * for the same reason as {@link #setPrefetchParallelismForTest(int)}. <b>Do not call from
   * production code.</b>
   */
  public static int getPrefetchAvailablePermitsForTest() {
    return PREFETCH_LIMIT.availablePermits();
  }

  /**
   * Release the currently guarded leaf page.
   */
  private void clearCurrentLeaf() {
    // Nothing to release: the reader holds no guard. Clearing only drops the references so an
    // idle reader does not keep a leaf object (and its stamp) reachable.
    currentLeaf = null;
    currentLeafRef = null;
    currentLeafBinding = HOTLeafPage.STAMP_INVALID;
    currentLeafStamp = HOTLeafPage.STAMP_INVALID;
  }

  /**
   * Get the storage engine reader.
   */
  StorageEngineReader getStorageEngineReader() {
    return storageEngineReader;
  }

  @Override
  public void close() {
    clearCurrentLeaf();
    clearPath();
  }
}
