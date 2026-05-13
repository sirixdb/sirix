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
  /**
   * Per-level snapshot of {@link HOTIndirectPage#getMostSignificantBitIndex} captured during
   * the PEXT-routed descent. Used by {@link #lowerOrUpperBound} (Binna §4.2 lower_or_upper_bound,
   * reference: {@code HOTSingleThreaded.hpp:347-415}) to walk the search-stack back up to the
   * branching depth where the searchKey actually diverges from the candidate leaf's key.
   */
  private final short[] pathMsbAtDepth = new short[MAX_TREE_HEIGHT];
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
   * Result of a lower-bound or upper-bound seek. The leaf is positioned via the reader's
   * internal path-stack; subsequent {@link #advanceToNextLeaf()} calls continue iteration in
   * lex order (HOT children are sorted by first-key, so leftmost-first sibling traversal is
   * lex-monotonic). When the seek lands past every key in the trie, {@link #leaf} is
   * {@code null} and the caller should treat the cursor as exhausted.
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
  }

  /**
   * Locate the first entry whose key is {@code >= searchKey}, in lex order.
   *
   * <p>Reference: Robert Binna, <i>The Height Optimized Trie</i>, §4.2; reference impl
   * {@code HOTSingleThreaded::lower_or_upper_bound} ({@code HOTSingleThreaded.hpp:347-415}).</p>
   *
   * <p>Algorithm (5 phases, ports the C++ reference 1:1):</p>
   * <ol>
   *   <li><b>PEXT-routed descent with stack.</b> Use the existing
   *       {@link #navigateToLeaf(PageReference, byte[])} machinery — captures
   *       {@code (parentNode, childIdx, mostSignificantBitIndex)} at every level.
   *       Lands at a candidate leaf chosen by partial-key match, which may not be the
   *       lex-correct leaf when {@code searchKey} doesn't exist.</li>
   *   <li><b>Mismatch-bit detection.</b> Compute the first absolute bit position where
   *       any entry in the candidate leaf differs from {@code searchKey} (via
   *       {@link DiscriminativeBitComputer#computeDifferingBit(byte[], byte[])} on the
   *       leaf's first key — same partial-key prefix above the disc bit means same
   *       mismatch info). If keys are identical the candidate IS the lower bound.</li>
   *   <li><b>Walk stack up to branching depth.</b> Pop levels while the disc bit lies
   *       below the level's most-significant disc bit — bits above the disc bit already
   *       matched perfectly, so those levels routed correctly; bits at or above the disc
   *       bit determine where {@code searchKey} actually branches.</li>
   *   <li><b>Compute affected subtree at branching depth.</b> Find the contiguous run of
   *       siblings sharing the matched entry's bit-prefix above the disc bit. HOT children
   *       are stored lex-sorted by first-key, so disc-bit-prefix groups are contiguous in
   *       child-index order. Walk outward from the matched index using
   *       {@link DiscriminativeBitComputer#computeDifferingBit} on first-keys.</li>
   *   <li><b>Position at next entry.</b> If the searchKey's bit at the disc position is 1,
   *       lower-bound is one past the affected subtree; if 0, it is the first index of the
   *       affected subtree. Descend leftmost from there. If the next index falls past the
   *       branching node's last child, bubble up via {@link #advanceToNextLeaf()}.</li>
   * </ol>
   *
   * @param rootRef    root of the HOT subtree
   * @param searchKey  the lex search key
   * @return position of the first entry {@code >= searchKey}, or
   *         {@link LowerBoundResult#EXHAUSTED} when no such entry exists
   */
  public LowerBoundResult lowerBound(PageReference rootRef, byte[] searchKey) {
    // Phase 7u — opt-in via -Dhot.strict.phase7u.lexprimary=true: use lex-descent as the
    // PRIMARY routing algorithm. Lex-descent is correct under HOT I8 (children sorted by
    // firstKey), and unaffected by I5/I6 violations from byte-10 encoder discontinuity.
    // Cost: one extra firstKey-load per indirect-child during descent; works on any
    // I8-conformant trie.
    if (Boolean.getBoolean("hot.strict.phase7u.lexprimary")) {
      return phase7uLexDescentFallback(rootRef, searchKey, true);
    }
    final LowerBoundResult result = lowerOrUpperBound(rootRef, searchKey, true);
    // Phase 7u — verify-and-fall-back. The normal PEXT descent + walk-up returns a result,
    // but for trees with I5/I6 violations from byte-10 encoder discontinuity, the returned
    // leaf may not contain searchKey even though it IS stored. Opt-out via
    // -Dhot.strict.phase7u.lexfallback.disable=true.
    if (Boolean.getBoolean("hot.strict.phase7u.lexfallback.disable")) return result;
    if (result == LowerBoundResult.EXHAUSTED) {
      return phase7uLexDescentFallback(rootRef, searchKey, true);
    }
    // Check whether returned leaf actually contains searchKey or has it as next-key.
    final HOTLeafPage leaf = result.leaf;
    final int idx = result.indexInLeaf;
    if (leaf != null && idx < leaf.getEntryCount()) {
      final byte[] key = leaf.getKey(idx);
      // For lower_bound semantics: returned key must be ≥ searchKey AND ≤ any other stored
      // key ≥ searchKey. If returned key < searchKey, PEXT routing missed — fall back.
      if (key != null && java.util.Arrays.compareUnsigned(key, searchKey) < 0) {
        return phase7uLexDescentFallback(rootRef, searchKey, true);
      }
    }
    return result;
  }

  /**
   * Locate the first entry whose key is {@code > searchKey}, in lex order. See
   * {@link #lowerBound(PageReference, byte[])} for algorithm details.
   */
  public LowerBoundResult upperBound(PageReference rootRef, byte[] searchKey) {
    return lowerOrUpperBound(rootRef, searchKey, false);
  }

  private LowerBoundResult lowerOrUpperBound(PageReference rootRef, byte[] searchKey,
      boolean isLowerBound) {
    Objects.requireNonNull(rootRef);
    Objects.requireNonNull(searchKey);

    // Phase 1: PEXT-routed descent with stack tracking.
    final HOTLeafPage candidateLeaf = navigateToLeaf(rootRef, searchKey);
    if (candidateLeaf == null) {
      // Phase 7u — lex-descent fallback. PEXT navigation returned null (= structural
      // failure). Try a pure-lex descent that ignores PEXT/partial-key routing.
      if (!Boolean.getBoolean("hot.strict.phase7u.lexfallback.disable")) {
        return phase7uLexDescentFallback(rootRef, searchKey, isLowerBound);
      }
      return LowerBoundResult.EXHAUSTED;
    }

    // Phase 2: try exact match in candidate leaf first (cheap fast path; matches the
    // C++ reference where exact match through PEXT is the common case).
    final int exact = candidateLeaf.findEntry(searchKey);
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
    final int candidateEntryCount = candidateLeaf.getEntryCount();
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
    final byte[] candidateKey = candidateLeaf.getFirstKey();
    final int discBit = DiscriminativeBitComputer.computeDifferingBit(candidateKey, searchKey);
    if (discBit < 0) {
      // Candidate first-key equals searchKey but findEntry didn't match: defensive path.
      // Treat as exact match at index 0.
      if (isLowerBound) {
        return new LowerBoundResult(candidateLeaf, 0);
      }
      return advanceOneFrom(candidateLeaf, 0);
    }
    final boolean searchKeyBit = DiscriminativeBitComputer.isBitSet(searchKey, discBit);

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
      //                  side of the disc-bit split ⇒ in-subtree.
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
    //   searchKeyBit == 1 → searchKey would land AFTER the affected subtree
    //   searchKeyBit == 0 → searchKey would land at the FIRST entry of the subtree
    // For upper_bound on a non-exact-match, the answer is the same as lower_bound (the
    // first key strictly greater than searchKey), because no leaf entry equals searchKey.
    final int nextChildIdx = searchKeyBit ? (lastIdx + 1) : firstIdx;
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
      return LowerBoundResult.EXHAUSTED;
    }
    final HOTLeafPage targetLeaf = descendToLeftmostLeaf(nextRef);
    if (targetLeaf == null) {
      return LowerBoundResult.EXHAUSTED;
    }
    return new LowerBoundResult(targetLeaf, 0);
  }

  /**
   * Phase 7u — Lex-descent fallback for the rare case where PEXT-routed descent + walk-up
   * cannot locate a key that IS stored in the trie. This happens when the writer's stored
   * partials don't match the actual subtree contents (I5 violation), causing PEXT to route
   * to the wrong leaf at some intermediate level.
   *
   * <p>Algorithm: at each indirect from the root, find the LAST child whose firstKey is
   * &le; searchKey (binary-search by firstKey, not by PEXT). Descend until a leaf is
   * reached, then findEntry within the leaf.
   *
   * <p>This is a correctness fallback — it costs one extra descent per call, only invoked
   * after the normal descent fails. For trees with 0 I8 violations (firstKey-sorted), the
   * lex-descent is guaranteed to find any stored key.
   *
   * <p>HFT-grade: no allocation; bounded by tree height.
   */
  private LowerBoundResult phase7uLexDescentFallback(PageReference rootRef, byte[] searchKey,
      boolean isLowerBound) {
    if (rootRef == null) return LowerBoundResult.EXHAUSTED;
    // CRITICAL: populate the path stack so subsequent advanceToNextLeaf() walks correctly.
    // HOTRangeCursor calls advanceToNextLeaf() after the initial lowerBound — if the path
    // stack is stale or empty, the cursor traverses the wrong subtree forward and misses
    // entries that ARE in the trie.
    pathDepth = 0;
    PageReference ref = rootRef;
    int depth = 0;
    final int MAX_DEPTH = 64;
    while (depth++ < MAX_DEPTH) {
      final Page page = loadPage(ref);
      if (page == null) return LowerBoundResult.EXHAUSTED;
      if (page instanceof HOTLeafPage leaf) {
        final int exact = leaf.findEntry(searchKey);
        if (exact >= 0) {
          if (isLowerBound) return new LowerBoundResult(leaf, exact);
          return advanceOneFrom(leaf, exact);
        }
        final int insertionPoint = -(exact + 1);
        if (insertionPoint < leaf.getEntryCount()) {
          return new LowerBoundResult(leaf, insertionPoint);
        }
        // searchKey is past this leaf's last entry — advance via path stack to next leaf.
        final HOTLeafPage next = advanceToNextLeaf();
        if (next == null) return LowerBoundResult.EXHAUSTED;
        return new LowerBoundResult(next, 0);
      }
      if (!(page instanceof HOTIndirectPage indirect)) return LowerBoundResult.EXHAUSTED;
      // Find the LAST child whose firstKey is ≤ searchKey.
      final int n = indirect.getNumChildren();
      int chosenIdx = -1;
      for (int i = 0; i < n; i++) {
        final byte[] fk = getFirstKeyOfChild(indirect, i);
        if (fk == null || fk.length == 0) continue;
        final int cmp = java.util.Arrays.compareUnsigned(fk, searchKey);
        if (cmp <= 0) {
          chosenIdx = i;
        } else {
          break;
        }
      }
      if (chosenIdx < 0) {
        // searchKey is smaller than all children's firstKeys → descend leftmost.
        chosenIdx = 0;
      }
      // Push to path stack BEFORE descending — required for advanceToNextLeaf() to bubble up.
      pushPath(ref, indirect, chosenIdx);
      ref = indirect.getChildReference(chosenIdx);
      if (ref == null) return LowerBoundResult.EXHAUSTED;
    }
    return LowerBoundResult.EXHAUSTED;
  }

  /**
   * Step one entry forward from {@code (leaf, idx)}, advancing across leaves via the path
   * stack when the leaf is exhausted. Used for upper_bound stepping past an exact match.
   */
  private LowerBoundResult advanceOneFrom(HOTLeafPage leaf, int idx) {
    if (idx + 1 < leaf.getEntryCount()) {
      return new LowerBoundResult(leaf, idx + 1);
    }
    final HOTLeafPage next = advanceToNextLeaf();
    if (next == null) {
      return LowerBoundResult.EXHAUSTED;
    }
    return new LowerBoundResult(next, 0);
  }

  /**
   * Resolve the lex-smallest key under {@code parent}'s child {@code childIdx}, descending
   * leftmost when the child is itself an indirect page. Mirrors HOTTrieWriter.getFirstKeyFromChild
   * but reuses this reader's {@link #loadPage} to swizzle on cold pages.
   *
   * <p>Cost: at most one full leftmost descent of the subtree per call. For typical fanout-32
   * HOT trees, the lower_bound walk-outward at the branching depth fires this O(run-length)
   * times, with run length bounded by {@code numChildren ≤ 32}.</p>
   */
  private byte[] getFirstKeyOfChild(HOTIndirectPage parent, int childIdx) {
    PageReference ref = parent.getChildReference(childIdx);
    while (ref != null) {
      final Page page = loadPage(ref);
      if (page == null) {
        return new byte[0];
      }
      if (page instanceof HOTLeafPage leaf) {
        return leaf.getFirstKey();
      }
      if (!(page instanceof HOTIndirectPage indirect)) {
        return new byte[0];
      }
      ref = indirect.getChildReference(0);
    }
    return new byte[0];
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

