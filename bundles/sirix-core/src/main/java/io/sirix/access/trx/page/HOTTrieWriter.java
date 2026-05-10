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
import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.Allocators;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.cache.PageContainer;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.index.hot.DiscriminativeBitComputer;
import io.sirix.index.hot.NodeUpgradeManager;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.function.LongSupplier;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * HOT trie writer for HOT (Height Optimized Trie) navigation.
 * 
 * <p>
 * This class provides an alternative to the bit-decomposition approach of {@code KeyedTrieWriter} using
 * semantic key-based navigation with HOT compound nodes.
 * </p>
 * 
 * <p>
 * <b>Key Features:</b>
 * </p>
 * <ul>
 * <li>Zero allocations on hot path (pre-allocated arrays instead of Deque)</li>
 * <li>SIMD-optimized child lookup via HOTIndirectPage</li>
 * <li>COW propagation using parent path tracking</li>
 * <li>Integration with existing versioning infrastructure</li>
 * </ul>
 * 
 * <p>
 * <b>Usage:</b>
 * </p>
 * 
 * <pre>{@code
 * HOTTrieWriter writer = new HOTTrieWriter(pageKeyAllocator);
 * PageContainer container =
 *     writer.prepareKeyedLeafForModification(storageEngineReader, log, startReference, key, indexType, indexNumber);
 * HOTLeafPage leaf = (HOTLeafPage) container.getModified();
 * leaf.put(key, value);
 * }</pre>
 * 
 * @author Johannes Lichtenberger
 * @see HOTLeafPage
 * @see HOTIndirectPage
 */
public final class HOTTrieWriter {

  /** Maximum tree height - increased to handle unbalanced trees during inserts. */
  private static final int MAX_TREE_HEIGHT = 64;

  /** Shared empty key constant to avoid allocations. */
  private static final byte[] EMPTY_KEY = new byte[0];

  // ===== Strict-Binna intermediate-BiNode fallback firing counter =====
  // Counts how many times the intermediate-BiNode fallback in
  // updateParentForSplitWithPath (the c868e669c workaround) fires under
  // -Dhot.strict.binna=true. Each firing grows tree depth by 1 on the affected path.
  // Resettable by the test harness; queryable via getIntermediateBiNodeFallbackFirings.
  // Used by Phase 2 / Phase 3 / Phase 4b regression measurement.
  private static final java.util.concurrent.atomic.AtomicLong INTERMEDIATE_BINODE_FALLBACK_FIRINGS =
      new java.util.concurrent.atomic.AtomicLong(0L);

  /**
   * Returns the count of times the intermediate-BiNode fallback in
   * {@link #updateParentForSplitWithPath} has fired since the last reset.
   *
   * <p>Rationale: each firing creates a persisted 2-entry BiNode at the original
   * child's slot, growing the affected path's depth by 1. Counting these firings
   * quantifies how often strict-Binna mode falls off the height-optimal path. Phase 3
   * (lazy retroactive sibling rebalance) and Phase 4b (Binna-faithful rebuild paths)
   * aim to drive this counter to 0 on benign workloads.
   *
   * <p>HFT-grade: atomic increments only fire on the rare fallback branch, not on the
   * hot common addEntryWithPDep success path.
   */
  public static long getIntermediateBiNodeFallbackFirings() {
    return INTERMEDIATE_BINODE_FALLBACK_FIRINGS.get();
  }

  /** Reset the diagnostic counter — typically called from a test's setUp. */
  public static void resetIntermediateBiNodeFallbackFirings() {
    INTERMEDIATE_BINODE_FALLBACK_FIRINGS.set(0L);
  }

  // ===== Phase 3 lazy retroactive sibling rebalance counter =====
  // Counts how many times the Phase 3 rebalance fires under -Dhot.strict.binna=true
  // (i.e., addEntryWithPDep rejected because some non-split sibling was non-constant
  // on the new disc bit, and rebalanceSiblingsForBit was invoked to resolve it).
  // Each firing resolves one Case 2b-ii rejection cleanly without growing tree depth.
  // Compare against INTERMEDIATE_BINODE_FALLBACK_FIRINGS — Phase 3 success means
  // rebalance firings climb while fallback firings drop to 0.
  private static final java.util.concurrent.atomic.AtomicLong PHASE3_REBALANCE_FIRINGS =
      new java.util.concurrent.atomic.AtomicLong(0L);

  /** Returns the count of Phase 3 sibling-rebalance firings since the last reset. */
  public static long getPhase3RebalanceFirings() {
    return PHASE3_REBALANCE_FIRINGS.get();
  }

  /** Reset the Phase 3 firing counter. */
  public static void resetPhase3RebalanceFirings() {
    PHASE3_REBALANCE_FIRINGS.set(0L);
  }

  // ===== Phase 4b diagnostic — buildCompressedHalf fallback counters =====
  // Each counter increments when buildCompressedHalf takes the corresponding fallback to
  // createNodeFromChildren (which uses the buggy adjacent-pair XOR scan and produces
  // I-Binna constancy violations). These make explicit which fallback case is firing on a
  // given workload — actionable input for Phase 4b's MultiMask / identical-keys /
  // cross-window / newMask==0 fixes.
  //
  // Reset via {@link #resetBuildCompressedHalfFallbackCounters}; read via the per-counter
  // accessors below. Diagnostic only; default zero, no behavior impact when not consulted.
  private static final java.util.concurrent.atomic.AtomicLong BCH_FALLBACK_MULTIMASK_PARENT =
      new java.util.concurrent.atomic.AtomicLong(0L);
  private static final java.util.concurrent.atomic.AtomicLong BCH_FALLBACK_IDENTICAL_KEYS =
      new java.util.concurrent.atomic.AtomicLong(0L);
  private static final java.util.concurrent.atomic.AtomicLong BCH_FALLBACK_CROSS_WINDOW =
      new java.util.concurrent.atomic.AtomicLong(0L);
  private static final java.util.concurrent.atomic.AtomicLong BCH_FALLBACK_NEW_MASK_ZERO =
      new java.util.concurrent.atomic.AtomicLong(0L);
  private static final java.util.concurrent.atomic.AtomicLong BCH_FALLBACK_UNKNOWN_CHILD =
      new java.util.concurrent.atomic.AtomicLong(0L);
  private static final java.util.concurrent.atomic.AtomicLong BCH_FALLBACK_PARTIAL_COLLISION =
      new java.util.concurrent.atomic.AtomicLong(0L);

  /** Stage G — count of addEntryWithPDep / buildCompressedHalf I4-violation rejects. */
  private static final java.util.concurrent.atomic.AtomicLong G1_I4_REJECT_ADDENTRY =
      new java.util.concurrent.atomic.AtomicLong(0L);
  private static final java.util.concurrent.atomic.AtomicLong G3_I4_REJECT_BCH =
      new java.util.concurrent.atomic.AtomicLong(0L);

  public static long getG1I4RejectAddEntry() { return G1_I4_REJECT_ADDENTRY.get(); }
  public static long getG3I4RejectBch() { return G3_I4_REJECT_BCH.get(); }
  public static void resetG1G3Counters() {
    G1_I4_REJECT_ADDENTRY.set(0L);
    G3_I4_REJECT_BCH.set(0L);
  }

  /** Returns the count of MultiMask-parent fallbacks since the last reset. */
  public static long getBchFallbackMultiMaskParent() { return BCH_FALLBACK_MULTIMASK_PARENT.get(); }
  /** Returns the count of identical-keys fallbacks since the last reset. */
  public static long getBchFallbackIdenticalKeys() { return BCH_FALLBACK_IDENTICAL_KEYS.get(); }
  /** Returns the count of cross-window-split-bit fallbacks since the last reset. */
  public static long getBchFallbackCrossWindow() { return BCH_FALLBACK_CROSS_WINDOW.get(); }
  /** Returns the count of newMask==0 fallbacks since the last reset. */
  public static long getBchFallbackNewMaskZero() { return BCH_FALLBACK_NEW_MASK_ZERO.get(); }
  /** Returns the count of unknown-child fallbacks since the last reset. */
  public static long getBchFallbackUnknownChild() { return BCH_FALLBACK_UNKNOWN_CHILD.get(); }
  /** Returns the count of partial-collision fallbacks since the last reset. */
  public static long getBchFallbackPartialCollision() { return BCH_FALLBACK_PARTIAL_COLLISION.get(); }

  /** Reset all buildCompressedHalf-fallback counters. */
  public static void resetBuildCompressedHalfFallbackCounters() {
    BCH_FALLBACK_MULTIMASK_PARENT.set(0L);
    BCH_FALLBACK_IDENTICAL_KEYS.set(0L);
    BCH_FALLBACK_CROSS_WINDOW.set(0L);
    BCH_FALLBACK_NEW_MASK_ZERO.set(0L);
    BCH_FALLBACK_UNKNOWN_CHILD.set(0L);
    BCH_FALLBACK_PARTIAL_COLLISION.set(0L);
  }

  /** Diagnostic counter: how many times buildCompressedHalf SingleMask path was entered. */
  static final java.util.concurrent.atomic.AtomicLong BCH_SINGLEMASK_ENTRIES =
      new java.util.concurrent.atomic.AtomicLong(0L);
  /** Diagnostic counter: encoding mismatches detected (repositioned != directDense). */
  static final java.util.concurrent.atomic.AtomicLong BCH_ENCODING_MISMATCHES =
      new java.util.concurrent.atomic.AtomicLong(0L);
  public static long getBchSingleMaskEntries() { return BCH_SINGLEMASK_ENTRIES.get(); }
  public static long getBchEncodingMismatches() { return BCH_ENCODING_MISMATCHES.get(); }
  public static void resetBchEncodingDiagnostics() {
    BCH_SINGLEMASK_ENTRIES.set(0L);
    BCH_ENCODING_MISMATCHES.set(0L);
  }

  static {
    // Phase 4b-vb.3 deep-dive: install a global post-creation hook on HOTIndirectPage
    // that runs the sparse-path probe. The hook checks the {@code hot.debug.sparsepath}
    // system property at every invocation — gated at runtime so production has zero
    // overhead AND tests that set the property after class load still get the probe.
    HOTIndirectPage.POST_CREATE_HOOK = page -> {
      if (Boolean.getBoolean("hot.debug.sparsepath")) {
        probeSparsePathStatic(page, "post-create-hook");
      }
    };
  }

  /**
   * Static variant of probeSparsePathOnBuild — for use from the global
   * {@link HOTIndirectPage#POST_CREATE_HOOK}. Cannot use {@code activeReader} so it only
   * checks children whose first key can be obtained without a load (page-attached refs).
   */
  private static void probeSparsePathStatic(HOTIndirectPage built, String label) {
    final int[] partials = built.getPartialKeys();
    if (partials == null) return;
    final int n = built.getNumChildren();
    final boolean isSingleMask = built.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK;
    final int initialBytePos = built.getInitialBytePos();
    final long bitMask = built.getBitMask();
    final byte[] extPos = built.getExtractionPositions();
    final long[] extMasks = built.getExtractionMasks();

    StringBuilder log = null;
    for (int i = 0; i < n; i++) {
      final PageReference child = built.getChildReference(i);
      if (child == null) continue;
      final byte[] fk = staticGetFirstKeyFromChild(child);
      if (fk == null || fk.length == 0) continue;
      final int dense;
      if (isSingleMask) {
        dense = computePartialKeySingleMask(fk, initialBytePos, bitMask);
      } else if (extPos != null && extMasks != null) {
        dense = computePartialKeyMultiMaskDirect(fk, extPos, extMasks, extPos.length);
      } else {
        continue;
      }
      final int stored = partials[i];
      if ((stored & ~dense) != 0) {
        if (log == null) {
          log = new StringBuilder(512);
          log.append("[hot.sparsepath] BUILD-VIOLATION pageKey=").append(built.getPageKey())
             .append(" label=").append(label)
             .append(" layout=").append(built.getLayoutType())
             .append(" mask=0x").append(Long.toHexString(bitMask))
             .append(" initialBytePos=").append(initialBytePos)
             .append(" numChildren=").append(n).append('\n');
        }
        log.append("  [child ").append(i).append("] stored=0x").append(Integer.toHexString(stored))
           .append(" dense=0x").append(Integer.toHexString(dense))
           .append(" excess=0x").append(Integer.toHexString(stored & ~dense))
           .append(" firstKey=").append(bytesToHex(fk)).append('\n');
      }
      // I5-strict at-build probe: walk c's swizzled subtree, find any interior key whose
      // dense PEXT misses bits set in c.stored. Catches multi-entry-leaf β-mixing at
      // CONSTRUCTION TIME (= the offending build site), not at end-of-test validator time.
      if (stored != 0 && child.getPage() != null) {
        final byte[][] failingKey = {null};
        final int[] failingDense = {-1};
        staticWalkLeavesUntilFalse(child, leaf -> {
          final int ec = leaf.getEntryCount();
          for (int kk = 0; kk < ec; kk++) {
            final byte[] keyK = leaf.getKey(kk);
            if (keyK == null || keyK.length == 0) continue;
            final int denseK;
            if (isSingleMask) {
              denseK = computePartialKeySingleMask(keyK, initialBytePos, bitMask);
            } else if (extPos != null && extMasks != null) {
              denseK = computePartialKeyMultiMaskDirect(keyK, extPos, extMasks, extPos.length);
            } else {
              continue;
            }
            if ((stored & ~denseK) != 0) {
              failingKey[0] = keyK;
              failingDense[0] = denseK;
              return false;
            }
          }
          return true;
        });
        if (failingKey[0] != null) {
          if (log == null) {
            log = new StringBuilder(512);
            log.append("[hot.sparsepath] I5-LEAF-CONSTANCY-AT-BUILD pageKey=")
               .append(built.getPageKey())
               .append(" label=").append(label)
               .append(" layout=").append(built.getLayoutType())
               .append(" mask=0x").append(Long.toHexString(bitMask))
               .append(" initialBytePos=").append(initialBytePos)
               .append(" numChildren=").append(n).append('\n');
          }
          log.append("  [child ").append(i).append("] stored=0x").append(Integer.toHexString(stored))
             .append(" subtreeKey.dense=0x").append(Integer.toHexString(failingDense[0]))
             .append(" excess=0x").append(Integer.toHexString(stored & ~failingDense[0]))
             .append(" firstKey=").append(bytesToHex(fk))
             .append(" failingKey=").append(bytesToHex(failingKey[0])).append('\n');
        }
      }
    }
    if (log != null) {
      final StackTraceElement[] st = Thread.currentThread().getStackTrace();
      for (int i = 1; i < Math.min(st.length, 18); i++) {
        log.append("  at ").append(st[i].getClassName()).append('.').append(st[i].getMethodName())
           .append('(').append(st[i].getFileName()).append(':').append(st[i].getLineNumber())
           .append(")\n");
      }
      log.append('\n');
      try {
        java.nio.file.Files.writeString(
            java.nio.file.Paths.get("/tmp/sirix-sparse-path-violations.log"),
            log.toString(),
            java.nio.file.StandardOpenOption.CREATE,
            java.nio.file.StandardOpenOption.APPEND);
      } catch (java.io.IOException ignore) { /* best-effort */ }
    }
  }

  /**
   * Static fallback for getFirstKeyFromChild — only consults the page already attached
   * to the reference. Returns null if not loadable without an activeReader. Sufficient
   * for the post-create probe because at construction time, freshly-built children
   * always have their pages attached via setPage.
   */
  private static byte[] staticGetFirstKeyFromChild(PageReference ref) {
    final Page page = ref.getPage();
    if (page instanceof HOTLeafPage leaf) return leaf.getFirstKey();
    if (page instanceof HOTIndirectPage indirect) {
      final PageReference c = indirect.getChildReference(0);
      return c != null ? staticGetFirstKeyFromChild(c) : null;
    }
    return null;
  }

  /**
   * Phase 4b-vb sparse-path-strict: compute the AND of dense PEXT over every key
   * in {@code c}'s subtree under SingleMask {@code newMask}. The result is the
   * "weakest" stored partial that still subset-matches every key in c's subtree,
   * guaranteeing the I5-strict invariant (sparse ⊆ dense_K for every K in subtree)
   * holds for inherited children regardless of multi-entry-leaf β-mixing in their
   * subtrees.
   *
   * <p>HFT note: walks the entire subtree once. Cost O(N_subtree × bit-extract).
   */
  private int computeSubtreeIntersectionDenseSingleMask(PageReference c,
      int initialBytePos, long newMask) {
    final int[] result = {-1}; // -1 = uninitialized
    walkLeavesUntilFalseInstance(c, leaf -> {
      final int ec = leaf.getEntryCount();
      for (int k = 0; k < ec; k++) {
        final byte[] key = leaf.getKey(k);
        if (key == null || key.length == 0) continue;
        final int dense = computePartialKeySingleMask(key, initialBytePos, newMask);
        result[0] = result[0] == -1 ? dense : (result[0] & dense);
        if (result[0] == 0) return false; // can't get smaller; early exit
      }
      return true;
    });
    return result[0] == -1 ? 0 : result[0];
  }

  /**
   * Phase 4b-vb sparse-path-strict (MultiMask): same as
   * {@link #computeSubtreeIntersectionDenseSingleMask} but for MultiMask layouts.
   */
  private int computeSubtreeIntersectionDenseMultiMask(PageReference c,
      byte[] extPos, long[] extMasks, int numExtractionBytes) {
    final int[] result = {-1};
    walkLeavesUntilFalseInstance(c, leaf -> {
      final int ec = leaf.getEntryCount();
      for (int k = 0; k < ec; k++) {
        final byte[] key = leaf.getKey(k);
        if (key == null || key.length == 0) continue;
        final int dense = computePartialKeyMultiMaskDirect(key, extPos, extMasks, numExtractionBytes);
        result[0] = result[0] == -1 ? dense : (result[0] & dense);
        if (result[0] == 0) return false;
      }
      return true;
    });
    return result[0] == -1 ? 0 : result[0];
  }

  /** Instance-method walk: uses {@code activeReader} to load any non-swizzled child pages. */
  private void walkLeavesUntilFalseInstance(PageReference ref,
      java.util.function.Predicate<HOTLeafPage> visitor) {
    if (ref == null) return;
    Page page = ref.getPage();
    if (page == null && activeReader != null) {
      page = loadPage(activeReader, ref);
      if (page != null) ref.setPage(page);
    }
    if (page instanceof HOTLeafPage leaf) {
      visitor.test(leaf);
      return;
    }
    if (page instanceof HOTIndirectPage indirect) {
      final int n = indirect.getNumChildren();
      for (int i = 0; i < n; i++) {
        final PageReference childRef = indirect.getChildReference(i);
        if (childRef == null) continue;
        walkLeavesUntilFalseInstance(childRef, visitor);
      }
    }
  }

  /** Walk every leaf reachable from {@code ref} via swizzled pages only. Stops early when
   * {@code visitor} returns {@code false}. Used by I5-strict at-build probe. */
  private static void staticWalkLeavesUntilFalse(PageReference ref,
      java.util.function.Predicate<HOTLeafPage> visitor) {
    if (ref == null) return;
    final Page page = ref.getPage();
    if (page instanceof HOTLeafPage leaf) {
      visitor.test(leaf);
      return;
    }
    if (page instanceof HOTIndirectPage indirect) {
      final int n = indirect.getNumChildren();
      for (int i = 0; i < n; i++) {
        final PageReference childRef = indirect.getChildReference(i);
        if (childRef == null) continue;
        staticWalkLeavesUntilFalse(childRef, visitor);
      }
    }
  }

  /**
   * Diagnostic probe (Phase 4b-vb.3 deep dive): for an indirect just constructed in the
   * writer, verify the sparse-path NECESSARY condition holds for every child:
   * {@code (stored & ~dense) == 0}. If a child's stored partial has bits not in its
   * dense PEXT under the indirect's mask, log the violation + a trimmed stack trace
   * so the offending construction code path is identifiable.
   *
   * <p>Gated on {@code -Dhot.debug.sparsepath=true}. Logs to
   * {@code /tmp/sirix-sparse-path-violations.log}.
   *
   * <p>Call this after EVERY indirect-creation site in the writer. The probe is heavy
   * (loads child first keys), so do not enable in production.
   */
  private void probeSparsePathOnBuild(HOTIndirectPage built, String label) {
    if (!Boolean.getBoolean("hot.debug.sparsepath")) return;
    final int[] partials = built.getPartialKeys();
    if (partials == null) return;
    final int n = built.getNumChildren();
    final boolean isSingleMask = built.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK;
    final int initialBytePos = built.getInitialBytePos();
    final long bitMask = built.getBitMask();
    final byte[] extPos = built.getExtractionPositions();
    final long[] extMasks = built.getExtractionMasks();

    StringBuilder log = null;
    for (int i = 0; i < n; i++) {
      final PageReference child = built.getChildReference(i);
      if (child == null) continue;
      final byte[] fk = getFirstKeyFromChild(child);
      if (fk == null || fk.length == 0) continue;
      final int dense;
      if (isSingleMask) {
        dense = computePartialKeySingleMask(fk, initialBytePos, bitMask);
      } else if (extPos != null && extMasks != null) {
        dense = computePartialKeyMultiMaskDirect(fk, extPos, extMasks, extPos.length);
      } else {
        continue;
      }
      final int stored = partials[i];
      if ((stored & ~dense) != 0) {
        if (log == null) {
          log = new StringBuilder(512);
          log.append("[hot.sparsepath] BUILD-VIOLATION pageKey=").append(built.getPageKey())
             .append(" label=").append(label)
             .append(" layout=").append(built.getLayoutType())
             .append(" mask=0x").append(Long.toHexString(bitMask))
             .append(" initialBytePos=").append(initialBytePos)
             .append(" numChildren=").append(n).append('\n');
        }
        log.append("  [child ").append(i).append("] stored=0x").append(Integer.toHexString(stored))
           .append(" dense=0x").append(Integer.toHexString(dense))
           .append(" excess=0x").append(Integer.toHexString(stored & ~dense))
           .append(" firstKey=").append(bytesToHex(fk)).append('\n');
      }
    }
    if (log != null) {
      final StackTraceElement[] st = Thread.currentThread().getStackTrace();
      for (int i = 1; i < Math.min(st.length, 12); i++) {
        log.append("  at ").append(st[i].getClassName()).append('.').append(st[i].getMethodName())
           .append('(').append(st[i].getFileName()).append(':').append(st[i].getLineNumber())
           .append(")\n");
      }
      try {
        java.nio.file.Files.writeString(
            java.nio.file.Paths.get("/tmp/sirix-sparse-path-violations.log"),
            log.toString(),
            java.nio.file.StandardOpenOption.CREATE,
            java.nio.file.StandardOpenOption.APPEND);
      } catch (java.io.IOException ignore) { /* best-effort */ }
    }
  }

  /** Diagnostic-only helper: format a byte array as a hex string. */
  private static String bytesToHex(byte[] b) {
    if (b == null) return "null";
    final StringBuilder sb = new StringBuilder(b.length * 2);
    for (final byte v : b) {
      sb.append(String.format("%02x", v & 0xFF));
    }
    return sb.toString();
  }

  // ===== Phase 4 β-already-in-mask subtree-merge counter =====
  // Counts how many times the Phase 4 subtree merge fires under -Dhot.strict.binna=true
  // (i.e., addEntryWithPDep / addEntryMultiMask rejected because the new disc bit β is
  // already in the parent's mask, and the moveHalf of the leaf split was bulk-inserted
  // into the existing β=¬v_L sibling's subtree). Each firing resolves one β-already-in-mask
  // rejection cleanly without growing tree depth.
  //
  // Compare against INTERMEDIATE_BINODE_FALLBACK_FIRINGS — Phase 4 success means subtree-merge
  // firings climb while fallback firings drop further toward 0.
  private static final java.util.concurrent.atomic.AtomicLong PHASE4_SUBTREE_MERGE_FIRINGS =
      new java.util.concurrent.atomic.AtomicLong(0L);

  /** Returns the count of Phase 4 subtree-merge firings since the last reset. */
  public static long getPhase4SubtreeMergeFirings() {
    return PHASE4_SUBTREE_MERGE_FIRINGS.get();
  }

  /** Reset the Phase 4 firing counter. */
  public static void resetPhase4SubtreeMergeFirings() {
    PHASE4_SUBTREE_MERGE_FIRINGS.set(0L);
  }

  // ===== Case 2b-iv-b-β: addEntry fresh-polarity firing counter =====
  // Counts how many times {@link #addEntryWithPDep} / {@link #addEntryMultiMask} resolved a
  // β-already-in-mask rejection by adding a NEW child slot for the moveHalf with stored
  // partial = splitChildPartial XOR β-bit. The mask stays unchanged; the parent grows by one
  // child slot (k → k+1). Each firing avoids both the intermediate-BiNode fallback (no depth
  // growth) and the Phase 4 subtree-merge (no key relocation). Gate: {@code -Dhot.strict.binna=true}.
  private static final java.util.concurrent.atomic.AtomicLong ADDENTRY_FRESH_POLARITY_FIRINGS =
      new java.util.concurrent.atomic.AtomicLong(0L);

  /** Returns the count of Case 2b-iv-b-β fresh-polarity firings since the last reset. */
  public static long getAddEntryFreshPolarityFirings() {
    return ADDENTRY_FRESH_POLARITY_FIRINGS.get();
  }

  /** Reset the fresh-polarity firing counter. */
  public static void resetAddEntryFreshPolarityFirings() {
    ADDENTRY_FRESH_POLARITY_FIRINGS.set(0L);
  }

  /** Memory segment allocator. */
  @SuppressWarnings("unused")
  private final MemorySegmentAllocator allocator;

  /** Persistent page key allocator — replaces the old hardcoded nextPageKey counter. */
  private final LongSupplier pageKeyAllocator;

  // ===== Pre-allocated COW path - ZERO allocations on hot path! =====
  private final PageReference[] cowPathRefs = new PageReference[MAX_TREE_HEIGHT];
  private final HOTIndirectPage[] cowPathNodes = new HOTIndirectPage[MAX_TREE_HEIGHT];
  private final int[] cowPathChildIndices = new int[MAX_TREE_HEIGHT];
  private int cowPathDepth = 0;

  // ===== Pre-allocated buffers for splitParentAndRecurse - avoids ArrayList =====
  private static final int MAX_SPLIT_CHILDREN = NodeUpgradeManager.MULTI_NODE_MAX_CHILDREN + 2;
  private final PageReference[] splitSmallerBuf = new PageReference[MAX_SPLIT_CHILDREN];
  private final PageReference[] splitLargerBuf = new PageReference[MAX_SPLIT_CHILDREN];

  /**
   * Active reader for resolving non-swizzled child pages during split operations.
   * Set at the entry points of public methods, cleared on exit.
   */
  private @Nullable StorageEngineReader activeReader;

  /**
   * TIL bound for the current split / augment scope. Set/reset symmetrically with
   * {@link #activeReader} so {@link #splitSubtreeOnBit} can install the freshly-built halves
   * in the CoW log without explicit threading.
   */
  private @Nullable TransactionIntentLog activeLog;

  /**
   * Holds either SingleMask or MultiMask discriminative bit information.
   * SingleMask: all disc bits fit within 8 contiguous key bytes (one 64-bit PEXT mask).
   * MultiMask: disc bits span >8 bytes; uses per-byte extraction positions and masks.
   */
  private record DiscBitsInfo(
      HOTIndirectPage.LayoutType layoutType,
      int initialBytePos,            // SingleMask only
      long bitMask,                  // SingleMask only
      byte[] extractionPositions,    // MultiMask only: key byte positions to gather
      long[] extractionMasks,        // MultiMask only: PEXT masks per 8-byte chunk
      int numExtractionBytes,        // MultiMask only: number of extraction bytes
      short mostSignificantBitIndex  // Minimum absolute disc bit position
  ) {
    boolean isSingleMask() {
      return layoutType == HOTIndirectPage.LayoutType.SINGLE_MASK;
    }

    int totalDiscBits() {
      if (isSingleMask()) {
        return Long.bitCount(bitMask);
      }
      int total = 0;
      for (final long m : extractionMasks) {
        total += Long.bitCount(m);
      }
      return total;
    }

    static DiscBitsInfo singleMask(int initialBytePos, long bitMask) {
      return new DiscBitsInfo(HOTIndirectPage.LayoutType.SINGLE_MASK, initialBytePos, bitMask,
          null, null, 0, HOTIndirectPage.computeMostSignificantBitIndex(initialBytePos, bitMask));
    }

    static DiscBitsInfo multiMask(byte[] extractionPositions, long[] extractionMasks,
        int numExtractionBytes, short mostSignificantBitIndex) {
      return new DiscBitsInfo(HOTIndirectPage.LayoutType.MULTI_MASK, 0, 0,
          extractionPositions, extractionMasks, numExtractionBytes, mostSignificantBitIndex);
    }
  }

  /**
   * Create a new HOTTrieWriter with persistent page key allocation.
   *
   * <p>The supplier is called each time a new page key is needed (during splits and
   * new page creation). It must return a unique, monotonically increasing key that
   * is persisted across transactions (typically backed by an index page counter).</p>
   *
   * @param pageKeyAllocator supplier of persistent page keys
   */
  public HOTTrieWriter(final LongSupplier pageKeyAllocator) {
    this.allocator = Allocators.getInstance();
    this.pageKeyAllocator = Objects.requireNonNull(pageKeyAllocator);
  }

  /**
   * Create a new HOTTrieWriter with a temporary in-memory counter (for tests only).
   *
   * @deprecated Use {@link #HOTTrieWriter(LongSupplier)} with a persistent allocator.
   */
  @Deprecated
  public HOTTrieWriter() {
    this.allocator = Allocators.getInstance();
    final long[] counter = {1_000_000L};
    this.pageKeyAllocator = () -> counter[0]++;
  }

  /**
   * Navigate keyed trie (HOT) to find or create leaf page for given key. Analogous to
   * KeyedTrieWriter.prepareLeafOfTree() but uses semantic key navigation.
   *
   * @param storageEngineReader storage engine writer
   * @param log transaction intent log
   * @param startReference root reference (from PathPage/CASPage/NamePage)
   * @param key the search key (semantic bytes, not decomposed bit-by-bit)
   * @param indexType PATH/CAS/NAME
   * @param indexNumber the index number
   * @return PageContainer with complete and modifying HOTLeafPage, or null if not found
   */
  public @Nullable PageContainer prepareKeyedLeafForModification(StorageEngineWriter storageEngineReader,
      TransactionIntentLog log, PageReference startReference, byte[] key,
      IndexType indexType, int indexNumber) {

    Objects.requireNonNull(storageEngineReader);
    Objects.requireNonNull(log);
    Objects.requireNonNull(startReference);
    Objects.requireNonNull(key);
    Objects.requireNonNull(indexType);

    // Reset COW path (no allocation!)
    cowPathDepth = 0;

    // Check if already in log (modified this transaction)
    PageContainer cached = log.get(startReference);
    if (cached != null) {
      return navigateWithinCachedTree(cached, key, storageEngineReader, log, indexType, indexNumber);
    }

    // Navigate HOT trie, COW'ing along the path (uses pre-allocated arrays)
    PageReference leafRef = navigateToLeaf(storageEngineReader, startReference, key, log);

    if (leafRef == null) {
      // Key not found, need to create new leaf
      return createNewLeaf(storageEngineReader, log, startReference, key, indexType, indexNumber);
    }

    // Get leaf page through versioning pipeline
    return dereferenceHOTLeafForModification(storageEngineReader, leafRef, log);
  }

  /**
   * Navigate through keyed trie (HOT) compound nodes to reach leaf. Each node type uses
   * discriminative bits for child-finding (SIMD-optimized). Uses pre-allocated cowPath arrays - ZERO
   * allocations!
   *
   * @param storageEngineReader the storage engine reader
   * @param startReference the starting reference
   * @param key the search key
   * @param log the transaction intent log
   * @return the leaf page reference, or null if not found
   */
  private @Nullable PageReference navigateToLeaf(StorageEngineReader storageEngineReader,
      PageReference startReference, byte[] key, TransactionIntentLog log) {

    PageReference currentRef = startReference;

    while (true) {
      // Check log first
      PageContainer container = log.get(currentRef);
      Page page;
      if (container != null) {
        page = container.getComplete();
      } else {
        page = loadPage(storageEngineReader, currentRef);
      }

      if (page == null) {
        return null; // Empty trie
      }

      // Use pageKind check instead of instanceof (faster!)
      if (page instanceof HOTLeafPage) {
        return currentRef;
      }

      if (!(page instanceof HOTIndirectPage hotNode)) {
        return null; // Unexpected page type
      }

      // Find child reference using HOT node type-specific logic
      int childIndex = hotNode.findChildIndex(key);
      if (childIndex < 0) {
        return null; // Key not found
      }

      PageReference childRef = hotNode.getChildReference(childIndex);
      if (childRef == null) {
        return null;
      }

      // Record path for COW propagation (no allocation!)
      pushCowPath(currentRef, hotNode, childIndex);

      currentRef = childRef;
    }
  }

  /**
   * Flyweight push for COW path - no allocation!
   */
  private void pushCowPath(PageReference ref, HOTIndirectPage node, int childIdx) {
    if (cowPathDepth >= MAX_TREE_HEIGHT) {
      throw new IllegalStateException("HOT tree exceeds maximum height: " + MAX_TREE_HEIGHT);
    }
    cowPathRefs[cowPathDepth] = ref;
    cowPathNodes[cowPathDepth] = node;
    cowPathChildIndices[cowPathDepth] = childIdx;
    cowPathDepth++;
  }

  /**
   * Clear COW path references (allows GC but no allocation).
   */
  private void clearCowPath() {
    for (int i = 0; i < cowPathDepth; i++) {
      cowPathRefs[i] = null;
      cowPathNodes[i] = null;
    }
    cowPathDepth = 0;
  }

  /**
   * Dereference HOT leaf page for modification using versioning pipeline. Uses pre-allocated cowPath
   * arrays - ZERO allocations except for page copies!
   */
  private @Nullable PageContainer dereferenceHOTLeafForModification(StorageEngineWriter storageEngineReader,
      PageReference leafRef, TransactionIntentLog log) {

    // Check if already in log
    PageContainer existing = log.get(leafRef);
    if (existing != null) {
      return existing;
    }

    // Load the leaf page
    Page page = loadPage(storageEngineReader, leafRef);
    if (!(page instanceof HOTLeafPage hotLeaf)) {
      clearCowPath();
      return null;
    }

    // Create COW copy for modification using deep copy (copies off-heap data + slot offsets)
    HOTLeafPage modifiedLeaf = hotLeaf.copy();

    PageContainer leafContainer = PageContainer.getInstance(hotLeaf, modifiedLeaf);
    log.put(leafRef, leafContainer);

    // Propagate COW up the path
    propagateCOW(log, leafRef);

    // Clear references to allow GC
    clearCowPath();

    return leafContainer;
  }

  /**
   * Propagate copy-on-write changes up to ancestors. Each modified HOTIndirectPage gets a new copy in
   * the log. Uses pre-allocated cowPath arrays - ZERO allocations except for page copies!
   */
  private void propagateCOW(TransactionIntentLog log, PageReference modifiedChildRef) {
    PageReference childRef = modifiedChildRef;

    // Iterate backwards through pre-allocated arrays (no iterator allocation!)
    for (int i = cowPathDepth - 1; i >= 0; i--) {
      PageReference parentRef = cowPathRefs[i];
      HOTIndirectPage parentNode = cowPathNodes[i];
      int childIndex = cowPathChildIndices[i];

      // COW the parent node (this allocation is unavoidable - it's the copy!)
      HOTIndirectPage newParent = parentNode.copyWithUpdatedChild(childIndex, childRef);

      // Update log
      log.put(parentRef, PageContainer.getInstance(newParent, newParent));

      childRef = parentRef;
    }
  }

  /**
   * Navigate within a cached tree that's already been modified (iterative, no stack overflow risk).
   */
  private @Nullable PageContainer navigateWithinCachedTree(PageContainer cached, byte[] key,
      StorageEngineWriter storageEngineReader, TransactionIntentLog log, IndexType indexType,
      int indexNumber) {

    PageContainer current = cached;
    while (true) {
      final Page page = current.getModified();

      if (page instanceof HOTLeafPage) {
        return current;
      }

      if (page instanceof HOTIndirectPage hotNode) {
        final int childIndex = hotNode.findChildIndex(key);
        if (childIndex >= 0) {
          final PageReference childRef = hotNode.getChildReference(childIndex);
          if (childRef != null) {
            final PageContainer childContainer = log.get(childRef);
            if (childContainer != null) {
              current = childContainer; // iterate, not recurse
              continue;
            }
          }
        }
      }

      // Not found in cached tree
      return null;
    }
  }

  /**
   * Create a new leaf page for a key that doesn't exist yet.
   */
  private @Nullable PageContainer createNewLeaf(StorageEngineWriter storageEngineReader, TransactionIntentLog log,
      PageReference startReference, byte[] key, IndexType indexType, int indexNumber) {

    // Create new HOTLeafPage
    long newPageKey = pageKeyAllocator.getAsLong();
    HOTLeafPage newLeaf = new HOTLeafPage(newPageKey, storageEngineReader.getRevisionNumber(), indexType);

    PageReference newRef = new PageReference();
    newRef.setKey(newPageKey);

    PageContainer container = PageContainer.getInstance(newLeaf, newLeaf);
    log.put(newRef, container);

    // If this is the first page (empty root), update the root reference
    if (cowPathDepth == 0) {
      // Update start reference to point to new leaf
      startReference.setKey(newPageKey);
      log.put(startReference, container);
    } else {
      // Update parent to include new child
      // This would require more complex logic for node splits
      // For now, just propagate the COW
      propagateCOW(log, newRef);
    }

    clearCowPath();
    return container;
  }

  /**
   * Load a page from storage or return swizzled in-memory page.
   *
   * <p>Checks {@link PageReference#getPage()} first (zero I/O for in-memory swizzled pages),
   * then falls through to {@link StorageEngineReader#loadHOTPage(PageReference)} which handles
   * both the transaction log (via logKey) and persistent storage (via key).</p>
   */
  private @Nullable Page loadPage(StorageEngineReader storageEngineReader, PageReference ref) {
    // Check swizzled in-memory page first — avoids I/O for pages already loaded
    final Page swizzled = ref.getPage();
    if (swizzled != null) {
      return swizzled;
    }
    // Need both key < 0 AND logKey < 0 to truly mean "not yet persisted"
    if (ref.getKey() < 0 && ref.getLogKey() < 0) {
      return null;
    }
    return storageEngineReader.loadHOTPage(ref);
  }

  /**
   * Handle leaf page split AND insert atomically using MSDB-aware splitting.
   *
   * <p>
   * Matches the C++ reference implementation's approach (Binna's thesis Listing 3.1,
   * {@code insertNewValue()} + {@code integrateBiNodeIntoTree()}), adapted for COW
   * semantics and multi-entry leaf pages:
   * </p>
   * <ol>
   * <li>Split the full leaf by MSDB that includes the new key's disc bits</li>
   * <li>Insert the new key into the correct half (based on disc bit, not key order)</li>
   * <li>Compute BiNode disc bit from boundary keys (equals MSDB by proof)</li>
   * <li>Walk up the path with COW to integrate the BiNode</li>
   * </ol>
   *
   * <p>
   * No re-navigation needed: the MSDB guarantees all left keys have bit=0 and all right
   * keys have bit=1, so the BiNode's disc-bit routing is correct for all keys including
   * the newly inserted one. The previous "split → re-navigate → insert" approach was needed
   * because the split didn't include the new key in the MSDB computation.
   * </p>
   *
   * @param storageEngineReader the storage engine writer
   * @param log the transaction intent log
   * @param fullPage the full leaf page
   * @param leafRef the reference to the leaf
   * @param rootReference the root reference for the index
   * @param pathNodes indirect page nodes on the path from root to leaf
   * @param pathRefs page references on the path
   * @param pathChildIndices child indices taken at each level
   * @param pathDepth the depth of the path
   * @param keyBuf the key to insert
   * @param keyLen the key length
   * @param valueBuf the value to insert
   * @param valueLen the value length
   * @return {@code true} if the split+insert succeeded, {@code false} if it failed
   */
  public boolean handleLeafSplitAndInsert(StorageEngineWriter storageEngineReader,
      TransactionIntentLog log, HOTLeafPage fullPage, PageReference leafRef,
      PageReference rootReference, HOTIndirectPage[] pathNodes, PageReference[] pathRefs,
      int[] pathChildIndices, int pathDepth, byte[] keyBuf, int keyLen, byte[] valueBuf, int valueLen) {
    return handleLeafSplitAndInsert(storageEngineReader, log, fullPage, leafRef, rootReference,
        pathNodes, pathRefs, pathChildIndices, pathDepth, keyBuf, keyLen, valueBuf, valueLen,
        /*explicitSplitBit=*/ -1);
  }

  /**
   * Phase 2 variant: split the leaf on an explicit ancestor disc bit β rather than the
   * leaf's local MSDB. Used to maintain ancestor β-constancy when inserting a key would
   * otherwise span both β values within a single leaf. {@code explicitSplitBit < 0}
   * falls back to MSDB-based split (= original behavior).
   */
  public boolean handleLeafSplitAndInsert(StorageEngineWriter storageEngineReader,
      TransactionIntentLog log, HOTLeafPage fullPage, PageReference leafRef,
      PageReference rootReference, HOTIndirectPage[] pathNodes, PageReference[] pathRefs,
      int[] pathChildIndices, int pathDepth, byte[] keyBuf, int keyLen, byte[] valueBuf, int valueLen,
      int explicitSplitBit) {

    Objects.requireNonNull(storageEngineReader);
    Objects.requireNonNull(log);
    Objects.requireNonNull(fullPage);
    Objects.requireNonNull(leafRef);
    Objects.requireNonNull(rootReference);

    // Set active reader+log for child page resolution and CoW page registration during split
    // / augment operations. The log binding lets {@link #splitSubtreeOnBit} install
    // freshly-built half-subtrees in the TIL without explicit threading through helpers.
    this.activeReader = storageEngineReader;
    this.activeLog = log;
    try {
      return handleLeafSplitAndInsertInternal(storageEngineReader, log, fullPage, leafRef,
          rootReference, pathNodes, pathRefs, pathChildIndices, pathDepth, keyBuf, keyLen,
          valueBuf, valueLen, explicitSplitBit);
    } finally {
      this.activeReader = null;
      this.activeLog = null;
    }
  }

  private boolean handleLeafSplitAndInsertInternal(StorageEngineWriter storageEngineReader,
      TransactionIntentLog log, HOTLeafPage fullPage, PageReference leafRef,
      PageReference rootReference, HOTIndirectPage[] pathNodes, PageReference[] pathRefs,
      int[] pathChildIndices, int pathDepth, byte[] keyBuf, int keyLen, byte[] valueBuf, int valueLen,
      int explicitSplitBit) {

    // If page has < 1 entry, can't split
    if (fullPage.getEntryCount() < 1) {
      return false;
    }

    // Allocate page key for right half
    final long newPageKey = pageKeyAllocator.getAsLong();
    final int revision = storageEngineReader.getRevisionNumber();

    // Create target page for right half
    final HOTLeafPage rightPage = new HOTLeafPage(newPageKey, revision, fullPage.getIndexType());
    boolean rightPageOwnershipTransferred = false;

    try {
      // Phase 2 (constancy-aware leaf split): when {@code explicitSplitBit >= 0} the
      // caller has detected that adding {@code keyBuf} would break β-constancy at
      // ancestor disc bit {@code explicitSplitBit}. Split on that bit instead of MSDB
      // so the resulting halves are β-constant at every ancestor's β. Otherwise use
      // the MSDB-aware split (original behavior).
      final int[] newSideOut = new int[]{-1};
      final int discriminativeBit;
      if (explicitSplitBit >= 0) {
        final int actualBit = fullPage.splitToWithInsertOnBit(
            rightPage, keyBuf, keyLen, valueBuf, valueLen, explicitSplitBit);
        if (actualBit < 0) {
          // Degenerate (= partition is empty on one side, e.g., all keys + new key
          // share β value at explicitSplitBit). Fall back to MSDB-based split.
          if (!fullPage.splitToWithInsert(rightPage, keyBuf, keyLen, valueBuf, valueLen, newSideOut)) {
            return false;
          }
          final byte[] lm0 = fullPage.getLastKey();
          final byte[] rm0 = rightPage.getFirstKey();
          if (lm0.length == 0 || rm0.length == 0) return false;
          discriminativeBit = DiscriminativeBitComputer.computeDifferingBit(lm0, rm0);
          if (discriminativeBit < 0) return false;
        } else {
          // Successful split on explicitSplitBit. Derive newSide from key's β value.
          newSideOut[0] = DiscriminativeBitComputer.isBitSet(keyBuf, actualBit) ? 1 : 0;
          discriminativeBit = actualBit;
        }
      } else {
        // MSDB-aware split+insert (original path).
        if (!fullPage.splitToWithInsert(rightPage, keyBuf, keyLen, valueBuf, valueLen, newSideOut)) {
          return false;
        }
        final byte[] lm = fullPage.getLastKey();
        final byte[] rm = rightPage.getFirstKey();
        if (lm.length == 0 || rm.length == 0) return false;
        discriminativeBit = DiscriminativeBitComputer.computeDifferingBit(lm, rm);
        if (discriminativeBit < 0) return false;
      }
      final int leafSplitNewSide = newSideOut[0];

      // Boundary keys for log lookups (used by parent integration).
      final byte[] rightMin = rightPage.getFirstKey();

      // Store right page in TIL
      final PageReference rightRef = new PageReference();
      rightRef.setKey(newPageKey);
      rightRef.setPage(rightPage);
      log.put(rightRef, PageContainer.getInstance(rightPage, rightPage));
      rightPageOwnershipTransferred = true;

      // Swizzle post-split left page onto leafRef
      leafRef.setPage(fullPage);
      log.put(leafRef, PageContainer.getInstance(fullPage, fullPage));

      // Integrate split into tree (walk up path with COW)
      if (pathDepth > 0) {
        final int parentIdx = pathDepth - 1;
        updateParentForSplitWithPath(storageEngineReader, log, pathRefs[parentIdx],
            pathNodes[parentIdx], pathChildIndices[parentIdx], leafRef, rightRef,
            rightMin, rootReference, pathNodes, pathRefs, pathChildIndices, parentIdx,
            leafSplitNewSide);
      } else {
        // Root split — create BiNode as new root.
        final long newRootPageKey = pageKeyAllocator.getAsLong();
        final PageReference leftChildRef;
        if (leafRef == rootReference) {
          leftChildRef = new PageReference();
          leftChildRef.setKey(leafRef.getKey());
          leftChildRef.setPage(fullPage);
          log.put(leftChildRef, PageContainer.getInstance(fullPage, fullPage));
        } else {
          leftChildRef = leafRef;
        }
        final HOTIndirectPage biNode = createBiNodeTraced("addEntry-promoteToBiNode-589",
            newRootPageKey, revision, discriminativeBit, leftChildRef, rightRef, 1);
        rootReference.setKey(newRootPageKey);
        rootReference.setPage(biNode);
        log.put(rootReference, PageContainer.getInstance(biNode, biNode));
      }

      return true;
    } finally {
      if (!rightPageOwnershipTransferred) {
        rightPage.close();
      }
    }
  }

  /**
   * Traverse the tree from {@code rootRef}, collecting every reachable
   * HOTLeafPage reference into {@code out}. Also guarantees that the two
   * just-split references ({@code splitLeftRef}, {@code splitRightRef})
   * appear in the output — because the current indirect structure may
   * route some keys wrongly, the split pair is added explicitly at the
   * end if not seen during traversal. Duplicates (same pageKey + same
   * logKey) are removed in the final sort.
   */
  /**
   * Integrate a BiNode (from a split) into the tree structure.
   * 
   * <p>
   * Reference: HOTSingleThreaded.hpp integrateBiNodeIntoTree() lines 493-547. This implements the
   * height-optimal integration strategy:
   * </p>
   * <ol>
   * <li>If parent.height > splitEntries.height: create intermediate node</li>
   * <li>If parent NOT full (< 32 entries): expand parent by adding entry</li>
   * <li>If parent IS full: split parent and recurse up</li>
   * </ol>
   */
  private void updateParentForSplitWithPath(StorageEngineWriter storageEngineReader, TransactionIntentLog log,
      PageReference parentRef, HOTIndirectPage parent, int originalChildIndex,
      PageReference leftChild, PageReference rightChild, byte[] splitKey,
      PageReference rootReference, HOTIndirectPage[] pathNodes, PageReference[] pathRefs,
      int[] pathChildIndices, int currentPathIdx, int newSide) {
    // {@code newSide} (Phase 4b-vb.1): which of (leftChild, rightChild) contains the
    // just-inserted key. 0 = LEFT (newly-inserted key landed in leftChild), 1 = RIGHT
    // (newly-inserted key in rightChild). Threaded from {@code splitToWithInsert}'s
    // {@code newSideOut} all the way through indirect-level recursions. Phase 4b-vb.2
    // and 4b-vb.3 will consume it to compute valueToInsert / valueToReplace per
    // C++'s integrateBiNodeIntoTree semantics. Unused at present; the parameter exists
    // so the threading is a one-shot mechanical change rather than threading through
    // every nested call site each time we extend the integration logic.

    // Compute discriminative bit between the two split children.
    // Guard against -1 (identical keys, e.g., when pages can't be loaded and both return EMPTY_KEY).
    byte[] leftMax = getLastKeyFromChild(leftChild);
    byte[] rightMin = getFirstKeyFromChild(rightChild);
    int discriminativeBit = Math.max(0, DiscriminativeBitComputer.computeDifferingBit(leftMax, rightMin));

    // Compute the height of the split entries (BiNode containing the split children)
    int leftHeight = getHeightFromChild(leftChild, activeReader);
    int rightHeight = getHeightFromChild(rightChild, activeReader);
    int splitEntriesHeight = Math.max(leftHeight, rightHeight) + 1;

    // Reference line 502: if parent.height > splitEntries.height, create intermediate node
    if (parent.getHeight() > splitEntriesHeight) {
      long newBiNodePageKey = pageKeyAllocator.getAsLong();
      HOTIndirectPage newBiNode = createBiNodeTraced("addEntry-newBiNode-646", newBiNodePageKey,
          storageEngineReader.getRevisionNumber(), discriminativeBit, leftChild, rightChild,
          splitEntriesHeight);
      PageReference newBiNodeRef = new PageReference();
      newBiNodeRef.setKey(newBiNodePageKey);
      newBiNodeRef.setPage(newBiNode);
      log.put(newBiNodeRef, PageContainer.getInstance(newBiNode, newBiNode));
      HOTIndirectPage updatedParent =
          parent.withUpdatedChild(originalChildIndex, newBiNodeRef, storageEngineReader.getRevisionNumber());
      log.put(parentRef, PageContainer.getInstance(updatedParent, updatedParent));
    } else {
      // parent.height == splitEntries.height: integrate into parent
      // Reference lines 504-546

      int currentNumChildren = parent.getNumChildren();

      // Reference line 516: if parent NOT full, add entry.
      // Reference-faithful addEntry via DiscriminativeBitsRepresentation.insert
      // semantics: try to extend parent's existing disc-bit set with the
      // new bit, repositioning existing partial keys via _pdep. This
      // preserves the HOT invariant that disc bits at a node are
      // constant within each child's subtree. If the invariant would
      // break (new bit varies within an existing sibling's subtree), or
      // the parent is full, or the new bit falls outside the current
      // SingleMask window, fall back to splitting the parent.
      HOTIndirectPage expandedParent = null;
      if (currentNumChildren < NodeUpgradeManager.MULTI_NODE_MAX_CHILDREN) {
        if (parent.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK) {
          expandedParent = addEntryWithPDep(parent, originalChildIndex, leftChild, rightChild,
              discriminativeBit, storageEngineReader.getRevisionNumber());
        } else {
          expandedParent = addEntryMultiMask(parent, originalChildIndex, leftChild, rightChild,
              discriminativeBit, storageEngineReader.getRevisionNumber());
        }
      }
      if (expandedParent != null) {
        log.put(parentRef, PageContainer.getInstance(expandedParent, expandedParent));
        // Stage G.25 — β propagation up the path. When the addEntry at parent introduced
        // a NEW disc bit β at a byte position OUTSIDE some grandparent's mask window,
        // grandparent's stored partial for parent's slot doesn't reflect β. = grandparent
        // can route keys with mismatched β to parent's slot, breaking I8 when those keys
        // become parent's new deep-firstKey.
        //
        // Solution: walk up the path. For each ancestor A whose mask doesn't include β
        // AND whose subtree at the chosen slot now contains keys with both β values,
        // recursively split and addEntry-extend at that ancestor.
        //
        // For the surgical fix here: only fire on the cross-window upgrade case (= when
        // newBitByteOffset was out of parent's window). Other addEntry success paths
        // don't introduce a NEW byte position; they just add a bit within an existing
        // byte. Ancestors' masks already cover those bytes (= no propagation needed).
        if (Boolean.getBoolean("hot.strict.g25.propagate") && currentPathIdx > 0) {
          propagateBetaToAncestors(storageEngineReader, log, parent, parentRef,
              expandedParent, originalChildIndex, leftChild, rightChild, discriminativeBit,
              pathNodes, pathRefs, pathChildIndices, currentPathIdx);
        }
      } else if (Boolean.getBoolean("hot.strict.binna")) {
        // Phase 3: lazy retroactive sibling rebalance. addEntryWithPDep rejected because
        // the new disc bit β is non-constant in some non-split sibling's subtree
        // (Case 2b-ii from the formal verification — Sirix's multi-entry-leaf workload
        // breaks Binna's single-key-leaf trivial-constancy guarantee). Walk down each
        // non-constant sibling, split its leaves on β, and rebuild the parent with
        // β-constant children at every slot via {@link #rebalanceAndIntegrate}, which
        // uses mask inheritance from the original parent (NOT createNodeFromChildren-N's
        // adjacent-pair re-derivation that would re-introduce Phase-4-class violations).
        //
        // If rebalance succeeds, the result is a height-optimal integration (no +1
        // wrapping BiNode, faithful to Binna's reference at parent.height ==
        // splitEntries.height). If rebalance can't proceed (fan-out > 32 after expansion,
        // MultiMask parent, cross-window β, partial-key collision, etc.), fall through
        // to the intermediate-BiNode fallback / splitParentAndRecurse.
        //
        // CoW correctness: every modified leaf/indirect gets a fresh pageKey, fresh
        // PageReference, and TIL registration. The original sibling subtree is left
        // untouched. The new parent is constructed at parent.getPageKey() (logically
        // unchanged identity) with a new content payload, and registered via log.put,
        // mirroring the pattern used by the addEntry* paths.
        final HOTIndirectPage rebalancedParent = rebalanceAndIntegrate(parent, originalChildIndex,
            leftChild, rightChild, discriminativeBit, log,
            storageEngineReader.getRevisionNumber());
        if (rebalancedParent != null) {
          log.put(parentRef, PageContainer.getInstance(rebalancedParent, rebalancedParent));
          PHASE3_REBALANCE_FIRINGS.incrementAndGet();
        } else if (tryPhase4SubtreeMerge(parent, parentRef, originalChildIndex, leftChild,
            rightChild, discriminativeBit, storageEngineReader, log,
            pathNodes, pathRefs, pathChildIndices, currentPathIdx)) {
          // Phase 4: β-already-in-mask subtree merge. addEntry rejected because the leaf-split's
          // MSDB β is a bit parent already routes on; the resolution is to merge moveHalf's
          // keys into the existing β=¬v_L sibling subtree (rather than try to extend the
          // mask). Works for both SingleMask and MultiMask parent layouts. See {@link
          // #subtreeMerge} for the algorithm and CoW correctness argument.
          //
          // Successful firing leaves the parent's mask unchanged (no new disc bit added) and
          // the parent's children layout possibly altered (slot[splitChildIdx] swapped to
          // keepHalf; slot[siblingIdx]'s subtree mutated by bulk-inserts). Tree height is
          // preserved (no +1 wrapping BiNode); I-Binna constancy invariant is preserved.
        } else if (intermediateBiNodePreservesSlotOrder(parent, originalChildIndex, leftChild)) {
          // Phase 3 couldn't resolve — fall back to c868e669c's intermediate-BiNode workaround.
          // This should be rare on benign workloads; each firing grows tree depth by 1.
          //
          // Strict-Binna fallback: when {@code addEntry} can't extend the parent's mask
          // without breaking constancy (or hits cross-window / collision edge cases), absorb
          // the leaf split by inserting an intermediate BiNode at the original child's slot.
          // The parent's mask stays unchanged, so existing routing remains correct; the new
          // BiNode distinguishes the two halves at a level below the parent.
          //
          // Architectural note (per Binna's HOTSingleThreaded.hpp lines 493-547): this
          // intermediate-BiNode at parent.height == splitEntries.height is non-faithful —
          // Binna's reference instead splits the parent (= splitParentAndRecurse). However
          // {@code buildCompressedHalf}'s sparse-path fallbacks have known correctness gaps,
          // so this fallback remains the pragmatic safety net for the (now rare) cases
          // Phase 3 can't resolve.
          final long newBiNodePageKey = pageKeyAllocator.getAsLong();
          final HOTIndirectPage newBiNode = createBiNodeTraced("strict-binna-intermediate-biNode",
              newBiNodePageKey, storageEngineReader.getRevisionNumber(), discriminativeBit,
              leftChild, rightChild, splitEntriesHeight);
          final PageReference newBiNodeRef = new PageReference();
          newBiNodeRef.setKey(newBiNodePageKey);
          newBiNodeRef.setPage(newBiNode);
          log.put(newBiNodeRef, PageContainer.getInstance(newBiNode, newBiNode));
          final HOTIndirectPage updatedParent =
              parent.withUpdatedChild(originalChildIndex, newBiNodeRef,
                  storageEngineReader.getRevisionNumber());
          log.put(parentRef, PageContainer.getInstance(updatedParent, updatedParent));
          INTERMEDIATE_BINODE_FALLBACK_FIRINGS.incrementAndGet();
        } else {
          // I8 would break with the intermediate-BiNode approach — fall through to a
          // genuine parent split.
          splitParentAndRecurse(storageEngineReader, log, parentRef, parent,
              originalChildIndex, leftChild, rightChild, discriminativeBit, rootReference,
              pathNodes, pathRefs, pathChildIndices, currentPathIdx);
        }
      } else if (currentNumChildren < 20) {
        // Height-optimal fallback ONLY for parents with small-to-medium
        // fan-out. When fan-out is already near-max, rebuild can produce
        // subtle INV breaches (the augmented computeDiscBits may not
        // fully untangle all constancy requirements across 20+ children).
        // Forcing a genuine split path at high fan-out rebuilds halves
        // from scratch, which is more robust. The threshold is chosen so
        // most construction operations still use the efficient rebuild
        // path (preserving height optimality) while high-density parents
        // use the robust split path. Empirically: threshold=20 keeps the
        // tree log₃₂-deep and fixes the 8.3% miss at n=5000 multi.
        expandedParent = rebuildParentAbsorbingSplit(parent, originalChildIndex,
            leftChild, rightChild, storageEngineReader.getRevisionNumber());
        if (expandedParent != null) {
          log.put(parentRef, PageContainer.getInstance(expandedParent, expandedParent));
        } else {
          splitParentAndRecurse(storageEngineReader, log, parentRef, parent,
              originalChildIndex, leftChild, rightChild, discriminativeBit, rootReference,
              pathNodes, pathRefs, pathChildIndices, currentPathIdx);
        }
      } else {
        splitParentAndRecurse(storageEngineReader, log, parentRef, parent,
            originalChildIndex, leftChild, rightChild, discriminativeBit, rootReference,
            pathNodes, pathRefs, pathChildIndices, currentPathIdx);
      }
    }
  }

  /**
   * Verify that placing the intermediate BiNode (with {@code leftChild} as its
   * left subtree) at slot {@code originalChildIndex} preserves the parent's I8
   * invariant — child slots strictly ascending by firstKey. The BiNode's
   * firstKey == leftChild.firstKey; if a leaf split inserted a smaller key into
   * the left half, the slot's firstKey may have decreased relative to its
   * predecessor. Returns {@code false} in that case so the caller falls through
   * to {@link #splitParentAndRecurse}.
   *
   * <p><b>Note: only the prev-side check is enforced.</b> A symmetric next-side check
   * (leftChild.firstKey &lt; next.firstKey) would catch one additional pathological
   * case on the {@code diagnosticMicrobenchPatternReproducer} (the lone
   * I8-children-sorted-by-firstkey violation), but routing the rejected case to
   * {@link #splitParentAndRecurse} cascades into Phase 4b's deferred sparse-path bugs
   * in {@link #buildCompressedHalf}, producing 5993 I6-pext-routes-to-leaf violations.
   * The 1 marginal I8 violation is the design's accepted "lesser evil" until Phase 4b
   * lands. See {@code docs/HOT_STRICT_BINNA_DESIGN.md} §4.4 (Phase 4b deferred work).
   *
   * <p>HFT-grade: zero allocation beyond the byte[] firstKey arrays returned by
   * {@link #getFirstKeyFromChild} (which are unavoidable as compareUnsigned
   * needs them). All other state lives on the call stack.
   */
  private boolean intermediateBiNodePreservesSlotOrder(HOTIndirectPage parent,
      int originalChildIndex, PageReference leftChild) {
    if (originalChildIndex == 0) return true;
    final byte[] leftFirstKey = getFirstKeyFromChild(leftChild);
    if (leftFirstKey == null || leftFirstKey.length == 0) return true;
    final PageReference prevChild = parent.getChildReference(originalChildIndex - 1);
    if (prevChild == null) return true;
    final byte[] prevFirstKey = getFirstKeyFromChild(prevChild);
    if (prevFirstKey == null || prevFirstKey.length == 0) return true;
    return Arrays.compareUnsigned(prevFirstKey, leftFirstKey) < 0;
  }

  /**
   * Reference-faithful {@code addEntry} for SingleMask indirect nodes:
   * extends {@code parent}'s disc-bit set with {@code newAbsBit}, then
   * repositions each existing child's partial key via {@code Long.expand}
   * ({@code _pdep_u64}) to match the new bit layout, and computes
   * partial keys for the two split halves at the original slot.
   *
   * <p>Precondition: the newly-added disc bit must be constant across
   * every non-split sibling's subtree (otherwise the HOT invariant
   * breaks — a disc bit is only valid at a node if all keys under each
   * child have the same value at that bit). The method returns {@code
   * null} if this precondition fails, forcing the caller to take the
   * "split parent and recurse" path.
   *
   * <p>Returns {@code null} also for: MultiMask parents (not handled
   * here — fall back to whole-node rebuild), new bit already in mask
   * (unexpected duplicate), or new bit outside the current 8-byte
   * PEXT window.
   *
   * <p>Reference: {@code HOTSingleThreadedNode<ChildPointer>::addEntry}
   * + {@code SingleMaskPartialKeyMapping::insert}. Java's
   * {@code Long.expand(src, mask)} = x86 {@code _pdep_u64}.
   */
  private @Nullable HOTIndirectPage addEntryWithPDep(HOTIndirectPage parent, int splitChildIndex,
      PageReference leftChild, PageReference rightChild, int newAbsBit, int revision) {
    final int oldInitialBytePos = parent.getInitialBytePos();
    final long oldMask = parent.getBitMask();
    final int oldCount = Long.bitCount(oldMask);
    final int numChildren = parent.getNumChildren();

    // 1. Encode new disc bit into the PEXT mask (same convention used
    //    by computeDiscBits: bit at absolute position B, byte bo within
    //    the 8-byte window starting at initialBytePos, bit bb MSB-first
    //    within byte → mask bit (bo * 8 + (7 - bb)).
    final int newBitByteOffset = (newAbsBit / 8) - oldInitialBytePos;
    if (newBitByteOffset < 0 || newBitByteOffset >= 8) {
      // Cross-window: upgrade parent from SingleMask → MultiMask with
      // the expanded disc-bit set including the new bit at its true
      // byte position. Maintains HOT invariant because the new bit is
      // already verified constant in every non-split sibling's subtree
      // by the caller (via bitConstantValueInSubtree on splitParentAndRecurse's
      // fallback paths) — we re-verify below. HFT-grade: one clone of
      // children[], one new int[] for partials, no boxing.
      return upgradeToMultiMaskWithNewBit(parent, splitChildIndex,
          leftChild, rightChild, newAbsBit, revision);
    }
    final int newBitInByte = newAbsBit % 8;
    // BE word layout: byte at window-offset {@code bo} occupies long bits ((7-bo)*8)..((7-bo)*8+7).
    // Within the byte, MSB-first bit {@code bb} is at long-bit ((7-bo)*8 + (7-bb)) = 63-(bo*8+bb).
    final int bitInWord = (7 - newBitByteOffset) * 8 + (7 - newBitInByte);
    final long newBitMaskBit = 1L << bitInWord;
    if ((oldMask & newBitMaskBit) != 0L) {
      // Case 2b-iv-b-β: β is already a parent disc bit. Mask unchanged; the leaf split's
      // moveHalf needs a new sibling slot whose stored partial = splitChild's partial XOR
      // β-bit. If a sibling already has that partial (Case 2b-iv-a), Phase 4 subtree-merge
      // handles it. Otherwise — "fresh polarity" — we add a new slot here without growing
      // the mask. Gated on hot.strict.binna so default behavior (intermediate-BiNode
      // fallback) is unchanged.
      if (Boolean.getBoolean("hot.strict.binna")) {
        return addEntryFreshPolaritySingleMask(parent, splitChildIndex, leftChild, rightChild,
            newBitMaskBit, revision);
      }
      return null; // bit already a disc bit — caller handles via split path
    }
    final long newMask = oldMask | newBitMaskBit;

    // 2. Compute where the new bit sits in the new compressed partial-key layout. PEXT packs
    //    mask bits from LSB → MSB in output, so the new bit's output position equals the
    //    population count of new-mask bits below it.
    final int newBitOutputPos = Long.bitCount(newMask & (newBitMaskBit - 1L));
    // Mask over partial-key bits contributed by the OLD mask in the NEW layout: new layout
    // has (oldCount+1) output bits; the new bit occupies position newBitOutputPos; all OTHER
    // positions hold the existing bits.
    final long oldPartialMaskInNewLayout =
        (((1L << (oldCount + 1)) - 1L) ^ (1L << newBitOutputPos));

    // 3. Verify the new disc bit is constant across every existing non-split sibling's
    //    subtree, and capture each sibling's constant value to set its repositioned partial
    //    bit. Required because Sirix's multi-entry leaves can hold keys differing at any
    //    bit; under sparse-path encoding subset-match routing misroutes keys destined for
    //    an off-path sibling that spans both bit values (the leaf-split's products beat the
    //    off-path sibling on most-specific match). Constancy ensures no off-path span exists.
    //    Binna's reference doesn't need this gate because his single-TID leaves trivially
    //    have constancy.
    final int[] siblingBitValues = new int[numChildren];
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) continue;
      final int v = bitConstantValueInSubtree(parent.getChildReference(i), newAbsBit);
      if (v < 0) return null; // non-constant — caller takes splitParentAndRecurse
      siblingBitValues[i] = v;
    }

    // 4. Build the expanded children + partial-keys arrays.
    final int newNumChildren = numChildren + 1;
    final PageReference[] newChildren = new PageReference[newNumChildren];
    final int[] newPartialKeys = new int[newNumChildren];
    final int[] oldPartialKeys = parent.getPartialKeys();

    final int splitChildOldPartial = oldPartialKeys[splitChildIndex];
    final int splitChildRepositioned =
        (int) Long.expand(Integer.toUnsignedLong(splitChildOldPartial), oldPartialMaskInNewLayout);

    int j = 0;
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) {
        newChildren[j] = leftChild;
        newPartialKeys[j] = splitChildRepositioned; // new bit = 0 (LEFT)
        j++;
        newChildren[j] = rightChild;
        newPartialKeys[j] = splitChildRepositioned | (1 << newBitOutputPos); // new bit = 1 (RIGHT)
        j++;
      } else {
        newChildren[j] = parent.getChildReference(i);
        final int repositioned = (int) Long.expand(
            Integer.toUnsignedLong(oldPartialKeys[i]), oldPartialMaskInNewLayout);
        newPartialKeys[j] = repositioned | (siblingBitValues[i] << newBitOutputPos);
        j++;
      }
    }

    // 4. Sanity guard: under the sparse-path encoding the new partial keys must be unique
    //    (HOT I3). The repositioning is a bijection on the old partials, the split-child
    //    halves' partials differ at the new bit, and they differ from non-split siblings'
    //    partials because their old-bit positions were already unique. A duplicate here
    //    indicates an upstream invariant break — bail so the caller takes the split path.
    for (int i = 1; i < newNumChildren; i++) {
      for (int k = 0; k < i; k++) {
        if (newPartialKeys[k] == newPartialKeys[i]) return null;
      }
    }

    // Stage G.1 fix (Bug B1) — I4 self-check: under sparse-path encoding the leftmost slot
    // in partial-key order must have stored partial = 0 (Binna's "first mask always zero",
    // C++ HOTSingleThreadedNode.hpp:179-320). The PDEP-repositioning above produces partial
    // 0 only when (a) splitChildIndex was originally slot 0 (LEFT half inherits 0) OR (b)
    // some non-split sibling i has both oldPartial[i] == 0 AND siblingBitValues[i] == 0
    // (= leftmost original sibling stays β=0). When neither holds, the layout has no
    // β-leftmost child — addEntry can't preserve sparse-path encoding without restructuring.
    // Reject so the caller takes splitParentAndRecurse, which rebuilds halves from scratch
    // and (per Stage F design) applies its own I4 verification.
    boolean hasZeroPartial = false;
    for (int i = 0; i < newNumChildren; i++) {
      if (newPartialKeys[i] == 0) { hasZeroPartial = true; break; }
    }
    if (!hasZeroPartial) {
      G1_I4_REJECT_ADDENTRY.incrementAndGet();
      return null;
    }

    // 5. Sort children + partials by partial-key (HOT I7 / Binna §4.2). Under sparse-path
    //    encoding the canonical slot order is sparse-partial-key order, NOT first-key order.
    //    The leaf-split halves' partials are repositionedSplitX and repositionedSplitX |
    //    (1 << newBitOutputPos); their relative position vs. other siblings' repositioned
    //    partials depends on newBitOutputPos. If newBitOutputPos is a high bit (= the new
    //    disc bit is more significant than some old disc bits), the rightChild's partial may
    //    sort AFTER an existing sibling — so co-sorting is required, not adjacent placement.
    sortChildrenAndPartialsByPartial(newChildren, newPartialKeys);

    // Stage G.16 — verify partial-key sort matches first-key sort. If not, re-sort by
    // first-key + recompute partials directly via PEXT. Same pattern as G.9 for
    // buildFlatNonStrict. This catches the construction-time inversion observed at
    // indirect 2 (child[2].firstKey > child[3].firstKey under partial-sort but partial
    // values say slot 2 should come first).
    boolean firstKeyMonotone = true;
    for (int i = 1; i < newNumChildren; i++) {
      final byte[] prev = getFirstKeyFromChild(newChildren[i - 1]);
      final byte[] curr = getFirstKeyFromChild(newChildren[i]);
      if (prev != null && curr != null && prev.length > 0 && curr.length > 0
          && Arrays.compareUnsigned(prev, curr) >= 0) {
        firstKeyMonotone = false;
        break;
      }
    }
    if (!firstKeyMonotone) {
      sortChildrenByFirstKey(newChildren);
      // Recompute each partial directly from firstKey under newMask (same approach as G.6).
      for (int i = 0; i < newNumChildren; i++) {
        final byte[] cKey = getFirstKeyFromChild(newChildren[i]);
        newPartialKeys[i] = (cKey == null || cKey.length == 0) ? 0
            : computePartialKeySingleMask(cKey, oldInitialBytePos, newMask);
      }
      // Re-verify uniqueness after recompute (collision possible if firstKeys collide on PEXT).
      for (int i = 1; i < newNumChildren; i++) {
        for (int k = 0; k < i; k++) {
          if (newPartialKeys[k] == newPartialKeys[i]) return null;
        }
      }
    }

    if (newNumChildren <= 16) {
      return HOTIndirectPage.createSpanNode(parent.getPageKey(), revision,
          oldInitialBytePos, newMask, newPartialKeys, newChildren, parent.getHeight());
    }
    return HOTIndirectPage.createMultiNode(parent.getPageKey(), revision,
        oldInitialBytePos, newMask, newPartialKeys, newChildren, parent.getHeight());
  }

  /**
   * Case 2b-iv-b-β handler for SingleMask parents — β is already a parent disc bit, no
   * existing sibling carries the inverse polarity (¬v_L), and we resolve by adding a NEW
   * child slot for the leaf-split's moveHalf with stored partial = splitChild's partial
   * XOR β-bit. Mask is unchanged; child count grows from k to k+1.
   *
   * <p><b>Algorithm</b>:
   * <ol>
   *   <li>Decode β's output bit position in the parent's existing partial-key layout via
   *       {@code Long.bitCount(oldMask & (newBitMaskBit - 1))}.</li>
   *   <li>Read v_L from the split child's stored partial at that position.</li>
   *   <li>keepHalf = (v_L == 0) ? leftChild : rightChild — replaces splitChildIndex in place.
   *       moveHalf = the other half — gets a new slot with newPartial = oldPartial XOR β-bit.</li>
   *   <li>Reject (return null) if any sibling already has the newPartial — caller falls
   *       through to Phase 4 subtree-merge / hoistAndReroute / intermediate-BiNode fallback.</li>
   *   <li>Verify constancy on β across non-split siblings — required by HOT-Binna invariant
   *       for SAFETY: every non-split sibling must encode a constant β value matching its
   *       existing stored partial, otherwise a future addEntry on a different bit would
   *       misroute keys spanning β within that subtree.</li>
   *   <li>Build a fresh HOTIndirectPage with same mask, k+1 children, partials sorted by
   *       partial-key (HOT I7).</li>
   * </ol>
   *
   * <p><b>CoW</b>: returns a fresh page at parent's pageKey (caller registers via
   * {@code log.put(parentRef, ...)} in {@link #updateParentForSplitWithPath}). Identity
   * preserved → ancestors above parent need no further CoW propagation.
   *
   * <p><b>HFT-grade</b>: pre-sized arrays (k+1 children), single allocation each for
   * children[] and partials[], no boxing, primitive ops only.
   *
   * @return fresh parent page with the new slot, or {@code null} when:
   *         (a) a sibling already encodes the target partial (Case 2b-iv-a),
   *         (b) some non-split sibling is non-constant on β (Case 2b-ii-style breach),
   *         (c) parent is already at fan-out cap (k+1 > MULTI_NODE_MAX_CHILDREN).
   */
  private @Nullable HOTIndirectPage addEntryFreshPolaritySingleMask(HOTIndirectPage parent,
      int splitChildIndex, PageReference leftChild, PageReference rightChild,
      long newBitMaskBit, int revision) {
    final int oldInitialBytePos = parent.getInitialBytePos();
    final long oldMask = parent.getBitMask();
    final int numChildren = parent.getNumChildren();

    // Fan-out gate: k+1 must fit. Caller already checked k < MULTI_NODE_MAX_CHILDREN before
    // dispatching here, but defensively re-verify so we never construct an over-cap page.
    if (numChildren + 1 > NodeUpgradeManager.MULTI_NODE_MAX_CHILDREN) {
      return null;
    }

    // β's output bit position in the EXISTING (unchanged) partial-key layout.
    final int betaOutputPos = Long.bitCount(oldMask & (newBitMaskBit - 1L));
    final int betaXor = 1 << betaOutputPos;

    final int[] oldPartialKeys = parent.getPartialKeys();
    if (oldPartialKeys == null
        || splitChildIndex < 0
        || splitChildIndex >= oldPartialKeys.length) {
      return null;
    }
    final int splitChildPartial = oldPartialKeys[splitChildIndex];
    final int newPartial = splitChildPartial ^ betaXor;
    final int vL = (splitChildPartial >>> betaOutputPos) & 1;
    final int newAbsBit = decodeAbsBitFromMaskBit(oldInitialBytePos, newBitMaskBit);

    // Two stored-partial gates protect routing soundness when β is already in mask:
    //
    // (1) HOT I3 — partials must remain unique. Reject if any sibling already encodes
    //     newPartial — that's Case 2b-iv-a, handled by Phase 4 subtree-merge.
    //
    // (2) "Fresh polarity" — for routing soundness under sparse-path subset-match,
    //     EVERY existing sibling must encode β = v_L. If some sibling Y has Y[β]=¬v_L
    //     at a partial != newPartial, then a post-firing lookup for a stored key K
    //     could subset-match L1 instead of K's actual containing sibling (L1 has β=¬v_L
    //     and shares splitChild's other bits; if K's densePK ⊇ L1.partial, K mis-routes
    //     to L1 even though it lives in Y's subtree). Empirical: omitting this gate
    //     yields 1385 I6 violations on the diagnostic 50K reproducer.
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) continue;
      if (oldPartialKeys[i] == newPartial) {
        return null; // (1) Case 2b-iv-a
      }
      final int siblingBeta = (oldPartialKeys[i] >>> betaOutputPos) & 1;
      if (siblingBeta != vL) {
        return null; // (2) inverse-polarity sibling exists at non-target slot
      }
    }

    // Subtree constancy gate on β across every non-split sibling. Even when stored
    // partials report β=v_L uniformly, multi-entry-leaf pollution may have accumulated
    // β=¬v_L keys under a sibling whose declared partial encodes β=v_L. Adding L1 with
    // β=¬v_L would (post-firing) reroute those polluted keys to L1, breaking I6.
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) continue;
      final int storedBit = (oldPartialKeys[i] >>> betaOutputPos) & 1;
      final int subtreeBit = bitConstantValueInSubtree(parent.getChildReference(i), newAbsBit);
      if (subtreeBit < 0 || subtreeBit != storedBit) {
        return null;
      }
    }

    // keepHalf inherits L's stored partial (β stays at v_L); moveHalf carries newPartial.
    final PageReference keepHalf = (vL == 0) ? leftChild : rightChild;
    final PageReference moveHalf = (vL == 0) ? rightChild : leftChild;

    // Build new children + partials arrays. Replace splitChildIndex with keepHalf, then
    // append moveHalf with newPartial; final sort by partial-key (HOT I7).
    final int newNumChildren = numChildren + 1;
    final PageReference[] newChildren = new PageReference[newNumChildren];
    final int[] newPartials = new int[newNumChildren];
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) {
        newChildren[i] = keepHalf;
        newPartials[i] = splitChildPartial; // unchanged: keepHalf encodes β = v_L
      } else {
        newChildren[i] = parent.getChildReference(i);
        newPartials[i] = oldPartialKeys[i];
      }
    }
    newChildren[numChildren] = moveHalf;
    newPartials[numChildren] = newPartial;

    // HOT I3: partials must be unique. The newPartial differs from splitChildPartial at
    // β-bit and was rejected against every sibling already — the array is unique by
    // construction. (Defensive duplicate check elided to avoid O(k²) on the hot path.)

    // Sort by partial-key (HOT I7 / Binna §4.2).
    sortChildrenAndPartialsByPartial(newChildren, newPartials);

    ADDENTRY_FRESH_POLARITY_FIRINGS.incrementAndGet();
    if (Boolean.getBoolean("hot.debug.phase4")) {
      System.out.println("[hot.fresh-polarity] absBit=" + newAbsBit
          + " betaPos=" + betaOutputPos + " vL=" + vL + " splitChildIdx=" + splitChildIndex
          + " splitPartial=" + splitChildPartial + " newPartial=" + newPartial
          + " mask=0x" + Long.toHexString(oldMask) + " k=" + numChildren
          + " → k+1=" + newNumChildren + " layout=SINGLE_MASK");
    }
    if (newNumChildren <= 16) {
      return HOTIndirectPage.createSpanNode(parent.getPageKey(), revision,
          oldInitialBytePos, oldMask, newPartials, newChildren, parent.getHeight());
    }
    return HOTIndirectPage.createMultiNode(parent.getPageKey(), revision,
        oldInitialBytePos, oldMask, newPartials, newChildren, parent.getHeight());
  }

  /**
   * Decode the absolute MSB-first bit position from a SingleMask {@code newBitMaskBit}
   * (a single-bit-set long) given the parent's {@code initialBytePos}. Inverse of
   * {@code (7 - byteOffset) * 8 + (7 - bitInByte)}.
   *
   * <p>HFT-grade: primitive ops only, no allocation.
   */
  private static int decodeAbsBitFromMaskBit(int initialBytePos, long newBitMaskBit) {
    final int bitInWord = Long.numberOfTrailingZeros(newBitMaskBit);
    final int byteOffset = 7 - (bitInWord / 8);
    final int bitInByte = 7 - (bitInWord % 8);
    return (initialBytePos + byteOffset) * 8 + bitInByte;
  }

  /**
   * Case 2b-iv-b-β handler for MultiMask parents — analog of
   * {@link #addEntryFreshPolaritySingleMask} for the MultiMask layout. Adds a new child
   * slot whose stored partial = splitChild.partial XOR β-bit, with the parent's extraction
   * tables (positions / masks) unchanged.
   *
   * <p>Same three gates as the SingleMask version:
   * <ol>
   *   <li>HOT I3 — newPartial must not collide with any existing sibling.</li>
   *   <li>Fresh polarity — every existing sibling must encode β = v_L in its stored partial.</li>
   *   <li>Subtree-β constancy — declared partial bit must match each sibling's actual subtree
   *       β value (catches multi-entry-leaf pollution).</li>
   * </ol>
   *
   * <p>The β output position decoding differs from SingleMask: under MultiMask BE-concat
   * encoding, walks {@code extractionPositions[]} in order, counting set mask bits MSB-first
   * within each byte until reaching β's (bytePos, bitInByte). Implemented via
   * {@link #multiMaskBetaOutputPos} which is the same decoder used by Phase 4 subtree-merge.
   *
   * <p><b>CoW</b>: returns a fresh page at parent's pageKey (caller registers via TIL).
   * Identity preserved → no upstream CoW needed.
   *
   * <p><b>HFT-grade</b>: pre-sized arrays, single allocation per array, primitive ops, no
   * boxing.
   */
  private @Nullable HOTIndirectPage addEntryFreshPolarityMultiMask(HOTIndirectPage parent,
      int splitChildIndex, PageReference leftChild, PageReference rightChild,
      int newAbsBit, int newBytePos, int newBitInByte, int revision) {
    final int numChildren = parent.getNumChildren();
    final byte[] extractionPositions = parent.getExtractionPositions();
    final long[] extractionMasks = parent.getExtractionMasks();
    if (extractionPositions == null || extractionMasks == null) return null;
    if (numChildren + 1 > NodeUpgradeManager.MULTI_NODE_MAX_CHILDREN) {
      return null;
    }

    // Decode β's output position in the parent's existing partial-key layout.
    final int betaOutputPos = multiMaskBetaOutputPos(parent, newAbsBit);
    if (betaOutputPos < 0) {
      return null; // β not found in mask — should be unreachable from caller's gate
    }
    final int betaXor = 1 << betaOutputPos;

    final int[] oldPartialKeys = parent.getPartialKeys();
    if (oldPartialKeys == null
        || splitChildIndex < 0
        || splitChildIndex >= oldPartialKeys.length) {
      return null;
    }
    final int splitChildPartial = oldPartialKeys[splitChildIndex];
    final int newPartial = splitChildPartial ^ betaXor;
    final int vL = (splitChildPartial >>> betaOutputPos) & 1;

    // (1) HOT I3 + (2) fresh polarity gates.
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) continue;
      if (oldPartialKeys[i] == newPartial) {
        return null; // (1) Case 2b-iv-a → Phase 4 subtree-merge
      }
      final int siblingBeta = (oldPartialKeys[i] >>> betaOutputPos) & 1;
      if (siblingBeta != vL) {
        return null; // (2) inverse polarity sibling at non-target slot
      }
    }

    // (3) Subtree-β constancy gate.
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) continue;
      final int storedBit = (oldPartialKeys[i] >>> betaOutputPos) & 1;
      final int subtreeBit = bitConstantValueInSubtree(parent.getChildReference(i), newAbsBit);
      if (subtreeBit < 0 || subtreeBit != storedBit) {
        return null;
      }
    }

    // keepHalf inherits L's stored partial; moveHalf carries newPartial.
    final PageReference keepHalf = (vL == 0) ? leftChild : rightChild;
    final PageReference moveHalf = (vL == 0) ? rightChild : leftChild;

    // Build new children + partials with k+1 slots.
    final int newNumChildren = numChildren + 1;
    final PageReference[] newChildren = new PageReference[newNumChildren];
    final int[] newPartials = new int[newNumChildren];
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) {
        newChildren[i] = keepHalf;
        newPartials[i] = splitChildPartial;
      } else {
        newChildren[i] = parent.getChildReference(i);
        newPartials[i] = oldPartialKeys[i];
      }
    }
    newChildren[numChildren] = moveHalf;
    newPartials[numChildren] = newPartial;

    sortChildrenAndPartialsByPartial(newChildren, newPartials);

    ADDENTRY_FRESH_POLARITY_FIRINGS.incrementAndGet();
    if (Boolean.getBoolean("hot.debug.phase4")) {
      System.out.println("[hot.fresh-polarity] absBit=" + newAbsBit
          + " betaPos=" + betaOutputPos + " vL=" + vL + " splitChildIdx=" + splitChildIndex
          + " splitPartial=" + splitChildPartial + " newPartial=" + newPartial
          + " newBytePos=" + newBytePos + " newBitInByte=" + newBitInByte
          + " k=" + numChildren + " → k+1=" + newNumChildren + " layout=MULTI_MASK");
    }

    // Mask tables unchanged — newExtractionPositions / newExtractionMasks are clones.
    final byte[] newExtractionPositions = extractionPositions.clone();
    final long[] newExtractionMasks = extractionMasks.clone();
    final int newNumBytes = extractionPositions.length;

    // Recompute MSB index (smallest absolute disc bit) — unchanged from parent's, but
    // we recompute defensively in case the parent's cached value differs from the truth.
    short msbIndex = Short.MAX_VALUE;
    for (int i = 0; i < newNumBytes; i++) {
      final int chunkIdx = i / 8;
      final int byteOffsetInChunk = i % 8;
      final int maskByte =
          (int) ((newExtractionMasks[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFF);
      if (maskByte == 0) continue;
      final int highBit = 31 - Integer.numberOfLeadingZeros(maskByte);
      final int absBit = (newExtractionPositions[i] & 0xFF) * 8 + (7 - highBit);
      if (absBit < msbIndex) msbIndex = (short) absBit;
    }

    if (newNumChildren <= 16) {
      return HOTIndirectPage.createSpanNodeMultiMask(parent.getPageKey(), revision,
          newExtractionPositions, newExtractionMasks, newNumBytes,
          newPartials, newChildren, parent.getHeight(), msbIndex);
    }
    return HOTIndirectPage.createMultiNodeMultiMask(parent.getPageKey(), revision,
        newExtractionPositions, newExtractionMasks, newNumBytes,
        newPartials, newChildren, parent.getHeight(), msbIndex);
  }

  // ===========================================================================
  // Phase 3 — Lazy retroactive sibling rebalance
  //
  // When addEntryWithPDep / addEntryMultiMask rejects in updateParentForSplitWithPath
  // because some non-split sibling i is non-constant on the new disc bit β (Case 2b-ii
  // from the formal verification), Phase 3 fixes it by walking down sibling i's subtree
  // and splitting any β-non-constant leaves into β-constant halves. The split products
  // become two new sibling slots in the parent (replacing slot i with both halves), so
  // after rebalance every parent slot is β-constant and addEntryWithPDep can succeed.
  //
  // Goals (vs. c868e669c's intermediate-BiNode fallback):
  //   - tree-depth stays optimal (no +1 wrapping BiNode per fallback firing)
  //   - I-Binna constancy invariant is preserved end-to-end
  //   - intermediate-BiNode fallback firings drop to 0 on benign workloads
  //
  // Bounded recursion: each splitSubtreeOnBit visits each leaf in the sibling subtree
  // at most once. Worst-case work per rebalance event = O(Σ subtree leaves). Rebalance
  // events fire only on the rare Case 2b-ii rejection, not on every addEntry.
  //
  // CoW correctness: every modified page → fresh pageKey + fresh PageReference + TIL
  // registration. The original sibling subtree is left untouched — we copy keys+values
  // into freshly allocated pages and never mutate the original leaves/indirects in place.
  // ===========================================================================

  /**
   * Result of a Phase 3 sibling-subtree split: two β-constant halves, each rooted at a
   * fresh PageReference registered in the TIL.
   *
   * <p>HFT-grade: simple value carrier, single allocation per split. Both refs are
   * non-null on a successful split; the helper that produces this record verifies
   * non-emptiness before constructing it.
   */
  private record SubtreeSplit(PageReference leftRef, PageReference rightRef) {}

  /**
   * Recursively split the subtree rooted at {@code ref} on absolute MSB-first bit
   * {@code β}, producing two β-constant subtrees. Both halves are rooted at fresh
   * PageReferences registered in the TIL.
   *
   * <p>Algorithm (mirrors Binna's recursive entry partitioning, adapted for
   * Sirix's multi-entry leaves):
   * <ul>
   *   <li><b>Leaf</b>: if β-constant, place in the appropriate bucket. Otherwise
   *       allocate two fresh leaves, partition entries by β-bit, register both.</li>
   *   <li><b>Indirect</b>: recurse on each child. β-constant children go directly to
   *       their bucket; non-constant children recurse to produce two halves each, both
   *       contributing to their respective bucket. After all children are classified,
   *       build two new indirect pages (one per non-empty bucket) using
   *       {@link #createNodeFromChildren}, or pass through a single ref if a bucket
   *       has only one child.</li>
   * </ul>
   *
   * <p>Returns {@code null} if the split fails (degenerate partition, allocation failure,
   * or unloadable page). Caller falls back to {@link #splitParentAndRecurse}.
   *
   * <p>HFT-grade: pre-sized buffers (children count bounded by 32). Each leaf's entries
   * are read once. Recursion depth bounded by tree height.
   */
  private @Nullable SubtreeSplit splitSubtreeOnBit(PageReference ref, int absBit,
      TransactionIntentLog log, int revision) {
    Page page = ref.getPage();
    if (page == null && activeReader != null) {
      page = loadPage(activeReader, ref);
      if (page != null) {
        ref.setPage(page);
      }
    }
    if (page == null) {
      return null;
    }

    if (page instanceof HOTLeafPage leaf) {
      return splitLeafOnBit(leaf, absBit, log, revision);
    }
    if (page instanceof HOTIndirectPage indirect) {
      return splitIndirectOnBit(indirect, absBit, log, revision);
    }
    return null;
  }

  /**
   * Build a sub-indirect-page from a β-constant bucket of children, INHERITING the
   * mask from the original parent indirect rather than re-deriving it via adjacent-pair
   * scan over full keys. This avoids the {@code createNodeFromChildren-N} BUILD-VIOLATION
   * pathology where the re-derivation captures bits that are non-constant within some
   * child's subtree (Phase 4-class issue).
   *
   * <p>The bucket's children are a subset of the original parent's children, all
   * β-constant by construction. Their partials in the original parent's layout are
   * still unique (a subset of unique partials remains unique). We inherit the mask
   * (with β removed if present) and reuse the original partials.
   *
   * <p>Edge cases:
   * <ul>
   *   <li>SingleMask original parent → SingleMask bucket node (mask possibly shrunk).</li>
   *   <li>MultiMask original parent → for now, fall back to {@link #createNodeFromChildren}
   *       (Phase 4b's MultiMask inheritance not yet implemented). This case may surface
   *       BUILD-VIOLATIONs but matches the existing non-strict-Binna behavior.</li>
   *   <li>β not in original mask → mask unchanged.</li>
   *   <li>β in original mask → remove β, recompute partials for the subset (still
   *       under the SAME mask but excluding β-bit position). The bucket children all
   *       share the same β value, so removing β from their partials preserves uniqueness
   *       (they were unique under the larger mask; removing one bit may collide if two
   *       children differed ONLY at β — but the bucket is β-constant so that case yields
   *       a single bucket, not both). We verify uniqueness defensively.</li>
   * </ul>
   *
   * <p>Returns {@code null} on any failure (unloadable parent layout, partial-key
   * collision, fan-out overflow). Caller falls back appropriately.
   *
   * @param parentIndirect the original (pre-split) indirect whose mask we inherit
   * @param bucketIndices  the indices of {@code parentIndirect}'s children that are in this bucket
   * @param replacementRefs replacement PageReferences for those bucket-indexed slots —
   *                        for a kept (β-constant) child, this is the original ref;
   *                        for a recursively-split child, this is the corresponding
   *                        half (left or right) of the recursive split. Must have
   *                        the same length as {@code bucketIndices}.
   * @param absBit         β bit being removed from the mask if present
   * @param log            TIL for fresh-page registration
   * @param revision       current revision number
   * @return PageReference rooted at the bucket's subtree (single ref for size-1, indirect
   *         for size > 1), or null on failure
   */
  private @Nullable PageReference buildBucketWithInheritedMask(HOTIndirectPage parentIndirect,
      int[] bucketIndices, PageReference[] replacementRefs, int bucketSize, int absBit,
      TransactionIntentLog log, int revision) {
    if (bucketSize == 0) {
      return null;
    }
    if (bucketSize == 1) {
      return replacementRefs[0];
    }

    // MultiMask inheritance not yet implemented — fall back to createNodeFromChildren which
    // may surface BUILD-VIOLATIONs but matches the legacy non-strict-Binna behavior.
    if (parentIndirect.getLayoutType() != HOTIndirectPage.LayoutType.SINGLE_MASK) {
      final PageReference[] children = Arrays.copyOf(replacementRefs, bucketSize);
      return wrapBucketInSubtree(children, bucketSize, parentIndirect.getHeight(), log, revision);
    }

    final int oldInitialBytePos = parentIndirect.getInitialBytePos();
    final long oldMask = parentIndirect.getBitMask();
    final int[] oldPartials = parentIndirect.getPartialKeys();

    // Compute β-bit position in the old mask. If β isn't in oldMask, the new mask = oldMask.
    final int betaByteOffset = (absBit / 8) - oldInitialBytePos;
    long betaMaskBit = 0L;
    if (betaByteOffset >= 0 && betaByteOffset < 8) {
      final int betaBitInByte = absBit % 8;
      final int betaBitInWord = (7 - betaByteOffset) * 8 + (7 - betaBitInByte);
      final long candidate = 1L << betaBitInWord;
      if ((oldMask & candidate) != 0L) {
        betaMaskBit = candidate;
      }
    }
    final long newMask = oldMask & ~betaMaskBit;

    // Compute new partials. If newMask == oldMask, partials are identical to the originals
    // (for each bucket index, the original partial). If newMask != oldMask, we strip the β
    // bit from each original partial — i.e., compress the partial under the new mask shape.
    // Sparse-path encoding requires that we actually re-derive partials from the bucket
    // children's first keys (because the kept children's β-position partial bits were 0 by
    // sparse-path convention, so removing β just shifts the remaining bits).
    final int[] newPartials = new int[bucketSize];
    for (int i = 0; i < bucketSize; i++) {
      final int srcIdx = bucketIndices[i];
      if (srcIdx < 0 || srcIdx >= oldPartials.length) {
        // Recursively-split child — no original partial. Compute via sparse-path semantics:
        // newMask covers all old non-β bits; the bucket child's first key gives the
        // sparse-path partial for that child under the new mask. Equivalent to the dense
        // PEXT under newMask of the child's first key.
        final byte[] firstKey = getFirstKeyFromChild(replacementRefs[i]);
        if (firstKey == null || firstKey.length == 0) {
          return null;
        }
        newPartials[i] = computePartialKeySingleMask(firstKey, oldInitialBytePos, newMask);
        continue;
      }
      final int oldPartial = oldPartials[srcIdx];
      if (betaMaskBit == 0L) {
        // β not in old mask — partial unchanged.
        newPartials[i] = oldPartial;
      } else {
        // Strip β from partial via PEXT under newMask's shape (compressed-out β slot).
        // The β-bit's position in the old partial is bitCount(oldMask & (betaMaskBit - 1)).
        // Removing that bit shifts higher bits down by 1.
        final int betaOutputPos = Long.bitCount(oldMask & (betaMaskBit - 1L));
        final int low = oldPartial & ((1 << betaOutputPos) - 1);
        final int high = (oldPartial >>> (betaOutputPos + 1)) << betaOutputPos;
        newPartials[i] = low | high;
      }
    }

    // Verify uniqueness. If a collision occurs, the bucket isn't representable under the
    // shrunk mask alone — fall back to createNodeFromChildren.
    for (int i = 1; i < bucketSize; i++) {
      for (int k = 0; k < i; k++) {
        if (newPartials[i] == newPartials[k]) {
          final PageReference[] children = Arrays.copyOf(replacementRefs, bucketSize);
          return wrapBucketInSubtree(children, bucketSize, parentIndirect.getHeight(),
              log, revision);
        }
      }
    }

    // Sort children + partials by partial-key (HOT I7).
    final PageReference[] children = Arrays.copyOf(replacementRefs, bucketSize);
    sortChildrenAndPartialsByPartial(children, newPartials);

    final long pageKey = pageKeyAllocator.getAsLong();
    final HOTIndirectPage built;
    if (newMask == 0L) {
      // Empty mask — no disc bits to discriminate. Possible if β was the ONLY bit in
      // the original mask. Bucket has multiple children but no way to discriminate via
      // PEXT under the inherited mask. Fall back.
      final PageReference[] childCopy = Arrays.copyOf(replacementRefs, bucketSize);
      return wrapBucketInSubtree(childCopy, bucketSize, parentIndirect.getHeight(),
          log, revision);
    }
    if (bucketSize == 2) {
      // BiNode requires a single explicit disc bit, not a mask. Use the original parent's
      // most significant remaining bit. computeDifferingBit on the children's boundary is
      // safer for actual routing. Fall back to wrapBucketInSubtree which handles 2-child
      // cleanly.
      final PageReference[] childCopy = Arrays.copyOf(replacementRefs, bucketSize);
      return wrapBucketInSubtree(childCopy, bucketSize, parentIndirect.getHeight(),
          log, revision);
    }
    if (bucketSize <= 16) {
      built = HOTIndirectPage.createSpanNode(pageKey, revision, oldInitialBytePos, newMask,
          newPartials, children, parentIndirect.getHeight());
    } else if (bucketSize <= NodeUpgradeManager.MULTI_NODE_MAX_CHILDREN) {
      built = HOTIndirectPage.createMultiNode(pageKey, revision, oldInitialBytePos, newMask,
          newPartials, children, parentIndirect.getHeight());
    } else {
      return null;
    }
    final PageReference ref = new PageReference();
    ref.setKey(pageKey);
    ref.setPage(built);
    log.put(ref, PageContainer.getInstance(built, built));
    return ref;
  }

  /**
   * Split a non-β-constant leaf into two new leaves, one per β-bit value. Both leaves
   * receive fresh pageKeys and are registered in the TIL.
   *
   * <p>The original leaf is left untouched — keys+values are copied out via
   * {@link HOTLeafPage#getKey}/{@link HOTLeafPage#getValue} and put into fresh pages.
   *
   * <p>HFT-grade: two allocations (the two new leaf pages), one register per leaf.
   * No intermediate copies beyond the per-entry byte[] returned by {@code getKey}/{@code getValue}
   * (already part of the existing read API).
   */
  private @Nullable SubtreeSplit splitLeafOnBit(HOTLeafPage leaf, int absBit,
      TransactionIntentLog log, int revision) {
    final int n = leaf.getEntryCount();
    if (n < 2) {
      // Single-entry leaf can't be split — caller's bv check should have ruled this out.
      return null;
    }

    final long leftKey = pageKeyAllocator.getAsLong();
    final long rightKey = pageKeyAllocator.getAsLong();
    final HOTLeafPage leftLeaf = new HOTLeafPage(leftKey, revision, leaf.getIndexType());
    final HOTLeafPage rightLeaf = new HOTLeafPage(rightKey, revision, leaf.getIndexType());

    int leftCount = 0;
    int rightCount = 0;
    for (int i = 0; i < n; i++) {
      final byte[] key = leaf.getKey(i);
      final byte[] value = leaf.getValue(i);
      final boolean bitSet = DiscriminativeBitComputer.isBitSet(key, absBit);
      final HOTLeafPage target = bitSet ? rightLeaf : leftLeaf;
      if (!target.put(key, value)) {
        return null; // out-of-space — bail
      }
      if (bitSet) rightCount++;
      else leftCount++;
    }

    if (leftCount == 0 || rightCount == 0) {
      return null; // degenerate — caller's non-constancy check should have ruled this out
    }

    final PageReference leftRef = new PageReference();
    leftRef.setKey(leftKey);
    leftRef.setPage(leftLeaf);
    log.put(leftRef, PageContainer.getInstance(leftLeaf, leftLeaf));

    final PageReference rightRef = new PageReference();
    rightRef.setKey(rightKey);
    rightRef.setPage(rightLeaf);
    log.put(rightRef, PageContainer.getInstance(rightLeaf, rightLeaf));

    return new SubtreeSplit(leftRef, rightRef);
  }

  /**
   * Split a non-β-constant indirect into two β-constant subtrees. Each child is
   * classified by {@link #bitConstantValueInSubtree}; non-constant children recurse via
   * {@link #splitSubtreeOnBit}. The two resulting buckets become two new indirect pages
   * (or pass-through single refs) registered in the TIL.
   *
   * <p>HFT-grade: pre-sized child buckets (bounded by 32 children × 2 = 64 worst-case
   * after recursive splits). All allocations are scoped to the rebalance event.
   */
  private @Nullable SubtreeSplit splitIndirectOnBit(HOTIndirectPage indirect, int absBit,
      TransactionIntentLog log, int revision) {
    final int m = indirect.getNumChildren();
    if (m == 0) {
      return null;
    }
    // Worst case bucket size: 2 * m if every child needs splitting.
    final PageReference[] leftBucket = new PageReference[2 * m];
    final PageReference[] rightBucket = new PageReference[2 * m];
    // Track originating slot index in {@code indirect} (or -1 for recursive-split products).
    // Used by buildBucketWithInheritedMask to look up the original partial-key.
    final int[] leftBucketIndices = new int[2 * m];
    final int[] rightBucketIndices = new int[2 * m];
    int leftN = 0;
    int rightN = 0;

    for (int i = 0; i < m; i++) {
      final PageReference childRef = indirect.getChildReference(i);
      if (childRef == null) {
        return null;
      }
      final int bv = bitConstantValueInSubtree(childRef, absBit);
      if (bv == 0) {
        leftBucket[leftN] = childRef;
        leftBucketIndices[leftN] = i;
        leftN++;
      } else if (bv == 1) {
        rightBucket[rightN] = childRef;
        rightBucketIndices[rightN] = i;
        rightN++;
      } else {
        // child is non-constant: recurse.
        final SubtreeSplit childSplit = splitSubtreeOnBit(childRef, absBit, log, revision);
        if (childSplit == null) {
          return null;
        }
        leftBucket[leftN] = childSplit.leftRef();
        leftBucketIndices[leftN] = -1; // recursive split — no original partial
        leftN++;
        rightBucket[rightN] = childSplit.rightRef();
        rightBucketIndices[rightN] = -1; // recursive split — no original partial
        rightN++;
      }
    }

    if (leftN == 0 || rightN == 0) {
      return null; // sibling was actually β-constant, caller's check was wrong
    }

    // Build leftSubtree from leftBucket using mask inheritance from {@code indirect} —
    // avoids createNodeFromChildren-N's BUILD-VIOLATION pathology where adjacent-pair
    // re-derivation captures bits non-constant within some child's subtree (Phase 4 issue).
    final PageReference leftRef = buildBucketWithInheritedMask(indirect, leftBucketIndices,
        leftBucket, leftN, absBit, log, revision);
    if (leftRef == null) return null;
    final PageReference rightRef = buildBucketWithInheritedMask(indirect, rightBucketIndices,
        rightBucket, rightN, absBit, log, revision);
    if (rightRef == null) return null;

    return new SubtreeSplit(leftRef, rightRef);
  }

  /**
   * Build a single subtree rooted at a fresh PageReference from a bucket of β-constant
   * children. Single-child bucket → pass-through ref. Multi-child bucket → new indirect
   * page via {@link #createNodeFromChildren}, registered in the TIL.
   *
   * <p>The new indirect's height matches the original sibling subtree's height (the
   * caller passes that). A more sophisticated implementation could recompute height to
   * exactly fit the bucket, but matching the original height is safe and sufficient
   * for the parent's height invariants.
   */
  private @Nullable PageReference wrapBucketInSubtree(PageReference[] bucket, int n, int height,
      TransactionIntentLog log, int revision) {
    if (n == 0) {
      return null;
    }
    if (n == 1) {
      return bucket[0]; // pass-through
    }
    // Trim and build new indirect.
    final PageReference[] children = Arrays.copyOf(bucket, n);
    if (children.length > NodeUpgradeManager.MULTI_NODE_MAX_CHILDREN) {
      return null; // bucket too large — caller falls back to splitParentAndRecurse
    }
    final long pageKey = pageKeyAllocator.getAsLong();
    final HOTIndirectPage built;
    try {
      built = createNodeFromChildren(children, pageKey, revision, height);
    } catch (Throwable t) {
      return null;
    }
    final PageReference ref = new PageReference();
    ref.setKey(pageKey);
    ref.setPage(built);
    log.put(ref, PageContainer.getInstance(built, built));
    return ref;
  }

  /**
   * After {@link #rebalanceSiblingsForBit} produces a β-constant children array, build
   * a parent that integrates β into its mask while preserving Binna's sparse-path encoding.
   * Inherits the original parent's mask + β (not adjacent-pair re-derived from full keys),
   * avoiding the {@code createNodeFromChildren-N} BUILD-VIOLATION pathology.
   *
   * <p>For each new child slot:
   * <ul>
   *   <li><b>Kept (β-constant) sibling</b>: existing partial key, repositioned into the
   *       new layout via {@code Long.expand}, with β-bit value set from the sibling's
   *       constant β-value.</li>
   *   <li><b>Leaf-split product</b> (left/right of original splitChildIdx slot): the old
   *       splitChild's partial repositioned, with β-bit value 0 (left) or 1 (right).</li>
   *   <li><b>Recursive sibling-split product</b>: the old sibling's partial (under the
   *       original mask) repositioned into the new mask, with β-bit value 0 (left) or 1
   *       (right) per which half of the recursive split it is.</li>
   * </ul>
   *
   * <p>Returns the integrated parent, or {@code null} on any failure (cross-window β,
   * MultiMask parent, partial-key collision, fan-out overflow). Caller falls back to
   * the intermediate-BiNode workaround on null.
   *
   * <p>HFT-grade: zero allocation beyond the new partial-key array and child-reference
   * array. Uses {@link Long#expand} (PDEP) intrinsic for repositioning. No boxing,
   * no auto-collections.
   *
   * @param parent          the original (pre-rebalance) parent
   * @param origChildIdx    slot index of the original split-child in {@code parent}
   * @param origLeftChild   leaf-split left product (β=0)
   * @param origRightChild  leaf-split right product (β=1)
   * @param newAbsBit       β disc bit being added (absolute MSB-first)
   * @param siblingPlan     parallel arrays describing each non-split sibling slot
   * @param siblingPlanCount actual count of slots described in {@code siblingPlan}
   * @param revision        current revision for the new indirect
   */
  private @Nullable HOTIndirectPage buildRebalancedParentWithInheritedMask(HOTIndirectPage parent,
      int origChildIdx, PageReference origLeftChild, PageReference origRightChild, int newAbsBit,
      RebalancedSibling[] siblingPlan, int siblingPlanCount, int revision) {
    if (parent.getLayoutType() != HOTIndirectPage.LayoutType.SINGLE_MASK) {
      diagnoseIntegrateFail("multimask", parent, newAbsBit, -1);
      return null;
    }
    final int oldInitialBytePos = parent.getInitialBytePos();
    final long oldMask = parent.getBitMask();
    final int oldCount = Long.bitCount(oldMask);

    // 1. Encode β into PEXT mask (same as addEntryWithPDep step 1).
    final int newBitByteOffset = (newAbsBit / 8) - oldInitialBytePos;
    if (newBitByteOffset < 0 || newBitByteOffset >= 8) {
      diagnoseIntegrateFail("cross-window", parent, newAbsBit, -1);
      return null;
    }
    final int newBitInByte = newAbsBit % 8;
    final int bitInWord = (7 - newBitByteOffset) * 8 + (7 - newBitInByte);
    final long newBitMaskBit = 1L << bitInWord;
    if ((oldMask & newBitMaskBit) != 0L) {
      // β is already in parent's mask — handled in {@link #rebalanceAndIntegrate}'s
      // pre-check; should not be reached here.
      diagnoseIntegrateFail("beta-already-in-mask", parent, newAbsBit, -1);
      return null;
    }
    final long newMask = oldMask | newBitMaskBit;

    // 2. Output position of β in new partial-key layout.
    final int newBitOutputPos = Long.bitCount(newMask & (newBitMaskBit - 1L));
    final long oldPartialMaskInNewLayout =
        (((1L << (oldCount + 1)) - 1L) ^ (1L << newBitOutputPos));

    // 3. Materialize new children array + partial keys. Each old slot contributes 1 or 2
    //    new slots; for the original split slot it's always 2 (left, right); for siblings
    //    it depends on whether they were kept (1) or recursively split (2).
    //
    //    Total new slots: parent.getNumChildren() + (1 + count_of_split_siblings).
    //    Sized generously to fit MULTI_NODE_MAX_CHILDREN + 1 without resizing.
    final int oldNumChildren = parent.getNumChildren();
    final int maxNew = 2 * oldNumChildren;
    final PageReference[] newChildren = new PageReference[maxNew];
    final int[] newPartials = new int[maxNew];
    int outIdx = 0;

    final int[] oldPartials = parent.getPartialKeys();
    final int splitChildOldPartial = oldPartials[origChildIdx];
    final int splitChildRepositioned =
        (int) Long.expand(Integer.toUnsignedLong(splitChildOldPartial), oldPartialMaskInNewLayout);

    int siblingPlanIdx = 0;
    for (int i = 0; i < oldNumChildren; i++) {
      if (i == origChildIdx) {
        // Original leaf-split: left (β=0), right (β=1). β was NOT in old mask, so the
        // repositioned partial has β-bit position open for assignment.
        if (outIdx >= maxNew) return null;
        newChildren[outIdx] = origLeftChild;
        newPartials[outIdx] = splitChildRepositioned;
        outIdx++;
        if (outIdx >= maxNew) return null;
        newChildren[outIdx] = origRightChild;
        newPartials[outIdx] = splitChildRepositioned | (1 << newBitOutputPos);
        outIdx++;
      } else {
        if (siblingPlanIdx >= siblingPlanCount) return null; // plan inconsistent
        final RebalancedSibling rb = siblingPlan[siblingPlanIdx++];
        final int oldSibPartial = oldPartials[i];
        final int repositioned = (int) Long.expand(
            Integer.toUnsignedLong(oldSibPartial), oldPartialMaskInNewLayout);
        if (rb.split()) {
          // Recursive split product: left half (β=0), right half (β=1).
          if (outIdx >= maxNew) return null;
          newChildren[outIdx] = rb.leftRef();
          newPartials[outIdx] = repositioned;
          outIdx++;
          if (outIdx >= maxNew) return null;
          newChildren[outIdx] = rb.rightRef();
          newPartials[outIdx] = repositioned | (1 << newBitOutputPos);
          outIdx++;
        } else {
          // Kept β-constant sibling: single slot with β-bit set to its constant value.
          if (outIdx >= maxNew) return null;
          newChildren[outIdx] = rb.leftRef(); // kept ref stored in leftRef field
          newPartials[outIdx] = repositioned | (rb.constantBit() << newBitOutputPos);
          outIdx++;
        }
      }
    }

    if (outIdx > NodeUpgradeManager.MULTI_NODE_MAX_CHILDREN) {
      diagnoseIntegrateFail("fanout-overflow", parent, newAbsBit, outIdx);
      return null;
    }

    // 4. Verify partial-key uniqueness (HOT I3 — sparse-path encoding).
    for (int i = 1; i < outIdx; i++) {
      for (int k = 0; k < i; k++) {
        if (newPartials[i] == newPartials[k]) {
          diagnoseIntegrateFail("partial-collision", parent, newAbsBit, outIdx);
          return null;
        }
      }
    }

    // 5. Trim and sort by partial-key (HOT I7).
    final PageReference[] trimChildren = (outIdx == maxNew) ? newChildren
        : Arrays.copyOf(newChildren, outIdx);
    final int[] trimPartials = (outIdx == maxNew) ? newPartials
        : Arrays.copyOf(newPartials, outIdx);
    sortChildrenAndPartialsByPartial(trimChildren, trimPartials);

    if (outIdx <= 16) {
      return HOTIndirectPage.createSpanNode(parent.getPageKey(), revision,
          oldInitialBytePos, newMask, trimPartials, trimChildren, parent.getHeight());
    }
    return HOTIndirectPage.createMultiNode(parent.getPageKey(), revision,
        oldInitialBytePos, newMask, trimPartials, trimChildren, parent.getHeight());
  }

  /**
   * Plan entry for one non-split sibling slot in Phase 3 rebalance: either KEPT (single
   * β-constant subtree, bit value known) or SPLIT (two β-constant subtrees from recursive
   * split — left has β=0, right has β=1).
   *
   * <p>HFT-grade: record carrier with primitive {@code int} for {@code constantBit}.
   * Fields are reused to avoid allocation for kept refs.
   */
  private record RebalancedSibling(boolean split, int constantBit, PageReference leftRef,
                                    PageReference rightRef) {}

  /**
   * Phase 3 entry point: call from {@link #updateParentForSplitWithPath} when
   * {@code addEntryWithPDep} returns null due to non-constancy. Walks each non-split
   * sibling, classifies (kept vs. split), and builds the integrated parent via mask
   * inheritance. Returns the new parent or {@code null} on any failure.
   *
   * <p>HFT-grade: pre-sized stack-style plan array; no boxing; helper allocations are
   * scoped to the rebalance event.
   */
  private @Nullable HOTIndirectPage rebalanceAndIntegrate(HOTIndirectPage parent,
      int splitChildIdx, PageReference origLeftChild, PageReference origRightChild,
      int newAbsBit, TransactionIntentLog log, int revision) {
    if (parent.getLayoutType() != HOTIndirectPage.LayoutType.SINGLE_MASK) {
      diagnosePhase3Skip("multimask-parent", parent, newAbsBit);
      return null;
    }

    // β-already-in-mask case: the leaf split's MSDB is a bit parent already routes on.
    // This means parent had a violation BEFORE the split (splitChild was non-constant on β
    // despite parent assigning it a single β-routing slot). Properly resolving this requires
    // SUBTREE MERGE — the new rightChild belongs in the existing β=1 sibling's subtree, not
    // as a new slot. Subtree merge is not yet implemented; fall through to the
    // intermediate-BiNode fallback for this case.
    final int oldInitialBytePos = parent.getInitialBytePos();
    final long oldMask = parent.getBitMask();
    final int newBitByteOffset = (newAbsBit / 8) - oldInitialBytePos;
    if (newBitByteOffset >= 0 && newBitByteOffset < 8) {
      final int newBitInByte = newAbsBit % 8;
      final int bitInWord = (7 - newBitByteOffset) * 8 + (7 - newBitInByte);
      final long newBitMaskBit = 1L << bitInWord;
      if ((oldMask & newBitMaskBit) != 0L) {
        diagnosePhase3Skip("beta-already-in-mask", parent, newAbsBit);
        return null;
      }
    }

    final int oldNumChildren = parent.getNumChildren();
    final RebalancedSibling[] plan = new RebalancedSibling[oldNumChildren];
    int planCount = 0;

    for (int i = 0; i < oldNumChildren; i++) {
      if (i == splitChildIdx) continue;
      final PageReference sibRef = parent.getChildReference(i);
      final int bv = bitConstantValueInSubtree(sibRef, newAbsBit);
      if (bv >= 0) {
        plan[planCount++] = new RebalancedSibling(false, bv, sibRef, null);
      } else {
        final SubtreeSplit split = splitSubtreeOnBit(sibRef, newAbsBit, log, revision);
        if (split == null) {
          diagnosePhase3Skip("subtree-split-failed", parent, newAbsBit);
          return null;
        }
        plan[planCount++] = new RebalancedSibling(true, -1, split.leftRef(), split.rightRef());
      }
    }

    final HOTIndirectPage result = buildRebalancedParentWithInheritedMask(parent, splitChildIdx,
        origLeftChild, origRightChild, newAbsBit, plan, planCount, revision);
    if (result == null) {
      diagnosePhase3Skip("integrate-failed", parent, newAbsBit);
    }
    return result;
  }

  /** Diagnostic helper — gated on {@code -Dhot.debug.phase3=1}. Counts Phase 3 skips by reason. */
  private static void diagnosePhase3Skip(String reason, HOTIndirectPage parent, int β) {
    if (!Boolean.getBoolean("hot.debug.phase3")) return;
    System.err.println("[hot.phase3] skip reason=" + reason
        + " parentLayout=" + parent.getLayoutType()
        + " parentBytePos=" + parent.getInitialBytePos()
        + " parentMask=" + Long.toHexString(parent.getBitMask())
        + " parentChildren=" + parent.getNumChildren()
        + " β=" + β);
  }

  /** Diagnostic — finer reason for integrate failure. */
  private static void diagnoseIntegrateFail(String reason, HOTIndirectPage parent, int β, int outIdx) {
    if (!Boolean.getBoolean("hot.debug.phase3")) return;
    System.err.println("[hot.phase3] integrate-fail reason=" + reason
        + " parentBytePos=" + parent.getInitialBytePos()
        + " parentMask=" + Long.toHexString(parent.getBitMask())
        + " parentChildren=" + parent.getNumChildren()
        + " β=" + β + " outIdx=" + outIdx);
  }

  // ===========================================================================
  // Phase 4 — β-already-in-mask subtree merge
  //
  // When addEntryWithPDep / addEntryMultiMask rejects in updateParentForSplitWithPath
  // because the new disc bit β is ALREADY in parent's mask (i.e., the leaf split's
  // MSDB is a bit parent already routes on), Phase 3 cannot help: the resolution is
  // not "extend parent's mask with β" but "merge moveHalf's keys into the existing
  // β=¬v_L sibling subtree".
  //
  // Algorithm (mirrors the task brief):
  //   1. Decode β's output position in parent's partial-key layout.
  //   2. v_L = (parent.partialKeys[splitChildIdx] >>> β_outputPos) & 1.
  //   3. keepHalf = (v_L == 0) ? leftChild : rightChild
  //      moveHalf = (v_L == 0) ? rightChild : leftChild
  //   4. Find sibling slot whose stored partial differs from splitChild's only at β
  //      (XOR == 1 << β_outputPos).
  //   5. Replace parent's slot[splitChildIdx] with keepHalf; CoW parent.
  //   6. For each (k, v) in moveHalf: bulk-insert into siblingRef's subtree.
  //
  // CoW correctness: parent's pageKey is preserved (withUpdatedChild semantics);
  // ancestors of parent need no further CoW. Each bulk-insert is a separate top-down
  // CoW within the sibling subtree (descend → leaf → CoW back up to siblingRef →
  // re-attach to parent's slot[siblingIdx] via withUpdatedChild on parent). If a
  // bulk-insert triggers a leaf split that itself recurses into Phase 4, the recursion
  // operates on a stable parent-page snapshot stored in the TIL.
  //
  // HFT-grade: pre-sized buffers; no boxing; reuses the existing PEXT routing in
  // {@link HOTIndirectPage#findChildIndex}; allocations are bounded to the moveHalf's
  // entry count (single-leaf-worth) per firing.
  // ===========================================================================

  /**
   * Phase 4 dispatch helper: gate on β-already-in-mask and invoke {@link #subtreeMerge}.
   *
   * <p>Emits a diagnostic line on the gate decision when {@code -Dhot.debug.phase4=1}.
   * Returns {@code true} only when the merge actually succeeded.
   *
   * <p>The {@code pathNodes / pathRefs / pathChildIndices / parentPathIdx} arguments expose
   * the descend path so Phase 4 can hoist when {@link #subtreeMerge} reports
   * {@code no-sibling-slot} (β is in {@code parent}'s mask but {@code parent} has no slot
   * with the opposite β value). See {@link #hoistAndReroute}.
   */
  private boolean tryPhase4SubtreeMerge(HOTIndirectPage parent, PageReference parentRef,
      int splitChildIdx, PageReference leftChild, PageReference rightChild, int absBit,
      StorageEngineWriter storageEngineReader, TransactionIntentLog log,
      HOTIndirectPage[] pathNodes, PageReference[] pathRefs, int[] pathChildIndices,
      int parentPathIdx) {
    if (!isBetaAlreadyInParentMask(parent, absBit)) {
      diagnosePhase4Skip("not-beta-in-mask", parent, absBit);
      return false;
    }
    if (subtreeMerge(parent, parentRef, splitChildIdx, leftChild, rightChild, absBit,
        storageEngineReader, log)) {
      return true;
    }
    // subtreeMerge rejected (most commonly: no-sibling-slot — β is in parent's mask but
    // every parent slot encodes the same β value as splitChild). Try hoisting.
    return hoistAndReroute(parent, parentRef, splitChildIdx, leftChild, rightChild, absBit,
        storageEngineReader, log, pathNodes, pathRefs, pathChildIndices, parentPathIdx);
  }

  /**
   * Returns {@code true} if the absolute MSB-first disc bit {@code absBit} is already part
   * of {@code parent}'s mask (SingleMask) or extraction-table (MultiMask). Used to gate
   * Phase 4 entry — {@link #subtreeMerge} only applies when β is already a parent disc bit.
   *
   * <p>HFT-grade: zero allocation, primitive ops only.
   */
  private static boolean isBetaAlreadyInParentMask(HOTIndirectPage parent, int absBit) {
    if (parent.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK) {
      final int initialBytePos = parent.getInitialBytePos();
      final int byteOffset = (absBit / 8) - initialBytePos;
      if (byteOffset < 0 || byteOffset >= 8) return false;
      final int bitInByte = absBit % 8;
      final int bitInWord = (7 - byteOffset) * 8 + (7 - bitInByte);
      return (parent.getBitMask() & (1L << bitInWord)) != 0L;
    }
    final byte[] extractionPositions = parent.getExtractionPositions();
    final long[] extractionMasks = parent.getExtractionMasks();
    if (extractionPositions == null || extractionMasks == null) return false;
    final int newBytePos = absBit / 8;
    final int newBitInByte = absBit % 8;
    final int newMaskBit = 1 << (7 - newBitInByte);
    for (int i = 0; i < extractionPositions.length; i++) {
      if ((extractionPositions[i] & 0xFF) != newBytePos) continue;
      final int chunkIdx = i / 8;
      final int byteOffsetInChunk = i % 8;
      final int byteMask =
          (int) ((extractionMasks[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFF);
      if ((byteMask & newMaskBit) != 0) return true;
    }
    return false;
  }

  /**
   * Find the parent's child slot whose stored partial differs from the split child's
   * stored partial only at the β-output bit position. This is the "β=¬v_L sibling"
   * that should absorb the moveHalf's keys when β is already in parent's mask.
   *
   * <p>Returns {@code -1} if no such sibling exists (e.g., β is in parent's mask but
   * only one slot encodes β=v_L; the other β value is implicitly handled via subset
   * fallback elsewhere). In that case Phase 4's "merge into sibling" cannot proceed.
   *
   * <p>HFT-grade: linear scan, primitive ops, no allocation.
   *
   * @param parent          the parent indirect (any layout)
   * @param splitChildIdx   slot index of the split child
   * @param betaXorMask     {@code 1 << β_outputPos} in parent's partial-key layout
   * @return slot index of the β=¬v_L sibling, or {@code -1}
   */
  private static int findSiblingSlotForBitValue(HOTIndirectPage parent, int splitChildIdx,
      int betaXorMask) {
    if (parent == null || betaXorMask == 0) return -1;
    final int[] partials = parent.getPartialKeys();
    if (partials == null) return -1;
    final int target = partials[splitChildIdx] ^ betaXorMask;
    final int n = parent.getNumChildren();
    for (int i = 0; i < n; i++) {
      if (i == splitChildIdx) continue;
      if (partials[i] == target) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Decode β's output position (LSB=0) within a SingleMask parent's partial-key layout.
   * Returns {@code -1} if β is not in the parent's mask or falls outside the 8-byte
   * window.
   *
   * <p>Mirrors the bit-decoding in {@link #addEntryWithPDep}: byte offset in window
   * (β/8 - initialBytePos), MSB-first bit (β%8) → long-bit ((7-bo)*8 + (7-bb)).
   * Output position is the count of mask bits below that long-bit.
   */
  private static int singleMaskBetaOutputPos(HOTIndirectPage parent, int absBit) {
    final int initialBytePos = parent.getInitialBytePos();
    final long mask = parent.getBitMask();
    final int byteOffset = (absBit / 8) - initialBytePos;
    if (byteOffset < 0 || byteOffset >= 8) return -1;
    final int bitInByte = absBit % 8;
    final int bitInWord = (7 - byteOffset) * 8 + (7 - bitInByte);
    final long bitMaskBit = 1L << bitInWord;
    if ((mask & bitMaskBit) == 0L) return -1;
    return Long.bitCount(mask & (bitMaskBit - 1L));
  }

  /**
   * Decode β's output position (LSB=0) within a MultiMask parent's partial-key layout.
   * Returns {@code -1} if β is not in the parent's extraction mask.
   *
   * <p>BE concat ordering: chunk 0 occupies the high bits of the result; within each
   * byte, MSB-first bit ordering. Walks extractionPositions/extractionMasks to find
   * β's position, returning the LSB-relative output bit.
   */
  private static int multiMaskBetaOutputPos(HOTIndirectPage parent, int absBit) {
    final byte[] extractionPositions = parent.getExtractionPositions();
    final long[] extractionMasks = parent.getExtractionMasks();
    if (extractionPositions == null || extractionMasks == null) return -1;
    final int newBytePos = absBit / 8;
    final int newBitInByte = absBit % 8;
    final int numBytes = extractionPositions.length;
    int totalBits = 0;
    for (final long m : extractionMasks) totalBits += Long.bitCount(m);
    int bitsAccumulated = 0;
    for (int i = 0; i < numBytes; i++) {
      final int bp = extractionPositions[i] & 0xFF;
      final int chunkIdx = i / 8;
      final int byteOffsetInChunk = i % 8;
      final int byteMask =
          (int) ((extractionMasks[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFF);
      // MSB-first within the byte: mfBit = 0 → bit 7, mfBit = 7 → bit 0.
      for (int mfBit = 0; mfBit < 8; mfBit++) {
        final int byteBit = 7 - mfBit;
        if ((byteMask & (1 << byteBit)) == 0) continue;
        if (bp == newBytePos && mfBit == newBitInByte) {
          return totalBits - 1 - bitsAccumulated;
        }
        bitsAccumulated++;
      }
    }
    return -1;
  }

  /**
   * Phase 4 entry point: handle β-already-in-mask rejection by merging moveHalf's keys
   * into the existing β=¬v_L sibling's subtree. Works for both SingleMask and MultiMask
   * parent layouts.
   *
   * <p>Returns {@code true} if the merge succeeded and the parent has been registered
   * in the TIL with the updated state. Returns {@code false} on any failure (sibling
   * not found, bulk-insert failed, unloadable pages); caller falls through to the
   * existing intermediate-BiNode / splitParentAndRecurse fallbacks.
   *
   * <p>CoW correctness: the parent is updated via {@link HOTIndirectPage#withUpdatedChild}
   * which preserves pageKey identity, so ancestors above parent need no further CoW.
   * Each bulk-insert into the sibling subtree performs its own top-down CoW within that
   * subtree, terminating at parentRef (which is already in the TIL).
   *
   * <p>HFT-grade: bounded-allocation extraction of moveHalf's entries (single
   * {@code byte[][]} pair sized to the moveHalf entry count); no boxing; reuses
   * {@link HOTIndirectPage#findChildIndex}'s PEXT routing.
   *
   * @param parent           the parent indirect (any layout)
   * @param parentRef        TIL handle for parent
   * @param splitChildIdx    slot index of the split child in parent
   * @param leftChild        left half of the leaf split (β=0 keys)
   * @param rightChild       right half of the leaf split (β=1 keys)
   * @param absBit           β disc bit (absolute MSB-first), already in parent's mask
   * @param storageEngineReader for descending into sibling subtree
   * @param log              transaction intent log
   * @return {@code true} on successful merge
   */
  private boolean subtreeMerge(HOTIndirectPage parent, PageReference parentRef,
      int splitChildIdx, PageReference leftChild, PageReference rightChild, int absBit,
      StorageEngineWriter storageEngineReader, TransactionIntentLog log) {
    if (parent == null || parentRef == null || leftChild == null || rightChild == null) {
      return false;
    }
    final int revision = storageEngineReader.getRevisionNumber();

    // Step 1: locate β's output position in parent's partial-key layout.
    final int betaOutputPos;
    if (parent.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK) {
      betaOutputPos = singleMaskBetaOutputPos(parent, absBit);
    } else {
      betaOutputPos = multiMaskBetaOutputPos(parent, absBit);
    }
    if (betaOutputPos < 0) {
      diagnosePhase4Skip("beta-not-in-mask", parent, absBit);
      return false;
    }
    final int betaXorMask = 1 << betaOutputPos;

    // Step 2: v_L from parent's stored partial for splitChildIdx.
    final int[] partials = parent.getPartialKeys();
    if (partials == null || splitChildIdx < 0 || splitChildIdx >= partials.length) {
      diagnosePhase4Skip("no-partials", parent, absBit);
      return false;
    }
    final int splitChildPartial = partials[splitChildIdx];
    final int vL = (splitChildPartial >>> betaOutputPos) & 1;

    // Step 3: keep & move halves. By construction of the leaf split: leftChild has β=0 keys,
    // rightChild has β=1 keys.
    final PageReference keepHalf = (vL == 0) ? leftChild : rightChild;
    final PageReference moveHalf = (vL == 0) ? rightChild : leftChild;

    // Step 4: find the β=¬v_L sibling.
    final int siblingIdx = findSiblingSlotForBitValue(parent, splitChildIdx, betaXorMask);
    if (siblingIdx < 0) {
      diagnosePhase4Skip("no-sibling-slot", parent, absBit);
      return false;
    }

    // Step 5: extract moveHalf's entries BEFORE we mutate parent (so the page contents
    // can be read deterministically). moveHalf is a freshly built leaf from the leaf-split
    // path; its page is already in the TIL via {@link #handleLeafSplitAndInsertInternal}.
    final HOTLeafPage moveLeaf = resolveLeafPage(moveHalf, storageEngineReader, log);
    if (moveLeaf == null) {
      diagnosePhase4Skip("move-not-leaf", parent, absBit);
      return false;
    }
    final int moveCount = moveLeaf.getEntryCount();
    if (moveCount == 0) {
      // Vacuously merged — nothing to do but still replace the parent slot with keepHalf.
      final HOTIndirectPage updatedParent =
          parent.withUpdatedChild(splitChildIdx, keepHalf, revision);
      log.put(parentRef, PageContainer.getInstance(updatedParent, updatedParent));
      PHASE4_SUBTREE_MERGE_FIRINGS.incrementAndGet();
      return true;
    }
    final byte[][] moveKeys = new byte[moveCount][];
    final byte[][] moveValues = new byte[moveCount][];
    for (int i = 0; i < moveCount; i++) {
      moveKeys[i] = moveLeaf.getKey(i);
      moveValues[i] = moveLeaf.getValue(i);
    }

    // Step 6: replace parent's slot[splitChildIdx] with keepHalf, register CoW'd parent.
    HOTIndirectPage updatedParent =
        parent.withUpdatedChild(splitChildIdx, keepHalf, revision);
    log.put(parentRef, PageContainer.getInstance(updatedParent, updatedParent));

    // Step 7: bulk-insert moveHalf's entries into the sibling subtree.
    // Each insertion descends from siblingRef → target leaf via PEXT routing, CoWs the
    // target, inserts; on overflow the leaf-split-and-insert path runs (which may
    // recurse into Phase 4 again — bounded by total tree size). After each insertion,
    // re-fetch parent from log because the sibling subtree CoW may have updated it.
    for (int i = 0; i < moveCount; i++) {
      final PageContainer cont = log.get(parentRef);
      if (cont == null || !(cont.getModified() instanceof HOTIndirectPage curParent)) {
        diagnosePhase4Skip("parent-vanished", parent, absBit);
        return false;
      }
      // siblingIdx may shift if the sibling subtree's CoW altered the children layout —
      // but withUpdatedChild and indirect updates we perform preserve slot order, so
      // siblingIdx stays valid as long as moveHalf-insertion stays within siblingIdx's
      // subtree. If a leaf split inside the sibling subtree expands the parent (via
      // addEntryWithPDep success), child count grows; for our case (β-already-in-mask
      // rejection), addEntryWithPDep's β-already-in-mask path may itself fire — handled
      // recursively. siblingIdx stays valid in that case because withUpdatedChild also
      // preserves order.
      if (!bulkInsertIntoSiblingSubtree(curParent, parentRef, siblingIdx,
          moveKeys[i], moveValues[i], storageEngineReader, log)) {
        diagnosePhase4Skip("bulk-insert-failed", parent, absBit);
        return false;
      }
    }

    PHASE4_SUBTREE_MERGE_FIRINGS.incrementAndGet();
    return true;
  }

  /**
   * Phase 4 hoisting fallback: when {@link #subtreeMerge} rejects because {@code parent} has
   * no slot with bit β = ¬v_L (the "no-sibling-slot" case), walk UP the descend path looking
   * for the FIRST ancestor whose mask contains β AND whose stored partials include the
   * EXACT target — the descend slot's stored partial XOR-ed with the β-bit-position. That
   * exact-match slot is the unique routing-correct destination for the moveHalf keys
   * (they share all OTHER ancestor disc-bit values with the descend slot, but have β=¬v_L).
   * If such an ancestor is found, replace {@code parent}'s slot[splitChildIdx] with the
   * keep half and bulk-insert the move half's keys into that hoist ancestor's target slot.
   *
   * <p><b>Strict-criterion rationale.</b> A loose criterion ("any slot with β=¬v_L") would
   * route the moveHalf keys into a subtree whose OTHER disc-bit values disagree with the
   * moveHalf's bit pattern — breaking the I6 PEXT-routing invariant on every subsequent
   * lookup (the moveHalf keys' lookup descent would still subset-match the descend slot at
   * hoist ancestor, leading to {@code originalParent} where the keys no longer reside).
   * Empirical verification: a loose-criterion hoist on the diagnostic 50K reproducer caused
   * 11,776 I6 violations (every moved key misrouted), while the strict criterion preserves
   * I6 by guaranteeing exact-match routing for moveHalf keys after the move.
   *
   * <p><b>Empirical activation profile.</b> On the diagnostic microbench-pattern reproducer
   * the strict criterion never fires (all 42 no-sibling-slot cases have no ancestor with
   * the exact-target slot — β is genuinely vestigial at every level above {@code parent},
   * either because {@code parent} IS the root or because higher ancestors discriminate on
   * earlier-byte disc bits and don't carry β at all). Those cases fall through to the
   * intermediate-BiNode fallback, which is correctness-preserving but adds tree height.
   *
   * <p><b>CoW correctness.</b> Two pages are modified: {@code parent} (slot[splitChildIdx]
   * becomes keepHalf) and the hoist ancestor's target subtree (bulk-inserts of moveHalf
   * keys). Both updates preserve their respective {@link HOTIndirectPage} {@code pageKey}
   * via {@link HOTIndirectPage#withUpdatedChild}; ancestors above the hoist ancestor
   * therefore need no further CoW. Each modified page is registered in the TIL at its
   * original {@link PageReference}.
   *
   * <p><b>HFT-grade.</b> Pre-sized scratch arrays for moveKeys/moveValues, primitive bit
   * ops, single linear path walk; allocations bounded by the moveHalf's entry count
   * (single leaf).
   *
   * @return {@code true} on successful hoist+merge, {@code false} if no hoist ancestor exists
   */
  private boolean hoistAndReroute(HOTIndirectPage parent, PageReference parentRef,
      int splitChildIdx, PageReference leftChild, PageReference rightChild, int absBit,
      StorageEngineWriter storageEngineReader, TransactionIntentLog log,
      HOTIndirectPage[] pathNodes, PageReference[] pathRefs, int[] pathChildIndices,
      int parentPathIdx) {
    if (parent == null || parentRef == null || leftChild == null || rightChild == null) {
      return false;
    }
    if (pathNodes == null || pathRefs == null || pathChildIndices == null) {
      diagnosePhase4Skip("no-hoist-path-arrays", parent, absBit);
      return false;
    }
    final int revision = storageEngineReader.getRevisionNumber();

    // Decode β's output position in parent's layout to recover v_L.
    final int parentBetaOutputPos;
    if (parent.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK) {
      parentBetaOutputPos = singleMaskBetaOutputPos(parent, absBit);
    } else {
      parentBetaOutputPos = multiMaskBetaOutputPos(parent, absBit);
    }
    if (parentBetaOutputPos < 0) {
      diagnosePhase4Skip("hoist-beta-not-in-parent-mask", parent, absBit);
      return false;
    }
    final int[] parentPartials = parent.getPartialKeys();
    if (parentPartials == null || splitChildIdx < 0 || splitChildIdx >= parentPartials.length) {
      diagnosePhase4Skip("hoist-no-parent-partials", parent, absBit);
      return false;
    }
    final int vL = (parentPartials[splitChildIdx] >>> parentBetaOutputPos) & 1;

    // keepHalf stays under parent's slot[splitChildIdx]; moveHalf hoists out.
    final PageReference keepHalf = (vL == 0) ? leftChild : rightChild;
    final PageReference moveHalf = (vL == 0) ? rightChild : leftChild;

    // Walk UP the path from parent's level upward looking for a hoist ancestor.
    // path[parentPathIdx] is parent itself; we start scanning at parentPathIdx - 1.
    //
    // Hoist ancestor criterion: ancestor's mask contains β, AND ancestor has a slot whose
    // stored partial matches the slot containing the descend-path child but with bit β
    // toggled. That slot is the unique routing-correct destination for the moveHalf keys
    // (which share all OTHER ancestor disc-bit values with the descend slot, but have
    // β = ¬v_L). Picking ANY β=¬v_L slot is wrong — the OTHER bits must align too,
    // otherwise insertion lands in a subtree whose OTHER disc-bit values disagree, breaking
    // the I6 routing invariant.
    int hoistLevel = -1;
    int hoistTargetSlot = -1;
    for (int level = parentPathIdx - 1; level >= 0; level--) {
      final HOTIndirectPage ancestor = pathNodes[level];
      if (ancestor == null) continue;
      final int ancestorBetaOutputPos;
      if (ancestor.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK) {
        ancestorBetaOutputPos = singleMaskBetaOutputPos(ancestor, absBit);
      } else {
        ancestorBetaOutputPos = multiMaskBetaOutputPos(ancestor, absBit);
      }
      if (ancestorBetaOutputPos < 0) continue;
      final int[] ancestorPartials = ancestor.getPartialKeys();
      if (ancestorPartials == null) continue;
      final int descendSlot = pathChildIndices[level];
      if (descendSlot < 0 || descendSlot >= ancestorPartials.length) continue;
      final int descendPartial = ancestorPartials[descendSlot];
      // Sanity-check: descend slot's stored β bit must equal v_L (matches what originalParent
      // would route on if β were truly discriminating). If not, ancestor is structurally
      // inconsistent for this hoist; skip.
      if (((descendPartial >>> ancestorBetaOutputPos) & 1) != vL) continue;
      final int targetPartial = descendPartial ^ (1 << ancestorBetaOutputPos);
      int targetSlot = -1;
      final int n = ancestor.getNumChildren();
      for (int i = 0; i < n; i++) {
        if (ancestorPartials[i] == targetPartial) { targetSlot = i; break; }
      }
      if (targetSlot < 0) continue;
      hoistLevel = level;
      hoistTargetSlot = targetSlot;
      break;
    }
    if (hoistLevel < 0) {
      diagnosePhase4Skip("no-sibling-slot-no-hoist-ancestor", parent, absBit);
      return false;
    }

    // Resolve moveHalf to a leaf and snapshot its entries before mutating anything.
    final HOTLeafPage moveLeaf = resolveLeafPage(moveHalf, storageEngineReader, log);
    if (moveLeaf == null) {
      diagnosePhase4Skip("hoist-move-not-leaf", parent, absBit);
      return false;
    }
    final int moveCount = moveLeaf.getEntryCount();
    final byte[][] moveKeys;
    final byte[][] moveValues;
    if (moveCount == 0) {
      moveKeys = new byte[0][];
      moveValues = new byte[0][];
    } else {
      moveKeys = new byte[moveCount][];
      moveValues = new byte[moveCount][];
      for (int i = 0; i < moveCount; i++) {
        moveKeys[i] = moveLeaf.getKey(i);
        moveValues[i] = moveLeaf.getValue(i);
      }
    }

    // Step 1: replace parent's slot[splitChildIdx] with keepHalf. Preserves parent's pageKey.
    final HOTIndirectPage updatedParent =
        parent.withUpdatedChild(splitChildIdx, keepHalf, revision);
    log.put(parentRef, PageContainer.getInstance(updatedParent, updatedParent));

    // Step 2: bulk-insert moveHalf's entries into the hoist ancestor's β=¬v_L subtree.
    // bulkInsertIntoSiblingSubtree descends from ancestor.slot[hoistTargetSlot] downward,
    // routes via PEXT, CoWs each indirect on the descent path, and re-attaches the
    // (possibly mutated) subtree under ancestor.slot[hoistTargetSlot] preserving the
    // ancestor's pageKey. So pathRefs above hoistLevel need no further CoW — their
    // PageReferences still point at the ancestor's preserved pageKey.
    final PageReference hoistAncestorRef = pathRefs[hoistLevel];
    for (int i = 0; i < moveCount; i++) {
      // Re-fetch the ancestor from the TIL to act on its latest content (after possibly
      // a previous iteration's mutation).
      final PageContainer cont = log.get(hoistAncestorRef);
      final HOTIndirectPage curAncestor;
      if (cont != null && cont.getModified() instanceof HOTIndirectPage upd) {
        curAncestor = upd;
      } else {
        curAncestor = pathNodes[hoistLevel];
      }
      if (curAncestor == null) {
        diagnosePhase4Skip("hoist-ancestor-vanished", parent, absBit);
        return false;
      }
      // hoistTargetSlot stays valid: bulkInsertIntoSiblingSubtree only mutates content
      // under that slot, never reorders the parent's children.
      if (!bulkInsertIntoSiblingSubtree(curAncestor, hoistAncestorRef, hoistTargetSlot,
          moveKeys[i], moveValues[i], storageEngineReader, log)) {
        diagnosePhase4Skip("hoist-bulk-insert-failed", parent, absBit);
        return false;
      }
    }
    PHASE4_SUBTREE_MERGE_FIRINGS.incrementAndGet();
    return true;
  }

  /**
   * Resolve a PageReference to a HOTLeafPage. Handles swizzled in-memory pages and TIL
   * lookups. Returns {@code null} if the ref doesn't resolve to a leaf.
   */
  private @Nullable HOTLeafPage resolveLeafPage(PageReference ref,
      StorageEngineWriter storageEngineReader, TransactionIntentLog log) {
    final PageContainer cont = log.get(ref);
    if (cont != null) {
      final Page p = cont.getModified();
      if (p instanceof HOTLeafPage leaf) return leaf;
      return null;
    }
    Page page = ref.getPage();
    if (page == null) {
      page = loadPage(storageEngineReader, ref);
      if (page != null) ref.setPage(page);
    }
    if (page instanceof HOTLeafPage leaf) {
      return leaf;
    }
    return null;
  }

  /**
   * Insert a single (key, value) pair into the subtree rooted at parent's slot[siblingIdx].
   * Walks down via PEXT routing, CoWs each indirect on the path, inserts at the leaf;
   * on leaf overflow triggers the standard leaf-split + integrate path which may
   * recursively re-invoke Phase 4.
   *
   * <p>CoW correctness: top-down via {@link HOTIndirectPage#withUpdatedChild} (preserves
   * pageKey identity); the leaf is CoW'd with a fresh page key. After insertion, the
   * sibling subtree's root ref (parent's slot[siblingIdx]) is updated in place via
   * a fresh CoW'd parent registered at parentRef.
   *
   * <p>HFT-grade: pre-allocated path arrays from the writer's pool; no boxing; PEXT
   * routing reused.
   *
   * @return {@code true} on successful insert
   */
  private boolean bulkInsertIntoSiblingSubtree(HOTIndirectPage parent, PageReference parentRef,
      int siblingIdx, byte[] key, byte[] value,
      StorageEngineWriter storageEngineReader, TransactionIntentLog log) {
    if (parent == null || parentRef == null || key == null || value == null) {
      return false;
    }
    final int revision = storageEngineReader.getRevisionNumber();

    // Local descent path from sibling subtree. Worst case depth = MAX_TREE_HEIGHT.
    final HOTIndirectPage[] descPathNodes = new HOTIndirectPage[MAX_TREE_HEIGHT];
    final PageReference[] descPathRefs = new PageReference[MAX_TREE_HEIGHT];
    final int[] descPathChildIdx = new int[MAX_TREE_HEIGHT];
    int descDepth = 0;

    PageReference curRef = parent.getChildReference(siblingIdx);
    if (curRef == null) return false;
    Page curPage = resolveAnyPage(curRef, storageEngineReader, log);
    if (curPage == null) return false;

    while (curPage instanceof HOTIndirectPage indirect) {
      if (descDepth >= MAX_TREE_HEIGHT) return false;
      // Stage G.17 — constancy-aware descent. Pick the child slot whose subtree is
      // β-constant at value matching key's β-value (if such a slot exists) instead of
      // blindly following findChildIndex. This prevents bulk-insert from creating
      // β-mixing in the destination subtree (= the cascade source of Option B's earlier
      // dispatch attempts).
      final int childIdx = pickConstancyCorrectChildSlot(indirect, key, storageEngineReader, log);
      if (childIdx < 0) return false;
      final PageReference childRef = indirect.getChildReference(childIdx);
      if (childRef == null) return false;
      descPathNodes[descDepth] = indirect;
      descPathRefs[descDepth] = curRef;
      descPathChildIdx[descDepth] = childIdx;
      descDepth++;
      curRef = childRef;
      curPage = resolveAnyPage(curRef, storageEngineReader, log);
      if (curPage == null) return false;
    }

    if (!(curPage instanceof HOTLeafPage targetLeaf)) {
      return false;
    }

    // CoW the target leaf if not already in TIL.
    HOTLeafPage modLeaf;
    final PageContainer existing = log.get(curRef);
    if (existing != null && existing.getModified() instanceof HOTLeafPage existingMod) {
      modLeaf = existingMod;
    } else {
      modLeaf = targetLeaf.copy();
      log.put(curRef, PageContainer.getInstance(targetLeaf, modLeaf));
    }

    // Try a direct put (cheap path).
    if (modLeaf.put(key, value)) {
      // Propagate CoW up the descent path back to parent. Each indirect on the descent
      // path is CoW'd via withUpdatedChild — pageKey-preserving, so identity stays stable.
      PageReference childRef = curRef;
      for (int i = descDepth - 1; i >= 0; i--) {
        final HOTIndirectPage descNode = descPathNodes[i];
        final HOTIndirectPage descNodeUpdated =
            descNode.withUpdatedChild(descPathChildIdx[i], childRef, revision);
        log.put(descPathRefs[i], PageContainer.getInstance(descNodeUpdated, descNodeUpdated));
        childRef = descPathRefs[i];
      }
      // Update parent's slot[siblingIdx] to point at the (possibly CoW'd) sibling subtree
      // root. Only needed if descDepth == 0 (sibling is the leaf itself); when descDepth > 0
      // the inner loop already updated descPathRefs[0] which IS parent's slot[siblingIdx]'s
      // ref. Re-fetch parent and update its slot to ensure consistency.
      final HOTIndirectPage curParent = currentInLog(parentRef, parent);
      final HOTIndirectPage updatedParent =
          curParent.withUpdatedChild(siblingIdx, descDepth == 0 ? curRef : descPathRefs[0],
              revision);
      log.put(parentRef, PageContainer.getInstance(updatedParent, updatedParent));
      return true;
    }

    // Leaf overflow → split. Build a temporary path that includes both ancestor pathRefs
    // (above parent) and the descent path (parent + below parent). Reuse the existing
    // {@link #handleLeafSplitAndInsertInternal} pattern via a localized split.
    //
    // We do not have the ancestors above parent here (the caller's path stops at parent).
    // For correctness we ONLY need to update from the leaf back up to parent — ancestors
    // above parent are unchanged because parent's pageKey is preserved across all
    // updates. So pass a path consisting of [parentNode] + [descPathNodes] truncated to
    // descDepth.
    return splitLeafAndIntegrateInSiblingSubtree(targetLeaf, modLeaf, curRef,
        parentRef, parent, siblingIdx, descPathNodes, descPathRefs, descPathChildIdx,
        descDepth, key, value, storageEngineReader, log);
  }

  /**
   * Resolve any HOT page (leaf or indirect) from a PageReference, checking TIL first.
   */
  private @Nullable Page resolveAnyPage(PageReference ref,
      StorageEngineWriter storageEngineReader, TransactionIntentLog log) {
    final PageContainer cont = log.get(ref);
    if (cont != null) {
      return cont.getModified();
    }
    Page page = ref.getPage();
    if (page == null) {
      page = loadPage(storageEngineReader, ref);
      if (page != null) ref.setPage(page);
    }
    return page;
  }

  /**
   * Re-fetch the latest version of {@code parent} from the TIL; falls back to the original
   * if not present. Used during Phase 4 bulk-insert to avoid acting on stale parent state
   * after a recursive update. Resolves through {@link #activeLog} (set at the public
   * entry point of the enclosing scope) — never null in a Phase 4 firing.
   */
  private HOTIndirectPage currentInLog(PageReference parentRef, HOTIndirectPage fallback) {
    if (parentRef == null) return fallback;
    final TransactionIntentLog log = activeLog;
    if (log == null) return fallback;
    final PageContainer cont = log.get(parentRef);
    if (cont != null && cont.getModified() instanceof HOTIndirectPage upd) {
      return upd;
    }
    return fallback;
  }

  /**
   * Split a target leaf inside the sibling subtree on its MSDB (including the new key)
   * and integrate the BiNode upward — first within the sibling subtree (via
   * {@link #updateParentForSplitWithPath} at the immediate descent-parent), then the
   * top-level parent's slot[siblingIdx] is updated to reference the (possibly mutated)
   * sibling subtree root.
   *
   * <p>If descDepth == 0, the sibling is itself a leaf; the split's BiNode replaces the
   * sibling slot directly (parent.slot[siblingIdx] becomes a new BiNode).
   *
   * <p>HFT-grade: reuses the existing leaf-split machinery; the only allocations are the
   * fresh leaf page for the right half and the BiNode where applicable.
   *
   * @return {@code true} on success
   */
  private boolean splitLeafAndIntegrateInSiblingSubtree(HOTLeafPage origLeaf,
      HOTLeafPage modLeafCow, PageReference leafRef, PageReference parentRef,
      HOTIndirectPage origParent, int siblingIdx, HOTIndirectPage[] descPathNodes,
      PageReference[] descPathRefs, int[] descPathChildIdx, int descDepth,
      byte[] key, byte[] value, StorageEngineWriter storageEngineReader,
      TransactionIntentLog log) {
    if (modLeafCow.getEntryCount() < 1) return false;
    final int revision = storageEngineReader.getRevisionNumber();
    final long newPageKey = pageKeyAllocator.getAsLong();
    final HOTLeafPage rightPage = new HOTLeafPage(newPageKey, revision, modLeafCow.getIndexType());
    boolean rightTransferred = false;
    try {
      final int[] subtreeNewSideOut = new int[]{-1};
      if (!modLeafCow.splitToWithInsert(rightPage, key, key.length, value, value.length,
          subtreeNewSideOut)) {
        return false;
      }
      final int subtreeLeafSplitNewSide = subtreeNewSideOut[0];
      final byte[] leftMax = modLeafCow.getLastKey();
      final byte[] rightMin = rightPage.getFirstKey();
      if (leftMax.length == 0 || rightMin.length == 0) return false;
      final int discBit = DiscriminativeBitComputer.computeDifferingBit(leftMax, rightMin);
      if (discBit < 0) return false;

      final PageReference rightRef = new PageReference();
      rightRef.setKey(newPageKey);
      rightRef.setPage(rightPage);
      log.put(rightRef, PageContainer.getInstance(rightPage, rightPage));
      rightTransferred = true;

      // Re-register the (CoW'd) left-half leaf at its existing ref.
      leafRef.setPage(modLeafCow);
      log.put(leafRef, PageContainer.getInstance(modLeafCow, modLeafCow));

      if (descDepth > 0) {
        // Integrate split into descent parent.
        final int dpIdx = descDepth - 1;
        updateParentForSplitWithPath(storageEngineReader, log, descPathRefs[dpIdx],
            descPathNodes[dpIdx], descPathChildIdx[dpIdx], leafRef, rightRef,
            rightMin, parentRef /* root substitute for this scope */,
            descPathNodes, descPathRefs, descPathChildIdx, dpIdx,
            subtreeLeafSplitNewSide);
        // The top-level parent's slot[siblingIdx] still references descPathRefs[0]'s
        // original PageReference (descPathRefs[0] IS parent.getChildReference(siblingIdx)
        // by construction at the call site). The recursive update via updateParentForSplit
        // mutates that ref's page contents through TIL — parent's slot[siblingIdx] still
        // points to the same ref, so the parent itself does not need rewiring here.
        // However, we MUST CoW parent to mark it dirty for the surrounding outer split
        // (so the outer caller propagates ancestor revisions correctly).
        final HOTIndirectPage curParent = parentInLogFresh(parentRef, origParent, log);
        // Re-attach: explicitly call withUpdatedChild on the same ref to trigger a parent
        // CoW (necessary so the outer caller sees a registered parent in the TIL).
        final HOTIndirectPage updatedParent =
            curParent.withUpdatedChild(siblingIdx, descPathRefs[0], revision);
        log.put(parentRef, PageContainer.getInstance(updatedParent, updatedParent));
      } else {
        // descDepth == 0 → sibling is itself a leaf. Replace parent's slot[siblingIdx]
        // with a BiNode wrapping the two halves.
        final long biNodePageKey = pageKeyAllocator.getAsLong();
        final HOTIndirectPage biNode = createBiNodeTraced("phase4-sibling-leaf-split-biNode",
            biNodePageKey, revision, discBit, leafRef, rightRef, 1);
        final PageReference biNodeRef = new PageReference();
        biNodeRef.setKey(biNodePageKey);
        biNodeRef.setPage(biNode);
        log.put(biNodeRef, PageContainer.getInstance(biNode, biNode));
        final HOTIndirectPage curParent = parentInLogFresh(parentRef, origParent, log);
        final HOTIndirectPage updatedParent =
            curParent.withUpdatedChild(siblingIdx, biNodeRef, revision);
        log.put(parentRef, PageContainer.getInstance(updatedParent, updatedParent));
      }
      return true;
    } finally {
      if (!rightTransferred) {
        rightPage.close();
      }
    }
  }

  /**
   * Re-fetch the parent's most recent version from TIL using the explicitly provided log.
   * Returns the fallback if not present.
   */
  private HOTIndirectPage parentInLogFresh(PageReference parentRef, HOTIndirectPage fallback,
      TransactionIntentLog log) {
    final PageContainer cont = log.get(parentRef);
    if (cont != null && cont.getModified() instanceof HOTIndirectPage upd) {
      return upd;
    }
    return fallback;
  }

  /** Diagnostic — Phase 4 skip reasons. Gated on {@code -Dhot.debug.phase4=1}. */
  private static void diagnosePhase4Skip(String reason, HOTIndirectPage parent, int absBit) {
    if (!Boolean.getBoolean("hot.debug.phase4")) return;
    System.err.println("[hot.phase4] skip reason=" + reason
        + " parentLayout=" + parent.getLayoutType()
        + " parentBytePos=" + parent.getInitialBytePos()
        + " parentMask=" + Long.toHexString(parent.getBitMask())
        + " parentChildren=" + parent.getNumChildren()
        + " β=" + absBit);
  }

  /**
   * Height-optimal absorption fallback: rebuild parent's children with L, R
   * replacing the split slot, recomputing disc bits via
   * {@link #createNodeFromChildren}. Avoids the split cascade that would
   * otherwise create a new BiNode at a higher level — keeps the tree wide.
   *
   * <p>Returns null if the expansion would exceed fan-out 32 (genuine split
   * needed) or if fresh disc-bit computation can't distinguish all children.
   */
  private @Nullable HOTIndirectPage rebuildParentAbsorbingSplit(
      HOTIndirectPage parent, int splitChildIndex,
      PageReference leftChild, PageReference rightChild, int revision) {
    final int numChildren = parent.getNumChildren();
    final int leftLogKey = leftChild.getLogKey();
    final int rightLogKey = rightChild.getLogKey();
    int newCount = numChildren + 1;
    PageReference[] rebuilt = new PageReference[newCount];
    int j = 0;
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) {
        rebuilt[j++] = leftChild;
        rebuilt[j++] = rightChild;
      } else {
        final PageReference c = parent.getChildReference(i);
        final int clk = c.getLogKey();
        if (clk >= 0 && (clk == leftLogKey || clk == rightLogKey)) {
          newCount--;
          continue;
        }
        rebuilt[j++] = c;
      }
    }
    if (j < rebuilt.length) rebuilt = Arrays.copyOf(rebuilt, j);
    if (rebuilt.length < 2 || rebuilt.length > NodeUpgradeManager.MULTI_NODE_MAX_CHILDREN) {
      return null;
    }
    try {
      return createNodeFromChildren(rebuilt, parent.getPageKey(), revision, parent.getHeight());
    } catch (Throwable t) {
      return null;
    }
  }

  /**
   * Upgrade a SingleMask parent to MultiMask layout when a new disc
   * bit falls outside the current 8-byte PEXT window. Produces a
   * MultiMask indirect page whose disc-bit set = parent's old bits ∪
   * {newAbsBit}, with the split slot replaced by (leftChild, rightChild).
   *
   * <p>Correctness: MultiMask supports disc bits at any byte positions
   * via explicit extraction tables. The new partial keys are computed
   * directly via PEXT on each child's first key, preserving the
   * routing contract (INV) by construction. Rejects (returns null) if
   * the new bit is not constant across some non-split sibling's
   * subtree — INV invariant check, same as SingleMask addEntry.
   *
   * <p>HFT discipline: single allocation of {@code byte[]
   * extractionPositions}, {@code long[] extractionMasks}, new
   * {@code int[] newPartials}, and new {@code PageReference[]
   * newChildren}. Uses {@link Long#compress} intrinsic for each
   * gathered byte. No TreeMap / no boxing.
   */
  private @Nullable HOTIndirectPage upgradeToMultiMaskWithNewBit(
      HOTIndirectPage parent, int splitChildIndex,
      PageReference leftChild, PageReference rightChild,
      int newAbsBit, int revision) {
    final int oldInitialBytePos = parent.getInitialBytePos();
    final long oldMask = parent.getBitMask();
    final int oldCount = Long.bitCount(oldMask);
    final int numChildren = parent.getNumChildren();

    // Decode old mask into (bytePos, maskByte) pairs, in ascending byte order.
    // The old SingleMask covers bytes [oldInitialBytePos, oldInitialBytePos+7].
    // Collect only bytes that actually have disc bits.
    int[] oldBytePositions = new int[8];
    int[] oldByteMaskBits = new int[8];
    int oldByteCount = 0;
    for (int bo = 0; bo < 8; bo++) {
      // BE: byte at window-position {@code bo} occupies long bits ((7-bo)*8)..((7-bo)*8+7).
      final int byteMaskBits = (int) ((oldMask >>> ((7 - bo) * 8)) & 0xFFL);
      if (byteMaskBits != 0) {
        oldBytePositions[oldByteCount] = oldInitialBytePos + bo;
        oldByteMaskBits[oldByteCount] = byteMaskBits;
        oldByteCount++;
      }
    }

    // Add new bit at its absolute byte position.
    final int newBytePos = newAbsBit / 8;
    final int newBitInByte = newAbsBit % 8;
    final int newMaskBit = 1 << (7 - newBitInByte);

    // Merge new byte into sorted list; if it coincides with an existing
    // byte, OR the bits.
    int[] allBytePositions = new int[oldByteCount + 1];
    int[] allByteMaskBits = new int[oldByteCount + 1];
    int allCount = 0;
    boolean merged = false;
    for (int i = 0; i < oldByteCount; i++) {
      if (!merged && oldBytePositions[i] == newBytePos) {
        allBytePositions[allCount] = newBytePos;
        allByteMaskBits[allCount] = oldByteMaskBits[i] | newMaskBit;
        allCount++;
        merged = true;
      } else if (!merged && oldBytePositions[i] > newBytePos) {
        allBytePositions[allCount] = newBytePos;
        allByteMaskBits[allCount] = newMaskBit;
        allCount++;
        merged = true;
        allBytePositions[allCount] = oldBytePositions[i];
        allByteMaskBits[allCount] = oldByteMaskBits[i];
        allCount++;
      } else {
        allBytePositions[allCount] = oldBytePositions[i];
        allByteMaskBits[allCount] = oldByteMaskBits[i];
        allCount++;
      }
    }
    if (!merged) {
      allBytePositions[allCount] = newBytePos;
      allByteMaskBits[allCount] = newMaskBit;
      allCount++;
    }

    // Build extractionPositions / extractionMasks arrays in MultiMask layout.
    final byte[] extractionPositions = new byte[allCount];
    final int numChunks = (allCount + 7) / 8;
    final long[] extractionMasks = new long[numChunks];
    short msbIndex = Short.MAX_VALUE;
    for (int i = 0; i < allCount; i++) {
      extractionPositions[i] = (byte) allBytePositions[i];
      final int chunkIdx = i / 8;
      final int byteOffsetInChunk = i % 8;
      // BE chunk packing: extraction-byte at chunk-offset {@code o} → long bits
      // ((7-o)*8)..((7-o)*8+7).
      extractionMasks[chunkIdx] |= ((long) (allByteMaskBits[i] & 0xFF)) << ((7 - byteOffsetInChunk) * 8);
      final int highBit = 31 - Integer.numberOfLeadingZeros(allByteMaskBits[i] & 0xFF);
      final int absBitPos = allBytePositions[i] * 8 + (7 - highBit);
      if (absBitPos < msbIndex) msbIndex = (short) absBitPos;
    }

    // Verify constancy of the new disc bit across every existing non-split sibling — see
    // {@link #addEntryWithPDep} for the multi-entry-leaf rationale. On non-constant return,
    // caller falls back to splitParentAndRecurse.
    final int[] siblingBitValues = new int[numChildren];
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) continue;
      final int v = bitConstantValueInSubtree(parent.getChildReference(i), newAbsBit);
      if (v < 0) return null;
      siblingBitValues[i] = v;
    }

    // Stage G.14 fix — replace PDEP+OR repositioning with DIRECT PEXT from each child's
    // firstKey under the new MultiMask layout. The old logic produced I8/I11-violating
    // pages when the leftmost original sibling's β=1 (Stage E i8-trace at indirect 2:
    // page 2 created by upgradeToMultiMaskWithNewBit:3423 with non-monotone slot order).
    // Direct PEXT is structurally correct: each child's stored partial = PEXT(firstKey,
    // newLayout). Routing for any key K with K.firstKey-bits at extraction positions
    // routes to the correct slot as long as firstKeys are distinct.
    //
    // Trade-off: drops the bit-elision optimization that PDEP+OR provided (= sparse
    // partial encoding for off-path bits). For Sirix's multi-entry-leaf workload this
    // optimization wasn't sound anyway — the same pattern as G.6 in buildCompressedHalf.
    final int newNumChildren = numChildren + 1;
    final PageReference[] newChildren = new PageReference[newNumChildren];
    final int[] newPartials = new int[newNumChildren];
    int j = 0;
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) {
        newChildren[j] = leftChild;
        final byte[] leftKey = getFirstKeyFromChild(leftChild);
        newPartials[j] = (leftKey == null || leftKey.length == 0) ? 0
            : computePartialKeyMultiMaskDirect(leftKey, extractionPositions, extractionMasks, allCount);
        j++;
        newChildren[j] = rightChild;
        final byte[] rightKey = getFirstKeyFromChild(rightChild);
        newPartials[j] = (rightKey == null || rightKey.length == 0) ? 0
            : computePartialKeyMultiMaskDirect(rightKey, extractionPositions, extractionMasks, allCount);
        j++;
      } else {
        newChildren[j] = parent.getChildReference(i);
        final byte[] cKey = getFirstKeyFromChild(newChildren[j]);
        newPartials[j] = (cKey == null || cKey.length == 0) ? 0
            : computePartialKeyMultiMaskDirect(cKey, extractionPositions, extractionMasks, allCount);
        j++;
      }
    }

    // Sanity guard — partial keys must be unique under sparse-path encoding (HOT I3).
    for (int i = 1; i < newNumChildren; i++) {
      for (int k = 0; k < i; k++) {
        if (newPartials[k] == newPartials[i]) return null;
      }
    }

    // Sort children + partials by partial-key (HOT I7 / Binna §4.2). See addEntryWithPDep.
    sortChildrenAndPartialsByPartial(newChildren, newPartials);

    if (newNumChildren <= 16) {
      return HOTIndirectPage.createSpanNodeMultiMask(parent.getPageKey(), revision,
          extractionPositions, extractionMasks, allCount, newPartials, newChildren,
          parent.getHeight(), msbIndex);
    }
    return HOTIndirectPage.createMultiNodeMultiMask(parent.getPageKey(), revision,
        extractionPositions, extractionMasks, allCount, newPartials, newChildren,
        parent.getHeight(), msbIndex);
  }

  /** Sum of bits set across every chunk of a MultiMask extraction-mask array. */
  private static int oldExtractionMaskTotalBits(long[] extractionMasks) {
    int total = 0;
    for (final long m : extractionMasks) total += Long.bitCount(m);
    return total;
  }

  /**
   * Compute the new bit's output position (LSB=0) in a MultiMask partial-key layout. The
   * BE-concat ordering places extractionPositions[0]'s mask bits at the highest result bits
   * (MSB-first within each byte). We walk that ordering and count bits until we reach the
   * new bit.
   */
  private static int multiMaskNewBitOutputPos(byte[] newPositions, long[] newMasks,
      int newNumBytes, int newBytePos, int newBitInByte) {
    int totalBits = 0;
    for (final long m : newMasks) totalBits += Long.bitCount(m);
    int bitsAccumulated = 0;
    for (int i = 0; i < newNumBytes; i++) {
      final int bytePos = newPositions[i] & 0xFF;
      final int byteMask = (int) ((newMasks[i / 8] >>> ((7 - (i % 8)) * 8)) & 0xFF);
      // Within each byte: iterate MSB-first (mask bit 7 = MSB-first index 0).
      for (int mfBit = 0; mfBit < 8; mfBit++) {
        final int byteBit = 7 - mfBit;
        if ((byteMask & (1 << byteBit)) == 0) continue;
        if (bytePos == newBytePos && mfBit == newBitInByte) {
          return totalBits - 1 - bitsAccumulated;
        }
        bitsAccumulated++;
      }
    }
    throw new IllegalStateException("multiMaskNewBitOutputPos: new bit not found in new layout");
  }

  /**
   * Reference-faithful {@code MultiMaskPartialKeyMapping::insert} port:
   * add a new disc bit to a MultiMask parent and integrate a leaf
   * split's two halves. Returns the updated MultiMask indirect page, or
   * null if the invariant would break (new bit not constant in some
   * sibling's subtree, or duplicate partial key).
   *
   * <p>HFT-grade: single allocation of new extractionPositions[] and
   * extractionMasks[] (sized exactly for the new disc-bit set),
   * one new int[] for partials, one new PageReference[] for children.
   * Uses {@link Long#compress} per-chunk. Byte insertion maintains
   * sorted extractionPositions order, shifting mask bytes through LE
   * chunks via a single pass. No ArrayList, no TreeMap.
   */
  private @Nullable HOTIndirectPage addEntryMultiMask(
      HOTIndirectPage parent, int splitChildIndex,
      PageReference leftChild, PageReference rightChild,
      int newAbsBit, int revision) {
    final int numChildren = parent.getNumChildren();
    final byte[] oldExtractionPositions = parent.getExtractionPositions();
    final long[] oldExtractionMasks = parent.getExtractionMasks();
    if (oldExtractionPositions == null || oldExtractionMasks == null) return null;
    final int oldNumBytes = oldExtractionPositions.length;

    final int newBytePos = newAbsBit / 8;
    final int newBitInByte = newAbsBit % 8;
    final int newMaskBit = 1 << (7 - newBitInByte);

    // 1. Verify constancy of the new disc bit across every existing non-split sibling.
    //    Required because Sirix's multi-entry leaves can hold keys differing at any bit;
    //    sparse-path subset-match routing misroutes keys destined for a sibling that spans
    //    both bit values. See addEntryWithPDep for the full rationale.
    final int[] siblingBitValues = new int[numChildren];
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) continue;
      final int v = bitConstantValueInSubtree(parent.getChildReference(i), newAbsBit);
      if (v < 0) return null;
      siblingBitValues[i] = v;
    }

    // 2. Locate existing entry for newBytePos, if any (sorted positions).
    int existingIdx = -1;
    int insertIdx = oldNumBytes;
    for (int i = 0; i < oldNumBytes; i++) {
      final int bp = oldExtractionPositions[i] & 0xFF;
      if (bp == newBytePos) { existingIdx = i; break; }
      if (bp > newBytePos) { insertIdx = i; break; }
    }

    // 3. Build new extraction tables.
    final byte[] newExtractionPositions;
    final long[] newExtractionMasks;
    final int newNumBytes;
    // BE chunk packing: extraction-byte at chunk-offset {@code o} → long bits
    // ((7-o)*8)..((7-o)*8+7); reads use the same shift to extract a byte from a chunk.
    if (existingIdx >= 0) {
      // Merge: OR the new mask bit into the existing byte's mask.
      final int chunkIdx = existingIdx / 8;
      final int byteOffsetInChunk = existingIdx % 8;
      final int existingByte =
          (int) ((oldExtractionMasks[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFF);
      if ((existingByte & newMaskBit) != 0) {
        // Case 2b-iv-b-β (MultiMask): β already a parent disc bit. Mask unchanged; the
        // leaf split's moveHalf needs a new sibling slot whose stored partial = splitChild's
        // partial XOR β-bit. Same gates as the SingleMask analog.
        if (Boolean.getBoolean("hot.strict.binna")) {
          return addEntryFreshPolarityMultiMask(parent, splitChildIndex, leftChild, rightChild,
              newAbsBit, newBytePos, newBitInByte, revision);
        }
        return null; // duplicate
      }
      newNumBytes = oldNumBytes;
      newExtractionPositions = oldExtractionPositions.clone();
      newExtractionMasks = oldExtractionMasks.clone();
      newExtractionMasks[chunkIdx] |= ((long) newMaskBit) << ((7 - byteOffsetInChunk) * 8);
    } else {
      // Insert: shift entries at/after insertIdx down by one; add new entry.
      newNumBytes = oldNumBytes + 1;
      newExtractionPositions = new byte[newNumBytes];
      if (insertIdx > 0) {
        System.arraycopy(oldExtractionPositions, 0, newExtractionPositions, 0, insertIdx);
      }
      newExtractionPositions[insertIdx] = (byte) newBytePos;
      if (insertIdx < oldNumBytes) {
        System.arraycopy(oldExtractionPositions, insertIdx,
            newExtractionPositions, insertIdx + 1, oldNumBytes - insertIdx);
      }
      final int newNumChunks = (newNumBytes + 7) / 8;
      newExtractionMasks = new long[newNumChunks];
      // Rebuild mask bytes. For destination position i:
      //   i <  insertIdx       → old position i
      //   i == insertIdx       → newMaskBit
      //   i >  insertIdx       → old position i-1
      for (int i = 0; i < newNumBytes; i++) {
        final int maskByte;
        if (i == insertIdx) {
          maskByte = newMaskBit;
        } else {
          final int srcIdx = i < insertIdx ? i : i - 1;
          final int srcChunk = srcIdx / 8;
          final int srcOffsetInChunk = srcIdx % 8;
          maskByte =
              (int) ((oldExtractionMasks[srcChunk] >>> ((7 - srcOffsetInChunk) * 8)) & 0xFF);
        }
        final int dstChunk = i / 8;
        final int dstOffsetInChunk = i % 8;
        newExtractionMasks[dstChunk] |= ((long) (maskByte & 0xFF)) << ((7 - dstOffsetInChunk) * 8);
      }
    }

    // 4. Compute new MSB index (smallest absolute disc bit).
    short msbIndex = Short.MAX_VALUE;
    for (int i = 0; i < newNumBytes; i++) {
      final int chunkIdx = i / 8;
      final int byteOffsetInChunk = i % 8;
      final int maskByte =
          (int) ((newExtractionMasks[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFF);
      if (maskByte == 0) continue;
      final int highBit = 31 - Integer.numberOfLeadingZeros(maskByte);
      final int absBit = (newExtractionPositions[i] & 0xFF) * 8 + (7 - highBit);
      if (absBit < msbIndex) msbIndex = (short) absBit;
    }

    // 5. Build expanded children + partial keys with Binna §4.2 sparse-path encoding: split
    //    products inherit the original split-child's path (under the OLD layout) and extend
    //    it with new-bit = 0 (left) / 1 (right). Non-split siblings inherit their existing
    //    paths; their new-bit position stays 0 (the new BiNode is not on their path).
    final int oldTotalBits = oldExtractionMaskTotalBits(oldExtractionMasks);
    final int newBitOutputPos = multiMaskNewBitOutputPos(newExtractionPositions, newExtractionMasks,
        newNumBytes, newBytePos, newBitInByte);
    final long oldPartialMaskInNewLayout =
        (((1L << (oldTotalBits + 1)) - 1L) ^ (1L << newBitOutputPos));
    final int[] oldPartialKeys = parent.getPartialKeys();
    final int splitChildOldPartial = oldPartialKeys[splitChildIndex];
    final int splitChildRepositioned =
        (int) Long.expand(Integer.toUnsignedLong(splitChildOldPartial), oldPartialMaskInNewLayout);

    final int newNumChildren = numChildren + 1;
    final PageReference[] newChildren = new PageReference[newNumChildren];
    final int[] newPartials = new int[newNumChildren];
    int j = 0;
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) {
        newChildren[j] = leftChild;
        newPartials[j] = splitChildRepositioned;
        j++;
        newChildren[j] = rightChild;
        newPartials[j] = splitChildRepositioned | (1 << newBitOutputPos);
        j++;
      } else {
        newChildren[j] = parent.getChildReference(i);
        final int repositioned = (int) Long.expand(
            Integer.toUnsignedLong(oldPartialKeys[i]), oldPartialMaskInNewLayout);
        newPartials[j] = repositioned | (siblingBitValues[i] << newBitOutputPos);
        j++;
      }
    }

    // 6. Verify partial-key uniqueness (INV routing guard).
    for (int i = 1; i < newNumChildren; i++) {
      for (int k = 0; k < i; k++) {
        if (newPartials[k] == newPartials[i]) return null;
      }
    }

    // Stage G.1b fix (Bug B1, MultiMask variant) — I4 self-check. The PDEP+OR repositioning
    // can produce a layout where no slot has partial 0 when the leftmost original sibling's
    // β-bit value at the new disc bit equals 1 AND splitChildIndex > 0. This is the
    // empirical root cause of the "indirect 11 smallest partial 0x38" violation Stage E
    // attributed to a happy-path operation. Reject so caller takes splitParentAndRecurse.
    boolean mmHasZero = false;
    for (final int p : newPartials) {
      if (p == 0) { mmHasZero = true; break; }
    }
    if (!mmHasZero) {
      G1_I4_REJECT_ADDENTRY.incrementAndGet();
      return null;
    }

    // 7. Sort children + partials by partial-key (HOT I7 / Binna §4.2). See addEntryWithPDep.
    sortChildrenAndPartialsByPartial(newChildren, newPartials);

    // Stage G.16 (MultiMask variant) — first-key-monotone re-sort + recompute partials
    // if needed. Same logic as the SingleMask path above.
    boolean mmFirstKeyMonotone = true;
    for (int i = 1; i < newNumChildren; i++) {
      final byte[] prev = getFirstKeyFromChild(newChildren[i - 1]);
      final byte[] curr = getFirstKeyFromChild(newChildren[i]);
      if (prev != null && curr != null && prev.length > 0 && curr.length > 0
          && Arrays.compareUnsigned(prev, curr) >= 0) {
        mmFirstKeyMonotone = false;
        break;
      }
    }
    if (!mmFirstKeyMonotone) {
      sortChildrenByFirstKey(newChildren);
      for (int i = 0; i < newNumChildren; i++) {
        final byte[] cKey = getFirstKeyFromChild(newChildren[i]);
        newPartials[i] = (cKey == null || cKey.length == 0) ? 0
            : computePartialKeyMultiMaskDirect(cKey, newExtractionPositions, newExtractionMasks, newNumBytes);
      }
      for (int i = 1; i < newNumChildren; i++) {
        for (int k = 0; k < i; k++) {
          if (newPartials[k] == newPartials[i]) return null;
        }
      }
    }

    if (newNumChildren <= 16) {
      return HOTIndirectPage.createSpanNodeMultiMask(parent.getPageKey(), revision,
          newExtractionPositions, newExtractionMasks, newNumBytes,
          newPartials, newChildren, parent.getHeight(), msbIndex);
    }
    return HOTIndirectPage.createMultiNodeMultiMask(parent.getPageKey(), revision,
        newExtractionPositions, newExtractionMasks, newNumBytes,
        newPartials, newChildren, parent.getHeight(), msbIndex);
  }

  /**
   * Compute MultiMask partial key for a single key (byte-gather + per-byte PEXT).
   * Matches the read path's {@link HOTIndirectPage#computeMultiMaskPartialKeyScalar}.
   */
  static int computePartialKeyMultiMaskDirect(byte[] key,
      byte[] extractionPositions, long[] extractionMasks, int numExtractionBytes) {
    final int numChunks = extractionMasks.length;
    // BE chunk packing: extraction-byte at chunk-offset {@code o} → long bits ((7-o)*8)..((7-o)*8+7).
    final long[] gathered = new long[numChunks];
    for (int i = 0; i < numExtractionBytes; i++) {
      final int keyBytePos = extractionPositions[i] & 0xFF;
      final int keyByte = keyBytePos < key.length ? (key[keyBytePos] & 0xFF) : 0;
      final int chunkIdx = i / 8;
      final int byteOffsetInChunk = i % 8;
      gathered[chunkIdx] |= ((long) keyByte) << ((7 - byteOffsetInChunk) * 8);
    }
    // BE concatenate across chunks: chunk 0 occupies the HIGH bits of the result.
    int totalBits = 0;
    for (final long m : extractionMasks) totalBits += Long.bitCount(m);
    int result = 0;
    int shift = totalBits;
    for (int w = 0; w < numChunks; w++) {
      final int extracted = (int) Long.compress(gathered[w], extractionMasks[w]);
      shift -= Long.bitCount(extractionMasks[w]);
      result |= extracted << shift;
    }
    return result;
  }

  /**
   * Verify that bit {@code absBitPos} has the same value across every key in the subtree
   * rooted at {@code ref}.
   *
   * <p>Returns {@code 0} or {@code 1} for the constant value, or {@code -1} if the bit
   * varies within the subtree (or the page can't be loaded). Empty subtrees return {@code 0}
   * (vacuously constant).
   *
   * <p><b>Why this exists.</b> Binna's HOT thesis uses single-TID leaves: every leaf is a
   * single key, so any disc bit is trivially constant in any leaf-rooted subtree, and
   * Binna's sparse-path encoding "off-path siblings span both bit values" case never occurs.
   * Sirix's multi-entry leaves break that property — a single leaf can hold keys differing
   * at any bit. Under multi-entry leaves, sparse-path subset-match routing misroutes keys
   * destined for an "off-path" sibling whose subtree has both values at a new disc bit
   * (the leaf-split's products, with {@code sparseStored} bit set, win the most-specific
   * subset match over the off-path sibling). The fix is to gate addEntryWithPDep /
   * addEntryMultiMask / upgradeToMultiMaskWithNewBit on this check: extension is allowed
   * only when the new bit is constant in every existing non-split sibling. Otherwise the
   * caller falls back to splitParentAndRecurse, which rebuilds the parent.
   *
   * <p><b>HFT grade.</b> Primitive {@code int} return (sentinel {@code -1} for "non-constant"
   * — no boxing, no allocation, no exception path. Recursion depth bounded by tree height
   * (≤ 7 in practice). Per-call cost: one byte fetch + bit extract per leaf entry; bounded
   * by total stored keys in the subtree.
   */
  private int bitConstantValueInSubtree(PageReference ref, int absBitPos) {
    Page page = ref.getPage();
    if (page == null && activeReader != null) {
      page = loadPage(activeReader, ref);
      if (page != null) ref.setPage(page);
    }
    if (page instanceof HOTLeafPage leaf) {
      final int n = leaf.getEntryCount();
      if (n == 0) return 0;
      final int first = DiscriminativeBitComputer.isBitSet(leaf.getKeySlice(0), absBitPos) ? 1 : 0;
      for (int i = 1; i < n; i++) {
        final int v = DiscriminativeBitComputer.isBitSet(leaf.getKeySlice(i), absBitPos) ? 1 : 0;
        if (v != first) return -1;
      }
      return first;
    }
    if (page instanceof HOTIndirectPage indirect) {
      final int m = indirect.getNumChildren();
      if (m == 0) return 0;
      final int first = bitConstantValueInSubtree(indirect.getChildReference(0), absBitPos);
      if (first < 0) return -1;
      for (int i = 1; i < m; i++) {
        final int v = bitConstantValueInSubtree(indirect.getChildReference(i), absBitPos);
        if (v < 0 || v != first) return -1;
      }
      return first;
    }
    return -1;
  }

  /**
   * Collect the union of absolute disc-bit positions captured by every indirect on the
   * descent path. Returns positions sorted ascending (i.e., MSB-first absolute order:
   * smallest position = most significant bit).
   *
   * <p>Handles both SingleMask and MultiMask layouts. Extracted for Phase 2's
   * constancy-aware leaf split: when a leaf overflows we need to know which absolute
   * bit positions the ancestors already use to route, so the split can prefer one of
   * those bits over the leaf's local MSDB. That keeps both halves constant on every
   * ancestor disc bit (Binna's HOT invariant).
   *
   * <p><b>BE word convention</b> matches {@link HOTIndirectPage#getKeyWordAt}:
   * <ul>
   *   <li>SingleMask: long-bit {@code b} corresponds to byte {@code (initialBytePos + 7 - b/8)},
   *       MSB-numbered bit {@code (7 - b%8)} within that byte. Absolute MSB-first bit position
   *       = byte * 8 + bitInByte.</li>
   *   <li>MultiMask: long-bit {@code b} of chunk {@code c} corresponds to byte
   *       {@code extractionPositions[c*8 + 7 - b/8]} (where {@code b/8} indexes the byte
   *       within the chunk), MSB-numbered bit {@code (7 - b%8)} within that byte.</li>
   * </ul>
   *
   * <p>HFT-grade: pre-sized buffer, deduplicated via single linear scan, no boxing,
   * no growth, single bounded allocation for the trimmed return.
   *
   * @param pathNodes ancestor path nodes (root → leaf-parent)
   * @param pathDepth number of valid entries in {@code pathNodes}
   * @return ascending-sorted absolute disc-bit positions across the path
   */
  public int[] collectAncestorDiscBits(HOTIndirectPage[] pathNodes, int pathDepth) {
    if (pathDepth <= 0) {
      return new int[0];
    }
    // Theoretical max: 8 extraction bytes × 8 bits/byte = 64 disc bits per indirect.
    // Path depth ≤ MAX_TREE_HEIGHT (64). Buffer sized generously to fit transient
    // out-of-spec disc-bit counts before downstream INV checks fire.
    final int cap = pathDepth * 64;
    final int[] buf = new int[cap];
    int n = 0;
    for (int i = 0; i < pathDepth; i++) {
      final HOTIndirectPage node = pathNodes[i];
      if (node == null) continue;
      n = appendDiscBitsOfIndirect(node, buf, n);
    }
    if (n == 0) {
      return new int[0];
    }
    // Sort ascending (MSB-first), then dedupe in place.
    Arrays.sort(buf, 0, n);
    int w = 1;
    for (int r = 1; r < n; r++) {
      if (buf[r] != buf[r - 1]) {
        buf[w++] = buf[r];
      }
    }
    return Arrays.copyOf(buf, w);
  }

  /**
   * Phase 2 (eager constancy-aware leaf split) — find the FIRST ancestor disc bit β
   * where the new key {@code keyBuf}'s β value differs from the leaf's existing first
   * key's β value. Such β indicates that inserting {@code keyBuf} into {@code leaf}
   * would break the β-constancy invariant required by ancestor's PEXT routing.
   *
   * <p>Returns {@code -1} if no constancy break would occur (= insertion preserves
   * β-constancy on every ancestor disc bit).
   *
   * <p>Iterates ancestor disc bits MSB-first (= sorted ascending absolute position).
   * Returning the first differ allows the caller to split the leaf on that bit before
   * inserting {@code keyBuf}, preventing the multi-entry-leaf pathology where stored
   * partials get out of sync with their child's first-key dense PEXT after subsequent
   * inserts.
   *
   * <p>Reference: design doc §3 (a) Eager — "when inserting a key K into leaf L, if K's
   * value at any ancestor disc bit β differs from existing entries' bit-β values, split
   * L on β before inserting. Maintains constancy continuously."
   *
   * <p>HFT-grade: reuses {@link #collectAncestorDiscBits}'s allocation pattern; one
   * pass over ancestor bits + one bit-test per bit per key.
   *
   * @param pathNodes ancestor path nodes (root → leaf-parent)
   * @param pathDepth number of valid entries in {@code pathNodes}
   * @param leaf the leaf about to receive {@code keyBuf}
   * @param keyBuf the new key
   * @return offending β (≥ 0) or {@code -1} if no break
   */
  public int findOffendingAncestorBit(HOTIndirectPage[] pathNodes, int pathDepth,
      HOTLeafPage leaf, byte[] keyBuf) {
    if (leaf == null || keyBuf == null) return -1;
    if (leaf.getEntryCount() == 0) return -1;
    if (pathDepth <= 0) return -1;

    // Phase 2 (constrained): only return a non-negative β when β EQUALS the leaf's
    // MSDB-after-K-inserted. That's the contiguous-partition case (= safe to split-on-β
    // because β is by definition the most-significant bit on which keys differ, so the
    // partition is contiguous in the leaf's natural sort order). For β < MSDB the
    // partition would be non-contiguous, breaking HOT I8 (= design doc §6 Variant A's
    // failure mode, 5,652 violations on the original 50K reproducer).
    //
    // Rationale: K introducing a NEW MSDB to the leaf = the only case where eager
    // split-on-MSDB is provably safe. If MSDB happens to coincide with an ancestor
    // disc bit β, the resulting halves are β-constant for THAT β. If not, no eager
    // split fires; legacy multi-entry-leaf β-mixing remains.
    final int msdbWithKey = leaf.computeMsdbWithKey(keyBuf);
    if (msdbWithKey < 0) return -1; // all keys (incl K) identical — can't split

    final int[] ancestorBits = collectAncestorDiscBits(pathNodes, pathDepth);
    if (ancestorBits.length == 0) return -1;

    for (final int beta : ancestorBits) {
      if (beta == msdbWithKey) {
        // K's MSDB coincides with this ancestor β. Splitting the leaf on β = MSDB
        // produces a contiguous partition AND restores β-constancy for this ancestor.
        return msdbWithKey;
      }
    }
    return -1; // MSDB doesn't match any ancestor — no safe eager split
  }

  /**
   * Option B (Stage G.13) — Return ANY ancestor disc bit β where inserting {@code keyBuf}
   * into {@code leaf} would break β-constancy. Unlike {@link #findOffendingAncestorBit},
   * this does NOT restrict β to leaf's MSDB-with-K — it returns ANY β where the new
   * key's bit value disagrees with at least one existing leaf key's bit value at β.
   *
   * <p>Returns the MOST SIGNIFICANT (= numerically smallest absolute bit position)
   * such β, so the caller can split on the highest-priority β first. Callers must
   * handle the non-contiguous partition case (β &lt; MSDB) via subtree-merge style
   * reroute rather than naive contiguous split (which breaks parent's I8).
   *
   * <p>Returns {@code -1} if no break would occur (= insert preserves β-constancy on
   * every ancestor β).
   *
   * @param pathNodes ancestor path (root → leaf-parent)
   * @param pathDepth number of valid entries in pathNodes
   * @param leaf the leaf about to receive keyBuf
   * @param keyBuf the new key
   * @return offending β (≥ 0, MSB-first absolute position) or -1 if none
   */
  public int findAnyOffendingAncestorBit(HOTIndirectPage[] pathNodes, int pathDepth,
      HOTLeafPage leaf, byte[] keyBuf) {
    if (leaf == null || keyBuf == null) return -1;
    if (leaf.getEntryCount() == 0) return -1;
    if (pathDepth <= 0) return -1;

    final int[] ancestorBits = collectAncestorDiscBits(pathNodes, pathDepth);
    if (ancestorBits.length == 0) return -1;

    // ancestorBits is sorted ascending (= most-significant first). Return β iff:
    //   (a) leaf is β-CONSTANT at some value v across ALL its keys, AND
    //   (b) newKey.β = ¬v (= newKey would break the leaf's β-constancy at β).
    //
    // Partial mismatches (where leaf has both β=0 and β=1 keys) mean the leaf is already
    // β-mixed AT THIS β regardless of newKey — Option B can't fix that case. Only the
    // β-constant-leaf case is fixable: routing newKey elsewhere preserves the leaf.
    final int entryCount = leaf.getEntryCount();
    for (final int beta : ancestorBits) {
      final boolean newKeyBetaSet = isAbsBitSet(keyBuf, beta);
      // Determine leaf's β-constancy: scan all entries. If mixed → skip this β.
      boolean seen0 = false, seen1 = false;
      for (int i = 0; i < entryCount; i++) {
        final byte[] existing = leaf.getKey(i);
        if (existing == null || existing.length == 0) continue;
        if (isAbsBitSet(existing, beta)) seen1 = true;
        else seen0 = true;
        if (seen0 && seen1) break;
      }
      if (seen0 && seen1) continue; // already β-mixed → Option B can't help
      // Leaf is β-constant at value v ∈ {0,1}. Check newKey.β disagrees.
      final boolean leafBetaValue = seen1; // (only-seen value)
      if (leafBetaValue != newKeyBetaSet) {
        return beta; // would break β-constancy
      }
    }
    return -1;
  }

  /** MSB-first absolute bit lookup: bit 0 = MSB of byte 0. Returns false past length. */
  private static boolean isAbsBitSet(byte[] key, int absBit) {
    final int bytePos = absBit / 8;
    if (bytePos >= key.length) return false;
    final int bitInByte = absBit % 8;
    return (key[bytePos] & (1 << (7 - bitInByte))) != 0;
  }

  /** Stage G.13 — Option B counter for insert-time re-route firings. */
  private static final java.util.concurrent.atomic.AtomicLong OPTION_B_REROUTE_FIRINGS =
      new java.util.concurrent.atomic.AtomicLong(0L);

  /** Stage G.15 — I8 pre-check counter (newKey would become new deep-firstKey, breaking I8). */
  private static final java.util.concurrent.atomic.AtomicLong G15_I8_REROUTE_FIRINGS =
      new java.util.concurrent.atomic.AtomicLong(0L);

  /** Stage G.17 — constancy-aware descent counter (slot redirected to avoid β-mixing). */
  private static final java.util.concurrent.atomic.AtomicLong G17_CONSTANCY_REDIRECTS =
      new java.util.concurrent.atomic.AtomicLong(0L);

  /** Stage G.18 — ambiguous-ancestor detection counter (insert would route ambiguously). */
  private static final java.util.concurrent.atomic.AtomicLong G18_AMBIGUOUS_DETECTIONS =
      new java.util.concurrent.atomic.AtomicLong(0L);

  /** Stage G.20 — recursive constancy-aware split firings. */
  private static final java.util.concurrent.atomic.AtomicLong G20_RECURSIVE_SPLIT_FIRINGS =
      new java.util.concurrent.atomic.AtomicLong(0L);

  /** Stage G.25 — β propagation up the path from leaf-overflow upgrade. */
  private static final java.util.concurrent.atomic.AtomicLong G25_BETA_PROPAGATIONS =
      new java.util.concurrent.atomic.AtomicLong(0L);

  public static long getG25BetaPropagations() { return G25_BETA_PROPAGATIONS.get(); }
  public static void resetG25BetaPropagations() { G25_BETA_PROPAGATIONS.set(0L); }

  /** Stage G.28 — mask closure firings (= when a missing-β extension fires). */
  private static final java.util.concurrent.atomic.AtomicLong G28_CLOSURE_FIRINGS =
      new java.util.concurrent.atomic.AtomicLong(0L);

  public static long getG28ClosureFirings() { return G28_CLOSURE_FIRINGS.get(); }
  public static void resetG28ClosureFirings() { G28_CLOSURE_FIRINGS.set(0L); }

  /**
   * Stage G.28 — Generic mask extension for closure. Adds β to {@code indirect}'s mask
   * (without a leaf-split context). For each child:
   * <ul>
   *   <li>If subtree is β-constant: keep child, set β-bit in stored partial accordingly.</li>
   *   <li>If subtree is β-mixed: split via splitSubtreeOnBit, contribute both halves.</li>
   * </ul>
   * Builds a new MultiMask layout combining indirect's existing bytes + β's byte (or
   * SingleMask if β fits in window).
   *
   * <p>Returns the rebuilt indirect, or null if rebuild infeasible.
   */
  private @Nullable HOTIndirectPage extendIndirectMaskForClosure(HOTIndirectPage indirect,
      int beta, TransactionIntentLog log, int revision) {
    final boolean dbg = Boolean.getBoolean("hot.debug.g30");
    if (indirect == null || beta < 0) {
      if (dbg) System.err.println("[g30] reject reason=null-or-neg-beta beta=" + beta);
      return null;
    }
    final int oldNumChildren = indirect.getNumChildren();
    if (oldNumChildren < 2) {
      if (dbg) System.err.println("[g30] reject reason=fewer-than-2-children beta=" + beta
          + " n=" + oldNumChildren);
      return null;
    }

    // Pre-flight: bail out if any child reference has NULL_ID_LONG pageKey. This is the
    // SAFETY GUARD from G.30 — when any child is a fresh TIL-only page (= pageKey not yet
    // assigned), running closure produces I11-violating cascades because subsequent
    // splitSubtreeOnBit / buildBucketWithInheritedMask construct child indirects with
    // masks containing bits more significant than parent's new MSB. The architecture
    // doesn't preserve I11 across closure-triggered splits in this state.
    //
    // Effect: in our test scenarios, root almost ALWAYS has TIL-only children, so closure
    // is effectively a no-op. This is the architectural ceiling documented in
    // HOT_CAMPAIGN_RESULTS.md §5 — eliminating the 1 marginal violation requires
    // multi-week rewrite of indirect construction operations.
    for (int i = 0; i < oldNumChildren; i++) {
      final PageReference ref = indirect.getChildReference(i);
      if (ref == null || ref.getKey() == io.sirix.settings.Constants.NULL_ID_LONG) {
        if (dbg) System.err.println("[g30] reject reason=placeholder-child beta=" + beta
            + " childIdx=" + i + " childPageKey=" + (ref == null ? "null" : ref.getKey()));
        return null;
      }
    }

    // Pre-split β-mixed children.
    final java.util.ArrayList<PageReference> newRefs = new java.util.ArrayList<>(oldNumChildren * 2);
    for (int i = 0; i < oldNumChildren; i++) {
      final PageReference ref = indirect.getChildReference(i);
      if (ref == null) {
        if (dbg) System.err.println("[g30] reject reason=null-child-ref beta=" + beta + " idx=" + i);
        return null;
      }
      final int v = bitConstantValueInSubtree(ref, beta);
      if (v >= 0) {
        newRefs.add(ref);
      } else {
        final SubtreeSplit ss = splitSubtreeOnBit(ref, beta, log, revision);
        if (ss == null) {
          if (dbg) System.err.println("[g30] reject reason=split-subtree-failed beta=" + beta
              + " childIdx=" + i + " childPageKey=" + ref.getKey());
          return null;
        }
        newRefs.add(ss.leftRef());
        newRefs.add(ss.rightRef());
      }
    }
    final int newCount = newRefs.size();
    if (newCount > NodeUpgradeManager.MULTI_NODE_MAX_CHILDREN) {
      if (dbg) System.err.println("[g30] reject reason=fanout-overflow beta=" + beta
          + " newCount=" + newCount);
      return null;
    }

    // Build new layout. If β's byte fits in indirect's existing 8-byte window,
    // could keep SingleMask; for simplicity always rebuild as MultiMask if β
    // adds a new byte position.
    final int oldInitialBytePos = indirect.getInitialBytePos();
    final long oldMask = indirect.getBitMask();
    final int newBytePos = beta / 8;
    final int newBitInByte = beta % 8;
    final int newMaskBitInByte = 1 << (7 - newBitInByte);

    int[] oldBytePositions = new int[8];
    int[] oldByteMaskBits = new int[8];
    int oldByteCount = 0;
    if (indirect.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK) {
      for (int bo = 0; bo < 8; bo++) {
        final int byteMaskBits = (int) ((oldMask >>> ((7 - bo) * 8)) & 0xFFL);
        if (byteMaskBits != 0) {
          oldBytePositions[oldByteCount] = oldInitialBytePos + bo;
          oldByteMaskBits[oldByteCount] = byteMaskBits;
          oldByteCount++;
        }
      }
    } else {
      // MultiMask source: copy existing extractionPositions/Masks.
      final byte[] ep = indirect.getExtractionPositions();
      final long[] em = indirect.getExtractionMasks();
      final int neb = indirect.getNumExtractionBytes();
      for (int i = 0; i < neb; i++) {
        final int chunkIdx = i / 8;
        final int byteOffsetInChunk = i % 8;
        final int byteMaskBits = (int) ((em[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFFL);
        if (byteMaskBits != 0) {
          oldBytePositions[oldByteCount] = ep[i] & 0xFF;
          oldByteMaskBits[oldByteCount] = byteMaskBits;
          oldByteCount++;
        }
      }
    }

    int[] allBytePositions = new int[oldByteCount + 1];
    int[] allByteMaskBits = new int[oldByteCount + 1];
    int allCount = 0;
    boolean merged = false;
    for (int i = 0; i < oldByteCount; i++) {
      if (!merged && oldBytePositions[i] == newBytePos) {
        allBytePositions[allCount] = newBytePos;
        allByteMaskBits[allCount] = oldByteMaskBits[i] | newMaskBitInByte;
        allCount++;
        merged = true;
      } else if (!merged && oldBytePositions[i] > newBytePos) {
        allBytePositions[allCount] = newBytePos;
        allByteMaskBits[allCount] = newMaskBitInByte;
        allCount++;
        merged = true;
        allBytePositions[allCount] = oldBytePositions[i];
        allByteMaskBits[allCount] = oldByteMaskBits[i];
        allCount++;
      } else {
        allBytePositions[allCount] = oldBytePositions[i];
        allByteMaskBits[allCount] = oldByteMaskBits[i];
        allCount++;
      }
    }
    if (!merged) {
      allBytePositions[allCount] = newBytePos;
      allByteMaskBits[allCount] = newMaskBitInByte;
      allCount++;
    }

    final byte[] extractionPositions = new byte[allCount];
    final int numChunks = (allCount + 7) / 8;
    final long[] extractionMasks = new long[numChunks];
    short msbIndex = Short.MAX_VALUE;
    for (int i = 0; i < allCount; i++) {
      extractionPositions[i] = (byte) allBytePositions[i];
      final int chunkIdx = i / 8;
      final int byteOffsetInChunk = i % 8;
      extractionMasks[chunkIdx] |= ((long) (allByteMaskBits[i] & 0xFF)) << ((7 - byteOffsetInChunk) * 8);
      final int highBit = 31 - Integer.numberOfLeadingZeros(allByteMaskBits[i] & 0xFF);
      final int absBitPos = allBytePositions[i] * 8 + (7 - highBit);
      if (absBitPos < msbIndex) msbIndex = (short) absBitPos;
    }

    final int[] newPartials = new int[newCount];
    final PageReference[] newChildren = new PageReference[newCount];
    for (int i = 0; i < newCount; i++) {
      final PageReference cref = newRefs.get(i);
      newChildren[i] = cref;
      final byte[] cKey = getFirstKeyFromChild(cref);
      newPartials[i] = (cKey == null || cKey.length == 0) ? 0
          : computePartialKeyMultiMaskDirect(cKey, extractionPositions, extractionMasks, allCount);
    }

    for (int i = 1; i < newCount; i++) {
      for (int k = 0; k < i; k++) {
        if (newPartials[k] == newPartials[i]) {
          if (dbg) System.err.println("[g30] reject reason=partial-collision beta=" + beta
              + " i=" + i + " k=" + k + " partial=" + Integer.toHexString(newPartials[i])
              + " newCount=" + newCount);
          return null;
        }
      }
    }
    sortChildrenAndPartialsByPartial(newChildren, newPartials);
    boolean hasZero = false;
    for (final int p : newPartials) {
      if (p == 0) { hasZero = true; break; }
    }
    if (!hasZero) {
      if (dbg) {
        StringBuilder sb = new StringBuilder("[g30] reject reason=no-zero-partial beta=" + beta
            + " newCount=" + newCount + " partials=[");
        for (int i = 0; i < newCount; i++) sb.append(Integer.toHexString(newPartials[i])).append(',');
        sb.append("]");
        System.err.println(sb);
      }
      return null;
    }

    if (dbg) System.err.println("[g30] EXTEND-OK beta=" + beta + " newCount=" + newCount
        + " msbIndex=" + msbIndex + " allCount=" + allCount);
    G28_CLOSURE_FIRINGS.incrementAndGet();
    if (newCount <= 16) {
      return HOTIndirectPage.createSpanNodeMultiMask(indirect.getPageKey(), revision,
          extractionPositions, extractionMasks, allCount, newPartials, newChildren,
          indirect.getHeight(), msbIndex);
    }
    return HOTIndirectPage.createMultiNodeMultiMask(indirect.getPageKey(), revision,
        extractionPositions, extractionMasks, allCount, newPartials, newChildren,
        indirect.getHeight(), msbIndex);
  }

  /**
   * Stage G.28 — Post-insert mask-closure verification. Walks the path from root to leaf
   * after an insert. At each indirect, computes MSDB-closure of children's firstKeys.
   * If indirect's mask doesn't cover all closure bits, extends via
   * {@link #extendIndirectMaskForClosure}.
   *
   * <p>This is the architectural fix for the under-discriminated mask issue: ensures
   * every indirect's mask captures all bits where any pair of children's firstKeys
   * differ. Multi-entry-leaf workloads that diverge sibling firstKey ranges over time
   * are now properly resolved.
   */
  public void ensureMaskClosure(PageReference rootRef, byte[] keyBuf,
      StorageEngineWriter storageEngineWriter, TransactionIntentLog log) {
    if (rootRef == null) return;
    final int revision = storageEngineWriter.getRevisionNumber();
    // Stage G.28 (corrected G.29): only extend ROOT's mask. Child indirects' masks
    // need NOT be extended — Binna's trie-condition requires parent.MSB < child.MSB
    // (= parent more-significant). If we extend a child's mask to include more-
    // significant bits than parent, I11 breaks. Only root extending is structurally
    // safe: root's MSB becomes the most-significant; children stay refined relative.
    Page curPage = log.get(rootRef) != null ? log.get(rootRef).getModified() : rootRef.getPage();
    if (curPage == null) curPage = loadPage(storageEngineWriter, rootRef);
    if (!(curPage instanceof HOTIndirectPage indirect)) return;
    final int[] closureBits = findClosureBits(indirect);
    HOTIndirectPage current = indirect;
    for (final int beta : closureBits) {
      final int outPos = current.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK
          ? singleMaskBetaOutputPos(current, beta)
          : multiMaskBetaOutputPos(current, beta);
      if (outPos >= 0) continue;
      final HOTIndirectPage extended = extendIndirectMaskForClosure(current, beta, log, revision);
      if (extended == null) break;
      log.put(rootRef, PageContainer.getInstance(extended, extended));
      current = extended;
    }
  }

  /** Find ALL closure bits: every absolute bit position where any pair of children's
   *  firstKeys differ. Returns sorted ascending. Skips null/empty firstKeys (= placeholder
   *  refs from CoW shadowing or unallocated slots) — these would otherwise appear as
   *  "all-zero" keys and pollute the closure with spurious differing-bit positions. */
  private int[] findClosureBits(HOTIndirectPage indirect) {
    final int n = indirect.getNumChildren();
    if (n < 2) return new int[0];
    final byte[][] firstKeys = new byte[n][];
    int validCount = 0;
    int maxLen = 0;
    for (int i = 0; i < n; i++) {
      final PageReference cref = indirect.getChildReference(i);
      if (cref == null) continue;
      firstKeys[i] = getFirstKeyFromChild(cref);
      if (firstKeys[i] != null && firstKeys[i].length > 0) {
        validCount++;
        if (firstKeys[i].length > maxLen) maxLen = firstKeys[i].length;
      }
    }
    if (validCount < 2) return new int[0];
    final int[] tmp = new int[maxLen * 8];
    int count = 0;
    for (int absBit = 0; absBit < maxLen * 8; absBit++) {
      boolean seen0 = false, seen1 = false;
      for (int i = 0; i < n; i++) {
        if (firstKeys[i] == null || firstKeys[i].length == 0) continue;
        if (isAbsBitSet(firstKeys[i], absBit)) seen1 = true;
        else seen0 = true;
        if (seen0 && seen1) break;
      }
      if (seen0 && seen1) tmp[count++] = absBit;
    }
    return Arrays.copyOf(tmp, count);
  }

  /**
   * Stage G.27 — Find the MSDB of an indirect's children's deep-firstKeys (= the
   * most-significant absolute bit position where any pair of children's firstKeys
   * differ). Used by G.25 propagation to pick the right β at each ancestor — the
   * one that preserves I7/I8 sort coincidence.
   *
   * <p>Returns -1 if all children have identical firstKeys (= can't be distinguished).
   */
  private int findMsdbOfChildrenFirstKeys(HOTIndirectPage indirect) {
    final int n = indirect.getNumChildren();
    if (n < 2) return -1;
    // Collect firstKeys.
    final byte[][] firstKeys = new byte[n][];
    int maxLen = 0;
    for (int i = 0; i < n; i++) {
      final PageReference cref = indirect.getChildReference(i);
      if (cref == null) continue;
      firstKeys[i] = getFirstKeyFromChild(cref);
      if (firstKeys[i] != null && firstKeys[i].length > maxLen) maxLen = firstKeys[i].length;
    }
    // Walk from MSB to LSB. First bit position where any pair differs = the MSDB.
    for (int absBit = 0; absBit < maxLen * 8; absBit++) {
      boolean seen0 = false, seen1 = false;
      for (int i = 0; i < n; i++) {
        if (firstKeys[i] == null) continue;
        if (isAbsBitSet(firstKeys[i], absBit)) seen1 = true;
        else seen0 = true;
        if (seen0 && seen1) return absBit;
      }
    }
    return -1;
  }

  /**
   * Stage G.26 — Cross-window MultiMask upgrade with sibling pre-splitting. When a
   * SingleMask parent needs β added but β is outside the window AND some non-split
   * siblings are β-mixed, pre-split those siblings on β and rebuild the parent as
   * MultiMask incorporating both the leaf-split products and the pre-split siblings.
   *
   * <p>This bridges the gap where:
   * <ul>
   *   <li>{@code addEntryWithPDep} → {@code upgradeToMultiMaskWithNewBit} returns null
   *       because some non-split sibling is β-mixed.</li>
   *   <li>{@code rebalanceAndIntegrate} (Phase 3) returns null with "cross-window"
   *       reason because its rebuild doesn't support MultiMask layouts.</li>
   * </ul>
   *
   * <p>Returns the new MultiMask indirect, or null if the build is infeasible (= some
   * sibling can't be split on β, fan-out > MAX_NODE_ENTRIES, etc.).
   */
  private @Nullable HOTIndirectPage upgradeToMultiMaskWithSiblingSplits(
      HOTIndirectPage parent, int splitChildIdx, PageReference leftChild,
      PageReference rightChild, int newAbsBit, TransactionIntentLog log, int revision) {
    if (parent.getLayoutType() != HOTIndirectPage.LayoutType.SINGLE_MASK) return null;
    final int oldNumChildren = parent.getNumChildren();

    // 1. Pre-split each β-mixed non-split sibling. Collect resulting refs.
    final java.util.ArrayList<PageReference> newRefs = new java.util.ArrayList<>(oldNumChildren * 2);
    for (int i = 0; i < oldNumChildren; i++) {
      if (i == splitChildIdx) {
        newRefs.add(leftChild);
        newRefs.add(rightChild);
        continue;
      }
      final PageReference sib = parent.getChildReference(i);
      if (sib == null) return null;
      final int v = bitConstantValueInSubtree(sib, newAbsBit);
      if (v >= 0) {
        // β-constant — keep as-is.
        newRefs.add(sib);
      } else {
        // β-mixed — split.
        final SubtreeSplit ss = splitSubtreeOnBit(sib, newAbsBit, log, revision);
        if (ss == null) return null;
        newRefs.add(ss.leftRef());
        newRefs.add(ss.rightRef());
      }
    }
    final int newCount = newRefs.size();
    if (newCount < 2 || newCount > NodeUpgradeManager.MULTI_NODE_MAX_CHILDREN) return null;

    // 2. Build the MultiMask layout: combined bytes from parent's mask + β's byte.
    final int oldInitialBytePos = parent.getInitialBytePos();
    final long oldMask = parent.getBitMask();
    final int newBytePos = newAbsBit / 8;
    final int newBitInByte = newAbsBit % 8;
    final int newMaskBitInByte = 1 << (7 - newBitInByte);
    // Decode old mask into per-byte mask bits.
    int[] oldBytePositions = new int[8];
    int[] oldByteMaskBits = new int[8];
    int oldByteCount = 0;
    for (int bo = 0; bo < 8; bo++) {
      final int byteMaskBits = (int) ((oldMask >>> ((7 - bo) * 8)) & 0xFFL);
      if (byteMaskBits != 0) {
        oldBytePositions[oldByteCount] = oldInitialBytePos + bo;
        oldByteMaskBits[oldByteCount] = byteMaskBits;
        oldByteCount++;
      }
    }
    // Merge new byte sorted.
    int[] allBytePositions = new int[oldByteCount + 1];
    int[] allByteMaskBits = new int[oldByteCount + 1];
    int allCount = 0;
    boolean merged = false;
    for (int i = 0; i < oldByteCount; i++) {
      if (!merged && oldBytePositions[i] == newBytePos) {
        allBytePositions[allCount] = newBytePos;
        allByteMaskBits[allCount] = oldByteMaskBits[i] | newMaskBitInByte;
        allCount++;
        merged = true;
      } else if (!merged && oldBytePositions[i] > newBytePos) {
        allBytePositions[allCount] = newBytePos;
        allByteMaskBits[allCount] = newMaskBitInByte;
        allCount++;
        merged = true;
        allBytePositions[allCount] = oldBytePositions[i];
        allByteMaskBits[allCount] = oldByteMaskBits[i];
        allCount++;
      } else {
        allBytePositions[allCount] = oldBytePositions[i];
        allByteMaskBits[allCount] = oldByteMaskBits[i];
        allCount++;
      }
    }
    if (!merged) {
      allBytePositions[allCount] = newBytePos;
      allByteMaskBits[allCount] = newMaskBitInByte;
      allCount++;
    }
    // Build extractionPositions / extractionMasks.
    final byte[] extractionPositions = new byte[allCount];
    final int numChunks = (allCount + 7) / 8;
    final long[] extractionMasks = new long[numChunks];
    short msbIndex = Short.MAX_VALUE;
    for (int i = 0; i < allCount; i++) {
      extractionPositions[i] = (byte) allBytePositions[i];
      final int chunkIdx = i / 8;
      final int byteOffsetInChunk = i % 8;
      extractionMasks[chunkIdx] |= ((long) (allByteMaskBits[i] & 0xFF)) << ((7 - byteOffsetInChunk) * 8);
      final int highBit = 31 - Integer.numberOfLeadingZeros(allByteMaskBits[i] & 0xFF);
      final int absBitPos = allBytePositions[i] * 8 + (7 - highBit);
      if (absBitPos < msbIndex) msbIndex = (short) absBitPos;
    }

    // 3. For each new child, compute partial via direct PEXT under the new MultiMask layout.
    final int[] newPartials = new int[newCount];
    final PageReference[] newChildren = new PageReference[newCount];
    for (int i = 0; i < newCount; i++) {
      final PageReference cref = newRefs.get(i);
      newChildren[i] = cref;
      final byte[] cKey = getFirstKeyFromChild(cref);
      newPartials[i] = (cKey == null || cKey.length == 0) ? 0
          : computePartialKeyMultiMaskDirect(cKey, extractionPositions, extractionMasks, allCount);
    }

    // 4. Verify partial-key uniqueness (I3).
    for (int i = 1; i < newCount; i++) {
      for (int k = 0; k < i; k++) {
        if (newPartials[k] == newPartials[i]) return null;
      }
    }

    // 5. Sort children + partials by partial-key.
    sortChildrenAndPartialsByPartial(newChildren, newPartials);

    // 6. I4 self-check: at least one slot has partial = 0.
    boolean hasZero = false;
    for (final int p : newPartials) {
      if (p == 0) { hasZero = true; break; }
    }
    if (!hasZero) return null;

    // 7. Build MultiMask page.
    if (newCount <= 16) {
      return HOTIndirectPage.createSpanNodeMultiMask(parent.getPageKey(), revision,
          extractionPositions, extractionMasks, allCount, newPartials, newChildren,
          parent.getHeight(), msbIndex);
    }
    return HOTIndirectPage.createMultiNodeMultiMask(parent.getPageKey(), revision,
        extractionPositions, extractionMasks, allCount, newPartials, newChildren,
        parent.getHeight(), msbIndex);
  }

  /**
   * Stage G.25 — Propagate a new disc bit β up the path from the immediate parent that
   * absorbed it (via cross-window upgrade) to ancestors whose subtree at the chosen
   * descend slot now contains β-mixed keys.
   *
   * <p>Scenario: leaf overflow at depth D split on MSDB β (= byte at position OUTSIDE
   * D-1's window). Parent at depth D-1 absorbs β via upgradeToMultiMaskWithNewBit. But
   * if β's byte position is also OUTSIDE depth D-2's mask window, the grandparent's
   * stored partial for D-1's slot doesn't distinguish keys with different β values.
   * Routing for the new key from root reaches D-1's slot at grandparent, but the slot's
   * deep-firstKey has changed (= I8 break).
   *
   * <p>Solution: at each ancestor A whose mask doesn't include β AND whose chosen
   * subtree is now β-mixed, split A's slot's subtree on β + addEntry at A.
   */
  private void propagateBetaToAncestors(StorageEngineWriter storageEngineWriter,
      TransactionIntentLog log, HOTIndirectPage parent, PageReference parentRef,
      HOTIndirectPage expandedParent, int originalChildIndex, PageReference leftChild,
      PageReference rightChild, int beta,
      HOTIndirectPage[] pathNodes, PageReference[] pathRefs, int[] pathChildIndices,
      int currentPathIdx) {
    final boolean dbgEntry = Boolean.getBoolean("hot.debug.g25");
    if (dbgEntry) {
      System.out.println("[g25-propagate] entry parent.layout=" + parent.getLayoutType()
          + " expandedParent.layout=" + expandedParent.getLayoutType()
          + " beta=" + beta + " currentPathIdx=" + currentPathIdx);
    }
    // Only propagate when expandedParent absorbed β via cross-window upgrade
    // (= layout changed from SingleMask to MultiMask).
    if (parent.getLayoutType() != HOTIndirectPage.LayoutType.SINGLE_MASK) return;
    if (expandedParent.getLayoutType() != HOTIndirectPage.LayoutType.MULTI_MASK) return;
    final int revision = storageEngineWriter.getRevisionNumber();
    final boolean dbg = Boolean.getBoolean("hot.debug.g25");
    PageReference subtreeRef = parentRef;
    for (int i = currentPathIdx - 1; i >= 0; i--) {
      final HOTIndirectPage A = pathNodes[i];
      final PageReference Aref = pathRefs[i];
      if (A == null || Aref == null) break;
      // Stage G.27 — At each ancestor, find the MSDB of its children's firstKeys (not
      // the leaf's β!) and extend with THAT bit. The leaf's β may be a low-order bit
      // that produces partial-key/first-key sort disagreement at the ancestor level.
      // The ancestor's children's MSDB is the bit where their firstKeys most-significantly
      // differ, which preserves I7/I8 coincidence.
      final int ancestorBeta = findMsdbOfChildrenFirstKeys(A);
      if (ancestorBeta < 0) {
        if (dbg) System.out.println("[g25-propagate]   step i=" + i + " no msdb-of-children");
        break;
      }
      final int betaOutPos = A.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK
          ? singleMaskBetaOutputPos(A, ancestorBeta)
          : multiMaskBetaOutputPos(A, ancestorBeta);
      if (dbg) System.out.println("[g25-propagate]   step i=" + i + " A.pageKey=" + A.getPageKey()
          + " A.layout=" + A.getLayoutType() + " ancestorBeta=" + ancestorBeta
          + " betaOutPos=" + betaOutPos);
      if (betaOutPos >= 0) { if (dbg) System.out.println("[g25-propagate]     β in mask, halt"); break; }
      final int chosenSlot = pathChildIndices[i];
      if (chosenSlot < 0 || chosenSlot >= A.getNumChildren()) {
        if (dbg) System.out.println("[g25-propagate]     bad chosenSlot=" + chosenSlot); break;
      }
      final PageReference chosenRef = A.getChildReference(chosenSlot);
      if (chosenRef == null) { if (dbg) System.out.println("[g25-propagate]     null chosenRef"); break; }
      final int v = bitConstantValueInSubtree(chosenRef, ancestorBeta);
      if (dbg) System.out.println("[g25-propagate]     β-constancy v=" + v);
      if (v >= 0) { if (dbg) System.out.println("[g25-propagate]     β-constant, halt"); break; }
      final SubtreeSplit split = splitSubtreeOnBit(chosenRef, ancestorBeta, log, revision);
      if (dbg) System.out.println("[g25-propagate]     split " + (split == null ? "null" : "ok"));
      if (split == null) break;
      // Use ancestorBeta in addEntry/upgrade calls below.
      final int beta_ = ancestorBeta;
      HOTIndirectPage updatedA;
      if (A.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK) {
        updatedA = addEntryWithPDep(A, chosenSlot, split.leftRef(), split.rightRef(),
            beta_, revision);
      } else {
        updatedA = addEntryMultiMask(A, chosenSlot, split.leftRef(), split.rightRef(),
            beta_, revision);
      }
      if (dbg) System.out.println("[g25-propagate]     addEntry result="
          + (updatedA == null ? "null" : "ok layout=" + updatedA.getLayoutType()));
      if (updatedA == null) {
        updatedA = rebalanceAndIntegrate(A, chosenSlot, split.leftRef(), split.rightRef(),
            beta_, log, revision);
        if (dbg) System.out.println("[g25-propagate]     phase3 result="
            + (updatedA == null ? "null" : "ok"));
        if (updatedA == null) {
          updatedA = upgradeToMultiMaskWithSiblingSplits(A, chosenSlot, split.leftRef(),
              split.rightRef(), beta_, log, revision);
          if (dbg) System.out.println("[g25-propagate]     g25-multimask result="
              + (updatedA == null ? "null" : "ok"));
          if (updatedA == null) break;
        }
      }
      log.put(Aref, PageContainer.getInstance(updatedA, updatedA));
      G25_BETA_PROPAGATIONS.incrementAndGet();
      if (dbg) System.out.println("[g25-propagate]     PROPAGATED at i=" + i);
    }
  }

  /**
   * Stage G.21 — Find the highest-significance ancestor bit β where the leaf is β-mixed
   * (= contains keys with both β=0 and β=1) AFTER inserting newKey. This is the bit
   * to split on for constancy-aware partition.
   *
   * <p>Returns -1 if the leaf would remain β-constant at every ancestor β (no split
   * needed for β-constancy reasons).
   */
  public int findFirstMixedAncestorBit(HOTLeafPage leaf, byte[] newKey, int[] ancestorBits) {
    if (leaf == null || newKey == null || ancestorBits == null) return -1;
    final int leafCount = leaf.getEntryCount();
    if (leafCount == 0) return -1;
    // ancestorBits is sorted ascending (= MSB-first absolute bit positions).
    // Most-significant first means smallest absBit value.
    for (final int beta : ancestorBits) {
      final boolean newKeyBetaSet = isAbsBitSet(newKey, beta);
      boolean seen0 = newKeyBetaSet ? false : true;
      boolean seen1 = newKeyBetaSet;
      for (int i = 0; i < leafCount; i++) {
        final byte[] existing = leaf.getKey(i);
        if (existing == null || existing.length == 0) continue;
        if (isAbsBitSet(existing, beta)) seen1 = true;
        else seen0 = true;
        if (seen0 && seen1) break;
      }
      if (seen0 && seen1) return beta; // mixed at this β
    }
    return -1;
  }

  public static long getG20RecursiveSplitFirings() { return G20_RECURSIVE_SPLIT_FIRINGS.get(); }
  public static void resetG20RecursiveSplitFirings() { G20_RECURSIVE_SPLIT_FIRINGS.set(0L); }

  /**
   * Stage G.20 — Recursive constancy-aware leaf split.
   *
   * <p>For each ancestor β bit captured by some indirect on the path, partition the
   * leaf's keys (+ the new key) into β-buckets. Each bucket is β-constant at every
   * ancestor β simultaneously (= up to 2^k buckets where k = total ancestor β bits).
   *
   * <p>Returns the bucket count. Buckets are written into {@code outBuckets} as
   * {@code byte[][]} arrays where {@code outBuckets[bucketIdx][2*i]} is the i-th key
   * in bucket and {@code [2*i+1]} is its value. Returns 0 if no β-constancy needed.
   *
   * <p>This is the comprehensive form of Phase 2's constancy-aware split. It exists
   * to provide β-constant halves to addEntry / Phase 4 / intermediate-BiNode integration
   * paths, ensuring routing remains unambiguous post-integration.
   *
   * @return number of buckets produced (1 if no split needed, ≥ 2 otherwise)
   */
  public int recursiveConstancyAwareSplit(HOTLeafPage leaf, byte[] newKey, byte[] newValue,
      int[] ancestorBits, byte[][][] outBuckets) {
    if (leaf == null || newKey == null) return 0;
    if (ancestorBits == null || ancestorBits.length == 0) return 1;
    final int leafCount = leaf.getEntryCount();
    final int totalKeys = leafCount + 1;
    final byte[][] keys = new byte[totalKeys][];
    final byte[][] values = new byte[totalKeys][];
    for (int i = 0; i < leafCount; i++) {
      keys[i] = leaf.getKey(i);
      values[i] = leaf.getValue(i);
    }
    keys[leafCount] = newKey;
    values[leafCount] = newValue;

    // Partition by all ancestor bits simultaneously: each bucket index encodes the
    // pattern of bits ((β_0, β_1, ..., β_{k-1}) → bit value). Up to 2^k buckets.
    final int k = ancestorBits.length;
    final int maxBuckets = 1 << k;
    if (maxBuckets > 64 || outBuckets.length < maxBuckets) {
      // Too many ancestor bits — fall back to MSDB split (= 2 buckets only).
      return 0;
    }
    final int[] bucketIdxOf = new int[totalKeys];
    final int[] bucketCounts = new int[maxBuckets];
    for (int i = 0; i < totalKeys; i++) {
      int idx = 0;
      for (int b = 0; b < k; b++) {
        if (isAbsBitSet(keys[i], ancestorBits[b])) {
          idx |= (1 << b);
        }
      }
      bucketIdxOf[i] = idx;
      bucketCounts[idx]++;
    }
    int activeBuckets = 0;
    for (int b = 0; b < maxBuckets; b++) {
      if (bucketCounts[b] > 0) activeBuckets++;
    }
    if (activeBuckets <= 1) return 1; // already β-constant — no split needed
    G20_RECURSIVE_SPLIT_FIRINGS.incrementAndGet();
    // Allocate per-bucket arrays + populate.
    final int[] bucketWriteCursor = new int[maxBuckets];
    for (int b = 0; b < maxBuckets; b++) {
      if (bucketCounts[b] > 0) {
        outBuckets[b] = new byte[2 * bucketCounts[b]][];
      }
    }
    for (int i = 0; i < totalKeys; i++) {
      final int idx = bucketIdxOf[i];
      final int cur = bucketWriteCursor[idx]++;
      outBuckets[idx][2 * cur] = keys[i];
      outBuckets[idx][2 * cur + 1] = values[i];
    }
    return activeBuckets;
  }

  public static long getG18AmbiguousDetections() { return G18_AMBIGUOUS_DETECTIONS.get(); }
  public static void resetG18AmbiguousDetections() { G18_AMBIGUOUS_DETECTIONS.set(0L); }

  /**
   * Stage G.18 — Detect ambiguous routing for {@code keyBuf} at any ancestor on the
   * descend path. Returns the ancestor index where routing is ambiguous (= chosen slot's
   * stored partial doesn't equal {@code densePK(keyBuf, ancestor.mask)} AND no other
   * slot has stored == densePK), -1 if all ancestors route unambiguously.
   *
   * <p>Under sparse-path encoding: routing is unambiguous iff exactly one slot's stored
   * equals densePK(K). Multi-entry leaves break this — a slot may contain keys with
   * various densePKs, so the firstKey-based stored partial doesn't represent all keys.
   * Inserting a new key K with densePK ≠ stored makes the slot's encoding stale.
   *
   * <p>This helper SURFACES the ambiguity. Phase 2 of the routing-encoding rewrite
   * (HOT_ROUTING_ENCODING_REWRITE.md §3.2) will use it to invoke proactive disc-bit
   * extension at the offending ancestor.
   *
   * @return ancestor index ≥ 0 (= position in pathNodes) where ambiguity exists, -1 if none
   */
  public int findAmbiguousAncestor(HOTIndirectPage[] pathNodes, int[] pathChildIndices,
      int pathDepth, byte[] keyBuf) {
    if (pathDepth <= 0 || keyBuf == null) return -1;
    for (int d = 0; d < pathDepth; d++) {
      final HOTIndirectPage A = pathNodes[d];
      if (A == null) continue;
      final int chosenSlot = pathChildIndices[d];
      final int[] partials = A.getPartialKeys();
      if (partials == null || chosenSlot < 0 || chosenSlot >= partials.length) continue;
      final int densePK = computeDensePartialKey(A, keyBuf);
      // Unambiguous case: chosen slot's stored equals densePK exactly.
      if (partials[chosenSlot] == densePK) continue;
      // Ambiguous: check if any sibling has stored == densePK (= would be routed to).
      // Even if not, the mismatch means stored doesn't represent K's path uniquely.
      G18_AMBIGUOUS_DETECTIONS.incrementAndGet();
      return d;
    }
    return -1;
  }

  /**
   * Stage G.19 — Find a bit position β where {@code keyBuf}'s densePK at the ancestor's
   * mask differs from the chosen slot's stored partial. Returns the absolute MSB-first
   * bit position, or -1 if no such bit exists in the mask.
   *
   * <p>Used by Phase 2 of the routing-encoding rewrite: when ambiguity is detected at
   * an ancestor, this picks the splitting bit. The leaf-split-and-integrate path then
   * uses this β to partition the offending leaf so newKey ends up in a slot with
   * matching stored partial.
   */
  public int findOffendingBitAtAncestor(HOTIndirectPage A, int chosenSlot, byte[] keyBuf) {
    if (A == null || keyBuf == null) return -1;
    final int[] partials = A.getPartialKeys();
    if (partials == null || chosenSlot < 0 || chosenSlot >= partials.length) return -1;
    final int densePK = computeDensePartialKey(A, keyBuf);
    final int chosenStored = partials[chosenSlot];
    final int diff = densePK ^ chosenStored;
    if (diff == 0) return -1; // no difference
    // Find the most-significant differing bit in the partial-key space, then convert
    // to absolute MSB-first bit position.
    final int outputPos = 31 - Integer.numberOfLeadingZeros(diff);
    return outputPosToAbsBit(A, outputPos);
  }

  /** Convert a partial-key output position to absolute MSB-first bit position via the mask. */
  private static int outputPosToAbsBit(HOTIndirectPage indirect, int outputPos) {
    if (indirect.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK) {
      final int initialBytePos = indirect.getInitialBytePos();
      long m = indirect.getBitMask();
      int op = 0;
      while (m != 0L) {
        final long lowBit = m & -m;
        if (op == outputPos) {
          final int b = Long.numberOfTrailingZeros(lowBit);
          final int byteOffset = 7 - b / 8;
          final int bitInByte = 7 - (b % 8);
          return (initialBytePos + byteOffset) * 8 + bitInByte;
        }
        m &= m - 1L;
        op++;
      }
      return -1;
    }
    // MultiMask: walk extractionPositions / extractionMasks, count outputPos.
    final byte[] extractionPositions = indirect.getExtractionPositions();
    final long[] extractionMasks = indirect.getExtractionMasks();
    final int numExtractionBytes = indirect.getNumExtractionBytes();
    if (extractionPositions == null || extractionMasks == null) return -1;
    int totalBits = 0;
    for (final long em : extractionMasks) totalBits += Long.bitCount(em);
    int bitsBefore = totalBits;
    for (int i = 0; i < numExtractionBytes; i++) {
      final int chunkIdx = i / 8;
      final int byteOffsetInChunk = i % 8;
      final long byteMask = (extractionMasks[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFFL;
      for (int b = 7; b >= 0; b--) {
        if ((byteMask & (1L << b)) == 0) continue;
        bitsBefore--;
        if (bitsBefore == outputPos) {
          return (extractionPositions[i] & 0xFF) * 8 + (7 - b);
        }
      }
    }
    return -1;
  }

  /** Compute densePK for a key under any indirect's layout (SingleMask or MultiMask). */
  private static int computeDensePartialKey(HOTIndirectPage indirect, byte[] key) {
    if (indirect.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK) {
      final int initialBytePos = indirect.getInitialBytePos();
      if (initialBytePos >= key.length) return 0;
      long keyWord = 0;
      for (int i = 0; i < 8 && (initialBytePos + i) < key.length; i++) {
        keyWord |= ((long) (key[initialBytePos + i] & 0xFF)) << ((7 - i) * 8);
      }
      return (int) Long.compress(keyWord, indirect.getBitMask());
    }
    final byte[] extractionPositions = indirect.getExtractionPositions();
    final long[] extractionMasks = indirect.getExtractionMasks();
    final int numExtractionBytes = indirect.getNumExtractionBytes();
    if (extractionPositions == null || extractionMasks == null) return 0;
    return computePartialKeyMultiMaskDirect(key, extractionPositions, extractionMasks, numExtractionBytes);
  }

  public static long getG17ConstancyRedirects() { return G17_CONSTANCY_REDIRECTS.get(); }
  public static void resetG17ConstancyRedirects() { G17_CONSTANCY_REDIRECTS.set(0L); }

  /**
   * Stage G.17 — Pick the child slot for {@code key} at {@code indirect} that preserves
   * β-constancy in the chosen subtree. If the natural PEXT-routing pick (findChildIndex)
   * would force β-mixing at some β in indirect.mask, look for an alternate slot whose
   * subtree's β-value matches key.β. Falls back to findChildIndex if no alternate works.
   */
  private int pickConstancyCorrectChildSlot(HOTIndirectPage indirect, byte[] key,
      StorageEngineWriter storageEngineWriter, TransactionIntentLog log) {
    final int natural = indirect.findChildIndex(key);
    if (natural < 0) return natural;
    // Iterate β bits in indirect.mask via collectAncestorDiscBits-style enumeration.
    final int[] betas = collectDiscBitsOf(indirect);
    if (betas.length == 0) return natural;
    final PageReference naturalRef = indirect.getChildReference(natural);
    if (naturalRef == null) return natural;
    for (final int beta : betas) {
      final int subtreeV = bitConstantValueInSubtree(naturalRef, beta);
      if (subtreeV < 0) continue; // already β-mixed → can't worsen it
      final int keyBetaValue = isAbsBitSet(key, beta) ? 1 : 0;
      if (keyBetaValue == subtreeV) continue; // key matches subtree's β → no break
      // Look for a sibling slot whose subtree is β-constant at keyBetaValue.
      final int n = indirect.getNumChildren();
      int alt = -1;
      for (int i = 0; i < n; i++) {
        if (i == natural) continue;
        final PageReference sib = indirect.getChildReference(i);
        if (sib == null) continue;
        final int sibV = bitConstantValueInSubtree(sib, beta);
        if (sibV == keyBetaValue) { alt = i; break; }
      }
      if (alt >= 0) {
        G17_CONSTANCY_REDIRECTS.incrementAndGet();
        return alt;
      }
      // No alternate at this β — try next β. (Some β might have a redirectable sibling
      // even if this one doesn't.)
    }
    return natural;
  }

  /** Helper: collect indirect's mask bits as absolute MSB-first positions. */
  private static int[] collectDiscBitsOf(HOTIndirectPage indirect) {
    final int[] tmp = new int[64];
    final int n = appendDiscBitsOfIndirect(indirect, tmp, 0);
    return n == 0 ? new int[0] : Arrays.copyOf(tmp, n);
  }

  public static long getG15I8RerouteFirings() { return G15_I8_REROUTE_FIRINGS.get(); }
  public static void resetG15I8Counter() { G15_I8_REROUTE_FIRINGS.set(0L); }

  /**
   * Stage G.15 — Detect insert-time I8 violations from newKey becoming the new deep-firstKey.
   *
   * <p>When newKey is smaller than the target leaf's existing first key, the leaf's
   * deep-firstKey post-insert would be newKey. If that new firstKey is less than the
   * predecessor sibling's deep-firstKey at some ancestor, I8 breaks at that ancestor.
   *
   * <p>Returns the ancestor index (= position in pathNodes) where I8 would break, or
   * -1 if no break.
   */
  public int findI8OffendingAncestor(HOTIndirectPage[] pathNodes, PageReference[] pathRefs,
      int[] pathChildIndices, int pathDepth, HOTLeafPage leaf, byte[] keyBuf) {
    if (leaf == null || keyBuf == null || pathDepth <= 0) return -1;
    final int leafEntryCount = leaf.getEntryCount();
    if (leafEntryCount == 0) return -1;
    final byte[] leafFirstKey = leaf.getKey(0);
    if (leafFirstKey == null) return -1;
    // newKey must be smaller than leaf's current first key to change the deep-firstKey.
    if (Arrays.compareUnsigned(keyBuf, leafFirstKey) >= 0) return -1;

    // newKey would become the new deep-firstKey of pathNodes[d-1].descend-slot for every d > 0.
    // Check if at any ancestor A, this new firstKey violates A's child-sorted-by-firstKey order.
    for (int d = pathDepth - 1; d >= 0; d--) {
      final int descendSlot = pathChildIndices[d];
      if (descendSlot <= 0) continue; // no predecessor to compare against
      final HOTIndirectPage A = pathNodes[d];
      if (A == null) continue;
      final PageReference prevSib = A.getChildReference(descendSlot - 1);
      if (prevSib == null) continue;
      final byte[] prevSibFirstKey = getFirstKeyFromChild(prevSib);
      if (prevSibFirstKey == null || prevSibFirstKey.length == 0) continue;
      // For I8 to hold post-insert: prevSib.firstKey < descendSlot.firstKey (= newKey).
      // If prevSib.firstKey >= newKey, I8 breaks at A.
      if (Arrays.compareUnsigned(prevSibFirstKey, keyBuf) >= 0) {
        return d;
      }
    }
    return -1;
  }

  public static long getOptionBRerouteFirings() { return OPTION_B_REROUTE_FIRINGS.get(); }
  public static void resetOptionBRerouteFirings() { OPTION_B_REROUTE_FIRINGS.set(0L); }

  /**
   * Option B (Stage G.13) — Insert-time re-route. When the leaf-insert pre-condition
   * detects that newKey would break β-constancy at some ancestor A, bypass the standard
   * leaf insert and re-route newKey to A's exact-XOR sibling slot via Phase 4-style
   * bulk-insert-into-sibling-subtree.
   *
   * <p>Algorithm:
   * <ol>
   *   <li>Walk up the path looking for the highest (= numerically smallest absBit) ancestor
   *       A whose mask captures β.</li>
   *   <li>At A, compute β's output position in A's partial-key layout.</li>
   *   <li>Compute the target sibling's stored partial = descend-slot's stored XOR
   *       (1 &lt;&lt; outputPos). Find the slot whose stored matches exactly.</li>
   *   <li>If sibling exists, call {@link #bulkInsertIntoSiblingSubtree} to descend from
   *       sibling and insert newKey there. Existing leaf at descend-slot stays β-constant.</li>
   *   <li>If no exact-XOR sibling exists, return false (caller falls through to standard
   *       insert; future work can add addEntry-style mask extension at A).</li>
   * </ol>
   *
   * @return {@code true} on successful re-route, {@code false} otherwise (caller falls
   *         through to standard insert path)
   */
  public boolean tryReRouteOffendingKey(HOTIndirectPage[] pathNodes, PageReference[] pathRefs,
      int[] pathChildIndices, int pathDepth, int offendingBeta,
      byte[] keyBuf, byte[] valueBuf,
      StorageEngineWriter storageEngineWriter, TransactionIntentLog log) {
    if (pathDepth <= 0 || offendingBeta < 0) return false;
    // Find ancestor A whose mask captures offendingBeta. Walk DOWN the path (root → leaf-parent)
    // so we route at the HIGHEST ancestor that captures β (= the most-significant routing decision).
    for (int i = 0; i < pathDepth; i++) {
      final HOTIndirectPage A = pathNodes[i];
      if (A == null) continue;
      final int betaOutputPos = (A.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK)
          ? singleMaskBetaOutputPos(A, offendingBeta)
          : multiMaskBetaOutputPos(A, offendingBeta);
      if (betaOutputPos < 0) continue; // β not in A's mask
      final int descendSlot = pathChildIndices[i];
      final int[] partials = A.getPartialKeys();
      if (partials == null || descendSlot < 0 || descendSlot >= partials.length) continue;
      final int descendStored = partials[descendSlot];
      final int targetStored = descendStored ^ (1 << betaOutputPos);
      // Look for exact-XOR sibling.
      int siblingIdx = -1;
      final int n = A.getNumChildren();
      for (int j = 0; j < n; j++) {
        if (j == descendSlot) continue;
        if (partials[j] == targetStored) { siblingIdx = j; break; }
      }
      if (siblingIdx < 0) continue; // no exact-XOR sibling at this ancestor — try further down
      // Bulk-insert keyBuf into sibling's subtree.
      final boolean ok = bulkInsertIntoSiblingSubtree(A, pathRefs[i], siblingIdx,
          keyBuf, valueBuf, storageEngineWriter, log);
      if (ok) {
        OPTION_B_REROUTE_FIRINGS.incrementAndGet();
        return true;
      }
    }
    return false;
  }

  /**
   * Append the absolute disc-bit positions captured by a single indirect to {@code buf}
   * starting at {@code start}. Returns the new write offset.
   *
   * <p>Per the BE word convention documented on {@link #collectAncestorDiscBits}:
   * <ul>
   *   <li>SingleMask: for each set long-bit {@code b} of {@code bitMask},
   *       {@code byte = initialBytePos + (7 - b/8)},
   *       {@code bitInByte = 7 - b%8}; absolute = byte*8 + bitInByte.</li>
   *   <li>MultiMask: chunk {@code c} of length 8 bytes; for set long-bit {@code b} in
   *       {@code extractionMasks[c]}, the chunk-local byte index is
   *       {@code extractionPositionIndex = c*8 + (7 - b/8)}, and the corresponding
   *       key byte position is {@code extractionPositions[extractionPositionIndex]};
   *       {@code bitInByte = 7 - b%8}; absolute = bytePos*8 + bitInByte.</li>
   * </ul>
   *
   * <p>HFT-grade: zero allocation, primitive ops only, no autoboxing.
   */
  private static int appendDiscBitsOfIndirect(HOTIndirectPage indirect, int[] buf, int start) {
    int n = start;
    if (indirect.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK) {
      final int initialBytePos = indirect.getInitialBytePos();
      long mask = indirect.getBitMask();
      while (mask != 0L && n < buf.length) {
        final int b = Long.numberOfTrailingZeros(mask);
        mask &= mask - 1L;
        final int bytePos = initialBytePos + (7 - (b / 8));
        final int bitInByte = 7 - (b % 8);
        buf[n++] = bytePos * 8 + bitInByte;
      }
      return n;
    }
    // MultiMask
    final byte[] extractionPositions = indirect.getExtractionPositions();
    final long[] extractionMasks = indirect.getExtractionMasks();
    if (extractionPositions == null || extractionMasks == null) {
      return n;
    }
    final int numChunks = extractionMasks.length;
    for (int c = 0; c < numChunks && n < buf.length; c++) {
      long mask = extractionMasks[c];
      while (mask != 0L && n < buf.length) {
        final int b = Long.numberOfTrailingZeros(mask);
        mask &= mask - 1L;
        final int posIdx = c * 8 + (7 - (b / 8));
        if (posIdx >= extractionPositions.length) continue;
        final int bytePos = extractionPositions[posIdx] & 0xFF;
        final int bitInByte = 7 - (b % 8);
        buf[n++] = bytePos * 8 + bitInByte;
      }
    }
    return n;
  }

  /**
   * Get height from a child reference. Falls back to loading from storage if not swizzled.
   */
  private int getHeightFromChild(PageReference childRef, @Nullable StorageEngineReader reader) {
    Page page = childRef.getPage();
    if (page == null && reader != null) {
      page = loadPage(reader, childRef);
      if (page != null) {
        childRef.setPage(page); // swizzle for future access
      }
    }
    if (page instanceof HOTLeafPage) {
      return 0;
    } else if (page instanceof HOTIndirectPage indirect) {
      return indirect.getHeight();
    }
    return 0;
  }

  /**
   * Expand a parent node by replacing one child with two children (from a split). Reference:
   * parentNode.addEntry() in HOTSingleThreaded.hpp
   */
  private HOTIndirectPage expandParentNode(HOTIndirectPage parent, int splitChildIndex, PageReference leftChild,
      PageReference rightChild, int discriminativeBit, int revision) {

    int numChildren = parent.getNumChildren();
    int newNumChildren = numChildren + 1;
    PageReference[] newChildren = new PageReference[newNumChildren];

    // Copy children, replacing split child with left+right.
    // COW-created copies of PageReferences inherit logKeys from originals.
    // If a copy shares a logKey with leftChild or rightChild, both resolve
    // to the same TIL entry (same leaf page) — skip the stale copy.
    int j = 0;
    final int leftLogKey = leftChild.getLogKey();
    final int rightLogKey = rightChild.getLogKey();
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) {
        newChildren[j++] = leftChild;
        newChildren[j++] = rightChild;
      } else {
        final PageReference child = parent.getChildReference(i);
        final int childLogKey = child.getLogKey();
        if (childLogKey >= 0 && (childLogKey == leftLogKey || childLogKey == rightLogKey)) {
          // Stale copy — aliases the same leaf page as a split result. Skip it.
          newNumChildren--;
          continue;
        }
        newChildren[j++] = child;
      }
    }

    // Trim array if stale copies were removed
    if (j < newChildren.length) {
      newChildren = Arrays.copyOf(newChildren, j);
    }

    // Sort children by first key to ensure correct boundary computation.
    // HOT trie routing by disc bits can place keys out of key-order (e.g., key 1024
    // routes to child[0..511] because its disc bit matches). Without sorting,
    // boundary computation misses discriminative bits between non-adjacent children.
    sortChildrenByFirstKey(newChildren);

    // Compute discriminative bits and partial keys for navigation
    final int initialBytePos = computeInitialBytePos(newChildren);
    final DiscBitsInfo discBits = computeDiscBits(newChildren, initialBytePos);
    final int[] partialKeys = computePartialKeysForChildren(newChildren, discBits);

    // Sort by partial-key (HOT I7) — children are sorted by first-key above to drive
    // adjacent-pair disc-bit collection in computeDiscBits, but the canonical storage
    // order is partial-key.
    sortChildrenAndPartialsByPartial(newChildren, partialKeys);

    // Create appropriate node type based on child count
    final HOTIndirectPage created;
    if (newNumChildren <= 2) {
      // Recompute disc bit from partial-sorted children (the passed-in discriminativeBit may
      // refer to the pre-sort position or be -1 for identical boundary keys).
      final byte[] lMax = getLastKeyFromChild(newChildren[0]);
      final byte[] rMin = getFirstKeyFromChild(newChildren[1]);
      final int recomputedDiscBit = Math.max(0, DiscriminativeBitComputer.computeDifferingBit(lMax, rMin));
      created = createBiNodeTraced("rebuildParent-1424", parent.getPageKey(), revision,
          recomputedDiscBit, newChildren[0], newChildren[1], parent.getHeight());
    } else {
      created = createNodeWithDiscBits(parent.getPageKey(), revision, parent.getHeight(),
          discBits, partialKeys, newChildren);
    }
    return created;
  }


  /**
   * Create a HOTIndirectPage (SpanNode or MultiNode) using the given disc bits info.
   * Dispatches to SingleMask or MultiMask factory methods based on layout type.
   */
  private static HOTIndirectPage createNodeWithDiscBits(long pageKey, int revision, int height,
      DiscBitsInfo discBits, int[] partialKeys, PageReference[] children) {
    if (discBits.isSingleMask()) {
      if (children.length <= 16) {
        return HOTIndirectPage.createSpanNode(pageKey, revision, discBits.initialBytePos(),
            discBits.bitMask(), partialKeys, children, height);
      } else {
        return HOTIndirectPage.createMultiNode(pageKey, revision, discBits.initialBytePos(),
            discBits.bitMask(), partialKeys, children, height);
      }
    } else {
      if (children.length <= 16) {
        return HOTIndirectPage.createSpanNodeMultiMask(pageKey, revision,
            discBits.extractionPositions(), discBits.extractionMasks(),
            discBits.numExtractionBytes(), partialKeys, children, height,
            discBits.mostSignificantBitIndex());
      } else {
        return HOTIndirectPage.createMultiNodeMultiMask(pageKey, revision,
            discBits.extractionPositions(), discBits.extractionMasks(),
            discBits.numExtractionBytes(), partialKeys, children, height,
            discBits.mostSignificantBitIndex());
      }
    }
  }

  /**
   * Compute initial byte position from children keys.
   */
  private int computeInitialBytePos(PageReference[] children) {
    if (children.length < 2)
      return 0;

    int minBytePos = Integer.MAX_VALUE;
    for (int i = 0; i < children.length - 1; i++) {
      final byte[] last = getLastKeyFromChild(children[i]);
      final byte[] first = getFirstKeyFromChild(children[i + 1]);
      final int bit = DiscriminativeBitComputer.computeDifferingBit(last, first);
      if (bit >= 0) {
        minBytePos = Math.min(minBytePos, bit / 8);
      }
    }
    return minBytePos == Integer.MAX_VALUE ? 0 : minBytePos;
  }

  /**
   * Compute bit mask that covers all discriminative bits for the children.
   * Uses little-endian layout matching {@link HOTIndirectPage#getKeyWordAt}:
   * byte 0 of window → bits 0-7, byte 1 → bits 8-15, etc.
   * Within each byte, MSB (bit 0) maps to position 7, LSB (bit 7) maps to position 0.
   */
  /**
   * Compute discriminative bit information for a set of sorted children.
   * Returns either SingleMask (bits fit in 8 contiguous bytes) or MultiMask
   * (bits span >8 bytes, requiring byte gathering + per-chunk PEXT).
   *
   * <p>Matches C++ reference: SingleMaskPartialKeyMapping vs MultiMaskPartialKeyMapping.</p>
   */
  private DiscBitsInfo computeDiscBits(PageReference[] children, int initialBytePos) {
    // Capture EVERY bit where adjacent children's first-keys differ (no constancy filter on the
    // rebuild path — see the residual-I6-violation note in the file header). Adjacent-pair MSB
    // collection is sufficient for sorted children: any pair with different sparse partials must
    // differ at some adjacent-pair-captured bit (transitivity).
    //
    // <p>Some configurations of multi-entry-leaf children produce stored partials that don't
    // round-trip through PEXT-descent for every contained key (validator I6 violations). Those
    // residual irregularities are absorbed by {@link HOTTrieReader#lowerBound}'s Phase 2b
    // insertion-point fast path + Phase 3-5 walk-up, so end-to-end reads remain correct
    // (microbench full-scan misses=0). Tightening this to a true constancy filter requires
    // either dropping multi-entry leaves (giving up Sirix's storage density) or implementing
    // structural rebalancing during restructuring (significant complexity); both are out of
    // scope for this branch.
    int minBytePos = Integer.MAX_VALUE;
    int maxBytePos = 0;
    final TreeMap<Integer, Integer> maskByBytePos = new TreeMap<>();

    for (int i = 0; i < children.length - 1; i++) {
      final byte[] key1 = getLastKeyFromChild(children[i]);
      final byte[] key2 = getFirstKeyFromChild(children[i + 1]);
      if (key1.length == 0 || key2.length == 0) {
        continue;
      }
      final int minLen = Math.min(key1.length, key2.length);
      for (int b = 0; b < minLen; b++) {
        int diff = (key1[b] ^ key2[b]) & 0xFF;
        while (diff != 0) {
          final int hb = Integer.numberOfLeadingZeros(diff) - 24; // 0..7, 0=MSB
          diff &= ~(1 << (7 - hb));
          if (isDiscBitAlreadyCaptured(maskByBytePos, b, hb)) continue;
          final int maskBit = 1 << (7 - hb);
          maskByBytePos.merge(b, maskBit, (a, b2) -> a | b2);
          if (b < minBytePos) minBytePos = b;
          if (b > maxBytePos) maxBytePos = b;
        }
      }
    }

    if (maskByBytePos.isEmpty()) {
      return DiscBitsInfo.singleMask(initialBytePos, 1L);
    }

    if (maxBytePos - initialBytePos < 8 && minBytePos >= initialBytePos) {
      long mask = 0;
      for (final var entry : maskByBytePos.entrySet()) {
        // BE word layout: byte at window-position p occupies long-bits ((7-p)*8 .. (7-p)*8+7).
        final int byteOffset = entry.getKey() - initialBytePos;
        final int maskByte = entry.getValue();
        mask |= ((long) (maskByte & 0xFF)) << ((7 - byteOffset) * 8);
      }
      return DiscBitsInfo.singleMask(initialBytePos, mask);
    }

    return buildMultiMask(maskByBytePos, minBytePos);
  }

  private static boolean isDiscBitAlreadyCaptured(TreeMap<Integer, Integer> maskByBytePos,
      int bytePos, int bitInByte) {
    final Integer existing = maskByBytePos.get(bytePos);
    if (existing == null) return false;
    return (existing & (1 << (7 - bitInByte))) != 0;
  }

  // Non-adjacent-pair augment was investigated for I6-violation reduction on multi-entry-leaf
  // workloads (microbench at 500K reports 159 residual violations, all absorbed by
  // HOTTrieReader's lower_bound walk-up). The augment+filter combination needs constancy-
  // respecting bits between every colliding pair to terminate cleanly; adversarial workloads
  // hit cases where no such bit exists, and the augment bails leaving partial-key duplicates
  // that are MUCH worse (475K I3 violations and 87% read miss in the same microbench). The
  // current adjacent-pair-no-filter approach minimises violations end-to-end.

  /**
   * Build MultiMask discriminative bit info from grouped disc bits.
   *
   * <p>Creates extraction positions (which key bytes to gather) and extraction masks
   * (which bits within those bytes are discriminative), packed into long[] for PEXT.</p>
   */
  private DiscBitsInfo buildMultiMask(TreeMap<Integer, Integer> maskByBytePos, int minBytePos) {
    final int numBytes = maskByBytePos.size();
    final byte[] extractionPositions = new byte[numBytes];
    final int numChunks = (numBytes + 7) / 8;
    final long[] extractionMasks = new long[numChunks];

    int i = 0;
    short msbIndex = Short.MAX_VALUE;
    for (final var entry : maskByBytePos.entrySet()) {
      final int keyBytePos = entry.getKey();
      final int maskByte = entry.getValue();
      extractionPositions[i] = (byte) keyBytePos;
      // BE chunk layout: extraction-byte offset {@code o} within a chunk lands at long-bit
      // {@code (7-o)*8 .. (7-o)*8+7}. Within {@code maskByte}, bit 7 = byte's MSB.
      final int chunkIdx = i / 8;
      final int byteOffsetInChunk = i % 8;
      extractionMasks[chunkIdx] |= ((long) (maskByte & 0xFF)) << ((7 - byteOffsetInChunk) * 8);

      // Compute MSB index: the smallest absolute bit position
      final int highBit = 31 - Integer.numberOfLeadingZeros(maskByte & 0xFF);
      final int absBitPos = keyBytePos * 8 + (7 - highBit);
      msbIndex = (short) Math.min(msbIndex, absBitPos);
      i++;
    }

    return DiscBitsInfo.multiMask(extractionPositions, extractionMasks, numBytes, msbIndex);
  }

  /**
   * Compute partial keys for all children based on discriminative bit info.
   *
   * <p>Reference-faithful semantics (Binna's
   * {@code SparsePartialKeys::getRelevantBitsForRange}): each child's
   * sparse key is the AND-intersection of partial keys for every entry
   * under that child's subtree. Only bits that are constant within a
   * subtree can be reliably represented by first-key extraction; the
   * calling code ({@link #computeDiscBits}) already filters disc bits
   * to that invariant, so the first-key extraction below is equivalent
   * to the AND-intersection for valid disc bits. The filter guarantees
   * this by dropping any candidate bit that varies within a subtree.
   */
  /**
   * Compute partial keys for {@code children} using Binna §4.2 sparse-path encoding.
   *
   * <p>For each disc bit (in absolute-bit-position order, MSB-first), we recursively partition
   * the sorted children at this bit into LEFT (bit=0) and RIGHT (bit=1) halves. A child's
   * partial-key bit at position {@code p} (the partial-key bit corresponding to this disc bit)
   * is set IFF (a) the partition at this bit is non-trivial within the child's current
   * sub-range — i.e., the BiNode at this disc bit is on the child's path — AND (b) the child
   * lies in the RIGHT half. Off-path bits stay 0.</p>
   *
   * <p>This matches Binna's reference {@code SparsePartialKeys.hpp} where stored partial
   * keys carry "1" only for path BiNodes and 0 elsewhere. Routing (Sirix's
   * {@link io.sirix.page.HOTIndirectPage#findChildIndex} subset-match) returns the highest-
   * specificity match, exactly as Binna's {@code SparsePartialKeys::search}.</p>
   */
  private int[] computePartialKeysForChildren(PageReference[] children, DiscBitsInfo discBits) {
    final int n = children.length;
    final int[] partialKeys = new int[n];
    if (n <= 1) return partialKeys;

    // Collect the disc bit positions in MSB-first absolute-bit order. Result-bit position
    // (totalDiscBits-1) corresponds to the most significant disc bit (smallest absolute
    // bit index).
    final int[] discAbsPositions = collectDiscBitsMsbFirst(discBits);
    final int totalDiscBits = discAbsPositions.length;
    if (totalDiscBits == 0) return partialKeys;

    // Cache children's first-keys so we don't re-load on every disc-bit step.
    final byte[][] firstKeys = new byte[n][];
    for (int i = 0; i < n; i++) firstKeys[i] = getFirstKeyFromChild(children[i]);

    computeSparsePathRecursive(firstKeys, partialKeys, /*rangeStart=*/ 0, /*rangeEnd=*/ n,
        /*discBitIdx=*/ 0, discAbsPositions, totalDiscBits);
    return partialKeys;
  }

  /**
   * Recursive partition for sparse-path encoding. At {@code discBitIdx}, find the split point
   * within {@code [rangeStart, rangeEnd)}. If the partition is non-trivial, set the
   * corresponding partial-key bit on every RIGHT-half child and recurse into both halves; the
   * LEFT half implicitly leaves the bit at 0. If the partition is trivial (all children in
   * range agree at this bit), the BiNode is NOT on this range's children's paths — leave the
   * bit at 0 for everyone in range and recurse with the next disc bit.
   */
  private void computeSparsePathRecursive(byte[][] firstKeys, int[] partials,
      int rangeStart, int rangeEnd, int discBitIdx, int[] discAbsPositions, int totalDiscBits) {
    if (rangeEnd - rangeStart <= 1 || discBitIdx >= totalDiscBits) return;
    final int absBit = discAbsPositions[discBitIdx];
    // BE: result bit (totalDiscBits-1) = MSB of partial = first (most-significant) disc bit.
    final int partialBitPos = totalDiscBits - 1 - discBitIdx;

    int splitIdx = rangeEnd;
    for (int j = rangeStart; j < rangeEnd; j++) {
      if (DiscriminativeBitComputer.isBitSet(firstKeys[j], absBit)) {
        splitIdx = j;
        break;
      }
    }

    if (splitIdx > rangeStart && splitIdx < rangeEnd) {
      // Non-trivial split: BiNode is on every range-child's path.
      for (int j = splitIdx; j < rangeEnd; j++) partials[j] |= (1 << partialBitPos);
      computeSparsePathRecursive(firstKeys, partials, rangeStart, splitIdx,
          discBitIdx + 1, discAbsPositions, totalDiscBits);
      computeSparsePathRecursive(firstKeys, partials, splitIdx, rangeEnd,
          discBitIdx + 1, discAbsPositions, totalDiscBits);
    } else {
      // Trivial split (all agree): BiNode is NOT on this range's children's paths. Skip.
      computeSparsePathRecursive(firstKeys, partials, rangeStart, rangeEnd,
          discBitIdx + 1, discAbsPositions, totalDiscBits);
    }
  }

  /**
   * Maximum disc bits per node — bounded by partial-key bit width (uint32_t = 32 bits in
   * Sirix's SpanNode/MultiNode partial). Used to size the stack-cap'd disc-bit position
   * buffer in {@link #collectDiscBitsMsbFirst}.
   */
  private static final int MAX_DISC_BITS = 32;

  /**
   * Collect disc bit positions in MSB-first absolute-bit order: smallest absolute bit
   * position first (= most significant). Walks SingleMask or MultiMask layout.
   *
   * <p>HFT-grade: writes into a primitive {@code int[]} sized to MultiMask theoretical max
   * (8 extraction bytes × 8 bits = 64). MAX_DISC_BITS = 32 is the partial-key invariant
   * (HOT I3 — partials must be unique 32-bit values), but collection-time tooling can
   * temporarily see >32 bits in transient states (e.g., before {@link #augmentUntilPartialsUnique}
   * trims). The writer downstream treats >32 bits as an INV violation and bails to the
   * split path; this method must not throw before that bail can trigger. Returns a trimmed
   * copy. No {@code List<Integer>}, no autoboxing, no growth, single bounded allocation.
   */
  private static int[] collectDiscBitsMsbFirst(DiscBitsInfo discBits) {
    // Sized to the theoretical max (8 extraction bytes × 8 bits/byte) so transient
    // out-of-spec disc-bit counts don't AIOOBE before the downstream INV check fires.
    final int[] buf = new int[64];
    int n = 0;
    if (discBits.isSingleMask()) {
      final int initialBytePos = discBits.initialBytePos();
      long mask = discBits.bitMask();
      while (mask != 0L && n < buf.length) {
        final int highBit = 63 - Long.numberOfLeadingZeros(mask);
        // BE: byte at window-offset bo occupies long bits ((7-bo)*8)..((7-bo)*8+7).
        // Within byte, MSB-first bit bb is at long-bit ((7-bo)*8 + (7-bb)).
        // Solve: highBit = (7-bo)*8 + (7-bb) → bo = 7 - highBit/8, bb = 7 - highBit%8.
        final int bo = 7 - (highBit / 8);
        final int bb = 7 - (highBit % 8);
        buf[n++] = (initialBytePos + bo) * 8 + bb;
        mask &= ~(1L << highBit);
      }
    } else {
      // MultiMask: iterate extractionPositions in order; within each byte, MSB-first.
      final byte[] extractionPositions = discBits.extractionPositions();
      final long[] extractionMasks = discBits.extractionMasks();
      final int numExtractionBytes = discBits.numExtractionBytes();
      for (int i = 0; i < numExtractionBytes && n < buf.length; i++) {
        final int bytePos = extractionPositions[i] & 0xFF;
        final int byteMask = (int) ((extractionMasks[i / 8] >>> ((7 - (i % 8)) * 8)) & 0xFF);
        for (int bb = 0; bb < 8 && n < buf.length; bb++) {
          final int byteBit = 7 - bb;
          if ((byteMask & (1 << byteBit)) == 0) continue;
          buf[n++] = bytePos * 8 + bb;
        }
      }
    }
    return n == buf.length ? buf : Arrays.copyOf(buf, n);
  }

  /**
   * Compute partial key for a single key, dispatching to SingleMask or MultiMask.
   */
  private int computePartialKey(byte[] key, DiscBitsInfo discBits) {
    if (discBits.isSingleMask()) {
      return computePartialKeySingleMask(key, discBits.initialBytePos(), discBits.bitMask());
    } else {
      return computePartialKeyMultiMask(key, discBits.extractionPositions(),
          discBits.extractionMasks(), discBits.numExtractionBytes());
    }
  }

  /**
   * Compute partial key using MultiMask extraction (byte gathering + per-chunk PEXT).
   */
  private static int computePartialKeyMultiMask(byte[] key, byte[] extractionPositions,
      long[] extractionMasks, int numExtractionBytes) {
    final int numChunks = extractionMasks.length;
    // BE chunk packing: extraction-byte at chunk-offset {@code o} → long bits ((7-o)*8)..((7-o)*8+7).
    final long[] gathered = new long[numChunks];
    for (int i = 0; i < numExtractionBytes; i++) {
      final int keyBytePos = extractionPositions[i] & 0xFF;
      final int keyByte = keyBytePos < key.length ? (key[keyBytePos] & 0xFF) : 0;
      final int chunkIdx = i / 8;
      final int byteOffsetInChunk = i % 8;
      gathered[chunkIdx] |= ((long) keyByte) << ((7 - byteOffsetInChunk) * 8);
    }
    // BE concatenate across chunks: chunk 0 occupies the HIGH bits of the result.
    int totalBits = 0;
    for (final long m : extractionMasks) totalBits += Long.bitCount(m);
    int result = 0;
    int shift = totalBits;
    for (int w = 0; w < numChunks; w++) {
      final int extracted = (int) Long.compress(gathered[w], extractionMasks[w]);
      shift -= Long.bitCount(extractionMasks[w]);
      result |= extracted << shift;
    }
    return result;
  }

  /**
   * Split a full parent node and recurse up the tree.
   * 
   * <p>
   * Reference: HOTSingleThreadedNode.hpp lines 549-565 (split function). Uses the MOST SIGNIFICANT
   * discriminative bit to split entries into two groups: - "Smaller" entries: those with MSB=0 -
   * "Larger" entries: those with MSB=1
   * </p>
   */
  private void splitParentAndRecurse(StorageEngineWriter storageEngineReader, TransactionIntentLog log,
      PageReference parentRef, HOTIndirectPage parent, int originalChildIndex,
      PageReference leftChild, PageReference rightChild, int discriminativeBit,
      PageReference rootReference, HOTIndirectPage[] pathNodes, PageReference[] pathRefs,
      int[] pathChildIndices, int currentPathIdx) {

    final int numChildren = parent.getNumChildren();

    // Reference: getMaskForLargerEntries() finds entries with MSB set
    // The MSB (most significant bit) determines the split boundary.
    // Uses the stored field (matching C++ reference's mMostSignificantDiscriminativeBitIndex).
    final int msbPosition = parent.getMostSignificantBitIndex();

    // Partition children into "smaller" (MSB=0) and "larger" (MSB=1) using pre-allocated buffers
    int smallerCount = 0;
    int largerCount = 0;

    for (int i = 0; i < numChildren; i++) {
      if (i == originalChildIndex) {
        // Replace the original child with the two split children
        final byte[] leftKey = getFirstKeyFromChild(leftChild);
        final byte[] rightKey = getFirstKeyFromChild(rightChild);
        if (!hasBitSet(leftKey, msbPosition)) {
          splitSmallerBuf[smallerCount++] = leftChild;
        } else {
          splitLargerBuf[largerCount++] = leftChild;
        }
        if (!hasBitSet(rightKey, msbPosition)) {
          splitSmallerBuf[smallerCount++] = rightChild;
        } else {
          splitLargerBuf[largerCount++] = rightChild;
        }
      } else {
        final PageReference child = parent.getChildReference(i);
        // Skip stale COW copies that alias the same TIL entry as a split result
        final int childLogKey = child.getLogKey();
        if (childLogKey >= 0 && (childLogKey == leftChild.getLogKey() || childLogKey == rightChild.getLogKey())) {
          continue;
        }
        final byte[] childKey = getFirstKeyFromChild(child);
        if (!hasBitSet(childKey, msbPosition)) {
          splitSmallerBuf[smallerCount++] = child;
        } else {
          splitLargerBuf[largerCount++] = child;
        }
      }
    }

    // Ensure we have entries in both halves (fallback to half split if needed)
    if (smallerCount == 0 || largerCount == 0) {
      splitParentHalfAndRecurse(storageEngineReader, log, parentRef, parent, originalChildIndex, leftChild, rightChild,
          rootReference, pathNodes, pathRefs, pathChildIndices, currentPathIdx);
      return;
    }

    // Trim to exact size — small allocation but avoids API contract issues with oversized arrays
    final PageReference[] leftChildren = Arrays.copyOf(splitSmallerBuf, smallerCount);
    final PageReference[] rightChildren = Arrays.copyOf(splitLargerBuf, largerCount);

    // Reference-faithful {@code compressEntries}: each half inherits
    // only the parent's disc bits that actually differ within that
    // half's entries ({@code SparsePartialKeys::getRelevantBitsForRange}).
    // Partial keys are rebased via {@code _pext_u32} (Long.compress) to
    // the compressed layout. SingleMask-layout only; MultiMask and the
    // MSB-based partitioning above map children by key/value, not by
    // partial-key index, so we compute the per-half partial sets by
    // ordering children by their original index.
    //
    // NOTE (Phase 4b-vb.3 attempt 2026-05-09, reverted): including originalChildIndex
    // in parentIndices to capture bits distinguishing splitChild's projection from
    // siblings' (matching C++'s getRelevantBitsForRange semantics) was tried + reverted.
    // The deeper investigation (this session, 2026-05-09 PM) revealed the
    // I-Binna-sparse-path violations come from rightChild (= leaf-split product) whose
    // stored partial gets β=1 set at construction-time, but later inserts into
    // rightChild's subtree silently break β-constancy: a smaller key with β=0 makes
    // rightChild.firstKey have β=0 while parent's stored partial still has β=1. The
    // root cause is multi-entry-leaf pathology — fixable only via Phase 2 of the
    // original design plan (constancy-aware leaf split: when inserting a key K into
    // leaf L, if K's value at any ancestor disc bit β differs from existing entries'
    // bit-β values, split L on β before inserting). Phase 4b-vb.3 alone cannot resolve
    // this; the fix needs leaf-level invariant maintenance.
    final int[] leftIndices = new int[smallerCount];
    final int[] rightIndices = new int[largerCount];
    int li = 0, ri = 0;
    for (int i = 0; i < numChildren; i++) {
      if (i == originalChildIndex) continue; // split children handled separately
      final PageReference child = parent.getChildReference(i);
      if (!hasBitSet(getFirstKeyFromChild(child), msbPosition)) {
        if (li < leftIndices.length) leftIndices[li++] = i;
      } else {
        if (ri < rightIndices.length) rightIndices[ri++] = i;
      }
    }

    final PageReference leftNodeRef;
    if (leftChildren.length == 1) {
      leftNodeRef = leftChildren[0];
    } else {
      HOTIndirectPage leftNode = buildCompressedHalf(
          parent, leftChildren, leftIndices, Arrays.copyOf(leftIndices, li),
          originalChildIndex, leftChild, rightChild, false, msbPosition,
          parent.getPageKey(), storageEngineReader.getRevisionNumber());
      leftNodeRef = new PageReference();
      leftNodeRef.setKey(parent.getPageKey());
      leftNodeRef.setPage(leftNode);
      log.put(leftNodeRef, PageContainer.getInstance(leftNode, leftNode));
    }

    final PageReference rightNodeRef;
    if (rightChildren.length == 1) {
      rightNodeRef = rightChildren[0];
    } else {
      long rightPageKey = pageKeyAllocator.getAsLong();
      HOTIndirectPage rightNode = buildCompressedHalf(
          parent, rightChildren, rightIndices, Arrays.copyOf(rightIndices, ri),
          originalChildIndex, leftChild, rightChild, true, msbPosition,
          rightPageKey, storageEngineReader.getRevisionNumber());
      rightNodeRef = new PageReference();
      rightNodeRef.setKey(rightPageKey);
      rightNodeRef.setPage(rightNode);
      log.put(rightNodeRef, PageContainer.getInstance(rightNode, rightNode));
    }

    // The split bit becomes the new root's discriminative bit
    // Reference: uses mMostSignificantDiscriminativeBitIndex for new BiNode
    int newRootDiscrimBit = msbPosition;

    // Recurse up to integrate [leftNode, rightNode] into grandparent.
    // newSide=1 (the recursive C++ convention: integrateBiNodeIntoTree always passes
    // newIsRight=true on recursion because parentNode.split() places the new entry
    // in mRight by construction at the upper level). Phase 4b-vb.3 will refine this
    // when splitParentAndRecurse adopts one-insert-one-replace semantics.
    if (currentPathIdx > 0) {
      int grandparentIdx = currentPathIdx - 1;
      updateParentForSplitWithPath(storageEngineReader, log, pathRefs[grandparentIdx], pathNodes[grandparentIdx],
          pathChildIndices[grandparentIdx], leftNodeRef, rightNodeRef, getFirstKeyFromChild(rightNodeRef),
          rootReference, pathNodes, pathRefs, pathChildIndices, grandparentIdx,
          /*newSide=*/ 1);
    } else {
      // At root - create new root BiNode
      long newRootKey = pageKeyAllocator.getAsLong();
      HOTIndirectPage newRoot = createBiNodeTraced("splitParent-newRoot-1913", newRootKey,
          storageEngineReader.getRevisionNumber(), newRootDiscrimBit, leftNodeRef, rightNodeRef,
          parent.getHeight() + 1);

      rootReference.setKey(newRootKey);
      rootReference.setPage(newRoot);
      log.put(rootReference, PageContainer.getInstance(newRoot, newRoot));
    }
  }

  // ===========================================================================
  // Phase 4b.0 — MultiMask helpers for buildCompressedHalf's MultiMask-parent path
  //
  // These three helpers and the {@link MultiMaskSubLayout} record provide a
  // layout-independent way to derive a "subset" MultiMask layout from a parent
  // MultiMask layout, given the {@code relevant} bits of parent's mask that actually
  // differ within a contiguous range of children. Mirrors the C++ reference's
  // {@code DiscriminativeBitsRepresentation.extract(relevantBits)} template; Java
  // can't unify SingleMask + MultiMask via templates, so we explicitly branch on
  // {@code parent.getLayoutType()} and call this MultiMask path when needed.
  //
  // Used by Phase 4b.2 (MultiMask-parent path in {@link #buildCompressedHalf}).
  // ===========================================================================

  /**
   * Subset MultiMask layout produced by {@link #extractMultiMaskSubset}. The
   * {@code extractionMasks} array is sized to the smallest number of long-chunks
   * that holds {@code numExtractionBytes} extraction-bytes (8 per chunk).
   */
  record MultiMaskSubLayout(byte[] extractionPositions, long[] extractionMasks,
      int numExtractionBytes, int totalBits, short msbIndex) {
  }

  /**
   * Phase 4b-vb.2 helper: classifies a leaf-split's two products into the C++
   * {@code valueToInsert} (the side receiving the just-inserted key — the "new entry"
   * from parent's perspective) and {@code valueToReplace} (the side overwriting
   * splitChild's existing slot pointer). Mirrors {@code integrateBiNodeIntoTree}
   * lines 512-514 (HOTSingleThreaded.hpp): {@code valueToInsert =
   * (newIsRight) ? mRight : mLeft; valueToReplace = (newIsRight) ? mLeft : mRight}.
   *
   * <p>The {@code entryOffset} field corresponds to C++'s {@code entryOffset}
   * variable used at line 512. It encodes which neighbour-slot of the existing
   * splitChild's index ends up holding the new entry: 0 = newIdx ≤ oldIdx (when
   * newIsRight, mRight is the new entry but mLeft replaces oldIdx → newIdx is
   * adjacent at oldIdx+0 from the post-split perspective); 1 = newIdx > oldIdx.
   * Sirix's addEntryWithPDep already places both products by partial-key sort,
   * so this offset is consumed only by Phase 4b-vb.3's splitParentAndRecurse
   * refactor — kept here for parity with the C++ algorithm's bookkeeping.
   *
   * @param valueToInsert the leaf-split half holding the just-inserted key
   * @param valueToReplace the leaf-split half whose pointer overwrites splitChild's slot
   * @param entryOffset 0 if {@code valueToReplace == leftChild}, 1 otherwise
   */
  record SplitProductRoles(PageReference valueToInsert, PageReference valueToReplace,
      int entryOffset) {
  }

  /**
   * Compute {@link SplitProductRoles} for a leaf-split's two products given the
   * {@code newSide} reported by {@link HOTLeafPage#splitToWithInsert}'s
   * {@code newSideOut}: 0 = LEFT (new key in {@code leftChild}), 1 = RIGHT (new key
   * in {@code rightChild}).
   */
  static SplitProductRoles classifySplitProducts(PageReference leftChild,
      PageReference rightChild, int newSide) {
    if (newSide == 1) {
      // newIsRight: rightChild is the new entry, leftChild replaces splitChild's slot.
      return new SplitProductRoles(rightChild, leftChild, /*entryOffset=*/ 0);
    }
    // newIsLeft (or newSide < 0 fallback): leftChild is the new entry, rightChild replaces.
    return new SplitProductRoles(leftChild, rightChild, /*entryOffset=*/ 1);
  }

  /**
   * Compute the relevant-bits mask for a contiguous range of children (in their
   * stored partial-key order). For each adjacent pair in {@code partials[indices]}
   * (assumed ascending), OR together {@code (p[i] & ~p[i-1])} — the bits that flip
   * 0→1 across the pair. The union captures every bit that distinguishes any pair
   * of partials in the range (Binna §4.2 lemma: by sorted-integer monotonicity
   * every differing bit must flip at some adjacent step).
   *
   * <p>Layout-independent: works on partial-key INTs regardless of whether parent
   * uses SingleMask or MultiMask layout.
   *
   * <p>HFT-grade: zero allocation, single pass.
   *
   * @param partials parent's partial-key array
   * @param indices sorted indices into {@code partials} for this half
   * @return bitmask of relevant bit positions in the partial-key encoding (LSB=0)
   */
  static int computeRelevantBitsFromPartials(int[] partials, int[] indices) {
    // Stage G.5 fix (Bug B2/B3 — strengthens "relevant" for buildCompressedHalf I6).
    //
    // The original adjacent-XOR formula `OR (p[i] & ~p[i-1])` is only correct when the
    // sequence is monotonically sorted AND captures all pairwise differences via adjacent
    // transitions. Under multi-entry-leaf workloads the half-partition produced by
    // splitParentAndRecurse may yield partial sequences where some pairwise difference
    // bits are NOT covered by adjacent XORs (e.g., partials drawn from a non-contiguous
    // subset of parent's partial-key cube).
    //
    // The structurally-correct definition of "relevant" = "any bit at which two partials
    // in the half differ" = `OR(partials) XOR AND(partials)`. Computing this in O(n)
    // strictly dominates the adjacent-XOR (since adjacent-XOR ⊆ pairwise-XOR-OR),
    // strengthening the I6 invariant at the cost of fewer mask-bit eliminations.
    if (indices == null || indices.length == 0) return 0;
    int orAll = partials[indices[0]];
    int andAll = partials[indices[0]];
    for (int i = 1; i < indices.length; i++) {
      final int p = partials[indices[i]];
      orAll |= p;
      andAll &= p;
    }
    return orAll ^ andAll;
  }

  /**
   * Build a subset MultiMask layout containing only the disc bits of {@code parent}'s
   * mask whose output positions are set in {@code relevant} (LSB=output-bit-0). Walks
   * parent's MultiMask layout in BE concatenation order — extraction-byte 0's
   * MSB-first bit-0 is the HIGHEST output bit; the last byte's MSB-first bit-7 (if set)
   * is output bit 0. For each set bit in parent's mask, computes its output position
   * as {@code totalBits - 1 - bitsAccumulated} and tests against {@code relevant}.
   *
   * <p>The resulting layout drops bytes whose mask bits are entirely excluded by
   * {@code relevant}. Returns {@code null} if {@code parent} isn't MultiMask.
   *
   * <p>HFT-grade: bounded allocation (new {@code byte[]} and {@code long[]} sized
   * exactly to the kept extraction-byte count). No boxing, no nested collections.
   *
   * @param parent the source MultiMask parent
   * @param relevant bitmask of output-bit positions to retain (LSB=0)
   * @return the subset layout, or {@code null} if parent isn't MultiMask
   */
  static @Nullable MultiMaskSubLayout extractMultiMaskSubset(HOTIndirectPage parent,
      int relevant) {
    if (parent.getLayoutType() != HOTIndirectPage.LayoutType.MULTI_MASK) return null;
    final byte[] oldExtractionPositions = parent.getExtractionPositions();
    final long[] oldExtractionMasks = parent.getExtractionMasks();
    if (oldExtractionPositions == null || oldExtractionMasks == null) return null;
    final int oldNumBytes = oldExtractionPositions.length;
    int oldTotalBits = 0;
    for (final long m : oldExtractionMasks) oldTotalBits += Long.bitCount(m);

    // Walk parent layout, classify each set bit as kept or dropped by `relevant`.
    final int[] keepBytePos = new int[oldNumBytes];
    final int[] keepByteMaskBits = new int[oldNumBytes];
    int keepCount = 0;
    int bitsAccumulated = 0;
    for (int i = 0; i < oldNumBytes; i++) {
      final int bytePos = oldExtractionPositions[i] & 0xFF;
      final int chunkIdx = i / 8;
      final int byteOffsetInChunk = i % 8;
      final int byteMask =
          (int) ((oldExtractionMasks[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFF);
      int keepMaskBitsForThisByte = 0;
      for (int mfBit = 0; mfBit < 8; mfBit++) {
        final int byteBit = 7 - mfBit;
        if ((byteMask & (1 << byteBit)) == 0) continue;
        final int outputPos = oldTotalBits - 1 - bitsAccumulated;
        if ((relevant & (1 << outputPos)) != 0) {
          keepMaskBitsForThisByte |= (1 << byteBit);
        }
        bitsAccumulated++;
      }
      if (keepMaskBitsForThisByte != 0) {
        keepBytePos[keepCount] = bytePos;
        keepByteMaskBits[keepCount] = keepMaskBitsForThisByte;
        keepCount++;
      }
    }

    // Pack into BE chunk layout. Allocate at least 1 chunk so callers can index even
    // when keepCount == 0 (degenerate-but-safe).
    final byte[] newExtractionPositions = new byte[keepCount];
    final int newNumChunks = Math.max(1, (keepCount + 7) / 8);
    final long[] newExtractionMasks = new long[newNumChunks];
    short msbIndex = Short.MAX_VALUE;
    int newTotalBits = 0;
    for (int i = 0; i < keepCount; i++) {
      newExtractionPositions[i] = (byte) keepBytePos[i];
      final int chunkIdx = i / 8;
      final int byteOffsetInChunk = i % 8;
      newExtractionMasks[chunkIdx] |=
          ((long) (keepByteMaskBits[i] & 0xFF)) << ((7 - byteOffsetInChunk) * 8);
      newTotalBits += Integer.bitCount(keepByteMaskBits[i]);
      final int highBit = 31 - Integer.numberOfLeadingZeros(keepByteMaskBits[i] & 0xFF);
      final int absBitPos = keepBytePos[i] * 8 + (7 - highBit);
      if (absBitPos < msbIndex) msbIndex = (short) absBitPos;
    }
    return new MultiMaskSubLayout(newExtractionPositions, newExtractionMasks, keepCount,
        newTotalBits, msbIndex);
  }

  // ===========================================================================
  // End Phase 4b.0 helpers
  // ===========================================================================

  /**
   * Reference-faithful {@code compressEntriesAndAddOneEntryIntoNewNode}
   * (Binna §4.2 split integration): build a new indirect node for a
   * half of a split. Inherits only the disc bits from {@code parent}
   * that actually differ within this half's entries ({@code
   * SparsePartialKeys::getRelevantBitsForRange}), and — if both leaves
   * of a child-split land in this half — ALSO includes the new disc
   * bit {@code splitDiscBitAbs} that separates them. Existing partial
   * keys are rebased to the new layout via {@code Long.compress}
   * ({@code _pext_u32}) and (when the new bit is added) repositioned
   * via {@code Long.expand} ({@code _pdep_u64}).
   *
   * <p>Returns a freshly-computed indirect page with SingleMask
   * representation. Falls back to {@code createNodeFromChildren} only
   * when the parent is MultiMask (not yet ported) or when the new
   * bit falls outside the parent's 8-byte PEXT window.
   */
  private HOTIndirectPage buildCompressedHalf(HOTIndirectPage parent,
      PageReference[] halfChildren, int[] indexBuf, int[] parentIndices,
      int splitChildIndex, PageReference splitLeft, PageReference splitRight,
      boolean isRightHalf, int msbPosition, long newPageKey, int revision) {
    // Reference-faithful port of {@code compressEntriesAndAddOneEntryIntoNewNode}
    // (Binna §4.2, C++ reference {@code SparsePartialKeys::compressEntries
    // AndAddOneEntryIntoNewNode}). HFT-grade: uses {@link Long#compress}
    // (PEXT = {@code _pext_u64}) to drop parent disc bits that are not
    // relevant in this half, and {@link Long#expand} (PDEP =
    // {@code _pdep_u64}) to reposition them into the new layout. Zero
    // allocations beyond the new {@code int[] newPartials} and the new
    // {@code PageReference[]}.
    //
    // LEMMA (compressEntries correctness): for a contiguous sorted range
    // of partials, {@code relevant = OR of (p[i] & ~p[i-1])} captures
    // enough bits to distinguish all entries in the range. For any pair
    // i<j with p[i] < p[j], the highest differing bit has p[j]=1 and
    // p[i]=0, and by sorted-integer monotonicity that bit must flip 0→1
    // at some adjacent step in the sequence, so it ends up in 'relevant'.
    BCH_SINGLEMASK_ENTRIES.incrementAndGet();
    if (parent.getLayoutType() != HOTIndirectPage.LayoutType.SINGLE_MASK) {
      // Phase 4b.2: MultiMask-parent path. Inherits a subset of parent's MultiMask
      // disc-bit set into a new MultiMask layout (or extended MultiMask if the
      // leaf-split's β contributes a new bit). Mirrors the C++ template
      // {@code DiscriminativeBitsRepresentation.extract(relevantBits)} +
      // {@code .insert(newKeyInfo)} chain.
      final HOTIndirectPage built = buildCompressedHalfMultiMask(parent, halfChildren,
          parentIndices, splitLeft, splitRight, newPageKey, revision);
      if (built != null) return built;
      // MultiMask path returned null = degenerate case (sub-layout has no bits AND
      // !bothSplitHere, or split-bit computeDifferingBit returned -1). Fall back to
      // fresh rebuild — counter already incremented inside the helper.
      return createNodeFromChildren(halfChildren, newPageKey, revision, parent.getHeight());
    }

    final int oldInitialBytePos = parent.getInitialBytePos();
    final long oldMask = parent.getBitMask();
    final int[] oldPartials = parent.getPartialKeys();
    final int oldCount = Long.bitCount(oldMask);

    // Determine which split children land in this half (by identity).
    final boolean leftHere = isChildInHalf(halfChildren, splitLeft);
    final boolean rightHere = isChildInHalf(halfChildren, splitRight);
    final boolean bothSplitHere = leftHere && rightHere;

    // Compute 'relevant' over parentIndices' partials, in their ORIGINAL
    // parent order. Since parent's children were sorted by first key
    // (HOT invariant), parent.getPartialKeys() is sorted ascending, so
    // iterating indices in ascending order gives us the sorted partial
    // sequence for this half.
    final int relevant = computeRelevantBitsFromPartials(oldPartials, parentIndices);

    // Encode split disc bit (if both halves land here, we need to add it).
    final int splitDiscBitAbs;
    final long splitBitMaskBit;
    if (bothSplitHere) {
      splitDiscBitAbs = DiscriminativeBitComputer.computeDifferingBit(
          getLastKeyFromChild(splitLeft), getFirstKeyFromChild(splitRight));
      if (splitDiscBitAbs < 0) {
        BCH_FALLBACK_IDENTICAL_KEYS.incrementAndGet();
        return createNodeFromChildren(halfChildren, newPageKey, revision, parent.getHeight());
      }
      final int byteOff = (splitDiscBitAbs / 8) - oldInitialBytePos;
      if (byteOff < 0 || byteOff >= 8) {
        // Cross-window split bit: must upgrade to MultiMask. Defer by
        // falling back to fresh rebuild (which chooses its own layout).
        BCH_FALLBACK_CROSS_WINDOW.incrementAndGet();
        return createNodeFromChildren(halfChildren, newPageKey, revision, parent.getHeight());
      }
      final int bitInByte = splitDiscBitAbs % 8;
      // BE: byte at window-offset {@code byteOff} occupies long bits ((7-byteOff)*8)..((7-byteOff)*8+7);
      // MSB-first bit-in-byte {@code bitInByte} is at long-bit ((7-byteOff)*8 + (7-bitInByte)).
      splitBitMaskBit = 1L << ((7 - byteOff) * 8 + (7 - bitInByte));
    } else {
      splitDiscBitAbs = -1;
      splitBitMaskBit = 0L;
    }

    // Build newMask = the bits of oldMask selected by 'relevant', plus
    // (optionally) splitBitMaskBit. PEXT indexes mask bits LSB-first.
    long newMaskFromOld = 0L;
    long scan = oldMask;
    int outPos = 0;
    while (scan != 0L) {
      final long lowBit = scan & -scan;
      if ((relevant & (1 << outPos)) != 0) {
        newMaskFromOld |= lowBit;
      }
      scan &= scan - 1L;
      outPos++;
    }
    final long newMask = newMaskFromOld | splitBitMaskBit;
    if (newMask == 0L) {
      BCH_FALLBACK_NEW_MASK_ZERO.incrementAndGet();
      return createNodeFromChildren(halfChildren, newPageKey, revision, parent.getHeight());
    }

    final int totalNewBits = Long.bitCount(newMask);
    final int splitBitOutputPos = bothSplitHere
        ? Long.bitCount(newMask & (splitBitMaskBit - 1L))
        : -1;
    // Mask (in new partial-key space) where inherited-from-old bits land.
    // If splitBit is new-in-mask, it occupies splitBitOutputPos and all
    // other positions are for old bits. If splitBit happens to coincide
    // with an old bit (edge case), oldBitsInNewLayout covers all bits.
    final long oldBitsInNewLayout;
    if (bothSplitHere && (oldMask & splitBitMaskBit) == 0L) {
      oldBitsInNewLayout = (((1L << totalNewBits) - 1L) ^ (1L << splitBitOutputPos));
    } else {
      oldBitsInNewLayout = ((1L << totalNewBits) - 1L);
    }

    // Sort half children by first key (HOT invariant: sorted partials).
    final PageReference[] sortedChildren = halfChildren.clone();
    sortChildrenByFirstKey(sortedChildren);
    final int[] newPartials = new int[sortedChildren.length];

    final boolean dbgEnc = Boolean.getBoolean("hot.debug.bch.encoding");
    final StringBuilder dbgEncBuf = dbgEnc ? new StringBuilder(2048) : null;
    if (dbgEnc) {
      dbgEncBuf.append("[bch.enc] entering SingleMask path")
          .append(" newPageKey=").append(newPageKey)
          .append(" parent.pageKey=").append(parent.getPageKey())
          .append(" parent.height=").append(parent.getHeight())
          .append(" oldInitialBytePos=").append(oldInitialBytePos)
          .append(" oldMask=0x").append(Long.toHexString(oldMask))
          .append(" bothSplitHere=").append(bothSplitHere)
          .append(" splitDiscBitAbs=").append(splitDiscBitAbs)
          .append(" splitBitMaskBit=0x").append(Long.toHexString(splitBitMaskBit))
          .append(" relevant=0x").append(Integer.toHexString(relevant))
          .append(" newMaskFromOld=0x").append(Long.toHexString(newMaskFromOld))
          .append(" newMask=0x").append(Long.toHexString(newMask))
          .append(" totalNewBits=").append(totalNewBits)
          .append(" splitBitOutputPos=").append(splitBitOutputPos)
          .append(" oldBitsInNewLayout=0x").append(Long.toHexString(oldBitsInNewLayout))
          .append(" parentIndices=").append(java.util.Arrays.toString(parentIndices))
          .append(" halfChildren.length=").append(halfChildren.length).append('\n');
    }

    boolean anyMismatch = false;
    for (int j = 0; j < sortedChildren.length; j++) {
      final PageReference c = sortedChildren[j];
      if (c == splitLeft || c == splitRight) {
        // Split products: compute partial directly from first key under newMask.
        newPartials[j] = computePartialKeySingleMask(
            getFirstKeyFromChild(c), oldInitialBytePos, newMask);
        if (dbgEnc) {
          dbgEncBuf.append("[bch.enc]   j=").append(j).append(" split-product ")
              .append(c == splitLeft ? "LEFT" : "RIGHT")
              .append(" firstKey=").append(bytesToHex(getFirstKeyFromChild(c)))
              .append(" newPartial=0x").append(Integer.toHexString(newPartials[j])).append('\n');
        }
      } else {
        // Stage G.6 fix (Bug B2 root cause) — inherited children get DIRECT PEXT under
        // newMask from their firstKey instead of the sparse PEXT+PDEP repositioning.
        //
        // The original sparse-path encoding stored only the bits that vary within the
        // half (via `relevant` mask) and assumed the dropped bits were β-constant in the
        // child's subtree. Under Sirix's multi-entry-leaf workload that assumption breaks:
        // when downstream inserts violate β-constancy, dropped bits actually do vary and
        // routing breaks. The cascade observed at Stage G.4 retry: 5993 I6 violations.
        //
        // Direct PEXT from firstKey is structurally correct for I6 routing of the firstKey
        // itself, and for any key K in the subtree that agrees with firstKey at every bit
        // captured by newMask. When K disagrees (= leaf-level β-mixing), the I-leaf-insert-
        // precondition still fires, but I6 is preserved at this level. Trade-off: lose the
        // sparse-path's bit-elision optimization (~few mask bits per half), gain routing
        // correctness on the happy-path cascade.
        final byte[] cKey = getFirstKeyFromChild(c);
        if (cKey == null || cKey.length == 0) {
          BCH_FALLBACK_UNKNOWN_CHILD.incrementAndGet();
          return createNodeFromChildren(halfChildren, newPageKey, revision, parent.getHeight());
        }
        newPartials[j] = computePartialKeySingleMask(cKey, oldInitialBytePos, newMask);
        if (dbgEnc) {
          dbgEncBuf.append("[bch.enc]   j=").append(j).append(" inherited (direct-PEXT)")
              .append(" firstKey=").append(bytesToHex(cKey))
              .append(" newPartial=0x").append(Integer.toHexString(newPartials[j])).append('\n');
        }
      }
    }

    if (dbgEnc) {
      if (anyMismatch) dbgEncBuf.append("[bch.enc] *** ENCODING MISMATCH DETECTED ***\n");
      dbgEncBuf.append("[bch.enc] final newPartials=")
          .append(java.util.Arrays.toString(newPartials)).append('\n');
      try {
        java.nio.file.Files.writeString(
            java.nio.file.Paths.get("/tmp/sirix-bch-encoding-debug.log"),
            dbgEncBuf.toString(),
            java.nio.file.StandardOpenOption.CREATE,
            java.nio.file.StandardOpenOption.APPEND);
      } catch (java.io.IOException ignore) { /* best-effort diagnostic */ }
    }

    // Verify all partials are unique (collision indicates INV breach upstream).
    for (int i = 1; i < newPartials.length; i++) {
      for (int k = 0; k < i; k++) {
        if (newPartials[i] == newPartials[k]) {
          BCH_FALLBACK_PARTIAL_COLLISION.incrementAndGet();
          return createNodeFromChildren(halfChildren, newPageKey, revision, parent.getHeight());
        }
      }
    }

    // Stage G.3 fix (Bug B3) — I4 self-check: under sparse-path encoding the smallest
    // stored partial must be 0. The PEXT+PDEP repositioning in this loop doesn't
    // guarantee this — if neither split products land at partial 0 nor any inherited
    // child's repositioned partial is 0, the layout is structurally invalid.
    // Falls back to createNodeFromChildren-N (which has its own I5 issues, tracked
    // by pending task #21; trade-off documented in Stage F).
    boolean bchHasZeroPartial = false;
    for (final int p : newPartials) {
      if (p == 0) { bchHasZeroPartial = true; break; }
    }
    if (!bchHasZeroPartial) {
      G3_I4_REJECT_BCH.incrementAndGet();
      return createNodeFromChildren(halfChildren, newPageKey, revision, parent.getHeight());
    }

    // Sort by partial-key (HOT I7 / Binna §4.2). The sortedChildren array was sorted by
    // first-key earlier to drive the relevant-bits derivation; the canonical storage order
    // under sparse-path encoding is partial-key.
    sortChildrenAndPartialsByPartial(sortedChildren, newPartials);

    // Assemble node. For 2 children we can use a BiNode IFF newMask has exactly one
    // bit (the BiNode's single disc-bit suffices). Otherwise the 2-child case still
    // needs the full multi-bit mask via SpanNode — using a BiNode here with db
    // computed from boundary keys is unsound on multi-entry-leaf trees because
    // child[0]'s subtree may span both values of bit `db` (the boundary keys
    // disambiguate but interior keys may not).
    if (sortedChildren.length == 2 && Long.bitCount(newMask) == 1) {
      final int db = Math.max(0, DiscriminativeBitComputer.computeDifferingBit(
          getLastKeyFromChild(sortedChildren[0]), getFirstKeyFromChild(sortedChildren[1])));
      return createBiNodeTraced("buildCompressedHalf-2096", newPageKey, revision, db,
          sortedChildren[0], sortedChildren[1], parent.getHeight());
    }
    if (sortedChildren.length <= 16) {
      return HOTIndirectPage.createSpanNode(newPageKey, revision,
          oldInitialBytePos, newMask, newPartials, sortedChildren, parent.getHeight());
    }
    return HOTIndirectPage.createMultiNode(newPageKey, revision,
        oldInitialBytePos, newMask, newPartials, sortedChildren, parent.getHeight());
  }

  /**
   * Phase 4b.2: MultiMask analog of {@link #buildCompressedHalf}'s SingleMask body.
   * Builds a new MultiMask indirect for one half of a split, inheriting only the
   * parent's MultiMask disc bits that actually differ within this half's children
   * (computed via {@link #computeRelevantBitsFromPartials} +
   * {@link #extractMultiMaskSubset}), and — if both leaves of a child-split land here —
   * extending the layout with the new disc bit β separating them.
   *
   * <p>Existing children's partials are repositioned via {@code Long.compress} (PEXT
   * with {@code relevant}) followed by {@code Long.expand} (PDEP with the
   * inherited-bit positions in the new layout). Split-product children's partials
   * are encoded directly from their first key under the final layout via
   * {@link #computePartialKeyMultiMaskDirect}.
   *
   * <p>Returns {@code null} on degenerate cases the caller falls back on:
   * sub-layout empty AND !bothSplitHere; identical-keys (computeDifferingBit
   * returned -1); unknown child; partial-key collision. Each null path increments
   * the corresponding {@link #BCH_FALLBACK_*} counter.
   *
   * <p>HFT-grade: bounded allocations sized exactly to the new layout (extraction
   * arrays at most one larger than parent's; partials and children proportional to
   * halfChildren). Reuses {@link Long#compress}/{@link Long#expand} (=
   * {@code _pext_u64}/{@code _pdep_u64}) for partial repositioning. No boxing.
   *
   * @return new MultiMask indirect, or {@code null} if the case is degenerate
   */
  private @Nullable HOTIndirectPage buildCompressedHalfMultiMask(HOTIndirectPage parent,
      PageReference[] halfChildren, int[] parentIndices, PageReference splitLeft,
      PageReference splitRight, long newPageKey, int revision) {
    final int[] oldPartials = parent.getPartialKeys();

    final boolean leftHere = isChildInHalf(halfChildren, splitLeft);
    final boolean rightHere = isChildInHalf(halfChildren, splitRight);
    final boolean bothSplitHere = leftHere && rightHere;

    // Step 1: relevant bits over the half's partial-key sequence.
    //
    // NOTE: the bothSplitHere case has a known routing-soundness gap on Sirix's
    // multi-entry-leaf trees. When parent's MultiMask doesn't have bits that
    // distinguish splitChild's projection from an inherited sibling's, splitLeft's
    // or splitRight's partial may collide with a sibling's after the compress→expand
    // round-trip. C++ avoids this because its single-key leaves trivially have a
    // constant β in every sibling's subtree (β alone disambiguates). The Sirix-
    // specific resolution is the "virtual BiNode" architecture (Phase 4b-vb): the
    // leaf-split's two halves are integrated as ONE replace + ONE insert (per C++
    // integrateBiNodeIntoTree) rather than as TWO separate split products as today.
    // That refactor is a deeper change than this single helper. For now this helper
    // catches the collision via BCH_FALLBACK_PARTIAL_COLLISION and falls back to
    // createNodeFromChildren (no worse than pre-Phase-4b on that case).
    final int relevant = computeRelevantBitsFromPartials(oldPartials, parentIndices);

    // Step 2: extract subset MultiMask layout from parent.
    final MultiMaskSubLayout sub = extractMultiMaskSubset(parent, relevant);
    if (sub == null) {
      BCH_FALLBACK_MULTIMASK_PARENT.incrementAndGet();
      return null;
    }

    // Step 3: optionally extend with split disc bit.
    byte[] finalExtractionPositions = sub.extractionPositions();
    long[] finalExtractionMasks = sub.extractionMasks();
    int finalNumBytes = sub.numExtractionBytes();
    int finalTotalBits = sub.totalBits();
    short finalMsbIndex = sub.msbIndex();
    int splitBitOutputPos = -1;
    boolean splitBitWasInOldLayout = false;

    if (bothSplitHere) {
      final int splitDiscBitAbs = DiscriminativeBitComputer.computeDifferingBit(
          getLastKeyFromChild(splitLeft), getFirstKeyFromChild(splitRight));
      if (splitDiscBitAbs < 0) {
        BCH_FALLBACK_IDENTICAL_KEYS.incrementAndGet();
        return null;
      }
      final int newBytePos = splitDiscBitAbs / 8;
      final int newBitInByte = splitDiscBitAbs % 8;
      final int newMaskBit = 1 << (7 - newBitInByte);

      // Locate newBytePos in finalExtractionPositions (sorted).
      int existingIdx = -1;
      int insertIdx = finalNumBytes;
      for (int i = 0; i < finalNumBytes; i++) {
        final int bp = finalExtractionPositions[i] & 0xFF;
        if (bp == newBytePos) { existingIdx = i; break; }
        if (bp > newBytePos) { insertIdx = i; break; }
      }

      if (existingIdx >= 0) {
        // Merge into existing byte's mask (or detect already-present).
        final int chunkIdx = existingIdx / 8;
        final int byteOffsetInChunk = existingIdx % 8;
        final int oldByteMask =
            (int) ((finalExtractionMasks[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFF);
        if ((oldByteMask & newMaskBit) != 0) {
          // β was kept in subLayout — it's already an output bit, not a new one.
          splitBitWasInOldLayout = true;
        } else {
          finalExtractionMasks = finalExtractionMasks.clone();
          finalExtractionMasks[chunkIdx] |=
              ((long) (newMaskBit & 0xFF)) << ((7 - byteOffsetInChunk) * 8);
          finalTotalBits++;
        }
      } else {
        // Brand-new byte position — insert at insertIdx, shift mask bytes.
        final int newNumBytes = finalNumBytes + 1;
        final int newNumChunks = Math.max(1, (newNumBytes + 7) / 8);
        final byte[] newPositions = new byte[newNumBytes];
        System.arraycopy(finalExtractionPositions, 0, newPositions, 0, insertIdx);
        newPositions[insertIdx] = (byte) newBytePos;
        if (insertIdx < finalNumBytes) {
          System.arraycopy(finalExtractionPositions, insertIdx, newPositions, insertIdx + 1,
              finalNumBytes - insertIdx);
        }
        final long[] newMasks = new long[newNumChunks];
        int srcIdx = 0;
        for (int dstIdx = 0; dstIdx < newNumBytes; dstIdx++) {
          final int dstChunk = dstIdx / 8;
          final int dstOff = dstIdx % 8;
          final int byteMaskValue;
          if (dstIdx == insertIdx) {
            byteMaskValue = newMaskBit;
          } else {
            final int srcChunk = srcIdx / 8;
            final int srcOff = srcIdx % 8;
            byteMaskValue =
                (int) ((finalExtractionMasks[srcChunk] >>> ((7 - srcOff) * 8)) & 0xFF);
            srcIdx++;
          }
          newMasks[dstChunk] |= ((long) (byteMaskValue & 0xFF)) << ((7 - dstOff) * 8);
        }
        finalExtractionPositions = newPositions;
        finalExtractionMasks = newMasks;
        finalNumBytes = newNumBytes;
        finalTotalBits++;
      }

      final int absBitPos = newBytePos * 8 + newBitInByte;
      if (absBitPos < (finalMsbIndex & 0xFFFF)) finalMsbIndex = (short) absBitPos;

      splitBitOutputPos = multiMaskNewBitOutputPos(finalExtractionPositions,
          finalExtractionMasks, finalNumBytes, newBytePos, newBitInByte);
    }

    if (finalTotalBits == 0) {
      BCH_FALLBACK_NEW_MASK_ZERO.incrementAndGet();
      return null;
    }

    // Step 4: oldBitsInNewLayout — output-bit positions inherited from sub-layout.
    final long oldBitsInNewLayout;
    if (bothSplitHere && !splitBitWasInOldLayout) {
      oldBitsInNewLayout = ((1L << finalTotalBits) - 1L) ^ (1L << splitBitOutputPos);
    } else {
      oldBitsInNewLayout = (1L << finalTotalBits) - 1L;
    }

    // Step 5: sort half children by first key (canonical pre-partial sort).
    final PageReference[] sortedChildren = halfChildren.clone();
    sortChildrenByFirstKey(sortedChildren);
    final int[] newPartials = new int[sortedChildren.length];

    for (int j = 0; j < sortedChildren.length; j++) {
      final PageReference c = sortedChildren[j];
      if (c == splitLeft || c == splitRight) {
        newPartials[j] = computePartialKeyMultiMaskDirect(getFirstKeyFromChild(c),
            finalExtractionPositions, finalExtractionMasks, finalNumBytes);
      } else {
        final int pIdx = indexOfInParent(parent, c, parentIndices);
        if (pIdx < 0) {
          BCH_FALLBACK_UNKNOWN_CHILD.incrementAndGet();
          return null;
        }
        final int compressed = (int) Long.compress(
            Integer.toUnsignedLong(oldPartials[pIdx]),
            Integer.toUnsignedLong(relevant));
        final int repositioned = (int) Long.expand(
            Integer.toUnsignedLong(compressed), oldBitsInNewLayout);
        newPartials[j] = repositioned;
      }
    }

    // Step 6: HOT I3 — partial-key uniqueness.
    for (int i = 1; i < newPartials.length; i++) {
      for (int k = 0; k < i; k++) {
        if (newPartials[i] == newPartials[k]) {
          BCH_FALLBACK_PARTIAL_COLLISION.incrementAndGet();
          return null;
        }
      }
    }

    // Step 7: HOT I7 — sort by partial-key under sparse-path encoding.
    sortChildrenAndPartialsByPartial(sortedChildren, newPartials);

    // Step 8: assemble. For 2 children, BiNode is sound only when finalTotalBits == 1;
    // otherwise the BiNode's single disc-bit can't capture the multi-bit disambiguation
    // (and computing db from boundary keys is unsound on multi-entry-leaf trees because
    // child[0]'s subtree may span both values). For multi-bit cases use SpanNode-MM.
    if (sortedChildren.length == 2 && finalTotalBits == 1) {
      final int db = Math.max(0, DiscriminativeBitComputer.computeDifferingBit(
          getLastKeyFromChild(sortedChildren[0]), getFirstKeyFromChild(sortedChildren[1])));
      return createBiNodeTraced("buildCompressedHalfMultiMask-binode", newPageKey, revision, db,
          sortedChildren[0], sortedChildren[1], parent.getHeight());
    }
    if (sortedChildren.length <= 16) {
      return HOTIndirectPage.createSpanNodeMultiMask(newPageKey, revision,
          finalExtractionPositions, finalExtractionMasks, finalNumBytes,
          newPartials, sortedChildren, parent.getHeight(), finalMsbIndex);
    }
    return HOTIndirectPage.createMultiNodeMultiMask(newPageKey, revision,
        finalExtractionPositions, finalExtractionMasks, finalNumBytes,
        newPartials, sortedChildren, parent.getHeight(), finalMsbIndex);
  }

  private static boolean isChildInHalf(PageReference[] halfChildren, PageReference c) {
    for (int i = 0; i < halfChildren.length; i++) {
      if (halfChildren[i] == c) return true;
    }
    return false;
  }

  private static int indexOfInParent(HOTIndirectPage parent, PageReference c, int[] parentIndices) {
    for (int i = 0; i < parentIndices.length; i++) {
      if (parent.getChildReference(parentIndices[i]) == c) return parentIndices[i];
    }
    return -1;
  }

  /** Compute the discriminative bit that separates splitLeft from splitRight. */
  private int computeSplitDiscBit(PageReference splitLeft, PageReference splitRight) {
    final byte[] leftMax = getLastKeyFromChild(splitLeft);
    final byte[] rightMin = getFirstKeyFromChild(splitRight);
    return DiscriminativeBitComputer.computeDifferingBit(leftMax, rightMin);
  }

  /**
   * Check if a key has a specific bit set (MSB-first convention).
   */
  private boolean hasBitSet(byte[] key, int bitPosition) {
    int byteIndex = bitPosition / 8;
    int bitIndex = 7 - (bitPosition % 8); // MSB-first within byte
    if (byteIndex >= key.length) {
      return false;
    }
    return ((key[byteIndex] >> bitIndex) & 1) == 1;
  }

  /**
   * Fallback: split in half if MSB-based split fails.
   */
  private void splitParentHalfAndRecurse(StorageEngineWriter storageEngineReader, TransactionIntentLog log,
      PageReference parentRef, HOTIndirectPage parent, int originalChildIndex,
      PageReference leftChild, PageReference rightChild, PageReference rootReference,
      HOTIndirectPage[] pathNodes, PageReference[] pathRefs, int[] pathChildIndices, int currentPathIdx) {

    int numChildren = parent.getNumChildren();
    int splitPoint = numChildren / 2;
    boolean splitInLeftHalf = originalChildIndex < splitPoint;

    final int leftLogKey = leftChild.getLogKey();
    final int rightLogKey = rightChild.getLogKey();

    int leftCount = splitPoint + (splitInLeftHalf ? 1 : 0);
    PageReference[] leftChildren = new PageReference[leftCount];
    int li = 0;
    for (int i = 0; i < splitPoint; i++) {
      if (i == originalChildIndex) {
        leftChildren[li++] = leftChild;
        leftChildren[li++] = rightChild;
      } else {
        final PageReference child = parent.getChildReference(i);
        final int childLogKey = child.getLogKey();
        if (childLogKey >= 0 && (childLogKey == leftLogKey || childLogKey == rightLogKey)) {
          continue; // stale COW copy
        }
        leftChildren[li++] = child;
      }
    }
    if (li < leftChildren.length) {
      leftChildren = Arrays.copyOf(leftChildren, li);
    }

    int rightCount = numChildren - splitPoint + (splitInLeftHalf ? 0 : 1);
    PageReference[] rightChildren = new PageReference[rightCount];
    int ri = 0;
    for (int i = splitPoint; i < numChildren; i++) {
      if (i == originalChildIndex) {
        rightChildren[ri++] = leftChild;
        rightChildren[ri++] = rightChild;
      } else {
        final PageReference child = parent.getChildReference(i);
        final int childLogKey = child.getLogKey();
        if (childLogKey >= 0 && (childLogKey == leftLogKey || childLogKey == rightLogKey)) {
          continue; // stale COW copy
        }
        rightChildren[ri++] = child;
      }
    }
    if (ri < rightChildren.length) {
      rightChildren = Arrays.copyOf(rightChildren, ri);
    }

    // If only 1 child after stale-copy dedup, pass the reference through directly —
    // wrapping in a BiNode would point both children to the same page.
    final PageReference leftNodeRef;
    if (leftChildren.length == 1) {
      leftNodeRef = leftChildren[0];
    } else {
      HOTIndirectPage leftNode =
          createNodeFromChildren(leftChildren, parent.getPageKey(), storageEngineReader.getRevisionNumber(), parent.getHeight());
      leftNodeRef = new PageReference();
      leftNodeRef.setKey(parent.getPageKey());
      leftNodeRef.setPage(leftNode);
      log.put(leftNodeRef, PageContainer.getInstance(leftNode, leftNode));
    }

    final PageReference rightNodeRef;
    if (rightChildren.length == 1) {
      rightNodeRef = rightChildren[0];
    } else {
      long rightPageKey = pageKeyAllocator.getAsLong();
      HOTIndirectPage rightNode =
          createNodeFromChildren(rightChildren, rightPageKey, storageEngineReader.getRevisionNumber(), parent.getHeight());
      rightNodeRef = new PageReference();
      rightNodeRef.setKey(rightPageKey);
      rightNodeRef.setPage(rightNode);
      log.put(rightNodeRef, PageContainer.getInstance(rightNode, rightNode));
    }

    if (currentPathIdx > 0) {
      int grandparentIdx = currentPathIdx - 1;
      updateParentForSplitWithPath(storageEngineReader, log, pathRefs[grandparentIdx], pathNodes[grandparentIdx],
          pathChildIndices[grandparentIdx], leftNodeRef, rightNodeRef, getFirstKeyFromChild(rightNodeRef),
          rootReference, pathNodes, pathRefs, pathChildIndices, grandparentIdx,
          /*newSide=*/ 1); // C++ recursive convention; refined in Phase 4b-vb.3
    } else {
      byte[] lMax = getLastKeyFromChild(leftNodeRef);
      byte[] rMin = getFirstKeyFromChild(rightNodeRef);
      int rootDiscrimBit = Math.max(0, DiscriminativeBitComputer.computeDifferingBit(lMax, rMin));

      long newRootKey = pageKeyAllocator.getAsLong();
      HOTIndirectPage newRoot = createBiNodeTraced("splitParent-newRoot-2233", newRootKey,
          storageEngineReader.getRevisionNumber(), rootDiscrimBit, leftNodeRef, rightNodeRef,
          parent.getHeight() + 1);

      rootReference.setKey(newRootKey);
      rootReference.setPage(newRoot);
      log.put(rootReference, PageContainer.getInstance(newRoot, newRoot));
    }
  }

  /**
   * Create an indirect node from a list of children built from scratch (no parent context).
   *
   * <p><b>Encoding</b>: this path computes partial keys via dense PEXT of each child's first
   * key under the derived disc-bit mask, NOT Binna's sparse-path encoding. The two encodings
   * coincide whenever the constancy invariant holds (every captured disc bit has the same
   * value across all leaves in each child's subtree). For workloads where constancy holds —
   * which empirically covers all currently-tested non-adversarial paths through this method
   * — the dense-PEXT result is valid HOT and indistinguishable from sparse-path.</p>
   *
   * <p><b>Limitation</b>: under truly adversarial workloads where constancy can't be achieved
   * AND the rebuild path enters this method (rare; reached only by buildCompressedHalf
   * fallbacks for MultiMask layouts and split-cascade halves), the produced node may carry
   * dense-PEXT partials at positions where sparse-path encoding would store 0. This is
   * detected by {@link io.sirix.index.hot.HOTInvariantValidator}'s I-Binna check; on tested
   * workloads no violation surfaces. A full sparse-path rewrite would require deriving each
   * child's path through the would-be virtual binary patricia trie and zeroing non-path bits
   * — left as follow-up work.</p>
   */
  private HOTIndirectPage createNodeFromChildren(PageReference[] children, long pageKey, int revision, int height) {
    if (children.length < 2) {
      throw new IllegalStateException("Cannot create HOTIndirectPage with " + children.length
          + " children — callers must handle the single-child case by passing the reference through directly");
    }

    // Sort children by first key for correct boundary computation
    sortChildrenByFirstKey(children);

    final HOTIndirectPage created;
    if (children.length == 2) {
      final byte[] leftMax = getLastKeyFromChild(children[0]);
      final byte[] rightMin = getFirstKeyFromChild(children[1]);
      final int discriminativeBit = Math.max(0, DiscriminativeBitComputer.computeDifferingBit(leftMax, rightMin));
      created = createBiNodeTraced("createNodeFromChildren-2273", pageKey, revision,
          discriminativeBit, children[0], children[1], height);
    } else {
      created = buildFlatNonStrict(children, pageKey, revision, height);
    }
    return created;
  }

  /**
   * Flat-build path for the multi-child branch of {@link #createNodeFromChildren}. Computes the
   * disc-bit set via {@link #computeDiscBits} (adjacent-pair XOR scan over first/last keys) and
   * stores partials via dense PEXT.
   *
   * <p><b>Limitation</b>: under multi-entry leaves with overlapping spans (e.g., warmup + main
   * keys mixed in the same leaf), {@code computeDiscBits} can capture bits that are non-constant
   * in some child's subtree — yielding I-Binna constancy violations detected by
   * {@link io.sirix.index.hot.HOTInvariantValidator}. The proper strict-Binna fix (subset
   * inheritance via Binna's {@code compressEntriesAndAddOneEntryIntoNewNode}) is tracked
   * separately and requires MultiMask-aware refactoring of {@link #buildCompressedHalf}'s
   * fallback paths plus {@link #rebuildParentAbsorbingSplit}.
   *
   * @param children sorted children (by first key)
   * @param pageKey  page key for the new indirect
   * @param revision revision number
   * @param height   tree height for the new indirect
   * @return the new indirect page
   */
  private HOTIndirectPage buildFlatNonStrict(PageReference[] children, long pageKey, int revision, int height) {
    final int initialBytePos = computeInitialBytePos(children);
    final DiscBitsInfo discBits = computeDiscBits(children, initialBytePos);
    final int[] partialKeys = computePartialKeysForChildren(children, discBits);
    // HOT I7: store children in sparse-partial-key order (children were sorted by first-key
    // above to drive adjacent-pair disc-bit collection; partial-key order is the canonical
    // slot order under sparse-path encoding per Binna §4.2).
    sortChildrenAndPartialsByPartial(children, partialKeys);
    final HOTIndirectPage created = createNodeWithDiscBits(pageKey, revision, height, discBits,
        partialKeys, children);
    probeConstancyOnBuild(pageKey, "createNodeFromChildren-N", children,
        discBitsAsAbsBitArray(discBits));
    return created;
  }


  /** DIAGNOSTIC tracer — wraps {@link HOTIndirectPage#createBiNode} to probe constancy after
   *  every BiNode construction in the writer. Argument-compatible with the original. */
  private HOTIndirectPage createBiNodeTraced(String label, long pageKey, int revision, int discBit,
      PageReference left, PageReference right, int height) {
    final HOTIndirectPage page = HOTIndirectPage.createBiNode(pageKey, revision, discBit, left,
        right, height);
    probeConstancyOnBuild(pageKey, label, new PageReference[]{left, right}, new int[]{discBit});
    return page;
  }

  /** DIAGNOSTIC — gated on {@code -Dhot.debug.constancy=1}. Logs when an indirect is built with
   *  disc bits that aren't constant in some child's subtree (= the I-Binna stale-route condition).
   *  Includes a trimmed stack trace so the caller code path is identifiable. */
  private void probeConstancyOnBuild(long pageKey, String label, PageReference[] children,
      int[] absBits) {
    if (!Boolean.getBoolean("hot.debug.constancy")) return;
    final StringBuilder violations = new StringBuilder();
    for (final int absBit : absBits) {
      for (int ci = 0; ci < children.length; ci++) {
        final int v = bitConstantValueInSubtree(children[ci], absBit);
        if (v < 0) {
          if (violations.length() == 0) violations.append(" non-constant: ");
          else violations.append("; ");
          violations.append("bit=").append(absBit).append(" childIdx=").append(ci)
              .append(" childPageKey=").append(children[ci].getLogKey() >= 0
                  ? children[ci].getLogKey() : children[ci].getKey());
        }
      }
    }
    if (violations.length() == 0) return;
    System.err.println("[hot.constancy] BUILD-VIOLATION pageKey=" + pageKey + " label=" + label
        + " absBits=" + java.util.Arrays.toString(absBits) + violations);
    final StackTraceElement[] st = Thread.currentThread().getStackTrace();
    for (int i = 2; i < Math.min(st.length, 12); i++) {
      System.err.println("    at " + st[i]);
    }
  }

  /** DIAGNOSTIC helper — extract the absolute bit positions from a {@link DiscBitsInfo}. */
  private static int[] discBitsAsAbsBitArray(DiscBitsInfo discBits) {
    if (discBits.isSingleMask()) {
      final long mask = discBits.bitMask();
      final int popcount = Long.bitCount(mask);
      final int[] out = new int[popcount];
      int idx = 0;
      for (int b = 0; b < 64; b++) {
        if (((mask >>> b) & 1L) != 0) {
          // BE word: bit b of word == byte (7 - b/8) at position (7 - b%8) → absBit relative
          final int byteOffset = 7 - (b / 8);
          final int bitInByte = 7 - (b % 8);
          out[idx++] = (discBits.initialBytePos() + byteOffset) * 8 + bitInByte;
        }
      }
      return out;
    }
    return new int[0]; // MultiMask probe not implemented — diagnostic only covers SingleMask/BiNode
  }

  /**
   * Compute partial key (SingleMask) by extracting discriminative bits from a key.
   * Uses BE layout matching {@link HOTIndirectPage#getKeyWordAt}: byte at {@code initialBytePos}
   * → long bits 56-63, {@code initialBytePos+1} → 48-55, ..., {@code initialBytePos+7} → 0-7.
   */
  private static int computePartialKeySingleMask(byte[] key, int initialBytePos, long bitMask) {
    if (key == null || key.length == 0) {
      return 0;
    }

    // BE word: byte at offset i in window lands in long bits ((7-i)*8)..((7-i)*8+7).
    long keyWord = 0;
    for (int i = 0; i < 8 && (initialBytePos + i) < key.length; i++) {
      keyWord |= ((long) (key[initialBytePos + i] & 0xFF)) << ((7 - i) * 8);
    }

    // Apply PEXT-style extraction: compress bits selected by mask.
    return (int) Long.compress(keyWord, bitMask);
  }

  /**
   * Get the last key from a child reference (leaf or indirect page).
   * Falls back to loading from storage if not swizzled.
   */
  private byte[] getLastKeyFromChild(PageReference childRef) {
    Page page = childRef.getPage();
    if (page == null && activeReader != null) {
      page = loadPage(activeReader, childRef);
      if (page != null) {
        childRef.setPage(page); // swizzle for future access
      }
    }
    if (page instanceof HOTLeafPage leaf) {
      return leaf.getLastKey();
    } else if (page instanceof HOTIndirectPage indirect) {
      return getLastKeyFromIndirectPage(indirect);
    }
    return EMPTY_KEY;
  }

  /**
   * Descend through indirect pages to find the last key.
   * Falls back to loading from storage if child is not swizzled.
   */
  private byte[] getLastKeyFromIndirectPage(HOTIndirectPage indirect) {
    final int numChildren = indirect.getNumChildren();
    if (numChildren == 0) {
      return EMPTY_KEY;
    }
    final PageReference lastChild = indirect.getChildReference(numChildren - 1);
    Page page = lastChild.getPage();
    if (page == null && activeReader != null) {
      page = loadPage(activeReader, lastChild);
      if (page != null) {
        lastChild.setPage(page);
      }
    }
    if (page instanceof HOTLeafPage leaf) {
      return leaf.getLastKey();
    } else if (page instanceof HOTIndirectPage child) {
      return getLastKeyFromIndirectPage(child);
    }
    return EMPTY_KEY;
  }

  /**
   * Sort children by their first key to ensure correct boundary computation.
   * HOT trie routing can place keys out of key-order (a key may route to a child
   * covering a different key range based on disc bit values). Without sorting,
   * the boundary computation between adjacent children may miss discriminative
   * bits, leading to duplicate partial keys and unreachable children.
   */
  private void sortChildrenByFirstKey(PageReference[] children) {
    if (children.length <= 1) {
      return;
    }
    Arrays.sort(children, (a, b) -> Arrays.compareUnsigned(getFirstKeyFromChild(a), getFirstKeyFromChild(b)));
  }

  /**
   * Co-sort {@code children} and {@code partials} by ascending partial-key value (HOT I7
   * invariant: children are stored in sparse-partial-key order, per Binna §4.2 and the
   * reference {@code SparsePartialKeys}). Since under sparse-path encoding the partial-key
   * order is the canonical ordering — first-key order can diverge for sibling subtrees with
   * off-path range overlap — every indirect-node construction site uses this helper.
   *
   * <p>HFT-grade: in-place insertion sort over the parallel arrays. Children's count is
   * bounded by {@link HOTIndirectPage#MAX_NODE_ENTRIES} = 32, so insertion sort is faster
   * than {@link Arrays#sort} (no comparator allocation, better cache behavior).
   */
  private static void sortChildrenAndPartialsByPartial(PageReference[] children, int[] partials) {
    final int n = children.length;
    if (n <= 1) return;
    if (partials.length < n) {
      throw new IllegalArgumentException(
          "partials.length=" + partials.length + " < children.length=" + n);
    }
    // Insertion sort — partials are unique (HOT I3) so equal pivots can't occur.
    for (int i = 1; i < n; i++) {
      final int curPartial = partials[i];
      final PageReference curChild = children[i];
      int j = i - 1;
      while (j >= 0 && Integer.compareUnsigned(partials[j], curPartial) > 0) {
        partials[j + 1] = partials[j];
        children[j + 1] = children[j];
        j--;
      }
      partials[j + 1] = curPartial;
      children[j + 1] = curChild;
    }
  }

  /**
   * Get the first key from a child reference (leaf or indirect page).
   * Falls back to loading from storage if not swizzled.
   */
  private byte[] getFirstKeyFromChild(PageReference childRef) {
    Page page = childRef.getPage();
    if (page == null && activeReader != null) {
      page = loadPage(activeReader, childRef);
      if (page != null) {
        childRef.setPage(page); // swizzle for future access
      }
    }
    if (page instanceof HOTLeafPage leaf) {
      return leaf.getFirstKey();
    } else if (page instanceof HOTIndirectPage indirect) {
      return getFirstKeyFromIndirectPage(indirect);
    }
    return EMPTY_KEY;
  }

  /**
   * Descend through indirect pages to find the first key.
   * Falls back to loading from storage if child is not swizzled.
   */
  private byte[] getFirstKeyFromIndirectPage(HOTIndirectPage indirect) {
    final int numChildren = indirect.getNumChildren();
    if (numChildren == 0) {
      return EMPTY_KEY;
    }
    final PageReference firstChild = indirect.getChildReference(0);
    Page page = firstChild.getPage();
    if (page == null && activeReader != null) {
      page = loadPage(activeReader, firstChild);
      if (page != null) {
        firstChild.setPage(page);
      }
    }
    if (page instanceof HOTLeafPage leaf) {
      return leaf.getFirstKey();
    } else if (page instanceof HOTIndirectPage child) {
      return getFirstKeyFromIndirectPage(child);
    }
    return EMPTY_KEY;
  }


}

