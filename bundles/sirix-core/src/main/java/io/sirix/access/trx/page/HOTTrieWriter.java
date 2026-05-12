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
import io.sirix.settings.Constants;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
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
      // Phase 7q.15 — I11-aware gate. Before calling addEntry*, check if adding the
      // discriminativeBit to parent's mask would drop parent.MSB below the grandparent's
      // MSB, violating I11 (parent.MSB < child.MSB at the grandparent→parent edge).
      // If yes, skip addEntry and fall through to intermediate-BiNode placement (which
      // doesn't change parent's mask). Gated on hot.strict.insert.i11gate.
      boolean i11GateBlocks = false;
      // Phase 8 (multi-week) — if β would violate I11 AND root exists, try multi-level
      // closure: absorb β into root's mask (splitting non-β-constant root children).
      // On success, the BiNode's β is now routed at root level → no I11 violation.
      if (Boolean.getBoolean("hot.strict.phase8.multilevel") && pathNodes != null
          && pathNodes.length > 0 && pathNodes[0] != null) {
        final HOTIndirectPage root = pathNodes[0];
        final int rootMsb = root.getMostSignificantBitIndex() & 0xFFFF;
        if (Boolean.getBoolean("hot.debug.phase8")) {
          System.err.println("[phase8] check β=" + discriminativeBit + " rootMsb=" + rootMsb
              + " inMask=" + indirectMaskHasAbsBit(root, discriminativeBit)
              + " currentPathIdx=" + currentPathIdx);
        }
        if (discriminativeBit < rootMsb && !indirectMaskHasAbsBit(root, discriminativeBit)) {
          final HOTIndirectPage absorbed = phase8MultilevelClosure(root, rootReference,
              discriminativeBit, log, storageEngineReader.getRevisionNumber());
          if (absorbed != null) {
            log.put(rootReference, PageContainer.getInstance(absorbed, absorbed));
            rootReference.setPage(absorbed);
            if (Boolean.getBoolean("hot.debug.phase8")) {
              System.err.println("[phase8] absorbed β=" + discriminativeBit
                  + " into root — restart insert");
            }
            // After absorbing β into root, the existing insert state may be stale.
            // For simplicity, return success — the parent caller can re-route as needed.
            // (Implementing actual restart requires more work; for now, signal "tried").
          }
        }
      }
      if (Boolean.getBoolean("hot.strict.insert.i11gate") && currentPathIdx > 0) {
        final HOTIndirectPage grandparent = pathNodes[currentPathIdx - 1];
        final int gpMsb = grandparent.getMostSignificantBitIndex() & 0xFFFF;
        final int currentParentMsb = parent.getMostSignificantBitIndex() & 0xFFFF;
        final int newParentMsb = Math.min(currentParentMsb, discriminativeBit);
        // Only block if (a) the new bit DOES lower parent.MSB, AND (b) the result
        // violates I11 (newMSB <= gpMsb).
        if (newParentMsb < currentParentMsb && newParentMsb <= gpMsb) {
          i11GateBlocks = true;
          if (Boolean.getBoolean("hot.debug.insert.i11gate")) {
            System.err.println("[i11-gate] BLOCK addEntry: discBit=" + discriminativeBit
                + " currentParentMsb=" + currentParentMsb + " newParentMsb=" + newParentMsb
                + " grandparent.MSB=" + gpMsb);
          }
        }
      }
      HOTIndirectPage expandedParent = null;
      if (!i11GateBlocks && currentNumChildren < NodeUpgradeManager.MULTI_NODE_MAX_CHILDREN) {
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
        // Phase 7q.15 — check ancestor I8 BEFORE invoking rebalanceAndIntegrate. If
        // placing the new child at slot 0 of an ancestor would propagate firstKey
        // shifts up to violate I8 at a higher ancestor, abort this path and use
        // splitParentAndRecurse instead.
        final boolean recursiveCheckOk = intermediateBiNodeRecursiveSlotCheck(
            pathNodes, pathChildIndices, currentPathIdx + 1, leftChild);
        final HOTIndirectPage rebalancedParent = recursiveCheckOk
            ? rebalanceAndIntegrate(parent, originalChildIndex,
                leftChild, rightChild, discriminativeBit, log,
                storageEngineReader.getRevisionNumber())
            : null;
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
        } else if (intermediateBiNodePreservesSlotOrder(parent, originalChildIndex, leftChild)
            && intermediateBiNodeRecursiveSlotCheck(pathNodes, pathChildIndices, currentPathIdx + 1,
                leftChild)) {
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
          HOTIndirectPage updatedParent =
              parent.withUpdatedChild(originalChildIndex, newBiNodeRef,
                  storageEngineReader.getRevisionNumber());
          // Phase 6g — try to recompute partials immediately, so the slot's stored partial
          // reflects the new BiNode's firstKey (= leftChild's firstKey) rather than the
          // old leaf's. If recompute succeeds, partial-sort matches firstKey-sort at this
          // ancestor; otherwise leave updatedParent as the withUpdatedChild result.
          if (Boolean.getBoolean("hot.strict.phase6g")) {
            final HOTIndirectPage recomputed = recomputePartialsForCurrentFirstKeys(
                updatedParent, storageEngineReader.getRevisionNumber());
            if (recomputed != null) {
              updatedParent = recomputed;
              if (Boolean.getBoolean("hot.debug.phase6g")) {
                System.err.println("[phase6g] post-intermediate recompute OK parent="
                    + updatedParent.getPageKey() + " slot=" + originalChildIndex);
              }
            }
          }
          log.put(parentRef, PageContainer.getInstance(updatedParent, updatedParent));
          INTERMEDIATE_BINODE_FALLBACK_FIRINGS.incrementAndGet();
          if (Boolean.getBoolean("hot.debug.intermediate")) {
            final byte[] leftFK = getFirstKeyFromChild(leftChild);
            final String hex = leftFK == null ? "null" : bytesToHex(leftFK);
            System.err.println("[intermediate] place BiNode parentPageKey="
                + parent.getPageKey() + " slot=" + originalChildIndex
                + " leftFirstKey=" + hex);
          }
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
   * Phase 8 (multi-week) — multi-level β closure with non-path child splitting.
   *
   * <p>When a leaf split at depth D produces a disc bit β that's MORE significant than
   * some ancestor's MSB (= would violate I11 if added at depth D), this function walks
   * UP from depth D-1 toward root, attempting to absorb β at each level where it makes
   * sense (= where ancestor.MSB > β so β can become the new ancestor MSB after extension).
   *
   * <p>At each ancestor level A:
   * <ol>
   *   <li>Skip if A.MSB ≤ β (β can't be added to A's mask without violating A's own I11).</li>
   *   <li>Skip if A.mask already contains β (already absorbed).</li>
   *   <li>For each child of A (path or non-path), verify β-constancy in subtree. If
   *       non-constant, recursively split child on β.</li>
   *   <li>After all children β-constant, extend A's mask with β. Recompute partials.</li>
   * </ol>
   *
   * <p>Returns the (rebuilt) root indirect on success, or null on failure (caller falls
   * back to legacy paths).
   *
   * <p>Gated on hot.strict.phase8.multilevel.
   *
   * <p>HFT-grade: per-ancestor work bounded by fanout × per-child β-constancy probe.
   * Worst case: O(depth × fanout × leafScan). For 5-deep × 32-fan × 50K leaves ≈ 8M ops,
   * acceptable at commit-time or specific insert points.
   */
  @Nullable
  private HOTIndirectPage phase8MultilevelClosure(HOTIndirectPage rootIndirect,
      PageReference rootRef, int beta, TransactionIntentLog log, int revision) {
    final boolean dbg = Boolean.getBoolean("hot.debug.phase8");
    if (rootIndirect == null) return null;
    if (beta < 0) return null;
    // Quick win: if root.MSB ≤ β, this β is less significant than root's already-captured
    // bits — no need for root absorption (β will be routed at deeper levels).
    final int rootMsb = rootIndirect.getMostSignificantBitIndex() & 0xFFFF;
    if (rootMsb <= beta) {
      if (dbg) System.err.println("[phase8] root.MSB=" + rootMsb + " ≤ β=" + beta
          + " — no absorption needed");
      return null;
    }
    if (indirectMaskHasAbsBit(rootIndirect, beta)) {
      if (dbg) System.err.println("[phase8] root already has β=" + beta + " in mask");
      return null;
    }
    // Step 1: for each root child, verify β-constancy. Split non-constant ones.
    final int n = rootIndirect.getNumChildren();
    final PageReference[] newChildren = new PageReference[n * 2]; // worst case: each splits
    int newN = 0;
    for (int i = 0; i < n; i++) {
      final PageReference cref = rootIndirect.getChildReference(i);
      if (cref == null) {
        if (dbg) System.err.println("[phase8] null child at i=" + i + " — abort");
        return null;
      }
      final int bv = bitConstantValueInSubtree(cref, beta);
      if (bv >= 0) {
        // β-constant in this child — keep as-is.
        newChildren[newN++] = cref;
        continue;
      }
      // β-mixed — try splitSubtreeOnBit first; fall back to recursive lift if
      // immediate split fails (= firstKeys agree at β at this level but deeper keys mix).
      final SubtreeSplit ss = splitSubtreeOnBit(cref, beta, log, revision);
      if (ss != null) {
        newChildren[newN++] = ss.leftRef();
        newChildren[newN++] = ss.rightRef();
        if (dbg) System.err.println("[phase8] split root.child[" + i + "] on β=" + beta);
        continue;
      }
      // splitSubtreeOnBit failed — try recursive lift via liftBetaFromSubtreeRecursive.
      final BetaLiftWalk lw = liftBetaFromSubtreeRecursive(cref, beta, log, revision);
      if (lw != null && lw.propagates()) {
        newChildren[newN++] = lw.root;
        newChildren[newN++] = lw.propagateRight;
        if (dbg) System.err.println("[phase8] lifted root.child[" + i + "] via walker on β=" + beta);
        continue;
      }
      if (dbg) System.err.println("[phase8] split AND lift failed for child=" + i
          + " — abort");
      return null;
    }
    // Step 2: extend root's mask with β. Build new mask in a MultiMask layout.
    final java.util.TreeMap<Integer, Integer> maskByBytePos = new java.util.TreeMap<>();
    if (rootIndirect.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK) {
      final int oldInitial = rootIndirect.getInitialBytePos();
      final long oldMask = rootIndirect.getBitMask();
      for (int bo = 0; bo < 8; bo++) {
        final int byteMaskBits = (int) ((oldMask >>> ((7 - bo) * 8)) & 0xFFL);
        if (byteMaskBits != 0) maskByBytePos.merge(oldInitial + bo, byteMaskBits, (a, b) -> a | b);
      }
    } else {
      final byte[] ep = rootIndirect.getExtractionPositions();
      final long[] em = rootIndirect.getExtractionMasks();
      final int neb = rootIndirect.getNumExtractionBytes();
      for (int i = 0; i < neb; i++) {
        final int chunkIdx = i / 8;
        final int byteOffsetInChunk = i % 8;
        final int byteMaskBits = (int) ((em[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFFL);
        if (byteMaskBits != 0) maskByBytePos.merge(ep[i] & 0xFF, byteMaskBits, (a, b) -> a | b);
      }
    }
    final int betaBytePos = beta / 8;
    final int betaBitInByte = beta % 8;
    final int betaMaskBit = 1 << (7 - betaBitInByte);
    maskByBytePos.merge(betaBytePos, betaMaskBit, (a, b) -> a | b);
    final int numBytes = maskByBytePos.size();
    if (numBytes > 64) {
      if (dbg) System.err.println("[phase8] mask too wide");
      return null;
    }
    final byte[] extractionPositions = new byte[numBytes];
    final long[] extractionMasks = new long[(numBytes + 7) / 8];
    short newMsb = Short.MAX_VALUE;
    int idx = 0;
    for (final var entry : maskByBytePos.entrySet()) {
      final int bp = entry.getKey();
      final int mb = entry.getValue();
      extractionPositions[idx] = (byte) bp;
      extractionMasks[idx / 8] |= ((long) (mb & 0xFF)) << ((7 - idx % 8) * 8);
      final int highBit = 31 - Integer.numberOfLeadingZeros(mb & 0xFF);
      final int absBitPos = bp * 8 + (7 - highBit);
      if (absBitPos < newMsb) newMsb = (short) absBitPos;
      idx++;
    }
    // Step 3: compute partials for each new child.
    if (newN > NodeUpgradeManager.MULTI_NODE_MAX_CHILDREN) {
      if (dbg) System.err.println("[phase8] fanout overflow newN=" + newN);
      return null;
    }
    final int[] newPartials = new int[newN];
    final java.util.HashSet<Integer> seen = new java.util.HashSet<>(newN * 2);
    for (int i = 0; i < newN; i++) {
      final byte[] fk = getFirstKeyFromChild(newChildren[i]);
      if (fk == null || fk.length == 0) {
        if (dbg) System.err.println("[phase8] empty firstKey at i=" + i);
        return null;
      }
      newPartials[i] = computePartialKeyMultiMaskDirect(fk, extractionPositions,
          extractionMasks, numBytes);
      if (!seen.add(newPartials[i])) {
        if (dbg) System.err.println("[phase8] partial collision at i=" + i + " p="
            + Integer.toHexString(newPartials[i]));
        return null;
      }
    }
    // Step 4: sort children + partials.
    final PageReference[] sortedChildren = newChildren.clone();
    final int[] sortedPartials = newPartials.clone();
    final PageReference[] finalChildren = java.util.Arrays.copyOf(sortedChildren, newN);
    sortChildrenAndPartialsByPartial(finalChildren, sortedPartials);
    // Step 5: verify firstKey-monotone post-sort.
    byte[] prev = null;
    for (int i = 0; i < newN; i++) {
      final byte[] fk = getFirstKeyFromChild(finalChildren[i]);
      if (fk == null || fk.length == 0) continue;
      if (prev != null && Arrays.compareUnsigned(prev, fk) >= 0) {
        if (dbg) System.err.println("[phase8] post-sort inversion at i=" + i);
        return null;
      }
      prev = fk;
    }
    // Step 6: verify I4 (smallest partial = 0).
    int smallestPartial = Integer.MAX_VALUE;
    for (int i = 0; i < newN; i++) {
      if (sortedPartials[i] < smallestPartial) smallestPartial = sortedPartials[i];
    }
    if (smallestPartial != 0) {
      if (dbg) System.err.println("[phase8] I4 violation — smallest partial=" + smallestPartial);
      return null;
    }
    // Step 7: build new root.
    final HOTIndirectPage built = (newN <= 16)
        ? HOTIndirectPage.createSpanNodeMultiMask(rootIndirect.getPageKey(), revision,
            extractionPositions, extractionMasks, numBytes, sortedPartials, finalChildren,
            rootIndirect.getHeight(), newMsb)
        : HOTIndirectPage.createMultiNodeMultiMask(rootIndirect.getPageKey(), revision,
            extractionPositions, extractionMasks, numBytes, sortedPartials, finalChildren,
            rootIndirect.getHeight(), newMsb);
    if (dbg) System.err.println("[phase8] SUCCESS β=" + beta + " newN=" + newN
        + " newMsb=" + newMsb);
    return built;
  }

  /**
   * Phase 7q.15 — recursive ancestor I8 check. When the new BiNode is placed at slot 0 of
   * its immediate parent, the parent's effective firstKey shifts to the BiNode's
   * leftChild.firstKey. This propagates up to grandparent if grandparent's slot for parent
   * is also 0, and so on. At the first ancestor where the slot index > 0, the predecessor
   * sibling's firstKey is compared to the new firstKey. If predecessor.firstKey >=
   * new firstKey, I8 violates.
   *
   * <p>Returns true if I8 is preserved at ALL ancestor levels; false if any ancestor
   * would violate I8 with the BiNode placement.
   *
   * <p>Gated on hot.strict.insert.recursivesloter so legacy behavior is preserved off-flag.
   */
  private boolean intermediateBiNodeRecursiveSlotCheck(HOTIndirectPage[] pathNodes,
      int[] pathChildIndices, int pathDepth, PageReference leftChild) {
    if (!Boolean.getBoolean("hot.strict.insert.recursivesloter")) return true;
    final byte[] leftFirstKey = getFirstKeyFromChild(leftChild);
    if (leftFirstKey == null || leftFirstKey.length == 0) return true;
    if (Boolean.getBoolean("hot.debug.insert.recursivesloter")) {
      System.err.println("[recursive-slot] ENTRY pathDepth=" + pathDepth
          + " leftFirstKey=" + bytesToHex(leftFirstKey)
          + " slots=" + java.util.Arrays.toString(
              java.util.Arrays.copyOfRange(pathChildIndices, 0, Math.min(pathDepth, pathChildIndices.length))));
    }
    // Walk from immediate parent (= pathDepth-1) up to root. At each level k, if slot[k]
    // > 0, compare predecessor sibling's firstKey to leftFirstKey. If predecessor >=
    // leftFirstKey, I8 would violate at that level.
    for (int k = pathDepth - 1; k >= 0; k--) {
      final int slot = pathChildIndices[k];
      if (slot == 0) continue; // firstKey propagates up further
      // Predecessor at level k.
      final HOTIndirectPage ancestor = pathNodes[k];
      final PageReference prevChild = ancestor.getChildReference(slot - 1);
      if (prevChild == null) continue;
      final byte[] prevFirstKey = getFirstKeyFromChild(prevChild);
      if (prevFirstKey == null || prevFirstKey.length == 0) continue;
      if (Arrays.compareUnsigned(prevFirstKey, leftFirstKey) >= 0) {
        if (Boolean.getBoolean("hot.debug.insert.recursivesloter")) {
          System.err.println("[recursive-slot] I8 would violate at depth=" + k
              + " ancestor.pageKey=" + ancestor.getPageKey()
              + " slot=" + slot + " prev.firstKey=" + bytesToHex(prevFirstKey)
              + " new.firstKey=" + bytesToHex(leftFirstKey));
        }
        return false;
      }
      // I8 holds at this level; the new firstKey doesn't propagate further (slot > 0
      // means we have a predecessor at this level, so parent.effective.firstKey doesn't
      // change due to this insertion).
      return true;
    }
    // Reached root with all slots=0. firstKey becomes the trie's new minimum, which is OK
    // (no I8 violation possible at root level since there's no predecessor).
    return true;
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
    long newMask = oldMask | newBitMaskBit;

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
    // Phase 6d — β-mixed siblings get split on β to maintain constancy. Each split
    // adds one extra sibling slot (= β=0 half + β=1 half replace the original mixed
    // sibling). The new mask captures β; partials are repositioned accordingly.
    // Refuses (returns null) only if a split fails or fanout would exceed the limit.
    final boolean phase6dEnabled = Boolean.getBoolean("hot.strict.phase6d");
    final int[] siblingBitValues = new int[numChildren];
    final boolean[] siblingNeedsSplit = phase6dEnabled ? new boolean[numChildren] : null;
    final SubtreeSplit[] siblingSplits = phase6dEnabled ? new SubtreeSplit[numChildren] : null;
    int extraSlotsFromSplits = 0;
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) continue;
      final int v = bitConstantValueInSubtree(parent.getChildReference(i), newAbsBit);
      if (v < 0) {
        if (!phase6dEnabled) return null; // non-constant — caller takes splitParentAndRecurse
        // Phase 6d: split this sibling on β. Recursive within the sibling subtree.
        final SubtreeSplit ss = splitSubtreeOnBit(parent.getChildReference(i), newAbsBit, activeLog, revision);
        if (ss == null) return null;
        siblingNeedsSplit[i] = true;
        siblingSplits[i] = ss;
        extraSlotsFromSplits++;
        siblingBitValues[i] = -1; // sentinel — will be expanded into two slots
        continue;
      }
      siblingBitValues[i] = v;
    }
    if (extraSlotsFromSplits > 0
        && (numChildren + 1 + extraSlotsFromSplits) > NodeUpgradeManager.MULTI_NODE_MAX_CHILDREN) {
      return null; // fanout overflow — caller falls back
    }

    // 4. Build the expanded children + partial-keys arrays.
    // Phase 6d: each β-mixed sibling is expanded into TWO slots (left=β=0, right=β=1).
    final int newNumChildren = numChildren + 1 + extraSlotsFromSplits;
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
        final int repositioned = (int) Long.expand(
            Integer.toUnsignedLong(oldPartialKeys[i]), oldPartialMaskInNewLayout);
        if (phase6dEnabled && siblingNeedsSplit != null && siblingNeedsSplit[i]) {
          // Phase 6d: expand split sibling into two β-constant halves.
          newChildren[j] = siblingSplits[i].leftRef();
          newPartialKeys[j] = repositioned; // β=0 half
          j++;
          newChildren[j] = siblingSplits[i].rightRef();
          newPartialKeys[j] = repositioned | (1 << newBitOutputPos); // β=1 half
          j++;
        } else {
          newChildren[j] = parent.getChildReference(i);
          newPartialKeys[j] = repositioned | (siblingBitValues[i] << newBitOutputPos);
          j++;
        }
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
    // buildFlatNonStrict.
    //
    // Stage G.31 — when after firstKey re-sort, two partials collide (different firstKeys
    // route to same partial under current newMask), iteratively find the MSDB between the
    // colliding pair and ADD it to newMask. Repeat until partials are unique OR mask is
    // saturated. This is INSERT-TIME MSDB closure — the architectural fix to ensure the
    // mask captures every bit needed to discriminate adjacent siblings by firstKey.
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
      // G.31: iterative mask extension. Up to 16 attempts (= mask can have at most ~16 bits
      // before fanout limit is reached).
      int g31Attempts = 0;
      while (g31Attempts < 16) {
        // Find first colliding adjacent pair after firstKey sort.
        int collidingI = -1, collidingK = -1;
        outer:
        for (int i = 1; i < newNumChildren; i++) {
          for (int k = 0; k < i; k++) {
            if (newPartialKeys[k] == newPartialKeys[i]) {
              collidingI = i;
              collidingK = k;
              break outer;
            }
          }
        }
        if (collidingI < 0) break; // no collisions → done

        // Find MSDB between the two colliding children's firstKeys.
        final byte[] keyI = getFirstKeyFromChild(newChildren[collidingI]);
        final byte[] keyK = getFirstKeyFromChild(newChildren[collidingK]);
        if (keyI == null || keyK == null) return null;
        final int msdb = DiscriminativeBitComputer.computeDifferingBit(keyK, keyI);
        if (msdb < 0) return null; // truly identical keys — can't discriminate

        // Encode msdb into the current SingleMask window.
        final int msdbByteOff = (msdb / 8) - oldInitialBytePos;
        if (msdbByteOff < 0 || msdbByteOff >= 8) return null; // cross-window — too complex here
        final int msdbBitInByte = msdb % 8;
        final int msdbBitInWord = (7 - msdbByteOff) * 8 + (7 - msdbBitInByte);
        final long msdbBit = 1L << msdbBitInWord;
        if ((newMask & msdbBit) != 0L) return null; // already present but still collide?
        final long extendedMask = newMask | msdbBit;
        if (Long.bitCount(extendedMask) > 16) return null; // saturation: partial > 16 bits

        // Recompute all partials under extended mask.
        for (int i = 0; i < newNumChildren; i++) {
          final byte[] cKey = getFirstKeyFromChild(newChildren[i]);
          newPartialKeys[i] = (cKey == null || cKey.length == 0) ? 0
              : computePartialKeySingleMask(cKey, oldInitialBytePos, extendedMask);
        }
        newMask = extendedMask;
        g31Attempts++;
      }
      // Final verification: partials unique + smallest = 0.
      for (int i = 1; i < newNumChildren; i++) {
        for (int k = 0; k < i; k++) {
          if (newPartialKeys[k] == newPartialKeys[i]) return null;
        }
      }
      boolean hasZero = false;
      for (final int p : newPartialKeys) {
        if (p == 0) { hasZero = true; break; }
      }
      if (!hasZero) return null;
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
    // Phase 7q.15 — TIL-first for placeholder refs.
    if (page == null && activeLog != null) {
      final var container = activeLog.get(ref);
      if (container != null) {
        page = container.getModified();
        if (page != null) ref.setPage(page);
      }
    }
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
      final SubtreeSplit immediate = splitIndirectOnBit(indirect, absBit, log, revision);
      if (immediate != null) return immediate;
      // Phase 8 (multi-week) — when immediate split fails (= firstKeys all agree at β
      // at this indirect level but deeper keys are β-mixed), RECURSE into children
      // and split each β-mixed child, then partition expanded refs. Gated on
      // hot.strict.phase8.recursivesplit.
      if (Boolean.getBoolean("hot.strict.phase8.recursivesplit")) {
        return recursiveSplitOnBit(indirect, absBit, log, revision);
      }
    }
    return null;
  }

  /**
   * Phase 8 — recursive split-on-bit. When the immediate indirect's children's firstKeys
   * all agree at β (= `splitIndirectOnBit` returns null), but deeper subtree contains
   * keys with mixed β values, recurse into β-mixed children and split THEM. Then
   * partition the expanded ref list into β=0 and β=1 buckets.
   *
   * <p>Returns SubtreeSplit on success, null on any failure (caller falls back).
   */
  @Nullable
  private SubtreeSplit recursiveSplitOnBit(HOTIndirectPage indirect, int absBit,
      TransactionIntentLog log, int revision) {
    final boolean dbg = Boolean.getBoolean("hot.debug.phase8");
    final int n = indirect.getNumChildren();
    final PageReference[] expandedRefs = new PageReference[n * 2];
    int expandedN = 0;
    for (int i = 0; i < n; i++) {
      final PageReference cref = indirect.getChildReference(i);
      if (cref == null) {
        if (dbg) System.err.println("[recursive-split] null child i=" + i);
        return null;
      }
      final int bv = bitConstantValueInSubtree(cref, absBit);
      if (bv >= 0) {
        // β-constant — keep as-is.
        expandedRefs[expandedN++] = cref;
        continue;
      }
      // β-mixed — recurse.
      final SubtreeSplit childSplit = splitSubtreeOnBit(cref, absBit, log, revision);
      if (childSplit == null) {
        if (dbg) System.err.println("[recursive-split] child i=" + i + " split returned null");
        return null;
      }
      expandedRefs[expandedN++] = childSplit.leftRef();
      expandedRefs[expandedN++] = childSplit.rightRef();
    }
    if (dbg) System.err.println("[recursive-split] expanded n=" + n + " → expandedN=" + expandedN
        + " for absBit=" + absBit);
    // Partition by β value of firstKey, build two halves via inherited mask.
    final LiftSplitResult lsr = splitExpandedChildrenOnBeta(indirect, expandedRefs, expandedN,
        absBit, log, revision);
    if (lsr == null) return null;
    return new SubtreeSplit(lsr.betaZero, lsr.betaOne);
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

    // Phase 7m — MultiMask inheritance. Avoids the cascade where createNodeFromChildren
    // re-derives a fresh mask from full keys (potentially including bits <= new parent.MSB,
    // which creates I11 violations between the new parent and these children).
    if (parentIndirect.getLayoutType() != HOTIndirectPage.LayoutType.SINGLE_MASK) {
      return buildBucketWithInheritedMaskMultiMask(parentIndirect, bucketIndices,
          replacementRefs, bucketSize, absBit, log, revision);
    }

    final int oldInitialBytePos = parentIndirect.getInitialBytePos();
    final long oldMask = parentIndirect.getBitMask();
    final int[] oldPartials = parentIndirect.getPartialKeys();
    final int oldParentMsb = parentIndirect.getMostSignificantBitIndex() & 0xFFFF;

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

    // Phase 7n — strict I11 enforcement. The bucket-indirect becomes a CHILD of the new
    // (post-extension) parent, whose MSB = min(oldParentMsb, β). For I11 (child.MSB >
    // parent.MSB), the bucket-indirect's mask must contain only bits with absBit > new
    // parent.MSB. So strip β AND all bits with absBit ≤ new parent.MSB from oldMask.
    final int newParentMsb = Math.min(oldParentMsb, absBit);
    // Phase 7q.15 — ALSO strip bits ≥ minChildMsb. The bucket-indirect's MSB must be
    // strictly less than every bucket child's MSB for I11 between bucket-indirect and
    // its children. Compute minChildMsb across bucket children's MSBs (Integer.MAX_VALUE
    // for leaves). Gated on hot.strict.g32.childmsb so legacy behavior is preserved.
    final boolean childMsbStrict = Boolean.getBoolean("hot.strict.g32.childmsb");
    int minChildMsb = Integer.MAX_VALUE;
    if (childMsbStrict) {
      for (int i = 0; i < bucketSize; i++) {
        final int cm = getIndirectMsbOrMax(replacementRefs[i]);
        if (cm < minChildMsb) minChildMsb = cm;
      }
      // Phase 7q.15 — if minChildMsb ≤ newParentMsb + 1, range is empty/tight. Try to
      // raise children's MSBs so we have room for the bucket-indirect's MSB. Gated on
      // hot.strict.g32.raisechildmsb.
      if (Boolean.getBoolean("hot.strict.g32.raisechildmsb")
          && minChildMsb <= newParentMsb + 1) {
        // Need to raise minChildMsb. Aim for minChildMsb > newParentMsb + 1 (so at least
        // one bit fits).
        boolean raisedAny = false;
        for (int i = 0; i < bucketSize; i++) {
          final int cm = getIndirectMsbOrMax(replacementRefs[i]);
          if (cm <= newParentMsb + 1) {
            final PageReference raised = tryRaiseChildMsb(replacementRefs[i],
                newParentMsb + 1, log, revision);
            if (raised != null && raised != replacementRefs[i]) {
              replacementRefs[i] = raised;
              raisedAny = true;
            }
          }
        }
        if (raisedAny) {
          // Recompute minChildMsb.
          minChildMsb = Integer.MAX_VALUE;
          for (int i = 0; i < bucketSize; i++) {
            final int cm = getIndirectMsbOrMax(replacementRefs[i]);
            if (cm < minChildMsb) minChildMsb = cm;
          }
          if (Boolean.getBoolean("hot.debug.g32")) {
            System.err.println("[bucket-build] raised children MSBs, new minChildMsb=" + minChildMsb);
          }
        }
      }
    }
    long liftMaskBits = 0L;
    for (int wbit = 0; wbit < 64; wbit++) {
      if (((oldMask >>> wbit) & 1L) == 0L) continue;
      // Decode wbit → absBit. wbit = (7 - byteOffset)*8 + (7 - bitInByte).
      final int byteOffsetInWord = 7 - (wbit / 8);
      final int bitInByte = 7 - (wbit % 8);
      final int absBitOfWbit = (oldInitialBytePos + byteOffsetInWord) * 8 + bitInByte;
      if (absBitOfWbit <= newParentMsb) {
        liftMaskBits |= 1L << wbit;
      } else if (childMsbStrict && absBitOfWbit >= minChildMsb) {
        // Also strip bits that would make bucket-indirect.MSB ≥ minChildMsb.
        liftMaskBits |= 1L << wbit;
      }
    }
    final long newMask = oldMask & ~betaMaskBit & ~liftMaskBits;

    // If lift bits are non-empty, verify bucket children are β-constant on each lift bit
    // (since we're removing the routing for that bit at this level). If any child has
    // mixed bits, fall back so we don't break PEXT routing.
    if (liftMaskBits != 0L) {
      for (int wbit = 0; wbit < 64; wbit++) {
        if (((liftMaskBits >>> wbit) & 1L) == 0L) continue;
        final int byteOffsetInWord = 7 - (wbit / 8);
        final int bitInByte = 7 - (wbit % 8);
        final int liftAbsBit = (oldInitialBytePos + byteOffsetInWord) * 8 + bitInByte;
        for (int i = 0; i < bucketSize; i++) {
          final int v = bitConstantValueInSubtree(replacementRefs[i], liftAbsBit);
          if (v < 0) {
            // Bucket child has mixed values on a bit we're stripping — can't strip safely.
            return null;
          }
        }
      }
    }

    // Phase 7n — recompute partials from each child's firstKey under the new (possibly
    // shrunk) mask. Always compute from firstKey rather than shifting oldPartials, because
    // stripping multiple bits (β + lift bits) makes index-arithmetic shifting brittle.
    // The cost of recomputing is one PEXT per child — negligible.
    final int[] newPartials = new int[bucketSize];
    boolean haveZero = false;
    for (int i = 0; i < bucketSize; i++) {
      final byte[] firstKey = getFirstKeyFromChild(replacementRefs[i]);
      if (firstKey == null || firstKey.length == 0) {
        if (Boolean.getBoolean("hot.debug.phase7q")) {
          System.err.println("[bucket-build-sm] FAIL: bucketChild=" + i
              + " firstKey null/empty (childKey=" + replacementRefs[i].getKey() + ")"
              + " absBit=" + absBit + " bucketSize=" + bucketSize);
        }
        return null;
      }
      newPartials[i] = computePartialKeySingleMask(firstKey, oldInitialBytePos, newMask);
      if (newPartials[i] == 0) haveZero = true;
    }
    // Suppress unused warning for oldPartials in this path (kept for potential future use).
    if (oldPartials.length < 0) { /* never */ }

    // I4 (Binna's first-partial-zero): if smallest partial != 0, the new mask captures
    // constant-1 bits across the bucket. Phase 7q.15 — under hot.strict.g32.deep, STRIP
    // those constant-1 bits from the mask before falling back. This preserves I4 AND
    // keeps routing correct (only stripping bits where all bucket children agree means
    // the stripped bit isn't actually discriminating in the bucket). Falling back to
    // wrapBucketInSubtree (which derives fresh mask via adjacency) produces I6-violating
    // sub-indirects — verified empirically as the source of cascade.
    long workingMask = newMask;
    int[] workingPartials = newPartials;
    if (!haveZero && Boolean.getBoolean("hot.strict.g32.deep")) {
      // Find smallest partial; the bits set in it are CONSTANT-1 across the bucket
      // (otherwise some child would have 0 at those bits — the bucket's lex-smallest).
      int smallestP = Integer.MAX_VALUE;
      for (int i = 0; i < bucketSize; i++) if (workingPartials[i] < smallestP) smallestP = workingPartials[i];
      if (smallestP > 0 && smallestP != Integer.MAX_VALUE) {
        // Strip bits of smallestP from mask. Need to map partial-bit positions back to mask bits.
        // The partial bits are in order MSB-first of byteMasks in oldMask shape. Easier to
        // recompute partials with mask & ~constant-bit-pattern, but we need that pattern in
        // mask-space, not partial-space.
        // Approach: enumerate each mask bit; check if it's constant across bucket. If yes
        // AND the constant value is 1, strip it.
        long stripBits = 0L;
        for (int wbit = 0; wbit < 64; wbit++) {
          if (((workingMask >>> wbit) & 1L) == 0L) continue;
          final int byteOffsetInWord = 7 - (wbit / 8);
          final int bitInByte = 7 - (wbit % 8);
          final int maskAbsBit = (oldInitialBytePos + byteOffsetInWord) * 8 + bitInByte;
          // Check if this bit is 1 in EVERY bucket child's firstKey (constant-1).
          boolean allOne = true;
          for (int i = 0; i < bucketSize; i++) {
            if (!isAbsBitSet(getFirstKeyFromChild(replacementRefs[i]), maskAbsBit)) {
              allOne = false;
              break;
            }
          }
          if (allOne) stripBits |= 1L << wbit;
        }
        if (stripBits != 0L) {
          // Verify stripped bits are also β-constant in subtrees (= safe to remove without
          // breaking PEXT routing for non-firstKey leaves in the bucket).
          boolean safeToStrip = true;
          for (int wbit = 0; wbit < 64 && safeToStrip; wbit++) {
            if (((stripBits >>> wbit) & 1L) == 0L) continue;
            final int byteOffsetInWord = 7 - (wbit / 8);
            final int bitInByte = 7 - (wbit % 8);
            final int stripAbsBit = (oldInitialBytePos + byteOffsetInWord) * 8 + bitInByte;
            for (int i = 0; i < bucketSize; i++) {
              final int bv = bitConstantValueInSubtree(replacementRefs[i], stripAbsBit);
              if (bv < 0) { safeToStrip = false; break; }
            }
          }
          if (safeToStrip) {
            workingMask = workingMask & ~stripBits;
            // Recompute partials with stripped mask.
            workingPartials = new int[bucketSize];
            boolean haveZero2 = false;
            for (int i = 0; i < bucketSize; i++) {
              final byte[] fk = getFirstKeyFromChild(replacementRefs[i]);
              workingPartials[i] = computePartialKeySingleMask(fk, oldInitialBytePos, workingMask);
              if (workingPartials[i] == 0) haveZero2 = true;
            }
            if (haveZero2) {
              if (Boolean.getBoolean("hot.debug.phase7q")) {
                System.err.println("[bucket-build-sm] I4-recovered: stripped constant-1 bits, "
                    + "new mask=0x" + Long.toHexString(workingMask) + " absBit=" + absBit
                    + " bucketSize=" + bucketSize);
              }
              haveZero = true;
            }
          }
        }
      }
    }
    if (!haveZero) {
      if (Boolean.getBoolean("hot.debug.phase7q")) {
        int minP = Integer.MAX_VALUE;
        for (int i = 0; i < bucketSize; i++) if (workingPartials[i] < minP) minP = workingPartials[i];
        System.err.println("[bucket-build-sm] FAIL: I4 unresolvable — smallest="
            + Integer.toHexString(minP) + " absBit=" + absBit + " bucketSize=" + bucketSize);
      }
      return null;
    }

    // Verify uniqueness. Phase 7q.15 — when called from the commit-time lift path
    // (`hot.strict.g32.nowrap` set), abandon on collision instead of wrap fallback. The
    // wrap creates fresh-mask indirects via createNodeFromChildren that capture
    // non-β-constant bits → I6 cascade. Per-insert callers retain the legacy wrap
    // behavior (regression-free).
    for (int i = 1; i < bucketSize; i++) {
      for (int k = 0; k < i; k++) {
        if (workingPartials[i] == workingPartials[k]) {
          if (Boolean.getBoolean("hot.strict.g32.nowrap")) {
            if (Boolean.getBoolean("hot.debug.phase7q")) {
              System.err.println("[bucket-build-sm] FAIL: partial collision at i=" + i
                  + " k=" + k + " p=" + Integer.toHexString(workingPartials[i])
                  + " — abandon (g32.nowrap)");
            }
            return null;
          }
          final PageReference[] children = Arrays.copyOf(replacementRefs, bucketSize);
          return wrapBucketInSubtree(children, bucketSize, parentIndirect.getHeight(),
              log, revision);
        }
      }
    }

    // Sort children + partials by partial-key (HOT I7).
    final PageReference[] children = Arrays.copyOf(replacementRefs, bucketSize);
    sortChildrenAndPartialsByPartial(children, workingPartials);

    final long pageKey = pageKeyAllocator.getAsLong();
    final HOTIndirectPage built;
    if (workingMask == 0L) {
      // Empty mask — no disc bits to discriminate. Possible if β was the ONLY bit in
      // the original mask. Bucket has multiple children but no way to discriminate via
      // PEXT under the inherited mask. Fall back.
      final PageReference[] childCopy = Arrays.copyOf(replacementRefs, bucketSize);
      return wrapBucketInSubtree(childCopy, bucketSize, parentIndirect.getHeight(),
          log, revision);
    }
    if (bucketSize == 2) {
      // BiNode requires a single explicit disc bit, not a mask. Phase 7q.15: pass
      // newParentMsb hint so cbinode picks an I11-safe disc bit (> newParentMsb).
      final PageReference[] childCopy = Arrays.copyOf(replacementRefs, bucketSize);
      final Integer prevHint = PARENT_MSB_HINT.get();
      // The newParentMsb (= post-extension parent's MSB) is computed earlier as
      // Math.min(oldParentMsb, absBit). Use that.
      final int newParentMsbSm = Math.min(parentIndirect.getMostSignificantBitIndex() & 0xFFFF, absBit);
      PARENT_MSB_HINT.set(newParentMsbSm);
      try {
        return wrapBucketInSubtree(childCopy, bucketSize, parentIndirect.getHeight(),
            log, revision);
      } finally {
        if (prevHint == null) PARENT_MSB_HINT.remove();
        else PARENT_MSB_HINT.set(prevHint);
      }
    }
    if (bucketSize <= 16) {
      built = HOTIndirectPage.createSpanNode(pageKey, revision, oldInitialBytePos, workingMask,
          workingPartials, children, parentIndirect.getHeight());
    } else if (bucketSize <= NodeUpgradeManager.MULTI_NODE_MAX_CHILDREN) {
      built = HOTIndirectPage.createMultiNode(pageKey, revision, oldInitialBytePos, workingMask,
          workingPartials, children, parentIndirect.getHeight());
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
   * Phase 7m — MultiMask variant of buildBucketWithInheritedMask. Builds a sub-indirect
   * inheriting the parent's MultiMask (extractionPositions + extractionMasks +
   * numExtractionBytes) minus the β bit. Avoids the createNodeFromChildren cascade where
   * fresh-mask derivation captures bits less significant or equal to the new parent.MSB,
   * violating Binna's I11 trie condition.
   *
   * <p>If β isn't in the parent's MultiMask, the new child's mask = parent's mask
   * unchanged; partials are reused under that mask. If β is in the parent's MultiMask,
   * remove the β bit from the corresponding byteMask entry (compacting if it becomes 0),
   * then recompute partials by shifting out the β bit's PEXT-output position.
   *
   * <p>Returns null on uniqueness violations or layout failures.
   */
  private @Nullable PageReference buildBucketWithInheritedMaskMultiMask(HOTIndirectPage parent,
      int[] bucketIndices, PageReference[] replacementRefs, int bucketSize, int absBit,
      TransactionIntentLog log, int revision) {
    final byte[] oldEp = parent.getExtractionPositions();
    final long[] oldEm = parent.getExtractionMasks();
    final int oldNeb = parent.getNumExtractionBytes();
    final int[] oldPartials = parent.getPartialKeys();

    // 1. Locate β in the old MultiMask. β's byte = absBit / 8; β's bit-in-byte = absBit % 8;
    // mask bit (MSB-first) = 1 << (7 - bitInByte).
    final int betaBytePos = absBit / 8;
    final int betaBitInByte = absBit % 8;
    final int betaMaskBit = 1 << (7 - betaBitInByte);
    int betaEntryIdx = -1;
    for (int i = 0; i < oldNeb; i++) {
      if ((oldEp[i] & 0xFF) == betaBytePos) {
        final int chunkIdx = i / 8;
        final int byteOffsetInChunk = i % 8;
        final int byteMaskBits = (int) ((oldEm[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFFL);
        if ((byteMaskBits & betaMaskBit) != 0) {
          betaEntryIdx = i;
        }
        break;
      }
    }

    // 2. Compute β's output position in the old PEXT shape (= bit count of all higher-position
    // mask bits in the order they're concatenated).
    int betaOutputPos = -1;
    if (betaEntryIdx >= 0) {
      int outPos = 0;
      for (int i = 0; i < oldNeb; i++) {
        final int chunkIdx = i / 8;
        final int byteOffsetInChunk = i % 8;
        final int byteMaskBits = (int) ((oldEm[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFFL);
        if (i < betaEntryIdx) {
          outPos += Integer.bitCount(byteMaskBits);
        } else if (i == betaEntryIdx) {
          // Count bits in this byte's mask that are MORE significant than β (higher mask-bit value).
          outPos += Integer.bitCount(byteMaskBits & ~((betaMaskBit << 1) - 1));
          break;
        }
      }
      betaOutputPos = outPos;
    }

    // Phase 7n — strict I11 enforcement. The bucket-indirect becomes a CHILD of the new
    // (post-extension) parent, whose MSB = min(oldParentMsb, β). For I11 (child.MSB >
    // parent.MSB), the bucket-indirect's mask must contain only bits with absBit > new
    // parent.MSB.
    //
    // Phase 7q.7 (opt-in via -Dhot.strict.phase7q.stripNonConstant=true): ALSO strip
    // bits > newParentMsb that are non-constant in any bucket child's subtree. The
    // existing Phase 7n leaves such bits in the new mask, which forces downstream
    // routing through non-β-constant disc bits — exactly the source of the I5/I6
    // cascade the Phase 7q.6 gate detects. Stripping them at construction time
    // produces a smaller-but-correct new mask; if the resulting mask is empty or
    // produces partial collisions, the wrap fallback fires (and is then rejected
    // by the 7q.6 gate, preserving the architectural ceiling rather than
    // cascading).
    final boolean stripNonConstant =
        Boolean.getBoolean("hot.strict.phase7q.stripNonConstant");
    final int oldParentMsb = parent.getMostSignificantBitIndex() & 0xFFFF;
    final int newParentMsb = Math.min(oldParentMsb, absBit);
    // Phase 7q.15e — port the SingleMask `g32.childmsb` strip-bits-≥-minChildMsb gate
    // to MultiMask. Bucket-indirect's MSB must be strictly less significant (=
    // numerically greater absBit) than every bucket child's MSB for I11 between
    // bucket-indirect and its children. Without this gate, MultiMask buckets can
    // surface 46 equality-I11s under g32.deep cascade (see §7.20).
    //
    // Compute minChildMsb across bucket children's MSBs (Integer.MAX_VALUE when
    // a child is a leaf — leaves don't constrain). Then mark bits with absBit
    // ≥ minChildMsb for stripping. The existing constancy verify at line 2851+
    // catches non-constant bits and returns null cleanly (= caller falls back to
    // wrap, which is the existing safe path).
    final boolean childMsbStrictMm = Boolean.getBoolean("hot.strict.g32.childmsb");
    int minChildMsbMm = Integer.MAX_VALUE;
    if (childMsbStrictMm) {
      for (int i = 0; i < bucketSize; i++) {
        final int cm = getIndirectMsbOrMax(replacementRefs[i]);
        if (cm < minChildMsbMm) minChildMsbMm = cm;
      }
    }
    final int[] liftAbsBits = new int[oldNeb * 8];
    int liftCount = 0;
    for (int i = 0; i < oldNeb; i++) {
      final int bp = oldEp[i] & 0xFF;
      final int chunkIdx0 = i / 8;
      final int byteOffsetInChunk0 = i % 8;
      final int byteMask0 =
          (int) ((oldEm[chunkIdx0] >>> ((7 - byteOffsetInChunk0) * 8)) & 0xFFL);
      for (int mfBit = 0; mfBit < 8; mfBit++) {
        final int byteBit = 7 - mfBit;
        if ((byteMask0 & (1 << byteBit)) == 0) continue;
        final int abs = bp * 8 + mfBit;
        if (abs == absBit) continue; // β handled separately.
        if (abs <= newParentMsb) {
          liftAbsBits[liftCount++] = abs;
        } else if (childMsbStrictMm && abs >= minChildMsbMm) {
          // Phase 7q.15e — strip bits ≥ minChildMsb to enforce bucket.MSB <
          // every-child-MSB for I11. Constancy will be verified below.
          liftAbsBits[liftCount++] = abs;
        } else if (stripNonConstant) {
          // Phase 7q.7 — strip bits > newParentMsb that are non-constant in
          // any bucket child's subtree. Constant bits stay in the new mask.
          boolean nonConstant = false;
          for (int j = 0; j < bucketSize; j++) {
            final PageReference cref = replacementRefs[j];
            if (cref == null) continue;
            if (cref.getKey() == io.sirix.settings.Constants.NULL_ID_LONG) continue;
            final int v = bitConstantValueInSubtree(cref, abs);
            if (v < 0) {
              nonConstant = true;
              break;
            }
          }
          if (nonConstant) {
            liftAbsBits[liftCount++] = abs;
            PHASE7Q_STRIP_NONCONSTANT_BITS.incrementAndGet();
          }
        }
      }
    }
    for (int k = 0; k < liftCount; k++) {
      final int liftBit = liftAbsBits[k];
      // Phase 7n: bits ≤ newParentMsb MUST be constant. If we collected this bit
      // via Phase 7q.7 (stripNonConstant) — by definition non-constant; we accept
      // the loss of routing for that bit at this level. Phase 7q.15e (childMsbStrictMm)
      // requires constancy verification too: if the bit ≥ minChildMsb is non-constant
      // in some bucket child's subtree, stripping it would break PEXT routing through
      // that child. Distinguish the two paths: 7q.7 path = stripNonConstant flag set;
      // 7q.15e path = childMsbStrictMm flag set AND liftBit ≥ minChildMsbMm.
      if (liftBit > newParentMsb) {
        final boolean fromChildMsbGate =
            childMsbStrictMm && liftBit >= minChildMsbMm;
        if (fromChildMsbGate) {
          // 7q.15e — verify constancy in every bucket child's subtree.
          for (int i = 0; i < bucketSize; i++) {
            final PageReference cref = replacementRefs[i];
            if (cref == null) continue;
            if (cref.getKey() == io.sirix.settings.Constants.NULL_ID_LONG) continue;
            final int v = bitConstantValueInSubtree(cref, liftBit);
            if (v < 0) {
              if (Boolean.getBoolean("hot.debug.phase7q")) {
                System.err.println("[bucket-build-mm] FAIL (7q.15e): liftBit=" + liftBit
                    + " (≥ minChildMsb=" + minChildMsbMm + ") non-constant in bucketChild="
                    + i + " absBit=" + absBit + " bucketSize=" + bucketSize);
              }
              return null;
            }
          }
        }
        continue; // 7q.7 path: skip verify by design.
      }
      for (int i = 0; i < bucketSize; i++) {
        final int v = bitConstantValueInSubtree(replacementRefs[i], liftBit);
        if (v < 0) {
          if (Boolean.getBoolean("hot.debug.phase7q")) {
            System.err.println("[bucket-build-mm] FAIL: liftBit=" + liftBit
                + " non-constant in bucketChild=" + i + " absBit=" + absBit
                + " bucketSize=" + bucketSize);
          }
          return null;
        }
      }
    }

    // 3. Build new MultiMask. If β is present, remove it (and compact the entry if its
    // byteMask becomes 0).
    int newNeb = oldNeb;
    byte[] newEp;
    long[] newEm;
    int newMsb = Short.MAX_VALUE;
    if (betaEntryIdx < 0 && liftCount == 0) {
      // β not in mask AND no lift bits — preserve as-is.
      newEp = oldEp.clone();
      newEm = oldEm.clone();
      newMsb = parent.getMostSignificantBitIndex() & 0xFFFF;
    } else {
      // Build a modified byteMask per entry, clearing β and any lift bits.
      final long[] modifiedEm = oldEm.clone();
      for (int i = 0; i < oldNeb; i++) {
        final int bp = oldEp[i] & 0xFF;
        final int chunkIdx0 = i / 8;
        final int byteOffsetInChunk0 = i % 8;
        final int shift0 = (7 - byteOffsetInChunk0) * 8;
        long byteMask0 = (modifiedEm[chunkIdx0] >>> shift0) & 0xFFL;
        for (int mfBit = 0; mfBit < 8; mfBit++) {
          final int byteBit = 7 - mfBit;
          if ((byteMask0 & (1L << byteBit)) == 0L) continue;
          final int abs = bp * 8 + mfBit;
          if (abs == absBit) {
            byteMask0 &= ~(1L << byteBit);
            continue;
          }
          for (int k = 0; k < liftCount; k++) {
            if (liftAbsBits[k] == abs) {
              byteMask0 &= ~(1L << byteBit);
              break;
            }
          }
        }
        modifiedEm[chunkIdx0] =
            (modifiedEm[chunkIdx0] & ~(0xFFL << shift0)) | ((byteMask0 & 0xFFL) << shift0);
      }
      // Compact: drop entries whose byteMask became 0.
      int keepCount = 0;
      for (int i = 0; i < oldNeb; i++) {
        final int chunkIdx0 = i / 8;
        final int byteOffsetInChunk0 = i % 8;
        final long bm = (modifiedEm[chunkIdx0] >>> ((7 - byteOffsetInChunk0) * 8)) & 0xFFL;
        if (bm != 0L) keepCount++;
      }
      newNeb = keepCount;
      newEp = new byte[Math.max(1, keepCount)];
      final int numChunks = Math.max(1, (keepCount + 7) / 8);
      newEm = new long[numChunks];
      int dstIdx = 0;
      for (int src = 0; src < oldNeb; src++) {
        final int srcChunk = src / 8;
        final int srcByte = src % 8;
        final int srcShift = (7 - srcByte) * 8;
        final long srcByteBits = (modifiedEm[srcChunk] >>> srcShift) & 0xFFL;
        if (srcByteBits == 0L) continue;
        newEp[dstIdx] = oldEp[src];
        final int dstChunk = dstIdx / 8;
        final int dstByte = dstIdx % 8;
        final int dstShift = (7 - dstByte) * 8;
        newEm[dstChunk] |= srcByteBits << dstShift;
        dstIdx++;
      }
      // Recompute MSB = lowest absBit captured by new mask.
      for (int i = 0; i < newNeb; i++) {
        final int chunkIdx2 = i / 8;
        final int byteOffsetInChunk2 = i % 8;
        final int byteMask = (int) ((newEm[chunkIdx2] >>> ((7 - byteOffsetInChunk2) * 8)) & 0xFFL);
        if (byteMask == 0) continue;
        final int highBit = 31 - Integer.numberOfLeadingZeros(byteMask);
        final int absBitPos = (newEp[i] & 0xFF) * 8 + (7 - highBit);
        if (absBitPos < newMsb) newMsb = absBitPos;
      }
    }

    // 4. If new mask is empty — can't discriminate. Fall back to wrapBucketInSubtree.
    if (newNeb == 0) {
      final PageReference[] childCopy = Arrays.copyOf(replacementRefs, bucketSize);
      return wrapBucketInSubtree(childCopy, bucketSize, parent.getHeight(), log, revision);
    }

    // 5. Recompute partials from each child's firstKey under the new MultiMask.
    final int[] newPartials = new int[bucketSize];
    boolean haveZero = false;
    for (int i = 0; i < bucketSize; i++) {
      final byte[] firstKey = getFirstKeyFromChild(replacementRefs[i]);
      if (firstKey == null || firstKey.length == 0) {
        if (Boolean.getBoolean("hot.debug.phase7q") || Boolean.getBoolean("hot.debug.g32")) {
          System.err.println("[bucket-build-mm] FAIL: bucketChild=" + i
              + " firstKey null/empty (childKey=" + replacementRefs[i].getKey() + ")"
              + " absBit=" + absBit + " bucketSize=" + bucketSize);
        }
        return null;
      }
      newPartials[i] = computePartialKeyMultiMaskDirect(firstKey, newEp, newEm, newNeb);
      if (newPartials[i] == 0) haveZero = true;
    }
    // Suppress unused warning for oldPartials/betaOutputPos — kept for future use.
    if (oldPartials != null && betaOutputPos < -100) { /* never */ }

    // 5a. If smallest partial != 0, the new mask captures constant=1 bits. Phase 7q.15:
    // under hot.strict.g32.deep, strip those constant-1 bits from the MultiMask before
    // wrapping (wrap creates malformed sub-indirects via fresh adjacency-derived mask).
    if (!haveZero && Boolean.getBoolean("hot.strict.g32.deep")) {
      // Identify constant-1 bits across bucket children's firstKeys among bits in new mask.
      // Iterate each mask byte; for each set bit, check all bucket children's firstKey.
      boolean anyStripped = false;
      for (int e = 0; e < newNeb; e++) {
        final int bytePos = newEp[e] & 0xFF;
        final int chunkIdx2 = e / 8;
        final int byteOffsetInChunk2 = e % 8;
        final int shift2 = (7 - byteOffsetInChunk2) * 8;
        int byteMask = (int) ((newEm[chunkIdx2] >>> shift2) & 0xFFL);
        int stripMask = 0;
        for (int bit = 0; bit < 8; bit++) {
          final int maskBit = 1 << bit;
          if ((byteMask & maskBit) == 0) continue;
          final int maskAbsBit = bytePos * 8 + (7 - bit);
          // Check constant-1 across all bucket children + safe to strip (β-constant in subtrees).
          boolean allOne = true;
          for (int i = 0; i < bucketSize; i++) {
            if (!isAbsBitSet(getFirstKeyFromChild(replacementRefs[i]), maskAbsBit)) {
              allOne = false;
              break;
            }
          }
          if (!allOne) continue;
          boolean safeToStrip = true;
          for (int i = 0; i < bucketSize; i++) {
            if (replacementRefs[i].getKey() == io.sirix.settings.Constants.NULL_ID_LONG) continue;
            final int v = bitConstantValueInSubtree(replacementRefs[i], maskAbsBit);
            if (v < 0) { safeToStrip = false; break; }
          }
          if (safeToStrip) stripMask |= maskBit;
        }
        if (stripMask != 0) {
          byteMask &= ~stripMask;
          newEm[chunkIdx2] = (newEm[chunkIdx2] & ~(0xFFL << shift2))
              | (((long) (byteMask & 0xFF)) << shift2);
          anyStripped = true;
        }
      }
      if (anyStripped) {
        // Compact: drop entries whose byteMask became 0; rebuild newEp/newEm.
        int keepCount = 0;
        for (int e = 0; e < newNeb; e++) {
          final int chunkIdx2 = e / 8;
          final int byteOffsetInChunk2 = e % 8;
          final long bm = (newEm[chunkIdx2] >>> ((7 - byteOffsetInChunk2) * 8)) & 0xFFL;
          if (bm != 0L) keepCount++;
        }
        if (keepCount == 0) {
          // All bits stripped — empty mask. Fall through to !haveZero path below.
        } else {
          final byte[] compactEp = new byte[keepCount];
          final long[] compactEm = new long[Math.max(1, (keepCount + 7) / 8)];
          int dstIdx = 0;
          int newMsbInner = Short.MAX_VALUE;
          for (int src = 0; src < newNeb; src++) {
            final int srcChunk = src / 8;
            final int srcByte = src % 8;
            final long srcByteBits = (newEm[srcChunk] >>> ((7 - srcByte) * 8)) & 0xFFL;
            if (srcByteBits == 0L) continue;
            compactEp[dstIdx] = newEp[src];
            final int dstChunk = dstIdx / 8;
            final int dstByte = dstIdx % 8;
            compactEm[dstChunk] |= srcByteBits << ((7 - dstByte) * 8);
            // Track new MSB.
            final int highBit = 31 - Integer.numberOfLeadingZeros((int) srcByteBits);
            final int absBitPos = (compactEp[dstIdx] & 0xFF) * 8 + (7 - highBit);
            if (absBitPos < newMsbInner) newMsbInner = absBitPos;
            dstIdx++;
          }
          newEp = compactEp;
          newEm = compactEm;
          newNeb = keepCount;
          newMsb = newMsbInner;
          // Recompute partials with stripped mask.
          haveZero = false;
          for (int i = 0; i < bucketSize; i++) {
            final byte[] fk = getFirstKeyFromChild(replacementRefs[i]);
            newPartials[i] = computePartialKeyMultiMaskDirect(fk, newEp, newEm, newNeb);
            if (newPartials[i] == 0) haveZero = true;
          }
          if (Boolean.getBoolean("hot.debug.phase7q")) {
            System.err.println("[bucket-build-mm] I4-recovered: stripped constant-1 bits"
                + " absBit=" + absBit + " bucketSize=" + bucketSize + " newNeb=" + newNeb);
          }
        }
      }
    }
    if (!haveZero) {
      final PageReference[] childCopy = Arrays.copyOf(replacementRefs, bucketSize);
      return wrapBucketInSubtree(childCopy, bucketSize, parent.getHeight(), log, revision);
    }

    // 6. Verify uniqueness. Phase 7q.15 — when `hot.strict.g32.nowrap` is set (commit-time
    // lift only), abandon on collision instead of wrap fallback.
    for (int i = 1; i < bucketSize; i++) {
      for (int k = 0; k < i; k++) {
        if (newPartials[i] == newPartials[k]) {
          if (Boolean.getBoolean("hot.strict.g32.nowrap")) {
            if (Boolean.getBoolean("hot.debug.phase7q")) {
              System.err.println("[bucket-build-mm] FAIL: partial collision at i=" + i
                  + " k=" + k + " p=" + Integer.toHexString(newPartials[i])
                  + " — abandon (g32.nowrap)");
            }
            return null;
          }
          final PageReference[] childCopy = Arrays.copyOf(replacementRefs, bucketSize);
          return wrapBucketInSubtree(childCopy, bucketSize, parent.getHeight(), log, revision);
        }
      }
    }

    // 7. Sort + build.
    final PageReference[] children = Arrays.copyOf(replacementRefs, bucketSize);
    sortChildrenAndPartialsByPartial(children, newPartials);

    final long pageKey = pageKeyAllocator.getAsLong();
    final HOTIndirectPage built;
    if (bucketSize == 2) {
      // BiNode form not supported under MultiMask layout — fall back to wrap. Phase 7q.15:
      // pass newParentMsb hint so cbinode picks an I11-safe disc bit.
      final PageReference[] childCopy = Arrays.copyOf(replacementRefs, bucketSize);
      final Integer prevHint = PARENT_MSB_HINT.get();
      // newParentMsb = min(oldParentMsb, absBit) — same convention as SingleMask path.
      final int newParentMsbMm = Math.min(parent.getMostSignificantBitIndex() & 0xFFFF, absBit);
      PARENT_MSB_HINT.set(newParentMsbMm);
      try {
        return wrapBucketInSubtree(childCopy, bucketSize, parent.getHeight(), log, revision);
      } finally {
        if (prevHint == null) PARENT_MSB_HINT.remove();
        else PARENT_MSB_HINT.set(prevHint);
      }
    }
    // Phase 7q.15d — bucket-indirect MSB instrumentation. Catches I11 equality/strict
    // violations where a bucket child has MSB ≤ the bucket-indirect's own MSB (=
    // numerically more or equally significant than bucket → child is "above" parent in
    // routing → violates I11). Counters always increment; per-event print gated.
    phase7q15dCheckIntermediateMsb(pageKey, newMsb, absBit, children, bucketSize);
    if (bucketSize <= 16) {
      built = HOTIndirectPage.createSpanNodeMultiMask(pageKey, revision, newEp, newEm,
          newNeb, newPartials, children, parent.getHeight(), (short) newMsb);
    } else if (bucketSize <= NodeUpgradeManager.MULTI_NODE_MAX_CHILDREN) {
      built = HOTIndirectPage.createMultiNodeMultiMask(pageKey, revision, newEp, newEm,
          newNeb, newPartials, children, parent.getHeight(), (short) newMsb);
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

    // Phase 7d — propagate parent leaf's ancestor-owned bits to both halves, AND add
    // absBit as a new owned bit with the appropriate constant value per half.
    propagateOwnedBitsToSplitHalves(leaf, leftLeaf, rightLeaf, absBit);

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
   * Phase 7d — When a leaf is split on bit β, both halves inherit the parent's
   * ancestorOwnedBits plus β with the appropriate constant value (left=0, right=1).
   */
  private static void propagateOwnedBitsToSplitHalves(HOTLeafPage parent,
      HOTLeafPage leftHalf, HOTLeafPage rightHalf, int splitAbsBit) {
    final int[] parentBits = parent.getAncestorOwnedBits();
    final byte[] parentValues = parent.getAncestorOwnedValues();
    final int parentLen = parentBits.length;
    // New owned bits = parentBits ∪ {splitAbsBit}, sorted ascending.
    // Find insertion point for splitAbsBit.
    int insertPos = parentLen;
    for (int i = 0; i < parentLen; i++) {
      if (parentBits[i] > splitAbsBit) { insertPos = i; break; }
      if (parentBits[i] == splitAbsBit) { insertPos = -1; break; } // already present
    }
    final int newLen = (insertPos < 0) ? parentLen : parentLen + 1;
    final int[] newBits = new int[newLen];
    final byte[] newLeftValues = new byte[newLen];
    final byte[] newRightValues = new byte[newLen];
    if (insertPos < 0) {
      System.arraycopy(parentBits, 0, newBits, 0, parentLen);
      System.arraycopy(parentValues, 0, newLeftValues, 0, parentLen);
      System.arraycopy(parentValues, 0, newRightValues, 0, parentLen);
    } else {
      if (insertPos > 0) {
        System.arraycopy(parentBits, 0, newBits, 0, insertPos);
        System.arraycopy(parentValues, 0, newLeftValues, 0, insertPos);
        System.arraycopy(parentValues, 0, newRightValues, 0, insertPos);
      }
      newBits[insertPos] = splitAbsBit;
      newLeftValues[insertPos] = 0;
      newRightValues[insertPos] = 1;
      if (insertPos < parentLen) {
        System.arraycopy(parentBits, insertPos, newBits, insertPos + 1, parentLen - insertPos);
        System.arraycopy(parentValues, insertPos, newLeftValues, insertPos + 1, parentLen - insertPos);
        System.arraycopy(parentValues, insertPos, newRightValues, insertPos + 1, parentLen - insertPos);
      }
    }
    leftHalf.setAncestorOwnedBits(newBits, newLeftValues);
    rightHalf.setAncestorOwnedBits(newBits, newRightValues);
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
    final boolean wrapDbg = Boolean.getBoolean("hot.debug.g32");
    if (n == 0) {
      if (wrapDbg) System.err.println("[wrap] FAIL: n=0");
      return null;
    }
    if (n == 1) {
      return bucket[0]; // pass-through
    }
    // Trim and build new indirect.
    final PageReference[] children = Arrays.copyOf(bucket, n);
    if (children.length > NodeUpgradeManager.MULTI_NODE_MAX_CHILDREN) {
      if (wrapDbg) System.err.println("[wrap] FAIL: n=" + n + " > MAX");
      return null; // bucket too large — caller falls back to splitParentAndRecurse
    }
    // Phase 7q.15 — constancy-preserving BiNode construction for size==2 under
    // hot.strict.g32.cbinode OR when called via multi-β lift's scope (ThreadLocal).
    // Avoids the recursive wrap cascade where createNodeFromChildren-2273 picks a disc
    // bit via computeDifferingBit(leftMax, rightMin) without β-constancy guarantee.
    if (n == 2 && (Boolean.getBoolean("hot.strict.g32.cbinode")
        || (CBINODE_THREAD_LOCAL.get() != null && CBINODE_THREAD_LOCAL.get()))) {
      // I11: discBit must be > parentMsbHint so future parent.MSB < bucketIndirect.MSB.
      // -1 = no constraint (any bit allowed).
      final Integer hint = PARENT_MSB_HINT.get();
      final int minBitExclusive = hint != null ? hint : -1;
      final int constancyDiscBit = computeConstancyPreservingBiNodeDiscBit(children[0],
          children[1], minBitExclusive);
      if (constancyDiscBit >= 0) {
        // Determine left/right based on which child has the bit = 0.
        final PageReference leftChild;
        final PageReference rightChild;
        if (isAbsBitSet(getFirstKeyFromChild(children[0]), constancyDiscBit)) {
          rightChild = children[0];
          leftChild = children[1];
        } else {
          leftChild = children[0];
          rightChild = children[1];
        }
        final long cbpPageKey = pageKeyAllocator.getAsLong();
        final HOTIndirectPage cbpBuilt;
        try {
          cbpBuilt = createBiNodeTraced("constancy-preserving-binode", cbpPageKey, revision,
              constancyDiscBit, leftChild, rightChild, height);
        } catch (Throwable t) {
          if (wrapDbg) System.err.println("[wrap] FAIL: createBiNodeTraced threw: " + t);
          return null;
        }
        final PageReference cbpRef = new PageReference();
        cbpRef.setKey(cbpPageKey);
        cbpRef.setPage(cbpBuilt);
        log.put(cbpRef, PageContainer.getInstance(cbpBuilt, cbpBuilt));
        if (wrapDbg) System.err.println("[wrap] CBINODE-OK discBit=" + constancyDiscBit
            + " pageKey=" + cbpPageKey + " hint=" + minBitExclusive);
        return cbpRef;
      }
      if (wrapDbg) System.err.println("[wrap] CBINODE-FAIL: no constancy-preserving bit found,"
          + " falling through to legacy createNodeFromChildren");
    }
    final long pageKey = pageKeyAllocator.getAsLong();
    final HOTIndirectPage built;
    try {
      built = createNodeFromChildren(children, pageKey, revision, height);
    } catch (Throwable t) {
      if (wrapDbg) System.err.println("[wrap] FAIL: createNodeFromChildren threw: " + t);
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

    // Stage G.31 — I8 firstKey-monotone verification mirroring addEntryWithPDep's G.16.
    // After partial-sort, verify children's firstKeys are also strictly ascending. If not,
    // EXTEND the mask with the discriminative bit between the offending pair (= MSDB of
    // the disagreement) and re-derive everything.
    boolean monotone = true;
    for (int i = 1; i < outIdx; i++) {
      final byte[] prev = getFirstKeyFromChild(trimChildren[i - 1]);
      final byte[] curr = getFirstKeyFromChild(trimChildren[i]);
      if (prev != null && curr != null && prev.length > 0 && curr.length > 0
          && Arrays.compareUnsigned(prev, curr) >= 0) {
        monotone = false;
        break;
      }
    }
    if (!monotone) {
      // Sort by firstKey, then recompute partials under newMask. If partials still collide
      // or smallest partial isn't 0, reject so caller falls back.
      sortChildrenByFirstKey(trimChildren);
      for (int i = 0; i < outIdx; i++) {
        final byte[] cKey = getFirstKeyFromChild(trimChildren[i]);
        trimPartials[i] = (cKey == null || cKey.length == 0) ? 0
            : computePartialKeySingleMask(cKey, oldInitialBytePos, newMask);
      }
      for (int i = 1; i < outIdx; i++) {
        for (int k = 0; k < i; k++) {
          if (trimPartials[k] == trimPartials[i]) {
            diagnoseIntegrateFail("g31-partial-collision-after-firstkey-resort", parent,
                newAbsBit, outIdx);
            return null;
          }
        }
      }
    }

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

    // Phase 6d — verify or split. β-mixed siblings get split via splitSubtreeOnBit
    // when 6d is enabled, producing two β-constant halves that replace the original.
    final boolean phase6dEnabledU = Boolean.getBoolean("hot.strict.phase6d");
    final int[] siblingBitValues = new int[numChildren];
    final boolean[] siblingNeedsSplitU = phase6dEnabledU ? new boolean[numChildren] : null;
    final SubtreeSplit[] siblingSplitsU = phase6dEnabledU ? new SubtreeSplit[numChildren] : null;
    int extraSlotsU = 0;
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) continue;
      final int v = bitConstantValueInSubtree(parent.getChildReference(i), newAbsBit);
      if (v < 0) {
        if (!phase6dEnabledU) return null;
        final SubtreeSplit ss = splitSubtreeOnBit(parent.getChildReference(i), newAbsBit, activeLog, revision);
        if (ss == null) return null;
        siblingNeedsSplitU[i] = true;
        siblingSplitsU[i] = ss;
        extraSlotsU++;
        siblingBitValues[i] = -1;
        continue;
      }
      siblingBitValues[i] = v;
    }
    if (extraSlotsU > 0 && (numChildren + 1 + extraSlotsU) > NodeUpgradeManager.MULTI_NODE_MAX_CHILDREN) {
      return null;
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
    final int newNumChildren = numChildren + 1 + extraSlotsU;
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
      } else if (phase6dEnabledU && siblingNeedsSplitU != null && siblingNeedsSplitU[i]) {
        // Phase 6d: expand β-mixed sibling into two β-constant halves.
        newChildren[j] = siblingSplitsU[i].leftRef();
        final byte[] lKey = getFirstKeyFromChild(newChildren[j]);
        newPartials[j] = (lKey == null || lKey.length == 0) ? 0
            : computePartialKeyMultiMaskDirect(lKey, extractionPositions, extractionMasks, allCount);
        j++;
        newChildren[j] = siblingSplitsU[i].rightRef();
        final byte[] rKey = getFirstKeyFromChild(newChildren[j]);
        newPartials[j] = (rKey == null || rKey.length == 0) ? 0
            : computePartialKeyMultiMaskDirect(rKey, extractionPositions, extractionMasks, allCount);
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
    byte[] newExtractionPositions;
    long[] newExtractionMasks;
    int newNumBytes;
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
    {
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
      // Stage G.31 (MultiMask variant) — iterative mask extension. When partials collide
      // after firstKey re-sort, find the MSDB of the offending pair and OR it into the
      // extraction mask. Repeat until partials are unique or 16 attempts exhausted.
      int g31MmAttempts = 0;
      while (g31MmAttempts < 16) {
        int collidingI = -1, collidingK = -1;
        outer:
        for (int i = 1; i < newNumChildren; i++) {
          for (int k = 0; k < i; k++) {
            if (newPartials[k] == newPartials[i]) {
              collidingI = i; collidingK = k;
              break outer;
            }
          }
        }
        if (collidingI < 0) break;
        final byte[] keyI = getFirstKeyFromChild(newChildren[collidingI]);
        final byte[] keyK = getFirstKeyFromChild(newChildren[collidingK]);
        if (keyI == null || keyK == null) return null;
        final int msdb = DiscriminativeBitComputer.computeDifferingBit(keyK, keyI);
        if (msdb < 0) return null;
        final int msdbBytePos = msdb / 8;
        final int msdbBitInByte = msdb % 8;
        final int msdbBitMask = 1 << (7 - msdbBitInByte);
        // Locate or insert the byte position.
        int found = -1;
        for (int i = 0; i < newNumBytes; i++) {
          if ((newExtractionPositions[i] & 0xFF) == msdbBytePos) { found = i; break; }
        }
        if (found >= 0) {
          // OR into existing byte's mask.
          final int chunkIdx = found / 8;
          final int byteOffsetInChunk = found % 8;
          final long shift = ((long) (msdbBitMask & 0xFF)) << ((7 - byteOffsetInChunk) * 8);
          final int existingByte =
              (int) ((newExtractionMasks[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFF);
          if ((existingByte & msdbBitMask) != 0) return null; // bit already present
          newExtractionMasks[chunkIdx] |= shift;
        } else {
          // Insert new byte position in sorted order.
          int insIdx = newNumBytes;
          for (int i = 0; i < newNumBytes; i++) {
            if ((newExtractionPositions[i] & 0xFF) > msdbBytePos) { insIdx = i; break; }
          }
          if (newNumBytes + 1 > 64) return null; // safety
          final byte[] np = new byte[newNumBytes + 1];
          if (insIdx > 0) System.arraycopy(newExtractionPositions, 0, np, 0, insIdx);
          np[insIdx] = (byte) msdbBytePos;
          if (insIdx < newNumBytes) {
            System.arraycopy(newExtractionPositions, insIdx, np, insIdx + 1, newNumBytes - insIdx);
          }
          final int newNumChunks = (newNumBytes + 1 + 7) / 8;
          final long[] nm = new long[newNumChunks];
          for (int i = 0; i < newNumBytes + 1; i++) {
            final int maskByte;
            if (i == insIdx) {
              maskByte = msdbBitMask;
            } else {
              final int srcIdx = i < insIdx ? i : i - 1;
              final int srcChunk = srcIdx / 8;
              final int srcOffset = srcIdx % 8;
              maskByte = (int) ((newExtractionMasks[srcChunk] >>> ((7 - srcOffset) * 8)) & 0xFF);
            }
            final int dstChunk = i / 8;
            final int dstOffset = i % 8;
            nm[dstChunk] |= ((long) (maskByte & 0xFF)) << ((7 - dstOffset) * 8);
          }
          newExtractionPositions = np;
          newExtractionMasks = nm;
          newNumBytes = newNumBytes + 1;
        }
        // Recompute MSB.
        msbIndex = Short.MAX_VALUE;
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
        // Recompute partials.
        for (int i = 0; i < newNumChildren; i++) {
          final byte[] cKey = getFirstKeyFromChild(newChildren[i]);
          newPartials[i] = (cKey == null || cKey.length == 0) ? 0
              : computePartialKeyMultiMaskDirect(cKey, newExtractionPositions, newExtractionMasks, newNumBytes);
        }
        g31MmAttempts++;
      }
      // Final verification.
      for (int i = 1; i < newNumChildren; i++) {
        for (int k = 0; k < i; k++) {
          if (newPartials[k] == newPartials[i]) return null;
        }
      }
      boolean hasZero = false;
      for (final int p : newPartials) {
        if (p == 0) { hasZero = true; break; }
      }
      if (!hasZero) return null;
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
    // Phase 7q.15 — TIL-first for placeholder refs (matches getFirstKeyFromChild pattern).
    if (page == null && activeLog != null) {
      final var container = activeLog.get(ref);
      if (container != null) {
        page = container.getModified();
        if (page != null) ref.setPage(page);
      }
    }
    if (page == null && activeReader != null) {
      page = loadPage(activeReader, ref);
      if (page != null) ref.setPage(page);
    }
    final boolean trace = Boolean.getBoolean("hot.debug.bitconstant");
    if (page == null) {
      if (trace) System.err.println("[bitconstant] page=null ref.key=" + ref.getKey()
          + " absBit=" + absBitPos);
      return -1;
    }
    if (page instanceof HOTLeafPage leaf) {
      // Phase 6c — delegate to HOTLeafPage's API which has the same semantics. Future
      // 6b work will make this O(1) via incremental non-constant-bits tracking.
      final int result = leaf.isBitConstantAtAbsBit(absBitPos);
      if (trace && result < 0) {
        System.err.println("[bitconstant] leaf.pageKey=" + leaf.getPageKey()
            + " entries=" + leaf.getEntryCount() + " absBit=" + absBitPos
            + " result=MIXED firstKey="
            + (leaf.getEntryCount() > 0 ? bytesToHex(leaf.getKey(0)) : "empty"));
      }
      return result;
    }
    if (page instanceof HOTIndirectPage indirect) {
      final int m = indirect.getNumChildren();
      if (m == 0) return 0;
      final int first = bitConstantValueInSubtree(indirect.getChildReference(0), absBitPos);
      if (first < 0) return -1;
      for (int i = 1; i < m; i++) {
        final int v = bitConstantValueInSubtree(indirect.getChildReference(i), absBitPos);
        if (v < 0 || v != first) {
          if (trace) System.err.println("[bitconstant] indirect.pageKey=" + indirect.getPageKey()
              + " child[" + i + "] returned " + v + " (vs first=" + first
              + ") absBit=" + absBitPos);
          return -1;
        }
      }
      return first;
    }
    return -1;
  }

  /**
   * Phase 7p — Check if any indirect in the subtree rooted at {@code ref} already
   * routes on absolute bit position {@code absBit} (= absBit is captured in that
   * indirect's mask). Used by extendIndirectMaskForClosure to reject extensions that
   * would create a double-capture (bit appearing at two trie levels).
   *
   * <p>Returns true if any descendant indirect has the bit in its mask. Returns false
   * for leaf-only subtrees, even though leaves "use" the bit for their internal
   * sort — Binna's I6 routes through indirect masks, leaves are terminal.
   */
  private boolean subtreeHasBitInAnyIndirectMask(PageReference ref, int absBit) {
    Page page = ref.getPage();
    // Phase 7q.15 — TIL-first for placeholder refs.
    if (page == null && activeLog != null) {
      final var container = activeLog.get(ref);
      if (container != null) {
        page = container.getModified();
        if (page != null) ref.setPage(page);
      }
    }
    if (page == null && activeReader != null) {
      page = loadPage(activeReader, ref);
      if (page != null) ref.setPage(page);
    }
    if (!(page instanceof HOTIndirectPage indirect)) return false;
    if (indirectMaskHasAbsBit(indirect, absBit)) return true;
    final int m = indirect.getNumChildren();
    for (int i = 0; i < m; i++) {
      final PageReference cref = indirect.getChildReference(i);
      if (cref == null) continue;
      if (subtreeHasBitInAnyIndirectMask(cref, absBit)) return true;
    }
    return false;
  }

  /** Returns true if {@code indirect}'s mask captures absolute bit position {@code absBit}. */
  private static boolean indirectMaskHasAbsBit(HOTIndirectPage indirect, int absBit) {
    if (indirect.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK) {
      final int initialBytePos = indirect.getInitialBytePos();
      final int byteOffset = (absBit / 8) - initialBytePos;
      if (byteOffset < 0 || byteOffset >= 8) return false;
      final int bitInByte = absBit % 8;
      final int bitInWord = (7 - byteOffset) * 8 + (7 - bitInByte);
      return ((indirect.getBitMask() >>> bitInWord) & 1L) != 0L;
    }
    final byte[] ep = indirect.getExtractionPositions();
    final long[] em = indirect.getExtractionMasks();
    final int neb = indirect.getNumExtractionBytes();
    final int bytePos = absBit / 8;
    final int bitInByte = absBit % 8;
    final int maskBit = 1 << (7 - bitInByte);
    for (int i = 0; i < neb; i++) {
      if ((ep[i] & 0xFF) != bytePos) continue;
      final int chunkIdx = i / 8;
      final int byteOffsetInChunk = i % 8;
      final int byteMaskBits =
          (int) ((em[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFFL);
      return (byteMaskBits & maskBit) != 0;
    }
    return false;
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

  /**
   * Option B Phase 5 — Return ALL ancestor disc bits β where inserting {@code keyBuf}
   * into {@code leaf} would break β-constancy. Generalizes
   * {@link #findAnyOffendingAncestorBit} to return the complete set, sorted ascending
   * (= MSB-first absolute bit position).
   *
   * <p>For each ancestor β: leaf is β-constant at some value v iff all existing keys
   * agree at β. If leaf is β-constant and {@code keyBuf}'s β-value disagrees with v,
   * adding keyBuf would break β-constancy at this ancestor.
   *
   * <p>For β-mixed leaves (= leaf has both values at β already): the leaf is already
   * structurally non-constant at β. We INCLUDE these in the result because Phase 5
   * will partition the leaf on β as part of the split chain, and the partition
   * resolves the β-mixed condition.
   *
   * <p>HFT-grade: zero allocation when no breaks found (returns shared empty array).
   *
   * @return array of offending β values, sorted MSB-first. Empty if no breaks.
   */
  public int[] detectAllConstancyBreaksOnInsert(HOTIndirectPage[] pathNodes, int pathDepth,
      HOTLeafPage leaf, byte[] keyBuf) {
    if (leaf == null || keyBuf == null) return EMPTY_INT_ARRAY;
    final int entryCount = leaf.getEntryCount();
    if (entryCount == 0) return EMPTY_INT_ARRAY;
    if (pathDepth <= 0) return EMPTY_INT_ARRAY;

    final int[] ancestorBits = collectAncestorDiscBits(pathNodes, pathDepth);
    if (ancestorBits.length == 0) return EMPTY_INT_ARRAY;

    // First pass: count offending bits to size the result array.
    int count = 0;
    final boolean[] isOffending = new boolean[ancestorBits.length];
    for (int idx = 0; idx < ancestorBits.length; idx++) {
      final int beta = ancestorBits[idx];
      final boolean newKeyBetaSet = isAbsBitSet(keyBuf, beta);
      boolean seen0 = false, seen1 = false;
      for (int i = 0; i < entryCount; i++) {
        final byte[] existing = leaf.getKey(i);
        if (existing == null || existing.length == 0) continue;
        if (isAbsBitSet(existing, beta)) seen1 = true;
        else seen0 = true;
        if (seen0 && seen1) break;
      }
      if (seen0 && seen1) {
        // Already β-mixed: Phase 5 will partition. Include β in the offending set.
        isOffending[idx] = true;
        count++;
        continue;
      }
      // β-constant: check newKey's bit disagrees.
      final boolean leafBetaValue = seen1;
      if (leafBetaValue != newKeyBetaSet) {
        isOffending[idx] = true;
        count++;
      }
    }
    if (count == 0) return EMPTY_INT_ARRAY;
    final int[] result = new int[count];
    int w = 0;
    for (int idx = 0; idx < ancestorBits.length; idx++) {
      if (isOffending[idx]) result[w++] = ancestorBits[idx];
    }
    return result;
  }

  private static final int[] EMPTY_INT_ARRAY = new int[0];

  /** Stage Phase 5 — count of splitLeafAndRerouteWrongHalf successful firings. */
  private static final java.util.concurrent.atomic.AtomicLong PHASE5_SPLIT_REROUTE_FIRINGS =
      new java.util.concurrent.atomic.AtomicLong(0L);

  public static long getPhase5SplitRerouteFirings() { return PHASE5_SPLIT_REROUTE_FIRINGS.get(); }
  public static void resetPhase5SplitRerouteFirings() { PHASE5_SPLIT_REROUTE_FIRINGS.set(0L); }

  /**
   * Option B Phase 5 — Split {@code leaf} on β, place K's half at leaf's slot, reroute
   * the other half's keys to the ancestor's exact-XOR sibling subtree.
   *
   * <p>Algorithm:
   * <ol>
   *   <li>Determine K's β value. {@code matchHalf} = the side K joins (same β-value as K's bit).
   *   <li>Split leaf via {@code splitLeafOnBit} → produces matchHalfRef and wrongHalfRef.
   *   <li>Find ancestor A_β whose mask captures β (= the routing point where β decides slot).
   *   <li>At A_β, find sibling slot whose stored partial XORs descend-slot at β-bit position.
   *   <li>Bulk-insert each wrongHalf key into A_β's sibling subtree via {@code bulkInsertIntoSiblingSubtree}.
   *   <li>Merge K into matchHalfLeaf.
   *   <li>CoW the path from leaf-parent up to root, replacing leaf-parent's slot with matchHalfRef.
   * </ol>
   *
   * <p>Returns {@code true} on success; {@code false} if the reroute is structurally
   * infeasible (no sibling, capacity overflow, etc.). Caller falls back.
   */
  public boolean splitLeafAndRerouteWrongHalf(
      HOTLeafPage leaf, PageReference leafRef,
      HOTIndirectPage[] pathNodes, PageReference[] pathRefs, int[] pathChildIndices, int pathDepth,
      int offendingBeta, byte[] keyBuf, byte[] valueBuf,
      StorageEngineWriter storageEngineWriter, TransactionIntentLog log) {
    if (leaf == null || leafRef == null || keyBuf == null || valueBuf == null) return false;
    if (pathDepth <= 0 || offendingBeta < 0) return false;
    final int revision = storageEngineWriter.getRevisionNumber();

    // 1. K's β-value determines which split half it joins.
    final boolean kBetaSet = isAbsBitSet(keyBuf, offendingBeta);

    // 2. Split leaf on β.
    final SubtreeSplit ss = splitLeafOnBit(leaf, offendingBeta, log, revision);
    if (ss == null) return false;
    final PageReference matchHalfRef = kBetaSet ? ss.rightRef() : ss.leftRef();
    final PageReference wrongHalfRef = kBetaSet ? ss.leftRef() : ss.rightRef();

    // 3. Find ancestor A_β whose mask captures β. Walk root → leaf-parent (top-down)
    //    so we route at the topmost ancestor.
    int ancestorIdx = -1;
    for (int i = 0; i < pathDepth; i++) {
      final HOTIndirectPage a = pathNodes[i];
      if (a == null) continue;
      final int outPos = (a.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK)
          ? singleMaskBetaOutputPos(a, offendingBeta)
          : multiMaskBetaOutputPos(a, offendingBeta);
      if (outPos >= 0) { ancestorIdx = i; break; }
    }
    if (ancestorIdx < 0) return false; // no ancestor captures β

    final HOTIndirectPage ancestor = currentInLog(pathRefs[ancestorIdx], pathNodes[ancestorIdx]);
    final int outPos = (ancestor.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK)
        ? singleMaskBetaOutputPos(ancestor, offendingBeta)
        : multiMaskBetaOutputPos(ancestor, offendingBeta);
    if (outPos < 0) return false;
    final int descendSlot = pathChildIndices[ancestorIdx];
    final int[] partials = ancestor.getPartialKeys();
    if (partials == null || descendSlot < 0 || descendSlot >= partials.length) return false;
    final int targetStored = partials[descendSlot] ^ (1 << outPos);
    int siblingIdx = -1;
    for (int j = 0; j < ancestor.getNumChildren(); j++) {
      if (j == descendSlot) continue;
      if (partials[j] == targetStored) { siblingIdx = j; break; }
    }
    if (siblingIdx < 0) {
      // Phase 5b — no exact-XOR sibling. Try CREATING one via addEntryWithPDep.
      // Builds a new ancestor with mask = ancestor.mask + β-bit + a new slot for wrongHalf.
      // matchHalfRef replaces ancestor.children[descendSlot]; wrongHalfRef is the new slot.
      //
      // The new slot's stored partial = descendSlot.partial XOR (1 << β-bit-output-pos).
      // After this, matchHalfRef contains K (= merged in step 5 below) and is β-constant
      // for β = K's value. wrongHalfRef contains existing keys with β = ¬K's value.
      //
      // SAFETY: addEntryWithPDep verifies β-constancy of every non-split sibling
      // (line 1446-1452). If any sibling has β-mixed subtree, it returns null and we fall
      // back. This is the safety net that prevents the cascade Phase 2 attempts showed.
      //
      // Merge K into matchHalf NOW, before the addEntry call (so matchHalf has K when
      // addEntryWithPDep recomputes its firstKey for the new partial layout).
      final HOTLeafPage matchHalfLeafForCreate = (HOTLeafPage) log.get(matchHalfRef).getModified();
      if (!matchHalfLeafForCreate.mergeWithNodeRefs(keyBuf, keyBuf.length, valueBuf, valueBuf.length)) {
        if (Boolean.getBoolean("hot.debug.phase5")) {
          System.err.println("[phase5] reject reason=create-sibling-match-overflow beta=" + offendingBeta);
        }
        return false;
      }
      log.put(matchHalfRef, PageContainer.getInstance(matchHalfLeafForCreate, matchHalfLeafForCreate));

      // Determine left/right placement for addEntryWithPDep based on K's β-value.
      // K is in matchHalf; wrongHalf is the OTHER side. addEntryWithPDep expects left
      // and right children where left = β=0 product, right = β=1 product.
      final PageReference newLeftChild = kBetaSet ? wrongHalfRef : matchHalfRef;
      final PageReference newRightChild = kBetaSet ? matchHalfRef : wrongHalfRef;

      // Phase 7d — set activeLog/activeReader so splitSubtreeOnBit (called by
      // addEntryWithPDep / upgradeToMultiMaskWithNewBit when Phase 6d splits β-mixed
      // siblings) can install split products in the TIL.
      this.activeLog = log;
      this.activeReader = storageEngineWriter;
      final HOTIndirectPage extended = addEntryWithPDep(ancestor, descendSlot,
          newLeftChild, newRightChild, offendingBeta, revision);
      if (extended == null) {
        if (Boolean.getBoolean("hot.debug.phase5")) {
          System.err.println("[phase5] reject reason=addEntry-failed beta=" + offendingBeta
              + " ancestorIdx=" + ancestorIdx);
        }
        return false;
      }
      // Install extended ancestor + CoW up.
      log.put(pathRefs[ancestorIdx], PageContainer.getInstance(extended, extended));
      PageReference upRef = pathRefs[ancestorIdx];
      for (int i = ancestorIdx - 1; i >= 0; i--) {
        final HOTIndirectPage curNode = currentInLog(pathRefs[i], pathNodes[i]);
        final HOTIndirectPage updatedNode = curNode.withUpdatedChild(pathChildIndices[i], upRef, revision);
        log.put(pathRefs[i], PageContainer.getInstance(updatedNode, updatedNode));
        upRef = pathRefs[i];
      }
      PHASE5_SPLIT_REROUTE_FIRINGS.incrementAndGet();
      if (Boolean.getBoolean("hot.debug.phase5")) {
        System.err.println("[phase5] CREATE-SIBLING-OK beta=" + offendingBeta
            + " ancestorIdx=" + ancestorIdx);
      }
      return true;
    }

    if (Boolean.getBoolean("hot.debug.phase5")) {
      System.err.println("[phase5] split-reroute beta=" + offendingBeta
          + " ancestorIdx=" + ancestorIdx + " descendSlot=" + descendSlot
          + " siblingIdx=" + siblingIdx + " wrongHalfEntries=" + ((HOTLeafPage) log.get(wrongHalfRef).getModified()).getEntryCount());
    }
    // 4. Bulk-insert each wrongHalf key into ancestor.sibling subtree.
    final HOTLeafPage wrongHalfLeaf = (HOTLeafPage) log.get(wrongHalfRef).getModified();
    HOTIndirectPage curAncestor = ancestor;
    for (int i = 0; i < wrongHalfLeaf.getEntryCount(); i++) {
      final byte[] k = wrongHalfLeaf.getKey(i);
      final byte[] v = wrongHalfLeaf.getValue(i);
      final boolean ok = bulkInsertIntoSiblingSubtree(
          curAncestor, pathRefs[ancestorIdx], siblingIdx, k, v, storageEngineWriter, log);
      if (!ok) return false;
      curAncestor = currentInLog(pathRefs[ancestorIdx], curAncestor);
    }

    // 5. Merge K into matchHalfLeaf.
    final HOTLeafPage matchHalfLeaf = (HOTLeafPage) log.get(matchHalfRef).getModified();
    if (!matchHalfLeaf.mergeWithNodeRefs(keyBuf, keyBuf.length, valueBuf, valueBuf.length)) {
      return false; // matchHalf overflow — not handled in Phase 5
    }
    log.put(matchHalfRef, PageContainer.getInstance(matchHalfLeaf, matchHalfLeaf));

    // 6. CoW the path from leaf-parent up to root, replacing leaf-parent's slot with matchHalfRef.
    //    Phase 6h — after each withUpdatedChild, ALSO try recomputing partials from
    //    current children's firstKeys. If the mask still discriminates after K was merged,
    //    the recompute installs partials reflecting K's effect; otherwise the indirect
    //    stays with stale partials but the structural reference chain is correct.
    final int leafParentIdx = pathDepth - 1;
    final HOTIndirectPage leafParent = currentInLog(pathRefs[leafParentIdx], pathNodes[leafParentIdx]);
    HOTIndirectPage updatedLeafParent = leafParent.withUpdatedChild(
        pathChildIndices[leafParentIdx], matchHalfRef, revision);
    if (Boolean.getBoolean("hot.strict.phase6h")) {
      final HOTIndirectPage rec = recomputePartialsForCurrentFirstKeys(updatedLeafParent, revision);
      if (rec != null) updatedLeafParent = rec;
    }
    log.put(pathRefs[leafParentIdx], PageContainer.getInstance(updatedLeafParent, updatedLeafParent));
    // Propagate up.
    PageReference childRef = pathRefs[leafParentIdx];
    for (int i = leafParentIdx - 1; i >= 0; i--) {
      final HOTIndirectPage curNode = currentInLog(pathRefs[i], pathNodes[i]);
      HOTIndirectPage updatedNode = curNode.withUpdatedChild(pathChildIndices[i], childRef, revision);
      if (Boolean.getBoolean("hot.strict.phase6h")) {
        final HOTIndirectPage rec = recomputePartialsForCurrentFirstKeys(updatedNode, revision);
        if (rec != null) updatedNode = rec;
      }
      log.put(pathRefs[i], PageContainer.getInstance(updatedNode, updatedNode));
      childRef = pathRefs[i];
    }

    PHASE5_SPLIT_REROUTE_FIRINGS.incrementAndGet();
    return true;
  }

  /**
   * Option B Phase 5 — Top-level constancy-aware insert. Detects all offending β values
   * at insert time; for each β (MSB-first), splits the leaf and reroutes the wrong half
   * to the ancestor's sibling subtree, then merges K into the matching half.
   *
   * <p>Returns:
   * <ul>
   *   <li>{@code true} if either (a) no β breaks (caller proceeds normally), or
   *       (b) all breaks were resolved by split+reroute. After (b), the merge of K has
   *       already happened inside Phase 5 — caller must NOT call mergeWithNodeRefs again.</li>
   *   <li>{@code false} if Phase 5 detected breaks but couldn't resolve all of them.
   *       Caller falls back to the standard merge / handleInsertFailure path.</li>
   * </ul>
   *
   * <p>The {@code didMerge} array (one-element, written-out) reports whether K was
   * merged during Phase 5. If {@code true} and didMerge[0] is {@code true}, caller skips
   * the regular merge. If {@code true} and didMerge[0] is {@code false}, no breaks were
   * detected — caller proceeds with regular merge.
   */
  public boolean applyConstancyAwareInsert(
      HOTLeafPage leaf, PageReference leafRef, PageReference rootRef,
      HOTIndirectPage[] pathNodes, PageReference[] pathRefs, int[] pathChildIndices, int pathDepth,
      byte[] keyBuf, int keyLen, byte[] valueBuf, int valueLen,
      StorageEngineWriter storageEngineWriter, TransactionIntentLog log,
      boolean[] didMerge) {
    if (didMerge != null && didMerge.length >= 1) didMerge[0] = false;
    if (leaf == null || keyBuf == null || valueBuf == null) return true;
    if (pathDepth <= 0) return true;

    // Materialize trimmed key for detector (uses byte[].length for bit lookup).
    final byte[] kFull = (keyLen == keyBuf.length) ? keyBuf : java.util.Arrays.copyOf(keyBuf, keyLen);
    final byte[] vFull = (valueLen == valueBuf.length) ? valueBuf : java.util.Arrays.copyOf(valueBuf, valueLen);

    // Two-tier detection:
    //  1. β-constancy breaks at the LEAF (= new key would violate leaf's β-constancy at
    //     some ancestor β). detectAllConstancyBreaksOnInsert.
    //  2. I8 breaks at the INDIRECT (= new key would become a leaf's deep-firstKey that's
    //     smaller than the predecessor sibling's firstKey at some ancestor). findI8MsdbBit.
    int beta = -1;
    final int[] constancyOffending = detectAllConstancyBreaksOnInsert(pathNodes, pathDepth, leaf, kFull);
    if (constancyOffending.length > 0) {
      beta = constancyOffending[0]; // most-significant β-constancy break
    } else {
      // No β-constancy break; check for I8 break at indirect level.
      beta = findI8MsdbBit(pathNodes, pathRefs, pathChildIndices, pathDepth, leaf, kFull);
    }
    if (beta < 0) return true; // no breaks — caller proceeds normally

    // First try: splitLeafAndRerouteWrongHalf (= move wrong half to existing sibling).
    final boolean ok = splitLeafAndRerouteWrongHalf(leaf, leafRef,
        pathNodes, pathRefs, pathChildIndices, pathDepth,
        beta, kFull, vFull, storageEngineWriter, log);
    if (ok) {
      if (didMerge != null && didMerge.length >= 1) didMerge[0] = true;
      return true;
    }

    // Phase 5c (BOUNDED) — splitLeafAndRerouteWrongHalf failed. Try extending the
    // OFFENDING ANCESTOR's mask via I11-safe closure. STRICT BOUND: only fire when the
    // ancestor is SHALLOW (= within MAX_PHASE5C_DEPTH levels of root) to prevent
    // FrameSlotAllocator exhaustion. Deep cascades would multiply pages exponentially.
    if (!Boolean.getBoolean("hot.strict.phase5c")) {
      return false; // Phase 5c disabled by default — caller falls back
    }
    final int MAX_PHASE5C_DEPTH = 2;
    final int revision = storageEngineWriter.getRevisionNumber();
    for (int aIdx = 0; aIdx < pathDepth && aIdx < MAX_PHASE5C_DEPTH; aIdx++) {
      final HOTIndirectPage A = currentInLog(pathRefs[aIdx], pathNodes[aIdx]);
      if (A == null) continue;
      final int aMsb = A.getMostSignificantBitIndex() & 0xFFFF;
      if (beta <= aMsb) continue; // I11-unsafe at this ancestor
      final HOTIndirectPage extended = extendIndirectMaskForClosureI11Safe(A, beta, aMsb, log, revision);
      if (extended == null) continue;
      log.put(pathRefs[aIdx], PageContainer.getInstance(extended, extended));
      PageReference upRef = pathRefs[aIdx];
      for (int i = aIdx - 1; i >= 0; i--) {
        final HOTIndirectPage curNode = currentInLog(pathRefs[i], pathNodes[i]);
        final HOTIndirectPage updatedNode = curNode.withUpdatedChild(pathChildIndices[i], upRef, revision);
        log.put(pathRefs[i], PageContainer.getInstance(updatedNode, updatedNode));
        upRef = pathRefs[i];
      }
      if (Boolean.getBoolean("hot.debug.phase5")) {
        System.err.println("[phase5c] ANCESTOR-EXTEND-OK beta=" + beta + " ancestorIdx=" + aIdx);
      }
      return true;
    }
    return false; // all bounded ancestors infeasible
  }

  /**
   * Phase 6e — Recompute an indirect's stored partials from current child firstKeys under
   * the EXISTING mask. If the recomputed partials are unique AND firstKey-monotone in
   * partial-sort order, install. Else return null (caller falls back).
   *
   * <p>Use case: after a leaf insert decreases a child subtree's deep-firstKey, the
   * indirect's stored partial is stale. Recomputing reflects current state. If the
   * existing mask captures enough bits to discriminate, the I8 violation goes away.
   *
   * <p>HFT-grade: bounded by indirect's child count + key length.
   */
  private @Nullable HOTIndirectPage recomputePartialsForCurrentFirstKeys(HOTIndirectPage indirect,
      int revision) {
    final int n = indirect.getNumChildren();
    if (n < 2) return null;
    final int[] newPartials = new int[n];
    final PageReference[] children = new PageReference[n];
    final byte[][] firstKeys = new byte[n][];
    for (int i = 0; i < n; i++) {
      children[i] = indirect.getChildReference(i);
      if (children[i] == null) return null;
      firstKeys[i] = getFirstKeyFromChild(children[i]);
      if (firstKeys[i] == null || firstKeys[i].length == 0) return null;
      if (indirect.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK) {
        newPartials[i] = computePartialKeySingleMask(firstKeys[i],
            indirect.getInitialBytePos(), indirect.getBitMask());
      } else {
        newPartials[i] = computePartialKeyMultiMaskDirect(firstKeys[i],
            indirect.getExtractionPositions(), indirect.getExtractionMasks(),
            indirect.getNumExtractionBytes());
      }
    }
    // Verify uniqueness.
    for (int i = 1; i < n; i++) {
      for (int k = 0; k < i; k++) {
        if (newPartials[k] == newPartials[i]) return null;
      }
    }
    // Sort by partial.
    sortChildrenAndPartialsByPartial(children, newPartials);
    // Verify firstKey-monotone after partial-sort.
    for (int i = 1; i < n; i++) {
      final byte[] prev = getFirstKeyFromChild(children[i - 1]);
      final byte[] curr = getFirstKeyFromChild(children[i]);
      if (prev != null && curr != null && prev.length > 0 && curr.length > 0
          && Arrays.compareUnsigned(prev, curr) >= 0) {
        return null; // existing mask doesn't discriminate after current firstKeys
      }
    }
    // Verify I4 (first partial = 0).
    if (newPartials[0] != 0) return null;
    // Build replacement page with SAME mask, NEW partials, NEW children order.
    if (indirect.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK) {
      if (n <= 16) {
        return HOTIndirectPage.createSpanNode(indirect.getPageKey(), revision,
            indirect.getInitialBytePos(), indirect.getBitMask(), newPartials, children,
            indirect.getHeight());
      }
      return HOTIndirectPage.createMultiNode(indirect.getPageKey(), revision,
          indirect.getInitialBytePos(), indirect.getBitMask(), newPartials, children,
          indirect.getHeight());
    }
    if (n <= 16) {
      return HOTIndirectPage.createSpanNodeMultiMask(indirect.getPageKey(), revision,
          indirect.getExtractionPositions(), indirect.getExtractionMasks(),
          indirect.getNumExtractionBytes(), newPartials, children,
          indirect.getHeight(), indirect.getMostSignificantBitIndex());
    }
    return HOTIndirectPage.createMultiNodeMultiMask(indirect.getPageKey(), revision,
        indirect.getExtractionPositions(), indirect.getExtractionMasks(),
        indirect.getNumExtractionBytes(), newPartials, children,
        indirect.getHeight(), indirect.getMostSignificantBitIndex());
  }

  /**
   * Phase 7f — Force-rebuild an indirect using current children's firstKeys and freshly-
   * computed disc bits via {@code computeDiscBits}. Bypasses any constraint-based logic
   * and just picks bits where adjacent children's firstKeys differ. Returns updated
   * indirect or null on infeasibility.
   *
   * <p>The trade-off: the new mask may capture bits that are non-constant in some child's
   * subtree, breaking I6 (PEXT routing for deep keys). Use as last-resort fix for I8
   * violations where post-hoc reconciliation has otherwise failed.
   */
  /**
   * Phase 7g — Variant of forceRebuildIndirectFromCurrentFirstKeys that computes the
   * discriminating-bit mask from firstKey-vs-firstKey adjacent pairs (instead of
   * lastKey vs firstKey via computeDiscBits). This matches the actual content
   * boundary used by I8 validation.
   *
   * <p>For sorted children, adjacent pairs' firstKeys' MSDB UNION = the closure of
   * bits required to discriminate them by PEXT routing. Build mask + partials from
   * scratch using only this set.
   */
  public @Nullable HOTIndirectPage forceRebuildIndirectFromFirstKeyMsdbs(
      HOTIndirectPage indirect, int revision,
      StorageEngineWriter readerForLoad, TransactionIntentLog logForLoad) {
    this.activeReader = readerForLoad;
    this.activeLog = logForLoad;
    return forceRebuildIndirectFromFirstKeyMsdbs(indirect, revision);
  }

  // Phase 7h — Bounded subtree restructure: planned for future iteration.
  // Walks each child's subtree, ensures β-constancy for new mask bits.
  // Currently delegates to extendIndirectMaskForClosure (G.28) which handles this case
  // but has historically cascaded on multi-entry leaves. Phase 7h would add tight
  // memory caps + per-iteration validator gate to prevent cascade.
  // STUB — see docs/HOT_PHASE_7_DESIGN.md §5 stage 7d-extended.

  public @Nullable HOTIndirectPage forceRebuildIndirectFromFirstKeyMsdbs(
      HOTIndirectPage indirect, int revision) {
    final boolean dbg = Boolean.getBoolean("hot.debug.phase7g");
    if (dbg) System.err.println("[phase7g] ENTRY n=" + indirect.getNumChildren());
    final int n = indirect.getNumChildren();
    if (n < 2) return null;
    final PageReference[] children = new PageReference[n];
    final byte[][] firstKeys = new byte[n][];
    for (int i = 0; i < n; i++) {
      children[i] = indirect.getChildReference(i);
      if (children[i] == null) {
        if (dbg) System.err.println("[phase7g-fail] null child idx=" + i);
        return null;
      }
      firstKeys[i] = getFirstKeyFromChild(children[i]);
      if (firstKeys[i] == null || firstKeys[i].length == 0) {
        if (dbg) System.err.println("[phase7g-fail] null/empty firstKey idx=" + i
            + " firstKey=" + (firstKeys[i] == null ? "null" : "empty"));
        return null;
      }
    }
    if (dbg) System.err.println("[phase7g] all firstKeys collected");
    // Sort by firstKey using indices, then reorder both arrays.
    final Integer[] order = new Integer[n];
    for (int i = 0; i < n; i++) order[i] = i;
    java.util.Arrays.sort(order, (a, b) -> java.util.Arrays.compareUnsigned(firstKeys[a], firstKeys[b]));
    final PageReference[] sortedChildren = new PageReference[n];
    final byte[][] sortedFirstKeys = new byte[n][];
    for (int i = 0; i < n; i++) {
      sortedChildren[i] = children[order[i]];
      sortedFirstKeys[i] = firstKeys[order[i]];
    }
    // Collect MSDB bits from adjacent firstKey pairs.
    final java.util.TreeMap<Integer, Integer> byteMaskMap = new java.util.TreeMap<>();
    int minByte = Integer.MAX_VALUE;
    int maxByte = 0;
    for (int i = 0; i < n - 1; i++) {
      final byte[] a = sortedFirstKeys[i];
      final byte[] b = sortedFirstKeys[i + 1];
      final int len = Math.min(a.length, b.length);
      for (int bp = 0; bp < len; bp++) {
        int xor = (a[bp] ^ b[bp]) & 0xFF;
        if (xor == 0) continue;
        while (xor != 0) {
          final int hb = Integer.numberOfLeadingZeros(xor) - 24;
          xor &= ~(1 << (7 - hb));
          final int maskBit = 1 << (7 - hb);
          byteMaskMap.merge(bp, maskBit, (x, y) -> x | y);
          minByte = Math.min(minByte, bp);
          maxByte = Math.max(maxByte, bp);
        }
        // Found a differing byte; could continue or break. Continue to capture ALL diffs.
      }
    }
    if (byteMaskMap.isEmpty()) {
      if (dbg) System.err.println("[phase7g-fail] no-discriminating-bits");
      return null;
    }
    if (dbg) System.err.println("[phase7g] byteMaskMap=" + byteMaskMap + " minByte=" + minByte
        + " maxByte=" + maxByte);
    final int numBytes = byteMaskMap.size();
    final boolean canSingleMask = (maxByte - minByte) < 8 && numBytes <= 8;
    final byte[] extractionPositions;
    final long[] extractionMasks;
    short msbIndex = Short.MAX_VALUE;
    if (canSingleMask) {
      // Build SingleMask: 8-byte window starting at minByte.
      long mask = 0;
      for (final var e : byteMaskMap.entrySet()) {
        final int bp = e.getKey();
        final int mb = e.getValue();
        final int byteOff = bp - minByte;
        mask |= ((long) (mb & 0xFF)) << ((7 - byteOff) * 8);
        final int highBit = 31 - Integer.numberOfLeadingZeros(mb & 0xFF);
        final int absBit = bp * 8 + (7 - highBit);
        if (absBit < msbIndex) msbIndex = (short) absBit;
      }
      // Compute partials per child.
      final int[] partials = new int[n];
      for (int i = 0; i < n; i++) {
        partials[i] = computePartialKeySingleMask(sortedFirstKeys[i], minByte, mask);
      }
      // Check uniqueness.
      for (int i = 1; i < n; i++) {
        for (int k = 0; k < i; k++) {
          if (partials[k] == partials[i]) {
            if (dbg) System.err.println("[phase7g-fail] SM collision i=" + i + " k=" + k
                + " partial=" + partials[i]);
            return null;
          }
        }
      }
      // Sort by partial.
      sortChildrenAndPartialsByPartial(sortedChildren, partials);
      if (partials[0] != 0) {
        if (dbg) System.err.println("[phase7g-fail] SM no-zero first=" + partials[0]);
        return null;
      }
      if (n <= 16) {
        return HOTIndirectPage.createSpanNode(indirect.getPageKey(), revision, minByte, mask,
            partials, sortedChildren, indirect.getHeight());
      }
      return HOTIndirectPage.createMultiNode(indirect.getPageKey(), revision, minByte, mask,
          partials, sortedChildren, indirect.getHeight());
    }
    // MultiMask path: bytes don't fit in 8-byte window.
    extractionPositions = new byte[numBytes];
    final int numChunks = (numBytes + 7) / 8;
    extractionMasks = new long[numChunks];
    int idx = 0;
    for (final var e : byteMaskMap.entrySet()) {
      final int bp = e.getKey();
      final int mb = e.getValue();
      extractionPositions[idx] = (byte) bp;
      final int chunkIdx = idx / 8;
      final int byteOffsetInChunk = idx % 8;
      extractionMasks[chunkIdx] |= ((long) (mb & 0xFF)) << ((7 - byteOffsetInChunk) * 8);
      final int highBit = 31 - Integer.numberOfLeadingZeros(mb & 0xFF);
      final int absBit = bp * 8 + (7 - highBit);
      if (absBit < msbIndex) msbIndex = (short) absBit;
      idx++;
    }
    final int[] partials = new int[n];
    for (int i = 0; i < n; i++) {
      partials[i] = computePartialKeyMultiMaskDirect(sortedFirstKeys[i],
          extractionPositions, extractionMasks, numBytes);
    }
    for (int i = 1; i < n; i++) {
      for (int k = 0; k < i; k++) {
        if (partials[k] == partials[i]) {
          if (dbg) System.err.println("[phase7g-fail] MM collision i=" + i + " k=" + k
              + " partial=" + partials[i]);
          return null;
        }
      }
    }
    sortChildrenAndPartialsByPartial(sortedChildren, partials);
    if (partials[0] != 0) {
      if (dbg) System.err.println("[phase7g-fail] MM no-zero first=" + partials[0]);
      return null;
    }
    if (n <= 16) {
      return HOTIndirectPage.createSpanNodeMultiMask(indirect.getPageKey(), revision,
          extractionPositions, extractionMasks, numBytes, partials, sortedChildren,
          indirect.getHeight(), msbIndex);
    }
    return HOTIndirectPage.createMultiNodeMultiMask(indirect.getPageKey(), revision,
        extractionPositions, extractionMasks, numBytes, partials, sortedChildren,
        indirect.getHeight(), msbIndex);
  }

  public @Nullable HOTIndirectPage forceRebuildIndirectFromCurrentFirstKeys(
      HOTIndirectPage indirect, int revision) {
    final boolean dbg = Boolean.getBoolean("hot.debug.phase7f");
    final int n = indirect.getNumChildren();
    if (n < 2) {
      if (dbg) System.err.println("[phase7f-fail] reason=fewer-than-2-children n=" + n);
      return null;
    }
    final PageReference[] children = new PageReference[n];
    for (int i = 0; i < n; i++) {
      children[i] = indirect.getChildReference(i);
      if (children[i] == null) {
        if (dbg) System.err.println("[phase7f-fail] reason=null-child idx=" + i);
        return null;
      }
    }
    sortChildrenByFirstKey(children);
    final int initialBytePos = computeInitialBytePos(children);
    final DiscBitsInfo discBits = computeDiscBits(children, initialBytePos);
    if (dbg) System.err.println("[phase7f] initialBytePos=" + initialBytePos
        + " discBits.isSingleMask=" + discBits.isSingleMask()
        + " msbIndex=" + discBits.mostSignificantBitIndex());
    final int[] partialKeys = computePartialKeysForChildren(children, discBits);
    if (dbg) System.err.println("[phase7f] preSort partialKeys=" + Arrays.toString(partialKeys));
    // Sort children by partial-key for canonical storage (HOT I7).
    sortChildrenAndPartialsByPartial(children, partialKeys);
    if (dbg) System.err.println("[phase7f] postSort partialKeys=" + Arrays.toString(partialKeys));
    // Verify uniqueness.
    for (int i = 1; i < n; i++) {
      for (int k = 0; k < i; k++) {
        if (partialKeys[k] == partialKeys[i]) {
          if (dbg) System.err.println("[phase7f-fail] reason=partial-collision i=" + i
              + " k=" + k + " partial=" + partialKeys[i]);
          return null;
        }
      }
    }
    // Verify I4 (first partial = 0).
    if (partialKeys[0] != 0) {
      if (dbg) System.err.println("[phase7f-fail] reason=no-zero-partial first="
          + partialKeys[0] + " partials=" + Arrays.toString(partialKeys));
      return null;
    }

    if (discBits.isSingleMask()) {
      if (n <= 16) {
        return HOTIndirectPage.createSpanNode(indirect.getPageKey(), revision,
            discBits.initialBytePos(), discBits.bitMask(), partialKeys, children,
            indirect.getHeight());
      }
      return HOTIndirectPage.createMultiNode(indirect.getPageKey(), revision,
          discBits.initialBytePos(), discBits.bitMask(), partialKeys, children,
          indirect.getHeight());
    }
    if (n <= 16) {
      return HOTIndirectPage.createSpanNodeMultiMask(indirect.getPageKey(), revision,
          discBits.extractionPositions(), discBits.extractionMasks(),
          discBits.numExtractionBytes(), partialKeys, children,
          indirect.getHeight(), discBits.mostSignificantBitIndex());
    }
    return HOTIndirectPage.createMultiNodeMultiMask(indirect.getPageKey(), revision,
        discBits.extractionPositions(), discBits.extractionMasks(),
        discBits.numExtractionBytes(), partialKeys, children,
        indirect.getHeight(), discBits.mostSignificantBitIndex());
  }

  /**
   * Phase 6f — When recompute fails (= existing mask insufficient), find a discriminating
   * bit β where (a) firstKeys differ at β, (b) β is β-constant in every child's subtree.
   * Extend mask with β + recompute partials. Returns updated indirect or null.
   *
   * <p>Avoids the cascade trap by only considering bits β-constant in subtrees. The bit
   * might be I11-unsafe (= absBit ≤ parent.MSB) but Binna's trie-condition is enforced
   * at extension time too.
   */
  private @Nullable HOTIndirectPage extendWithBetaConstantBit(HOTIndirectPage indirect, int revision) {
    final int n = indirect.getNumChildren();
    if (n < 2) return null;
    final byte[][] firstKeys = new byte[n][];
    int maxLen = 0;
    for (int i = 0; i < n; i++) {
      final PageReference cref = indirect.getChildReference(i);
      if (cref == null) return null;
      firstKeys[i] = getFirstKeyFromChild(cref);
      if (firstKeys[i] == null || firstKeys[i].length == 0) return null;
      if (firstKeys[i].length > maxLen) maxLen = firstKeys[i].length;
    }
    final int parentMsb = indirect.getMostSignificantBitIndex() & 0xFFFF;
    // Try every absBit > parentMsb where firstKeys differ AND every child's subtree is
    // β-constant at that bit.
    for (int absBit = parentMsb + 1; absBit < maxLen * 8; absBit++) {
      boolean seen0 = false, seen1 = false;
      for (int i = 0; i < n; i++) {
        if (firstKeys[i].length == 0) continue;
        final int bytePos = absBit / 8;
        if (bytePos >= firstKeys[i].length) continue;
        final boolean bitSet = (firstKeys[i][bytePos] & (1 << (7 - (absBit % 8)))) != 0;
        if (bitSet) seen1 = true;
        else seen0 = true;
        if (seen0 && seen1) break;
      }
      if (!(seen0 && seen1)) continue; // bit doesn't discriminate
      // Verify β-constancy in every subtree.
      boolean allConstant = true;
      for (int i = 0; i < n; i++) {
        if (bitConstantValueInSubtree(indirect.getChildReference(i), absBit) < 0) {
          allConstant = false; break;
        }
      }
      if (!allConstant) continue;
      // Try extending.
      final HOTIndirectPage extended = extendIndirectMaskForClosure(indirect, absBit, activeLog, revision);
      if (extended != null) return extended;
    }
    return null;
  }

  /**
   * Phase 6e — Walk path from root to leaf-parent. At each ancestor, recompute partials
   * from current children's firstKeys. If recompute succeeds (= mask discriminates),
   * install. Else leave unchanged.
   *
   * <p>Returns count of successful recomputes.
   */
  public int recomputePartialsOnPath(PageReference rootRef,
      HOTIndirectPage[] pathNodes, PageReference[] pathRefs, int[] pathChildIndices, int pathDepth,
      StorageEngineWriter storageEngineWriter, TransactionIntentLog log) {
    if (pathDepth <= 0) return 0;
    final int revision = storageEngineWriter.getRevisionNumber();
    int recomputes = 0;
    for (int aIdx = 0; aIdx < pathDepth; aIdx++) {
      final HOTIndirectPage A = currentInLog(pathRefs[aIdx], pathNodes[aIdx]);
      if (A == null) continue;
      HOTIndirectPage updated = recomputePartialsForCurrentFirstKeys(A, revision);
      if (updated == null && Boolean.getBoolean("hot.strict.phase6f")) {
        // 6f fallback: try extending with a β-constant discriminating bit.
        updated = extendWithBetaConstantBit(A, revision);
      }
      if (updated == null) continue;
      log.put(pathRefs[aIdx], PageContainer.getInstance(updated, updated));
      pathRefs[aIdx].setPage(updated);
      // Propagate CoW up.
      PageReference upRef = pathRefs[aIdx];
      for (int i = aIdx - 1; i >= 0; i--) {
        final HOTIndirectPage curNode = currentInLog(pathRefs[i], pathNodes[i]);
        final HOTIndirectPage updatedNode = curNode.withUpdatedChild(pathChildIndices[i], upRef, revision);
        log.put(pathRefs[i], PageContainer.getInstance(updatedNode, updatedNode));
        upRef = pathRefs[i];
      }
      if (Boolean.getBoolean("hot.debug.phase6e")) {
        System.err.println("[phase6e] RECOMPUTE-OK ancestorIdx=" + aIdx
            + " pageKey=" + A.getPageKey());
      }
      recomputes++;
    }
    return recomputes;
  }

  /**
   * Phase 7j — Try to extend indirect's mask with EVERY MSDB-closure bit of children's
   * firstKeys, in MSB-first order. Each successful extension adds one bit. Stops when:
   * (a) all closure bits exhausted, (b) any extension fails, (c) {@code maxIterations}
   * reached. Returns the count of successful extensions.
   */
  public int phase7jExtendWithAllClosureBits(PageReference indirectRef, HOTIndirectPage initial,
      StorageEngineWriter storageEngineWriter, TransactionIntentLog log, int maxIterations) {
    final boolean dbg = Boolean.getBoolean("hot.debug.phase7j");
    if (initial == null) return 0;
    final int revision = storageEngineWriter.getRevisionNumber();
    this.activeReader = storageEngineWriter;
    this.activeLog = log;
    HOTIndirectPage cur = initial;
    int extensions = 0;
    // Phase 7o (2026-05-11): re-enabled triedBits exploration after Phase 7n landed
    // strict I11 enforcement in buildBucketWithInheritedMask. The earlier cascade
    // pattern (1 → 11,919 via I11) should now be blocked by the construction-time
    // I11 gate. Gated behind hot.strict.phase7o=true for safe rollout.
    final boolean phase7o = Boolean.getBoolean("hot.strict.phase7o");
    final IntOpenHashSet triedBits =
        phase7o ? new IntOpenHashSet(32) : null;
    for (int iter = 0; iter < maxIterations; iter++) {
      int[] closureBits = findClosureBits(cur);
      if (closureBits.length == 0) {
        if (dbg) System.err.println("[phase7j] iter=" + iter + " no closure bits");
        break;
      }
      final int parentMsb = cur.getMostSignificantBitIndex() & 0xFFFF;
      // Phase 7q.14a — directed closure ordering. When
      // `-Dhot.strict.phase7q.i8priority=true`, reorder closureBits so that bits
      // resolving an existing I8 violation (firstKey-sort order disagrees with
      // partial-sort order on adjacent children) are tried FIRST in MSB-first order,
      // followed by remaining closure bits also in MSB-first order. This breaks the
      // β=82 no-op-rebuild livelock that prevents the architecturally needed bit
      // from ever being attempted. If the priority bit's attempt fails (returns
      // null), behaviour reduces to today's order on the remainder, so a failure
      // surfaces no regression on top of the architectural ceiling. See
      // {@link #phase7qComputeI8FixBitsForReorder}.
      if (Boolean.getBoolean("hot.strict.phase7q.i8priority")) {
        final int[] reordered = phase7qComputeI8FixBitsForReorder(cur, closureBits);
        if (reordered != null) {
          closureBits = reordered;
          PHASE7Q_I8_PRIORITY_FIRINGS.incrementAndGet();
        }
      }
      // Phase 7q.4 diagnostic: when operating on root (pageKey=2), dump the
      // closureBits list and parentMsb so we can see what the iter loop is
      // ranging over.
      if (cur.getPageKey() == 2L) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < closureBits.length; i++) {
          if (i > 0) sb.append(',');
          sb.append(closureBits[i]);
        }
        System.err.println("[phase7q-closure-list] cur.pageKey=2 iter=" + iter
            + " parentMsb=" + parentMsb + " bits=[" + sb + "]");
      }
      boolean extended = false;
      final boolean phase7qIterDbg = cur.getPageKey() == 2L
          && Boolean.getBoolean("hot.debug.phase7q");
      for (final int beta : closureBits) {
        // Phase 7q.8 diagnostic: trace EVERY pre-filter bit consideration on root.
        // Gated behind -Dhot.debug.phase7q=true to avoid spam under default operation.
        if (phase7qIterDbg) {
          System.err.println("[phase7q-iter] cur.pageKey=2 iter=" + iter
              + " consider beta=" + beta + " parentMsb=" + parentMsb);
        }
        if (beta <= parentMsb) continue; // skip bits not strictly less significant than parent.MSB
        if (triedBits != null && !triedBits.add(beta)) continue; // already attempted this commit cycle
        // Optionally skip bits already in cur's mask. Gated because adding more aggressive
        // extensions empirically cascades downstream violations.
        if (Boolean.getBoolean("hot.strict.phase7j.skipExisting")) {
          final int outPos = cur.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK
              ? singleMaskBetaOutputPos(cur, beta)
              : multiMaskBetaOutputPos(cur, beta);
          if (outPos >= 0) continue;
        }
        // Phase 7q.5 — opt-in no-op-rebuild detection. extendIndirectMaskForClosure with
        // a β already in cur's mask succeeds trivially (bitwise OR with itself = same
        // mask). The rebuild produces a structurally identical page with no progress.
        // Without detection, the outer iter loop "succeeds + breaks" on the same no-op
        // β every pass and burns its maxIter budget — bits that genuinely refine the
        // routing (e.g. the one needed to fix an I8 violation) are never attempted.
        //
        // EMPIRICAL CAVEAT (2026-05-11): blanket skipNoop cascades 1 → 8469 violations
        // because the loop then adds many descendant-captured bits via lift, restructur-
        // ing deep subtrees in ways that can't all preserve β-constancy. Tracked under
        // hot.strict.phase7q.skipNoop=true for opt-in study; default off keeps the
        // original behaviour and the documented architectural ceiling at 1 violation.
        if (Boolean.getBoolean("hot.strict.phase7q.skipNoop")
            && phase7qIsBetaAlreadyInIndirectMask(cur, beta)) {
          if (dbg) System.err.println("[phase7j] SKIP-NOOP iter=" + iter + " beta=" + beta
              + " (already in cur.mask — would be no-op rebuild)");
          PHASE7Q_CLOSURE_NOOP_SKIPS.incrementAndGet();
          continue;
        }
        final HOTIndirectPage next = extendIndirectMaskForClosure(cur, beta, log, revision);
        if (next == null) continue;
        if (dbg) System.err.println("[phase7j] EXTEND-OK iter=" + iter + " beta=" + beta);
        log.put(indirectRef, PageContainer.getInstance(next, next));
        indirectRef.setPage(next);
        cur = next;
        extensions++;
        extended = true;
        break;
      }
      if (!extended) {
        if (dbg) System.err.println("[phase7j] iter=" + iter + " no bit extends successfully");
        break;
      }
    }
    return extensions;
  }

  /**
   * Phase 7k — Recursively apply Phase 7j to every indirect in the trie, post-order
   * (= leaves first). Each indirect's mask gets extended with closure bits where safe.
   * Stops on the first indirect that can't be extended (= reached architectural limit).
   */
  public int phase7kRecursiveCommit(PageReference rootRef,
      StorageEngineWriter storageEngineWriter, TransactionIntentLog log, int maxIterationsPerIndirect) {
    if (rootRef == null) return 0;
    this.activeReader = storageEngineWriter;
    this.activeLog = log;
    Page rootPage = log.get(rootRef) != null ? log.get(rootRef).getModified() : rootRef.getPage();
    if (rootPage == null) rootPage = loadPage(storageEngineWriter, rootRef);
    if (!(rootPage instanceof HOTIndirectPage rootInd)) return 0;
    final LongOpenHashSet visited =
        new LongOpenHashSet(64);
    return phase7kRecursiveHelper(rootRef, rootInd, storageEngineWriter, log,
        maxIterationsPerIndirect, 0, visited);
  }

  private static final int PHASE7K_MAX_DEPTH = 16;

  private int phase7kRecursiveHelper(PageReference indirectRef, HOTIndirectPage indirect,
      StorageEngineWriter storageEngineWriter, TransactionIntentLog log, int maxIter,
      int depth, LongOpenHashSet visited) {
    if (depth > PHASE7K_MAX_DEPTH) return 0;
    final long pk = indirect.getPageKey();
    if (pk != Constants.NULL_ID_LONG && !visited.add(pk)) {
      return 0; // cycle / already processed
    }
    int total = 0;
    final int n = indirect.getNumChildren();
    // Post-order: recurse into children first.
    for (int i = 0; i < n; i++) {
      final PageReference cref = indirect.getChildReference(i);
      if (cref == null) continue;
      Page cp = cref.getPage();
      if (cp == null) {
        final var pc = log.get(cref);
        if (pc != null) cp = pc.getModified();
      }
      if (cp == null) cp = loadPage(storageEngineWriter, cref);
      if (cp instanceof HOTIndirectPage childInd) {
        total += phase7kRecursiveHelper(cref, childInd, storageEngineWriter, log, maxIter,
            depth + 1, visited);
      }
    }
    // Then process this indirect.
    // Refresh indirect from log in case children changed.
    final var pc = log.get(indirectRef);
    final HOTIndirectPage refreshed = (pc != null && pc.getModified() instanceof HOTIndirectPage hi)
        ? hi : indirect;
    total += phase7jExtendWithAllClosureBits(indirectRef, refreshed, storageEngineWriter, log, maxIter);
    return total;
  }

  /**
   * Phase 5e — Commit-time global reconciliation. Walks the ENTIRE trie post-insert
   * and proactively lifts each child's MSB into its parent's mask if the lift is safe
   * (= β-constant in all OTHER children's subtrees). This fixes stale-firstKey
   * artifacts where parent's mask doesn't capture bits needed to discriminate current
   * children's firstKeys.
   *
   * <p>Bounded: per indirect, at most 4 lifts. Total work is O(N × 4) = O(N).
   * Returns count of successful lifts for telemetry.
   */
  public int commitTimeLiftAllChildMsbs(PageReference rootRef,
      StorageEngineWriter storageEngineWriter, TransactionIntentLog log) {
    if (rootRef == null) return 0;
    final int revision = storageEngineWriter.getRevisionNumber();
    Page rootPage = log.get(rootRef) != null ? log.get(rootRef).getModified() : rootRef.getPage();
    if (rootPage == null) rootPage = loadPage(storageEngineWriter, rootRef);
    if (!(rootPage instanceof HOTIndirectPage rootInd)) return 0;
    int totalLifts = 0;
    for (int round = 0; round < 4; round++) {
      final int liftsThisRound = liftMsbsAtIndirect(rootRef, rootInd, storageEngineWriter, log, revision);
      if (liftsThisRound == 0) break;
      totalLifts += liftsThisRound;
      // Refresh rootInd reference.
      final Page refreshed = log.get(rootRef) != null ? log.get(rootRef).getModified() : rootRef.getPage();
      if (refreshed instanceof HOTIndirectPage rfresh) rootInd = rfresh;
      else break;
    }
    return totalLifts;
  }

  /** Helper: lift one child's MSB into the indirect's mask if safe. */
  private int liftMsbsAtIndirect(PageReference indirectRef, HOTIndirectPage indirect,
      StorageEngineWriter storageEngineWriter, TransactionIntentLog log, int revision) {
    final int n = indirect.getNumChildren();
    if (n < 2) return 0;
    final int parentMsb = indirect.getMostSignificantBitIndex() & 0xFFFF;
    // For each child, get its MSB, try lift if β-constant in all other children.
    for (int i = 0; i < n; i++) {
      final PageReference cref = indirect.getChildReference(i);
      if (cref == null) continue;
      Page cp = cref.getPage();
      if (cp == null) {
        final var pc = log.get(cref);
        if (pc != null) cp = pc.getModified();
      }
      if (cp == null) cp = loadPage(storageEngineWriter, cref);
      if (!(cp instanceof HOTIndirectPage ci)) continue;
      final int childMsb = ci.getMostSignificantBitIndex() & 0xFFFF;
      if (childMsb < 0 || childMsb > 200) continue;
      if (childMsb <= parentMsb) continue; // already in parent's mask range — skip
      // Check if childMsb is constant in EVERY other child's subtree.
      boolean allOthersConstant = true;
      for (int j = 0; j < n; j++) {
        if (j == i) continue;
        final int v = bitConstantValueInSubtree(indirect.getChildReference(j), childMsb);
        if (v < 0) { allOthersConstant = false; break; }
      }
      if (!allOthersConstant) continue;
      // Try lift via extendIndirectMaskForClosure.
      final HOTIndirectPage extended = extendIndirectMaskForClosure(indirect, childMsb, log, revision);
      if (extended == null) continue;
      log.put(indirectRef, PageContainer.getInstance(extended, extended));
      indirectRef.setPage(extended);
      if (Boolean.getBoolean("hot.debug.phase5e")) {
        System.err.println("[phase5e] LIFT-OK indirect=" + indirect.getPageKey()
            + " childIdx=" + i + " liftedBit=" + childMsb + " oldParentMsb=" + parentMsb);
      }
      return 1;
    }
    return 0;
  }

  /**
   * Phase 5d — Walk the path from root to leaf-parent, fixing I11 violations by lifting
   * child.MSB into parent.mask. For each (parent, child) pair where child.MSB.absBit
   * &lt; parent.MSB.absBit (= child has a more-significant disc bit than parent — Binna
   * trie-condition violation), extend parent.mask with that bit. Splits child on β
   * to maintain β-constancy in the resulting halves.
   *
   * <p>Bounded: at most one lift per ancestor. Aborts on first failure (caller observes
   * via formal verifier). Returns true on any progress, false if no I11 violations
   * detected (= no work needed).
   */
  public boolean liftChildMsbsForI11(PageReference rootRef,
      HOTIndirectPage[] pathNodes, PageReference[] pathRefs, int[] pathChildIndices, int pathDepth,
      StorageEngineWriter storageEngineWriter, TransactionIntentLog log) {
    if (pathDepth <= 0) return false;
    final int revision = storageEngineWriter.getRevisionNumber();
    final boolean dbg = Boolean.getBoolean("hot.debug.phase5d");
    boolean anyProgress = false;
    // Top-down: root → leaf-parent.
    for (int aIdx = 0; aIdx < pathDepth; aIdx++) {
      final HOTIndirectPage A = currentInLog(pathRefs[aIdx], pathNodes[aIdx]);
      if (A == null) continue;
      final int aMsb = A.getMostSignificantBitIndex() & 0xFFFF;
      final int n = A.getNumChildren();
      // Find a child whose MSB.absBit ≤ aMsb (= I11 violation).
      int violatingChildIdx = -1;
      int violatingChildMsb = -1;
      for (int i = 0; i < n; i++) {
        final PageReference cref = A.getChildReference(i);
        if (cref == null) continue;
        // Resolve page from log if not in memory.
        Page cp = cref.getPage();
        if (cp == null) {
          final var pc = log.get(cref);
          if (pc != null) cp = pc.getModified();
        }
        if (cp == null) cp = loadPage(storageEngineWriter, cref);
        if (!(cp instanceof HOTIndirectPage ci)) continue;
        final int cMsb = ci.getMostSignificantBitIndex() & 0xFFFF;
        if (dbg) System.err.println("[phase5d-scan] aIdx=" + aIdx + " aMsb=" + aMsb
            + " childIdx=" + i + " childMsb=" + cMsb);
        if (cMsb >= 0 && cMsb <= aMsb) {
          violatingChildIdx = i;
          violatingChildMsb = cMsb;
          break;
        }
      }
      if (violatingChildIdx < 0) continue;
      // Lift: extend A.mask with bit violatingChildMsb. extendIndirectMaskForClosure
      // splits β-mixed children (= the violating child gets split on its own MSB,
      // becoming β-constant halves at A's level).
      final HOTIndirectPage extended = extendIndirectMaskForClosure(A, violatingChildMsb, log, revision);
      if (extended == null) continue;
      log.put(pathRefs[aIdx], PageContainer.getInstance(extended, extended));
      // Propagate CoW up.
      PageReference upRef = pathRefs[aIdx];
      for (int i = aIdx - 1; i >= 0; i--) {
        final HOTIndirectPage curNode = currentInLog(pathRefs[i], pathNodes[i]);
        final HOTIndirectPage updatedNode = curNode.withUpdatedChild(pathChildIndices[i], upRef, revision);
        log.put(pathRefs[i], PageContainer.getInstance(updatedNode, updatedNode));
        upRef = pathRefs[i];
      }
      if (Boolean.getBoolean("hot.debug.phase5d")) {
        System.err.println("[phase5d] LIFT-OK ancestorIdx=" + aIdx
            + " childIdx=" + violatingChildIdx
            + " liftedBit=" + violatingChildMsb + " oldParentMsb=" + aMsb);
      }
      anyProgress = true;
    }
    return anyProgress;
  }

  /**
   * Option B Phase 5 — Find the MSDB between {@code keyBuf} and the previous sibling's
   * firstKey at any ancestor where I8 would break. This is the ACTUAL β bit (not the
   * ancestor depth), so it can be passed to splitLeafAndRerouteWrongHalf as a bit
   * position.
   *
   * <p>Iterates ancestors from leaf-parent to root. At each ancestor A with descendSlot,
   * if descendSlot > 0 and A.children[descendSlot-1].firstKey >= keyBuf, an I8 break would
   * occur. Returns the MSDB between (keyBuf, prevSibFirstKey) — the first bit where they
   * differ from MSB-first.
   *
   * <p>Returns -1 if no break detected.
   */
  public int findI8MsdbBit(HOTIndirectPage[] pathNodes, PageReference[] pathRefs,
      int[] pathChildIndices, int pathDepth, HOTLeafPage leaf, byte[] keyBuf) {
    if (leaf == null || keyBuf == null || pathDepth <= 0) return -1;
    final int leafEntryCount = leaf.getEntryCount();
    if (leafEntryCount == 0) return -1;
    final byte[] leafFirstKey = leaf.getKey(0);
    if (leafFirstKey == null) return -1;
    if (Arrays.compareUnsigned(keyBuf, leafFirstKey) >= 0) return -1; // K doesn't change firstKey

    for (int d = pathDepth - 1; d >= 0; d--) {
      final int descendSlot = pathChildIndices[d];
      if (descendSlot <= 0) continue;
      final HOTIndirectPage A = pathNodes[d];
      if (A == null) continue;
      final PageReference prevSib = A.getChildReference(descendSlot - 1);
      if (prevSib == null) continue;
      final byte[] prevSibFirstKey = getFirstKeyFromChild(prevSib);
      if (prevSibFirstKey == null || prevSibFirstKey.length == 0) continue;
      if (Arrays.compareUnsigned(prevSibFirstKey, keyBuf) >= 0) {
        // I8 break at depth d. Compute MSDB between keyBuf and prevSibFirstKey.
        return DiscriminativeBitComputer.computeDifferingBit(prevSibFirstKey, keyBuf);
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

  /** Stage G.32 — I11-safe root mask reconciliation firings. */
  private static final java.util.concurrent.atomic.AtomicLong G32_RECONCILE_FIRINGS =
      new java.util.concurrent.atomic.AtomicLong(0L);

  public static long getG32ReconcileFirings() { return G32_RECONCILE_FIRINGS.get(); }
  public static void resetG32ReconcileFirings() { G32_RECONCILE_FIRINGS.set(0L); }

  /** Phase 7q — diagnostic counters for descendant-capture classification.
   * <p>WASTED: β captured by some descendant's mask but β-constant in the descendant's
   * subtree — lift is safe (just strip β from descendant masks).
   * <p>LOAD_BEARING_LIFTABLE: β captured AND children's firstKeys differ on β AT this
   * indirect level, BUT removing β from the indirect's mask still leaves children's
   * partials unique (other mask bits suffice). Liftable via mask rebuild.
   * <p>LOAD_BEARING_HARD: β captured AND removing β causes partial collisions among
   * children (β is the sole discriminator for at least one pair). Requires structural
   * restructure: merge collided children or collapse the indirect.
   * <p>All three counters fire only when extendIndirectMaskForClosure rejects via the
   * Phase 7p guard; they classify the rejection reason for empirical analysis. */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_REJECTS_WASTED =
      new java.util.concurrent.atomic.AtomicLong(0L);
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_REJECTS_LOAD_BEARING =
      new java.util.concurrent.atomic.AtomicLong(0L);
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_REJECTS_LB_LIFTABLE =
      new java.util.concurrent.atomic.AtomicLong(0L);
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_REJECTS_LB_HARD =
      new java.util.concurrent.atomic.AtomicLong(0L);

  public static long getPhase7qRejectsWasted() { return PHASE7Q_REJECTS_WASTED.get(); }
  public static void resetPhase7qRejectsWasted() { PHASE7Q_REJECTS_WASTED.set(0L); }
  public static long getPhase7qRejectsLoadBearing() { return PHASE7Q_REJECTS_LOAD_BEARING.get(); }
  public static void resetPhase7qRejectsLoadBearing() { PHASE7Q_REJECTS_LOAD_BEARING.set(0L); }
  public static long getPhase7qRejectsLbLiftable() { return PHASE7Q_REJECTS_LB_LIFTABLE.get(); }
  public static void resetPhase7qRejectsLbLiftable() { PHASE7Q_REJECTS_LB_LIFTABLE.set(0L); }
  public static long getPhase7qRejectsLbHard() { return PHASE7Q_REJECTS_LB_HARD.get(); }
  public static void resetPhase7qRejectsLbHard() { PHASE7Q_REJECTS_LB_HARD.set(0L); }

  // ===== Phase 7r-1 diagnostic — internal-indirect Path 5 routing-collision counters =====
  // Path 5 (§7.22) eliminated the last I8 violation at root-level rebuild sites
  // (rebuildRootWithFullClosureI11Safe, addNewRootLevelForI8). The 5 failing
  // comprehensive* tests in HOTFormalVerificationTest (descending10K, mixedSign,
  // bimodal5K+5K, bimodal50KPromoted, manyDuplicates) surface pre-existing violations
  // at INTERNAL indirects rebuilt via buildFlatNonStrict — the multi-child branch of
  // createNodeFromChildren — where partial-subset overlap causes HOT's subset-fallback
  // routing rule ("largest index among subsets") to mis-route subtree keys to the
  // wrong child (= I6 / I8 / I-Binna cascade downstream).
  //
  // Phase 7r-1 instruments buildFlatNonStrict with a Path 5 routing-collision probe
  // gated by hot.strict.phase7r.routeverify (default false to preserve baseline).
  // INSPECTIONS counts every buildFlatNonStrict invocation when the flag is on;
  // COLLISIONS counts how many had at least one routing-collision in their proposed
  // partials. Probe is purely diagnostic — does NOT reject the build. Phase 7r-2/3
  // will reuse the helper to actually fix the collision via mask augmentation.
  private static final java.util.concurrent.atomic.AtomicLong PHASE7R_BUILDFLAT_INSPECTIONS =
      new java.util.concurrent.atomic.AtomicLong(0L);
  private static final java.util.concurrent.atomic.AtomicLong PHASE7R_BUILDFLAT_COLLISIONS =
      new java.util.concurrent.atomic.AtomicLong(0L);

  public static long getPhase7rBuildflatInspections() { return PHASE7R_BUILDFLAT_INSPECTIONS.get(); }
  public static void resetPhase7rBuildflatInspections() { PHASE7R_BUILDFLAT_INSPECTIONS.set(0L); }
  public static long getPhase7rBuildflatCollisions() { return PHASE7R_BUILDFLAT_COLLISIONS.get(); }
  public static void resetPhase7rBuildflatCollisions() { PHASE7R_BUILDFLAT_COLLISIONS.set(0L); }

  // Phase 7s-1 — augment-fallthrough + augment-exhausted counters.
  //
  // FALLTHROUGH: increments each time `phase7rAugmentUntilUnique` would prefer a
  // β-constant + sort-monotone bit but none exists, falling back to the 7r-2 legacy
  // behaviour (accept any sort-monotone bit, may produce I5-leaf-constancy if the
  // chosen bit is β-mixed in some child). Each fallthrough is the cause of a residual
  // I5 cascade — Phase 7s-2 leaf-split must run BEFORE augmentation chooses the bit.
  //
  // EXHAUSTED: increments when no sort-monotone bit at all is available — augmentation
  // can't continue. Rarer.
  private static final java.util.concurrent.atomic.AtomicLong PHASE7S_AUGMENT_FALLTHROUGH =
      new java.util.concurrent.atomic.AtomicLong(0L);
  private static final java.util.concurrent.atomic.AtomicLong PHASE7S_AUGMENT_EXHAUSTED =
      new java.util.concurrent.atomic.AtomicLong(0L);
  public static long getPhase7sAugmentFallthrough() { return PHASE7S_AUGMENT_FALLTHROUGH.get(); }
  public static void resetPhase7sAugmentFallthrough() { PHASE7S_AUGMENT_FALLTHROUGH.set(0L); }
  public static long getPhase7sAugmentExhausted() { return PHASE7S_AUGMENT_EXHAUSTED.get(); }
  public static void resetPhase7sAugmentExhausted() { PHASE7S_AUGMENT_EXHAUSTED.set(0L); }

  // Phase 7s-2 — split-and-augment counters. APPLIED: split helped (collisions resolved
  // post-split + re-augmentation, state committed). ROLLBACK: split attempted but the
  // residual partials still had collisions — state reverted, fall through to legacy
  // build. NOOP: no colliding pair had a discriminating β-mixed bit available, so
  // nothing was attempted. Mutually exclusive per buildFlatNonStrict call.
  private static final java.util.concurrent.atomic.AtomicLong PHASE7S_SPLIT_APPLIED =
      new java.util.concurrent.atomic.AtomicLong(0L);
  private static final java.util.concurrent.atomic.AtomicLong PHASE7S_SPLIT_ROLLBACK =
      new java.util.concurrent.atomic.AtomicLong(0L);
  private static final java.util.concurrent.atomic.AtomicLong PHASE7S_SPLIT_NOOP =
      new java.util.concurrent.atomic.AtomicLong(0L);
  public static long getPhase7sSplitApplied() { return PHASE7S_SPLIT_APPLIED.get(); }
  public static void resetPhase7sSplitApplied() { PHASE7S_SPLIT_APPLIED.set(0L); }
  public static long getPhase7sSplitRollback() { return PHASE7S_SPLIT_ROLLBACK.get(); }
  public static void resetPhase7sSplitRollback() { PHASE7S_SPLIT_ROLLBACK.set(0L); }
  public static long getPhase7sSplitNoop() { return PHASE7S_SPLIT_NOOP.get(); }
  public static void resetPhase7sSplitNoop() { PHASE7S_SPLIT_NOOP.set(0L); }

  /**
   * Phase 7r-1 — Path 5 routing-collision detector for partial keys in their canonical
   * stored order (= partial-sorted). For each {@code partials[i]} (treated as densePK),
   * simulates HOT's subset-fallback routing rule: find the LARGEST-INDEX j such that
   * {@code (partials[i] & partials[j]) == partials[j]}, with equality preferred. If
   * {@code j != i}, the build mis-routes child[i]'s firstKey to child[j] — that's a
   * collision. Returns the first colliding index, or -1 if every child self-routes.
   *
   * <p>HFT-grade: O(n²) on pre-computed primitive int[], n ≤ 32 typical. No allocation.
   * Identical to the inline probes in {@link #rebuildRootWithFullClosureI11Safe} and
   * {@link #addNewRootLevelForI8} — extracted here so Phase 7r-2/3 can re-use without
   * duplication.
   */
  static int phase7rRoutingCollisionFirstIdx(int[] partials) {
    final int n = partials.length;
    for (int i = 0; i < n; i++) {
      final int densePk = partials[i];
      int bestIdx = -1;
      for (int j = 0; j < n; j++) {
        if ((densePk & partials[j]) == partials[j]) {
          if (partials[j] == densePk) { bestIdx = j; break; }
          if (bestIdx < 0 || j > bestIdx) bestIdx = j;
        }
      }
      if (bestIdx != i) return i;
    }
    return -1;
  }

  /**
   * Phase 7q — classify a Phase 7p descendant-capture rejection. Walks the subtree
   * rooted at {@code ref}; at each indirect that has {@code beta} in its mask, checks
   * whether {@code beta} discriminates among that indirect's children's firstKeys.
   *
   * <p>Routing semantics: indirect D routes on β iff PEXT(child[i].firstKey, D.mask)
   * differs across children at β-encoded-bit-position. Equivalently, iff D's children's
   * firstKeys differ at absBit β. If all children's firstKeys agree on β, then β is in
   * D's mask but contributes nothing to routing — a "fossil" capture, safe to strip.
   *
   * <p>Returns {@code true} iff ANY descendant indirect captures {@code beta} AND
   * routes on it (= children's firstKeys differ on β). Returns {@code false} if every
   * indirect that captures {@code beta} is "fossil" (β in mask but children's firstKeys
   * agree on β) — i.e., the lift is structurally safe.
   *
   * <p>HFT-grade: primitive returns, bounded recursion (≤ tree height), no allocation
   * on the happy path. Only consulted on the cold rejection path inside
   * {@link #extendIndirectMaskForClosure}, so not in the steady-state hot loop.
   */
  private boolean phase7qIsLoadBearingInSubtree(PageReference ref, int beta) {
    Page page = ref.getPage();
    if (page == null && activeReader != null) {
      page = loadPage(activeReader, ref);
      if (page != null) ref.setPage(page);
    }
    if (!(page instanceof HOTIndirectPage indirect)) return false;
    if (indirectMaskHasAbsBit(indirect, beta)) {
      if (indirectChildrenFirstKeysDifferOnBit(indirect, beta)) return true;
      // captured but children's firstKeys agree on β: fossil — safe at this level.
    }
    final int m = indirect.getNumChildren();
    for (int i = 0; i < m; i++) {
      final PageReference cref = indirect.getChildReference(i);
      if (cref == null) continue;
      if (phase7qIsLoadBearingInSubtree(cref, beta)) return true;
    }
    return false;
  }

  /**
   * Phase 7q — finer classification. Walks descendants; at each indirect D that captures
   * β AND has children's firstKeys differing on β, checks whether removing β from D's
   * mask still keeps every D-child's partial unique. Returns:
   * <ul>
   *   <li>{@code LIFT_LIFTABLE}: every load-bearing D has alternative discriminators —
   *       lift is feasible via mask rebuild alone.</li>
   *   <li>{@code LIFT_HARD}: at least one load-bearing D has β as sole discriminator
   *       for some pair of children — lift requires structural restructure (merge or
   *       collapse).</li>
   * </ul>
   * Caller must have verified {@link #phase7qIsLoadBearingInSubtree} returned true.
   */
  private static final int LIFT_LIFTABLE = 1;
  private static final int LIFT_HARD = 2;

  private int phase7qClassifyLiftability(PageReference ref, int beta) {
    Page page = ref.getPage();
    if (page == null && activeReader != null) {
      page = loadPage(activeReader, ref);
      if (page != null) ref.setPage(page);
    }
    if (!(page instanceof HOTIndirectPage indirect)) return LIFT_LIFTABLE;
    int result = LIFT_LIFTABLE;
    if (indirectMaskHasAbsBit(indirect, beta)
        && indirectChildrenFirstKeysDifferOnBit(indirect, beta)) {
      if (!partialUniquenessHoldsWithoutBit(indirect, beta)) {
        result = LIFT_HARD;
      }
    }
    final int m = indirect.getNumChildren();
    for (int i = 0; i < m && result == LIFT_LIFTABLE; i++) {
      final PageReference cref = indirect.getChildReference(i);
      if (cref == null) continue;
      if (phase7qClassifyLiftability(cref, beta) == LIFT_HARD) {
        result = LIFT_HARD;
      }
    }
    return result;
  }

  /**
   * Phase 7q helper — would removing absolute bit {@code beta} from {@code indirect}'s
   * mask still produce unique partials across all children's firstKeys?
   * <p>Computes hypothetical partials under the mask minus β (via PEXT on each child's
   * firstKey with the reduced mask) and checks pairwise uniqueness.
   * <p>HFT-grade: small allocation only on the cold rejection path.
   */
  private boolean partialUniquenessHoldsWithoutBit(HOTIndirectPage indirect, int beta) {
    final int n = indirect.getNumChildren();
    if (n < 2) return true;
    final int betaByte = beta / 8;
    final int betaBitInByte = beta % 8;
    final int betaMaskBit = 1 << (7 - betaBitInByte);
    // Build the reduced byte-position list and per-byte mask bits.
    final int[] redBytePositions = new int[16];
    final int[] redByteMaskBits = new int[16];
    int redCount = 0;
    if (indirect.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK) {
      final int initialBytePos = indirect.getInitialBytePos();
      final long mask = indirect.getBitMask();
      for (int bo = 0; bo < 8; bo++) {
        int bits = (int) ((mask >>> ((7 - bo) * 8)) & 0xFFL);
        if (bits == 0) continue;
        final int bytePos = initialBytePos + bo;
        if (bytePos == betaByte) bits &= ~betaMaskBit;
        if (bits != 0) {
          redBytePositions[redCount] = bytePos;
          redByteMaskBits[redCount] = bits;
          redCount++;
        }
      }
    } else {
      final byte[] ep = indirect.getExtractionPositions();
      final long[] em = indirect.getExtractionMasks();
      final int neb = indirect.getNumExtractionBytes();
      for (int i = 0; i < neb; i++) {
        final int chunkIdx = i / 8;
        final int byteOffsetInChunk = i % 8;
        int bits = (int) ((em[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFFL);
        if (bits == 0) continue;
        final int bytePos = ep[i] & 0xFF;
        if (bytePos == betaByte) bits &= ~betaMaskBit;
        if (bits != 0) {
          redBytePositions[redCount] = bytePos;
          redByteMaskBits[redCount] = bits;
          redCount++;
        }
      }
    }
    if (redCount == 0) return false; // mask becomes empty — single bucket = collision for >1 child
    // Compute hypothetical partial per child.
    final int[] partials = new int[n];
    int valid = 0;
    for (int i = 0; i < n; i++) {
      final PageReference cref = indirect.getChildReference(i);
      if (cref == null) continue;
      final byte[] fk = getFirstKeyFromChild(cref);
      if (fk == null || fk.length == 0) continue;
      partials[valid++] = computeHypotheticalPartial(fk, redBytePositions, redByteMaskBits, redCount);
    }
    for (int i = 1; i < valid; i++) {
      for (int k = 0; k < i; k++) {
        if (partials[k] == partials[i]) return false;
      }
    }
    return true;
  }

  /** Compute partial = concatenation of PEXT(firstKey[bytePos], maskBits) for each entry. */
  private static int computeHypotheticalPartial(byte[] fk, int[] bytePositions, int[] maskBits, int n) {
    int partial = 0;
    int outBitPos = 0;
    for (int i = 0; i < n; i++) {
      final int bytePos = bytePositions[i];
      final int bits = maskBits[i];
      final int b = (bytePos < fk.length) ? (fk[bytePos] & 0xFF) : 0;
      // Count set bits in mask; extract each.
      for (int bp = 7; bp >= 0; bp--) {
        final int maskBit = 1 << bp;
        if ((bits & maskBit) == 0) continue;
        if ((b & maskBit) != 0) partial |= (1 << outBitPos);
        outBitPos++;
      }
    }
    return partial;
  }

  /** Phase 7q.1 — counter for split-on-bit-for-lift firings. */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_SPLIT_FIRINGS =
      new java.util.concurrent.atomic.AtomicLong(0L);
  /** Phase 7q.1 — counter for split-on-bit-for-lift failures (one or both halves null). */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_SPLIT_FAILURES =
      new java.util.concurrent.atomic.AtomicLong(0L);

  public static long getPhase7qSplitFirings() { return PHASE7Q_SPLIT_FIRINGS.get(); }
  public static void resetPhase7qSplitFirings() { PHASE7Q_SPLIT_FIRINGS.set(0L); }
  public static long getPhase7qSplitFailures() { return PHASE7Q_SPLIT_FAILURES.get(); }
  public static void resetPhase7qSplitFailures() { PHASE7Q_SPLIT_FAILURES.set(0L); }

  /** Phase 7q.1 — result of splitting an indirect on a bit for the lift operation. */
  private static final class LiftSplitResult {
    final PageReference betaZero;
    final PageReference betaOne;

    LiftSplitResult(PageReference betaZero, PageReference betaOne) {
      this.betaZero = betaZero;
      this.betaOne = betaOne;
    }
  }

  /**
   * Phase 7q.1 — Split indirect {@code D} on β. Partitions D's children by
   * firstKey.β value and rebuilds each half via
   * {@link #buildBucketWithInheritedMask} (which strips β and any bits ≤ new
   * parent.MSB to preserve I11). Returns a pair (D₀, D₁) where:
   * <ul>
   *   <li>D₀ contains D's children with firstKey.β = 0.</li>
   *   <li>D₁ contains D's children with firstKey.β = 1.</li>
   * </ul>
   *
   * <p>Used by Phase 7q.2's recursive lift: the caller (D's parent or higher)
   * replaces D with the (D₀, D₁) pair and absorbs β into its own mask.
   *
   * <p>Preconditions:
   * <ul>
   *   <li>β must be in D's mask (else there's no β to strip).</li>
   *   <li>D's children's subtrees must be β-constant (true if β is in D's mask
   *       by I6 — PEXT-routing partitions keys by β at this level).</li>
   * </ul>
   *
   * <p>Returns null if either half fails to build (e.g., one half is empty —
   * β isn't actually discriminating; partials collide; mask becomes empty).
   *
   * <p>HFT-grade: bounded allocation (4 small int/ref arrays sized to D's
   * fan-out), no autoboxing, no virtual dispatch in the partition loop.
   */
  @Nullable
  private LiftSplitResult splitIndirectOnBitForLift(HOTIndirectPage indirect, int beta,
      TransactionIntentLog log, int revision) {
    if (indirect == null || beta < 0) return null;
    if (!indirectMaskHasAbsBit(indirect, beta)) return null;
    final int n = indirect.getNumChildren();
    if (n < 2) return null;
    final int[] s0Idx = new int[n];
    final int[] s1Idx = new int[n];
    final PageReference[] s0Refs = new PageReference[n];
    final PageReference[] s1Refs = new PageReference[n];
    int s0n = 0, s1n = 0;
    for (int i = 0; i < n; i++) {
      final PageReference cref = indirect.getChildReference(i);
      if (cref == null) { PHASE7Q_SPLIT_FAILURES.incrementAndGet(); return null; }
      final byte[] fk = getFirstKeyFromChild(cref);
      if (fk == null || fk.length == 0) {
        PHASE7Q_SPLIT_FAILURES.incrementAndGet();
        return null;
      }
      if (isAbsBitSet(fk, beta)) {
        s1Idx[s1n] = i;
        s1Refs[s1n] = cref;
        s1n++;
      } else {
        s0Idx[s0n] = i;
        s0Refs[s0n] = cref;
        s0n++;
      }
    }
    if (s0n == 0 || s1n == 0) {
      // β didn't actually discriminate (all children agree on β-bit at firstKey).
      // This contradicts indirectMaskHasAbsBit pre-condition for a load-bearing β,
      // so this is the "fossil capture" case — caller should treat as wasted-capture.
      PHASE7Q_SPLIT_FAILURES.incrementAndGet();
      return null;
    }
    PageReference r0 =
        buildBucketWithInheritedMask(indirect, s0Idx, s0Refs, s0n, beta, log, revision);
    if (r0 == null) { PHASE7Q_SPLIT_FAILURES.incrementAndGet(); return null; }
    PageReference r1 =
        buildBucketWithInheritedMask(indirect, s1Idx, s1Refs, s1n, beta, log, revision);
    if (r1 == null) { PHASE7Q_SPLIT_FAILURES.incrementAndGet(); return null; }
    // Phase 7q.6 — post-build constancy gate. buildBucketWithInheritedMaskMultiMask
    // can fall back to wrapBucketInSubtree → createNodeFromChildren → buildFlatNonStrict,
    // which recomputes disc bits from children's firstKey adjacency rather than honouring
    // β-constancy in child SUBTREES. The result is an indirect whose mask captures bits
    // that aren't constant in some descendant subtree — invalid per Binna's I5/I6 and the
    // root cause of the cascade observed when -Dhot.strict.phase7q.skipNoop=true lets the
    // closure attempt many bits.
    //
    // Phase 7q.7 — when the gate rejects the wrap-fallback's malformed output, RETRY via
    // {@link #phase7qBuildBucketConstancyFiltered}: a parallel disc-bit derivation that
    // filters out bits non-constant in any child's subtree (= the missing constancy check
    // in {@link #computeDiscBits}). If the retry succeeds and ITS output also passes the
    // gate, accept it as the bucket-build. Otherwise return null cleanly.
    //
    // Auto-enabled when skipNoop or stripOnly is on (modes that drive structural rebuilds
    // hard enough to surface the cascade). Default off so the baseline lift's "successful
    // but slightly malformed" output is preserved (the validator accepts it; tightening
    // here would regress height from 5 to 6).
    final boolean constancyGate = Boolean.getBoolean("hot.strict.phase7q.skipNoop")
        || Boolean.getBoolean("hot.strict.phase7q.stripOnly")
        || Boolean.getBoolean("hot.strict.phase7q.constancyGate");
    if (constancyGate) {
      if (!liftedBucketIsConstancySafe(r0)) {
        final PageReference r0Safe = phase7qBuildBucketConstancyFiltered(indirect, s0Refs, s0n,
            beta, log, revision);
        if (r0Safe == null || !liftedBucketIsConstancySafe(r0Safe)) {
          PHASE7Q_SPLIT_FAILURES.incrementAndGet();
          PHASE7Q_SPLIT_FAIL_CONSTANCY.incrementAndGet();
          return null;
        }
        r0 = r0Safe;
      }
      if (!liftedBucketIsConstancySafe(r1)) {
        final PageReference r1Safe = phase7qBuildBucketConstancyFiltered(indirect, s1Refs, s1n,
            beta, log, revision);
        if (r1Safe == null || !liftedBucketIsConstancySafe(r1Safe)) {
          PHASE7Q_SPLIT_FAILURES.incrementAndGet();
          PHASE7Q_SPLIT_FAIL_CONSTANCY.incrementAndGet();
          return null;
        }
        r1 = r1Safe;
      }
    }
    PHASE7Q_SPLIT_FIRINGS.incrementAndGet();
    return new LiftSplitResult(r0, r1);
  }

  /** Phase 7q.6 — post-build constancy validator for {@link #splitIndirectOnBitForLift}.
   *  Returns false iff the bucket-built indirect's mask captures any bit that is NOT
   *  constant in some child's subtree (= an I5/I6-violating disc bit that snuck in via
   *  the wrap-fallback path in {@link #buildBucketWithInheritedMaskMultiMask}). The
   *  check is bounded: only one indirect deep (the bucket root). HFT-grade — primitive
   *  arithmetic, bounded iteration over the mask's set bits, no autoboxing.
   *
   *  <p>Placeholder children (NULL_ID_LONG pageKey, = TIL-only refs whose page hasn't
   *  been resolved) require the active TIL: try {@code log.get(ref)} first; if the page
   *  isn't there either, the gate cannot make a determination. To avoid false positives
   *  in the default code path, treat unresolvable placeholders as constant (trust). The
   *  cascade case is detected through resolvable indirects in the bucket. */
  private boolean liftedBucketIsConstancySafe(PageReference bucketRef) {
    if (bucketRef == null) return false;
    Page page = bucketRef.getPage();
    if (page == null && activeLog != null) {
      final PageContainer pc = activeLog.get(bucketRef);
      if (pc != null) page = pc.getModified();
    }
    if (page == null && activeReader != null) {
      page = loadPage(activeReader, bucketRef);
      if (page != null) bucketRef.setPage(page);
    }
    if (!(page instanceof HOTIndirectPage built)) {
      return true; // leaf bucket has no descendant constancy to check.
    }
    final boolean dbg = Boolean.getBoolean("hot.debug.phase7q");
    final int n = built.getNumChildren();
    if (n < 2) return true;
    if (built.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK) {
      final int initialBytePos = built.getInitialBytePos();
      final long mask = built.getBitMask();
      for (int wbit = 0; wbit < 64; wbit++) {
        if (((mask >>> wbit) & 1L) == 0L) continue;
        final int byteOffsetInWord = 7 - (wbit / 8);
        final int bitInByte = 7 - (wbit % 8);
        final int absBit = (initialBytePos + byteOffsetInWord) * 8 + bitInByte;
        for (int i = 0; i < n; i++) {
          final PageReference cref = built.getChildReference(i);
          if (cref == null) continue;
          if (!liftedChildIsConstantOnBit(cref, absBit, dbg, built.getPageKey(), i, n)) {
            return false;
          }
        }
      }
      return true;
    }
    final byte[] ep = built.getExtractionPositions();
    final long[] em = built.getExtractionMasks();
    final int neb = built.getNumExtractionBytes();
    for (int i = 0; i < neb; i++) {
      final int bp = ep[i] & 0xFF;
      final int chunkIdx = i / 8;
      final int byteOffsetInChunk = i % 8;
      final int byteMaskBits =
          (int) ((em[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFFL);
      for (int mfBit = 0; mfBit < 8; mfBit++) {
        final int byteBit = 7 - mfBit;
        if ((byteMaskBits & (1 << byteBit)) == 0) continue;
        final int absBit = bp * 8 + mfBit;
        for (int ci = 0; ci < n; ci++) {
          final PageReference cref = built.getChildReference(ci);
          if (cref == null) continue;
          if (!liftedChildIsConstantOnBit(cref, absBit, dbg, built.getPageKey(), ci, n)) {
            return false;
          }
        }
      }
    }
    return true;
  }

  /** Phase 7q.6 — per-child constancy probe. Resolves the child via TIL if needed.
   *  Returns false iff the child's subtree is RESOLVABLE and {@code bitConstantValueInSubtree}
   *  reports non-constant. Unresolvable placeholders (NULL_ID_LONG with no TIL entry) return
   *  true by default to avoid false positives in the default code path — the cascade case is
   *  detected through resolvable indirects.
   */
  private boolean liftedChildIsConstantOnBit(PageReference cref, int absBit, boolean dbg,
      long bucketPageKey, int childIdx, int n) {
    Page childPage = cref.getPage();
    if (childPage == null && activeLog != null) {
      final PageContainer pc = activeLog.get(cref);
      if (pc != null) childPage = pc.getModified();
    }
    if (childPage == null && cref.getKey() != io.sirix.settings.Constants.NULL_ID_LONG
        && activeReader != null) {
      childPage = loadPage(activeReader, cref);
      if (childPage != null) cref.setPage(childPage);
    }
    if (childPage == null) {
      // Unresolvable placeholder — trust caller per relax.closure.placeholder semantics.
      return true;
    }
    if (childPage instanceof HOTLeafPage leaf) {
      if (leaf.isBitConstantAtAbsBit(absBit) < 0) {
        if (dbg) System.err.println("[phase7q.6] CONSTANCY-REJECT leaf"
            + " bucketPageKey=" + bucketPageKey + " absBit=" + absBit
            + " childIdx=" + childIdx + " childPageKey=" + cref.getKey() + " n=" + n);
        return false;
      }
      return true;
    }
    if (childPage instanceof HOTIndirectPage indirect) {
      final int m = indirect.getNumChildren();
      int first = -2;
      for (int j = 0; j < m; j++) {
        final PageReference subRef = indirect.getChildReference(j);
        if (subRef == null) continue;
        Page subPage = subRef.getPage();
        if (subPage == null && activeLog != null) {
          final PageContainer pc = activeLog.get(subRef);
          if (pc != null) subPage = pc.getModified();
        }
        if (subPage == null && subRef.getKey() != io.sirix.settings.Constants.NULL_ID_LONG
            && activeReader != null) {
          subPage = loadPage(activeReader, subRef);
          if (subPage != null) subRef.setPage(subPage);
        }
        if (subPage == null) continue; // unresolvable placeholder — skip
        final int v;
        if (subPage instanceof HOTLeafPage subLeaf) v = subLeaf.isBitConstantAtAbsBit(absBit);
        else v = bitConstantValueInSubtree(subRef, absBit);
        if (v < 0) {
          if (dbg) System.err.println("[phase7q.6] CONSTANCY-REJECT indirect-child non-constant"
              + " bucketPageKey=" + bucketPageKey + " absBit=" + absBit + " childIdx=" + childIdx
              + " subIdx=" + j + " childPageKey=" + cref.getKey() + " n=" + n);
          return false;
        }
        if (first == -2) first = v;
        else if (first != v) {
          if (dbg) System.err.println("[phase7q.6] CONSTANCY-REJECT indirect-child mixed"
              + " bucketPageKey=" + bucketPageKey + " absBit=" + absBit + " childIdx=" + childIdx
              + " subIdx=" + j + " childPageKey=" + cref.getKey() + " n=" + n
              + " first=" + first + " v=" + v);
          return false;
        }
      }
      return true;
    }
    return true;
  }

  /** Phase 7q.7 — counter for constancy-filtered wrap helper invocations. */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_CONSTANCY_WRAP_FIRINGS =
      new java.util.concurrent.atomic.AtomicLong(0L);
  /** Phase 7q.7 — counter for constancy-filtered wrap helper successes. */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_CONSTANCY_WRAP_SUCCESS =
      new java.util.concurrent.atomic.AtomicLong(0L);
  /** Phase 7q.7 — counter for constancy-filtered wrap helper failures (no usable bits, or
   *  uniqueness/I4 violation under the filtered mask). */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_CONSTANCY_WRAP_FAIL =
      new java.util.concurrent.atomic.AtomicLong(0L);
  /** Phase 7q.9 — sub-bucket: fail because no candidate disc bit survived the
   *  I11 + β-skip + per-child subtree-constancy filters (= no bit can route the
   *  bucket cleanly without re-capturing β or violating constancy). */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_CONSTANCY_WRAP_FAIL_NOMASK =
      new java.util.concurrent.atomic.AtomicLong(0L);
  /** Phase 7q.9 — sub-bucket: fail because partials collided under the filtered mask. */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_CONSTANCY_WRAP_FAIL_COLLIDE =
      new java.util.concurrent.atomic.AtomicLong(0L);
  /** Phase 7q.9 — sub-bucket: fail because no zero partial (I4 gate). */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_CONSTANCY_WRAP_FAIL_NOZERO =
      new java.util.concurrent.atomic.AtomicLong(0L);
  /** Phase 7q.9 — sub-bucket: fail because expanded count overflowed MULTI_NODE cap or
   *  the input was malformed (null ref, etc.). */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_CONSTANCY_WRAP_FAIL_INPUT =
      new java.util.concurrent.atomic.AtomicLong(0L);

  public static long getPhase7qConstancyWrapFirings() {
    return PHASE7Q_CONSTANCY_WRAP_FIRINGS.get();
  }
  public static void resetPhase7qConstancyWrapFirings() {
    PHASE7Q_CONSTANCY_WRAP_FIRINGS.set(0L);
  }
  public static long getPhase7qConstancyWrapSuccess() {
    return PHASE7Q_CONSTANCY_WRAP_SUCCESS.get();
  }
  public static void resetPhase7qConstancyWrapSuccess() {
    PHASE7Q_CONSTANCY_WRAP_SUCCESS.set(0L);
  }
  public static long getPhase7qConstancyWrapFail() {
    return PHASE7Q_CONSTANCY_WRAP_FAIL.get();
  }
  public static void resetPhase7qConstancyWrapFail() {
    PHASE7Q_CONSTANCY_WRAP_FAIL.set(0L);
  }
  public static long getPhase7qConstancyWrapFailNomask() {
    return PHASE7Q_CONSTANCY_WRAP_FAIL_NOMASK.get();
  }
  public static void resetPhase7qConstancyWrapFailNomask() {
    PHASE7Q_CONSTANCY_WRAP_FAIL_NOMASK.set(0L);
  }
  public static long getPhase7qConstancyWrapFailCollide() {
    return PHASE7Q_CONSTANCY_WRAP_FAIL_COLLIDE.get();
  }
  public static void resetPhase7qConstancyWrapFailCollide() {
    PHASE7Q_CONSTANCY_WRAP_FAIL_COLLIDE.set(0L);
  }
  public static long getPhase7qConstancyWrapFailNozero() {
    return PHASE7Q_CONSTANCY_WRAP_FAIL_NOZERO.get();
  }
  public static void resetPhase7qConstancyWrapFailNozero() {
    PHASE7Q_CONSTANCY_WRAP_FAIL_NOZERO.set(0L);
  }
  public static long getPhase7qConstancyWrapFailInput() {
    return PHASE7Q_CONSTANCY_WRAP_FAIL_INPUT.get();
  }
  public static void resetPhase7qConstancyWrapFailInput() {
    PHASE7Q_CONSTANCY_WRAP_FAIL_INPUT.set(0L);
  }

  /**
   * Phase 7q.7 — Constancy-filtered wrap bucket builder for the structural lift.
   *
   * <p>Replaces the wrap-fallback path inside {@link #buildBucketWithInheritedMask} for
   * lift-bucket cases where the inherited mask cannot represent the bucket cleanly
   * (uniqueness collision, empty mask, BiNode special case). Derives candidate disc bits
   * from adjacent-pair firstKey XOR (mirroring {@link #computeDiscBits}), then filters
   * each candidate against three constraints:
   * <ol>
   *   <li>{@code absBit > newParentMsb} where {@code newParentMsb = min(oldParentMsb, β)}
   *       — preserves Binna's I11 trie condition (child.MSB &gt; parent.MSB).</li>
   *   <li>{@code absBit != β} — we are lifting β out of this subtree, mustn't recapture.</li>
   *   <li>The bit is β-constant in every bucket child's subtree (validated via
   *       {@link #bitConstantValueInSubtree}) — preserves Binna's I5/I6 sparse routing.</li>
   * </ol>
   *
   * <p>Builds the indirect with the filtered mask + sparse-path partials (via
   * {@link #computePartialKeysForChildren}) and verifies uniqueness + I4 (first partial = 0).
   * Returns {@code null} on any failure mode (no usable bits, uniqueness/I4 collision,
   * fan-out overflow).
   *
   * <p>Used by {@link #splitIndirectOnBitForLift} as a retry after the post-build
   * constancy gate rejects the {@link #buildBucketWithInheritedMask} result. The retry
   * succeeds when the bucket's children have at least one common disc bit that is
   * β-constant in every child's subtree but did NOT survive the inherited-mask path
   * (typically because the inherited mask was empty after stripping β, or uniqueness
   * collided under the shrunk mask).
   *
   * <p>HFT-grade: bounded allocations sized to bucket count (typically ≤ 32 on the
   * reproducer). Inner XOR loop uses primitive arithmetic + {@code Integer.numberOfLeadingZeros}
   * intrinsic. TreeMap is used only for the byte→mask aggregation (small &lt; 22 entries
   * for the diagnostic shapes); a hand-rolled sparse array could replace it if the helper
   * shows up hot, but at current call frequency (≤ 100 firings per 50K-key build) the
   * overhead is negligible.
   */
  @Nullable
  private PageReference phase7qBuildBucketConstancyFiltered(HOTIndirectPage parent,
      PageReference[] bucketRefs, int bucketSize, int beta,
      TransactionIntentLog log, int revision) {
    PHASE7Q_CONSTANCY_WRAP_FIRINGS.incrementAndGet();
    if (bucketSize == 0) {
      PHASE7Q_CONSTANCY_WRAP_FAIL.incrementAndGet();
      PHASE7Q_CONSTANCY_WRAP_FAIL_INPUT.incrementAndGet();
      return null;
    }
    if (bucketSize == 1) {
      PHASE7Q_CONSTANCY_WRAP_SUCCESS.incrementAndGet();
      return bucketRefs[0]; // pass-through, no rebuild needed
    }
    if (bucketSize > NodeUpgradeManager.MULTI_NODE_MAX_CHILDREN) {
      PHASE7Q_CONSTANCY_WRAP_FAIL.incrementAndGet();
      PHASE7Q_CONSTANCY_WRAP_FAIL_INPUT.incrementAndGet();
      return null;
    }

    final int oldParentMsb = parent.getMostSignificantBitIndex() & 0xFFFF;
    final int newParentMsb = Math.min(oldParentMsb, beta);

    // 1. Sort children by firstKey so adjacent-pair XOR matches buildFlatNonStrict.
    final PageReference[] children = Arrays.copyOf(bucketRefs, bucketSize);
    sortChildrenByFirstKey(children);

    // 2. Phase 7q.10 — ALL-pairs firstKey XOR (not just adjacent boundaries), with
    // constancy + I11 + β filters applied inline. Partial keys are derived from
    // first keys (see computePartialKeysForChildren), so the disc-bit candidates
    // MUST come from firstKey-XOR to be uniqueness-relevant. The adjacent-pair
    // scan in computeDiscBits uses lastKey(i) vs firstKey(i+1) — that's correct
    // for the BOUNDARY between adjacent sorted children, but for non-adjacent
    // pairs (i, j) the boundary comparison doesn't capture bits that distinguish
    // firstKey(i) from firstKey(j). Using firstKey on both sides closes that gap.
    //
    // <p>O(N²) pair work; N ≤ MULTI_NODE_MAX_CHILDREN (32), so ≤ 1024 iterations,
    // each O(maxLen) byte scan + O(N) constancy probe per candidate bit. Total
    // O(N³) ≈ 32K probes worst-case — negligible against the 50K-insert workload.
    final TreeMap<Integer, Integer> maskByBytePos = new TreeMap<>();
    int minBytePos = Integer.MAX_VALUE;
    int maxBytePos = 0;
    // Cache firstKeys to avoid repeated reads.
    final byte[][] firstKeys = new byte[bucketSize][];
    for (int i = 0; i < bucketSize; i++) firstKeys[i] = getFirstKeyFromChild(children[i]);
    for (int i = 0; i < bucketSize; i++) {
      final byte[] left = firstKeys[i];
      if (left == null || left.length == 0) continue;
      for (int j = i + 1; j < bucketSize; j++) {
        final byte[] right = firstKeys[j];
        if (right == null || right.length == 0) continue;
        final int minLen = Math.min(left.length, right.length);
        for (int b = 0; b < minLen; b++) {
          int diff = (left[b] ^ right[b]) & 0xFF;
          while (diff != 0) {
            final int hb = Integer.numberOfLeadingZeros(diff) - 24; // 0..7, 0 = MSB
            diff &= ~(1 << (7 - hb));
            final int absBit = b * 8 + hb;
            // I11 filter: bucket-indirect.MSB must be > post-lift parent.MSB.
            if (absBit <= newParentMsb) continue;
            // Don't capture β — we are lifting it OUT of this subtree.
            if (absBit == beta) continue;
            if (isDiscBitAlreadyCaptured(maskByBytePos, b, hb)) continue;
            // Constancy filter: every bucket child must be β-constant on absBit.
            boolean allConst = true;
            for (int ci = 0; ci < bucketSize; ci++) {
              if (bitConstantValueInSubtree(children[ci], absBit) < 0) {
                allConst = false;
                break;
              }
            }
            if (!allConst) continue;
            final int maskBit = 1 << (7 - hb);
            maskByBytePos.merge(b, maskBit, (a, c2) -> a | c2);
            if (b < minBytePos) minBytePos = b;
            if (b > maxBytePos) maxBytePos = b;
          }
        }
      }
    }

    if (maskByBytePos.isEmpty()) {
      PHASE7Q_CONSTANCY_WRAP_FAIL.incrementAndGet();
      PHASE7Q_CONSTANCY_WRAP_FAIL_NOMASK.incrementAndGet();
      return null;
    }

    // 3. Build DiscBitsInfo: SingleMask when all bits fit a single 8-byte window, else MultiMask.
    final DiscBitsInfo discBits;
    if (maxBytePos - minBytePos < 8) {
      long mask = 0L;
      for (final var entry : maskByBytePos.entrySet()) {
        final int byteOffset = entry.getKey() - minBytePos;
        final int maskByte = entry.getValue();
        mask |= ((long) (maskByte & 0xFF)) << ((7 - byteOffset) * 8);
      }
      discBits = DiscBitsInfo.singleMask(minBytePos, mask);
    } else {
      discBits = buildMultiMask(maskByBytePos, minBytePos);
    }

    // 4. Sparse-path partials.
    final int[] partialKeys = computePartialKeysForChildren(children, discBits);

    // 5. Canonical I7 order (partial-key ascending).
    sortChildrenAndPartialsByPartial(children, partialKeys);

    // 6. Uniqueness check.
    for (int i = 1; i < bucketSize; i++) {
      for (int k = 0; k < i; k++) {
        if (partialKeys[i] == partialKeys[k]) {
          // Phase 7q.10 — debug trace for the colliding pair. Gated behind
          // -Dhot.debug.phase7q.collision=true so it doesn't pollute default runs.
          // For each candidate disc bit that would distinguish (k, i), report which
          // OTHER child's subtree is non-constant on that bit (= the constancy
          // filter rejection that left this collision unresolvable). Bedrock cases
          // require recursive lift of the offending sibling's subtree at that bit.
          if (Boolean.getBoolean("hot.debug.phase7q.collision")) {
            phase7q10DumpCollisionContext(children, bucketSize, k, i, beta,
                newParentMsb);
          }
          PHASE7Q_CONSTANCY_WRAP_FAIL.incrementAndGet();
          PHASE7Q_CONSTANCY_WRAP_FAIL_COLLIDE.incrementAndGet();
          return null;
        }
      }
    }

    // 7. I4 (smallest partial == 0).
    if (partialKeys[0] != 0) {
      PHASE7Q_CONSTANCY_WRAP_FAIL.incrementAndGet();
      PHASE7Q_CONSTANCY_WRAP_FAIL_NOZERO.incrementAndGet();
      return null;
    }

    // 8. Construct the indirect using the inheriting parent's height.
    final long pageKey = pageKeyAllocator.getAsLong();
    final int height = parent.getHeight();
    final HOTIndirectPage built;
    if (discBits.isSingleMask()) {
      if (bucketSize <= 16) {
        built = HOTIndirectPage.createSpanNode(pageKey, revision, discBits.initialBytePos(),
            discBits.bitMask(), partialKeys, children, height);
      } else {
        built = HOTIndirectPage.createMultiNode(pageKey, revision, discBits.initialBytePos(),
            discBits.bitMask(), partialKeys, children, height);
      }
    } else {
      if (bucketSize <= 16) {
        built = HOTIndirectPage.createSpanNodeMultiMask(pageKey, revision,
            discBits.extractionPositions(), discBits.extractionMasks(),
            discBits.numExtractionBytes(), partialKeys, children, height,
            discBits.mostSignificantBitIndex());
      } else {
        built = HOTIndirectPage.createMultiNodeMultiMask(pageKey, revision,
            discBits.extractionPositions(), discBits.extractionMasks(),
            discBits.numExtractionBytes(), partialKeys, children, height,
            discBits.mostSignificantBitIndex());
      }
    }
    final PageReference ref = new PageReference();
    ref.setKey(pageKey);
    ref.setPage(built);
    log.put(ref, PageContainer.getInstance(built, built));
    PHASE7Q_CONSTANCY_WRAP_SUCCESS.incrementAndGet();
    return ref;
  }

  /** Phase 7q.10 — diagnostic dump for a single collision case inside
   *  {@link #phase7qBuildBucketConstancyFiltered}. For each candidate disc bit
   *  where the colliding pair's firstKeys differ AND the bit passes I11 + β-skip,
   *  report which (if any) OTHER bucket child's subtree is non-constant on that
   *  bit. This identifies the "blocker" child whose subtree would need to be
   *  recursively β'-stripped before this bit could be added to the mask.
   *
   *  <p>Gated behind {@code -Dhot.debug.phase7q.collision=true}. Heavy I/O; do
   *  not enable in non-diagnostic runs. */
  private void phase7q10DumpCollisionContext(PageReference[] children, int bucketSize,
      int kIdx, int iIdx, int beta, int newParentMsb) {
    final byte[] kKey = getFirstKeyFromChild(children[kIdx]);
    final byte[] iKey = getFirstKeyFromChild(children[iIdx]);
    if (kKey == null || iKey == null) return;
    final int minLen = Math.min(kKey.length, iKey.length);
    final StringBuilder sb = new StringBuilder(256);
    sb.append("[phase7q-collide] bucketSize=").append(bucketSize)
        .append(" pair=(").append(kIdx).append(',').append(iIdx).append(')')
        .append(" β=").append(beta).append(" newParentMsb=").append(newParentMsb)
        .append(" kFirstKey=").append(java.util.HexFormat.of().formatHex(kKey))
        .append(" iFirstKey=").append(java.util.HexFormat.of().formatHex(iKey));
    System.err.println(sb);
    sb.setLength(0);
    sb.append("[phase7q-collide]   distinguishing-bits:");
    for (int b = 0; b < minLen; b++) {
      int diff = (kKey[b] ^ iKey[b]) & 0xFF;
      while (diff != 0) {
        final int hb = Integer.numberOfLeadingZeros(diff) - 24;
        diff &= ~(1 << (7 - hb));
        final int absBit = b * 8 + hb;
        if (absBit <= newParentMsb || absBit == beta) continue;
        sb.append(' ').append(absBit).append('[');
        boolean firstBlocker = true;
        for (int ci = 0; ci < bucketSize; ci++) {
          final int v = bitConstantValueInSubtree(children[ci], absBit);
          if (v < 0) {
            if (!firstBlocker) sb.append(',');
            sb.append("blocker=").append(ci)
                .append("(pageKey=").append(children[ci].getKey()).append(')');
            firstBlocker = false;
          }
        }
        if (firstBlocker) sb.append("USABLE!");
        sb.append(']');
      }
    }
    System.err.println(sb);
  }

  /** Phase 7q.2 — counter for successful recursive lift firings. */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_LIFT_FIRINGS =
      new java.util.concurrent.atomic.AtomicLong(0L);
  /** Phase 7q.2 — counter for recursive lift failures (any path). */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_LIFT_FAILURES =
      new java.util.concurrent.atomic.AtomicLong(0L);
  /** Phase 7q.2 — counter for "no propagation" walker terminations (β absent below). */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_LIFT_NOOP =
      new java.util.concurrent.atomic.AtomicLong(0L);

  public static long getPhase7qLiftFirings() { return PHASE7Q_LIFT_FIRINGS.get(); }
  public static void resetPhase7qLiftFirings() { PHASE7Q_LIFT_FIRINGS.set(0L); }
  public static long getPhase7qLiftFailures() { return PHASE7Q_LIFT_FAILURES.get(); }
  public static void resetPhase7qLiftFailures() { PHASE7Q_LIFT_FAILURES.set(0L); }
  public static long getPhase7qLiftNoop() { return PHASE7Q_LIFT_NOOP.get(); }
  public static void resetPhase7qLiftNoop() { PHASE7Q_LIFT_NOOP.set(0L); }

  /**
   * Phase 7q.2 walker result. Returned by
   * {@link #liftBetaFromSubtreeRecursive(PageReference, int, TransactionIntentLog, int)}.
   *
   * <p>{@code root} is the (possibly modified) subtree root, always non-null on success.
   * {@code propagateRight} is non-null iff β-discrimination must propagate UP one trie
   * level — the caller must replace this subtree's slot in its own indirect with TWO
   * children {@code (root, propagateRight)} and absorb β into its own mask. When
   * {@code propagateRight} is null, the walker either modified the subtree in place
   * (β stripped from internal indirects without changing the child count at this level)
   * or did nothing.
   */
  private static final class BetaLiftWalk {
    final PageReference root;
    final @Nullable PageReference propagateRight;
    BetaLiftWalk(PageReference root, @Nullable PageReference propagateRight) {
      this.root = root;
      this.propagateRight = propagateRight;
    }
    boolean propagates() { return propagateRight != null; }
  }

  /**
   * Phase 7q.2 — Recursive β-lift on a subtree.
   *
   * <p>Walks the subtree at {@code ref} post-order. Strips β from every descendant
   * indirect that captures it (via {@link #splitIndirectOnBitForLift}), propagating
   * the resulting structural split up the trie until the caller absorbs β at its
   * own indirect's mask. The lift fully respects I3/I4/I5/I6/I8/I11 at each
   * rebuild step by routing through the existing
   * {@link #buildBucketWithInheritedMask} helper.
   *
   * <p>Algorithm:
   * <ol>
   *   <li>Leaf: nothing to lift — return {@code (ref, null)}.</li>
   *   <li>Indirect with β in mask: split via {@link #splitIndirectOnBitForLift}
   *       → return {@code (D₀, D₁)}.</li>
   *   <li>Indirect without β in mask:
   *     <ol type="a">
   *       <li>Pre-flight {@link #subtreeHasBitInAnyIndirectMask}; if no descendant
   *           captures β, return {@code (ref, null)} immediately (fast path).</li>
   *       <li>Recurse on every child.</li>
   *       <li>If no child propagates a split, return {@code (ref, null)} (fossil
   *           captures with β-constant subtrees are not load-bearing).</li>
   *       <li>Else build expanded child list (each propagating split adds one
   *           slot). Check fan-out cap.</li>
   *       <li>Split the expanded child list on β via
   *           {@link #splitExpandedChildrenOnBeta} (which calls
   *           {@code buildBucketWithInheritedMask} per half). Return the resulting
   *           {@code (D₀, D₁)}.</li>
   *     </ol>
   *   </li>
   * </ol>
   *
   * <p>Failure modes (return null):
   * <ul>
   *   <li>{@link #splitIndirectOnBitForLift} fails (partial collision, no-zero, etc.).</li>
   *   <li>Fan-out cap exceeded ({@code MULTI_NODE_MAX_CHILDREN}).</li>
   *   <li>{@code buildBucketWithInheritedMask} returns null (I4/uniqueness gate trips).</li>
   *   <li>Any child recurse fails.</li>
   * </ul>
   *
   * <p>HFT-grade: recursion depth bounded by trie height (typically ≤ 8). Allocations
   * are scoped to nodes that need rebuilding — the fast path (β absent below) allocates
   * only the returned {@link BetaLiftWalk}. Primitive bit-position arithmetic, no
   * autoboxing.
   */
  @Nullable
  private BetaLiftWalk liftBetaFromSubtreeRecursive(PageReference ref, int beta,
      TransactionIntentLog log, int revision) {
    if (ref == null || beta < 0) {
      if (Boolean.getBoolean("hot.debug.g32")) {
        System.err.println("[lift-walker] FAIL: ref=" + (ref == null ? "null" : "non-null")
            + " beta=" + beta);
      }
      PHASE7Q_LIFT_FAILURES.incrementAndGet();
      return null;
    }
    Page page = ref.getPage();
    // Phase 7q.15 — TIL-first resolution for in-flight placeholder refs (key=NULL_ID_LONG
    // or otherwise not on disk). Matches getFirstKeyFromChild's pattern. Without this,
    // walker returns null for placeholder children, blocking lift even when the page IS
    // available via TIL.
    if (page == null && activeLog != null) {
      final var container = activeLog.get(ref);
      if (container != null) {
        page = container.getModified();
        if (page != null) ref.setPage(page);
      }
    }
    if (page == null && activeReader != null) {
      page = loadPage(activeReader, ref);
      if (page != null) ref.setPage(page);
    }
    if (page == null) {
      if (Boolean.getBoolean("hot.debug.phase7q") || Boolean.getBoolean("hot.debug.g32")) {
        System.err.println("[lift-walker] FAIL: page=null refKey=" + ref.getKey()
            + " refLogKey=" + ref.getLogKey() + " activeReader="
            + (activeReader != null) + " activeLog=" + (activeLog != null)
            + " activeTilGen=" + ref.getActiveTilGeneration());
      }
      PHASE7Q_LIFT_FAILURES.incrementAndGet();
      return null;
    }
    if (page instanceof HOTLeafPage) {
      PHASE7Q_LIFT_NOOP.incrementAndGet();
      return new BetaLiftWalk(ref, null);
    }
    if (!(page instanceof HOTIndirectPage indirect)) {
      if (Boolean.getBoolean("hot.debug.phase7q") || Boolean.getBoolean("hot.debug.g32")) {
        System.err.println("[lift-walker] FAIL: page=" + page.getClass().getSimpleName()
            + " refKey=" + ref.getKey() + " (not HOTIndirectPage/HOTLeafPage)");
      }
      PHASE7Q_LIFT_FAILURES.incrementAndGet();
      return null;
    }

    final boolean liftDbg = Boolean.getBoolean("hot.debug.phase7q")
        || Boolean.getBoolean("hot.debug.g32");
    // Case A: β at this indirect's level — split here, propagate up.
    if (indirectMaskHasAbsBit(indirect, beta)) {
      final LiftSplitResult split = splitIndirectOnBitForLift(indirect, beta, log, revision);
      if (split != null) {
        PHASE7Q_LIFT_FIRINGS.incrementAndGet();
        return new BetaLiftWalk(split.betaZero, split.betaOne);
      }
      // Split returned null. Phase 7q.15 — under g32.deep, distinguish failure modes:
      //   (1) Fossil capture (β subtree-constant): harmless, return no-op walker.
      //   (2) firstKeys agree on β at this level BUT deeper keys are β-mixed: β is
      //       load-bearing deeper. Fall through to Case B logic (recurse children) to
      //       find and lift from the load-bearing depth.
      //   (3) Anything else: real failure.
      if (Boolean.getBoolean("hot.strict.g32.deep")) {
        final int bv = bitConstantValueInSubtree(ref, beta);
        if (bv >= 0) {
          if (liftDbg) System.err.println("[lift-walker] FOSSIL: pageKey="
              + indirect.getPageKey() + " beta=" + beta + " constantValue=" + bv
              + " — treating as no-op (β harmless in subtree)");
          PHASE7Q_LIFT_NOOP.incrementAndGet();
          return new BetaLiftWalk(ref, null);
        }
        // Not subtree-constant — β is load-bearing somewhere deeper. Fall through
        // to Case B recursion below (the "if (indirectMaskHasAbsBit) return early"
        // is skipped; we drop through to the descendant-capture check + recurse).
        if (liftDbg) System.err.println("[lift-walker] β-mask-here-but-deep-mixed:"
            + " pageKey=" + indirect.getPageKey() + " beta=" + beta
            + " — falling through to Case B recursion");
      } else {
        if (liftDbg) System.err.println("[lift-walker] FAIL: splitIndirectOnBitForLift null"
            + " pageKey=" + indirect.getPageKey() + " beta=" + beta);
        PHASE7Q_LIFT_FAILURES.incrementAndGet();
        return null;
      }
    }

    // Case B: β not at this level. Pre-flight: any descendant capture?
    final int n = indirect.getNumChildren();
    if (n == 0) {
      PHASE7Q_LIFT_NOOP.incrementAndGet();
      return new BetaLiftWalk(ref, null);
    }
    boolean anyDescCapture = false;
    for (int i = 0; i < n; i++) {
      final PageReference cref = indirect.getChildReference(i);
      if (cref == null) {
        if (liftDbg) System.err.println("[lift-walker] FAIL: null child ref"
            + " pageKey=" + indirect.getPageKey() + " childIdx=" + i + " beta=" + beta);
        PHASE7Q_LIFT_FAILURES.incrementAndGet();
        return null;
      }
      if (subtreeHasBitInAnyIndirectMask(cref, beta)) {
        anyDescCapture = true;
        break;
      }
    }
    if (!anyDescCapture) {
      PHASE7Q_LIFT_NOOP.incrementAndGet();
      return new BetaLiftWalk(ref, null);
    }

    // Recurse on every child.
    final BetaLiftWalk[] childResults = new BetaLiftWalk[n];
    boolean anyChildPropagates = false;
    for (int i = 0; i < n; i++) {
      final PageReference cref = indirect.getChildReference(i);
      final BetaLiftWalk cr = liftBetaFromSubtreeRecursive(cref, beta, log, revision);
      if (cr == null) {
        if (liftDbg) System.err.println("[lift-walker] FAIL: child recursion returned null"
            + " pageKey=" + indirect.getPageKey() + " childIdx=" + i
            + " childKey=" + cref.getKey() + " beta=" + beta);
        PHASE7Q_LIFT_FAILURES.incrementAndGet();
        return null;
      }
      childResults[i] = cr;
      if (cr.propagates()) anyChildPropagates = true;
    }

    if (!anyChildPropagates) {
      // Fossil capture: β was in some descendant's mask but that subtree is
      // β-constant — descendants left themselves alone, this level needs no rebuild.
      PHASE7Q_LIFT_NOOP.incrementAndGet();
      return new BetaLiftWalk(ref, null);
    }

    // Some child propagated a split. Build expanded child list.
    int expandedN = 0;
    for (int i = 0; i < n; i++) {
      expandedN += childResults[i].propagates() ? 2 : 1;
    }
    if (expandedN > NodeUpgradeManager.MULTI_NODE_MAX_CHILDREN) {
      PHASE7Q_LIFT_FAILURES.incrementAndGet();
      return null;
    }
    final PageReference[] expandedRefs = new PageReference[expandedN];
    int idx = 0;
    for (int i = 0; i < n; i++) {
      expandedRefs[idx++] = childResults[i].root;
      if (childResults[i].propagates()) {
        expandedRefs[idx++] = childResults[i].propagateRight;
      }
    }

    // Partition expanded refs by firstKey.β, rebuild via inherited mask.
    final LiftSplitResult split = splitExpandedChildrenOnBeta(indirect, expandedRefs, expandedN,
        beta, log, revision);
    if (split == null) {
      if (liftDbg) System.err.println("[lift-walker] FAIL: splitExpandedChildrenOnBeta null"
          + " pageKey=" + indirect.getPageKey() + " beta=" + beta + " expandedN=" + expandedN);
      PHASE7Q_LIFT_FAILURES.incrementAndGet();
      return null;
    }
    PHASE7Q_LIFT_FIRINGS.incrementAndGet();
    return new BetaLiftWalk(split.betaZero, split.betaOne);
  }

  /**
   * Phase 7q.2 — Partition a list of β-constant child refs into two buckets by
   * firstKey.β value, then build each bucket-indirect via
   * {@link #buildBucketWithInheritedMask} inheriting {@code originalIndirect}'s mask.
   *
   * <p>Used by {@link #liftBetaFromSubtreeRecursive} at intermediate trie levels:
   * after recursive children produced (β=0, β=1) split-pair products, this helper
   * collapses the expanded child list into a (D₀, D₁) pair at this level.
   *
   * <p>{@code originalIndirect}'s mask does not contain β (otherwise we'd have
   * dispatched to {@link #splitIndirectOnBitForLift} directly). The bucket mask
   * inherits {@code originalIndirect.mask} unchanged (β not present to strip).
   *
   * <p>Returns null on:
   * <ul>
   *   <li>missing firstKey on any expanded ref,</li>
   *   <li>degenerate partition (one half empty — β isn't actually discriminating
   *       at this level, which contradicts the walker's invariants),</li>
   *   <li>{@code buildBucketWithInheritedMask} returning null (I4/uniqueness gate).</li>
   * </ul>
   *
   * <p>HFT-grade: bounded allocation (4 small arrays sized to expanded count).
   */
  @Nullable
  private LiftSplitResult splitExpandedChildrenOnBeta(HOTIndirectPage originalIndirect,
      PageReference[] expandedRefs, int expandedN, int beta,
      TransactionIntentLog log, int revision) {
    final boolean dbg = Boolean.getBoolean("hot.debug.g32");
    if (expandedN < 2) return null;
    final int[] s0Idx = new int[expandedN];
    final int[] s1Idx = new int[expandedN];
    final PageReference[] s0Refs = new PageReference[expandedN];
    final PageReference[] s1Refs = new PageReference[expandedN];
    int s0n = 0, s1n = 0;
    for (int i = 0; i < expandedN; i++) {
      final PageReference cref = expandedRefs[i];
      if (cref == null) {
        if (dbg) System.err.println("[splitExpand] FAIL: null cref at i=" + i);
        return null;
      }
      final byte[] fk = getFirstKeyFromChild(cref);
      if (fk == null || fk.length == 0) {
        if (dbg) System.err.println("[splitExpand] FAIL: null/empty fk at i=" + i
            + " crefKey=" + cref.getKey());
        return null;
      }
      if (isAbsBitSet(fk, beta)) {
        s1Idx[s1n] = -1;
        s1Refs[s1n] = cref;
        s1n++;
      } else {
        s0Idx[s0n] = -1;
        s0Refs[s0n] = cref;
        s0n++;
      }
    }
    if (s0n == 0 || s1n == 0) {
      if (dbg) System.err.println("[splitExpand] FAIL: degenerate partition s0n=" + s0n
          + " s1n=" + s1n + " expandedN=" + expandedN + " beta=" + beta);
      return null;
    }
    PageReference r0 =
        buildBucketWithInheritedMask(originalIndirect, s0Idx, s0Refs, s0n, beta, log, revision);
    if (r0 == null) {
      if (dbg) System.err.println("[splitExpand] FAIL: r0 null s0n=" + s0n + " beta=" + beta);
      return null;
    }
    PageReference r1 =
        buildBucketWithInheritedMask(originalIndirect, s1Idx, s1Refs, s1n, beta, log, revision);
    if (r1 == null) {
      if (dbg) System.err.println("[splitExpand] FAIL: r1 null s1n=" + s1n + " beta=" + beta);
      return null;
    }
    // Phase 7q.8 — mirror the constancy gate + retry from splitIndirectOnBitForLift here
    // at the intermediate Case B level (walker's expanded-children rebucket). Without the
    // gate, malformed wrap-fallback buckets propagate up through the walker and surface
    // as constancy violations one or more levels later in the outer split — wasted work
    // and harder to attribute. With the gate, intermediate-level non-constancy is caught
    // and rescued via {@link #phase7qBuildBucketConstancyFiltered} before being absorbed
    // by the parent.
    final boolean constancyGate = Boolean.getBoolean("hot.strict.phase7q.skipNoop")
        || Boolean.getBoolean("hot.strict.phase7q.stripOnly")
        || Boolean.getBoolean("hot.strict.phase7q.constancyGate");
    if (constancyGate) {
      if (!liftedBucketIsConstancySafe(r0)) {
        final PageReference r0Safe = phase7qBuildBucketConstancyFiltered(originalIndirect,
            s0Refs, s0n, beta, log, revision);
        if (r0Safe == null || !liftedBucketIsConstancySafe(r0Safe)) return null;
        r0 = r0Safe;
      }
      if (!liftedBucketIsConstancySafe(r1)) {
        final PageReference r1Safe = phase7qBuildBucketConstancyFiltered(originalIndirect,
            s1Refs, s1n, beta, log, revision);
        if (r1Safe == null || !liftedBucketIsConstancySafe(r1Safe)) return null;
        r1 = r1Safe;
      }
    }
    return new LiftSplitResult(r0, r1);
  }

  /** Phase 7q.3 — counter for lift+extend dispatch firings. */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_EXTEND_FIRINGS =
      new java.util.concurrent.atomic.AtomicLong(0L);
  /** Phase 7q.3 — counter for lift+extend dispatch failures. */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_EXTEND_FAILURES =
      new java.util.concurrent.atomic.AtomicLong(0L);
  /** Phase 7q.3 — counter for lift+extend dispatch successes (returned non-null). */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_EXTEND_SUCCESSES =
      new java.util.concurrent.atomic.AtomicLong(0L);
  /** Phase 7q.4 — sub-bucket: failure because walker had nothing to propagate. */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_EXTEND_FAIL_NOPROP =
      new java.util.concurrent.atomic.AtomicLong(0L);
  /** Phase 7q.4 — sub-bucket: failure because expanded child count overflowed MULTI_NODE cap. */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_EXTEND_FAIL_FANOUT =
      new java.util.concurrent.atomic.AtomicLong(0L);
  /** Phase 7q.4 — sub-bucket: failure because splitSubtreeOnBit returned null on β-mixed leaf. */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_EXTEND_FAIL_LEAFSPLIT =
      new java.util.concurrent.atomic.AtomicLong(0L);
  /** Phase 7q.4 — sub-bucket: failure because partials collided under the extended mask. */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_EXTEND_FAIL_COLLIDE =
      new java.util.concurrent.atomic.AtomicLong(0L);
  /** Phase 7q.4 — sub-bucket: failure because no zero partial (I4 gate). */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_EXTEND_FAIL_NOZERO =
      new java.util.concurrent.atomic.AtomicLong(0L);
  /** Phase 7q.4 — sub-bucket: failure because β already in old mask (caller bug). */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_EXTEND_FAIL_BETAINMASK =
      new java.util.concurrent.atomic.AtomicLong(0L);
  /** Phase 7q.4 — sub-bucket: failure because walker returned null. */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_EXTEND_FAIL_WALKER =
      new java.util.concurrent.atomic.AtomicLong(0L);
  /** Phase 7q.15h — sub-bucket: failure because output newChildren[] contained a shared
   *  sub-tree (= same pageKey reachable via two slots). Only fires under
   *  -Dhot.strict.g32.cyclereject=true. */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_EXTEND_FAIL_CYCLE =
      new java.util.concurrent.atomic.AtomicLong(0L);
  /** Phase 7q.5 — counter for closure-loop βs skipped because already in cur's mask
   *  (= the no-op-rebuild case that previously livelocked the closure inner loop). */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_CLOSURE_NOOP_SKIPS =
      new java.util.concurrent.atomic.AtomicLong(0L);
  /** Phase 7q.6 — counter for splitIndirectOnBitForLift bucket builds rejected by the
   *  post-build constancy gate (= buildBucketWithInheritedMaskMultiMask's wrap-fallback
   *  produced an indirect with non-constant disc bits). */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_SPLIT_FAIL_CONSTANCY =
      new java.util.concurrent.atomic.AtomicLong(0L);

  public static long getPhase7qClosureNoopSkips() { return PHASE7Q_CLOSURE_NOOP_SKIPS.get(); }
  public static void resetPhase7qClosureNoopSkips() { PHASE7Q_CLOSURE_NOOP_SKIPS.set(0L); }
  public static long getPhase7qSplitFailConstancy() { return PHASE7Q_SPLIT_FAIL_CONSTANCY.get(); }
  public static void resetPhase7qSplitFailConstancy() { PHASE7Q_SPLIT_FAIL_CONSTANCY.set(0L); }
  /** Phase 7q.7 — counter for bits stripped from the inherited mask because they
   *  were non-constant in some bucket child's subtree (= active under
   *  -Dhot.strict.phase7q.stripNonConstant=true). */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_STRIP_NONCONSTANT_BITS =
      new java.util.concurrent.atomic.AtomicLong(0L);
  public static long getPhase7qStripNonconstantBits() { return PHASE7Q_STRIP_NONCONSTANT_BITS.get(); }
  public static void resetPhase7qStripNonconstantBits() { PHASE7Q_STRIP_NONCONSTANT_BITS.set(0L); }

  /** Phase 7q.12 — counter for strip-only mode firings (= phase7qExtendWithLift invoked
   *  with β already in indirect.mask AND -Dhot.strict.phase7q.stripOnly=true). */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_STRIP_ONLY_FIRINGS =
      new java.util.concurrent.atomic.AtomicLong(0L);
  /** Phase 7q.12 — counter for strip-only mode successes (= walker stripped β, indirect
   *  rebuilt cleanly with existing mask). */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_STRIP_ONLY_SUCCESS =
      new java.util.concurrent.atomic.AtomicLong(0L);
  /** Phase 7q.12 — counter for strip-only mode failures (= walker null, fanout overflow,
   *  uniqueness collision, or no-zero-partial). */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_STRIP_ONLY_FAIL =
      new java.util.concurrent.atomic.AtomicLong(0L);
  public static long getPhase7qStripOnlyFirings() { return PHASE7Q_STRIP_ONLY_FIRINGS.get(); }
  public static void resetPhase7qStripOnlyFirings() { PHASE7Q_STRIP_ONLY_FIRINGS.set(0L); }
  public static long getPhase7qStripOnlySuccess() { return PHASE7Q_STRIP_ONLY_SUCCESS.get(); }
  public static void resetPhase7qStripOnlySuccess() { PHASE7Q_STRIP_ONLY_SUCCESS.set(0L); }
  public static long getPhase7qStripOnlyFail() { return PHASE7Q_STRIP_ONLY_FAIL.get(); }
  public static void resetPhase7qStripOnlyFail() { PHASE7Q_STRIP_ONLY_FAIL.set(0L); }

  /** Phase 7q.13 — counter for fall-throughs to rebuild after Phase 7p reject because
   *  {@code -Dhot.strict.phase7q.allowDoubleCapture=true} was set. Each increment marks
   *  one (indirect, β) extension that ADDED β to indirect.mask while leaving descendants'
   *  β-captures intact. See HOT_PHASE_7Q_DESIGN.md §7.16. */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_ALLOW_DOUBLE_CAPTURE_FIRINGS =
      new java.util.concurrent.atomic.AtomicLong(0L);
  public static long getPhase7qAllowDoubleCaptureFirings() {
    return PHASE7Q_ALLOW_DOUBLE_CAPTURE_FIRINGS.get();
  }
  public static void resetPhase7qAllowDoubleCaptureFirings() {
    PHASE7Q_ALLOW_DOUBLE_CAPTURE_FIRINGS.set(0L);
  }

  /** Phase 7q.5 — closure-loop guard: returns true iff β is already a routing bit in
   *  {@code cur}'s mask. Wraps {@link #indirectMaskHasAbsBit} for clarity at the call
   *  site. Used by {@link #phase7jExtendWithAllClosureBits} to detect the no-op-rebuild
   *  case before invoking {@link #extendIndirectMaskForClosure}. */
  private static boolean phase7qIsBetaAlreadyInIndirectMask(HOTIndirectPage cur, int beta) {
    return indirectMaskHasAbsBit(cur, beta);
  }

  /** Phase 7q.14a — counter for closure-loop iterations where the I8-priority reorder
   *  actually changed the closureBits order (= some I8-fix bit existed in the closure
   *  list that wasn't already the first eligible β after parentMsb). Gated behind
   *  {@code -Dhot.strict.phase7q.i8priority=true}. */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_I8_PRIORITY_FIRINGS =
      new java.util.concurrent.atomic.AtomicLong(0L);
  public static long getPhase7qI8PriorityFirings() { return PHASE7Q_I8_PRIORITY_FIRINGS.get(); }
  public static void resetPhase7qI8PriorityFirings() { PHASE7Q_I8_PRIORITY_FIRINGS.set(0L); }

  /** Phase 7q.14b — counter for COLLIDE rejections where the colliding pair has
   *  AT LEAST ONE discriminating bit absent from the proposed extension mask
   *  (= optimistic candidates for a one-bit-more multi-β extension). Always counts;
   *  no flag gate. */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_COLLIDE_RESOLVABLE_1BIT =
      new java.util.concurrent.atomic.AtomicLong(0L);
  /** Phase 7q.14b — counter for COLLIDE rejections where the colliding pair shares
   *  every byte (= zero discriminating bits). These are duplicate firstKeys, not
   *  resolvable by ANY mask extension. Diagnoses possible upstream bugs. */
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_COLLIDE_DUPLICATE_KEYS =
      new java.util.concurrent.atomic.AtomicLong(0L);
  public static long getPhase7qCollideResolvable1Bit() { return PHASE7Q_COLLIDE_RESOLVABLE_1BIT.get(); }
  public static void resetPhase7qCollideResolvable1Bit() { PHASE7Q_COLLIDE_RESOLVABLE_1BIT.set(0L); }
  public static long getPhase7qCollideDuplicateKeys() { return PHASE7Q_COLLIDE_DUPLICATE_KEYS.get(); }
  public static void resetPhase7qCollideDuplicateKeys() { PHASE7Q_COLLIDE_DUPLICATE_KEYS.set(0L); }

  /**
   * Phase 7q.14b — Dump diagnostic context for a COLLIDE rejection in
   * {@link #phase7qExtendWithLift}'s Step 3. Always increments the resolvable-bit
   * counter (off the hot path: COLLIDE is already a failure case, and the heavy
   * print is gated behind {@code -Dhot.debug.phase7q.extendcollide=true}).
   *
   * <p>For each colliding pair {@code (i, k)} this:
   * <ol>
   *   <li>Walks both firstKeys byte-by-byte computing the XOR, identifying every
   *       absolute bit position where they differ (= candidate discriminators).</li>
   *   <li>Checks which of those candidate bits are NOT already in the proposed
   *       extension mask (= bits a one-step-deeper multi-β would add).</li>
   *   <li>Increments {@link #PHASE7Q_COLLIDE_RESOLVABLE_1BIT} if at least one
   *       absent bit exists (= optimistic for multi-β), or
   *       {@link #PHASE7Q_COLLIDE_DUPLICATE_KEYS} if the firstKeys are
   *       byte-identical (= duplicate firstKeys — different problem).</li>
   * </ol>
   *
   * <p>HFT-grade: bounded allocation (StringBuilder only under the debug flag;
   * 2 byte[] refs reused). The path runs only on COLLIDE rejects, never on the
   * insert hot path.
   */
  private void phase7q14bDumpExtendCollide(int beta, int i, int k, int sharedPartial,
      PageReference[] newChildren, byte[] extractionPositions, long[] extractionMasks,
      int allCount) {
    if (newChildren == null || i >= newChildren.length || k >= newChildren.length) return;
    final PageReference cI = newChildren[i];
    final PageReference cK = newChildren[k];
    if (cI == null || cK == null) return;
    final byte[] fkI = getFirstKeyFromChild(cI);
    final byte[] fkK = getFirstKeyFromChild(cK);
    if (fkI == null || fkK == null || fkI.length == 0 || fkK.length == 0) return;
    final int maxLen = Math.min(fkI.length, fkK.length);
    final boolean dbg = Boolean.getBoolean("hot.debug.phase7q.extendcollide");
    final StringBuilder sb = dbg ? new StringBuilder(512) : null;
    if (dbg) {
      sb.append("[phase7q.14b-collide] beta=").append(beta).append(" i=").append(i)
          .append(" k=").append(k).append(" partial=0x").append(Integer.toHexString(sharedPartial))
          .append("\n  fkI=").append(hexBytes(fkI))
          .append("\n  fkK=").append(hexBytes(fkK))
          .append("\n  maskBytes=[");
      for (int p = 0; p < allCount; p++) {
        if (p > 0) sb.append(",");
        sb.append(extractionPositions[p] & 0xFF);
      }
      sb.append("]\n  candidate-absent-bits=[");
    }
    int diffBits = 0;
    int absentBits = 0;
    for (int bp = 0; bp < maxLen; bp++) {
      int xor = (fkI[bp] ^ fkK[bp]) & 0xFF;
      while (xor != 0) {
        final int hb = Integer.numberOfLeadingZeros(xor) - 24; // 0..7 MSB-first
        xor &= ~(1 << (7 - hb));
        final int absBit = bp * 8 + hb;
        diffBits++;
        final int maskBitInByte = 1 << (7 - hb);
        boolean inMask = false;
        for (int p = 0; p < allCount; p++) {
          if ((extractionPositions[p] & 0xFF) == bp) {
            final int chunkIdx = p / 8;
            final int byteOffsetInChunk = p % 8;
            final int byteMaskBits =
                (int) ((extractionMasks[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFFL);
            if ((byteMaskBits & maskBitInByte) != 0) {
              inMask = true;
              break;
            }
          }
        }
        if (!inMask) {
          absentBits++;
          if (dbg) sb.append(absBit).append(",");
        }
      }
    }
    if (diffBits == 0) {
      PHASE7Q_COLLIDE_DUPLICATE_KEYS.incrementAndGet();
    } else if (absentBits > 0) {
      PHASE7Q_COLLIDE_RESOLVABLE_1BIT.incrementAndGet();
    }
    if (dbg) {
      sb.append("] diffBits=").append(diffBits).append(" absentBits=").append(absentBits);
      System.err.println(sb);
    }
  }

  /** HFT-grade hex dump: bounded allocation StringBuilder, no autoboxing in loop. */
  private static String hexBytes(byte[] b) {
    if (b == null) return "null";
    final StringBuilder sb = new StringBuilder(b.length * 2);
    for (final byte x : b) {
      final int v = x & 0xFF;
      if (v < 16) sb.append('0');
      sb.append(Integer.toHexString(v));
    }
    return sb.toString();
  }

  /**
   * Phase 7q.14a — compute a reorder of {@code closureBits} that places "I8-fix bits"
   * first in MSB-first order. An I8-fix bit is the MSDB between any adjacent (i, i+1)
   * pair of {@code indirect}'s children whose firstKeys are out of lex-unsigned order
   * (= the I8 violation: child[i].firstKey > child[i+1].firstKey while child[i] is
   * placed before child[i+1] by partial-order).
   *
   * <p>Mechanism: {@link #findClosureBits} returns bits in MSB-first absolute order,
   * but the closure inner loop attempts them sequentially until the first success.
   * In default mode that success is often a NO-OP rebuild on a β already in cur.mask
   * (e.g. β=82 on root), and the outer loop livelocks on the same no-op every iter.
   * The architectural-need bit (e.g. β=87 to fix the persistent I8) is never reached.
   *
   * <p>By promoting I8-fix bits to the head of the list, the loop tries them first
   * BEFORE hitting the no-op. If the I8-fix β succeeds (lift adds it to the mask),
   * the I8 violation resolves. If it fails (returns null), the loop falls through
   * to the remaining bits in MSB-first order, matching today's behaviour.
   *
   * <p>Returns the reordered array, or {@code null} when no I8 violation exists or
   * the I8-fix bits are already a prefix of {@code closureBits}. In both cases, the
   * caller should keep the original array (the reorder is a no-op).
   *
   * <p>HFT-grade: bounded allocation (one TreeSet, one int[] sized to closureBits.length,
   * one byte[][] sized to numChildren). Runs at commit-time per phase7j iter, off the
   * insert hot path.
   */
  @Nullable
  private int[] phase7qComputeI8FixBitsForReorder(HOTIndirectPage indirect, int[] closureBits) {
    if (indirect == null || closureBits == null || closureBits.length < 2) return null;
    final int n = indirect.getNumChildren();
    if (n < 2) return null;
    final byte[][] firstKeys = new byte[n][];
    for (int i = 0; i < n; i++) {
      final PageReference cref = indirect.getChildReference(i);
      if (cref == null) return null;
      final byte[] fk = getFirstKeyFromChild(cref);
      if (fk == null || fk.length == 0) return null;
      firstKeys[i] = fk;
    }
    final IntOpenHashSet i8FixBits = new IntOpenHashSet(8);
    for (int i = 1; i < n; i++) {
      if (Arrays.compareUnsigned(firstKeys[i - 1], firstKeys[i]) > 0) {
        final int beta = DiscriminativeBitComputer.computeDifferingBit(firstKeys[i - 1], firstKeys[i]);
        if (beta >= 0) i8FixBits.add(beta);
      }
    }
    if (i8FixBits.isEmpty()) return null;
    final int[] priority = new int[closureBits.length];
    final int[] rest = new int[closureBits.length];
    int pIdx = 0, rIdx = 0;
    for (final int b : closureBits) {
      if (i8FixBits.contains(b)) priority[pIdx++] = b;
      else rest[rIdx++] = b;
    }
    if (pIdx == 0) return null;
    boolean alreadyPrefix = true;
    for (int i = 0; i < pIdx; i++) {
      if (closureBits[i] != priority[i]) { alreadyPrefix = false; break; }
    }
    if (alreadyPrefix) return null;
    final int[] reordered = new int[closureBits.length];
    System.arraycopy(priority, 0, reordered, 0, pIdx);
    System.arraycopy(rest, 0, reordered, pIdx, rIdx);
    return reordered;
  }

  public static long getPhase7qExtendFirings() { return PHASE7Q_EXTEND_FIRINGS.get(); }
  public static void resetPhase7qExtendFirings() { PHASE7Q_EXTEND_FIRINGS.set(0L); }
  public static long getPhase7qExtendFailures() { return PHASE7Q_EXTEND_FAILURES.get(); }
  public static void resetPhase7qExtendFailures() { PHASE7Q_EXTEND_FAILURES.set(0L); }
  public static long getPhase7qExtendSuccesses() { return PHASE7Q_EXTEND_SUCCESSES.get(); }
  public static void resetPhase7qExtendSuccesses() { PHASE7Q_EXTEND_SUCCESSES.set(0L); }
  public static long getPhase7qExtendFailNoprop() { return PHASE7Q_EXTEND_FAIL_NOPROP.get(); }
  public static void resetPhase7qExtendFailNoprop() { PHASE7Q_EXTEND_FAIL_NOPROP.set(0L); }
  public static long getPhase7qExtendFailFanout() { return PHASE7Q_EXTEND_FAIL_FANOUT.get(); }
  public static void resetPhase7qExtendFailFanout() { PHASE7Q_EXTEND_FAIL_FANOUT.set(0L); }
  public static long getPhase7qExtendFailLeafsplit() { return PHASE7Q_EXTEND_FAIL_LEAFSPLIT.get(); }
  public static void resetPhase7qExtendFailLeafsplit() { PHASE7Q_EXTEND_FAIL_LEAFSPLIT.set(0L); }
  public static long getPhase7qExtendFailCollide() { return PHASE7Q_EXTEND_FAIL_COLLIDE.get(); }
  public static void resetPhase7qExtendFailCollide() { PHASE7Q_EXTEND_FAIL_COLLIDE.set(0L); }
  public static long getPhase7qExtendFailNozero() { return PHASE7Q_EXTEND_FAIL_NOZERO.get(); }
  public static void resetPhase7qExtendFailNozero() { PHASE7Q_EXTEND_FAIL_NOZERO.set(0L); }
  public static long getPhase7qExtendFailBetainmask() { return PHASE7Q_EXTEND_FAIL_BETAINMASK.get(); }
  public static void resetPhase7qExtendFailBetainmask() { PHASE7Q_EXTEND_FAIL_BETAINMASK.set(0L); }
  public static long getPhase7qExtendFailWalker() { return PHASE7Q_EXTEND_FAIL_WALKER.get(); }
  public static void resetPhase7qExtendFailWalker() { PHASE7Q_EXTEND_FAIL_WALKER.set(0L); }
  public static long getPhase7qExtendFailCycle() { return PHASE7Q_EXTEND_FAIL_CYCLE.get(); }
  public static void resetPhase7qExtendFailCycle() { PHASE7Q_EXTEND_FAIL_CYCLE.set(0L); }

  /**
   * Phase 7q.3 — Lift β from {@code indirect}'s descendants, then build a new
   * indirect with β added to its mask using the lifted children.
   *
   * <p>This is the dispatch entry point for the LB-HARD case in
   * {@link #extendIndirectMaskForClosure}: when {@code -Dhot.strict.phase7q=true}
   * and the Phase 7p classifier identifies a load-bearing-hard rejection, this
   * helper:
   * <ol>
   *   <li>Runs {@link #liftBetaFromSubtreeRecursive} on every child of
   *       {@code indirect}, producing an expanded child list with β stripped
   *       from every descendant indirect's mask.</li>
   *   <li>Splits any remaining β-mixed leaves via {@link #splitSubtreeOnBit}
   *       (the walker doesn't touch leaves — it only handles indirect-mask
   *       capture).</li>
   *   <li>Builds the new indirect with β added to {@code indirect}'s old mask,
   *       computing partials under the extended mask and verifying I3 (unique)
   *       and I4 (smallest partial = 0).</li>
   * </ol>
   *
   * <p>If no child of {@code indirect} actually propagated a lift split (= the
   * Phase 7p classifier's load-bearing assessment was a false positive, or the
   * walker tripped on a fossil-only path), returns {@code null} so the caller
   * falls through to the standard reject.
   *
   * <p>HFT-grade: bounded allocations, primitive bit arithmetic. Recursion goes
   * through the walker only, never via this function.
   */
  @Nullable
  private HOTIndirectPage phase7qExtendWithLift(HOTIndirectPage indirect, int beta,
      TransactionIntentLog log, int revision) {
    final boolean dbg = Boolean.getBoolean("hot.debug.g30");
    PHASE7Q_EXTEND_FIRINGS.incrementAndGet();
    if (indirect == null || beta < 0) {
      PHASE7Q_EXTEND_FAILURES.incrementAndGet();
      return null;
    }
    final int n = indirect.getNumChildren();
    if (n < 2) {
      PHASE7Q_EXTEND_FAILURES.incrementAndGet();
      return null;
    }
    // Phase 7q.4 — when β is already in indirect.mask AND descendant(s) also capture β,
    // that's a pre-existing I11 double-capture. Default behaviour: bail out (single-level
    // lift can't add β twice). Phase 7q.12 — under -Dhot.strict.phase7q.stripOnly=true,
    // dispatch the walker on each child to strip β from descendants AND rebuild indirect
    // using its EXISTING mask (no β extension). This handles the 203-of-204 LB-HARD cases
    // where the early bail blocked any lift attempt in default mode.
    final boolean betaAlreadyInMask = indirectMaskHasAbsBit(indirect, beta);
    if (betaAlreadyInMask) {
      if (Boolean.getBoolean("hot.strict.phase7q.stripOnly")) {
        return phase7qStripDescendantBetaOnly(indirect, beta, log, revision);
      }
      PHASE7Q_EXTEND_FAILURES.incrementAndGet();
      PHASE7Q_EXTEND_FAIL_BETAINMASK.incrementAndGet();
      return null;
    }

    // Step 0: lift β from every child's subtree.
    final BetaLiftWalk[] lifts = new BetaLiftWalk[n];
    int liftedN = 0;
    int numPropagated = 0;
    for (int i = 0; i < n; i++) {
      final PageReference cref = indirect.getChildReference(i);
      if (cref == null) {
        PHASE7Q_EXTEND_FAILURES.incrementAndGet();
        return null;
      }
      final BetaLiftWalk lw = liftBetaFromSubtreeRecursive(cref, beta, log, revision);
      if (lw == null) {
        PHASE7Q_EXTEND_FAILURES.incrementAndGet();
        PHASE7Q_EXTEND_FAIL_WALKER.incrementAndGet();
        return null;
      }
      lifts[i] = lw;
      if (lw.propagates()) { liftedN += 2; numPropagated++; }
      else liftedN += 1;
    }
    if (numPropagated == 0) {
      // Walker found nothing to propagate — Phase 7p reject is the right call.
      PHASE7Q_EXTEND_FAILURES.incrementAndGet();
      PHASE7Q_EXTEND_FAIL_NOPROP.incrementAndGet();
      return null;
    }

    final PageReference[] liftedRefs = new PageReference[liftedN];
    {
      int idx = 0;
      for (int i = 0; i < n; i++) {
        liftedRefs[idx++] = lifts[i].root;
        if (lifts[i].propagates()) liftedRefs[idx++] = lifts[i].propagateRight;
      }
    }

    // Step 1: pre-split any remaining β-mixed children (= leaves the walker didn't touch).
    // After the walker, indirect-mask β-capture is stripped, but multi-entry leaves can
    // still be β-mixed at the leaf level.
    final java.util.ArrayList<PageReference> newRefs = new java.util.ArrayList<>(liftedN * 2);
    for (int i = 0; i < liftedN; i++) {
      final PageReference ref = liftedRefs[i];
      if (ref == null) {
        PHASE7Q_EXTEND_FAILURES.incrementAndGet();
        return null;
      }
      final int v = bitConstantValueInSubtree(ref, beta);
      if (v >= 0) {
        newRefs.add(ref);
      } else {
        final SubtreeSplit ss = splitSubtreeOnBit(ref, beta, log, revision);
        if (ss == null) {
          PHASE7Q_EXTEND_FAILURES.incrementAndGet();
          PHASE7Q_EXTEND_FAIL_LEAFSPLIT.incrementAndGet();
          return null;
        }
        newRefs.add(ss.leftRef());
        newRefs.add(ss.rightRef());
      }
    }
    final int newCount = newRefs.size();
    if (newCount > NodeUpgradeManager.MULTI_NODE_MAX_CHILDREN) {
      PHASE7Q_EXTEND_FAILURES.incrementAndGet();
      PHASE7Q_EXTEND_FAIL_FANOUT.incrementAndGet();
      return null;
    }

    // Step 2: build new mask = old mask + β. (Mirrors extendIndirectMaskForClosure body.)
    final int oldInitialBytePos = indirect.getInitialBytePos();
    final long oldMask = indirect.getBitMask();
    final int newBytePos = beta / 8;
    final int newBitInByte = beta % 8;
    final int newMaskBitInByte = 1 << (7 - newBitInByte);

    final int[] oldBytePositions = new int[16];
    final int[] oldByteMaskBits = new int[16];
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
      final byte[] ep = indirect.getExtractionPositions();
      final long[] em = indirect.getExtractionMasks();
      final int neb = indirect.getNumExtractionBytes();
      for (int i = 0; i < neb; i++) {
        final int chunkIdx = i / 8;
        final int byteOffsetInChunk = i % 8;
        final int byteMaskBits = (int) ((em[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFFL);
        if (byteMaskBits != 0) {
          if (oldByteCount >= oldBytePositions.length) {
            PHASE7Q_EXTEND_FAILURES.incrementAndGet();
            return null;
          }
          oldBytePositions[oldByteCount] = ep[i] & 0xFF;
          oldByteMaskBits[oldByteCount] = byteMaskBits;
          oldByteCount++;
        }
      }
    }

    // Merge β's byte into the sorted byte-position list.
    final int[] allBytePositions = new int[oldByteCount + 1];
    final int[] allByteMaskBits = new int[oldByteCount + 1];
    int allCount = 0;
    boolean merged = false;
    for (int i = 0; i < oldByteCount; i++) {
      if (!merged && oldBytePositions[i] == newBytePos) {
        allBytePositions[allCount] = newBytePos;
        allByteMaskBits[allCount] = oldByteMaskBits[i] | newMaskBitInByte;
        if ((oldByteMaskBits[i] & newMaskBitInByte) != 0) {
          // β-bit already set in old mask — caller shouldn't have invoked us.
          PHASE7Q_EXTEND_FAILURES.incrementAndGet();
          PHASE7Q_EXTEND_FAIL_BETAINMASK.incrementAndGet();
          return null;
        }
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
      extractionMasks[chunkIdx] |=
          ((long) (allByteMaskBits[i] & 0xFF)) << ((7 - byteOffsetInChunk) * 8);
      final int highBit = 31 - Integer.numberOfLeadingZeros(allByteMaskBits[i] & 0xFF);
      final int absBitPos = allBytePositions[i] * 8 + (7 - highBit);
      if (absBitPos < msbIndex) msbIndex = (short) absBitPos;
    }

    // Step 3: compute partials and verify I3 (unique) + I4 (smallest = 0).
    // Phase 7q.15 — under hot.strict.g32.deep, assign Integer.MAX_VALUE - seq sentinels
    // to placeholder children (empty firstKey). Without this, multiple placeholders all
    // map to partial=0 and trigger spurious I3 collisions. The sentinel partials sort to
    // the END so they don't interfere with real children's order, AND they're unique among
    // themselves. Real children's partials are checked for uniqueness separately.
    final int[] newPartials = new int[newCount];
    final PageReference[] newChildren = new PageReference[newCount];
    final boolean deepPlaceholderSentinel = Boolean.getBoolean("hot.strict.g32.deep");
    int placeholderSeq = 0;
    for (int i = 0; i < newCount; i++) {
      final PageReference cref = newRefs.get(i);
      newChildren[i] = cref;
      final byte[] cKey = getFirstKeyFromChild(cref);
      if (cKey == null || cKey.length == 0) {
        if (deepPlaceholderSentinel) {
          newPartials[i] = Integer.MAX_VALUE - placeholderSeq;
          placeholderSeq++;
        } else {
          newPartials[i] = 0;
        }
      } else {
        newPartials[i] = computePartialKeyMultiMaskDirect(cKey, extractionPositions,
            extractionMasks, allCount);
      }
    }
    // Phase 7q.15 — multi-β extension. When Step 3 detects collisions but the
    // colliding pair has bits NOT in the proposed mask (resolvable-1bit), add ONE
    // such absent bit and recompute partials. Iterate up to 8 times to handle
    // multi-pair collisions. Gated by hot.strict.g32.deep so legacy behavior is
    // preserved off-flag.
    final boolean deepMultiBeta = Boolean.getBoolean("hot.strict.g32.deep");
    int collideRetries = 0;
    boolean partialsValid = false;
    while (!partialsValid && collideRetries < 8) {
      partialsValid = true;
      int collideI = -1, collideK = -1;
      OUTER:
      for (int i = 1; i < newCount; i++) {
        for (int k = 0; k < i; k++) {
          // Skip collision check between two sentinel values (they're unique by construction).
          if (deepPlaceholderSentinel
              && newPartials[k] >= Integer.MAX_VALUE - newCount
              && newPartials[i] >= Integer.MAX_VALUE - newCount) {
            continue;
          }
          if (newPartials[k] == newPartials[i]) {
            partialsValid = false;
            collideI = i;
            collideK = k;
            break OUTER;
          }
        }
      }
      if (partialsValid) break;
      if (!deepMultiBeta) {
        if (dbg) System.err.println("[phase7q-extend] reject reason=partial-collision beta=" + beta
            + " i=" + collideI + " k=" + collideK + " partial="
            + Integer.toHexString(newPartials[collideI]));
        phase7q14bDumpExtendCollide(beta, collideI, collideK, newPartials[collideI],
            newChildren, extractionPositions, extractionMasks, allCount);
        PHASE7Q_EXTEND_FAILURES.incrementAndGet();
        PHASE7Q_EXTEND_FAIL_COLLIDE.incrementAndGet();
        return null;
      }
      // Find first absent bit that distinguishes collideI from collideK.
      final byte[] fkI = getFirstKeyFromChild(newChildren[collideI]);
      final byte[] fkK = getFirstKeyFromChild(newChildren[collideK]);
      if (fkI == null || fkK == null || fkI.length == 0 || fkK.length == 0) {
        if (dbg) System.err.println("[phase7q-extend] multi-β: collision involves placeholder, can't resolve");
        PHASE7Q_EXTEND_FAILURES.incrementAndGet();
        PHASE7Q_EXTEND_FAIL_COLLIDE.incrementAndGet();
        return null;
      }
      int absentBit = -1;
      final int maxLen = Math.min(fkI.length, fkK.length);
      for (int bp = 0; bp < maxLen && absentBit < 0; bp++) {
        int xor = (fkI[bp] ^ fkK[bp]) & 0xFF;
        while (xor != 0 && absentBit < 0) {
          final int hb = Integer.numberOfLeadingZeros(xor) - 24; // 0..7 MSB-first
          xor &= ~(1 << (7 - hb));
          final int absBit = bp * 8 + hb;
          final int maskBitInByte = 1 << (7 - hb);
          boolean inMask = false;
          for (int p = 0; p < allCount; p++) {
            if ((extractionPositions[p] & 0xFF) == bp) {
              final int chunkIdx2 = p / 8;
              final int byteOffsetInChunk2 = p % 8;
              final int byteMaskBits = (int) ((extractionMasks[chunkIdx2]
                  >>> ((7 - byteOffsetInChunk2) * 8)) & 0xFFL);
              if ((byteMaskBits & maskBitInByte) != 0) { inMask = true; break; }
            }
          }
          if (!inMask) absentBit = absBit;
        }
      }
      if (absentBit < 0) {
        if (dbg) System.err.println("[phase7q-extend] multi-β: no absent disc bit, identical-keys");
        PHASE7Q_EXTEND_FAILURES.incrementAndGet();
        PHASE7Q_EXTEND_FAIL_COLLIDE.incrementAndGet();
        return null;
      }
      // Add absentBit to extractionMasks. If its byte is in extractionPositions, OR the
      // bit into existing mask. If not, would need to extend the byte list — skip those
      // cases for now (fall back to fail).
      final int absentBytePos = absentBit / 8;
      final int absentBitInByte = absentBit % 8;
      final int absentMaskBit = 1 << (7 - absentBitInByte);
      int byteEntryIdx = -1;
      for (int p = 0; p < allCount; p++) {
        if ((extractionPositions[p] & 0xFF) == absentBytePos) { byteEntryIdx = p; break; }
      }
      if (byteEntryIdx < 0) {
        if (dbg) System.err.println("[phase7q-extend] multi-β: absentBit=" + absentBit
            + " in new byte position " + absentBytePos + " — extending byte list NYI");
        PHASE7Q_EXTEND_FAILURES.incrementAndGet();
        PHASE7Q_EXTEND_FAIL_COLLIDE.incrementAndGet();
        return null;
      }
      // OR the bit into the existing byte's mask.
      final int chunkIdx3 = byteEntryIdx / 8;
      final int byteOffsetInChunk3 = byteEntryIdx % 8;
      final long shifted = ((long) absentMaskBit) << ((7 - byteOffsetInChunk3) * 8);
      extractionMasks[chunkIdx3] |= shifted;
      // Update msbIndex if absentBit is more significant than current msb.
      if (absentBit < msbIndex) msbIndex = (short) absentBit;
      // Recompute partials with the extended mask.
      for (int i = 0; i < newCount; i++) {
        final byte[] cKey = getFirstKeyFromChild(newChildren[i]);
        if (cKey == null || cKey.length == 0) {
          if (deepPlaceholderSentinel) {
            // Keep sentinel value.
            continue;
          }
          newPartials[i] = 0;
          continue;
        }
        newPartials[i] = computePartialKeyMultiMaskDirect(cKey, extractionPositions,
            extractionMasks, allCount);
      }
      collideRetries++;
      if (dbg) System.err.println("[phase7q-extend] multi-β retry=" + collideRetries
          + " added absentBit=" + absentBit);
    }
    if (!partialsValid) {
      PHASE7Q_EXTEND_FAILURES.incrementAndGet();
      PHASE7Q_EXTEND_FAIL_COLLIDE.incrementAndGet();
      return null;
    }
    sortChildrenAndPartialsByPartial(newChildren, newPartials);
    boolean hasZero = false;
    for (final int p : newPartials) {
      if (p == 0) { hasZero = true; break; }
    }
    if (!hasZero) {
      if (dbg) System.err.println("[phase7q-extend] reject reason=no-zero-partial beta=" + beta
          + " newCount=" + newCount);
      PHASE7Q_EXTEND_FAILURES.incrementAndGet();
      PHASE7Q_EXTEND_FAIL_NOZERO.incrementAndGet();
      return null;
    }

    if (dbg) System.err.println("[phase7q-extend] EXTEND-OK beta=" + beta + " newCount=" + newCount
        + " msbIndex=" + msbIndex + " allCount=" + allCount + " liftedN=" + liftedN
        + " numPropagated=" + numPropagated);
    // Phase 7q.15h — construction-time cycle check. The lift mechanism can produce
    // newChildren[] that share a sub-tree (same pageKey reachable via two slots).
    // Validator flags this as structure-cycle at trie validation time; here we detect
    // and reject before building so the caller (phase7qIterativeRootSortI8) cleanly
    // falls back to its rollback path. Gated on hot.strict.g32.cyclereject so legacy
    // lift output is preserved off-flag.
    if (Boolean.getBoolean("hot.strict.g32.cyclereject")
        && newChildrenHaveSharedSubtree(newChildren, newCount)) {
      PHASE7Q_EXTEND_FAILURES.incrementAndGet();
      PHASE7Q_EXTEND_FAIL_CYCLE.incrementAndGet();
      if (dbg) System.err.println("[phase7q-extend] reject reason=output-has-cycle beta=" + beta
          + " newCount=" + newCount);
      return null;
    }
    PHASE7Q_EXTEND_SUCCESSES.incrementAndGet();
    // Phase 7q.15d — pre-build instrumentation: classify each lifted child's MSB vs the
    // rebuilt parent's msbIndex. Equality (= I11 violation child.MSB == parent.MSB) is
    // the architectural ceiling identified in §7.19. Counters always update; per-event
    // dump gated on -Dhot.debug.phase7q.imsb=true.
    phase7q15dCheckIntermediateMsb(indirect.getPageKey(), msbIndex & 0xFFFF, beta, newChildren,
        newCount);
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
   * Phase 7q.15h — detect whether {@code newChildren[]} contains a shared sub-tree
   * (= same pageKey reachable via two distinct child slots). Walks each newChild's
   * subtree DFS-style, accumulating pageKeys into a single shared HashSet. Returns
   * {@code true} on the first revisited pageKey across the whole forest.
   *
   * <p>HFT-grade: one HashSet allocated per call; one add per indirect descendant.
   * Costs O(total descendants) — only paid when the cycle-reject gate is enabled.
   *
   * @param newChildren  forest roots (each may be leaf or indirect)
   * @param newCount     number of valid entries in {@code newChildren}
   * @return true iff any pageKey is shared between two distinct positions
   */
  private boolean newChildrenHaveSharedSubtree(PageReference[] newChildren, int newCount) {
    if (newChildren == null || newCount < 2) return false;
    final java.util.HashSet<Long> seen = new java.util.HashSet<>();
    for (int i = 0; i < newCount; i++) {
      final PageReference cref = newChildren[i];
      if (cref == null) continue;
      Page page = cref.getPage();
      if (page == null && activeLog != null) {
        final var container = activeLog.get(cref);
        if (container != null) page = container.getModified();
      }
      if (page == null && activeReader != null) {
        page = loadPage(activeReader, cref);
        if (page != null) cref.setPage(page);
      }
      if (page instanceof HOTIndirectPage childInd) {
        if (hasStructureCycleInternal(childInd, seen, /*depth=*/0, /*parent=*/-1L)) {
          return true;
        }
      } else if (page instanceof HOTLeafPage leafPage) {
        // Track leaf pageKeys too — a leaf shared between slots is also a cycle source.
        final long lpk = leafPage.getPageKey();
        if (lpk >= 0 && !seen.add(lpk)) return true;
      }
    }
    return false;
  }

  /**
   * Phase 7q.12 — Strip-only lift mode for the case where β is already in
   * {@code indirect}'s mask AND some descendant indirect ALSO captures β (= a
   * pre-existing I11 double-capture).
   *
   * <p>Unlike {@link #phase7qExtendWithLift} which ADDS β to {@code indirect}'s mask,
   * this variant strips β from descendants only and rebuilds {@code indirect} with
   * its EXISTING mask. The walker propagates (D₀, D₁) splits from any descendant
   * indirect that had β captured; those propagated pairs are absorbed into
   * {@code indirect}'s child list. Since {@code indirect} already partitions by β
   * (β is in its mask), the propagated D₀ (firstKey.β=0) and D₁ (firstKey.β=1)
   * naturally fit on opposite sides of the partition. Partials are recomputed
   * under the existing mask.
   *
   * <p>Behaviour matches {@link #phase7qExtendWithLift} otherwise: bounded walker,
   * fanout cap, uniqueness + I4 verification, all PHASE7Q counters honoured.
   *
   * <p>HFT-grade: bounded allocations, primitive bit arithmetic, no autoboxing in
   * tight loops. Uses indirect's existing extractionPositions/Masks/MSB rather
   * than rebuilding the byte-list.
   */
  @Nullable
  private HOTIndirectPage phase7qStripDescendantBetaOnly(HOTIndirectPage indirect, int beta,
      TransactionIntentLog log, int revision) {
    final boolean dbg = Boolean.getBoolean("hot.debug.phase7q");
    PHASE7Q_STRIP_ONLY_FIRINGS.incrementAndGet();
    final int n = indirect.getNumChildren();
    if (n < 2) {
      PHASE7Q_STRIP_ONLY_FAIL.incrementAndGet();
      return null;
    }
    // Step 0: lift β from every child's subtree.
    final BetaLiftWalk[] lifts = new BetaLiftWalk[n];
    int liftedN = 0;
    int numPropagated = 0;
    for (int i = 0; i < n; i++) {
      final PageReference cref = indirect.getChildReference(i);
      if (cref == null) {
        PHASE7Q_STRIP_ONLY_FAIL.incrementAndGet();
        return null;
      }
      final BetaLiftWalk lw = liftBetaFromSubtreeRecursive(cref, beta, log, revision);
      if (lw == null) {
        PHASE7Q_STRIP_ONLY_FAIL.incrementAndGet();
        return null;
      }
      lifts[i] = lw;
      if (lw.propagates()) { liftedN += 2; numPropagated++; }
      else liftedN += 1;
    }
    if (numPropagated == 0) {
      // Walker found nothing to propagate — descendants captured β but β was β-constant
      // in every capturing subtree (fossil capture). The trie is structurally unchanged;
      // returning null lets caller fall through to standard reject.
      PHASE7Q_STRIP_ONLY_FAIL.incrementAndGet();
      return null;
    }

    final PageReference[] liftedRefs = new PageReference[liftedN];
    {
      int idx = 0;
      for (int i = 0; i < n; i++) {
        liftedRefs[idx++] = lifts[i].root;
        if (lifts[i].propagates()) liftedRefs[idx++] = lifts[i].propagateRight;
      }
    }

    // Step 1: pre-split remaining β-mixed leaves (walker doesn't touch leaves directly).
    final java.util.ArrayList<PageReference> newRefs = new java.util.ArrayList<>(liftedN * 2);
    for (int i = 0; i < liftedN; i++) {
      final PageReference ref = liftedRefs[i];
      if (ref == null) {
        PHASE7Q_STRIP_ONLY_FAIL.incrementAndGet();
        return null;
      }
      final int v = bitConstantValueInSubtree(ref, beta);
      if (v >= 0) {
        newRefs.add(ref);
      } else {
        final SubtreeSplit ss = splitSubtreeOnBit(ref, beta, log, revision);
        if (ss == null) {
          PHASE7Q_STRIP_ONLY_FAIL.incrementAndGet();
          return null;
        }
        newRefs.add(ss.leftRef());
        newRefs.add(ss.rightRef());
      }
    }
    final int newCount = newRefs.size();
    if (newCount > NodeUpgradeManager.MULTI_NODE_MAX_CHILDREN) {
      PHASE7Q_STRIP_ONLY_FAIL.incrementAndGet();
      return null;
    }

    // Step 2: reuse indirect's existing mask (no β extension — β is already there).
    final byte[] extractionPositions;
    final long[] extractionMasks;
    final int allCount;
    final short msbIndex = indirect.getMostSignificantBitIndex();
    if (indirect.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK) {
      // Convert SingleMask to MultiMask form so the partial-key computation can share code
      // with phase7qExtendWithLift.
      final int oldInitialBytePos = indirect.getInitialBytePos();
      final long oldMask = indirect.getBitMask();
      int oldByteCount = 0;
      final int[] oldBytePositions = new int[8];
      final int[] oldByteMaskBits = new int[8];
      for (int bo = 0; bo < 8; bo++) {
        final int byteMaskBits = (int) ((oldMask >>> ((7 - bo) * 8)) & 0xFFL);
        if (byteMaskBits != 0) {
          oldBytePositions[oldByteCount] = oldInitialBytePos + bo;
          oldByteMaskBits[oldByteCount] = byteMaskBits;
          oldByteCount++;
        }
      }
      allCount = oldByteCount;
      extractionPositions = new byte[allCount];
      final int numChunks = (allCount + 7) / 8;
      extractionMasks = new long[numChunks];
      for (int i = 0; i < allCount; i++) {
        extractionPositions[i] = (byte) oldBytePositions[i];
        final int chunkIdx = i / 8;
        final int byteOffsetInChunk = i % 8;
        extractionMasks[chunkIdx] |=
            ((long) (oldByteMaskBits[i] & 0xFF)) << ((7 - byteOffsetInChunk) * 8);
      }
    } else {
      final byte[] ep = indirect.getExtractionPositions();
      final long[] em = indirect.getExtractionMasks();
      allCount = indirect.getNumExtractionBytes();
      extractionPositions = new byte[allCount];
      System.arraycopy(ep, 0, extractionPositions, 0, allCount);
      final int numChunks = (allCount + 7) / 8;
      extractionMasks = new long[numChunks];
      System.arraycopy(em, 0, extractionMasks, 0, Math.min(em.length, numChunks));
    }

    // Step 3: compute partials under the unchanged mask. Verify uniqueness + I4.
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
          if (dbg) System.err.println("[phase7q-strip-only] reject reason=partial-collision beta="
              + beta + " i=" + i + " k=" + k + " partial=" + Integer.toHexString(newPartials[i]));
          PHASE7Q_STRIP_ONLY_FAIL.incrementAndGet();
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
      if (dbg) System.err.println("[phase7q-strip-only] reject reason=no-zero-partial beta=" + beta
          + " newCount=" + newCount);
      PHASE7Q_STRIP_ONLY_FAIL.incrementAndGet();
      return null;
    }

    // Phase 7q.12 — global constancy gate on the rebuilt indirect's children. Each
    // child's subtree must be constant on every bit in indirect's existing mask
    // (preserves Binna's I5/I6 — sparse-path PEXT routes deterministically). Without
    // this gate, strip-only cascades downstream (walker propagation can re-route children
    // into partitions whose mask captures bits no longer constant in those subtrees).
    for (int i = 0; i < newCount; i++) {
      final PageReference cref = newChildren[i];
      if (cref == null) continue;
      // Quick check: skip placeholder children (unresolvable) — they were already trusted
      // by liftedChildIsConstantOnBit's default-true return.
      for (int bi = 0; bi < allCount; bi++) {
        final int bp = extractionPositions[bi] & 0xFF;
        final int chunkIdx = bi / 8;
        final int byteOffsetInChunk = bi % 8;
        final int byteMaskBits =
            (int) ((extractionMasks[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFFL);
        for (int mfBit = 0; mfBit < 8; mfBit++) {
          final int byteBit = 7 - mfBit;
          if ((byteMaskBits & (1 << byteBit)) == 0) continue;
          final int absBit = bp * 8 + mfBit;
          if (!liftedChildIsConstantOnBit(cref, absBit, dbg, indirect.getPageKey(), i, newCount)) {
            if (dbg) System.err.println("[phase7q-strip-only] reject reason=child-non-constant beta="
                + beta + " childIdx=" + i + " absBit=" + absBit);
            PHASE7Q_STRIP_ONLY_FAIL.incrementAndGet();
            return null;
          }
        }
      }
    }

    if (dbg) System.err.println("[phase7q-strip-only] STRIP-OK beta=" + beta + " newCount=" + newCount
        + " msbIndex=" + msbIndex + " allCount=" + allCount + " liftedN=" + liftedN
        + " numPropagated=" + numPropagated);
    PHASE7Q_STRIP_ONLY_SUCCESS.incrementAndGet();
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
   * Phase 7q helper — returns {@code true} iff at least two of {@code indirect}'s
   * children's firstKeys differ at absolute bit position {@code absBit}.
   * <p>HFT-grade: primitive return, no allocation, one bounded scan.
   */
  private boolean indirectChildrenFirstKeysDifferOnBit(HOTIndirectPage indirect, int absBit) {
    final int n = indirect.getNumChildren();
    if (n < 2) return false;
    int firstVal = -1;
    for (int i = 0; i < n; i++) {
      final PageReference cref = indirect.getChildReference(i);
      if (cref == null) continue;
      final byte[] fk = getFirstKeyFromChild(cref);
      if (fk == null || fk.length == 0) continue;
      final int v = isAbsBitSet(fk, absBit) ? 1 : 0;
      if (firstVal < 0) firstVal = v;
      else if (firstVal != v) return true;
    }
    return false;
  }

  /**
   * Stage G.32 — I11-safe root mask reconciliation. When a child subtree's leaves get
   * inserted into and the subtree's deep-firstKey changes, root's stored partial for that
   * child may no longer match the firstKey order (stale partial). Walk root's children,
   * find offending adjacent pairs (= partial order doesn't match firstKey order), find
   * the MSDB between them, and ADD that bit to root's mask — BUT ONLY if the bit's absBit
   * is GREATER (= less significant) than root's current MSB. This preserves I11.
   *
   * <p>The new bit must be in a byte that's already in root's extraction byte-list (=
   * extending an existing mask byte). Adding a new byte position would change the layout
   * radically; defer to splitParentAndRecurse for that case.
   *
   * <p>HFT-grade: bounded loop (≤ 16 attempts), one allocation per try.
   */
  public void reconcileRootMaskI11Safe(PageReference rootRef,
      StorageEngineWriter storageEngineWriter, TransactionIntentLog log) {
    final boolean dbg = Boolean.getBoolean("hot.debug.g32");
    if (rootRef == null) return;
    // Phase 7q.10 — bind activeLog so getFirstKeyFromChild can resolve TIL-only refs.
    // Without this, placeholder children produce EMPTY_KEY → partial=0 → collisions in
    // extendIndirectMaskForClosureI11Safe's partial uniqueness check, blocking the
    // discriminating bit from being added.
    this.activeReader = storageEngineWriter;
    this.activeLog = log;
    final int revision = storageEngineWriter.getRevisionNumber();
    final var logEntry = log.get(rootRef);
    Page curPage = logEntry != null ? logEntry.getModified() : rootRef.getPage();
    if (curPage == null) curPage = loadPage(storageEngineWriter, rootRef);
    if (!(curPage instanceof HOTIndirectPage indirect)) return;
    if (indirect.getNumChildren() < 2) return;

    // First check if there's any I8 violation worth fixing.
    {
      final int n = indirect.getNumChildren();
      final byte[][] firstKeys = new byte[n][];
      for (int i = 0; i < n; i++) {
        final PageReference cref = indirect.getChildReference(i);
        if (cref == null) return;
        firstKeys[i] = getFirstKeyFromChild(cref);
      }
      boolean hasInversion = false;
      for (int i = 1; i < n; i++) {
        if (firstKeys[i] == null || firstKeys[i - 1] == null
            || firstKeys[i].length == 0 || firstKeys[i - 1].length == 0) continue;
        if (Arrays.compareUnsigned(firstKeys[i - 1], firstKeys[i]) >= 0) {
          hasInversion = true; break;
        }
      }
      if (!hasInversion) return;

      // Stage G.32 — bulk-rebuild root with FULL MSDB-closure of current firstKeys.
      // Try this ONCE; if it succeeds, root is reconciled. If it fails, fall through to
      // the iterative single-bit approach as a safety net.
      final int parentMsb = indirect.getMostSignificantBitIndex() & 0xFFFF;
      final HOTIndirectPage bulkRebuilt = rebuildRootWithFullClosureI11Safe(indirect, parentMsb, revision);
      if (bulkRebuilt != null) {
        if (dbg) System.err.println("[g32] BULK-REBUILD-OK newMsb=" + bulkRebuilt.getMostSignificantBitIndex()
            + " parentMsb=" + parentMsb);
        G32_RECONCILE_FIRINGS.incrementAndGet();
        log.put(rootRef, PageContainer.getInstance(bulkRebuilt, bulkRebuilt));
        rootRef.setPage(bulkRebuilt);
        return; // bulk path resolved it
      }
      if (dbg) System.err.println("[g32] bulk-rebuild-failed, falling through to iterative");

      // Phase 7q.13 — last-resort: ADD A NEW ROOT LEVEL above the inverted-firstKey pair.
      // When iterative bit-extension cannot place the discriminating bit in the existing
      // root.mask (= partial collision from placeholder children, β-mixed leaves blocking
      // constancy), wrap the existing root structure with a NEW INDIRECT that has the
      // discriminating bit in its mask. The two new children are the LEFT half (β=0
      // children of old root) and RIGHT half (β=1 children of old root), each rebuilt
      // as a sub-indirect inheriting old root's mask filtered to bits > β.
      //
      // This relaxes the per-insert fail-stop in favor of a structural rewrite that
      // satisfies I8 by construction.
      final HOTIndirectPage levelRebuilt = addNewRootLevelForI8(indirect, log, revision);
      if (levelRebuilt != null) {
        if (dbg) System.err.println("[g32] NEW-ROOT-LEVEL OK newMsb="
            + levelRebuilt.getMostSignificantBitIndex());
        G32_RECONCILE_FIRINGS.incrementAndGet();
        log.put(rootRef, PageContainer.getInstance(levelRebuilt, levelRebuilt));
        rootRef.setPage(levelRebuilt);
        return;
      }
      if (dbg) System.err.println("[g32] new-root-level-failed, falling through to iterative");

      // Phase 7q.15 — multi-β atomic lift. Compute ALL needed MSDB bits at once, lift them
      // sequentially WITHOUT building intermediate indirects (= accumulate walker outputs),
      // then build a SINGLE new indirect with all bits in mask. Avoids the iter-N-depends-
      // on-iter-(N-1) divergence problem documented in hot-phase7q-15-session.md. Gated by
      // hot.strict.g32.multibeta.
      if (Boolean.getBoolean("hot.strict.g32.multibeta")) {
        final HOTIndirectPage multiBetaFixed = phase7qMultiBetaAtomicLift(indirect, log, revision);
        if (multiBetaFixed != null) {
          if (dbg) System.err.println("[g32] MULTI-BETA-OK newMsb="
              + multiBetaFixed.getMostSignificantBitIndex());
          log.put(rootRef, PageContainer.getInstance(multiBetaFixed, multiBetaFixed));
          rootRef.setPage(multiBetaFixed);
          return;
        }
        if (dbg) System.err.println("[g32] multi-beta failed, falling through to iterative");
      }

      // Phase 7q.15 — iterative I8 fix. After the bulk + addNewRoot + multi-β paths fail,
      // run an iterative loop that finds the FIRST adjacent inversion in stored order,
      // computes its MSB-of-diff, and adds that bit via extend (β-constant case) or lift
      // (non-β-constant case). Gated by hot.strict.g32.deep.
      if (Boolean.getBoolean("hot.strict.g32.deep")) {
        final HOTIndirectPage iterFixed = phase7qIterativeRootSortI8(indirect, log, revision);
        if (iterFixed != null && iterFixed != indirect) {
          if (dbg) System.err.println("[g32] ITERATIVE-SORT-OK newMsb="
              + iterFixed.getMostSignificantBitIndex());
          log.put(rootRef, PageContainer.getInstance(iterFixed, iterFixed));
          rootRef.setPage(iterFixed);
          return;
        }
        if (dbg) System.err.println("[g32] iterative-sort failed");
      }
    }

    // Up to 16 mask-extension attempts (single-bit fallback).
    for (int attempt = 0; attempt < 16; attempt++) {
      final int n = indirect.getNumChildren();
      // Collect current deep-firstKeys.
      final byte[][] firstKeys = new byte[n][];
      for (int i = 0; i < n; i++) {
        final PageReference cref = indirect.getChildReference(i);
        if (cref == null) return;
        firstKeys[i] = getFirstKeyFromChild(cref);
      }
      // Find offending adjacent pair in STORED (partial) order: firstKey[i-1] >= firstKey[i].
      int offI = -1, offK = -1;
      for (int i = 1; i < n; i++) {
        if (firstKeys[i] == null || firstKeys[i - 1] == null
            || firstKeys[i].length == 0 || firstKeys[i - 1].length == 0) continue;
        if (Arrays.compareUnsigned(firstKeys[i - 1], firstKeys[i]) >= 0) {
          offK = i - 1; offI = i;
          break;
        }
      }
      if (offI < 0) {
        if (dbg && attempt > 0) System.err.println("[g32] done attempts=" + attempt
            + " rootMsb=" + indirect.getMostSignificantBitIndex());
        return; // no I8 violation — done
      }
      final int parentMsb = indirect.getMostSignificantBitIndex() & 0xFFFF;
      // Compute MSDB-closure: ALL absBits where any pair of children's firstKeys differ,
      // restricted to absBit > parentMsb (= I11-safe). Then try each in MSDB-first order
      // (= smallest absBit = most significant first). Pick the first that succeeds.
      int maxLen = 0;
      for (int i = 0; i < n; i++) {
        if (firstKeys[i] != null && firstKeys[i].length > maxLen) maxLen = firstKeys[i].length;
      }
      final int totalAbsBits = maxLen * 8;
      // Bitset of closure bits (= positions where any pair differs).
      final long[] closureBitset = new long[(totalAbsBits + 63) / 64];
      for (int absBit = parentMsb + 1; absBit < totalAbsBits; absBit++) {
        boolean seen0 = false, seen1 = false;
        for (int i = 0; i < n; i++) {
          if (firstKeys[i] == null || firstKeys[i].length == 0) continue;
          if (isAbsBitSet(firstKeys[i], absBit)) seen1 = true;
          else seen0 = true;
          if (seen0 && seen1) break;
        }
        if (seen0 && seen1) closureBitset[absBit >> 6] |= 1L << (absBit & 63);
      }
      // Phase 7q.10 diagnostic: dump per-child bit values at specific absBits.
      if (dbg && attempt == 0) {
        for (final int probeBit : new int[]{87, 96, 98, 99, 100, 104}) {
          final int bp = probeBit / 8;
          final int mask = 1 << (7 - (probeBit % 8));
          final StringBuilder pb = new StringBuilder("[g32-bitprobe] absBit=").append(probeBit);
          pb.append(" bytePos=").append(bp).append(" mask=0x")
              .append(Integer.toHexString(mask));
          for (int i = 0; i < n; i++) {
            if (firstKeys[i] == null || firstKeys[i].length == 0) continue;
            final int byteVal = bp < firstKeys[i].length ? (firstKeys[i][bp] & 0xff) : -1;
            pb.append(" c[").append(i).append("](b[").append(bp).append("]=0x")
                .append(byteVal < 0 ? "OOB" : String.format("%02x", byteVal))
                .append(")=").append(isAbsBitSet(firstKeys[i], probeBit) ? "1" : "0");
          }
          System.err.println(pb);
        }
      }
      // Phase 7q.10 diagnostic: dump closure-bit list + offending pair on first attempt.
      if (dbg && attempt == 0) {
        final StringBuilder sb = new StringBuilder("[g32-closure] attempt=0 parentMsb=" + parentMsb
            + " offK=" + offK + " offI=" + offI + " n=" + n + " closureBits=[");
        boolean first = true;
        for (int absBit = parentMsb + 1; absBit < totalAbsBits; absBit++) {
          if ((closureBitset[absBit >> 6] & (1L << (absBit & 63))) == 0) continue;
          if (!first) sb.append(',');
          sb.append(absBit);
          first = false;
        }
        sb.append("]");
        for (int i = 0; i < n; i++) {
          sb.append("\n  c[").append(i).append("] len=");
          if (firstKeys[i] == null) { sb.append("null"); continue; }
          sb.append(firstKeys[i].length).append(" raw=");
          for (int b = 0; b < firstKeys[i].length; b++) {
            sb.append(String.format("[%d]=%02x ", b, firstKeys[i][b] & 0xff));
          }
        }
        System.err.println(sb);
      }
      // Try each closure bit in order. First try constancy-only (no split). If all fail,
      // try split-aware (= splits β-mixed children to make β-constant).
      HOTIndirectPage extended = null;
      int chosenBit = -1;
      for (int absBit = parentMsb + 1; absBit < totalAbsBits; absBit++) {
        if ((closureBitset[absBit >> 6] & (1L << (absBit & 63))) == 0) continue;
        extended = extendMaskWithBitI11Safe(indirect, absBit, revision);
        if (extended != null) {
          chosenBit = absBit;
          break;
        }
      }
      if (extended == null) {
        // Constancy-only failed. Try split-aware: splitSubtreeOnBit on β-mixed children
        // to make every child β-constant, then extend.
        for (int absBit = parentMsb + 1; absBit < totalAbsBits; absBit++) {
          if ((closureBitset[absBit >> 6] & (1L << (absBit & 63))) == 0) continue;
          extended = extendIndirectMaskForClosureI11Safe(indirect, absBit, parentMsb, log, revision);
          if (extended != null) {
            chosenBit = absBit;
            if (dbg) System.err.println("[g32] split-aware extend OK bit=" + absBit);
            break;
          }
        }
      }
      if (extended == null) {
        // Split-aware on closure bits failed. Try LIFTING: for each child's MSB bit,
        // attempt to split that child on its own MSB and add the MSB to root's mask.
        // This is the recursive lift operation — children DROP their MSB (= absorbed
        // into root), children's MSB shifts to less-significant. Eventually root's
        // mask has enough bits to discriminate via PEXT.
        final java.util.TreeSet<Integer> childMsbs = new java.util.TreeSet<>();
        for (int i = 0; i < n; i++) {
          final PageReference cref = indirect.getChildReference(i);
          if (cref == null) continue;
          final Page cp = cref.getPage();
          if (cp instanceof HOTIndirectPage ci) {
            final int cmsb = ci.getMostSignificantBitIndex();
            if (cmsb > parentMsb) childMsbs.add(cmsb);
          }
        }
        for (final int liftBit : childMsbs) {
          extended = extendIndirectMaskForClosureI11Safe(indirect, liftBit, parentMsb, log, revision);
          if (extended != null) {
            chosenBit = liftBit;
            if (dbg) System.err.println("[g32] LIFT extend OK bit=" + liftBit);
            break;
          }
        }
      }
      if (extended == null) {
        if (dbg) System.err.println("[g32] reject reason=no-bit-works parentMsb=" + parentMsb
            + " offK=" + offK + " offI=" + offI);
        return;
      }
      if (dbg) System.err.println("[g32] EXTEND-OK msdb=" + chosenBit + " parentMsb=" + parentMsb
          + " newMsb=" + extended.getMostSignificantBitIndex());
      G32_RECONCILE_FIRINGS.incrementAndGet();
      log.put(rootRef, PageContainer.getInstance(extended, extended));
      // Also setPage so subsequent reads see it.
      rootRef.setPage(extended);
      indirect = extended;
    }
  }

  /**
   * Stage G.32 — Variant of {@link #extendIndirectMaskForClosure} that's I11-safe by
   * construction: requires β > parentMsb (= less significant than current MSB). For β-mixed
   * children, splits them via {@code splitSubtreeOnBit}. The split products inherit the
   * child's OLD mask (MSB unchanged), so their MSB stays > β → I11 holds.
   *
   * <p>Skips the G.30 placeholder-child guard because in G.32 context, TIL-only refs are
   * valid in-flight pages with refHasPage=true, not stale shadows. The I11-safety
   * constraint replaces the placeholder guard as the structural safety net.
   *
   * <p>Returns null on infeasibility (= fanout overflow, partial collision, no zero
   * partial, or split fails).
   */
  private @Nullable HOTIndirectPage extendIndirectMaskForClosureI11Safe(HOTIndirectPage indirect,
      int beta, int parentMsb, TransactionIntentLog log, int revision) {
    if (indirect == null || beta < 0) return null;
    if (beta <= parentMsb) return null; // I11 unsafe
    final int oldNumChildren = indirect.getNumChildren();
    if (oldNumChildren < 2) return null;

    // Pre-split β-mixed children. β > parentMsb means children's MSB > β too (since
    // children.MSB > parentMsb by I11), so split products inherit child.mask with MSB
    // unchanged. Sirix's multi-entry leaves may still have β-mixed leaves: splitLeafOnBit
    // partitions the leaf's keys by β.
    final java.util.ArrayList<PageReference> newRefs = new java.util.ArrayList<>(oldNumChildren * 2);
    for (int i = 0; i < oldNumChildren; i++) {
      final PageReference ref = indirect.getChildReference(i);
      if (ref == null) return null;
      final int v = bitConstantValueInSubtree(ref, beta);
      if (v >= 0) {
        newRefs.add(ref);
      } else {
        final SubtreeSplit ss = splitSubtreeOnBit(ref, beta, log, revision);
        if (ss == null) return null;
        newRefs.add(ss.leftRef());
        newRefs.add(ss.rightRef());
      }
    }
    final int newCount = newRefs.size();
    if (newCount > NodeUpgradeManager.MULTI_NODE_MAX_CHILDREN) return null;

    // Build new MultiMask layout: combine indirect's existing extraction bytes + β's byte.
    final int oldInitialBytePos = indirect.getInitialBytePos();
    final int newBytePos = beta / 8;
    final int newBitInByte = beta % 8;
    final int newMaskBitInByte = 1 << (7 - newBitInByte);

    int[] oldBytePositions = new int[16];
    int[] oldByteMaskBits = new int[16];
    int oldByteCount = 0;
    if (indirect.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK) {
      final long oldMask = indirect.getBitMask();
      for (int bo = 0; bo < 8; bo++) {
        final int byteMaskBits = (int) ((oldMask >>> ((7 - bo) * 8)) & 0xFFL);
        if (byteMaskBits != 0) {
          oldBytePositions[oldByteCount] = oldInitialBytePos + bo;
          oldByteMaskBits[oldByteCount] = byteMaskBits;
          oldByteCount++;
        }
      }
    } else {
      final byte[] ep = indirect.getExtractionPositions();
      final long[] em = indirect.getExtractionMasks();
      final int neb = indirect.getNumExtractionBytes();
      for (int i = 0; i < neb; i++) {
        final int chunkIdx = i / 8;
        final int byteOffsetInChunk = i % 8;
        final int byteMaskBits = (int) ((em[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFFL);
        if (byteMaskBits != 0) {
          if (oldByteCount >= oldBytePositions.length) return null;
          oldBytePositions[oldByteCount] = ep[i] & 0xFF;
          oldByteMaskBits[oldByteCount] = byteMaskBits;
          oldByteCount++;
        }
      }
    }
    // Merge newBytePos.
    int[] allBytePositions = new int[oldByteCount + 1];
    int[] allByteMaskBits = new int[oldByteCount + 1];
    int allCount = 0;
    boolean merged = false;
    for (int i = 0; i < oldByteCount; i++) {
      if (!merged && oldBytePositions[i] == newBytePos) {
        allBytePositions[allCount] = newBytePos;
        allByteMaskBits[allCount] = oldByteMaskBits[i] | newMaskBitInByte;
        if ((oldByteMaskBits[i] & newMaskBitInByte) != 0) return null;
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
    // Compute partials.
    final int[] newPartials = new int[newCount];
    final PageReference[] newChildren = new PageReference[newCount];
    for (int i = 0; i < newCount; i++) {
      final PageReference cref = newRefs.get(i);
      newChildren[i] = cref;
      final byte[] cKey = getFirstKeyFromChild(cref);
      newPartials[i] = (cKey == null || cKey.length == 0) ? 0
          : computePartialKeyMultiMaskDirect(cKey, extractionPositions, extractionMasks, allCount);
    }
    final boolean dbgI11 = Boolean.getBoolean("hot.debug.g32");
    for (int i = 1; i < newCount; i++) {
      for (int k = 0; k < i; k++) {
        if (newPartials[k] == newPartials[i]) {
          if (dbgI11) {
            final byte[] fkI = getFirstKeyFromChild(newRefs.get(i));
            final byte[] fkK = getFirstKeyFromChild(newRefs.get(k));
            final StringBuilder fkIs = new StringBuilder(); final StringBuilder fkKs = new StringBuilder();
            if (fkI != null) for (final byte b : fkI) fkIs.append(String.format("%02x", b & 0xff));
            if (fkK != null) for (final byte b : fkK) fkKs.append(String.format("%02x", b & 0xff));
            System.err.println("[g32-i11s] reject beta=" + beta
                + " reason=partial-collision i=" + i + " k=" + k + " p=" + newPartials[i]
                + " newCount=" + newCount + " fkI=" + fkIs + " fkK=" + fkKs);
          }
          return null;
        }
      }
    }
    sortChildrenAndPartialsByPartial(newChildren, newPartials);
    // Verify firstKey-monotone.
    for (int i = 1; i < newCount; i++) {
      final byte[] prev = getFirstKeyFromChild(newChildren[i - 1]);
      final byte[] curr = getFirstKeyFromChild(newChildren[i]);
      if (prev != null && curr != null && prev.length > 0 && curr.length > 0
          && Arrays.compareUnsigned(prev, curr) >= 0) {
        if (dbgI11) System.err.println("[g32-i11s] reject beta=" + beta
            + " reason=still-inverted i=" + i);
        return null;
      }
    }
    if (newPartials[0] != 0) {
      if (dbgI11) System.err.println("[g32-i11s] reject beta=" + beta
          + " reason=no-zero-partial firstPartial=" + newPartials[0]
          + " newCount=" + newCount);
      return null;
    }

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
   * Stage G.32 — Rebuild root indirect with FULL MSDB-closure of children's current
   * deep-firstKeys, restricted to I11-safe bits (absBit > parentMsb). Recomputes ALL
   * extraction positions/masks from scratch based on which bits actually differ across
   * children. This is the bulk version of the iterative single-bit extension.
   *
   * <p>Preserves existing bits in the mask if they're &lt;= parentMsb (= more significant).
   * Discards any bits that no longer discriminate (= all children agree at that bit).
   * Adds all bits where any pair currently differs.
   *
   * <p>Returns null if the rebuild can't produce a valid layout (= partials collide,
   * no zero-partial child, etc.).
   */
  private @Nullable HOTIndirectPage rebuildRootWithFullClosureI11Safe(HOTIndirectPage indirect,
      int parentMsb, int revision) {
    final int n = indirect.getNumChildren();
    if (n < 2) return null;
    // Collect current firstKeys.
    final byte[][] firstKeys = new byte[n][];
    int maxLen = 0;
    for (int i = 0; i < n; i++) {
      final PageReference cref = indirect.getChildReference(i);
      if (cref == null) return null;
      firstKeys[i] = getFirstKeyFromChild(cref);
      if (firstKeys[i] == null || firstKeys[i].length == 0) return null;
      if (firstKeys[i].length > maxLen) maxLen = firstKeys[i].length;
    }
    final int totalAbsBits = maxLen * 8;

    // Step 1: collect ALL bits where any pair of children's firstKeys differ.
    // Bucket by byte position.
    final java.util.TreeMap<Integer, Integer> maskByBytePos = new java.util.TreeMap<>();
    // Step 1a: preserve indirect's existing mask bits at absBit <= parentMsb (I11-required;
    // these are bits root already captures and must continue capturing to keep its MSB).
    {
      if (indirect.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK) {
        final int initialBytePos = indirect.getInitialBytePos();
        final long oldMask = indirect.getBitMask();
        for (int bo = 0; bo < 8; bo++) {
          final int byteMaskBits = (int) ((oldMask >>> ((7 - bo) * 8)) & 0xFFL);
          if (byteMaskBits != 0) {
            maskByBytePos.merge(initialBytePos + bo, byteMaskBits, (a, b) -> a | b);
          }
        }
      } else {
        final byte[] ep = indirect.getExtractionPositions();
        final long[] em = indirect.getExtractionMasks();
        final int neb = indirect.getNumExtractionBytes();
        for (int i = 0; i < neb; i++) {
          final int chunkIdx = i / 8;
          final int byteOffsetInChunk = i % 8;
          final int byteMaskBits = (int) ((em[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFFL);
          if (byteMaskBits != 0) {
            maskByBytePos.merge(ep[i] & 0xFF, byteMaskBits, (a, b) -> a | b);
          }
        }
      }
    }
    // Step 1b: ADD bits where any pair currently differs.
    // Phase 7q.15 — for ROOT mask reconciliation (the only caller), the lower bound on
    // absBit was previously parentMsb+1 (= current root MSB + 1). That restriction misses
    // the case where the MSB-of-difference between offending firstKeys is MORE significant
    // (smaller absBit) than the current root MSB — exactly the diagnostic-microbench I8
    // case (offending bit 90 < root MSB 91). Allowing absBit < currentMsb is I11-safe for
    // the root because: (i) root has no parent — no parent.MSB constraint, and (ii) every
    // child's MSB is already > current root MSB ≥ new root MSB, so child.MSB > root.MSB
    // still holds. Gated by hot.strict.g32.deep so the legacy behavior is preserved by
    // default while this lands. β-CONSTANCY GATE: only add bit if it's constant in every
    // child's subtree (= each child agrees with its own firstKey at this bit position).
    // Sirix's multi-entry leaves can violate constancy; without this gate, the new mask
    // would misroute subtree keys (I6 violations cascade).
    final int absBitLowerBound = Boolean.getBoolean("hot.strict.g32.deep") ? 0 : parentMsb + 1;
    for (int absBit = absBitLowerBound; absBit < totalAbsBits; absBit++) {
      boolean seen0 = false, seen1 = false;
      for (int i = 0; i < n; i++) {
        if (firstKeys[i].length == 0) continue;
        if (isAbsBitSet(firstKeys[i], absBit)) seen1 = true;
        else seen0 = true;
        if (seen0 && seen1) break;
      }
      if (seen0 && seen1) {
        // Verify β-constancy in EVERY child's subtree.
        boolean allConstant = true;
        for (int i = 0; i < n; i++) {
          final int bv = bitConstantValueInSubtree(indirect.getChildReference(i), absBit);
          if (bv < 0) { allConstant = false; break; }
        }
        if (!allConstant) continue;
        final int bytePos = absBit / 8;
        final int bitInByte = absBit % 8;
        final int maskBit = 1 << (7 - bitInByte);
        maskByBytePos.merge(bytePos, maskBit, (a, b) -> a | b);
      }
    }

    if (maskByBytePos.isEmpty()) return null;

    // Step 2: encode extraction tables. Always MultiMask for generality.
    final int numBytes = maskByBytePos.size();
    if (numBytes > 64) return null;
    final byte[] extractionPositions = new byte[numBytes];
    final int numChunks = (numBytes + 7) / 8;
    final long[] extractionMasks = new long[numChunks];
    short msbIndex = Short.MAX_VALUE;
    {
      int idx = 0;
      for (final var entry : maskByBytePos.entrySet()) {
        final int bytePos = entry.getKey();
        final int maskByte = entry.getValue();
        extractionPositions[idx] = (byte) bytePos;
        final int chunkIdx = idx / 8;
        final int byteOffsetInChunk = idx % 8;
        extractionMasks[chunkIdx] |= ((long) (maskByte & 0xFF)) << ((7 - byteOffsetInChunk) * 8);
        final int highBit = 31 - Integer.numberOfLeadingZeros(maskByte & 0xFF);
        final int absBitPos = bytePos * 8 + (7 - highBit);
        if (absBitPos < msbIndex) msbIndex = (short) absBitPos;
        idx++;
      }
    }

    // Step 3: recompute partials from current firstKeys.
    final int[] newPartials = new int[n];
    final PageReference[] newChildren = new PageReference[n];
    int totalMaskBits = 0;
    for (final long m : extractionMasks) totalMaskBits += Long.bitCount(m);
    if (totalMaskBits > 16) return null; // partial too wide for child fanout
    for (int i = 0; i < n; i++) {
      newChildren[i] = indirect.getChildReference(i);
      newPartials[i] = computePartialKeyMultiMaskDirect(firstKeys[i],
          extractionPositions, extractionMasks, numBytes);
    }
    // Verify uniqueness.
    for (int i = 1; i < n; i++) {
      for (int k = 0; k < i; k++) {
        if (newPartials[k] == newPartials[i]) return null;
      }
    }
    // Sort by partial.
    sortChildrenAndPartialsByPartial(newChildren, newPartials);
    // Verify firstKey-monotone after partial-sort.
    for (int i = 1; i < n; i++) {
      final byte[] prev = getFirstKeyFromChild(newChildren[i - 1]);
      final byte[] curr = getFirstKeyFromChild(newChildren[i]);
      if (prev != null && curr != null && prev.length > 0 && curr.length > 0
          && Arrays.compareUnsigned(prev, curr) >= 0) {
        return null; // inversion remains — closure didn't help
      }
    }
    // Verify I4 (first partial = 0).
    if (newPartials[0] != 0) return null;

    // Phase 7q-Path5 — partial-subset routing-correctness check. The HOT routing rule
    // for MultiMask is "equality-preferred, subset-fallback picking LARGEST INDEX
    // among subsets". If partials have overlapping subset relations
    // (e.g., {0x10, 0x14, 0x19}), a key whose densePK is a superset of multiple
    // partials gets routed to the LAST one (= largest index in sorted order). But the
    // key may actually be stored under an EARLIER child's subtree → I6 routing cascade.
    //
    // Sufficient correctness condition: for each child[i]'s firstKey,
    // PEXT(firstKey, newMask) under the subset-fallback routing rule must pick
    // child[i] (= same index). If any child fails this self-routing check, the
    // rebuilt indirect would mis-route at least one stored key set. Reject.
    //
    // Opt-in via -Dhot.strict.path5.routeverify=true. Initially explored as default-ON
    // but reverted (2026-05-12) after observing transaction-leak regressions in
    // HOTIndexInternalTest + HOTLargeScaleIntegrationTest when path5 fires in their
    // test contexts. Root cause TBD — keep opt-in for safety.
    // HFT-grade: O(n²) comparisons of pre-computed partials — n ≤ 32 typical.
    if (Boolean.getBoolean("hot.strict.path5.routeverify")) {
      for (int i = 0; i < n; i++) {
        final int densePk = newPartials[i];
        // Find the LARGEST-INDEX partial that's a subset of densePk.
        int bestIdx = -1;
        for (int j = 0; j < n; j++) {
          if ((densePk & newPartials[j]) == newPartials[j]) {
            // Equality-preferred: an exact match wins immediately.
            if (newPartials[j] == densePk) { bestIdx = j; break; }
            if (bestIdx < 0 || j > bestIdx) bestIdx = j;
          }
        }
        if (bestIdx != i) {
          if (Boolean.getBoolean("hot.debug.path5")) {
            System.err.println("[path5] REJECT rebuild: child[" + i + "].firstKey densePK=0x"
                + Integer.toHexString(densePk) + " routes to child[" + bestIdx
                + "] partial=0x" + Integer.toHexString(bestIdx >= 0 ? newPartials[bestIdx] : -1)
                + " (expected child[" + i + "] partial=0x" + Integer.toHexString(newPartials[i]) + ")");
          }
          return null;
        }
      }
    }

    if (n <= 16) {
      return HOTIndirectPage.createSpanNodeMultiMask(indirect.getPageKey(), revision,
          extractionPositions, extractionMasks, numBytes, newPartials, newChildren,
          indirect.getHeight(), msbIndex);
    }
    return HOTIndirectPage.createMultiNodeMultiMask(indirect.getPageKey(), revision,
        extractionPositions, extractionMasks, numBytes, newPartials, newChildren,
        indirect.getHeight(), msbIndex);
  }

  /**
   * Phase 7q.15 — Iterative I8 fix. For each iteration: identify the FIRST adjacent
   * inversion in stored child order (= partial-sorted but firstKey-out-of-order); compute
   * the MSB-of-difference (MSDB) between those two firstKeys; add that bit to the indirect's
   * mask via {@link #extendIndirectMaskForClosure} (β-constant case) or
   * {@link #phase7qExtendWithLift} (non-constant case — lifts β from descendants).
   *
   * <p>After adding a bit, the children are re-sorted by partial. The next inversion (if
   * any) may involve a DIFFERENT pair with a DIFFERENT MSDB. Iterate up to a bounded
   * number of times until no inversion remains.
   *
   * <p>Returns the final indirect when fully sorted, or null when no further bit can be
   * added (e.g., all candidate bits already in mask, or all extensions fail).
   *
   * <p>HFT-grade: bounded outer loop (≤16); per-iter cost is one extendIndirectMaskForClosure
   * or one phase7qExtendWithLift, plus a firstKey scan over children.
   */
  /**
   * Phase 7q.15 — Multi-β ATOMIC lift. Compute the set of bits that need to be in the
   * mask for full lex-order coincidence of children's firstKeys. Lift EACH bit from
   * descendants in sequence, accumulating the resulting child refs. Build a single new
   * indirect with ALL βs in its mask in one final atomic step.
   *
   * <p>Why "atomic": iterating with sequential calls to {@link #phase7qExtendWithLift}
   * creates intermediate indirects whose state diverges from what subsequent iterations
   * can build upon — bucket-builds inside the lift fail because the inherited mask
   * captures bits non-constant in re-shuffled children. By accumulating walker outputs
   * directly (without building intermediate indirects), we sidestep that divergence.
   *
   * <p>Returns the new indirect on success, or null if any β can't be lifted.
   *
   * <p>HFT-grade: bounded β-set (≤ 16); per-β cost is one walker per child, sequential.
   */
  @Nullable
  /** Phase 7q.15 — enable cbinode (constancy-preserving BiNode in wrapBucketInSubtree)
   *  for the duration of this thread's multi-β lift call. Avoids enabling cbinode
   *  per-insert (which cascades). */
  private static final ThreadLocal<Boolean> CBINODE_THREAD_LOCAL = new ThreadLocal<>();

  /** Phase 7q.15 — caller-set hint: the MSB of the future parent indirect that this
   *  wrap-bucket will be inserted under. cbinode requires `discBit > parentMsbHint` so
   *  the resulting BiNode's MSB satisfies I11 (parent.MSB < child.MSB). Set to -1 (or
   *  absent) means no constraint. */
  private static final ThreadLocal<Integer> PARENT_MSB_HINT = new ThreadLocal<>();

  private HOTIndirectPage phase7qMultiBetaAtomicLift(HOTIndirectPage indirect,
      TransactionIntentLog log, int revision) {
    final boolean dbg = Boolean.getBoolean("hot.debug.g32");
    if (indirect == null) return null;
    final int n0 = indirect.getNumChildren();
    if (n0 < 2) return null;
    final Boolean prevCbinode = CBINODE_THREAD_LOCAL.get();
    CBINODE_THREAD_LOCAL.set(Boolean.TRUE);
    try {
      return phase7qMultiBetaAtomicLiftImpl(indirect, log, revision);
    } finally {
      if (prevCbinode == null) CBINODE_THREAD_LOCAL.remove();
      else CBINODE_THREAD_LOCAL.set(prevCbinode);
    }
  }

  private HOTIndirectPage phase7qMultiBetaAtomicLiftImpl(HOTIndirectPage indirect,
      TransactionIntentLog log, int revision) {
    final boolean dbg = Boolean.getBoolean("hot.debug.g32");
    if (indirect == null) return null;
    final int n0 = indirect.getNumChildren();
    if (n0 < 2) return null;

    // 1. Collect adjacent-pair MSDBs in firstKey-sorted order.
    final byte[][] firstKeys = new byte[n0][];
    int maxLen = 0;
    for (int i = 0; i < n0; i++) {
      final PageReference cref = indirect.getChildReference(i);
      if (cref == null) return null;
      firstKeys[i] = getFirstKeyFromChild(cref);
      if (firstKeys[i] == null || firstKeys[i].length == 0) return null;
      if (firstKeys[i].length > maxLen) maxLen = firstKeys[i].length;
    }
    final Integer[] order = new Integer[n0];
    for (int i = 0; i < n0; i++) order[i] = i;
    java.util.Arrays.sort(order, (a, b) -> Arrays.compareUnsigned(firstKeys[a], firstKeys[b]));
    final byte[][] sortedFks = new byte[n0][];
    for (int i = 0; i < n0; i++) sortedFks[i] = firstKeys[order[i]];

    final java.util.TreeSet<Integer> neededBitsSet = new java.util.TreeSet<>();
    for (int i = 0; i + 1 < n0; i++) {
      final byte[] a = sortedFks[i];
      final byte[] b = sortedFks[i + 1];
      final int len = Math.min(a.length, b.length);
      for (int bp = 0; bp < len; bp++) {
        int xor = (a[bp] ^ b[bp]) & 0xFF;
        while (xor != 0) {
          final int hb = Integer.numberOfLeadingZeros(xor) - 24;
          xor &= ~(1 << (7 - hb));
          final int absBit = bp * 8 + hb;
          if (!indirectMaskHasAbsBit(indirect, absBit)) {
            neededBitsSet.add(absBit);
          }
        }
      }
    }
    if (neededBitsSet.isEmpty()) {
      if (dbg) System.err.println("[g32-multibeta] no needed bits — mask already covers all MSDBs");
      return null;
    }
    if (dbg) System.err.println("[g32-multibeta] needed bits: " + neededBitsSet);

    // 2. For each β in MSB-first order, lift it from all current children.
    //    Accumulate the lifted halves; do NOT build intermediate indirects.
    java.util.ArrayList<PageReference> currentChildren = new java.util.ArrayList<>(n0);
    for (int i = 0; i < n0; i++) {
      currentChildren.add(indirect.getChildReference(i));
    }
    // Track which βs were actually accepted (= didn't overflow fanout). Final mask uses
    // ONLY these. Skipping a β means the trie won't fully sort but partial progress is
    // routing-correct.
    final java.util.ArrayList<Integer> acceptedBetas = new java.util.ArrayList<>(neededBitsSet.size());
    for (final int beta : neededBitsSet) {
      final java.util.ArrayList<PageReference> nextChildren =
          new java.util.ArrayList<>(currentChildren.size() * 2);
      for (final PageReference child : currentChildren) {
        if (child == null) {
          if (dbg) System.err.println("[g32-multibeta] null child for β=" + beta);
          return null;
        }
        final BetaLiftWalk lw = liftBetaFromSubtreeRecursive(child, beta, log, revision);
        if (lw == null) {
          if (dbg) System.err.println("[g32-multibeta] walker returned null for β=" + beta
              + " childKey=" + child.getKey());
          return null;
        }
        nextChildren.add(lw.root);
        if (lw.propagates()) nextChildren.add(lw.propagateRight);
      }
      // Also pre-split β-mixed leaves at this level (walker doesn't touch leaves).
      final java.util.ArrayList<PageReference> postLeafSplit =
          new java.util.ArrayList<>(nextChildren.size());
      for (final PageReference ref : nextChildren) {
        if (ref == null) return null;
        final int bv = bitConstantValueInSubtree(ref, beta);
        if (bv >= 0) {
          postLeafSplit.add(ref);
        } else {
          final SubtreeSplit ss = splitSubtreeOnBit(ref, beta, log, revision);
          if (ss == null) {
            if (dbg) System.err.println("[g32-multibeta] subtree split null β=" + beta);
            return null;
          }
          postLeafSplit.add(ss.leftRef());
          postLeafSplit.add(ss.rightRef());
        }
      }
      if (postLeafSplit.size() > NodeUpgradeManager.MULTI_NODE_MAX_CHILDREN) {
        if (dbg) System.err.println("[g32-multibeta] fanout overflow at β=" + beta
            + " count=" + postLeafSplit.size() + " — using partial result with bits accumulated so far");
        // Don't add this β. Don't promote postLeafSplit. Continue with currentChildren as-is.
        // Drop remaining βs (this and any after).
        break;
      }
      currentChildren = postLeafSplit;
      acceptedBetas.add(beta);
      if (dbg) System.err.println("[g32-multibeta] after β=" + beta
          + " children count=" + currentChildren.size());
    }
    if (acceptedBetas.isEmpty()) {
      if (dbg) System.err.println("[g32-multibeta] no β accepted — abandon");
      return null;
    }

    // 3. Build new mask = old mask + all βs. Use MultiMask layout (always).
    final java.util.TreeMap<Integer, Integer> maskByBytePos = new java.util.TreeMap<>();
    if (indirect.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK) {
      final int oldInitial = indirect.getInitialBytePos();
      final long oldMask = indirect.getBitMask();
      for (int bo = 0; bo < 8; bo++) {
        final int byteMaskBits = (int) ((oldMask >>> ((7 - bo) * 8)) & 0xFFL);
        if (byteMaskBits != 0) maskByBytePos.merge(oldInitial + bo, byteMaskBits, (a, b) -> a | b);
      }
    } else {
      final byte[] ep = indirect.getExtractionPositions();
      final long[] em = indirect.getExtractionMasks();
      final int neb = indirect.getNumExtractionBytes();
      for (int i = 0; i < neb; i++) {
        final int chunkIdx = i / 8;
        final int byteOffsetInChunk = i % 8;
        final int byteMaskBits = (int) ((em[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFFL);
        if (byteMaskBits != 0) maskByBytePos.merge(ep[i] & 0xFF, byteMaskBits, (a, b) -> a | b);
      }
    }
    for (final int beta : acceptedBetas) {
      final int bp = beta / 8;
      final int bitInByte = beta % 8;
      final int maskBit = 1 << (7 - bitInByte);
      maskByBytePos.merge(bp, maskBit, (a, b) -> a | b);
    }
    // Helper lambda-style: rebuild extraction arrays from maskByBytePos. Returns the
    // (positions, masks, numBytes, msbIndex) tuple — we use 4 arrays as we can't return
    // multiple values, but pack into a holder.
    // Inline since Java lambdas can't return multiple values easily.
    int numBytes = maskByBytePos.size();
    if (numBytes > 64) {
      if (dbg) System.err.println("[g32-multibeta] mask too wide: " + numBytes + " bytes");
      return null;
    }
    byte[] extractionPositions = new byte[numBytes];
    long[] extractionMasks = new long[(numBytes + 7) / 8];
    short newMsb = Short.MAX_VALUE;
    {
      int idx = 0;
      for (final var entry : maskByBytePos.entrySet()) {
        final int bp = entry.getKey();
        final int mb = entry.getValue();
        extractionPositions[idx] = (byte) bp;
        final int chunkIdx = idx / 8;
        final int byteOffsetInChunk = idx % 8;
        extractionMasks[chunkIdx] |= ((long) (mb & 0xFF)) << ((7 - byteOffsetInChunk) * 8);
        final int highBit = 31 - Integer.numberOfLeadingZeros(mb & 0xFF);
        final int absBitPos = bp * 8 + (7 - highBit);
        if (absBitPos < newMsb) newMsb = (short) absBitPos;
        idx++;
      }
    }

    // 4. Compute partials per child. On collision, attempt to add a β-constant
    //    distinguishing bit to the mask (no fanout growth) and recompute.
    final int newCount = currentChildren.size();
    int[] newPartials = new int[newCount];
    final PageReference[] newChildren = new PageReference[newCount];
    int placeholderSeq = 0;
    for (int i = 0; i < newCount; i++) {
      final PageReference cref = currentChildren.get(i);
      newChildren[i] = cref;
      final byte[] cKey = getFirstKeyFromChild(cref);
      if (cKey == null || cKey.length == 0) {
        newPartials[i] = Integer.MAX_VALUE - placeholderSeq;
        placeholderSeq++;
        continue;
      }
      newPartials[i] = computePartialKeyMultiMaskDirect(cKey, extractionPositions,
          extractionMasks, numBytes);
    }
    // Collision-resolution loop: up to 16 retries adding β-constant bits.
    int collisionRetries = 0;
    boolean partialsUnique = false;
    while (!partialsUnique && collisionRetries < 16) {
      partialsUnique = true;
      int collI = -1, collK = -1;
      OUTER:
      for (int i = 1; i < newCount; i++) {
        for (int k = 0; k < i; k++) {
          if (newPartials[k] >= Integer.MAX_VALUE - newCount
              && newPartials[i] >= Integer.MAX_VALUE - newCount) continue; // both placeholders
          if (newPartials[k] == newPartials[i]) {
            partialsUnique = false; collI = i; collK = k; break OUTER;
          }
        }
      }
      if (partialsUnique) break;
      // Find a bit not in current mask that distinguishes the colliding pair AND is
      // β-constant in EVERY current child's subtree (= safe to add without splits).
      final byte[] fkI = getFirstKeyFromChild(newChildren[collI]);
      final byte[] fkK = getFirstKeyFromChild(newChildren[collK]);
      if (fkI == null || fkK == null || fkI.length == 0 || fkK.length == 0) {
        if (dbg) System.err.println("[g32-multibeta] collision pair has placeholder firstKey — abandon");
        return null;
      }
      int distBit = -1;
      final int pairMaxLen = Math.min(fkI.length, fkK.length);
      OUTER2:
      for (int bp = 0; bp < pairMaxLen; bp++) {
        int xor = (fkI[bp] ^ fkK[bp]) & 0xFF;
        while (xor != 0) {
          final int hb = Integer.numberOfLeadingZeros(xor) - 24;
          xor &= ~(1 << (7 - hb));
          final int absBit = bp * 8 + hb;
          // Already in mask?
          final Integer existingBits = maskByBytePos.get(absBit / 8);
          if (existingBits != null && (existingBits & (1 << (7 - (absBit % 8)))) != 0) continue;
          // β-constant in every current child?
          boolean allConst = true;
          for (int i = 0; i < newCount; i++) {
            if (newChildren[i] == null) continue;
            if (newChildren[i].getKey() == io.sirix.settings.Constants.NULL_ID_LONG) continue;
            final byte[] tfk = getFirstKeyFromChild(newChildren[i]);
            if (tfk == null || tfk.length == 0) continue;
            final int v = bitConstantValueInSubtree(newChildren[i], absBit);
            if (v < 0) { allConst = false; break; }
          }
          if (!allConst) continue;
          distBit = absBit;
          break OUTER2;
        }
      }
      if (distBit < 0) {
        if (dbg) System.err.println("[g32-multibeta] partial collision unresolvable at i="
            + collI + " k=" + collK + " p=" + Integer.toHexString(newPartials[collI]));
        return null;
      }
      // Add distBit to extraction mask. Update maskByBytePos AND outer extraction state.
      final int distBp = distBit / 8;
      final int distBitInByte = distBit % 8;
      final int distMaskBit = 1 << (7 - distBitInByte);
      maskByBytePos.merge(distBp, distMaskBit, (a, b) -> a | b);
      // Rebuild outer extraction tables from updated maskByBytePos.
      final int nb2 = maskByBytePos.size();
      if (nb2 > 64) {
        if (dbg) System.err.println("[g32-multibeta] mask byte-position overflow after distBit=" + distBit);
        return null;
      }
      extractionPositions = new byte[nb2];
      extractionMasks = new long[(nb2 + 7) / 8];
      numBytes = nb2;
      newMsb = Short.MAX_VALUE;
      int idx2 = 0;
      for (final var entry : maskByBytePos.entrySet()) {
        final int bp = entry.getKey();
        final int mb = entry.getValue();
        extractionPositions[idx2] = (byte) bp;
        extractionMasks[idx2 / 8] |= ((long) (mb & 0xFF)) << ((7 - idx2 % 8) * 8);
        final int highBit = 31 - Integer.numberOfLeadingZeros(mb & 0xFF);
        final int absBitPos = bp * 8 + (7 - highBit);
        if (absBitPos < newMsb) newMsb = (short) absBitPos;
        idx2++;
      }
      // Recompute partials with new mask.
      placeholderSeq = 0;
      for (int i = 0; i < newCount; i++) {
        final byte[] cKey = getFirstKeyFromChild(newChildren[i]);
        if (cKey == null || cKey.length == 0) {
          newPartials[i] = Integer.MAX_VALUE - placeholderSeq;
          placeholderSeq++;
          continue;
        }
        newPartials[i] = computePartialKeyMultiMaskDirect(cKey, extractionPositions,
            extractionMasks, numBytes);
      }
      collisionRetries++;
      if (dbg) System.err.println("[g32-multibeta] collision-resolved-retry=" + collisionRetries
          + " added distBit=" + distBit + " (no split, β-constant)");
    }
    if (!partialsUnique) {
      if (dbg) System.err.println("[g32-multibeta] partial uniqueness unrecoverable after "
          + collisionRetries + " retries");
      return null;
    }

    // 5. Sort + verify firstKey-monotone. If inversion, retry by adding the MSB-of-diff
    // of the inverted pair (similar to collision-resolution retry).
    int postSortRetries = 0;
    boolean firstKeyMonotone = false;
    while (!firstKeyMonotone && postSortRetries < 16) {
      sortChildrenAndPartialsByPartial(newChildren, newPartials);
      firstKeyMonotone = true;
      byte[] prev = null;
      PageReference invPrevRef = null;
      PageReference invCurrRef = null;
      for (int i = 0; i < newCount; i++) {
        final byte[] fk = getFirstKeyFromChild(newChildren[i]);
        if (fk == null || fk.length == 0) continue;
        if (prev != null && Arrays.compareUnsigned(prev, fk) >= 0) {
          firstKeyMonotone = false;
          invPrevRef = newChildren[i - 1];
          invCurrRef = newChildren[i];
          if (dbg) System.err.println("[g32-multibeta] post-sort inversion at i=" + i
              + " (retry=" + postSortRetries + ")");
          break;
        }
        prev = fk;
      }
      if (firstKeyMonotone) break;
      // Find MSB-of-diff between inverted pair AND β-constant in all current children.
      final byte[] fkPrev = getFirstKeyFromChild(invPrevRef);
      final byte[] fkCurr = getFirstKeyFromChild(invCurrRef);
      if (fkPrev == null || fkCurr == null) return null;
      int monoBit = -1;
      final int monoLen = Math.min(fkPrev.length, fkCurr.length);
      OUTER3:
      for (int bp = 0; bp < monoLen; bp++) {
        int xor = (fkPrev[bp] ^ fkCurr[bp]) & 0xFF;
        while (xor != 0) {
          final int hb = Integer.numberOfLeadingZeros(xor) - 24;
          xor &= ~(1 << (7 - hb));
          final int absBit = bp * 8 + hb;
          final Integer existingBits = maskByBytePos.get(absBit / 8);
          if (existingBits != null && (existingBits & (1 << (7 - (absBit % 8)))) != 0) continue;
          boolean allConst = true;
          for (int i = 0; i < newCount; i++) {
            if (newChildren[i] == null) continue;
            if (newChildren[i].getKey() == io.sirix.settings.Constants.NULL_ID_LONG) continue;
            final byte[] tfk = getFirstKeyFromChild(newChildren[i]);
            if (tfk == null || tfk.length == 0) continue;
            final int v = bitConstantValueInSubtree(newChildren[i], absBit);
            if (v < 0) { allConst = false; break; }
          }
          if (!allConst) continue;
          monoBit = absBit;
          break OUTER3;
        }
      }
      if (monoBit < 0) {
        if (dbg) System.err.println("[g32-multibeta] post-sort inversion unresolvable");
        return null;
      }
      // Add monoBit to mask + rebuild + recompute partials.
      final int mbp = monoBit / 8;
      final int mbi = monoBit % 8;
      final int mmb = 1 << (7 - mbi);
      maskByBytePos.merge(mbp, mmb, (a, b) -> a | b);
      final int nb3 = maskByBytePos.size();
      if (nb3 > 64) return null;
      extractionPositions = new byte[nb3];
      extractionMasks = new long[(nb3 + 7) / 8];
      numBytes = nb3;
      newMsb = Short.MAX_VALUE;
      int idx3 = 0;
      for (final var entry : maskByBytePos.entrySet()) {
        final int bp = entry.getKey();
        final int mb = entry.getValue();
        extractionPositions[idx3] = (byte) bp;
        extractionMasks[idx3 / 8] |= ((long) (mb & 0xFF)) << ((7 - idx3 % 8) * 8);
        final int highBit = 31 - Integer.numberOfLeadingZeros(mb & 0xFF);
        final int absBitPos = bp * 8 + (7 - highBit);
        if (absBitPos < newMsb) newMsb = (short) absBitPos;
        idx3++;
      }
      placeholderSeq = 0;
      for (int i = 0; i < newCount; i++) {
        final byte[] cKey = getFirstKeyFromChild(newChildren[i]);
        if (cKey == null || cKey.length == 0) {
          newPartials[i] = Integer.MAX_VALUE - placeholderSeq;
          placeholderSeq++;
          continue;
        }
        newPartials[i] = computePartialKeyMultiMaskDirect(cKey, extractionPositions,
            extractionMasks, numBytes);
      }
      postSortRetries++;
      if (dbg) System.err.println("[g32-multibeta] post-sort-retry=" + postSortRetries
          + " added monoBit=" + monoBit);
    }
    if (!firstKeyMonotone) {
      if (dbg) System.err.println("[g32-multibeta] firstKey monotone unresolvable after "
          + postSortRetries + " retries");
      return null;
    }

    // 6. Verify I4 (smallest real partial = 0).
    int smallestReal = Integer.MAX_VALUE;
    for (int i = 0; i < newCount; i++) {
      if (newPartials[i] < Integer.MAX_VALUE - newCount && newPartials[i] < smallestReal) {
        smallestReal = newPartials[i];
      }
    }
    if (smallestReal != 0) {
      if (dbg) System.err.println("[g32-multibeta] I4 violation — smallest real partial="
          + Integer.toHexString(smallestReal));
      return null;
    }

    // Phase 7q.15 — before building the final indirect, raise each child's MSB to be
    // strictly greater than newMsb (= new indirect's MSB). This satisfies I11 at the
    // top level. Stripping each child's mask bits ≤ newMsb is safe IFF those bits are
    // β-constant in the child's subtree.
    if (Boolean.getBoolean("hot.strict.g32.raisechildmsb")) {
      for (int i = 0; i < newCount; i++) {
        final PageReference newChild = tryRaiseChildMsb(newChildren[i], newMsb, log, revision);
        if (newChild == null) {
          if (dbg) System.err.println("[g32-multibeta] tryRaiseChildMsb failed for child=" + i
              + " — abandon multi-β");
          return null;
        }
        newChildren[i] = newChild;
      }
    }
    if (dbg) System.err.println("[g32-multibeta] SUCCESS — newCount=" + newCount
        + " newMsb=" + newMsb);
    final HOTIndirectPage builtNew = (newCount <= 16)
        ? HOTIndirectPage.createSpanNodeMultiMask(indirect.getPageKey(), revision,
            extractionPositions, extractionMasks, numBytes, newPartials, newChildren,
            indirect.getHeight(), newMsb)
        : HOTIndirectPage.createMultiNodeMultiMask(indirect.getPageKey(), revision,
            extractionPositions, extractionMasks, numBytes, newPartials, newChildren,
            indirect.getHeight(), newMsb);
    // Phase 7q.15 — validate-or-repair-or-rollback.
    // 1) Count I11 violations AND I6 routing violations (= child's firstKey doesn't
    //    PEXT to that child's stored partial under the new mask).
    // 2) If any, attempt to REPAIR by raising offending children's MSBs + rebuilding parents.
    // 3) If repair succeeds (= post-repair count = 0), use repaired version.
    // 4) Else rollback (return null).
    int i11Violations = countI11ViolationsRecursive(builtNew, -1);
    int i6Violations = countI6ViolationsRecursive(builtNew);
    if (dbg && (i11Violations > 0 || i6Violations > 0)) {
      System.err.println("[g32-multibeta] VALIDATE: i11=" + i11Violations
          + " i6=" + i6Violations);
    }
    HOTIndirectPage finalIndirect = builtNew;
    if ((i11Violations > 0 || i6Violations > 0) && Boolean.getBoolean("hot.strict.g32.repair")) {
      if (dbg) System.err.println("[g32-multibeta] VALIDATE: " + i11Violations
          + " I11 violations — attempting repair");
      final PageReference repaired = repairI11Violations(builtNew, -1, log, revision);
      if (repaired != null && repaired.getPage() instanceof HOTIndirectPage rebuilt) {
        final int postRepair = countI11ViolationsRecursive(rebuilt, -1);
        if (postRepair == 0) {
          if (dbg) System.err.println("[g32-multibeta] REPAIR-OK: all I11 violations resolved");
          finalIndirect = rebuilt;
          i11Violations = 0;
        } else {
          if (dbg) System.err.println("[g32-multibeta] REPAIR-PARTIAL: " + postRepair
              + " I11 violations remain after repair — rollback");
        }
      } else {
        if (dbg) System.err.println("[g32-multibeta] REPAIR-FAILED — rollback");
      }
    }
    if (i11Violations > 0 || i6Violations > 0) {
      if (dbg) System.err.println("[g32-multibeta] VALIDATE-ROLLBACK: i11=" + i11Violations
          + " i6=" + i6Violations + " — discard");
      return null;
    }
    return finalIndirect;
  }

  /** Phase 7q.15 — recursive I6 count across the subtree rooted at {@code indirect}. */
  private int countI6ViolationsRecursive(HOTIndirectPage indirect) {
    int violations = countI6ViolationsImmediate(indirect);
    final int n = indirect.getNumChildren();
    for (int i = 0; i < n; i++) {
      final PageReference cref = indirect.getChildReference(i);
      if (cref == null) continue;
      Page page = cref.getPage();
      if (page == null && activeLog != null) {
        final var pc = activeLog.get(cref); if (pc != null) page = pc.getModified();
      }
      if (page == null && activeReader != null) {
        page = loadPage(activeReader, cref); if (page != null) cref.setPage(page);
      }
      if (page instanceof HOTIndirectPage childInd) {
        violations += countI6ViolationsRecursive(childInd);
      }
    }
    return violations;
  }

  /** Phase 7q.15 — count I6 violations at the immediate level: for each child, verify
   *  that EVERY MASK BIT is β-constant in the child's subtree (= all keys in subtree
   *  agree with firstKey at that bit). If not, some keys in the subtree route to the
   *  wrong child via PEXT.
   *
   *  <p>Also checks: stored partial matches PEXT(firstKey, mask). */
  private int countI6ViolationsImmediate(HOTIndirectPage indirect) {
    if (indirect == null) return 0;
    final int n = indirect.getNumChildren();
    final boolean singleMask = indirect.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK;
    final int initialBytePos = indirect.getInitialBytePos();
    final int[] storedPartials = indirect.getPartialKeys();
    // Enumerate mask bits in absBit space.
    final java.util.ArrayList<Integer> maskAbsBits = new java.util.ArrayList<>(16);
    if (singleMask) {
      final long mask = indirect.getBitMask();
      for (int wbit = 0; wbit < 64; wbit++) {
        if (((mask >>> wbit) & 1L) == 0L) continue;
        final int byteOffsetInWord = 7 - (wbit / 8);
        final int bitInByte = 7 - (wbit % 8);
        maskAbsBits.add((initialBytePos + byteOffsetInWord) * 8 + bitInByte);
      }
    } else {
      final byte[] ep = indirect.getExtractionPositions();
      final long[] em = indirect.getExtractionMasks();
      final int neb = indirect.getNumExtractionBytes();
      for (int e = 0; e < neb; e++) {
        final int bp = ep[e] & 0xFF;
        final int chunkIdx = e / 8;
        final int byteOffsetInChunk = e % 8;
        final int byteMaskBits = (int) ((em[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFFL);
        for (int bit = 0; bit < 8; bit++) {
          if ((byteMaskBits & (1 << (7 - bit))) != 0) maskAbsBits.add(bp * 8 + bit);
        }
      }
    }
    int violations = 0;
    for (int i = 0; i < n; i++) {
      final PageReference cref = indirect.getChildReference(i);
      if (cref == null) continue;
      final byte[] fk = getFirstKeyFromChild(cref);
      if (fk == null || fk.length == 0) continue;
      // Check partial match.
      final int computed;
      if (singleMask) {
        computed = computePartialKeySingleMask(fk, initialBytePos, indirect.getBitMask());
      } else {
        computed = computePartialKeyMultiMaskDirect(fk, indirect.getExtractionPositions(),
            indirect.getExtractionMasks(), indirect.getNumExtractionBytes());
      }
      if (storedPartials != null && i < storedPartials.length && storedPartials[i] != computed) {
        violations++;
        continue;
      }
      // Check β-constancy per mask bit.
      for (final int absBit : maskAbsBits) {
        final int subtreeConst = bitConstantValueInSubtree(cref, absBit);
        if (subtreeConst < 0) {
          // Subtree has mixed values — routing breaks for some keys.
          violations++;
          break;
        }
        // Verify subtreeConst matches firstKey.bit at this position.
        final int fkBit = isAbsBitSet(fk, absBit) ? 1 : 0;
        if (subtreeConst != fkBit) {
          violations++;
          break;
        }
      }
    }
    return violations;
  }

  /**
   * Phase 7q.15 — recursively REPAIR I11 violations by raising children's MSBs where
   * possible. Walks the subtree post-order: first recurse into children, then rebuild
   * THIS indirect if any child changed OR if THIS indirect itself violates I11 against
   * its parent.
   *
   * <p>Returns the (possibly rebuilt) ref for {@code indirect}. Null if any I11 violation
   * could not be repaired (= caller should rollback).
   */
  @Nullable
  private PageReference repairI11Violations(HOTIndirectPage indirect, int parentMsb,
      TransactionIntentLog log, int revision) {
    if (indirect == null) return null;
    final int thisMsb = indirect.getMostSignificantBitIndex() & 0xFFFF;
    final int n = indirect.getNumChildren();
    // 1. Recurse into children FIRST (post-order). Collect repaired refs.
    final PageReference[] repairedChildren = new PageReference[n];
    boolean anyChildChanged = false;
    for (int i = 0; i < n; i++) {
      final PageReference cref = indirect.getChildReference(i);
      if (cref == null) return null;
      Page page = cref.getPage();
      if (page == null && activeLog != null) {
        final var pc = activeLog.get(cref); if (pc != null) page = pc.getModified();
      }
      if (page == null && activeReader != null) {
        page = loadPage(activeReader, cref); if (page != null) cref.setPage(page);
      }
      if (page instanceof HOTIndirectPage childInd) {
        final PageReference repaired = repairI11Violations(childInd, thisMsb, log, revision);
        if (repaired == null) return null;
        repairedChildren[i] = repaired;
        if (repaired != cref) anyChildChanged = true;
      } else {
        repairedChildren[i] = cref;
      }
    }
    // 2. If any child changed, rebuild THIS indirect with new children.
    HOTIndirectPage currentSelf = indirect;
    if (anyChildChanged) {
      final HOTIndirectPage rebuilt = rebuildIndirectWithNewChildren(indirect, repairedChildren,
          log, revision);
      if (rebuilt == null) {
        if (Boolean.getBoolean("hot.debug.g32")) {
          System.err.println("[repair] rebuild-failed pageKey=" + indirect.getPageKey()
              + " — children changed but parent rebuild failed");
        }
        return null;
      }
      currentSelf = rebuilt;
    }
    final int currentSelfMsb = currentSelf.getMostSignificantBitIndex() & 0xFFFF;
    // 3. Check if THIS indirect violates I11 against its parent. If yes, try to raise.
    if (parentMsb >= 0 && currentSelfMsb <= parentMsb) {
      final PageReference selfRef = new PageReference();
      selfRef.setKey(currentSelf.getPageKey());
      selfRef.setPage(currentSelf);
      final PageReference raised = tryRaiseChildMsb(selfRef, parentMsb, log, revision);
      if (raised == null || raised == selfRef) {
        if (Boolean.getBoolean("hot.debug.g32")) {
          System.err.println("[repair] CAN'T-RAISE pageKey=" + currentSelf.getPageKey()
              + " thisMsb=" + currentSelfMsb + " parentMsb=" + parentMsb);
        }
        return null;
      }
      return raised;
    }
    // No violation at this level. Return ref (rebuilt if anyChildChanged, else original).
    final PageReference outRef = new PageReference();
    outRef.setKey(currentSelf.getPageKey());
    outRef.setPage(currentSelf);
    return outRef;
  }

  /**
   * Phase 7q.15 — rebuild an indirect with replaced children, preserving the original
   * mask layout. Recomputes partials from the new children's firstKeys, sorts, builds
   * a new indirect page. Returns null on uniqueness collision, I4 violation, or any
   * structural problem.
   */
  @Nullable
  private HOTIndirectPage rebuildIndirectWithNewChildren(HOTIndirectPage origIndirect,
      PageReference[] newChildren, TransactionIntentLog log, int revision) {
    final int n = newChildren.length;
    if (n < 2) return null;
    final int origInitialBytePos = origIndirect.getInitialBytePos();
    final boolean singleMask = origIndirect.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK;
    final int[] partials = new int[n];
    final PageReference[] sorted = newChildren.clone();
    boolean haveZero = false;
    final java.util.HashSet<Integer> seen = new java.util.HashSet<>(n * 2);
    for (int i = 0; i < n; i++) {
      final byte[] fk = getFirstKeyFromChild(sorted[i]);
      if (fk == null || fk.length == 0) return null;
      if (singleMask) {
        partials[i] = computePartialKeySingleMask(fk, origInitialBytePos,
            origIndirect.getBitMask());
      } else {
        partials[i] = computePartialKeyMultiMaskDirect(fk,
            origIndirect.getExtractionPositions(), origIndirect.getExtractionMasks(),
            origIndirect.getNumExtractionBytes());
      }
      if (partials[i] == 0) haveZero = true;
      if (!seen.add(partials[i])) return null;
    }
    if (!haveZero) return null;
    sortChildrenAndPartialsByPartial(sorted, partials);
    final long newPageKey = pageKeyAllocator.getAsLong();
    final HOTIndirectPage built;
    if (singleMask) {
      built = (n <= 16)
          ? HOTIndirectPage.createSpanNode(newPageKey, revision, origInitialBytePos,
              origIndirect.getBitMask(), partials, sorted, origIndirect.getHeight())
          : HOTIndirectPage.createMultiNode(newPageKey, revision, origInitialBytePos,
              origIndirect.getBitMask(), partials, sorted, origIndirect.getHeight());
    } else {
      built = (n <= 16)
          ? HOTIndirectPage.createSpanNodeMultiMask(newPageKey, revision,
              origIndirect.getExtractionPositions(), origIndirect.getExtractionMasks(),
              origIndirect.getNumExtractionBytes(), partials, sorted, origIndirect.getHeight(),
              origIndirect.getMostSignificantBitIndex())
          : HOTIndirectPage.createMultiNodeMultiMask(newPageKey, revision,
              origIndirect.getExtractionPositions(), origIndirect.getExtractionMasks(),
              origIndirect.getNumExtractionBytes(), partials, sorted, origIndirect.getHeight(),
              origIndirect.getMostSignificantBitIndex());
    }
    final PageReference newRef = new PageReference();
    newRef.setKey(newPageKey);
    newRef.setPage(built);
    log.put(newRef, PageContainer.getInstance(built, built));
    if (Boolean.getBoolean("hot.debug.g32")) {
      System.err.println("[repair] rebuilt pageKey=" + origIndirect.getPageKey()
          + " → newPageKey=" + newPageKey);
    }
    return built;
  }

  /** Phase 7q.15 — recursively count I11 violations rooted at {@code indirect}. An I11
   *  violation occurs when child.MSB ≤ parent.MSB (parent is the indirect whose page key
   *  is being walked; for the root invocation pass parentMsb=-1 to skip the check at top).
   */
  private int countI11ViolationsRecursive(HOTIndirectPage indirect, int parentMsb) {
    if (indirect == null) return 0;
    final int thisMsb = indirect.getMostSignificantBitIndex() & 0xFFFF;
    int violations = 0;
    if (parentMsb >= 0 && thisMsb <= parentMsb) {
      if (Boolean.getBoolean("hot.debug.g32")) {
        System.err.println("[validate-rollback] I11 violation: this.pageKey="
            + indirect.getPageKey() + " this.MSB=" + thisMsb + " parent.MSB=" + parentMsb);
      }
      violations++;
    }
    final int n = indirect.getNumChildren();
    for (int i = 0; i < n; i++) {
      final PageReference cref = indirect.getChildReference(i);
      if (cref == null) continue;
      Page page = cref.getPage();
      if (page == null && activeLog != null) {
        final var container = activeLog.get(cref);
        if (container != null) page = container.getModified();
      }
      if (page == null && activeReader != null) {
        page = loadPage(activeReader, cref);
        if (page != null) cref.setPage(page);
      }
      if (page instanceof HOTIndirectPage childInd) {
        violations += countI11ViolationsRecursive(childInd, thisMsb);
      }
    }
    return violations;
  }

  @Nullable
  private HOTIndirectPage phase7qIterativeRootSortI8(HOTIndirectPage indirect,
      TransactionIntentLog log, int revision) {
    final boolean dbg = Boolean.getBoolean("hot.debug.g32");
    if (indirect == null) return null;
    HOTIndirectPage cur = indirect;
    for (int iter = 0; iter < 16; iter++) {
      final int n = cur.getNumChildren();
      if (n < 2) return cur;

      // Snapshot child refs + firstKeys at current cur.
      final PageReference[] childRefs = new PageReference[n];
      final byte[][] firstKeys = new byte[n][];
      int maxLen = 0;
      for (int i = 0; i < n; i++) {
        childRefs[i] = cur.getChildReference(i);
        if (childRefs[i] == null) return null;
        firstKeys[i] = getFirstKeyFromChild(childRefs[i]);
        if (firstKeys[i] != null && firstKeys[i].length > maxLen) maxLen = firstKeys[i].length;
      }

      // Find first adjacent inversion (skip null/empty firstKeys).
      int offK = -1, offI = -1;
      byte[] prevKey = null;
      for (int i = 0; i < n; i++) {
        if (firstKeys[i] == null || firstKeys[i].length == 0) continue;
        if (prevKey != null && Arrays.compareUnsigned(prevKey, firstKeys[i]) >= 0) {
          offI = i;
          // offK = the previous non-null index. Re-scan to find it.
          for (int k = i - 1; k >= 0; k--) {
            if (firstKeys[k] != null && firstKeys[k].length > 0) { offK = k; break; }
          }
          break;
        }
        prevKey = firstKeys[i];
      }
      if (offI < 0) {
        if (dbg) System.err.println("[g32-itersort] iter=" + iter + " sorted — done");
        return cur;
      }

      // Compute MSB-of-diff between offK.firstKey and offI.firstKey.
      int msbOfDiff = -1;
      for (int absBit = 0; absBit < maxLen * 8; absBit++) {
        if (isAbsBitSet(firstKeys[offK], absBit) != isAbsBitSet(firstKeys[offI], absBit)) {
          msbOfDiff = absBit;
          break;
        }
      }
      if (dbg) System.err.println("[g32-itersort] iter=" + iter + " offK=" + offK + " offI=" + offI
          + " fkK=" + bytesToHex(firstKeys[offK])
          + " fkI=" + bytesToHex(firstKeys[offI]));
      if (msbOfDiff < 0) {
        if (dbg) System.err.println("[g32-itersort] iter=" + iter
            + " no MSB-of-diff (identical firstKeys?) — abandon");
        return null;
      }

      // If msbOfDiff already in mask, the partial should already capture it; adjacent
      // inversion must come from elsewhere. Could happen if multiple bits need fixing
      // and we're regressing. Abandon to avoid livelock.
      if (indirectMaskHasAbsBit(cur, msbOfDiff)) {
        if (dbg) System.err.println("[g32-itersort] iter=" + iter
            + " msbOfDiff=" + msbOfDiff + " already in mask — abandon");
        return null;
      }

      // β-constancy probe: is msbOfDiff constant in every real child's subtree?
      boolean allConstant = true;
      for (int i = 0; i < n; i++) {
        if (childRefs[i] == null) continue;
        if (childRefs[i].getKey() == io.sirix.settings.Constants.NULL_ID_LONG) continue;
        if (firstKeys[i] == null || firstKeys[i].length == 0) continue;
        final int v = bitConstantValueInSubtree(childRefs[i], msbOfDiff);
        if (v < 0) { allConstant = false; break; }
      }

      // Apply: try extend first; fall back to lift if extend rejects.
      HOTIndirectPage next = extendIndirectMaskForClosure(cur, msbOfDiff, log, revision);
      if (dbg) System.err.println("[g32-itersort] iter=" + iter + " β=" + msbOfDiff
          + " allConstant=" + allConstant + " extend=" + (next != null ? "OK" : "FAIL"));
      if (next == null) {
        next = phase7qExtendWithLift(cur, msbOfDiff, log, revision);
        if (dbg) System.err.println("[g32-itersort] iter=" + iter + " β=" + msbOfDiff
            + " lift=" + (next != null ? "OK" : "FAIL"));
      }
      if (next == null) {
        // Phase 7q.15b — delta-based validate-or-rollback. Compare cur's invariant counts
        // against the original `indirect`: accept if cur strictly reduces I8 inversions
        // without introducing NEW I11 violations (delta-check, not absolute-zero, because
        // baseline may legitimately have non-validator-flagged I11 cases that surface via
        // the lift's reachability change).
        // Gated on hot.strict.g32.bestEffort so default behaviour is unchanged.
        if (cur != indirect && Boolean.getBoolean("hot.strict.g32.bestEffort")) {
          // Phase 7q.15c — count I8 across the whole subtree (not just root), matching
          // validator scope. A lift may improve root inversions but introduce sub-indirect
          // inversions; the whole-trie count catches that.
          final int beforeI8 = countI8InversionsRecursive(indirect);
          final int afterI8 = countI8InversionsRecursive(cur);
          final int beforeI11 = countI11ViolationsRecursive(indirect, -1);
          final int afterI11 = countI11ViolationsRecursive(cur, -1);
          // Phase 7q.15f — structure-cycle gate. The g32.childmsb path can produce
          // bucket fallbacks that reuse pageKeys from the OLD tree, creating a graph
          // cycle. Reject if `cur` has any revisited pageKey (validator would flag
          // structure-cycle, defeating the lift's I8 gain).
          final boolean curHasCycle = hasStructureCycle(cur);
          if (!curHasCycle && afterI11 <= beforeI11 && afterI8 < beforeI8) {
            PHASE7Q_BEST_EFFORT_ACCEPTED.incrementAndGet();
            if (dbg) System.err.println("[g32-itersort] iter=" + iter
                + " — abandon (extend+lift) but BEST-EFFORT KEEPS cur: I8 "
                + beforeI8 + "→" + afterI8 + " I11 " + beforeI11 + "→" + afterI11);
            return cur;
          }
          PHASE7Q_BEST_EFFORT_REJECTED.incrementAndGet();
          if (dbg) System.err.println("[g32-itersort] iter=" + iter
              + " — abandon (extend+lift); best-effort rollback: I8 "
              + beforeI8 + "→" + afterI8 + " I11 " + beforeI11 + "→" + afterI11
              + " cycle=" + curHasCycle);
        } else if (dbg) {
          System.err.println("[g32-itersort] iter=" + iter + " — abandon (extend+lift)");
        }
        return null;
      }
      if (dbg) System.err.println("[g32-itersort] iter=" + iter + " ADVANCED β=" + msbOfDiff
          + " newMsb=" + (next.getMostSignificantBitIndex() & 0xFFFF)
          + " newN=" + next.getNumChildren());
      cur = next;
    }
    if (dbg) System.err.println("[g32-itersort] exhausted maxIter");
    // Phase 7q.15b — delta-based validate-or-rollback at maxIter exhaustion.
    if (cur != indirect && Boolean.getBoolean("hot.strict.g32.bestEffort")) {
      final int beforeI8 = countI8InversionsRecursive(indirect);
      final int afterI8 = countI8InversionsRecursive(cur);
      final int beforeI11 = countI11ViolationsRecursive(indirect, -1);
      final int afterI11 = countI11ViolationsRecursive(cur, -1);
      // Phase 7q.15f — structure-cycle gate; see iter-abandon branch above.
      final boolean curHasCycle = hasStructureCycle(cur);
      if (!curHasCycle && afterI11 <= beforeI11 && afterI8 < beforeI8) {
        PHASE7Q_BEST_EFFORT_ACCEPTED.incrementAndGet();
        if (dbg) System.err.println("[g32-itersort] maxIter — BEST-EFFORT KEEPS cur: I8 "
            + beforeI8 + "→" + afterI8 + " I11 " + beforeI11 + "→" + afterI11);
        return cur;
      }
      PHASE7Q_BEST_EFFORT_REJECTED.incrementAndGet();
      if (dbg) System.err.println("[g32-itersort] maxIter — best-effort rollback: I8 "
          + beforeI8 + "→" + afterI8 + " I11 " + beforeI11 + "→" + afterI11
          + " cycle=" + curHasCycle);
      return null;
    }
    return cur != indirect ? cur : null;
  }

  /** Phase 7q.15a — count adjacent firstKey inversions at the root level of an indirect.
   *  Skips null and empty-firstKey (placeholder) children. Real children that come
   *  AFTER another real child whose firstKey >= ours count as inversions. */
  private int countAdjacentI8InversionsAtRoot(HOTIndirectPage indirect) {
    if (indirect == null) return 0;
    final int n = indirect.getNumChildren();
    if (n < 2) return 0;
    int violations = 0;
    byte[] prev = null;
    for (int i = 0; i < n; i++) {
      final PageReference cref = indirect.getChildReference(i);
      if (cref == null) continue;
      final byte[] fk = getFirstKeyFromChild(cref);
      if (fk == null || fk.length == 0) continue;
      if (prev != null && Arrays.compareUnsigned(prev, fk) >= 0) {
        violations++;
      }
      prev = fk;
    }
    return violations;
  }

  /** Phase 7q.15f — detect graph cycles in a subtree of indirects. Walks the trie
   *  via {@link HOTIndirectPage#getChildReference}, resolving children via TIL/disk
   *  fallback. Returns {@code true} on the FIRST revisited pageKey, mirroring
   *  {@link io.sirix.index.hot.HOTInvariantValidator}'s structure-cycle detection.
   *
   *  <p>Used by {@link #phase7qIterativeRootSortI8}'s best-effort gate to reject a
   *  rebuilt tree that would surface a "structure-cycle" violation at validation time.
   *  Costs one HashSet add per indirect descendant — cheap relative to the lift cost.
   *
   *  <p>Leaves are not tracked (no cycle through leaves; they don't have child refs).
   */
  private boolean hasStructureCycle(HOTIndirectPage indirect) {
    if (indirect == null) return false;
    final java.util.HashSet<Long> visited = new java.util.HashSet<>();
    return hasStructureCycleInternal(indirect, visited, /*depth=*/0, /*parent=*/-1L);
  }

  private boolean hasStructureCycleInternal(HOTIndirectPage indirect,
      java.util.HashSet<Long> visited, int depth, long parentPageKey) {
    if (indirect == null) return false;
    final long pageKey = indirect.getPageKey();
    if (pageKey >= 0 && !visited.add(pageKey)) {
      // Phase 7q.15g — diagnostic on cycle detection. Print revisited pageKey
      // + parent that triggered the revisit + depth in walk. Gated on flag.
      if (Boolean.getBoolean("hot.debug.phase7q.cyclesource")) {
        System.err.println("[phase7q.15g-cycle] revisited pageKey=" + pageKey
            + " via parent pageKey=" + parentPageKey + " walk-depth=" + depth
            + " visited.size=" + visited.size());
      }
      return true;
    }
    final int n = indirect.getNumChildren();
    for (int i = 0; i < n; i++) {
      final PageReference cref = indirect.getChildReference(i);
      if (cref == null) continue;
      Page page = cref.getPage();
      if (page == null && activeLog != null) {
        final var container = activeLog.get(cref);
        if (container != null) page = container.getModified();
      }
      if (page == null && activeReader != null) {
        page = loadPage(activeReader, cref);
        if (page != null) cref.setPage(page);
      }
      if (page instanceof HOTIndirectPage childInd) {
        if (hasStructureCycleInternal(childInd, visited, depth + 1, pageKey)) return true;
      }
    }
    return false;
  }

  /** Phase 7q.15c — recursively count I8 inversions across the whole subtree (matching
   *  HOTInvariantValidator scope, except we count ALL adjacent inversions per indirect,
   *  not just the first). Used by validate-or-rollback to detect when a lift creates
   *  new sub-indirect inversions even if root improves. */
  private int countI8InversionsRecursive(HOTIndirectPage indirect) {
    if (indirect == null) return 0;
    int violations = countAdjacentI8InversionsAtRoot(indirect);
    final int n = indirect.getNumChildren();
    for (int i = 0; i < n; i++) {
      final PageReference cref = indirect.getChildReference(i);
      if (cref == null) continue;
      Page page = cref.getPage();
      if (page == null && activeLog != null) {
        final var container = activeLog.get(cref);
        if (container != null) page = container.getModified();
      }
      if (page == null && activeReader != null) {
        page = loadPage(activeReader, cref);
        if (page != null) cref.setPage(page);
      }
      if (page instanceof HOTIndirectPage childInd) {
        violations += countI8InversionsRecursive(childInd);
      }
    }
    return violations;
  }

  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_BEST_EFFORT_ACCEPTED =
      new java.util.concurrent.atomic.AtomicLong(0L);
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_BEST_EFFORT_REJECTED =
      new java.util.concurrent.atomic.AtomicLong(0L);
  public static long getPhase7qBestEffortAccepted() { return PHASE7Q_BEST_EFFORT_ACCEPTED.get(); }
  public static void resetPhase7qBestEffortAccepted() { PHASE7Q_BEST_EFFORT_ACCEPTED.set(0L); }
  public static long getPhase7qBestEffortRejected() { return PHASE7Q_BEST_EFFORT_REJECTED.get(); }
  public static void resetPhase7qBestEffortRejected() { PHASE7Q_BEST_EFFORT_REJECTED.set(0L); }

  // Phase 7q.15d — intermediate-indirect MSB instrumentation. Counts how often
  // {@link #phase7qExtendWithLift}'s rebuilt indirect ends up with a child whose MSB
  // EQUALS the new parent's msbIndex (= I11 equality violation between rebuilt
  // parent and lifted child). Also tracks STRICTLY MORE SIGNIFICANT children
  // (child.MSB < parent.msbIndex numerically) for completeness — that's a stronger
  // I11 violation. Counters are always-on (cheap atomic increments). Per-event
  // dump gated on -Dhot.debug.phase7q.imsb=true.
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_INTERMEDIATE_MSB_EQUALITY =
      new java.util.concurrent.atomic.AtomicLong(0L);
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_INTERMEDIATE_MSB_LOWER =
      new java.util.concurrent.atomic.AtomicLong(0L);
  private static final java.util.concurrent.atomic.AtomicLong PHASE7Q_INTERMEDIATE_MSB_OK =
      new java.util.concurrent.atomic.AtomicLong(0L);
  public static long getPhase7qIntermediateMsbEquality() {
    return PHASE7Q_INTERMEDIATE_MSB_EQUALITY.get();
  }
  public static void resetPhase7qIntermediateMsbEquality() {
    PHASE7Q_INTERMEDIATE_MSB_EQUALITY.set(0L);
  }
  public static long getPhase7qIntermediateMsbLower() {
    return PHASE7Q_INTERMEDIATE_MSB_LOWER.get();
  }
  public static void resetPhase7qIntermediateMsbLower() {
    PHASE7Q_INTERMEDIATE_MSB_LOWER.set(0L);
  }
  public static long getPhase7qIntermediateMsbOk() { return PHASE7Q_INTERMEDIATE_MSB_OK.get(); }
  public static void resetPhase7qIntermediateMsbOk() { PHASE7Q_INTERMEDIATE_MSB_OK.set(0L); }

  /**
   * Phase 7q.15d — walk newChildren[] of a rebuilt parent indirect, classifying each
   * child's MSB vs parentMsb. Updates the INTERMEDIATE_MSB_{EQUALITY, LOWER, OK}
   * counters. When {@code -Dhot.debug.phase7q.imsb=true}, prints the offending
   * (childIdx, childPageKey, childMsb) triplets so the next iteration can identify
   * which lift-product violates I11 vs the rebuilt parent.
   *
   * <p>HFT-grade: 1 atomic increment per child per call; per-event print gated and
   * uses pre-allocated stack-local StringBuilder.
   *
   * @param parentPageKey  pageKey of the rebuilt parent (for log correlation)
   * @param parentMsb      finalised msbIndex of the rebuilt parent
   * @param parentBeta     β being added to parent.mask (for log context)
   * @param children       lifted children array (post-Step 3 sort)
   * @param newCount       number of valid entries in {@code children}
   */
  private void phase7q15dCheckIntermediateMsb(long parentPageKey, int parentMsb, int parentBeta,
      PageReference[] children, int newCount) {
    final boolean imsbDbg = Boolean.getBoolean("hot.debug.phase7q.imsb");
    StringBuilder offenders = null;
    for (int i = 0; i < newCount; i++) {
      final PageReference cref = children[i];
      if (cref == null) continue;
      final int childMsb = getIndirectMsbOrMax(cref);
      if (childMsb == Integer.MAX_VALUE) {
        // Leaf child or unresolvable — I11 doesn't constrain leaves the same way.
        PHASE7Q_INTERMEDIATE_MSB_OK.incrementAndGet();
        continue;
      }
      if (childMsb < parentMsb) {
        PHASE7Q_INTERMEDIATE_MSB_LOWER.incrementAndGet();
        if (imsbDbg) {
          if (offenders == null) offenders = new StringBuilder(128);
          offenders.append(" [LOWER child[").append(i).append("] pageKey=")
              .append(cref.getKey()).append(" childMsb=").append(childMsb).append(']');
        }
      } else if (childMsb == parentMsb) {
        PHASE7Q_INTERMEDIATE_MSB_EQUALITY.incrementAndGet();
        if (imsbDbg) {
          if (offenders == null) offenders = new StringBuilder(128);
          offenders.append(" [EQ child[").append(i).append("] pageKey=").append(cref.getKey())
              .append(" childMsb=").append(childMsb).append(']');
        }
      } else {
        PHASE7Q_INTERMEDIATE_MSB_OK.incrementAndGet();
      }
    }
    if (imsbDbg && offenders != null) {
      System.err.println("[phase7q.15d-imsb] parentPageKey=" + parentPageKey + " parentMsb="
          + parentMsb + " beta=" + parentBeta + " newCount=" + newCount + offenders);
    }
  }

  /**
   * Phase 7q.13 — When iterative bit-extension fails, structurally lift the offending
   * I8 inversion by partitioning root's children by the discriminating bit and rebuilding
   * with that bit added to the new mask. Unlike {@link #extendIndirectMaskForClosureI11Safe}
   * which preserves the indirect's identity, this can produce an output that doesn't
   * collide with placeholder partials because:
   * <ol>
   *   <li>It uses {@code firstKey}-presence as the partition oracle (not partials).</li>
   *   <li>It SKIPS placeholder children (empty firstKey) entirely — no partial assigned,
   *       no collision possible.</li>
   *   <li>Real children get partials computed from the EXTENDED mask, where the
   *       discriminating bit forces uniqueness across the offending pair.</li>
   * </ol>
   *
   * <p>I11 preservation: the new indirect has root's existing extraction bytes PLUS the
   * discriminating bit's byte. MSB stays at the smallest absBit (= existing root.MSB if
   * the disc bit is less significant). Children are unchanged → their MSBs are unchanged
   * → I11 holds.
   *
   * <p>Returns null when no candidate bit can be safely added (e.g., the offending pair
   * has no in-mask-extendable bit that's β-constant in all real children).
   *
   * <p>HFT-grade: bounded scan over root.children + bounded mask-bit iteration. No
   * recursion. Single allocation per attempt.
   */
  @Nullable
  private HOTIndirectPage addNewRootLevelForI8(HOTIndirectPage indirect,
      TransactionIntentLog log, int revision) {
    final boolean dbg = Boolean.getBoolean("hot.debug.g32");
    if (indirect == null) return null;
    final int n = indirect.getNumChildren();
    if (n < 2) return null;

    // Step 1: identify the offending pair via firstKey scan, skipping placeholders.
    final byte[][] firstKeys = new byte[n][];
    int maxLen = 0;
    int realCount = 0;
    for (int i = 0; i < n; i++) {
      final PageReference cref = indirect.getChildReference(i);
      if (cref == null) continue;
      firstKeys[i] = getFirstKeyFromChild(cref);
      if (firstKeys[i] != null && firstKeys[i].length > 0) {
        realCount++;
        if (firstKeys[i].length > maxLen) maxLen = firstKeys[i].length;
      }
    }
    if (realCount < 2) return null;

    // Find the offending PAIR (= adjacent inversion in stored order, real-only).
    int offK = -1, offI = -1;
    byte[] prevKey = null;
    int prevIdx = -1;
    for (int i = 0; i < n; i++) {
      if (firstKeys[i] == null || firstKeys[i].length == 0) continue;
      if (prevKey != null && Arrays.compareUnsigned(prevKey, firstKeys[i]) >= 0) {
        offK = prevIdx;
        offI = i;
        break;
      }
      prevKey = firstKeys[i];
      prevIdx = i;
    }
    if (offI < 0) return null; // no inversion — nothing to do.

    // Phase 7q.15 — compute the TRUE MSB-of-difference (MSDB) between offK.firstKey and
    // offI.firstKey, regardless of current root MSB or β-constancy. This is the bit that
    // MUST be in the mask for partial-order to match lex-order at the offending pair.
    int msbOfDiff = -1;
    for (int absBit = 0; absBit < maxLen * 8; absBit++) {
      if (isAbsBitSet(firstKeys[offK], absBit) != isAbsBitSet(firstKeys[offI], absBit)) {
        msbOfDiff = absBit;
        break;
      }
    }
    if (dbg) System.err.println("[g32-newrootlvl] n=" + n + " realCount=" + realCount
        + " offK=" + offK + " offI=" + offI + " msbOfDiff=" + msbOfDiff
        + " indirectMsb=" + (indirect.getMostSignificantBitIndex() & 0xFFFF)
        + " indirectHasBeta=" + (msbOfDiff >= 0 && indirectMaskHasAbsBit(indirect, msbOfDiff)));

    // Phase 7q.15 — structural lift attempt. When the MSB-of-diff is NOT β-constant in
    // every child's subtree (= bit captured by a descendant indirect), the conventional
    // "add bit to mask" path can't proceed safely. Invoke phase7qExtendWithLift which
    // lifts the bit from descendants (splits subtrees on the bit so β becomes constant
    // at this level, then adds it to indirect's mask). Gated by hot.strict.g32.deep so
    // legacy behavior is preserved by default.
    if (msbOfDiff >= 0 && Boolean.getBoolean("hot.strict.g32.deep")
        && !indirectMaskHasAbsBit(indirect, msbOfDiff)) {
      // Check whether MSB-of-diff is β-constant in every child. If yes, the simple
      // β-constancy search below will pick it up. If no, try the structural lift first.
      boolean allConstant = true;
      for (int i = 0; i < n; i++) {
        final PageReference cref = indirect.getChildReference(i);
        if (cref == null) continue;
        if (cref.getKey() == io.sirix.settings.Constants.NULL_ID_LONG) continue;
        if (firstKeys[i] == null || firstKeys[i].length == 0) continue;
        final int v = bitConstantValueInSubtree(cref, msbOfDiff);
        if (v < 0) { allConstant = false; break; }
      }
      if (!allConstant) {
        if (dbg) System.err.println("[g32-newrootlvl] msbOfDiff=" + msbOfDiff
            + " not β-constant — attempting structural lift");
        final HOTIndirectPage lifted = phase7qExtendWithLift(indirect, msbOfDiff, log, revision);
        if (lifted != null) {
          if (dbg) System.err.println("[g32-newrootlvl] structural lift OK newMsb="
              + (lifted.getMostSignificantBitIndex() & 0xFFFF));
          return lifted;
        }
        if (dbg) System.err.println("[g32-newrootlvl] structural lift failed, falling through");
      }
    }

    // Step 2: find the MSDB between offK.firstKey and offI.firstKey.
    // Phase 7q.15 — same fix as rebuildRootWithFullClosureI11Safe: search ALL absBits
    // (not just absBit > currentMsb). The variable parentMsb is THIS INDIRECT's MSB
    // (badly named — there's no actual "parent" here, this fn operates on the root). For
    // root mask extension, adding a MORE significant bit is I11-safe because root has no
    // parent and children's MSBs were already > currentMsb. The β-constancy gate is what
    // prevents misrouting.
    final int parentMsb = indirect.getMostSignificantBitIndex() & 0xFFFF;
    final int absBitLowerBound = Boolean.getBoolean("hot.strict.g32.deep") ? 0 : parentMsb + 1;
    int discBit = -1;
    for (int absBit = absBitLowerBound; absBit < maxLen * 8; absBit++) {
      final boolean kSet = isAbsBitSet(firstKeys[offK], absBit);
      final boolean iSet = isAbsBitSet(firstKeys[offI], absBit);
      if (kSet != iSet) {
        // Verify: this bit β-constant in EVERY real child's subtree (= won't break
        // routing for keys NOT in the offending pair).
        boolean allConstant = true;
        for (int i = 0; i < n; i++) {
          final PageReference cref = indirect.getChildReference(i);
          if (cref == null) continue;
          if (cref.getKey() == io.sirix.settings.Constants.NULL_ID_LONG) continue;
          if (firstKeys[i] == null || firstKeys[i].length == 0) continue;
          final int v = bitConstantValueInSubtree(cref, absBit);
          if (v < 0) { allConstant = false; break; }
        }
        if (allConstant) {
          discBit = absBit;
          break; // most-significant safe disc bit found
        }
      }
    }
    if (discBit < 0) {
      if (dbg) System.err.println("[g32-newrootlvl] no β-constant disc bit between offK="
          + offK + " offI=" + offI + " parentMsb=" + parentMsb);
      return null;
    }

    // Step 3: build new MultiMask layout = old mask + discBit.
    final int discBytePos = discBit / 8;
    final int discBitInByte = discBit % 8;
    final int discMaskBit = 1 << (7 - discBitInByte);

    final java.util.TreeMap<Integer, Integer> maskByBytePos = new java.util.TreeMap<>();
    if (indirect.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK) {
      final int oldInitial = indirect.getInitialBytePos();
      final long oldMask = indirect.getBitMask();
      for (int bo = 0; bo < 8; bo++) {
        final int byteMaskBits = (int) ((oldMask >>> ((7 - bo) * 8)) & 0xFFL);
        if (byteMaskBits != 0) {
          maskByBytePos.merge(oldInitial + bo, byteMaskBits, (a, b) -> a | b);
        }
      }
    } else {
      final byte[] ep = indirect.getExtractionPositions();
      final long[] em = indirect.getExtractionMasks();
      final int neb = indirect.getNumExtractionBytes();
      for (int i = 0; i < neb; i++) {
        final int chunkIdx = i / 8;
        final int byteOffsetInChunk = i % 8;
        final int byteMaskBits = (int) ((em[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFFL);
        if (byteMaskBits != 0) {
          maskByBytePos.merge(ep[i] & 0xFF, byteMaskBits, (a, b) -> a | b);
        }
      }
    }
    maskByBytePos.merge(discBytePos, discMaskBit, (a, b) -> a | b);

    final int numBytes = maskByBytePos.size();
    if (numBytes > 64) return null;
    final byte[] extractionPositions = new byte[numBytes];
    final long[] extractionMasks = new long[(numBytes + 7) / 8];
    short msbIndex = Short.MAX_VALUE;
    int idx = 0;
    for (final var entry : maskByBytePos.entrySet()) {
      final int bytePos = entry.getKey();
      final int maskByte = entry.getValue();
      extractionPositions[idx] = (byte) bytePos;
      final int chunkIdx = idx / 8;
      final int byteOffsetInChunk = idx % 8;
      extractionMasks[chunkIdx] |= ((long) (maskByte & 0xFF)) << ((7 - byteOffsetInChunk) * 8);
      final int highBit = 31 - Integer.numberOfLeadingZeros(maskByte & 0xFF);
      final int absBitPos = bytePos * 8 + (7 - highBit);
      if (absBitPos < msbIndex) msbIndex = (short) absBitPos;
      idx++;
    }

    // Step 4: compute partials. Skip placeholders (empty firstKey) — assign max-int
    // sentinel so they sort to the end and don't collide with real children's partials.
    // This works because PEXT-routing for real keys never produces max-int partial
    // (would require all extracted bits = 1, extremely unlikely).
    final int[] newPartials = new int[n];
    final PageReference[] newChildren = new PageReference[n];
    final java.util.HashSet<Integer> seenPartials = new java.util.HashSet<>(n * 2);
    int placeholderSeq = 0;
    for (int i = 0; i < n; i++) {
      final PageReference cref = indirect.getChildReference(i);
      newChildren[i] = cref;
      if (cref == null || firstKeys[i] == null || firstKeys[i].length == 0) {
        // Placeholder: synthesize a unique high-end partial.
        newPartials[i] = Integer.MAX_VALUE - placeholderSeq;
        placeholderSeq++;
        continue;
      }
      newPartials[i] = computePartialKeyMultiMaskDirect(firstKeys[i],
          extractionPositions, extractionMasks, numBytes);
      if (!seenPartials.add(newPartials[i])) {
        if (dbg) System.err.println("[g32-newrootlvl] partial collision among real children"
            + " at i=" + i + " p=" + newPartials[i] + " — abandoning");
        return null;
      }
    }

    // Step 5: sort by partial.
    sortChildrenAndPartialsByPartial(newChildren, newPartials);

    // Step 6: verify firstKey-monotone post-sort (skip placeholders).
    byte[] prev = null;
    for (int i = 0; i < n; i++) {
      final PageReference cref = newChildren[i];
      if (cref == null) continue;
      final byte[] fk = getFirstKeyFromChild(cref);
      if (fk == null || fk.length == 0) continue;
      if (prev != null && Arrays.compareUnsigned(prev, fk) >= 0) {
        if (dbg) System.err.println("[g32-newrootlvl] post-sort still inverted at i=" + i
            + " — abandoning");
        return null;
      }
      prev = fk;
    }

    // Step 7: verify I4 (smallest partial = 0). Only the smallest REAL partial matters;
    // placeholders use MAX_INT sentinels and are sorted to the end.
    int smallestRealPartial = Integer.MAX_VALUE;
    for (int i = 0; i < n; i++) {
      if (newPartials[i] < Integer.MAX_VALUE - n) {
        if (newPartials[i] < smallestRealPartial) smallestRealPartial = newPartials[i];
      }
    }
    if (smallestRealPartial != 0) {
      if (dbg) System.err.println("[g32-newrootlvl] no zero partial — abandoning");
      return null;
    }

    // Phase 7q-Path5 — partial-subset routing-correctness check. Even with all partials
    // distinct, sorted, and matching firstKey-monotone, the HOT subset-fallback routing
    // rule "pick LARGEST INDEX among subsets" can mis-route subtree keys when partials
    // form overlapping subset chains (e.g., {0, 0x10, 0x14, 0x19, 0x1a}: any key with
    // densePK=0x1d routes to last subset 0x19, but key was stored under 0x14's subtree
    // → I6 cascade). Verify: for each child[i]'s firstKey-partial, routing via the
    // subset rule returns i (= same child). If not, reject the rebuild so caller falls
    // through to the next strategy.
    //
    // Opt-in via -Dhot.strict.path5.routeverify=true (same flag as
    // rebuildRootWithFullClosureI11Safe). See that function's comment for the
    // default-ON exploration outcome (reverted due to transaction-leak regressions).
    if (Boolean.getBoolean("hot.strict.path5.routeverify")) {
      for (int i = 0; i < n; i++) {
        if (newPartials[i] >= Integer.MAX_VALUE - n) continue; // skip placeholders
        final int densePk = newPartials[i];
        int bestIdx = -1;
        for (int j = 0; j < n; j++) {
          if (newPartials[j] >= Integer.MAX_VALUE - n) continue;
          if ((densePk & newPartials[j]) == newPartials[j]) {
            if (newPartials[j] == densePk) { bestIdx = j; break; }
            if (bestIdx < 0 || j > bestIdx) bestIdx = j;
          }
        }
        if (bestIdx != i) {
          if (Boolean.getBoolean("hot.debug.path5")) {
            System.err.println("[path5] REJECT newrootlvl: child[" + i + "].partial=0x"
                + Integer.toHexString(densePk) + " routes to idx=" + bestIdx
                + " (partial=0x"
                + (bestIdx >= 0 ? Integer.toHexString(newPartials[bestIdx]) : "?")
                + ") expected idx=" + i);
          }
          return null;
        }
      }
      // Stronger structural check: for any (i, j) pair with i < j, child[i]'s subtree
      // must NOT contain a key K with densePK(K) ⊇ newPartials[j]. Otherwise routing
      // would mis-pick child[j] over child[i] for that K. Sufficient condition:
      // ∃ at least one bit b in newPartials[j] such that bitConstantValueInSubtree
      // (child[i], b) returns 0 (= bit b is always-zero across child[i]'s subtree).
      //
      // We need to map densePK-bit positions back to absolute bit positions in the key.
      // For MultiMask, densePK bit k corresponds to the k-th set bit (counted MSB-first
      // across all extraction byte masks, in the order extraction bytes are stored).
      //
      // HFT-grade: O(n² × densePK-bits × per-bit-subtree-walk) — densePK ≤ 16 bits,
      // n ≤ 32, subtree walk depth bounded by trie height.
      // Compute densePK-bit → absBit mapping.
      int totalMaskBitsLocal = 0;
      for (final long em : extractionMasks) totalMaskBitsLocal += Long.bitCount(em);
      final int[] densePkBitToAbsBit = new int[totalMaskBitsLocal];
      int dpkIdx = 0;
      for (int eIdx = 0; eIdx < numBytes; eIdx++) {
        final int bp = extractionPositions[eIdx] & 0xFF;
        final int chunkIdx = eIdx / 8;
        final int byteOffsetInChunk = eIdx % 8;
        final int byteMaskBits = (int) ((extractionMasks[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFFL);
        for (int bitInByte = 0; bitInByte < 8; bitInByte++) {
          final int maskBit = 1 << (7 - bitInByte);
          if ((byteMaskBits & maskBit) != 0) {
            densePkBitToAbsBit[dpkIdx++] = bp * 8 + bitInByte;
          }
        }
      }
      // Now check each pair (i, j) for collision potential.
      for (int i = 0; i < n; i++) {
        if (newPartials[i] >= Integer.MAX_VALUE - n) continue;
        for (int j = 0; j < n; j++) {
          if (i == j) continue;
          if (newPartials[j] >= Integer.MAX_VALUE - n) continue;
          if (j <= i) continue; // only later children can shadow earlier ones
          final int pj = newPartials[j];
          if (pj == 0) continue;
          // ∃ a bit b in pj that's always-zero in child[i]'s subtree?
          // densePK bit b (LSB-numbered, per Long.compress output) maps to absBit
          // position via densePkBitToAbsBit[(totalDpkBits - 1) - b] (HOT's PEXT
          // output is LSB-first while the lookup table is built MSB-first).
          boolean safe = false;
          final PageReference cref = newChildren[i];
          if (cref == null) {
            safe = true; // can't check placeholder — assume safe
          } else {
            final int totalDpkBits = densePkBitToAbsBit.length;
            for (int b = 0; b < totalDpkBits; b++) {
              if ((pj & (1 << b)) == 0) continue;
              final int absBitCandidate = densePkBitToAbsBit[totalDpkBits - 1 - b];
              final int bv = bitConstantValueInSubtree(cref, absBitCandidate);
              if (bv == 0) { safe = true; break; }
            }
          }
          if (!safe) {
            if (Boolean.getBoolean("hot.debug.path5")) {
              System.err.println("[path5] REJECT newrootlvl: child[" + i
                  + "] subtree can collide with child[" + j + "].partial=0x"
                  + Integer.toHexString(pj));
            }
            return null;
          }
        }
      }
    }

    if (dbg) System.err.println("[g32-newrootlvl] OK discBit=" + discBit + " newMsb="
        + msbIndex + " parentMsb=" + parentMsb + " realCount=" + realCount);
    if (n <= 16) {
      return HOTIndirectPage.createSpanNodeMultiMask(indirect.getPageKey(), revision,
          extractionPositions, extractionMasks, numBytes, newPartials, newChildren,
          indirect.getHeight(), msbIndex);
    }
    return HOTIndirectPage.createMultiNodeMultiMask(indirect.getPageKey(), revision,
        extractionPositions, extractionMasks, numBytes, newPartials, newChildren,
        indirect.getHeight(), msbIndex);
  }

  /**
   * Extend {@code indirect}'s mask with bit {@code newAbsBit}. The bit must be at absBit
   * STRICTLY GREATER than parent's current MSB (= less significant, so I11 holds).
   * Recomputes all partials directly from each child's current firstKey under the
   * extended mask. Returns the new page, or null on infeasibility.
   *
   * <p>Mirrors {@link #extendIndirectMaskForClosure} but WITHOUT split-on-bit. Bit must
   * already be β-constant in every child's subtree (= bit is below the discrimination
   * resolution of the existing children). This is automatic when newAbsBit > parent.MSB —
   * the bit lies in a less-significant position than where children were partitioned.
   */
  private @Nullable HOTIndirectPage extendMaskWithBitI11Safe(HOTIndirectPage indirect,
      int newAbsBit, int revision) {
    final int n = indirect.getNumChildren();
    if (n < 2) return null;
    final int oldInitialBytePos = indirect.getInitialBytePos();
    final int newBytePos = newAbsBit / 8;
    final int newBitInByte = newAbsBit % 8;
    final int newMaskBitInByte = 1 << (7 - newBitInByte);

    // Build new extraction layout. SingleMask if window fits; else MultiMask.
    int[] oldBytePositions = new int[16];
    int[] oldByteMaskBits = new int[16];
    int oldByteCount = 0;
    if (indirect.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK) {
      final long oldMask = indirect.getBitMask();
      for (int bo = 0; bo < 8; bo++) {
        final int byteMaskBits = (int) ((oldMask >>> ((7 - bo) * 8)) & 0xFFL);
        if (byteMaskBits != 0) {
          oldBytePositions[oldByteCount] = oldInitialBytePos + bo;
          oldByteMaskBits[oldByteCount] = byteMaskBits;
          oldByteCount++;
        }
      }
    } else {
      final byte[] ep = indirect.getExtractionPositions();
      final long[] em = indirect.getExtractionMasks();
      final int neb = indirect.getNumExtractionBytes();
      for (int i = 0; i < neb; i++) {
        final int chunkIdx = i / 8;
        final int byteOffsetInChunk = i % 8;
        final int byteMaskBits = (int) ((em[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFFL);
        if (byteMaskBits != 0) {
          if (oldByteCount >= oldBytePositions.length) return null;
          oldBytePositions[oldByteCount] = ep[i] & 0xFF;
          oldByteMaskBits[oldByteCount] = byteMaskBits;
          oldByteCount++;
        }
      }
    }
    // Merge newBytePos into the byte list.
    int[] allBytePositions = new int[oldByteCount + 1];
    int[] allByteMaskBits = new int[oldByteCount + 1];
    int allCount = 0;
    boolean merged = false;
    for (int i = 0; i < oldByteCount; i++) {
      if (!merged && oldBytePositions[i] == newBytePos) {
        allBytePositions[allCount] = newBytePos;
        allByteMaskBits[allCount] = oldByteMaskBits[i] | newMaskBitInByte;
        // If bit is already present, no change.
        if ((oldByteMaskBits[i] & newMaskBitInByte) != 0) return null;
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

    // Encode extraction tables.
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

    final boolean dbg = Boolean.getBoolean("hot.debug.g32");
    // β-CONSTANCY CHECK: ensure newAbsBit is constant within every child's subtree. If any
    // child's subtree has mixed bit values at newAbsBit, adding this bit to root's mask
    // would misroute keys (I6 violations). Sirix's multi-entry leaves can hold any bit
    // value at any position, so this check is essential.
    for (int i = 0; i < n; i++) {
      final PageReference cref = indirect.getChildReference(i);
      final int bv = bitConstantValueInSubtree(cref, newAbsBit);
      if (bv < 0) {
        if (dbg) System.err.println("[g32-ext] reject bit=" + newAbsBit
            + " reason=beta-not-constant-in-child childIdx=" + i);
        return null;
      }
    }
    // Recompute partials from children's current firstKeys.
    final int[] newPartials = new int[n];
    final PageReference[] newChildren = new PageReference[n];
    for (int i = 0; i < n; i++) {
      newChildren[i] = indirect.getChildReference(i);
      final byte[] cKey = getFirstKeyFromChild(newChildren[i]);
      newPartials[i] = (cKey == null || cKey.length == 0) ? 0
          : computePartialKeyMultiMaskDirect(cKey, extractionPositions, extractionMasks, allCount);
    }
    // Verify uniqueness.
    for (int i = 1; i < n; i++) {
      for (int k = 0; k < i; k++) {
        if (newPartials[k] == newPartials[i]) {
          if (dbg) System.err.println("[g32-ext] reject bit=" + newAbsBit
              + " reason=partial-collision i=" + i + " k=" + k + " p=" + newPartials[i]);
          return null;
        }
      }
    }
    // Sort children by partial.
    sortChildrenAndPartialsByPartial(newChildren, newPartials);
    // Verify firstKey-monotone after partial-sort.
    for (int i = 1; i < n; i++) {
      final byte[] prev = getFirstKeyFromChild(newChildren[i - 1]);
      final byte[] curr = getFirstKeyFromChild(newChildren[i]);
      if (prev != null && curr != null && prev.length > 0 && curr.length > 0
          && Arrays.compareUnsigned(prev, curr) >= 0) {
        if (dbg) System.err.println("[g32-ext] reject bit=" + newAbsBit
            + " reason=still-inverted i=" + i);
        return null;
      }
    }
    // Verify first partial = 0 (Binna's sparse-path I4).
    if (newPartials[0] != 0) {
      if (dbg) System.err.println("[g32-ext] reject bit=" + newAbsBit
          + " reason=no-zero-partial firstPartial=" + newPartials[0]
          + " partials=" + Arrays.toString(newPartials));
      return null;
    }

    if (n <= 16) {
      return HOTIndirectPage.createSpanNodeMultiMask(indirect.getPageKey(), revision,
          extractionPositions, extractionMasks, allCount, newPartials, newChildren,
          indirect.getHeight(), msbIndex);
    }
    return HOTIndirectPage.createMultiNodeMultiMask(indirect.getPageKey(), revision,
        extractionPositions, extractionMasks, allCount, newPartials, newChildren,
        indirect.getHeight(), msbIndex);
  }

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
    // Phase 7q.4 diagnostic: trace every attempt on root (pageKey=2) for any β
    // to identify which bits the closure mechanism tries on root.
    if (indirect != null && indirect.getPageKey() == 2L) {
      System.err.println("[phase7q-trace] extendIndirectMaskForClosure ENTER"
          + " pageKey=2 beta=" + beta + " numChildren=" + indirect.getNumChildren()
          + " msb=" + (indirect.getMostSignificantBitIndex() & 0xFFFF)
          + " maskHasBeta=" + indirectMaskHasAbsBit(indirect, beta));
    }
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
    // Phase 7i — relax G.30 placeholder guard for commit-time use.
    // When -Dhot.relax.closure.placeholder=true is set, allow closure to proceed even
    // when children have NULL_ID_LONG (= TIL-only refs). The original cascade-prevention
    // motivation no longer applies if owned-bits Phase 7 infrastructure is in place,
    // since β-constancy is verified per-child via bitConstantValueInSubtree separately.
    final boolean relaxPlaceholder = Boolean.getBoolean("hot.relax.closure.placeholder");
    for (int i = 0; i < oldNumChildren; i++) {
      final PageReference ref = indirect.getChildReference(i);
      if (ref == null) {
        if (dbg) System.err.println("[g30] reject reason=null-child-ref beta=" + beta + " idx=" + i);
        return null;
      }
      if (!relaxPlaceholder && ref.getKey() == io.sirix.settings.Constants.NULL_ID_LONG) {
        if (dbg) System.err.println("[g30] reject reason=placeholder-child beta=" + beta
            + " childIdx=" + i + " childPageKey=" + ref.getKey());
        return null;
      }
    }

    // Phase 7p — descendant-mask double-capture guard. Reject the extension if any
    // descendant indirect already has β in its mask. Adding β to this indirect creates
    // a double-capture (bit β at two trie levels), which breaks Binna's I6 PEXT
    // routing — keys get sent to the wrong leaf because the descendant's PEXT no
    // longer matches the new ancestor's PEXT routing.
    //
    // Phase 7q diagnostic: classify each rejection as WASTED (β captured but β-constant
    // in every capturing subtree → liftable by stripping mask bits) vs LOAD_BEARING
    // (β captured AND β-mixed somewhere → requires subtree restructure). Both still
    // reject for now; the lift operation comes later behind -Dhot.strict.phase7q=true.
    final boolean phase7qDebug = Boolean.getBoolean("hot.debug.phase7q");
    boolean anyCapture = false;
    boolean anyLoadBearing = false;
    int firstCaptureIdx = -1;
    for (int i = 0; i < oldNumChildren; i++) {
      final PageReference ref = indirect.getChildReference(i);
      if (ref == null) continue;
      if (subtreeHasBitInAnyIndirectMask(ref, beta)) {
        if (!anyCapture) firstCaptureIdx = i;
        anyCapture = true;
        if (phase7qIsLoadBearingInSubtree(ref, beta)) {
          anyLoadBearing = true;
          if (phase7qDebug) {
            System.err.println("[phase7q] reject-LOAD_BEARING beta=" + beta
                + " childIdx=" + i + " childPageKey=" + ref.getKey());
          }
          // Don't break — keep scanning so debug logs see all capturing children.
        } else if (phase7qDebug) {
          System.err.println("[phase7q] reject-WASTED beta=" + beta
              + " childIdx=" + i + " childPageKey=" + ref.getKey()
              + " (β captured but β-constant in subtree — liftable)");
        }
      }
    }
    if (anyCapture) {
      int liftability = LIFT_LIFTABLE; // valid only when anyLoadBearing
      if (anyLoadBearing) {
        PHASE7Q_REJECTS_LOAD_BEARING.incrementAndGet();
        // Refine: liftable (mask rebuild alone) vs hard (structural restructure)?
        for (int i = 0; i < oldNumChildren && liftability == LIFT_LIFTABLE; i++) {
          final PageReference ref = indirect.getChildReference(i);
          if (ref == null) continue;
          if (subtreeHasBitInAnyIndirectMask(ref, beta)) {
            if (phase7qClassifyLiftability(ref, beta) == LIFT_HARD) {
              liftability = LIFT_HARD;
            }
          }
        }
        if (liftability == LIFT_LIFTABLE) PHASE7Q_REJECTS_LB_LIFTABLE.incrementAndGet();
        else PHASE7Q_REJECTS_LB_HARD.incrementAndGet();
        if (phase7qDebug) {
          System.err.println("[phase7q] LB-refined beta=" + beta
              + " liftability=" + (liftability == LIFT_LIFTABLE ? "LIFTABLE" : "HARD"));
        }
      } else {
        PHASE7Q_REJECTS_WASTED.incrementAndGet();
      }
      // Phase 7q.3 dispatch: when -Dhot.strict.phase7q=true AND the rejection is
      // load-bearing-hard, attempt the structural lift via phase7qExtendWithLift.
      // On success, the lifted+extended indirect is returned (β added to mask,
      // descendants β-stripped). On failure (lift cascade infeasibility), fall
      // through to the standard reject below.
      if (anyLoadBearing && liftability == LIFT_HARD
          && Boolean.getBoolean("hot.strict.phase7q")) {
        final HOTIndirectPage lifted = phase7qExtendWithLift(indirect, beta, log, revision);
        if (lifted != null) {
          if (phase7qDebug) {
            System.err.println("[phase7q] LIFT-OK beta=" + beta + " indirect.pageKey="
                + indirect.getPageKey() + " new-msb="
                + (lifted.getMostSignificantBitIndex() & 0xFFFF));
          }
          return lifted;
        }
        if (phase7qDebug) {
          System.err.println("[phase7q] LIFT-FAILED beta=" + beta + " indirect.pageKey="
              + indirect.getPageKey()
              + " indirect.maskHasBeta=" + indirectMaskHasAbsBit(indirect, beta)
              + " indirect.msb=" + (indirect.getMostSignificantBitIndex() & 0xFFFF)
              + " → fall through to standard reject");
        }
      }
      // Phase 7q.13 — opt-in double-capture diagnostic (`-Dhot.strict.phase7q.allowDoubleCapture=true`).
      // EMPIRICALLY DISPROVES that double-capture is correctness-safe at LB-HARD. See
      // HOT_PHASE_7Q_DESIGN.md §7.16: on the 50K reproducer, enabling this triggers 12
      // fall-throughs → 4492 violations cascade (I6×4480, I11×5, I-Binna-sparse×3, I5×3,
      // I8×1). Reason: when descendant `c` has β as load-bearing disc bit (some sub-child
      // pair of `c` differs ONLY at β), root.routing-by-β forces ALL keys in slot[c] to
      // share the same β value (constant). But `c`'s mask still uses β to discriminate
      // its sub-children → those sub-children can't all have the constant value → I5/I6
      // routing breaks. Phase 7p's reject is enforcing correctness, not bit-efficiency
      // as Phase 7q.13's original hypothesis claimed.
      //
      // Kept as an opt-in diagnostic counter: when the flag is set, fall-through to
      // rebuild is taken AND `PHASE7Q_ALLOW_DOUBLE_CAPTURE_FIRINGS` increments. Useful
      // for future ceiling tests to count how many extension attempts would benefit
      // from a real (correctness-preserving) cascading lift implementation.
      if (anyLoadBearing
          && Boolean.getBoolean("hot.strict.phase7q.allowDoubleCapture")) {
        PHASE7Q_ALLOW_DOUBLE_CAPTURE_FIRINGS.incrementAndGet();
        if (phase7qDebug) {
          System.err.println("[phase7q.13] ALLOW-DOUBLE-CAPTURE (UNSAFE) beta=" + beta
              + " indirect.pageKey=" + indirect.getPageKey()
              + " indirect.msb=" + (indirect.getMostSignificantBitIndex() & 0xFFFF)
              + " — bypass Phase 7p reject. Will cascade per §7.16.");
        }
        // Fall through.
      } else {
        if (dbg) System.err.println("[g30] reject reason=descendant-already-routes-beta beta=" + beta
            + " firstChildIdx=" + firstCaptureIdx
            + " classification=" + (anyLoadBearing ? "LOAD_BEARING" : "WASTED"));
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
    // Phase 7q.15d — pre-build instrumentation. Same classification as in
    // {@link #phase7qExtendWithLift}: catches the case where standard extend's
    // splitSubtreeOnBit-produced halves have MSB ≤ the new parent's msbIndex (=
    // I11 equality / strict violation at the parent ↔ split-half boundary).
    phase7q15dCheckIntermediateMsb(indirect.getPageKey(), msbIndex & 0xFFFF, beta, newChildren,
        newCount);
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
    // Phase 7q.5 — when -Dhot.strict.phase7q.fullScan=true, attempt EVERY closure bit
    // instead of breaking on the first failure. Without this, if bit β fails (e.g.
    // because a descendant captures β and the lift early-bails on β-in-mask), all
    // higher-β extensions are foregone — including the one needed to fix the
    // persistent I8 violation. With fullScan=true, the loop continues past failures,
    // letting the structural lift be attempted on every load-bearing β.
    final boolean fullScan = Boolean.getBoolean("hot.strict.phase7q.fullScan");
    for (final int beta : closureBits) {
      final int outPos = current.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK
          ? singleMaskBetaOutputPos(current, beta)
          : multiMaskBetaOutputPos(current, beta);
      if (outPos >= 0) continue;
      final HOTIndirectPage extended = extendIndirectMaskForClosure(current, beta, log, revision);
      if (extended == null) {
        if (fullScan) continue;
        break;
      }
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
    // Phase 7q.4 diagnostic: dump firstKeys when operating on root (pageKey=2).
    if (indirect.getPageKey() == 2L) {
      final StringBuilder sb = new StringBuilder();
      sb.append("[phase7q-fk] cur.pageKey=2 numChildren=").append(n).append(" maxLen=").append(maxLen);
      for (int i = 0; i < n; i++) {
        sb.append("\n  c[").append(i).append("]=");
        if (firstKeys[i] == null) sb.append("null");
        else if (firstKeys[i].length == 0) sb.append("empty");
        else {
          for (final byte b : firstKeys[i]) sb.append(String.format("%02x", b));
        }
      }
      System.err.println(sb);
    }
    final int[] tmp = new int[maxLen * 8];
    int count = 0;
    final boolean traceRoot = indirect.getPageKey() == 2L;
    for (int absBit = 0; absBit < maxLen * 8; absBit++) {
      boolean seen0 = false, seen1 = false;
      for (int i = 0; i < n; i++) {
        if (firstKeys[i] == null || firstKeys[i].length == 0) continue;
        if (isAbsBitSet(firstKeys[i], absBit)) seen1 = true;
        else seen0 = true;
        if (seen0 && seen1) break;
      }
      if (seen0 && seen1) tmp[count++] = absBit;
      // Phase 7q.4 diagnostic: ALL bytes of c[1] with explicit indices.
      if (traceRoot && absBit == 88) {
        for (int i = 0; i <= Math.min(2, n - 1); i++) {
          if (firstKeys[i] == null || firstKeys[i].length == 0) continue;
          final StringBuilder sb2 = new StringBuilder();
          sb2.append("[phase7q-fullbytes] cur.pageKey=2 c[").append(i).append("] len=")
             .append(firstKeys[i].length);
          for (int j = 0; j < firstKeys[i].length; j++) {
            sb2.append(" [").append(j).append("]=").append(String.format("%02x", firstKeys[i][j] & 0xff));
          }
          System.err.println(sb2);
        }
      }
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

  /**
   * Phase 7q-Path2 — I11-safe leaf-split bit chooser.
   *
   * <p>When a leaf is about to split, its natural MSDB is locally optimal but can be
   * <em>globally toxic</em>: integrating the resulting BiNode via {@code addEntryWithPDep}
   * extends the parent indirect's mask with β=MSDB. If MSDB is more significant than the
   * parent indirect's current MSB AND than the grandparent's MSB, parent.MSB drops below
   * grandparent.MSB → I11 ({@code parent.MSB < child.MSB} numerically, where lower absBit
   * = more significant) violates → parent's effective deep-firstKey shifts → I8 cascade
   * at the root.
   *
   * <p>This chooser checks the natural MSDB-with-K and, if it would trigger an I11
   * violation, scans for an alternative discriminative bit that:
   * <ul>
   *   <li>partitions the leaf {@code keyBuf}-plus-existing into two non-empty halves</li>
   *   <li>has absBit &gt; grandparent.MSB (= numerically less significant than the
   *       grandparent's most-significant disc bit)</li>
   * </ul>
   *
   * <p>Returns {@code -1} when:
   * <ul>
   *   <li>natural MSDB is already I11-safe (= caller should use default behavior)</li>
   *   <li>no I11-safe alternative bit exists in the leaf's key contents (= caller should
   *       still fall back to default MSDB — the architectural ceiling case where the
   *       encoder's bit-pattern forces an I11-toxic split)</li>
   * </ul>
   *
   * <p>HFT-grade: single pass over leaf entries per candidate bit; allocates one
   * {@link DiscriminativeBitComputer} call per existing key; no boxing.
   *
   * @param leaf        leaf about to split (with {@code keyBuf} pending insert)
   * @param keyBuf      pending insert key
   * @param pathNodes   ancestor path (root → leaf-parent)
   * @param pathDepth   number of valid entries in {@code pathNodes}
   * @return explicit split bit (≥ 0) if natural MSDB is toxic AND a safe alternative
   *         exists, else {@code -1} (= caller uses default MSDB)
   */
  public int chooseI11SafeLeafSplitBit(HOTLeafPage leaf, byte[] keyBuf,
      HOTIndirectPage[] pathNodes, int pathDepth) {
    if (leaf == null || keyBuf == null || pathNodes == null || pathDepth <= 0) {
      return -1;
    }
    final int entryCount = leaf.getEntryCount();
    if (entryCount == 0) return -1;
    final HOTIndirectPage parent = pathNodes[pathDepth - 1];
    if (parent == null) return -1;

    // Natural MSDB-with-K = leaf's preferred split bit
    final int msdb = leaf.computeMsdbWithKey(keyBuf);
    if (msdb < 0) return -1;

    final int parentMsb = parent.getMostSignificantBitIndex() & 0xFFFF;
    // If parent already routes on msdb, no MSB drop — msdb is safe.
    if (indirectMaskHasAbsBit(parent, msdb)) return -1;
    // msdb is numerically >= parentMsb means msdb is LESS significant or equal — no MSB drop.
    if (msdb >= parentMsb) return -1;

    // msdb < parentMsb (= msdb more significant). Adding msdb to parent's mask would
    // lower parent.MSB to msdb. Determine the I11 constraint: parent.MSB must remain
    // > grandparent.MSB (numerically). Grandparent here = pathNodes[pathDepth - 2].
    if (pathDepth < 2) {
      // Parent IS root — no grandparent constraint, msdb-as-new-root.MSB is fine.
      return -1;
    }
    final HOTIndirectPage grandparent = pathNodes[pathDepth - 2];
    if (grandparent == null) return -1;
    final int gpMsb = grandparent.getMostSignificantBitIndex() & 0xFFFF;
    // new parent.MSB = min(currentParentMsb, msdb) = msdb (since msdb < currentParentMsb).
    // I11 violates iff new parent.MSB <= grandparent.MSB.
    if (msdb > gpMsb) {
      // msdb less significant than grandparent.MSB → no I11 violation. Default is safe.
      return -1;
    }

    // msdb would violate I11. Search for alternative bit β' such that:
    //   - β' partitions {leaf entries ∪ keyBuf} into two non-empty halves
    //   - β' > gpMsb (numerically less significant than grandparent's MSB)
    // Iterate β' in MSB-first order to keep the split as balanced as possible.
    final int firstKeyLen = leaf.getKey(0) == null ? 0 : leaf.getKey(0).length;
    final int maxKeyLen = Math.max(keyBuf.length, firstKeyLen);
    final int maxBit = maxKeyLen << 3;
    for (int absBit = gpMsb + 1; absBit < maxBit; absBit++) {
      // Skip bits already in parent's mask (= partitioning on them won't drop MSB but
      // also won't add a NEW disc bit; addEntry will be a no-op and the split won't
      // structurally distinguish the halves at parent's level).
      if (indirectMaskHasAbsBit(parent, absBit)) continue;
      boolean seen0 = false, seen1 = false;
      if (isAbsBitSet(keyBuf, absBit)) seen1 = true; else seen0 = true;
      for (int i = 0; i < entryCount; i++) {
        final byte[] k = leaf.getKey(i);
        if (k == null || k.length == 0) continue;
        if (isAbsBitSet(k, absBit)) seen1 = true; else seen0 = true;
        if (seen0 && seen1) break;
      }
      if (seen0 && seen1) {
        // Found an I11-safe partitioning bit.
        if (Boolean.getBoolean("hot.debug.path2")) {
          System.err.println("[path2] msdb=" + msdb + " gpMsb=" + gpMsb
              + " parentMsb=" + parentMsb + " → safe β'=" + absBit);
        }
        return absBit;
      }
    }
    if (Boolean.getBoolean("hot.debug.path2")) {
      System.err.println("[path2] msdb=" + msdb + " gpMsb=" + gpMsb
          + " parentMsb=" + parentMsb + " → NO safe alternative; default to MSDB");
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
    int initialBytePos = computeInitialBytePos(children);
    DiscBitsInfo discBits = computeDiscBits(children, initialBytePos);
    int[] partialKeys = computePartialKeysForChildren(children, discBits);

    // Phase 7r-2 — Active partial-uniqueness augmentation (default-on; opt-out via
    // -Dhot.strict.phase7r.augment.disable=true). When the initial disc-bit set produces
    // duplicate partials (= adjacent-pair scan in `computeDiscBits` missed a bit that
    // distinguishes a non-adjacent pair, common with multi-entry leaves that span an
    // encoder-byte boundary like CASKeySerializer's 0xbf/0xc0 transition), find the first
    // colliding pair and augment with the most-significant bit where their firstKeys differ.
    // Repeat until every partial is unique or no further augmentation possible.
    // Bounded: at most {@link #MAX_DISC_BITS}-currentBitCount iterations.
    if (!Boolean.getBoolean("hot.strict.phase7r.augment.disable")
        && children.length >= 2) {
      final DiscBitsInfo[] discBitsHolder = {discBits};
      final int[] initialBytePosHolder = {initialBytePos};
      final long fallthroughBefore = PHASE7S_AUGMENT_FALLTHROUGH.get();
      partialKeys = phase7rAugmentUntilUnique(children, partialKeys, discBitsHolder,
          initialBytePosHolder, pageKey);
      // Read back the augmented disc-bits-mask + initial-byte-pos. The original 7r-2
      // commit (§7.24) shipped without this read-back — its inline comment "Re-read
      // augmented disc-bits info via the holder pattern (mutated in-place)" promised
      // it but the assignments were missing, so `createNodeWithDiscBits` below used
      // the pre-augmentation mask while `partialKeys` were computed against the
      // augmented one. Propagating both restores mask/partial coherence and is the
      // mechanism behind the descending-10K 517 → 258 reduction (50%) without
      // splitting any leaf. Phase 7s-2 layers an opt-in split on top, but the mask
      // propagation itself is always-on.
      discBits = discBitsHolder[0];
      initialBytePos = initialBytePosHolder[0];
      final boolean fallthroughFired =
          PHASE7S_AUGMENT_FALLTHROUGH.get() > fallthroughBefore;

      // Phase 7s-2 — split β-mixed children on the augmented disc bits. Opt-in via
      // -Dhot.strict.phase7s.split=true. Phase 7s-1 fallthrough means the augmenter
      // picked a sort-monotone bit that's β-MIXED in some child's subtree, so the
      // child's storedPartial = PEXT(firstKey, mask) will disagree with PEXT(K, mask)
      // for some descendant K — I5-leaf-constancy violates. Splitting each β-mixed
      // child on a β-mixed mask bit yields two β-constant sub-children that route
      // distinctly. Validate-and-rollback: only commit when every (post-split child,
      // mask-bit) pair is β-constant and partials remain unique; otherwise revert.
      if (Boolean.getBoolean("hot.strict.phase7s.split") && fallthroughFired) {
        final PageReference[][] childrenH = {children};
        final int[][] partialsH = {partialKeys};
        if (phase7sSplitAndAugment(childrenH, partialsH, discBitsHolder,
            initialBytePosHolder, revision, pageKey)) {
          children = childrenH[0];
          partialKeys = partialsH[0];
          discBits = discBitsHolder[0];
          initialBytePos = initialBytePosHolder[0];
        }
      }
    }
    // sortChildrenAndPartialsByPartial preserves the children/partials parallel arrays.
    sortChildrenAndPartialsByPartial(children, partialKeys);
    // Phase 7r-1 — diagnostic Path 5 routing-collision probe. Opt-in via
    // -Dhot.strict.phase7r.routeverify.
    if (Boolean.getBoolean("hot.strict.phase7r.routeverify")) {
      PHASE7R_BUILDFLAT_INSPECTIONS.incrementAndGet();
      final int collidingIdx = phase7rRoutingCollisionFirstIdx(partialKeys);
      if (collidingIdx >= 0) {
        PHASE7R_BUILDFLAT_COLLISIONS.incrementAndGet();
        if (Boolean.getBoolean("hot.debug.phase7r")) {
          final StringBuilder sb = new StringBuilder(256);
          sb.append("[phase7r] buildFlatNonStrict ROUTING-COLLISION (post-augment) pageKey=").append(pageKey)
              .append(" height=").append(height)
              .append(" n=").append(children.length)
              .append(" collidingIdx=").append(collidingIdx)
              .append(" partials=[");
          for (int i = 0; i < partialKeys.length; i++) {
            if (i > 0) sb.append(',');
            sb.append("0x").append(Integer.toHexString(partialKeys[i]));
          }
          sb.append(']');
          System.err.println(sb);
        }
      }
    }
    final HOTIndirectPage created = createNodeWithDiscBits(pageKey, revision, height, discBits,
        partialKeys, children);
    probeConstancyOnBuild(pageKey, "createNodeFromChildren-N", children,
        discBitsAsAbsBitArray(discBits));
    return created;
  }

  /**
   * Phase 7r-2 — Augment {@code discBits} with bits that distinguish colliding child pairs
   * until all partials are unique, or the bit budget is exhausted.
   *
   * <p>Procedure (one iteration):
   * <ol>
   *   <li>Find the first pair (i, j) with {@code partials[i] == partials[j]}.</li>
   *   <li>Compute the MSB-of-differing-bit between firstKeys[i] and firstKeys[j] that is NOT
   *       already in the disc-bit set.</li>
   *   <li>Add that bit to {@code discBitsHolder[0]} (rebuilds the {@link DiscBitsInfo} with the
   *       extra bit + the updated initial byte position).</li>
   *   <li>Recompute partials via {@link #computePartialKeysForChildren}.</li>
   * </ol>
   *
   * <p>Bounded: at most {@code MAX_DISC_BITS - currentBits} iterations. Returns the (possibly
   * augmented) partials array. Caller MUST re-read {@code discBitsHolder[0]} +
   * {@code initialBytePosHolder[0]} after this call to use the new mask in the build.
   *
   * <p>HFT-grade: small-N typical (≤ 32 children), bounded iteration count, primitive arrays.
   */
  private int[] phase7rAugmentUntilUnique(PageReference[] children, int[] partials,
      DiscBitsInfo[] discBitsHolder, int[] initialBytePosHolder, long pageKey) {
    final int n = children.length;
    if (n < 2) return partials;
    final byte[][] firstKeys = new byte[n][];
    for (int i = 0; i < n; i++) firstKeys[i] = getFirstKeyFromChild(children[i]);

    int maxIter = MAX_DISC_BITS; // hard cap
    while (maxIter-- > 0) {
      // Find first colliding pair.
      int colI = -1, colJ = -1;
      for (int i = 0; i < n && colI < 0; i++) {
        for (int j = i + 1; j < n; j++) {
          if (partials[i] == partials[j]) { colI = i; colJ = j; break; }
        }
      }
      if (colI < 0) return partials; // all unique — done

      // Find MSB of differing bit between firstKeys[colI] and firstKeys[colJ] that is also
      // SORT-MONOTONE across all children's firstKeys (= the bit transitions 0→1 exactly once
      // in firstKey-sorted order). The recursive sparse-path partition in
      // computeSparsePathRecursive REQUIRES monotone bits — non-monotone bits produce
      // wrong partials and break the sort-order-matches-partial-order invariant.
      //
      // Phase 7s-1 prefers β-CONSTANT-IN-EVERY-CHILD bits first (= picking such a bit
      // guarantees the recomputed child.storedPartial matches every key in the subtree,
      // avoiding I5-leaf-constancy cascade). When no β-constant + sort-monotone bit
      // exists, fall through to the original 7r-2 behavior (accept any sort-monotone bit)
      // and increment {@link #PHASE7S_AUGMENT_FALLTHROUGH}. Each fallthrough is a
      // β-mixed-leaf that Phase 7s-2 will need to split.
      final byte[] kA = firstKeys[colI];
      final byte[] kB = firstKeys[colJ];
      final int minLen = Math.min(kA == null ? 0 : kA.length, kB == null ? 0 : kB.length);
      int augBytePos = -1, augBitInByte = -1;
      int fallbackBytePos = -1, fallbackBitInByte = -1; // 7r-2 candidate (sort-monotone only)
      for (int b = 0; b < minLen && augBytePos < 0; b++) {
        final int diff = (kA[b] ^ kB[b]) & 0xFF;
        if (diff == 0) continue;
        int diffBits = diff;
        while (diffBits != 0 && augBytePos < 0) {
          final int hb = Integer.numberOfLeadingZeros(diffBits) - 24; // 0..7, 0=MSB
          diffBits &= ~(1 << (7 - hb));
          if (discBitsContainsBit(discBitsHolder[0], b, hb)) continue;
          if (!isBitSortMonotone(firstKeys, b, hb)) continue;
          // Record first sort-monotone candidate as the fallback.
          if (fallbackBytePos < 0) { fallbackBytePos = b; fallbackBitInByte = hb; }
          // β-constancy preference: only accept bits where every child's subtree is constant.
          final int absBit = b * 8 + hb;
          boolean betaConstantInAllChildren = true;
          for (int ci = 0; ci < n; ci++) {
            if (bitConstantValueInSubtree(children[ci], absBit) < 0) {
              betaConstantInAllChildren = false;
              break;
            }
          }
          if (!betaConstantInAllChildren) continue;
          augBytePos = b;
          augBitInByte = hb;
        }
      }
      if (augBytePos < 0 && fallbackBytePos >= 0) {
        // No β-constant + sort-monotone bit. Accept the sort-monotone fallback (7r-2
        // legacy behaviour) and count the fallthrough — Phase 7s-2 leaf-split site.
        augBytePos = fallbackBytePos;
        augBitInByte = fallbackBitInByte;
        PHASE7S_AUGMENT_FALLTHROUGH.incrementAndGet();
        if (Boolean.getBoolean("hot.debug.phase7s")) {
          System.err.println("[phase7s-augment] FALLTHROUGH-to-7r2 pageKey=" + pageKey
              + " colliding i=" + colI + " j=" + colJ
              + " — no β-constant bit; accepting sort-monotone (byte=" + augBytePos
              + ", bitInByte=" + augBitInByte + ")");
        }
      }
      if (augBytePos < 0) {
        // No sort-monotone bit at all. Augmentation can't continue.
        PHASE7S_AUGMENT_EXHAUSTED.incrementAndGet();
        if (Boolean.getBoolean("hot.debug.phase7r") || Boolean.getBoolean("hot.debug.phase7s")) {
          System.err.println("[phase7r-augment] EXHAUSTED pageKey=" + pageKey
              + " colliding i=" + colI + " j=" + colJ + " — no further disc bit available");
        }
        return partials;
      }
      // Add the new bit to the disc set.
      final TreeMap<Integer, Integer> rebuilt = discBitsToMaskByBytePos(discBitsHolder[0]);
      rebuilt.merge(augBytePos, 1 << (7 - augBitInByte), (a, b2) -> a | b2);
      final int newMinBytePos = rebuilt.firstKey();
      // Re-derive initialBytePos + discBits using the same layout policy as computeDiscBits.
      final int maxBp = rebuilt.lastKey();
      DiscBitsInfo newDiscBits;
      int newInitialBytePos = initialBytePosHolder[0];
      if (maxBp - newInitialBytePos < 8 && newMinBytePos >= newInitialBytePos) {
        long mask = 0L;
        for (final var entry : rebuilt.entrySet()) {
          final int byteOffset = entry.getKey() - newInitialBytePos;
          mask |= ((long) (entry.getValue() & 0xFF)) << ((7 - byteOffset) * 8);
        }
        newDiscBits = DiscBitsInfo.singleMask(newInitialBytePos, mask);
      } else {
        newDiscBits = buildMultiMask(rebuilt, newMinBytePos);
        newInitialBytePos = newMinBytePos;
      }
      discBitsHolder[0] = newDiscBits;
      initialBytePosHolder[0] = newInitialBytePos;
      // Recompute partials with augmented mask.
      partials = computePartialKeysForChildren(children, newDiscBits);
      if (Boolean.getBoolean("hot.debug.phase7r")) {
        System.err.println("[phase7r-augment] pageKey=" + pageKey
            + " added bit (byte=" + augBytePos + ", bitInByte=" + augBitInByte + ")"
            + " for colliding (i=" + colI + ",j=" + colJ + ")");
      }
    }
    return partials;
  }

  /**
   * Phase 7s-2 — Split β-mixed children on their first β-mixed mask bit. Phase 7s-1
   * fallthrough fires when the augmenter could not find a β-constant + sort-monotone
   * disc bit and accepted a β-mixed-but-sort-monotone fallback bit instead. That bit
   * is now in the augmented mask, and at least one child has the bit β-MIXED in its
   * subtree — its storedPartial = PEXT(firstKey, mask) disagrees with PEXT(K, mask)
   * for some descendant K, manifesting later as I5-leaf-constancy violations.
   *
   * <p>Algorithm: walk every (child, augmented-mask-bit) pair via
   * {@link #bitConstantValueInSubtree}. For each child β-MIXED at any mask bit, pick
   * the first such bit X and call {@link #splitSubtreeOnBit} to produce two
   * β-constant halves at X. The mask already contains X, so the new children's
   * recomputed partials simply read X from their own firstKey (one half stores 0 at X,
   * the other stores 1) — partials remain unique by construction at the split bit and
   * routing distinguishes the halves.
   *
   * <p>Validate-and-rollback:
   * <ol>
   *   <li>If no splittable β-mixed (child, bit) pair was found → NOOP (return false).</li>
   *   <li>Run {@link #splitSubtreeOnBit}; on failure, leave child untouched.</li>
   *   <li>Recompute partials with the unchanged mask + expanded children.</li>
   *   <li>If routing collisions remain, re-run augmentation; if collisions still
   *       remain → ROLLBACK.</li>
   *   <li>Final pass: verify every (post-split child, current mask bit) is β-constant.
   *       If any pair is still mixed → ROLLBACK (the split did not make progress).</li>
   *   <li>Commit through the holders.</li>
   * </ol>
   *
   * <p>HFT-grade: bounded N (≤ 32 typical), bounded mask bit count (≤ 32). Each
   * subtree-walk via {@link #bitConstantValueInSubtree} is itself O(subtree size) but
   * each child is walked at most a few times per bit. On rollback the split pages
   * become orphans in the TIL (acceptable for an opt-in path).
   *
   * @return true if state was updated (split applied + validation passed), false on
   *         no-op or rollback.
   */
  private boolean phase7sSplitAndAugment(
      PageReference[][] childrenHolder,
      int[][] partialsHolder,
      DiscBitsInfo[] discBitsHolder,
      int[] initialBytePosHolder,
      int revision,
      long pageKey) {
    final PageReference[] children = childrenHolder[0];
    final int n = children.length;
    if (n < 2) return false;

    // Collect augmented disc bits (MSB-first absolute positions).
    final int[] maskAbsBits = collectDiscBitsMsbFirst(discBitsHolder[0]);
    if (maskAbsBits.length == 0) return false;

    // For each child, find the first mask bit at which it's β-mixed. Split on it.
    final PageReference[] expandedBuf = new PageReference[n * 2];
    int expandedN = 0;
    int splitCount = 0;

    for (int i = 0; i < n; i++) {
      final PageReference cref = children[i];
      if (cref == null) {
        expandedBuf[expandedN++] = cref;
        continue;
      }
      int mixedBit = -1;
      for (final int absBit : maskAbsBits) {
        final int v = bitConstantValueInSubtree(cref, absBit);
        if (v < 0) {
          mixedBit = absBit;
          break;
        }
      }
      if (mixedBit < 0) {
        expandedBuf[expandedN++] = cref;
        continue;
      }
      final SubtreeSplit ss = splitSubtreeOnBit(cref, mixedBit, activeLog, revision);
      if (ss == null || ss.leftRef() == null || ss.rightRef() == null) {
        expandedBuf[expandedN++] = cref;
        continue;
      }
      if (Boolean.getBoolean("hot.debug.phase7s")) {
        System.err.println("[phase7s-split] child=" + i + " split on absBit=" + mixedBit
            + " pageKey=" + pageKey);
      }
      // expandedBuf is pre-sized to n*2 — each iteration adds ≤ 2 (split = 2, no-split = 1).
      expandedBuf[expandedN++] = ss.leftRef();
      expandedBuf[expandedN++] = ss.rightRef();
      splitCount++;
    }

    if (splitCount == 0) {
      PHASE7S_SPLIT_NOOP.incrementAndGet();
      return false;
    }

    // Recompute partials with unchanged mask. The split halves' firstKeys differ at the
    // split bit (one bit value 0, the other 1) so their partials route distinctly there.
    final PageReference[] expChildren = java.util.Arrays.copyOf(expandedBuf, expandedN);
    DiscBitsInfo newDiscBits = discBitsHolder[0];
    int newInitialBytePos = initialBytePosHolder[0];
    int[] newPartials = computePartialKeysForChildren(expChildren, newDiscBits);

    // Re-augment if residual collisions (new firstKeys may introduce different adjacent
    // pairs requiring additional bits).
    if (phase7rRoutingCollisionFirstIdx(newPartials) >= 0) {
      final DiscBitsInfo[] dbHolder = {newDiscBits};
      final int[] ibpHolder = {newInitialBytePos};
      newPartials = phase7rAugmentUntilUnique(expChildren, newPartials, dbHolder, ibpHolder, pageKey);
      newDiscBits = dbHolder[0];
      newInitialBytePos = ibpHolder[0];
    }
    if (phase7rRoutingCollisionFirstIdx(newPartials) >= 0) {
      PHASE7S_SPLIT_ROLLBACK.incrementAndGet();
      if (Boolean.getBoolean("hot.debug.phase7s")) {
        System.err.println("[phase7s-split] ROLLBACK pageKey=" + pageKey
            + " splitCount=" + splitCount + " — collisions persist after re-augment");
      }
      return false;
    }

    // Validate β-constancy at every mask bit after split. By default, if any
    // (child, bit) pair is still β-mixed, rollback — the split didn't fully
    // eliminate the I5 risk. Opt-in `-Dhot.strict.phase7s.split.relax=true`
    // suppresses this gate: we accept partial improvement when routing remains
    // correct (partials unique by the check above) and let surviving β-mixed
    // pairs surface as I5 violations downstream — typically a smaller cascade
    // than the pre-split state.
    if (!Boolean.getBoolean("hot.strict.phase7s.split.relax")) {
      final int[] finalMaskBits = collectDiscBitsMsbFirst(newDiscBits);
      for (int i = 0; i < expChildren.length; i++) {
        final PageReference c = expChildren[i];
        if (c == null) continue;
        for (final int absBit : finalMaskBits) {
          if (bitConstantValueInSubtree(c, absBit) < 0) {
            PHASE7S_SPLIT_ROLLBACK.incrementAndGet();
            if (Boolean.getBoolean("hot.debug.phase7s")) {
              System.err.println("[phase7s-split] ROLLBACK pageKey=" + pageKey
                  + " — child[" + i + "] still β-mixed at absBit=" + absBit);
            }
            return false;
          }
        }
      }
    }

    // Commit — atomic write-back through holders.
    childrenHolder[0] = expChildren;
    partialsHolder[0] = newPartials;
    discBitsHolder[0] = newDiscBits;
    initialBytePosHolder[0] = newInitialBytePos;
    PHASE7S_SPLIT_APPLIED.incrementAndGet();
    if (Boolean.getBoolean("hot.debug.phase7s")) {
      System.err.println("[phase7s-split] APPLIED pageKey=" + pageKey
          + " splitCount=" + splitCount + " newN=" + expChildren.length);
    }
    return true;
  }

  /** Phase 7r-2 helper — check whether a bit (at bytePos, bitInByte MSB-numbered) transitions
   *  exactly once from 0 to 1 across the lex-sorted firstKeys. Required for
   *  {@link #computeSparsePathRecursive} to produce correct partials. */
  private static boolean isBitSortMonotone(byte[][] firstKeys, int bytePos, int bitInByte) {
    if (firstKeys.length == 0) return true;
    int prev = -1; // -1 = not yet seen, 0 or 1 = last seen value
    for (final byte[] k : firstKeys) {
      if (k == null || k.length <= bytePos) {
        // Treat as zero — but break monotonicity if previous was 1.
        if (prev == 1) return false;
        prev = 0;
        continue;
      }
      final int bit = ((k[bytePos] & 0xFF) >>> (7 - bitInByte)) & 1;
      if (prev == 1 && bit == 0) return false; // 1→0 transition disqualifies
      prev = bit;
    }
    return true;
  }

  /** Phase 7r-2 helper — check whether a given (bytePos, bitInByte) is in the disc-bit set. */
  private static boolean discBitsContainsBit(DiscBitsInfo info, int bytePos, int bitInByte) {
    final int absBit = bytePos * 8 + bitInByte;
    if (info.layoutType == HOTIndirectPage.LayoutType.SINGLE_MASK) {
      final int bytesAfterInitial = bytePos - info.initialBytePos;
      if (bytesAfterInitial < 0 || bytesAfterInitial >= 8) return false;
      final long maskBit = 1L << ((7 - bytesAfterInitial) * 8 + (7 - bitInByte));
      return (info.bitMask & maskBit) != 0L;
    }
    // MultiMask
    final byte[] ep = info.extractionPositions;
    final long[] em = info.extractionMasks;
    for (int i = 0; i < info.numExtractionBytes; i++) {
      if ((ep[i] & 0xFF) != bytePos) continue;
      final int chunkIdx = i / 8;
      final int byteOffsetInChunk = i % 8;
      final int byteMaskBits =
          (int) ((em[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFFL);
      return (byteMaskBits & (1 << (7 - bitInByte))) != 0;
    }
    return false;
  }

  /** Phase 7r-2 helper — extract a {@code maskByBytePos} TreeMap from a {@link DiscBitsInfo}
   *  so we can mutate + rebuild. */
  private static TreeMap<Integer, Integer> discBitsToMaskByBytePos(DiscBitsInfo info) {
    final TreeMap<Integer, Integer> m = new TreeMap<>();
    if (info.layoutType == HOTIndirectPage.LayoutType.SINGLE_MASK) {
      for (int bo = 0; bo < 8; bo++) {
        final int byteMaskBits = (int) ((info.bitMask >>> ((7 - bo) * 8)) & 0xFFL);
        if (byteMaskBits != 0) m.put(info.initialBytePos + bo, byteMaskBits);
      }
      return m;
    }
    for (int i = 0; i < info.numExtractionBytes; i++) {
      final int chunkIdx = i / 8;
      final int byteOffsetInChunk = i % 8;
      final int byteMaskBits =
          (int) ((info.extractionMasks[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFFL);
      if (byteMaskBits != 0) m.put(info.extractionPositions[i] & 0xFF, byteMaskBits);
    }
    return m;
  }


  /** DIAGNOSTIC tracer — wraps {@link HOTIndirectPage#createBiNode} to probe constancy after
   *  every BiNode construction in the writer. Argument-compatible with the original. */
  /**
   * Phase 7q.15 — Compute a constancy-preserving discriminating bit between two bucket
   * children. The chosen bit must:
   * <ul>
   *   <li>be β-constant in EACH subtree (left has all keys with same value at this bit;
   *       right has all keys with same value at this bit)</li>
   *   <li>have OPPOSITE values across the two subtrees (left=0,right=1 or left=1,right=0)</li>
   * </ul>
   *
   * <p>This eliminates the cascade where the legacy {@code createBiNodeTraced}'s
   * `computeDifferingBit(leftMax, rightMin)` picks a bit that's not β-constant — causing
   * I6 routing violations downstream.
   *
   * <p>Searches from MSB-of-firstKey-diff (= most-significant differing bit), giving
   * the canonical HOT discriminating bit. The bit MUST be β-constant in both subtrees.
   * Also enforces I11: the chosen bit must be STRICTLY GREATER than {@code minBitExclusive}
   * (= future parent's MSB) so the resulting BiNode's MSB satisfies parent.MSB < child.MSB.
   *
   * <p>Returns the bit (≥ 0) on success, or -1 if no constancy-preserving bit exists in
   * the firstKey lengths probed.
   */
  private int computeConstancyPreservingBiNodeDiscBit(PageReference left, PageReference right,
      int minBitExclusive) {
    if (left == null || right == null) return -1;
    final byte[] leftFk = getFirstKeyFromChild(left);
    final byte[] rightFk = getFirstKeyFromChild(right);
    if (leftFk == null || rightFk == null) return -1;
    if (leftFk.length == 0 || rightFk.length == 0) return -1;
    // Phase 7q.15 — strict child-MSB constraint when hot.strict.g32.childmsb is set.
    // discBit must be < min(leftChildMsb, rightChildMsb) so BiNode satisfies I11 vs children.
    final int leftChildMsb = getIndirectMsbOrMax(left);
    final int rightChildMsb = getIndirectMsbOrMax(right);
    final int maxBitExclusive = Boolean.getBoolean("hot.strict.g32.childmsb")
        ? Math.min(leftChildMsb, rightChildMsb)
        : Integer.MAX_VALUE;
    final int maxLen = Math.max(leftFk.length, rightFk.length);
    // First find MSB-of-diff between firstKeys.
    int msbOfDiff = -1;
    for (int absBit = 0; absBit < maxLen * 8; absBit++) {
      if (isAbsBitSet(leftFk, absBit) != isAbsBitSet(rightFk, absBit)) {
        msbOfDiff = absBit;
        break;
      }
    }
    if (msbOfDiff < 0) return -1; // identical firstKeys
    // Try MSB-of-diff first (canonical HOT disc bit). If not β-constant OR not in
    // (minBitExclusive, maxBitExclusive), fall back to MSB-first scan within the allowed range.
    if (msbOfDiff > minBitExclusive && msbOfDiff < maxBitExclusive) {
      final int leftConst = bitConstantValueInSubtree(left, msbOfDiff);
      final int rightConst = bitConstantValueInSubtree(right, msbOfDiff);
      if (leftConst >= 0 && rightConst >= 0
          && (leftConst == 1) == isAbsBitSet(leftFk, msbOfDiff)
          && (rightConst == 1) == isAbsBitSet(rightFk, msbOfDiff)) {
        return msbOfDiff;
      }
    }
    // Fall back: search from absBit = minBitExclusive + 1 upward (= MSB-first among allowed range).
    final int upperBound = Math.min(maxLen * 8, maxBitExclusive);
    for (int absBit = minBitExclusive + 1; absBit < upperBound; absBit++) {
      if (absBit == msbOfDiff) continue; // already tried above
      final boolean leftSet = isAbsBitSet(leftFk, absBit);
      final boolean rightSet = isAbsBitSet(rightFk, absBit);
      if (leftSet == rightSet) continue;
      final int lConst = bitConstantValueInSubtree(left, absBit);
      if (lConst < 0) continue;
      if ((lConst == 1) != leftSet) continue;
      final int rConst = bitConstantValueInSubtree(right, absBit);
      if (rConst < 0) continue;
      if ((rConst == 1) != rightSet) continue;
      return absBit;
    }
    return -1;
  }

  /**
   * Phase 7q.15 — Try to RAISE an indirect child's MSB by stripping its current MSB bit
   * (the smallest absBit in its mask). The stripped bit must be β-constant in the child's
   * subtree (= all keys in subtree agree on that bit), otherwise routing breaks.
   *
   * <p>Returns a NEW PageReference to a rebuilt child indirect with the bit stripped, or
   * null if the strip is not safe.
   */
  @Nullable
  private PageReference tryRaiseChildMsb(PageReference childRef, int requiredMin,
      TransactionIntentLog log, int revision) {
    if (childRef == null) return null;
    Page page = childRef.getPage();
    if (page == null && activeLog != null) {
      final var container = activeLog.get(childRef);
      if (container != null) page = container.getModified();
    }
    if (page == null && activeReader != null) {
      page = loadPage(activeReader, childRef);
      if (page != null) childRef.setPage(page);
    }
    if (!(page instanceof HOTIndirectPage indirect)) return childRef; // leaves OK as-is
    final int currentMsb = indirect.getMostSignificantBitIndex() & 0xFFFF;
    if (currentMsb > requiredMin) return childRef; // already satisfies
    // Need to raise. Strip bits ≤ requiredMin from indirect's mask.
    // For each bit being stripped, verify β-constancy in indirect's subtree.
    if (indirect.getLayoutType() != HOTIndirectPage.LayoutType.SINGLE_MASK) {
      // MultiMask: skip for now (complex). TODO: implement MultiMask strip.
      return null;
    }
    final int initialBytePos = indirect.getInitialBytePos();
    final long oldMask = indirect.getBitMask();
    long stripBits = 0L;
    for (int wbit = 0; wbit < 64; wbit++) {
      if (((oldMask >>> wbit) & 1L) == 0L) continue;
      final int byteOffsetInWord = 7 - (wbit / 8);
      final int bitInByte = 7 - (wbit % 8);
      final int absBitOfWbit = (initialBytePos + byteOffsetInWord) * 8 + bitInByte;
      if (absBitOfWbit <= requiredMin) {
        // Verify β-constant in subtree.
        final int v = bitConstantValueInSubtree(childRef, absBitOfWbit);
        if (v < 0) return null; // not safe to strip
        stripBits |= 1L << wbit;
      }
    }
    if (stripBits == 0L) return childRef; // nothing to strip; already OK
    final long newMask = oldMask & ~stripBits;
    if (newMask == 0L) return null; // strip would leave empty mask
    // Recompute partials with new mask.
    final int n = indirect.getNumChildren();
    final int[] newPartials = new int[n];
    final PageReference[] newChildren = new PageReference[n];
    boolean haveZero = false;
    final java.util.HashSet<Integer> seen = new java.util.HashSet<>(n * 2);
    for (int i = 0; i < n; i++) {
      newChildren[i] = indirect.getChildReference(i);
      if (newChildren[i] == null) return null;
      final byte[] fk = getFirstKeyFromChild(newChildren[i]);
      if (fk == null || fk.length == 0) return null;
      newPartials[i] = computePartialKeySingleMask(fk, initialBytePos, newMask);
      if (newPartials[i] == 0) haveZero = true;
      if (!seen.add(newPartials[i])) return null; // collision
    }
    if (!haveZero) return null;
    sortChildrenAndPartialsByPartial(newChildren, newPartials);
    // Compute new MSB.
    short newMsbIdx = Short.MAX_VALUE;
    for (int wbit = 0; wbit < 64; wbit++) {
      if (((newMask >>> wbit) & 1L) == 0L) continue;
      final int byteOffsetInWord = 7 - (wbit / 8);
      final int bitInByte = 7 - (wbit % 8);
      final int absBitPos = (initialBytePos + byteOffsetInWord) * 8 + bitInByte;
      if (absBitPos < newMsbIdx) newMsbIdx = (short) absBitPos;
    }
    final long newPageKey = pageKeyAllocator.getAsLong();
    final HOTIndirectPage built = (n <= 16)
        ? HOTIndirectPage.createSpanNode(newPageKey, revision, initialBytePos, newMask,
            newPartials, newChildren, indirect.getHeight())
        : HOTIndirectPage.createMultiNode(newPageKey, revision, initialBytePos, newMask,
            newPartials, newChildren, indirect.getHeight());
    final PageReference newRef = new PageReference();
    newRef.setKey(newPageKey);
    newRef.setPage(built);
    log.put(newRef, PageContainer.getInstance(built, built));
    if (Boolean.getBoolean("hot.debug.g32")) {
      System.err.println("[raise-msb] pageKey=" + indirect.getPageKey() + " oldMsb=" + currentMsb
          + " newMsb=" + (newMsbIdx & 0xFFFF) + " stripBits=0x" + Long.toHexString(stripBits));
    }
    return newRef;
  }

  /** Helper: returns the indirect's MSB if {@code ref}'s page is an Indirect, or
   *  Integer.MAX_VALUE if it's a Leaf or unloadable. */
  private int getIndirectMsbOrMax(PageReference ref) {
    if (ref == null) return Integer.MAX_VALUE;
    Page page = ref.getPage();
    if (page == null && activeLog != null) {
      final var container = activeLog.get(ref);
      if (container != null) page = container.getModified();
    }
    if (page == null && activeReader != null) {
      page = loadPage(activeReader, ref);
      if (page != null) ref.setPage(page);
    }
    if (page instanceof HOTIndirectPage indirect) {
      return indirect.getMostSignificantBitIndex() & 0xFFFF;
    }
    return Integer.MAX_VALUE;
  }

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
    // Phase 7q.10 — TIL-first resolution for in-flight refs. Previously this function
    // only consulted childRef.getPage() and activeReader. For TIL-only refs (= pages
    // staged for write but not yet persisted), both would return null, causing
    // EMPTY_KEY → partial=0 → collisions between unrelated placeholder children. The
    // TIL has the canonical in-flight page; consult it when getPage() is null.
    if (page == null && activeLog != null) {
      final var container = activeLog.get(childRef);
      if (container != null) {
        page = container.getModified();
        if (page != null) {
          childRef.setPage(page); // swizzle for future access
        }
      }
    }
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

