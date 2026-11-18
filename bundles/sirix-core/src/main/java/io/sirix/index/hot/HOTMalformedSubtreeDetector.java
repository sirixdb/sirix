/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.index.hot.page.HOTIndirectPage;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Structural validation of HOT secondary-index tries (see {@code
 * docs/HOT_INDEX_SPECIFICATION.md} §4.8). Walks a trie top-down and returns the <em>highest</em>
 * malformed indirect pages — the roots of the maximal drifted subtrees.
 *
 * <p>
 * <b>What "malformed" means.</b> An indirect is malformed when it violates a structural invariant
 * in its own block. The predicates mirror {@code HOTInvariantValidator}, the test-only invariant
 * oracle; a production detector cannot depend on that test-only class. The enforced predicates are
 * specified in {@code docs/HOT_INDEX_SPECIFICATION.md} §2.8. I11 is cross-level; a violation on
 * edge {@code (P, C)} is attributed to the <em>parent</em> {@code P}, whose child edge violates the
 * trie condition.
 *
 * <p>
 * <b>Highest-malformed semantics.</b> The walk is a pre-order DFS. When an indirect is found
 * malformed, its page is recorded and the walk does <em>not</em> descend into it. The returned
 * subtrees are pairwise non-nested; the caller decides how to handle the defects.
 *
 * <p>
 * <b>Purity.</b> The detector reads only; it never mutates a page or performs I/O. Pages are
 * obtained through a caller-supplied {@link PageResolver} so the same detector serves a commit-path
 * writer (TIL-aware resolution) and a unit test (swizzled-page resolution).
 *
 * <p>
 * <b>Leaf lifetime.</b> A resolved leaf has no lifetime guarantee of its own: the page cache may
 * evict it at any moment, and its frame is then handed out again and overwritten. The detector
 * therefore reads a leaf's keys only while it holds a guard on that leaf, and releases the guard as
 * soon as the read ends, so it pins at most one leaf at a time. A leaf that was evicted before the
 * guard could be taken is resolved again; see {@link #MAX_LEAF_RETAIN_ATTEMPTS}.
 *
 * <p>
 * <b>Cost.</b> One DFS per index. The I5 check walks each indirect's subtree keys; summed over a
 * root-to-leaf path a key is visited once per ancestor, and HOT's height is a small constant
 * (height-optimized — {@code log_k |S|}), so the pass is {@code O(|S|)} per index.
 *
 * @author Johannes Lichtenberger
 * @see HOTBulkBuilder
 * @see HOTIndirectPage#computeDensePartialKey(byte[])
 */
public final class HOTMalformedSubtreeDetector {

  /** Runaway-recursion guard. HOT is height-optimized (shallow); this is a generous upper bound. */
  private static final int MAX_DEPTH = 64;

  /**
   * How often one leaf read resolves its reference before giving up on a leaf that eviction closes
   * between resolution and guard acquisition. Every failed attempt resolves again, which reloads the
   * leaf, so losing all of them takes a cache that cannot keep a freshly loaded leaf for the few
   * instructions in between. Such a leaf is skipped like an unresolvable reference: the detector then
   * under-reports, and never reports a defect from bytes it could not hold still.
   */
  static final int MAX_LEAF_RETAIN_ATTEMPTS = 8;

  /**
   * Runs on each retained leaf before its keys are read; see {@link #setRetainedLeafTestHook}.
   * Volatile publishes the hook reference; it does not protect mutable hook state.
   */
  @SuppressWarnings("java:S3077")
  private static volatile @Nullable Consumer<HOTLeafPage> retainedLeafTestHook;

  /**
   * Resolves a {@link PageReference} to its in-memory {@link Page}. Returning {@code null} marks the
   * reference as unresolvable; the detector then skips that branch rather than failing.
   */
  @FunctionalInterface
  public interface PageResolver {
    @Nullable
    Page resolve(PageReference reference);
  }

  /**
   * A maximal malformed subtree: the indirect page that is the highest violating node of a drifted
   * region.
   *
   * @param reference the in-trie reference whose page is the malformed indirect
   * @param indirectPageKey the malformed indirect's page key
   * @param invariant the first violated invariant tag (diagnostic)
   * @param detail a human-readable description of the violation (diagnostic)
   */
  public record MalformedSubtree(PageReference reference, long indirectPageKey, String invariant, String detail) {
  }

  /** First violated invariant of an indirect's own block — {@code null} encodes "clean". */
  private record Defect(String invariant, String detail) {
  }

  private final PageResolver resolver;
  private final List<MalformedSubtree> malformed = new ArrayList<>();
  private final Set<Long> visitedIndirectPageKeys = new HashSet<>();

  private HOTMalformedSubtreeDetector(final PageResolver resolver) {
    this.resolver = resolver;
  }

  /**
   * Walk the HOT trie rooted at {@code rootReference} and return its highest malformed indirects.
   *
   * @param rootReference the trie root; {@code null} (an unmaterialized index) yields an empty result
   * @param resolver resolves page references to in-memory pages
   * @return the maximal malformed subtrees in pre-order; empty when the trie is canonical or has no
   *         indirect pages (a single-leaf index cannot have a malformed <em>indirect</em>)
   * @throws NullPointerException if {@code resolver} is {@code null}
   */
  public static List<MalformedSubtree> detect(final @Nullable PageReference rootReference,
      final PageResolver resolver) {
    Objects.requireNonNull(resolver, "resolver");
    if (rootReference == null) {
      return List.of();
    }
    final HOTMalformedSubtreeDetector detector = new HOTMalformedSubtreeDetector(resolver);
    final Page root = detector.resolve(rootReference);
    if (root instanceof HOTIndirectPage indirect) {
      detector.walk(rootReference, indirect, 0);
    }
    return List.copyOf(detector.malformed);
  }

  // ======================================================================
  // Pre-order DFS — record a malformed indirect and stop; descend into clean ones.
  // ======================================================================

  private void walk(final PageReference reference, final HOTIndirectPage indirect, final int depth) {
    if (depth > MAX_DEPTH) {
      return;
    }
    final long pageKey = indirect.getPageKey();
    if (pageKey >= 0 && !visitedIndirectPageKeys.add(pageKey)) {
      return; // cycle guard — a well-formed HOT has none, but never loop on a corrupt one
    }
    final Defect defect = firstViolation(indirect);
    if (defect != null) {
      // Stop here so a malformed descendant cannot produce a second report for this region.
      malformed.add(new MalformedSubtree(reference, pageKey, defect.invariant(), defect.detail()));
      return;
    }
    final int n = indirect.getNumChildren();
    for (int i = 0; i < n; i++) {
      final PageReference childReference = indirect.getChildReference(i);
      if (childReference == null) {
        continue;
      }
      final Page child = resolve(childReference);
      if (child instanceof HOTIndirectPage childIndirect) {
        walk(childReference, childIndirect, depth + 1);
      }
    }
  }

  // ======================================================================
  // Per-indirect predicates — cheapest first; the first failure flags the indirect.
  // ======================================================================

  private @Nullable Defect firstViolation(final HOTIndirectPage indirect) {
    final int n = indirect.getNumChildren();
    if (n < 2 || n > HOTIndirectPage.MAX_NODE_ENTRIES) {
      return new Defect("I10-child-count", "indirect must have 2-32 children, got: " + n);
    }

    final int[] partials = indirect.getPartialKeys();
    final Defect partialViolation = firstPartialViolation(partials, n);
    if (partialViolation != null) {
      return partialViolation;
    }
    final Defect trieViolation = firstTrieViolation(indirect, n);
    if (trieViolation != null) {
      return trieViolation;
    }
    final Defect rangeViolation = firstRangeViolation(indirect, n);
    if (rangeViolation != null) {
      return rangeViolation;
    }
    return firstConstancyViolation(indirect, partials, n);
  }

  private static @Nullable Defect firstPartialViolation(final int[] partials, final int n) {
    if (partials.length < n) {
      return null;
    }
    // I3 — partial-key uniqueness. n <= 32, so the O(n^2) scan beats a boxed HashSet.
    for (int i = 0; i < n; i++) {
      for (int j = i + 1; j < n; j++) {
        if (partials[i] == partials[j]) {
          return new Defect("I3-partial-key-uniqueness",
              "duplicate partial 0x" + Integer.toHexString(partials[i]) + " at children " + i + " and " + j);
        }
      }
    }

    // I4 — first-partial-zero: the unsigned-smallest stored partial must be 0.
    int minPartial = partials[0];
    for (int i = 1; i < n; i++) {
      if (Integer.compareUnsigned(partials[i], minPartial) < 0) {
        minPartial = partials[i];
      }
    }
    if (minPartial != 0) {
      return new Defect("I4-first-partial-zero",
          "smallest stored partial is 0x" + Integer.toHexString(minPartial) + " (must be 0)");
    }

    // I7 — partial keys strictly ascending (unsigned) across child slots.
    for (int i = 1; i < n; i++) {
      if (Integer.compareUnsigned(partials[i], partials[i - 1]) <= 0) {
        return new Defect("I7-partial-keys-sorted", "partials not ascending at " + (i - 1) + "->" + i + ": 0x"
            + Integer.toHexString(partials[i - 1]) + " vs 0x" + Integer.toHexString(partials[i]));
      }
    }

    return null;
  }

  private @Nullable Defect firstTrieViolation(final HOTIndirectPage indirect, final int n) {
    // I11 — trie condition. Every indirect child's MSB must be strictly less significant (a
    // larger absolute bit position) than this node's MSB. A violation belongs to this parent.
    final int parentMSB = indirect.getMostSignificantBitIndex();
    if (parentMSB >= 0) {
      for (int i = 0; i < n; i++) {
        final PageReference childReference = indirect.getChildReference(i);
        if (childReference == null) {
          continue;
        }
        if (resolve(childReference) instanceof HOTIndirectPage childIndirect) {
          final int childMSB = childIndirect.getMostSignificantBitIndex();
          if (childMSB >= 0 && childMSB <= parentMSB) {
            return new Defect("I11-trie-condition", "child[" + i + "]=" + childIndirect.getPageKey() + " MSB="
                + childMSB + " must be > parent MSB=" + parentMSB);
          }
        }
      }
    }

    return null;
  }

  private @Nullable Defect firstRangeViolation(final HOTIndirectPage indirect, final int n) {
    // I8 — children ordered by ascending first-key of their subtree — and I12, its strict form:
    // consecutive children's key RANGES must not interleave. I8 alone compares first keys, so a
    // child whose subtree also holds keys sorting after its successor's first key passes it; point
    // lookups still route, but a range scan silently emits keys out of order. The check is local on
    // purpose: a child's extremes are read off its leftmost/rightmost edge descents, which report
    // the true extremes exactly when the child's own subtree is clean — and detectAndHeal re-runs
    // this detector after every discharge until nothing is found, so a spurious pass against a
    // still-malformed child cannot survive to that fixed point. At the fixed point the local
    // condition implies full range-disjointness by induction over the height, at O(children x
    // height) per node instead of the O(subtree) key walk the direct range computation would cost.
    byte[] previousFirstKey = null;
    byte[] previousLastKey = null;
    for (int i = 0; i < n; i++) {
      final PageReference childReference = indirect.getChildReference(i);
      if (childReference == null) {
        continue;
      }
      final byte[] firstKey = firstKeyOfSubtree(childReference);
      if (firstKey == null) {
        continue;
      }
      if (previousFirstKey != null && Arrays.compareUnsigned(previousFirstKey, firstKey) >= 0) {
        return new Defect("I8-children-sorted-by-firstkey",
            "child[" + i + "] firstKey " + hex(firstKey) + " <= a preceding child's firstKey " + hex(previousFirstKey));
      }
      if (previousLastKey != null && Arrays.compareUnsigned(previousLastKey, firstKey) >= 0) {
        return new Defect("I12-subtree-ranges-disjoint", "child[" + i + "] starts at " + hex(firstKey)
            + " but a preceding child's subtree spans up to " + hex(previousLastKey) + " — the subtrees interleave");
      }
      previousFirstKey = firstKey;
      final byte[] lastKey = lastKeyOfSubtree(childReference);
      if (lastKey != null) {
        previousLastKey = lastKey;
      }
    }

    return null;
  }

  private @Nullable Defect firstConstancyViolation(final HOTIndirectPage indirect, final int[] partials, final int n) {
    // I5 — leaf-constancy. The expensive check (a subtree-wide key walk) runs last so a cheaper
    // defect short-circuits it. For each child with a non-zero stored partial, every key in the
    // child's subtree must have a dense partial key that supersets the stored partial.
    if (partials.length >= n) {
      final int[] failingDensePK = new int[1];
      for (int i = 0; i < n; i++) {
        final int sparsePartial = partials[i];
        if (sparsePartial == 0) {
          continue; // no on-path bit to verify (0 is a subset of every dense partial key)
        }
        final PageReference childReference = indirect.getChildReference(i);
        if (childReference == null) {
          continue;
        }
        final byte[] failingKey = findI5FailingKey(childReference, indirect, sparsePartial, 0, failingDensePK);
        if (failingKey != null) {
          return new Defect("I5-leaf-constancy",
              "child[" + i + "] stored partial 0x" + Integer.toHexString(sparsePartial)
                  + " is not a subset of dense PEXT 0x" + Integer.toHexString(failingDensePK[0]) + " of subtree key "
                  + hex(failingKey));
        }
      }
    }

    return null;
  }

  // ======================================================================
  // Subtree helpers.
  // ======================================================================

  /**
   * Find the first key in the subtree at {@code reference} whose dense partial key under
   * {@code maskNode}'s mask fails the sparse-path subset condition for {@code sparsePartial} (i.e.
   * {@code (sparsePartial & ~densePK) != 0}). Returns {@code null} when every key passes;
   * {@code failingDensePK[0]} receives the offending key's dense partial key.
   */
  private byte @Nullable [] findI5FailingKey(final PageReference reference, final HOTIndirectPage maskNode,
      final int sparsePartial, final int depth, final int[] failingDensePK) {
    if (depth > MAX_DEPTH) {
      return null;
    }
    final Page page = resolveRetained(reference);
    if (page instanceof HOTLeafPage leaf) {
      try {
        beforeLeafRead(leaf);
        final int entryCount = leaf.getEntryCount();
        for (int i = 0; i < entryCount; i++) {
          final byte[] key = leaf.getKey(i);
          if (key == null || key.length == 0) {
            continue;
          }
          // Tombstones are keys (foundation §1.2) — they participate in I5 like any other entry.
          final int densePartialKey = maskNode.computeDensePartialKey(key);
          if ((sparsePartial & ~densePartialKey) != 0) {
            failingDensePK[0] = densePartialKey;
            return key;
          }
        }
        return null;
      } finally {
        leaf.releaseGuard();
      }
    }
    if (page instanceof HOTIndirectPage indirect) {
      final int n = indirect.getNumChildren();
      for (int i = 0; i < n; i++) {
        final PageReference childReference = indirect.getChildReference(i);
        if (childReference == null) {
          continue;
        }
        final byte[] failing = findI5FailingKey(childReference, maskNode, sparsePartial, depth + 1, failingDensePK);
        if (failing != null) {
          return failing;
        }
      }
    }
    return null;
  }

  /**
   * The first (lex-smallest) key of the subtree at {@code reference}, found by leftmost descent.
   * Returns {@code null} for an empty or unresolvable subtree.
   */
  private byte @Nullable [] firstKeyOfSubtree(final PageReference reference) {
    PageReference current = reference;
    for (int depth = 0; depth <= MAX_DEPTH; depth++) {
      final Page page = resolveRetained(current);
      if (page instanceof HOTLeafPage leaf) {
        try {
          beforeLeafRead(leaf);
          return leaf.getEntryCount() == 0
              ? null
              : leaf.getFirstKey();
        } finally {
          leaf.releaseGuard();
        }
      }
      if (!(page instanceof HOTIndirectPage indirect) || indirect.getNumChildren() == 0) {
        return null;
      }
      current = indirect.getChildReference(0);
      if (current == null) {
        return null;
      }
    }
    return null;
  }

  /**
   * The last (lex-greatest) key of the subtree at {@code reference}, found by rightmost descent — the
   * true maximum once the subtree is internally clean, which is all the I12 boundary check needs (see
   * the comment there). Returns {@code null} for an empty or unresolvable subtree.
   */
  private byte @Nullable [] lastKeyOfSubtree(final PageReference reference) {
    PageReference current = reference;
    for (int depth = 0; depth <= MAX_DEPTH; depth++) {
      final Page page = resolveRetained(current);
      if (page instanceof HOTLeafPage leaf) {
        try {
          beforeLeafRead(leaf);
          final int entryCount = leaf.getEntryCount();
          return entryCount == 0
              ? null
              : leaf.getKey(entryCount - 1);
        } finally {
          leaf.releaseGuard();
        }
      }
      if (!(page instanceof HOTIndirectPage indirect) || indirect.getNumChildren() == 0) {
        return null;
      }
      current = indirect.getChildReference(indirect.getNumChildren() - 1);
      if (current == null) {
        return null;
      }
    }
    return null;
  }

  private @Nullable Page resolve(final @Nullable PageReference reference) {
    if (reference == null) {
      return null;
    }
    final Page page = resolver.resolve(reference);
    return page != null && !page.isClosed()
        ? page
        : null;
  }

  /**
   * Resolve {@code reference} for a read of leaf keys. A HOT leaf comes back holding one guard, which
   * the caller must release with {@link HOTLeafPage#releaseGuard()} once it has read; while the guard
   * is held an eviction only orphans the leaf and its frame stays intact. Any other page comes back
   * exactly as {@link #resolve} returns it, because an indirect page lives on the heap.
   *
   * @return the page, or {@code null} when the reference is unresolvable or its leaf could not be
   *         retained within {@link #MAX_LEAF_RETAIN_ATTEMPTS} resolutions
   */
  private @Nullable Page resolveRetained(final @Nullable PageReference reference) {
    if (reference == null) {
      return null;
    }
    for (int attempt = 0; attempt < MAX_LEAF_RETAIN_ATTEMPTS; attempt++) {
      final Page page = resolver.resolve(reference);
      if (!(page instanceof HOTLeafPage leaf)) {
        return page != null && !page.isClosed()
            ? page
            : null;
      }
      if (leaf.acquireGuard()) {
        return leaf;
      }
      // Closed or being torn down: evicted since it was resolved. Resolving again reloads it.
    }
    return null;
  }

  private static void beforeLeafRead(final HOTLeafPage leaf) {
    final Consumer<HOTLeafPage> hook = retainedLeafTestHook;
    if (hook != null) {
      hook.accept(leaf);
    }
  }

  /**
   * Test seam: {@code hook} runs on the detecting thread for every leaf the detector has retained,
   * immediately before it reads that leaf's keys. A test uses it to evict the leaf at exactly the
   * point where an unguarded read would lose its frame. {@code null} removes the hook.
   */
  static void setRetainedLeafTestHook(final @Nullable Consumer<HOTLeafPage> hook) {
    retainedLeafTestHook = hook;
  }

  private static String hex(final byte @Nullable [] bytes) {
    return bytes == null
        ? "null"
        : HexFormat.of().formatHex(bytes);
  }
}
