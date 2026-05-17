/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
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

/**
 * Detection half of the HOT detect-and-rebuild structural fix (see {@code
 * docs/HOT_FORMAL_FOUNDATION.md} §8). Walks a HOT secondary-index trie top-down and returns the
 * <em>highest</em> malformed indirect pages — the roots of the maximal drifted subtrees.
 *
 * <p><b>What "malformed" means.</b> An indirect is malformed when it violates a structural
 * invariant in its own block. The predicates mirror {@code HOTInvariantValidator} (the executable
 * specification of the foundation document's §2 invariants); a production detector cannot depend
 * on that test-only class, so the per-indirect predicate logic is reproduced here:
 * <ul>
 *   <li><b>I3</b> — partial-key uniqueness: no two children share a stored partial.</li>
 *   <li><b>I4</b> — first-partial-zero: the smallest stored partial is {@code 0} (Binna's "first
 *       mask always zero" rule under sparse-path encoding).</li>
 *   <li><b>I5</b> — leaf-constancy: for every child {@code i} with a non-zero stored partial
 *       {@code p_i}, every key {@code K} in {@code subtree(child_i)} satisfies
 *       {@code (p_i & ~densePK(K, mask)) == 0}. I5 is the routing-soundness linchpin
 *       (foundation Theorem 2).</li>
 *   <li><b>I7</b> — partial keys strictly ascending (unsigned) across child slots.</li>
 *   <li><b>I8</b> — children ordered by ascending first-key of their subtree (range-scan
 *       correctness — {@code HOTRangeCursor} advances by child index).</li>
 *   <li><b>I11</b> — trie condition: each indirect child's most-significant discriminative bit is
 *       strictly less significant than this node's. I11 is cross-level; a violation on edge
 *       {@code (P, C)} is attributed to the <em>parent</em> {@code P}, because rebuilding
 *       {@code P}'s subtree (which contains both {@code P} and {@code C}) is what fixes it.</li>
 * </ul>
 *
 * <p><b>Highest-malformed semantics.</b> The walk is a pre-order DFS. When an indirect is found
 * malformed, its page is recorded and the walk does <em>not</em> descend into it: rebuilding the
 * highest malformed indirect with {@link HOTBulkBuilder} produces a canonical subtree (foundation
 * Theorem 1) and therefore subsumes every malformed descendant (Binna Lemma 3 — a subtree's
 * canonical form depends only on its own keys). The returned subtrees are pairwise non-nested.
 *
 * <p><b>Purity.</b> The detector reads only; it never mutates a page or performs I/O. Pages are
 * obtained through a caller-supplied {@link PageResolver} so the same detector serves a
 * commit-path writer (TIL-aware resolution) and a unit test (swizzled-page resolution).
 *
 * <p><b>Cost.</b> One DFS per index. The I5 check walks each indirect's subtree keys; summed over
 * a root-to-leaf path a key is visited once per ancestor, and HOT's height is a small constant
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
   * Resolves a {@link PageReference} to its in-memory {@link Page}. Returning {@code null} marks
   * the reference as unresolvable; the detector then skips that branch rather than failing.
   */
  @FunctionalInterface
  public interface PageResolver {
    @Nullable Page resolve(PageReference reference);
  }

  /**
   * A maximal malformed subtree: the indirect page that is the highest violating node of a
   * drifted region. Rebuilding the subtree rooted here with {@link HOTBulkBuilder} repairs it.
   *
   * @param reference        the in-trie reference whose page is the malformed indirect (the
   *                         copy-on-write splice point for a rebuild)
   * @param indirectPageKey  the malformed indirect's page key
   * @param invariant        the first violated invariant tag (diagnostic)
   * @param detail           a human-readable description of the violation (diagnostic)
   */
  public record MalformedSubtree(PageReference reference, long indirectPageKey, String invariant,
                                 String detail) {}

  /** First violated invariant of an indirect's own block — {@code null} encodes "clean". */
  private record Defect(String invariant, String detail) {}

  private final PageResolver resolver;
  private final List<MalformedSubtree> malformed = new ArrayList<>();
  private final Set<Long> visitedIndirectPageKeys = new HashSet<>();

  private HOTMalformedSubtreeDetector(final PageResolver resolver) {
    this.resolver = resolver;
  }

  /**
   * Walk the HOT trie rooted at {@code rootReference} and return its highest malformed indirects.
   *
   * @param rootReference the trie root; {@code null} (an unmaterialized index) yields an empty
   *                      result
   * @param resolver      resolves page references to in-memory pages
   * @return the maximal malformed subtrees in pre-order; empty when the trie is canonical or has
   *         no indirect pages (a single-leaf index cannot have a malformed <em>indirect</em>)
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

  private void walk(final PageReference reference, final HOTIndirectPage indirect,
      final int depth) {
    if (depth > MAX_DEPTH) {
      return;
    }
    final long pageKey = indirect.getPageKey();
    if (pageKey >= 0 && !visitedIndirectPageKeys.add(pageKey)) {
      return; // cycle guard — a well-formed HOT has none, but never loop on a corrupt one
    }
    final Defect defect = firstViolation(indirect);
    if (defect != null) {
      // Highest malformed indirect of this region: record it and do not descend — a rebuild of
      // this subtree subsumes every malformed descendant (foundation §8 / Binna Lemma 3).
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
    final int[] partials = indirect.getPartialKeys();
    final boolean partialsUsable = partials.length >= n;

    // I3 — partial-key uniqueness. n <= 32, so the O(n^2) scan beats a boxed HashSet.
    if (partialsUsable) {
      for (int i = 0; i < n; i++) {
        for (int j = i + 1; j < n; j++) {
          if (partials[i] == partials[j]) {
            return new Defect("I3-partial-key-uniqueness",
                "duplicate partial 0x" + Integer.toHexString(partials[i]) + " at children "
                    + i + " and " + j);
          }
        }
      }
    }

    // I4 — first-partial-zero: the unsigned-smallest stored partial must be 0.
    if (partialsUsable && n > 0) {
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
    }

    // I7 — partial keys strictly ascending (unsigned) across child slots.
    if (partialsUsable) {
      for (int i = 1; i < n; i++) {
        if (Integer.compareUnsigned(partials[i], partials[i - 1]) <= 0) {
          return new Defect("I7-partial-keys-sorted",
              "partials not ascending at " + (i - 1) + "->" + i + ": 0x"
                  + Integer.toHexString(partials[i - 1]) + " vs 0x"
                  + Integer.toHexString(partials[i]));
        }
      }
    }

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
            return new Defect("I11-trie-condition",
                "child[" + i + "]=" + childIndirect.getPageKey() + " MSB=" + childMSB
                    + " must be > parent MSB=" + parentMSB);
          }
        }
      }
    }

    // I8 — children ordered by ascending first-key of their subtree.
    byte[] previousFirstKey = null;
    for (int i = 0; i < n; i++) {
      final PageReference childReference = indirect.getChildReference(i);
      if (childReference == null) {
        continue;
      }
      final byte[] firstKey = firstKeyOfSubtree(childReference);
      if (firstKey == null) {
        continue;
      }
      if (previousFirstKey != null
          && Arrays.compareUnsigned(previousFirstKey, firstKey) >= 0) {
        return new Defect("I8-children-sorted-by-firstkey",
            "child[" + i + "] firstKey " + hex(firstKey) + " <= a preceding child's firstKey "
                + hex(previousFirstKey));
      }
      previousFirstKey = firstKey;
    }

    // I5 — leaf-constancy. The expensive check (a subtree-wide key walk) runs last so a cheaper
    // defect short-circuits it. For each child with a non-zero stored partial, every key in the
    // child's subtree must have a dense partial key that supersets the stored partial.
    if (partialsUsable) {
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
        final byte[] failingKey =
            findI5FailingKey(childReference, indirect, sparsePartial, 0, failingDensePK);
        if (failingKey != null) {
          return new Defect("I5-leaf-constancy",
              "child[" + i + "] stored partial 0x" + Integer.toHexString(sparsePartial)
                  + " is not a subset of dense PEXT 0x" + Integer.toHexString(failingDensePK[0])
                  + " of subtree key " + hex(failingKey));
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
   * {@code maskNode}'s mask fails the sparse-path subset condition for {@code sparsePartial}
   * (i.e. {@code (sparsePartial & ~densePK) != 0}). Returns {@code null} when every key passes;
   * {@code failingDensePK[0]} receives the offending key's dense partial key.
   */
  private byte @Nullable [] findI5FailingKey(final PageReference reference,
      final HOTIndirectPage maskNode, final int sparsePartial, final int depth,
      final int[] failingDensePK) {
    if (depth > MAX_DEPTH) {
      return null;
    }
    final Page page = resolve(reference);
    if (page instanceof HOTLeafPage leaf) {
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
    }
    if (page instanceof HOTIndirectPage indirect) {
      final int n = indirect.getNumChildren();
      for (int i = 0; i < n; i++) {
        final PageReference childReference = indirect.getChildReference(i);
        if (childReference == null) {
          continue;
        }
        final byte[] failing =
            findI5FailingKey(childReference, maskNode, sparsePartial, depth + 1, failingDensePK);
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
      final Page page = resolve(current);
      if (page instanceof HOTLeafPage leaf) {
        return leaf.getEntryCount() == 0 ? null : leaf.getFirstKey();
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

  private @Nullable Page resolve(final @Nullable PageReference reference) {
    if (reference == null) {
      return null;
    }
    final Page page = resolver.resolve(reference);
    return page != null && !page.isClosed() ? page : null;
  }

  private static String hex(final byte @Nullable [] bytes) {
    return bytes == null ? "null" : HexFormat.of().formatHex(bytes);
  }
}
