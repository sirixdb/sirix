/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.api.StorageEngineReader;
import io.sirix.index.IndexType;
import io.sirix.page.CASPage;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.NamePage;
import io.sirix.page.PageReference;
import io.sirix.page.PathPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.interfaces.Page;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Test-only validator that walks an entire HOT trie and asserts the structural invariants from
 * Robert Binna's HOT thesis (§4.2) plus Sirix-specific multi-rev / chunked-bitmap properties.
 *
 * <p>Run as: {@code HOTInvariantValidator.validate(rootRef, reader).assertOk()}.
 *
 * <p>Each invariant is named explicitly so a regression points at a specific structural property.
 * Numbering matches {@code docs/HOT_INVARIANTS_CATALOG.md} (Stage A of the formal-verification
 * effort):
 * <ol>
 *   <li><b>I1 — leaf-key-uniqueness</b>: every key within a leaf is unique; cross-leaf — no
 *       stored key appears in two leaves.</li>
 *   <li><b>I2 — leaf-lex-sorted</b>: leaf entries are stored in ascending lex order.</li>
 *   <li><b>I3 — partial-key-uniqueness</b>: within an indirect node, no two children share a
 *       partial key (would collide PEXT routing).</li>
 *   <li><b>I4 — first-partial-zero</b>: under sparse-path encoding the leftmost child's stored
 *       partial is zero (Binna's "first mask always zero" rule — every BiNode on the leftmost
 *       child's path takes the LEFT side, so all on-path bits are 0).</li>
 *   <li><b>I5 — leaf-constancy</b>: every captured discriminative bit is constant across all
 *       leaf-keys reachable from each child of the indirect that captures it.</li>
 *   <li><b>I6 — pext-routes-to-leaf</b>: for every stored key K, descending from the root via
 *       {@code findChildIndex(K)} must terminate at the leaf that actually contains K.</li>
 *   <li><b>I7 — partial-keys-sorted</b>: under BE encoding, child slots appear in ascending
 *       partial-key order (so that path-stack forward iteration is partial-key-monotonic).</li>
 *   <li><b>I8 — children-sorted-by-firstkey</b>: child slots are also ordered by first-key
 *       (writer enforces {@code sortChildrenByFirstKey}).</li>
 *   <li><b>I9 — height-bounded</b>: trie height ≤ {@code maxHeight} (default 32 — generous
 *       upper bound for ≤ 2³² entries).</li>
 *   <li><b>I10 — fanout-bounded</b>: every indirect has ≥ 2 children and ≤
 *       {@link HOTIndirectPage#MAX_NODE_ENTRIES} children.</li>
 *   <li><b>I11 — trie-condition</b>: disc bits become strictly less significant down the tree,
 *       i.e. for every parent → child indirect edge, every bit in {@code child.mask} has a
 *       larger absolute bit position than {@code parent.MSB} (= no double-use of disc bits on a
 *       path; matches C++ {@code HOTSingleThreaded.hpp:521-523} {@code assert(parent.MSB <
 *       splitBit)}).</li>
 *   <li><b>I-Binna-sparse-path</b>: per Binna §4.2 — stored partial p_i of child c_i must
 *       satisfy {@code (p_i & ~PEXT(c_i.firstKey, mask)) == 0} (= every bit set in stored is
 *       also set in dense PEXT of firstKey). Verified for firstKey only.</li>
 *   <li><b>I-leaf-insert-precondition</b>: a STATIC helper, not a tree-walk. Given a hypothetical
 *       new key being inserted, walks the PEXT-descent path and reports whether the insert would
 *       break I5 at any ancestor (i.e., would a key with disagreeing β value be added to a
 *       previously β-constant subtree). Used by Stage D's post-mutation gate to surface the
 *       leaf-insert I5 gap at the operation that creates it. See {@link
 *       #checkLeafInsertPreservesI5}.</li>
 * </ol>
 *
 * <p>Each violation is collected as a {@link Violation} so the caller can report all problems
 * in one pass instead of failing at the first one.
 */
public final class HOTInvariantValidator {

  /** Reasonable upper bound for HOT height under all realistic key counts. */
  public static final int DEFAULT_MAX_HEIGHT = 32;

  private final StorageEngineReader reader;
  private final int maxHeight;

  /** Collected violations (empty == validation passed). */
  private final List<Violation> violations = new ArrayList<>();

  /** Stored keys discovered during the walk — used for I6 PEXT-routing verification. */
  private final List<byte[]> storedKeys = new ArrayList<>();

  /** Maximum observed trie height. */
  private int observedHeight;

  /** Visited indirect-page keys — protects against cycles (none expected in HOT, but safety). */
  private final Set<Long> visitedIndirectPageKeys = new HashSet<>();

  /** Visited leaf-page keys. */
  private final Set<Long> visitedLeafPageKeys = new HashSet<>();

  private HOTInvariantValidator(StorageEngineReader reader, int maxHeight) {
    this.reader = reader;
    this.maxHeight = maxHeight;
  }

  /**
   * Validate a HOT trie rooted at {@code rootRef}. Returns a result that can be queried
   * for violations or asserted via {@link Result#assertOk()}.
   */
  public static Result validate(PageReference rootRef, StorageEngineReader reader) {
    return validate(rootRef, reader, DEFAULT_MAX_HEIGHT);
  }

  /**
   * Resolve the HOT-trie root for the given index type and number using the same logic as
   * {@link io.sirix.index.hot.AbstractHOTIndexReader#getRootReference()}. Returns {@code null}
   * if the index has not been materialized for the current revision.
   */
  public static @Nullable PageReference resolveRootRef(StorageEngineReader reader,
      IndexType indexType, int indexNumber) {
    final RevisionRootPage rootPage = reader.getActualRevisionRootPage();
    return switch (indexType) {
      case PATH -> {
        final PathPage pathPage = reader.getPathPage(rootPage);
        if (pathPage == null || indexNumber >= pathPage.getReferences().size()) yield null;
        yield pathPage.getOrCreateReference(indexNumber);
      }
      case CAS -> {
        final CASPage casPage = reader.getCASPage(rootPage);
        if (casPage == null || indexNumber >= casPage.getReferences().size()) yield null;
        yield casPage.getOrCreateReference(indexNumber);
      }
      case NAME -> {
        final NamePage namePage = reader.getNamePage(rootPage);
        if (namePage == null || indexNumber >= namePage.getReferences().size()) yield null;
        yield namePage.getOrCreateReference(indexNumber);
      }
      default -> null;
    };
  }

  /** Validate the HOT trie of the given index type / number for the reader's current revision. */
  public static Result validateIndex(StorageEngineReader reader, IndexType indexType,
      int indexNumber) {
    final PageReference rootRef = resolveRootRef(reader, indexType, indexNumber);
    return validate(rootRef, reader);
  }

  public static Result validate(PageReference rootRef, StorageEngineReader reader, int maxHeight) {
    final HOTInvariantValidator v = new HOTInvariantValidator(reader, maxHeight);
    if (rootRef == null) {
      return new Result(List.of(), 0, 0);
    }
    final Page root = v.loadPage(rootRef);
    if (root == null) {
      // Empty trie — vacuously valid.
      return new Result(List.of(), 0, 0);
    }
    v.walk(root, /*depth=*/ 0, /*ancestorIndirects=*/ List.of());
    // I1-cross-leaf: no stored key may appear in two leaves. This is the strongest data-
    // integrity check — distinguishes "stale routing" (one key in one leaf, PEXT goes to a
    // different leaf, but only one canonical home) from "key duplication" (the SAME key body
    // present in two leaves, which would imply data divergence under updates).
    final Set<String> seen = new HashSet<>(v.storedKeys.size() * 2);
    for (final byte[] k : v.storedKeys) {
      final String h = bytesHex(k);
      if (!seen.add(h)) {
        v.addViolation("I1-cross-leaf-uniqueness",
            "stored key " + h + " appears in more than one leaf — structural duplication", null);
      }
    }
    // I6: every stored key must PEXT-route from rootRef back to its containing leaf.
    if (root instanceof HOTIndirectPage) {
      v.verifyPextRouting(rootRef);
    }
    return new Result(v.violations, v.storedKeys.size(), v.observedHeight);
  }

  // ===== walk =====

  private void walk(Page page, int depth, List<HOTIndirectPage> ancestorIndirects) {
    if (depth > maxHeight) {
      addViolation("I9-height-bounded", "depth " + depth + " exceeds maxHeight " + maxHeight, null);
      return;
    }
    if (depth > observedHeight) observedHeight = depth;

    if (page instanceof HOTLeafPage leaf) {
      validateLeaf(leaf, ancestorIndirects);
      return;
    }
    if (!(page instanceof HOTIndirectPage indirect)) {
      addViolation("page-kind", "unexpected page kind " + page.getClass().getSimpleName(), null);
      return;
    }

    // Cycle guard.
    final long pageKey = indirect.getPageKey();
    if (pageKey >= 0 && !visitedIndirectPageKeys.add(pageKey)) {
      addViolation("structure-cycle", "indirect page " + pageKey + " revisited", null);
      return;
    }

    validateIndirect(indirect);

    // Descend.
    final int n = indirect.getNumChildren();
    final List<HOTIndirectPage> nextAncestors = new ArrayList<>(ancestorIndirects.size() + 1);
    nextAncestors.addAll(ancestorIndirects);
    nextAncestors.add(indirect);

    for (int i = 0; i < n; i++) {
      final PageReference childRef = indirect.getChildReference(i);
      if (childRef == null) {
        addViolation("null-child", "indirect[" + indirect.getPageKey() + "].child[" + i + "] = null",
            indirect);
        continue;
      }
      final Page child = loadPage(childRef);
      if (child == null) {
        final Page rawPage = childRef.getPage();
        addViolation("unloadable-child",
            "indirect[" + indirect.getPageKey() + "].child[" + i + "] could not be loaded"
                + " (logKey=" + childRef.getLogKey()
                + " gen=" + childRef.getActiveTilGeneration()
                + " diskKey=" + childRef.getKey()
                + " rawPage=" + (rawPage == null ? "null" : rawPage.getClass().getSimpleName()
                    + (rawPage.isClosed() ? "-CLOSED" : "-OPEN"))
                + ")", indirect);
        continue;
      }
      // I11 — trie-condition: disc bits become strictly less significant down the tree.
      // Equivalent C++ assert: HOTSingleThreaded.hpp:521-523 {@code assert(parent.MSB < splitBit)}.
      // For every bit b set in childIndirect.mask we require {@code b > parent.MSB} (in
      // absolute MSB-first bit position numbering — larger position = less significant).
      if (child instanceof HOTIndirectPage childIndirect) {
        validateI11TrieCondition(indirect, childIndirect, i);
      }
      walk(child, depth + 1, nextAncestors);
    }
  }

  /**
   * I11 — trie-condition. For every bit b set in {@code child}'s mask, b must be a less-
   * significant bit position than {@code parent.MSB} (= b's absolute position is greater
   * than parent.MSB's). Skips when either side has no mask set (e.g., MultiNode root with
   * empty bitMask uses childIndex routing instead — handled below).
   */
  private void validateI11TrieCondition(HOTIndirectPage parent, HOTIndirectPage child,
      int childSlot) {
    final int parentMSB = parent.getMostSignificantBitIndex();
    if (parentMSB < 0) return; // parent's MSB undefined — skip
    final int childMSB = child.getMostSignificantBitIndex();
    if (childMSB < 0) return; // child has no disc bits — vacuously OK (rare)
    // Equivalent absolute bit positions are MSB-first: smaller position = MORE significant.
    // Trie condition: parent's most-significant bit MUST be MORE significant than every
    // bit in child's mask, i.e., parent.MSB < every child bit position.
    if (childMSB <= parentMSB) {
      addViolation("I11-trie-condition",
          "indirect " + parent.getPageKey() + " (MSB=" + parentMSB + ") → child[" + childSlot
              + "]=" + child.getPageKey() + " (MSB=" + childMSB + "): child.MSB must be > parent.MSB"
              + " (down-tree disc bits must become less significant — Binna trie condition)",
          parent);
    }
  }

  // ===== leaf validation =====

  private void validateLeaf(HOTLeafPage leaf, List<HOTIndirectPage> ancestorIndirects) {
    final long pageKey = leaf.getPageKey();
    if (pageKey >= 0) visitedLeafPageKeys.add(pageKey);

    final int entryCount = leaf.getEntryCount();
    if (entryCount == 0) {
      // Empty leaves shouldn't normally appear in a populated trie, but tolerate.
      return;
    }

    byte[] previousKey = null;
    final Set<String> keysInThisLeaf = new HashSet<>();
    for (int i = 0; i < entryCount; i++) {
      final byte[] key = leaf.getKey(i);
      // I1 — leaf-key-uniqueness.
      final String hex = bytesHex(key);
      if (!keysInThisLeaf.add(hex)) {
        addViolation("I1-leaf-key-uniqueness",
            "duplicate key " + hex + " within leaf " + leaf.getPageKey(), null);
      }

      // I2 — leaf-lex-sorted.
      if (previousKey != null) {
        final int cmp = Arrays.compareUnsigned(previousKey, key);
        if (cmp >= 0) {
          addViolation("I2-leaf-lex-sorted",
              "leaf " + leaf.getPageKey() + " entries[" + (i - 1) + ".." + i
                  + "] not strictly increasing: " + bytesHex(previousKey) + " vs " + hex, null);
        }
      }
      previousKey = key;

      final byte[] value = leaf.getValue(i);
      if (NodeReferencesSerializer.isTombstone(value, 0, value.length)) {
        continue;
      }
      storedKeys.add(key);
    }

    // I5 — every captured disc bit (across the entire ancestor chain) must agree on the value
    // exhibited by the leaf's keys with the partial-key path that led here.
    // Verified globally via PEXT routing in {@link #verifyPextRouting}; per-key constancy follows.
  }

  // ===== indirect validation =====

  private void validateIndirect(HOTIndirectPage indirect) {
    final int n = indirect.getNumChildren();
    // I10 — fanout-bounded.
    if (n < 2) {
      addViolation("I10-fanout-bounded",
          "indirect " + indirect.getPageKey() + " has " + n + " children (< 2)", indirect);
    }
    if (n > HOTIndirectPage.MAX_NODE_ENTRIES) {
      addViolation("I10-fanout-bounded",
          "indirect " + indirect.getPageKey() + " has " + n + " children (> "
              + HOTIndirectPage.MAX_NODE_ENTRIES + ")", indirect);
    }

    // I3 — partial-key uniqueness.
    final int[] partials = indirect.getPartialKeys();
    if (partials != null && partials.length >= n) {
      final Set<Integer> seen = new HashSet<>(n * 2);
      for (int i = 0; i < n; i++) {
        if (!seen.add(partials[i])) {
          addViolation("I3-partial-key-uniqueness",
              "indirect " + indirect.getPageKey() + " has duplicate partial key 0x"
                  + Integer.toHexString(partials[i]) + " across children", indirect);
          break;
        }
      }
    }

    // I4 — first-partial-zero (Binna's "first mask always zero" rule).
    //
    // Under sparse-path encoding the leftmost child by partial-key sort takes the LEFT side
    // at every BiNode on its path → all on-path bits are 0 → its stored partial = 0. Binna's
    // C++ reference (HOTSingleThreadedNode.hpp:179, 244, 267, 292, 320) emphasizes:
    // "THIS IS IMPORTANT FOR THE TREE TO HAVE FAST LOOKUP AND MAINTAIN INTEGRITY!! THE FIRST
    // MASK ALWAYS IS ZERO!!"
    //
    // We check the slot whose stored partial is the unsigned-minimum (= leftmost under partial
    // sort) rather than slot 0 specifically — this is more permissive but exercises the same
    // semantic property: the smallest stored partial must be zero.
    if (partials != null && partials.length >= n && n > 0) {
      int minPartial = partials[0];
      for (int i = 1; i < n; i++) {
        if (Integer.compareUnsigned(partials[i], minPartial) < 0) minPartial = partials[i];
      }
      if (minPartial != 0) {
        addViolation("I4-first-partial-zero",
            "indirect " + indirect.getPageKey() + " smallest stored partial is 0x"
                + Integer.toHexString(minPartial) + " (must be 0 under sparse-path encoding —"
                + " Binna's 'first mask always zero' rule)", indirect);
      }
    }

    // I7 — partial keys ascending across child slots (BE invariant: partial-key order ≡ lex on
    // disc bits ≡ child-slot order under sortChildrenByFirstKey when no high-order non-disc
    // bits flip the order).
    if (partials != null && partials.length >= n) {
      for (int i = 1; i < n; i++) {
        if (Integer.compareUnsigned(partials[i], partials[i - 1]) <= 0) {
          addViolation("I7-partial-keys-sorted",
              "indirect " + indirect.getPageKey() + " partial keys not ascending at " + (i - 1)
                  + "→" + i + ": 0x" + Integer.toHexString(partials[i - 1]) + " vs 0x"
                  + Integer.toHexString(partials[i]), indirect);
          break;
        }
      }
    }

    // I8 — children sorted by first-key.
    // I8 is NOT merely cosmetic: HOTRangeCursor does in-order trie traversal with a parent
    // stack (no sibling pointers, for CoW-compatibility) and advances by child INDEX, so a
    // range scan is correct only when child-index order equals key order. In a well-formed
    // HOT trie I7 (partials ascending) and I8 (children by firstKey) are equivalent — both
    // express "child index order == lex order"; they diverge only when an indirect's mask
    // misses a discriminating bit, which is itself the malformation worth catching.
    byte[] previousFirstKey = null;
    for (int i = 0; i < n; i++) {
      final PageReference childRef = indirect.getChildReference(i);
      if (childRef == null) continue;
      final byte[] firstKey = firstKeyOfSubtree(childRef);
      if (firstKey == null) continue;
      if (previousFirstKey != null) {
        final int cmp = Arrays.compareUnsigned(previousFirstKey, firstKey);
        if (cmp >= 0) {
          // Diagnostic dump: mask + per-child (firstKey, stored partial, dense PEXT). When
          // partial order and firstKey order disagree, the indirect's mask is missing the
          // most-significant bit that differs between the inverted pair (MSDB-closure gap).
          final StringBuilder dump = new StringBuilder(256);
          dump.append(" mask=0x").append(Long.toHexString(indirect.getBitMask()))
              .append(" initialBytePos=").append(indirect.getInitialBytePos()).append(" dump=[");
          for (int k = 0; k < n; k++) {
            final byte[] fk = firstKeyOfSubtree(indirect.getChildReference(k));
            final int densePKk = fk == null ? 0 : computeDensePartialKey(indirect, fk);
            dump.append("c").append(k)
                .append("(sparse=0x").append(partials != null && partials.length > k
                    ? Integer.toHexString(partials[k]) : "?")
                .append(",dense=0x").append(Integer.toHexString(densePKk))
                .append(",fk=").append(fk == null ? "null"
                    : bytesHex(fk).substring(0, Math.min(44, bytesHex(fk).length())))
                .append(") ");
          }
          dump.append(']');
          addViolation("I8-children-sorted-by-firstkey",
              "indirect " + indirect.getPageKey() + " child[" + (i - 1) + "].firstKey "
                  + bytesHex(previousFirstKey) + " >= child[" + i + "].firstKey "
                  + bytesHex(firstKey) + dump, indirect);
          break;
        }
      }
      previousFirstKey = firstKey;
    }

    // I-Binna — sparse-path encoding (necessary condition).
    //
    // Per Binna's HOT thesis §4.2 and the reference {@code SparsePartialKeys.hpp}, the stored
    // partial of child {@code c_i} has bit {@code j} (in the partial-key bit space)
    // set IFF the corresponding disc bit's BiNode is on {@code c_i}'s path AND {@code c_i}'s
    // path takes the right (=1) side at that BiNode. Bits OFF the path stay 0.
    //
    // A NECESSARY condition for sparse-path encoding: every bit set in the stored partial
    // {@code p_i} must also be set in the dense PEXT extraction of {@code c_i}'s first key
    // under the indirect's mask. Equivalently, {@code p_i & ~PEXT(firstKey, mask) == 0}.
    // The reverse is NOT required — bits set in dense PEXT but not in {@code p_i} correspond
    // to off-path positions, intentionally elided under sparse-path encoding.
    if (partials != null && partials.length >= n) {
      for (int i = 0; i < n; i++) {
        final byte[] firstKey = firstKeyOfSubtree(indirect.getChildReference(i));
        if (firstKey == null) continue;
        final int densePK = computeDensePartialKey(indirect, firstKey);
        final int sparsePK = partials[i];
        if ((sparsePK & ~densePK) != 0) {
          // Dump the WHOLE indirect's per-child (firstKey, sparse, dense) so we can see what
          // ill-formed structure was produced and which child's path doesn't match its partial.
          final StringBuilder dump = new StringBuilder(256);
          dump.append(" mask-popcount=").append(Long.bitCount(indirect.getBitMask()))
              .append(" initialBytePos=").append(indirect.getInitialBytePos())
              .append(" mask=0x").append(Long.toHexString(indirect.getBitMask())).append(" dump=[");
          for (int k = 0; k < n; k++) {
            final PageReference cref = indirect.getChildReference(k);
            final byte[] fk = firstKeyOfSubtree(cref);
            final int densePKk = fk == null ? 0 : computeDensePartialKey(indirect, fk);
            final int sparsePKk = partials[k];
            final Page cPage = cref == null ? null : loadPage(cref);
            final String pageKind = cPage == null ? "null"
                : cPage instanceof HOTLeafPage cl ? "leaf(" + cl.getPageKey() + ",ec=" + cl.getEntryCount() + ")"
                : cPage instanceof HOTIndirectPage ci ? "ind(" + ci.getPageKey() + ",nc=" + ci.getNumChildren() + ")"
                : cPage.getClass().getSimpleName();
            dump.append("c").append(k)
                .append("(sparse=0x").append(Integer.toHexString(sparsePKk))
                .append(",dense=0x").append(Integer.toHexString(densePKk))
                .append(",pg=").append(pageKind)
                .append(",fk=").append(fk == null ? "null" : bytesHex(fk).substring(0, Math.min(48, bytesHex(fk).length())))
                .append(",lastK=").append(lastKeyOfChildHex(cref))
                .append(") ");
          }
          dump.append("]");
          addViolation("I-Binna-sparse-path",
              "indirect " + indirect.getPageKey() + " child[" + i + "] stored partial 0x"
                  + Integer.toHexString(sparsePK) + " has bits not in dense PEXT 0x"
                  + Integer.toHexString(densePK)
                  + " — sparse-path-encoded partials must satisfy (sparse & ~dense) == 0"
                  + dump,
              indirect);
          break; // one dump per indirect is enough
        }
      }
    }

    // I5-strict — leaf-level β-constancy for every ON-PATH bit (= every bit set in
    // c.stored). For each child c at slot i:
    //   For every key K in c's subtree (= reachable from c via descend):
    //     dense_K = PEXT(K, indirect.mask).
    //     Require c.stored ⊆ dense_K (i.e., (c.stored & ~dense_K) == 0).
    //
    // The existing I-Binna-sparse-path check above is the same predicate but only
    // tested against c.firstKey. I5-strict tests it against EVERY key in c's subtree,
    // catching multi-entry-leaf β-mixing where firstKey is fine but interior keys
    // violate the invariant. This is the missed-cases gap the campaign's fix attempts
    // kept stumbling on.
    if (partials != null && partials.length >= n) {
      for (int i = 0; i < n; i++) {
        final int sparsePK = partials[i];
        if (sparsePK == 0) continue; // no ON-PATH bit to verify
        final PageReference cref = indirect.getChildReference(i);
        if (cref == null) continue;
        final byte[] firstKey = firstKeyOfSubtree(cref);
        if (firstKey == null) continue;
        // Walk the entire subtree, checking each leaf's keys.
        final int[] failingKeyDensePK = {-1};
        final byte[][] failingKey = {null};
        walkLeavesUntilFalse(cref, leaf -> {
          final int ec = leaf.getEntryCount();
          for (int k = 0; k < ec; k++) {
            final byte[] keyK = leaf.getKey(k);
            if (keyK == null || keyK.length == 0) continue;
            final int denseK = computeDensePartialKey(indirect, keyK);
            if ((sparsePK & ~denseK) != 0) {
              failingKey[0] = keyK;
              failingKeyDensePK[0] = denseK;
              return false; // stop walking
            }
          }
          return true; // continue
        });
        if (failingKey[0] != null) {
          addViolation("I5-leaf-constancy",
              "indirect " + indirect.getPageKey() + " child[" + i + "] stored partial 0x"
                  + Integer.toHexString(sparsePK) + " has bits NOT in dense PEXT 0x"
                  + Integer.toHexString(failingKeyDensePK[0]) + " of subtree key "
                  + bytesHex(failingKey[0]) + " — at least one key in c's subtree fails"
                  + " the (sparse & ~dense) == 0 condition",
              indirect);
          // Continue checking remaining children, since each contributes its own data point.
        }
      }
    }
  }

  /**
   * Walk every leaf reachable from {@code ref} and apply {@code visitor}. Stops early
   * if {@code visitor.test(leaf)} returns {@code false}. Used by I5-strict.
   */
  private void walkLeavesUntilFalse(PageReference ref,
      java.util.function.Predicate<HOTLeafPage> visitor) {
    if (ref == null) return;
    final Page page = loadPage(ref);
    if (page instanceof HOTLeafPage leaf) {
      final boolean unusedVerdict = visitor.test(leaf);
      return;
    }
    if (page instanceof HOTIndirectPage indirect) {
      final int n = indirect.getNumChildren();
      for (int i = 0; i < n; i++) {
        final PageReference childRef = indirect.getChildReference(i);
        if (childRef == null) continue;
        // Could short-circuit on visitor returning false, but the predicate-style
        // visitor doesn't expose that to the recursive caller. For now, walk all.
        walkLeavesUntilFalse(childRef, visitor);
      }
    }
  }

  // ===== I6 — PEXT routes every stored key to its real leaf =====

  private void verifyPextRouting(PageReference rootRef) {
    final boolean trace = System.getProperty("hot.debug.i6trace") != null;
    boolean tracedOnce = false;
    for (final byte[] key : storedKeys) {
      final HOTLeafPage routed = pextDescend(rootRef, key);
      if (routed == null) {
        addViolation("I6-pext-routes-to-leaf",
            "PEXT descent for stored key " + bytesHex(key) + " returned null leaf", null);
        continue;
      }
      if (routed.findEntry(key) < 0) {
        if (trace && !tracedOnce) {
          System.out.println(tracePextDescend(rootRef, key));
          // Also locate the leaf that ACTUALLY contains this key, by scanning every stored
          // leaf. This reveals the structural divergence — the descent wanted to go to leaf
          // X based on partial-key routing, but the key was stored in leaf Y by some prior
          // tree restructuring.
          final long actualLeafKey = findActualLeafForKey(rootRef, key);
          System.out.println("  ACTUAL leaf containing key: pageKey=" + actualLeafKey);
          if (actualLeafKey >= 0) {
            System.out.println(tracePathToLeaf(rootRef, actualLeafKey, key));
          }
          tracedOnce = true;
        }
        addViolation("I6-pext-routes-to-leaf",
            "PEXT descent for stored key " + bytesHex(key)
                + " landed in leaf " + routed.getPageKey() + " which does not contain it",
            null);
      }
    }
  }

  private @Nullable HOTLeafPage pextDescend(PageReference startRef, byte[] key) {
    PageReference ref = startRef;
    for (int depth = 0; depth <= maxHeight; depth++) {
      final Page page = loadPage(ref);
      if (page == null) return null;
      if (page instanceof HOTLeafPage leaf) return leaf;
      if (!(page instanceof HOTIndirectPage indirect)) return null;
      final int childIdx = indirect.findChildIndex(key);
      if (childIdx < 0) return null;
      ref = indirect.getChildReference(childIdx);
      if (ref == null) return null;
    }
    return null;
  }

  /**
   * Diagnostic: full-scan every reachable leaf for {@code key} and return its {@code pageKey},
   * or {@code -1} if no leaf contains it. Used by the I6 trace to identify where a misrouted
   * stored key actually lives.
   */
  private long findActualLeafForKey(PageReference startRef, byte[] key) {
    return findActualLeafForKeyRec(startRef, key, 0);
  }

  private long findActualLeafForKeyRec(PageReference ref, byte[] key, int depth) {
    if (depth > maxHeight + 1) return -1;
    final Page page = loadPage(ref);
    if (page instanceof HOTLeafPage leaf) {
      return leaf.findEntry(key) >= 0 ? leaf.getPageKey() : -1;
    }
    if (!(page instanceof HOTIndirectPage indirect)) return -1;
    for (int i = 0; i < indirect.getNumChildren(); i++) {
      final PageReference childRef = indirect.getChildReference(i);
      if (childRef == null) continue;
      final long found = findActualLeafForKeyRec(childRef, key, depth + 1);
      if (found >= 0) return found;
    }
    return -1;
  }

  /**
   * Diagnostic: traces the path from {@code startRef} to leaf with {@code targetLeafKey}, dumping
   * each indirect's mask, dense PEXT of {@code key}, and the stored partial at the child that
   * actually leads to the target leaf. Used to compare PEXT routing vs. actual leaf location for
   * I6 violations.
   */
  private String tracePathToLeaf(PageReference startRef, long targetLeafKey, byte[] key) {
    final StringBuilder sb = new StringBuilder(256);
    sb.append("  [path-to-actual-leaf=").append(targetLeafKey).append("]\n");
    tracePathToLeafRec(startRef, targetLeafKey, key, 0, sb);
    return sb.toString();
  }

  private boolean tracePathToLeafRec(PageReference ref, long targetLeafKey, byte[] key,
      int depth, StringBuilder sb) {
    if (depth > maxHeight + 1) return false;
    final Page page = loadPage(ref);
    if (page instanceof HOTLeafPage leaf) {
      return leaf.getPageKey() == targetLeafKey;
    }
    if (!(page instanceof HOTIndirectPage indirect)) return false;
    for (int i = 0; i < indirect.getNumChildren(); i++) {
      final PageReference childRef = indirect.getChildReference(i);
      if (childRef == null) continue;
      if (tracePathToLeafRec(childRef, targetLeafKey, key, depth + 1, sb)) {
        // Found a path through child[i] — log this level.
        final int densePK = computeDensePartialKey(indirect, key);
        final int[] partials = indirect.getPartialKeys();
        sb.append("    depth=").append(depth).append(" indirect=").append(indirect.getPageKey())
            .append(" densePK=0x").append(Integer.toHexString(densePK))
            .append(" → child[").append(i).append("] partial=0x")
            .append(partials != null && i < partials.length
                ? Integer.toHexString(partials[i]) : "?")
            .append(" (subset-match=")
            .append(partials != null && i < partials.length
                ? ((densePK & partials[i]) == partials[i]) : "?")
            .append(")\n");
        return true;
      }
    }
    return false;
  }

  /**
   * Diagnostic: traces the PEXT-descent path for {@code key} from {@code startRef}. At each
   * indirect level emits {@code pageKey, mask popcount, dense PEXT of key, child index chosen,
   * child's stored partial}. Used to debug I6 violations — call from the I6 check site when a
   * stored key fails to route to its actual leaf.
   */
  String tracePextDescend(PageReference startRef, byte[] key) {
    final StringBuilder sb = new StringBuilder(256);
    sb.append("[pext-trace] key=").append(bytesHex(key)).append(":\n");
    PageReference ref = startRef;
    for (int depth = 0; depth <= maxHeight; depth++) {
      final Page page = loadPage(ref);
      if (page == null) { sb.append("  depth=").append(depth).append(" → null\n"); break; }
      if (page instanceof HOTLeafPage leaf) {
        sb.append("  depth=").append(depth).append(" leaf=").append(leaf.getPageKey())
            .append(" ec=").append(leaf.getEntryCount())
            .append(" contains=").append(leaf.findEntry(key) >= 0).append("\n");
        break;
      }
      if (!(page instanceof HOTIndirectPage indirect)) {
        sb.append("  depth=").append(depth).append(" unknown page type\n"); break;
      }
      final int densePK = computeDensePartialKey(indirect, key);
      final int childIdx = indirect.findChildIndex(key);
      final int[] partials = indirect.getPartialKeys();
      final int nc = indirect.getNumChildren();
      sb.append("  depth=").append(depth).append(" indirect=").append(indirect.getPageKey())
          .append(" mask-popcount=").append(Long.bitCount(indirect.getBitMask()))
          .append(" initialBytePos=").append(indirect.getInitialBytePos())
          .append(" densePK=0x").append(Integer.toHexString(densePK))
          .append(" → childIdx=").append(childIdx).append("/").append(nc);
      if (childIdx >= 0 && partials != null && partials.length > childIdx) {
        sb.append(" childPartial=0x").append(Integer.toHexString(partials[childIdx]));
      }
      sb.append("\n  partials=[");
      for (int k = 0; partials != null && k < Math.min(nc, partials.length); k++) {
        if (k > 0) sb.append(',');
        sb.append("0x").append(Integer.toHexString(partials[k]));
      }
      sb.append("]\n");
      if (childIdx < 0) break;
      ref = indirect.getChildReference(childIdx);
      if (ref == null) break;
    }
    return sb.toString();
  }

  // ===== I-leaf-insert-precondition (Stage C, surfaces Op 1/2's I5 gap) =====

  /**
   * Static check: would inserting {@code newKey} into the trie rooted at {@code rootRef}
   * violate I5 (leaf β-constancy) at any ancestor on the PEXT-descent path?
   *
   * <p>Algorithm: walk PEXT-descent path. At each ancestor A:
   * <ul>
   *   <li>Compute the chosen child slot c via {@code A.findChildIndex(newKey)}.</li>
   *   <li>For every disc bit β captured by A's mask: scan c's subtree for existing keys'
   *       β-bit values. If existing keys agree on a single value v ∈ {0,1} but
   *       {@code newKey.β != v}, report a violation: inserting newKey would change c's
   *       subtree from β-constant to β-mixed.</li>
   *   <li>Mixed existing values do NOT trigger a violation — they indicate either an
   *       already-broken I5 (separately reported by I5-leaf-constancy on full validation)
   *       or sparse-path off-path bits (legal under Binna's encoding).</li>
   * </ul>
   *
   * <p>This surfaces the campaign's recurring root cause from {@code
   * HOT_OPERATIONS_INVARIANTS_MATRIX.md} §5.4: Op 1 (leaf.put) inserts a key with
   * disagreeing β value into a previously β-constant subtree, silently breaking I5.
   * Routing soundness alone doesn't prevent it because subset-match routing
   * {@code (dense & stored) == stored} only enforces a one-sided constraint.
   *
   * <p><b>Performance.</b> One full subtree scan per ancestor disc bit. Acceptable for
   * Stage D's gated post-mutation check on the 50K reproducer; not appropriate for
   * production paths.
   *
   * @return list of violations (empty if insert preserves I5 along the descent path)
   */
  public static List<Violation> checkLeafInsertPreservesI5(PageReference rootRef, byte[] newKey,
      StorageEngineReader reader) {
    if (rootRef == null || newKey == null || reader == null) return List.of();
    final HOTInvariantValidator v = new HOTInvariantValidator(reader, DEFAULT_MAX_HEIGHT);
    PageReference ref = rootRef;
    for (int depth = 0; depth <= v.maxHeight; depth++) {
      final Page page = v.loadPage(ref);
      if (page == null) break;
      if (page instanceof HOTLeafPage) break;
      if (!(page instanceof HOTIndirectPage indirect)) break;
      final int chosenIdx = indirect.findChildIndex(newKey);
      if (chosenIdx < 0) break;
      final PageReference chosenRef = indirect.getChildReference(chosenIdx);
      if (chosenRef == null) break;
      v.checkPreconditionAtIndirect(indirect, chosenIdx, chosenRef, newKey);
      ref = chosenRef;
    }
    return List.copyOf(v.violations);
  }

  /**
   * Per-ancestor logic for {@link #checkLeafInsertPreservesI5}. Iterates each absolute
   * disc bit position β captured by {@code indirect}'s mask (handling both SingleMask and
   * MultiMask layouts), runs {@link #scanSubtreeBetaValue} over the chosen child's subtree,
   * and emits a violation when subtree is β-constant at value v but {@code newKey.β != v}.
   */
  private void checkPreconditionAtIndirect(HOTIndirectPage indirect, int chosenIdx,
      PageReference chosenRef, byte[] newKey) {
    final long mask = indirect.getBitMask();
    if (indirect.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK && mask != 0L) {
      final int initialBytePos = indirect.getInitialBytePos();
      long m = mask;
      while (m != 0L) {
        final long lowBit = m & -m;
        final int bitInWord = Long.numberOfTrailingZeros(lowBit);
        // BE: long-bit b → byteOffset = 7 - b/8, bitInByte = 7 - b%8.
        final int byteOffset = 7 - bitInWord / 8;
        final int bitInByte = 7 - (bitInWord % 8);
        final int absBit = (initialBytePos + byteOffset) * 8 + bitInByte;
        evaluateBetaPrecondition(indirect, chosenIdx, chosenRef, newKey, absBit);
        m &= m - 1L;
      }
      return;
    }
    // MultiMask: iterate the captured bytes via extractionPositions + extractionMasks.
    final byte[] extractionPositions = indirect.getExtractionPositions();
    final long[] extractionMasks = indirect.getExtractionMasks();
    final int numExtractionBytes = indirect.getNumExtractionBytes();
    if (extractionPositions == null || extractionMasks == null) return;
    for (int i = 0; i < numExtractionBytes; i++) {
      final int keyBytePos = extractionPositions[i] & 0xFF;
      final int chunkIdx = i / 8;
      final int byteOffsetInChunk = i % 8;
      // The 8 bits of this byte sit at long-bits ((7-byteOffsetInChunk)*8) .. +7 in extractionMasks[chunkIdx].
      final long byteMask = (extractionMasks[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFFL;
      if (byteMask == 0) continue;
      for (int b = 0; b < 8; b++) {
        // MSB-first within the byte: bit-in-byte b corresponds to byteMask bit (7-b).
        if ((byteMask & (1L << (7 - b))) == 0) continue;
        final int absBit = keyBytePos * 8 + b;
        evaluateBetaPrecondition(indirect, chosenIdx, chosenRef, newKey, absBit);
      }
    }
  }

  /** Common per-β logic shared by SingleMask and MultiMask layouts. */
  private void evaluateBetaPrecondition(HOTIndirectPage indirect, int chosenIdx,
      PageReference chosenRef, byte[] newKey, int absBit) {
    final int newKeyBetaValue = isBitSetAbsolute(newKey, absBit) ? 1 : 0;
    final int subtreeBetaState = scanSubtreeBetaValue(chosenRef, absBit);
    if (subtreeBetaState == 0 && newKeyBetaValue != 0) {
      addViolation("I-leaf-insert-precondition",
          "indirect " + indirect.getPageKey() + " child[" + chosenIdx + "]'s subtree is β=0-constant"
              + " at absBit=" + absBit + " but newKey has β=1 — insert would turn subtree β-mixed",
          indirect);
    } else if (subtreeBetaState == 1 && newKeyBetaValue != 1) {
      addViolation("I-leaf-insert-precondition",
          "indirect " + indirect.getPageKey() + " child[" + chosenIdx + "]'s subtree is β=1-constant"
              + " at absBit=" + absBit + " but newKey has β=0 — insert would turn subtree β-mixed",
          indirect);
    }
    // subtreeBetaState == 2 (mixed) or 3 (empty) → don't flag (already-broken or vacuous).
  }

  /**
   * Scan every leaf reachable from {@code ref}, evaluate each key's bit at absolute
   * MSB-first position {@code absBit}, return:
   * <ul>
   *   <li>{@code 0} — every key has β=0 (β-constant at value 0)</li>
   *   <li>{@code 1} — every key has β=1 (β-constant at value 1)</li>
   *   <li>{@code 2} — at least one key has β=0 AND at least one has β=1 (mixed)</li>
   *   <li>{@code 3} — no keys observed (empty subtree or unloadable)</li>
   * </ul>
   */
  private int scanSubtreeBetaValue(PageReference ref, int absBit) {
    final boolean[] seen0 = {false};
    final boolean[] seen1 = {false};
    walkLeavesUntilFalse(ref, leaf -> {
      final int ec = leaf.getEntryCount();
      for (int k = 0; k < ec; k++) {
        final byte[] key = leaf.getKey(k);
        if (key == null || key.length == 0) continue;
        if (isBitSetAbsolute(key, absBit)) seen1[0] = true;
        else seen0[0] = true;
        if (seen0[0] && seen1[0]) return false; // early exit on mixed
      }
      return true;
    });
    if (seen0[0] && seen1[0]) return 2;
    if (seen0[0]) return 0;
    if (seen1[0]) return 1;
    return 3;
  }

  /**
   * MSB-first absolute bit lookup: bit at absolute position {@code absBit} = byte
   * {@code absBit/8}, bit-within-byte {@code 7-(absBit%8)} (so {@code absBit==0} = MSB
   * of {@code key[0]}). Returns {@code false} for out-of-range positions (= bit is
   * implicit zero past the key length).
   */
  private static boolean isBitSetAbsolute(byte[] key, int absBit) {
    final int bytePos = absBit / 8;
    if (bytePos >= key.length) return false;
    final int bitInByte = absBit % 8;
    return (key[bytePos] & (1 << (7 - bitInByte))) != 0;
  }

  // ===== helpers =====

  private @Nullable Page loadPage(PageReference ref) {
    // Always prefer TIL-aware resolution via reader.loadHOTPage — this ensures we see
    // the latest version of pages updated mid-transaction (e.g., after leaf splits,
    // intermediate indirect pages in TIL supersede stale ref.getPage() values).
    final Page tilPage = reader.loadHOTPage(ref);
    if (tilPage != null && !tilPage.isClosed()) {
      return tilPage;
    }
    final Page inMemory = ref.getPage();
    if (inMemory != null && !inMemory.isClosed()) {
      return inMemory;
    }
    return null;
  }

  /**
   * Compute the dense PEXT extraction of {@code key} under {@code indirect}'s mask. Mirrors
   * {@link io.sirix.page.HOTIndirectPage}'s SingleMask / MultiMask partial-key extraction so
   * the validator can compare stored sparse-path partials against the would-be dense PEXT.
   */
  private static int computeDensePartialKey(HOTIndirectPage indirect, byte[] key) {
    if (indirect.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK) {
      final int initialBytePos = indirect.getInitialBytePos();
      final long bitMask = indirect.getBitMask();
      long keyWord = 0;
      for (int i = 0; i < 8 && (initialBytePos + i) < key.length; i++) {
        keyWord |= ((long) (key[initialBytePos + i] & 0xFF)) << ((7 - i) * 8);
      }
      return (int) Long.compress(keyWord, bitMask);
    }
    // MultiMask: BE chunk packing matching computePartialKeyMultiMask in HOTIndirectPage.
    final byte[] extractionPositions = indirect.getExtractionPositions();
    final long[] extractionMasks = indirect.getExtractionMasks();
    final int numExtractionBytes = indirect.getNumExtractionBytes();
    if (extractionPositions == null || extractionMasks == null) return 0;
    final int numChunks = extractionMasks.length;
    final long[] gathered = new long[numChunks];
    for (int i = 0; i < numExtractionBytes; i++) {
      final int keyBytePos = extractionPositions[i] & 0xFF;
      final int keyByte = keyBytePos < key.length ? (key[keyBytePos] & 0xFF) : 0;
      final int chunkIdx = i / 8;
      final int byteOffsetInChunk = i % 8;
      gathered[chunkIdx] |= ((long) keyByte) << ((7 - byteOffsetInChunk) * 8);
    }
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

  /** Cheap "last key in subtree" — walks rightmost path. Bounded by maxHeight. Returns hex
   * (truncated to 48 chars) for diagnostic dumps. */
  private String lastKeyOfChildHex(PageReference ref) {
    PageReference cur = ref;
    for (int depth = 0; depth <= maxHeight; depth++) {
      final Page page = loadPage(cur);
      if (page == null) return "null";
      if (page instanceof HOTLeafPage leaf) {
        if (leaf.getEntryCount() == 0) return "empty";
        final String hex = bytesHex(leaf.getKey(leaf.getEntryCount() - 1));
        return hex.substring(0, Math.min(48, hex.length()));
      }
      if (!(page instanceof HOTIndirectPage indirect)) return "?";
      if (indirect.getNumChildren() == 0) return "?";
      cur = indirect.getChildReference(indirect.getNumChildren() - 1);
      if (cur == null) return "null";
    }
    return "?";
  }

  /** Cheap "first key in subtree" — walks leftmost path. Bounded by maxHeight. */
  private byte @Nullable [] firstKeyOfSubtree(PageReference ref) {
    PageReference cur = ref;
    for (int depth = 0; depth <= maxHeight; depth++) {
      final Page page = loadPage(cur);
      if (page == null) return null;
      if (page instanceof HOTLeafPage leaf) {
        if (leaf.getEntryCount() == 0) return null;
        return leaf.getFirstKey();
      }
      if (!(page instanceof HOTIndirectPage indirect)) return null;
      if (indirect.getNumChildren() == 0) return null;
      cur = indirect.getChildReference(0);
      if (cur == null) return null;
    }
    return null;
  }

  private void addViolation(String invariant, String message, @Nullable HOTIndirectPage parent) {
    violations.add(new Violation(invariant, message,
        parent == null ? -1L : parent.getPageKey()));
  }

  private static String bytesHex(byte[] b) {
    if (b == null) return "null";
    return java.util.HexFormat.of().formatHex(b);
  }



  // ===== result types =====

  /** A single invariant violation. */
  public record Violation(String invariant, String message, long parentPageKey) {
    @Override
    public String toString() {
      return "[" + invariant + "] " + message
          + (parentPageKey >= 0 ? " (parent=" + parentPageKey + ")" : "");
    }
  }

  /** Invariant tags that are <em>structurally</em> consequential for direct PEXT descent but were
   * historically worked around by chunked-bitmap range-scan readers.
   *
   * <p><b>UPDATED 2026-05-12:</b> Now EMPTY. Previously contained I3, I6, I7 — those were "soft"
   * because of the HOT augment-bailout limitation in `createNodeFromChildren`. After the
   * structural-Binna campaign (Phases 5–7q + Path 5 routing-collision check, default-ON via
   * `hot.strict.{g32, phase7q, g32.deep, g32.multibeta, g32.childmsb, path5.routeverify} = true`),
   * the augment-bailout no longer surfaces these violations on conforming workloads. They are
   * promoted to HARD violations — any occurrence is a real correctness gap.</p>
   *
   * <p>Kept as a tag list (rather than deleted) so {@link #assertNoHardViolations()} and
   * {@link #hardViolations()} remain API-stable; callers that want the old lenient behavior can
   * pass an explicit override set if they need to characterize a pre-fix workload.</p>
   */
  public static final Set<String> STRUCTURAL_LIMITATION_INVARIANTS = Set.of();

  /** Aggregate result of one validation run. */
  public static final class Result {
    private final List<Violation> violations;
    private final int storedKeyCount;
    private final int observedHeight;

    Result(List<Violation> violations, int storedKeyCount, int observedHeight) {
      this.violations = List.copyOf(violations);
      this.storedKeyCount = storedKeyCount;
      this.observedHeight = observedHeight;
    }

    public List<Violation> violations() {
      return violations;
    }

    public int storedKeyCount() {
      return storedKeyCount;
    }

    public int observedHeight() {
      return observedHeight;
    }

    public boolean isOk() {
      return violations.isEmpty();
    }

    /** Violations that are NOT in the structural-limitation set — i.e., must hold for any
     * HOT correctness claim. */
    public List<Violation> hardViolations() {
      return violations.stream()
          .filter(v -> !STRUCTURAL_LIMITATION_INVARIANTS.contains(v.invariant()))
          .toList();
    }

    /** Violations that ARE in the structural-limitation set — surfaces them as warnings. */
    public List<Violation> structuralLimitationViolations() {
      return violations.stream()
          .filter(v -> STRUCTURAL_LIMITATION_INVARIANTS.contains(v.invariant()))
          .toList();
    }

    /**
     * Assert no violations were found. Throws {@link AssertionError} listing every violation if
     * any were collected.
     */
    public void assertOk() {
      if (violations.isEmpty()) return;
      final StringBuilder sb = new StringBuilder("HOT invariant violations (")
          .append(violations.size()).append("):\n");
      for (final Violation v : violations) {
        sb.append("  ").append(v).append('\n');
      }
      throw new AssertionError(sb.toString());
    }

    /**
     * Assert no <em>hard</em> violations were found — invariants in
     * {@link #STRUCTURAL_LIMITATION_INVARIANTS} are merely reported via the returned summary.
     * Use for adversarial workloads where the documented HOT-augment-bailout limitation may
     * surface I3 / I6 / I7 violations that don't affect production reads.
     */
    public String assertNoHardViolations() {
      final List<Violation> hard = hardViolations();
      final List<Violation> soft = structuralLimitationViolations();
      if (!hard.isEmpty()) {
        final StringBuilder sb = new StringBuilder("HOT HARD invariant violations (")
            .append(hard.size()).append("):\n");
        for (final Violation v : hard) sb.append("  ").append(v).append('\n');
        if (!soft.isEmpty()) {
          sb.append("Plus ").append(soft.size())
              .append(" structural-limitation violations (I3/I6/I7) — see HOT augment bailout.\n");
        }
        throw new AssertionError(sb.toString());
      }
      if (soft.isEmpty()) return "ok (no violations)";
      return "ok (" + soft.size() + " structural-limitation warnings — see HOT augment bailout)";
    }
  }
}
