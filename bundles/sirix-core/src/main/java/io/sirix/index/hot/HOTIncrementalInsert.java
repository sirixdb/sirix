/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.index.IndexType;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Faithful port of Binna's incremental HOT insert (descent, split, integrate, cascade), adapted
 * for SirixDB's persistent page structure and multi-value leaf pages. See
 * {@code docs/HOT_INCREMENTAL_PORT_PLAN.md} for the design and {@code
 * docs/HOT_INCREMENTAL_SPLIT_VERIFICATION.md} for the correctness proofs.
 *
 * <p>This class provides the {@link BiNode} type, the leaf-page and indirect-node split
 * primitives ({@link #splitLeafPage}, {@link #splitIndirect}, {@link #addEntry}), the descent
 * analysis ({@link #analyzeDescent}), and the spine integration with the capacity cascade
 * ({@link #integrate}). The orchestration that drives them on the live insert path is
 * {@code AbstractHOTIndexWriter.doIndex}.
 *
 * @author Johannes Lichtenberger
 */
public final class HOTIncrementalInsert {

  private HOTIncrementalInsert() {
    throw new AssertionError("utility class — static primitives only");
  }

  /** Diagnostic counter — {@link #addEntry} straddle-guard rejections (process-wide). Lets a
   *  verification test confirm the fix path is genuinely exercised. */
  private static final java.util.concurrent.atomic.AtomicLong STRADDLE_GUARD_REJECTIONS =
      new java.util.concurrent.atomic.AtomicLong();

  /** Number of {@link #addEntry} straddle-guard rejections since the last reset. */
  public static long straddleGuardRejections() {
    return STRADDLE_GUARD_REJECTIONS.get();
  }

  /** Reset the straddle-guard rejection counter. */
  public static void resetStraddleGuardRejections() {
    STRADDLE_GUARD_REJECTIONS.set(0L);
  }

  /**
   * A <b>virtual</b> node — the unit of a split result in flight. It mirrors Binna's ephemeral
   * {@code BiNode} (the return value of {@code split}, immediately consumed by
   * {@code integrateBiNodeIntoTree}). A {@code BiNode} is <em>never</em> a page: it has no page
   * key, no transaction-log entry, and no serialized form. When a {@code BiNode} must stand
   * alone it is materialized as a 2-entry {@link HOTIndirectPage}; physically the trie is only
   * compound nodes and leaf pages.
   *
   * @param discriminativeBitIndex the absolute, MSB-first bit position this BiNode splits on
   * @param height                 {@code 1 + max(height(left), height(right))}; a leaf page has
   *                                height 0
   * @param left                   the side whose keys have {@code discriminativeBitIndex == 0}
   * @param right                  the side whose keys have {@code discriminativeBitIndex == 1}
   */
  public record BiNode(int discriminativeBitIndex, int height, PageReference left,
                       PageReference right) {}

  /**
   * Split an overflowing leaf page into a {@link BiNode} of two pages, incorporating the entry
   * {@code (newKey, newValue)} that did not fit.
   *
   * <p>The split bit is the <em>key-set MSDB</em> — the most significant bit on which the leaf's
   * keys differ — computed as {@code msdb(minKey, maxKey)} of the sorted union (the union's
   * extremes bracket every key, so they witness the set's MSDB). Because the union is sorted and
   * the split bit is its MSDB, the partition is a clean prefix (bit 0) / suffix (bit 1) cut, so
   * both halves are complete {@code R(S)}-subtrees (Fact R1 — {@code HOT_FORMAL_FOUNDATION.md})
   * and the produced {@code BiNode} satisfies the trie condition when integrated under the
   * leaf's parent.
   *
   * <p>The caller has already decided (the merge-vs-branch test) that {@code newKey} belongs in
   * this leaf's {@code R(S)}-subtree; if it duplicates an existing key the values are OR-merged
   * ({@link #mergeIndexValues}). Tombstones are entries and are carried through unchanged.
   *
   * <p><b>Purity.</b> Allocates only new pages; never mutates or closes {@code source}. The
   * caller owns {@code source}'s lifecycle (it becomes an orphaned page after the splice).
   *
   * @param source           the overflowing leaf page (≥ 2 entries; the caller checks
   *                         {@link HOTLeafPage#canSplit()})
   * @param newKey           the key that did not fit
   * @param newValue         the value that did not fit
   * @param revision         the revision stamped onto every created page
   * @param indexType        the index type
   * @param pageKeyAllocator supplier of fresh persistent page keys
   * @return the split result — a {@code BiNode} over the two halves
   */
  public static BiNode splitLeafPage(final HOTLeafPage source, final byte[] newKey,
      final byte[] newValue, final int revision, final IndexType indexType,
      final LongSupplier pageKeyAllocator) {
    Objects.requireNonNull(source, "source");
    Objects.requireNonNull(newKey, "newKey");
    Objects.requireNonNull(newValue, "newValue");
    Objects.requireNonNull(indexType, "indexType");
    Objects.requireNonNull(pageKeyAllocator, "pageKeyAllocator");

    // The sorted, distinct union of the leaf's entries with the new one spliced in at its
    // lexicographic position (its value OR-merged into an existing key, if present).
    final int sourceCount = source.getEntryCount();
    final List<HOTBulkBuilder.Entry> union = new ArrayList<>(sourceCount + 1);
    boolean placed = false;
    for (int i = 0; i < sourceCount; i++) {
      final byte[] key = source.getKey(i);
      final int cmp = Arrays.compareUnsigned(key, newKey);
      if (cmp == 0) {
        union.add(new HOTBulkBuilder.Entry(key, mergeIndexValues(source.getValue(i), newValue)));
        placed = true;
      } else {
        if (!placed && cmp > 0) {
          union.add(new HOTBulkBuilder.Entry(newKey, newValue));
          placed = true;
        }
        union.add(new HOTBulkBuilder.Entry(key, source.getValue(i)));
      }
    }
    if (!placed) {
      union.add(new HOTBulkBuilder.Entry(newKey, newValue));
    }
    final int n = union.size();
    if (n < 2) {
      throw new IllegalArgumentException("a leaf split needs at least two entries, got " + n);
    }

    // Split bit = the union's key-set MSDB. The sorted union's extremes witness it.
    final int splitBit = HOTBulkBuilder.msdb(union.get(0).key(), union.get(n - 1).key());

    // A sorted union split on its own MSDB is a clean prefix(bit 0)/suffix(bit 1) cut: m is the
    // first entry with the split bit set. m ∈ [1, n-1] — union[0] is the min (bit 0), union[n-1]
    // the max (bit 1).
    int m = 0;
    while (m < n && !HOTBulkBuilder.bitAt(union.get(m).key(), splitBit)) {
      m++;
    }

    final Page left = buildHalf(union.subList(0, m), revision, indexType, pageKeyAllocator);
    final Page right = buildHalf(union.subList(m, n), revision, indexType, pageKeyAllocator);
    final int height = 1 + Math.max(heightOf(left), heightOf(right));
    return new BiNode(splitBit, height, swizzle(left), swizzle(right));
  }

  /**
   * Build one half of a split. The common case — a half that fits a single leaf page — is built
   * directly, with no {@code R(S)} machinery; a half too large for one page (entry count or, on
   * a value-heavy index, byte capacity) falls back to {@link HOTBulkBuilder} for a canonical
   * multi-page subtree.
   */
  private static Page buildHalf(final List<HOTBulkBuilder.Entry> entries, final int revision,
      final IndexType indexType, final LongSupplier pageKeyAllocator) {
    if (entries.size() <= HOTLeafPage.MAX_ENTRIES) {
      final HOTLeafPage leaf =
          new HOTLeafPage(pageKeyAllocator.getAsLong(), revision, indexType);
      boolean fits = true;
      for (final HOTBulkBuilder.Entry entry : entries) {
        if (!leaf.put(entry.key(), entry.value())) {
          fits = false; // byte-capacity overflow
          break;
        }
      }
      if (fits) {
        return leaf;
      }
      leaf.close(); // discard the speculative page and fall back to a multi-page build
    }
    return HOTBulkBuilder.build(entries, revision, indexType, pageKeyAllocator).rootPage();
  }

  /**
   * Split an overflowing indirect compound node into a {@link BiNode} of two fresh compound
   * nodes — Binna's {@code split} ({@code HOTSingleThreadedNode.hpp:549-565}).
   *
   * <p>The node is partitioned at its <em>own</em> most significant discriminative bit
   * ({@code node.MSB} — the root BiNode of the block's flattened binary trie). Children whose
   * partial has {@code node.MSB} clear form the left half, the rest the right half. Because the
   * children are in ascending partial-key order (I7) and {@code node.MSB} carries the highest
   * partial weight, the partition is a clean prefix / suffix cut.
   *
   * <p>Each half is {@code compressEntries}'d ({@link #compressHalf}) into a fresh node: every
   * discriminative bit that is constant across the half — {@code node.MSB} itself, plus any bit
   * that only branched in the sibling half — is dropped (a stale bit kept across split
   * generations would inflate the 32-bit partial key), the surviving bits are re-packed
   * MSB-first, and the layout (SingleMask / MultiMask) is re-chosen for the half's narrower bit
   * span. The trie condition (I11) holds for the result — {@code B.discriminativeBitIndex ==
   * node.MSB} and every disc bit of either half is strictly less significant (every child of
   * {@code node} is an {@code R(S)}-subtree, constant on {@code node}'s disc bits). See
   * {@code docs/HOT_INCREMENTAL_SPLIT_VERIFICATION.md} Theorem II(b).
   *
   * <p><b>Purity.</b> Allocates only new compound pages; never mutates {@code node} or any
   * child. The halves share {@code node}'s (unchanged) child subtrees by reference — the
   * copy-on-write discipline: a split rewrites the spine, not the subtrees.
   *
   * @param node             the overflowing compound node (≥ 2 children, ≥ 1 disc bit)
   * @param revision         the revision stamped onto every created page
   * @param pageKeyAllocator supplier of fresh persistent page keys
   * @return the split result — a {@code BiNode} on {@code node.MSB}
   */
  public static BiNode splitIndirect(final HOTIndirectPage node, final int revision,
      final LongSupplier pageKeyAllocator) {
    Objects.requireNonNull(node, "node");
    Objects.requireNonNull(pageKeyAllocator, "pageKeyAllocator");
    final int[] discBits = discriminativeBits(node);
    final int m = discBits.length;
    final int n = node.getNumChildren();
    if (m == 0 || n < 2) {
      throw new IllegalArgumentException(
          "an indirect split needs >= 2 children and >= 1 disc bit, got n=" + n + " m=" + m);
    }
    final int[] partials = node.getPartialKeys();
    final PageReference[] children = childReferences(node);

    // node.MSB = discBits[0] carries the highest partial weight; partials are ascending (I7),
    // so the first child with node.MSB set begins the right half — a clean prefix/suffix cut.
    final int topWeight = 1 << (m - 1);
    int s = 0;
    while (s < n && (partials[s] & topWeight) == 0) {
      s++;
    }
    if (s == 0 || s == n) {
      throw new IllegalArgumentException(
          "node.MSB does not discriminate the children — not a valid HOT node");
    }

    final PageReference left = compressHalf(Arrays.copyOfRange(children, 0, s),
        Arrays.copyOfRange(partials, 0, s), discBits, revision, pageKeyAllocator);
    final PageReference right = compressHalf(Arrays.copyOfRange(children, s, n),
        Arrays.copyOfRange(partials, s, n), discBits, revision, pageKeyAllocator);
    final int height = 1 + Math.max(heightOf(left.getPage()), heightOf(right.getPage()));
    return new BiNode(discBits[0], height, left, right);
  }

  /**
   * {@code compressEntries} for one half of an indirect split: drop every discriminative bit
   * that is constant across {@code halfChildren} (it no longer branches), re-pack the surviving
   * bits into MSB-first partial keys, and assemble a fresh compound node. A half of a single
   * child is the bare child reference — Binna's 1:31 caveat ({@code
   * HOTSingleThreaded.hpp:524-528}): a lone entry is pulled up, never wrapped.
   *
   * @param halfChildren the half's child references, in ascending partial-key order
   * @param halfPartials the half's stored partials (parallel to {@code halfChildren}), encoded
   *                     against the parent's full {@code discBits}
   * @param discBits     the parent node's discriminative bits, ascending absolute positions
   * @return the assembled half (a fresh swizzled compound node, or the lone child reference)
   */
  private static PageReference compressHalf(final PageReference[] halfChildren,
      final int[] halfPartials, final int[] discBits, final int revision,
      final LongSupplier pageKeyAllocator) {
    final int n = halfChildren.length;
    if (n == 1) {
      return halfChildren[0]; // 1:31 — a lone child is pulled up bare, no compound wrapper.
    }
    final int m = discBits.length;
    // A disc bit is live in this half iff its partial bit is not constant across the children.
    // node.MSB is constant by construction (0 in the left half, 1 in the right); a bit that
    // only branched in the sibling half is constant 0 here. Both are dropped.
    final boolean[] live = new boolean[m];
    int liveCount = 0;
    for (int k = 0; k < m; k++) {
      final int weight = 1 << (m - 1 - k);
      final int first = halfPartials[0] & weight;
      boolean varies = false;
      for (int i = 1; i < n; i++) {
        if ((halfPartials[i] & weight) != first) {
          varies = true;
          break;
        }
      }
      live[k] = varies;
      if (varies) {
        liveCount++;
      }
    }
    final int[] liveDiscBits = new int[liveCount];
    final int[] liveOldColumns = new int[liveCount];
    int li = 0;
    for (int k = 0; k < m; k++) {
      if (live[k]) {
        liveDiscBits[li] = discBits[k];
        liveOldColumns[li] = k;
        li++;
      }
    }
    // Re-pack: live column k (old index liveOldColumns[k]) carries new weight 1 << (liveCount-1-k).
    final int[] newPartials = new int[n];
    for (int i = 0; i < n; i++) {
      int p = 0;
      for (int k = 0; k < liveCount; k++) {
        if ((halfPartials[i] & (1 << (m - 1 - liveOldColumns[k]))) != 0) {
          p |= 1 << (liveCount - 1 - k);
        }
      }
      newPartials[i] = p;
    }
    int maxChildHeight = 0;
    for (final PageReference child : halfChildren) {
      maxChildHeight = Math.max(maxChildHeight, heightOf(child.getPage()));
    }
    return swizzle(HOTBulkBuilder.assembleIndirect(liveDiscBits, newPartials, halfChildren,
        maxChildHeight + 1, revision, pageKeyAllocator));
  }

  /**
   * Split a <em>full</em> compound node while inserting one new entry — Binna's {@code split}
   * ({@code HOTSingleThreadedNode.hpp:549}), the full-node case of {@code insertNewValue}. The
   * node is partitioned at its own most significant discriminative bit ({@code node.MSB}) into
   * two compound halves; the new entry — described by {@code info} computed on the <em>original</em>
   * node — folds into whichever half owns its affected subtree, and the result is a {@link BiNode}
   * on {@code node.MSB} the caller integrates where the node sat.
   *
   * <p>The affected half is built by {@link #compressRangeWithEntry} (Binna's
   * {@code compressEntriesAndAddOneEntryIntoNewNode} — a fused range-compress + entry-insert that
   * works in the original node's coordinate space, never a re-derivation on an already-compressed
   * half); the other half by {@link #compressHalf} (a plain range-compress).
   *
   * <p><b>Precondition.</b> {@code beta} is a genuinely new discriminative bit of {@code node}
   * (not one of its existing ones) — only then is the affected subtree one-sided on {@code beta}
   * (it is the subtree the key branches off, constant on {@code beta} because {@code beta} is
   * their most significant differing bit). {@code info.affectedCount() < node.getNumChildren()} —
   * the affected subtree is not the whole node (that is the pull-up case). {@code node} must have
   * &ge; 2 children and &ge; 1 discriminative bit.
   *
   * <p><b>Purity.</b> Allocates only new pages; never mutates {@code node} or any child.
   *
   * @param node             the full compound node to split
   * @param info             the insert information computed on {@code node} ({@link #getInsertInformation})
   * @param beta             the new entry's discriminative bit (absolute, MSB-first)
   * @param betaValue        the new key's value at {@code beta} (0 or 1)
   * @param newChildRef      the new child (the branch key's single-entry leaf)
   * @param revision         the revision stamped onto every created page
   * @param pageKeyAllocator supplier of fresh persistent page keys
   * @return the split result — a {@code BiNode} on {@code node.MSB}
   */
  public static BiNode splitIndirectWithEntry(final HOTIndirectPage node, final InsertInfo info,
      final int beta, final int betaValue, final PageReference newChildRef, final int revision,
      final LongSupplier pageKeyAllocator) {
    Objects.requireNonNull(node, "node");
    Objects.requireNonNull(info, "info");
    Objects.requireNonNull(newChildRef, "newChildRef");
    final int[] discBits = discriminativeBits(node);
    final int n = node.getNumChildren();
    if (discBits.length == 0 || n < 2) {
      throw new IllegalArgumentException(
          "an indirect split needs >= 2 children and >= 1 disc bit, got n=" + n);
    }
    if (info.affectedCount() == n) {
      throw new IllegalArgumentException(
          "the affected subtree is the whole node — this is the pull-up case, not split");
    }
    if (Arrays.binarySearch(discBits, beta) >= 0) {
      throw new IllegalArgumentException("beta " + beta + " is already a discriminative bit of "
          + "the node — splitIndirectWithEntry requires a genuinely new discriminative bit (an "
          + "existing one would give a two-sided affected subtree)");
    }
    final int[] partials = node.getPartialKeys();
    final PageReference[] children = childReferences(node);
    final int splitPoint = indirectSplitPoint(node);
    if (splitPoint == 0 || splitPoint == n) {
      throw new IllegalArgumentException(
          "node.MSB does not discriminate the children — not a valid HOT node");
    }
    final int nodeMsb = discBits[0];

    // The affected subtree shares the descended child's prefix above beta; node.MSB is more
    // significant than beta, so the whole affected subtree lies in one half — the one holding
    // the descended child. That half folds the new entry in; the other is a plain compress.
    final PageReference left;
    final PageReference right;
    if (info.firstAffected() >= splitPoint) {
      left = compressHalf(Arrays.copyOfRange(children, 0, splitPoint),
          Arrays.copyOfRange(partials, 0, splitPoint), discBits, revision, pageKeyAllocator);
      right = compressRangeWithEntry(discBits, partials, children, splitPoint, n - splitPoint,
          info, beta, betaValue, newChildRef, revision, pageKeyAllocator);
    } else {
      left = compressRangeWithEntry(discBits, partials, children, 0, splitPoint, info, beta,
          betaValue, newChildRef, revision, pageKeyAllocator);
      right = compressHalf(Arrays.copyOfRange(children, splitPoint, n),
          Arrays.copyOfRange(partials, splitPoint, n), discBits, revision, pageKeyAllocator);
    }
    final int height = 1 + Math.max(heightOf(left.getPage()), heightOf(right.getPage()));
    return new BiNode(nodeMsb, height, left, right);
  }

  /**
   * Build one half of a {@link #splitIndirectWithEntry}, folding the new entry in — Binna's
   * {@code compressEntriesAndAddOneEntryIntoNewNode} ({@code HOTSingleThreadedNode.hpp:511} and
   * the {@code compressRangeIntoNewNode}+insert constructor at {@code :186}).
   *
   * <p>The range {@code [firstIndexInRange, firstIndexInRange + rangeLength)} of {@code node}'s
   * children is re-compressed onto its own {@code relevantBits} (the discriminative bits that
   * still branch within the range — Binna's {@code getRelevantBitsForRange}), {@code beta} is
   * folded in, and the new child is placed beside the affected subtree on {@code beta}'s
   * {@code betaValue} side. Crucially the {@code info} is the one computed on the <em>original</em>
   * node — {@code subtreePrefix} is re-encoded into the compressed layout, never re-derived.
   *
   * <p>A range of a single child is the {@code 1:31} caveat: the lone child and the new child
   * become a materialized 2-entry node on {@code beta}.
   */
  private static PageReference compressRangeWithEntry(final int[] discBits, final int[] partials,
      final PageReference[] children, final int firstIndexInRange, final int rangeLength,
      final InsertInfo info, final int beta, final int betaValue, final PageReference newChildRef,
      final int revision, final LongSupplier pageKeyAllocator) {
    if (rangeLength == 1) {
      // 1:31 — the half is the lone affected child; pair it with the new child under beta.
      final PageReference existing = children[firstIndexInRange];
      final PageReference biLeft = betaValue == 1 ? existing : newChildRef;
      final PageReference biRight = betaValue == 1 ? newChildRef : existing;
      final int h = 1 + Math.max(heightOf(biLeft.getPage()), heightOf(biRight.getPage()));
      return swizzle(materialize(new BiNode(beta, h, biLeft, biRight), revision, pageKeyAllocator));
    }
    final int m = discBits.length;

    // relevantBits — the discriminative-bit columns that still branch within the range. For a
    // sorted partial sequence the bits newly set at each step capture exactly the varying columns
    // (Binna's getRelevantBitsForRange).
    int relevantBits = 0;
    for (int i = firstIndexInRange + 1; i < firstIndexInRange + rangeLength; i++) {
      relevantBits |= partials[i] & ~partials[i - 1];
    }
    final int liveCount = Integer.bitCount(relevantBits);
    final int[] liveOldColumn = new int[liveCount];
    final int[] liveDiscBits = new int[liveCount];
    for (int k = 0, li = 0; k < m; k++) {
      if ((relevantBits & (1 << (m - 1 - k))) != 0) {
        liveOldColumn[li] = k;
        liveDiscBits[li] = discBits[k];
        li++;
      }
    }

    // beta is a genuinely new discriminative bit of the node (a splitIndirectWithEntry
    // precondition) — hence new to the half too. It joins the compressed layout at its sorted
    // position; liveDiscBits is a subset of the node's disc bits, so beta is never among them.
    int betaFinalColumn = 0;
    while (betaFinalColumn < liveCount && liveDiscBits[betaFinalColumn] < beta) {
      betaFinalColumn++;
    }
    final int[] finalDiscBits = new int[liveCount + 1];
    System.arraycopy(liveDiscBits, 0, finalDiscBits, 0, betaFinalColumn);
    finalDiscBits[betaFinalColumn] = beta;
    System.arraycopy(liveDiscBits, betaFinalColumn, finalDiscBits, betaFinalColumn + 1,
        liveCount - betaFinalColumn);
    final int m2 = liveCount + 1;
    final int betaWeight = 1 << (m2 - 1 - betaFinalColumn);

    final int numBefore = info.firstAffected() - firstIndexInRange;
    final int affectedCount = info.affectedCount();
    if (numBefore < 0 || numBefore + affectedCount > rangeLength) {
      throw new IllegalStateException("affected subtree [" + info.firstAffected() + ", +"
          + affectedCount + ") is not contained in the split range [" + firstIndexInRange + ", +"
          + rangeLength + ")");
    }
    final int newN = rangeLength + 1;
    final int[] newPartials = new int[newN];
    final PageReference[] newChildren = new PageReference[newN];

    // Children before the affected subtree — re-encoded, beta column left at 0.
    for (int t = 0; t < numBefore; t++) {
      final int src = firstIndexInRange + t;
      newChildren[t] = children[src];
      newPartials[t] = reencodeCompressed(partials[src], m, m2, liveCount, liveOldColumn,
          betaFinalColumn);
    }
    // The new child: the affected subtree's above-beta prefix re-encoded, plus beta = betaValue.
    final int newEntryTarget = numBefore + (betaValue == 1 ? affectedCount : 0);
    newChildren[newEntryTarget] = newChildRef;
    newPartials[newEntryTarget] = reencodeCompressed(info.subtreePrefix(), m, m2, liveCount,
        liveOldColumn, betaFinalColumn) | (betaValue == 1 ? betaWeight : 0);
    // The affected subtree — re-encoded, on beta's 1 - betaValue side. beta is new to these
    // children (it is new to the node), so the re-encoding leaves their beta column 0; it is set
    // uniformly here — the affected subtree is one-sided on beta (beta = msdb).
    final int firstAffectedTarget = numBefore + (1 - betaValue);
    for (int a = 0; a < affectedCount; a++) {
      final int src = info.firstAffected() + a;
      newChildren[firstAffectedTarget + a] = children[src];
      int p = reencodeCompressed(partials[src], m, m2, liveCount, liveOldColumn, betaFinalColumn);
      if (betaValue == 0) {
        p |= betaWeight;
      }
      newPartials[firstAffectedTarget + a] = p;
    }
    // Children after the affected subtree — re-encoded, beta column 0.
    final int afterSrcStart = firstIndexInRange + numBefore + affectedCount;
    final int afterTargetStart = numBefore + affectedCount + 1;
    final int afterCount = rangeLength - numBefore - affectedCount;
    for (int i = 0; i < afterCount; i++) {
      newChildren[afterTargetStart + i] = children[afterSrcStart + i];
      newPartials[afterTargetStart + i] = reencodeCompressed(partials[afterSrcStart + i], m, m2,
          liveCount, liveOldColumn, betaFinalColumn);
    }

    int maxChildHeight = 0;
    for (final PageReference child : newChildren) {
      maxChildHeight = Math.max(maxChildHeight, heightOf(child.getPage()));
    }
    return swizzle(HOTBulkBuilder.assembleIndirect(finalDiscBits, newPartials, newChildren,
        maxChildHeight + 1, revision, pageKeyAllocator));
  }

  /**
   * Re-encode an {@code m}-column partial key into the {@code m2}-column layout of a compressed
   * split half: keep only the columns that survived the compression ({@code liveOldColumn}),
   * re-pack them MSB-first, and skip {@code betaFinalColumn} (the new {@code beta} column). The
   * {@code beta} column itself is left 0; the caller sets it.
   */
  private static int reencodeCompressed(final int fullPartial, final int m, final int m2,
      final int liveCount, final int[] liveOldColumn, final int betaFinalColumn) {
    int out = 0;
    for (int i = 0; i < liveCount; i++) {
      if ((fullPartial & (1 << (m - 1 - liveOldColumn[i]))) != 0) {
        final int finalColumn = i >= betaFinalColumn ? i + 1 : i;
        out |= 1 << (m2 - 1 - finalColumn);
      }
    }
    return out;
  }

  /**
   * For every adjacent child pair of {@code node}, whether it is <em>BiNode-paired</em> — the two
   * children of a single {@code BiNode} in the node's flattened binary trie whose subtree is
   * exactly those two children. Their key sets together then form one complete {@code R(S)}-subtree
   * — the precondition for collapsing them ({@link #mergeBiNodePairedLeaves}).
   *
   * <p>This is the port of Binna's flattened-trie traversal ({@code collectEntryDepth},
   * {@code HOTSingleThreadedNode.hpp}): the sorted partial-key range is recursively partitioned at
   * its most significant <em>differing</em> bit (the range's root BiNode). A pair is BiNode-paired
   * exactly when the recursion isolates it as a two-element range — that range <em>is</em> a
   * BiNode whose two children are precisely those entries. A one-bit partial-key difference alone
   * is <em>not</em> sufficient: the two entries may sit in different sub-branches of a deeper
   * BiNode, and merging them would not yield a clean {@code R(S)}-subtree (an I6 routing break).
   *
   * @return {@code result[i]} iff children {@code i} and {@code i + 1} are BiNode-paired;
   *         {@code result} has length {@code max(0, numChildren - 1)}
   */
  public static boolean[] biNodePairs(final HOTIndirectPage node) {
    Objects.requireNonNull(node, "node");
    final int n = node.getNumChildren();
    final boolean[] paired = new boolean[Math.max(0, n - 1)];
    if (n >= 2) {
      markBiNodePairs(node.getPartialKeys(), 0, n - 1, paired);
    }
    return paired;
  }

  /**
   * Recursively partition the sorted partial-key range {@code [lo, hi]} at its most significant
   * differing bit, marking every two-element range as a BiNode pair. {@code partials[lo]} and
   * {@code partials[hi]} are the range's extremes (sorted ascending, distinct — I7/I3), so the
   * highest set bit of their XOR is the range's root-BiNode bit.
   */
  private static void markBiNodePairs(final int[] partials, final int lo, final int hi,
      final boolean[] paired) {
    if (hi <= lo) {
      return;
    }
    if (hi == lo + 1) {
      paired[lo] = true; // a two-element range is itself a BiNode over those two children
      return;
    }
    final int splitBit = Integer.highestOneBit(partials[lo] ^ partials[hi]);
    int mid = lo + 1;
    while ((partials[mid] & splitBit) == 0) {
      mid++;
    }
    markBiNodePairs(partials, lo, mid - 1, paired);
    markBiNodePairs(partials, mid, hi, paired);
  }

  /**
   * Whether children {@code leftIndex} and {@code leftIndex + 1} of {@code node} are
   * BiNode-paired — see {@link #biNodePairs}. Recomputes the whole pairing; a caller testing many
   * indices of one node should call {@link #biNodePairs} once instead.
   */
  public static boolean areBiNodePaired(final HOTIndirectPage node, final int leftIndex) {
    Objects.requireNonNull(node, "node");
    return leftIndex >= 0 && leftIndex + 1 < node.getNumChildren()
        && biNodePairs(node)[leftIndex];
  }

  /**
   * Collapse two adjacent BiNode-paired leaf children of {@code node} into a single merged leaf —
   * the inverse of a leaf-page split, the structural core of incremental leaf consolidation. The
   * joining {@code BiNode} disappears; if its discriminative bit discriminated only this pair it
   * is dropped from the node, otherwise it is kept (it still branches elsewhere). Either way the
   * re-encoding is delegated to {@link #compressHalf}, which drops exactly the bits that became
   * constant and re-packs the surviving ones.
   *
   * <p>The two leaves' partials already agree everywhere except the joining bit, and the lower
   * child carries that bit as 0, so the merged leaf inherits {@code partials[leftIndex]}
   * unchanged — the joining {@code BiNode}'s position.
   *
   * <p><b>Purity.</b> Allocates only the new compound page; never mutates {@code node}. The
   * caller owns {@code mergedLeaf} (it must hold exactly the union of the two children's
   * entries) and the two collapsed leaf pages.
   *
   * @param node             the compound node whose pair is collapsed
   * @param leftIndex        the lower of the two adjacent (BiNode-paired) child indices
   * @param mergedLeaf       the leaf page holding the union of the two children's entries
   * @param revision         the revision stamped onto the created page
   * @param pageKeyAllocator supplier of fresh persistent page keys
   * @return the rebuilt node (or, if it collapses to one child, that lone child reference)
   */
  public static PageReference mergeBiNodePairedLeaves(final HOTIndirectPage node,
      final int leftIndex, final HOTLeafPage mergedLeaf, final int revision,
      final LongSupplier pageKeyAllocator) {
    Objects.requireNonNull(node, "node");
    Objects.requireNonNull(mergedLeaf, "mergedLeaf");
    Objects.requireNonNull(pageKeyAllocator, "pageKeyAllocator");
    final int n = node.getNumChildren();
    Objects.checkIndex(leftIndex, n - 1);
    if (!biNodePairs(node)[leftIndex]) {
      throw new IllegalArgumentException("children " + leftIndex + " and " + (leftIndex + 1)
          + " are not BiNode-paired — their union is not a single complete R(S)-subtree");
    }
    final int[] partials = node.getPartialKeys();
    final int[] discBits = discriminativeBits(node);
    final PageReference[] oldChildren = childReferences(node);
    final PageReference[] newChildren = new PageReference[n - 1];
    final int[] newPartials = new int[n - 1];
    for (int j = 0; j < n - 1; j++) {
      if (j < leftIndex) {
        newChildren[j] = oldChildren[j];
        newPartials[j] = partials[j];
      } else if (j == leftIndex) {
        newChildren[j] = swizzle(mergedLeaf);
        newPartials[j] = partials[leftIndex]; // the lower child already carries the joining bit 0
      } else {
        newChildren[j] = oldChildren[j + 1];
        newPartials[j] = partials[j + 1];
      }
    }
    return compressHalf(newChildren, newPartials, discBits, revision, pageKeyAllocator);
  }

  /**
   * Consolidate {@code node}'s leaf children — the thesis's <em>underflow</em> rule (§3.3.2)
   * applied with the leaf page treated as a {@code k}-constrained node: two sibling leaf pages
   * whose combined entry count is small enough are an underflow and are node-merged. The
   * incremental insert leaves the trie over-partitioned (a faithful leaf split at the key-set
   * MSDB is uneven, freezing a small half; a branch starts a single-entry leaf) — without
   * consolidation the leaves drift to a fraction of capacity.
   *
   * <p>Greedily merges every adjacent BiNode-paired leaf-child pair whose union stays at or below
   * {@code targetMaxEntries}, repeating until none remain. Each merge is a
   * {@link #mergeBiNodePairedLeaves} (drops the joining discriminative bit). Only fires while the
   * node keeps &ge; 3 children, so the node never collapses to a lone leaf (which would drop a
   * level and stale ancestor heights).
   *
   * <p><b>Purity.</b> Allocates only new pages; never mutates {@code node}. Each merge replaces
   * two leaf pages with one — the two collapsed children's references are appended to
   * {@code droppedLeavesOut} so the caller can release their off-heap slots (a merged-away leaf
   * is unreachable from the result and must not pin its 64KB segment until end-of-transaction).
   *
   * @param droppedLeavesOut sink for every leaf reference this consolidation merged away
   * @return the consolidated node, or {@code node} itself when nothing was mergeable
   */
  public static HOTIndirectPage consolidateNodeLeaves(final HOTIndirectPage node,
      final int targetMaxEntries, final int revision, final IndexType indexType,
      final LongSupplier pageKeyAllocator, final List<PageReference> droppedLeavesOut) {
    Objects.requireNonNull(node, "node");
    Objects.requireNonNull(indexType, "indexType");
    Objects.requireNonNull(pageKeyAllocator, "pageKeyAllocator");
    Objects.requireNonNull(droppedLeavesOut, "droppedLeavesOut");
    HOTIndirectPage current = node;
    boolean merged = true;
    while (merged && current.getNumChildren() >= 3) {
      merged = false;
      final boolean[] pairs = biNodePairs(current);
      for (int i = 0; i < pairs.length; i++) {
        if (!pairs[i]
            || !(current.getChildReference(i).getPage() instanceof HOTLeafPage left)
            || !(current.getChildReference(i + 1).getPage() instanceof HOTLeafPage right)
            || left.getEntryCount() + right.getEntryCount() > targetMaxEntries) {
          continue;
        }
        final HOTLeafPage mergedLeaf =
            new HOTLeafPage(pageKeyAllocator.getAsLong(), revision, indexType);
        boolean fits = true;
        for (int e = 0; e < left.getEntryCount() && fits; e++) {
          fits = mergedLeaf.put(left.getKey(e), left.getValue(e));
        }
        for (int e = 0; e < right.getEntryCount() && fits; e++) {
          fits = mergedLeaf.put(right.getKey(e), right.getValue(e));
        }
        if (!fits) {
          mergedLeaf.close();
          continue;
        }
        droppedLeavesOut.add(current.getChildReference(i));
        droppedLeavesOut.add(current.getChildReference(i + 1));
        current = (HOTIndirectPage) mergeBiNodePairedLeaves(current, i, mergedLeaf, revision,
            pageKeyAllocator).getPage();
        merged = true;
        break;
      }
    }
    return current;
  }

  /**
   * Integrate a {@link BiNode} into a not-full compound node — Binna's {@code addEntry}
   * ({@code HOTSingleThreadedNode.hpp:415}). The {@code biNode} is the split of {@code node}'s
   * child at {@code affectedChildIndex} (a leaf-page split, or a child compound-node split
   * whose height matches {@code node}'s level): that one child slot is replaced by the biNode's
   * two children — {@code biNode.left} into the old slot, {@code biNode.right} into a fresh
   * slot right after it — and {@code biNode.discriminativeBitIndex} is folded in as a new
   * discriminative bit.
   *
   * <p>{@code biNode.discriminativeBitIndex} is always genuinely new to {@code node}: every
   * child of {@code node} is a complete {@code R(S)}-subtree, so its keys are constant on every
   * one of {@code node}'s disc bits (I5); a split bit is by definition a bit on which that
   * subtree's keys <em>differ</em>, hence disjoint from {@code node}'s disc-bit set. Adding it
   * keeps the trie condition: it is less significant than {@code node.MSB} and more significant
   * than the biNode's two subtrees' bits.
   *
   * <p><b>Purity.</b> Returns a fresh compound node; never mutates {@code node} or the biNode.
   *
   * @param node               the not-full compound node to integrate into (&lt; 32 children)
   * @param biNode             the split result to fold in
   * @param affectedChildIndex the slot of {@code node} whose child {@code biNode} split
   * @param revision           the revision stamped onto the created page
   * @param pageKeyAllocator   supplier of a fresh persistent page key
   * @return a fresh compound node with one more child and one more discriminative bit
   */
  public static HOTIndirectPage addEntry(final HOTIndirectPage node, final BiNode biNode,
      final int affectedChildIndex, final int revision, final LongSupplier pageKeyAllocator) {
    Objects.requireNonNull(node, "node");
    Objects.requireNonNull(biNode, "biNode");
    Objects.requireNonNull(pageKeyAllocator, "pageKeyAllocator");
    final int n = node.getNumChildren();
    Objects.checkIndex(affectedChildIndex, n);
    if (n >= HOTIndirectPage.MAX_NODE_ENTRIES) {
      throw new IllegalArgumentException(
          "node is full (" + n + " children) — the caller must split it, not addEntry");
    }
    final int[] discBits = discriminativeBits(node);
    final int m = discBits.length;
    final int beta = biNode.discriminativeBitIndex();
    final int found = Arrays.binarySearch(discBits, beta);
    if (found >= 0) {
      throw new IllegalArgumentException("split bit " + beta
          + " is already a discriminative bit of the node — not a valid integration");
    }
    // Straddle guard (docs/HOT_ADDENTRY_STRADDLE_FIX.md): folding beta in re-encodes every
    // non-affected sibling with beta-column 0, which assumes each sibling subtree is
    // beta-constant. A Sirix multi-value leaf sibling can span beta — adding beta then
    // violates I5. The fold is impossible; the driver recanonicalizes (integrate stamps the
    // depth onto the exception).
    if (!splitBitIsSafe(node, beta, affectedChildIndex, 1)) {
      STRADDLE_GUARD_REJECTIONS.incrementAndGet();
      throw new HOTStraddleException("split bit " + beta + " straddles a non-affected sibling "
          + "subtree of node " + node.getPageKey() + " — incremental fold would violate I5", -1);
    }
    final int betaColumn = -found - 1;          // beta's index in the new (m+1)-bit disc-bit set
    final int[] newDiscBits = new int[m + 1];
    System.arraycopy(discBits, 0, newDiscBits, 0, betaColumn);
    newDiscBits[betaColumn] = beta;
    System.arraycopy(discBits, betaColumn, newDiscBits, betaColumn + 1, m - betaColumn);

    final int[] oldPartials = node.getPartialKeys();
    final PageReference[] oldChildren = childReferences(node);
    final PageReference[] newChildren = new PageReference[n + 1];
    final int[] newPartials = new int[n + 1];
    for (int j = 0; j < n + 1; j++) {
      final int oldIndex;
      final int betaValue;
      if (j < affectedChildIndex) {
        oldIndex = j;
        betaValue = 0;
      } else if (j == affectedChildIndex) {
        oldIndex = affectedChildIndex;          // biNode.left — the beta = 0 side
        betaValue = 0;
        newChildren[j] = biNode.left();
      } else if (j == affectedChildIndex + 1) {
        oldIndex = affectedChildIndex;          // biNode.right — the beta = 1 side
        betaValue = 1;
        newChildren[j] = biNode.right();
      } else {
        oldIndex = j - 1;
        betaValue = 0;
      }
      if (newChildren[j] == null) {
        newChildren[j] = oldChildren[oldIndex];
      }
      newPartials[j] = reencodeWithNewBit(oldPartials[oldIndex], m, betaColumn, betaValue);
    }
    int maxChildHeight = 0;
    for (final PageReference child : newChildren) {
      maxChildHeight = Math.max(maxChildHeight, heightOf(child.getPage()));
    }
    return HOTBulkBuilder.assembleIndirect(newDiscBits, newPartials, newChildren,
        maxChildHeight + 1, revision, pageKeyAllocator);
  }

  /**
   * The key-set MSDB of a child subtree — by Lemma 1 ({@code docs/HOT_ADDENTRY_STRADDLE_FIX.md}
   * §3.1) every key in it agrees on every strictly-more-significant bit. {@code O(1)} with no
   * descent: a compound child reports its stored MSB — for a canonical node the most
   * significant discriminative bit <em>is</em> the most significant bit its key set differs on;
   * a leaf child computes {@code msdb} of its own first and last key; a one-key leaf is
   * constant on every bit (Binna's single-entry-leaf case) so it reports the "never straddles"
   * sentinel {@link Integer#MAX_VALUE} (also avoiding {@code msdb} of equal keys).
   *
   * @param subtree the resolved root page of the child subtree
   * @return the absolute MSB-first index of the subtree's key-set MSDB
   */
  private static int subtreeMsdb(final Page subtree) {
    if (subtree instanceof HOTIndirectPage indirect) {
      return indirect.getMostSignificantBitIndex();
    }
    final HOTLeafPage leaf = (HOTLeafPage) subtree;
    final int entryCount = leaf.getEntryCount();
    return entryCount < 2
        ? Integer.MAX_VALUE
        : HOTBulkBuilder.msdb(leaf.getKey(0), leaf.getKey(entryCount - 1));
  }

  /**
   * Whether the discriminative bit {@code beta} can be folded into {@code node} without
   * violating I5 — i.e. no <em>non-affected</em> child subtree straddles {@code beta} (has keys
   * with {@code beta} set and keys with it clear). The affected run {@code [affectedFirst,
   * affectedFirst + affectedCount)} is exempt — it is the subtree being split <em>on</em>
   * {@code beta}, and {@code addEntry}'s standing precondition makes its replacements
   * {@code beta}-constant.
   *
   * <p>Each sibling is tested in two steps ({@code docs/HOT_ADDENTRY_STRADDLE_FIX.md} §4.2,
   * §5.3): a cheap {@code O(1)} pre-filter — {@code beta} strictly more significant than the
   * sibling's key-set MSDB ⇒ by Lemma 1 it cannot straddle — and, only when that is
   * inconclusive, a precise {@link #subtreeStraddles} scan. The result is therefore
   * <em>exact</em> for fully-resolved siblings: no false negatives <em>and</em> no false
   * positives. An unresolved page (defensive — the driver pre-resolves path children) is
   * treated conservatively as a straddle.
   *
   * @param node          the compound node {@code beta} would be folded into
   * @param beta          the new discriminative bit (absolute, MSB-first)
   * @param affectedFirst the first index of the exempt affected run
   * @param affectedCount the size of the exempt affected run
   * @return {@code true} iff folding {@code beta} in is straddle-free
   */
  static boolean splitBitIsSafe(final HOTIndirectPage node, final int beta,
      final int affectedFirst, final int affectedCount) {
    final int n = node.getNumChildren();
    for (int c = 0; c < n; c++) {
      if (c >= affectedFirst && c < affectedFirst + affectedCount) {
        continue;                                   // affected run — exempt (split ON beta)
      }
      final PageReference childRef = node.getChildReference(c);
      final Page childPage = childRef == null ? null : childRef.getPage();
      if (childPage == null) {
        return false;                               // unresolved — conservatively unsafe
      }
      // Pre-filter: beta strictly more significant (smaller absolute index) than the sibling's
      // key-set MSDB ⇒ Lemma 1 ⇒ the sibling cannot straddle beta — skip the scan.
      if (beta < subtreeMsdb(childPage)) {
        continue;
      }
      // Inconclusive — scan the sibling precisely.
      if (subtreeStraddles(childPage, beta)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Whether {@code subtree}'s key set straddles bit {@code beta} — contains a key with
   * {@code beta} set and a key with it clear. A precise scan, used by {@link #splitBitIsSafe}
   * once the cheap MSDB pre-filter is inconclusive. An unresolved descendant page is treated
   * conservatively as a straddle.
   */
  private static boolean subtreeStraddles(final Page subtree, final int beta) {
    return straddleScan(subtree, beta, new int[] {-1});
  }

  /**
   * Recursive helper for {@link #subtreeStraddles}. {@code firstValue} is a one-cell holder:
   * {@code -1} until the first key is seen, then that key's {@code beta} bit value. Returns
   * {@code true} as soon as a key disagrees with the first (an early exit).
   */
  private static boolean straddleScan(final Page page, final int beta, final int[] firstValue) {
    if (page == null) {
      return true;                                  // unresolved descendant — conservatively a straddle
    }
    if (page instanceof HOTLeafPage leaf) {
      final int count = leaf.getEntryCount();
      for (int i = 0; i < count; i++) {
        final int v = HOTBulkBuilder.bitAt(leaf.getKey(i), beta) ? 1 : 0;
        if (firstValue[0] < 0) {
          firstValue[0] = v;
        } else if (firstValue[0] != v) {
          return true;
        }
      }
      return false;
    }
    final HOTIndirectPage indirect = (HOTIndirectPage) page;
    for (int i = 0; i < indirect.getNumChildren(); i++) {
      final PageReference ref = indirect.getChildReference(i);
      if (straddleScan(ref == null ? null : ref.getPage(), beta, firstValue)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Re-encode a partial key when a new discriminative bit is inserted at column
   * {@code betaColumn}. The old key spans {@code m} columns; the result spans {@code m + 1},
   * with the old columns shifted past {@code betaColumn} and the new column set to
   * {@code betaValue}. Both encodings are MSB-first (column 0 = most significant).
   */
  private static int reencodeWithNewBit(final int oldPartial, final int m, final int betaColumn,
      final int betaValue) {
    final int m2 = m + 1;
    int p = 0;
    for (int k = 0; k < m; k++) {
      if ((oldPartial & (1 << (m - 1 - k))) != 0) {
        final int newColumn = k < betaColumn ? k : k + 1;
        p |= 1 << (m2 - 1 - newColumn);
      }
    }
    if (betaValue != 0) {
      p |= 1 << (m2 - 1 - betaColumn);
    }
    return p;
  }

  /**
   * The post-analysis of HOT's approximate insert descent — Binna's {@code getInsertInformation}
   * ({@code HOTSingleThreadedNode.hpp:383}). For the node a branch insert stopped at, the child
   * slot {@code entryIndex} the descent took, and the mismatch bit {@code beta}, it reports
   * whether {@code beta} is already a discriminative bit of the node and the extent of the
   * affected subtree.
   *
   * @param betaIsDiscBit   {@code true} iff {@code beta} is already a discriminative bit of the
   *                        node — the descent misrouted and the key must re-route, not branch
   * @param affectedCount   number of the node's children {@code beta}'s new BiNode roots — the
   *                        contiguous run sharing {@code entryIndex}'s above-{@code beta} prefix
   * @param firstAffected   the lowest child index of that run
   * @param subtreePrefix   {@code partials[entryIndex]} masked to the columns more significant
   *                        than {@code beta} — the partial the new key's leaf inherits
   */
  public record InsertInfo(boolean betaIsDiscBit, int affectedCount, int firstAffected,
                           int subtreePrefix) {}

  /**
   * Compute the {@link InsertInfo} for a branch insert — Binna's {@code getInsertInformation}.
   * {@code prefixBits} are the partial-key columns whose discriminative bit is more significant
   * than {@code beta} (Binna's {@code getPrefixBitsMask}); the affected subtree is the children
   * whose partial agrees with {@code entryIndex}'s on those columns (Binna's
   * {@code getAffectedSubtreeMask} = {@code (partial & prefixBits) == subtreePrefix}). Children
   * are in ascending partial-key order (I7) and share a prefix, so the affected set is a
   * contiguous run.
   */
  static InsertInfo getInsertInformation(final HOTIndirectPage node, final int entryIndex,
      final int beta) {
    final int[] discBits = discriminativeBits(node);
    final int m = discBits.length;
    final int[] partials = node.getPartialKeys();
    int prefixBits = 0;
    boolean betaIsDiscBit = false;
    for (int k = 0; k < m; k++) {
      if (discBits[k] < beta) {
        prefixBits |= 1 << (m - 1 - k);   // column k carries weight 1 << (m - 1 - k)
      } else if (discBits[k] == beta) {
        betaIsDiscBit = true;
      }
    }
    final int subtreePrefix = partials[entryIndex] & prefixBits;
    final int n = node.getNumChildren();
    int count = 0;
    int first = -1;
    for (int i = 0; i < n; i++) {
      if ((partials[i] & prefixBits) == subtreePrefix) {
        if (first < 0) {
          first = i;
        }
        count++;
      }
    }
    return new InsertInfo(betaIsDiscBit, count, first, subtreePrefix);
  }

  /**
   * Fold a new discriminative bit {@code beta} and a new child into a not-full compound node —
   * Binna's {@code insertNewValue} / {@code addEntry}-with-{@code InsertInformation}. The new
   * BiNode on {@code beta} roots the affected subtree {@code [firstAffected, firstAffected +
   * affectedCount)}: those children move to {@code beta}'s {@code 1 - newChildBetaValue} side
   * (keeping their full partials), and {@code newChildRef} becomes the lone child on
   * {@code beta}'s {@code newChildBetaValue} side.
   *
   * <p>Unlike {@link #addEntry} (which splits one child slot — correct only when both halves
   * inherit that slot's full partial, e.g. a leaf-page overflow), the new child here inherits
   * only {@code subtreePrefix} — the affected subtree's prefix above {@code beta} — so its
   * partial bits <em>below</em> {@code beta} are zero. A branch key agrees with the affected
   * subtree only above {@code beta}; copying the slot's lower bits would misplace it.
   *
   * <p><b>Purity.</b> Returns a fresh compound node; never mutates {@code node}. Requires
   * {@code node} to be not full (its child count + 1 &le; {@link HOTIndirectPage#MAX_NODE_ENTRIES})
   * and {@code beta} to not already be a discriminative bit of {@code node}.
   *
   * @param node              the not-full compound node {@code beta} joins
   * @param beta              the new discriminative bit (absolute, MSB-first)
   * @param newChildBetaValue the new key's value at {@code beta} (0 or 1)
   * @param firstAffected     the affected subtree's first child index ({@link InsertInfo})
   * @param affectedCount     the affected subtree's size ({@link InsertInfo})
   * @param subtreePrefix     the affected subtree's above-{@code beta} prefix ({@link InsertInfo})
   * @param newChildRef       the new child (the branch key's single-entry leaf)
   * @param height            the assembled node's height ({@code node}'s height — adding a leaf
   *                          child never raises it)
   * @param revision          the revision stamped onto the created page
   * @param pageKeyAllocator  supplier of a fresh persistent page key
   * @return a fresh compound node with one more child and {@code beta} as a new disc bit
   */
  public static HOTIndirectPage addEntryWithInsertInfo(final HOTIndirectPage node, final int beta,
      final int newChildBetaValue, final int firstAffected, final int affectedCount,
      final int subtreePrefix, final PageReference newChildRef, final int height,
      final int revision, final LongSupplier pageKeyAllocator) {
    Objects.requireNonNull(node, "node");
    Objects.requireNonNull(newChildRef, "newChildRef");
    final int[] discBits = discriminativeBits(node);
    final int m = discBits.length;
    final int found = Arrays.binarySearch(discBits, beta);
    if (found >= 0) {
      throw new IllegalArgumentException(
          "split bit " + beta + " is already a discriminative bit of the node");
    }
    final int betaColumn = -found - 1;
    final int[] newDiscBits = new int[m + 1];
    System.arraycopy(discBits, 0, newDiscBits, 0, betaColumn);
    newDiscBits[betaColumn] = beta;
    System.arraycopy(discBits, betaColumn, newDiscBits, betaColumn + 1, m - betaColumn);

    final int[] oldPartials = node.getPartialKeys();
    final PageReference[] oldChildren = childReferences(node);
    final int n = oldChildren.length;
    final int affectedBetaValue = 1 - newChildBetaValue;

    final PageReference[] newChildren = new PageReference[n + 1];
    final int[] newPartials = new int[n + 1];
    // Children before the affected subtree — unchanged, beta column 0 (beta is not on their path).
    for (int i = 0; i < firstAffected; i++) {
      newChildren[i] = oldChildren[i];
      newPartials[i] = reencodeWithNewBit(oldPartials[i], m, betaColumn, 0);
    }
    // The group [firstAffected, firstAffected + affectedCount]: the affected subtree on beta's
    // 1 - newChildBetaValue side, the new child alone on the newChildBetaValue side. The beta = 0
    // side sorts first.
    final int newChildSlot = newChildBetaValue == 0 ? firstAffected : firstAffected + affectedCount;
    final int affectedStart = newChildBetaValue == 0 ? firstAffected + 1 : firstAffected;
    newChildren[newChildSlot] = newChildRef;
    newPartials[newChildSlot] = reencodeWithNewBit(subtreePrefix, m, betaColumn, newChildBetaValue);
    for (int a = 0; a < affectedCount; a++) {
      newChildren[affectedStart + a] = oldChildren[firstAffected + a];
      newPartials[affectedStart + a] =
          reencodeWithNewBit(oldPartials[firstAffected + a], m, betaColumn, affectedBetaValue);
    }
    // Children after the affected subtree — unchanged, beta column 0.
    for (int i = firstAffected + affectedCount; i < n; i++) {
      newChildren[i + 1] = oldChildren[i];
      newPartials[i + 1] = reencodeWithNewBit(oldPartials[i], m, betaColumn, 0);
    }
    return HOTBulkBuilder.assembleIndirect(newDiscBits, newPartials, newChildren, height, revision,
        pageKeyAllocator);
  }

  /**
   * Add a child for a new combination of {@code node}'s <em>existing</em> discriminative bits —
   * Binna's {@code addEntry} for the case where the mismatch bit is already a discriminative bit
   * ({@code DiscriminativeBitsRepresentation.insert} is a no-op — no bit is added, only an entry).
   *
   * <p>{@code sparsePartialKey} is the new child's sparse-path partial: the discriminative-bit
   * pattern on the path from {@code node}'s flattened binary trie root to the new child, with
   * every off-path (more specific) bit zero — a fresh single-entry leaf is its own subtree root.
   * The discriminative bits, the mask, and every other child's stored partial are unchanged; the
   * new child slots in at {@code sparsePartialKey}'s ascending position (I7).
   *
   * <p><b>Purity.</b> Returns a fresh compound node; never mutates {@code node}. Requires
   * {@code node} to be not full and {@code sparsePartialKey} to not already be a child partial.
   *
   * @param node             the not-full compound node the key branches at
   * @param sparsePartialKey the new child's sparse-path partial key
   * @param newChildRef      the new child (the branch key's single-entry leaf)
   * @param height           the assembled node's height ({@code node}'s height — adding a leaf
   *                         child never raises it)
   * @param revision         the revision stamped onto the created page
   * @param pageKeyAllocator supplier of a fresh persistent page key
   * @return a fresh compound node with one more child, same discriminative bits
   */
  public static HOTIndirectPage addChildAtCombination(final HOTIndirectPage node,
      final int sparsePartialKey, final PageReference newChildRef, final int height,
      final int revision, final LongSupplier pageKeyAllocator) {
    Objects.requireNonNull(node, "node");
    Objects.requireNonNull(newChildRef, "newChildRef");
    final int[] discBits = discriminativeBits(node);
    final int[] oldPartials = node.getPartialKeys();
    final PageReference[] oldChildren = childReferences(node);
    final int n = oldChildren.length;
    // Children are in ascending partial-key order (I7); find sparsePartialKey's insertion point.
    int pos = 0;
    while (pos < n && oldPartials[pos] < sparsePartialKey) {
      pos++;
    }
    if (pos < n && oldPartials[pos] == sparsePartialKey) {
      throw new IllegalArgumentException("sparse partial key " + sparsePartialKey
          + " is already a child of the node — not a new combination");
    }
    final int[] newPartials = new int[n + 1];
    final PageReference[] newChildren = new PageReference[n + 1];
    System.arraycopy(oldPartials, 0, newPartials, 0, pos);
    System.arraycopy(oldChildren, 0, newChildren, 0, pos);
    newPartials[pos] = sparsePartialKey;
    newChildren[pos] = newChildRef;
    System.arraycopy(oldPartials, pos, newPartials, pos + 1, n - pos);
    System.arraycopy(oldChildren, pos, newChildren, pos + 1, n - pos);
    return HOTBulkBuilder.assembleIndirect(discBits, newPartials, newChildren, height, revision,
        pageKeyAllocator);
  }

  /**
   * The structural outcome of an insert descent — Binna's {@code searchForInsert} +
   * {@code executeForDiffingKeys} + insert-depth ({@code HOTSingleThreaded.hpp:227-269}),
   * adapted for SirixDB's multi-value leaf pages. Produced by {@link #analyzeDescent} and
   * consumed by the integration step to decide merge-vs-branch and where to attach a new
   * {@link BiNode}.
   *
   * @param leaf               the leaf page the key routed to
   * @param residentKey        the leaf key the new key was diffed against — the one sharing the
   *                           longest prefix with it; {@code null} when the leaf is empty
   * @param mismatchBit        β — the most significant bit on which the new key differs from
   *                           {@code residentKey}; {@code -1} when the key is already present
   *                           or the leaf is empty
   * @param insertDepth        d* — the index in the path of the compound node whose block β
   *                           belongs in; {@code -1} when the path has no compound node, or the
   *                           key is already present
   * @param affectedChildIndex the child slot taken at {@code insertDepth} — the subtree a
   *                           {@code BiNode} on β attaches beside; {@code -1} when d* is -1
   * @param keyAlreadyPresent  {@code true} iff the key is already an entry of {@code leaf}
   */
  public record DescentAnalysis(HOTLeafPage leaf, byte[] residentKey, int mismatchBit,
      int insertDepth, int affectedChildIndex, boolean keyAlreadyPresent) {}

  /**
   * Analyze an already-walked insert descent — the post-processing of Binna's
   * {@code searchForInsert}: pick the resident key the new key diverged from, compute the
   * mismatch bit β, and walk the parent stack to the insert depth d*. Pure — reads the pages,
   * allocates only the result.
   *
   * <p><b>Resident key.</b> Binna's single-entry leaf yields the resident trivially; a Sirix
   * leaf page holds up to 512 sorted keys, so the resident is the leaf key sharing the longest
   * common prefix with {@code key} — equivalently the key {@code r} maximizing
   * {@code index(msdb(key, r))}. The leaf is a sorted contiguous range, so that key is a
   * neighbor of {@code key}'s insertion point and only the two neighbors need be examined.
   *
   * <p><b>Insert depth.</b> {@code while (pathNodes[d+1].MSB more significant than β) ++d} —
   * the reference's {@code HOTSingleThreaded.hpp:265-269}. With the trie condition (I11) the
   * path MSBs strictly increase down the tree, so d* is the count of path compound nodes below
   * the root whose MSB is more significant than β.
   *
   * @param pathNodes        the compound nodes on the descent path, root first
   * @param pathChildIndices the child slot chosen at each path node (parallel to pathNodes)
   * @param pathDepth        the number of compound nodes on the path (0 ⇒ the index is a leaf)
   * @param leaf             the leaf page the key routed to
   * @param key              the full key being inserted
   * @return the descent analysis
   */
  public static DescentAnalysis analyzeDescent(final HOTIndirectPage[] pathNodes,
      final int[] pathChildIndices, final int pathDepth, final HOTLeafPage leaf,
      final byte[] key) {
    Objects.requireNonNull(pathNodes, "pathNodes");
    Objects.requireNonNull(pathChildIndices, "pathChildIndices");
    Objects.requireNonNull(leaf, "leaf");
    Objects.requireNonNull(key, "key");

    // Locate the key's insertion point among the leaf's sorted entries (I2).
    final int entryCount = leaf.getEntryCount();
    int lo = 0;
    int hi = entryCount;
    boolean present = false;
    while (lo < hi) {
      final int mid = (lo + hi) >>> 1;
      final int cmp = Arrays.compareUnsigned(leaf.getKey(mid), key);
      if (cmp < 0) {
        lo = mid + 1;
      } else if (cmp > 0) {
        hi = mid;
      } else {
        present = true;
        break;
      }
    }
    if (present || entryCount == 0) {
      return new DescentAnalysis(leaf, present ? key : null, -1, -1, -1, present);
    }

    // The resident key is the insertion-point neighbor sharing the longest prefix with key —
    // the one with the larger (more specific) msdb. Only the two neighbors can be the longest:
    // any leaf key sorts outside (key's predecessor, key's successor], so a longer shared
    // prefix than a neighbor's is impossible.
    final int insertionPoint = lo;
    byte[] resident = null;
    int beta = -1;
    if (insertionPoint > 0) {
      resident = leaf.getKey(insertionPoint - 1);
      beta = HOTBulkBuilder.msdb(key, resident);
    }
    if (insertionPoint < entryCount) {
      final byte[] successor = leaf.getKey(insertionPoint);
      final int successorBeta = HOTBulkBuilder.msdb(key, successor);
      if (successorBeta > beta) {        // larger absolute index ⇒ more specific ⇒ longer prefix
        beta = successorBeta;
        resident = successor;
      }
    }

    // Insert depth d* — descend the parent stack while the next compound node's MSB is more
    // significant (smaller absolute index) than β. The leaf has no MSB, so the loop's upper
    // bound is the stack's last compound node (Binna's UINT16_MAX leaf sentinel).
    int insertDepth = pathDepth == 0 ? -1 : 0;
    while (insertDepth + 1 < pathDepth
        && pathNodes[insertDepth + 1].getMostSignificantBitIndex() < beta) {
      insertDepth++;
    }
    final int affectedChildIndex = insertDepth < 0 ? -1 : pathChildIndices[insertDepth];
    return new DescentAnalysis(leaf, resident, beta, insertDepth, affectedChildIndex, false);
  }

  /**
   * The outcome of {@link #integrate}: the (possibly new) index-root reference and the single
   * spine reference whose page {@code integrate} re-pointed via {@link PageReference#setPage}.
   *
   * <p>{@code integrate} re-points exactly one spine reference (the capacity cascade recurses,
   * but only the level where it stops does a {@code setPage}). Every page the integration freshly
   * created hangs at or below {@code touchedRef}; the caller registers that subtree in the
   * transaction-intent log by walking down from {@code touchedRef}, stopping at shared subtrees.
   *
   * @param rootRef    the index-root reference — {@code spineRefs[0]}, re-pointed if the root grew
   * @param touchedRef the spine reference {@code integrate} re-pointed; root of the fresh subtree
   */
  public record IntegrationResult(PageReference rootRef, PageReference touchedRef) {}

  /**
   * Integrate a {@link BiNode} into the descent spine — Binna's {@code integrateBiNodeIntoTree}
   * ({@code HOTSingleThreaded.hpp:496-547}), adapted for SirixDB's persistent pages.
   *
   * <p>{@code biNode} is the split result that must take the position of the spine node at
   * {@code currentDepth} (the leaf when {@code currentDepth == spineNodes.length}). The four
   * cases mirror the reference:
   * <ul>
   *   <li><b>new root</b> ({@code currentDepth == 0}) — materialize {@code biNode} as a 2-entry
   *       compound node; it becomes the new index root, growing the height by one.</li>
   *   <li><b>intermediate node</b> ({@code parent.height > biNode.height}) — materialize
   *       {@code biNode} as a 2-entry node and slot it where the old node was; the parent's
   *       block is untouched.</li>
   *   <li><b>addEntry</b> (parent not full) — fold {@code biNode}'s bit and children into the
   *       parent's block via {@link #addEntry}.</li>
   *   <li><b>capacity cascade</b> (parent full) — {@link #splitIndirect} the parent, fold
   *       {@code biNode} into the half that owns the affected child, and recurse one level
   *       shallower with the parent's own split {@code BiNode}.</li>
   * </ul>
   *
   * <p><b>Spine.</b> {@code spineNodes[0..D-1]} are the descent path's compound nodes (root
   * first); {@code spineRefs[i]} is the reference {@code spineNodes[i]} resides under, and
   * {@code spineRefs[D]} is the leaf's reference; {@code childSlots[i]} is the slot the descent
   * took at {@code spineNodes[i]}. The descent has already copy-on-written the spine, so
   * re-pointing a {@code spineRefs[d]} (here via {@link PageReference#setPage}) is observed by
   * every ancestor without rebuilding it. Every page {@code integrate} creates is swizzled into
   * its reference; the caller registers the new pages in the transaction-intent log.
   *
   * <p><b>Precondition.</b> Neither child of {@code biNode} may be {@code spineRefs[currentDepth]}
   * itself — the new-root and intermediate-node cases re-point that slot to a node built from the
   * BiNode's children, so aliasing it would make the node its own descendant (a page cycle). A
   * caller wrapping an existing spine subtree must pass a fresh {@link PageReference} to it.
   *
   * @param spineNodes       the descent path's compound nodes, root first
   * @param spineRefs        the spine's references — {@code spineNodes.length + 1} of them, the
   *                         last being the leaf's reference
   * @param childSlots       the child slot taken at each spine node
   * @param currentDepth     the depth whose subtree {@code biNode} replaces
   * @param biNode           the split result to integrate
   * @param revision         the revision stamped onto every created page
   * @param pageKeyAllocator supplier of fresh persistent page keys
   * @return the integration result — the (possibly new) root reference and the touched reference
   */
  public static IntegrationResult integrate(final HOTIndirectPage[] spineNodes,
      final PageReference[] spineRefs, final int[] childSlots, final int currentDepth,
      final BiNode biNode, final int revision, final LongSupplier pageKeyAllocator) {
    Objects.requireNonNull(biNode, "biNode");
    Objects.requireNonNull(pageKeyAllocator, "pageKeyAllocator");

    if (currentDepth == 0) {
      spineRefs[0].setPage(materialize(biNode, revision, pageKeyAllocator));
      return new IntegrationResult(spineRefs[0], spineRefs[0]);
    }
    final int parentDepth = currentDepth - 1;
    final HOTIndirectPage parent = spineNodes[parentDepth];

    if (parent.getHeight() > biNode.height()) {
      // Intermediate node: biNode's subtree is shorter than the parent's level — give it its
      // own 2-entry compound node in the slot the old node occupied; the parent is untouched.
      spineRefs[currentDepth].setPage(materialize(biNode, revision, pageKeyAllocator));
      return new IntegrationResult(spineRefs[0], spineRefs[currentDepth]);
    }

    final int affectedChildIndex = childSlots[parentDepth];
    if (parent.getNumChildren() < HOTIndirectPage.MAX_NODE_ENTRIES) {
      final HOTIndirectPage folded;
      try {
        folded = addEntry(parent, biNode, affectedChildIndex, revision, pageKeyAllocator);
      } catch (HOTStraddleException straddle) {
        throw tagStraddle(straddle, parentDepth);   // stamp the unsafe node's spine depth
      }
      spineRefs[parentDepth].setPage(folded);
      return new IntegrationResult(spineRefs[0], spineRefs[parentDepth]);
    }

    // Capacity cascade: the parent is full. Its MSB must be more significant than β — the trie
    // condition; otherwise β could not branch strictly below the parent (Theorem II(d)).
    if (parent.getMostSignificantBitIndex() >= biNode.discriminativeBitIndex()) {
      throw new IllegalStateException("trie condition violated at depth " + parentDepth
          + ": parent.MSB=" + parent.getMostSignificantBitIndex() + " >= beta="
          + biNode.discriminativeBitIndex());
    }
    final int splitPoint = indirectSplitPoint(parent);
    final BiNode parentSplit = splitIndirect(parent, revision, pageKeyAllocator);
    final BiNode cascaded;
    try {
      cascaded = foldIntoHalf(parentSplit, splitPoint, parent.getNumChildren(),
          affectedChildIndex, biNode, revision, pageKeyAllocator);
    } catch (HOTStraddleException straddle) {
      throw tagStraddle(straddle, parentDepth);     // the cascade's addEntry into a half straddled
    }
    return integrate(spineNodes, spineRefs, childSlots, parentDepth, cascaded, revision,
        pageKeyAllocator);
  }

  /**
   * Stamp an untagged {@link HOTStraddleException} with {@code nodeDepth} — the spine depth of
   * the node whose {@code addEntry} straddled — so the driver scopes the recanonicalization to
   * that node's subtree. An already-tagged exception (a deeper {@code integrate} frame stamped
   * it first) propagates unchanged.
   */
  private static HOTStraddleException tagStraddle(final HOTStraddleException straddle,
      final int nodeDepth) {
    return straddle.nodeDepthHint >= 0 ? straddle
        : new HOTStraddleException(straddle.getMessage(), nodeDepth);
  }

  /**
   * Fold {@code biNode} into the half of {@code parentSplit} that owns the affected child and
   * return the parent's split {@code BiNode} with that half rebuilt. The capacity cascade uses
   * it: when a full parent splits at {@code splitPoint}, the affected child lands in exactly one
   * half, which is therefore not full and can absorb {@code biNode} via {@link #addEntry}. The
   * 1:31 caveat: a half of a single child is a bare reference (no compound wrapper), and
   * {@code biNode} — the split of exactly that child — replaces it directly.
   */
  private static BiNode foldIntoHalf(final BiNode parentSplit, final int splitPoint,
      final int parentChildCount, final int affectedChildIndex, final BiNode biNode,
      final int revision, final LongSupplier pageKeyAllocator) {
    final boolean inLeftHalf = affectedChildIndex < splitPoint;
    final int halfChildCount = inLeftHalf ? splitPoint : parentChildCount - splitPoint;
    final PageReference targetRef = inLeftHalf ? parentSplit.left() : parentSplit.right();
    final PageReference otherRef = inLeftHalf ? parentSplit.right() : parentSplit.left();

    final PageReference rebuiltRef;
    if (halfChildCount == 1) {
      // 1:31 — the half is the lone affected child; biNode (its split) replaces it.
      rebuiltRef = swizzle(materialize(biNode, revision, pageKeyAllocator));
    } else {
      final HOTIndirectPage half = (HOTIndirectPage) targetRef.getPage();
      final int indexInHalf = inLeftHalf ? affectedChildIndex : affectedChildIndex - splitPoint;
      rebuiltRef = swizzle(addEntry(half, biNode, indexInHalf, revision, pageKeyAllocator));
    }
    final PageReference left = inLeftHalf ? rebuiltRef : otherRef;
    final PageReference right = inLeftHalf ? otherRef : rebuiltRef;
    final int height = 1 + Math.max(heightOf(left.getPage()), heightOf(right.getPage()));
    return new BiNode(parentSplit.discriminativeBitIndex(), height, left, right);
  }

  /** Materialize a virtual {@link BiNode} as a standalone 2-entry compound node. */
  private static HOTIndirectPage materialize(final BiNode biNode, final int revision,
      final LongSupplier pageKeyAllocator) {
    return HOTIndirectPage.createBiNode(pageKeyAllocator.getAsLong(), revision,
        biNode.discriminativeBitIndex(), biNode.left(), biNode.right(), biNode.height());
  }

  /**
   * The split point of an indirect compound node — the index of the first child whose stored
   * partial has {@code node.MSB} set. {@link #splitIndirect} partitions the children there; the
   * capacity cascade needs the same point to locate the affected child's half.
   */
  private static int indirectSplitPoint(final HOTIndirectPage node) {
    final int[] discBits = discriminativeBits(node);
    final int[] partials = node.getPartialKeys();
    final int topWeight = 1 << (discBits.length - 1);
    final int n = node.getNumChildren();
    int s = 0;
    while (s < n && (partials[s] & topWeight) == 0) {
      s++;
    }
    return s;
  }

  /**
   * Recover a compound node's discriminative bits as sorted ascending absolute (MSB-first) bit
   * positions — {@code result[0]} is the node's MSB. Handles both the SingleMask layout (a
   * 64-bit mask over an 8-byte window) and the MultiMask layout (per-key-byte masks); it is the
   * inverse of {@link HOTBulkBuilder#assembleIndirect}'s mask construction.
   */
  static int[] discriminativeBits(final HOTIndirectPage node) {
    if (node.getLayoutType() == HOTIndirectPage.LayoutType.MULTI_MASK) {
      final byte[] positions = node.getExtractionPositions();
      final long[] masks = node.getExtractionMasks();
      final int numExtractionBytes = node.getNumExtractionBytes();
      final int[] bits = new int[node.getTotalDiscBits()];
      int idx = 0;
      // Extraction bytes are stored in ascending key-byte order; within a byte the mask bit
      // 1 << (7 - b) corresponds to MSB-first bit-in-byte b — iterate b ascending for sorted out.
      for (int o = 0; o < numExtractionBytes; o++) {
        final int bytePos = positions[o] & 0xFF;
        final int byteMask = (int) ((masks[o / 8] >>> ((7 - o % 8) * 8)) & 0xFFL);
        for (int b = 0; b < 8; b++) {
          if ((byteMask & (1 << (7 - b))) != 0) {
            bits[idx++] = bytePos * 8 + b;
          }
        }
      }
      return bits;
    }
    final long bitMask = node.getBitMask();
    final int initialBytePos = node.getInitialBytePos();
    final int[] bits = new int[Long.bitCount(bitMask)];
    int idx = 0;
    for (int longBit = 63; longBit >= 0; longBit--) {   // long bit 63 = most significant
      if ((bitMask & (1L << longBit)) != 0) {
        bits[idx++] = initialBytePos * 8 + (63 - longBit);
      }
    }
    return bits;
  }

  /** Snapshot a compound node's child references into a fresh array (the node is not mutated). */
  private static PageReference[] childReferences(final HOTIndirectPage node) {
    final PageReference[] children = new PageReference[node.getNumChildren()];
    for (int i = 0; i < children.length; i++) {
      children[i] = node.getChildReference(i);
    }
    return children;
  }

  /**
   * Merge a HOT secondary-index value into an existing one. Index values are chunked-bitmap
   * {@link NodeReferences}; merging two fragments of the same key is the bitwise OR of their
   * chunk bitmaps. A tombstone carries no live bits, so a reinsert over a tombstone yields the
   * incoming value.
   */
  static byte[] mergeIndexValues(final byte[] existing, final byte[] incoming) {
    if (NodeReferencesSerializer.isTombstone(existing, 0, existing.length)
        || NodeReferencesSerializer.isTombstone(incoming, 0, incoming.length)) {
      return incoming;
    }
    final NodeReferences merged = NodeReferencesSerializer.deserialize(existing);
    merged.getNodeKeys().or(NodeReferencesSerializer.deserialize(incoming).getNodeKeys());
    final byte[] out = new byte[NodeReferencesSerializer.computeSerializedSize(merged)];
    NodeReferencesSerializer.serialize(merged, out, 0);
    return out;
  }

  private static int heightOf(final Page page) {
    return page instanceof HOTIndirectPage indirect ? indirect.getHeight() : 0;
  }

  private static PageReference swizzle(final Page page) {
    final PageReference reference = new PageReference();
    reference.setPage(page);
    return reference;
  }
}
