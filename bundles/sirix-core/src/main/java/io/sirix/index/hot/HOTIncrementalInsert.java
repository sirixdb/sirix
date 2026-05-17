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
 * <p><b>Work in progress.</b> This class is built incrementally per the port plan. Steps 1-4
 * provide the {@link BiNode} type, the leaf-page and indirect-node split primitives
 * ({@link #splitLeafPage}, {@link #splitIndirect}, {@link #addEntry}), the descent analysis
 * ({@link #analyzeDescent}), and the spine integration with the capacity cascade
 * ({@link #integrate}). Step 5 — wiring into the live insert path — follows.
 *
 * @author Johannes Lichtenberger
 */
public final class HOTIncrementalInsert {

  private HOTIncrementalInsert() {
    throw new AssertionError("utility class — static primitives only");
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
   * @param spineNodes       the descent path's compound nodes, root first
   * @param spineRefs        the spine's references — {@code spineNodes.length + 1} of them, the
   *                         last being the leaf's reference
   * @param childSlots       the child slot taken at each spine node
   * @param currentDepth     the depth whose subtree {@code biNode} replaces
   * @param biNode           the split result to integrate
   * @param revision         the revision stamped onto every created page
   * @param pageKeyAllocator supplier of fresh persistent page keys
   * @return the index-root reference — {@code spineRefs[0]}, re-pointed when the root changed
   */
  public static PageReference integrate(final HOTIndirectPage[] spineNodes,
      final PageReference[] spineRefs, final int[] childSlots, final int currentDepth,
      final BiNode biNode, final int revision, final LongSupplier pageKeyAllocator) {
    Objects.requireNonNull(biNode, "biNode");
    Objects.requireNonNull(pageKeyAllocator, "pageKeyAllocator");

    if (currentDepth == 0) {
      spineRefs[0].setPage(materialize(biNode, revision, pageKeyAllocator));
      return spineRefs[0];
    }
    final int parentDepth = currentDepth - 1;
    final HOTIndirectPage parent = spineNodes[parentDepth];

    if (parent.getHeight() > biNode.height()) {
      // Intermediate node: biNode's subtree is shorter than the parent's level — give it its
      // own 2-entry compound node in the slot the old node occupied; the parent is untouched.
      spineRefs[currentDepth].setPage(materialize(biNode, revision, pageKeyAllocator));
      return spineRefs[0];
    }

    final int affectedChildIndex = childSlots[parentDepth];
    if (parent.getNumChildren() < HOTIndirectPage.MAX_NODE_ENTRIES) {
      spineRefs[parentDepth].setPage(
          addEntry(parent, biNode, affectedChildIndex, revision, pageKeyAllocator));
      return spineRefs[0];
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
    final BiNode cascaded = foldIntoHalf(parentSplit, splitPoint, parent.getNumChildren(),
        affectedChildIndex, biNode, revision, pageKeyAllocator);
    return integrate(spineNodes, spineRefs, childSlots, parentDepth, cascaded, revision,
        pageKeyAllocator);
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
