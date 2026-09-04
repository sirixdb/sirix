/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.cache.FrameSlotAllocator;
import io.sirix.index.IndexType;
import io.sirix.index.hot.HOTIncrementalInsert.BiNode;
import io.sirix.index.hot.HOTMalformedSubtreeDetector.MalformedSubtree;
import io.sirix.index.hot.HOTMalformedSubtreeDetector.PageResolver;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HexFormat;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Verification of {@link HOTIncrementalInsert#splitIndirect} and
 * {@link HOTIncrementalInsert#addEntry} — step 2 of the faithful incremental port
 * ({@code docs/HOT_INCREMENTAL_PORT_PLAN.md} §3 step 2).
 *
 * <p>
 * The two primitives are exercised over canonical tries produced by {@link HOTBulkBuilder} (correct
 * by {@code HOT_FORMAL_FOUNDATION.md} Theorem 1). Each result is checked two ways:
 * <ul>
 * <li><b>structurally</b> — {@link HOTMalformedSubtreeDetector} reports zero malformed subtrees (I3
 * / I4 / I5 / I7 / I8 / I11);</li>
 * <li><b>by routing</b> — every key PEXT-descends from the rebuilt root to a leaf that actually
 * contains it, and the key multiset is exactly preserved (cross-check against the
 * {@code HOTBulkBuilder} input — {@code HOT_INCREMENTAL_SPLIT_VERIFICATION.md} Theorem IV
 * corollary).</li>
 * </ul>
 *
 * <p>
 * Both the SingleMask layout (8-byte keys) and the MultiMask layout (40-byte keys, disc bits
 * spanning &gt; 8 bytes) are covered — asserted explicitly so a coverage regression fails.
 */
@DisplayName("HOTIncrementalInsert.splitIndirect / addEntry — faithful indirect-node split")
final class HOTIndirectPageSplitFaithfulTest {

  /** Resolver for in-memory (swizzled) pages. */
  private static final PageResolver RESOLVER = PageReference::getPage;

  /** A non-tombstone {@code NodeReferences} value (node key {@code 7}) shared by every entry. */
  private static final byte[] VALUE = nodeRef(7L);

  // ======================================================================
  // splitIndirect.
  // ======================================================================

  @Test
  @DisplayName("splitting a canonical indirect root yields two clean, correctly-routing halves")
  void splitIndirectProducesCleanHalves() {
    int checked = 0;
    boolean sawSingleMask = false;
    boolean sawMultiMask = false;
    for (final Workload workload : Workload.values()) {
      for (final int size : workload.sizes) {
        for (int seed = 0; seed < 3; seed++) {
          final AtomicLong allocator = new AtomicLong(1);
          final List<byte[]> keys = workload.generate(size, seed);
          final HOTBulkBuilder.BuildResult built =
              HOTBulkBuilder.build(entries(keys), 1, IndexType.CAS, allocator::getAndIncrement);
          if (!(built.rootPage() instanceof HOTIndirectPage root)) {
            closeLeavesOf(built.rootPage());
            continue;
          }
          if (root.getLayoutType() == HOTIndirectPage.LayoutType.MULTI_MASK) {
            sawMultiMask = true;
          } else {
            sawSingleMask = true;
          }
          final String label = workload + " size=" + size + " seed=" + seed;

          final BiNode split = HOTIncrementalInsert.splitIndirect(root, 1, allocator::getAndIncrement);
          final HOTIndirectPage materialized = materialize(split, allocator);
          final PageReference rootRef = swizzle(materialized);

          assertClean(rootRef, label);
          assertRoutesAll(rootRef, keys, label);
          assertEquals(hexSet(keys), collectKeys(materialized), label + ": split must preserve the exact key set");

          closeLeavesOf(materialized);
          checked++;
        }
      }
    }
    assertTrue(sawSingleMask, "no SingleMask root exercised — coverage gap");
    assertTrue(sawMultiMask, "no MultiMask root exercised — coverage gap");
    System.out.println("[splitIndirect] " + checked + " canonical roots split — clean");
  }

  @Test
  @DisplayName("a half can itself be split — stale disc bits are dropped across generations")
  void splitIndirectIsRepeatable() {
    int checked = 0;
    for (final Workload workload : Workload.values()) {
      final AtomicLong allocator = new AtomicLong(1);
      final List<byte[]> keys = workload.generate(workload.sizes[workload.sizes.length - 1], 0);
      final HOTBulkBuilder.BuildResult built =
          HOTBulkBuilder.build(entries(keys), 1, IndexType.CAS, allocator::getAndIncrement);
      if (!(built.rootPage() instanceof HOTIndirectPage root)) {
        closeLeavesOf(built.rootPage());
        continue;
      }
      final BiNode first = HOTIncrementalInsert.splitIndirect(root, 1, allocator::getAndIncrement);
      // Re-split whichever half is itself a multi-child compound node.
      for (final PageReference halfRef : List.of(first.left(), first.right())) {
        if (halfRef.getPage() instanceof HOTIndirectPage half && half.getNumChildren() >= 2) {
          final BiNode second = HOTIncrementalInsert.splitIndirect(half, 1, allocator::getAndIncrement);
          final HOTIndirectPage materialized = materialize(second, allocator);
          final PageReference rootRef = swizzle(materialized);
          final String label = workload + " re-split";
          assertClean(rootRef, label);
          assertRoutesAll(rootRef, collectKeyBytes(half), label);
          checked++;
        }
      }
      closeLeavesOf(materialize(first, allocator));
    }
    assertTrue(checked > 0, "no half was re-split — coverage gap");
    System.out.println("[splitIndirect] " + checked + " re-splits — clean");
  }

  // ======================================================================
  // addEntry.
  // ======================================================================

  @Test
  @DisplayName("addEntry folds a leaf-page split into a not-full node — clean, canonical")
  void addEntryIntegratesLeafSplit() {
    int checked = 0;
    boolean sawMultiMask = false;
    for (final Workload workload : Workload.values()) {
      for (final int size : new int[] {700, 5_000}) {
        final AtomicLong allocator = new AtomicLong(1);
        final List<byte[]> keys = workload.generate(size, 1);
        final HOTBulkBuilder.BuildResult built =
            HOTBulkBuilder.build(entries(keys), 1, IndexType.CAS, allocator::getAndIncrement);
        if (!(built.rootPage() instanceof HOTIndirectPage root) || root.getHeight() != 1) {
          closeLeavesOf(built.rootPage());
          continue; // need a height-1 root so the halves' children are leaf pages
        }
        // splitIndirect produces a not-full compound node with leaf children — the addEntry target.
        final BiNode split = HOTIncrementalInsert.splitIndirect(root, 1, allocator::getAndIncrement);
        final PageReference targetRef = split.left().getPage() instanceof HOTIndirectPage
            ? split.left()
            : split.right();
        final PageReference orphanHalf = targetRef == split.left()
            ? split.right()
            : split.left();
        if (!(targetRef.getPage() instanceof HOTIndirectPage target)) {
          continue;
        }
        if (target.getLayoutType() == HOTIndirectPage.LayoutType.MULTI_MASK) {
          sawMultiMask = true;
        }
        final int slot = firstMultiEntryLeafSlot(target);
        if (slot < 0) {
          closeLeavesOf(built.rootPage());
          continue;
        }
        final HOTLeafPage leaf = (HOTLeafPage) target.getChildReference(slot).getPage();
        final TreeSet<String> before = collectKeys(target);
        final String label = workload + " size=" + size;

        // splitLeafPage with an existing key OR-merges values — the key set is unchanged.
        final BiNode leafSplit = HOTIncrementalInsert.splitLeafPage(leaf, leaf.getKey(0), VALUE, 1, IndexType.CAS,
            allocator::getAndIncrement);
        // addEntry folds the split in canonically — straddling-sibling folds are off-path and
        // routing-correct (docs/HOT_STRADDLE_GUARD_REMOVAL_PLAN.md).
        final HOTIndirectPage integrated =
            HOTIncrementalInsert.addEntry(target, leafSplit, slot, 1, allocator::getAndIncrement);
        final PageReference rootRef = swizzle(integrated);
        assertEquals(target.getNumChildren() + 1, integrated.getNumChildren(),
            label + ": addEntry adds exactly one child");
        assertClean(rootRef, label);
        assertRoutesAll(rootRef, toByteList(before), label);
        assertEquals(before, collectKeys(integrated),
            label + ": addEntry of a duplicate-key leaf split must preserve the key set");
        closeAll(integrated, orphanHalf.getPage(), leaf);
        checked++;
      }
    }
    assertTrue(checked > 0, "no addEntry case ran — coverage gap");
    assertTrue(sawMultiMask, "no MultiMask addEntry target exercised — coverage gap");
    System.out.println("[addEntry] " + checked + " clean leaf-split integrations");
  }

  @Test
  @DisplayName("addEntry accepts a brand-new in-range key — clean, canonical")
  void addEntryAcceptsFreshKey() {
    int checked = 0;
    for (final int size : new int[] {700, 5_000, 12_000}) {
      final AtomicLong allocator = new AtomicLong(1);
      final List<byte[]> keys = Workload.RANDOM8.generate(size, 2);
      final HOTBulkBuilder.BuildResult built =
          HOTBulkBuilder.build(entries(keys), 1, IndexType.CAS, allocator::getAndIncrement);
      if (!(built.rootPage() instanceof HOTIndirectPage root) || root.getHeight() != 1) {
        closeLeavesOf(built.rootPage());
        continue;
      }
      final BiNode split = HOTIncrementalInsert.splitIndirect(root, 1, allocator::getAndIncrement);
      final PageReference targetRef = split.left().getPage() instanceof HOTIndirectPage
          ? split.left()
          : split.right();
      final PageReference orphanHalf = targetRef == split.left()
          ? split.right()
          : split.left();
      if (!(targetRef.getPage() instanceof HOTIndirectPage target)) {
        continue;
      }
      final int slot = firstMultiEntryLeafSlot(target);
      if (slot < 0) {
        closeAll(materialize(split, allocator));
        continue;
      }
      final HOTLeafPage leaf = (HOTLeafPage) target.getChildReference(slot).getPage();
      final byte[] freshKey = freshKeyInside(leaf);
      if (freshKey == null) {
        closeAll(materialize(split, allocator));
        continue;
      }
      final TreeSet<String> expected = collectKeys(target);
      expected.add(hex(freshKey));
      final String label = "fresh-key size=" + size;

      final BiNode leafSplit =
          HOTIncrementalInsert.splitLeafPage(leaf, freshKey, VALUE, 1, IndexType.CAS, allocator::getAndIncrement);
      // addEntry folds the split in canonically (docs/HOT_STRADDLE_GUARD_REMOVAL_PLAN.md).
      final HOTIndirectPage integrated =
          HOTIncrementalInsert.addEntry(target, leafSplit, slot, 1, allocator::getAndIncrement);
      final PageReference rootRef = swizzle(integrated);
      assertClean(rootRef, label);
      assertEquals(expected, collectKeys(integrated), label + ": the new key joins the node's key set");
      assertRoutesAll(rootRef, toByteList(expected), label);
      closeAll(integrated, orphanHalf.getPage(), leaf);
      checked++;
    }
    assertTrue(checked > 0, "no fresh-key addEntry case ran — coverage gap");
    System.out.println("[addEntry] " + checked + " clean fresh-key integrations");
  }

  @Test
  @DisplayName("addEntry folds a straddle-free leaf split cleanly")
  void addEntryFoldsAStraddleFreeSplit() {
    final AtomicLong allocator = new AtomicLong(1);
    // A 2-child node: a 2-key leaf to split plus a single-entry sibling. A one-key leaf is
    // constant on every bit, so it can never straddle the split bit — a minimal canonical
    // addEntry fold (docs/HOT_STRADDLE_GUARD_REMOVAL_PLAN.md).
    final HOTLeafPage splittable = new HOTLeafPage(allocator.getAndIncrement(), 1, IndexType.CAS);
    assertTrue(splittable.put(beKey(0L), VALUE));
    assertTrue(splittable.put(beKey(1L), VALUE));
    final HOTLeafPage singleton = new HOTLeafPage(allocator.getAndIncrement(), 1, IndexType.CAS);
    assertTrue(singleton.put(beKey(1L << 63), VALUE));
    // Disc bit 0 (the MSB) separates them — splittable's keys have it clear, the singleton's set.
    final HOTIndirectPage node =
        HOTIndirectPage.createBiNode(allocator.getAndIncrement(), 1, 0, swizzle(splittable), swizzle(singleton), 1);

    final BiNode leafSplit =
        HOTIncrementalInsert.splitLeafPage(splittable, beKey(0L), VALUE, 1, IndexType.CAS, allocator::getAndIncrement);
    final HOTIndirectPage integrated = HOTIncrementalInsert.addEntry(node, leafSplit, 0, 1, allocator::getAndIncrement);
    final PageReference rootRef = swizzle(integrated);

    assertEquals(3, integrated.getNumChildren(), "the fold adds one child to the 2-child node");
    assertClean(rootRef, "straddle-free-fold");
    assertRoutesAll(rootRef, List.of(beKey(0L), beKey(1L), beKey(1L << 63)), "straddle-free-fold");
    closeAll(integrated, splittable);
    System.out.println("[addEntry] straddle-free leaf split folded cleanly");
  }

  // ======================================================================
  // splitIndirectWithEntry — Binna's full-node split (split + insert fused).
  // ======================================================================

  @Test
  @DisplayName("splitIndirectWithEntry splits a node and inserts a branch key — clean, routing-correct")
  void splitIndirectWithEntryProducesCleanResult() {
    int checked = 0;
    for (final Workload workload : Workload.values()) {
      for (final int size : workload.sizes) {
        final AtomicLong allocator = new AtomicLong(1);
        final List<byte[]> keys = workload.generate(size, 7);
        final HOTBulkBuilder.BuildResult built =
            HOTBulkBuilder.build(entries(keys), 1, IndexType.CAS, allocator::getAndIncrement);
        if (!(built.rootPage() instanceof HOTIndirectPage node) || node.getNumChildren() < 2) {
          closeLeavesOf(built.rootPage());
          continue;
        }
        final int[] discBits = HOTIncrementalInsert.discriminativeBits(node);
        if (discBits.length < 2) {
          closeLeavesOf(built.rootPage());
          continue;
        }
        final Set<String> present = hexSet(keys);
        final List<HOTLeafPage> freshLeaves = new ArrayList<>();
        // Direct construction: a branch key K is an existing key (from child entryIndex's
        // subtree) with exactly bit beta flipped. K agrees with that subtree above beta and
        // differs at beta — a genuine, consistent branch at this node. beta is a bit strictly
        // between two of the node's discriminative bits: genuinely new to the node (the
        // splitIndirectWithEntry precondition), so the affected subtree is one-sided on it.
        for (int entryIndex = 0; entryIndex < node.getNumChildren(); entryIndex++) {
          final byte[] baseKey = firstKeyOf(node.getChildReference(entryIndex).getPage());
          if (baseKey == null) {
            continue;
          }
          for (int d = 0; d + 1 < discBits.length; d++) {
            if (discBits[d + 1] - discBits[d] < 2) {
              continue; // adjacent disc bits — no bit strictly between them
            }
            final int beta = (discBits[d] + discBits[d + 1]) >>> 1;
            final byte[] key = flipBit(baseKey, beta);
            if (present.contains(hex(key))) {
              continue;
            }
            final HOTIncrementalInsert.InsertInfo info =
                HOTIncrementalInsert.getInsertInformation(node, entryIndex, beta);
            if (info.affectedCount() == node.getNumChildren()) {
              continue; // affected subtree is the whole node — pull-up, not split
            }
            final int betaValue = HOTBulkBuilder.bitAt(key, beta)
                ? 1
                : 0;
            final HOTLeafPage freshLeaf = new HOTLeafPage(allocator.getAndIncrement(), 1, IndexType.CAS);
            assertTrue(freshLeaf.put(key, VALUE), "fresh single-entry leaf must accept the key");
            freshLeaves.add(freshLeaf);

            final BiNode biNode = HOTIncrementalInsert.splitIndirectWithEntry(node, info, beta, betaValue,
                swizzle(freshLeaf), 1, allocator::getAndIncrement);
            final HOTIndirectPage materialized = materialize(biNode, allocator);
            final PageReference rootRef = swizzle(materialized);
            final String label = workload + " size=" + size + " e=" + entryIndex + " beta=" + beta;

            final TreeSet<String> expected = collectKeys(node);
            expected.add(hex(key));
            assertEquals(expected, collectKeys(materialized),
                label + ": split-with-entry must yield the node's keys plus the new key");
            assertClean(rootRef, label);
            assertRoutesAll(rootRef, toByteList(expected), label);
            checked++;
          }
        }
        closeLeavesOf(built.rootPage());
        for (final HOTLeafPage freshLeaf : freshLeaves) {
          freshLeaf.close();
        }
      }
    }
    assertTrue(checked > 0, "no splitIndirectWithEntry case exercised — coverage gap");
    System.out.println("[splitIndirectWithEntry] " + checked + " full-node splits — clean");
  }

  /** The first key of {@code page}'s subtree (descends to its leftmost leaf), or {@code null}. */
  private static byte[] firstKeyOf(final Page page) {
    Page current = page;
    while (current instanceof HOTIndirectPage indirect) {
      current = indirect.getChildReference(0).getPage();
    }
    return current instanceof HOTLeafPage leaf && leaf.getEntryCount() > 0
        ? leaf.getKey(0)
        : null;
  }

  /** A copy of {@code key} with the absolute, MSB-first bit {@code beta} flipped. */
  private static byte[] flipBit(final byte[] key, final int beta) {
    final byte[] out = key.clone();
    out[beta >>> 3] ^= (byte) (1 << (7 - (beta & 7)));
    return out;
  }

  // ======================================================================
  // mergeBiNodePairedLeaves — incremental leaf consolidation (inverse of a leaf split).
  // ======================================================================

  @Test
  @DisplayName("repeated consolidation retires unpublished intermediates and reports only original leaves")
  void repeatedConsolidationOwnsIntermediateLeaves() {
    final AtomicLong allocator = new AtomicLong(1);
    final ConsolidationChain chain = consolidationChain(allocator, IndexType.CAS);
    final FrameSlotAllocator frameAllocator = FrameSlotAllocator.getInstance();
    final int frameClass = FrameSlotAllocator.indexForSize(HOTLeafPage.DEFAULT_SIZE);
    final int liveBefore = frameAllocator.liveSlotCount(frameClass);
    final List<PageReference> dropped = new ArrayList<>();

    final HOTIndirectPage consolidated = HOTIncrementalInsert.consolidateNodeLeaves(chain.parent, 4, 2, IndexType.CAS,
        allocator::getAndIncrement, dropped);
    try {
      assertEquals(2, consolidated.getNumChildren(), "the three-leaf left chain must collapse twice");
      assertEquals(List.of(chain.references[0], chain.references[1], chain.references[2]), dropped,
          "only original/loggable leaves may be handed to TransactionIntentLog retirement");
      assertEquals(liveBefore + 1, frameAllocator.liveSlotCount(frameClass),
          "only the final replacement may remain live; the first unpublished merge must be closed");
      assertEquals(collectKeys(chain.parent), collectKeys(consolidated));
      assertClean(swizzle(consolidated), "repeated consolidation ownership");
    } finally {
      closeAll(chain.parent, consolidated);
    }
  }

  @Test
  @DisplayName("a later consolidation allocation failure retires every unpublished replacement")
  void repeatedConsolidationFailureClosesIntermediates() {
    final AtomicLong fixtureAllocator = new AtomicLong(1);
    final ConsolidationChain chain = consolidationChain(fixtureAllocator, IndexType.CAS);
    final FrameSlotAllocator frameAllocator = FrameSlotAllocator.getInstance();
    final int frameClass = FrameSlotAllocator.indexForSize(HOTLeafPage.DEFAULT_SIZE);
    final int liveBefore = frameAllocator.liveSlotCount(frameClass);
    final AtomicLong allocationCalls = new AtomicLong();
    final IllegalStateException sentinel = new IllegalStateException("injected second-merge parent allocation");
    final List<PageReference> dropped = new ArrayList<>();

    try {
      final IllegalStateException failure = assertThrows(IllegalStateException.class,
          () -> HOTIncrementalInsert.consolidateNodeLeaves(chain.parent, 4, 2, IndexType.CAS, () -> {
            final long call = allocationCalls.incrementAndGet();
            if (call == 4) {
              throw sentinel;
            }
            return 10_000 + call;
          }, dropped));

      assertSame(sentinel, failure);
      assertEquals(4, allocationCalls.get(), "the fault must occur after building the first merged leaf");
      assertTrue(dropped.isEmpty(), "a failed pure consolidation must not transfer source ownership");
      assertEquals(liveBefore, frameAllocator.liveSlotCount(frameClass),
          "the first intermediate and the second in-flight leaf must both be retired");
    } finally {
      closeAll(chain.parent);
    }
  }

  @Test
  @DisplayName("projection side references make a leaf pair ineligible for consolidation")
  void consolidationPreservesProjectionSideReferencesBySkippingPair() {
    final AtomicLong allocator = new AtomicLong(1);
    final ConsolidationChain chain = consolidationChain(allocator, IndexType.PROJECTION);
    final PageReference segmentReference = new PageReference();
    final long segmentKey = HOTLeafPage.overflowPageRefKey(0L, 0);
    chain.leaves[0].setPageReference(segmentKey, segmentReference);
    final List<PageReference> dropped = new ArrayList<>();

    try {
      final HOTIndirectPage consolidated = HOTIncrementalInsert.consolidateNodeLeaves(chain.parent, 4, 2,
          IndexType.PROJECTION, allocator::getAndIncrement, dropped);

      assertSame(chain.parent, consolidated,
          "key/value-only consolidation must not consume a projection leaf with side references");
      assertTrue(dropped.isEmpty(), "the caller must have no projection leaves to retire");
      assertEquals(1, chain.leaves[0].segmentRefCount());
      assertSame(segmentReference, chain.leaves[0].getPageReference(segmentKey));
    } finally {
      closeAll(chain.parent);
    }
  }

  @Test
  @DisplayName("projection consolidation preserves a zero-length opaque value without mutating its source")
  void projectionConsolidationPreservesZeroLengthValue() {
    final AtomicLong allocator = new AtomicLong(1);
    final HOTLeafPage[] leaves = {projectionLeaf(allocator, new byte[] {0x00}, new byte[0]),
        projectionLeaf(allocator, new byte[] {0x20}, new byte[] {(byte) 0xA5}),
        projectionLeaf(allocator, new byte[] {0x40}, new byte[] {0x12, 0x34}),
        projectionLeaf(allocator, new byte[] {(byte) 0x80}, new byte[] {(byte) 0xFF})};
    final PageReference[] references = {swizzle(leaves[0]), swizzle(leaves[1]), swizzle(leaves[2]), swizzle(leaves[3])};
    final HOTIndirectPage parent = HOTBulkBuilder.assembleIndirect(new int[] {0, 1, 2}, new int[] {0, 1, 2, 4},
        references, 1, 1, allocator::getAndIncrement);
    final List<PageReference> dropped = new ArrayList<>();
    HOTIndirectPage consolidated = null;

    try {
      consolidated = HOTIncrementalInsert.consolidateNodeLeaves(parent, 4, 2, IndexType.PROJECTION,
          allocator::getAndIncrement, dropped);

      assertEquals(2, consolidated.getNumChildren(), "the mergeable projection chain must consolidate");
      assertArrayEquals(new byte[0], valueFor(consolidated, new byte[] {0x00}));
      assertArrayEquals(new byte[] {(byte) 0xA5}, valueFor(consolidated, new byte[] {0x20}));
      assertEquals(1, leaves[0].getEntryCount(), "pure consolidation must not mutate the source leaf");
      assertEquals(0, HOTLeafPage.refLength(leaves[0].valueRef(0)),
          "the source's zero-length value must remain intact");
      assertEquals(List.of(references[0], references[1], references[2]), dropped,
          "only original leaves may transfer to transaction-log retirement");
    } finally {
      closeAll(parent, consolidated);
    }
  }

  @Test
  @DisplayName("mergeBiNodePairedLeaves collapses two adjacent leaves — clean, key-set-preserving")
  void mergeBiNodePairedLeavesProducesCleanResult() {
    int checked = 0;
    boolean sawCompoundResult = false;
    // A fresh bulk build can no longer contain a mergeable BiNode pair BY CONSTRUCTION — the
    // highest-fitting-subtree cut already keeps every page-fitting R(S) subtree whole. The
    // over-partitioned shape the merge exists for is produced by the INCREMENTAL path, so the
    // scenario manufactures it the production way: splitLeafPage a canonical leaf (re-inserting
    // an existing key keeps the key set), fold the halves back in via addEntry, and merge the
    // resulting BiNode-paired pair — the exact split-then-consolidate flow of the live writer.
    for (final int size : new int[] {700, 1_400}) {
      final AtomicLong allocator = new AtomicLong(1);
      final List<byte[]> keys = Workload.WIDE_SPAN.generate(size, 3);
      final HOTBulkBuilder.BuildResult built =
          HOTBulkBuilder.build(entries(keys), 1, IndexType.CAS, allocator::getAndIncrement);
      if (!(built.rootPage() instanceof HOTIndirectPage canonical) || canonical.getHeight() != 1) {
        closeLeavesOf(built.rootPage());
        continue;
      }
      HOTIndirectPage root = canonical;
      for (int slot = 0; slot < root.getNumChildren(); slot++) {
        if (root.getChildReference(slot).getPage() instanceof HOTLeafPage splittable && splittable.getEntryCount() >= 2
            && Arrays.binarySearch(HOTIncrementalInsert.discriminativeBits(root),
                HOTBulkBuilder.msdb(splittable.getKey(0), splittable.getKey(splittable.getEntryCount() - 1))) < 0) {
          final HOTIncrementalInsert.BiNode leafSplit = HOTIncrementalInsert.splitLeafPage(splittable,
              splittable.getKey(0), VALUE, 1, IndexType.CAS, allocator::getAndIncrement);
          root = HOTIncrementalInsert.addEntry(root, leafSplit, slot, 1, allocator::getAndIncrement);
          break;
        }
      }
      final TreeSet<String> rootKeys = collectKeys(root);
      final List<HOTLeafPage> mergedLeaves = new ArrayList<>();
      for (int i = 0; i + 1 < root.getNumChildren(); i++) {
        if (!HOTIncrementalInsert.areBiNodePaired(root, i)) {
          continue;
        }
        if (!(root.getChildReference(i).getPage() instanceof HOTLeafPage left)
            || !(root.getChildReference(i + 1).getPage() instanceof HOTLeafPage right)) {
          continue;
        }
        final HOTLeafPage mergedLeaf = new HOTLeafPage(allocator.getAndIncrement(), 1, IndexType.CAS);
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
        mergedLeaves.add(mergedLeaf);

        final PageReference mergedRef =
            HOTIncrementalInsert.mergeBiNodePairedLeaves(root, i, mergedLeaf, 1, allocator::getAndIncrement);
        final String label = "WIDE_SPAN size=" + size + " pair=" + i;
        if (mergedRef.getPage() instanceof HOTIndirectPage) {
          sawCompoundResult = true;
        }
        assertEquals(rootKeys, collectKeys(mergedRef.getPage()),
            label + ": a leaf merge must preserve the exact key set");
        assertClean(mergedRef, label);
        assertRoutesAll(mergedRef, toByteList(rootKeys), label);
        checked++;
      }
      // The folded root holds the split halves (fresh pages the canonical tree never saw);
      // close() is idempotent, so the shared canonical leaves tolerate the double visit.
      closeLeavesOf(root);
      closeLeavesOf(built.rootPage());
      for (final HOTLeafPage mergedLeaf : mergedLeaves) {
        mergedLeaf.close();
      }
    }
    assertTrue(checked > 0, "no mergeBiNodePairedLeaves case exercised — coverage gap");
    assertTrue(sawCompoundResult, "no compound-node merge result exercised — coverage gap");
    System.out.println("[mergeBiNodePairedLeaves] " + checked + " leaf merges — clean");
  }

  @Test
  @DisplayName("adjacent-pair replacement drops a parent discriminator that becomes constant")
  void replaceAdjacentPairRecompressesDeadParentColumn() {
    final AtomicLong allocator = new AtomicLong(1);
    final byte[] key00 = {(byte) 0x00};
    final byte[] key01 = {(byte) 0x40};
    final byte[] key10 = {(byte) 0x80};
    final HOTLeafPage leaf00 = leaf(allocator, key00);
    final HOTLeafPage leaf01 = leaf(allocator, key01);
    final HOTLeafPage leaf10 = leaf(allocator, key10);
    final PageReference ref00 = swizzle(leaf00);
    final PageReference ref01 = swizzle(leaf01);
    final PageReference ref10 = swizzle(leaf10);

    // Sparse block-trie entries 00, 01, 10 over parent bits [a=0,b=1]. Collapsing the first
    // BiNode pair removes 01 from the parent. Column b is then constant zero in the surviving
    // old-coordinate entries [00,10] and must disappear; it remains discriminative inside mini.
    final HOTIndirectPage parent = HOTBulkBuilder.assembleIndirect(new int[] {0, 1}, new int[] {0, 1, 2},
        new PageReference[] {ref00, ref01, ref10}, 1, 1, allocator::getAndIncrement);
    final HOTIndirectPage mini = HOTBulkBuilder.assembleIndirect(new int[] {1}, new int[] {0, 1},
        new PageReference[] {ref00, ref01}, 1, 1, allocator::getAndIncrement);

    final PageReference resultRef =
        HOTIncrementalInsert.replaceAdjacentPairAndCompress(parent, 0, swizzle(mini), 1, allocator::getAndIncrement);
    assertTrue(resultRef.getPage() instanceof HOTIndirectPage);
    final HOTIndirectPage result = (HOTIndirectPage) resultRef.getPage();
    assertArrayEquals(new int[] {0}, HOTIncrementalInsert.discriminativeBits(result));
    assertArrayEquals(new int[] {0, 1}, result.getPartialKeysRef());
    assertSame(mini, result.getChildReference(0).getPage());
    assertEquals(0, result.findChildIndex(key00));
    assertEquals(0, result.findChildIndex(key01));
    assertEquals(1, result.findChildIndex(key10));
    assertClean(resultRef, "dead parent discriminator recompression");
    assertRoutesAll(resultRef, List.of(key00, key01, key10), "dead parent discriminator recompression");

    closeAll(parent, result);
  }

  @Test
  @DisplayName("minimal complete range expands a non-sibling adjacent pair to three leaves")
  void replaceMinimalCompleteThreeLeafRange() {
    final AtomicLong allocator = new AtomicLong(1);
    final byte[] key000 = {(byte) 0x00};
    final byte[] key010 = {(byte) 0x40};
    final byte[] key011 = {(byte) 0x60};
    final byte[] key100 = {(byte) 0x80};
    final HOTLeafPage leaf000 = leaf(allocator, key000);
    final HOTLeafPage leaf010 = leaf(allocator, key010);
    final HOTLeafPage leaf011 = leaf(allocator, key011);
    final HOTLeafPage leaf100 = leaf(allocator, key100);
    final PageReference ref000 = swizzle(leaf000);
    final PageReference ref010 = swizzle(leaf010);
    final PageReference ref011 = swizzle(leaf011);
    final PageReference ref100 = swizzle(leaf100);

    // In sparse entries [000,010,011,100], adjacent 000/010 are not siblings: 010 shares a
    // deeper BiNode with 011. Their smallest complete flattened range is therefore the first
    // three children, rooted at parent column b.
    final HOTIndirectPage parent = HOTBulkBuilder.assembleIndirect(new int[] {0, 1, 2}, new int[] {0, 2, 3, 4},
        new PageReference[] {ref000, ref010, ref011, ref100}, 1, 1, allocator::getAndIncrement);
    final HOTIncrementalInsert.ChildRange range = HOTIncrementalInsert.minimalBiNodeRangeContaining(parent, 0, 1);
    assertEquals(0, range.fromInclusive());
    assertEquals(3, range.toExclusive());

    final HOTIndirectPage mini = HOTBulkBuilder.assembleIndirect(new int[] {1, 2}, new int[] {0, 2, 3},
        new PageReference[] {ref000, ref010, ref011}, 1, 1, allocator::getAndIncrement);
    final PageReference miniRef = swizzle(mini);
    assertThrows(IllegalArgumentException.class,
        () -> HOTIncrementalInsert.replaceChildRangeAndCompress(parent, 0, 2, miniRef, 1, allocator::getAndIncrement));

    final PageReference resultRef = HOTIncrementalInsert.replaceChildRangeAndCompress(parent, range.fromInclusive(),
        range.toExclusive(), miniRef, 1, allocator::getAndIncrement);
    assertTrue(resultRef.getPage() instanceof HOTIndirectPage);
    final HOTIndirectPage result = (HOTIndirectPage) resultRef.getPage();
    assertArrayEquals(new int[] {0}, HOTIncrementalInsert.discriminativeBits(result));
    assertArrayEquals(new int[] {0, 1}, result.getPartialKeysRef());
    assertSame(mini, result.getChildReference(0).getPage());
    assertClean(resultRef, "three-leaf complete frontier");
    assertRoutesAll(resultRef, List.of(key000, key010, key011, key100), "three-leaf complete frontier");

    closeAll(parent, result);
  }

  @Test
  @DisplayName("a complete frontier spanning the parent pulls its mini-root up by identity")
  void replaceWholeParentFrontierPullsUpReplacement() {
    final AtomicLong allocator = new AtomicLong(1);
    final byte[] key000 = {(byte) 0x00};
    final byte[] key010 = {(byte) 0x40};
    final byte[] key011 = {(byte) 0x60};
    final HOTLeafPage leaf000 = leaf(allocator, key000);
    final HOTLeafPage leaf010 = leaf(allocator, key010);
    final HOTLeafPage leaf011 = leaf(allocator, key011);
    final PageReference ref000 = swizzle(leaf000);
    final PageReference ref010 = swizzle(leaf010);
    final PageReference ref011 = swizzle(leaf011);
    final HOTIndirectPage parent = HOTBulkBuilder.assembleIndirect(new int[] {1, 2}, new int[] {0, 2, 3},
        new PageReference[] {ref000, ref010, ref011}, 1, 1, allocator::getAndIncrement);
    assertClean(swizzle(parent), "whole-parent frontier source");
    final HOTIncrementalInsert.ChildRange range = HOTIncrementalInsert.minimalBiNodeRangeContaining(parent, 0, 1);
    assertEquals(0, range.fromInclusive());
    assertEquals(parent.getNumChildren(), range.toExclusive());

    final HOTIndirectPage mini = HOTBulkBuilder.assembleIndirect(new int[] {1, 2}, new int[] {0, 2, 3},
        new PageReference[] {ref000, ref010, ref011}, 1, 1, allocator::getAndIncrement);
    final PageReference miniRef = swizzle(mini);
    final PageReference resultRef = HOTIncrementalInsert.replaceChildRangeAndCompress(parent, range.fromInclusive(),
        range.toExclusive(), miniRef, 1, allocator::getAndIncrement);

    assertSame(miniRef, resultRef, "a lone surviving range root must be pulled up without a wrapper");
    assertClean(resultRef, "whole-parent frontier pull-up");
    assertRoutesAll(resultRef, List.of(key000, key010, key011), "whole-parent frontier pull-up");

    closeAll(parent, mini);
  }

  @Test
  @DisplayName("a leaf split's two halves are BiNode-paired and merge back — round-trip")
  void mergeUndoesLeafSplit() {
    int checked = 0;
    for (final Workload workload : Workload.values()) {
      for (final int size : new int[] {700, 5_000}) {
        final AtomicLong allocator = new AtomicLong(1);
        final List<byte[]> keys = workload.generate(size, 1);
        final HOTBulkBuilder.BuildResult built =
            HOTBulkBuilder.build(entries(keys), 1, IndexType.CAS, allocator::getAndIncrement);
        if (!(built.rootPage() instanceof HOTIndirectPage root) || root.getHeight() != 1) {
          closeLeavesOf(built.rootPage());
          continue; // need a height-1 root so the halves' children are leaf pages
        }
        // splitIndirect yields a not-full compound node with leaf children — addEntry's target,
        // guaranteed below capacity (it holds one half of the original root's children).
        final BiNode split = HOTIncrementalInsert.splitIndirect(root, 1, allocator::getAndIncrement);
        final PageReference targetRef = split.left().getPage() instanceof HOTIndirectPage
            ? split.left()
            : split.right();
        final PageReference orphanHalf = targetRef == split.left()
            ? split.right()
            : split.left();
        if (!(targetRef.getPage() instanceof HOTIndirectPage target)) {
          continue;
        }
        final int slot = firstMultiEntryLeafSlot(target);
        if (slot < 0) {
          closeLeavesOf(built.rootPage());
          continue;
        }
        final HOTLeafPage leaf = (HOTLeafPage) target.getChildReference(slot).getPage();
        final TreeSet<String> targetKeys = collectKeys(target);
        final String label = workload + " size=" + size;

        // Split the leaf (re-inserting an existing key keeps the key set) and fold the BiNode in.
        final BiNode leafSplit = HOTIncrementalInsert.splitLeafPage(leaf, leaf.getKey(0), VALUE, 1, IndexType.CAS,
            allocator::getAndIncrement);
        final HOTIndirectPage afterSplit =
            HOTIncrementalInsert.addEntry(target, leafSplit, slot, 1, allocator::getAndIncrement);
        // The halves carry the split slot's lower discriminative bits, yet they ARE a BiNode pair
        // — the depth-based test must recognize that (the one-bit-difference test alone failed).
        assertTrue(HOTIncrementalInsert.areBiNodePaired(afterSplit, slot),
            label + ": a leaf split's two halves must be detected as BiNode-paired");

        final HOTLeafPage left = (HOTLeafPage) afterSplit.getChildReference(slot).getPage();
        final HOTLeafPage right = (HOTLeafPage) afterSplit.getChildReference(slot + 1).getPage();
        final HOTLeafPage mergedLeaf = new HOTLeafPage(allocator.getAndIncrement(), 1, IndexType.CAS);
        for (int e = 0; e < left.getEntryCount(); e++) {
          assertTrue(mergedLeaf.put(left.getKey(e), left.getValue(e)));
        }
        for (int e = 0; e < right.getEntryCount(); e++) {
          assertTrue(mergedLeaf.put(right.getKey(e), right.getValue(e)));
        }
        final PageReference mergedRef =
            HOTIncrementalInsert.mergeBiNodePairedLeaves(afterSplit, slot, mergedLeaf, 1, allocator::getAndIncrement);
        assertEquals(targetKeys, collectKeys(mergedRef.getPage()),
            label + ": split then merge must round-trip to the original key set");
        assertClean(mergedRef, label);
        assertRoutesAll(mergedRef, toByteList(targetKeys), label);
        checked++;

        closeAll(afterSplit, mergedRef.getPage(), mergedLeaf, orphanHalf.getPage(), leaf);
      }
    }
    assertTrue(checked > 0, "no leaf-split round-trip exercised — coverage gap");
    System.out.println("[mergeUndoesLeafSplit] " + checked + " clean round-trips");
  }

  // ======================================================================
  // Key workloads.
  // ======================================================================

  /** Adversarial key workloads — chosen to exercise both the SingleMask and MultiMask layouts. */
  private enum Workload {
    /** 8-byte big-endian {@code 0..n-1} — disc bits in the low bytes, SingleMask. */
    ASCENDING8(new int[] {600, 5_000, 20_000}),
    /** 8-byte big-endian random longs — disc bits across all 8 bytes, SingleMask. */
    RANDOM8(new int[] {600, 5_000, 20_000}),
    /**
     * 40-byte keys: an 8-value byte-0 prefix plus a far-out random tail at bytes 30-39 (bytes 1-29 are
     * zero). The root block exhausts the 7 byte-0 BiNodes then must reach the byte-30 tail to fill its
     * 32 children — disc bits span &gt; 8 bytes, forcing the MultiMask layout.
     */
    WIDE_SPAN(new int[] {700, 4_000});

    private final int[] sizes;

    Workload(final int[] sizes) {
      this.sizes = sizes;
    }

    List<byte[]> generate(final int size, final int seed) {
      final Random random = new Random((ordinal() * 0x9E3779B9L) ^ (size * 131L) ^ seed);
      return switch (this) {
        case ASCENDING8 -> {
          final List<byte[]> keys = new ArrayList<>(size);
          for (long i = 0; i < size; i++) {
            keys.add(beKey(i));
          }
          yield keys; // already strictly ascending
        }
        case RANDOM8 -> {
          final TreeSet<Long> set = new TreeSet<>(Long::compareUnsigned);
          while (set.size() < size) {
            set.add(random.nextLong());
          }
          final List<byte[]> keys = new ArrayList<>(size);
          for (final long k : set) {
            keys.add(beKey(k));
          }
          yield keys;
        }
        case WIDE_SPAN -> {
          final TreeSet<byte[]> set = new TreeSet<>(Arrays::compareUnsigned);
          while (set.size() < size) {
            final byte[] key = new byte[40];
            key[0] = (byte) random.nextInt(8); // 8 top-level byte-0 groups
            for (int i = 30; i < 40; i++) { // far-out random tail; bytes 1-29 zero
              key[i] = (byte) random.nextInt(256);
            }
            set.add(key);
          }
          yield new ArrayList<>(set);
        }
      };
    }
  }

  // ======================================================================
  // Verification helpers.
  // ======================================================================

  /** Assert the trie rooted at {@code rootRef} has zero malformed subtrees. */
  private static void assertClean(final PageReference rootRef, final String label) {
    final List<MalformedSubtree> malformed = HOTMalformedSubtreeDetector.detect(rootRef, RESOLVER);
    assertTrue(malformed.isEmpty(), label + ": malformed subtree(s) " + malformed);
  }

  /** Assert every key PEXT-descends from {@code rootRef} to a leaf that contains it. */
  private static void assertRoutesAll(final PageReference rootRef, final List<byte[]> keys, final String label) {
    for (final byte[] key : keys) {
      Page page = rootRef.getPage();
      int depth = 0;
      while (page instanceof HOTIndirectPage indirect) {
        if (++depth > 64) {
          fail(label + ": descent for " + hex(key) + " exceeded depth 64");
        }
        final int childIndex = indirect.findChildIndex(key);
        assertTrue(childIndex >= 0, label + ": routing returned NOT_FOUND for " + hex(key));
        page = indirect.getChildReference(childIndex).getPage();
      }
      assertTrue(page instanceof HOTLeafPage, label + ": descent for " + hex(key) + " did not reach a leaf");
      final HOTLeafPage leaf = (HOTLeafPage) page;
      assertTrue(leaf.findEntry(key) >= 0,
          label + ": " + hex(key) + " routed to leaf " + leaf.getPageKey() + " without it");
    }
  }

  /** Materialize a virtual {@link BiNode} as a standalone 2-entry compound node. */
  private static HOTIndirectPage materialize(final BiNode biNode, final AtomicLong allocator) {
    return HOTIndirectPage.createBiNode(allocator.getAndIncrement(), 1, biNode.discriminativeBitIndex(), biNode.left(),
        biNode.right(), biNode.height());
  }

  /**
   * The first slot of a {@code >= 2}-entry (so splittable) leaf child of {@code node}, or {@code -1}
   * if none.
   */
  private static int firstMultiEntryLeafSlot(final HOTIndirectPage node) {
    for (int i = 0; i < node.getNumChildren(); i++) {
      if (node.getChildReference(i).getPage() instanceof HOTLeafPage leaf && leaf.getEntryCount() >= 2) {
        return i;
      }
    }
    return -1;
  }

  /** A key strictly between two adjacent keys of {@code leaf}, or {@code null} if none exists. */
  private static byte[] freshKeyInside(final HOTLeafPage leaf) {
    for (int i = 1; i < leaf.getEntryCount(); i++) {
      final long lo = decodeBe(leaf.getKey(i - 1));
      final long hi = decodeBe(leaf.getKey(i));
      final long mid = lo + ((hi - lo) >>> 1);
      if (Long.compareUnsigned(lo, mid) < 0 && Long.compareUnsigned(mid, hi) < 0) {
        return beKey(mid);
      }
    }
    return null;
  }

  // ======================================================================
  // Key / page utilities.
  // ======================================================================

  private static List<HOTBulkBuilder.Entry> entries(final List<byte[]> sortedKeys) {
    final List<HOTBulkBuilder.Entry> entries = new ArrayList<>(sortedKeys.size());
    for (final byte[] key : sortedKeys) {
      entries.add(new HOTBulkBuilder.Entry(key, VALUE));
    }
    return entries;
  }

  private static HOTLeafPage leaf(final AtomicLong allocator, final byte[] key) {
    return leaf(allocator, key, IndexType.CAS);
  }

  private static HOTLeafPage leaf(final AtomicLong allocator, final byte[] key, final IndexType indexType) {
    final HOTLeafPage leaf = new HOTLeafPage(allocator.getAndIncrement(), 1, indexType);
    assertTrue(leaf.put(key, VALUE));
    return leaf;
  }

  private static HOTLeafPage projectionLeaf(final AtomicLong allocator, final byte[] key, final byte[] value) {
    final HOTLeafPage leaf = new HOTLeafPage(allocator.getAndIncrement(), 1, IndexType.PROJECTION);
    assertTrue(leaf.put(key, value));
    return leaf;
  }

  private static ConsolidationChain consolidationChain(final AtomicLong allocator, final IndexType indexType) {
    final HOTLeafPage[] leaves =
        {leaf(allocator, new byte[] {0x00}, indexType), leaf(allocator, new byte[] {0x20}, indexType),
            leaf(allocator, new byte[] {0x40}, indexType), leaf(allocator, new byte[] {(byte) 0x80}, indexType)};
    final PageReference[] references = {swizzle(leaves[0]), swizzle(leaves[1]), swizzle(leaves[2]), swizzle(leaves[3])};
    final HOTIndirectPage parent = HOTBulkBuilder.assembleIndirect(new int[] {0, 1, 2}, new int[] {0, 1, 2, 4},
        references, 1, 1, allocator::getAndIncrement);
    return new ConsolidationChain(parent, leaves, references);
  }

  private record ConsolidationChain(HOTIndirectPage parent, HOTLeafPage[] leaves, PageReference[] references) {
  }

  private static TreeSet<String> collectKeys(final Page page) {
    final TreeSet<String> keys = new TreeSet<>();
    collectKeysInto(page, keys);
    return keys;
  }

  private static byte[] valueFor(final Page root, final byte[] key) {
    Page page = root;
    while (page instanceof HOTIndirectPage indirect) {
      final int childIndex = indirect.findChildIndex(key);
      assertTrue(childIndex >= 0, "projection key must route through the consolidated trie");
      page = indirect.getChildReference(childIndex).getPage();
    }
    assertTrue(page instanceof HOTLeafPage, "projection key must route to a leaf");
    final HOTLeafPage leaf = (HOTLeafPage) page;
    final int entryIndex = leaf.findEntry(key);
    assertTrue(entryIndex >= 0, "projection key must survive consolidation");
    final long valueRef = leaf.valueRef(entryIndex);
    final int valueLength = HOTLeafPage.refLength(valueRef);
    final byte[] value = new byte[valueLength];
    if (valueLength > 0) {
      leaf.copyRefInto(valueRef, 0, value, 0, valueLength);
    }
    return value;
  }

  private static void collectKeysInto(final Page page, final TreeSet<String> out) {
    if (page instanceof HOTLeafPage leaf) {
      for (int i = 0; i < leaf.getEntryCount(); i++) {
        out.add(hex(leaf.getKey(i)));
      }
    } else if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        collectKeysInto(indirect.getChildReference(i).getPage(), out);
      }
    }
  }

  private static List<byte[]> collectKeyBytes(final Page page) {
    final List<byte[]> keys = new ArrayList<>();
    collectKeyBytesInto(page, keys);
    return keys;
  }

  private static void collectKeyBytesInto(final Page page, final List<byte[]> out) {
    if (page instanceof HOTLeafPage leaf) {
      for (int i = 0; i < leaf.getEntryCount(); i++) {
        out.add(leaf.getKey(i));
      }
    } else if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        collectKeyBytesInto(indirect.getChildReference(i).getPage(), out);
      }
    }
  }

  private static TreeSet<String> hexSet(final List<byte[]> keys) {
    final TreeSet<String> set = new TreeSet<>();
    for (final byte[] key : keys) {
      set.add(hex(key));
    }
    return set;
  }

  private static List<byte[]> toByteList(final Set<String> hexKeys) {
    final List<byte[]> keys = new ArrayList<>(hexKeys.size());
    for (final String h : hexKeys) {
      keys.add(HexFormat.of().parseHex(h));
    }
    return keys;
  }

  /** Close every leaf page reachable from each root, each exactly once (identity-deduplicated). */
  private static void closeAll(final Page... roots) {
    final Set<HOTLeafPage> leaves = Collections.newSetFromMap(new IdentityHashMap<>());
    for (final Page root : roots) {
      collectLeafPages(root, leaves);
    }
    for (final HOTLeafPage leaf : leaves) {
      leaf.close();
    }
  }

  private static void closeLeavesOf(final Page page) {
    closeAll(page);
  }

  private static void collectLeafPages(final Page page, final Set<HOTLeafPage> out) {
    if (page instanceof HOTLeafPage leaf) {
      out.add(leaf);
    } else if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        final PageReference reference = indirect.getChildReference(i);
        if (reference != null && reference.getPage() != null) {
          collectLeafPages(reference.getPage(), out);
        }
      }
    }
  }

  private static byte[] nodeRef(final long bit) {
    final NodeReferences references = new NodeReferences();
    references.getNodeKeys().add(bit);
    final byte[] out = new byte[NodeReferencesSerializer.computeSerializedSize(references)];
    NodeReferencesSerializer.serialize(references, out, 0);
    return out;
  }

  private static byte[] beKey(final long value) {
    final byte[] bytes = new byte[8];
    for (int i = 0; i < 8; i++) {
      bytes[i] = (byte) (value >>> (56 - 8 * i));
    }
    return bytes;
  }

  private static long decodeBe(final byte[] key) {
    long value = 0;
    for (int i = 0; i < 8; i++) {
      value = (value << 8) | (key[i] & 0xFF);
    }
    return value;
  }

  private static PageReference swizzle(final Page page) {
    final PageReference reference = new PageReference();
    reference.setPage(page);
    return reference;
  }

  private static String hex(final byte[] bytes) {
    return HexFormat.of().formatHex(bytes);
  }
}
