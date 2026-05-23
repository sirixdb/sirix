/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.index.IndexType;
import io.sirix.index.hot.HOTMalformedSubtreeDetector.MalformedSubtree;
import io.sirix.index.hot.HOTMalformedSubtreeDetector.PageResolver;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verification of {@link HOTMalformedSubtreeDetector} — the detection half of the HOT
 * detect-and-rebuild structural fix ({@code docs/HOT_FORMAL_FOUNDATION.md} §8).
 *
 * <p>Two properties are tested. <b>No false positives:</b> a canonical trie produced by
 * {@link HOTBulkBuilder} (correct by Theorem 1) must yield <em>zero</em> malformed subtrees —
 * otherwise the commit-time rebuild would churn endlessly. <b>Detection:</b> a trie carrying a
 * deliberate single-invariant defect (I3 / I4 / I5 / I7 / I8 / I11) must flag exactly the
 * indirect that owns the defect, and the "highest malformed" walk must stop at the topmost
 * violating node rather than also reporting its subsumed descendants.
 *
 * <p>Pages are swizzled into their {@link PageReference} (via {@link PageReference#setPage}), so
 * the {@link PageResolver} is simply {@link PageReference#getPage}.
 */
@DisplayName("HOTMalformedSubtreeDetector — detection and highest-malformed semantics")
final class HOTMalformedSubtreeDetectorTest {

  /** Resolver for in-memory (swizzled) pages. */
  private static final PageResolver RESOLVER = PageReference::getPage;

  /** A short non-tombstone value for hand-built leaf entries. */
  private static final byte[] VALUE = {1, 2, 3};

  /** A tombstone value ({@code NodeReferences} tagged 0xFE) — a key, exercised by the I5 walk. */
  private static final byte[] TOMBSTONE = {(byte) 0xFE};

  // ======================================================================
  // No false positives — canonical bulk-built tries have no malformed subtrees.
  // ======================================================================

  @Test
  @DisplayName("canonical bulk-built tries report zero malformed subtrees")
  void cleanBulkBuiltTriesHaveNoMalformedSubtrees() {
    final int[] sizes = {2, 50, 600, 5_000, 20_000};
    int checked = 0;
    int multiLevel = 0;
    for (int gen = 0; gen < 4; gen++) {
      for (final int n : sizes) {
        for (int seed = 0; seed < 2; seed++) {
          final Set<Long> keys = generate(gen, n, new Random((((long) gen) << 32) ^ (n * 31L) ^ seed));
          if (keys.size() < 2) {
            continue;
          }
          final HOTBulkBuilder.BuildResult built = HOTBulkBuilder.build(
              entries(keys), /*revision*/ 1, IndexType.CAS, new AtomicLong(1)::getAndIncrement);
          final List<MalformedSubtree> malformed =
              HOTMalformedSubtreeDetector.detect(built.rootReference(), RESOLVER);
          assertTrue(malformed.isEmpty(),
              "gen=" + gen + " n=" + keys.size() + " seed=" + seed
                  + " — canonical trie flagged as malformed: " + malformed);
          checked++;
          if (built.indirectCount() > 1) {
            multiLevel++;
          }
          closeLeaves(built.rootPage());
        }
      }
    }
    assertTrue(multiLevel > 0, "no multi-level tries were exercised — coverage gap");
    System.out.println("[clean] " + checked + " canonical tries — 0 malformed subtrees ("
        + multiLevel + " multi-level)");
  }

  @Test
  @DisplayName("a null root and a single-leaf root yield an empty result")
  void degenerateRootsYieldEmptyResult() {
    assertTrue(HOTMalformedSubtreeDetector.detect(null, RESOLVER).isEmpty(),
        "null root must yield empty");

    // A handful of keys fit one leaf page — the root is a leaf, so there is no malformed
    // *indirect* to report.
    final Set<Long> few = new HashSet<>();
    for (long k = 0; k < 16; k++) {
      few.add(k);
    }
    final HOTBulkBuilder.BuildResult built = HOTBulkBuilder.build(
        entries(few), 1, IndexType.NAME, new AtomicLong(1)::getAndIncrement);
    assertTrue(built.rootPage() instanceof HOTLeafPage, "16-key root should be a single leaf");
    assertTrue(HOTMalformedSubtreeDetector.detect(built.rootReference(), RESOLVER).isEmpty(),
        "single-leaf-root index must yield empty");
    closeLeaves(built.rootPage());
  }

  // ======================================================================
  // Per-invariant detection — a deliberate single defect flags exactly its indirect.
  // ======================================================================

  @Test
  @DisplayName("I3 — two children sharing a stored partial are detected")
  void detectsI3PartialKeyDuplication() {
    final HOTIndirectPage indirect = HOTIndirectPage.createSpanNode(10, 1, 0, 1L << 56,
        new int[] {0, 0}, new PageReference[] {ref(leaf(1, beKey(1))), ref(leaf(2, beKey(2)))}, 1);
    assertSingleDefect(indirect, "I3-partial-key-uniqueness");
  }

  @Test
  @DisplayName("I4 — a non-zero smallest stored partial is detected")
  void detectsI4FirstPartialNonZero() {
    final HOTIndirectPage indirect = HOTIndirectPage.createSpanNode(11, 1, 0, 1L << 56,
        new int[] {1, 2}, new PageReference[] {ref(leaf(1, beKey(1))), ref(leaf(2, beKey(2)))}, 1);
    assertSingleDefect(indirect, "I4-first-partial-zero");
  }

  @Test
  @DisplayName("I7 — partial keys that are not strictly ascending are detected")
  void detectsI7PartialsNotAscending() {
    final HOTIndirectPage indirect = HOTIndirectPage.createSpanNode(12, 1, 0, 1L << 56,
        new int[] {0, 2, 1},
        new PageReference[] {ref(leaf(1, beKey(1))), ref(leaf(2, beKey(2))), ref(leaf(3, beKey(3)))},
        1);
    assertSingleDefect(indirect, "I7-partial-keys-sorted");
  }

  @Test
  @DisplayName("I8 — children not ordered by subtree first-key are detected")
  void detectsI8ChildrenNotSortedByFirstKey() {
    // Partials ascending (I7 clean) but child[0]'s subtree starts at a larger key than child[1]'s.
    final HOTLeafPage high = leaf(1, beKey(0x2000_0000_0000_0000L));
    final HOTLeafPage low = leaf(2, beKey(0x1000_0000_0000_0000L));
    final HOTIndirectPage indirect = HOTIndirectPage.createSpanNode(13, 1, 0, 1L << 56,
        new int[] {0, 1}, new PageReference[] {ref(high), ref(low)}, 1);
    assertSingleDefect(indirect, "I8-children-sorted-by-firstkey");
  }

  @Test
  @DisplayName("I5 — an interior subtree key disagreeing on a captured disc bit is detected")
  void detectsI5LeafConstancyViolation() {
    // SingleMask capturing only byte 1's MSB (absolute bit 8): a non-most-significant disc bit.
    // child[1] (stored partial 1) expects every subtree key to have that bit set; the third key
    // 0x2900 has it clear, yet still sorts last — so I5 breaks while I7/I8 stay clean.
    final HOTLeafPage zeroBit = leaf(1, new byte[] {0x10, 0x00}, new byte[] {0x11, 0x00});
    final HOTLeafPage oneBit = leaf(2,
        new byte[] {0x20, (byte) 0x80}, new byte[] {0x21, (byte) 0x80}, new byte[] {0x29, 0x00});
    final HOTIndirectPage indirect = HOTIndirectPage.createSpanNode(14, 1, 0, 1L << 55,
        new int[] {0, 1}, new PageReference[] {ref(zeroBit), ref(oneBit)}, 1);
    assertSingleDefect(indirect, "I5-leaf-constancy");
  }

  // ======================================================================
  // Two-level tries — cross-level detection and highest-malformed semantics.
  // ======================================================================

  @Test
  @DisplayName("a hand-built clean two-level trie reports zero malformed subtrees")
  void handBuiltCleanTwoLevelTrieIsClean() {
    final TwoLevel trie = cleanTwoLevelTrie();
    assertTrue(HOTMalformedSubtreeDetector.detect(trie.rootRef, RESOLVER).isEmpty(),
        "hand-built clean two-level trie must not be flagged");
    closeLeaves(trie.rootRef.getPage());
  }

  @Test
  @DisplayName("I11 — a child indirect whose MSB is not less significant flags the parent")
  void detectsI11TrieConditionAtParent() {
    final TwoLevel trie = cleanTwoLevelTrie();
    // Force child[0]'s mask so its most-significant disc bit collides with the root's — the
    // trie condition (down-tree disc bits become less significant) breaks on edge root -> child0.
    trie.child0Ref.setPage(withCollidingMostSignificantBit(trie.child0));

    final List<MalformedSubtree> malformed =
        HOTMalformedSubtreeDetector.detect(trie.rootRef, RESOLVER);
    assertEquals(1, malformed.size(), "exactly the root owns the I11 edge violation");
    assertEquals(trie.root.getPageKey(), malformed.get(0).indirectPageKey());
    assertEquals("I11-trie-condition", malformed.get(0).invariant());
    closeLeaves(trie.rootRef.getPage());
  }

  @Test
  @DisplayName("a malformed root subsumes its malformed descendants — only the root is reported")
  void malformedRootSubsumesMalformedDescendants() {
    final TwoLevel trie = cleanTwoLevelTrie();
    trie.child0Ref.setPage(withCorruptedPartials(trie.child0));   // a malformed descendant
    trie.rootRef.setPage(withCorruptedPartials(trie.root));       // a malformed ancestor

    final List<MalformedSubtree> malformed =
        HOTMalformedSubtreeDetector.detect(trie.rootRef, RESOLVER);
    assertEquals(1, malformed.size(), "the rebuild of the root subsumes the malformed child");
    assertEquals(trie.root.getPageKey(), malformed.get(0).indirectPageKey());
    assertFalse(malformed.stream().anyMatch(m -> m.indirectPageKey() == trie.child0.getPageKey()),
        "the subsumed descendant must not be reported separately");
    closeLeaves(trie.rootRef.getPage());
  }

  @Test
  @DisplayName("the walk descends through a clean root to flag a malformed child")
  void cleanRootDescendsToMalformedChild() {
    final TwoLevel trie = cleanTwoLevelTrie();
    // Corrupt only child[0]'s partials — its mask (hence its MSB) and children are unchanged, so
    // the root stays canonical and the walk must descend into it to find the defect.
    trie.child0Ref.setPage(withCorruptedPartials(trie.child0));

    final List<MalformedSubtree> malformed =
        HOTMalformedSubtreeDetector.detect(trie.rootRef, RESOLVER);
    assertEquals(1, malformed.size(), "only the malformed child should be reported");
    assertEquals(trie.child0.getPageKey(), malformed.get(0).indirectPageKey());
    assertEquals("I3-partial-key-uniqueness", malformed.get(0).invariant());
    closeLeaves(trie.rootRef.getPage());
  }

  // ======================================================================
  // Helpers.
  // ======================================================================

  /** Assert a hand-built one-indirect trie flags exactly that indirect with the given invariant. */
  private static void assertSingleDefect(final HOTIndirectPage indirect, final String invariant) {
    final PageReference rootRef = ref(indirect);
    final List<MalformedSubtree> malformed =
        HOTMalformedSubtreeDetector.detect(rootRef, RESOLVER);
    assertEquals(1, malformed.size(), "expected exactly one malformed subtree, got " + malformed);
    assertEquals(indirect.getPageKey(), malformed.get(0).indirectPageKey());
    assertEquals(invariant, malformed.get(0).invariant(),
        "wrong invariant: " + malformed.get(0).detail());
    closeLeaves(rootRef.getPage());
  }

  /** A clean two-level trie: a hand-built BiNode root over two canonical bulk-built subtrees. */
  private record TwoLevel(PageReference rootRef, HOTIndirectPage root,
                          PageReference child0Ref, HOTIndirectPage child0,
                          PageReference child1Ref) {}

  /**
   * Build a canonical two-level trie. The two subtrees hold disjoint key ranges separated by
   * byte 0's least-significant bit (absolute bit 7): subtree 0's keys all have it clear, subtree
   * 1's all set. The hand-built root is a BiNode discriminating on exactly that bit, so it is
   * itself invariant-clean (partials {0,1}; child MSBs deep in the key, well below bit 7).
   */
  private static TwoLevel cleanTwoLevelTrie() {
    final AtomicLong allocator = new AtomicLong(1);
    final Set<Long> low = new HashSet<>();
    final Set<Long> high = new HashSet<>();
    for (long i = 0; i < 2_000; i++) {
      low.add(i);                              // byte 0 == 0x00
      high.add(0x0100_0000_0000_0000L + i);    // byte 0 == 0x01
    }
    final HOTBulkBuilder.BuildResult built0 = HOTBulkBuilder.build(
        entries(low), 1, IndexType.CAS, allocator::getAndIncrement);
    final HOTBulkBuilder.BuildResult built1 = HOTBulkBuilder.build(
        entries(high), 1, IndexType.CAS, allocator::getAndIncrement);
    final HOTIndirectPage child0 = (HOTIndirectPage) built0.rootPage();
    final int height = 1 + Math.max(child0.getHeight(),
        built1.rootPage() instanceof HOTIndirectPage ip ? ip.getHeight() : 0);
    // bitMask 1L<<56 captures absolute key bit 7 (byte 0's LSB) — the bit separating the ranges.
    final HOTIndirectPage root = HOTIndirectPage.createSpanNode(
        allocator.getAndIncrement(), 1, 0, 1L << 56, new int[] {0, 1},
        new PageReference[] {built0.rootReference(), built1.rootReference()}, height);
    return new TwoLevel(ref(root), root, built0.rootReference(), child0, built1.rootReference());
  }

  /** Rebuild {@code source} with all-zero partials — children {@code 0} and {@code 1} then
   * collide, an I3 defect, while the mask, MSB and children stay identical. */
  private static HOTIndirectPage withCorruptedPartials(final HOTIndirectPage source) {
    return rebuild(source, source.getInitialBytePos(), source.getBitMask(),
        new int[source.getNumChildren()], childReferences(source));
  }

  /** Rebuild {@code source} with a mask whose most-significant disc bit is absolute bit 0 — the
   * most significant position possible — so any parent's MSB fails the trie condition. */
  private static HOTIndirectPage withCollidingMostSignificantBit(final HOTIndirectPage source) {
    return rebuild(source, 0, 1L << 63, source.getPartialKeys(), childReferences(source));
  }

  private static HOTIndirectPage rebuild(final HOTIndirectPage source, final int initialBytePos,
      final long bitMask, final int[] partials, final PageReference[] children) {
    return source.getNodeType() == HOTIndirectPage.NodeType.SPAN_NODE
        ? HOTIndirectPage.createSpanNode(source.getPageKey(), source.getRevision(), initialBytePos,
            bitMask, partials, children, source.getHeight())
        : HOTIndirectPage.createMultiNode(source.getPageKey(), source.getRevision(), initialBytePos,
            bitMask, partials, children, source.getHeight());
  }

  private static PageReference[] childReferences(final HOTIndirectPage indirect) {
    final PageReference[] children = new PageReference[indirect.getNumChildren()];
    for (int i = 0; i < children.length; i++) {
      children[i] = indirect.getChildReference(i);
    }
    return children;
  }

  /** A leaf page holding the given keys (each paired with {@link #VALUE}), inserted in order. */
  private static HOTLeafPage leaf(final long pageKey, final byte[]... keys) {
    final HOTLeafPage leaf = new HOTLeafPage(pageKey, /*revision*/ 1, IndexType.CAS);
    for (final byte[] key : keys) {
      if (!leaf.put(key, VALUE)) {
        throw new IllegalStateException("leaf.put failed for " + HexFormat.of().formatHex(key));
      }
    }
    return leaf;
  }

  private static PageReference ref(final Page page) {
    final PageReference reference = new PageReference();
    reference.setPage(page);
    return reference;
  }

  /** Generator {@code gen}: 0 ascending, 1 uniform random, 2 common-prefix, 3 bimodal. */
  private static Set<Long> generate(final int gen, final int n, final Random random) {
    final Set<Long> keys = new HashSet<>(n * 2);
    switch (gen) {
      case 0 -> { for (long i = 0; i < n; i++) keys.add(i); }
      case 1 -> { while (keys.size() < n) keys.add(random.nextLong()); }
      case 2 -> { while (keys.size() < n)
          keys.add(0x1234_5678_0000_0000L | (random.nextLong() & 0xFFFF_FFFFL)); }
      default -> { while (keys.size() < n) keys.add((keys.size() & 1) == 0
          ? random.nextInt(1 << 16)
          : 0x6000_0000_0000_0000L + random.nextInt(1 << 16)); }
    }
    return keys;
  }

  /** Sorted, distinct entry list from a long key set — 8-byte big-endian keys, every 5th a
   * tombstone (tombstones are keys and must survive the I5 subtree walk unflagged). */
  private static List<HOTBulkBuilder.Entry> entries(final Set<Long> keySet) {
    final long[] keys = keySet.stream().mapToLong(Long::longValue).toArray();
    for (int i = 0; i < keys.length; i++) {
      keys[i] ^= Long.MIN_VALUE; // flip sign bit so signed sort agrees with unsigned
    }
    java.util.Arrays.sort(keys);
    for (int i = 0; i < keys.length; i++) {
      keys[i] ^= Long.MIN_VALUE;
    }
    final List<HOTBulkBuilder.Entry> entries = new ArrayList<>(keys.length);
    for (int i = 0; i < keys.length; i++) {
      entries.add(new HOTBulkBuilder.Entry(beKey(keys[i]), i % 5 == 2 ? TOMBSTONE : VALUE));
    }
    return entries;
  }

  /** 8-byte big-endian encoding of a long (so byte order equals unsigned numeric order). */
  private static byte[] beKey(final long value) {
    final byte[] bytes = new byte[8];
    for (int i = 0; i < 8; i++) {
      bytes[i] = (byte) (value >>> (56 - 8 * i));
    }
    return bytes;
  }

  /** Release the off-heap segment of every leaf page reachable from {@code page}. */
  private static void closeLeaves(final Page page) {
    if (page instanceof HOTLeafPage leaf) {
      leaf.close();
    } else if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        final PageReference reference = indirect.getChildReference(i);
        if (reference != null && reference.getPage() != null) {
          closeLeaves(reference.getPage());
        }
      }
    }
  }
}
