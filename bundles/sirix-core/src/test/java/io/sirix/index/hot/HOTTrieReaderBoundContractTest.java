/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.api.StorageEngineReader;
import io.sirix.index.IndexType;
import io.sirix.index.hot.page.HOTIndirectPage;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.index.hot.reader.HOTRangeCursor;
import io.sirix.index.hot.reader.HOTTrieReader;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.stubbing.Answer;

import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * The lower- and upper-bound contract of {@link HOTTrieReader} (HOT index specification §4.3.1, LB1
 * to LB6) on hand-built tries small enough to check against every probe of their key universe.
 *
 * <p>
 * PEXT routing reads only the discriminative bits of the nodes on the path, so an absent search key
 * can be routed into a leaf it sorts entirely below or above. Each fixture pins one such geometry
 * and asserts that the probe really has it before asserting the result, so a later change of the
 * fixture cannot turn a test into one that passes without reaching its case.
 * </p>
 */
final class HOTTrieReaderBoundContractTest {

  private static final HexFormat HEX = HexFormat.of();
  private static final byte[] VALUE = {0x11, 0x22, 0x33};

  private final List<HOTLeafPage> leaves = new ArrayList<>();
  private long nextPageKey = 1L;

  @AfterEach
  void closeLeaves() {
    for (final HOTLeafPage leaf : leaves) {
      if (!leaf.isClosed()) {
        leaf.close();
      }
    }
    leaves.clear();
  }

  // ======================================================================
  // LB1, LB2, LB5: a search key routed into a leaf it sorts entirely below.
  // ======================================================================

  /**
   * The regression for issue 1427. One-byte keys, a BiNode on bit 6 over {@code {18, 19}} and
   * {@code {1a, 1b}}: {@code 12} has bit 6 set and is routed to the right leaf, where its insertion
   * point is 0, yet both keys of the left leaf are {@code >= 12} too.
   */
  @ParameterizedTest(name = "firstKeyMemo={0}")
  @ValueSource(booleans = {true, false})
  void seekBelowTheRoutedLeafLandsOnTheEarlierLeaf(final boolean firstKeyMemo) {
    final HOTLeafPage left = leaf(0x18, 0x19);
    final HOTLeafPage right = leaf(0x1A, 0x1B);
    final Trie trie = trie(HOTIndirectPage.createBiNode(nextPageKey++, 1, 6, ref(left), ref(right), 1), left, right);

    try (HOTTrieReader reader = reader(firstKeyMemo)) {
      final byte[] probe = key(0x12);
      assertSame(right, reader.navigateToLeaf(trie.root, probe), "12 has bit 6 set and must route to the right leaf");
      assertEquals(-1, right.findEntry(probe), "12 must sort below every key of the leaf it is routed to");

      final HOTTrieReader.LowerBoundResult lower = reader.lowerBound(trie.root, probe);
      assertSame(left, lower.leaf, "lowerBound(12) must land on the leaf that holds 18");
      assertEquals(0, lower.indexInLeaf);
      // LB5: the path stack follows the returned leaf, so the scan continues with the right leaf.
      assertSame(right, reader.advanceToNextLeaf());
      assertNull(reader.advanceToNextLeaf());

      final HOTTrieReader.LowerBoundResult upper = reader.upperBound(trie.root, probe);
      assertSame(left, upper.leaf, "upperBound(12) must land on the leaf that holds 18");
      assertEquals(0, upper.indexInLeaf);

      assertKeys(List.of(key(0x18), key(0x19), key(0x1A), key(0x1B)), scan(reader, trie.root, probe, null),
          "range(12, END)");
      assertKeys(List.of(key(0x18), key(0x19)), scan(reader, trie.root, probe, key(0x19)), "range(12, 19)");

      assertEveryProbeMatchesTheOracle(reader, trie);
    }
  }

  /**
   * The same geometry with the top bit set in every key, so the mismatch bit of a probe below the
   * trie is absolute bit 0: bit 0 is a real mismatch position, not "no mismatch".
   */
  @Test
  void mismatchAtBitZeroIsWalkedLikeAnyOtherBit() {
    final HOTLeafPage left = leaf(0x98, 0x99);
    final HOTLeafPage right = leaf(0x9A, 0x9B);
    final Trie trie = trie(HOTIndirectPage.createBiNode(nextPageKey++, 1, 6, ref(left), ref(right), 1), left, right);

    try (HOTTrieReader reader = reader(true)) {
      // 12 = 0001 0010 is routed right by bit 6 and differs from 9a at bit 0.
      assertSame(right, reader.navigateToLeaf(trie.root, key(0x12)));
      assertSeek(reader, trie, key(0x12), key(0x98), key(0x98));
      // 9e = 1001 1110 is routed right, sorts above 9b, and differs from 9a at bit 5.
      assertSeek(reader, trie, key(0x9E), null, null);
      assertEveryProbeMatchesTheOracle(reader, trie);
    }
  }

  /**
   * A BiNode on bit 0 separates its children at the most significant bit, so a sibling's first key
   * differs from the matched first key at bit 0: the smallest possible difference is a difference.
   */
  @Test
  void siblingsThatDifferAtBitZeroAreOutsideTheAffectedSubtree() {
    final HOTLeafPage left = leaf(0x10, 0x11);
    final HOTLeafPage right = leaf(0x90, 0x91);
    final Trie trie = trie(HOTIndirectPage.createBiNode(nextPageKey++, 1, 0, ref(left), ref(right), 1), left, right);

    try (HOTTrieReader reader = reader(true)) {
      // 85 is routed right and sorts below 90; the left leaf is not part of its affected subtree.
      assertSame(right, reader.navigateToLeaf(trie.root, key(0x85)));
      assertSeek(reader, trie, key(0x85), key(0x90), key(0x90));
      // 30 is routed left and sorts above 11; the right leaf is not part of its affected subtree.
      assertSame(left, reader.navigateToLeaf(trie.root, key(0x30)));
      assertSeek(reader, trie, key(0x30), key(0x90), key(0x90));
      assertEveryProbeMatchesTheOracle(reader, trie);
    }
  }

  // ======================================================================
  // Phase 4: the affected subtree is a run of several children.
  // ======================================================================

  /**
   * One node over the bits {1, 5, 6} with the leaves A {10, 11}, B {50, 51}, C {52, 53} and D {54,
   * 55}. A probe below C or D that differs from them above bit 5 has B, C and D as its affected
   * subtree and A outside it.
   */
  @ParameterizedTest(name = "firstKeyMemo={0}")
  @ValueSource(booleans = {true, false})
  void seekBelowWalksLeftAcrossTheWholeAffectedSubtreeAndNoFurther(final boolean firstKeyMemo) {
    final FourLeafNode node = fourLeafNode();

    try (HOTTrieReader reader = reader(firstKeyMemo)) {
      // 42 = 0100 0010: bits 1, 5, 6 = 1, 0, 1 routes to C; it differs from 52 at bit 3.
      assertSame(node.c, reader.navigateToLeaf(node.trie.root, key(0x42)));
      assertSeek(reader, node.trie, key(0x42), key(0x50), key(0x50));
      // 46 = 0100 0110: bits 1, 5, 6 = 1, 1, 1 routes to D; the run is two children wide to its left.
      assertSame(node.d, reader.navigateToLeaf(node.trie.root, key(0x46)));
      assertSeek(reader, node.trie, key(0x46), key(0x50), key(0x50));
      // 5a = 0101 1010 routes to C, sorts above it and above D: the run extends right past D.
      assertSame(node.c, reader.navigateToLeaf(node.trie.root, key(0x5A)));
      assertSeek(reader, node.trie, key(0x5A), null, null);
      // 30 routes to A and sorts above it; B differs from A at bit 1, above the mismatch bit 2.
      assertSame(node.a, reader.navigateToLeaf(node.trie.root, key(0x30)));
      assertSeek(reader, node.trie, key(0x30), key(0x50), key(0x50));

      assertEveryProbeMatchesTheOracle(reader, node.trie);
    }
  }

  /**
   * LB6: phase 4 resolves first keys only on the side that decides the result. The first-key memo of
   * the branching node records exactly which children a seek resolved.
   */
  @Test
  void aSeekResolvesSiblingFirstKeysOnlyOnTheDecidingSide() {
    final FourLeafNode below = fourLeafNode();
    try (HOTTrieReader reader = reader(true)) {
      // Below C: C itself, B (inside the run) and A (the child that ends it); never D.
      assertSeek(reader, below.trie, key(0x42), key(0x50), key(0x50));
      assertNotNull(below.node.cachedChildFirstKey(0));
      assertNotNull(below.node.cachedChildFirstKey(1));
      assertNotNull(below.node.cachedChildFirstKey(2));
      assertNull(below.node.cachedChildFirstKey(3), "a seek below the routed leaf must not resolve a right sibling");
    }

    final FourLeafNode above = fourLeafNode();
    try (HOTTrieReader reader = reader(true)) {
      // 59 = 0101 1001 routes to B, sorts above it, and differs from 50 at bit 4: C and D follow.
      assertSame(above.b, reader.navigateToLeaf(above.trie.root, key(0x59)));
      assertSeek(reader, above.trie, key(0x59), null, null);
      assertNull(above.node.cachedChildFirstKey(0), "a seek above the routed leaf must not resolve a left sibling");
      assertNotNull(above.node.cachedChildFirstKey(1));
      assertNotNull(above.node.cachedChildFirstKey(2));
      assertNotNull(above.node.cachedChildFirstKey(3));
    }

    final FourLeafNode inside = fourLeafNode();
    try (HOTTrieReader reader = reader(true)) {
      // A probe strictly inside its leaf, and a stored key, resolve no first key at all.
      final HOTLeafPage wide = leaf(0x20, 0x24, 0x28);
      final Trie two = trie(HOTIndirectPage.createBiNode(nextPageKey++, 1, 1, ref(wide), ref(leaf(0x60, 0x61)), 1));
      final HOTIndirectPage twoRoot = (HOTIndirectPage) two.root.getPage();
      final HOTTrieReader.LowerBoundResult strictlyInside = reader.lowerBound(two.root, key(0x22));
      assertSame(wide, strictlyInside.leaf);
      assertEquals(1, strictlyInside.indexInLeaf);
      assertNull(twoRoot.cachedChildFirstKey(0));
      assertNull(twoRoot.cachedChildFirstKey(1));

      assertSeek(reader, inside.trie, key(0x52), key(0x52), key(0x53));
      for (int child = 0; child < 4; child++) {
        assertNull(inside.node.cachedChildFirstKey(child), "an exact hit must not resolve a first key");
      }
    }
  }

  // ======================================================================
  // Phase 3: the mismatch bit lies above the node the leaf hangs under.
  // ======================================================================

  /**
   * A root BiNode on bit 1 over the leaf A {10, 11} and an inner node on the bits {5, 6} over B, C
   * and D. A probe routed to D that differs from it at bit 3 has the whole inner node as its affected
   * subtree, so the walk leaves the inner node and the bound is the inner node's leftmost key.
   */
  @ParameterizedTest(name = "firstKeyMemo={0}")
  @ValueSource(booleans = {true, false})
  void seekBelowLeavesTheNodeWhoseBitsAllLieBelowTheMismatch(final boolean firstKeyMemo) {
    final HOTLeafPage a = leaf(0x10, 0x11);
    final HOTLeafPage b = leaf(0x50, 0x51);
    final HOTLeafPage c = leaf(0x52, 0x53);
    final HOTLeafPage d = leaf(0x54, 0x55);
    final HOTIndirectPage inner = HOTIndirectPage.createSpanNode(nextPageKey++, 1, 0, bitMask(5, 6),
        new int[] {0, 1, 2}, new PageReference[] {ref(b), ref(c), ref(d)}, 1);
    final Trie trie = trie(HOTIndirectPage.createBiNode(nextPageKey++, 1, 1, ref(a), ref(inner), 2), a, b, c, d);

    try (HOTTrieReader reader = reader(firstKeyMemo)) {
      // 46 = 0100 0110 routes to D and differs from 54 at bit 3, above the inner node's bits 5 and 6.
      assertSame(d, reader.navigateToLeaf(trie.root, key(0x46)));
      final HOTTrieReader.LowerBoundResult lower = reader.lowerBound(trie.root, key(0x46));
      assertSame(b, lower.leaf);
      assertEquals(0, lower.indexInLeaf);
      // LB5: from B the scan continues with C and D below the same inner node.
      assertSame(c, reader.advanceToNextLeaf());
      assertSame(d, reader.advanceToNextLeaf());
      assertNull(reader.advanceToNextLeaf());

      // 7f routes to D, sorts above it and differs at bit 2: the bound lies past the whole trie.
      assertSame(d, reader.navigateToLeaf(trie.root, key(0x7F)));
      assertSeek(reader, trie, key(0x7F), null, null);
      // 30 routes to A, sorts above it and differs at bit 2: the bound is the inner node's first key.
      assertSame(a, reader.navigateToLeaf(trie.root, key(0x30)));
      assertSeek(reader, trie, key(0x30), key(0x50), key(0x50));
      // 56 routes to D, sorts above it and differs at bit 6: the mismatch stays inside the inner node.
      assertSame(d, reader.navigateToLeaf(trie.root, key(0x56)));
      assertSeek(reader, trie, key(0x56), null, null);

      assertEveryProbeMatchesTheOracle(reader, trie);
    }
  }

  // ======================================================================
  // LB4: search keys that no bit separates from a stored key.
  // ======================================================================

  /**
   * A search key the routed leaf's first key merely extends is ordered before it by length, and the
   * bound is that first key: with a zero continuation there is no mismatch bit to walk with.
   */
  @Test
  void aSearchKeyTheFirstKeyExtendsStaysAtThatKey() {
    final HOTLeafPage left = leaf(key(0x10));
    final HOTLeafPage right = leaf(key(0x50, 0x00), key(0x50, 0x01));
    final Trie trie = trie(HOTIndirectPage.createBiNode(nextPageKey++, 1, 1, ref(left), ref(right), 1), left, right);

    try (HOTTrieReader reader = reader(true)) {
      // 50 routes like 50 00, from which no bit separates it.
      assertSame(right, reader.navigateToLeaf(trie.root, key(0x50)));
      assertEquals(-1, right.findEntry(key(0x50)));
      assertSeek(reader, trie, key(0x50), key(0x50, 0x00), key(0x50, 0x00));
      // The empty key is a prefix of every key; it routes on all-zero bits to the left leaf.
      assertSame(left, reader.navigateToLeaf(trie.root, new byte[0]));
      assertSeek(reader, trie, new byte[0], key(0x10), key(0x10));
      // An exact hit is unaffected: the upper bound steps past it.
      assertSeek(reader, trie, key(0x50, 0x00), key(0x50, 0x00), key(0x50, 0x01));
      assertEveryProbeMatchesTheOracle(reader, trie);
    }

    // The continuation need not be zero: the first key is still the smallest key that extends it.
    final HOTLeafPage first = leaf(key(0x10));
    final HOTLeafPage extended = leaf(key(0x50, 0x01), key(0x50, 0x02));
    final Trie nonZero =
        trie(HOTIndirectPage.createBiNode(nextPageKey++, 1, 1, ref(first), ref(extended), 1), first, extended);
    try (HOTTrieReader reader = reader(true)) {
      assertSame(extended, reader.navigateToLeaf(nonZero.root, key(0x50)));
      assertSeek(reader, nonZero, key(0x50), key(0x50, 0x01), key(0x50, 0x01));
      assertEveryProbeMatchesTheOracle(reader, nonZero);
    }
  }

  /**
   * A search key that is the routed leaf's first key followed only by zero bytes sorts above it by
   * length, and its bound is the first entry of the next leaf.
   */
  @Test
  void aZeroContinuationOfTheFirstKeyAdvancesToTheNextLeaf() {
    final HOTLeafPage left = leaf(key(0x10));
    final HOTLeafPage right = leaf(key(0x50));
    final Trie trie = trie(HOTIndirectPage.createBiNode(nextPageKey++, 1, 1, ref(left), ref(right), 1), left, right);

    try (HOTTrieReader reader = reader(true)) {
      assertSame(left, reader.navigateToLeaf(trie.root, key(0x10, 0x00)));
      assertEquals(-2, left.findEntry(key(0x10, 0x00)), "10 00 must sort above the only key of its leaf");
      final HOTTrieReader.LowerBoundResult lower = reader.lowerBound(trie.root, key(0x10, 0x00));
      assertSame(right, lower.leaf);
      assertEquals(0, lower.indexInLeaf);
      assertNull(reader.advanceToNextLeaf(), "the path stack must follow the leaf the seek advanced to");

      assertSeek(reader, trie, key(0x10, 0x00), key(0x50), key(0x50));
      assertSeek(reader, trie, key(0x10, 0x00, 0x00), key(0x50), key(0x50));
      assertSeek(reader, trie, key(0x50, 0x00), null, null);
      assertSeek(reader, trie, key(0x50, 0x00, 0x00, 0x00), null, null);
      // A continuation with a set bit has a mismatch bit and takes the walk.
      assertSeek(reader, trie, key(0x10, 0x00, 0x01), key(0x50), key(0x50));
      assertSeek(reader, trie, key(0x10, 0x01), key(0x50), key(0x50));
      assertSeek(reader, trie, key(0x50, 0x00, 0x01), null, null);
      assertEveryProbeMatchesTheOracle(reader, trie);
    }

    // With the extension stored in the same leaf, the zero continuation lies strictly inside it.
    final HOTLeafPage withExtension = leaf(key(0x10), key(0x10, 0x80));
    final HOTLeafPage last = leaf(key(0x50));
    final Trie inside =
        trie(HOTIndirectPage.createBiNode(nextPageKey++, 1, 1, ref(withExtension), ref(last), 1), withExtension, last);
    try (HOTTrieReader reader = reader(true)) {
      assertSeek(reader, inside, key(0x10, 0x00), key(0x10, 0x80), key(0x10, 0x80));
      assertEveryProbeMatchesTheOracle(reader, inside);
    }
  }

  // ======================================================================
  // A root leaf: the insertion point decides alone.
  // ======================================================================

  @Test
  void aRootLeafAnswersFromItsInsertionPoint() {
    final HOTLeafPage only = leaf(key(0x10), key(0x20));
    final Trie trie = trie(only, only);
    try (HOTTrieReader reader = reader(true)) {
      assertSeek(reader, trie, new byte[0], key(0x10), key(0x10));
      assertSeek(reader, trie, key(0x05), key(0x10), key(0x10));
      assertSeek(reader, trie, key(0x10), key(0x10), key(0x20));
      assertSeek(reader, trie, key(0x15), key(0x20), key(0x20));
      assertSeek(reader, trie, key(0x20), key(0x20), null);
      assertSeek(reader, trie, key(0x20, 0x00), null, null);
      assertSeek(reader, trie, key(0x30), null, null);
      assertEveryProbeMatchesTheOracle(reader, trie);
    }

    // No bit separates 10 00 from the only stored key; it still sorts above it.
    final HOTLeafPage single = leaf(key(0x10));
    final Trie one = trie(single, single);
    try (HOTTrieReader reader = reader(true)) {
      assertSeek(reader, one, key(0x10, 0x00), null, null);
      assertSeek(reader, one, key(0x10, 0x00, 0x00), null, null);
      assertSeek(reader, one, new byte[0], key(0x10), key(0x10));
      assertEveryProbeMatchesTheOracle(reader, one);
    }

    // An empty root leaf is the empty trie: no key is at or above anything.
    final PageReference emptyRoot = ref(leaf(new byte[0][]));
    try (HOTTrieReader reader = reader(true)) {
      assertNull(reader.lowerBound(emptyRoot, new byte[0]).leaf);
      assertNull(reader.lowerBound(emptyRoot, key(0x10)).leaf);
      assertNull(reader.upperBound(emptyRoot, key(0x10)).leaf);
      assertKeys(List.of(), scan(reader, emptyRoot, key(0x10), null), "range over the empty trie");
    }
  }

  // ======================================================================
  // Error case: a route that reaches no page is corruption, never a bound.
  // ======================================================================

  @Test
  void anUnresolvedRouteOrSiblingIsCorruptionNotABound() {
    final HOTLeafPage right = leaf(0x1A, 0x1B);
    // The left child of the four-key trie resolves to nothing: no page, no storage key.
    final PageReference root =
        ref(HOTIndirectPage.createBiNode(nextPageKey++, 1, 6, new PageReference(), ref(right), 1));

    try (HOTTrieReader reader = reader(true)) {
      // 12 is routed to the right leaf and sorts below it, so its bound depends on the left child.
      // Answering from the routed leaf would hide the missing subtree behind a plausible position.
      assertSame(right, reader.navigateToLeaf(root, key(0x12)));
      assertCorruption(() -> reader.lowerBound(root, key(0x12)));
      assertCorruption(() -> reader.upperBound(root, key(0x12)));
      assertCorruption(() -> reader.range(root, key(0x12), null));
      // 18 is routed to the missing child itself.
      assertCorruption(() -> reader.lowerBound(root, key(0x18)));
      // A stored key of the resolvable leaf needs nothing else and is still found.
      final HOTTrieReader.LowerBoundResult stored = reader.lowerBound(root, key(0x1B));
      assertSame(right, stored.leaf);
      assertEquals(1, stored.indexInLeaf);
    }
  }

  private static void assertCorruption(final Executable seek) {
    final IllegalStateException failure = assertThrows(IllegalStateException.class, seek);
    assertTrue(failure.getMessage().contains("HOT structural corruption"), failure.getMessage());
  }

  // ======================================================================
  // Torn reads: the routed leaf is reclaimed under the seek.
  // ======================================================================

  /** Which of the seek's two read batches of the routed leaf is disturbed, and how. */
  enum Disturbance {
    FIRST_BATCH_RECLAIMED(true, true, false), FIRST_BATCH_FAILS_WHILE_RECLAIMED(true, true,
        true), FIRST_BATCH_FAILS_ON_STABLE_BYTES(true, false, true), SECOND_BATCH_RECLAIMED(false, true,
            false), SECOND_BATCH_FAILS_WHILE_RECLAIMED(false, true,
                true), SECOND_BATCH_FAILS_ON_STABLE_BYTES(false, false, true);

    private final boolean firstBatch;
    private final boolean reclaims;
    private final boolean fails;

    Disturbance(final boolean firstBatch, final boolean reclaims, final boolean fails) {
      this.firstBatch = firstBatch;
      this.reclaims = reclaims;
      this.fails = fails;
    }
  }

  /**
   * The reader reads unpinned leaves and validates their stamp before a decision escapes. A leaf
   * reclaimed under either read batch of the seek, the insertion point or the mismatch bit, makes the
   * seek start over on guarded copies and still return the bound. A read that fails on stable bytes
   * is corruption and is reported, never retried.
   */
  @ParameterizedTest(name = "{0}")
  @EnumSource(Disturbance.class)
  void aLeafReclaimedUnderTheSeekIsReReadOnAGuardedCopy(final Disturbance disturbance) {
    try (Arena arena = Arena.ofShared()) {
      final List<HOTLeafPage> copies = new ArrayList<>();
      try {
        // The four-key trie again, with both leaves resolved through the storage engine.
        final PageReference leftRef = new PageReference().setKey(101L);
        final PageReference rightRef = new PageReference().setKey(102L);
        final HOTIndirectPage rootPage = HOTIndirectPage.createBiNode(nextPageKey++, 1, 6, leftRef, rightRef, 1);
        final PageReference root = ref(rootPage);
        final IllegalStateException readFailure = new IllegalStateException("leaf read failed");
        final boolean[] armed = {true};

        final StorageEngineReader storage = mock(StorageEngineReader.class);
        when(storage.loadHOTPageAndGuard(root)).thenReturn(rootPage);
        when(storage.loadHOTPage(leftRef)).thenAnswer(_ -> leafCopy(arena, copies, 0x18, 0x19));
        when(storage.loadHOTPageAndGuard(leftRef)).thenAnswer(_ -> guarded(leafCopy(arena, copies, 0x18, 0x19)));
        when(storage.loadHOTPage(rightRef)).thenAnswer(
            _ -> disturbed(leafCopy(arena, copies, 0x1A, 0x1B), disturbance, armed, readFailure));
        when(storage.loadHOTPageAndGuard(rightRef)).thenAnswer(
            _ -> guarded(disturbed(leafCopy(arena, copies, 0x1A, 0x1B), disturbance, armed, readFailure)));

        try (HOTTrieReader reader = new HOTTrieReader(storage)) {
          // 12 is routed to the right leaf and sorts below it, so the seek runs both read batches.
          if (disturbance.reclaims) {
            final HOTTrieReader.LowerBoundResult lower = reader.lowerBound(root, key(0x12));
            assertNotNull(lower.leaf, "the retried seek must still find the bound");
            assertKey(key(0x18), lower.leaf.getKey(lower.indexInLeaf), "lowerBound(12) after a reclaimed leaf");
            assertEquals(1, lower.leaf.getGuardCount(), "after a torn read the reader guards the leaf it returns");
          } else {
            final byte[] searchKey = key(0x12);
            assertSame(readFailure,
                assertThrows(IllegalStateException.class, () -> reader.lowerBound(root, searchKey)));
          }
          assertFalse(armed[0], "the disturbance must have fired");
        }
        for (final HOTLeafPage copy : copies) {
          assertEquals(0, copy.getGuardCount(), "closing the reader must release every guard");
        }
      } finally {
        for (final HOTLeafPage copy : copies) {
          copy.close();
        }
      }
    }
  }

  /**
   * A fresh copy of a leaf, as the storage engine hands one out for a cold or reclaimed reference.
   */
  private static HOTLeafPage leafCopy(final Arena arena, final List<HOTLeafPage> copies, final int... oneByteKeys) {
    final HOTLeafPage leaf = spy(new HOTLeafPage(100L, 1, IndexType.CAS, arena.allocate(HOTLeafPage.DEFAULT_SIZE), null,
        new char[HOTLeafPage.MAX_ENTRIES], 0, 0));
    for (final int oneByteKey : oneByteKeys) {
      assertTrue(leaf.put(key(oneByteKey), VALUE), "fixture key must fit its leaf");
    }
    copies.add(leaf);
    return leaf;
  }

  private static HOTLeafPage guarded(final HOTLeafPage leaf) {
    assertTrue(leaf.acquireGuard());
    return leaf;
  }

  /**
   * Disturb the last read of one batch, once: the entry count closes the first batch, the mismatch
   * bit the second. Reclaiming closes the copy, which is what moves its stamp.
   */
  private static HOTLeafPage disturbed(final HOTLeafPage leaf, final Disturbance disturbance, final boolean[] armed,
      final IllegalStateException readFailure) {
    final Answer<Object> answer = invocation -> {
      if (!armed[0]) {
        return invocation.callRealMethod();
      }
      armed[0] = false;
      final Object result = invocation.callRealMethod();
      if (disturbance.reclaims) {
        leaf.close();
      }
      if (disturbance.fails) {
        throw readFailure;
      }
      return result;
    };
    if (disturbance.firstBatch) {
      doAnswer(answer).when(leaf).getEntryCount();
    } else {
      doAnswer(answer).when(leaf).msdbWith(anyInt(), any(byte[].class), anyInt());
    }
    return leaf;
  }

  // ======================================================================
  // LB3: only the valid prefix of a reusable buffer takes part.
  // ======================================================================

  @Test
  void bytesPastTheSearchKeyLengthNeverTakePart() {
    final FourLeafNode node = fourLeafNode();
    final HOTLeafPage left = leaf(key(0x10));
    final HOTLeafPage right = leaf(key(0x50));
    final Trie zero = trie(HOTIndirectPage.createBiNode(nextPageKey++, 1, 1, ref(left), ref(right), 1), left, right);

    try (HOTTrieReader reader = reader(true)) {
      for (final int staleTail : new int[] {0x00, 0x01, 0x7F, 0xA5, 0xFF}) {
        // Below the routed leaf: 42 with a stale tail must still walk left to 50.
        final byte[] below = buffer(staleTail, 0x42);
        assertKey(key(0x50), seek(reader, node.trie.root, below, 1, false), "lowerBound(42) tail " + staleTail);
        assertKey(key(0x50), seek(reader, node.trie.root, below, 1, true), "upperBound(42) tail " + staleTail);
        // Above the routed leaf: 5a with a stale tail must still run past D.
        final byte[] above = buffer(staleTail, 0x5A);
        assertKey(null, seek(reader, node.trie.root, above, 1, false), "lowerBound(5a) tail " + staleTail);
        // A zero continuation is judged on its valid bytes only: 10 00 advances, whatever follows it.
        final byte[] continuation = buffer(staleTail, 0x10, 0x00);
        assertKey(key(0x50), seek(reader, zero.root, continuation, 2, false), "lowerBound(10 00) tail " + staleTail);
        // An exact key in an oversized buffer is still exact.
        final byte[] exact = buffer(staleTail, 0x10);
        assertKey(key(0x10), seek(reader, zero.root, exact, 1, false), "lowerBound(10) tail " + staleTail);
        assertKey(key(0x50), seek(reader, zero.root, exact, 1, true), "upperBound(10) tail " + staleTail);
        // The empty prefix of a non-empty buffer is the empty key.
        assertKey(key(0x10), seek(reader, zero.root, exact, 0, false), "lowerBound(<empty>) tail " + staleTail);
      }
    }
  }

  // ======================================================================
  // Fixtures.
  // ======================================================================

  /** One span node over the bits {1, 5, 6}: A {10, 11}, B {50, 51}, C {52, 53}, D {54, 55}. */
  private FourLeafNode fourLeafNode() {
    final HOTLeafPage a = leaf(0x10, 0x11);
    final HOTLeafPage b = leaf(0x50, 0x51);
    final HOTLeafPage c = leaf(0x52, 0x53);
    final HOTLeafPage d = leaf(0x54, 0x55);
    // Sparse partial keys over (bit 1, bit 5, bit 6): A 000, B 100, C 101, D 110.
    final HOTIndirectPage node = HOTIndirectPage.createSpanNode(nextPageKey++, 1, 0, bitMask(1, 5, 6),
        new int[] {0b000, 0b100, 0b101, 0b110}, new PageReference[] {ref(a), ref(b), ref(c), ref(d)}, 1);
    return new FourLeafNode(trie(node, a, b, c, d), node, a, b, c, d);
  }

  private record FourLeafNode(Trie trie, HOTIndirectPage node, HOTLeafPage a, HOTLeafPage b, HOTLeafPage c,
      HOTLeafPage d) {
  }

  /** A trie and the sorted keys it stores. */
  private record Trie(PageReference root, List<byte[]> oracle) {
  }

  private Trie trie(final Page rootPage, final HOTLeafPage... leavesInOrder) {
    final PageReference root = ref(rootPage);
    final List<byte[]> oracle = new ArrayList<>();
    for (final HOTLeafPage leaf : leavesInOrder) {
      for (int i = 0; i < leaf.getEntryCount(); i++) {
        oracle.add(leaf.getKey(i));
      }
    }
    for (int i = 1; i < oracle.size(); i++) {
      assertTrue(Arrays.compareUnsigned(oracle.get(i - 1), oracle.get(i)) < 0, "fixture keys must ascend");
    }
    // Every fixture must be a trie the production detector and the test validator both accept:
    // a seek result on a malformed trie proves nothing about the reader.
    assertEquals(List.of(), HOTMalformedSubtreeDetector.detect(root, PageReference::getPage), "malformed fixture");
    assertEquals(List.of(), HOTInvariantValidator.validate(root, mock(StorageEngineReader.class)).violations(),
        "invalid fixture");
    return new Trie(root, oracle);
  }

  private HOTLeafPage leaf(final int... oneByteKeys) {
    final byte[][] keys = new byte[oneByteKeys.length][];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = key(oneByteKeys[i]);
    }
    return leaf(keys);
  }

  private HOTLeafPage leaf(final byte[]... keys) {
    final HOTLeafPage leaf = new HOTLeafPage(nextPageKey++, 1, IndexType.CAS);
    leaves.add(leaf);
    for (final byte[] key : keys) {
      assertTrue(leaf.put(key, VALUE), "fixture key must fit its leaf");
    }
    return leaf;
  }

  private static PageReference ref(final Page page) {
    final PageReference reference = new PageReference();
    reference.setPage(page);
    return reference;
  }

  /**
   * A reader over swizzled in-memory pages. A reader without a transaction intent log memoizes
   * resolved first keys in the parent node; a writer-backed one resolves every first key by a
   * leftmost descent. Both must give the same answers.
   */
  private static HOTTrieReader reader(final boolean firstKeyMemo) {
    final StorageEngineReader storage = mock(StorageEngineReader.class);
    when(storage.hasTrxIntentLog()).thenReturn(!firstKeyMemo);
    return new HOTTrieReader(storage);
  }

  /** The single-mask extraction mask of a node whose discriminative bits all lie in key bytes 0-7. */
  private static long bitMask(final int... absoluteBits) {
    long mask = 0L;
    for (final int bit : absoluteBits) {
      mask |= 1L << (63 - bit);
    }
    return mask;
  }

  private static byte[] key(final int... bytes) {
    final byte[] key = new byte[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      key[i] = (byte) bytes[i];
    }
    return key;
  }

  /** The given key bytes followed by seven stale bytes that are not part of the search key. */
  private static byte[] buffer(final int staleTail, final int... keyBytes) {
    final byte[] buffer = new byte[keyBytes.length + 7];
    Arrays.fill(buffer, (byte) staleTail);
    for (int i = 0; i < keyBytes.length; i++) {
      buffer[i] = (byte) keyBytes[i];
    }
    return buffer;
  }

  // ======================================================================
  // Oracle.
  // ======================================================================

  /**
   * Every probe of the fixture's key universe against the sorted-set oracle: the empty key, all one-
   * and two-byte keys, and every stored key followed by one to three zero bytes or by {@code 00 01}.
   * One-byte probes also check the range that starts at them, open-ended and bounded.
   */
  private static void assertEveryProbeMatchesTheOracle(final HOTTrieReader reader, final Trie trie) {
    assertSeekMatchesOracle(reader, trie, new byte[0]);
    for (int first = 0; first < 256; first++) {
      final byte[] one = key(first);
      assertSeekMatchesOracle(reader, trie, one);
      assertRange(reader, trie, one, null);
      assertRange(reader, trie, one, trie.oracle.get(trie.oracle.size() / 2));
      for (int second = 0; second < 256; second++) {
        assertSeekMatchesOracle(reader, trie, key(first, second));
      }
    }
    for (final byte[] stored : trie.oracle) {
      assertSeekMatchesOracle(reader, trie, stored);
      for (int zeros = 1; zeros <= 3; zeros++) {
        assertSeekMatchesOracle(reader, trie, Arrays.copyOf(stored, stored.length + zeros));
      }
      final byte[] zeroThenOne = Arrays.copyOf(stored, stored.length + 2);
      zeroThenOne[zeroThenOne.length - 1] = 0x01;
      assertSeekMatchesOracle(reader, trie, zeroThenOne);
    }
    assertKeys(trie.oracle, scan(reader, trie.root, null, null), "unbounded range");
  }

  private static void assertSeekMatchesOracle(final HOTTrieReader reader, final Trie trie, final byte[] probe) {
    assertSeek(reader, trie, probe, keyAt(trie.oracle, lowerIndex(trie.oracle, probe, false)),
        keyAt(trie.oracle, lowerIndex(trie.oracle, probe, true)));
  }

  /** Assert both bounds, after checking the expectation itself against the sorted-set oracle. */
  private static void assertSeek(final HOTTrieReader reader, final Trie trie, final byte[] probe,
      final byte[] expectedLower, final byte[] expectedUpper) {
    assertBound(keyAt(trie.oracle, lowerIndex(trie.oracle, probe, false)), expectedLower,
        "test expectation for lowerBound", probe);
    assertBound(keyAt(trie.oracle, lowerIndex(trie.oracle, probe, true)), expectedUpper,
        "test expectation for upperBound", probe);
    assertBound(expectedLower, seek(reader, trie.root, probe, probe.length, false), "lowerBound", probe);
    assertBound(expectedUpper, seek(reader, trie.root, probe, probe.length, true), "upperBound", probe);
  }

  /** The message is built only on a mismatch: the exhaustive sweeps call this millions of times. */
  private static void assertBound(final byte[] expected, final byte[] actual, final String operation,
      final byte[] probe) {
    if (!Arrays.equals(expected, actual)) {
      fail(operation + "(" + hex(probe) + ") must be " + hex(expected) + " but was " + hex(actual));
    }
  }

  private static byte[] seek(final HOTTrieReader reader, final PageReference root, final byte[] probe,
      final int probeLength, final boolean upper) {
    final HOTTrieReader.LowerBoundResult result = upper
        ? reader.upperBound(root, probe, probeLength)
        : reader.lowerBound(root, probe, probeLength);
    if (result.leaf == null) {
      assertEquals(-1, result.indexInLeaf, "an exhausted seek carries no entry index");
      return null;
    }
    // The fixture pages are heap-held and never evicted, so the stamp cannot move under the read.
    final byte[] key = result.leaf.getKey(result.indexInLeaf);
    assertTrue(reader.validateCurrentLeaf(), "in-memory fixture leaves are never reclaimed");
    assertSame(result.leaf, reader.currentLeafPage(), "the reader must be positioned on the leaf it returned");
    return key;
  }

  private static List<byte[]> scan(final HOTTrieReader reader, final PageReference root, final byte[] from,
      final byte[] to) {
    final List<byte[]> keys = new ArrayList<>();
    try (HOTRangeCursor cursor = reader.range(root, from, to)) {
      while (cursor.hasNext()) {
        keys.add(cursor.next().keyBytes());
      }
    }
    return keys;
  }

  private static void assertRange(final HOTTrieReader reader, final Trie trie, final byte[] from, final byte[] to) {
    final List<byte[]> expected = expectedRange(trie.oracle, from, to);
    final List<byte[]> actual = scan(reader, trie.root, from, to);
    boolean equal = expected.size() == actual.size();
    for (int i = 0; equal && i < expected.size(); i++) {
      equal = Arrays.equals(expected.get(i), actual.get(i));
    }
    if (!equal) {
      fail("range(" + hex(from) + ", " + hex(to) + ") must be " + hex(expected) + " but was " + hex(actual));
    }
  }

  private static List<byte[]> expectedRange(final List<byte[]> oracle, final byte[] from, final byte[] to) {
    final int first = lowerIndex(oracle, from, false);
    final int afterLast = to == null
        ? oracle.size()
        : lowerIndex(oracle, to, true);
    return oracle.subList(first, Math.max(first, afterLast));
  }

  /** Index of the first oracle key {@code >= probe}, or {@code > probe} when {@code strict}. */
  private static int lowerIndex(final List<byte[]> oracle, final byte[] probe, final boolean strict) {
    int low = 0;
    int high = oracle.size();
    while (low < high) {
      final int middle = (low + high) >>> 1;
      final int comparison = Arrays.compareUnsigned(oracle.get(middle), probe);
      if (comparison < 0 || strict && comparison == 0) {
        low = middle + 1;
      } else {
        high = middle;
      }
    }
    return low;
  }

  private static byte[] keyAt(final List<byte[]> oracle, final int index) {
    return index == oracle.size()
        ? null
        : oracle.get(index);
  }

  private static void assertKey(final byte[] expected, final byte[] actual, final String description) {
    if (!Arrays.equals(expected, actual)) {
      fail(description + " must be " + hex(expected) + " but was " + hex(actual));
    }
  }

  private static void assertKeys(final List<byte[]> expected, final List<byte[]> actual, final String description) {
    boolean equal = expected.size() == actual.size();
    for (int i = 0; equal && i < expected.size(); i++) {
      equal = Arrays.equals(expected.get(i), actual.get(i));
    }
    if (!equal) {
      fail(description + " must be " + hex(expected) + " but was " + hex(actual));
    }
  }

  private static String hex(final List<byte[]> keys) {
    final StringBuilder text = new StringBuilder(keys.size() * 4 + 2).append('[');
    for (int i = 0; i < keys.size(); i++) {
      if (i > 0) {
        text.append(' ');
      }
      text.append(hex(keys.get(i)));
    }
    return text.append(']').toString();
  }

  private static String hex(final byte[] key) {
    return key == null
        ? "END"
        : key.length == 0
            ? "<empty>"
            : HEX.formatHex(key);
  }
}
