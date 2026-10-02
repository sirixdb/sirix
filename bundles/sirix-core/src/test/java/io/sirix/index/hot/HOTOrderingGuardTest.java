/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.BufferManager;
import io.sirix.cache.PageContainer;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.index.hot.AbstractHOTIndexWriter.LeafNavigationResult;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.PathPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Sparse routing can disagree with key order even when every stored key routes correctly.
 *
 * <p>
 * The pair and multi-child insertions failed on unmodified main in {@code h:pair-leaf} and
 * {@code h:fold-multi}, respectively, with a malformed published path. The full-frontier fixtures
 * drive the writer's structural candidate builder and its strand handler directly, one candidate or
 * one discharge at a time: main declined an otherwise valid candidate because slice compression put
 * bit 4 above a child branching on bit 3 (I11), aborted outright where a half had no canonical
 * block at all, and published a leaf slot stretched across its neighbour. They do not claim an
 * ordinary insertion reaches each of those candidates for these small tries.
 * </p>
 */
final class HOTOrderingGuardTest {

  @Test
  void pairMaximumMustNotCrossAnAncestorsNextSibling() {
    try (final Fixture fixture = new Fixture()) {
      final PageReference child = fixture.node(new int[] {1, 4}, new int[] {0, 1, 2}, fixture.leaf(0x00),
          fixture.leaf(0x08), fixture.leaf(0x40));
      fixture.install(
          fixture.node(new int[] {0, 3}, new int[] {0, 1, 2}, child, fixture.leaf(0x50), fixture.leaf(0x80)));
      fixture.assertKeys(0x00, 0x08, 0x40, 0x50, 0x80);
      final long delegated = AbstractHOTIndexWriter.BRANCH_SPINE_ORDER_DELEGATED.get();
      assertDoesNotThrow(() -> fixture.writer.insert(0x60), () -> "handler=" + fixture.writer.lastDispatchHandler);
      fixture.assertKeys(0x00, 0x08, 0x40, 0x50, 0x60, 0x80);
      // The branch guard catches this shape before the leaf-pair handler now. Exercise the pair
      // predicate independently below so its ancestor walk remains covered too.
      assertTrue(AbstractHOTIndexWriter.BRANCH_SPINE_ORDER_DELEGATED.get() > delegated);
    }
  }

  @Test
  void branchMaximumMustNotCrossAnAncestorsNextSibling() {
    try (final Fixture fixture = new Fixture()) {
      final PageReference child = fixture.node(new int[] {1, 4}, new int[] {0, 2, 3}, fixture.leaf(0x00),
          fixture.leaf(0x40), fixture.leaf(0x48));
      fixture.install(
          fixture.node(new int[] {0, 3}, new int[] {0, 1, 2}, child, fixture.leaf(0x50), fixture.leaf(0x80)));
      fixture.assertKeys(0x00, 0x40, 0x48, 0x50, 0x80);
      final long delegated = AbstractHOTIndexWriter.BRANCH_SPINE_ORDER_DELEGATED.get();
      assertDoesNotThrow(() -> fixture.writer.insert(0x60), () -> "handler=" + fixture.writer.lastDispatchHandler);
      fixture.assertKeys(0x00, 0x40, 0x48, 0x50, 0x60, 0x80);
      assertTrue(AbstractHOTIndexWriter.BRANCH_SPINE_ORDER_DELEGATED.get() > delegated);
    }
  }

  @Test
  void boundaryPlacementMustNotCrossItsNextSibling() {
    try (final Fixture fixture = new Fixture()) {
      final PageReference child = fixture.node(new int[] {5}, new int[] {0, 1}, fixture.leaf(0x00), fixture.leaf(0x04));
      fixture.install(
          fixture.node(new int[] {0, 3}, new int[] {0, 1, 2}, child, fixture.leaf(0x10), fixture.leaf(0x80)));
      fixture.assertKeys(0x00, 0x04, 0x10, 0x80);
      fixture.writer.insert(0x20);
      fixture.assertKeys(0x00, 0x04, 0x10, 0x20, 0x80);
    }
  }

  @Test
  void frontierHalfMustKeepItsChildsMoreSignificantBit() {
    try (final Fixture fixture = new Fixture()) {
      final PageReference child = fixture.node(new int[] {3}, new int[] {0, 1}, fixture.leaf(0x80), fixture.leaf(0x90));
      fixture.install(
          fixture.node(new int[] {0, 4}, new int[] {0, 2, 3}, fixture.leaf(0x00), child, fixture.leaf(0x98)));
      fixture.assertKeys(0x00, 0x80, 0x90, 0x98);
      fixture.writer.insert(0x88);
      fixture.assertKeys(0x00, 0x80, 0x88, 0x90, 0x98);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void persistentFrontierSplitMustRecanonicalizeAHalfThatDropsAChildsBit(final boolean lowerHalf) throws Exception {
    try (final Fixture fixture = new Fixture()) {
      final PageReference child = fixture.node(new int[] {3}, new int[] {0, 1}, fixture.leaf(lowerHalf
          ? 0x00
          : 0x80), fixture.leaf(
              lowerHalf
                  ? 0x10
                  : 0x90));
      if (lowerHalf) {
        fixture.install(
            fixture.node(new int[] {0, 4}, new int[] {0, 1, 2}, child, fixture.leaf(0x18), fixture.leaf(0x80)));
        fixture.assertKeys(0x00, 0x10, 0x18, 0x80);
      } else {
        fixture.install(
            fixture.node(new int[] {0, 4}, new int[] {0, 2, 3}, fixture.leaf(0x00), child, fixture.leaf(0x98)));
        fixture.assertKeys(0x00, 0x80, 0x90, 0x98);
      }
      final long recanonicalized = AbstractHOTIndexWriter.FRONTIER_SPLIT_HALF_RECANONICALIZED.get();
      // Splitting before 78 drops bit 0 from the retained half, leaving bit 4 above the child
      // that still branches on bit 3. Drive the full candidate, not the smaller leaf-only frontier.
      final Method splice = Arrays.stream(AbstractHOTIndexWriter.class.getDeclaredMethods())
                                  .filter(method -> method.getName().equals("trySpliceCompleteFrontier"))
                                  .findFirst()
                                  .orElseThrow();
      splice.setAccessible(true);
      final Constructor<?> frontierConstructor =
          splice.getParameterTypes()[3].getDeclaredConstructor(int.class, int.class);
      frontierConstructor.setAccessible(true);
      final Object frontier = frontierConstructor.newInstance(0, 3);
      final Object absent = Arrays.stream(splice.getParameterTypes()[6].getEnumConstants())
                                  .filter(value -> value.toString().equals("ABSENT"))
                                  .findFirst()
                                  .orElseThrow();
      final LeafNavigationResult route = fixture.writer.prepareLeafOfTree(fixture.root, key(0x78), 1);
      // The normal caller lends resident children to the resolver-free compression primitive.
      final Method loadChildren =
          AbstractHOTIndexWriter.class.getDeclaredMethod("ensureNodeChildrenLoaded", HOTIndirectPage.class);
      loadChildren.setAccessible(true);
      loadChildren.invoke(fixture.writer, route.pathNodes()[0]);
      try {
        assertEquals(true,
            splice.invoke(fixture.writer, route, route.pathNodes()[0], 0, frontier, key(0x78), key(0x78), absent),
            "a complete frontier over a valid trie must produce an invariant-clean candidate");
      } catch (final InvocationTargetException failure) {
        throw new AssertionError("the complete frontier must accept a valid source trie", failure.getCause());
      }
      if (lowerHalf) {
        fixture.assertKeys(0x00, 0x10, 0x18, 0x78, 0x80);
      } else {
        fixture.assertKeys(0x00, 0x78, 0x80, 0x90, 0x98);
      }
      assertTrue(AbstractHOTIndexWriter.FRONTIER_SPLIT_HALF_RECANONICALIZED.get() > recanonicalized);
    }
  }

  @Test
  void aHalfWithNoCanonicalBlockMustDeclineTheCandidate() throws Exception {
    try (final Fixture fixture = new Fixture()) {
      final PageReference source = installFrontierWithoutACanonicalHalf(fixture);
      fixture.assertKeys(FRONTIER_WITHOUT_A_CANONICAL_HALF_KEYS);

      final long recanonicalized = AbstractHOTIndexWriter.FRONTIER_SPLIT_HALF_RECANONICALIZED.get();
      final Method splice = Arrays.stream(AbstractHOTIndexWriter.class.getDeclaredMethods())
                                  .filter(method -> method.getName().equals("trySpliceCompleteFrontier"))
                                  .findFirst()
                                  .orElseThrow();
      splice.setAccessible(true);
      final Constructor<?> frontierConstructor =
          splice.getParameterTypes()[3].getDeclaredConstructor(int.class, int.class);
      frontierConstructor.setAccessible(true);
      final Object absent = Arrays.stream(splice.getParameterTypes()[6].getEnumConstants())
                                  .filter(value -> value.toString().equals("ABSENT"))
                                  .findFirst()
                                  .orElseThrow();
      final LeafNavigationResult route = fixture.writer.prepareLeafOfTree(fixture.root, key(0x80), 1);
      final Method loadChildren =
          AbstractHOTIndexWriter.class.getDeclaredMethod("ensureNodeChildrenLoaded", HOTIndirectPage.class);
      loadChildren.setAccessible(true);
      loadChildren.invoke(fixture.writer, route.pathNodes()[0]);
      try {
        assertEquals(false,
            splice.invoke(fixture.writer, route, route.pathNodes()[0], 0, frontierConstructor.newInstance(0, 32),
                key(0x80), key(0x80), absent),
            "a half with no canonical block must reject the candidate, not abort the insert");
      } catch (final InvocationTargetException failure) {
        throw new AssertionError("the candidate must be declined, never aborted", failure.getCause());
      }

      assertEquals(recanonicalized, AbstractHOTIndexWriter.FRONTIER_SPLIT_HALF_RECANONICALIZED.get(),
          "a half that never produced a block must not be counted as recanonicalized");
      // The declined candidate leaves the trie exactly as it was, down to the straddlers' own leaves.
      fixture.assertKeys(FRONTIER_WITHOUT_A_CANONICAL_HALF_KEYS);
      assertEquals(32, assertInstanceOf(HOTIndirectPage.class, fixture.page(source)).getNumChildren());
    }
  }

  /**
   * The decline keeps the retry loop in control of the refusal, which is the whole of what it buys
   * here: no wider or higher frontier can publish after it, because the dead end belongs to the
   * boundary child and the boundary key, not to the frontier's width.
   * {@code splitSubtreeBeforeKey} always descends through {@link
   * AbstractHOTIndexWriter#lexicographicBoundaryChild}, so every wider frontier reaches the same
   * child and splits it at the same point; and where a wider frontier would avoid that child, the
   * join around {@code K} straddles the compressed source at the same bit and re-derives the very
   * same column-dropped slice over it. The fan-out dead end itself needs 31 parts in one slice,
   * which only the full width of a 32-child node supplies — a narrower frontier has strictly fewer
   * parts, so it cannot dead-end where a wider one succeeds. A successful retry after <em>this</em>
   * decline is therefore not constructible; the retry legs it does enable are the ones the other
   * candidate checks reject ({@code freshStructuralPagesMalformed}, {@code
   * canPropagateIncrementalSplice}), which are pre-existing and unchanged.
   */
  @Test
  void aDeclinedRecanonicalizationMustStayInsideTheRetryLoop() {
    try (final Fixture fixture = new Fixture()) {
      installFrontierWithoutACanonicalHalf(fixture);
      fixture.assertKeys(FRONTIER_WITHOUT_A_CANONICAL_HALF_KEYS);
      final long declined = AbstractHOTIndexWriter.FRONTIER_SPLIT_HALF_DECLINED.get();
      final long spliced = AbstractHOTIndexWriter.COMPLETE_STRUCTURAL_FRONTIER_SPLICE.get();

      // The decline hands control back to the retry loop instead of aborting from inside the split:
      // the loop attempts the minimal frontier and then the whole bounded block at that level, and
      // only when no frontier can build a clean candidate does it refuse with its own error.
      final IllegalStateException refusal =
          assertThrows(IllegalStateException.class, () -> fixture.writer.insert(0x80));
      assertTrue(refusal.getMessage().startsWith("HOT could not construct an invariant-clean incremental frontier"),
          () -> "the loop must exhaust every frontier before refusing, got: " + refusal);
      assertTrue(AbstractHOTIndexWriter.FRONTIER_SPLIT_HALF_DECLINED.get() >= declined + 2,
          "both frontier attempts at that level must reach the declining half");
      assertEquals(spliced, AbstractHOTIndexWriter.COMPLETE_STRUCTURAL_FRONTIER_SPLICE.get(),
          "no candidate may be published while every frontier declines");
      // Nothing a declined attempt built survives: the trie still holds exactly its own keys, in order.
      fixture.assertKeys(FRONTIER_WITHOUT_A_CANONICAL_HALF_KEYS);
    }
  }

  /** The keys {@link #installFrontierWithoutACanonicalHalf} stores, in order. */
  private static final int[] FRONTIER_WITHOUT_A_CANONICAL_HALF_KEYS =
      {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11,
          0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1E, 0x3E, 0x3F, 0x7F, 0xC0};

  /**
   * A valid trie for which no half of the frontier before {@code 0x80} has a canonical block. Its
   * root holds 32 children, the last of which is the only one setting the mask's most significant
   * column, so the split keeps 31 of them and the retained slice drops that column — leaving bit 2
   * above both of the children that discriminate on it, exactly as a Direction-1 sub-insert leaves
   * them. The slice therefore has to be recanonicalized, and its join reaches the 32-part fan-out on
   * the first straddler and then needs the second one too.
   *
   * <p>
   * Every key routes to its own leaf and every sparse partial is a subset of its subtree's keys
   * ({@link Fixture#assertKeys} and {@link HOTInvariantValidator} check both), so the refusal is a
   * property of the frontier, not of a malformed fixture.
   * </p>
   *
   * @return the installed root
   */
  private static PageReference installFrontierWithoutACanonicalHalf(final Fixture fixture) {
    final PageReference[] children = new PageReference[32];
    final int[] partials = new int[32];
    for (int slot = 0; slot < 29; slot++) {
      children[slot] = fixture.leaf(slot);
      partials[slot] = slot;
    }
    children[29] = fixture.node(new int[] {2}, new int[] {0, 1}, fixture.leaf(0x1E), fixture.leaf(0x3E));
    partials[29] = 0x1E;
    children[30] = fixture.node(new int[] {1}, new int[] {0, 1}, fixture.leaf(0x3F), fixture.leaf(0x7F));
    partials[30] = 0x3F;
    children[31] = fixture.leaf(0xC0);
    partials[31] = 0xC0;
    final PageReference root = fixture.node(new int[] {0, 1, 2, 3, 4, 5, 6, 7}, partials, children);
    fixture.install(root);
    return root;
  }

  @Test
  void pairGuardChecksAncestorMaximumAndAcceptsAnIndexExtreme() throws Exception {
    try (final Fixture fixture = new Fixture()) {
      final PageReference child = fixture.node(new int[] {1, 4}, new int[] {0, 1, 2}, fixture.leaf(0x00),
          fixture.leaf(0x08), fixture.leaf(0x40));
      fixture.install(
          fixture.node(new int[] {0, 3}, new int[] {0, 1, 2}, child, fixture.leaf(0x50), fixture.leaf(0x80)));
      fixture.assertKeys(0x00, 0x08, 0x40, 0x50, 0x80);
      final LeafNavigationResult route = fixture.writer.prepareLeafOfTree(fixture.root, key(0x40), 1);
      assertTrue(fixture.guard("pairKeepsSpineOrder", route, 1, 0x4f));
      assertFalse(fixture.guard("pairKeepsSpineOrder", route, 1, 0x50), "equality also overlaps the neighbour");
      assertFalse(fixture.guard("pairKeepsSpineOrder", route, 1, 0x60), "the leaf's parent has no next sibling slot");
      final LeafNavigationResult last = fixture.writer.prepareLeafOfTree(fixture.root, key(0x80), 1);
      assertTrue(fixture.guard("pairKeepsSpineOrder", last, 1, 0xff));
    }
  }

  @Test
  void strandDischargeMustNotMoveTheLeafSlotPastItsNeighbour() throws Exception {
    try (final Fixture fixture = new Fixture()) {
      // Bit 6 tells the two leaves apart, so 0x4C routes to the first one (a zero column claims
      // nothing) although it sorts after the second one's 0x4A.
      final PageReference child =
          fixture.node(new int[] {6}, new int[] {0, 1}, fixture.leaf(0x48, 0x49), fixture.leaf(0x4A));
      fixture.install(fixture.node(new int[] {0}, new int[] {0, 1}, child, fixture.leaf(0x80)));
      fixture.assertKeys(0x48, 0x49, 0x4A, 0x80);
      final Method discharge = AbstractHOTIndexWriter.class.getDeclaredMethod("strandDischargeSplitIntegrate",
          LeafNavigationResult.class, byte[].class, byte[].class);
      discharge.setAccessible(true);

      // The handler keeps the descended leaf's slot, so 0x4C would stretch it across the next
      // sibling's whole range — an integrate cascade that folds cleanly and still breaks I12.
      assertEquals(false, discharge.invoke(fixture.writer, fixture.writer.prepareLeafOfTree(fixture.root, key(0x4C), 1),
          key(0x4C), key(0x4C)), "a strand discharge that moves the leaf slot past its neighbour must decline");
      fixture.assertKeys(0x48, 0x49, 0x4A, 0x80);
    }
  }

  @Test
  void spineGuardsCheckMinimumMaximumAndTheNearestNeighbour() throws Exception {
    try (final Fixture fixture = new Fixture()) {
      final PageReference child = fixture.node(new int[] {2, 4}, new int[] {0, 1, 2}, fixture.leaf(0x40),
          fixture.leaf(0x48), fixture.leaf(0x60));
      fixture.install(
          fixture.node(new int[] {0, 1}, new int[] {0, 1, 2}, fixture.leaf(0x20), child, fixture.leaf(0x80)));
      fixture.assertKeys(0x20, 0x40, 0x48, 0x60, 0x80);
      final LeafNavigationResult first = fixture.writer.prepareLeafOfTree(fixture.root, key(0x40), 1);
      assertTrue(fixture.guard("pairKeepsSpineOrder", first, 0, 0x30));
      assertFalse(fixture.guard("pairKeepsSpineOrder", first, 0, 0x20));
      assertFalse(fixture.guard("pairKeepsSpineOrder", first, 0, 0x10));
      assertTrue(fixture.guard("keyKeepsSpineOrder", first, 1, 0x50), "inside the subtree's range");
      assertTrue(fixture.guard("keyKeepsSpineOrder", first, 1, 0x30));
      assertFalse(fixture.guard("keyKeepsSpineOrder", first, 1, 0x20));
      assertTrue(fixture.guard("keyKeepsSpineOrder", first, 1, 0x70));
      assertFalse(fixture.guard("keyKeepsSpineOrder", first, 1, 0x80));
      assertTrue(fixture.guard("keyKeepsSpineOrder", first, 0, 0xff), "the root has no ancestor boundary");

      final LeafNavigationResult middle = fixture.writer.prepareLeafOfTree(fixture.root, key(0x48), 1);
      assertFalse(fixture.guard("pairKeepsSpineOrder", middle, 0, 0x40));
      assertTrue(fixture.guard("pairKeepsSpineOrder", middle, 0, 0x44));
      assertFalse(fixture.guard("pairKeepsSpineOrder", middle, 1, 0x60));
      assertTrue(fixture.guard("pairKeepsSpineOrder", middle, 1, 0x58));
      assertFalse(fixture.guard("keyKeepsSpineOrder", middle, middle.pathDepth(), 0x40));
      assertFalse(fixture.guard("keyKeepsSpineOrder", middle, middle.pathDepth(), 0x60));
      final LeafNavigationResult minimum = fixture.writer.prepareLeafOfTree(fixture.root, key(0x20), 1);
      assertTrue(fixture.guard("pairKeepsSpineOrder", minimum, 0, 0x00));
    }
  }

  @Test
  void aSplitHalfPlacementMustKeepBothExtremesOfItsSlot() throws Exception {
    try (final Fixture fixture = new Fixture()) {
      final PageReference child = fixture.node(new int[] {2, 4}, new int[] {0, 1, 2}, fixture.leaf(0x40),
          fixture.leaf(0x48), fixture.leaf(0x60));
      fixture.install(
          fixture.node(new int[] {0, 1}, new int[] {0, 1, 2}, fixture.leaf(0x20), child, fixture.leaf(0x80)));
      fixture.assertKeys(0x20, 0x40, 0x48, 0x60, 0x80);
      final LeafNavigationResult route = fixture.writer.prepareLeafOfTree(fixture.root, key(0x48), 1);
      final HOTIndirectPage half = route.pathNodes()[1];

      // The sub-insert target is the half's middle slot, so both its neighbours end the propagation.
      assertTrue(fixture.splitHalfGuard(route, 1, half, false, 1, 0x48), "inside the target subtree's range");
      assertTrue(fixture.splitHalfGuard(route, 1, half, false, 1, 0x44));
      assertFalse(fixture.splitHalfGuard(route, 1, half, false, 1, 0x40), "the new minimum reaches its neighbour");
      assertTrue(fixture.splitHalfGuard(route, 1, half, false, 1, 0x50));
      assertFalse(fixture.splitHalfGuard(route, 1, half, false, 1, 0x60), "the new maximum reaches its neighbour");

      // The half's last slot: its maximum is the upper half's, hence the split subtree's, so the
      // boundary with d*'s own next sibling decides.
      assertTrue(fixture.splitHalfGuard(route, 1, half, true, 2, 0x70));
      assertFalse(fixture.splitHalfGuard(route, 1, half, true, 2, 0x80), "the ancestor's next sibling starts there");
      assertTrue(fixture.splitHalfGuard(route, 1, half, false, 2, 0x80), "the upper half still carries the maximum");

      // The mirror on the minimum side, which the lower half carries.
      assertTrue(fixture.splitHalfGuard(route, 1, half, false, 0, 0x30));
      assertFalse(fixture.splitHalfGuard(route, 1, half, false, 0, 0x20), "the ancestor's previous sibling ends there");
      assertTrue(fixture.splitHalfGuard(route, 1, half, true, 0, 0x10), "the lower half still carries the minimum");
    }
  }

  @Test
  void aFullNodeSplitMustKeepTheKeyOnItsOwnHalf() throws Exception {
    try (final Fixture fixture = new Fixture()) {
      final PageReference child = fixture.node(new int[] {2, 4}, new int[] {0, 1, 2}, fixture.leaf(0x40),
          fixture.leaf(0x48), fixture.leaf(0x60));
      final PageReference rootRef =
          fixture.node(new int[] {0, 1}, new int[] {0, 1, 2}, fixture.leaf(0x20), child, fixture.leaf(0x80));
      fixture.install(rootRef);
      fixture.assertKeys(0x20, 0x40, 0x48, 0x60, 0x80);
      final HOTIndirectPage inner = assertInstanceOf(HOTIndirectPage.class, fixture.page(child));
      final HOTIndirectPage root = assertInstanceOf(HOTIndirectPage.class, fixture.page(rootRef));

      // inner splits at bit 2 into [0x40, 0x48] and [0x60]; root at bit 0 into [0x20, 0x60] and [0x80].
      assertTrue(fixture.halvesKeepKeyApart(inner, true, 0x50));
      assertFalse(fixture.halvesKeepKeyApart(inner, true, 0x44), "the lower half already holds that range");
      assertTrue(fixture.halvesKeepKeyApart(inner, false, 0x50));
      assertFalse(fixture.halvesKeepKeyApart(inner, false, 0x70), "the upper half already holds that range");
      assertTrue(fixture.halvesKeepKeyApart(root, true, 0x70));
      assertFalse(fixture.halvesKeepKeyApart(root, true, 0x50));
      assertTrue(fixture.halvesKeepKeyApart(root, false, 0x70));
      assertFalse(fixture.halvesKeepKeyApart(root, false, 0x90));
    }
  }

  @Test
  void aSliceMsbMustBeFoundInTheSignBitColumnToo() throws Exception {
    try (final Fixture fixture = new Fixture()) {
      // 32 discriminative bits make column 0 weigh 1 << 31, so its extracted value is negative. The
      // gap at bit 1 leaves room for a child whose own MSB sits above every other column.
      final int[] bits = new int[32];
      for (int column = 1; column < 32; column++) {
        bits[column] = column + 1;
      }
      final PageReference lowStraddler = fixture.node(new int[] {1}, new int[] {0, 1},
          fixture.wideLeaf(0x20000000), fixture.wideLeaf(0x60000000));
      final PageReference highStraddler = fixture.node(new int[] {1}, new int[] {0, 1},
          fixture.wideLeaf(0xA0000000), fixture.wideLeaf(0xE0000000));
      final PageReference lowest = fixture.wideLeaf(0x00000000);
      final PageReference signBitChild = fixture.wideLeaf(0x80000000);
      final PageReference nodeRef = fixture.node(bits, new int[] {0x00000000, 0x40000000, 0x80000000, 0xC0000000},
          lowest, lowStraddler, signBitChild, highStraddler);
      final HOTIndirectPage node = assertInstanceOf(HOTIndirectPage.class, fixture.page(nodeRef));
      assertEquals(0, node.getMostSignificantBitIndex(), "the sign-bit column is the node's own MSB");

      // The whole slice varies in the sign-bit column alone, which is more significant than either
      // straddler's bit 1: the plain compression keeps the trie condition.
      assertTrue(fixture.sliceKeepsTrieCondition(node, 0, 4, 0, lowest));
      // Dropping that column leaves bit 2 above the straddler's bit 1, and a slice whose first
      // retained partial has the sign bit set still finds its true column.
      assertFalse(fixture.sliceKeepsTrieCondition(node, 0, 2, 0, lowest));
      assertFalse(fixture.sliceKeepsTrieCondition(node, 2, 4, 2, signBitChild));
      // Removing the only child that sets the sign-bit column drops it from the slice as well.
      assertTrue(fixture.sliceKeepsTrieCondition(node, 0, 3, 0, lowest));
      assertFalse(fixture.sliceKeepsTrieCondition(node, 0, 3, 2, null));

      // The sign-bit column is the one a running "not yet seen" sentinel of -1 cannot tell from a
      // real value, and only a slice whose FIRST retained partial sets it can expose that: every
      // later slot re-enters the sentinel arm, so the column is reported constant however it
      // varies. I7 (partials strictly ascending unsigned) makes that unreachable on a well-formed
      // node, so the decision is pinned here on a node whose partials descend deliberately — the
      // predicate reads columns and child MSBs, and must not borrow another node's ordering.
      final PageReference signBitRoot = fixture.node(new int[] {0}, new int[] {0, 1},
          fixture.wideLeaf(0x00000000), fixture.wideLeaf(0x80000000));
      final PageReference descending = fixture.node(bits, new int[] {0x80000000, 0x00000000}, signBitRoot,
          fixture.wideLeaf(0x40000000));
      assertEquals(0,
          assertInstanceOf(HOTIndirectPage.class, fixture.page(signBitRoot)).getMostSignificantBitIndex());
      assertFalse(
          fixture.sliceKeepsTrieCondition(assertInstanceOf(HOTIndirectPage.class, fixture.page(descending)), 0, 2, 0,
              signBitRoot),
          "the sign-bit column varies across this slice, so it is the slice's MSB and no child may share it");
    }
  }

  @Test
  void anUnorderedSliceMustDeclineInsteadOfAbortingTheSplit() throws Exception {
    try (final Fixture fixture = new Fixture()) {
      // The join's inputs are the node's own children here, so their ranges are an assumption about
      // stored data. Both ways of breaking it must decline, never throw out of the split.
      final PageReference interleaved = fixture.leaf(0x40, 0x60);
      final PageReference inside = fixture.leaf(0x50);
      final PageReference overlapping =
          fixture.node(new int[] {0}, new int[] {0, 1}, interleaved, inside);
      assertNull(fixture.recanonicalizeChildSlice(
          assertInstanceOf(HOTIndirectPage.class, fixture.page(overlapping)), 0, 2, 0, interleaved));

      final PageReference empty = fixture.leaf();
      final PageReference populated = fixture.leaf(0x50);
      final PageReference unresolvable = fixture.node(new int[] {0}, new int[] {0, 1}, empty, populated);
      assertNull(fixture.recanonicalizeChildSlice(
          assertInstanceOf(HOTIndirectPage.class, fixture.page(unresolvable)), 0, 2, 0, empty));

      for (final PageReference reference : List.of(interleaved, inside, empty, populated)) {
        assertFalse(assertInstanceOf(HOTLeafPage.class, fixture.page(reference)).isClosed(),
            "a declining slice must not retire a leaf the trie still owns");
      }
    }
  }

  private static byte[] key(final int value) {
    return new byte[] {(byte) value};
  }

  /** A four-byte key, so a node can carry the full 32 discriminative bits a partial key holds. */
  private static byte[] wideKey(final int value) {
    return new byte[] {(byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value};
  }

  private static final class Fixture implements AutoCloseable {
    private final TransactionIntentLog log =
        new TransactionIntentLog(mock(BufferManager.class, RETURNS_DEEP_STUBS), 64);
    private final AtomicLong pageKeys = new AtomicLong(100);
    private final StorageEngineWriter storage = mock(StorageEngineWriter.class, RETURNS_DEEP_STUBS);
    private final TestWriter writer;
    private PageReference root;

    private Fixture() {
      final RevisionRootPage revisionRoot = mock(RevisionRootPage.class);
      final PathPage pathPage = mock(PathPage.class);
      when(storage.getLog()).thenReturn(log);
      when(storage.getRevisionNumber()).thenReturn(2);
      when(storage.getActualRevisionRootPage()).thenReturn(revisionRoot);
      when(storage.getPathPage(revisionRoot)).thenReturn(pathPage);
      doReturn(pathPage).when(storage).prepareSecondaryIndexPage(IndexType.PATH);
      when(pathPage.incrementAndGetMaxHotPageKey(0)).thenAnswer(ignored -> pageKeys.getAndIncrement());
      when(storage.loadHOTPage(any(PageReference.class))).thenAnswer(invocation -> page(invocation.getArgument(0)));
      writer = new TestWriter(storage);
    }

    private PageReference leaf(final int... keys) {
      final HOTLeafPage leaf = new HOTLeafPage(pageKeys.getAndIncrement(), 1, IndexType.PATH);
      for (final int value : keys) {
        assertTrue(leaf.put(key(value), key(value)));
      }
      return register(leaf);
    }

    private PageReference wideLeaf(final int... keys) {
      final HOTLeafPage leaf = new HOTLeafPage(pageKeys.getAndIncrement(), 1, IndexType.PATH);
      for (final int value : keys) {
        assertTrue(leaf.put(wideKey(value), wideKey(value)));
      }
      return register(leaf);
    }

    private PageReference node(final int[] bits, final int[] partials, final PageReference... children) {
      int height = 1;
      for (final PageReference child : children) {
        if (page(child) instanceof HOTIndirectPage indirect) {
          height = Math.max(height, indirect.getHeight() + 1);
        }
      }
      return register(HOTBulkBuilder.assembleIndirect(bits, partials, children, height, 1, pageKeys::getAndIncrement));
    }

    private PageReference register(final Page page) {
      final PageReference reference = new PageReference();
      reference.setPage(page);
      log.put(reference, PageContainer.getInstance(page, page));
      return reference;
    }

    private Page page(final PageReference reference) {
      final PageContainer container = log.get(reference);
      return container == null
          ? reference.getPage()
          : container.getModified();
    }

    private void install(final PageReference reference) {
      root = reference;
      writer.rootReference = reference;
    }

    private void assertKeys(final int... expected) {
      HOTInvariantValidator.validate(root, storage).assertOk();
      final List<Integer> actual = new ArrayList<>();
      collect(root, actual);
      assertEquals(Arrays.stream(expected).boxed().toList(), actual, "physical traversal must be exact and ordered");
      for (final int value : expected) {
        Page current = page(root);
        for (int depth = 0; current instanceof HOTIndirectPage node && depth < 32; depth++) {
          current = page(node.getChildReference(node.findChildIndex(key(value))));
        }
        final HOTLeafPage leaf = assertInstanceOf(HOTLeafPage.class, current);
        assertTrue(leaf.findEntry(key(value)) >= 0, "key must route to its owning leaf: " + value);
        assertArrayEquals(key(value), leaf.copyStoredValue(leaf.findEntry(key(value))));
      }
    }

    private boolean guard(final String name, final LeafNavigationResult route, final int placement, final int value)
        throws ReflectiveOperationException {
      final Method method =
          AbstractHOTIndexWriter.class.getDeclaredMethod(name, LeafNavigationResult.class, int.class, byte[].class);
      method.setAccessible(true);
      return (boolean) method.invoke(writer, route, placement, key(value));
    }

    private boolean splitHalfGuard(final LeafNavigationResult route, final int insertDepth,
        final HOTIndirectPage half, final boolean rightHalf, final int affectedIdx, final int value)
        throws ReflectiveOperationException {
      final Method method = AbstractHOTIndexWriter.class.getDeclaredMethod("isSplitHalfDirectionOneSafe",
          LeafNavigationResult.class, int.class, HOTIndirectPage.class, boolean.class, int.class, byte[].class);
      method.setAccessible(true);
      return (boolean) method.invoke(writer, route, insertDepth, half, rightHalf, affectedIdx, key(value));
    }

    private boolean halvesKeepKeyApart(final HOTIndirectPage node, final boolean keyJoinsUpperHalf, final int value)
        throws ReflectiveOperationException {
      final Method method = AbstractHOTIndexWriter.class.getDeclaredMethod("splitHalvesKeepKeyApart",
          HOTIndirectPage.class, boolean.class, byte[].class);
      method.setAccessible(true);
      return (boolean) method.invoke(writer, node, keyJoinsUpperHalf, key(value));
    }

    private boolean sliceKeepsTrieCondition(final HOTIndirectPage node, final int fromInclusive,
        final int toExclusive, final int replacedChildIndex, final PageReference replacement)
        throws ReflectiveOperationException {
      final Method method = AbstractHOTIndexWriter.class.getDeclaredMethod("sliceKeepsTrieCondition",
          HOTIndirectPage.class, int.class, int.class, int.class, PageReference.class);
      method.setAccessible(true);
      return (boolean) method.invoke(writer, node, fromInclusive, toExclusive, replacedChildIndex, replacement);
    }

    private Object recanonicalizeChildSlice(final HOTIndirectPage node, final int fromInclusive,
        final int toExclusive, final int replacedChildIndex, final PageReference replacement) throws Exception {
      final Method method = AbstractHOTIndexWriter.class.getDeclaredMethod("recanonicalizeChildSlice",
          HOTIndirectPage.class, int.class, int.class, int.class, PageReference.class, int.class, List.class);
      method.setAccessible(true);
      try {
        return method.invoke(writer, node, fromInclusive, toExclusive, replacedChildIndex, replacement, 2,
            new ArrayList<PageReference>());
      } catch (final InvocationTargetException failure) {
        throw new AssertionError("an unorderable slice must be declined, never aborted", failure.getCause());
      }
    }

    private void collect(final PageReference reference, final List<Integer> actual) {
      final Page current = page(reference);
      if (current instanceof HOTLeafPage leaf) {
        for (int i = 0; i < leaf.getEntryCount(); i++) {
          actual.add(Byte.toUnsignedInt(leaf.getKey(i)[0]));
        }
      } else {
        final HOTIndirectPage node = assertInstanceOf(HOTIndirectPage.class, current);
        for (int i = 0; i < node.getNumChildren(); i++) {
          final PageReference child = node.getChildReference(i);
          if (page(child) instanceof HOTIndirectPage indirect) {
            assertTrue(indirect.getMostSignificantBitIndex() > node.getMostSignificantBitIndex(),
                "I11: child must discriminate below its parent");
          }
          collect(child, actual);
        }
      }
    }

    @Override
    public void close() {
      log.close();
    }
  }

  private static final class TestWriter extends AbstractHOTIndexWriter<byte[]> {
    private byte[] keyBuffer = new byte[8];

    private TestWriter(final StorageEngineWriter storage) {
      super(storage, IndexType.PATH, 0);
    }

    private void insert(final int value) {
      doIndex(key(value), 1, key(value), 1);
    }

    @Override
    protected byte[] getKeyBuffer() {
      return keyBuffer;
    }

    @Override
    protected void setKeyBuffer(final byte[] buffer) {
      keyBuffer = buffer;
    }

    @Override
    protected int serializeKey(final byte[] key, final byte[] buffer, final int offset) {
      System.arraycopy(key, 0, buffer, offset, key.length);
      return key.length;
    }

    @Override
    protected void prepareIndexPage() {
      // The fixture installs a TIL-owned root directly.
    }
  }
}
