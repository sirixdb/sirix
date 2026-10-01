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
      // Two children straddle a bit of their parent's mask, as a Direction-1 sub-insert leaves them.
      // Splitting before 0x7F keeps 31 of the 32 children, which drops the slice's most significant
      // column and leaves bit 1 above the first straddler: the half has to be recanonicalized. Its
      // join splits that straddler, reaching the 32-part fan-out, and then needs the second one too,
      // so no canonical block exists for this half.
      final PageReference[] children = new PageReference[32];
      final int[] partials = new int[32];
      for (int slot = 0; slot < 20; slot++) {
        children[slot] = fixture.leaf(slot);
        partials[slot] = slot;
      }
      children[20] = fixture.node(new int[] {1}, new int[] {0, 1}, fixture.leaf(0x3E), fixture.leaf(0x40));
      partials[20] = 0x3E;
      for (int slot = 21; slot < 28; slot++) {
        children[slot] = fixture.leaf(0x41 + slot - 21);
        partials[slot] = 0x41 + slot - 21;
      }
      children[28] = fixture.node(new int[] {2}, new int[] {0, 1}, fixture.leaf(0x5E), fixture.leaf(0x60));
      partials[28] = 0x5E;
      children[29] = fixture.leaf(0x61);
      partials[29] = 0x61;
      children[30] = fixture.leaf(0x7E);
      partials[30] = 0x7E;
      children[31] = fixture.leaf(0x80);
      partials[31] = 0x80;
      final PageReference source = fixture.node(new int[] {0, 1, 2, 3, 4, 5, 6, 7}, partials, children);
      fixture.install(fixture.node(new int[] {0}, new int[] {0, 1}, source, fixture.leaf(0xC0)));

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
      final LeafNavigationResult route = fixture.writer.prepareLeafOfTree(fixture.root, key(0x7F), 1);
      final Method loadChildren =
          AbstractHOTIndexWriter.class.getDeclaredMethod("ensureNodeChildrenLoaded", HOTIndirectPage.class);
      loadChildren.setAccessible(true);
      loadChildren.invoke(fixture.writer, route.pathNodes()[0]);
      try {
        assertEquals(false,
            splice.invoke(fixture.writer, route, route.pathNodes()[0], 0, frontierConstructor.newInstance(0, 1),
                key(0x7F), key(0x7F), absent),
            "a half with no canonical block must reject the candidate, not abort the insert");
      } catch (final InvocationTargetException failure) {
        throw new AssertionError("the candidate must be declined, never aborted", failure.getCause());
      }

      // The declined candidate leaves the source trie and the straddler's own leaves untouched, so
      // spliceCompleteFrontierIncrementally can go on to the wider frontier and the higher levels.
      assertEquals(recanonicalized, AbstractHOTIndexWriter.FRONTIER_SPLIT_HALF_RECANONICALIZED.get(),
          "a half that never produced a block must not be counted as recanonicalized");
      final HOTIndirectPage straddler = assertInstanceOf(HOTIndirectPage.class, fixture.page(children[20]));
      assertEquals(2, straddler.getNumChildren());
      for (int slot = 0; slot < 2; slot++) {
        final HOTLeafPage leaf = assertInstanceOf(HOTLeafPage.class, fixture.page(straddler.getChildReference(slot)));
        assertFalse(leaf.isClosed(), "a shared leaf of a declined straddle split must stay owned by the trie");
        assertArrayEquals(key(slot == 0
            ? 0x3E
            : 0x40), leaf.getFirstKey());
      }
      assertEquals(32, assertInstanceOf(HOTIndirectPage.class, fixture.page(source)).getNumChildren());
    }
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

  private static byte[] key(final int value) {
    return new byte[] {(byte) value};
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
