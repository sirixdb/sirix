package io.sirix.index.hot;

import io.sirix.api.StorageEngineWriter;
import io.sirix.access.ResourceConfiguration;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.page.PageKind;
import io.sirix.page.SerializationType;
import io.sirix.cache.PageContainer;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.index.hot.page.HOTIndirectPage;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.Test;

import java.util.IdentityHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.when;

final class HOTCommitNormalizerTest {
  @Test
  void deserializedCompoundStartsNormalizedUntilStructurallyEdited() {
    final ResourceConfiguration config = ResourceConfiguration.newBuilder("normalization").build();
    final PageReference left = new PageReference();
    left.setKey(100);
    final PageReference right = new PageReference();
    right.setKey(101);
    final HOTIndirectPage original = HOTIndirectPage.createBiNode(3, 1, 0, left, right, 2);
    assertTrue(original.needsNormalization());
    final BytesOut<?> encoded = Bytes.elasticHeapByteBuffer();
    PageKind.HOT_INDIRECT_PAGE.serializePage(config, encoded, original, SerializationType.DATA);
    final BytesIn<?> source = encoded.bytesForRead();
    source.readByte();
    final HOTIndirectPage decoded =
        (HOTIndirectPage) PageKind.HOT_INDIRECT_PAGE.deserializePage(config, source, SerializationType.DATA);
    assertFalse(decoded.needsNormalization(), "durable blocks are opaque normalization boundaries");
    assertEquals(2, decoded.getHeight());
    decoded.setChildReference(0, left);
    assertTrue(decoded.needsNormalization(), "a private structural edit re-arms normalization");
  }

  @Test
  void normalizedCompoundIsSkippedUntilItsPrivateStructureChanges() {
    try (HOTLeafPage a = leaf(1); HOTLeafPage b = leaf(2)) {
      final Fixture fixture = new Fixture();
      final HOTIndirectPage child = HOTIndirectPage.createBiNode(3, 0, 1, ref(a), ref(b), 1);
      final HOTIndirectPage root = HOTIndirectPage.createBiNode(4, 1, 0, ref(child), ref(b), 2);
      final PageReference reference = fixture.dirty(root);
      assertTrue(root.needsNormalization());
      assertSame(root, HOTCommitNormalizer.normalize(reference, root, fixture.writer, fixture.scratch));
      assertFalse(root.needsNormalization());
      clearInvocations(fixture.writer);
      assertSame(root, HOTCommitNormalizer.normalize(reference, root, fixture.writer, fixture.scratch));
      verifyNoInteractions(fixture.writer);
      final HOTIndirectPage copy = new HOTIndirectPage(root);
      assertFalse(copy.needsNormalization());
      copy.markNormalizationNeeded();
      assertTrue(copy.needsNormalization());
      assertFalse(root.needsNormalization(), "private hint must not mutate the historical page");
      final HOTIndirectPage pendingCopy = new HOTIndirectPage(copy);
      assertTrue(pendingCopy.needsNormalization());
      pendingCopy.markNormalized();
      assertFalse(pendingCopy.needsNormalization());
      assertTrue(copy.needsNormalization());
      pendingCopy.setChildReference(1, ref(a));
      assertTrue(pendingCopy.needsNormalization());
      assertTrue(root.copyWithUpdatedChild(1, ref(a)).needsNormalization());
      assertTrue(root.withUpdatedChild(1, ref(a), 2).needsNormalization());
      final HOTIndirectPage grown = new HOTIndirectPage(root);
      grown.setHeightForWrite(3);
      assertTrue(grown.needsNormalization());
      assertFalse(root.needsNormalization());
    }
  }

  @Test
  void heightUpdateIsPrivateToTheCopyAndFitsTheWireField() {
    try (HOTLeafPage a = leaf(1); HOTLeafPage b = leaf(2)) {
      final HOTIndirectPage committed = HOTIndirectPage.createBiNode(3, 0, 0, ref(a), ref(b), 1);
      final HOTIndirectPage copy = new HOTIndirectPage(committed);
      copy.setHeightForWrite(255);
      assertEquals(255, copy.getHeight());
      assertEquals(1, committed.getHeight());
      copy.setHeightForWrite(1);
      assertEquals(1, copy.getHeight());
      assertThrows(IllegalArgumentException.class, () -> copy.setHeightForWrite(0));
      assertThrows(IllegalArgumentException.class, () -> copy.setHeightForWrite(256));
    }
  }

  @Test
  void foldsDirtySpineWithoutRewritingAnyLeaf() {
    try (HOTLeafPage a = leaf(1); HOTLeafPage b = leaf(2); HOTLeafPage c = leaf(3); HOTLeafPage d = leaf(4)) {
      final Fixture fixture = new Fixture();
      final PageReference ar = fixture.dirty(a);
      final PageReference br = fixture.dirty(b);
      final PageReference cr = fixture.dirty(c);
      final PageReference dr = fixture.dirty(d);
      final PageReference low = fixture.dirty(HOTIndirectPage.createBiNode(5, 1, 2, ar, br, 1));
      final PageReference middle = fixture.dirty(HOTIndirectPage.createBiNode(6, 1, 1, low, cr, 2));
      final HOTIndirectPage root = HOTIndirectPage.createBiNode(7, 1, 0, middle, dr, 3);
      final PageReference rootRef = fixture.dirty(root);
      final Page[] before = fixture.routes(rootRef);
      final HOTIndirectPage folded = HOTCommitNormalizer.normalize(rootRef, root, fixture.writer, fixture.scratch);
      assertEquals(1, folded.getHeight());
      assertEquals(4, folded.getNumChildren());
      assertEquals(root.getPageKey(), folded.getPageKey());
      assertSame(ar, folded.getChildReference(0));
      assertSame(br, folded.getChildReference(1));
      assertSame(cr, folded.getChildReference(2));
      assertSame(dr, folded.getChildReference(3));
      final Page[] after = fixture.routes(rootRef);
      for (int i = 0; i < before.length; i++) {
        assertSame(before[i], after[i], "route for byte " + i);
      }
      assertSame(folded, HOTCommitNormalizer.normalize(rootRef, folded, fixture.writer, fixture.scratch));
      assertSame(a, fixture.entries.get(ar).getModified());
      assertSame(b, fixture.entries.get(br).getModified());
    }
  }

  @Test
  void cleanCompoundIsAnOpaqueBoundaryEvenWhenItWouldFit() {
    try (HOTLeafPage a = leaf(1); HOTLeafPage b = leaf(2); HOTLeafPage c = leaf(3)) {
      final Fixture fixture = new Fixture();
      final PageReference ar = ref(a);
      final PageReference br = ref(b);
      final HOTIndirectPage clean = HOTIndirectPage.createBiNode(4, 0, 1, ar, br, 1);
      final PageReference cleanRef = ref(clean);
      final HOTIndirectPage root = HOTIndirectPage.createBiNode(5, 1, 0, cleanRef, fixture.dirty(c), 2);
      final PageReference rootRef = fixture.dirty(root);
      final HOTIndirectPage normalized = HOTCommitNormalizer.normalize(rootRef, root, fixture.writer, fixture.scratch);
      assertSame(root, normalized);
      assertSame(cleanRef, normalized.getChildReference(0));
      assertEquals(2, normalized.getHeight());
      assertEquals(1, clean.getHeight());
      verify(fixture.writer, never()).loadHOTPage(same(ar));
      verify(fixture.writer, never()).loadHOTPage(same(br));
    }
  }

  @Test
  void foldsDirtyChildBesideCleanBoundaryAndKeepsExactMaximumHeight() {
    try (HOTLeafPage a = leaf(1); HOTLeafPage b = leaf(2); HOTLeafPage c = leaf(3); HOTLeafPage d = leaf(4)) {
      final Fixture fixture = new Fixture();
      final PageReference dirty =
          fixture.dirty(HOTIndirectPage.createBiNode(5, 1, 1, fixture.dirty(a), fixture.dirty(b), 1));
      final PageReference cr = ref(c);
      final PageReference dr = ref(d);
      final HOTIndirectPage clean = HOTIndirectPage.createBiNode(6, 0, 1, cr, dr, 1);
      final PageReference cleanRef = new PageReference();
      cleanRef.setKey(0); // zero is a valid durable key, even while the clean root is cold
      when(fixture.writer.loadHOTPage(same(cleanRef))).thenReturn(clean);
      final HOTIndirectPage root = HOTIndirectPage.createBiNode(7, 1, 0, dirty, cleanRef, 2);
      final PageReference rootRef = fixture.dirty(root);
      final HOTIndirectPage normalized = HOTCommitNormalizer.normalize(rootRef, root, fixture.writer, fixture.scratch);
      assertEquals(3, normalized.getNumChildren());
      assertEquals(2, normalized.getHeight());
      assertSame(cleanRef, normalized.getChildReference(2));
      assertSame(clean, cleanRef.getPage());
      verify(fixture.writer, never()).loadHOTPage(same(cr));
      verify(fixture.writer, never()).loadHOTPage(same(dr));
    }
  }

  @Test
  void foldHeightSurvivesEvictionOfPreviouslyResolvedBoundaryRoots() {
    try (HOTLeafPage a = leaf(1); HOTLeafPage b = leaf(2); HOTLeafPage c = leaf(3)) {
      final Fixture fixture = new Fixture();
      final PageReference ar = ref(a);
      final PageReference br = ref(b);
      final PageReference cr = ref(c);
      final HOTIndirectPage clean = HOTIndirectPage.createBiNode(4, 0, 3, ar, br, 1);
      final PageReference cleanRef = ref(clean);
      cleanRef.setKey(4);
      final PageReference dirty = fixture.dirty(HOTIndirectPage.createBiNode(5, 1, 1, cleanRef, br, 2));
      final HOTIndirectPage root = HOTIndirectPage.createBiNode(6, 1, 0, dirty, cr, 3);
      final PageReference rootRef = fixture.dirty(root);
      when(fixture.writer.loadHOTPage(any(PageReference.class))).thenAnswer(call -> {
        final PageReference reference = call.getArgument(0);
        // A later boundary load may evict any earlier clean page from its reference.
        cleanRef.setPage(null);
        if (reference == cleanRef) {
          return clean;
        }
        return reference.getPage();
      });
      final HOTIndirectPage folded = HOTCommitNormalizer.normalize(rootRef, root, fixture.writer, fixture.scratch);
      assertEquals(3, folded.getNumChildren());
      assertEquals(2, folded.getHeight(), "one clean inner level remains below the folded root");
      assertSame(cleanRef, folded.getChildReference(0));
      assertNull(cleanRef.getPage(), "the fold must tolerate eviction, rather than relying on swizzling");
    }
  }

  @Test
  void repairsMaximumWhenChildShrinksButFullParentCannotFold() {
    final HOTLeafPage[] leaves = new HOTLeafPage[34];
    try {
      for (int i = 0; i < leaves.length; i++) {
        leaves[i] = leaf(i + 1);
      }
      final Fixture fixture = new Fixture();
      final PageReference bottom =
          fixture.dirty(HOTIndirectPage.createBiNode(40, 1, 6, ref(leaves[0]), ref(leaves[1]), 1));
      final PageReference middle = fixture.dirty(HOTIndirectPage.createBiNode(41, 1, 5, bottom, ref(leaves[2]), 2));
      final PageReference[] children = new PageReference[32];
      final int[] partials = new int[32];
      children[0] = middle;
      for (int i = 1; i < children.length; i++) {
        children[i] = ref(leaves[i + 2]);
        partials[i] = i;
      }
      final HOTIndirectPage root =
          HOTBulkBuilder.assembleIndirect(new int[] {0, 1, 2, 3, 4}, partials, children, 3, 1, IndexType.CAS, () -> 42);
      final PageReference rootRef = fixture.dirty(root);

      assertSame(root, HOTCommitNormalizer.normalize(rootRef, root, fixture.writer, fixture.scratch));
      assertEquals(32, root.getNumChildren());
      assertEquals(2, root.getHeight(), "the child shrank even though 34 slots would not fit in this parent");
      assertEquals(1, ((HOTIndirectPage) fixture.writer.loadHOTPage(middle)).getHeight());
    } finally {
      for (final HOTLeafPage leaf : leaves) {
        if (leaf != null) {
          leaf.close();
        }
      }
    }
  }

  @Test
  void leafLevelNoFoldDoesNotEvenVisitTheLogOrSwizzleChildren() {
    final Fixture fixture = new Fixture();
    try (HOTLeafPage a = leaf(1); HOTLeafPage b = leaf(2)) {
      final PageReference ar = fixture.dirty(a);
      final PageReference br = fixture.dirty(b);
      final HOTIndirectPage root = HOTIndirectPage.createBiNode(3, 1, 0, ar, br, 1);
      final PageReference rootRef = fixture.dirty(root);
      root.setChildReference(0, ar); // structural publication can mark a leaf-level block
      assertFalse(root.needsNormalization(), "height one still has no compound child to fold");
      assertSame(root, HOTCommitNormalizer.normalize(rootRef, root, fixture.writer, fixture.scratch));
      assertNull(ar.getPage());
      assertNull(br.getPage());
      verifyNoInteractions(fixture.writer);
    }
  }

  @Test
  void noFoldKeepsColdDurableChildrenOpaqueWithoutLoadingThem() {
    final Fixture fixture = new Fixture();
    final PageReference a = new PageReference();
    a.setKey(100);
    final PageReference b = new PageReference();
    b.setKey(101);
    final HOTIndirectPage root = HOTIndirectPage.createBiNode(3, 1, 0, a, b, 1);
    final PageReference rootRef = fixture.dirty(root);
    assertSame(root, HOTCommitNormalizer.normalize(rootRef, root, fixture.writer, fixture.scratch));
    verify(fixture.writer, never()).loadHOTPage(any(PageReference.class));
    assertEquals(1, root.getHeight());
  }

  @Test
  void refusedFullParentDoesNotLoadColdSiblingsOrGrandchildren() {
    final Fixture fixture = new Fixture();
    final PageReference a = new PageReference();
    a.setKey(100);
    final PageReference b = new PageReference();
    b.setKey(101);
    final HOTIndirectPage child = HOTIndirectPage.createBiNode(3, 1, 5, a, b, 1);
    final PageReference[] children = new PageReference[32];
    final int[] partials = new int[32];
    children[0] = fixture.dirty(child);
    for (int slot = 1; slot < 32; slot++) {
      children[slot] = new PageReference();
      children[slot].setKey(200 + slot);
      partials[slot] = slot;
    }
    final HOTIndirectPage root = HOTIndirectPage.createMultiNode(4, 1, 0, 0xF800000000000000L, partials, children, 2);
    final PageReference rootRef = fixture.dirty(root);
    assertSame(root, HOTCommitNormalizer.normalize(rootRef, root, fixture.writer, fixture.scratch));
    verify(fixture.writer, never()).loadHOTPage(any(PageReference.class));
    assertSame(child, children[0].getPage(), "visited dirty boundaries retain their current page without I/O");
    assertEquals(2, root.getHeight());
    // A full block cannot fold: one ownership lookup per clean child is the entire work budget.
    for (int slot = 1; slot < children.length; slot++) {
      verify(fixture.log, times(1)).get(same(children[slot]));
    }
  }

  @Test
  void shrinkingANonmaximumChildDoesNotReadTheUnaffectedCleanMaximum() {
    final Fixture fixture = new Fixture();
    try (HOTLeafPage a = leaf(1); HOTLeafPage b = leaf(2); HOTLeafPage c = leaf(3)) {
      final PageReference bottom = fixture.dirty(HOTIndirectPage.createBiNode(4, 1, 6, ref(a), ref(b), 1));
      final HOTIndirectPage middle = HOTIndirectPage.createBiNode(5, 1, 5, bottom, ref(c), 2);
      final PageReference[] children = new PageReference[32];
      final int[] partials = new int[32];
      children[0] = fixture.dirty(middle);
      final HOTIndirectPage cleanBottom = HOTIndirectPage.createBiNode(6, 0, 7, ref(a), ref(b), 1);
      final HOTIndirectPage cleanMiddle = HOTIndirectPage.createBiNode(7, 0, 6, ref(cleanBottom), ref(c), 2);
      final HOTIndirectPage cleanTop = HOTIndirectPage.createBiNode(8, 0, 5, ref(cleanMiddle), ref(c), 3);
      final PageReference maximum = ref(cleanTop);
      children[1] = maximum;
      for (int slot = 1; slot < 32; slot++) {
        if (slot > 1)
          children[slot] = ref(c);
        partials[slot] = slot;
      }
      final HOTIndirectPage root = HOTIndirectPage.createMultiNode(9, 1, 0, 0xF800000000000000L, partials, children, 4);
      final PageReference rootRef = fixture.dirty(root);
      assertSame(root, HOTCommitNormalizer.normalize(rootRef, root, fixture.writer, fixture.scratch));
      assertEquals(4, root.getHeight());
      assertEquals(1, ((HOTIndirectPage) fixture.entries.get(children[0]).getModified()).getHeight());
      verify(fixture.writer, never()).loadHOTPage(same(maximum));
    }
  }

  @Test
  void workspaceReleasesPageReferencesAfterFailureAndCanBeReused() {
    final Fixture fixture = new Fixture();
    try (HOTLeafPage a = leaf(1); HOTLeafPage b = leaf(2); HOTLeafPage c = leaf(3)) {
      final PageReference cycle = new PageReference();
      final HOTIndirectPage malformed = HOTIndirectPage.createBiNode(4, 1, 0, cycle, fixture.dirty(c), 2);
      fixture.entries.put(cycle, PageContainer.getInstance(malformed, malformed));
      assertThrows(IllegalStateException.class,
          () -> HOTCommitNormalizer.normalize(cycle, malformed, fixture.writer, fixture.scratch));
      assertEquals(0, fixture.scratch.unionBitCount);
      for (final HOTIndirectPage ancestor : fixture.scratch.ancestors)
        assertNull(ancestor);
      for (final PageReference child : fixture.scratch.children)
        assertNull(child);
      final PageReference lower =
          fixture.dirty(HOTIndirectPage.createBiNode(5, 1, 1, fixture.dirty(a), fixture.dirty(b), 1));
      final HOTIndirectPage valid = HOTIndirectPage.createBiNode(6, 1, 0, lower, fixture.dirty(c), 2);
      final PageReference root = fixture.dirty(valid);
      assertEquals(1, HOTCommitNormalizer.normalize(root, valid, fixture.writer, fixture.scratch).getHeight());
      assertEquals(0, fixture.scratch.unionBitCount);
      for (final HOTIndirectPage ancestor : fixture.scratch.ancestors)
        assertNull(ancestor);
      for (final PageReference child : fixture.scratch.children)
        assertNull(child);
    }
  }

  @Test
  void malformedDirtyCyclePoisonsCommit() {
    final Fixture fixture = new Fixture();
    final PageReference cycle = new PageReference();
    try (HOTLeafPage leaf = leaf(1)) {
      final HOTIndirectPage root = HOTIndirectPage.createBiNode(2, 1, 0, cycle, fixture.dirty(leaf), 2);
      fixture.entries.put(cycle, PageContainer.getInstance(root, root));
      final IllegalStateException failure = assertThrows(IllegalStateException.class,
          () -> HOTCommitNormalizer.normalize(cycle, root, fixture.writer, fixture.scratch));
      assertEquals("Cycle in dirty HOT commit spine", failure.getMessage());
      // Entry ownership, root-frame ownership, child edge, repeated-frame ownership.
      // Detect the first back edge, rather than traversing the cycle a second time.
      verify(fixture.log, times(4)).get(same(cycle));
      verify(fixture.writer).markTransactionRollbackOnly(failure);
    }
  }

  @Test
  void rejectsStaleOwnershipAndUnresolvedChildrenBeforePublishing() {
    try (HOTLeafPage a = leaf(1); HOTLeafPage b = leaf(2)) {
      final Fixture fixture = new Fixture();
      final HOTIndirectPage child = HOTIndirectPage.createBiNode(3, 0, 1, ref(a), ref(b), 1);
      final HOTIndirectPage root = HOTIndirectPage.createBiNode(4, 1, 0, ref(child), ref(b), 2);
      final PageReference rootRef = fixture.dirty(root);
      final HOTIndirectPage stale = new HOTIndirectPage(root);
      final IllegalStateException ownership = assertThrows(IllegalStateException.class,
          () -> HOTCommitNormalizer.normalize(rootRef, stale, fixture.writer, fixture.scratch));
      verify(fixture.writer).markTransactionRollbackOnly(ownership);
      assertSame(root, fixture.entries.get(rootRef).getModified());

      root.getChildReference(0).setPage(null);
      final IllegalStateException unresolved = assertThrows(IllegalStateException.class,
          () -> HOTCommitNormalizer.normalize(rootRef, root, fixture.writer, fixture.scratch));
      verify(fixture.writer).markTransactionRollbackOnly(unresolved);
      assertSame(root, fixture.entries.get(rootRef).getModified());
    }
  }

  @Test
  void cleanRootIsReturnedWithoutWalkingItsChildren() {
    try (HOTLeafPage a = leaf(1); HOTLeafPage b = leaf(2)) {
      final Fixture fixture = new Fixture();
      final HOTIndirectPage root = HOTIndirectPage.createBiNode(3, 0, 0, ref(a), ref(b), 1);
      assertSame(root, HOTCommitNormalizer.normalize(ref(root), root, fixture.writer, fixture.scratch));
      verify(fixture.writer, never()).loadHOTPage(any(PageReference.class));
    }
  }

  @Test
  void boundsDirtyRecursionAtTheWriterSpineLimit() {
    try (HOTLeafPage leaf = leaf(1)) {
      for (int count : new int[] {64, 65}) {
        final Fixture fixture = new Fixture();
        HOTIndirectPage root = HOTIndirectPage.createBiNode(count + 1, 1, count - 1, fixture.dirty(leaf), ref(leaf), 1);
        PageReference rootRef = fixture.dirty(root);
        for (int bit = count - 2; bit >= 0; bit--) {
          root = HOTIndirectPage.createBiNode(bit + 2, 1, bit, rootRef, ref(leaf), count - bit);
          rootRef = fixture.dirty(root);
        }
        final PageReference reference = rootRef;
        final HOTIndirectPage page = root;
        if (count == 64) {
          HOTCommitNormalizer.normalize(reference, page, fixture.writer, fixture.scratch);
        } else {
          final IllegalStateException failure = assertThrows(IllegalStateException.class,
              () -> HOTCommitNormalizer.normalize(reference, page, fixture.writer, fixture.scratch));
          verify(fixture.writer).markTransactionRollbackOnly(failure);
        }
      }
    }
  }

  @Test
  void boundsActualDirtyDepthEvenWhenStoredHeightsUnderstateIt() {
    try (HOTLeafPage leaf = leaf(1)) {
      final Fixture fixture = new Fixture();
      PageReference reference = ref(leaf);
      // The writer contract permits 64 dirty frames. Understated metadata must not bypass it.
      for (int depth = 0; depth < 65; depth++) {
        reference = fixture.dirty(HOTIndirectPage.createBiNode(depth + 2, 1, 64 - depth, reference, ref(leaf), 2));
      }
      final PageReference rootRef = reference;
      final HOTIndirectPage root = (HOTIndirectPage) fixture.entries.get(rootRef).getModified();
      final IllegalStateException failure = assertThrows(IllegalStateException.class,
          () -> HOTCommitNormalizer.normalize(rootRef, root, fixture.writer, fixture.scratch));
      assertEquals("Dirty HOT commit spine exceeds bounded writer depth", failure.getMessage());
      verify(fixture.writer).markTransactionRollbackOnly(failure);
      for (final HOTIndirectPage ancestor : fixture.scratch.ancestors) {
        assertNull(ancestor);
      }
    }
  }

  private static HOTLeafPage leaf(final long key) {
    return new HOTLeafPage(key, 1, IndexType.CAS);
  }

  private static PageReference ref(final Page page) {
    final PageReference ref = new PageReference();
    ref.setPage(page);
    return ref;
  }

  private static final class Fixture {
    final HOTCommitNormalizer.Scratch scratch = new HOTCommitNormalizer.Scratch();
    private final StorageEngineWriter writer = mock(StorageEngineWriter.class);
    private final TransactionIntentLog log = mock(TransactionIntentLog.class);
    private final Map<PageReference, PageContainer> entries = new IdentityHashMap<>();

    private Fixture() {
      when(writer.getLog()).thenReturn(log);
      when(log.get(any(PageReference.class))).thenAnswer(call -> entries.get(call.getArgument(0)));
      doAnswer(call -> {
        final PageReference ref = call.getArgument(0);
        entries.put(ref, call.getArgument(1));
        ref.setPage(null); // mirror TIL ownership, not a permanently swizzled test tree
        return null;
      }).when(log).put(any(PageReference.class), any(PageContainer.class));
      when(writer.loadHOTPage(any(PageReference.class))).thenAnswer(call -> {
        final PageReference ref = call.getArgument(0);
        final PageContainer container = entries.get(ref);
        return container == null
            ? ref.getPage()
            : container.getModified();
      });
    }

    private PageReference dirty(final Page page) {
      final PageReference ref = new PageReference();
      entries.put(ref, PageContainer.getInstance(page, page));
      return ref;
    }

    private Page[] routes(final PageReference root) {
      final Page[] result = new Page[256];
      for (int value = 0; value < result.length; value++) {
        final byte[] key = {(byte) value};
        Page page = writer.loadHOTPage(root);
        while (page instanceof HOTIndirectPage indirect) {
          page = writer.loadHOTPage(indirect.getChildReference(indirect.findChildIndex(key)));
        }
        result[value] = page;
      }
      return result;
    }
  }
}
