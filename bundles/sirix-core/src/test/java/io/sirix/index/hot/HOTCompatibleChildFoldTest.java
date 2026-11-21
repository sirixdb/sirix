package io.sirix.index.hot;

import io.sirix.index.IndexType;
import io.sirix.index.hot.page.HOTIndirectPage;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Random;
import java.util.Arrays;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The oracle composes the original two routers for every possible eight-bit key. */
final class HOTCompatibleChildFoldTest {
  @ParameterizedTest
  @CsvSource({"249,256,true", "248,256,false", "247,255,true"})
  void respectsTheUnsignedByteLimitOnlyForMultiMask(final int parentByte, final int childByte,
      final boolean representable) {
    try (HOTLeafPage a = leaf(1); HOTLeafPage b = leaf(2); HOTLeafPage c = leaf(3)) {
      final HOTIndirectPage child = HOTIndirectPage.createBiNode(4, 1, childByte * 8, ref(a), ref(b), 1);
      final HOTIndirectPage parent = HOTIndirectPage.createBiNode(5, 1, parentByte * 8, ref(child), ref(c), 2);
      final HOTIndirectPage folded =
          HOTIncrementalInsert.foldCompatibleChild(parent, 0, child, 1, 10, new HOTCommitNormalizer.Scratch());
      if (!representable) {
        assertNull(folded, "a MultiMask must not truncate byte 256 to byte zero");
        return;
      }
      assertNotNull(folded);
      for (int value = 0; value < 4; value++) {
        final byte[] key = new byte[childByte + 1];
        key[parentByte] = (byte) ((value & 2) << 6);
        key[childByte] = (byte) ((value & 1) << 7);
        Page expected = parent.getChildReference(parent.findChildIndex(key)).getPage();
        if (expected == child) {
          expected = child.getChildReference(child.findChildIndex(key)).getPage();
        }
        assertSame(expected, folded.getChildReference(folded.findChildIndex(key)).getPage());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  void foldsOverlappingOffPathColumnsWithoutChangingAnyRoute(final int childBit) {
    try (HOTLeafPage a = leaf(1);
        HOTLeafPage b = leaf(2);
        HOTLeafPage c = leaf(3);
        HOTLeafPage d = leaf(4);
        HOTLeafPage e = leaf(5)) {
      final HOTIndirectPage child = HOTIndirectPage.createBiNode(6, 1, childBit, ref(a), ref(b), 1);
      final HOTIndirectPage parent = HOTIndirectPage.createMultiNode(7, 1, 0, 0xE000000000000000L,
          new int[] {0, 4, 6, 7}, new PageReference[] {ref(child), ref(c), ref(d), ref(e)}, 2);
      final HOTIndirectPage folded =
          HOTIncrementalInsert.foldCompatibleChild(parent, 0, child, 1, 10, new HOTCommitNormalizer.Scratch());
      assertNotNull(folded);
      assertEquals(5, folded.getNumChildren());
      assertEquals(1, folded.getHeight());
      for (int value = 0; value < 256; value++) {
        final byte[] key = {(byte) value};
        Page expected = parent.getChildReference(parent.findChildIndex(key)).getPage();
        if (expected == child) {
          expected = child.getChildReference(child.findChildIndex(key)).getPage();
        }
        assertSame(expected, folded.getChildReference(folded.findChildIndex(key)).getPage());
      }
      assertSame(child, parent.getChildReference(0).getPage(), "source remains unchanged before publication");
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3})
  void foldsAFreshColumnAtEveryParentSlot(final int slot) {
    try (HOTLeafPage a = leaf(1);
        HOTLeafPage b = leaf(2);
        HOTLeafPage c = leaf(3);
        HOTLeafPage d = leaf(4);
        HOTLeafPage e = leaf(5)) {
      final HOTIndirectPage child = HOTIndirectPage.createBiNode(6, 1, 3, ref(a), ref(b), 1);
      final PageReference[] siblings = {ref(c), ref(d), ref(e)};
      final PageReference[] children = new PageReference[4];
      for (int i = 0, sibling = 0; i < 4; i++) {
        children[i] = i == slot
            ? ref(child)
            : siblings[sibling++];
      }
      final HOTIndirectPage parent =
          HOTIndirectPage.createMultiNode(7, 1, 0, 0xC000000000000000L, new int[] {0, 1, 2, 3}, children, 2);
      final HOTIndirectPage folded =
          HOTIncrementalInsert.foldCompatibleChild(parent, slot, child, 1, 10, new HOTCommitNormalizer.Scratch());
      assertNotNull(folded);
      assertEquals(5, folded.getNumChildren());
      assertEquals(1, folded.getHeight());
      for (int value = 0; value < 256; value++) {
        final byte[] key = {(byte) value};
        Page expected = parent.getChildReference(parent.findChildIndex(key)).getPage();
        if (expected == child) {
          expected = child.getChildReference(child.findChildIndex(key)).getPage();
        }
        assertSame(expected, folded.getChildReference(folded.findChildIndex(key)).getPage());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {31, 32})
  void enforcesExactFanoutBoundary(final int childCount) {
    final HOTLeafPage[] leaves = new HOTLeafPage[33];
    try {
      final PageReference[] children = new PageReference[childCount];
      final int[] partials = new int[childCount];
      for (int i = 0; i < 33; i++) {
        leaves[i] = leaf(i + 1);
        if (i < childCount) {
          children[i] = ref(leaves[i]);
          partials[i] = i;
        }
      }
      final HOTIndirectPage child =
          HOTIndirectPage.createMultiNode(40, 1, 0, 0x7C00000000000000L, partials, children, 1);
      final HOTIndirectPage parent = HOTIndirectPage.createBiNode(41, 1, 0, ref(child), ref(leaves[childCount]), 2);
      final HOTIndirectPage folded =
          HOTIncrementalInsert.foldCompatibleChild(parent, 0, child, 1, 50, new HOTCommitNormalizer.Scratch());
      if (childCount == 32) {
        assertNull(folded);
      } else {
        assertNotNull(folded);
        assertEquals(32, folded.getNumChildren());
        assertEquals(1, folded.getHeight());
      }
    } finally {
      for (final HOTLeafPage leaf : leaves) {
        if (leaf != null) {
          leaf.close();
        }
      }
    }
  }

  @Test
  void refusesAChildThatTestsAnAlreadyDecidedPathBit() {
    try (HOTLeafPage a = leaf(1); HOTLeafPage b = leaf(2); HOTLeafPage c = leaf(3)) {
      final HOTIndirectPage child = HOTIndirectPage.createBiNode(4, 1, 0, ref(a), ref(b), 1);
      final HOTIndirectPage parent = HOTIndirectPage.createBiNode(5, 1, 0, ref(child), ref(c), 2);
      assertNull(HOTIncrementalInsert.foldCompatibleChild(parent, 0, child, 1, 10, new HOTCommitNormalizer.Scratch()));
    }
  }

  @Test
  void distinguishesParentPathsWithDifferentDecisionDepths() {
    try (HOTLeafPage a = leaf(1);
        HOTLeafPage b = leaf(2);
        HOTLeafPage c = leaf(3);
        HOTLeafPage d = leaf(4);
        HOTLeafPage e = leaf(5)) {
      final HOTIndirectPage child = HOTIndirectPage.createBiNode(6, 1, 2, ref(a), ref(b), 1);
      final HOTIndirectPage parent = HOTIndirectPage.createMultiNode(7, 1, 0, 0xE000000000000000L,
          new int[] {0, 1, 2, 4}, new PageReference[] {ref(c), ref(child), ref(d), ref(e)}, 2);
      assertNull(HOTIncrementalInsert.foldCompatibleChild(parent, 1, child, 1, 10, new HOTCommitNormalizer.Scratch()));
      final HOTIndirectPage boundaryChild = HOTIndirectPage.createBiNode(8, 1, 1, ref(a), ref(b), 1);
      final HOTIndirectPage boundaryParent = HOTIndirectPage.createMultiNode(9, 1, 0, 0xE000000000000000L,
          new int[] {0, 4, 6, 7}, new PageReference[] {ref(c), ref(boundaryChild), ref(d), ref(e)}, 2);
      assertNull(HOTIncrementalInsert.foldCompatibleChild(boundaryParent, 1, boundaryChild, 1, 10,
          new HOTCommitNormalizer.Scratch()));
    }
  }

  @Test
  void preservesParentDecisionOrderEvenWhenSparsePartialsRemainSorted() {
    try (HOTLeafPage a = leaf(1);
        HOTLeafPage b = leaf(2);
        HOTLeafPage c = leaf(3);
        HOTLeafPage d = leaf(4);
        HOTLeafPage e = leaf(5)) {
      // All three compositions would have unique, ordered partials. Their child's first mask
      // bit still precedes or repeats a decision on the selected parent path, so they must decline.
      final HOTIndirectPage earlyChild = HOTIndirectPage.createBiNode(6, 1, 1, ref(a), ref(b), 1);
      final HOTIndirectPage parent = HOTIndirectPage.createMultiNode(7, 1, 0, 0xA000000000000000L,
          new int[] {0, 1, 2, 3}, new PageReference[] {ref(c), ref(earlyChild), ref(d), ref(e)}, 2);
      assertNull(
          HOTIncrementalInsert.foldCompatibleChild(parent, 1, earlyChild, 1, 20, new HOTCommitNormalizer.Scratch()));
      final HOTIndirectPage repeatedChild = HOTIndirectPage.createMultiNode(8, 1, 0, 0x5000000000000000L,
          new int[] {0, 1}, new PageReference[] {ref(a), ref(b)}, 1);
      final HOTIndirectPage boundary = HOTIndirectPage.createMultiNode(9, 1, 0, 0xE000000000000000L,
          new int[] {0, 4, 6, 7}, new PageReference[] {ref(c), ref(repeatedChild), ref(d), ref(e)}, 2);
      assertNull(HOTIncrementalInsert.foldCompatibleChild(boundary, 1, repeatedChild, 1, 20,
          new HOTCommitNormalizer.Scratch()));
      final HOTIndirectPage deepChild = HOTIndirectPage.createMultiNode(10, 1, 0, 0x3000000000000000L, new int[] {0, 1},
          new PageReference[] {ref(a), ref(b)}, 1);
      final HOTIndirectPage uneven = HOTIndirectPage.createMultiNode(11, 1, 0, 0xE000000000000000L,
          new int[] {0, 1, 2, 4}, new PageReference[] {ref(c), ref(deepChild), ref(d), ref(e)}, 2);
      assertNull(
          HOTIncrementalInsert.foldCompatibleChild(uneven, 1, deepChild, 1, 20, new HOTCommitNormalizer.Scratch()));
    }
  }

  @Test
  void preservesAllThirtyTwoMaskColumnsAndRefusesAThirtyThird() {
    try (HOTLeafPage a = leaf(1); HOTLeafPage b = leaf(2); HOTLeafPage c = leaf(3)) {
      final HOTIndirectPage child = HOTIndirectPage.createBiNode(4, 1, 31, ref(a), ref(b), 1);
      final HOTIndirectPage parent = HOTIndirectPage.createMultiNode(5, 1, 0, 0xFFFFFFFF00000000L,
          new int[] {0, Integer.MIN_VALUE}, new PageReference[] {ref(child), ref(c)}, 2);
      final HOTIndirectPage folded =
          HOTIncrementalInsert.foldCompatibleChild(parent, 0, child, 1, 10, new HOTCommitNormalizer.Scratch());
      assertNotNull(folded);
      assertEquals(32, folded.getDiscriminativeBits().length);
      for (int value = 0; value < 4; value++) {
        final byte[] key = {(byte) ((value & 2) << 6), 0, 0, (byte) (value & 1)};
        Page expected = parent.getChildReference(parent.findChildIndex(key)).getPage();
        if (expected == child) {
          expected = child.getChildReference(child.findChildIndex(key)).getPage();
        }
        assertSame(expected, folded.getChildReference(folded.findChildIndex(key)).getPage());
      }
      final HOTIndirectPage widerChild = HOTIndirectPage.createBiNode(6, 1, 32, ref(a), ref(b), 1);
      final HOTIndirectPage widerParent = HOTIndirectPage.createMultiNode(7, 1, 0, 0xFFFFFFFF00000000L,
          new int[] {0, Integer.MIN_VALUE}, new PageReference[] {ref(widerChild), ref(c)}, 2);
      assertNull(HOTIncrementalInsert.foldCompatibleChild(widerParent, 0, widerChild, 1, 10,
          new HOTCommitNormalizer.Scratch()));
    }
  }

  @Test
  void unresolvedGrandchildCannotBeCountedAsALeaf() {
    try (HOTLeafPage a = leaf(1); HOTLeafPage c = leaf(3)) {
      final PageReference missing = new PageReference();
      missing.setKey(100);
      final HOTIndirectPage child = HOTIndirectPage.createBiNode(4, 1, 1, ref(a), missing, 2);
      final HOTIndirectPage parent = HOTIndirectPage.createBiNode(5, 1, 0, ref(child), ref(c), 3);
      assertThrows(IllegalStateException.class,
          () -> HOTIncrementalInsert.foldCompatibleChild(parent, 0, child, 1, 10, new HOTCommitNormalizer.Scratch()));
    }
  }

  @Test
  void everyAcceptedSparseCompositionMatchesBothRoutersForAllByteKeys() {
    final HOTLeafPage[] leaves = new HOTLeafPage[16];
    final HOTCommitNormalizer.Scratch scratch = new HOTCommitNormalizer.Scratch();
    final Random random = new Random(0x1452);
    final AtomicLong pageKeys = new AtomicLong(100);
    int accepted = 0;
    try {
      for (int i = 0; i < leaves.length; i++) {
        leaves[i] = leaf(i + 1);
      }
      for (int trial = 0; trial < 400; trial++) {
        final int parentMask = random.nextInt(255) + 1;
        final int childMask = random.nextInt(255) + 1;
        final int[] parentPartials = sparsePartials(random, Integer.bitCount(parentMask));
        final int[] childPartials = sparsePartials(random, Integer.bitCount(childMask));
        final PageReference[] grandchildren = new PageReference[childPartials.length];
        for (int i = 0; i < grandchildren.length; i++) {
          grandchildren[i] = ref(leaves[8 + i]);
        }
        final HOTIndirectPage child = HOTIndirectPage.createMultiNode(pageKeys.getAndIncrement(), 1, 0,
            (long) childMask << 56, childPartials, grandchildren, 1);
        final int slot = random.nextInt(parentPartials.length);
        final PageReference[] children = new PageReference[parentPartials.length];
        for (int i = 0; i < children.length; i++) {
          children[i] = ref(i == slot
              ? child
              : leaves[i]);
        }
        final HOTIndirectPage parent = HOTIndirectPage.createMultiNode(pageKeys.getAndIncrement(), 1, 0,
            (long) parentMask << 56, parentPartials, children, 2);
        final HOTIndirectPage folded = HOTIncrementalInsert.foldCompatibleChild(parent, slot, child, 1,
            pageKeys.getAndIncrement(), new HOTCommitNormalizer.Scratch());
        if (folded == null) {
          continue;
        }
        accepted++;
        for (int value = 0; value < 256; value++) {
          final byte[] key = {(byte) value};
          Page expected = parent.getChildReference(parent.findChildIndex(key)).getPage();
          if (expected == child) {
            expected = child.getChildReference(child.findChildIndex(key)).getPage();
          }
          assertSame(expected, folded.getChildReference(folded.findChildIndex(key)).getPage(),
              "composed sparse router differs for trial " + trial + " byte " + value);
        }
      }
      assertTrue(accepted > 0, "the differential oracle must exercise accepted folds");
    } finally {
      for (final HOTLeafPage leaf : leaves) {
        if (leaf != null) {
          leaf.close();
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 72, 264})
  void replacementOwnsItsRoutingStateAfterWorkspaceReuse(final int childBit) {
    final HOTCommitNormalizer.Scratch scratch = new HOTCommitNormalizer.Scratch();
    Arrays.fill(scratch.shuffleIndices, Integer.MAX_VALUE);
    try (HOTLeafPage a = leaf(1); HOTLeafPage b = leaf(2); HOTLeafPage c = leaf(3)) {
      final HOTIndirectPage child = HOTIndirectPage.createBiNode(4, 1, childBit, ref(a), ref(b), 1);
      final HOTIndirectPage parent = HOTIndirectPage.createBiNode(5, 1, 0, ref(child), ref(c), 2);
      final HOTIndirectPage folded = HOTIncrementalInsert.foldCompatibleChild(parent, 0, child, 1, 10, scratch);
      assertNotNull(folded);
      Arrays.fill(scratch.unionBits, 2047);
      Arrays.fill(scratch.partials, -1);
      Arrays.fill(scratch.children, null);
      Arrays.fill(scratch.shuffleIndices, 31);
      for (int value = 0; value < 4; value++) {
        final byte[] key = new byte[(childBit >>> 3) + 1];
        key[0] = (byte) ((value & 2) << 6);
        key[childBit >>> 3] |= (byte) ((value & 1) << (7 - (childBit & 7)));
        Page expected = parent.getChildReference(parent.findChildIndex(key)).getPage();
        if (expected == child)
          expected = child.getChildReference(child.findChildIndex(key)).getPage();
        assertSame(expected, folded.getChildReference(folded.findChildIndex(key)).getPage());
      }
    }
  }

  @Test
  void primitiveAssemblyPreservesEveryMaskColumnWithoutBorrowingScratch() {
    final Random random = new Random(1452);
    final HOTCommitNormalizer.Scratch scratch = new HOTCommitNormalizer.Scratch();
    try (HOTLeafPage a = leaf(1); HOTLeafPage b = leaf(2)) {
      final PageReference[] children = {ref(a), ref(b)};
      for (int count = 1; count <= 32; count++) {
        final TreeSet<Integer> positions = new TreeSet<>();
        while (positions.size() < count)
          positions.add(random.nextInt(2048));
        final int[] bits = new int[count];
        int i = 0;
        for (final int bit : positions)
          bits[i++] = bit;
        final HOTIndirectPage output = HOTIndirectPage.createFoldedNode(3, 1, 255, bits, count,
            new int[] {0, 1 << (count - 1)}, children, 2, scratch.shuffleIndices);
        assertArrayEquals(bits, output.getDiscriminativeBits());
        final int[] copied = new int[count + 1];
        copied[count] = -1;
        assertEquals(count, output.copyDiscriminativeBits(copied));
        assertEquals(-1, copied[count], "mask copy must not overwrite the unused scratch suffix");
        assertArrayEquals(bits, Arrays.copyOf(copied, count));
        assertThrows(IndexOutOfBoundsException.class, () -> output.copyDiscriminativeBits(new int[0]));
      }
    }
  }

  @Test
  void primitiveAssemblyUsesExactExtractionStorageAndPreservesTheLastSingleMaskBit() {
    final HOTCommitNormalizer.Scratch scratch = new HOTCommitNormalizer.Scratch();
    try (HOTLeafPage a = leaf(1); HOTLeafPage b = leaf(2)) {
      final PageReference[] children = {ref(a), ref(b)};
      final HOTIndirectPage multi = HOTIndirectPage.createFoldedNode(3, 1, 1, new int[] {0, 1, 2, 72}, 4,
          new int[] {0, 8}, children, 2, scratch.shuffleIndices);
      assertArrayEquals(new byte[] {0, 9}, multi.getExtractionPositions());
      assertArrayEquals(new long[] {0xE080000000000000L}, multi.getExtractionMasks());
      final HOTIndirectPage single = HOTIndirectPage.createFoldedNode(4, 1, 1, new int[] {0, 63}, 2, new int[] {0, 1},
          children, 2, scratch.shuffleIndices);
      assertArrayEquals(new int[] {0, 63}, single.getDiscriminativeBits());
      final byte[] low = new byte[8];
      final byte[] high = new byte[8];
      high[7] = 1;
      assertEquals(0, single.findChildIndex(low));
      assertEquals(1, single.findChildIndex(high));
    }
  }

  @Test
  void sixteenChildOutputKeepsTheEstablishedSpanRepresentation() {
    final HOTCommitNormalizer.Scratch scratch = new HOTCommitNormalizer.Scratch();
    try (HOTLeafPage a = leaf(1)) {
      final PageReference[] children = new PageReference[16];
      final int[] partials = new int[16];
      for (int slot = 0; slot < children.length; slot++) {
        children[slot] = ref(a);
        partials[slot] = slot;
      }
      final HOTIndirectPage expected =
          HOTIndirectPage.createSpanNode(2, 1, 0, 0xF000000000000000L, partials, children, 1);
      final HOTIndirectPage actual = HOTIndirectPage.createFoldedNode(2, 1, 1, new int[] {0, 1, 2, 3}, 4, partials,
          children, 16, scratch.shuffleIndices);
      assertEquals(expected.getNodeType(), actual.getNodeType());
      for (int key = 0; key < 256; key++) {
        final byte[] bytes = {(byte) key};
        assertEquals(expected.findChildIndex(bytes), actual.findChildIndex(bytes));
      }
    }
  }

  @Test
  void primitiveAssemblyRejectsInvalidDimensionsAndUnrepresentableMasks() {
    final HOTCommitNormalizer.Scratch scratch = new HOTCommitNormalizer.Scratch();
    try (HOTLeafPage a = leaf(1); HOTLeafPage b = leaf(2)) {
      final PageReference[] children = {ref(a), ref(b)};
      final int[] partials = {0, 1};
      for (final int height : new int[] {0, 256}) {
        assertThrows(IllegalArgumentException.class, () -> HOTIndirectPage.createFoldedNode(3, 1, height, new int[] {0},
            1, partials, children, 2, scratch.shuffleIndices));
      }
      for (final int count : new int[] {1, 33}) {
        assertThrows(IllegalArgumentException.class, () -> HOTIndirectPage.createFoldedNode(3, 1, 1, new int[] {0}, 1,
            partials, children, count, scratch.shuffleIndices));
      }
      for (final int bitCount : new int[] {0, 33}) {
        assertThrows(IllegalArgumentException.class, () -> HOTIndirectPage.createFoldedNode(3, 1, 1, new int[33],
            bitCount, partials, children, 2, scratch.shuffleIndices));
      }
      for (final int[] bits : new int[][] {{-1, 0}, {1, 1}, {2, 1}, {0, 2048}}) {
        assertThrows(IllegalArgumentException.class,
            () -> HOTIndirectPage.createFoldedNode(3, 1, 1, bits, 2, partials, children, 2, scratch.shuffleIndices));
      }
    }
  }

  private static int[] sparsePartials(final Random random, final int bits) {
    final int count = 2 + random.nextInt(Math.min(6, 1 << bits) - 1);
    final TreeSet<Integer> values = new TreeSet<>();
    values.add(0);
    while (values.size() < count) {
      values.add(random.nextInt(1 << bits));
    }
    final int[] result = new int[count];
    int i = 0;
    for (final int value : values) {
      result[i++] = value;
    }
    return result;
  }

  private static HOTLeafPage leaf(final long key) {
    return new HOTLeafPage(key, 1, IndexType.CAS);
  }

  private static PageReference ref(final Page page) {
    final PageReference reference = new PageReference();
    reference.setPage(page);
    return reference;
  }
}
