/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.node.Bytes;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.node.Utils;
import io.sirix.page.delegates.BitmapReferencesPage;
import io.sirix.page.delegates.FullReferencesPage;
import io.sirix.page.delegates.ReferencesPage4;
import io.sirix.page.interfaces.Page;
import io.sirix.page.interfaces.PageFragmentKey;
import io.sirix.settings.Constants;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Wire and cold-read coverage for allocation-free structural-page serializer bridges. */
final class StructuralPageNativeWireTest {

  private static final int BUFFER_CAPACITY = 64 * 1024;
  private static final int OBSOLETE_CHILD_INDEX_BYTES = 256;

  private static final long REFERENCE_HASH = 0x0102_0304_0506_0708L;

  @Test
  void hotIndirectAbaUsesCompactWireAndColdReadsBothLayouts() throws IOException {
    final ResourceConfiguration config = ResourceConfiguration.newBuilder("hot-indirect-native-wire").build();
    final HOTIndirectPage pageA = multiMaskPage();
    final HOTIndirectPage pageB = singleMaskMultiNode();
    final PagePersister persister = new PagePersister();

    try (MemorySegmentBytesOut sink = new MemorySegmentBytesOut(BUFFER_CAPACITY)) {
      final byte[] firstAWire = serialize(persister, config, sink, pageA);
      final byte[] pageBWire = serialize(persister, config, sink, pageB);
      final byte[] secondAWire = serialize(persister, config, sink, pageA);

      assertArrayEquals(firstAWire, secondAWire,
          "A/B/A reuse must not retain the SingleMask node's partial-key payload");
      assertArrayEquals(serializeHOTIndirectWithObsoleteTail(pageA), secondAWire);

      final byte[] obsoletePageBWire = serializeHOTIndirectWithObsoleteTail(pageB);
      assertEquals(obsoletePageBWire.length - OBSOLETE_CHILD_INDEX_BYTES, pageBWire.length,
          "SingleMask MultiNode wire must drop exactly the obsolete 256-byte child-index tail");
      assertArrayEquals(pageBWire, Arrays.copyOf(obsoletePageBWire, pageBWire.length),
          "the format break must remove only the obsolete tail");
      assertEquals(13, pageBWire[0] & 0xFF, "HOT indirect page-kind id remains stable");

      final HOTIndirectPage coldA =
          (HOTIndirectPage) persister.deserializePage(config, Bytes.wrapForRead(secondAWire), SerializationType.DATA);
      final HOTIndirectPage coldB =
          (HOTIndirectPage) persister.deserializePage(config, Bytes.wrapForRead(pageBWire), SerializationType.DATA);
      try {
        assertEquals(HOTIndirectPage.LayoutType.MULTI_MASK, coldA.getLayoutType());
        assertArrayEquals(pageA.getExtractionPositions(), coldA.getExtractionPositions());
        assertArrayEquals(pageA.getExtractionMasks(), coldA.getExtractionMasks());
        assertArrayEquals(pageA.getPartialKeys(), coldA.getPartialKeys());
        assertChildKeys(pageA, coldA);
        assertMultiMaskRoutingParity(pageA, coldA);

        assertEquals(HOTIndirectPage.LayoutType.SINGLE_MASK, coldB.getLayoutType());
        assertEquals(HOTIndirectPage.NodeType.MULTI_NODE, coldB.getNodeType());
        assertArrayEquals(pageB.getPartialKeys(), coldB.getPartialKeys());
        assertChildKeys(pageB, coldB);
        assertSingleMaskRoutingParity(pageB, coldB);
      } finally {
        coldB.close();
        coldA.close();
      }
    }
  }

  @Test
  void indirectDelegateAbaRetainsLegacyWireAndColdNullSemantics() throws IOException {
    final ResourceConfiguration config = ResourceConfiguration.newBuilder("indirect-delegate-native-wire").build();
    final PagePersister persister = new PagePersister();

    final IndirectPage referencesA = referencesPage(new int[] {3, 901}, new long[] {3_003L, 9_001L});
    final IndirectPage referencesB =
        referencesPage(new int[] {1, 7, 511, 1_023}, new long[] {101L, 107L, 611L, 1_123L});
    final IndirectPage bitmapA = bitmapPage(new int[] {0, 17, 511, 700, 1_023}, 20_000L);
    final IndirectPage bitmapB = bitmapPage(new int[] {2, 9, 33, 65, 129, 257, 513, 769}, 30_000L);
    final IndirectPage fullA = fullPage(new int[] {0, 37, 999}, 40_000L);
    final IndirectPage fullB = fullPage(new int[] {1, 2, 3, 4, 1_023}, 50_000L);

    try (MemorySegmentBytesOut sink = new MemorySegmentBytesOut(BUFFER_CAPACITY)) {
      final IndirectPage coldReferences = assertIndirectAba(persister, config, sink, referencesA, referencesB,
          new int[] {3, 901}, new long[] {3_003L, 9_001L}, ReferencesPage4.class);
      final IndirectPage coldBitmap = assertIndirectAba(persister, config, sink, bitmapA, bitmapB,
          new int[] {0, 17, 511, 700, 1_023}, keysFromBase(20_000L, 5), BitmapReferencesPage.class);
      final IndirectPage coldFull = assertIndirectAba(persister, config, sink, fullA, fullB, new int[] {0, 37, 999},
          keysFromBase(40_000L, 3), FullReferencesPage.class);
      try {
        final ReferencesPage4 coldReferencesDelegate = (ReferencesPage4) coldReferences.delegate();
        assertEquals(2, coldReferencesDelegate.getOffsets().size());

        final BitmapReferencesPage coldBitmapDelegate = (BitmapReferencesPage) coldBitmap.delegate();
        assertEquals(5, coldBitmapDelegate.getBitmap().cardinality());
        assertEquals(5, coldBitmapDelegate.getReferences().size());

        final FullReferencesPage coldDelegate = (FullReferencesPage) coldFull.delegate();
        assertNull(coldDelegate.referenceAt(1), "unset full-delegate slots must remain null on cold read");
        assertNull(coldDelegate.referenceAt(1_023));
      } finally {
        coldFull.close();
        coldBitmap.close();
        coldReferences.close();
      }
    } finally {
      fullB.close();
      fullA.close();
      bitmapB.close();
      bitmapA.close();
      referencesB.close();
      referencesA.close();
    }
  }

  private static IndirectPage assertIndirectAba(final PagePersister persister, final ResourceConfiguration config,
      final MemorySegmentBytesOut sink, final IndirectPage pageA, final IndirectPage pageB, final int[] expectedOffsets,
      final long[] expectedKeys, final Class<? extends Page> delegateClass) throws IOException {
    final byte[] firstAWire = serialize(persister, config, sink, pageA);
    final byte[] pageBWire = serialize(persister, config, sink, pageB);
    final byte[] secondAWire = serialize(persister, config, sink, pageA);

    assertArrayEquals(firstAWire, secondAWire,
        "A/B/A reuse must not retain state from " + pageB.delegate().getClass().getSimpleName());
    assertArrayEquals(serializeIndirectLegacy(pageA), secondAWire);
    assertArrayEquals(serializeIndirectLegacy(pageB), pageBWire);

    final IndirectPage coldPage =
        (IndirectPage) persister.deserializePage(config, Bytes.wrapForRead(secondAWire), SerializationType.DATA);
    assertInstanceOf(delegateClass, coldPage.delegate());
    for (int index = 0; index < expectedOffsets.length; index++) {
      final PageReference reference = coldPage.getOrCreateReference(expectedOffsets[index]);
      assertEquals(expectedKeys[index], reference.getKey());
      assertTrue(reference.hasHash());
      assertEquals(REFERENCE_HASH, reference.getHashAsLong());
    }
    return coldPage;
  }

  private static byte[] serialize(final PagePersister persister, final ResourceConfiguration config,
      final MemorySegmentBytesOut sink, final Page page) throws IOException {
    sink.clear();
    persister.serializePage(config, sink, page, SerializationType.DATA);
    return sink.toByteArray();
  }

  /** Reconstruct the previous framing so the test can pin the format break to one removed suffix. */
  private static byte[] serializeHOTIndirectWithObsoleteTail(final HOTIndirectPage page) {
    try (MemorySegmentBytesOut sink = new MemorySegmentBytesOut(BUFFER_CAPACITY)) {
      sink.writeByte(PageKind.HOT_INDIRECT_PAGE.getID());
      PageKind.writeVersionAndFlags(sink);
      Utils.putVarLong(sink, page.getPageKey());
      sink.writeInt(page.getRevision());
      sink.writeByte((byte) page.getHeight());
      sink.writeByte(page.getNodeType().getID());
      sink.writeByte(page.getLayoutType().getID());
      sink.writeInt(page.getNumChildren());

      if (page.getLayoutType() == HOTIndirectPage.LayoutType.MULTI_MASK) {
        sink.writeShort(page.getMostSignificantBitIndex());
        sink.writeShort((short) page.getNumExtractionBytes());
        final byte[] extractionPositions = page.getExtractionPositions();
        if (extractionPositions != null) {
          sink.write(extractionPositions);
        }
        final long[] extractionMasks = page.getExtractionMasks();
        if (extractionMasks != null) {
          for (final long mask : extractionMasks) {
            sink.writeLong(mask);
          }
        }
      } else {
        sink.writeShort((short) page.getInitialBytePos());
        sink.writeLong(page.getBitMask());
        sink.writeShort(page.getMostSignificantBitIndex());
      }

      final int partialWidth = HOTIndirectPage.determinePartialKeyWidthFromBitCount(page.getTotalDiscBits());
      for (final int partialKey : page.getPartialKeys()) {
        if (partialWidth <= 1) {
          sink.writeByte((byte) partialKey);
        } else if (partialWidth <= 2) {
          sink.writeShort((short) partialKey);
        } else {
          sink.writeInt(partialKey);
        }
      }

      for (int index = 0; index < page.getNumChildren(); index++) {
        final PageReference reference = page.getChildReference(index);
        if (reference == null) {
          sink.writeLong(Constants.NULL_ID_LONG);
          sink.writeByte((byte) 0);
        } else {
          sink.writeLong(reference.getKey());
          final List<PageFragmentKey> fragments = reference.getPageFragments();
          sink.writeByte((byte) fragments.size());
          for (int fragmentIndex = 0; fragmentIndex < fragments.size(); fragmentIndex++) {
            final PageFragmentKey fragment = fragments.get(fragmentIndex);
            sink.writeInt(fragment.revision());
            sink.writeLong(fragment.key());
          }
        }
      }

      if (page.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK
          && page.getNodeType() == HOTIndirectPage.NodeType.MULTI_NODE) {
        sink.write(new byte[OBSOLETE_CHILD_INDEX_BYTES]);
      }
      return sink.toByteArray();
    }
  }

  private static byte[] serializeIndirectLegacy(final IndirectPage page) {
    try (MemorySegmentBytesOut sink = new MemorySegmentBytesOut(BUFFER_CAPACITY)) {
      sink.writeByte(PageKind.INDIRECTPAGE.getID());
      PageKind.writeVersionAndFlags(sink);
      final Page delegate = page.delegate();
      if (delegate instanceof ReferencesPage4 referencesPage) {
        sink.writeByte((byte) 0);
        sink.writeByte((byte) referencesPage.getReferences().size());
        for (int index = 0; index < referencesPage.getReferences().size(); index++) {
          writeLegacyReference(sink, referencesPage.getReferences().get(index));
        }
        for (int index = 0; index < referencesPage.getOffsets().size(); index++) {
          sink.writeShort(referencesPage.getOffsets().getShort(index));
        }
      } else if (delegate instanceof BitmapReferencesPage bitmapPage) {
        sink.writeByte((byte) 1);
        writeLegacyBitmap(sink, bitmapPage.getBitmap());
        for (int index = 0; index < bitmapPage.getReferences().size(); index++) {
          writeLegacyReference(sink, bitmapPage.getReferences().get(index));
        }
      } else if (delegate instanceof FullReferencesPage fullPage) {
        sink.writeByte((byte) 2);
        final List<PageReference> references = fullPage.getReferences();
        final BitSet bitmap = new BitSet(Constants.INP_REFERENCE_COUNT);
        for (int index = 0; index < references.size(); index++) {
          if (references.get(index) != null) {
            bitmap.set(index);
          }
        }
        writeLegacyBitmap(sink, bitmap);
        for (int index = 0; index < references.size(); index++) {
          final PageReference reference = references.get(index);
          if (reference != null) {
            writeLegacyReference(sink, reference);
          }
        }
      } else {
        throw new AssertionError("unexpected delegate " + delegate.getClass());
      }
      return sink.toByteArray();
    }
  }

  private static void writeLegacyBitmap(final MemorySegmentBytesOut sink, final BitSet bitmap) {
    final byte[] bitmapBytes = bitmap.toByteArray();
    sink.writeShort((short) bitmapBytes.length);
    sink.write(bitmapBytes);
  }

  private static void writeLegacyReference(final MemorySegmentBytesOut sink, final PageReference reference) {
    final List<PageFragmentKey> fragments = reference.getPageFragments();
    sink.writeByte((byte) fragments.size());
    for (int index = 0; index < fragments.size(); index++) {
      final PageFragmentKey fragment = fragments.get(index);
      sink.writeInt(fragment.revision());
      sink.writeLong(fragment.key());
    }
    sink.writeLong(reference.getKey());
    if (!reference.hasHash()) {
      sink.writeByte((byte) 0);
    } else {
      sink.writeByte((byte) 1);
      sink.writeLong(Long.reverseBytes(reference.getHashAsLong()));
    }
  }

  private static HOTIndirectPage multiMaskPage() {
    final byte[] positions = {0, 9, 17};
    final long[] masks = {0x8040_2000_0000_0000L};
    return HOTIndirectPage.createSpanNodeMultiMask(11L, 7, positions, masks, positions.length, new int[] {0, 1, 3, 7},
        childReferences(4, 1_000L), 3, (short) 0);
  }

  private static HOTIndirectPage singleMaskMultiNode() {
    final int[] partialKeys = new int[17];
    for (int index = 0; index < partialKeys.length; index++) {
      partialKeys[index] = index;
    }
    return HOTIndirectPage.createMultiNode(12L, 8, 5, 0xF800_0000_0000_0000L, partialKeys, childReferences(17, 2_000L),
        4);
  }

  private static void assertSingleMaskRoutingParity(final HOTIndirectPage hot, final HOTIndirectPage cold) {
    final byte[] key = new byte[6];
    for (int partial = 0; partial < 32; partial++) {
      key[5] = (byte) (partial << 3);
      assertEquals(hot.findChildIndex(key), cold.findChildIndex(key),
          "cold SingleMask routing differs for dense partial " + partial);
    }
  }

  private static void assertMultiMaskRoutingParity(final HOTIndirectPage hot, final HOTIndirectPage cold) {
    final byte[] key = new byte[18];
    for (int partial = 0; partial < 8; partial++) {
      key[0] = (byte) ((partial & 0b100) == 0
          ? 0
          : 0x80);
      key[9] = (byte) ((partial & 0b010) == 0
          ? 0
          : 0x40);
      key[17] = (byte) ((partial & 0b001) == 0
          ? 0
          : 0x20);
      assertEquals(hot.findChildIndex(key), cold.findChildIndex(key),
          "cold MultiMask routing differs for dense partial " + partial);
    }
  }

  private static PageReference[] childReferences(final int count, final long keyBase) {
    final PageReference[] references = new PageReference[count];
    for (int index = 0; index < count; index++) {
      references[index] = new PageReference().setKey(keyBase + index);
    }
    references[0].addPageFragment(new PageFragmentKeyImpl(2, keyBase - 1, 0L, 0L));
    return references;
  }

  private static void assertChildKeys(final HOTIndirectPage expected, final HOTIndirectPage actual) {
    assertEquals(expected.getNumChildren(), actual.getNumChildren());
    for (int index = 0; index < expected.getNumChildren(); index++) {
      assertEquals(expected.getChildReference(index).getKey(), actual.getChildReference(index).getKey());
      assertEquals(expected.getChildReference(index).getPageFragments(),
          actual.getChildReference(index).getPageFragments());
    }
  }

  private static IndirectPage referencesPage(final int[] offsets, final long[] keys) {
    final ReferencesPage4 delegate = new ReferencesPage4();
    for (int index = 0; index < offsets.length; index++) {
      delegate.setOrCreateReference(offsets[index], referenceWithHash(keys[index]));
    }
    return new IndirectPage(delegate);
  }

  private static IndirectPage bitmapPage(final int[] offsets, final long keyBase) {
    final BitmapReferencesPage delegate = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT);
    for (int index = 0; index < offsets.length; index++) {
      delegate.setOrCreateReference(offsets[index], referenceWithHash(keyBase + index));
    }
    return new IndirectPage(delegate);
  }

  private static PageReference referenceWithHash(final long key) {
    final PageReference reference = new PageReference().setKey(key);
    reference.setHash(REFERENCE_HASH);
    return reference;
  }

  private static IndirectPage fullPage(final int[] offsets, final long keyBase) {
    final BitmapReferencesPage bitmapDelegate = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT);
    for (int index = 0; index < offsets.length; index++) {
      bitmapDelegate.setOrCreateReference(offsets[index], referenceWithHash(keyBase + index));
    }
    return new IndirectPage(new FullReferencesPage(bitmapDelegate));
  }

  private static long[] keysFromBase(final long keyBase, final int count) {
    final long[] keys = new long[count];
    for (int index = 0; index < count; index++) {
      keys[index] = keyBase + index;
    }
    return keys;
  }
}
