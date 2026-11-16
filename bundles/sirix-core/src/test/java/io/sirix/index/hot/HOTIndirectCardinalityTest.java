package io.sirix.index.hot;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.index.hot.page.HOTIndirectPage;
import io.sirix.index.hot.page.HOTIndirectPage.LayoutType;
import io.sirix.index.hot.page.HOTIndirectPage.NodeType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.page.PageKind;
import io.sirix.page.PageReference;
import io.sirix.page.SerializationType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

final class HOTIndirectCardinalityTest {

  @Test
  void everyFactoryEnforcesTheLegalChildCountRange() {
    for (final int count : new int[] {0, 1, 2, 16, 17, 32, 33}) {
      for (int factory = 0; factory < 5; factory++) {
        final int selectedFactory = factory;
        final boolean valid = count >= 2 && count <= (factory < 3
            ? 16
            : 32);
        if (valid) {
          final HOTIndirectPage page = create(selectedFactory, count);
          assertEquals(count, page.getNumChildren());
          assertEquals(count, new HOTIndirectPage(page).getNumChildren());
          assertEquals(count, page.copyWithNewPageKey(10, 2).getNumChildren());
          assertEquals(count, page.copyWithUpdatedChild(0, new PageReference()).getNumChildren());
          assertEquals(count, page.withUpdatedChild(0, new PageReference(), 2).getNumChildren());
        } else {
          assertThrows(IllegalArgumentException.class, () -> create(selectedFactory, count),
              "factory=" + factory + " count=" + count);
        }
      }
    }
    assertEquals(2, HOTIndirectPage.createBiNode(1, 1, 0, new PageReference(), new PageReference()).getNumChildren());
    assertEquals(2,
        HOTIndirectPage.createBiNode(1, 1, 0, new PageReference(), new PageReference(), 1).getNumChildren());
    for (final int[] bits : new int[][] {{0}, {0, 72}}) {
      assertThrows(IllegalArgumentException.class, () -> HOTBulkBuilder.assembleIndirect(bits, new int[] {0},
          new PageReference[] {new PageReference()}, 1, 1, IndexType.CAS, () -> 1));
    }
  }

  @Test
  void readerRejectsCorruptCountsBeforeReadingTheBody() {
    final ResourceConfiguration config = ResourceConfiguration.newBuilder("cardinality").build();
    for (final NodeType nodeType : NodeType.values()) {
      for (final LayoutType layout : LayoutType.values()) {
        for (final int count : new int[] {Integer.MIN_VALUE, -1, 0, 1, 33, Integer.MAX_VALUE}) {
          final BytesOut<?> body = Bytes.elasticHeapByteBuffer();
          body.writeByte((byte) 0);
          body.writeByte((byte) 0);
          body.writeByte((byte) 1);
          body.writeInt(1);
          body.writeByte((byte) 1);
          body.writeByte(nodeType.getID());
          body.writeByte(layout.getID());
          body.writeInt(count);
          final IllegalStateException error = assertThrows(IllegalStateException.class,
              () -> PageKind.HOT_INDIRECT_PAGE.deserializePage(config, body.bytesForRead(), SerializationType.DATA));
          assertTrue(error.getMessage().contains("child count"));
          assertTrue(error.getMessage().contains(Integer.toString(count)));
        }
      }
    }
  }

  @Test
  void readerPreservesTwoChildPagesInEveryLayout() {
    final ResourceConfiguration config = ResourceConfiguration.newBuilder("cardinality").build();
    for (int factory = 0; factory < 5; factory++) {
      final HOTIndirectPage original = create(factory, 2);
      final BytesOut<?> encoded = Bytes.elasticHeapByteBuffer();
      PageKind.HOT_INDIRECT_PAGE.serializePage(config, encoded, original, SerializationType.DATA);
      final var source = encoded.bytesForRead();
      source.readByte();
      final HOTIndirectPage decoded =
          (HOTIndirectPage) PageKind.HOT_INDIRECT_PAGE.deserializePage(config, source, SerializationType.DATA);
      assertEquals(original.getNodeType(), decoded.getNodeType());
      assertEquals(original.getLayoutType(), decoded.getLayoutType());
      assertEquals(2, decoded.getNumChildren());
      assertEquals(0, decoded.findChildIndex(new byte[16]));
      assertEquals(1, decoded.findChildIndex(new byte[] {(byte) 0x80}));
      assertEquals(100, decoded.getChildReference(0).getKey());
      assertEquals(101, decoded.getChildReference(1).getKey());
    }
  }

  @Test
  void structuralDetectionAndWriterPreflightRejectInvalidInMemoryCounts() throws ReflectiveOperationException {
    final Field countField = HOTIndirectPage.class.getDeclaredField("numChildren");
    countField.setAccessible(true);
    final Method publicationCheck =
        AbstractHOTIndexWriter.class.getDeclaredMethod("nodeStructurallyMalformed", HOTIndirectPage.class);
    publicationCheck.setAccessible(true);
    final AbstractHOTIndexWriter<?> writer = mock(AbstractHOTIndexWriter.class);
    for (final int count : new int[] {0, 1, 33}) {
      final HOTIndirectPage page = create(3, 2);
      countField.setInt(page, count);
      final PageReference root = new PageReference();
      root.setPage(page);
      final var defects = HOTMalformedSubtreeDetector.detect(root, PageReference::getPage);
      assertEquals(1, defects.size());
      assertEquals("I10-child-count", defects.getFirst().invariant());
      assertEquals(Boolean.TRUE, publicationCheck.invoke(writer, page));
      final var footprint = AbstractHOTIndexWriter.measureBoundedRebuildFootprint(page, reference -> {
        throw new AssertionError("invalid indirect must be refused before resolving a child");
      }, new AbstractHOTIndexWriter.RebuildFootprint());
      assertFalse(footprint.withinBudget());
      assertEquals(AbstractHOTIndexWriter.RebuildFootprintStatus.INVALID_PAGE, footprint.status());
    }
  }

  private static HOTIndirectPage create(final int factory, final int count) {
    final PageReference[] children = new PageReference[count];
    final int[] partials = new int[count];
    for (int i = 0; i < count; i++) {
      children[i] = new PageReference();
      children[i].setKey(100 + i);
      partials[i] = i;
    }
    final long mask = count <= 2
        ? Long.MIN_VALUE
        : 0xF800_0000_0000_0000L;
    return switch (factory) {
      case 0 -> HOTIndirectPage.createSpanNode(1, 1, 0, mask, partials, children);
      case 1 -> HOTIndirectPage.createSpanNode(1, 1, 0, mask, partials, children, 1);
      case 2 -> HOTIndirectPage.createSpanNodeMultiMask(1, 1, new byte[] {0}, new long[] {count <= 2
          ? 0x80
          : 0xF8}, 1, partials, children, 1, (short) 0);
      case 3 -> HOTIndirectPage.createMultiNode(1, 1, 0, mask, partials, children, 1);
      case 4 -> HOTIndirectPage.createMultiNodeMultiMask(1, 1, new byte[] {0}, new long[] {count <= 2
          ? 0x80
          : 0xF8}, 1, partials, children, 1, (short) 0);
      default -> throw new AssertionError(factory);
    };
  }
}
