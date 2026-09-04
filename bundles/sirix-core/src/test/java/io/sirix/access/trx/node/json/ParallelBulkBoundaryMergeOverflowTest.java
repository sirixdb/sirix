/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.json.NullNode;
import io.sirix.node.json.ObjectNamedBooleanNode;
import io.sirix.node.json.StringNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.OverflowPage;
import io.sirix.page.PageKind;
import io.sirix.page.PageLayout;
import io.sirix.page.PageReference;
import io.sirix.page.SerializationType;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.lang.reflect.Method;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Pins the 256-KiB held-tail merge's inline-to-sidecar transition. */
final class ParallelBulkBoundaryMergeOverflowTest {

  private static final int REVISION = 3;
  private static final int SOURCE_SLOT = 900;
  private static final long NODE_KEY = SOURCE_SLOT;
  private static final long PARENT_KEY = 701;
  private static final long RIGHT_SIBLING_KEY = 702;
  private static final long LEFT_SIBLING_KEY = 703;
  private static final int NAME_KEY = 704;
  private static final long PATH_NODE_KEY = 705;
  private static final long HASH = 706;
  private static final long NULL_KEY = Fixed.NULL_NODE_KEY.getStandardProperty();
  private static final LongHashFunction HASH_FUNCTION = LongHashFunction.xx3();

  @Test
  void saturatedBoundaryMergePublishesCompleteFusedSideCarrier() throws ReflectiveOperationException {
    final ResourceConfiguration config = ResourceConfiguration.newBuilder("parallel-boundary-overflow").build();
    final KeyValueLeafPage target = new KeyValueLeafPage(0, IndexType.DOCUMENT, config, REVISION, null, null);
    final KeyValueLeafPage source = new KeyValueLeafPage(0, IndexType.DOCUMENT, config, REVISION, null, null);
    KeyValueLeafPage roundTripped = null;
    try {
      fillWithValidRecords(target);
      final int sourceBytes = writeSourceBoolean(source);
      final int remaining = (int) (target.getSlottedPage().byteSize() - PageLayout.HEAP_START
          - PageLayout.getHeapEnd(target.getSlottedPage()));
      assertTrue(remaining < sourceBytes, "fixture must force copySlotFromPage's overflow branch");
      assertEquals(KeyValueLeafPage.MAX_SLOTTED_PAGE_CAPACITY, target.getSlottedPage().byteSize());

      final Method merge =
          ParallelBulkJsonImporter.class.getDeclaredMethod("mergeInto", KeyValueLeafPage.class, KeyValueLeafPage.class);
      merge.setAccessible(true);
      merge.invoke(null, target, source);

      assertEquals(KeyValueLeafPage.MAX_SLOTTED_PAGE_CAPACITY, target.getSlottedPage().byteSize(),
          "the boundary merge must never request a larger frame");
      assertCompleteSideCarrier(target);
      assertOverflowValue(config, target);

      roundTripped = deserialize(config, serialize(config, target));
      assertCompleteSideCarrier(roundTripped);
      assertArrayEquals(new int[] {SOURCE_SLOT}, roundTripped.getObjectKeySlotsForNameKey(NAME_KEY));
    } finally {
      if (roundTripped != null) {
        roundTripped.close();
      }
      source.close();
      target.close();
    }
  }

  private static int writeSourceBoolean(final KeyValueLeafPage source) {
    final ObjectNamedBooleanNode scratch = new ObjectNamedBooleanNode(0, HASH_FUNCTION);
    final long offset = source.prepareHeapForDirectWriteOrOverflow(scratch.estimateSerializedSize(), 0);
    assertFalse(offset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW);
    final int recordBytes =
        ObjectNamedBooleanNode.writeNewRecord(source.getSlottedPage(), offset, scratch.getHeapOffsets(), NODE_KEY,
            PARENT_KEY, RIGHT_SIBLING_KEY, LEFT_SIBLING_KEY, NAME_KEY, PATH_NODE_KEY, 1, REVISION, HASH, true);
    source.completeDirectWrite(NodeKind.OBJECT_NAMED_BOOLEAN.getId(), NODE_KEY, SOURCE_SLOT, recordBytes, null);
    return recordBytes;
  }

  private static void fillWithValidRecords(final KeyValueLeafPage page) {
    final byte[] value = new byte[440];
    Arrays.fill(value, (byte) 'x');
    final StringNode stringScratch = new StringNode(0, 0, Constants.NULL_REVISION_NUMBER, REVISION, NULL_KEY, NULL_KEY,
        0, new byte[0], HASH_FUNCTION, (SirixDeweyID) null, false, null);
    final NullNode nullScratch = new NullNode(0, 0, Constants.NULL_REVISION_NUMBER, REVISION, NULL_KEY, NULL_KEY, 0,
        HASH_FUNCTION, (SirixDeweyID) null);
    final MemorySegment sizing = MemorySegment.ofArray(new byte[512]);
    int slot = 0;

    while (slot < SOURCE_SLOT) {
      final int bytes = StringNode.writeNewRecord(sizing, 0, stringScratch.getHeapOffsets(), slot, NULL_KEY, NULL_KEY,
          NULL_KEY, Constants.NULL_REVISION_NUMBER, REVISION, value, 0, value.length, false);
      final long offset = page.prepareHeapForDirectWriteOrOverflow(bytes, 0);
      if (offset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
        break;
      }
      final int written = StringNode.writeNewRecord(page.getSlottedPage(), offset, stringScratch.getHeapOffsets(), slot,
          NULL_KEY, NULL_KEY, NULL_KEY, Constants.NULL_REVISION_NUMBER, REVISION, value, 0, value.length, false);
      assertEquals(bytes, written);
      page.completeDirectWrite(NodeKind.STRING_VALUE.getId(), slot, slot, written, null);
      slot++;
    }

    while (slot < SOURCE_SLOT) {
      final int bytes = NullNode.writeNewRecord(sizing, 0, nullScratch.getHeapOffsets(), slot, NULL_KEY, NULL_KEY,
          NULL_KEY, Constants.NULL_REVISION_NUMBER, REVISION);
      final long offset = page.prepareHeapForDirectWriteOrOverflow(bytes, 0);
      if (offset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
        break;
      }
      final int written = NullNode.writeNewRecord(page.getSlottedPage(), offset, nullScratch.getHeapOffsets(), slot,
          NULL_KEY, NULL_KEY, NULL_KEY, Constants.NULL_REVISION_NUMBER, REVISION);
      assertEquals(bytes, written);
      page.completeDirectWrite(NodeKind.NULL_VALUE.getId(), slot, slot, written, null);
      slot++;
    }
  }

  private static void assertCompleteSideCarrier(final KeyValueLeafPage page) {
    assertTrue(page.hasSideSlot(SOURCE_SLOT));
    assertFalse(PageLayout.isSlotPopulated(page.getSlottedPage(), SOURCE_SLOT));
    assertEquals(NodeKind.OBJECT_NAMED_BOOLEAN.getId(), page.getSideSlotNodeKindId(SOURCE_SLOT));
    assertEquals(PARENT_KEY, page.getSlotParentKey(SOURCE_SLOT));
    assertEquals(NAME_KEY, page.getObjectKeyNameKeyFromSlot(SOURCE_SLOT));
    assertEquals(PATH_NODE_KEY, page.getObjectKeyPathNodeKeyFromSlot(SOURCE_SLOT, NODE_KEY));
    assertTrue(page.getFusedObjectNamedBooleanValueFromSlot(SOURCE_SLOT));
    final PageReference reference = page.getPageReference(NODE_KEY);
    assertNotNull(reference);
  }

  private static void assertOverflowValue(final ResourceConfiguration config, final KeyValueLeafPage page) {
    final PageReference reference = page.getPageReference(NODE_KEY);
    final OverflowPage overflow = assertInstanceOf(OverflowPage.class, reference.getPage());
    final DataRecord decoded =
        config.recordPersister.deserialize(new ByteArrayBytesIn(overflow.getDataBytes()), NODE_KEY, null, config);
    final ObjectNamedBooleanNode node = assertInstanceOf(ObjectNamedBooleanNode.class, decoded);
    assertEquals(PARENT_KEY, node.getParentKey());
    assertEquals(RIGHT_SIBLING_KEY, node.getRightSiblingKey());
    assertEquals(LEFT_SIBLING_KEY, node.getLeftSiblingKey());
    assertEquals(NAME_KEY, node.getNameKey());
    assertEquals(PATH_NODE_KEY, node.getPathNodeKey());
    assertEquals(1, node.getPreviousRevisionNumber());
    assertEquals(REVISION, node.getLastModifiedRevisionNumber());
    assertEquals(HASH, node.getHash());
    assertTrue(node.getValue());
  }

  private static byte[] serialize(final ResourceConfiguration config, final KeyValueLeafPage page) {
    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    PageKind.KEYVALUELEAFPAGE.serializePage(config, sink, page, SerializationType.DATA);
    return sink.toByteArray();
  }

  private static KeyValueLeafPage deserialize(final ResourceConfiguration config, final byte[] bytes) {
    final BytesIn<?> source = Bytes.wrapForRead(bytes);
    source.readByte();
    return (KeyValueLeafPage) PageKind.KEYVALUELEAFPAGE.deserializePage(config, source, SerializationType.DATA);
  }
}
