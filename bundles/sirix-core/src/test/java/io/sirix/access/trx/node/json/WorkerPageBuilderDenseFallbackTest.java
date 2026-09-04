/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import io.sirix.access.ResourceConfiguration;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.json.NumberNode;
import io.sirix.node.json.ObjectNamedArrayNode;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.node.json.ObjectNamedObjectNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.OverflowPage;
import io.sirix.page.PageLayout;
import io.sirix.page.PageReference;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Regression for the parallel worker's fixed-record fallback on a dense 256-KiB record page. */
final class WorkerPageBuilderDenseFallbackTest {

  private static final long NULL_KEY = Fixed.NULL_NODE_KEY.getStandardProperty();
  private static final int REVISION = 3;
  private static final int FILLER_COUNT = 800;

  @Test
  void latePrimitivesAndContainersRemainCompleteAndBecomeOverflowCarriers() {
    final ResourceConfiguration config =
        ResourceConfiguration.newBuilder("worker-dense-fallback").buildPathSummary(false).build();
    final Object2IntOpenHashMap<String> nameKeys = nameKeys();
    final WorkerPageBuilder builder = new WorkerPageBuilder(config, REVISION, LongHashFunction.xx3(), true, nameKeys, 1,
        Constants.NDP_NODE_COUNT - 1L);
    final long rootKey = builder.createObjectNode(NULL_KEY, NULL_KEY, 0);
    final byte[] filler = "界".repeat(110).getBytes(StandardCharsets.UTF_8);
    for (int index = 0; index < FILLER_COUNT; index++) {
      builder.createObjectNamedStringNode(rootKey, NULL_KEY, NULL_KEY, 10, "filler", filler, filler.length);
    }
    // Consume any tail too small for another ~400-byte string but still large enough for a fixed
    // record. Once one null falls back, the remaining calls stay on the cold records[] path.
    for (int index = 0; index < 128; index++) {
      builder.createNullNode(rootKey, NULL_KEY, NULL_KEY, 0);
    }
    // The smallest primitive can still fit in a sub-20-byte tail after larger records have stopped.
    // Drain that final tail before the records whose materialized state we verify below.
    for (int index = 0; index < 16; index++) {
      builder.createNumberNode(rootKey, NULL_KEY, NULL_KEY, 0, 0);
    }

    final long intKey = builder.createNumberNode(rootKey, 111, 112, Integer.MIN_VALUE, 20);
    final long longKey = builder.createNumberNode(rootKey, 121, 122, Long.MAX_VALUE, 20);
    final long namedIntKey = builder.createObjectNamedNumberNode(rootKey, 131, 132, 21, "int", Integer.MAX_VALUE);
    final long namedLongKey = builder.createObjectNamedNumberNode(rootKey, 141, 142, 22, "long", Long.MIN_VALUE);
    final long objectKey = builder.createObjectNamedObjectNode(rootKey, 151, 23, "object");
    final long arrayKey = builder.createObjectNamedArrayNode(rootKey, 161, 24, "array");
    builder.fixupContainer(objectKey, 701, 702, 2, 703);
    builder.fixupContainer(arrayKey, 801, 802, 2, 803);

    final List<KeyValueLeafPage> pages = builder.finish(arrayKey);
    assertEquals(1, pages.size());
    final KeyValueLeafPage page = pages.getFirst();
    try {
      assertNotNull(page.getRecord(slotOf(intKey)),
          "fixture did not exhaust the heap: capacity=" + page.getSlottedPage().byteSize() + ", heapEnd="
              + PageLayout.getHeapEnd(page.getSlottedPage()) + ", ceiling="
              + KeyValueLeafPage.MAX_SLOTTED_PAGE_CAPACITY);
      assertMaterializedNumber(page, intKey, Integer.class, Integer.MIN_VALUE, rootKey, 111, 112);
      assertMaterializedNumber(page, longKey, Long.class, Long.MAX_VALUE, rootKey, 121, 122);
      assertMaterializedNamedNumber(page, namedIntKey, Integer.class, Integer.MAX_VALUE, rootKey, 131, 132, 21,
          nameKeys.getInt("int"));
      assertMaterializedNamedNumber(page, namedLongKey, Long.class, Long.MIN_VALUE, rootKey, 141, 142, 22,
          nameKeys.getInt("long"));
      assertMaterializedContainer(page, objectKey, ObjectNamedObjectNode.class, rootKey, 151, 703, 701, 702, 23,
          nameKeys.getInt("object"));
      assertMaterializedContainer(page, arrayKey, ObjectNamedArrayNode.class, rootKey, 161, 803, 801, 802, 24,
          nameKeys.getInt("array"));

      page.addReferences(config);
      assertOverflowReference(page, intKey);
      assertOverflowReference(page, longKey);
      assertOverflowCarrier(page, namedIntKey, NodeKind.OBJECT_NAMED_NUMBER);
      assertOverflowCarrier(page, namedLongKey, NodeKind.OBJECT_NAMED_NUMBER);
      assertOverflowCarrier(page, objectKey, NodeKind.OBJECT_NAMED_OBJECT);
      assertOverflowCarrier(page, arrayKey, NodeKind.OBJECT_NAMED_ARRAY);
      assertTrue(Arrays.stream(page.getObjectKeySlotsForNameKey(nameKeys.getInt("int")))
                       .anyMatch(slot -> slot == slotOf(namedIntKey)));
      assertTrue(Arrays.stream(page.getObjectKeySlotsForNameKey(nameKeys.getInt("object")))
                       .anyMatch(slot -> slot == slotOf(objectKey)));

      final NumberNode decodedInt = assertInstanceOf(NumberNode.class, decodeOverflow(config, page, intKey));
      assertInstanceOf(Integer.class, decodedInt.getValue());
      assertEquals(Integer.MIN_VALUE, decodedInt.getValue());
      final ObjectNamedNumberNode decodedLong =
          assertInstanceOf(ObjectNamedNumberNode.class, decodeOverflow(config, page, namedLongKey));
      assertInstanceOf(Long.class, decodedLong.getValue());
      assertEquals(Long.MIN_VALUE, decodedLong.getValue());
    } finally {
      page.close();
    }
  }

  private static Object2IntOpenHashMap<String> nameKeys() {
    final Object2IntOpenHashMap<String> keys = new Object2IntOpenHashMap<>();
    keys.defaultReturnValue(Integer.MIN_VALUE);
    keys.put("filler", 100);
    keys.put("int", 101);
    keys.put("long", 102);
    keys.put("object", 103);
    keys.put("array", 104);
    return keys;
  }

  private static void assertMaterializedNumber(final KeyValueLeafPage page, final long nodeKey,
      final Class<? extends Number> valueType, final Number value, final long parentKey, final long leftSiblingKey,
      final long rightSiblingKey) {
    final NumberNode node = assertInstanceOf(NumberNode.class, page.getRecord(slotOf(nodeKey)));
    assertInstanceOf(valueType, node.getValue());
    assertEquals(value, node.getValue());
    assertCommonState(node, parentKey, leftSiblingKey, rightSiblingKey);
  }

  private static void assertMaterializedNamedNumber(final KeyValueLeafPage page, final long nodeKey,
      final Class<? extends Number> valueType, final Number value, final long parentKey, final long leftSiblingKey,
      final long rightSiblingKey, final long pathNodeKey, final int nameKey) {
    final ObjectNamedNumberNode node = assertInstanceOf(ObjectNamedNumberNode.class, page.getRecord(slotOf(nodeKey)));
    assertInstanceOf(valueType, node.getValue());
    assertEquals(value, node.getValue());
    assertEquals(pathNodeKey, node.getPathNodeKey());
    assertEquals(nameKey, node.getNameKey());
    assertCommonState(node, parentKey, leftSiblingKey, rightSiblingKey);
  }

  private static <T extends StructNode> void assertMaterializedContainer(final KeyValueLeafPage page,
      final long nodeKey, final Class<T> type, final long parentKey, final long leftSiblingKey,
      final long rightSiblingKey, final long firstChildKey, final long lastChildKey, final long pathNodeKey,
      final int nameKey) {
    final T node = assertInstanceOf(type, page.getRecord(slotOf(nodeKey)));
    assertCommonState(node, parentKey, leftSiblingKey, rightSiblingKey);
    assertEquals(firstChildKey, node.getFirstChildKey());
    assertEquals(lastChildKey, node.getLastChildKey());
    assertEquals(2, node.getChildCount());
    final io.sirix.node.interfaces.NameNode nameNode = (io.sirix.node.interfaces.NameNode) node;
    assertEquals(pathNodeKey, nameNode.getPathNodeKey());
    assertEquals(nameKey, nameNode.getLocalNameKey());
  }

  private static void assertCommonState(final StructNode node, final long parentKey, final long leftSiblingKey,
      final long rightSiblingKey) {
    assertEquals(parentKey, node.getParentKey());
    assertEquals(leftSiblingKey, node.getLeftSiblingKey());
    assertEquals(rightSiblingKey, node.getRightSiblingKey());
    assertEquals(Constants.NULL_REVISION_NUMBER, node.getPreviousRevisionNumber());
    assertEquals(REVISION, node.getLastModifiedRevisionNumber());
  }

  private static void assertOverflowCarrier(final KeyValueLeafPage page, final long nodeKey,
      final NodeKind expectedKind) {
    final int slot = slotOf(nodeKey);
    assertTrue(page.hasSideSlot(slot));
    assertEquals(expectedKind.getId(), page.getSideSlotNodeKindId(slot));
    final PageReference reference = page.getPageReference(nodeKey);
    assertNotNull(reference);
    assertInstanceOf(OverflowPage.class, reference.getPage());
  }

  private static void assertOverflowReference(final KeyValueLeafPage page, final long nodeKey) {
    final int slot = slotOf(nodeKey);
    assertFalse(page.hasSideSlot(slot));
    assertFalse(page.hasSlottedPageSlot(nodeKey));
    final PageReference reference = page.getPageReference(nodeKey);
    assertNotNull(reference);
    assertInstanceOf(OverflowPage.class, reference.getPage());
  }

  private static DataRecord decodeOverflow(final ResourceConfiguration config, final KeyValueLeafPage page,
      final long nodeKey) {
    final OverflowPage overflowPage = assertInstanceOf(OverflowPage.class, page.getPageReference(nodeKey).getPage());
    return config.recordPersister.deserialize(new ByteArrayBytesIn(overflowPage.getDataBytes()), nodeKey,
        page.getDeweyIdAsByteArray(slotOf(nodeKey)), config);
  }

  private static int slotOf(final long nodeKey) {
    return (int) (nodeKey & (Constants.NDP_NODE_COUNT - 1));
  }
}
