/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import io.brackit.query.atomic.QNm;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexType;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.json.ArrayNode;
import io.sirix.node.json.BooleanNode;
import io.sirix.node.json.NullNode;
import io.sirix.node.json.NumberNode;
import io.sirix.node.json.ObjectNamedArrayNode;
import io.sirix.node.json.ObjectNamedBooleanNode;
import io.sirix.node.json.ObjectNamedNullNode;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.node.json.ObjectNamedObjectNode;
import io.sirix.node.json.ObjectNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageLayout;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Verifies the factory's cold fallback when an otherwise-inline record meets a full page heap. */
final class DensePageDirectCreationFallbackTest {

  private static final int REVISION = 7;
  private static final long FIRST_FREE_NODE_KEY = 900;

  private KeyValueLeafPage page;
  private JsonNodeFactoryImpl factory;

  @BeforeEach
  void setUp() {
    final ResourceConfiguration config =
        ResourceConfiguration.newBuilder("dense-json-factory-fallback").useDeweyIDs(true).build();
    page = new KeyValueLeafPage(0, IndexType.DOCUMENT, config, REVISION, null, null);
    saturatePageHeap(page);
    factory = newFactory(page, FIRST_FREE_NODE_KEY);
  }

  private static JsonNodeFactoryImpl newFactory(final KeyValueLeafPage targetPage, final long firstNodeKey) {
    final StorageEngineWriter writer = mock(StorageEngineWriter.class);
    final long[] nextNodeKey = {firstNodeKey};
    final long[] allocatedNodeKey = {Constants.NULL_ID_LONG};
    when(writer.getRevisionNumber()).thenReturn(REVISION);
    doAnswer(invocation -> {
      allocatedNodeKey[0] = nextNodeKey[0]++;
      return null;
    }).when(writer).allocateForDocumentCreation();
    when(writer.getAllocKvl()).thenReturn(targetPage);
    when(writer.getAllocNodeKey()).thenAnswer(invocation -> allocatedNodeKey[0]);
    when(writer.getAllocSlotOffset()).thenAnswer(
        invocation -> (int) (allocatedNodeKey[0] & (Constants.NDP_NODE_COUNT - 1)));
    when(writer.createNameKey(anyString(), any(NodeKind.class))).thenAnswer(
        invocation -> invocation.getArgument(0, String.class).hashCode());
    return new JsonNodeFactoryImpl(LongHashFunction.xx3(), writer);
  }

  @AfterEach
  void tearDown() {
    page.close();
  }

  @Test
  void materializesEveryFixedJsonKindWithoutLosingWireRelevantState() {
    final SirixDeweyID deweyId = new SirixDeweyID("1.3.5");

    final ArrayNode array = factory.createJsonArrayNode(11, 12, 13, 14, deweyId);
    assertPending(array);
    assertEquals(14, array.getPathNodeKey());
    assertStructuralCreationState(array, 11, 12, 13, deweyId);

    final ObjectNode object = factory.createJsonObjectNode(21, 22, 23, deweyId);
    assertPending(object);
    assertStructuralCreationState(object, 21, 22, 23, deweyId);

    final NullNode nullNode = factory.createJsonNullNode(31, 32, 33, deweyId);
    assertPending(nullNode);
    assertLeafCreationState(nullNode, 31, 32, 33, deweyId);

    final BooleanNode booleanNode = factory.createJsonBooleanNode(41, 42, 43, true, deweyId);
    assertPending(booleanNode);
    assertLeafCreationState(booleanNode, 41, 42, 43, deweyId);
    assertEquals(true, booleanNode.getValue());

    final NumberNode integerNode = factory.createJsonNumberNode(51, 52, 53, 54, deweyId);
    assertPending(integerNode);
    assertLeafCreationState(integerNode, 51, 52, 53, deweyId);
    assertInstanceOf(Integer.class, integerNode.getValue());
    assertEquals(54, integerNode.getValue());

    final NumberNode longNode = factory.createJsonNumberNode(61, 62, 63, Long.MAX_VALUE - 7, deweyId);
    assertPending(longNode);
    assertLeafCreationState(longNode, 61, 62, 63, deweyId);
    assertInstanceOf(Long.class, longNode.getValue());
    assertEquals(Long.MAX_VALUE - 7, longNode.getValue());

    final NumberNode doubleNode = factory.createJsonNumberNode(71, 72, 73, 74.5d, deweyId);
    assertPending(doubleNode);
    assertLeafCreationState(doubleNode, 71, 72, 73, deweyId);
    assertInstanceOf(Double.class, doubleNode.getValue());
    assertEquals(74.5d, doubleNode.getValue());

    final ObjectNamedBooleanNode namedBoolean =
        factory.createJsonObjectNamedBooleanNode(81, 82, 83, 84, "bool", false, deweyId);
    assertPending(namedBoolean);
    assertNamedCreationState(namedBoolean, 81, 82, 83, 84, "bool", deweyId);
    assertFalse(namedBoolean.getValue());

    final ObjectNamedNumberNode namedInteger =
        factory.createJsonObjectNamedNumberNode(91, 92, 93, 94, "int", Integer.MIN_VALUE, deweyId);
    assertPending(namedInteger);
    assertNamedCreationState(namedInteger, 91, 92, 93, 94, "int", deweyId);
    assertInstanceOf(Integer.class, namedInteger.getValue());
    assertEquals(Integer.MIN_VALUE, namedInteger.getValue());

    final ObjectNamedNumberNode namedLong =
        factory.createJsonObjectNamedNumberNode(101, 102, 103, 104, "long", Long.MIN_VALUE, deweyId);
    assertPending(namedLong);
    assertNamedCreationState(namedLong, 101, 102, 103, 104, "long", deweyId);
    assertInstanceOf(Long.class, namedLong.getValue());
    assertEquals(Long.MIN_VALUE, namedLong.getValue());

    final ObjectNamedNullNode namedNull = factory.createJsonObjectNamedNullNode(111, 112, 113, 114, "null", deweyId);
    assertPending(namedNull);
    assertNamedCreationState(namedNull, 111, 112, 113, 114, "null", deweyId);

    final ObjectNamedObjectNode namedObject =
        factory.createJsonObjectNamedObjectNode(121, 122, 123, 124, "object", deweyId);
    assertPending(namedObject);
    assertNamedCreationState(namedObject, 121, 122, 123, 124, "object", deweyId);
    assertEquals(new QNm("object"), namedObject.getName());
    assertStructuralCreationState(namedObject, 121, 122, 123, deweyId);

    final ObjectNamedArrayNode namedArray =
        factory.createJsonObjectNamedArrayNode(131, 132, 133, 134, "array", deweyId);
    assertPending(namedArray);
    assertNamedCreationState(namedArray, 131, 132, 133, 134, "array", deweyId);
    assertEquals(new QNm("array"), namedArray.getName());
    assertStructuralCreationState(namedArray, 131, 132, 133, deweyId);
  }

  @Test
  void oversizedBoxedNumbersDivertBeforeWritingIntoAFreshPageHeap() {
    final ResourceConfiguration config = ResourceConfiguration.newBuilder("large-number-factory-fallback").build();
    final KeyValueLeafPage freshPage = new KeyValueLeafPage(0, IndexType.DOCUMENT, config, REVISION, null, null);
    try {
      final JsonNodeFactoryImpl freshFactory = newFactory(freshPage, 1);
      // Magnitudes one byte past the inline record cap, so the encoded record can never fit inline
      // whatever the cap is: the premise of this test is the diversion, not a byte count.
      final BigInteger hugeInteger = BigInteger.ONE.shiftLeft(8 * (Constants.MAX_RECORD_SIZE + 1));
      final BigDecimal hugeDecimal = new BigDecimal(BigInteger.ONE.shiftLeft(8 * (Constants.MAX_RECORD_SIZE + 2)), 37);
      assertEquals(0, PageLayout.getHeapEnd(freshPage.getSlottedPage()));

      final NumberNode integerNode = freshFactory.createJsonNumberNode(10, 11, 12, hugeInteger, null);
      assertSame(integerNode, freshPage.getRecord((int) integerNode.getNodeKey()));
      assertEquals(hugeInteger, integerNode.getValue());
      assertEquals(0, PageLayout.getHeapEnd(freshPage.getSlottedPage()),
          "oversized preflight must not reserve or publish an inline record");

      final ObjectNamedNumberNode decimalNode =
          freshFactory.createJsonObjectNamedNumberNode(20, 21, 22, 23, "decimal", hugeDecimal, null);
      assertSame(decimalNode, freshPage.getRecord((int) decimalNode.getNodeKey()));
      assertEquals(hugeDecimal, decimalNode.getValue());
      assertEquals(0, PageLayout.getHeapEnd(freshPage.getSlottedPage()),
          "named oversized preflight must not reserve or publish an inline record");
    } finally {
      freshPage.close();
    }
  }

  private void assertPending(final DataRecord record) {
    final int slot = (int) (record.getNodeKey() & (Constants.NDP_NODE_COUNT - 1));
    assertSame(record, page.getRecord(slot), "the sentinel branch must retain the materialized record");
    assertFalse(((FlyweightNode) record).isWriteSingleton(), "the fallback must not retain a reusable singleton");
    assertEquals(Constants.NULL_REVISION_NUMBER, record.getPreviousRevisionNumber());
    assertEquals(REVISION, record.getLastModifiedRevisionNumber());
  }

  private static void assertLeafCreationState(final io.sirix.node.interfaces.StructNode node, final long parentKey,
      final long leftSiblingKey, final long rightSiblingKey, final SirixDeweyID deweyId) {
    assertEquals(parentKey, node.getParentKey());
    assertEquals(leftSiblingKey, node.getLeftSiblingKey());
    assertEquals(rightSiblingKey, node.getRightSiblingKey());
    assertEquals(deweyId, node.getDeweyID());
  }

  private static void assertStructuralCreationState(final io.sirix.node.interfaces.StructNode node,
      final long parentKey, final long leftSiblingKey, final long rightSiblingKey, final SirixDeweyID deweyId) {
    assertLeafCreationState(node, parentKey, leftSiblingKey, rightSiblingKey, deweyId);
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getFirstChildKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getLastChildKey());
    assertEquals(0, node.getChildCount());
    assertEquals(0, node.getDescendantCount());
  }

  private static void assertNamedCreationState(final io.sirix.node.interfaces.StructNode node, final long parentKey,
      final long leftSiblingKey, final long rightSiblingKey, final long pathNodeKey, final String name,
      final SirixDeweyID deweyId) {
    assertLeafCreationState(node, parentKey, leftSiblingKey, rightSiblingKey, deweyId);
    final io.sirix.node.interfaces.NameNode nameNode = (io.sirix.node.interfaces.NameNode) node;
    assertEquals(pathNodeKey, nameNode.getPathNodeKey());
    assertEquals(name.hashCode(), nameNode.getLocalNameKey());
  }

  private static void saturatePageHeap(final KeyValueLeafPage page) {
    int slot = 0;
    while (true) {
      final long offset = page.prepareHeapForDirectWriteOrOverflow(500, 0);
      if (offset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
        break;
      }
      page.completeDirectWrite(NodeKind.STRING_VALUE.getId(), slot, slot, 500, null);
      slot++;
    }
    final int remaining =
        (int) (page.getSlottedPage().byteSize() - PageLayout.HEAP_START - PageLayout.getHeapEnd(page.getSlottedPage()));
    final int finalRecordBytes = remaining - PageLayout.DEWEY_ID_TRAILER_SIZE;
    if (finalRecordBytes >= 0) {
      final long offset = page.prepareHeapForDirectWriteOrOverflow(finalRecordBytes, 0);
      assertFalse(offset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW);
      page.completeDirectWrite(NodeKind.STRING_VALUE.getId(), slot, slot, finalRecordBytes, null);
    }
    assertEquals(KeyValueLeafPage.MAX_SLOTTED_PAGE_CAPACITY, page.getSlottedPage().byteSize());
    assertEquals(KeyValueLeafPage.DIRECT_WRITE_OVERFLOW, page.prepareHeapForDirectWriteOrOverflow(1, 0));
  }
}
