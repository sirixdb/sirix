/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.xml;

import io.brackit.query.atomic.QNm;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexType;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.xml.AttributeNode;
import io.sirix.node.xml.CommentNode;
import io.sirix.node.xml.ElementNode;
import io.sirix.node.xml.NamespaceNode;
import io.sirix.node.xml.PINode;
import io.sirix.node.xml.TextNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageLayout;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Regression coverage for XML factory records that cannot enter the bounded slotted-page heap. */
final class DensePageDirectCreationFallbackTest {

  private static final int REVISION = 9;
  private static final long NULL_KEY = Fixed.NULL_NODE_KEY.getStandardProperty();

  @Test
  void saturatedPageMaterializesFixedXmlKindsWithCompleteMetadata() {
    final ResourceConfiguration config =
        ResourceConfiguration.newBuilder("dense-xml-factory-fallback").useDeweyIDs(true).build();
    final KeyValueLeafPage page = new KeyValueLeafPage(0, IndexType.DOCUMENT, config, REVISION, null, null);
    try {
      saturatePageHeap(page);
      final XmlNodeFactoryImpl factory = newFactory(page, 700);
      final SirixDeweyID elementId = new SirixDeweyID("1.3.5");
      final QNm elementName = new QNm("urn:element", "p", "element");

      final ElementNode element = factory.createElementNode(11, 12, 13, elementName, 14, elementId);
      assertPending(page, element);
      assertStructState(element, 11, 12, 13, elementId);
      assertEquals(14, element.getPathNodeKey());
      assertEquals(elementName, element.getName());
      assertEquals(0, element.getAttributeCount());
      assertEquals(0, element.getNamespaceCount());

      final SirixDeweyID namespaceId = new SirixDeweyID("1.3.7");
      final QNm namespaceName = new QNm("urn:namespace", "ns", "");
      final NamespaceNode namespace = factory.createNamespaceNode(21, namespaceName, 22, namespaceId);
      assertPending(page, namespace);
      assertEquals(21, namespace.getParentKey());
      assertEquals(22, namespace.getPathNodeKey());
      assertEquals(namespaceName, namespace.getName());
      assertEquals(namespaceId, namespace.getDeweyID());
    } finally {
      page.close();
    }
  }

  @Test
  void oversizedXmlValuesDivertBeforeWritingAndOwnTheirPayloads() {
    final ResourceConfiguration config =
        ResourceConfiguration.newBuilder("large-xml-factory-fallback").useDeweyIDs(true).build();
    final KeyValueLeafPage page = new KeyValueLeafPage(0, IndexType.DOCUMENT, config, REVISION, null, null);
    try {
      final XmlNodeFactoryImpl factory = newFactory(page, 1);
      assertEquals(0, PageLayout.getHeapEnd(page.getSlottedPage()));

      final byte[] textInput = payload((byte) 1);
      final byte[] expectedText = textInput.clone();
      final SirixDeweyID textId = new SirixDeweyID("1.1");
      final TextNode text = factory.createTextNode(31, 32, 33, textInput, false, textId);
      textInput[0] ^= 0x7f;
      assertPending(page, text);
      assertStructState(text, 31, 32, 33, textId);
      assertArrayEquals(expectedText, text.getRawValue());
      assertFalse(text.isCompressed());
      assertHeapUntouched(page);

      final byte[] attributeInput = payload((byte) 2);
      final byte[] expectedAttribute = attributeInput.clone();
      final QNm attributeName = new QNm("urn:attribute", "a", "attribute");
      final SirixDeweyID attributeId = new SirixDeweyID("1.2");
      final AttributeNode attribute = factory.createAttributeNode(41, attributeName, attributeInput, 42, attributeId);
      attributeInput[0] ^= 0x7f;
      assertPending(page, attribute);
      assertEquals(41, attribute.getParentKey());
      assertEquals(42, attribute.getPathNodeKey());
      assertEquals(attributeName, attribute.getName());
      assertEquals(attributeId, attribute.getDeweyID());
      assertArrayEquals(expectedAttribute, attribute.getRawValue());
      assertHeapUntouched(page);

      final byte[] piInput = payload((byte) 3);
      final byte[] expectedPi = piInput.clone();
      final QNm target = new QNm("urn:pi", "pi", "target");
      final SirixDeweyID piId = new SirixDeweyID("1.3");
      final PINode pi = factory.createPINode(51, 52, 53, target, piInput, false, 54, piId);
      piInput[0] ^= 0x7f;
      assertPending(page, pi);
      assertStructState(pi, 51, 52, 53, piId);
      assertEquals(54, pi.getPathNodeKey());
      assertEquals(target, pi.getName());
      assertArrayEquals(expectedPi, pi.getRawValue());
      assertFalse(pi.isCompressed());
      assertHeapUntouched(page);

      final byte[] commentInput = payload((byte) 4);
      final byte[] expectedComment = commentInput.clone();
      final SirixDeweyID commentId = new SirixDeweyID("1.4");
      final CommentNode comment = factory.createCommentNode(61, 62, 63, commentInput, false, commentId);
      commentInput[0] ^= 0x7f;
      assertPending(page, comment);
      assertStructState(comment, 61, 62, 63, commentId);
      assertArrayEquals(expectedComment, comment.getRawValue());
      assertFalse(comment.isCompressed());
      assertHeapUntouched(page);
    } finally {
      page.close();
    }
  }

  private static XmlNodeFactoryImpl newFactory(final KeyValueLeafPage page, final long firstNodeKey) {
    final StorageEngineWriter writer = mock(StorageEngineWriter.class);
    final long[] nextNodeKey = {firstNodeKey};
    final long[] allocatedNodeKey = {Constants.NULL_ID_LONG};
    when(writer.getRevisionNumber()).thenReturn(REVISION);
    when(writer.createNameKey(anyString(), any(NodeKind.class))).thenAnswer(
        invocation -> invocation.getArgument(0, String.class).hashCode());
    doAnswer(invocation -> {
      allocatedNodeKey[0] = nextNodeKey[0]++;
      return null;
    }).when(writer).allocateForDocumentCreation();
    when(writer.getAllocKvl()).thenReturn(page);
    when(writer.getAllocNodeKey()).thenAnswer(invocation -> allocatedNodeKey[0]);
    when(writer.getAllocSlotOffset()).thenAnswer(
        invocation -> (int) (allocatedNodeKey[0] & (Constants.NDP_NODE_COUNT - 1)));
    return new XmlNodeFactoryImpl(LongHashFunction.xx3(), writer);
  }

  private static byte[] payload(final byte marker) {
    // Past the inline record cap by the same margin whatever the cap is: the premise of every caller is
    // the diversion, not a byte count.
    final byte[] payload = new byte[Constants.MAX_RECORD_SIZE + 88];
    Arrays.fill(payload, marker);
    return payload;
  }

  private static void assertPending(final KeyValueLeafPage page, final DataRecord record) {
    final int slot = (int) (record.getNodeKey() & (Constants.NDP_NODE_COUNT - 1));
    assertSame(record, page.getRecord(slot));
    assertFalse(((FlyweightNode) record).isWriteSingleton());
    assertEquals(Constants.NULL_REVISION_NUMBER, record.getPreviousRevisionNumber());
    assertEquals(REVISION, record.getLastModifiedRevisionNumber());
  }

  private static void assertStructState(final StructNode node, final long parentKey, final long leftSiblingKey,
      final long rightSiblingKey, final SirixDeweyID deweyId) {
    assertEquals(parentKey, node.getParentKey());
    assertEquals(leftSiblingKey, node.getLeftSiblingKey());
    assertEquals(rightSiblingKey, node.getRightSiblingKey());
    assertEquals(NULL_KEY, node.getFirstChildKey());
    assertEquals(NULL_KEY, node.getLastChildKey());
    assertEquals(0, node.getChildCount());
    assertEquals(0, node.getDescendantCount());
    assertEquals(deweyId, node.getDeweyID());
  }

  private static void assertHeapUntouched(final KeyValueLeafPage page) {
    assertEquals(0, PageLayout.getHeapEnd(page.getSlottedPage()),
        "oversized preflight must not reserve or publish inline bytes");
  }

  private static void saturatePageHeap(final KeyValueLeafPage page) {
    int slot = 0;
    while (true) {
      final long offset = page.prepareHeapForDirectWriteOrOverflow(500, 0);
      if (offset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
        break;
      }
      page.completeDirectWrite(NodeKind.TEXT.getId(), slot, slot, 500, null);
      slot++;
    }
    final int remaining =
        (int) (page.getSlottedPage().byteSize() - PageLayout.HEAP_START - PageLayout.getHeapEnd(page.getSlottedPage()));
    final int finalRecordBytes = remaining - PageLayout.DEWEY_ID_TRAILER_SIZE;
    if (finalRecordBytes >= 0) {
      final long offset = page.prepareHeapForDirectWriteOrOverflow(finalRecordBytes, 0);
      assertFalse(offset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW);
      page.completeDirectWrite(NodeKind.TEXT.getId(), slot, slot, finalRecordBytes, null);
    }
    assertEquals(KeyValueLeafPage.DIRECT_WRITE_OVERFLOW, page.prepareHeapForDirectWriteOrOverflow(1, 0));
  }
}
