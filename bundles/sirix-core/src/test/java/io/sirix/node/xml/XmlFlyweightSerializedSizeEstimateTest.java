package io.sirix.node.xml;

import io.brackit.query.atomic.QNm;
import io.sirix.node.interfaces.FlyweightNode;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import net.openhft.hashing.LongHashFunction;
import org.junit.Test;

import java.lang.foreign.Arena;
import java.util.function.IntUnaryOperator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class XmlFlyweightSerializedSizeEstimateTest {

  @Test
  public void valuePayloadEstimatesSaturateAtTheIntBoundary() {
    assertPayloadEstimateSaturates(TextNode::estimateSerializedSize);
    assertPayloadEstimateSaturates(AttributeNode::estimateSerializedSize);
    assertPayloadEstimateSaturates(CommentNode::estimateSerializedSize);
    assertPayloadEstimateSaturates(PINode::estimateSerializedSize);
  }

  @Test
  public void elementKeyEstimateSaturatesBeforeCountMultiplicationCanWrap() {
    final int metadataBytes = ElementNode.estimateSerializedSize(0, 0);
    final int largestUncappedCount = (Integer.MAX_VALUE - metadataBytes) / 10;

    assertTrue(ElementNode.estimateSerializedSize(largestUncappedCount, 0) < Integer.MAX_VALUE);
    assertEquals(Integer.MAX_VALUE, ElementNode.estimateSerializedSize(largestUncappedCount + 1, 0));
    assertEquals(Integer.MAX_VALUE, ElementNode.estimateSerializedSize(Integer.MAX_VALUE, Integer.MAX_VALUE));
  }

  @Test
  public void ordinaryEstimatesRemainConservative() {
    final byte[] value = new byte[257];
    final LongHashFunction hashFunction = LongHashFunction.xx3();

    final TextNode textNode = new TextNode(1L, hashFunction);
    textNode.setRawValue(value);
    assertValueEstimateAndSerialization(textNode, TextNode.estimateSerializedSize(value.length));

    final AttributeNode attributeNode = new AttributeNode(1L, hashFunction);
    attributeNode.setRawValue(value);
    assertValueEstimateAndSerialization(attributeNode, AttributeNode.estimateSerializedSize(value.length));

    final CommentNode commentNode = new CommentNode(1L, hashFunction);
    commentNode.setRawValue(value);
    assertValueEstimateAndSerialization(commentNode, CommentNode.estimateSerializedSize(value.length));

    final PINode piNode = new PINode(1L, hashFunction);
    piNode.setRawValue(value);
    assertValueEstimateAndSerialization(piNode, PINode.estimateSerializedSize(value.length));

    final LongArrayList attributeKeys = new LongArrayList(new long[] {2L});
    final LongArrayList namespaceKeys = new LongArrayList(new long[] {3L});
    final ElementNode elementNode = new ElementNode(1L, 0L, 0, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, 0, hashFunction,
        (byte[]) null, attributeKeys, namespaceKeys, new QNm("element"));
    assertEquals(ElementNode.estimateSerializedSize(1, 1), elementNode.estimateSerializedSize());
    assertEstimateCoversSerializedBytes(elementNode);
  }

  private static void assertPayloadEstimateSaturates(final IntUnaryOperator estimator) {
    final int metadataBytes = estimator.applyAsInt(0);
    final int largestUncappedPayload = Integer.MAX_VALUE - metadataBytes;

    assertEquals(Integer.MAX_VALUE - 1, estimator.applyAsInt(largestUncappedPayload - 1));
    assertEquals(Integer.MAX_VALUE, estimator.applyAsInt(largestUncappedPayload));
    assertEquals(Integer.MAX_VALUE, estimator.applyAsInt(Integer.MAX_VALUE));
  }

  private static void assertValueEstimateAndSerialization(final FlyweightNode node, final int expectedEstimate) {
    assertEquals(expectedEstimate, node.estimateSerializedSize());
    assertEstimateCoversSerializedBytes(node);
  }

  private static void assertEstimateCoversSerializedBytes(final FlyweightNode node) {
    final int estimate = node.estimateSerializedSize();
    try (Arena arena = Arena.ofConfined()) {
      final int actual = node.serializeToHeap(arena.allocate(estimate), 0L);
      assertTrue(node.getKind() + " wrote " + actual + " bytes with estimate " + estimate, actual <= estimate);
    }
  }
}
