package io.sirix.node.json;

import io.sirix.node.interfaces.FlyweightNode;
import net.openhft.hashing.LongHashFunction;
import org.junit.Test;

import java.lang.foreign.Arena;
import java.math.BigInteger;
import java.util.function.IntUnaryOperator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class JsonFlyweightSerializedSizeEstimateTest {

  @Test
  public void saturatingNarrowingNeverWraps() {
    assertEquals(0, FlyweightNode.saturatingSerializedSize(0L));
    assertEquals(Integer.MAX_VALUE - 1, FlyweightNode.saturatingSerializedSize((long) Integer.MAX_VALUE - 1L));
    assertEquals(Integer.MAX_VALUE, FlyweightNode.saturatingSerializedSize(Integer.MAX_VALUE));
    assertEquals(Integer.MAX_VALUE, FlyweightNode.saturatingSerializedSize(Long.MAX_VALUE));
  }

  @Test
  public void stringPayloadEstimatesSaturateAtTheIntBoundary() {
    assertPayloadEstimateSaturates(StringNode::estimateSerializedSize);
    assertPayloadEstimateSaturates(ObjectNamedStringNode::estimateSerializedSize);
  }

  @Test
  public void ordinaryStringEstimatesRemainConservative() {
    final byte[] value = new byte[257];
    final LongHashFunction hashFunction = LongHashFunction.xx3();

    final StringNode stringNode = new StringNode(1L, hashFunction);
    stringNode.setRawValue(value);
    assertEquals(StringNode.estimateSerializedSize(value.length), stringNode.estimateSerializedSize());
    assertEstimateCoversSerializedBytes(stringNode);

    final ObjectNamedStringNode namedStringNode = new ObjectNamedStringNode(1L, hashFunction);
    namedStringNode.setRawValue(value);
    assertEquals(ObjectNamedStringNode.estimateSerializedSize(value.length), namedStringNode.estimateSerializedSize());
    assertEstimateCoversSerializedBytes(namedStringNode);
  }

  @Test
  public void numericEstimatesUseNonWrappingLongArithmetic() {
    final BigInteger maximalReportedBitLength = new BigInteger("0") {
      @Override
      public int bitLength() {
        return Integer.MAX_VALUE;
      }
    };

    assertTrue(NumberNode.estimateSerializedSize(maximalReportedBitLength) > 0);
    assertTrue(ObjectNamedNumberNode.estimateSerializedSize(maximalReportedBitLength) > 0);

    final NumberNode numberNode = new NumberNode(1L, LongHashFunction.xx3());
    numberNode.setValue(Long.MIN_VALUE);
    assertEstimateCoversSerializedBytes(numberNode);

    final ObjectNamedNumberNode namedNumberNode = new ObjectNamedNumberNode(1L, LongHashFunction.xx3());
    namedNumberNode.setValue(Long.MIN_VALUE);
    assertEstimateCoversSerializedBytes(namedNumberNode);
  }

  private static void assertPayloadEstimateSaturates(final IntUnaryOperator estimator) {
    final int metadataBytes = estimator.applyAsInt(0);
    final int largestUncappedPayload = Integer.MAX_VALUE - metadataBytes;

    assertEquals(Integer.MAX_VALUE - 1, estimator.applyAsInt(largestUncappedPayload - 1));
    assertEquals(Integer.MAX_VALUE, estimator.applyAsInt(largestUncappedPayload));
    assertEquals(Integer.MAX_VALUE, estimator.applyAsInt(Integer.MAX_VALUE));
  }

  private static void assertEstimateCoversSerializedBytes(final FlyweightNode node) {
    final int estimate = node.estimateSerializedSize();
    try (Arena arena = Arena.ofConfined()) {
      final int actual = node.serializeToHeap(arena.allocate(estimate), 0L);
      assertTrue(node.getKind() + " wrote " + actual + " bytes with estimate " + estimate, actual <= estimate);
    }
  }
}
