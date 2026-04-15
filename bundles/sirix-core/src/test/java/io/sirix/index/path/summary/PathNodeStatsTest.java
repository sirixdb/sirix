package io.sirix.index.path.summary;

import io.brackit.query.atomic.QNm;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.delegates.NameNodeDelegate;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.delegates.StructNodeDelegate;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the stats mutators/accessors added to {@link PathNode}. Delegates are
 * constructed with dummy/sentinel values since the stats code paths don't touch them.
 */
final class PathNodeStatsTest {

  private static PathNode newPathNode() {
    final NodeDelegate nodeDel = new NodeDelegate(1L, -1L, LongHashFunction.xx3(),
        -1, 0, (SirixDeweyID) null);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel, -1L, -1L, -1L, -1L, 0L, 0L);
    final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, -1, -1, 42, 0);
    return new PathNode(new QNm("age"), nodeDel, structDel, nameDel,
        NodeKind.OBJECT_KEY, 1, 1);
  }

  @Test
  void freshNode_hasEmptyStats() {
    final PathNode node = newPathNode();
    assertEquals(0, node.getStatsValueCount());
    assertEquals(0, node.getStatsNullCount());
    assertEquals(0, node.getStatsSum());
    assertEquals(Long.MAX_VALUE, node.getStatsMin());
    assertEquals(Long.MIN_VALUE, node.getStatsMax());
    assertFalse(node.hasNumericStats());
    assertFalse(node.hasBytesStats());
    assertFalse(node.isStatsMinDirty());
    assertFalse(node.isStatsMaxDirty());
  }

  @Test
  void recordLongValues_updatesCountSumMinMax() {
    final PathNode node = newPathNode();
    node.recordLongValue(42);
    node.recordLongValue(7);
    node.recordLongValue(100);
    assertEquals(3, node.getStatsValueCount());
    assertEquals(149, node.getStatsSum());
    assertEquals(7, node.getStatsMin());
    assertEquals(100, node.getStatsMax());
    assertTrue(node.hasNumericStats());
  }

  @Test
  void recordLongValues_populatesHll() {
    final PathNode node = newPathNode();
    for (int i = 0; i < 1000; i++) {
      node.recordLongValue(i);
    }
    final HyperLogLogSketch hll = node.getHllSketch();
    assertNotNull(hll);
    final long est = hll.estimate();
    assertTrue(Math.abs(est - 1000) < 50, "estimate " + est + " not near 1000");
  }

  @Test
  void recordBytesValues_updatesMinMax() {
    final PathNode node = newPathNode();
    node.recordBytesValue("banana".getBytes());
    node.recordBytesValue("apple".getBytes());
    node.recordBytesValue("cherry".getBytes());
    assertEquals(3, node.getStatsValueCount());
    assertArrayEquals("apple".getBytes(), node.getStatsMinBytes());
    assertArrayEquals("cherry".getBytes(), node.getStatsMaxBytes());
    assertTrue(node.hasBytesStats());
  }

  @Test
  void recordBytesValue_clonesInputSoLaterMutationDoesNotCorrupt() {
    final PathNode node = newPathNode();
    final byte[] input = "aaa".getBytes();
    node.recordBytesValue(input);
    input[0] = 'z'; // mutate the caller's buffer
    assertArrayEquals("aaa".getBytes(), node.getStatsMinBytes(),
        "stored bound must not alias caller's buffer");
  }

  @Test
  void recordNullValue_onlyBumpsNullCount() {
    final PathNode node = newPathNode();
    node.recordNullValue();
    node.recordNullValue();
    assertEquals(2, node.getStatsNullCount());
    assertEquals(0, node.getStatsValueCount());
  }

  @Test
  void recordBooleanValue_treatedAs01() {
    final PathNode node = newPathNode();
    node.recordBooleanValue(true);
    node.recordBooleanValue(true);
    node.recordBooleanValue(false);
    assertEquals(3, node.getStatsValueCount());
    assertEquals(2, node.getStatsSum());
    assertEquals(0, node.getStatsMin());
    assertEquals(1, node.getStatsMax());
  }

  @Test
  void removeLongValue_marksMinMaxDirtyWhenBoundaryHit() {
    final PathNode node = newPathNode();
    node.recordLongValue(5);
    node.recordLongValue(10);
    node.recordLongValue(20);
    node.removeLongValue(10); // interior value → no dirty
    assertFalse(node.isStatsMinDirty());
    assertFalse(node.isStatsMaxDirty());
    assertEquals(2, node.getStatsValueCount());
    assertEquals(25, node.getStatsSum()); // 5+10+20 - 10 = 25

    node.removeLongValue(5); // was min
    assertTrue(node.isStatsMinDirty());
    assertFalse(node.isStatsMaxDirty());

    node.removeLongValue(20); // was max
    assertTrue(node.isStatsMaxDirty());
  }

  @Test
  void removeBytesValue_marksMinMaxDirtyWhenBoundaryHit() {
    final PathNode node = newPathNode();
    node.recordBytesValue("apple".getBytes());
    node.recordBytesValue("cherry".getBytes());
    node.removeBytesValue("apple".getBytes());
    assertTrue(node.isStatsMinDirty());
    assertFalse(node.isStatsMaxDirty());
  }

  @Test
  void clearMinDirty_replacesBoundAndClearsFlag() {
    final PathNode node = newPathNode();
    node.recordLongValue(5);
    node.removeLongValue(5);
    assertTrue(node.isStatsMinDirty());
    node.clearMinDirty(Long.MAX_VALUE);
    assertFalse(node.isStatsMinDirty());
    assertEquals(Long.MAX_VALUE, node.getStatsMin());
  }

  @Test
  void setStatsState_roundTripPreservesAllFields() {
    final PathNode node = newPathNode();
    final HyperLogLogSketch hll = new HyperLogLogSketch();
    hll.add(1L);
    hll.add(2L);
    final byte[] minB = "a".getBytes();
    final byte[] maxB = "z".getBytes();
    node.setStatsState(5, 2, 100, -3, 77, minB, maxB, hll, true, false);
    assertEquals(5, node.getStatsValueCount());
    assertEquals(2, node.getStatsNullCount());
    assertEquals(100, node.getStatsSum());
    assertEquals(-3, node.getStatsMin());
    assertEquals(77, node.getStatsMax());
    assertArrayEquals(minB, node.getStatsMinBytes());
    assertArrayEquals(maxB, node.getStatsMaxBytes());
    assertEquals(hll, node.getHllSketch());
    assertTrue(node.isStatsMinDirty());
    assertFalse(node.isStatsMaxDirty());
  }
}
