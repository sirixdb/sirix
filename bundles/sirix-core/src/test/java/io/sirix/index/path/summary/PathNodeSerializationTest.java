package io.sirix.index.path.summary;

import io.brackit.query.atomic.QNm;
import io.sirix.access.ResourceConfiguration;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.delegates.NameNodeDelegate;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.delegates.StructNodeDelegate;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trip tests for the optional PathStatistics trailer in
 * {@link NodeKind#PATH}'s serializer.
 */
final class PathNodeSerializationTest {

  private static PathNode freshNode() {
    final NodeDelegate nodeDel = new NodeDelegate(1L, -1L, LongHashFunction.xx3(),
        -1, 0, (SirixDeweyID) null);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel, -1L, -1L, -1L, -1L, 0L, 0L);
    final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, -1, -1, 42, 0);
    return new PathNode(new QNm("age"), nodeDel, structDel, nameDel,
        NodeKind.OBJECT_KEY, 1, 1);
  }

  private static ResourceConfiguration config(final boolean withStats) {
    final var builder = new ResourceConfiguration.Builder("test-path-stats")
        .buildPathSummary(true);
    if (withStats) {
      builder.buildPathStatistics(true);
    }
    return builder.build();
  }

  private static PathNode roundTrip(final PathNode original, final ResourceConfiguration cfg) {
    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    NodeKind.PATH.serialize(sink, original, cfg);
    final var source = sink.asBytesIn();
    return (PathNode) NodeKind.PATH.deserialize(source,
        original.getNodeKey(), null, cfg);
  }

  @Test
  void roundTripWithStatsDisabled_preservesStructOnly() {
    final PathNode original = freshNode();
    // Even if stats were populated at runtime, they're not serialized when the flag is off.
    original.recordLongValue(42);
    original.recordLongValue(7);

    final PathNode restored = roundTrip(original, config(false));

    assertEquals(original.getReferences(), restored.getReferences());
    assertEquals(original.getLevel(), restored.getLevel());
    // Stats are absent because the config flag is off.
    assertEquals(0, restored.getStatsValueCount());
    assertEquals(Long.MAX_VALUE, restored.getStatsMin());
  }

  @Test
  void roundTripWithStatsEnabled_preservesNumericStats() {
    final PathNode original = freshNode();
    for (long i = 1; i <= 100; i++) {
      original.recordLongValue(i);
    }
    original.recordNullValue();
    original.recordNullValue();

    final PathNode restored = roundTrip(original, config(true));

    assertEquals(100, restored.getStatsValueCount());
    assertEquals(2, restored.getStatsNullCount());
    assertEquals(5050, restored.getStatsSum()); // 1+2+...+100
    assertEquals(1, restored.getStatsMin());
    assertEquals(100, restored.getStatsMax());
    assertNotNull(restored.getHllSketch());
    final long est = restored.getHllSketch().estimate();
    assertTrue(Math.abs(est - 100) < 10, "HLL estimate " + est + " not near 100");
  }

  @Test
  void roundTripWithStatsEnabled_preservesBytesStats() {
    final PathNode original = freshNode();
    original.recordBytesValue("apple".getBytes());
    original.recordBytesValue("banana".getBytes());
    original.recordBytesValue("cherry".getBytes());

    final PathNode restored = roundTrip(original, config(true));

    assertEquals(3, restored.getStatsValueCount());
    assertArrayEquals("apple".getBytes(), restored.getStatsMinBytes());
    assertArrayEquals("cherry".getBytes(), restored.getStatsMaxBytes());
  }

  @Test
  void roundTripEmptyStats_whenEnabledButNoValuesRecorded() {
    final PathNode original = freshNode();
    final PathNode restored = roundTrip(original, config(true));

    assertEquals(0, restored.getStatsValueCount());
    assertEquals(Long.MAX_VALUE, restored.getStatsMin());
    assertEquals(Long.MIN_VALUE, restored.getStatsMax());
    assertNull(restored.getStatsMinBytes());
    assertNull(restored.getStatsMaxBytes());
    assertNull(restored.getHllSketch());
  }

  @Test
  void roundTripDirtyFlags_preserved() {
    final PathNode original = freshNode();
    original.recordLongValue(5);
    original.recordLongValue(10);
    original.removeLongValue(5); // sets minDirty
    assertTrue(original.isStatsMinDirty());

    final PathNode restored = roundTrip(original, config(true));
    assertTrue(restored.isStatsMinDirty());
  }

  @Test
  void roundTripPageKeysBitmap_preserved() {
    final PathNode original = freshNode();
    final IntOpenHashSet pages = new IntOpenHashSet();
    // Mix contiguous runs (compress to run-length containers) with sparse
    // entries (array containers) to exercise both RoaringBitmap formats.
    for (int p = 100; p < 200; p++) pages.add(p);
    pages.add(5); pages.add(42_000); pages.add(1_000_000);
    original.mergePageKeys(pages);
    assertNotNull(original.getPageKeys());

    final PathNode restored = roundTrip(original, config(true));
    assertArrayEquals(original.getPageKeysArray(), restored.getPageKeysArray(),
        "pageKeys bitmap must survive serialize/deserialize byte-for-byte");
  }

  @Test
  void roundTripPageKeysAbsent_staysNull() {
    final PathNode original = freshNode();
    original.recordLongValue(1);  // stats present, bitmap absent
    final PathNode restored = roundTrip(original, config(true));
    assertNull(restored.getPageKeys(),
        "absent bitmap must round-trip as null (not empty) — preserves legacy semantics");
  }

  @Test
  void roundTripPageKeysEmpty_roundTripsAsEmpty() {
    final PathNode original = freshNode();
    original.setPageKeys(new RoaringBitmap()); // explicit empty bitmap
    final PathNode restored = roundTrip(original, config(true));
    assertNotNull(restored.getPageKeys());
    assertEquals(0, restored.getPageKeys().getCardinality(),
        "an explicit empty bitmap is distinguishable from null — a completed "
      + "scan that proved the nameKey lives on no page");
  }
}
