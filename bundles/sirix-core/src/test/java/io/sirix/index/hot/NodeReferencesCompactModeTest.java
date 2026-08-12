/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.cache.Allocators;
import io.sirix.index.IndexType;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTLeafPage;
import io.sirix.utils.OS;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.LongIterator;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The compact (sorted-array) representation of {@link NodeReferences} and the
 * {@link NodeReferencesSerializer.ChunkAccumulator} that produces it must be observably identical
 * to the bitmap-backed twin through every accessor, including equality across representations and
 * serializer round-trips.
 */
class NodeReferencesCompactModeTest {

  private static NodeReferences bitmapBacked(final long... keys) {
    final NodeReferences refs = new NodeReferences();
    for (final long k : keys) {
      refs.addNodeKey(k);
    }
    return refs;
  }

  private static NodeReferences compact(final long... keys) {
    return NodeReferences.ofSortedArray(keys.clone(), keys.length);
  }

  @Test
  void compactAccessorsMatchBitmapTwin() {
    final long[] keys = {3L, 70_000L, (5L << 16) | 42L, Long.MAX_VALUE - 1};
    final NodeReferences a = compact(keys);
    final NodeReferences b = bitmapBacked(keys);

    assertEquals(keys.length, a.cardinality());
    assertEquals(b.cardinality(), a.cardinality());
    assertTrue(a.hasNodeKeys());
    for (final long k : keys) {
      assertTrue(a.contains(k), "contains " + k);
      assertTrue(a.isPresent(k));
    }
    assertFalse(a.contains(4L));

    final List<Long> got = new ArrayList<>();
    a.forEachNodeKey(got::add);
    assertEquals(keys.length, got.size());
    final LongIterator it = a.nodeKeyIterator();
    int i = 0;
    while (it.hasNext()) {
      assertEquals(keys[i], it.next());
      assertEquals(keys[i], got.get(i));
      i++;
    }

    // Equality and hash are representation-independent, both directions.
    assertEquals(a, b);
    assertEquals(b, a);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  void materializationAndMutationKeepOneSourceOfTruth() {
    final NodeReferences refs = compact(1L, 2L, 3L);
    assertEquals(3, refs.getNodeKeys().getLongCardinality()); // materializes lazily
    refs.addNodeKey(4L); // mutation after materialization
    assertEquals(4, refs.cardinality());
    assertTrue(refs.contains(4L));
    assertTrue(refs.removeNodeKey(2L));
    assertEquals(3, refs.cardinality());
    assertFalse(refs.contains(2L));

    // Mutation on a fresh compact instance materializes first, transparently.
    final NodeReferences refs2 = compact(10L, 20L);
    refs2.addNodeKey(15L);
    assertEquals(3, refs2.cardinality());
    assertEquals(bitmapBacked(10L, 15L, 20L), refs2);
  }

  @Test
  void serializerRoundTripIsRepresentationIndependent() {
    final long[] keys = {7L, 8L, 9L};
    final byte[] fromCompact = NodeReferencesSerializer.serialize(compact(keys));
    final byte[] fromBitmap = NodeReferencesSerializer.serialize(bitmapBacked(keys));
    assertArrayEquals(fromBitmap, fromCompact);
    assertEquals(compact(keys), NodeReferencesSerializer.deserialize(fromCompact));
  }

  @Test
  void emptyCompactBehavesLikeTombstone() {
    final NodeReferences empty = NodeReferences.ofSortedArray(new long[0], 0);
    assertFalse(empty.hasNodeKeys());
    assertEquals(0, empty.cardinality());
    assertFalse(empty.nodeKeyIterator().hasNext());
    assertEquals(new NodeReferences(), empty);
  }

  @Test
  void accumulatorEmitsCompactAndSpillsToBitmap() {
    if (OS.isWindows()) {
      return;
    }
    Allocators.getInstance().init(64L * 1024 * 1024);

    // Small accumulation from real leaf slots -> compact result identical to the slow merge.
    final HOTLeafPage leaf = new HOTLeafPage(1L, 1, IndexType.CAS);
    final NodeReferences chunkA = bitmapBacked(0x0001L, 0x00FFL);
    final NodeReferences chunkB = bitmapBacked(0x0002L);
    assertTrue(leaf.put(new byte[] {1}, NodeReferencesSerializer.serialize(chunkA)));
    assertTrue(leaf.put(new byte[] {2}, NodeReferencesSerializer.serialize(chunkB)));

    final NodeReferencesSerializer.ChunkAccumulator accumulator = new NodeReferencesSerializer.ChunkAccumulator();
    accumulator.addChunk(leaf, leaf.valueRef(0), 3L << 16);
    accumulator.addChunk(leaf, leaf.valueRef(1), 7L << 16);
    final NodeReferences result = accumulator.toNodeReferencesAndReset();
    assertEquals(bitmapBacked((3L << 16) | 0x0001L, (3L << 16) | 0x00FFL, (7L << 16) | 0x0002L), result);

    // Reuse after reset: nothing accumulated -> null.
    assertNull(accumulator.toNodeReferencesAndReset());

    // Spill past the compact limit: accumulate 600 keys through many chunk payloads.
    long expectedCount = 0;
    for (int chunk = 0; chunk < 10; chunk++) {
      final NodeReferences bits = new NodeReferences();
      for (int v = 0; v < 60; v++) {
        bits.addNodeKey(v * 7);
      }
      final byte[] serialized = NodeReferencesSerializer.serialize(bits);
      assertTrue(leaf.put(new byte[] {(byte) (10 + chunk)}, serialized));
      expectedCount += 60;
    }
    for (int chunk = 0; chunk < 10; chunk++) {
      final int idx = leaf.findEntry(new byte[] {(byte) (10 + chunk)});
      accumulator.addChunk(leaf, leaf.valueRef(idx), (long) (chunk + 100) << 16);
    }
    final NodeReferences spilled = accumulator.toNodeReferencesAndReset();
    assertEquals(expectedCount, spilled.cardinality());
    assertTrue(spilled.contains((100L << 16) | 0L));
    assertTrue(spilled.contains((109L << 16) | (59 * 7)));
    leaf.close();
  }
}
