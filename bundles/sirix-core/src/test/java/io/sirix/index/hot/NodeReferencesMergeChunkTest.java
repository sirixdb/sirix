/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Differential test for {@code NodeReferencesSerializer.mergeChunkInto}.
 *
 * <p>{@code mergeChunkInto} is a second decoder for the same wire format the
 * {@code deserialize} → {@code getNodeKeys} → {@code LongIterator} path already decodes. Two
 * decoders that disagree on one byte is a silently-wrong index, so this pins them to each other
 * across both encodings rather than testing the new one in isolation: for every input, merging
 * in place must produce exactly the bitmap the old materializing path produced.
 *
 * <p>The {@code PACKED_THRESHOLD} boundary (64) is swept explicitly, since that is where the
 * encoding flips from packed to Roaring and where an off-by-one would hide.
 */
final class NodeReferencesMergeChunkTest {

  /** The reference implementation: exactly what the scan paths did before mergeChunkInto. */
  private static Roaring64Bitmap mergeTheOldWay(final byte[] chunkBytes, final long highBits) {
    final Roaring64Bitmap merged = new Roaring64Bitmap();
    if (NodeReferencesSerializer.isTombstone(chunkBytes, 0, chunkBytes.length)) {
      return merged;
    }
    final NodeReferences refs = NodeReferencesSerializer.deserialize(chunkBytes);
    final Roaring64Bitmap bitmap = refs.getNodeKeys();
    final LongIterator it = bitmap.getLongIterator();
    while (it.hasNext()) {
      merged.add(highBits | (it.next() & 0xFFFFL));
    }
    return merged;
  }

  private static MemorySegment toSegment(final Arena arena, final byte[] bytes) {
    final MemorySegment seg = arena.allocate(bytes.length, 1);
    MemorySegment.copy(MemorySegment.ofArray(bytes), 0, seg, 0, bytes.length);
    return seg;
  }

  private static void assertSameAsOldPath(final byte[] encoded, final long highBits) {
    try (Arena arena = Arena.ofConfined()) {
      final Roaring64Bitmap expected = mergeTheOldWay(encoded, highBits);

      final Roaring64Bitmap actual = new Roaring64Bitmap();
      NodeReferencesSerializer.mergeChunkInto(toSegment(arena, encoded), highBits, actual);

      assertEquals(expected.getLongCardinality(), actual.getLongCardinality(),
          "cardinality differs from the materializing path");
      final LongIterator it = expected.getLongIterator();
      while (it.hasNext()) {
        final long key = it.next();
        assertTrue(actual.contains(key), "in-place merge lost key " + key);
      }
    }
  }

  private static byte[] encode(final long... keys) {
    final NodeReferences refs = new NodeReferences();
    for (final long k : keys) {
      refs.addNodeKey(k);
    }
    return NodeReferencesSerializer.serialize(refs);
  }

  /** Sweeps the packed→Roaring boundary; both encodings must decode identically in place. */
  @Test
  void matchesMaterializingPathAcrossTheEncodingBoundary() {
    final long highBits = 7L << 16;
    for (int count = 0; count <= 130; count++) {
      final long[] keys = new long[count];
      for (int i = 0; i < count; i++) {
        keys[i] = i * 3L + 1L; // stays inside the low 16 bits the chunk format stores
      }
      assertSameAsOldPath(encode(keys), highBits);
    }
  }

  /** Random sets, including duplicates and boundary values within the 16-bit chunk space. */
  @Test
  void matchesMaterializingPathOnRandomSets() {
    final Random random = new Random(20260726L);
    for (int trial = 0; trial < 300; trial++) {
      final int count = random.nextInt(200);
      final long[] keys = new long[count];
      for (int i = 0; i < count; i++) {
        keys[i] = random.nextInt(0x10000);
      }
      final long highBits = ((long) random.nextInt(1 << 20)) << 16;
      assertSameAsOldPath(encode(keys), highBits);
    }
  }

  /** 0 and 0xFFFF are the extremes of the stored key space — both must survive the OR. */
  @Test
  void preservesChunkKeyExtremes() {
    assertSameAsOldPath(encode(0L, 0xFFFFL), 0L);
    assertSameAsOldPath(encode(0L, 0xFFFFL), 0xFFFFL << 16);
    assertSameAsOldPath(encode(0L, 1L, 0xFFFEL, 0xFFFFL), 123L << 16);
  }

  /** A tombstone contributes nothing and must report that it added nothing. */
  @Test
  void tombstoneAddsNothing() {
    try (Arena arena = Arena.ofConfined()) {
      final byte[] tombstone = NodeReferencesSerializer.serialize(new NodeReferences());
      assertTrue(NodeReferencesSerializer.isTombstone(tombstone, 0, tombstone.length));

      final Roaring64Bitmap dest = new Roaring64Bitmap();
      assertFalse(NodeReferencesSerializer.mergeChunkInto(toSegment(arena, tombstone), 0L, dest));
      assertTrue(dest.isEmpty());
    }
  }

  /** An empty view is "absent", not a format error. */
  @Test
  void emptyViewAddsNothing() {
    try (Arena arena = Arena.ofConfined()) {
      final Roaring64Bitmap dest = new Roaring64Bitmap();
      assertFalse(NodeReferencesSerializer.mergeChunkInto(arena.allocate(0), 0L, dest));
      assertTrue(dest.isEmpty());
    }
  }

  /** Merging several chunks accumulates, and the high bits keep them disjoint. */
  @Test
  void mergesMultipleChunksDisjointly() {
    try (Arena arena = Arena.ofConfined()) {
      final Roaring64Bitmap dest = new Roaring64Bitmap();
      NodeReferencesSerializer.mergeChunkInto(toSegment(arena, encode(1L, 2L)), 0L, dest);
      NodeReferencesSerializer.mergeChunkInto(toSegment(arena, encode(1L, 2L)), 1L << 16, dest);

      assertEquals(4, dest.getLongCardinality());
      assertTrue(dest.contains(1L));
      assertTrue(dest.contains(2L));
      assertTrue(dest.contains((1L << 16) | 1L));
      assertTrue(dest.contains((1L << 16) | 2L));
    }
  }

  /**
   * The Roaring branch decodes into a per-thread scratch bitmap reused across calls, so a decode
   * must never expose keys from the chunk decoded before it — that would be a silently over-broad
   * index result. This interleaves large (Roaring-encoded) chunks with disjoint contents and
   * asserts each decode contributes only its own keys.
   *
   * <p>Scope note: this pins the no-bleed PROPERTY, not the {@code clear()} call that currently
   * implements it. Removing {@code clear()} does not fail this test, because
   * {@code Roaring64Bitmap.deserialize} already replaces the bitmap's contents wholesale. The
   * {@code clear()} is kept as cheap insurance against that changing in a future Roaring release —
   * which is exactly the change this test would catch.
   */
  @Test
  void reusedRoaringScratchDoesNotBleedBetweenChunks() {
    // Above PACKED_THRESHOLD (64) so both chunks take the Roaring branch.
    final long[] firstKeys = new long[100];
    for (int i = 0; i < firstKeys.length; i++) {
      firstKeys[i] = i;
    }
    final long[] secondKeys = new long[80];
    for (int i = 0; i < secondKeys.length; i++) {
      secondKeys[i] = 40000L + i;
    }
    final byte[] first = encode(firstKeys);
    final byte[] second = encode(secondKeys);

    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment firstSeg = toSegment(arena, first);
      final MemorySegment secondSeg = toSegment(arena, second);

      for (int round = 0; round < 5; round++) {
        final Roaring64Bitmap onlySecond = new Roaring64Bitmap();
        // Decode the first chunk into a throwaway bitmap so the scratch holds its keys...
        NodeReferencesSerializer.mergeChunkInto(firstSeg, 0L, new Roaring64Bitmap());
        // ...then decode the second: it must contribute ONLY its own keys.
        NodeReferencesSerializer.mergeChunkInto(secondSeg, 0L, onlySecond);

        assertEquals(secondKeys.length, onlySecond.getLongCardinality(),
            "scratch bitmap bled keys from the previously decoded chunk");
        for (final long k : firstKeys) {
          assertFalse(onlySecond.contains(k), "leaked key " + k + " from the previous chunk");
        }
        for (final long k : secondKeys) {
          assertTrue(onlySecond.contains(k));
        }
      }
    }
  }

  /** A truncated packed payload must fail loudly, not read past the view. */
  @Test
  void truncatedPackedPayloadThrows() {
    try (Arena arena = Arena.ofConfined()) {
      final byte[] good = encode(1L, 2L, 3L);
      final byte[] truncated = new byte[good.length - 4];
      System.arraycopy(good, 0, truncated, 0, truncated.length);

      final MemorySegment seg = toSegment(arena, truncated);
      final Roaring64Bitmap dest = new Roaring64Bitmap();
      assertThrows(IllegalArgumentException.class,
          () -> NodeReferencesSerializer.mergeChunkInto(seg, 0L, dest));
    }
  }

  /** An unknown format byte must be rejected rather than silently treated as packed. */
  @Test
  void unknownFormatThrows() {
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment seg = arena.allocate(4, 1);
      seg.set(ValueLayout.JAVA_BYTE, 0, (byte) 0x7A);
      final Roaring64Bitmap dest = new Roaring64Bitmap();
      assertThrows(IllegalArgumentException.class,
          () -> NodeReferencesSerializer.mergeChunkInto(seg, 0L, dest));
    }
  }
}
