package io.sirix.index.interval;

import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The mask must agree with per-key iteration for every chunk representation Roaring can produce.
 */
final class PostingMaskTest {
  @Test
  void packedRunSetsExactlyItsBits() {
    final PostingMask mask = PostingMask.from(NodeReferences.ofSortedArray(new long[] {0, 1, 63, 64, 65535}));
    assertFalse(mask.isEmpty());
    assertEquals(0b11L | (1L << 63), mask.word(0));
    assertEquals(1L, mask.word(1));
    assertEquals(1L << 63, mask.word(1023));
    assertEquals(0L, mask.word(2));
  }

  @Test
  void sparseDenseAndRunContainersAgreeWithPerKeyIteration() {
    final Roaring64Bitmap sparse = new Roaring64Bitmap();
    for (final long key : new long[] {3, 64, 65, 4096, 65534}) {
      sparse.addLong(key);
    }
    final Roaring64Bitmap dense = new Roaring64Bitmap();
    dense.addRange(0L, 65536L);
    dense.removeLong(5);
    dense.removeLong(64);
    dense.removeLong(65535);
    final Roaring64Bitmap runs = new Roaring64Bitmap();
    runs.addRange(100L, 7000L);
    runs.addRange(7001L, 7002L);
    runs.addRange(60000L, 65536L);
    runs.runOptimize();
    for (final Roaring64Bitmap bitmap : new Roaring64Bitmap[] {sparse, dense, runs}) {
      final long[] expected = new long[1024];
      bitmap.forEach(key -> expected[(int) (key >>> 6)] |= 1L << key);
      final PostingMask mask = PostingMask.from(new NodeReferences(bitmap));
      final long[] actual = new long[1024];
      for (int word = 0; word < 1024; word++) {
        actual[word] = mask.word(word);
      }
      assertArrayEquals(expected, actual);
    }
  }

  @Test
  void emptyAndNullReferencesGiveTheEmptyMask() {
    assertTrue(PostingMask.from(null).isEmpty());
    assertTrue(PostingMask.from(new NodeReferences(new Roaring64Bitmap())).isEmpty());
    assertEquals(0L, PostingMask.from(null).word(1023));
  }

  @Test
  void keysOutsideTheChunkAreRejectedForBothRepresentations() {
    assertThrows(IllegalArgumentException.class,
        () -> PostingMask.from(NodeReferences.ofSortedArray(new long[] {1, 65536})));
    final Roaring64Bitmap bitmap = new Roaring64Bitmap();
    bitmap.addLong(1);
    bitmap.addLong(70000);
    assertThrows(IllegalArgumentException.class, () -> PostingMask.from(new NodeReferences(bitmap)));
  }

  @Test
  void fillRangeCoversWordBoundaries() {
    final long[] words = new long[4];
    PostingMask.fillRange(words, 0, 64);
    assertEquals(-1L, words[0]);
    PostingMask.fillRange(words, 127, 129);
    assertEquals(1L << 63, words[1]);
    assertEquals(1L, words[2]);
    PostingMask.fillRange(words, 190, 256);
    assertEquals(1L | (-1L << 62), words[2]);
    assertEquals(-1L, words[3]);
    PostingMask.fillRange(words, 70, 70);
    assertEquals(1L << 63, words[1]);
  }
}
