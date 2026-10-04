package io.sirix.index.hot;

import io.sirix.cache.Allocators;
import io.sirix.index.IndexType;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTLeafPage;
import io.sirix.utils.OS;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.lang.reflect.Field;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PostingDeltaAccumulatorTest {

  @ParameterizedTest
  @ValueSource(longs = {0, 0xFFFFFFFFL})
  void chronologicalDeltasRetainCompactSortedPostings(final long chunk) throws ReflectiveOperationException {
    final long[] bits = {40, 0, 60, 120, 10, 80, 200, 0, 0, 40};
    final boolean[] removes = {false, false, false, true, false, true, false, true, false, true};
    final TreeSet<Long> expected = new TreeSet<>();
    for (final long bit : new long[] {20, 40, 80, 120}) {
      expected.add((chunk << 16) | bit);
    }
    try (final HOTLeafPage base = leaf(20, 40, 80, 120)) {
      final NodeReferencesSerializer.ChunkAccumulator accumulator = new NodeReferencesSerializer.ChunkAccumulator();
      for (int end = 0; end < bits.length; end++) {
        if (removes[end]) {
          expected.remove((chunk << 16) | bits[end]);
        } else {
          expected.add((chunk << 16) | bits[end]);
        }
        accumulator.addChunk(base, base.valueRef(0), chunk << 16);
        for (int delta = 0; delta <= end; delta++) {
          apply(accumulator, chunk, bits[delta], removes[delta]);
        }
        final NodeReferences result = accumulator.toNodeReferencesAndReset();
        assertNotNull(result);
        assertInstanceOf(long[].class, representation(result), "small corrections must retain the compact read path");
        assertArrayEquals(expected.stream().mapToLong(Long::longValue).toArray(), result.toSortedArray());
        for (final long key : expected) {
          assertTrue(result.contains(key));
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {7, 8, 511, 512})
  void duplicatesAndGrowthRespectTheCompactLimit(final int count) throws ReflectiveOperationException {
    final long[] bits = new long[count];
    for (int i = 0; i < count; i++) {
      bits[i] = (i + 1) * 2L;
    }
    try (final HOTLeafPage base = leaf(bits)) {
      final NodeReferencesSerializer.ChunkAccumulator accumulator = new NodeReferencesSerializer.ChunkAccumulator();
      accumulator.addChunk(base, base.valueRef(0), 0);
      apply(accumulator, 0, 2, false);
      apply(accumulator, 0, count * 2L, false);
      final NodeReferences duplicate = accumulator.toNodeReferencesAndReset();
      assertNotNull(duplicate);
      assertInstanceOf(long[].class, representation(duplicate));
      assertArrayEquals(bits, duplicate.toSortedArray());

      accumulator.addChunk(base, base.valueRef(0), 0);
      apply(accumulator, 0, 1, false);
      final NodeReferences inserted = accumulator.toNodeReferencesAndReset();
      assertNotNull(inserted);
      assertEquals(count + 1, inserted.cardinality());
      assertTrue(inserted.contains(1));
      if (count == 512) {
        assertInstanceOf(Roaring64Bitmap.class, representation(inserted));
      } else {
        assertInstanceOf(long[].class, representation(inserted));
      }
      final long[] expected = new long[count + 1];
      expected[0] = 1;
      System.arraycopy(bits, 0, expected, 1, count);
      assertArrayEquals(expected, inserted.toSortedArray());
    }
  }

  private static void apply(final NodeReferencesSerializer.ChunkAccumulator accumulator, final long chunk,
      final long bit, final boolean remove) {
    try (final HOTLeafPage delta = leaf(bit)) {
      assertTrue(accumulator.applyDelta(delta, delta.valueRef(0), chunk, PostingDeltas.suffix(0, remove), null));
    }
  }

  private static HOTLeafPage leaf(final long... bits) {
    if (!OS.isWindows()) {
      Allocators.getInstance().init(64L * 1024 * 1024);
    }
    final NodeReferences references = new NodeReferences();
    for (final long bit : bits) {
      references.addNodeKey(bit);
    }
    final HOTLeafPage leaf = new HOTLeafPage(1, 1, IndexType.CAS);
    assertTrue(leaf.put(new byte[] {1}, NodeReferencesSerializer.serialize(references)));
    return leaf;
  }

  /** Inspect the allocation contract without adding a test-only production accessor. */
  private static Object representation(final NodeReferences references) throws ReflectiveOperationException {
    final Field field = NodeReferences.class.getDeclaredField("refs");
    field.setAccessible(true);
    return field.get(references);
  }
}
