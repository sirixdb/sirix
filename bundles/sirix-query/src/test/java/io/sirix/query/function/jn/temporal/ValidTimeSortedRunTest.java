package io.sirix.query.function.jn.temporal;

import io.sirix.index.interval.PostingMask;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.query.function.jn.temporal.ValidTimeIntervalIndex.ChunkCandidates;
import io.sirix.query.function.jn.temporal.ValidTimeIntervalIndex.Evidence;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.Arrays;
import java.util.Random;
import java.util.function.LongFunction;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The in-place sorted hand-off ({@code acceptSorted}) must select exactly what the bitmap path
 * selects: the producer reuses its array after every call, chunks promote to bitmaps at the same
 * unique threshold, tied ends keep only verified keys, and strict-start removal still applies.
 */
final class ValidTimeSortedRunTest {
  private static final int WORDS = 1024;

  private static long[] randomMask(final Random random, final double density) {
    final long[] words = new long[WORDS];
    for (int word = 0; word < WORDS; word++) {
      for (int bit = 0; bit < Long.SIZE; bit++) {
        if (random.nextDouble() < density) {
          words[word] |= 1L << bit;
        }
      }
    }
    return words;
  }

  private static PostingMask mask(final long[] words) {
    final Roaring64Bitmap bitmap = new Roaring64Bitmap();
    for (int word = 0; word < WORDS; word++) {
      for (long bits = words[word]; bits != 0; bits &= bits - 1) {
        bitmap.addLong(((long) word << 6) | Long.numberOfTrailingZeros(bits));
      }
    }
    return PostingMask.from(new NodeReferences(bitmap));
  }

  private static long[] randomRun(final Random random, final int count) {
    final Roaring64Bitmap keys = new Roaring64Bitmap();
    while (keys.getLongCardinality() < count) {
      keys.addLong(random.nextInt(1 << 16));
    }
    return keys.toArray();
  }

  /** Mirrors the producer: one reused buffer, the run only valid during the call. */
  private static void handOver(final ChunkCandidates candidates, final long[] buffer, final long base, final long[] run,
      final boolean verifyOnly) {
    System.arraycopy(run, 0, buffer, 0, run.length);
    candidates.acceptSorted(base, buffer, run.length, verifyOnly);
    Arrays.fill(buffer, 0, run.length, -1L);
  }

  @Test
  void sortedHandOffSelectsWhatTheBitmapPathSelects() {
    final Random random = new Random(20261026);
    for (int round = 0; round < 40; round++) {
      final long[] membershipWords = randomMask(random, random.nextDouble());
      final long[] verificationWords = randomMask(random, random.nextDouble() * 0.2);
      final LongFunction<PostingMask> membership = base -> mask(membershipWords);
      final LongFunction<PostingMask> verification = base -> mask(verificationWords);
      final ChunkCandidates sorted = new ChunkCandidates(membership, verification);
      final ChunkCandidates bitmaps = new ChunkCandidates(membership, verification);
      final long[] buffer = new long[1 << 16];
      final int chunks = 1 + random.nextInt(4);
      final int runs = 1 + random.nextInt(60);
      for (int index = 0; index < runs; index++) {
        final long base = (long) random.nextInt(chunks) << 16;
        final int count = random.nextInt(6) == 0
            ? 1 + random.nextInt(3000)
            : 1 + random.nextInt(40);
        final long[] run = randomRun(random, count);
        final boolean verifyOnly = random.nextInt(5) == 0;
        handOver(sorted, buffer, base, run, verifyOnly);
        final NodeReferences references = NodeReferences.ofSortedArray(run.clone());
        references.getNodeKeys(); // forces the bitmap representation
        bitmaps.accept(base, references, verifyOnly);
      }
      if (random.nextBoolean()) {
        final long base = (long) random.nextInt(chunks) << 16;
        final long[] starts = randomRun(random, 1 + random.nextInt(200));
        sorted.removeExactStarts(base, NodeReferences.ofSortedArray(starts.clone()), false);
        bitmaps.removeExactStarts(base, NodeReferences.ofSortedArray(starts.clone()), false);
      }
      final Evidence expected = bitmaps.evidence();
      final Evidence actual = sorted.evidence();
      assertArrayEquals(expected.members(), actual.members(), "round " + round);
      assertArrayEquals(expected.unverified().toArray(), actual.unverified().toArray(), "round " + round);
      for (int index = 1; index < actual.members().length; index++) {
        assertTrue(actual.members()[index - 1] < actual.members()[index], "ascending at " + index);
      }
    }
  }

  @Test
  void sortedHandOffPromotesAtTheUniqueThresholdOnly() {
    final long[] everything = new long[WORDS];
    Arrays.fill(everything, -1L);
    final PostingMask membership = mask(everything);
    final PostingMask none = PostingMask.from(null);
    for (final int unique : new int[] {1023, 1024}) {
      final ChunkCandidates candidates = new ChunkCandidates(base -> membership, base -> none);
      final long[] run = new long[unique];
      for (int key = 0; key < unique; key++) {
        run[key] = key;
      }
      final long[] buffer = new long[1 << 16];
      for (int repeat = 0; repeat < 3; repeat++) {
        handOver(candidates, buffer, 0, run, false);
      }
      final long[] members = candidates.evidence().members();
      assertArrayEquals(run, members);
    }
  }

  @Test
  void sortedHandOffIgnoresEmptyRunsAndNonMembers() {
    final PostingMask membership = PostingMask.from(NodeReferences.ofSortedArray(new long[] {5, 70}));
    final PostingMask none = PostingMask.from(null);
    final ChunkCandidates candidates = new ChunkCandidates(base -> membership, base -> none);
    final long[] buffer = new long[16];
    candidates.acceptSorted(0, buffer, 0, false);
    handOver(candidates, buffer, 0, new long[] {1, 2, 3, 64, 65}, false);
    handOver(candidates, buffer, 65536, new long[] {5, 70, 71}, true);
    assertEquals(0, candidates.evidence().members().length);
    handOver(candidates, buffer, 65536, new long[] {4, 5, 6, 70}, false);
    assertArrayEquals(new long[] {65536 + 5, 65536 + 70}, candidates.evidence().members());
  }
}
