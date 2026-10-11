package io.sirix.query.function.jn.temporal;

import io.sirix.index.interval.PostingMask;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.query.function.jn.temporal.ValidTimeIntervalIndex.ChunkCandidates;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * Dense postings arrive as Roaring bitmap or run containers and are marked word-wise rather than
 * key by key; the emitted keys must still equal the per-key reference, with ties handled the same.
 */
final class ValidTimeDenseChunkTest {
  @Test
  void denseContainersEmitTheSameKeysAsPerKeyMarking() {
    final Roaring64Bitmap members = new Roaring64Bitmap();
    members.addRange(0L, 65536L);
    members.removeLong(7);
    members.removeLong(4095);
    final PostingMask membership = PostingMask.from(new NodeReferences(members));
    final PostingMask verification = PostingMask.from(NodeReferences.ofSortedArray(new long[] {64, 4096, 65535}));
    final Roaring64Bitmap dense = new Roaring64Bitmap();
    dense.addRange(0L, 8192L);
    dense.addRange(60000L, 65536L);
    final Roaring64Bitmap runs = new Roaring64Bitmap();
    runs.addRange(4000L, 4200L);
    runs.addRange(65000L, 65536L);
    runs.runOptimize();
    final NodeReferences sparse = NodeReferences.ofSortedArray(new long[] {7, 64, 4095, 4096, 30000});
    final var candidates = new ChunkCandidates(base -> membership, base -> verification);
    candidates.accept(0, new NodeReferences(dense), false);
    candidates.accept(65536, new NodeReferences(runs), true);
    candidates.accept(0, sparse, false);
    candidates.accept(65536, sparse, false);
    candidates.removeExactStarts(65536, NodeReferences.ofSortedArray(new long[] {4096, 65535}), false);
    final var evidence = candidates.evidence();

    final Roaring64Bitmap expected = new Roaring64Bitmap();
    dense.forEach(key -> {
      if (members.contains(key)) {
        expected.addLong(key);
      }
    });
    // verifyOnly keeps only keys the verification mask confirms, in the second chunk.
    runs.forEach(key -> {
      if (members.contains(key) && verification.word((int) (key >>> 6)) << ~key < 0) {
        expected.addLong(65536 + key);
      }
    });
    sparse.forEachNodeKey(key -> {
      if (members.contains(key)) {
        expected.addLong(key);
        expected.addLong(65536 + key);
      }
    });
    // Exact starts are removed from the second chunk unless verification keeps them.
    expected.removeLong(65536 + 4096);
    expected.removeLong(65536 + 65535);
    for (final long key : new long[] {4096, 65535}) {
      if (verification.word((int) (key >>> 6)) << ~key < 0
          && (dense.contains(key) || runs.contains(key) || sparse.contains(key))) {
        expected.addLong(65536 + key);
      }
    }
    assertArrayEquals(expected.toArray(), evidence.keys());
  }
}
