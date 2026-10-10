package io.sirix.query.function.jn.temporal;

import io.sirix.index.interval.PostingMask;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.query.function.jn.temporal.ValidTimeIntervalIndex.ChunkCandidates;

public final class ValidTimeRunProbe {
  private ValidTimeRunProbe() {}

  public static long[] select(final int chunks, final NodeReferences references, final PostingMask membership,
      final PostingMask verification) {
    return select(chunks, references, membership, verification, 1);
  }

  public static long[] select(final int chunks, final NodeReferences references, final PostingMask membership,
      final PostingMask verification, final int runs) {
    if (chunks < 0 || runs < 1) {
      throw new IllegalArgumentException("Chunk count must be nonnegative and run count positive");
    }
    final var candidates = new ChunkCandidates(base -> membership, base -> verification);
    for (int chunk = chunks - 1; chunk >= 0; chunk--) {
      for (int run = 0; run < runs; run++) {
        candidates.accept((long) chunk << 16, references, false);
      }
    }
    return candidates.evidence().keys();
  }
}
