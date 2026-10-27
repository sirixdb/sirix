package io.sirix.index.interval;

import io.sirix.index.redblacktree.keyvalue.NodeReferences;

import java.util.Arrays;

/** Receives 64K posting chunks as owned references or borrowed compact runs. */
@FunctionalInterface
public interface PostingChunkConsumer {
  /** A tied exclusive end contributes only records requiring exact endpoint verification. */
  void accept(long chunkBase, NodeReferences references, boolean verifyOnly);

  /**
   * A compact chunk handed over without materialising {@link NodeReferences}: {@code keys[0, count)}
   * are chunk-local (below 65536), strictly ascending and only valid for the duration of the call,
   * because the producer reuses the array for the next chunk. The default copies them into
   * {@link #accept}; consumers on a hot path override it to read the run in place.
   */
  default void acceptSorted(final long chunkBase, final long[] keys, final int count, final boolean verifyOnly) {
    if (count == 0) {
      return;
    }
    accept(chunkBase, NodeReferences.ofSortedArray(Arrays.copyOf(keys, count)), verifyOnly);
  }
}
