package io.sirix.index.projection;

/**
 * Where a kernel puts the 128-bit hashes it computes: a {@link DistinctHash128Set} directly, or a
 * worker's buffered handle on a {@link SharedDistinctHash128Set} several workers fill together.
 */
public interface DistinctHash128Sink {
  /** Record the key {@code (lo, hi)}; whether it was new is not answered here. */
  void put(long lo, long hi);
}
