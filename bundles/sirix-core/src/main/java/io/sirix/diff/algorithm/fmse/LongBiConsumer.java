package io.sirix.diff.algorithm.fmse;

/**
 * A functional interface that accepts two primitive {@code long} values. Used in the FMSE matching
 * algorithm to avoid autoboxing overhead from {@code BiConsumer<Long, Long>}.
 */
@FunctionalInterface
public interface LongBiConsumer {
  void accept(long x, long y);
}
