package io.sirix.index.projection;

/** Receiver of 64-bit values whose distinct count is wanted; the same value may arrive any number of times. */
public interface DistinctLongSink {
  /** Record {@code value}; whether it was new is not answered here. */
  void put(long value);
}
