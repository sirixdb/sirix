/*
 * [New BSD License]
 * Copyright (c) 2024, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.index.interval;

import java.util.function.LongConsumer;

/**
 * Storage SPI the {@link RelationalIntervalTree} drives: one logical ordered map from a composite
 * key {@code (forkNode, endpoint)} to a multiset of record references (node keys).
 *
 * <p>The SirixDB-backed implementation encodes the composite key as a single order-preserving byte
 * string {@code [forkNode:8][endpoint:8]} so {@link #scan} over a fixed {@code forkNode} with an
 * endpoint sub-range is one contiguous HOT-trie range scan. The in-memory reference implementation
 * used by the tests has identical observable semantics.
 *
 * <p>Everything is primitive {@code long}; the only allocation happens inside the concrete store,
 * never in the {@link RelationalIntervalTree} query path.
 *
 * @author Johannes Lichtenberger
 */
public interface OrderedStore {

  /** Add {@code ref} under the composite key {@code (forkNode, endpoint)}. */
  void insert(long forkNode, long endpoint, long ref);

  /** Remove one occurrence of {@code ref} under {@code (forkNode, endpoint)}; no-op if absent. */
  void remove(long forkNode, long endpoint, long ref);

  /**
   * Stream every {@code ref} stored under {@code forkNode} whose endpoint lies in
   * {@code [endpointLo, endpointHi]} (both inclusive) to {@code out}. A ref may be delivered more
   * than once if it was inserted more than once under the same key; callers dedup if required.
   */
  void scan(long forkNode, long endpointLo, long endpointHi, LongConsumer out);
}
