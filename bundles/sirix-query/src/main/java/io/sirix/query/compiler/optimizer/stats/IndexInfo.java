package io.sirix.query.compiler.optimizer.stats;

import io.sirix.index.IndexType;

/**
 * Metadata about an index that covers a specific path.
 *
 * @param indexId the index definition ID, or -1 if no index
 * @param type the index type (from {@link io.sirix.index.IndexType}), or null if no index
 * @param exists whether a matching index exists
 */
public record IndexInfo(int indexId, IndexType type, boolean exists) {

  /** Sentinel for no index found. */
  public static final IndexInfo NO_INDEX = new IndexInfo(-1, null, false);
}
