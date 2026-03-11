package io.sirix.query.compiler.optimizer.stats;

/**
 * Metadata about an index that covers a specific path.
 *
 * @param indexId the index definition ID, or -1 if no index
 * @param type the index type
 * @param exists whether a matching index exists
 */
public record IndexInfo(int indexId, IndexType type, boolean exists) {

  /** Sentinel for no index found. */
  public static final IndexInfo NO_INDEX = new IndexInfo(-1, IndexType.NONE, false);
}
