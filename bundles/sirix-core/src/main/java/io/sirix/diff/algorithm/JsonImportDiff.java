package io.sirix.diff.algorithm;

import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;

/**
 * Interface for JSON tree-to-tree diff algorithms that produce edit scripts.
 */
public interface JsonImportDiff {

  /**
   * Computes the diff and applies it to the write transaction, transforming the old revision into
   * the new revision.
   *
   * @param wtx write transaction on the old revision
   * @param rtx read-only transaction on the new revision
   */
  void diff(JsonNodeTrx wtx, JsonNodeReadOnlyTrx rtx);

  /**
   * Returns the name of this diff algorithm.
   *
   * @return the algorithm name
   */
  String getName();
}
