/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

/**
 * A resource-wide value dictionary reached its byte budget while a build was still feeding it.
 *
 * <p>
 * This is a DECLINE, not a failure: a bounded structure hitting its bound must give the build a way
 * to stop cheaply, and the build's answer is to abandon the projection (slot 0 keeps its stale
 * tombstone, readers stay on the generic pipeline) and let the LOAD COMPLETE. What it must never
 * become is the alternative that produced it — an arena that keeps doubling until the collector
 * spends every core and the load stops producing rows without ever failing.
 * </p>
 *
 * <p>
 * Typed on purpose rather than an {@code IllegalStateException}: the catch that abandons the
 * projection has to distinguish "this dictionary got too big", which is expected at scale and
 * recoverable, from a genuine encoding fault, which is not. Same discipline as the projection
 * column store's refusal types — a control-flow signal must never be mistaken for corruption.
 * </p>
 */
public final class GlobalDictionaryBudgetExceededException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  /** The column whose dictionary hit the bound. */
  private final int column;

  /** Bytes the dictionary retained when it stopped. */
  private final long retainedBytes;

  /** The bound it was given. */
  private final long budgetBytes;

  /** Distinct values interned before it stopped. */
  private final int entryCount;

  GlobalDictionaryBudgetExceededException(final int column, final long retainedBytes, final long budgetBytes,
      final int entryCount) {
    super("Resource-wide value dictionary for column " + column + " reached its budget: retained " + retainedBytes
        + " B across " + entryCount + " distinct values, budget " + budgetBytes
        + " B. The projection is abandoned for this load (readers fall back to the generic pipeline); the load"
        + " itself completes. Raise -Dsirix.projection.globalDict.budgetBytes, pass an expected-row-count hint"
        + " so the election can decline this column up front, or build the index after the load with"
        + " jn:create-projection-index.");
    this.column = column;
    this.retainedBytes = retainedBytes;
    this.budgetBytes = budgetBytes;
    this.entryCount = entryCount;
  }

  /** The column whose dictionary hit the bound. */
  public int column() {
    return column;
  }

  /** Bytes the dictionary retained when it stopped. */
  public long retainedBytes() {
    return retainedBytes;
  }

  /** The bound it was given. */
  public long budgetBytes() {
    return budgetBytes;
  }

  /** Distinct values interned before it stopped. */
  public int entryCount() {
    return entryCount;
  }
}
