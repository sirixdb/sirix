/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

/**
 * A persisted projection's leaves disagree about what its columns MEAN — refuse the store, and say
 * so in those words.
 *
 * <p>
 * This is not corruption and must never be reported as such. The bytes decode fine; what is broken
 * is an agreement between them, which is a WRITER's mistake and is repaired by replacing the
 * unusable definition with a fresh tree, not by quarantining a column. Task #45 spent four rounds
 * chasing "known-corrupt BODY segment" before the actual fault turned out to be a leaf descriptor
 * that maintenance had rewritten with the declared column kind while the payload and the metadata
 * still carried the elected one; the message blamed the bytes because the code had no way to say
 * anything else.
 * </p>
 *
 * <p>
 * Catch sites must therefore treat this DIFFERENTLY from {@link IllegalStateException}: decline the
 * whole store and let the query take the generic pipeline, but do NOT set the per-column corrupt
 * memo. That memo is process-lifetime and store-wide, so recording a disagreement in it disables
 * every fast path on a column whose bytes were never in question (task #50).
 * </p>
 *
 * <p>
 * It extends {@link IllegalStateException} so that existing handlers keep degrading safely rather
 * than propagating a fresh unchecked type out of paths that never expected one — the narrower catch
 * has to come first to get the better behaviour, and a handler that has not been taught the
 * difference still fails safe.
 * </p>
 */
public final class ProjectionStoreInconsistentException extends IllegalStateException {

  private static final long serialVersionUID = 1L;

  /** Index of the leaf whose descriptor disagreed with leaf 0. */
  private final int leaf;

  /**
   * @param leaf index of the disagreeing leaf within the store's directory list
   * @param detail what the disagreement is, in terms a reader can act on
   */
  public ProjectionStoreInconsistentException(final int leaf, final String detail) {
    super("Projection store is INCONSISTENT, not corrupt: leaf " + leaf + " disagrees with leaf 0 about the column"
        + " encodings (" + detail + "). Every leaf of one projection must declare the same kinds; the store's bytes"
        + " are fine but its leaves no longer describe the same thing, so no route over it can be trusted. Queries"
        + " take the generic pipeline. Drop the unusable definition, commit, and create a replacement in a new tree.");
    this.leaf = leaf;
  }

  /** Index of the leaf whose descriptor disagreed with leaf 0. */
  public int leaf() {
    return leaf;
  }
}
