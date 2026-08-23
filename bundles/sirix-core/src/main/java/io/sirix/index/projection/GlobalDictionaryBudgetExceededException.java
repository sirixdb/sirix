/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

/**
 * A resource-wide value dictionary declined an aggregate-budget or structural admission while a
 * build was still feeding it.
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

  /** Structural admission reason, or {@code null} for the configured aggregate byte budget. */
  private final String admissionDetail;

  GlobalDictionaryBudgetExceededException(final int column, final long retainedBytes, final long budgetBytes,
      final int entryCount) {
    this(column, retainedBytes, budgetBytes, entryCount, null);
  }

  GlobalDictionaryBudgetExceededException(final int column, final long retainedBytes, final long budgetBytes,
      final int entryCount, final String admissionDetail) {
    super(message(column, retainedBytes, budgetBytes, entryCount, admissionDetail));
    this.column = column;
    this.retainedBytes = retainedBytes;
    this.budgetBytes = budgetBytes;
    this.entryCount = entryCount;
    this.admissionDetail = admissionDetail;
  }

  private static String message(final int column, final long retainedBytes, final long budgetBytes,
      final int entryCount, final String admissionDetail) {
    final String reason = admissionDetail == null
        ? "reached its configured aggregate budget of " + budgetBytes + " B"
        : "declined an unsafe allocation before mutation: " + admissionDetail;
    final String remediation = admissionDetail == null
        ? " Raise the configured byte budget, use per-leaf dictionaries, or split the ingest into"
            + " bounded append generations."
        : " Use per-leaf dictionaries or split the ingest into bounded append generations; raising"
            + " the byte budget cannot override a structural allocation ceiling.";
    return "Resource-wide value dictionary for column " + column + " " + reason + "; retained " + retainedBytes
        + " B across " + entryCount
        + " distinct values. The projection is abandoned for this load (readers fall back to the generic"
        + " pipeline); the load itself completes." + remediation;
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

  /** Structural admission reason, or {@code null} when only the aggregate byte budget was hit. */
  public String admissionDetail() {
    return admissionDetail;
  }
}
