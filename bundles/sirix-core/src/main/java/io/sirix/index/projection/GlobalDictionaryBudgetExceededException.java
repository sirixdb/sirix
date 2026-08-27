/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.util.Objects;

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
 *
 * <h2>Why the breaching quantity is carried separately from the retention</h2>
 *
 * <p>
 * Every byte-budget guard compares retention PLUS a reservation — the allocation an insert is about
 * to make, the workspace a flush needs, the projected flush peak — against the budget, and a
 * dictionary whose retention ALONE already exceeded the bound would have been refused one admission
 * earlier. So {@code retainedBytes < budgetBytes} holds on essentially every refusal, and an
 * operator notice quoting retention against the budget reads as though the guard misfired. The term
 * that actually tripped the comparison therefore travels with the decline, named
 * ({@link #breachingTerm()}) and valued ({@link #breachingBytes()}), so the printed arithmetic
 * explains the breach it announces and the remediation can quote a budget worth raising to.
 */
public final class GlobalDictionaryBudgetExceededException extends RuntimeException {

  private static final long serialVersionUID = 2L;

  /** The column whose dictionary hit the bound. */
  private final int column;

  /** Bytes the dictionary retained when it stopped. */
  private final long retainedBytes;

  /**
   * The quantity that was actually compared against the budget, in bytes; equal to
   * {@link #retainedBytes} when no byte comparison took place.
   */
  private final long breachingBytes;

  /**
   * What {@link #breachingBytes} is — {@code "retained+pending"}, {@code "flush-peak"}, … — or
   * {@code null} when the decline was structural and no byte quantity was weighed at all.
   */
  private final String breachingTerm;

  /** The bound it was given. */
  private final long budgetBytes;

  /** Distinct values interned before it stopped. */
  private final int entryCount;

  /** Structural admission reason, or {@code null} for the configured aggregate byte budget. */
  private final String admissionDetail;

  /**
   * A byte-budget breach: {@code breachingBytes} is the term the guard weighed, {@code breachingTerm}
   * names it, and both are reported against {@code budgetBytes}.
   */
  static GlobalDictionaryBudgetExceededException budgetBreach(final int column, final long retainedBytes,
      final long breachingBytes, final String breachingTerm, final long budgetBytes, final int entryCount,
      final String admissionDetail) {
    return new GlobalDictionaryBudgetExceededException(column, retainedBytes, breachingBytes,
        Objects.requireNonNull(breachingTerm, "breachingTerm must name the term that tripped the budget"), budgetBytes,
        entryCount, admissionDetail);
  }

  /**
   * A structural admission ceiling — a value too long for the V0 layout, an append generation at its
   * entry limit, a chunk directory at its reference limit. Nothing was weighed against the byte
   * budget, so nothing may be reported as having exceeded it.
   */
  static GlobalDictionaryBudgetExceededException structuralDecline(final int column, final long retainedBytes,
      final long budgetBytes, final int entryCount, final String admissionDetail) {
    return new GlobalDictionaryBudgetExceededException(column, retainedBytes, retainedBytes, null, budgetBytes,
        entryCount, Objects.requireNonNull(admissionDetail, "a structural decline must state which ceiling it hit"));
  }

  private GlobalDictionaryBudgetExceededException(final int column, final long retainedBytes, final long breachingBytes,
      final String breachingTerm, final long budgetBytes, final int entryCount, final String admissionDetail) {
    super(message(column, retainedBytes, breachingBytes, breachingTerm, budgetBytes, entryCount, admissionDetail));
    this.column = column;
    this.retainedBytes = retainedBytes;
    this.breachingBytes = breachingBytes;
    this.breachingTerm = breachingTerm;
    this.budgetBytes = budgetBytes;
    this.entryCount = entryCount;
    this.admissionDetail = admissionDetail;
  }

  private static String message(final int column, final long retainedBytes, final long breachingBytes,
      final String breachingTerm, final long budgetBytes, final int entryCount, final String admissionDetail) {
    final StringBuilder message = new StringBuilder(384);
    message.append("Resource-wide value dictionary for column ").append(column);
    if (admissionDetail == null) {
      message.append(" reached its configured aggregate budget of ").append(budgetBytes).append(" B");
    } else {
      message.append(" declined an unsafe allocation before mutation: ").append(admissionDetail);
    }
    if (breachingTerm != null) {
      message.append("; its ")
             .append(breachingTerm)
             .append(" came to ")
             .append(breachingBytes)
             .append(" B, past the ")
             .append(budgetBytes)
             .append(" B budget, with ")
             .append(retainedBytes)
             .append(" B already retained across ");
    } else {
      message.append("; retained ").append(retainedBytes).append(" B across ");
    }
    message.append(entryCount)
           .append(" distinct values. The projection is abandoned for this load (readers fall back to the generic"
               + " pipeline); the load itself completes.");
    if (admissionDetail == null) {
      // The notice's own remedy has to quote a number worth raising to, or it sends the operator
      // back to guess at the bound the guard already computed for them.
      message.append(" Raise the configured byte budget to at least ")
             .append(breachingBytes)
             .append(" B, use per-leaf dictionaries, or split the ingest into bounded append generations.");
    } else {
      message.append(" Use per-leaf dictionaries or split the ingest into bounded append generations; raising"
          + " the byte budget cannot override a structural allocation ceiling.");
    }
    return message.toString();
  }

  /** The column whose dictionary hit the bound. */
  public int column() {
    return column;
  }

  /** Bytes the dictionary retained when it stopped. */
  public long retainedBytes() {
    return retainedBytes;
  }

  /**
   * The quantity the guard actually weighed against {@link #budgetBytes()}, in bytes. Strictly
   * greater than the budget whenever {@link #breachingTerm()} is non-{@code null}; equal to
   * {@link #retainedBytes()} for a structural decline, which weighed nothing.
   */
  public long breachingBytes() {
    return breachingBytes;
  }

  /**
   * What {@link #breachingBytes()} is, for a message that has to explain its own arithmetic, or
   * {@code null} when the decline was structural rather than a byte-budget breach.
   */
  public String breachingTerm() {
    return breachingTerm;
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
