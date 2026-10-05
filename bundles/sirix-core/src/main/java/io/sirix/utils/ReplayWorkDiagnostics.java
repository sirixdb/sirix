package io.sirix.utils;

import java.util.concurrent.atomic.LongAdder;

/**
 * Work units for identity replay and its source bookkeeping. Disabled in production; a static-final
 * gate removes event calls from compiled hot paths. Cursor/storage events are global (including
 * writer reinitialization and nested reader fallbacks), so captures must isolate the measured
 * operation.
 */
public final class ReplayWorkDiagnostics {
  public static final boolean ENABLED = Boolean.getBoolean("sirix.replay.workDiag");
  private static final LongAdder RECORD_VISITS = new LongAdder();
  private static final LongAdder PATH_STEPS = new LongAdder();
  private static final LongAdder CREATED_IDENTITIES = new LongAdder();
  private static final LongAdder STAGED_RECORDS = new LongAdder();
  private static final LongAdder ANCESTOR_STEPS = new LongAdder();
  private static final LongAdder SIDECAR_READS = new LongAdder();
  private static final LongAdder FALLBACK_PAGES = new LongAdder();
  private static final LongAdder BOOKKEEPING = new LongAdder();

  private ReplayWorkDiagnostics() {}

  public static void recordVisited() {
    if (ENABLED)
      RECORD_VISITS.increment();
  }

  public static void pathStep() {
    if (ENABLED)
      PATH_STEPS.increment();
  }

  public static void identityCreated() {
    if (ENABLED)
      CREATED_IDENTITIES.increment();
  }

  public static void recordStaged() {
    if (ENABLED)
      STAGED_RECORDS.increment();
  }

  public static void ancestorStep() {
    if (ENABLED)
      ANCESTOR_STEPS.increment();
  }

  public static void sidecarRead() {
    if (ENABLED)
      SIDECAR_READS.increment();
  }

  public static void fallbackPage() {
    if (ENABLED)
      FALLBACK_PAGES.increment();
  }

  public static void bookkeeping(final long operations) {
    if (ENABLED)
      BOOKKEEPING.add(operations);
  }

  public static long recordVisits() {
    return RECORD_VISITS.sum();
  }

  public static long pathSteps() {
    return PATH_STEPS.sum();
  }

  public static long createdIdentities() {
    return CREATED_IDENTITIES.sum();
  }

  public static long stagedRecords() {
    return STAGED_RECORDS.sum();
  }

  public static long ancestorSteps() {
    return ANCESTOR_STEPS.sum();
  }

  public static long sidecarReads() {
    return SIDECAR_READS.sum();
  }

  public static long fallbackPages() {
    return FALLBACK_PAGES.sum();
  }

  public static long bookkeepingOperations() {
    return BOOKKEEPING.sum();
  }
}
