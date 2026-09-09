/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import java.util.concurrent.atomic.LongAdder;

/**
 * Process-wide counters for the bulk-adoption lane of the storage engine writer: how the overflow
 * carriers of adopted leaves were dispositioned, and whether the async flush lane ever fell back to
 * pinning a leaf with carriers still pending. Diagnostics only — read by loaders' reports and by
 * tests; never consulted by the engine.
 *
 * <p>
 * The writer class itself is package-private; this holder is what a loader outside the package can
 * print. {@code unstaged > 0} means the backend cannot stage immutable pages before the root and
 * every leaf holding a carrier stays pinned until commit; {@code pinnedAfterDeferralCap > 0} means
 * the epoch ordering that publishes staged carriers before re-promotion has regressed.
 * </p>
 */
public final class BulkAdoptionDiagnostics {

  private static final LongAdder CARRIERS_STAGED = new LongAdder();
  private static final LongAdder CARRIERS_UNSTAGED = new LongAdder();
  private static final LongAdder CARRIERS_OVERSIZED = new LongAdder();
  private static final LongAdder CARRIERS_REFUSED = new LongAdder();
  private static final LongAdder KVL_PAGES_PINNED_AFTER_DEFERRAL_CAP = new LongAdder();
  private static final LongAdder KVL_ENCODES_DISCARDED_FOR_UNRESOLVED_CARRIERS = new LongAdder();
  private static final LongAdder KVL_ENCODES_SKIPPED_FOR_UNRESOLVED_CARRIERS = new LongAdder();

  private BulkAdoptionDiagnostics() {
    throw new AssertionError("no instances");
  }

  static void carrierStaged() {
    CARRIERS_STAGED.increment();
  }

  static void carrierUnstaged() {
    CARRIERS_UNSTAGED.increment();
  }

  static void carrierOversized() {
    CARRIERS_OVERSIZED.increment();
  }

  static void carrierRefused() {
    CARRIERS_REFUSED.increment();
  }

  static void kvlPagePinnedAfterDeferralCap() {
    KVL_PAGES_PINNED_AFTER_DEFERRAL_CAP.increment();
  }

  static void kvlEncodeDiscardedForUnresolvedCarriers() {
    KVL_ENCODES_DISCARDED_FOR_UNRESOLVED_CARRIERS.increment();
  }

  static void kvlEncodeSkippedForUnresolvedCarriers() {
    KVL_ENCODES_SKIPPED_FOR_UNRESOLVED_CARRIERS.increment();
  }

  /** Adopted-leaf overflow carriers staged as immutable side pages. */
  public static long carriersStaged() {
    return CARRIERS_STAGED.sum();
  }

  /** Carriers a non-staging backend left resident until final commit. */
  public static long carriersUnstaged() {
    return CARRIERS_UNSTAGED.sum();
  }

  /** Carriers larger than one whole side-page batch, left resident. */
  public static long carriersOversized() {
    return CARRIERS_OVERSIZED.sum();
  }

  /** Carriers the staging lane refused for a reason the adopter did not predict (defect signal). */
  public static long carriersRefused() {
    return CARRIERS_REFUSED.sum();
  }

  /** Leaves pinned after exhausting their deferrals with carriers still pending (defect signal). */
  public static long kvlPagesPinnedAfterDeferralCap() {
    return KVL_PAGES_PINNED_AFTER_DEFERRAL_CAP.sum();
  }

  /**
   * Background pre-serializations that ran to completion and were then thrown away because the encode
   * had minted overflow carriers with no durable key. Each one is a full body encode — region build
   * and codec included — whose bytes never reach the file.
   */
  public static long kvlEncodesDiscardedForUnresolvedCarriers() {
    return KVL_ENCODES_DISCARDED_FOR_UNRESOLVED_CARRIERS.sum();
  }

  /**
   * Pre-serializations the flush lane declined to start because this page had already been refused
   * for unresolved carriers. The counterpart to {@link #kvlEncodesDiscardedForUnresolvedCarriers()}:
   * encodes that did NOT happen. Zero with {@code -Dsirix.flush.skipRefusedOverflowLeaves=false}.
   */
  public static long kvlEncodesSkippedForUnresolvedCarriers() {
    return KVL_ENCODES_SKIPPED_FOR_UNRESOLVED_CARRIERS.sum();
  }

  /** Reset every counter; tests call this before a measured load. */
  public static void reset() {
    CARRIERS_STAGED.reset();
    CARRIERS_UNSTAGED.reset();
    CARRIERS_OVERSIZED.reset();
    CARRIERS_REFUSED.reset();
    KVL_PAGES_PINNED_AFTER_DEFERRAL_CAP.reset();
    KVL_ENCODES_DISCARDED_FOR_UNRESOLVED_CARRIERS.reset();
    KVL_ENCODES_SKIPPED_FOR_UNRESOLVED_CARRIERS.reset();
  }
}
