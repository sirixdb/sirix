/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.cache.Allocators;
import io.sirix.cache.AggregationBudget;

/**
 * Planning view of the allocator-owned aggregation floor. Caches cannot commit its eighth of the
 * native budget. Each scan acquires exclusive capacity before planning and pays actual region
 * capacity through its grant. Ordinary and bounded top-k plans use the same granted bytes; neither
 * divides free headroom again nor manufactures a minimum group count when memory is unavailable.
 * Fixed worker and rounding overhead is removed by {@link GroupWorkspace#preparePlan}; the spill
 * planner then charges the full stripe, index growth and merge overlap per resident group.
 */
public final class AggregationShare {

  /** One scan plans for at most this fraction of the allocator's budget. */
  public static final int BUDGET_DIVISOR = AggregationBudget.DIVISOR;

  private static volatile long budgetForTesting = -1L;
  private static volatile long headroomForTesting = -1L;

  private AggregationShare() {}

  /**
   * THE rule, for an explicit budget and headroom (pure: tests, and the planner's own pure twins).
   *
   * @param budget the allocator's budget in bytes
   * @param headroom ungranted execution capacity plus the scan's own exclusive grant
   * @return {@code min(budget / 8, headroom)}, never negative
   */
  public static long plannedBytes(final long budget, final long headroom) {
    return Math.max(0L, Math.min(budget / BUDGET_DIVISOR, headroom));
  }

  /** A bounded top-k has the same exclusive execution allowance as every other scan. */
  public static long plannedBoundedBytes(final long budget, final long headroom) {
    return plannedBytes(budget, headroom);
  }

  /** The share right now, for a scan that holds nothing yet. */
  public static long plannedBytes() {
    return plannedBytes(budgetBytes(), headroomBytes(null));
  }

  /** The allocator's budget: the upper bound of everything it lends. The test override when set. */
  public static long budgetBytes() {
    final long testing = budgetForTesting;
    if (testing >= 0L) {
      return testing;
    }
    return Math.max(0L, Allocators.getInstance().getMaxBufferSize());
  }

  /**
   * Execution capacity available for admission plus this scan's existing grant. This is a planning
   * diagnostic only; admission must atomically acquire a grant before using capacity. The test
   * override applies when set.
   *
   * @param workspace the planning scan's workspace, or {@code null} for a scan that holds nothing
   */
  public static long headroomBytes(final GroupWorkspace workspace) {
    final long testing = headroomForTesting;
    if (testing >= 0L) {
      return testing;
    }
    final AggregationBudget execution = Allocators.getInstance().aggregationBudget();
    final long own = workspace == null
        ? 0L
        : workspace.grantedBytes();
    return Math.max(0L, execution.capacity() - execution.grantedBytes()) + own;
  }

  /**
   * Test seam: pin the budget the planner reads.
   *
   * @param bytes the budget in bytes, or a negative value to restore the allocator's
   * @return the previous override, for restoring in a finally block
   */
  public static long setBudgetForTesting(final long bytes) {
    final long previous = budgetForTesting;
    budgetForTesting = bytes;
    return previous;
  }

  /**
   * Test seam: pin the headroom the planner reads.
   *
   * @param bytes the headroom in bytes, or a negative value to restore the allocator's
   * @return the previous override, for restoring in a finally block
   */
  public static long setHeadroomForTesting(final long bytes) {
    final long previous = headroomForTesting;
    headroomForTesting = bytes;
    return previous;
  }

  /** The figures behind a planned share, for the planner's diagnostics line. */
  public static String describe() {
    final long budget = budgetBytes();
    final long headroom = headroomBytes(null);
    final AggregationBudget execution = Allocators.getInstance().aggregationBudget();
    return "allocatorBudgetMB=" + (budget >> 20) + " allocatorHeadroomMB=" + (headroom >> 20) + " aggregationShareMB="
        + (plannedBytes(budget, headroom) >> 20) + " executionCapacityBytes=" + execution.capacity()
        + " executionGrantedBytes=" + execution.grantedBytes() + " executionPeakGrantedBytes="
        + execution.peakGrantedBytes() + " executionAdmissionWaits=" + execution.waits();
  }
}
