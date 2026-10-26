package io.sirix.index.changelog;

/**
 * Bounded work for one query execution, shared across temporal aggregate revisions and
 * declarations.
 */
public final class TemporalAggregateCatchUpBudget {
  private TemporalAggregateCatchUpBudget() {}

  /** Execution-owned budget, replenished for each execution even when its context is reused. */
  public static final class Budget {
    private final boolean complete;
    private int chunks = 512;
    private int entries = 16_384;
    private int cells = 262_144;

    public Budget() {
      this(false);
    }

    private Budget(final boolean complete) {
      this.complete = complete;
    }

    /** Exact combining cannot abandon a valid tail because an earlier point consumed its budget. */
    public static Budget complete() {
      return new Budget(true);
    }

    public void chunk() {
      if (!complete && chunks-- <= 0) {
        throw new LimitExceeded();
      }
    }

    /** Charge compact endpoint/value units before decoding or allocating their arithmetic state. */
    public void summary(final int endpoints, final int frequencies, final int lanes) {
      if (endpoints < 0 || frequencies < 0 || lanes < 0 || lanes > 16) {
        throw new IllegalArgumentException("Invalid summary work");
      }
      if (complete) {
        return;
      }
      final long units = (long) endpoints + frequencies;
      final long requiredCells = (long) endpoints * (1 + 3 * lanes) + 5L * frequencies;
      if (units > entries || requiredCells > cells) {
        throw new LimitExceeded();
      }
      entries -= (int) units;
      cells -= (int) requiredCells;
    }


  }

  /** Expected route decline; do not log a stack trace or return a partial result. */
  public static final class LimitExceeded extends RuntimeException {
    public LimitExceeded() {
      super("Temporal aggregate catch-up work budget exhausted", null, false, false);
    }
  }

}
