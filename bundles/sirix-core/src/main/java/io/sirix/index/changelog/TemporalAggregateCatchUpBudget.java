package io.sirix.index.changelog;

/** Bounded query-owned work shared across temporal aggregate revisions and declarations. */
public final class TemporalAggregateCatchUpBudget {
  private TemporalAggregateCatchUpBudget() {}

  /** Query-owned budget, shared across revisions and groupings. */
  public static final class Budget {
    private int chunks = 512;
    private int entries = 16_384;
    private int cells = 262_144;

    public void chunk() {
      if (chunks-- <= 0) {
        throw new LimitExceeded();
      }
    }

    /** Charge compact endpoint/value units before decoding or allocating their arithmetic state. */
    public void summary(final int endpoints, final int frequencies, final int lanes) {
      if (endpoints < 0 || frequencies < 0 || lanes < 0 || lanes > 16) {
        throw new IllegalArgumentException("Invalid summary work");
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
