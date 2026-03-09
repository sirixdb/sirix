package io.sirix.utils;

/**
 * Lightweight replacement for {@code com.google.common.collect.ComparisonChain}.
 * Uses the sentinel pattern: once the result is determined (non-zero), all subsequent
 * comparisons are skipped at zero cost by returning the INACTIVE instance.
 *
 * <p>Thread-safety: not thread-safe (single-threaded usage only).
 */
public abstract class ComparisonChain {

  /** The active chain that performs comparisons until a non-zero result is found. */
  private static final ComparisonChain ACTIVE = new ComparisonChain() {
    @Override
    public ComparisonChain compare(final Comparable<?> left, final Comparable<?> right) {
      @SuppressWarnings("unchecked")
      final Comparable<Object> typedLeft = (Comparable<Object>) left;
      final int result = typedLeft.compareTo(right);
      return classify(result);
    }

    @Override
    public ComparisonChain compare(final int left, final int right) {
      return classify(Integer.compare(left, right));
    }

    @Override
    public ComparisonChain compare(final long left, final long right) {
      return classify(Long.compare(left, right));
    }

    @Override
    public ComparisonChain compare(final double left, final double right) {
      return classify(Double.compare(left, right));
    }

    @Override
    public ComparisonChain compare(final float left, final float right) {
      return classify(Float.compare(left, right));
    }

    @Override
    public ComparisonChain compareTrueFirst(final boolean left, final boolean right) {
      return classify(Boolean.compare(right, left)); // reversed: true < false
    }

    @Override
    public ComparisonChain compareFalseFirst(final boolean left, final boolean right) {
      return classify(Boolean.compare(left, right));
    }

    @Override
    public int result() {
      return 0;
    }

    private ComparisonChain classify(final int result) {
      if (result < 0) {
        return LESS;
      }
      if (result > 0) {
        return GREATER;
      }
      return ACTIVE;
    }
  };

  /** Sentinel for a determined "less than" result -- all subsequent comparisons are no-ops. */
  private static final ComparisonChain LESS = new InactiveComparisonChain(-1);

  /** Sentinel for a determined "greater than" result -- all subsequent comparisons are no-ops. */
  private static final ComparisonChain GREATER = new InactiveComparisonChain(1);

  private ComparisonChain() {
  }

  /**
   * Begins a new comparison chain.
   *
   * @return the active comparison chain
   */
  public static ComparisonChain start() {
    return ACTIVE;
  }

  /**
   * Compares two {@link Comparable} values, proceeding only if all previous comparisons
   * resulted in equality.
   *
   * @param left  the left value
   * @param right the right value
   * @return this chain for further comparisons
   */
  public abstract ComparisonChain compare(Comparable<?> left, Comparable<?> right);

  /**
   * Compares two {@code int} values without boxing.
   *
   * @param left  the left value
   * @param right the right value
   * @return this chain for further comparisons
   */
  public abstract ComparisonChain compare(int left, int right);

  /**
   * Compares two {@code long} values without boxing.
   *
   * @param left  the left value
   * @param right the right value
   * @return this chain for further comparisons
   */
  public abstract ComparisonChain compare(long left, long right);

  /**
   * Compares two {@code double} values without boxing.
   *
   * @param left  the left value
   * @param right the right value
   * @return this chain for further comparisons
   */
  public abstract ComparisonChain compare(double left, double right);

  /**
   * Compares two {@code float} values without boxing.
   *
   * @param left  the left value
   * @param right the right value
   * @return this chain for further comparisons
   */
  public abstract ComparisonChain compare(float left, float right);

  /**
   * Compares two {@code boolean} values, considering {@code true} to be less than {@code false}.
   *
   * @param left  the left value
   * @param right the right value
   * @return this chain for further comparisons
   */
  public abstract ComparisonChain compareTrueFirst(boolean left, boolean right);

  /**
   * Compares two {@code boolean} values, considering {@code false} to be less than {@code true}.
   *
   * @param left  the left value
   * @param right the right value
   * @return this chain for further comparisons
   */
  public abstract ComparisonChain compareFalseFirst(boolean left, boolean right);

  /**
   * Returns the final comparison result.
   *
   * @return a negative value, zero, or a positive value following {@link Comparable} convention
   */
  public abstract int result();

  /**
   * Inactive sentinel that short-circuits all subsequent comparisons.
   * The result is already determined.
   */
  private static final class InactiveComparisonChain extends ComparisonChain {
    private final int result;

    InactiveComparisonChain(final int result) {
      this.result = result;
    }

    @Override
    public ComparisonChain compare(final Comparable<?> left, final Comparable<?> right) {
      return this;
    }

    @Override
    public ComparisonChain compare(final int left, final int right) {
      return this;
    }

    @Override
    public ComparisonChain compare(final long left, final long right) {
      return this;
    }

    @Override
    public ComparisonChain compare(final double left, final double right) {
      return this;
    }

    @Override
    public ComparisonChain compare(final float left, final float right) {
      return this;
    }

    @Override
    public ComparisonChain compareTrueFirst(final boolean left, final boolean right) {
      return this;
    }

    @Override
    public ComparisonChain compareFalseFirst(final boolean left, final boolean right) {
      return this;
    }

    @Override
    public int result() {
      return result;
    }
  }
}
