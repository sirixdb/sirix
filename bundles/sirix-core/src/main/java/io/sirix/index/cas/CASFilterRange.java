package io.sirix.index.cas;

import io.sirix.index.path.PCRCollector;
import io.sirix.index.path.PathFilter;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;
import org.jspecify.annotations.Nullable;

import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * CASFilter filter.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class CASFilterRange {

  /** {@link PathFilter} instance to filter specific paths. */
  private final PathFilter pathFilter;

  /** The minimum value, or {@code null} for an unbounded lower end. */
  private final Atomic min;

  /** The maximum value, or {@code null} for an unbounded upper end. */
  private final Atomic max;

  /** {@code true} if the minimum should be included, {@code false} otherwise */
  private final boolean incMin;

  /** {@code true} if the maximum should be included, {@code false} otherwise */
  private final boolean incMax;

  /**
   * Constructor. Initializes the internal state.
   *
   * @param paths paths to match
   * @param min the minimum value, or {@code null} for an unbounded lower end (one-sided range)
   * @param max the maximum value, or {@code null} for an unbounded upper end (one-sided range)
   * @param incMin include the minimum value
   * @param incMax include the maximum value
   * @param pcrCollector the PCR collector used
   */
  public CASFilterRange(final Set<Path<QNm>> paths, final Atomic min, final Atomic max, final boolean incMin,
      final boolean incMax, final PCRCollector pcrCollector) {
    this.pathFilter = new PathFilter(requireNonNull(paths), pcrCollector);
    this.min = min;
    this.max = max;
    this.incMin = incMin;
    this.incMax = incMax;
  }

  /** The minimum value, or {@code null} for an unbounded lower end. */
  public @Nullable Atomic getMin() {
    return min;
  }

  /** The maximum value, or {@code null} for an unbounded upper end. */
  public @Nullable Atomic getMax() {
    return max;
  }

  /** Whether the minimum value itself is in range. */
  public boolean isMinInclusive() {
    return incMin;
  }

  /** Whether the maximum value itself is in range. */
  public boolean isMaxInclusive() {
    return incMax;
  }

  /**
   * Get the set of path class records (PCRs) for filtering.
   *
   * @return set of PCRs from the path filter
   */
  public Set<Long> getPCRs() {
    return pathFilter.getPCRs();
  }

  /**
   * Check if an atomic value is within the range bounds.
   * 
   * @param key the atomic value to check
   * @return true if in range, false otherwise
   */
  public boolean inRange(Atomic key) {
    final int minKeyCompare = (min != null)
        ? min.compareTo(key)
        : -1;
    final int maxKeyCompare = (max != null)
        ? max.compareTo(key)
        : 1;

    final boolean lowerBoundValid = ((minKeyCompare == 0) && (incMin)) || (minKeyCompare < 0);
    final boolean upperBoundValid = ((maxKeyCompare == 0) && (incMax)) || (maxKeyCompare > 0);

    return upperBoundValid && lowerBoundValid;
  }
}
