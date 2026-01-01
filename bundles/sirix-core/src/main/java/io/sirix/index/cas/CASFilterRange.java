package io.sirix.index.cas;

import io.sirix.index.AtomicUtil;
import io.sirix.index.Filter;
import io.sirix.index.path.PCRCollector;
import io.sirix.index.path.PathFilter;
import io.sirix.index.redblacktree.RBNodeKey;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;

import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * CASFilter filter.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class CASFilterRange implements Filter {

  /** {@link PathFilter} instance to filter specific paths. */
  private final PathFilter pathFilter;

  /** The minimum value. */
  private final Atomic min;

  /** The maximum value. */
  private final Atomic max;

  /** {@code true} if the minimum should be included, {@code false} otherwise */
  private final boolean incMin;

  /** {@code true} if the maximum should be included, {@code false} otherwise */
  private final boolean incMax;

  /**
   * Constructor. Initializes the internal state.
   *
   * @param paths paths to match
   * @param min the minimum value
   * @param max the maximum value
   * @param incMin include the minimum value
   * @param incMax include the maximum value
   * @param pcrCollector the PCR collector used
   */
  public CASFilterRange(final Set<Path<QNm>> paths, final Atomic min, final Atomic max,
      final boolean incMin, final boolean incMax, final PCRCollector pcrCollector) {
    this.pathFilter = new PathFilter(requireNonNull(paths), pcrCollector);
    this.min = requireNonNull(min);
    this.max = requireNonNull(max);
    this.incMin = incMin;
    this.incMax = incMax;
  }

  /**
   * Get the set of path class records (PCRs) for filtering.
   * @return set of PCRs from the path filter
   */
  public Set<Long> getPCRs() {
    return pathFilter.getPCRs();
  }

  @Override
  public <K extends Comparable<? super K>> boolean filter(final RBNodeKey<K> node) {
    final K key = node.getKey();
    if (key instanceof CASValue casValue) {
      final boolean filtered = pathFilter.filter(node);

      if (filtered) {
        return inRange(AtomicUtil.toType(casValue.getAtomicValue(), casValue.getType()));
      }
    }
    return false;
  }

  /**
   * Check if an atomic value is within the range bounds.
   * @param key the atomic value to check
   * @return true if in range, false otherwise
   */
  public boolean inRange(Atomic key) {
    final int minKeyCompare = (min != null) ? min.compareTo(key) : -1;
    final int maxKeyCompare = (max != null) ? max.compareTo(key) : 1;

    final boolean lowerBoundValid = ((minKeyCompare == 0) && (incMin)) || (minKeyCompare < 0);
    final boolean upperBoundValid = ((maxKeyCompare == 0) && (incMax)) || (maxKeyCompare > 0);

    return upperBoundValid && lowerBoundValid;
  }
}
