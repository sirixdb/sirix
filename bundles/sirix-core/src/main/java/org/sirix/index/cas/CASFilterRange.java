package org.sirix.index.cas;

import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.sirix.index.AtomicUtil;
import org.sirix.index.Filter;
import org.sirix.index.redblacktree.RBNode;
import org.sirix.index.redblacktree.keyvalue.CASValue;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.sirix.index.path.PCRCollector;
import org.sirix.index.path.PathFilter;

import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * CASFilter filter.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class CASFilterRange implements Filter {

  /** The paths to filter. */
  private final Set<Path<QNm>> mPaths;

  /** {@link PathFilter} instance to filter specific paths. */
  private final PathFilter mPathFilter;

  /** The minimum value. */
  private final Atomic mMin;

  /** The maximum value. */
  private final Atomic mMax;

  /** {@code true} if the minimum should be included, {@code false} otherwise */
  private final boolean mIncMin;

  /** {@code true} if the maximum should be included, {@code false} otherwise */
  private final boolean mIncMax;

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
    mPaths = requireNonNull(paths);
    mPathFilter = new PathFilter(mPaths, pcrCollector);
    mMin = requireNonNull(min);
    mMax = requireNonNull(max);
    mIncMin = incMin;
    mIncMax = incMax;
  }

  @Override
  public <K extends Comparable<? super K>> boolean filter(final RBNode<K, NodeReferences> node) {
    final K key = node.getKey();
    if (key instanceof CASValue) {
      final CASValue casValue = (CASValue) key;
      final boolean filtered = mPathFilter.filter(node);

      if (filtered) {
        return inRange(AtomicUtil.toType(casValue.getAtomicValue(), casValue.getType()));
      }
    }
    return false;
  }

  private <K extends Comparable<? super K>> boolean inRange(Atomic key) {
    final int minKeyCompare = (mMin != null) ? mMin.compareTo(key) : -1;
    final int maxKeyCompare = (mMax != null) ? mMax.compareTo(key) : 1;

    final boolean lowerBoundValid = ((minKeyCompare == 0) && (mIncMin)) || (minKeyCompare < 0);
    final boolean upperBoundValid = ((maxKeyCompare == 0) && (mIncMax)) || (maxKeyCompare > 0);

    return upperBoundValid && lowerBoundValid;
  }
}
