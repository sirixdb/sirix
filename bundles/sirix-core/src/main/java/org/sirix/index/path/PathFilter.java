package org.sirix.index.path;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.sirix.index.Filter;
import org.sirix.index.avltree.AVLNode;
import org.sirix.index.avltree.keyvalue.CASValue;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.index.path.summary.PathSummaryReader;

import java.util.Collections;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Path filter for {@link PathSummaryReader}, filtering specific path types.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class PathFilter implements Filter {

  /** Type to filter. */
  private final boolean mGenericPath;

  /** The paths to filter. */
  private final Set<Path<QNm>> mPaths;

  /** Maximum known path class record (PCR). */
  private long mMaxKnownPCR;

  /** Set of PCRs to filter. */
  private Set<Long> mPCRFilter;

  /** Path class record collector. */
  private final PCRCollector mPCRCollector;

  /**
   * Constructor. Initializes the internal state.
   *
   * @param paths paths to match
   * @param pcrCollector path class record collector
   */
  public PathFilter(final Set<Path<QNm>> paths, final PCRCollector pcrCollector) {
    mPaths = requireNonNull(paths, "The paths must not be null.");
    mPCRCollector =
        requireNonNull(pcrCollector, "The path class record collector must not be null.");
    mGenericPath = mPaths.isEmpty();
    final PCRValue pcrValue = mPCRCollector.getPCRsForPaths(mPaths);
    mMaxKnownPCR = pcrValue.getMaxPCR();
    mPCRFilter = pcrValue.getPCRs();
  }

  public Set<Long> getPCRs() {
    return Collections.unmodifiableSet(mPCRFilter);
  }

  public PCRCollector getPCRCollector() {
    return mPCRCollector;
  }

  /**
   * Filter the node.
   *
   * @param node node to filter
   * @return {@code true} if the node has been filtered, {@code false} otherwise
   */
  @Override
  public <K extends Comparable<? super K>> boolean filter(final AVLNode<K, NodeReferences> node) {
    if (mGenericPath) {
      return true;
    }

    final K key = node.getKey();

    long pcr;
    if (key instanceof Long)
      pcr = (Long) key;
    else if (key instanceof CASValue)
      pcr = ((CASValue) key).getPathNodeKey();
    else
      throw new IllegalStateException();

    if (pcr > mMaxKnownPCR) {
      final PCRValue pcrValue = mPCRCollector.getPCRsForPaths(mPaths);
      mMaxKnownPCR = pcrValue.getMaxPCR();
      mPCRFilter = pcrValue.getPCRs();
    }

    return mPCRFilter.contains(pcr);
  }
}
