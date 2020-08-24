package org.sirix.index.path;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.sirix.index.Filter;
import org.sirix.index.redblacktree.RBNode;
import org.sirix.index.redblacktree.keyvalue.CASValue;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;
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
  private final boolean genericPath;

  /** The paths to filter. */
  private final Set<Path<QNm>> paths;

  /** Maximum known path class record (PCR). */
  private long maxKnownPCR;

  /** Set of PCRs to filter. */
  private Set<Long> pcrFilter;

  /** Path class record collector. */
  private final PCRCollector pcrCollector;

  /**
   * Constructor. Initializes the internal state.
   *
   * @param paths paths to match
   * @param pcrCollector path class record collector
   */
  public PathFilter(final Set<Path<QNm>> paths, final PCRCollector pcrCollector) {
    this.paths = requireNonNull(paths, "The paths must not be null.");
    this.pcrCollector =
        requireNonNull(pcrCollector, "The path class record collector must not be null.");
    genericPath = this.paths.isEmpty();
    final PCRValue pcrValue = this.pcrCollector.getPCRsForPaths(this.paths);
    maxKnownPCR = pcrValue.getMaxPCR();
    pcrFilter = pcrValue.getPCRs();
  }

  public Set<Long> getPCRs() {
    return Collections.unmodifiableSet(pcrFilter);
  }

  public PCRCollector getPCRCollector() {
    return pcrCollector;
  }

  /**
   * Filter the node.
   *
   * @param node node to filter
   * @return {@code true} if the node has been filtered, {@code false} otherwise
   */
  @Override
  public <K extends Comparable<? super K>> boolean filter(final RBNode<K, NodeReferences> node) {
    if (genericPath) {
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

    if (pcr > maxKnownPCR) {
      final PCRValue pcrValue = pcrCollector.getPCRsForPaths(paths);
      maxKnownPCR = pcrValue.getMaxPCR();
      pcrFilter = pcrValue.getPCRs();
    }

    return pcrFilter.contains(pcr);
  }
}
