package org.sirix.index.cas;

import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.sirix.index.Filter;
import org.sirix.index.SearchMode;
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
public final class CASFilter implements Filter {

  /** The paths to filter. */
  private final Set<Path<QNm>> paths;

  /** {@link PathFilter} instance to filter specific paths. */
  private final PathFilter pathFilter;

  /** The key to compare. */
  private final Atomic key;

  /** Denotes the search mode. */
  private final SearchMode mode;

  /**
   * Constructor. Initializes the internal state.
   *
   * @param paths paths to match
   * @param key the atomic key to filter
   * @param mode the search mode to apply
   * @param pcrCollector the path class record collector
   */
  public CASFilter(final Set<Path<QNm>> paths, final Atomic key, final SearchMode mode,
      final PCRCollector pcrCollector) {
    this.paths = requireNonNull(paths);
    pathFilter = new PathFilter(this.paths, pcrCollector);
    this.key = key;
    this.mode = requireNonNull(mode);
  }

  public Set<Long> getPCRs() {
    return pathFilter.getPCRs();
  }

  public PCRCollector getPCRCollector() {
    return pathFilter.getPCRCollector();
  }

  public SearchMode getMode() {
    return mode;
  }

  public Atomic getKey() {
    return key;
  }

  /**
   * Filter the node.
   *
   * @param node node to filter
   * @return {@code true} if the node has been filtered, {@code false} otherwise
   */
  @Override
  public <K extends Comparable<? super K>> boolean filter(final RBNode<K, NodeReferences> node) {
    final K key = node.getKey();
    if (key instanceof CASValue) {
      final CASValue casValue = (CASValue) key;
      return pathFilter.filter(node) && (this.key == null || mode.compare(this.key, casValue.getAtomicValue()) == 0);
    }
    return true;
  }
}
