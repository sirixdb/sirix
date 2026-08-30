package io.sirix.index.cas;

import io.sirix.index.SearchMode;
import io.sirix.index.path.PCRCollector;
import io.sirix.index.path.PathFilter;
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
public final class CASFilter {

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
    this.pathFilter = new PathFilter(requireNonNull(paths), pcrCollector);
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

}
