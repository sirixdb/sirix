package io.sirix.index.path;

import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;
import io.sirix.index.path.summary.PathSummaryReader;

import java.util.Collections;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Path filter for {@link PathSummaryReader}, filtering specific path types.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class PathFilter {

  /** Set of PCRs to filter. */
  private final Set<Long> pcrFilter;

  /** Path class record collector. */
  private final PCRCollector pcrCollector;

  private final boolean hasPathConstraint;

  /**
   * Constructor. Initializes the internal state.
   *
   * @param paths paths to match
   * @param pcrCollector path class record collector
   */
  public PathFilter(final Set<Path<QNm>> paths, final PCRCollector pcrCollector) {
    this.pcrCollector = requireNonNull(pcrCollector, "The path class record collector must not be null.");
    requireNonNull(paths, "The paths must not be null.");
    hasPathConstraint = !paths.isEmpty();
    pcrFilter = Collections.unmodifiableSet(this.pcrCollector.getPCRsForPaths(paths).getPCRs());
  }

  /** An empty resolved PCR set matches nothing when path expressions were supplied. */
  public boolean hasPathConstraint() {
    return hasPathConstraint;
  }

  public Set<Long> getPCRs() {
    return pcrFilter;
  }

  public PCRCollector getPCRCollector() {
    return pcrCollector;
  }

}
