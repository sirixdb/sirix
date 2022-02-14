package org.sirix.axis.filter;

import org.checkerframework.checker.index.qual.NonNegative;
import org.sirix.index.path.summary.PathSummaryReader;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Path filter for {@link PathSummaryReader}, filtering the path levels.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class PathLevelFilter extends AbstractFilter<PathSummaryReader> {

  /** Node level to filter. */
  private final int mLevel;

  /** {@link PathSummaryReader} instance. */
  private final PathSummaryReader mPathSummary;

  /**
   * Constructor. Initializes the internal state.
   *
   * @param rtx transaction this filter is bound to
   * @param level level of node
   */
  public PathLevelFilter(final PathSummaryReader rtx, final @NonNegative int level) {
    super(rtx);
    checkArgument(level >= 0);
    mPathSummary = rtx;
    mLevel = level;
  }

  @Override
  public boolean filter() {
    return mLevel == mPathSummary.getLevel();
  }

  /**
   * Get filter level.
   *
   * @return level to filter
   */
  int getFilterLevel() {
    return mLevel;
  }
}
