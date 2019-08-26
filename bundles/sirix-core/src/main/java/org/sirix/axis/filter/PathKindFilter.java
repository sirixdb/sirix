package org.sirix.axis.filter;

import static com.google.common.base.Preconditions.checkArgument;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.NodeKind;

/**
 * Path filter for {@link PathSummaryReader}, filtering specific path types.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class PathKindFilter extends AbstractFilter<PathSummaryReader> {

  /** Type to filter. */
  private NodeKind mType;

  /**
   * Constructor. Initializes the internal state.
   *
   * @param rtx transaction this filter is bound to
   * @param type type to match
   */
  public PathKindFilter(final PathSummaryReader rtx, final NodeKind type) {
    super(rtx);
    checkArgument(rtx instanceof PathSummaryReader);
    mType = type;
  }

  @Override
  public boolean filter() {
    return mType == getTrx().getPathKind();
  }
}
