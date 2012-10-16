package org.sirix.axis.filter;

import static com.google.common.base.Preconditions.checkArgument;

import javax.annotation.Nonnull;

import org.sirix.api.NodeReadTrx;
import org.sirix.index.path.PathNode;
import org.sirix.index.path.PathSummary;
import org.sirix.node.Kind;

/**
 * Path filter for {@link PathSummary}, filtering specific path types.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public class PathKindFilter extends AbsFilter {

  /** Type to filter. */
  private Kind mType;

  /**
   * Constructor. Initializes the internal state.
   * 
   * @param pRtx
   *          transaction this filter is bound to
   * @param pType
   *          type to match
   */
  public PathKindFilter(final @Nonnull NodeReadTrx pRtx, final @Nonnull Kind pType) {
    super(pRtx);
    checkArgument(pRtx instanceof PathSummary);
    mType = pType;
  }

  @Override
  public boolean filter() {
    return mType == getTrx().getPathKind();
  }
}
