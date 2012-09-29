package org.sirix.axis.filter;

import static com.google.common.base.Preconditions.checkArgument;

import javax.annotation.Nonnull;

import org.sirix.api.INodeReadTrx;
import org.sirix.index.path.PathNode;
import org.sirix.index.path.PathSummary;
import org.sirix.node.EKind;

/**
 * Path filter for {@link PathSummary}, filtering specific path types.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public class PathKindFilter extends AbsFilter {

  /** Type to filter. */
  private EKind mType;

  /**
   * Constructor. Initializes the internal state.
   * 
   * @param pRtx
   *          transaction this filter is bound to
   * @param pType
   *          type to match
   */
  public PathKindFilter(final @Nonnull INodeReadTrx pRtx, final @Nonnull EKind pType) {
    super(pRtx);
    checkArgument(pRtx instanceof PathSummary);
    mType = pType;
  }

  @Override
  public boolean filter() {
    return mType == getTransaction().getPathKind();
  }
}
