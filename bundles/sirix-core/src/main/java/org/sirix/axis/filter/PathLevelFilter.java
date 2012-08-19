package org.sirix.axis.filter;

import static com.google.common.base.Preconditions.checkArgument;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.INodeReadTrx;
import org.sirix.index.path.PathNode;
import org.sirix.index.path.PathSummary;

/**
 * Path filter for {@link PathSummary}, filtering the path levels.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class PathLevelFilter extends AbsFilter {

  /** Node level to filter. */
  private int mLevel;

  /**
   * Constructor. Initializes the internal state.
   * 
   * @param pRtx
   *          transaction this filter is bound to
   * @param pLevel
   *          level of node
   */
  public PathLevelFilter(final @Nonnull INodeReadTrx pRtx,
    final @Nonnegative int pLevel) {
    super(pRtx);
    checkArgument(pRtx instanceof PathSummary);
    checkArgument(pLevel >= 0);
    mLevel = pLevel;
  }

  @Override
  public boolean filter() {
    return mLevel == ((PathNode)getTransaction().getNode()).getLevel();
  }
}
