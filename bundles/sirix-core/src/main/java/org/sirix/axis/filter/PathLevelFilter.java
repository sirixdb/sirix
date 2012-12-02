package org.sirix.axis.filter;

import static com.google.common.base.Preconditions.checkArgument;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.index.path.PathSummaryReader;

/**
 * Path filter for {@link PathSummaryReader}, filtering the path levels.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class PathLevelFilter extends AbstractFilter {

	/** Node level to filter. */
	private int mLevel;

	/** {@link PathSummaryReader} instance. */
	private final PathSummaryReader mPathSummary;

	/**
	 * Constructor. Initializes the internal state.
	 * 
	 * @param pRtx
	 *          transaction this filter is bound to
	 * @param pLevel
	 *          level of node
	 */
	public PathLevelFilter(final @Nonnull PathSummaryReader pRtx,
			final @Nonnegative int pLevel) {
		super(pRtx);
		checkArgument(pLevel >= 0);
		mPathSummary = pRtx;
		mLevel = pLevel;
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
