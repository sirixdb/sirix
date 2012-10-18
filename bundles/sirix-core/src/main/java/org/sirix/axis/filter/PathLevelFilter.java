package org.sirix.axis.filter;

import static com.google.common.base.Preconditions.checkArgument;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.index.path.PathSummary;

/**
 * Path filter for {@link PathSummary}, filtering the path levels.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class PathLevelFilter extends AbstractFilter {

	/** Node level to filter. */
	private int mLevel;

	/** {@link PathSummary} instance. */
	private final PathSummary mPathSummary;

	/**
	 * Constructor. Initializes the internal state.
	 * 
	 * @param pRtx
	 *          transaction this filter is bound to
	 * @param pLevel
	 *          level of node
	 */
	public PathLevelFilter(final @Nonnull PathSummary pRtx,
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
