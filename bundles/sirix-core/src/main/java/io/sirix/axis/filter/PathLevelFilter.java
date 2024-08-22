package io.sirix.axis.filter;

import io.sirix.index.path.summary.PathSummaryReader;
import org.checkerframework.checker.index.qual.NonNegative;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Path filter for {@link PathSummaryReader}, filtering the path levels.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public final class PathLevelFilter extends AbstractFilter<PathSummaryReader> {

	/**
	 * Node level to filter.
	 */
	private final int level;

	/**
	 * {@link PathSummaryReader} instance.
	 */
	private final PathSummaryReader pathSummary;

	/**
	 * Constructor. Initializes the internal state.
	 *
	 * @param rtx
	 *            transaction this filter is bound to
	 * @param level
	 *            level of node
	 */
	public PathLevelFilter(final PathSummaryReader rtx, final @NonNegative int level) {
		super(rtx);
		checkArgument(level >= 0);
		pathSummary = rtx;
		this.level = level;
	}

	@Override
	public boolean filter() {
		return level == pathSummary.getLevel();
	}

	/**
	 * Get filter level.
	 *
	 * @return level to filter
	 */
	int getFilterLevel() {
		return level;
	}
}
