package org.sirix.index.cas;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;

import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Type;
import org.sirix.api.NodeReadTrx;
import org.sirix.index.Filter;
import org.sirix.index.avltree.AVLNode;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.index.path.PathFilter;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

/**
 * CASFilter filter.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class CASFilterRange implements Filter {

	/** Logger. */
	private static final LogWrapper LOGGER = new LogWrapper(
			LoggerFactory.getLogger(CASFilterRange.class));

	/** The paths to filter. */
	private final Set<Path<QNm>> mPaths;

	/** Sirix {@link NodeReadTrx}. */
	private final NodeReadTrx mRtx;

	/** {@link PathFilter} instance to filter specific paths. */
	private final PathFilter mPathFilter;

	private final Atomic mMin;

	private final Atomic mMax;

	private final boolean mIncMin;

	private final boolean mIncMax;

	private final Type mType;

	/**
	 * Constructor. Initializes the internal state.
	 * 
	 * @param rtx
	 *          transaction this filter is bound to
	 * @param paths
	 *          paths to match
	 */
	public CASFilterRange(final NodeReadTrx rtx, final Set<Path<QNm>> paths,
			final Atomic min, final Atomic max, final boolean incMin,
			final boolean incMax) {
		mRtx = checkNotNull(rtx);
		mPaths = checkNotNull(paths);
		mPathFilter = new PathFilter(mRtx, mPaths);
		mMin = checkNotNull(min);
		mMax = checkNotNull(max);
		mIncMin = incMin;
		mIncMax = incMax;
		mType = min.type();
	}

	/**
	 * Filter the node.
	 * 
	 * @param node
	 *          node to filter
	 * @return {@code true} if the node has been filtered, {@code false} otherwise
	 */
	@Override
	public <K extends Comparable<? super K>> boolean filter(
			final AVLNode<K, NodeReferences> node) {

		final boolean filtered = mPathFilter.filter(node);
		
		if (filtered) {
			
		}
		return true;
	}
	
	private <K extends Comparable<? super K>> boolean inRange(Atomic key) throws DocumentException {
		final int minKeyCompare = (mMin != null) ? mMin.compareTo(key)
				: -1;
		final int maxKeyCompare = (mMax != null) ? mMax.compareTo(key) : 1;

		final boolean lowerBoundValid = ((minKeyCompare == 0) && (mIncMin))
				|| (minKeyCompare < 0);
		final boolean upperBoundValid = ((maxKeyCompare == 0) && (mIncMax))
				|| (maxKeyCompare > 0);

		return upperBoundValid && lowerBoundValid;
	}
}