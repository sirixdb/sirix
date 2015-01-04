package org.sirix.index.path;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.util.path.PathException;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.index.Filter;
import org.sirix.index.avltree.AVLNode;
import org.sirix.index.avltree.keyvalue.CASValue;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

/**
 * Path filter for {@link PathSummaryReader}, filtering specific path types.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class PathFilter implements Filter {

	/** Logger. */
	private static final LogWrapper LOGGER = new LogWrapper(
			LoggerFactory.getLogger(PathFilter.class));

	/** Type to filter. */
	private final boolean mGenericPath;

	/** The paths to filter. */
	private final Set<Path<QNm>> mPaths;

	/** Maximum known path class record (PCR). */
	private long mMaxKnownPCR;

	/** Set of PCRs to filter. */
	private Set<Long> mPCRFilter;

	/** Sirix {@link NodeReadTrx}. */
	private final NodeReadTrx mRtx;

	/**
	 * Constructor. Initializes the internal state.
	 * 
	 * @param rtx
	 *          transaction this filter is bound to
	 * @param paths
	 *          paths to match
	 */
	public PathFilter(final NodeReadTrx rtx, final Set<Path<QNm>> paths) {
		mRtx = checkNotNull(rtx);
		mPaths = checkNotNull(paths);
		mGenericPath = mPaths.isEmpty();
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
		if (mGenericPath) {
			return true;
		}

		final K key = node.getKey();

		long pcr = 0;
		if (key instanceof Long)
			pcr = (Long) key;
		else if (key instanceof CASValue)
			pcr = ((CASValue) key).getPathNodeKey();
		else
			throw new IllegalStateException();

		if (pcr > mMaxKnownPCR) {
			try (final PathSummaryReader reader = mRtx instanceof NodeWriteTrx ? ((NodeWriteTrx) mRtx)
					.getPathSummary() : mRtx.getSession().openPathSummary(
					mRtx.getRevisionNumber())) {
				mMaxKnownPCR = reader.getMaxNodeKey();
				mPCRFilter = reader.getPCRsForPaths(mPaths);
			} catch (final PathException e) {
				LOGGER.error(e.getMessage(), e);
			}
		}

		return mPCRFilter.contains(pcr);
	}
}
