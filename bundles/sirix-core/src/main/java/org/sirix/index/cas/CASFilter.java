package org.sirix.index.cas;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;

import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.sirix.api.NodeReadTrx;
import org.sirix.index.Filter;
import org.sirix.index.SearchMode;
import org.sirix.index.avltree.AVLNode;
import org.sirix.index.avltree.keyvalue.CASValue;
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
public final class CASFilter implements Filter {

	/** Logger. */
	private static final LogWrapper LOGGER = new LogWrapper(
			LoggerFactory.getLogger(CASFilter.class));

	/** The paths to filter. */
	private final Set<Path<QNm>> mPaths;

	/** Sirix {@link NodeReadTrx}. */
	private final NodeReadTrx mRtx;

	/** {@link PathFilter} instance to filter specific paths. */
	private final PathFilter mPathFilter;

	private final Atomic mKey;

	private final SearchMode mMode;

	/**
	 * Constructor. Initializes the internal state.
	 * 
	 * @param rtx
	 *          transaction this filter is bound to
	 * @param paths
	 *          paths to match
	 */
	public CASFilter(final NodeReadTrx rtx, final Set<Path<QNm>> paths,
			final Atomic key, final SearchMode mode) {
		mRtx = checkNotNull(rtx);
		mPaths = checkNotNull(paths);
		mPathFilter = new PathFilter(mRtx, mPaths);
		mKey = checkNotNull(key);
		mMode = checkNotNull(mode);
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
		final K key = node.getKey();
		if (key instanceof CASValue) {
			final CASValue casValue = (CASValue) key;
			if (mPathFilter.filter(node)
					&& mMode.compare(mKey, casValue.getAtomicValue()) == 0) {
				return true;
			}
		}
		return false;
	}
}