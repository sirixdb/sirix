package org.sirix.index.cas;

import static java.util.Objects.requireNonNull;

import java.util.Set;

import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.sirix.index.Filter;
import org.sirix.index.SearchMode;
import org.sirix.index.avltree.AVLNode;
import org.sirix.index.avltree.keyvalue.CASValue;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.index.path.PCRCollector;
import org.sirix.index.path.PathFilter;

/**
 * CASFilter filter.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class CASFilter implements Filter {

	/** The paths to filter. */
	private final Set<Path<QNm>> mPaths;

	/** {@link PathFilter} instance to filter specific paths. */
	private final PathFilter mPathFilter;

	/** The key to compare. */
	private final Atomic mKey;

	/** Denotes the search mode. */
	private final SearchMode mMode;

	/**
	 * Constructor. Initializes the internal state.
	 *
	 * @param rtx transaction this filter is bound to
	 * @param paths paths to match
	 * @param key the atomic key to filter
	 * @param mode the search mode to apply
	 * @param pcrCollector the path class record collector
	 */
	public CASFilter(final Set<Path<QNm>> paths, final Atomic key, final SearchMode mode,
			final PCRCollector pcrCollector) {
		mPaths = requireNonNull(paths);
		mPathFilter = new PathFilter(mPaths, pcrCollector);
		mKey = requireNonNull(key);
		mMode = requireNonNull(mode);
	}

	public Set<Long> getPCRs() {
		return mPathFilter.getPCRs();
	}

	public PCRCollector getPCRCollector() {
		return mPathFilter.getPCRCollector();
	}

	public SearchMode getMode() {
		return mMode;
	}

	public Atomic getKey() {
		return mKey;
	}

	/**
	 * Filter the node.
	 *
	 * @param node node to filter
	 * @return {@code true} if the node has been filtered, {@code false} otherwise
	 */
	@Override
	public <K extends Comparable<? super K>> boolean filter(final AVLNode<K, NodeReferences> node) {
		final K key = node.getKey();
		if (key instanceof CASValue) {
			final CASValue casValue = (CASValue) key;
			if (mPathFilter.filter(node) && mMode.compare(mKey, casValue.getAtomicValue()) == 0) {
				return true;
			}
		}
		return false;
	}
}
