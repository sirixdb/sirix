package org.sirix.axis.filter;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;

import javax.annotation.Nonnull;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.util.path.PathException;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

/**
 * Path filter for {@link PathSummaryReader}, filtering specific path types.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class PathFilter extends AbstractFilter {
	
	/** Logger. */
	private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(PathFilter.class));

	/** Type to filter. */
	private final boolean mGenericPath;
	
	/** The paths to filter. */
	private final Set<Path<QNm>> mPaths;
	
	/** Maximum known path class record (PCR). */
	private long mMaxKnownPCR;
	
	/** Set of PCRs to filter. */
	private Set<Long> mPCRFilter;

	/**
	 * Constructor. Initializes the internal state.
	 * 
	 * @param rtx
	 *          transaction this filter is bound to
	 * @param paths
	 *          paths to match
	 */
	public PathFilter(final @Nonnull NodeReadTrx rtx,
			final @Nonnull Set<Path<QNm>> paths) {
		super(rtx);
		mPaths = checkNotNull(paths);
		mGenericPath = mPaths.isEmpty();
	}

	@Override
	public boolean filter() {
		if (mGenericPath) {
			return true;
		}

		final NodeReadTrx trx = getTrx();
		if (trx.isNameNode()) {
			final long pcr = trx.getNameNode().getPathNodeKey();
			if (pcr > mMaxKnownPCR) {
				try (final PathSummaryReader reader = trx instanceof NodeWriteTrx ? ((NodeWriteTrx) trx)
						.getPathSummary() : trx.getSession().openPathSummary(
						trx.getRevisionNumber())) {
					mMaxKnownPCR = reader.getMaxNodeKey();
					mPCRFilter = reader.getPCRsForPaths(mPaths);
				} catch (final PathException | SirixException e) {
					LOGGER.error(e.getMessage(), e);
				}
			}

			return (mPCRFilter.contains(pcr));
		}
		return false;
	}
}
