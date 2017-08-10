package org.sirix.index.path;

import java.util.Objects;
import java.util.Set;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.util.path.PathException;
import org.sirix.api.XdmNodeReadTrx;
import org.sirix.api.XdmNodeWriteTrx;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

public final class PCRCollectorImpl implements PCRCollector {

	/** Logger. */
	private static final LogWrapper LOGGER =
			new LogWrapper(LoggerFactory.getLogger(PCRCollectorImpl.class));

	private final XdmNodeReadTrx mRtx;

	public PCRCollectorImpl(final XdmNodeReadTrx rtx) {
		mRtx = Objects.requireNonNull(rtx, "The transaction must not be null.");
	}

	@Override
	public PCRValue getPCRsForPaths(Set<Path<QNm>> paths) {
		try (final PathSummaryReader reader =
				mRtx instanceof XdmNodeWriteTrx ? ((XdmNodeWriteTrx) mRtx).getPathSummary()
						: mRtx.getResourceManager().openPathSummary(mRtx.getRevisionNumber())) {
			final long maxPCR = reader.getMaxNodeKey();
			final Set<Long> pathClassRecords = reader.getPCRsForPaths(paths);
			return PCRValue.getInstance(maxPCR, pathClassRecords);
		} catch (final PathException e) {
			LOGGER.error(e.getMessage(), e);
		}

		return PCRValue.getEmptyInstance();
	}
}
