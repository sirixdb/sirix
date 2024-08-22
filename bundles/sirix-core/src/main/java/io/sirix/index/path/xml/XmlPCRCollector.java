package io.sirix.index.path.xml;

import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.index.path.AbstractPCRCollector;
import io.sirix.index.path.PCRCollector;
import io.sirix.index.path.PCRValue;
import io.sirix.index.path.summary.PathSummaryReader;

import java.util.Objects;
import java.util.Set;

public final class XmlPCRCollector extends AbstractPCRCollector implements PCRCollector {

	private final NodeReadOnlyTrx mRtx;

	public XmlPCRCollector(final XmlNodeReadOnlyTrx rtx) {
		mRtx = Objects.requireNonNull(rtx, "The transaction must not be null.");
	}

	@Override
	public PCRValue getPCRsForPaths(Set<Path<QNm>> paths) {
		final PathSummaryReader reader = mRtx instanceof XmlNodeTrx
				? ((XmlNodeTrx) mRtx).getPathSummary()
				: mRtx.getResourceSession().openPathSummary(mRtx.getRevisionNumber());
		try {
			return getPcrValue(paths, reader);
		} finally {
			if (!(mRtx instanceof XmlNodeTrx)) {
				reader.close();
			}
		}
	}
}
