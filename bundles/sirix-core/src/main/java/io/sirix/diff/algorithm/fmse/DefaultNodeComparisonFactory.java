package io.sirix.diff.algorithm.fmse;

import java.util.Map;

import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.brackit.query.atomic.QNm;
import io.sirix.index.path.summary.PathSummaryReader;

public final class DefaultNodeComparisonFactory implements NodeComparisonFactory {

	@Override
	public NodeComparator<Long> createLeafNodeEqualityChecker(QNm id, XmlNodeReadOnlyTrx oldRtx,
			XmlNodeReadOnlyTrx newRtx, PathSummaryReader oldPathSummary, PathSummaryReader newPathSummary,
			FMSENodeComparisonUtils nodeComparisonUtils) {
		return new LeafNodeComparator(id, oldRtx, newRtx, oldPathSummary, newPathSummary, nodeComparisonUtils);
	}

	@Override
	public NodeComparator<Long> createInnerNodeEqualityChecker(QNm idName, Matching matching, XmlNodeReadOnlyTrx oldRtx,
			XmlNodeReadOnlyTrx newRtx, FMSENodeComparisonUtils nodeComparisonUtils, Map<Long, Long> descendantsOldRev,
			Map<Long, Long> descendantsNewRev) {
		return new InnerNodeComparator(idName, matching, oldRtx, newRtx, nodeComparisonUtils, descendantsOldRev,
				descendantsNewRev);
	}

}
