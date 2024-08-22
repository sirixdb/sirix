package io.sirix.diff.algorithm.fmse;

import java.util.Map;

import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.brackit.query.atomic.QNm;
import io.sirix.index.path.summary.PathSummaryReader;

public interface NodeComparisonFactory {
	NodeComparator<Long> createLeafNodeEqualityChecker(QNm id, XmlNodeReadOnlyTrx oldRtx, XmlNodeReadOnlyTrx newRtx,
			PathSummaryReader oldPathSummary, PathSummaryReader newPathSummary,
			FMSENodeComparisonUtils nodeComparisonUtils);

	NodeComparator<Long> createInnerNodeEqualityChecker(QNm idName, Matching matching, XmlNodeReadOnlyTrx oldRtx,
			XmlNodeReadOnlyTrx newRtx, FMSENodeComparisonUtils nodeComparisonUtils, Map<Long, Long> descendantsOldRev,
			Map<Long, Long> descendantsNewRev);
}
