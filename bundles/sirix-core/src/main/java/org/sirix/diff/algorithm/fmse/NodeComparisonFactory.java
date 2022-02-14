package org.sirix.diff.algorithm.fmse;

import org.brackit.xquery.atomic.QNm;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.index.path.summary.PathSummaryReader;

import java.util.Map;

public interface NodeComparisonFactory {
  NodeComparator<Long> createLeafNodeEqualityChecker(QNm id, XmlNodeReadOnlyTrx oldRtx, XmlNodeReadOnlyTrx newRtx,
      PathSummaryReader oldPathSummary, PathSummaryReader newPathSummary, FMSENodeComparisonUtils nodeComparisonUtils);

  NodeComparator<Long> createInnerNodeEqualityChecker(QNm idName, Matching matching, XmlNodeReadOnlyTrx oldRtx,
      XmlNodeReadOnlyTrx newRtx, FMSENodeComparisonUtils nodeComparisonUtils, Map<Long, Long> descendantsOldRev,
      Map<Long, Long> descendantsNewRev);
}
