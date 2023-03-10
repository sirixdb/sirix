package org.sirix.index.path;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.sirix.index.path.summary.PathSummaryReader;

import java.util.Set;

public abstract class AbstractPCRCollector implements PCRCollector {
  public PCRValue getPcrValue(Set<Path<QNm>> paths, PathSummaryReader reader) {
    final long maxPCR = reader.getMaxNodeKey();
    final Set<Long> pathClassRecords = reader.getPCRsForPaths(paths);
    return PCRValue.getInstance(maxPCR, pathClassRecords);
  }
}
