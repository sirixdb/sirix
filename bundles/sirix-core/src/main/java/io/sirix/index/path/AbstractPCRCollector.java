package io.sirix.index.path;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import io.sirix.index.path.summary.PathSummaryReader;

import java.util.Set;

public abstract class AbstractPCRCollector implements PCRCollector {
  public PCRValue getPcrValue(Set<Path<QNm>> paths, PathSummaryReader reader) {
    var maxPCR = reader.getMaxNodeKey();
    var pathClassRecords = reader.getPCRsForPaths(paths);
    return PCRValue.getInstance(maxPCR, pathClassRecords);
  }
}
