package org.sirix.index.path;

import java.util.Set;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;

public interface PCRCollector {
  PCRValue getPCRsForPaths(Set<Path<QNm>> paths);
}
