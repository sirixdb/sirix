package org.sirix.index.path;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;

import java.util.Set;

public interface PCRCollector {
  PCRValue getPCRsForPaths(Set<Path<QNm>> paths);
}
