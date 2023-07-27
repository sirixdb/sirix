package io.sirix.index.path;

import java.util.Set;
import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;

public interface PCRCollector {
  PCRValue getPCRsForPaths(Set<Path<QNm>> paths);
}
