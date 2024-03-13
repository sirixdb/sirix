package io.sirix.cache;

import io.brackit.query.atomic.QNm;
import io.sirix.index.path.summary.PathNode;
import io.sirix.node.interfaces.StructNode;

import java.util.Map;
import java.util.Set;

public record PathSummaryData(StructNode currentNode, StructNode[] pathNodeMapping, Map<QNm, Set<PathNode>> qnmMapping) {
}
