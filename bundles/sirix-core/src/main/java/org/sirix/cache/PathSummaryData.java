package org.sirix.cache;

import one.jasyncfio.collections.IntObjectHashMap;
import org.brackit.xquery.atomic.QNm;
import org.sirix.index.path.summary.PathNode;
import org.sirix.node.interfaces.StructNode;

import java.util.Map;
import java.util.Set;

public record PathSummaryData(StructNode currentNode, IntObjectHashMap<StructNode> pathNodeMapping, Map<QNm, Set<PathNode>> qnmMapping) {
}
