package io.sirix.cache;

import io.sirix.index.path.summary.PathNode;
import io.sirix.node.interfaces.StructNode;
import io.sirix.utils.IntToObjectMap;
import org.brackit.xquery.atomic.QNm;

import java.util.Map;
import java.util.Set;

public record PathSummaryData(StructNode currentNode, IntToObjectMap<StructNode> pathNodeMapping, Map<QNm, Set<PathNode>> qnmMapping) {
}
