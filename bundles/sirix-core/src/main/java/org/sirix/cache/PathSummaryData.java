package org.sirix.cache;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import org.brackit.xquery.atomic.QNm;
import org.sirix.index.path.summary.PathNode;
import org.sirix.node.interfaces.StructNode;

import java.util.Map;
import java.util.Set;

public record PathSummaryData(StructNode currentNode, Long2ObjectMap<StructNode> pathNodeMapping, Map<QNm, Set<PathNode>> qnmMapping) {
}
