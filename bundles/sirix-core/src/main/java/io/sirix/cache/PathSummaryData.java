package io.sirix.cache;

import io.brackit.query.atomic.QNm;
import io.sirix.index.path.summary.PathNode;
import io.sirix.node.interfaces.StructNode;

import java.util.Map;
import java.util.Set;

/**
 * Snapshot of a path summary's in-memory form, shared between a {@code PathSummaryReader} and the
 * cache it was built for.
 *
 * @param currentNode where the reader's cursor stood when the snapshot was taken
 * @param pathNodeMapping path node key to path node, indexed directly by key
 * @param qnmMapping name to the set of path nodes carrying it
 * @param childLookupCache {@code (parent, name, kind)} to child path node key
 */
public record PathSummaryData(StructNode currentNode, StructNode[] pathNodeMapping, Map<QNm, Set<PathNode>> qnmMapping,
    PathSummaryChildIndex childLookupCache) {
}
