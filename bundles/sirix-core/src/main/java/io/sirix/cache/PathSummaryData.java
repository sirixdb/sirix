package io.sirix.cache;

import io.brackit.query.atomic.QNm;
import io.sirix.index.path.summary.PathNode;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.StructNode;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

import java.util.Map;
import java.util.Set;

public record PathSummaryData(
    StructNode currentNode, 
    StructNode[] pathNodeMapping, 
    Map<QNm, Set<PathNode>> qnmMapping,
    Long2LongOpenHashMap childLookupCache) {
  
  /**
   * Compute a hash key for parent-child lookups.
   * Combines parentNodeKey, name hash, and kind ordinal into a single long.
   *
   * @param parentNodeKey the parent path node key
   * @param childName     the child name
   * @param childKind     the child node kind
   * @return a hash key for the lookup cache
   */
  public static long computeChildLookupKey(long parentNodeKey, QNm childName, NodeKind childKind) {
    // Combine: parentNodeKey (upper 32 bits) + nameHash (24 bits) + kindOrdinal (8 bits)
    // This allows for up to 4B parent keys, 16M unique name hashes, 256 node kinds
    int nameHash = childName.hashCode() & 0x00FFFFFF; // 24 bits
    int kindOrdinal = childKind.ordinal() & 0xFF;      // 8 bits
    return (parentNodeKey << 32) | ((long) nameHash << 8) | kindOrdinal;
  }
}
