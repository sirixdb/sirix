package io.sirix.query.compiler.optimizer.stats;

/**
 * Physical statistics profile for an access operator at a specific JSON path.
 * Populated from PathSummary and IndexController metadata.
 *
 * <p>Adapted from Weiner et al.'s Base Profile concept — profiles at leaf
 * operators of the query graph that contain physical storage statistics.</p>
 *
 * @param nodeCount number of nodes at this path (from PathNode.getReferences())
 * @param pathLevel depth in the JSON tree (from PathNode.getLevel()), or -1 if unknown
 * @param hasIndex whether an index covers this access
 * @param indexType the type of covering index, or NONE
 */
public record BaseProfile(
    long nodeCount,
    int pathLevel,
    boolean hasIndex,
    IndexType indexType
) {

  /** Sentinel for unknown/empty profile. */
  public static final BaseProfile UNKNOWN = new BaseProfile(-1L, -1, false, IndexType.NONE);
}
