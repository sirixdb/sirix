package io.sirix.query.compiler.optimizer.stats;

import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;

/**
 * Provides query optimization statistics from SirixDB's storage layer.
 *
 * <p>Adapted from Moraes Filho/Härder's AXS-Stat framework for JSON:
 * per-path cardinality via PathSummary, index availability via IndexController.</p>
 */
public interface StatisticsProvider {

  /**
   * Get cardinality for a JSON path (number of nodes matching the path pattern).
   * Uses PathSummaryReader.getPCRsForPath() + PathNode.getReferences().
   *
   * @return node count, or -1 if unknown
   */
  long getPathCardinality(Path<QNm> path, String databaseName, String resourceName, int revision);

  /**
   * Get total document size (number of nodes).
   * Uses NodeReadOnlyTrx.getDescendantCount() from document root.
   *
   * @return total node count, or -1 if unknown
   */
  long getTotalNodeCount(String databaseName, String resourceName, int revision);

  /**
   * Get index info for a path — checks CAS, PATH, NAME indexes.
   *
   * @return IndexInfo with indexId, type, and availability
   */
  IndexInfo getIndexInfo(Path<QNm> path, String databaseName, String resourceName, int revision);

  /**
   * Build a BaseProfile for an access operator at the given path.
   * Populates nodeCount, pathLevel, and index metadata.
   */
  BaseProfile buildBaseProfile(Path<QNm> path, String databaseName, String resourceName, int revision);
}
