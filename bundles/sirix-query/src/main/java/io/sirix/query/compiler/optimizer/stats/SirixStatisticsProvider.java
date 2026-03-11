package io.sirix.query.compiler.optimizer.stats;

import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.ResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.path.summary.PathNode;
import io.sirix.query.json.JsonDBStore;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Statistics provider backed by SirixDB's PathSummary and IndexController.
 *
 * <p>Caches path cardinalities and resource sessions to avoid repeated
 * lookups within a single query optimization pass. Call {@link #clearCaches()}
 * between queries to prevent stale statistics.</p>
 */
public final class SirixStatisticsProvider implements StatisticsProvider, AutoCloseable {

  private final JsonDBStore jsonStore;

  private final Map<PathCacheKey, Long> pathCardinalityCache;

  private final Map<String, ResourceSession<JsonNodeReadOnlyTrx, JsonNodeTrx>> sessionCache;

  public SirixStatisticsProvider(JsonDBStore jsonStore) {
    this.jsonStore = Objects.requireNonNull(jsonStore);
    this.pathCardinalityCache = new HashMap<>(64);
    this.sessionCache = new HashMap<>(4);
  }

  private ResourceSession<JsonNodeReadOnlyTrx, JsonNodeTrx> getSession(
      String databaseName, String resourceName) {
    final String key = databaseName + ":" + resourceName;
    ResourceSession<JsonNodeReadOnlyTrx, JsonNodeTrx> session = sessionCache.get(key);
    if (session != null) {
      return session;
    }

    try {
      final var jsonCollection = jsonStore.lookup(databaseName);
      session = jsonCollection.getDatabase().beginResourceSession(resourceName);
      sessionCache.put(key, session);
      return session;
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Clear all caches and close cached sessions. Call between queries
   * to prevent stale statistics and resource leaks.
   */
  public void clearCaches() {
    pathCardinalityCache.clear();
    for (final var session : sessionCache.values()) {
      if (session != null) {
        try {
          session.close();
        } catch (Exception ignored) {
          // best-effort cleanup
        }
      }
    }
    sessionCache.clear();
  }

  @Override
  public void close() {
    clearCaches();
  }

  @Override
  public long getPathCardinality(Path<QNm> path, String databaseName,
                                 String resourceName, int revision) {
    final var key = new PathCacheKey(path, databaseName, resourceName, revision);
    final Long cached = pathCardinalityCache.get(key);
    if (cached != null) {
      return cached;
    }

    try {
      final var resMgr = getSession(databaseName, resourceName);
      if (resMgr == null) {
        return -1L;
      }

      try (final var pathSummary = revision == -1
          ? resMgr.openPathSummary()
          : resMgr.openPathSummary(revision)) {

        final var pcrs = pathSummary.getPCRsForPath(path);
        long totalReferences = 0L;

        final var iter = pcrs.iterator();
        while (iter.hasNext()) {
          final long pcr = iter.nextLong();
          final PathNode pathNode = pathSummary.getPathNodeForPathNodeKey(pcr);
          if (pathNode != null) {
            totalReferences += pathNode.getReferences();
          }
        }

        pathCardinalityCache.put(key, totalReferences);
        return totalReferences;
      }
    } catch (Exception e) {
      return -1L;
    }
  }

  @Override
  public long getTotalNodeCount(String databaseName, String resourceName, int revision) {
    try {
      final var resMgr = getSession(databaseName, resourceName);
      if (resMgr == null) {
        return -1L;
      }

      try (final var rtx = revision == -1
          ? resMgr.beginNodeReadOnlyTrx()
          : resMgr.beginNodeReadOnlyTrx(revision)) {
        rtx.moveToDocumentRoot();
        return rtx.getDescendantCount();
      }
    } catch (Exception e) {
      return -1L;
    }
  }

  @Override
  public IndexInfo getIndexInfo(Path<QNm> path, String databaseName,
                                String resourceName, int revision) {
    try {
      final var resMgr = getSession(databaseName, resourceName);
      if (resMgr == null) {
        return IndexInfo.NO_INDEX;
      }

      final var indexController = revision == -1
          ? resMgr.getRtxIndexController(resMgr.getMostRecentRevisionNumber())
          : resMgr.getRtxIndexController(revision);

      final var indexes = indexController.getIndexes();

      // CAS index first (most useful for value predicates)
      for (final IndexDef indexDef : indexes.getIndexDefs()) {
        if (indexDef.isCasIndex()) {
          for (final Path<QNm> indexedPath : indexDef.getPaths()) {
            if (indexedPath.matches(path)) {
              return new IndexInfo(indexDef.getID(), IndexType.CAS, true);
            }
          }
        }
      }

      // Path index
      for (final IndexDef indexDef : indexes.getIndexDefs()) {
        if (indexDef.isPathIndex()) {
          for (final Path<QNm> indexedPath : indexDef.getPaths()) {
            if (indexedPath.matches(path)) {
              return new IndexInfo(indexDef.getID(), IndexType.PATH, true);
            }
          }
        }
      }

      // Name index — verify it covers the requested path's leaf name
      if (path.getLength() > 0) {
        final QNm leafName = path.tail();
        for (final IndexDef indexDef : indexes.getIndexDefs()) {
          if (indexDef.isNameIndex()
              && indexDef.getIncluded().contains(leafName)) {
            return new IndexInfo(indexDef.getID(), IndexType.NAME, true);
          }
        }
      }

      return IndexInfo.NO_INDEX;
    } catch (Exception e) {
      return IndexInfo.NO_INDEX;
    }
  }

  @Override
  public BaseProfile buildBaseProfile(Path<QNm> path, String databaseName,
                                      String resourceName, int revision) {
    final long nodeCount = getPathCardinality(path, databaseName, resourceName, revision);
    final IndexInfo indexInfo = getIndexInfo(path, databaseName, resourceName, revision);

    int pathLevel = -1;

    try {
      final var resMgr = getSession(databaseName, resourceName);
      if (resMgr != null) {
        try (final var pathSummary = revision == -1
            ? resMgr.openPathSummary()
            : resMgr.openPathSummary(revision)) {

          final var pcrs = pathSummary.getPCRsForPath(path);
          int minLevel = Integer.MAX_VALUE;
          final var iter = pcrs.iterator();
          while (iter.hasNext()) {
            final long pcr = iter.nextLong();
            final PathNode pathNode = pathSummary.getPathNodeForPathNodeKey(pcr);
            if (pathNode != null) {
              minLevel = Math.min(minLevel, pathNode.getLevel());
            }
          }
          if (minLevel != Integer.MAX_VALUE) {
            pathLevel = minLevel;
          }
        }
      }
    } catch (Exception e) {
      // defaults remain
    }

    return new BaseProfile(nodeCount, pathLevel, indexInfo.exists(), indexInfo.type());
  }

  private record PathCacheKey(Path<QNm> path, String databaseName, String resourceName, int revision) {}
}
