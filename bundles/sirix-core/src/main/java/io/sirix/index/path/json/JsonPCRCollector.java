package io.sirix.index.path.json;

import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.index.path.AbstractPCRCollector;
import io.sirix.index.path.PCRValue;
import io.sirix.index.path.summary.PathSummaryReader;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class JsonPCRCollector extends AbstractPCRCollector {

  /**
   * Largest number of cached path-class-record sets before the cache is dropped wholesale.
   *
   * <p>Entries are keyed per revision, so a long-lived writer would otherwise accumulate one set
   * per committed revision forever. Clearing outright rather than evicting a single entry keeps
   * this to one size check on a path that has to stay cheap; a miss costs one path-summary open.
   */
  private static final int MAX_CACHED = 1024;

  /**
   * Path-class records per (resource, revision, paths).
   *
   * <p>A committed revision's path summary is immutable, so its PCRs for a given set of paths
   * cannot change. That matters because resolving them OPENS AND CLOSES a whole
   * {@link PathSummaryReader}, and a CAS-index point lookup did it twice per query — once building
   * the {@code CASFilter} and once inside the index open. Measured on a 290,184-record store those
   * two walks were ~2 ms of a ~3 ms lookup whose actual trie descent is 12.8 us.
   */
  private static final Map<CacheKey, PCRValue> CACHE = new ConcurrentHashMap<>();

  /**
   * Cache identity: PCRs depend on the resource, the revision, and the paths asked for.
   *
   * <p>The resource is identified by its creation UUID, not its path. Deleting a resource and
   * recreating it at the SAME path restarts revision numbering, so a path-keyed entry from the
   * previous incarnation would be served for a completely different path summary — which is
   * exactly what the test suites do between cases.
   */
  private record CacheKey(Object resource, int revision, Set<Path<QNm>> paths) {
  }

  private final JsonNodeReadOnlyTrx rtx;

  public JsonPCRCollector(final JsonNodeReadOnlyTrx rtx) {
    this.rtx = Objects.requireNonNull(rtx, "The transaction must not be null.");
  }

  @Override
  public PCRValue getPCRsForPaths(final Set<Path<QNm>> paths) {
    // A write transaction's path summary is still being mutated, so nothing about it is cacheable.
    // It is also already held open by the transaction, so there is no reader to construct.
    if (rtx instanceof JsonNodeTrx wtx) {
      return getPcrValue(paths, wtx.getPathSummary());
    }

    final var config = rtx.getResourceSession().getResourceConfig();
    // Resources predating the UUID field have none; fall back to the path, which is still correct
    // for any resource that is not deleted and recreated underneath a live cache.
    final Object identity = config.resourceUuid != null ? config.resourceUuid : config.getResource().toString();
    final CacheKey key = new CacheKey(identity, rtx.getRevisionNumber(), paths);

    final PCRValue cached = CACHE.get(key);
    if (cached != null) {
      return cached;
    }

    final PathSummaryReader reader = rtx.getResourceSession().openPathSummary(rtx.getRevisionNumber());
    final PCRValue value;
    try {
      value = getPcrValue(paths, reader);
    } finally {
      reader.close();
    }

    if (CACHE.size() >= MAX_CACHED) {
      CACHE.clear();
    }
    CACHE.put(key, value);
    return value;
  }

  /**
   * Drop every cached path-class-record set.
   *
   * <p>Needed only when a resource is rebuilt in place under the same name so that revision
   * numbers restart — which tests do and production does not.
   */
  public static void invalidateCache() {
    CACHE.clear();
  }
}
