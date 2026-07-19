package io.sirix.query.json;

import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.ValidTimeConfig;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.query.compiler.optimizer.PlanCache;
import io.sirix.query.compiler.optimizer.stats.StatisticsCatalog;
import org.jspecify.annotations.Nullable;

import java.util.LinkedHashSet;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Shared recipe for creating the valid-time (bitemporal) interval index — the persistent
 * Relational-Interval-Tree over each record's {@code [validFrom, validTo]} interval. Used both by
 * the explicit {@code jn:create-valid-time-index} function and by the store layer's auto-creation
 * when a resource is created with a {@link ValidTimeConfig} (e.g. via the {@code jn:store}/
 * {@code jn:load} options {@code validFromPath}/{@code validToPath} or
 * {@code useConventionalValidTime}), so the two creation paths cannot drift.
 *
 * @author Johannes Lichtenberger
 */
public final class ValidTimeIndexes {

  private ValidTimeIndexes() {
    throw new AssertionError("May not be instantiated!");
  }

  /**
   * Derive the default indexed paths {@code /[]/<validFrom>} and {@code /[]/<validTo>} from the
   * resource's valid-time field configuration. Dotted (nested) configured paths such as
   * {@code $.meta.validFrom} are split into individual steps so the derived path always parses.
   * The builder/listener match valid-time fields by NAME from the resource's
   * {@link ValidTimeConfig}, so the exact path only serves index identification/serialization.
   *
   * @param validTimeConfig the resource's valid-time configuration
   * @return the two default paths, in validFrom/validTo order
   */
  public static Set<Path<QNm>> defaultPaths(final ValidTimeConfig validTimeConfig) {
    requireNonNull(validTimeConfig, "validTimeConfig must not be null");
    final Set<Path<QNm>> paths = new LinkedHashSet<>(4);
    paths.add(toArrayFieldPath(validTimeConfig.getNormalizedValidFromPath()));
    paths.add(toArrayFieldPath(validTimeConfig.getNormalizedValidToPath()));
    return paths;
  }

  private static Path<QNm> toArrayFieldPath(final String fieldPath) {
    return Path.parse("/[]/" + ValidTimeConfig.toSlashSeparatedPath(fieldPath), PathParser.Type.JSON);
  }

  /**
   * Create a valid-time interval index over the given paths within the given write transaction:
   * registers the index definition, builds the index from the transaction's current data and
   * attaches a change listener for subsequent modifications. Also invalidates cached query plans
   * and (best-effort) stale histogram statistics. Does NOT commit — the caller owns the
   * transaction lifecycle.
   *
   * @param controller the write-transaction index controller
   * @param wtx the open write transaction
   * @param paths the indexed paths (see {@link #defaultPaths(ValidTimeConfig)})
   * @param databaseName the database name for statistics invalidation, may be {@code null}
   * @param resourceName the resource name for statistics invalidation, may be {@code null}
   * @return the created index definition
   */
  public static IndexDef createIntervalIndex(final JsonIndexController controller, final JsonNodeTrx wtx,
      final Set<Path<QNm>> paths, final @Nullable String databaseName, final @Nullable String resourceName) {
    requireNonNull(controller, "controller must not be null");
    requireNonNull(wtx, "wtx must not be null");
    requireNonNull(paths, "paths must not be null");

    final IndexDef validTimeIdxDef = IndexDefs.createValidTimeIdxDef(paths,
        controller.getIndexes().getNrOfIndexDefsWithType(IndexType.VALIDTIME), IndexDef.DbType.JSON);
    controller.createIndexes(Set.of(validTimeIdxDef), wtx);

    // Invalidate cached query plans so the optimizer considers the new index.
    PlanCache.signalIndexSchemaChange();

    // Invalidate stale histogram statistics since the index schema changed. Best-effort; must not
    // prevent index creation.
    if (databaseName != null && resourceName != null) {
      try {
        StatisticsCatalog.getInstance().invalidate(databaseName, resourceName);
      } catch (final Exception e) {
        // Histogram invalidation is best-effort.
      }
    }

    return validTimeIdxDef;
  }

  /**
   * Create the valid-time interval index within the given write transaction when the resource is
   * configured with valid-time paths and no interval index exists yet (idempotent). Does not
   * commit — the caller owns the transaction lifecycle, so data shred and index creation can land
   * in one revision.
   *
   * @param resourceSession the resource session the write transaction belongs to
   * @param wtx the open write transaction (data may already be inserted but not yet committed)
   * @param databaseName the database (collection) name for statistics invalidation, may be
   *        {@code null}
   */
  public static void createValidTimeIntervalIndexIfConfigured(final JsonResourceSession resourceSession,
      final JsonNodeTrx wtx, final @Nullable String databaseName) {
    requireNonNull(resourceSession, "resourceSession must not be null");
    requireNonNull(wtx, "wtx must not be null");

    final ValidTimeConfig validTimeConfig = resourceSession.getResourceConfig().getValidTimeConfig();
    if (validTimeConfig == null) {
      return;
    }

    final JsonIndexController controller = resourceSession.getWtxIndexController(wtx.getRevisionNumber());
    if (controller.getIndexes().getNrOfIndexDefsWithType(IndexType.VALIDTIME) > 0) {
      return;
    }

    createIntervalIndex(controller, wtx, defaultPaths(validTimeConfig), databaseName,
        resourceSession.getResourceConfig().getName());
  }
}
