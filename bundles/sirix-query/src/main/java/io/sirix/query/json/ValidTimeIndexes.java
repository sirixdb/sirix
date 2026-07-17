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

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Helper for auto-creating the valid-time (bitemporal) interval index when a resource is created
 * through the JSONiq store layer with a {@link ValidTimeConfig} (e.g. via the
 * {@code jn:store}/{@code jn:load} options {@code validFromPath}/{@code validToPath} or
 * {@code useConventionalValidTime}).
 *
 * <p>
 * The created index is the same persistent Relational-Interval-Tree the explicit
 * {@code jn:create-valid-time-index} function builds: existing data in the write transaction is
 * indexed by a builder pass and a change listener maintains the index for subsequent modifications.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
final class ValidTimeIndexes {

  private ValidTimeIndexes() {
    throw new AssertionError("May not be instantiated!");
  }

  /**
   * Create the valid-time interval index within the given write transaction when the resource is
   * configured with valid-time paths and no interval index exists yet. Does not commit — the caller
   * owns the transaction lifecycle, so data shred and index creation land in one revision.
   *
   * @param resourceSession the resource session the write transaction belongs to
   * @param wtx the open write transaction (data may already be inserted but not yet committed)
   */
  static void createValidTimeIntervalIndexIfConfigured(final JsonResourceSession resourceSession,
      final JsonNodeTrx wtx) {
    final ValidTimeConfig validTimeConfig = resourceSession.getResourceConfig().getValidTimeConfig();
    if (validTimeConfig == null) {
      return;
    }

    final JsonIndexController controller = resourceSession.getWtxIndexController(wtx.getRevisionNumber());
    if (controller.getIndexes().getNrOfIndexDefsWithType(IndexType.VALIDTIME) > 0) {
      return;
    }

    // Same default paths as jn:create-valid-time-index: /[]/<validFrom> and /[]/<validTo>. The
    // builder/listener match valid-time fields by NAME from the resource's ValidTimeConfig, so the
    // paths only serve index identification/serialization.
    final Set<Path<QNm>> paths = new LinkedHashSet<>(4);
    paths.add(Path.parse("/[]/" + validTimeConfig.getNormalizedValidFromPath(), PathParser.Type.JSON));
    paths.add(Path.parse("/[]/" + validTimeConfig.getNormalizedValidToPath(), PathParser.Type.JSON));

    final IndexDef validTimeIdxDef = IndexDefs.createValidTimeIdxDef(paths, 0, IndexDef.DbType.JSON);
    controller.createIndexes(Set.of(validTimeIdxDef), wtx);

    // Invalidate cached query plans so the optimizer considers the new index.
    PlanCache.signalIndexSchemaChange();
  }
}
