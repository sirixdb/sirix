package io.sirix.query.function.jn.index.drop;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.QNm;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.projection.ProjectionIndexMetadata;
import io.sirix.query.json.JsonDBItem;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * <p>
 * Function for dropping projection indexes from a stored document — the
 * projection sibling of {@code jn:drop-valid-time-index}. If successful,
 * this function returns the document node. Supported signatures:
 * </p>
 * <ul>
 * <li><code>jn:drop-projection-index($doc as json-item()) as json-item()</code>
 * — drops ALL projection indexes on the resource.</li>
 * <li><code>jn:drop-projection-index($doc as json-item(), $idx-no as xs:int)
 * as json-item()</code> — drops the projection index with the given id
 * (see {@code jn:find-projection-index}).</li>
 * </ul>
 *
 * <p>The definition is removed from the catalogue and each dropped
 * projection's persisted metadata slot is overwritten with a stale
 * tombstone. The tombstone is REQUIRED for correctness, not hygiene: after
 * the drop no change listener maintains the sub-tree, so a later
 * re-creation reusing the id could otherwise mistake the leftover columns
 * for fresh ones even though records changed in between. Both writes ride
 * the session's write transaction — call {@code sdb:commit($doc)} to
 * persist. Revisions committed BEFORE the drop keep their catalogue entry
 * and payloads, so time-travel queries at those revisions continue to be
 * served by the projection.
 *
 * @author Johannes Lichtenberger
 */
public final class DropProjectionIndex extends AbstractFunction {

  /** Projection index DROP function name. */
  public static final QNm DROP_PROJECTION_INDEX =
      new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "drop-projection-index");

  public DropProjectionIndex(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    if (args.length != 1 && args.length != 2) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final JsonDBItem document = (JsonDBItem) args[0];
    final JsonNodeReadOnlyTrx rtx = document.getTrx();
    final JsonResourceSession resourceSession = rtx.getResourceSession();

    final var optionalWriteTrx = resourceSession.getNodeTrx();
    final JsonNodeTrx wtx = optionalWriteTrx.orElseGet(resourceSession::beginNodeTrx);

    if (rtx.getRevisionNumber() < resourceSession.getMostRecentRevisionNumber()) {
      wtx.revertTo(rtx.getRevisionNumber());
    }

    final JsonIndexController controller =
        wtx.getResourceSession().getWtxIndexController(wtx.getRevisionNumber());

    final Integer requestedId = args.length == 2 && args[1] != null ? ((Int32) args[1]).intValue() : null;

    final Set<IndexDef> toDrop = new LinkedHashSet<>();
    for (final IndexDef indexDef : controller.getIndexes().getIndexDefs()) {
      if (indexDef.getType() != IndexType.PROJECTION) {
        continue;
      }
      if (requestedId == null || indexDef.getID() == requestedId) {
        toDrop.add(indexDef);
      }
    }

    if (requestedId != null && toDrop.isEmpty()) {
      throw new QueryException(new QNm(
          "No PROJECTION index with id " + requestedId + " found on the resource."));
    }

    if (!toDrop.isEmpty()) {
      controller.dropIndexes(toDrop, wtx);
      // Tombstone each dropped sub-tree's metadata slot: with the listener
      // gone, subsequent record changes are untracked, so an id-reusing
      // re-creation must never accept the leftover columns as fresh.
      for (final IndexDef indexDef : toDrop) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), indexDef.getID());
        // The dropped sub-tree's row-group slots survive the tombstone, so the marker must carry
        // the store's physical layout — an id-reusing re-creation has to write them back under the
        // SAME layout (see ProjectionIndexMetadata#staleTombstone(boolean)).
        ProjectionIndexMetadata priorMeta;
        try {
          priorMeta = ProjectionIndexMetadata.parse(storage.getBlob(0));
        } catch (final RuntimeException corrupt) {
          priorMeta = null; // unreadable slot 0 — recover the layout structurally below
        }
        // An unreadable slot 0 must NOT silently downgrade the marker to the descriptor layout;
        // probe the surviving row-group slot keys instead, which the tombstone leaves in place.
        final boolean columnSegmentSlotLayout = priorMeta != null
            ? priorMeta.isColumnSegmentSlotLayout()
            : storage.probeColumnSegmentSlotLayout();
        storage.putBlob(0, ProjectionIndexMetadata.staleTombstone(columnSegmentSlotLayout).serialize());
      }
      // No PlanCache/statistics invalidation: projections route through the
      // vectorized executor's revision-scoped catalog lookups, not through
      // optimizer plan rewrites — revisions from this commit onward simply
      // no longer catalogue the definition.
    }

    return document;
  }
}
