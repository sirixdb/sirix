package io.sirix.query.function.jn.index.drop;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.QNm;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.jdm.type.AnyJsonItemType;
import io.brackit.query.jdm.type.AtomicType;
import io.brackit.query.jdm.type.Cardinality;
import io.brackit.query.jdm.type.SequenceType;
import io.brackit.query.module.StaticContext;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.query.compiler.optimizer.PlanCache;
import io.sirix.query.compiler.optimizer.stats.StatisticsCatalog;
import io.sirix.query.json.JsonDBItem;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * <p>
 * Function for dropping the valid-time (bitemporal) interval index from a stored document. If
 * successful, this function returns the document node. Supported signatures are:
 * </p>
 * <ul>
 * <li><code>jn:drop-valid-time-index($doc as json-item()) as json-item()</code> — drops ALL VALIDTIME
 * indexes on the resource.</li>
 * <li><code>jn:drop-valid-time-index($doc as json-item(), $idx-no as xs:int) as json-item()</code> —
 * drops the VALIDTIME index with the given id.</li>
 * </ul>
 *
 * <p>
 * Removing the index from the catalogue means that, from the new revision onward, the interval index
 * is no longer maintained on writes nor used by {@code jn:valid-at} / {@code jn:open-bitemporal} /
 * {@code jn:scan-valid-time-index} / the optimizer; those fall back to the CAS-narrowing path or the
 * linear scan and still return correct results. The catalogue removal is persisted on the next commit
 * (call {@code sdb:commit($doc)} after this function). The index's pages remain referenced by older
 * revisions, so time-travel queries at those revisions still use the index.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class DropValidTimeIndex extends AbstractFunction {

  /** Valid-time interval index DROP function name. */
  public static final QNm DROP_VALID_TIME_INDEX =
      new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "drop-valid-time-index");

  public DropValidTimeIndex(final QNm name, final Signature signature) {
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

    final JsonIndexController controller = wtx.getResourceSession().getWtxIndexController(wtx.getRevisionNumber());
    if (controller == null) {
      throw new QueryException(new QNm("Document not found."));
    }

    final Integer requestedId = args.length == 2 && args[1] != null ? ((Int32) args[1]).intValue() : null;

    // Collect the VALIDTIME index definitions to drop.
    final Set<IndexDef> toDrop = new LinkedHashSet<>();
    for (final IndexDef indexDef : controller.getIndexes().getIndexDefs()) {
      if (indexDef.getType() != IndexType.VALIDTIME) {
        continue;
      }
      if (requestedId == null || indexDef.getID() == requestedId) {
        toDrop.add(indexDef);
      }
    }

    if (requestedId != null && toDrop.isEmpty()) {
      throw new QueryException(new QNm("No VALIDTIME index with id " + requestedId + " found on the resource."));
    }

    if (!toDrop.isEmpty()) {
      controller.dropIndexes(toDrop, wtx);

      // Invalidate cached query plans so the optimizer stops preferring the now-dropped index.
      PlanCache.signalIndexSchemaChange();

      // Invalidate stale histogram statistics for this resource since the index schema changed.
      try {
        final String dbName = document.getCollection().getDatabase().getName();
        final String resName = resourceSession.getResourceConfig().getName();
        StatisticsCatalog.getInstance().invalidate(dbName, resName);
      } catch (final Exception e) {
        // Histogram invalidation is best-effort; must not prevent the drop.
      }
    }

    return document;
  }
}
