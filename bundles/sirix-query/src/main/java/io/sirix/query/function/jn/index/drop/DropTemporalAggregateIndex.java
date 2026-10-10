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
import io.sirix.query.json.JsonDBItem;

import java.util.LinkedHashSet;
import java.util.Set;

/** Remove temporal aggregate declarations while preserving their historical revision roots. */
public final class DropTemporalAggregateIndex extends AbstractFunction {

  /** Temporal aggregate DROP function name. */
  public static final QNm DROP_TEMPORAL_AGGREGATE_INDEX =
      new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "drop-temporal-aggregate-index");

  public DropTemporalAggregateIndex(final QNm name, final Signature signature) {
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
    final boolean wtxIsOurs = optionalWriteTrx.isEmpty();
    final JsonNodeTrx wtx = optionalWriteTrx.orElseGet(resourceSession::beginNodeTrx);
    boolean committedToCaller = false;
    try {
      if (rtx.getRevisionNumber() < resourceSession.getMostRecentRevisionNumber()) {
        wtx.revertTo(rtx.getRevisionNumber());
      }

      final JsonIndexController controller = wtx.getResourceSession().getWtxIndexController(wtx.getRevisionNumber());

      final Integer requestedId = args.length == 2 && args[1] != null
          ? ((Int32) args[1]).intValue()
          : null;

      final Set<IndexDef> toDrop = new LinkedHashSet<>();
      for (final IndexDef indexDef : controller.getIndexes().getIndexDefs()) {
        if (indexDef.getType() != IndexType.TEMPORAL_AGGREGATE) {
          continue;
        }
        if (requestedId == null || indexDef.getID() == requestedId) {
          toDrop.add(indexDef);
        }
      }

      if (requestedId != null && toDrop.isEmpty()) {
        throw new QueryException(
            new QNm("No TEMPORAL_AGGREGATE index with id " + requestedId + " found on the resource."));
      }

      dropAll(toDrop, controller, wtx);
      committedToCaller = !toDrop.isEmpty();
    } finally {
      if (wtxIsOurs && !committedToCaller) {
        wtx.close();
      }
    }

    return document;
  }

  private static void dropAll(final Set<IndexDef> toDrop, final JsonIndexController controller, final JsonNodeTrx wtx) {
    if (!toDrop.isEmpty()) {
      wtx.awaitPendingAsyncCommit();
      controller.dropIndexes(toDrop, wtx);
    }
  }
}
