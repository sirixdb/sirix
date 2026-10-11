package io.sirix.query.bench.bitemporal;

import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.aggregate.TemporalAggregateDeclarations;

import java.util.List;

/** SH1's explicit grouped point-index declarations; other resources opt in through the same API. */
public final class BitemporalTemporalAggregates {
  private BitemporalTemporalAggregates() {}

  public static void declare(final JsonResourceSession session, final JsonNodeTrx transaction, final String resource) {
    if (!BitemporalSchema.CONTRACTS.equals(resource)
        || Boolean.getBoolean("sirix.bitemporal.disableTemporalAggregates")) {
      return;
    }
    final JsonIndexController controller = session.getWtxIndexController(transaction.getRevisionNumber());
    TemporalAggregateDeclarations.declare(controller, transaction, BitemporalProjections.ROOT_PATH, List.of("grade"),
        List.of("count(qty)", "sum(qty)", "count(cost*qty)", "sum(cost*qty)"));
    TemporalAggregateDeclarations.declare(controller, transaction, BitemporalProjections.ROOT_PATH,
        List.of("sid", "grade"), List.of("count(cost)", "min(cost)", "max(cost)"));
  }
}
