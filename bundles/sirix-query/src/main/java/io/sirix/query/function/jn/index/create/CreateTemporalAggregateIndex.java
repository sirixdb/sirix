package io.sirix.query.function.jn.index.create;

import io.brackit.query.QueryContext;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.aggregate.TemporalAggregateDeclarations;
import io.sirix.query.json.JsonDBItem;

import java.util.ArrayList;
import java.util.List;

/** Declare point aggregates over a covering projection; the caller commits the resulting index. */
public final class CreateTemporalAggregateIndex extends AbstractFunction {
  public static final QNm NAME = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "create-temporal-aggregate-index");

  public CreateTemporalAggregateIndex(final Signature signature) {
    super(NAME, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext context, final QueryContext query, final Sequence[] arguments) {
    final JsonDBItem document = (JsonDBItem) arguments[0];
    final JsonResourceSession session = document.getResourceSession();
    final var existing = session.getNodeTrx();
    final JsonNodeTrx transaction = existing.orElseGet(session::beginNodeTrx);
    boolean succeeded = false;
    try {
      if (document.getTrx().getRevisionNumber() < session.getMostRecentRevisionNumber()) {
        transaction.revertTo(document.getTrx().getRevisionNumber());
      }
      transaction.awaitPendingAsyncCommit();
      final JsonIndexController controller = session.getWtxIndexController(transaction.getRevisionNumber());
      final Sequence result = TemporalAggregateDeclarations.declare(controller, transaction,
          ((Str) arguments[1]).stringValue(), strings(arguments[2]), strings(arguments[3])).materialize();
      succeeded = true;
      return result;
    } finally {
      if (!succeeded && existing.isEmpty()) {
        transaction.close();
      }
    }
  }

  private static List<String> strings(final Sequence sequence) {
    final List<String> values = new ArrayList<>();
    if (sequence != null) {
      try (Iter iterator = sequence.iterate()) {
        for (Item item = iterator.next(); item != null; item = iterator.next()) {
          values.add(((Str) item).stringValue());
        }
      }
    }
    return values;
  }
}
