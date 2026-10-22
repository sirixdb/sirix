package io.sirix.query.function.jn.temporal;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Bool;
import io.brackit.query.atomic.IntNumeric;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.QNm;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.index.aggregate.DerivedIndexMaintainer;
import io.sirix.query.function.jn.JNFun;
import io.sirix.query.json.JsonDBItem;

import java.time.Duration;

/** Wait for a durable maintenance frontier; historical batch interiors still use exact catch-up. */
public final class AwaitDerived extends AbstractFunction {
  public static final QNm NAME = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "await-derived");
  private static final Int64 DEFAULT_TIMEOUT_MILLIS = new Int64(30_000);
  private static final Int64 MAX_TIMEOUT_MILLIS = new Int64(Long.MAX_VALUE / 1_000_000);
  private static final Int64 ZERO = new Int64(0);
  private static final Int64 ONE = new Int64(1);

  public AwaitDerived(final Signature signature) {
    super(NAME, signature, false);
  }

  @Override
  public Sequence execute(final StaticContext context, final QueryContext query, final Sequence[] arguments) {
    if (!(arguments[0] instanceof JsonDBItem document)) {
      throw new QueryException(JNFun.ERR_INVALID_ARGUMENT, "Await requires a stored JSON document");
    }
    final IntNumeric requested = (IntNumeric) arguments[1];
    final IntNumeric timeout = arguments.length > 2
        ? (IntNumeric) arguments[2]
        : DEFAULT_TIMEOUT_MILLIS;
    if (requested.cmp(ONE) < 0
        || requested.cmp(new Int64(document.getResourceSession().getMostRecentRevisionNumber())) > 0
        || timeout.cmp(ZERO) < 0 || timeout.cmp(MAX_TIMEOUT_MILLIS) > 0) {
      throw new QueryException(JNFun.ERR_INVALID_ARGUMENT, "Invalid derived revision or timeout in milliseconds");
    }
    final DerivedIndexMaintainer.Resource resource =
        ((AbstractResourceSession<?, ?>) document.getResourceSession()).getDerivedResource();
    if (resource == null) {
      return Bool.FALSE;
    }
    try {
      return resource.awaitRevision(requested.intValue(), Duration.ofMillis(timeout.longValue()))
          ? Bool.TRUE
          : Bool.FALSE;
    } catch (final InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new QueryException(interrupted, JNFun.ERR_INVALID_ARGUMENT, "Interrupted waiting for derived maintenance");
    } catch (final IllegalStateException unavailable) {
      throw new QueryException(unavailable, JNFun.ERR_INVALID_ARGUMENT, "Derived maintenance unavailable");
    }
  }
}
