package io.sirix.query.function.sdb.trx;

import io.brackit.query.QueryContext;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.QNm;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.sirix.utils.CommitTimestamps;
import io.sirix.query.StructuredDBItem;
import io.sirix.query.function.sdb.SDBFun;

import java.time.Instant;

/**
 * <p>
 * Function for getting the current revision timestamp. Supported signature is:
 * </p>
 * <ul>
 * <li><code>sdb:timestamp($doc as xs:node) as xs:dateTime</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class GetRevisionTimestamp extends AbstractFunction {

  /** Get most recent revision function name. */
  public final static QNm TIMESTAMP = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX, "timestamp");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public GetRevisionTimestamp(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  /** Preserve VER-201 text without changing xs:dateTime comparisons or arithmetic. */
  private static final class CommitDateTime extends DateTime {
    private final String text;

    private CommitDateTime(final String text) {
      super(text);
      this.text = text;
    }

    @Override
    public String stringValue() {
      // Brackit's default dateTime text drops trailing fractional zeros.
      return text;
    }
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    final StructuredDBItem<?> document = ((StructuredDBItem<?>) args[0]);
    final Instant instant = document.getTrx().getRevisionTimestamp();
    final String dateTime = CommitTimestamps.format(instant);
    return new CommitDateTime(dateTime);
  }
}
