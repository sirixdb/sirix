package io.sirix.query.function.sdb.trx;

import io.brackit.query.QueryContext;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.QNm;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.sirix.query.StructuredDBItem;
import io.sirix.query.function.sdb.SDBFun;

import java.time.Instant;
import java.time.format.DateTimeFormatter;

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

  @Override
  public Sequence execute(final StaticContext staticContext, final QueryContext queryContext, final Sequence[] args) {
    final StructuredDBItem<?> document = ((StructuredDBItem<?>) args[0]);
    final Instant instant = document.getTrx().getRevisionTimestamp();
    final String dateTime = DateTimeFormatter.ISO_INSTANT.format(instant);
    return new DateTime(dateTime);
  }
}
