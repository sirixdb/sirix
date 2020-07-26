package org.sirix.xquery.function.sdb.trx;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.atomic.DateTime;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.xquery.StructuredDBItem;
import org.sirix.xquery.function.sdb.SDBFun;

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
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    final StructuredDBItem<?> doc = ((StructuredDBItem<?>) args[0]);
    final Instant instant = doc.getTrx().getRevisionTimestamp();
    final String dateTime = DateTimeFormatter.ISO_INSTANT.format(instant);
    return new DateTime(dateTime);
  }
}
