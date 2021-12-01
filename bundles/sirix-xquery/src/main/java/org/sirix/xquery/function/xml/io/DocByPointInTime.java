package org.sirix.xquery.function.xml.io;

import java.time.Instant;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.DateTime;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.rest.AuthRole;
import org.sirix.xquery.function.DateTimeToInstant;
import org.sirix.xquery.function.Roles;
import org.sirix.xquery.function.xml.XMLFun;
import org.sirix.xquery.node.XmlDBCollection;

/**
 * <p>
 * Function for opening a document in a collection/database. If successful, this function returns
 * the document-node. Supported signatures are:
 * </p>
 * <ul>
 * <li><code>xml:open($coll as xs:string, $res as xs:string, $pointInTime as xs:long) as node()</code>
 * </li>
 * </ul>
 *
 * @author Max Bechtold
 * @author Johannes Lichtenberger
 *
 */
public final class DocByPointInTime extends AbstractFunction {

  /** Open function name. */
  public final static QNm OPEN = new QNm(XMLFun.XML_NSURI, XMLFun.XML_PREFIX, "open");

  private final DateTimeToInstant dateTimeToInstant = new DateTimeToInstant();

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public DocByPointInTime(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    if (args.length != 3) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final XmlDBCollection col = (XmlDBCollection) ctx.getNodeStore().lookup(((Str) args[0]).stringValue());

    if (col == null) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    Roles.check(ctx, col.getName(), AuthRole.VIEW);

    final String expResName = ((Str) args[1]).stringValue();
    final DateTime dateTime = (DateTime) args[2];
    final Instant pointInTime = dateTimeToInstant.convert(dateTime);

    return col.getDocument(expResName, pointInTime);
  }
}
