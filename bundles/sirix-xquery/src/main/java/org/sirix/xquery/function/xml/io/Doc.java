package org.sirix.xquery.function.xml.io;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.jdm.Sequence;
import org.brackit.xquery.jdm.Signature;
import org.brackit.xquery.module.StaticContext;
import org.sirix.xquery.function.FunUtil;
import org.sirix.xquery.function.xml.XMLFun;
import org.sirix.xquery.node.XmlDBCollection;

/**
 * <p>
 * Function for opening a document in a collection/database. If successful, this function returns
 * the document-node. Supported signatures are:
 * </p>
 * <ul>
 * <li><code>xml:doc($coll as xs:string, $res as xs:string, $revision as xs:int?) as node()</code>
 * </li>
 * <li><code>xml:doc($coll as xs:string, $res as xs:string) as node()</code></li>
 * </ul>
 *
 * @author Max Bechtold
 * @author Johannes Lichtenberger
 *
 */
public final class Doc extends AbstractFunction {

  /** Doc function name. */
  public final static QNm DOC = new QNm(XMLFun.XML_NSURI, XMLFun.XML_PREFIX, "doc");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public Doc(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    if (args.length < 2 || args.length > 4) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final XmlDBCollection col = (XmlDBCollection) ctx.getNodeStore().lookup(((Str) args[0]).stringValue());

    if (col == null) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final String expResName = ((Str) args[1]).stringValue();
    final int revision = FunUtil.getInt(args, 2, "revision", -1, null, false);

    return col.getDocument(expResName, revision);
  }
}
