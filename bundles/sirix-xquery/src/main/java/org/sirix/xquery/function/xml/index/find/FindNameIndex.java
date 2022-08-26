package org.sirix.xquery.function.xml.index.find;

import java.util.Optional;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.Int32;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.expr.Cast;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.brackit.xquery.xdm.Type;
import org.sirix.access.trx.node.xml.XmlIndexController;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.index.IndexDef;
import org.sirix.xquery.function.xml.XMLFun;
import org.sirix.xquery.node.XmlDBNode;

/**
 * <p>
 * Function for finding a name index. If successful, this function returns the name-index number.
 * Otherwise it returns -1.
 *
 * Supported signatures are:
 * </p>
 * <ul>
 * <li><code>xml:find-name-index($doc as node(), $name as xs:QName) as xs:int</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class FindNameIndex extends AbstractFunction {

  /** CAS index function name. */
  public final static QNm FIND_NAME_INDEX = new QNm(XMLFun.XML_NSURI, XMLFun.XML_PREFIX, "find-name-index");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public FindNameIndex(QNm name, Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(StaticContext sctx, QueryContext ctx, Sequence[] args) {
    final XmlDBNode doc = (XmlDBNode) args[0];
    final XmlNodeReadOnlyTrx rtx = doc.getTrx();
    final XmlIndexController controller = rtx.getResourceSession().getRtxIndexController(rtx.getRevisionNumber());

    if (controller == null) {
      throw new QueryException(new QNm("Document not found: " + ((Str) args[1]).stringValue()));
    }

    final QNm qnm = (QNm) Cast.cast(sctx, (Atomic) args[1], Type.QNM, false);
    final Optional<IndexDef> indexDef = controller.getIndexes().findNameIndex(qnm);

    if (indexDef.isPresent())
      return new Int32(indexDef.get().getID());
    return new Int32(-1);
  }
}
