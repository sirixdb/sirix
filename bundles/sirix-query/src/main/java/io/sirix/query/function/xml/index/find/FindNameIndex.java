package io.sirix.query.function.xml.index.find;

import io.sirix.query.function.xml.XMLFun;
import io.sirix.query.node.XmlDBNode;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.Int32;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.expr.Cast;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.jdm.Sequence;
import org.brackit.xquery.jdm.Signature;
import org.brackit.xquery.jdm.Type;
import org.brackit.xquery.module.StaticContext;
import io.sirix.access.trx.node.xml.XmlIndexController;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.index.IndexDef;

import java.util.Optional;

/**
 * <p>
 * Function for finding a name index. If successful, this function returns the name-index number.
 * Otherwise it returns -1.
 * </p>
 * <p>
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

    return indexDef.map(def -> new Int32(def.getID())).orElseGet(() -> new Int32(-1));
  }
}
