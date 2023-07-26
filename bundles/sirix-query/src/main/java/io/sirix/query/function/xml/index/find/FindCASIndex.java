package io.sirix.query.function.xml.index.find;

import io.sirix.query.function.xml.XMLFun;
import io.sirix.query.node.XmlDBNode;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.Int32;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.jdm.Sequence;
import org.brackit.xquery.jdm.Signature;
import org.brackit.xquery.jdm.Type;
import org.brackit.xquery.module.Namespaces;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.util.path.Path;
import io.sirix.access.trx.node.xml.XmlIndexController;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.index.IndexDef;

import java.util.Optional;

/**
 * <p>
 * Function for finding a path index. If successful, this function returns the path-index number.
 * Otherwise it returns -1.
 * </p>
 * <p>
 * Supported signatures are:
 * </p>
 * <ul>
 * <li>
 * <code>xml:find-cas-index($doc as node(), $type as xs:string, $path as xs:string) as xs:int</code>
 * </li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class FindCASIndex extends AbstractFunction {

  /** CAS index function name. */
  public final static QNm FIND_CAS_INDEX = new QNm(XMLFun.XML_NSURI, XMLFun.XML_PREFIX, "find-cas-index");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public FindCASIndex(QNm name, Signature signature) {
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

    final QNm name = new QNm(Namespaces.XS_NSURI, ((Str) args[1]).stringValue());
    final Type type = sctx.getTypes().resolveAtomicType(name);
    final Path<QNm> path = Path.parse(((Str) args[2]).stringValue());
    final Optional<IndexDef> indexDef = controller.getIndexes().findCASIndex(path, type);

    return indexDef.map(def -> new Int32(def.getID())).orElseGet(() -> new Int32(-1));
  }
}
