package org.sirix.xquery.function.sdb.index.find;

import java.util.Optional;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.Int32;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.Namespaces;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.brackit.xquery.xdm.Type;
import org.sirix.access.trx.node.xdm.XdmIndexController;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.index.IndexDef;
import org.sirix.xquery.function.sdb.SDBFun;
import org.sirix.xquery.node.DBNode;

/**
 * <p>
 * Function for finding a path index. If successful, this function returns the path-index number.
 * Otherwise it returns -1.
 *
 * Supported signatures are:
 * </p>
 * <ul>
 * <li>
 * <code>sdb:find-cas-index($doc as xs:node, $type as xs:string, $path as xs:string) as xs:int</code>
 * </li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class FindCASIndex extends AbstractFunction {

  /** CAS index function name. */
  public final static QNm FIND_CAS_INDEX = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX, "find-cas-index");

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
  public Sequence execute(StaticContext sctx, QueryContext ctx, Sequence[] args) throws QueryException {
    final DBNode doc = (DBNode) args[0];
    final NodeReadOnlyTrx rtx = doc.getTrx();
    final XdmIndexController controller =
        (XdmIndexController) rtx.getResourceManager().getRtxIndexController(rtx.getRevisionNumber());

    if (controller == null) {
      throw new QueryException(new QNm("Document not found: " + ((Str) args[1]).stringValue()));
    }

    final QNm name = new QNm(Namespaces.XS_NSURI, ((Str) args[1]).stringValue());
    final Type type = sctx.getTypes().resolveAtomicType(name);
    final Path<QNm> path = Path.parse(((Str) args[2]).stringValue());
    final Optional<IndexDef> indexDef = controller.getIndexes().findCASIndex(path, type);

    if (indexDef.isPresent())
      return new Int32(indexDef.get().getID());
    return new Int32(-1);
  }
}
