package org.sirix.xquery.function.sdb.index.json.find;

import java.util.Optional;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.Int32;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.access.trx.node.json.JsonIndexController;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.index.IndexDef;
import org.sirix.xquery.function.sdb.SDBFun;
import org.sirix.xquery.json.JsonDBNode;

/**
 * <p>
 * Function for finding a path index. If successful, this function returns the path-index number.
 * Otherwise it returns -1.
 *
 * Supported signatures are:
 * </p>
 * <ul>
 * <li><code>sdb:find-path-index($doc as xs:node, $path as xs:string) as xs:int</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class FindPathIndex extends AbstractFunction {

  /** CAS index function name. */
  public final static QNm FIND_PATH_INDEX = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX, "find-path-index");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public FindPathIndex(QNm name, Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(StaticContext sctx, QueryContext ctx, Sequence[] args) {
    final JsonDBNode doc = (JsonDBNode) args[0];
    final JsonNodeReadOnlyTrx rtx = doc.getTrx();
    final JsonIndexController controller =
        (JsonIndexController) rtx.getResourceManager().getRtxIndexController(rtx.getRevisionNumber());

    if (controller == null) {
      throw new QueryException(new QNm("Document not found: " + ((Str) args[1]).stringValue()));
    }

    final Path<QNm> path = Path.parse(((Str) args[1]).stringValue());
    final Optional<IndexDef> indexDef = controller.getIndexes().findPathIndex(path);

    if (indexDef.isPresent())
      return new Int32(indexDef.get().getID());
    return new Int32(-1);
  }
}
