package org.sirix.xquery.function.jn.index.find;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.Int32;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.function.json.JSONFun;
import org.brackit.xquery.jdm.Sequence;
import org.brackit.xquery.jdm.Signature;
import org.brackit.xquery.module.StaticContext;
import org.sirix.access.trx.node.json.JsonIndexController;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.index.IndexDef;
import org.sirix.xquery.json.JsonDBItem;

import java.util.Optional;

/**
 * <p>
 * Function for finding a name index. If successful, this function returns the name-index number.
 * Otherwise it returns -1.
 *
 * Supported signatures are:
 * </p>
 * <ul>
 * <li><code>jn:find-name-index($doc as json-item(), $name as xs:string) as xs:int</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class FindNameIndex extends AbstractFunction {

  /** CAS index function name. */
  public final static QNm FIND_NAME_INDEX = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "find-name-index");

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
    final JsonDBItem doc = (JsonDBItem) args[0];
    final JsonNodeReadOnlyTrx rtx = doc.getTrx();
    final JsonIndexController controller = rtx.getResourceSession().getRtxIndexController(rtx.getRevisionNumber());

    if (controller == null) {
      throw new QueryException(new QNm("Document not found: " + ((Str) args[1]).stringValue()));
    }

    final QNm qnm = new QNm(((Str) args[1]).stringValue());
    final Optional<IndexDef> indexDef = controller.getIndexes().findNameIndex(qnm);

    return indexDef.map(IndexDef::getID).map(Int32::new).orElse(new Int32(-1));
  }
}
