package io.sirix.query.function.jn.index.find;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.query.json.JsonDBItem;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * Function for finding a projection index by its shape — the projection
 * sibling of {@code jn:find-path-index}. Returns the index definition
 * number, or {@code -1} if no projection with that shape is catalogued at
 * the document's revision. Supported signature:
 * </p>
 * <ul>
 * <li><code>jn:find-projection-index($doc as json-item(), $rootPath as
 * xs:string, $fields as xs:string*) as xs:int</code></li>
 * </ul>
 *
 * <p>Matching uses the parsed paths' canonical form (same identity rule as
 * {@code jn:create-projection-index}); declared column types are not part
 * of the lookup key here — shapes differing only in types are rare and the
 * id feeds {@code jn:drop-projection-index}, where dropping either is
 * intended.
 *
 * @author Johannes Lichtenberger
 */
public final class FindProjectionIndex extends AbstractFunction {

  /** Projection index FIND function name. */
  public static final QNm FIND_PROJECTION_INDEX =
      new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "find-projection-index");

  public FindProjectionIndex(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    if (args.length != 3) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final JsonDBItem document = (JsonDBItem) args[0];
    final JsonNodeReadOnlyTrx rtx = document.getTrx();
    final JsonIndexController controller =
        rtx.getResourceSession().getRtxIndexController(rtx.getRevisionNumber());

    final String rootPathCanonical =
        Path.parse(((Str) args[1]).stringValue(), PathParser.Type.JSON).toString();
    final List<String> fieldPathCanonicals = new ArrayList<>();
    final Iter it = args[2].iterate();
    Item next = it.next();
    while (next != null) {
      fieldPathCanonicals.add(
          Path.parse(((Str) next.atomize()).stringValue(), PathParser.Type.JSON).toString());
      next = it.next();
    }

    final IndexDef def = ProjectionIndexCatalog.findMatchingDef(
        controller.getIndexes().getIndexDefs(), rootPathCanonical,
        fieldPathCanonicals.toArray(new String[0]), null);
    return def == null ? new Int32(-1) : new Int32(def.getID());
  }
}
