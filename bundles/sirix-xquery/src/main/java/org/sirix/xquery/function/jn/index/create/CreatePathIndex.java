package org.sirix.xquery.function.jn.index.create;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.function.json.JSONFun;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Iter;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.access.trx.node.json.JsonIndexController;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.api.json.JsonResourceSession;
import org.sirix.exception.SirixIOException;
import org.sirix.index.IndexDef;
import org.sirix.index.IndexDefs;
import org.sirix.index.IndexType;
import org.sirix.xquery.json.JsonDBItem;
import com.google.common.collect.ImmutableSet;

/**
 * Function for creating path indexes on stored documents, optionally restricted to a set of paths.
 * If successful, this function returns statistics about the newly created index as an Json
 * fragment. Supported signatures are:<br>
 * <ul>
 * <li><code>jn:create-path-index($doc as json-item(), $paths as xs:string*) as json-item()</code></li>
 * <li><code>jn:create-path-index($doc as json-item()) as json-item()</code></li>
 * </ul>
 *
 * @author Max Bechtold
 * @author Johannes Lichtenberger
 *
 */
public final class CreatePathIndex extends AbstractFunction {

  /** Path index function name. */
  public final static QNm CREATE_PATH_INDEX = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "create-path-index");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public CreatePathIndex(QNm name, Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    if (args.length != 2 && args.length != 3) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final JsonDBItem doc = (JsonDBItem) args[0];
    final JsonNodeReadOnlyTrx rtx = doc.getTrx();
    final JsonResourceSession manager = rtx.getResourceSession();

    final Optional<JsonNodeTrx> optionalWriteTrx = manager.getNodeTrx();
    final JsonNodeTrx wtx = optionalWriteTrx.orElseGet(() -> manager.beginNodeTrx());

    if (rtx.getRevisionNumber() < manager.getMostRecentRevisionNumber()) {
      wtx.revertTo(rtx.getRevisionNumber());
    }

    final JsonIndexController controller = wtx.getResourceSession().getWtxIndexController(wtx.getRevisionNumber() - 1);

    if (controller == null) {
      throw new QueryException(new QNm("Document not found: " + ((Str) args[1]).stringValue()));
    }

    final Set<Path<QNm>> paths = new HashSet<>();
    if (args.length > 1 && args[1] != null) {
      final Iter it = args[1].iterate();
      Item next = it.next();
      while (next != null) {
        paths.add(Path.parse(((Str) next).stringValue(), org.brackit.xquery.util.path.PathParser.Type.JSON));
        next = it.next();
      }
    }

    final IndexDef idxDef =
        IndexDefs.createPathIdxDef(paths, controller.getIndexes().getNrOfIndexDefsWithType(IndexType.PATH), IndexDef.DbType.JSON);
    try {
      controller.createIndexes(ImmutableSet.of(idxDef), wtx);
    } catch (final SirixIOException e) {
      throw new QueryException(new QNm("I/O exception: " + e.getMessage()), e);
    }
    return idxDef.materialize();
  }

}
