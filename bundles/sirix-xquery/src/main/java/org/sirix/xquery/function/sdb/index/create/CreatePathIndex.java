package org.sirix.xquery.function.sdb.index.create;

import java.util.HashSet;
import java.util.Set;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Iter;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.access.trx.node.xml.XmlIndexController;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.index.IndexDef;
import org.sirix.index.IndexDefs;
import org.sirix.index.IndexType;
import org.sirix.xquery.function.sdb.SDBFun;
import org.sirix.xquery.node.DBNode;
import com.google.common.collect.ImmutableSet;

/**
 * Function for creating path indexes on stored documents, optionally restricted to a set of paths.
 * If successful, this function returns statistics about the newly created index as an XML fragment.
 * Supported signatures are:</br>
 * <ul>
 * <li><code>sdb:create-path-index($doc as node(), $paths as xs:string*) as
 * node()</code></li>
 * <li><code>sdb:create-path-index($doc as node()) as node()</code></li>
 * </ul>
 *
 * @author Max Bechtold
 * @author Johannes Lichtenberger
 *
 */
public final class CreatePathIndex extends AbstractFunction {

  /** Path index function name. */
  public final static QNm CREATE_PATH_INDEX = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX, "create-path-index");

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
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args)
      throws QueryException {
    if (args.length != 2 && args.length != 3) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final DBNode doc = ((DBNode) args[0]);
    final NodeReadOnlyTrx rtx = doc.getTrx();
    final XmlIndexController controller =
        (XmlIndexController) rtx.getResourceManager().getWtxIndexController(rtx.getRevisionNumber() - 1);

    if (!(doc.getTrx() instanceof XmlNodeTrx)) {
      throw new QueryException(new QNm("Collection must be updatable!"));
    }

    if (controller == null) {
      throw new QueryException(new QNm("Document not found: " + ((Str) args[1]).stringValue()));
    }

    if (!(doc.getTrx() instanceof XmlNodeTrx)) {
      throw new QueryException(new QNm("Collection must be updatable!"));
    }

    final Set<Path<QNm>> paths = new HashSet<>();
    if (args.length > 2 && args[2] != null) {
      final Iter it = args[2].iterate();
      Item next = it.next();
      while (next != null) {
        paths.add(Path.parse(((Str) next).stringValue()));
        next = it.next();
      }
    }

    final IndexDef idxDef =
        IndexDefs.createPathIdxDef(paths, controller.getIndexes().getNrOfIndexDefsWithType(IndexType.PATH));
    try {
      controller.createIndexes(ImmutableSet.of(idxDef), (XmlNodeTrx) doc.getTrx());
    } catch (final SirixIOException e) {
      throw new QueryException(new QNm("I/O exception: " + e.getMessage()), e);
    }
    return idxDef.materialize();
  }

}
