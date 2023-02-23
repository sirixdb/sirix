package org.sirix.xquery.function.xml.index.create;

import com.google.common.collect.ImmutableSet;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.jdm.Item;
import org.brackit.xquery.jdm.Iter;
import org.brackit.xquery.jdm.Sequence;
import org.brackit.xquery.jdm.Signature;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.util.path.Path;
import org.sirix.access.trx.node.xml.XmlIndexController;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.api.xml.XmlResourceSession;
import org.sirix.exception.SirixIOException;
import org.sirix.index.IndexDef;
import org.sirix.index.IndexDefs;
import org.sirix.index.IndexType;
import org.sirix.xquery.function.xml.XMLFun;
import org.sirix.xquery.node.XmlDBNode;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Function for creating path indexes on stored documents, optionally restricted to a set of paths.
 * If successful, this function returns statistics about the newly created index as an XML fragment.
 * Supported signatures are:<br>
 * <ul>
 * <li><code>xml:create-path-index($doc as node(), $paths as xs:string*) as
 * node()</code></li>
 * <li><code>xml:create-path-index($doc as node()) as node()</code></li>
 * </ul>
 *
 * @author Max Bechtold
 * @author Johannes Lichtenberger
 *
 */
public final class CreatePathIndex extends AbstractFunction {

  /** Path index function name. */
  public final static QNm CREATE_PATH_INDEX = new QNm(XMLFun.XML_NSURI, XMLFun.XML_PREFIX, "create-path-index");

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

    final XmlDBNode doc = ((XmlDBNode) args[0]);
    final XmlNodeReadOnlyTrx rtx = doc.getTrx();
    final XmlResourceSession manager = rtx.getResourceSession();

    final Optional<XmlNodeTrx> optionalWriteTrx = manager.getNodeTrx();
    final XmlNodeTrx wtx = optionalWriteTrx.orElseGet(() -> manager.beginNodeTrx());

    if (rtx.getRevisionNumber() < manager.getMostRecentRevisionNumber()) {
      wtx.revertTo(rtx.getRevisionNumber());
    }

    final XmlIndexController controller = wtx.getResourceSession().getWtxIndexController(wtx.getRevisionNumber() - 1);

    if (controller == null) {
      throw new QueryException(new QNm("Document not found: " + ((Str) args[1]).stringValue()));
    }

    final Set<Path<QNm>> paths = new HashSet<>();
    if (args.length > 1 && args[1] != null) {
      final Iter it = args[1].iterate();
      Item next = it.next();
      while (next != null) {
        paths.add(Path.parse(((Str) next).stringValue()));
        next = it.next();
      }
    }

    final IndexDef idxDef =
        IndexDefs.createPathIdxDef(paths, controller.getIndexes().getNrOfIndexDefsWithType(IndexType.PATH), IndexDef.DbType.XML);
    try {
      controller.createIndexes(ImmutableSet.of(idxDef), wtx);
    } catch (final SirixIOException e) {
      throw new QueryException(new QNm("I/O exception: " + e.getMessage()), e);
    }
    return idxDef.materialize();
  }

}
