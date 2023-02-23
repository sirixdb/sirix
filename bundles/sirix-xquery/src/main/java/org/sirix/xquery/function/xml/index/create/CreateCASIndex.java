package org.sirix.xquery.function.xml.index.create;

import com.google.common.collect.ImmutableSet;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.jdm.*;
import org.brackit.xquery.module.Namespaces;
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
 * <p>
 * Function for creating CAS indexes on stored documents, optionally restricted to a set of paths
 * and a content type. If successful, this function returns the document-node. Supported signatures
 * are:
 * </p>
 * <ul>
 * <li><code>xml:create-cas-index($doc as node(), $type as xs:string?, $paths as xs:string*) as node()</code></li>
 * <li><code>xml:create-cas-index($doc as node(), $type as xs:string?) as node()</code></li>
 * <li><code>xml:create-cas-index($doc as node()) as node()</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class CreateCASIndex extends AbstractFunction {

  /** CAS index function name. */
  public final static QNm CREATE_CAS_INDEX = new QNm(XMLFun.XML_NSURI, XMLFun.XML_PREFIX, "create-cas-index");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public CreateCASIndex(QNm name, Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(StaticContext sctx, QueryContext ctx, Sequence[] args) {
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

    Type type = null;
    if (args.length > 1 && args[1] != null) {
      final QNm name = new QNm(Namespaces.XS_NSURI, ((Str) args[1]).stringValue());
      type = sctx.getTypes().resolveAtomicType(name);
    }

    final Set<Path<QNm>> paths = new HashSet<>();
    if (args.length == 3 && args[2] != null) {
      final Iter it = args[2].iterate();
      Item next = it.next();
      while (next != null) {
        paths.add(Path.parse(((Str) next).stringValue()));
        next = it.next();
      }
    }

    final IndexDef idxDef = IndexDefs.createCASIdxDef(false, type, paths,
        controller.getIndexes().getNrOfIndexDefsWithType(IndexType.CAS), IndexDef.DbType.XML);
    try {
      controller.createIndexes(ImmutableSet.of(idxDef), wtx);
    } catch (final SirixIOException e) {
      throw new QueryException(new QNm("I/O exception: " + e.getMessage()), e);
    }

    return idxDef.materialize();
  }
}
