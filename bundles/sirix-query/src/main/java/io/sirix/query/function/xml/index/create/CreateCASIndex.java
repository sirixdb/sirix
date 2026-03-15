package io.sirix.query.function.xml.index.create;

import java.util.Set;
import io.sirix.query.function.xml.XMLFun;
import io.sirix.query.node.XmlDBNode;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.jdm.*;
import io.brackit.query.module.Namespaces;
import io.brackit.query.module.StaticContext;
import io.brackit.query.util.path.Path;
import io.sirix.access.trx.node.xml.XmlIndexController;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.query.compiler.optimizer.PlanCache;

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
    final XmlResourceSession resourceSession = rtx.getResourceSession();

    final Optional<XmlNodeTrx> optionalWriteTrx = resourceSession.getNodeTrx();
    final XmlNodeTrx wtx = optionalWriteTrx.orElseGet(() -> resourceSession.beginNodeTrx());

    if (rtx.getRevisionNumber() < resourceSession.getMostRecentRevisionNumber()) {
      wtx.revertTo(rtx.getRevisionNumber());
    }

    final XmlIndexController controller = wtx.getResourceSession().getWtxIndexController(wtx.getRevisionNumber());

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
      controller.createIndexes(Set.of(idxDef), wtx);
    } catch (final SirixIOException e) {
      throw new QueryException(new QNm("I/O exception: " + e.getMessage()), e);
    }

    PlanCache.signalIndexSchemaChange();

    return idxDef.materialize();
  }
}
