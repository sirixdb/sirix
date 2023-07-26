package io.sirix.query.function.jn.index.create;

import com.google.common.collect.ImmutableSet;
import io.sirix.query.json.JsonDBItem;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.function.json.JSONFun;
import org.brackit.xquery.jdm.Item;
import org.brackit.xquery.jdm.Iter;
import org.brackit.xquery.jdm.Sequence;
import org.brackit.xquery.jdm.Signature;
import org.brackit.xquery.module.StaticContext;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Function for creating name indexes on stored documents, optionally restricted to a set of
 * included {@code QNm}s. If successful, this function returns statistics about the newly created
 * index as an Json fragment. Supported signatures are:<br>
 * <ul>
 * <li><code>jn:create-name-index($doc as json-item(), $include as xs:QName*) as json-item()</code></li>
 * <li><code>jn:create-name-index($doc as json-item()) as json-item()</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class CreateNameIndex extends AbstractFunction {

  /** Path index function name. */
  public final static QNm CREATE_NAME_INDEX = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "create-name-index");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public CreateNameIndex(QNm name, Signature signature) {
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
    final JsonNodeTrx wtx = optionalWriteTrx.orElseGet(manager::beginNodeTrx);

    if (rtx.getRevisionNumber() < manager.getMostRecentRevisionNumber()) {
      wtx.revertTo(rtx.getRevisionNumber());
    }

    final JsonIndexController controller = wtx.getResourceSession().getWtxIndexController(wtx.getRevisionNumber() - 1);

    if (controller == null) {
      throw new QueryException(new QNm("Document not found: " + ((Str) args[1]).stringValue()));
    }

    final Set<QNm> include = new HashSet<>();
    if (args[1] != null) {
      final Iter it = args[1].iterate();
      Item next = it.next();
      while (next != null) {
        include.add(new QNm(((Str) next).stringValue()));
        next = it.next();
      }
    }

    final IndexDef idxDef = IndexDefs.createSelectiveNameIdxDef(include,
        controller.getIndexes().getNrOfIndexDefsWithType(IndexType.NAME), IndexDef.DbType.JSON);
    try {
      controller.createIndexes(ImmutableSet.of(idxDef), wtx);
    } catch (final SirixIOException e) {
      throw new QueryException(new QNm("I/O exception: " + e.getMessage()), e);
    }
    return idxDef.materialize();
  }

}
