package io.sirix.query.function.jn.index.create;

import com.google.common.collect.ImmutableSet;
import io.sirix.query.json.JsonDBItem;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
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
  public Sequence execute(final StaticContext staticContext, final QueryContext queryContext, final Sequence[] args) {
    if (args.length != 2 && args.length != 3) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final JsonDBItem document = (JsonDBItem) args[0];
    final JsonNodeReadOnlyTrx readOnlyTrx = document.getTrx();
    final JsonResourceSession resourceManager = readOnlyTrx.getResourceSession();

    final Optional<JsonNodeTrx> optionalWriteTrx = resourceManager.getNodeTrx();
    final JsonNodeTrx writeTrx = optionalWriteTrx.orElseGet(resourceManager::beginNodeTrx);

    if (readOnlyTrx.getRevisionNumber() < resourceManager.getMostRecentRevisionNumber()) {
      writeTrx.revertTo(readOnlyTrx.getRevisionNumber());
    }

    final JsonIndexController controller = writeTrx.getResourceSession().getWtxIndexController(writeTrx.getRevisionNumber() - 1);

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

    final IndexDef selectiveNameIdxDef = IndexDefs.createSelectiveNameIdxDef(include,
        controller.getIndexes().getNrOfIndexDefsWithType(IndexType.NAME), IndexDef.DbType.JSON);
    try {
      controller.createIndexes(ImmutableSet.of(selectiveNameIdxDef), writeTrx);
    } catch (final SirixIOException e) {
      throw new QueryException(new QNm("I/O exception: " + e.getMessage()), e);
    }
    return selectiveNameIdxDef.materialize();
  }

}
