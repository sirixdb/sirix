package io.sirix.query.function.jn.index.drop;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.QNm;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.Indexes;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.function.jn.index.IndexDdl;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * <p>
 * Function for dropping projection indexes from a stored document — the projection sibling of
 * {@code jn:drop-valid-time-index}. If successful, this function returns the document node.
 * Supported signatures:
 * </p>
 * <ul>
 * <li><code>jn:drop-projection-index($doc as json-item()) as json-item()</code> — drops ALL
 * projection indexes on the resource.</li>
 * <li><code>jn:drop-projection-index($doc as json-item(), $idx-no as xs:int)
 * as json-item()</code> — drops the projection index with the given id (see
 * {@code jn:find-projection-index}).</li>
 * </ul>
 *
 * <p>
 * The definition is removed from the catalogue; its immutable historical tree is left untouched.
 * Projection tree ids are never reused while their physical reference exists, so a later creation
 * receives a new, empty tree and cannot mistake unmaintained pre-drop columns for current data. The
 * catalogue change rides the session's write transaction — call {@code sdb:commit($doc)} to
 * persist. Revisions committed BEFORE the drop keep their catalogue entry and payloads, so
 * time-travel queries at those revisions continue to be served by the projection.
 *
 * @author Johannes Lichtenberger
 */
public final class DropProjectionIndex extends AbstractFunction {

  /** Projection index DROP function name. */
  public static final QNm DROP_PROJECTION_INDEX =
      new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "drop-projection-index");

  public DropProjectionIndex(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public boolean isUpdating() {
    return true;
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    if (args.length != 1 && args.length != 2) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final JsonDBItem document = (JsonDBItem) args[0];
    final JsonNodeReadOnlyTrx rtx = document.getTrx();
    final JsonResourceSession resourceSession = rtx.getResourceSession();

    final var optionalWriteTrx = resourceSession.getNodeTrx();
    if (optionalWriteTrx.isPresent()) {
      optionalWriteTrx.get().awaitPendingAsyncCommit();
    }
    final int revision = rtx.getRevisionNumber();
    final Indexes indexes = IndexDdl.usesWriter(document, optionalWriteTrx.orElse(null))
        ? resourceSession.getWtxIndexController(optionalWriteTrx.get().getRevisionNumber()).getIndexes()
        : resourceSession.getRtxIndexController(revision).getIndexes();
    final Integer requestedId = args.length == 2 && args[1] != null
        ? ((Int32) args[1]).intValue()
        : null;
    final Set<IndexDef> toDrop = new LinkedHashSet<>();
    for (final IndexDef indexDef : indexes.getIndexDefs()) {
      if (indexDef.getType() == IndexType.PROJECTION && (requestedId == null || indexDef.getID() == requestedId)) {
        toDrop.add(indexDef);
      }
    }
    if (requestedId != null && toDrop.isEmpty()) {
      throw new QueryException(new QNm("No PROJECTION index with id " + requestedId + " found on the resource."));
    }
    if (toDrop.isEmpty()) {
      return document;
    }
    // A wtx we OPEN here is ours to close on failure; success leaves it open for the caller's
    // commit. One the session already holds belongs to the caller and must survive this function.
    // Everything below therefore runs inside a try/finally — a throw that stranded a freshly-begun
    // wtx would hold the resource's single writer permit for the rest of the session, so the next
    // write of any kind blocks or fails.
    final boolean wtxIsOurs = optionalWriteTrx.isEmpty();
    final JsonNodeTrx wtx = optionalWriteTrx.orElseGet(resourceSession::beginNodeTrx);
    boolean succeeded = false;
    try {
      wtx.awaitPendingAsyncCommit();
      if (!IndexDdl.usesWriter(document, wtx)) {
        wtx.revertTo(revision);
      }

      final JsonIndexController controller = wtx.getResourceSession().getWtxIndexController(wtx.getRevisionNumber());

      controller.dropIndexes(toDrop, wtx);
      succeeded = true;
    } finally {
      if (wtxIsOurs && !succeeded) {
        wtx.close();
      }
    }

    return document;
  }

}
