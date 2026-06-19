package io.sirix.query.function.jn.index.create;

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
import io.brackit.query.util.path.Path;
import io.sirix.access.ValidTimeConfig;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.query.compiler.optimizer.PlanCache;
import io.sirix.query.compiler.optimizer.stats.StatisticsCatalog;
import io.sirix.query.json.JsonDBItem;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * <p>
 * Function for creating a valid-time (bitemporal) interval index on a stored document. The index is
 * a persistent Relational-Interval-Tree (HOT-backed) over each record OBJECT's
 * {@code [validFrom, validTo]} interval, used to accelerate {@code jn:valid-at} /
 * {@code jn:open-bitemporal} stabbing queries. If successful, this function returns the document
 * node. Supported signatures are:
 * </p>
 * <ul>
 * <li><code>jn:create-valid-time-index($doc as json-item(), $paths as xs:string*) as json-item()</code></li>
 * <li><code>jn:create-valid-time-index($doc as json-item()) as json-item()</code></li>
 * </ul>
 *
 * <p>
 * The resource MUST be configured with valid-time paths (see
 * {@link io.sirix.access.ResourceConfiguration.Builder#validTimePaths(String, String)}); the index
 * always indexes those two fields. {@code $paths}, when omitted, defaults to {@code /[]/<validFrom>}
 * and {@code /[]/<validTo>} built from the resource's valid-time field names. The backend is always
 * HOT regardless of the global {@code sirix.index.useHOT} setting, because the interval engine needs
 * the HOT trie's order-preserving range scans.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class CreateValidTimeIndex extends AbstractFunction {

  /** Valid-time interval index function name. */
  public static final QNm CREATE_VALID_TIME_INDEX =
      new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "create-valid-time-index");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public CreateValidTimeIndex(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    if (args.length != 1 && args.length != 2) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final JsonDBItem document = (JsonDBItem) args[0];
    final JsonNodeReadOnlyTrx rtx = document.getTrx();
    final JsonResourceSession resourceSession = rtx.getResourceSession();

    final ValidTimeConfig validTimeConfig = resourceSession.getResourceConfig().getValidTimeConfig();
    if (validTimeConfig == null) {
      throw new QueryException(new QNm("Resource does not have valid time configuration. "
          + "Configure valid time paths when creating the resource."));
    }

    final var optionalWriteTrx = resourceSession.getNodeTrx();
    final JsonNodeTrx wtx = optionalWriteTrx.orElseGet(resourceSession::beginNodeTrx);

    if (rtx.getRevisionNumber() < resourceSession.getMostRecentRevisionNumber()) {
      wtx.revertTo(rtx.getRevisionNumber());
    }

    final JsonIndexController controller = wtx.getResourceSession().getWtxIndexController(wtx.getRevisionNumber());
    if (controller == null) {
      throw new QueryException(new QNm("Document not found."));
    }

    final Set<Path<QNm>> paths = new LinkedHashSet<>();
    if (args.length == 2 && args[1] != null) {
      final Iter it = args[1].iterate();
      Item next = it.next();
      while (next != null) {
        paths.add(Path.parse(((Str) next).stringValue(), io.brackit.query.util.path.PathParser.Type.JSON));
        next = it.next();
      }
    }
    // Default the indexed paths to /[]/<validFrom> and /[]/<validTo> derived from the valid-time
    // field names. These are used for index identification/serialization; the builder/listener match
    // valid-time fields by NAME from the resource's ValidTimeConfig, so the exact path is not
    // load-bearing for correctness.
    if (paths.isEmpty()) {
      paths.add(Path.parse("/[]/" + validTimeConfig.getNormalizedValidFromPath(),
          io.brackit.query.util.path.PathParser.Type.JSON));
      paths.add(Path.parse("/[]/" + validTimeConfig.getNormalizedValidToPath(),
          io.brackit.query.util.path.PathParser.Type.JSON));
    }

    final IndexDef validTimeIdxDef = IndexDefs.createValidTimeIdxDef(paths,
        controller.getIndexes().getNrOfIndexDefsWithType(IndexType.VALIDTIME), IndexDef.DbType.JSON);
    try {
      controller.createIndexes(Set.of(validTimeIdxDef), wtx);
    } catch (final SirixIOException e) {
      throw new QueryException(new QNm("I/O exception: " + e.getMessage()), e);
    }

    // Invalidate cached query plans so the optimizer considers this new index.
    PlanCache.signalIndexSchemaChange();

    // Invalidate stale histogram statistics for this resource since the index schema changed.
    try {
      final String dbName = document.getCollection().getDatabase().getName();
      final String resName = resourceSession.getResourceConfig().getName();
      StatisticsCatalog.getInstance().invalidate(dbName, resName);
    } catch (final Exception e) {
      // Histogram invalidation is best-effort; must not prevent index creation.
    }

    return validTimeIdxDef.materialize();
  }
}
