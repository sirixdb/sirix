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
import io.brackit.query.util.path.PathParser;
import io.sirix.access.ValidTimeConfig;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.json.ValidTimeIndexes;

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

    // Idempotent: the interval index always indexes the resource's two configured valid-time
    // fields (regardless of $paths), so a second definition would be a full duplicate — a second
    // builder pass plus a second listener maintaining a redundant HOT tree on every write. This
    // matters since resources created with valid-time options via jn:store/jn:load already carry
    // an auto-created interval index.
    for (final IndexDef existingIndexDef : controller.getIndexes().getIndexDefs()) {
      if (existingIndexDef.getType() == IndexType.VALIDTIME) {
        return existingIndexDef.materialize();
      }
    }

    final Set<Path<QNm>> paths = new LinkedHashSet<>();
    if (args.length == 2 && args[1] != null) {
      final Iter it = args[1].iterate();
      Item next = it.next();
      while (next != null) {
        paths.add(Path.parse(((Str) next).stringValue(), PathParser.Type.JSON));
        next = it.next();
      }
    }
    // Default the indexed paths to /[]/<validFrom> and /[]/<validTo> derived from the valid-time
    // field names. These are used for index identification/serialization; the builder/listener match
    // valid-time fields by NAME from the resource's ValidTimeConfig, so the exact path is not
    // load-bearing for correctness.
    if (paths.isEmpty()) {
      paths.addAll(ValidTimeIndexes.defaultPaths(validTimeConfig));
    }

    String databaseName = null;
    try {
      databaseName = document.getCollection().getDatabase().getName();
    } catch (final Exception e) {
      // Statistics invalidation is best-effort; a missing database name must not prevent creation.
    }

    final IndexDef validTimeIdxDef;
    try {
      validTimeIdxDef = ValidTimeIndexes.createIntervalIndex(controller, wtx, paths, databaseName,
          resourceSession.getResourceConfig().getName());
    } catch (final SirixIOException e) {
      throw new QueryException(new QNm("I/O exception: " + e.getMessage()), e);
    }

    return validTimeIdxDef.materialize();
  }
}
