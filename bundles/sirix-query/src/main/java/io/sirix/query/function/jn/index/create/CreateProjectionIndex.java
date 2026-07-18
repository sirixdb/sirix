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
import io.brackit.query.jdm.Type;
import io.brackit.query.module.StaticContext;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.projection.ProjectionIndexLeafCodec;
import io.sirix.index.projection.ProjectionIndexLeafPage;
import io.sirix.index.projection.ProjectionIndexMetadata;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.json.JsonDBItem;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Function for creating a columnar <em>projection index</em> over a set of
 * record fields — the analytical fast path behind aggregate / filter /
 * group-by queries. Supported signatures:
 * <ul>
 * <li><code>jn:create-projection-index($doc as json-item(), $rootPath as xs:string,
 * $fields as xs:string*, $types as xs:string*) as json-item()</code></li>
 * <li><code>jn:create-projection-index($doc as json-item(), $rootPath as xs:string,
 * $fields as xs:string*) as json-item()</code> — every field typed as
 * {@code string}</li>
 * </ul>
 *
 * <p>{@code $rootPath} selects the record set (e.g. {@code /[]} for a
 * top-level array, {@code /wrapper/records/[]} for a nested one);
 * {@code $fields} are the projected column paths relative to the document
 * root; {@code $types} declare the per-column primitive shape —
 * {@code "long"} (also accepts {@code integer}/{@code int}),
 * {@code "boolean"} ({@code bool}), or {@code "string"} ({@code str}).
 * Floating-point column types are not supported yet: numeric columns store
 * 64-bit longs and non-integral values are flagged unrepresentable, so
 * declaring a {@code double} column would silently degrade — it is rejected
 * instead.
 *
 * <p>Projection indexes work like the other index families
 * ({@code jn:create-path-index} etc.): each definition is catalogued in the
 * resource's index set with its own id (numbered within the PROJECTION
 * type), a resource can carry SEVERAL projections side by side, and the
 * analytical executor discovers them through the revision-scoped catalog
 * and page layer ({@code ProjectionIndexCatalog}) — after re-opening a
 * database, queries use persisted projections WITHOUT re-running this
 * function. Calling it with an already-catalogued shape verifies the
 * persisted columns and returns; a stale or missing store (e.g. after an
 * update invalidated it) is rebuilt under the same definition; a different
 * shape creates an additional projection. Shape comparison uses the parsed
 * paths' canonical form, so spelling variants that parse to the same path
 * match.
 *
 * <p>The projection is built over the passed document's revision — like the
 * sibling functions, a document bound to an older revision reverts the
 * write transaction to that revision first — and written compactly (see
 * {@code ProjectionIndexLeafCodec}) together with a self-describing
 * {@link ProjectionIndexMetadata} payload into the session's write
 * transaction: call {@code sdb:commit($doc)} afterwards to persist.
 *
 * <p><b>Experimental.</b> The projection is a static snapshot maintained by
 * <em>invalidation</em>: an update transaction that touches the record set
 * tombstones the persisted columns, queries at later revisions fall back to
 * the always-correct generic pipeline, and re-running this function
 * rebuilds. The resource must be created with a path summary. Column lookup
 * is by trailing field name, so creation rejects field names that resolve
 * ambiguously under the record set.
 *
 * @author Johannes Lichtenberger
 */
public final class CreateProjectionIndex extends AbstractFunction {

  /** Projection index function name. */
  public static final QNm CREATE_PROJECTION_INDEX =
      new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "create-projection-index");

  public CreateProjectionIndex(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    if (args.length != 3 && args.length != 4) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }
    final JsonDBItem document = (JsonDBItem) args[0];
    final JsonResourceSession session = document.getTrx().getResourceSession();
    if (!session.getResourceConfig().withPathSummary) {
      throw new QueryException(new QNm(
          "jn:create-projection-index requires a resource created with a path summary "
              + "(buildPathSummary=true) — the projection builder resolves its paths through it."));
    }

    final String rootPathString = ((Str) args[1]).stringValue();
    final Path<QNm> rootPath = Path.parse(rootPathString, PathParser.Type.JSON);
    final List<Path<QNm>> fieldPaths = new ArrayList<>();
    final List<String> fieldNames = new ArrayList<>();
    final Set<String> seenNames = new HashSet<>();
    forEachString(args[2], value -> {
      final String name = lastStep(Path.parse(value, PathParser.Type.JSON).toString());
      if (name.isEmpty() || "[]".equals(name)) {
        throw new QueryException(new QNm(
            "Projected field path '" + value + "' must end in an object-key step."));
      }
      if (!seenNames.add(name)) {
        throw new QueryException(new QNm(
            "Duplicate projected field name '" + name + "' — column lookup is by trailing "
                + "field name, which must be unique."));
      }
      fieldPaths.add(Path.parse(value, PathParser.Type.JSON));
      fieldNames.add(name);
    });
    if (fieldPaths.isEmpty()) {
      throw new QueryException(new QNm("At least one projected field path is required."));
    }
    final List<Type> fieldTypes = new ArrayList<>(fieldPaths.size());
    if (args.length == 4 && args[3] != null) {
      forEachString(args[3], value -> fieldTypes.add(mapType(value)));
      if (fieldTypes.size() != fieldPaths.size()) {
        throw new QueryException(new QNm("Field/type count mismatch: " + fieldPaths.size()
            + " fields vs " + fieldTypes.size() + " types."));
      }
    } else {
      for (int i = 0; i < fieldPaths.size(); i++) {
        fieldTypes.add(Type.STR);
      }
    }

    final String resourceKey = session.getResourceConfig().getResource().toString();
    final int revision = document.getTrx().getRevisionNumber();
    final String[] names = fieldNames.toArray(new String[0]);

    // The resource's index catalogue is the durable source of truth for
    // which projections exist — same lifecycle as PATH/CAS/NAME indexes.
    // When the session holds an open write transaction, its controller's
    // catalogue is the current one (it sees defs catalogued earlier in the
    // same uncommitted transaction); otherwise the read-side controller of
    // the document's revision is.
    final Optional<JsonNodeTrx> openWtx = session.getNodeTrx();
    final JsonIndexController controller = openWtx.isPresent()
        ? session.getWtxIndexController(openWtx.get().getRevisionNumber())
        : session.getRtxIndexController(revision);
    final IndexDef existingDef =
        controller.getIndexes().findProjectionIndex(rootPath, fieldPaths, fieldTypes).orElse(null);
    if (existingDef != null) {
      // Persisted columns fresh at the document's revision? Nothing to do —
      // the executor loads them lazily through the same catalog path.
      if (ProjectionIndexCatalog.load(session, revision, existingDef) != null) {
        return existingDef.materialize();
      }
      // Stale (invalidated by updates), never committed, or unreadable —
      // rebuild under the same definition. Self-healing by design: leftover
      // sub-tree payloads from dropped/older definitions are overwritten.
      buildViaController(session, document, existingDef, rootPath, fieldPaths, fieldTypes,
          fieldNames);
      return existingDef.materialize();
    }

    // Legacy bootstrap: stores persisted by old bench setups carry leaves at
    // sub-tree 0 with no catalogued definition and no metadata payload. The
    // column count is the only shape evidence — keep the pre-catalogue
    // hydrate/guard semantics for them (registry wildcard pool).
    if (controller.getIndexes().getNrOfIndexDefsWithType(IndexType.PROJECTION) == 0
        && hydrateLegacy(session, revision, resourceKey, names)) {
      return IndexDefs.createProjectionIdxDef(rootPath, fieldPaths, fieldTypes, 0,
          IndexDef.DbType.JSON).materialize();
    }

    // New projection — catalogued, built and persisted through the index
    // controller, like the other index families.
    final IndexDef def = buildViaController(session, document, null, rootPath, fieldPaths,
        fieldTypes, fieldNames);
    return def.materialize();
  }

  /**
   * Build, catalogue and persist the projection through the
   * {@code IndexController} — the same lifecycle entry point the sibling
   * index-creation functions use. Mirrors {@code jn:create-path-index}
   * exactly: the session's write transaction is reused when open (beginning
   * a second would throw), begun otherwise; a document bound to an OLDER
   * revision reverts the transaction to that revision first; and nothing is
   * committed here — the caller's {@code sdb:commit($doc)} persists
   * catalogue and payloads atomically. Query-side visibility comes from the
   * revision-scoped catalog after commit, so uncommitted or rolled-back
   * builds are never observable elsewhere.
   *
   * @param defOrNull an already-catalogued definition to rebuild, or
   *                  {@code null} to create a new one under the next free id
   * @return the definition that was built
   */
  private static IndexDef buildViaController(final JsonResourceSession session,
      final JsonDBItem document, final IndexDef defOrNull, final Path<QNm> rootPath,
      final List<Path<QNm>> fieldPaths, final List<Type> fieldTypes, final List<String> fieldNames) {
    // Validate BEFORE touching any write transaction: a rejected creation
    // must neither leak a freshly-begun wtx (single-writer permit!) nor
    // have already discarded a reused transaction's uncommitted changes via
    // revertTo. The document's revision is exactly the state the build will
    // run over after the revert, so the committed path summary of that
    // revision is the right validation view.
    try (PathSummaryReader pathSummary =
        session.openPathSummary(document.getTrx().getRevisionNumber())) {
      assertUnambiguousFieldNames(pathSummary, rootPath, fieldPaths, fieldNames);
    }
    final Optional<JsonNodeTrx> existingWtx = session.getNodeTrx();
    final JsonNodeTrx wtx = existingWtx.orElseGet(session::beginNodeTrx);
    if (document.getTrx().getRevisionNumber() < session.getMostRecentRevisionNumber()) {
      wtx.revertTo(document.getTrx().getRevisionNumber());
    }
    final JsonIndexController wtxController = session.getWtxIndexController(wtx.getRevisionNumber());
    // Resolve the definition against the wtx controller's catalogue —
    // IndexDef has identity semantics, so re-adding a same-shaped def from
    // another controller would duplicate the entry.
    IndexDef def = defOrNull == null ? null
        : wtxController.getIndexes().getIndexDef(defOrNull.getID(), IndexType.PROJECTION);
    if (def == null) {
      def = IndexDefs.createProjectionIdxDef(rootPath, fieldPaths, fieldTypes,
          defOrNull != null ? defOrNull.getID() : nextProjectionIndexNumber(wtxController),
          IndexDef.DbType.JSON);
    }
    wtxController.createIndexes(Set.of(def), wtx);
    return def;
  }

  /**
   * Pre-catalogue bootstrap for bench-persisted stores: leaves (no metadata,
   * no catalogued def) at sub-tree 0, installed into the registry's wildcard
   * pool for the executor's fallback path. The column count is the only
   * shape evidence available — a mismatch fails loudly instead of
   * mislabeling.
   *
   * @return {@code true} when legacy payloads were found and installed
   */
  private static boolean hydrateLegacy(final JsonResourceSession session, final int revision,
      final String resourceKey, final String[] names) {
    try (JsonNodeReadOnlyTrx probeRtx = session.beginNodeReadOnlyTrx(revision)) {
      final List<byte[]> persisted =
          ProjectionIndexHOTStorage.readAll(probeRtx.getStorageEngineReader(), 0);
      if (persisted.isEmpty() || ProjectionIndexMetadata.parse(persisted.get(0)) != null) {
        return false;
      }
      final byte[] first = ProjectionIndexLeafCodec.decode(persisted.get(0));
      final int persistedColumns =
          first == null || first.length < 8 ? -1 : ProjectionIndexLeafPage.columnCountOf(first);
      if (persistedColumns != names.length) {
        throw new QueryException(new QNm(
            "A legacy projection with " + persistedColumns + " columns is already persisted for "
                + "this resource; re-creating with " + names.length
                + " columns is not supported for metadata-less stores."));
      }
      final List<byte[]> decoded = new ArrayList<>(persisted.size());
      decoded.add(first);
      for (int i = 1; i < persisted.size(); i++) {
        decoded.add(ProjectionIndexLeafCodec.decode(persisted.get(i)));
      }
      ProjectionIndexRegistry.installWildcard(resourceKey, names, decoded);
      return true;
    }
  }

  /** Next free id within the PROJECTION type (ids are unique per type). */
  private static int nextProjectionIndexNumber(final JsonIndexController controller) {
    int max = -1;
    for (final IndexDef def : controller.getIndexes().getIndexDefs()) {
      if (def.isProjectionIndex() && def.getID() > max) {
        max = def.getID();
      }
    }
    return max + 1;
  }

  /**
   * Column lookup in the executor is by trailing field name. If a projected
   * name also exists at a DIFFERENT path under the record set (e.g. both
   * {@code /[]/age} and {@code /[]/address/age}), queries touching the other
   * occurrence would silently read the projected column — reject the
   * creation as ambiguous instead.
   */
  private static void assertUnambiguousFieldNames(final PathSummaryReader pathSummary,
      final Path<QNm> rootPath, final List<Path<QNm>> fieldPaths, final List<String> fieldNames) {
    final LongSet rootPcrs = pathSummary.getPCRsForPaths(Set.of(rootPath));
    for (int i = 0; i < fieldPaths.size(); i++) {
      final String name = fieldNames.get(i);
      final LongSet ownPcrs = pathSummary.getPCRsForPaths(Set.of(fieldPaths.get(i)));
      final Path<QNm> anyWithName = new Path<QNm>().descendantObjectField(new QNm(name));
      final LongIterator byName = pathSummary.getPCRsForPaths(Set.of(anyWithName)).iterator();
      while (byName.hasNext()) {
        final long pcr = byName.nextLong();
        if (!ownPcrs.contains(pcr) && isUnderAny(pathSummary, pcr, rootPcrs)) {
          throw new QueryException(new QNm(
              "Projected field name '" + name + "' is ambiguous: it also occurs at a different "
                  + "path under the record set. Column lookup is by trailing field name, so the "
                  + "projection cannot distinguish the two occurrences."));
        }
      }
    }
  }

  /** Whether the path-summary node {@code pcr} has an ancestor in {@code rootPcrs}. */
  private static boolean isUnderAny(final PathSummaryReader pathSummary, final long pcr,
      final LongSet rootPcrs) {
    final long saved = pathSummary.getNodeKey();
    try {
      if (!pathSummary.moveTo(pcr)) {
        return false;
      }
      while (pathSummary.moveToParent()) {
        if (rootPcrs.contains(pathSummary.getNodeKey())) {
          return true;
        }
      }
      return false;
    } finally {
      pathSummary.moveTo(saved);
    }
  }

  private static void forEachString(final Sequence sequence, final Consumer<String> consumer) {
    final Iter it = sequence.iterate();
    Item next = it.next();
    while (next != null) {
      consumer.accept(((Str) next.atomize()).stringValue());
      next = it.next();
    }
  }

  /** Column name = the final object-key step of the (canonical) field path. */
  private static String lastStep(final String fieldPath) {
    final int slash = fieldPath.lastIndexOf('/');
    return slash < 0 ? fieldPath : fieldPath.substring(slash + 1);
  }

  private static Type mapType(final String type) {
    return switch (type.toLowerCase()) {
      case "long", "integer", "int" -> Type.LON;
      case "boolean", "bool" -> Type.BOOL;
      case "string", "str" -> Type.STR;
      default -> throw new QueryException(new QNm(
          "Unsupported projection column type '" + type + "' — use long (integer/int), boolean "
              + "(bool), or string (str). Floating-point columns are not supported: numeric "
              + "columns store 64-bit longs and would silently degrade for non-integral values."));
    };
  }
}
