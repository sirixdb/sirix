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
import io.sirix.index.projection.ProjectionIndexBuilder;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.projection.ProjectionIndexLeafCodec;
import io.sirix.index.projection.ProjectionIndexLeafPage;
import io.sirix.index.projection.ProjectionIndexMetadata;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.json.JsonDBItem;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.util.ArrayList;
import java.util.Arrays;
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
 * type), so a resource can carry SEVERAL projections side by side — the
 * analytical executor picks the narrowest one covering each query's columns.
 * Calling the function with a shape (root path + field paths + types) that
 * is already catalogued re-uses that definition: within a session it
 * short-circuits, after re-opening a database it <em>hydrates</em> the
 * persisted columns instead of rebuilding. A different shape creates an
 * additional projection. Shape comparison uses the parsed paths' canonical
 * form, so spelling variants that parse to the same path match.
 *
 * <p>The projection is built over the revision of the passed document and
 * written compactly (see {@code ProjectionIndexLeafCodec}) together with a
 * self-describing {@link ProjectionIndexMetadata} payload into the session's
 * write transaction — call {@code sdb:commit($doc)} afterwards to persist,
 * exactly like the sibling index-creation functions.
 *
 * <p><b>Experimental.</b> The projection is a static snapshot of the indexed
 * revision, maintained by <em>invalidation</em>: an update transaction that
 * touches the record set uninstalls the projection (queries fall back to the
 * always-correct generic pipeline) and marks the persisted columns stale, so
 * re-running this function rebuilds them. The resource must be created with
 * a path summary. The projection is catalogued and persisted only when the
 * passed document is at the most recent revision — for older revisions it is
 * still built and installed for this session, but not stored. Column lookup
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
      final String name = lastStep(value);
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
    final byte[] columnKinds = new byte[fieldTypes.size()];
    for (int i = 0; i < fieldTypes.size(); i++) {
      columnKinds[i] = columnKindOf(fieldTypes.get(i));
    }

    final String resourceKey = session.getResourceConfig().getResource().toString();
    final String[] names = fieldNames.toArray(new String[0]);
    final int revision = document.getTrx().getRevisionNumber();
    // Canonical (parsed) path spellings — metadata and catalogue comparisons
    // use these, so spelling variants that parse to the same path match.
    final String rootPathCanonical = rootPath.toString();
    final String[] fieldPathCanonicals = new String[fieldPaths.size()];
    for (int i = 0; i < fieldPaths.size(); i++) {
      fieldPathCanonicals[i] = fieldPaths.get(i).toString();
    }

    // The resource's index catalogue is the durable source of truth for
    // which projections exist — same lifecycle as PATH/CAS/NAME indexes.
    final JsonIndexController controller = session.getRtxIndexController(revision);
    final IndexDef existingDef =
        findMatchingProjectionDef(controller, rootPathCanonical, fieldPathCanonicals, fieldTypes);
    if (existingDef != null) {
      // Already bootstrapped this session? The registry is the live scan-side
      // source for the executor — nothing to do when this shape is in.
      if (ProjectionIndexRegistry.lookupExactFields(resourceKey, names) != null) {
        return existingDef.materialize();
      }
      if (hydrate(session, revision, existingDef.getID(), resourceKey, rootPathCanonical,
          fieldPathCanonicals, columnKinds)) {
        return existingDef.materialize();
      }
      // Catalogued but stale or never-persisted payloads (invalidated by an
      // update transaction, or the creating transaction never committed) —
      // rebuild under the same definition.
      buildPersistInstall(session, existingDef, resourceKey, rootPath, fieldPaths, fieldTypes,
          fieldNames, names, revision);
      return existingDef.materialize();
    }

    // Legacy bootstrap: stores persisted by the bench setups carry leaves at
    // sub-tree 0 with no catalogued definition and no metadata payload. The
    // column count is the only shape evidence — keep the pre-catalogue
    // hydrate/guard semantics for them.
    if (controller.getIndexes().getNrOfIndexDefsWithType(IndexType.PROJECTION) == 0
        && hydrateLegacy(session, revision, resourceKey, names)) {
      return IndexDefs.createProjectionIdxDef(rootPath, fieldPaths, fieldTypes, 0,
          IndexDef.DbType.JSON).materialize();
    }

    // New projection — catalogued, built, persisted and installed through
    // the index controller, like the other index families.
    final IndexDef def = buildPersistInstall(session, null, resourceKey, rootPath, fieldPaths,
        fieldTypes, fieldNames, names, revision);
    return def.materialize();
  }

  /**
   * Hydrate a catalogued projection from its persisted HOT sub-tree into the
   * in-memory registry, validating the requested shape against the persisted
   * {@link ProjectionIndexMetadata}.
   *
   * @return {@code true} when payloads were found and installed
   */
  private static boolean hydrate(final JsonResourceSession session, final int revision,
      final int indexNumber, final String resourceKey, final String rootPathCanonical,
      final String[] fieldPathCanonicals, final byte[] columnKinds) {
    try (JsonNodeReadOnlyTrx probeRtx = session.beginNodeReadOnlyTrx(revision)) {
      final List<byte[]> persisted =
          ProjectionIndexHOTStorage.readAll(probeRtx.getStorageEngineReader(), indexNumber);
      if (persisted.isEmpty()) {
        return false;
      }
      final ProjectionIndexMetadata metadata = ProjectionIndexMetadata.parse(persisted.get(0));
      if (metadata == null || metadata.isStale()) {
        // Invalidated by an update transaction (stale tombstone) or written
        // without metadata — the caller rebuilds under the same definition.
        return false;
      }
      if (!metadata.matches(rootPathCanonical, fieldPathCanonicals, columnKinds)) {
        throw new QueryException(new QNm(
            "Persisted projection #" + indexNumber + " does not match its catalogued shape (root "
                + metadata.rootPath() + ", fields " + Arrays.toString(metadata.fieldPaths())
                + ") — the store is corrupt or was written by an incompatible version."));
      }
      final int leafCount = metadata.leafCount();
      if (persisted.size() < leafCount + 1) {
        throw new QueryException(new QNm(
            "Persisted projection #" + indexNumber + " declares " + leafCount + " leaves but only "
                + (persisted.size() - 1) + " are stored — the store is corrupt."));
      }
      // Decode exactly the declared leaves — higher slots may hold stale
      // remnants of a previous, larger build.
      final List<byte[]> decoded = new ArrayList<>(leafCount);
      for (int i = 1; i <= leafCount; i++) {
        decoded.add(ProjectionIndexLeafCodec.decode(persisted.get(i)));
      }
      ProjectionIndexRegistry.installWildcard(resourceKey, metadata.fieldNames(), decoded);
      return true;
    }
  }

  /**
   * Pre-catalogue bootstrap for bench-persisted stores: leaves (no metadata,
   * no catalogued def) at sub-tree 0. The column count is the only shape
   * evidence available — a mismatch fails loudly instead of mislabeling.
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

  /**
   * Build, catalogue, persist and install the projection through the
   * {@code IndexController} — the same lifecycle entry point the other
   * index-creation functions use. For the most recent revision the
   * controller does everything ({@code createIndexes} catalogues the
   * definition, bulk-builds the columns, streams metadata + compact leaves
   * into the definition's HOT sub-tree, installs the registry entry and
   * registers the invalidation listener); the writes ride the session's
   * write transaction — reused when one is open, begun otherwise — and are
   * deliberately NOT committed here, matching {@code jn:create-path-index}:
   * the caller's {@code sdb:commit($doc)} persists catalogue and payloads
   * atomically. (Committing a separate revision here would be undone by
   * {@code sdb:commit}, which reverts to the passed document's revision
   * before committing.) For an older revision the projection is built
   * locally and installed for this session only.
   *
   * @param defOrNull an already-catalogued definition to rebuild, or
   *                  {@code null} to create a new one under the next free id
   * @return the definition that was built
   */
  private static IndexDef buildPersistInstall(final JsonResourceSession session,
      final IndexDef defOrNull, final String resourceKey, final Path<QNm> rootPath,
      final List<Path<QNm>> fieldPaths, final List<Type> fieldTypes, final List<String> fieldNames,
      final String[] names, final int revision) {
    try (PathSummaryReader pathSummary = session.openPathSummary(revision)) {
      assertUnambiguousFieldNames(pathSummary, rootPath, fieldPaths, fieldNames);
    }
    if (revision == session.getMostRecentRevisionNumber()) {
      final Optional<JsonNodeTrx> existingWtx = session.getNodeTrx();
      final JsonNodeTrx wtx = existingWtx.orElseGet(session::beginNodeTrx);
      final JsonIndexController wtxController =
          session.getWtxIndexController(wtx.getRevisionNumber());
      // Resolve the definition against the wtx controller's catalogue —
      // IndexDef has identity semantics, so re-adding a same-shaped def
      // from another controller would duplicate the entry.
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
    // Older revision: session-only build + install, no catalogue/persist.
    final IndexDef def = defOrNull != null ? defOrNull
        : IndexDefs.createProjectionIdxDef(rootPath, fieldPaths, fieldTypes,
            nextProjectionIndexNumber(session.getRtxIndexController(revision)),
            IndexDef.DbType.JSON);
    final List<byte[]> leaves = new ArrayList<>();
    final ProjectionIndexBuilder builder;
    try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision);
         PathSummaryReader pathSummary = session.openPathSummary(revision)) {
      builder = new ProjectionIndexBuilder(def, pathSummary, leaves::add);
      builder.build(rtx);
    }
    ProjectionIndexRegistry.installWildcard(resourceKey, names, leaves,
        builder.numericColumnNonIntegralFlags());
    return def;
  }

  /**
   * Find a catalogued PROJECTION definition with exactly this shape (root
   * path, ordered field paths, ordered types). Comparison uses the parsed
   * paths' canonical form.
   */
  private static IndexDef findMatchingProjectionDef(final JsonIndexController controller,
      final String rootPathCanonical, final String[] fieldPathCanonicals,
      final List<Type> fieldTypes) {
    for (final IndexDef def : controller.getIndexes().getIndexDefs()) {
      if (!def.isProjectionIndex()) {
        continue;
      }
      if (!rootPathCanonical.equals(def.getProjectionRootPath().toString())) {
        continue;
      }
      final List<Path<QNm>> defFields = def.getProjectionFields();
      if (defFields.size() != fieldPathCanonicals.length
          || !def.getProjectionFieldTypes().equals(fieldTypes)) {
        continue;
      }
      boolean same = true;
      for (int i = 0; i < defFields.size(); i++) {
        if (!defFields.get(i).toString().equals(fieldPathCanonicals[i])) {
          same = false;
          break;
        }
      }
      if (same) {
        return def;
      }
    }
    return null;
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

  /** Column name = the final object-key step of the field path. */
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

  private static byte columnKindOf(final Type type) {
    if (type == Type.LON) {
      return ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG;
    }
    if (type == Type.BOOL) {
      return ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN;
    }
    return ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT;
  }
}
