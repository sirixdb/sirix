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
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
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
 * <p>The projection is built over the revision of the passed document,
 * written compactly (see {@code ProjectionIndexLeafCodec}) together with a
 * self-describing {@link ProjectionIndexMetadata} payload into the session's
 * write transaction — call {@code sdb:commit($doc)} afterwards to persist,
 * exactly like the sibling {@code jn:create-path-index} family — and
 * installed in the in-memory registry, where the analytical executor picks
 * it up automatically. Calling the function again on a resource whose projection
 * is already persisted re-hydrates it instead of rebuilding — the intended
 * per-session bootstrap after re-opening a database. Hydration validates the
 * requested shape (root path, field paths, column types) against the
 * persisted metadata and fails loudly on a mismatch instead of silently
 * mislabeling columns. Use the same path spellings across calls — the
 * comparison is textual.
 *
 * <p><b>Experimental.</b> One projection per resource; the projection is a
 * static snapshot of the indexed revision (it is not yet maintained by
 * update transactions) and requires the resource to be created with a path
 * summary. It is persisted only when the passed document is at the most
 * recent revision — for older revisions the projection is still built and
 * installed for this session, but not stored. The registry entry answers for
 * any record-set path of the resource, so a resource with several distinct
 * record sets sharing field names should not use a projection yet; creation
 * rejects field names that resolve ambiguously under the record set.
 *
 * @author Johannes Lichtenberger
 */
public final class CreateProjectionIndex extends AbstractFunction {

  /** Projection index function name. */
  public static final QNm CREATE_PROJECTION_INDEX =
      new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "create-projection-index");

  /** IndexDef id — one projection sub-tree per resource for now. */
  private static final int INDEX_NUMBER = 0;

  /** HOT slot of the {@link ProjectionIndexMetadata} payload; leaves at 1..N. */
  private static final int METADATA_SLOT = 0;

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
    final List<String> fieldPathStrings = new ArrayList<>();
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
      fieldPathStrings.add(value);
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

    final IndexDef def = IndexDefs.createProjectionIdxDef(rootPath, fieldPaths, fieldTypes,
        INDEX_NUMBER, IndexDef.DbType.JSON);
    final String resourceKey = session.getResourceConfig().getResource().toString();
    final String[] names = fieldNames.toArray(new String[0]);

    // Already bootstrapped this session? The registry is the live source of
    // truth for the executor — nothing to do when the same shape is in.
    final ProjectionIndexRegistry.Handle installed = ProjectionIndexRegistry.lookup(resourceKey, null);
    if (installed != null && Arrays.equals(installed.fieldNames(), names)) {
      return def.materialize();
    }

    final int revision = document.getTrx().getRevisionNumber();

    // Fast path: a persisted projection exists — hydrate it instead of
    // rebuilding (the per-session bootstrap after re-opening a database).
    // The persisted metadata (slot 0) is authoritative for the projection's
    // shape; silently hydrating under a different field list would mislabel
    // columns and corrupt query results, so a mismatch fails loudly.
    try (JsonNodeReadOnlyTrx probeRtx = session.beginNodeReadOnlyTrx(revision)) {
      final List<byte[]> persisted =
          ProjectionIndexHOTStorage.readAll(probeRtx.getStorageEngineReader(), INDEX_NUMBER);
      if (!persisted.isEmpty()) {
        final ProjectionIndexMetadata metadata = ProjectionIndexMetadata.parse(persisted.get(0));
        if (metadata != null) {
          if (!metadata.matches(rootPathString, fieldPathStrings.toArray(new String[0]), columnKinds)) {
            throw new QueryException(new QNm(
                "A projection with a different shape is already persisted for this resource "
                    + "(root " + metadata.rootPath() + ", fields "
                    + Arrays.toString(metadata.fieldPaths())
                    + "); re-creating with a different shape is not supported yet."));
          }
          final List<byte[]> decoded = new ArrayList<>(persisted.size() - 1);
          for (int i = 1; i < persisted.size(); i++) {
            decoded.add(ProjectionIndexLeafCodec.decode(persisted.get(i)));
          }
          ProjectionIndexRegistry.installWildcard(resourceKey, metadata.fieldNames(), decoded);
          return def.materialize();
        }
        // Metadata-less store (persisted by the bench setups): the column
        // count is the only shape evidence available — check it before
        // decoding the full leaf list.
        final byte[] first = ProjectionIndexLeafCodec.decode(persisted.get(0));
        final int persistedColumns =
            first == null || first.length < 8 ? -1 : ProjectionIndexLeafPage.columnCountOf(first);
        if (persistedColumns != names.length) {
          throw new QueryException(new QNm(
              "A projection with " + persistedColumns + " columns is already persisted for this "
                  + "resource; re-creating with " + names.length + " columns is not supported yet."));
        }
        final List<byte[]> decoded = new ArrayList<>(persisted.size());
        decoded.add(first);
        for (int i = 1; i < persisted.size(); i++) {
          decoded.add(ProjectionIndexLeafCodec.decode(persisted.get(i)));
        }
        ProjectionIndexRegistry.installWildcard(resourceKey, names, decoded);
        return def.materialize();
      }
    }

    // Build over the passed document's revision, persist compactly (most
    // recent revision only), install.
    final List<byte[]> leaves = new ArrayList<>();
    final ProjectionIndexBuilder builder;
    try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision);
         PathSummaryReader pathSummary = session.openPathSummary(revision)) {
      assertUnambiguousFieldNames(pathSummary, rootPath, fieldPaths, fieldNames);
      builder = new ProjectionIndexBuilder(def, pathSummary, leaves::add);
      builder.build(rtx);
    }
    if (revision == session.getMostRecentRevisionNumber()) {
      persist(session, rootPathString, fieldPathStrings, names, columnKinds, leaves);
    }
    ProjectionIndexRegistry.installWildcard(resourceKey, names, leaves,
        builder.numericColumnNonIntegralFlags());
    return def.materialize();
  }

  /**
   * Write metadata (slot 0) + compact leaves (slots 1..N) into the session's
   * write transaction — reused when one is open (beginning a second would
   * throw), begun otherwise. Deliberately NOT committed here, matching the
   * sibling index-creation functions ({@code jn:create-path-index} etc.):
   * the caller's {@code sdb:commit($doc)} persists the writes. Committing a
   * separate revision here would be undone by {@code sdb:commit}, which
   * reverts to the passed document's revision before committing.
   */
  private static void persist(final JsonResourceSession session, final String rootPathString,
      final List<String> fieldPathStrings, final String[] names, final byte[] columnKinds,
      final List<byte[]> leaves) {
    final Optional<JsonNodeTrx> existingWtx = session.getNodeTrx();
    final JsonNodeTrx wtx = existingWtx.orElseGet(session::beginNodeTrx);
    final ProjectionIndexHOTStorage storage =
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
    final ProjectionIndexMetadata metadata = new ProjectionIndexMetadata(rootPathString,
        fieldPathStrings.toArray(new String[0]), names, columnKinds);
    storage.put(METADATA_SLOT, metadata.serialize());
    for (int i = 0; i < leaves.size(); i++) {
      storage.put(i + 1, ProjectionIndexLeafCodec.encode(leaves.get(i)));
    }
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
