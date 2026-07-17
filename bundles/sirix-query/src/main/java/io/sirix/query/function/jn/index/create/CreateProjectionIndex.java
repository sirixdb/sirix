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
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.json.JsonDBItem;

import java.util.ArrayList;
import java.util.List;
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
 * {@code "long"} (also accepts {@code integer}/{@code decimal}/{@code
 * double}), {@code "boolean"}, or {@code "string"}.
 *
 * <p>The projection is built over the document's most recent revision,
 * persisted compactly (see {@code ProjectionIndexLeafCodec}) and installed
 * in the in-memory registry, where the analytical executor picks it up
 * automatically. Calling the function again on a resource whose projection
 * is already persisted re-hydrates it instead of rebuilding — the intended
 * per-session bootstrap after re-opening a database.
 *
 * <p><b>Experimental.</b> One projection per resource; the projection is a
 * static snapshot of the indexed revision (it is not yet maintained by
 * update transactions) and requires the resource to be created with a path
 * summary.
 *
 * @author Johannes Lichtenberger
 */
public final class CreateProjectionIndex extends AbstractFunction {

  /** Projection index function name. */
  public static final QNm CREATE_PROJECTION_INDEX =
      new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "create-projection-index");

  /** IndexDef id — one projection sub-tree per resource for now. */
  private static final int INDEX_NUMBER = 0;

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

    final Path<QNm> rootPath = Path.parse(((Str) args[1]).stringValue(), PathParser.Type.JSON);
    final List<Path<QNm>> fieldPaths = new ArrayList<>();
    final List<String> fieldNames = new ArrayList<>();
    forEachString(args[2], value -> {
      fieldPaths.add(Path.parse(value, PathParser.Type.JSON));
      fieldNames.add(lastStep(value));
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

    final IndexDef def = IndexDefs.createProjectionIdxDef(rootPath, fieldPaths, fieldTypes,
        INDEX_NUMBER, IndexDef.DbType.JSON);
    final String resourceKey = session.getResourceConfig().getResource().toString();
    final int revision = session.getMostRecentRevisionNumber();
    final String[] names = fieldNames.toArray(new String[0]);

    // Fast path: a persisted projection exists — hydrate it instead of
    // rebuilding (the per-session bootstrap after re-opening a database).
    // The shape must match: silently hydrating a projection with a
    // different column count would mislabel columns and corrupt results.
    try (JsonNodeReadOnlyTrx probeRtx = session.beginNodeReadOnlyTrx(revision)) {
      final List<byte[]> persisted =
          ProjectionIndexHOTStorage.readAll(probeRtx.getStorageEngineReader(), INDEX_NUMBER);
      if (!persisted.isEmpty()) {
        final List<byte[]> decoded = new ArrayList<>(persisted.size());
        for (final byte[] payload : persisted) {
          decoded.add(ProjectionIndexLeafCodec.decode(payload));
        }
        final byte[] first = decoded.get(0);
        final int persistedColumns = first == null ? -1
            : (first[4] & 0xFF) | ((first[5] & 0xFF) << 8) | ((first[6] & 0xFF) << 16) | ((first[7] & 0xFF) << 24);
        if (persistedColumns != names.length) {
          throw new QueryException(new QNm(
              "A projection with " + persistedColumns + " columns is already persisted for this "
                  + "resource; re-creating with " + names.length + " columns is not supported yet."));
        }
        ProjectionIndexRegistry.installWildcard(resourceKey, names, decoded);
        return def.materialize();
      }
    }

    // Build over the most recent revision, persist compactly, install.
    final List<byte[]> leaves = new ArrayList<>();
    final ProjectionIndexBuilder builder;
    try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision);
         PathSummaryReader pathSummary = session.openPathSummary(revision)) {
      builder = new ProjectionIndexBuilder(def, pathSummary, leaves::add);
      builder.build(rtx);
    }
    try (JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      for (int i = 0; i < leaves.size(); i++) {
        storage.put(i, ProjectionIndexLeafCodec.encode(leaves.get(i)));
      }
      wtx.commit();
    }
    ProjectionIndexRegistry.installWildcard(resourceKey, names, leaves,
        builder.numericColumnNonIntegralFlags());
    return def.materialize();
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
      case "long", "integer", "int", "decimal", "double", "float", "number" -> Type.LON;
      case "boolean", "bool" -> Type.BOOL;
      case "string", "str" -> Type.STR;
      default -> throw new QueryException(new QNm(
          "Unknown projection column type '" + type + "' — use long, boolean, or string."));
    };
  }
}
