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
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.path.summary.PathNode;
import io.sirix.index.path.summary.PathStats;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexMetadata;
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
 * Function for creating a columnar <em>projection index</em> over a set of record fields — the
 * analytical fast path behind aggregate / filter / group-by queries. Supported signatures:
 * <ul>
 * <li><code>jn:create-projection-index($doc as json-item(), $rootPath as xs:string,
 * $fields as xs:string*, $types as xs:string*) as json-item()</code></li>
 * <li><code>jn:create-projection-index($doc as json-item(), $rootPath as xs:string,
 * $fields as xs:string*) as json-item()</code> — every field typed as {@code string}</li>
 * </ul>
 *
 * <p>
 * {@code $rootPath} selects the record set (e.g. {@code /[]} for a top-level array,
 * {@code /wrapper/records/[]} for a nested one); {@code $fields} are the projected column paths
 * relative to the document root; {@code $types} declare the per-column primitive shape —
 * {@code "long"} ({@code integer}/{@code int}), {@code "double"} ({@code float}), {@code "decimal"}
 * ({@code dec}), {@code "boolean"} ({@code bool}), {@code "string"} ({@code str}),
 * {@code "timestamp"} ({@code datetime}) or {@code "date"}, exactly as {@code mapType} accepts them
 * and its rejection message lists them. Double/decimal columns store exact doubles in an
 * order-preserving encoding; a decimal not exactly representable as a double marks the column
 * not-value-exact and value-exact consumers decline it (fail-closed). A declared temporal column
 * stores the epoch rather than the text and therefore requires every value to be exactly
 * {@code YYYY-MM-DDTHH:MM:SS} (timestamp) or {@code YYYY-MM-DD} (date);
 * {@code -Dsirix.projection.temporalKinds=false} makes such a column build and serve as an ordinary
 * string-dictionary column instead (see {@code ProjectionTemporalCodec}).
 *
 * <p>
 * Projection indexes work like the other index families ({@code jn:create-path-index} etc.): each
 * definition is catalogued in the resource's index set with its own id (numbered within the
 * PROJECTION type), a resource can carry SEVERAL projections side by side, and the analytical
 * executor discovers them through the revision-scoped catalog and page layer
 * ({@code ProjectionIndexCatalog}) — after re-opening a database, queries use persisted projections
 * WITHOUT re-running this function. Calling it with an already-catalogued shape verifies the
 * persisted columns and returns. A stale, missing, or unreadable store is never rebuilt in place:
 * the caller must drop and commit the unusable definition before creating a replacement, which gets
 * a previously unallocated projection tree. A different shape likewise creates an additional
 * projection. Shape comparison uses the parsed paths' canonical form, so spelling variants that
 * parse to the same path match.
 *
 * <p>
 * The projection is built over the passed document's revision — like the sibling functions, a
 * document bound to an older revision reverts the write transaction to that revision first — and
 * written in the one segmented projection format (see {@code ProjectionIndexColumnSegmentCodec})
 * together with a self-describing {@link ProjectionIndexMetadata} payload into the session's write
 * transaction: call {@code sdb:commit($doc)} afterwards to persist.
 *
 * <p>
 * <b>Experimental.</b> Once built, the projection is maintained INCREMENTALLY by the update
 * transactions that touch the record set — inserts, updates, deletes and moves rewrite only the
 * touched persistent units, so re-running this function is never needed to keep it current (see
 * {@code ProjectionIndexChangeListener} and {@code docs/PROJECTION_INDEXES.md}). A touched unit the
 * listener cannot attribute or read fails that transaction rather than degrading the index. The
 * resource must be created with a path summary. Column lookup is by the declared path relative to
 * the record set, so nested declarations that merely share a trailing name with another path are
 * accepted; see {@link #assertUnambiguousFieldNames} for the one shape still rejected.
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
      throw new QueryException(new QNm("jn:create-projection-index requires a resource created with a path summary "
          + "(buildPathSummary=true) — the projection builder resolves its paths through it."));
    }

    final String rootPathString = ((Str) args[1]).stringValue();
    final Path<QNm> rootPath = Path.parse(rootPathString, PathParser.Type.JSON);
    final List<Path<QNm>> fieldPaths = new ArrayList<>();
    final List<String> fieldNames = new ArrayList<>();
    final Set<String> seenNames = new HashSet<>();
    forEachString(args[2], value -> {
      // A path ending in an ARRAY step declares the field's ELEMENTS — a set column. Its column
      // name is the field step before the array layer, so `/[]/genres/[]` is the column `genres`
      // and collides with a scalar `/[]/genres` exactly as it should.
      final String canonical = Path.parse(value, PathParser.Type.JSON).toString();
      final String name = columnNameOf(canonical);
      if (name.isEmpty() || "[]".equals(name)) {
        throw new QueryException(
            new QNm("Projected field path '" + value + "' must end in an object-key step, or in an array "
                + "step naming the elements of an array-valued field (for example " + "'/[]/genres/[]')."));
      }
      if (!seenNames.add(name)) {
        throw new QueryException(new QNm("Duplicate projected field name '" + name + "' — column lookup is by trailing "
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
        throw new QueryException(
            new QNm("Field/type count mismatch: " + fieldPaths.size() + " fields vs " + fieldTypes.size() + " types."));
      }
    } else {
      // INFER from the path summary rather than defaulting to string. Defaulting made every
      // numeric column a string column, whereupon the extractor recorded each value as
      // present-but-UNREPRESENTABLE and the sparse-clean gate — correctly, fail-closed — refused to
      // serve it. The index then built, committed, reported success, and was never used by any
      // numeric predicate, with nothing said unless the query ran with -Dsirix.projDiag=true.
      // The summary already knows what these fields hold; asking it costs one open.
      try (final PathSummaryReader summary = session.openPathSummary(document.getTrx().getRevisionNumber())) {
        for (final Path<QNm> fieldPath : fieldPaths) {
          fieldTypes.add(inferFieldType(summary, fieldPath));
        }
      }
    }

    final int revision = document.getTrx().getRevisionNumber();

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
      // Probe through an open transaction's own writer so an index created earlier in this
      // transaction is visible. Otherwise use the committed, revision-scoped catalogue path. Both
      // are read-only: an unusable populated tree must never become an initializer target.
      final boolean usable = openWtx.isPresent()
          ? ProjectionIndexCatalog.loadUncommitted(openWtx.get().getStorageEngineWriter(), existingDef) != null
          : ProjectionIndexCatalog.load(session, revision, existingDef) != null;
      if (usable) {
        return existingDef.materialize();
      }
      throw new QueryException(new QNm("Projection index " + existingDef.getID()
          + " is catalogued but its store is missing, stale, or unreadable. It cannot be rebuilt in place. "
          + "Drop the definition, commit that change, and create the projection again; the replacement uses "
          + "a new, empty projection tree."));
    }

    // New projection — catalogued, built and persisted through the index
    // controller, like the other index families.
    final IndexDef def = buildViaController(session, document, rootPath, fieldPaths, fieldTypes, fieldNames);
    return def.materialize();
  }

  /**
   * Build, catalogue and persist the projection through the {@code IndexController} — the same
   * lifecycle entry point the sibling index-creation functions use. Mirrors
   * {@code jn:create-path-index} exactly: the session's write transaction is reused when open
   * (beginning a second would throw), begun otherwise; a document bound to an OLDER revision reverts
   * the transaction to that revision first; and nothing is committed here — the caller's
   * {@code sdb:commit($doc)} persists catalogue and payloads atomically. Query-side visibility comes
   * from the revision-scoped catalog after commit, so uncommitted or rolled-back builds are never
   * observable elsewhere.
   *
   * @return the definition that was built
   */
  private static IndexDef buildViaController(final JsonResourceSession session, final JsonDBItem document,
      final Path<QNm> rootPath, final List<Path<QNm>> fieldPaths, final List<Type> fieldTypes,
      final List<String> fieldNames) {
    // Validate BEFORE touching any write transaction: a rejected creation
    // must neither leak a freshly-begun wtx (single-writer permit!) nor
    // have already discarded a reused transaction's uncommitted changes via
    // revertTo. The document's revision is exactly the state the build will
    // run over after the revert, so the committed path summary of that
    // revision is the right validation view.
    try (PathSummaryReader pathSummary = session.openPathSummary(document.getTrx().getRevisionNumber())) {
      assertUnambiguousFieldNames(pathSummary, rootPath, fieldPaths, fieldNames);
    }
    final Optional<JsonNodeTrx> existingWtx = session.getNodeTrx();
    // Validation above cannot cover the BUILD itself: createIndexes walks the resource and writes
    // the columns, so it can still fail on I/O, a codec error or allocator pressure. A wtx WE began
    // must not survive that — it holds the resource's single writer permit, and a stranded one
    // blocks or fails every later write for the session's life. One the session already held stays
    // open: it is the caller's, and its uncommitted work is not ours to discard.
    final boolean wtxIsOurs = existingWtx.isEmpty();
    final JsonNodeTrx wtx = existingWtx.orElseGet(session::beginNodeTrx);
    boolean handedToCaller = false;
    try {
      if (document.getTrx().getRevisionNumber() < session.getMostRecentRevisionNumber()) {
        wtx.revertTo(document.getTrx().getRevisionNumber());
      }
      final JsonIndexController wtxController = session.getWtxIndexController(wtx.getRevisionNumber());
      final var storageEngineWriter = wtx.getStorageEngineWriter();
      final int indexNumber =
          storageEngineWriter.getProjectionIndexPage(storageEngineWriter.getActualRevisionRootPage())
                             .nextUnallocatedIndex();
      if (wtxController.getIndexes().getIndexDef(indexNumber, IndexType.PROJECTION) != null) {
        throw new IllegalStateException("Projection catalogue contains definition " + indexNumber
            + " without an initialized physical tree; refusing to reuse its id");
      }
      final IndexDef def =
          IndexDefs.createProjectionIdxDef(rootPath, fieldPaths, fieldTypes, indexNumber, IndexDef.DbType.JSON);
      wtxController.createIndexes(Set.of(def), wtx);
      // The built columns are uncommitted and the caller commits them, so from here a wtx we opened
      // is deliberately left open — closing it would throw the build away.
      handedToCaller = true;
      return def;
    } finally {
      if (wtxIsOurs && !handedToCaller) {
        wtx.close();
      }
    }
  }


  /**
   * Rejects declarations the executor could not tell apart at lookup time.
   *
   * <p>
   * Column lookup matches a projected column by its declared path <em>relative to the record set
   * root</em> — {@code ProjectionIndexRegistry.Handle#columnOf} compares the {@code fieldChains} the
   * catalog derives from the definition, so a query dereferencing {@code $r.address.age} produces the
   * token {@code address/age} and simply finds no column when only {@code /[]/age} is projected. A
   * trailing name recurring elsewhere under the record set is therefore NOT ambiguous, and rejecting
   * it would make whole corpora unprojectable for a hazard that no longer exists: the Bluesky corpus
   * of JSONBench carries {@code did} at six paths below {@code commit.record}, none of which any
   * query reads.
   *
   * <p>
   * One case still resolves by bare trailing name: a declared path that is not relativizable against
   * the declared root. {@code ProjectionIndexMetadata#relativeFieldChain} returns {@code null} for
   * it, the chain array carries {@code null} at that slot, and {@code columnOf} degrades to the
   * historical name comparison — so exactly those declarations still have to be name-unambiguous, and
   * exactly those are checked here.
   *
   * <p>
   * Two declared fields may still not share a trailing name; that is a separate rule, enforced by the
   * caller, because the column name is part of the projection's identity rather than of its lookup.
   */
  private static void assertUnambiguousFieldNames(final PathSummaryReader pathSummary, final Path<QNm> rootPath,
      final List<Path<QNm>> fieldPaths, final List<String> fieldNames) {
    final String declaredRoot = rootPath.toString();
    LongSet rootPcrs = null;
    for (int i = 0; i < fieldPaths.size(); i++) {
      if (ProjectionIndexMetadata.relativeFieldChain(declaredRoot, fieldPaths.get(i).toString()) != null) {
        // Path-qualified: the declared chain is what lookup compares, so it disambiguates itself.
        continue;
      }
      if (rootPcrs == null) {
        rootPcrs = pathSummary.getPCRsForPaths(Set.of(rootPath));
      }
      final String name = fieldNames.get(i);
      // A set column is declared at the ARRAY LAYER (`/[]/genres/[]`) while the name resolves to
      // the FIELD node (`/[]/genres`) — different path-summary nodes for one column. Both are
      // "own", or declaring the elements of an array would always report itself as a second
      // occurrence of its own field.
      final Set<Path<QNm>> ownPaths = new HashSet<>();
      ownPaths.add(fieldPaths.get(i));
      final Path<QNm> fieldOfSet = withoutTrailingArraySteps(fieldPaths.get(i));
      if (fieldOfSet != null) {
        ownPaths.add(fieldOfSet);
      }
      final LongSet ownPcrs = pathSummary.getPCRsForPaths(ownPaths);
      final Path<QNm> anyWithName = new Path<QNm>().descendantObjectField(new QNm(name));
      final LongIterator byName = pathSummary.getPCRsForPaths(Set.of(anyWithName)).iterator();
      while (byName.hasNext()) {
        final long pcr = byName.nextLong();
        if (!ownPcrs.contains(pcr) && isUnderAny(pathSummary, pcr, rootPcrs)) {
          throw new QueryException(
              new QNm("Projected field name '" + name + "' is ambiguous: it also occurs at a different "
                  + "path under the record set. Column lookup is by trailing field name, so the "
                  + "projection cannot distinguish the two occurrences."));
        }
      }
    }
  }

  /**
   * The declared path with its trailing array step(s) removed, or {@code null} when it had none.
   *
   * <p>
   * {@code /[]/genres/[]} → {@code /[]/genres}: the field whose elements the column holds.
   */
  private static Path<QNm> withoutTrailingArraySteps(final Path<QNm> path) {
    String text = path.toString();
    boolean trimmed = false;
    while (text.endsWith("/[]")) {
      text = text.substring(0, text.length() - 3);
      trimmed = true;
    }
    return trimmed && !text.isEmpty()
        ? Path.parse(text, PathParser.Type.JSON)
        : null;
  }

  /** Whether the path-summary node {@code pcr} has an ancestor in {@code rootPcrs}. */
  private static boolean isUnderAny(final PathSummaryReader pathSummary, final long pcr, final LongSet rootPcrs) {
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

  /**
   * Column name = the last OBJECT-KEY step, skipping any trailing array layers.
   *
   * <p>
   * {@code /[]/genres} and {@code /[]/genres/[]} both name the column {@code genres}: the first
   * declares the field, the second its elements. They are the same column from a query's point of
   * view, and naming them alike is what makes the duplicate check catch declaring both.
   */
  private static String columnNameOf(final String fieldPath) {
    String path = fieldPath;
    while (path.endsWith("/[]")) {
      path = path.substring(0, path.length() - 3);
    }
    return lastStep(path);
  }

  /** Column name = the final object-key step of the (canonical) field path. */
  private static String lastStep(final String fieldPath) {
    final int slash = fieldPath.lastIndexOf('/');
    return slash < 0
        ? fieldPath
        : fieldPath.substring(slash + 1);
  }

  /**
   * The type a field's OBSERVED values imply, from the path summary's statistics.
   *
   * <p>
   * Evidence, in the order it is decisive:
   * <ul>
   * <li>{@code minBytes} set — string values were recorded at this path.</li>
   * <li>{@code doubleTyped} — a floating-point value was seen, so a long column would truncate.</li>
   * <li>a numeric range was recorded ({@code min <= max}) — integral values only.</li>
   * </ul>
   *
   * <p>
   * Falls back to string when the path carries no statistics at all, which is the previous behaviour
   * and the safe direction: a string column holds anything, it just cannot be compared numerically.
   */
  private static Type inferFieldType(final PathSummaryReader summary, final Path<QNm> fieldPath) {
    try {
      final LongIterator pcrs = summary.getPCRsForPaths(Set.of(fieldPath)).iterator();
      while (pcrs.hasNext()) {
        if (!summary.moveTo(pcrs.nextLong())) {
          continue;
        }
        final PathNode node = summary.getPathNode();
        final PathStats stats = node == null
            ? null
            : node.getStats();
        if (stats == null) {
          continue;
        }
        if (stats.minBytes != null) {
          return Type.STR;
        }
        if (stats.doubleTyped) {
          return Type.DBL;
        }
        if (stats.min <= stats.max) {
          return Type.LON;
        }
      }
    } catch (final RuntimeException statsUnavailable) {
      // A resource without path statistics answers nothing here; string is the safe default.
    }
    return Type.STR;
  }

  private static Type mapType(final String type) {
    return switch (type.toLowerCase()) {
      case "long", "integer", "int" -> Type.LON;
      case "double", "float" -> Type.DBL;
      case "decimal", "dec" -> Type.DEC;
      case "boolean", "bool" -> Type.BOOL;
      case "string", "str" -> Type.STR;
      // Declared temporal columns: every value must be exactly YYYY-MM-DDTHH:MM:SS (timestamp) or
      // YYYY-MM-DD (date), and the column stores the epoch rather than the text.
      case "timestamp", "datetime" -> Type.DATI;
      case "date" -> Type.DATE;
      default -> throw new QueryException(new QNm("Unsupported projection column type '" + type
          + "' — use long (integer/int), double "
          + "(float), decimal (dec), boolean (bool), string (str), timestamp (datetime) or date. "
          + "Double/decimal columns " + "store exact doubles in an order-preserving encoding; decimals that are not "
          + "exactly representable as doubles mark the column not-value-exact and value-exact "
          + "consumers decline it (fail-closed)."));
    };
  }
}
