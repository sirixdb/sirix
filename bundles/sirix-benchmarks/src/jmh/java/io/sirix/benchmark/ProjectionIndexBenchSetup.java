/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.benchmark;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.io.IOUtils;
import io.brackit.query.util.path.PathParser;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.projection.ProjectionIndexMetadata;
import io.sirix.index.projection.ProjectionIndexRegistry;

import java.util.List;
import java.util.Set;

/**
 * Bench helper for a catalogued (age, active, dept, city) projection index over the current
 * revision of {@code session}'s resource.
 *
 * <p>
 * Lives in its own compilation unit so it can import {@link io.brackit.query.util.path.Path}
 * cleanly without colliding with {@link java.nio.file.Path} that the bench-main uses extensively.
 *
 * <p>
 * Creation uses the ordinary index-controller lifecycle and serving uses
 * {@link ProjectionIndexCatalog}. Benchmarks therefore exercise the same persisted segment-slot
 * store as production queries; there is no benchmark-only builder or registry injection.
 */
public final class ProjectionIndexBenchSetup {

  private ProjectionIndexBenchSetup() {}

  /**
   * Ensure the projection exists for {@code session}'s most recent revision and return its persisted
   * row-group and row counts. Existing definitions are loaded and validated, never rebuilt in place.
   */
  public static BuildResult ensureProjection(final JsonResourceSession session) {
    final Path<QNm> rootPath = Path.parse("/[]", PathParser.Type.JSON);
    final List<Path<QNm>> fieldPaths =
        List.of(Path.parse("/[]/age", PathParser.Type.JSON), Path.parse("/[]/active", PathParser.Type.JSON),
            Path.parse("/[]/dept", PathParser.Type.JSON), Path.parse("/[]/city", PathParser.Type.JSON));
    final List<Type> fieldTypes = List.of(Type.LON, Type.BOOL, Type.STR, Type.STR);
    return ensureProjection(session, rootPath, fieldPaths, fieldTypes);
  }

  /**
   * Shared catalogued creation path for benchmark-specific projection shapes.
   */
  static BuildResult ensureProjection(final JsonResourceSession session, final Path<QNm> rootPath,
      final List<Path<QNm>> fieldPaths, final List<Type> fieldTypes) {
    final int revision = session.getMostRecentRevisionNumber();
    final IndexDef existing = session.getRtxIndexController(revision)
                                     .getIndexes()
                                     .findProjectionIndex(rootPath, fieldPaths, fieldTypes)
                                     .orElse(null);
    final IndexDef def;
    if (existing != null) {
      def = existing;
    } else {
      if (!session.getResourceConfig().withPathSummary) {
        throw new IllegalStateException("Projection creation requires a path summary");
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final JsonIndexController controller = session.getWtxIndexController(wtx.getRevisionNumber());
        final var writer = wtx.getStorageEngineWriter();
        final int indexNumber =
            writer.getProjectionIndexPage(writer.getActualRevisionRootPage()).nextUnallocatedIndex();
        if (controller.getIndexes().getIndexDef(indexNumber, IndexType.PROJECTION) != null) {
          throw new IllegalStateException("Projection catalogue contains definition " + indexNumber
              + " without an initialized physical tree; refusing to reuse its id");
        }
        def = IndexDefs.createProjectionIdxDef(rootPath, fieldPaths, fieldTypes, indexNumber, IndexDef.DbType.JSON);
        controller.createIndexes(Set.of(def), wtx);
        wtx.commit();
      }
    }

    final int currentRevision = session.getMostRecentRevisionNumber();
    final ProjectionIndexRegistry.Handle handle = ProjectionIndexCatalog.load(session, currentRevision, def);
    if (handle == null) {
      throw new IllegalStateException("Catalogued projection " + def.getID()
          + " is missing, stale, malformed, or shape-incompatible; refusing an in-place rebuild");
    }
    final ProjectionColumnStore columnStore = handle.columnStoreOrNull();
    if (columnStore == null) {
      throw new IllegalStateException("Catalogued projection " + def.getID() + " did not load a segment-slot store");
    }
    long totalRows = 0L;
    for (int rowGroup = 0; rowGroup < handle.rowGroupCount(); rowGroup++) {
      totalRows += columnStore.rowCount(rowGroup);
    }
    return new BuildResult(handle.rowGroupCount(), totalRows);
  }

  /** Small immutable value carrier for diagnostic output. */
  public record BuildResult(int rowGroupCount, long totalRows) {
  }

  /**
   * PERSIST the projection as a CATALOGUED definition — the production discovery path
   * ({@code ProjectionIndexCatalog}), not the in-memory registry pool the other install here uses.
   * This is what a cold query really faces: the handle is rebuilt from the slot-0 metadata blob plus
   * a row-group directory walk, and each queried column's segments are read from storage on demand.
   * Nothing is placed in the registry, and the catalog is authoritative once a definition exists, so
   * the query path cannot silently fall back to RAM-resident leaves.
   *
   * @return the number of persisted row groups
   */
  /**
   * The JSONiq surface a user would actually call: {@code jn:create-projection-index} builds the
   * projection AND catalogues its definition, so {@code ProjectionIndexCatalog} — which is
   * authoritative once a definition exists — discovers it after re-open. Returns the query's result
   * string (the commit revision) for diagnostics.
   *
   * <p>
   * Hand-persisting the row-group slots is NOT equivalent: it stores the data but leaves the
   * definition undeclared, and the catalog then finds nothing to serve.
   */
  public static String createProjectionIndexViaQuery(final SirixCompileChain chain, final SirixQueryContext ctx,
      final String docExpr) {
    final String q = "let $doc := " + docExpr + "\n" + "let $stats := jn:create-projection-index($doc, '/[]',\n"
        + "    ('/[]/age', '/[]/active', '/[]/dept', '/[]/city'),\n" + "    ('long', 'boolean', 'string', 'string'))\n"
        + "return {\"revision\": sdb:commit($doc)}";
    final var buf = IOUtils.createBuffer();
    try (var ser = new StringSerializer(buf)) {
      ser.serialize(new Query(chain, q).execute(ctx));
    }
    return buf.toString();
  }


  /**
   * The first catalogued PROJECTION definition's id for {@code session}'s revision, or -1. Reads the
   * resource's index definitions the same way the catalog does.
   */
  public static int firstProjectionDefId(final JsonResourceSession session, final int revision) {
    try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      final var controller = session.getRtxIndexController(revision);
      for (final IndexDef def : controller.getIndexes().getIndexDefs()) {
        if (def.getType() == IndexType.PROJECTION) {
          return def.getID();
        }
      }
    }
    return -1;
  }

  /** Row-group count recorded in a persisted projection's slot-0 metadata blob, or 0. */
  public static int projectionRowGroupCount(final JsonResourceSession session, final int revision, final int defId) {
    try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      final ProjectionIndexMetadata metadata =
          ProjectionIndexMetadata.parse(ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), defId, 0L));
      return metadata == null
          ? 0
          : metadata.rowGroupCount();
    }
  }

}
