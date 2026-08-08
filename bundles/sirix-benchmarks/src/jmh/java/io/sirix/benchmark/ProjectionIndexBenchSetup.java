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
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.index.projection.ProjectionIndexBuilder;
import io.sirix.index.IndexType;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.projection.ProjectionIndexMetadata;
import io.sirix.index.projection.ProjectionIndexRegistry;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Bench helper: build a (age, active, dept, city) projection index over the
 * current revision of {@code session}'s resource and publish it wildcard
 * in {@link ProjectionIndexRegistry}.
 *
 * <p>Lives in its own compilation unit so it can import
 * {@link io.brackit.query.util.path.Path} cleanly without colliding with
 * {@link java.nio.file.Path} that the bench-main uses extensively.
 *
 * <p>Interim — will be removed once IndexController / IndexListener
 * wiring for {@code IndexType.PROJECTION} lands (task #57).
 */
public final class ProjectionIndexBenchSetup {

  private static final String[] FIELD_NAMES = {"age", "active", "dept", "city"};

  private ProjectionIndexBenchSetup() {
  }

  /**
   * Build the projection index for {@code session}'s most recent revision
   * and install it wildcard-keyed so any {@code sourcePath} Brackit passes
   * to {@code executePredicateCount} will match. Returns the number of
   * leaves produced.
   */
  public static BuildResult installWildcard(final JsonResourceSession session) {
    return buildAndInstallWildcard(session).result();
  }

  /**
   * Like {@link #installWildcard}, additionally returning everything needed to
   * {@link #reinstall} the SAME built leaves later — replacing the registry handle drops
   * its lazily decoded column store, which is how cold-cache benchmarks re-cool the
   * projection tier without paying an index rebuild.
   */
  public static Installed buildAndInstallWildcard(final JsonResourceSession session) {
    final Path<QNm> rootPath = Path.parse("/[]", PathParser.Type.JSON);
    final Path<QNm> agePath = Path.parse("/[]/age", PathParser.Type.JSON);
    final Path<QNm> activePath = Path.parse("/[]/active", PathParser.Type.JSON);
    final Path<QNm> deptPath = Path.parse("/[]/dept", PathParser.Type.JSON);
    final Path<QNm> cityPath = Path.parse("/[]/city", PathParser.Type.JSON);
    final IndexDef def = IndexDefs.createProjectionIdxDef(
        rootPath,
        List.of(agePath, activePath, deptPath, cityPath),
        List.of(Type.LON, Type.BOOL, Type.STR, Type.STR),
        0,
        IndexDef.DbType.JSON);

    final List<byte[]> leaves = new ArrayList<>();
    final ProjectionIndexBuilder builder;
    final int revision = session.getMostRecentRevisionNumber();
    try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision);
         PathSummaryReader pathSummary = session.openPathSummary(revision)) {
      builder = new ProjectionIndexBuilder(def, pathSummary, leaves::add);
      builder.build(rtx);
    }

    long totalRows = 0L;
    for (final byte[] payload : leaves) {
      totalRows += ByteBuffer.wrap(payload, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    // Install with the builder's integrality flags — without them the
    // registry treats every numeric column as "unknown provenance" and
    // SirixVectorizedExecutor#tryProjectionAggregate declines to serve
    // sum/avg/min/max from the projection, silently falling back to the
    // full storage scan.
    final String resourceKey = session.getResourceConfig().getResource().toString();
    final boolean[] flags = builder.numericColumnNonIntegralFlags();
    ProjectionIndexRegistry.installWildcard(resourceKey, FIELD_NAMES, leaves, flags);
    return new Installed(resourceKey, leaves, flags, new BuildResult(leaves.size(), totalRows));
  }

  /** Re-install the captured leaves under a FRESH registry handle (cold decoded state). */
  public static void reinstall(final Installed installed) {
    ProjectionIndexRegistry.installWildcard(installed.resourceKey(), FIELD_NAMES,
        installed.leaves(), installed.numericNonIntegral());
  }

  /** Small immutable value carrier for diagnostic output. */
  public record BuildResult(int rowGroupCount, long totalRows) {
  }

  /**
   * PERSIST the projection as a CATALOGUED definition — the production discovery path
   * ({@code ProjectionIndexCatalog}), not the in-memory registry pool the other install here
   * uses. This is what a cold query really faces: the handle is rebuilt from the slot-0
   * metadata blob plus a row-group directory walk, and each queried column's segments are
   * read from storage on demand. Nothing is placed in the registry, and the catalog is
   * authoritative once a definition exists, so the query path cannot silently fall back to
   * RAM-resident leaves.
   *
   * @return the number of persisted row groups
   */
  /**
   * The JSONiq surface a user would actually call: {@code jn:create-projection-index} builds
   * the projection AND catalogues its definition, so {@code ProjectionIndexCatalog} — which is
   * authoritative once a definition exists — discovers it after re-open. Returns the query's
   * result string (the commit revision) for diagnostics.
   *
   * <p>Hand-persisting the row-group slots is NOT equivalent: it stores the data but leaves the
   * definition undeclared, and the catalog then finds nothing to serve.
   */
  public static String createProjectionIndexViaQuery(final SirixCompileChain chain,
      final SirixQueryContext ctx, final String docExpr) {
    final String q = "let $doc := " + docExpr + "\n"
        + "let $stats := jn:create-projection-index($doc, '/[]',\n"
        + "    ('/[]/age', '/[]/active', '/[]/dept', '/[]/city'),\n"
        + "    ('long', 'boolean', 'string', 'string'))\n"
        + "return {\"revision\": sdb:commit($doc)}";
    final var buf = IOUtils.createBuffer();
    try (var ser = new StringSerializer(buf)) {
      ser.serialize(new Query(chain, q).execute(ctx));
    }
    return buf.toString();
  }


  /**
   * The first catalogued PROJECTION definition's id for {@code session}'s revision, or -1.
   * Reads the resource's index definitions the same way the catalog does.
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
  public static int projectionRowGroupCount(final JsonResourceSession session, final int revision,
      final int defId) {
    try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      final ProjectionIndexMetadata metadata = ProjectionIndexMetadata.parse(
          ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), defId, 0L));
      return metadata == null ? 0 : metadata.rowGroupCount();
    }
  }

  /** Everything needed to {@link #reinstall} the built projection without rebuilding it. */
  public record Installed(String resourceKey, List<byte[]> leaves, boolean[] numericNonIntegral,
      BuildResult result) {
  }
}
