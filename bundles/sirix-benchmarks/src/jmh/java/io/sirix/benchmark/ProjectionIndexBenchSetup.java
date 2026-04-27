/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.benchmark;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.index.projection.ProjectionIndexBuilder;
import io.sirix.index.projection.ProjectionIndexRegistry;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Bench helper: build a (age, active, dept) projection index over the
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

  private static final String[] FIELD_NAMES = {"age", "active", "dept"};

  private ProjectionIndexBenchSetup() {
  }

  /**
   * Build the projection index for {@code session}'s most recent revision
   * and install it wildcard-keyed so any {@code sourcePath} Brackit passes
   * to {@code executePredicateCount} will match. Returns the number of
   * leaves produced.
   */
  public static BuildResult installWildcard(final JsonResourceSession session) {
    final Path<QNm> rootPath = Path.parse("/[]", PathParser.Type.JSON);
    final Path<QNm> agePath = Path.parse("/[]/age", PathParser.Type.JSON);
    final Path<QNm> activePath = Path.parse("/[]/active", PathParser.Type.JSON);
    final Path<QNm> deptPath = Path.parse("/[]/dept", PathParser.Type.JSON);
    final IndexDef def = IndexDefs.createProjectionIdxDef(
        rootPath,
        List.of(agePath, activePath, deptPath),
        List.of(Type.LON, Type.BOOL, Type.STR),
        0,
        IndexDef.DbType.JSON);

    final List<byte[]> leaves = new ArrayList<>();
    final int revision = session.getMostRecentRevisionNumber();
    try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision);
         PathSummaryReader pathSummary = session.openPathSummary(revision)) {
      new ProjectionIndexBuilder(def, pathSummary, leaves::add).build(rtx);
    }

    long totalRows = 0L;
    for (final byte[] payload : leaves) {
      totalRows += ByteBuffer.wrap(payload, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    final String resourceKey = session.getResourceConfig().getResource().toString();
    ProjectionIndexRegistry.installWildcard(resourceKey, FIELD_NAMES, leaves);
    return new BuildResult(leaves.size(), totalRows);
  }

  /** Small immutable value carrier for diagnostic output. */
  public record BuildResult(int leafCount, long totalRows) {
  }
}
