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
 * Builds the projection index the bulk PostgreSQL comparison
 * ({@code docs/COMPARISON_POSTGRES_BULK.md}) needs: a {@code (year, title)} projection over the
 * movie corpus, published wildcard so any {@code sourcePath} Brackit passes will match.
 *
 * <p>Sibling of {@link ProjectionIndexBenchSetup}, which covers the synthetic
 * {@code (age, active, dept, city)} record shape. Both live in their own compilation unit so they
 * can import Brackit's {@link Path} without colliding with {@link java.nio.file.Path}, which the
 * bench mains use throughout.
 *
 * <p>Only the two columns the measured queries touch are declared. Every extra column costs a full
 * pass of build time, and declaring columns the benchmark never reads would inflate the build cost
 * this comparison reports.
 */
public final class MoviesProjectionSetup {

  private static final String[] FIELD_NAMES = {"year", "title"};

  private MoviesProjectionSetup() {
  }

  /**
   * Build the {@code (year, title)} projection for {@code session}'s most recent revision and
   * install it wildcard-keyed.
   *
   * @param session the resource session to project
   * @return leaf and row counts, for diagnostic output
   */
  public static ProjectionIndexBenchSetup.BuildResult installWildcard(final JsonResourceSession session) {
    final Path<QNm> rootPath = Path.parse("/[]", PathParser.Type.JSON);
    final Path<QNm> yearPath = Path.parse("/[]/year", PathParser.Type.JSON);
    final Path<QNm> titlePath = Path.parse("/[]/title", PathParser.Type.JSON);

    final IndexDef def = IndexDefs.createProjectionIdxDef(rootPath,
                                                          List.of(yearPath, titlePath),
                                                          List.of(Type.LON, Type.STR),
                                                          0,
                                                          IndexDef.DbType.JSON);

    final List<byte[]> leaves = new ArrayList<>();
    final ProjectionIndexBuilder builder;
    final int revision = session.getMostRecentRevisionNumber();
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision);
         final PathSummaryReader pathSummary = session.openPathSummary(revision)) {
      builder = new ProjectionIndexBuilder(def, pathSummary, leaves::add);
      builder.build(rtx);
    }

    long totalRows = 0L;
    for (final byte[] payload : leaves) {
      totalRows += ByteBuffer.wrap(payload, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    // The integrality flags must be passed through: without them the registry treats every numeric
    // column as unknown provenance, and SirixVectorizedExecutor#tryProjectionAggregate declines to
    // serve sum/avg/min/max from the projection - silently falling back to the full storage scan,
    // which is exactly the path this run exists to avoid measuring by accident.
    final String resourceKey = session.getResourceConfig().getResource().toString();
    ProjectionIndexRegistry.installWildcard(resourceKey, FIELD_NAMES, leaves,
                                            builder.numericColumnNonIntegralFlags());
    return new ProjectionIndexBenchSetup.BuildResult(leaves.size(), totalRows);
  }
}
