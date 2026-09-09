/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.bench;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;

import java.util.List;
import java.util.Set;

/**
 * Ensures that ScaleBench's projection exists through the ordinary catalogued index lifecycle.
 * There is no benchmark-only builder, persisted-byte rewrite, or registry injection: creation goes
 * through {@link JsonIndexController#createIndexes}, and serving goes through
 * {@link ProjectionIndexCatalog} exactly like a production query.
 */
final class ScaleBenchProjectionSetup {

  private static final Path<QNm> ROOT_PATH = Path.parse("/[]", PathParser.Type.JSON);
  private static final List<Path<QNm>> FIELD_PATHS =
      List.of(Path.parse("/[]/age", PathParser.Type.JSON), Path.parse("/[]/active", PathParser.Type.JSON),
          Path.parse("/[]/dept", PathParser.Type.JSON), Path.parse("/[]/city", PathParser.Type.JSON),
          Path.parse("/[]/amount", PathParser.Type.JSON), Path.parse("/[]/score", PathParser.Type.JSON));
  private static final List<Type> FIELD_TYPES = List.of(Type.LON, Type.BOOL, Type.STR, Type.STR, Type.LON, Type.LON);

  private ScaleBenchProjectionSetup() {}

  static int ensureProjection(final JsonResourceSession session) {
    final int revision = session.getMostRecentRevisionNumber();
    final IndexDef existing = session.getRtxIndexController(revision)
                                     .getIndexes()
                                     .findProjectionIndex(ROOT_PATH, FIELD_PATHS, FIELD_TYPES)
                                     .orElse(null);
    if (existing != null) {
      return requireUsable(session, revision, existing).rowGroupCount();
    }
    return createAndLoad(session);
  }

  /** Create once in a virgin physical tree, catalog and payloads in the same transaction. */
  private static int createAndLoad(final JsonResourceSession session) {
    if (!session.getResourceConfig().withPathSummary) {
      throw new IllegalStateException("ScaleBench projection creation requires a path summary. Re-shred with "
          + "buildPathSummary=true and create the projection during ingestion.");
    }

    final IndexDef def;
    try (JsonNodeTrx wtx = session.beginNodeTrx()) {
      final JsonIndexController controller = session.getWtxIndexController(wtx.getRevisionNumber());
      final var writer = wtx.getStorageEngineWriter();
      final int indexNumber = writer.getProjectionIndexPage(writer.getActualRevisionRootPage()).nextUnallocatedIndex();
      if (controller.getIndexes().getIndexDef(indexNumber, IndexType.PROJECTION) != null) {
        throw new IllegalStateException("Projection catalogue contains definition " + indexNumber
            + " without an initialized physical tree; refusing to reuse its id");
      }
      def = IndexDefs.createProjectionIdxDef(ROOT_PATH, FIELD_PATHS, FIELD_TYPES, indexNumber, IndexDef.DbType.JSON);
      controller.createIndexes(Set.of(def), wtx);
      wtx.commit();
    }

    final int revision = session.getMostRecentRevisionNumber();
    final ProjectionIndexRegistry.Handle handle = requireUsable(session, revision, def);
    System.out.printf("# Projection persisted through catalog: %,d row groups, projected resident weight %,d bytes%n",
        handle.rowGroupCount(), handle.projectedWeightBytes());
    return handle.rowGroupCount();
  }

  private static ProjectionIndexRegistry.Handle requireUsable(final JsonResourceSession session, final int revision,
      final IndexDef def) {
    final ProjectionIndexRegistry.Handle handle = ProjectionIndexCatalog.load(session, revision, def);
    if (handle == null) {
      throw new IllegalStateException("Catalogued projection " + def.getID()
          + " is missing, stale, malformed, or shape-incompatible. It cannot be rebuilt in place; drop the "
          + "definition, commit, and create it again under a fresh physical id.");
    }
    return handle;
  }
}
