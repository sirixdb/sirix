/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Several projections of one shape may coexist, sorted or not, and the shape-only finder returns the
 * first catalogued one. That choice must survive a reopen: the catalogue is restored in the order
 * it was persisted, never in an order derived from per-process hash codes.
 */
final class IndexesCatalogueOrderTest {

  private static final String RESOURCE = "catalogue-order";
  private static final List<Type> TYPES = List.of(Type.STR, Type.LON);

  @TempDir
  Path temporaryDirectory;

  @Test
  void theFirstCataloguedProjectionOfAShapeIsStableAcrossReopens() {
    final Path databasePath = temporaryDirectory.resolve("catalogue-order");
    final var root = parse("/[]", PathParser.Type.JSON);
    final var fields = List.of(parse("/[]/kind", PathParser.Type.JSON), parse("/[]/time", PathParser.Type.JSON));
    final IndexDef[] catalogued = {IndexDefs.createProjectionIdxDef(root, fields, TYPES, 0, IndexDef.DbType.JSON),
        IndexDefs.createProjectionIdxDef(root, fields, TYPES, 1, IndexDef.DbType.JSON,
            new ProjectionSortedSpec(List.of(0, 1))),
        IndexDefs.createProjectionIdxDef(root, fields, TYPES, 2, IndexDef.DbType.JSON,
            new ProjectionSortedSpec(List.of(1))),
        IndexDefs.createProjectionIdxDef(root, fields, TYPES, 3, IndexDef.DbType.JSON,
            new ProjectionSortedSpec(List.of(0)))};
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build()));
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(
            JsonShredder.createStringReader("[{\"kind\":\"a\",\"time\":1},{\"kind\":\"b\",\"time\":2}]"),
            JsonNodeTrx.Commit.NO);
        final JsonIndexController controller = session.getWtxIndexController(writer.getRevisionNumber());
        for (final IndexDef definition : catalogued) {
          controller.createIndexes(Set.of(definition), writer);
        }
        writer.commit();
      }
    }
    for (int first = 0; first < catalogued.length; first++) {
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
          JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        final Indexes indexes =
            session.<JsonIndexController>getRtxIndexController(session.getMostRecentRevisionNumber()).getIndexes();
        assertEquals(catalogued[first].getID(), indexes.findProjectionIndex(root, fields, TYPES).orElseThrow().getID(),
            "after reopen " + first);
        assertEquals(catalogued[first].getID(), indexes.findProjectionIndex(root, fields, null).orElseThrow().getID(),
            "after reopen " + first);
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final JsonIndexController controller = session.getWtxIndexController(writer.getRevisionNumber());
          controller.dropIndexes(Set.of(controller.getIndexes().getIndexDef(first, IndexType.PROJECTION)), writer);
          writer.commit();
        }
      }
    }
  }
}
