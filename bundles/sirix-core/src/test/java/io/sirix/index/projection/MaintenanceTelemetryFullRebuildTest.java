/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.JsonTestHelper;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code maintenanceTelemetry().fullRebuilds()} must be a MEASUREMENT, not the constant zero it
 * used to be.
 *
 * <p>
 * The maintenance contract is incremental — inserts, updates and deletes patch row groups in place
 * — and the counter is the witness: after a reset, a maintenance window that stayed incremental
 * reports zero, and one that quietly fell back to a complete {@code buildAndPersist} reports how
 * often. Both directions are pinned here, because a counter that can never move would make the zero
 * assertion vacuous — exactly the defect the hard-coded literal had.
 */
final class MaintenanceTelemetryFullRebuildTest {

  private static final int INDEX_NUMBER = 0;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    ProjectionIndexChangeListener.resetMaintenanceTelemetry();
  }

  @AfterEach
  void tearDown() {
    ProjectionIndexChangeListener.resetMaintenanceTelemetry();
    JsonTestHelper.deleteEverything();
  }

  @Test
  @DisplayName("a complete build ticks the counter; incremental maintenance leaves it untouched")
  void fullBuildTicksAndIncrementalMaintenanceDoesNot() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    final IndexDef definition = IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
        List.of(parse("/[]/value", PathParser.Type.JSON)), List.of(Type.LON), INDEX_NUMBER, IndexDef.DbType.JSON);

    // Shred a corpus and build the projection COMPLETELY once.
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      new JsonShredder.Builder(wtx, JsonShredder.createStringReader("[{\"value\":1},{\"value\":2},{\"value\":3}]"),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
    }
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      ProjectionIndexBuilder.buildAndPersist(definition, wtx.getPathSummary(), wtx, wtx.getStorageEngineWriter(),
          false);
      wtx.commit();
    }
    final long afterBuild = ProjectionIndexChangeListener.maintenanceTelemetry().fullRebuilds();
    assertTrue(afterBuild >= 1,
        "a complete buildAndPersist must tick fullRebuilds — a counter that cannot move makes every "
            + "zero assertion vacuous (it was hard-coded 0)");

    // The maintenance window a gate would measure: reset, then INCREMENTAL work only.
    ProjectionIndexChangeListener.resetMaintenanceTelemetry();
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      wtx.moveTo(1);
      wtx.moveToFirstChild(); // first record object
      wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"value\":4}"), JsonNodeTrx.Commit.NO);
      wtx.commit();
    }
    assertEquals(0L, ProjectionIndexChangeListener.maintenanceTelemetry().fullRebuilds(),
        "incremental insert maintenance quietly fell back to a complete rebuild");
    // Non-vacuity for the zero above: the counter is still ALIVE after the reset — a second
    // complete build ticks it from exactly this state. Without this, a reset that permanently
    // zeroed the counter (or a tick that stopped firing) would make the window assertion prove
    // nothing, which is precisely the hard-coded-zero defect this test exists to prevent.
    // A fresh index NUMBER: rebuilding a populated projection in place is itself forbidden by the
    // engine ("not virgin"), so liveness is probed with a new physical index.
    final IndexDef fresh = IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
        List.of(parse("/[]/value", PathParser.Type.JSON)), List.of(Type.LON), INDEX_NUMBER + 1, IndexDef.DbType.JSON);
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      ProjectionIndexBuilder.buildAndPersist(fresh, wtx.getPathSummary(), wtx, wtx.getStorageEngineWriter(), false);
      wtx.rollback();
    }
    assertEquals(1L, ProjectionIndexChangeListener.maintenanceTelemetry().fullRebuilds(),
        "the counter went dead after reset — the incremental-window zero above is vacuous");
  }
}
