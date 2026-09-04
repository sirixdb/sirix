package io.sirix.query.scan;

import io.sirix.access.Databases;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import io.sirix.index.projection.ProjectionTemporalCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * {@code -Dsirix.projection.temporalKinds=false}: a declared timestamp/date column builds and
 * serves as an ordinary per-leaf string column, exactly as it did before the temporal kinds
 * existed.
 *
 * <p>
 * This is the cheapest mutation witness the lever has — with the switch off, nothing in the
 * temporal path runs at all — and it is also the operational promise: a deployment that hits a
 * defect can turn the lever off and REBUILD, and every query keeps its answer. Both halves are
 * asserted here: the built column kind is {@code STRING_DICT}, and the same queries the
 * differential test runs against temporal columns still agree with the interpreter.
 */
final class TemporalColumnKillSwitchTest {
  private static final String DB = "temporal-col-db";
  private static final String RES = "records.jn";
  private static final int N = 2_000;
  private static final String DOC = "jn:doc('" + DB + "','" + RES + "')[]";

  private Path dbDir;
  private boolean previousSetting;

  @BeforeEach
  void setUp() throws Exception {
    // Off BEFORE the build: the switch is read when a declared type is mapped to a column kind.
    previousSetting = ProjectionTemporalCodec.setTemporalKindsEnabledForTesting(false);
    dbDir = Files.createTempDirectory("sirix-temporal-killswitch-");
    TemporalColumnDifferentialTest.createFixture(dbDir, TemporalColumnDifferentialTest.json(N));
  }

  @AfterEach
  void tearDown() {
    ProjectionTemporalCodec.setTemporalKindsEnabledForTesting(previousSetting);
    SirixVectorizedExecutor.STRICT_SERVING = false;
    io.brackit.query.compiler.translator.SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (dbDir != null) {
      Databases.removeDatabase(dbDir.resolve(DB));
    }
  }

  @Test
  @DisplayName("with the switch off the declared temporal columns are STRING_DICT and every answer holds")
  void killSwitchBuildsStringColumnsAndKeepsTheAnswers() throws Exception {
    try (var db = Databases.openJsonDatabase(dbDir.resolve(DB)); var session = db.beginResourceSession(RES)) {
      final ProjectionIndexRegistry.Handle handle =
          ProjectionIndexCatalog.lookupCovering(session, session.getResourceConfig().getResource().toString(),
              session.getMostRecentRevisionNumber(), new String[] {"[]"}, new String[] {"t", "d", "u", "v"});
      assertNotNull(handle, "the projection must be loadable with the switch off");
      assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT, handle.columnKindOf(handle.columnOf("t")),
          "the switch must build the timestamp column as a per-leaf string column");
      assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT, handle.columnKindOf(handle.columnOf("d")),
          "the switch must build the date column as a per-leaf string column");
    }
    // The switch is a BUILD decision; serving reads the store's kinds, so the temporal paths are
    // unreachable for this store whatever the flag says from here on.
    ProjectionTemporalCodec.setTemporalKindsEnabledForTesting(previousSetting);
    final byte[] scratch = new byte[ProjectionTemporalCodec.MAX_TEXT_LENGTH];
    final int length = ProjectionTemporalCodec.formatTimestamp(TemporalColumnDifferentialTest.epochOf(777), scratch, 0);
    final String full = new String(scratch, 0, length, java.nio.charset.StandardCharsets.UTF_8);
    final String day = full.substring(0, 10);
    for (final String query : List.of("for $h in " + DOC + " where $h.v ge 0 return $h.t",
        "min(for $h in " + DOC + " return $h.d)", "max(for $h in " + DOC + " return $h.t)",
        "subsequence(for $h in " + DOC + " order by $h.t return $h.t, 1, 15)",
        "for $h in " + DOC + " let $k := $h.d group by $k let $c := count($h) order by $k ascending "
            + "return {\"k\": $k, \"c\": $c}",
        "for $h in " + DOC + " let $k := substring($h.t, 1, 16) group by $k let $c := count($h) "
            + "order by $k ascending return {\"k\": $k, \"c\": $c}",
        "count(for $h in " + DOC + " where $h.t eq '" + full + "' return $h)",
        "count(for $h in " + DOC + " where $h.t ge '" + day + "' return $h)",
        "count(for $h in " + DOC + " where $h.d eq '" + day + "' return $h)")) {
      assertEquals(TemporalColumnDifferentialTest.run(dbDir, query, false),
          TemporalColumnDifferentialTest.run(dbDir, query, true), "the switched-off store diverges for: " + query);
    }
  }
}
