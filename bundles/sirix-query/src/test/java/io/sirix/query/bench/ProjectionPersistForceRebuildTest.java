package io.sirix.query.bench;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.index.projection.ProjectionIndexBuilder;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end regression for the projection tree's one-way initialization lifecycle.
 *
 * <ol>
 * <li>Shred a records array with the bench's six fields.</li>
 * <li>Build a 3-column projection ({@code age, active, dept}) via the real
 * {@link ProjectionIndexBuilder} into tree 0.</li>
 * <li>Prove that initializing tree 0 again with a different shape fails before mutation and leaves
 * every persisted row group byte-identical.</li>
 * <li>Build the 6-column projection into virgin tree 1 and prove that it hydrates
 * byte-identically.</li>
 * </ol>
 *
 * <p>
 * The fixture persists through the production builder, including locators, dictionaries, summaries,
 * Bloom/fence chunks and live slot-0 metadata strictly last. It therefore exercises the exact
 * boundary that once allowed a populated tree to be overwritten, not a leaves-only test double.
 */
public final class ProjectionPersistForceRebuildTest {

  /** ~59 projection leaves at the builder's 1024-rows-per-leaf granularity. */
  private static final int N = 60_000;
  private static final String DB = "proj-rebuild-db";
  private static final String RES = "records.jn";
  private static final int INDEX_NUMBER = 0;
  private java.nio.file.Path dbDir;

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-proj-rebuild-");
    final Random rng = new Random(42);
    final String[] depts = {"Engineering", "Sales", "Marketing", "Operations"};
    final String[] cities = {"New York City", "Los Angeles", "San Francisco", "Boston"};
    final StringBuilder sb = new StringBuilder(N * 110);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0)
        sb.append(',');
      sb.append("{\"id\":")
        .append(i)
        .append(",\"age\":")
        .append(18 + rng.nextInt(50))
        .append(",\"active\":")
        .append(rng.nextBoolean())
        .append(",\"dept\":\"")
        .append(depts[rng.nextInt(depts.length)])
        .append("\",\"city\":\"")
        .append(cities[rng.nextInt(cities.length)])
        .append("\",\"amount\":")
        .append(rng.nextInt(100_000))
        .append(",\"score\":")
        .append(rng.nextInt(1_000))
        .append('}');
    }
    sb.append(']');

    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb + "')").evaluate(ctx);
    }
  }

  @AfterEach
  void tearDown() {
    ProjectionIndexRegistry.clear();
    if (dbDir != null) {
      Databases.removeDatabase(dbDir.resolve(DB));
    }
  }

  @Test
  void populatedTreeRejectsReplacementAndFreshTreeHydratesByteIdentical() {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      try (final var database = Databases.openJsonDatabase(dbDir.resolve(DB));
          final JsonResourceSession session = database.beginResourceSession(RES)) {

        // ---- Phase 1: tree 0 receives its one permitted initialization. ----
        final Built threeCol = buildLeaves(session, threeColumnDef());
        final List<byte[]> threeColLeaves = threeCol.leaves();
        assertTrue(threeColLeaves.size() >= 32,
            "test needs enough leaves to split HOT pages, got " + threeColLeaves.size());
        persist(session, threeColumnDef());

        // ---- Phase 2: a replacement under tree 0 is rejected before touching it. ----
        final Built sixCol = buildLeaves(session, sixColumnDef(0));
        final List<byte[]> sixColLeaves = sixCol.leaves();
        assertEquals(threeColLeaves.size(), sixColLeaves.size(), "same rows, same rows-per-leaf → same leaf count");
        long grownLeaves = 0;
        for (int i = 0; i < sixColLeaves.size(); i++) {
          if (sixColLeaves.get(i).length > threeColLeaves.get(i).length) {
            grownLeaves++;
          }
        }
        assertTrue(grownLeaves > sixColLeaves.size() / 2,
            "6-column leaves must be larger than their 3-column predecessors (grown=" + grownLeaves + "/"
                + sixColLeaves.size() + ")");
        final IllegalStateException refusal =
            assertThrows(IllegalStateException.class, () -> persist(session, sixColumnDef(0)));
        assertTrue(refusal.getMessage().contains("virgin"), refusal::getMessage);
        try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          final List<byte[]> hydrated = ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(
              rtx.getStorageEngineReader(), INDEX_NUMBER, threeColLeaves.size());
          assertEquals(threeColLeaves.size(), hydrated.size());
          for (int i = 0; i < threeColLeaves.size(); i++) {
            assertArrayEquals(threeColLeaves.get(i), hydrated.get(i),
                "rejected replacement must preserve tree-0 leaf " + i);
          }
        }

        // ---- Phase 3: the new shape is initialized once in virgin tree 1. ----
        persist(session, sixColumnDef(1));
        try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          final List<byte[]> hydrated = ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(
              rtx.getStorageEngineReader(), 1, sixColLeaves.size());
          assertEquals(sixColLeaves.size(), hydrated.size());
          for (int i = 0; i < sixColLeaves.size(); i++) {
            assertArrayEquals(sixColLeaves.get(i), hydrated.get(i),
                "fresh tree-1 leaf " + i + " must hydrate byte-identically");
          }
        }
      }
    }
  }

  /** The byte-exact expected row groups built independently of persistence. */
  private record Built(List<byte[]> leaves) {
  }

  private static Built buildLeaves(final JsonResourceSession session, final IndexDef def) {
    final int revision = session.getMostRecentRevisionNumber();
    final List<byte[]> leaves = new ArrayList<>();
    try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision);
        PathSummaryReader pathSummary = session.openPathSummary(revision)) {
      final ProjectionIndexBuilder builder = new ProjectionIndexBuilder(def, pathSummary, leaves::add);
      builder.build(rtx);
      return new Built(leaves);
    }
  }

  /** Exercise the same canonical persistence boundary used by the benchmark and controllers. */
  private static void persist(final JsonResourceSession session, final IndexDef def) {
    try (JsonNodeTrx wtx = session.beginNodeTrx()) {
      ProjectionIndexBuilder.buildAndPersist(def, wtx.getPathSummary(), wtx, wtx.getStorageEngineWriter(), false);
      wtx.commit();
    }
  }

  private static IndexDef threeColumnDef() {
    return IndexDefs.createProjectionIdxDef(path("/[]"), List.of(path("/[]/age"), path("/[]/active"), path("/[]/dept")),
        List.of(Type.LON, Type.BOOL, Type.STR), INDEX_NUMBER, IndexDef.DbType.JSON);
  }

  private static IndexDef sixColumnDef(final int indexNumber) {
    return IndexDefs.createProjectionIdxDef(path("/[]"),
        List.of(path("/[]/age"), path("/[]/active"), path("/[]/dept"), path("/[]/city"), path("/[]/amount"),
            path("/[]/score")),
        List.of(Type.LON, Type.BOOL, Type.STR, Type.STR, Type.LON, Type.LON), indexNumber, IndexDef.DbType.JSON);
  }

  private static Path<QNm> path(final String p) {
    return Path.parse(p, PathParser.Type.JSON);
  }
}
