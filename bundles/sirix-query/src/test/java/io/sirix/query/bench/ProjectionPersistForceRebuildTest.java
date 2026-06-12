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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end regression for the projection force-rebuild persist failure
 * ({@code SirixIOException: Projection HOT chunk insert failed after split}):
 * mirrors {@code ScaleBenchProjectionSetup.installWildcard} with
 * {@code -Dsirix.projection.forceRebuild=true} over a database that already
 * carries SMALLER persisted leaves.
 *
 * <ol>
 *   <li>Shred a records array with the bench's six fields.</li>
 *   <li>Build a 3-column projection ({@code age, active, dept}) via the real
 *       {@link ProjectionIndexBuilder} and persist its leaves through
 *       {@link ProjectionIndexHOTStorage} — the historical on-disk state.</li>
 *   <li>Force-rebuild with all SIX columns over the same revision and persist
 *       the (strictly larger) leaves at the SAME slot ids — the failing
 *       scenario.</li>
 *   <li>Hydrate via {@link ProjectionIndexHOTStorage#readAll} and assert every
 *       leaf comes back byte-identical to the freshly built 6-column leaves;
 *       then drive the actual {@code installWildcard} fast path on top.</li>
 * </ol>
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
    final String[] depts = { "Engineering", "Sales", "Marketing", "Operations" };
    final String[] cities = { "New York City", "Los Angeles", "San Francisco", "Boston" };
    final StringBuilder sb = new StringBuilder(N * 110);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) sb.append(',');
      sb.append("{\"id\":").append(i)
        .append(",\"age\":").append(18 + rng.nextInt(50))
        .append(",\"active\":").append(rng.nextBoolean())
        .append(",\"dept\":\"").append(depts[rng.nextInt(depts.length)])
        .append("\",\"city\":\"").append(cities[rng.nextInt(cities.length)])
        .append("\",\"amount\":").append(rng.nextInt(100_000))
        .append(",\"score\":").append(rng.nextInt(1_000))
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
  void forceRebuildWithMoreColumns_persistsAndHydratesByteIdentical() {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      try (final var database = Databases.openJsonDatabase(dbDir.resolve(DB));
           final JsonResourceSession session = database.beginResourceSession(RES)) {

        // ---- Phase 1: historical state — 3-column projection persisted. ----
        final List<byte[]> threeColLeaves = buildLeaves(session, threeColumnDef());
        assertTrue(threeColLeaves.size() >= 32,
            "test needs enough leaves to split HOT pages, got " + threeColLeaves.size());
        persist(session, threeColLeaves);

        // ---- Phase 2: force-rebuild with all six columns (larger leaves). ----
        final List<byte[]> sixColLeaves = buildLeaves(session, sixColumnDef());
        assertEquals(threeColLeaves.size(), sixColLeaves.size(),
            "same rows, same rows-per-leaf → same leaf count");
        long grownLeaves = 0;
        for (int i = 0; i < sixColLeaves.size(); i++) {
          if (sixColLeaves.get(i).length > threeColLeaves.get(i).length) {
            grownLeaves++;
          }
        }
        assertTrue(grownLeaves > sixColLeaves.size() / 2,
            "6-column leaves must be larger than their 3-column predecessors (grown="
                + grownLeaves + "/" + sixColLeaves.size() + ")");
        persist(session, sixColLeaves);

        // ---- Phase 3: hydrate — every leaf byte-identical to the rebuild. ----
        try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          final List<byte[]> hydrated =
              ProjectionIndexHOTStorage.readAll(rtx.getStorageEngineReader(), INDEX_NUMBER);
          assertEquals(sixColLeaves.size(), hydrated.size(),
              "every persisted leaf must hydrate after the grow-rebuild");
          for (int i = 0; i < sixColLeaves.size(); i++) {
            assertArrayEquals(sixColLeaves.get(i), hydrated.get(i),
                "leaf " + i + " must be byte-identical after force-rebuild persist");
          }
        }

        // ---- Phase 4: the real bench entry point takes the fast (hydrate) path. ----
        final int installed = ScaleBenchProjectionSetup.installWildcard(session);
        assertEquals(sixColLeaves.size(), installed,
            "installWildcard must hydrate the persisted (rebuilt) projection");
      }
    }
  }

  private static List<byte[]> buildLeaves(final JsonResourceSession session, final IndexDef def) {
    final int revision = session.getMostRecentRevisionNumber();
    final List<byte[]> leaves = new ArrayList<>();
    try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision);
         PathSummaryReader pathSummary = session.openPathSummary(revision)) {
      final ProjectionIndexBuilder builder = new ProjectionIndexBuilder(def, pathSummary, leaves::add);
      builder.build(rtx);
    }
    return leaves;
  }

  /** Mirrors the persist loop in {@code ScaleBenchProjectionSetup.installWildcard}. */
  private static void persist(final JsonResourceSession session, final List<byte[]> leaves) {
    try (JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      for (int i = 0; i < leaves.size(); i++) {
        storage.put(i, leaves.get(i));
      }
      wtx.commit();
    }
  }

  private static IndexDef threeColumnDef() {
    return IndexDefs.createProjectionIdxDef(
        path("/[]"),
        List.of(path("/[]/age"), path("/[]/active"), path("/[]/dept")),
        List.of(Type.LON, Type.BOOL, Type.STR),
        INDEX_NUMBER,
        IndexDef.DbType.JSON);
  }

  private static IndexDef sixColumnDef() {
    return IndexDefs.createProjectionIdxDef(
        path("/[]"),
        List.of(path("/[]/age"), path("/[]/active"), path("/[]/dept"),
            path("/[]/city"), path("/[]/amount"), path("/[]/score")),
        List.of(Type.LON, Type.BOOL, Type.STR, Type.STR, Type.LON, Type.LON),
        INDEX_NUMBER,
        IndexDef.DbType.JSON);
  }

  private static Path<QNm> path(final String p) {
    return Path.parse(p, PathParser.Type.JSON);
  }
}
