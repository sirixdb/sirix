package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.atomic.Int64;
import io.brackit.query.jdm.Sequence;
import io.sirix.JsonTestHelper;
import io.sirix.access.Databases;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexLeafPage;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.node.NodeKind;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.scan.SirixVectorizedExecutor;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Stress/soak coverage for the projection index's incremental-maintenance path — the
 * production-readiness gaps the point tests don't reach:
 *
 * <ul>
 *   <li><b>Long randomized update soak</b> — hundreds of attributable changes (in-place
 *       updates, deletes, tail/middle inserts, invisible {@code {}}/null rows) across many
 *       commits, with a deterministic oracle checked after EVERY commit; the projection must
 *       keep serving (never silently tombstone) and stay row-exact.</li>
 *   <li><b>Vectorized ≡ interpreted differential</b> — a query battery (aggregates,
 *       predicate count, count-distinct, per-group counts) run through a session-bound chain
 *       and through the plain chain, byte-identical output plus a servedCount proof.</li>
 *   <li><b>Time travel</b> — every soak revision re-checked through a revision-pinned
 *       executor after the fact (revision-scoped catalog serving).</li>
 *   <li><b>Process-restart discovery</b> — registry wiped and the database re-opened cold;
 *       persisted leaves must serve the final state.</li>
 *   <li><b>Concurrent readers vs. writer</b> — REST-shaped concurrency (one session per
 *       reader) with revision-pinned checks racing ongoing maintenance commits.</li>
 *   <li><b>Tombstone → rebuild cycles</b> — repeated unattributable changes (subtree moves
 *       out of the record set), each followed by a same-definition re-create that must serve
 *       again.</li>
 * </ul>
 */
public final class ProjectionIndexStressTest extends AbstractJsonTest {

  private static final String SOAK_DB = "json-stress-soak";
  private static final String CONC_DB = "json-stress-conc";
  private static final String CYCLE_DB = "json-stress-cycle";
  private static final String SOAK_RESOURCE = "soak.jn";
  private static final String[] SOURCE_PATH = { "[]" };
  private static final String[] DEPTS = { "Eng", "Sales", "HR", "Ops" };

  private static final int INITIAL_RECORDS = 2200;
  private static final int COMMITS = 15;
  private static final int OPS_PER_COMMIT = 25;
  private static final int MIN_ROWS = 500;
  private static final long SEED = 0xC0FFEE_5EEDL;

  @BeforeEach
  public void clearProjectionStateBefore() {
    ProjectionIndexRegistry.clear();
    removeStressDatabases();
  }

  @AfterEach
  public void clearProjectionStateAfter() {
    ProjectionIndexRegistry.clear();
    removeStressDatabases();
  }

  /**
   * The stress suite uses DEDICATED database names: the shared {@code json-path1} store
   * lives under {@code java.io.tmpdir} and other modules' test helpers delete it — on CI
   * {@code :sirix-kotlin-api:test} runs IN PARALLEL with this module and would wipe the
   * store mid-soak. The helper-managed paths (PATH1/PATH2) are untouched here, so this
   * class cleans its own databases up.
   */
  private static void removeStressDatabases() {
    for (final String db : new String[] { SOAK_DB, CONC_DB, CYCLE_DB }) {
      Databases.removeDatabase(
          Path.of(JsonTestHelper.PATHS.PATH1.getFile().getParent().toString(), db));
    }
  }

  /**
   * Oracle row mirroring one child of the top-level array. All-null fields represent
   * field-less rows ({@code {}} records and JSON null elements alike — the aggregates
   * observe both identically as "no field present").
   */
  private static final class Row {
    Long age;
    Boolean active;
    String dept;

    static Row record(final Long age, final Boolean active, final String dept) {
      final Row row = new Row();
      row.age = age;
      row.active = active;
      row.dept = dept;
      return row;
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Long randomized soak: per-commit oracle checks, differential battery, time travel, reopen.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void randomizedIncrementalMaintenanceSoak() throws IOException {
    final Random rnd = new Random(SEED);
    final List<Row> oracle = new ArrayList<>(INITIAL_RECORDS + COMMITS * OPS_PER_COMMIT);
    storeInitialRecordSet(rnd, oracle);
    createProjection();

    final Map<Integer, long[]> expectedByRevision = new LinkedHashMap<>(COMMITS * 2);
    try (final Database<JsonResourceSession> database = openStressDatabase(SOAK_DB);
         final JsonResourceSession session = database.beginResourceSession(SOAK_RESOURCE)) {
      expectedByRevision.put(session.getMostRecentRevisionNumber(), oracleStats(oracle));

      int cumulativeMutations = 0;
      for (int commit = 0; commit < COMMITS; commit++) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          for (int op = 0; op < OPS_PER_COMMIT; op++) {
            cumulativeMutations += applyRandomOp(wtx, rnd, oracle);
          }
          wtx.commit();
        }
        final int revision = session.getMostRecentRevisionNumber();
        final long[] stats = oracleStats(oracle);
        expectedByRevision.put(revision, stats);
        verifyAgainstOracle(session, revision, stats, oracle.size(), cumulativeMutations);
      }

      // Time travel: EVERY soak revision must still be served exactly as recorded.
      for (final Map.Entry<Integer, long[]> entry : expectedByRevision.entrySet()) {
        verifyAggregates(session, entry.getKey(), entry.getValue());
      }
    }

    // Differential battery at the final state (catalog-discovered, in-process).
    runDifferentialBattery();

    // Simulated process restart: no in-memory registry/catalog state, cold re-open — the
    // persisted, incrementally patched leaves must serve the final state.
    ProjectionIndexRegistry.clear();
    final long[] finalStats = oracleStats(oracle);
    try (final Database<JsonResourceSession> database = openStressDatabase(SOAK_DB);
         final JsonResourceSession session = database.beginResourceSession(SOAK_RESOURCE)) {
      verifyAggregates(session, session.getMostRecentRevisionNumber(), finalStats);
      Assertions.assertTrue(servedRowCount(session, session.getMostRecentRevisionNumber()) >= oracle.size(),
          "cold re-open must serve at least the live rows");
    }
    runDifferentialBattery();
  }

  /**
   * Apply one random, incrementally-attributable operation to both the wtx and the oracle.
   *
   * @return {@code 1} when the operation may orphan a physical leaf slot (delete or
   *         in-place update — maintenance may append the re-extracted row instead of
   *         compacting), else {@code 0}
   */
  private static int applyRandomOp(final JsonNodeTrx wtx, final Random rnd, final List<Row> oracle) {
    final int dice = rnd.nextInt(100);
    if (dice < 50) {
      // In-place age update on a random age-bearing record.
      final int idx = pickAgeBearingRow(rnd, oracle);
      if (idx >= 0) {
        final long newAge = 18 + rnd.nextInt(73);
        moveToRow(wtx, idx);
        setAgeOnCurrentRecord(wtx, newAge);
        oracle.get(idx).age = newAge;
        return 1;
      }
      // No age-bearing record left (practically unreachable) — fall through to an insert.
    }
    if (dice < 65 && oracle.size() > MIN_ROWS) {
      final int idx = rnd.nextInt(oracle.size());
      moveToRow(wtx, idx);
      wtx.remove();
      oracle.remove(idx);
      return 1;
    }
    if (dice < 90) {
      final Row row = randomRecord(rnd);
      if (dice < 80 || oracle.isEmpty()) {
        moveToArray(wtx);
        wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader(recordJson(row)),
                                     JsonNodeTrx.Commit.NO);
        oracle.add(row);
      } else {
        final int idx = rnd.nextInt(oracle.size());
        moveToRow(wtx, idx);
        wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader(recordJson(row)),
                                        JsonNodeTrx.Commit.NO);
        oracle.add(idx + 1, row);
      }
      return 0;
    }
    if (dice < 95) {
      moveToArray(wtx);
      wtx.insertObjectAsLastChild();
      oracle.add(Row.record(null, null, null));
      return 0;
    }
    moveToArray(wtx);
    wtx.insertNullValueAsLastChild();
    oracle.add(Row.record(null, null, null));
    return 0;
  }

  /**
   * Per-commit verification: aggregate parity with the oracle plus leaf-level row totals.
   * Deleted and re-extracted rows may legitimately remain as presence-cleared physical
   * slots (mid-leaf maintenance clears a row's presence bits instead of compacting), so
   * the physical total is bounded by {@code live <= physical <= live + cumulative
   * mutations}; the aggregates prove the phantoms are dead.
   */
  private static void verifyAgainstOracle(final JsonResourceSession session, final int revision,
      final long[] stats, final int expectedRows, final int cumulativeMutations) {
    verifyAggregates(session, revision, stats);
    final int physicalRows = servedRowCount(session, revision);
    Assertions.assertTrue(physicalRows >= expectedRows,
        "physical rows " + physicalRows + " must cover the " + expectedRows
            + " live rows at revision " + revision);
    Assertions.assertTrue(physicalRows <= expectedRows + cumulativeMutations,
        "physical rows " + physicalRows + " must not exceed live " + expectedRows
            + " + cumulative mutations " + cumulativeMutations + " at revision " + revision);
  }

  /** Assert sum/count/min/max of "age" served at {@code revision} match the oracle stats. */
  private static void verifyAggregates(final JsonResourceSession session, final int revision,
      final long[] stats) {
    final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(session, revision, 2);
    try {
      Assertions.assertEquals(stats[0], aggregateLong(executor, "sum"),
          "sum(age) at revision " + revision);
      Assertions.assertEquals(stats[1], aggregateLong(executor, "count"),
          "count(age) at revision " + revision);
      Assertions.assertEquals(stats[2], aggregateLong(executor, "min"),
          "min(age) at revision " + revision);
      Assertions.assertEquals(stats[3], aggregateLong(executor, "max"),
          "max(age) at revision " + revision);
    } finally {
      executor.close();
    }
  }

  /** {@code [sum, count, min, max]} of the present ages in the oracle. */
  private static long[] oracleStats(final List<Row> oracle) {
    long sum = 0;
    long count = 0;
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (int i = 0; i < oracle.size(); i++) {
      final Long age = oracle.get(i).age;
      if (age != null) {
        sum += age;
        count++;
        min = Math.min(min, age);
        max = Math.max(max, age);
      }
    }
    Assertions.assertTrue(count > 0, "the soak must always keep age-bearing rows");
    return new long[] { sum, count, min, max };
  }

  private static long aggregateLong(final SirixVectorizedExecutor executor, final String func) {
    final Sequence result = executor.executeAggregate(null, SOURCE_PATH, func, "age");
    Assertions.assertNotNull(result,
        "the '" + func + "' aggregate must be SERVED — a null means the maintained projection "
            + "was silently tombstoned or refused");
    return ((Int64) result).longValue();
  }

  /** Total row count of the SERVED projection at {@code revision} (leaf-level integrity). */
  private static int servedRowCount(final JsonResourceSession session, final int revision) {
    try (final var rtx = session.beginNodeReadOnlyTrx(revision)) {
      final ProjectionIndexRegistry.Handle handle = session.getRtxIndexController(revision)
          .openProjectionIndex(rtx.getStorageEngineReader(), SOURCE_PATH, new String[] { "age" });
      Assertions.assertNotNull(handle, "the maintained projection must still be served");
      int rows = 0;
      for (final byte[] leaf : handle.leafPayloads()) {
        rows += ProjectionIndexLeafPage.deserialize(leaf).getRowCount();
      }
      return rows;
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Differential battery: session-bound (vectorized) vs. plain (interpreted) compile chain.
  // ---------------------------------------------------------------------------------------------

  private static final String BATTERY_DOC = "let $doc := jn:doc('" + SOAK_DB + "','" + SOAK_RESOURCE + "') ";

  private static final String[] BATTERY = {
      BATTERY_DOC + "return sum(for $r in $doc[] return $r.age)",
      BATTERY_DOC + "return count(for $r in $doc[] return $r.age)",
      BATTERY_DOC + "return min(for $r in $doc[] return $r.age)",
      BATTERY_DOC + "return max(for $r in $doc[] return $r.age)",
      BATTERY_DOC + "return count(for $r in $doc[] where $r.age > 40 and $r.active return $r)",
      BATTERY_DOC + "return count(for $r in $doc[] let $d := $r.dept group by $d return $d)",
      BATTERY_DOC + "return count(for $r in $doc[] where $r.dept = \"Eng\" return $r)",
  };

  /**
   * Runs the battery once through a session-bound chain (auto-wired vectorized executor) and
   * once through the plain chain; outputs must be identical and at least the plain aggregates
   * must be SERVED from the projection (servedCount proof).
   */
  private void runDifferentialBattery() throws IOException {
    final List<String> vectorized = new ArrayList<>(BATTERY.length);
    final List<String> interpreted = new ArrayList<>(BATTERY.length);

    try (final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup(SOAK_DB);
      final JsonResourceSession session = collection.getDatabase().beginResourceSession(SOAK_RESOURCE);
      final long servedBefore = ProjectionIndexCatalog.servedCount();
      try (final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store, session)) {
        for (final String query : BATTERY) {
          vectorized.add(evaluateToString(chain, ctx, query));
        }
      }
      Assertions.assertTrue(ProjectionIndexCatalog.servedCount() > servedBefore,
          "the battery must be SERVED from the catalogued projection, not the fallback");
    }

    try (final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      for (final String query : BATTERY) {
        interpreted.add(evaluateToString(chain, ctx, query));
      }
    }

    for (int i = 0; i < BATTERY.length; i++) {
      Assertions.assertEquals(interpreted.get(i), vectorized.get(i),
          "vectorized and interpreted answers must be identical for: " + BATTERY[i]);
    }
  }

  private static String evaluateToString(final SirixCompileChain chain, final SirixQueryContext ctx,
      final String query) throws IOException {
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
         final PrintWriter printWriter = new PrintWriter(out)) {
      new Query(chain, query).serialize(ctx, printWriter);
      printWriter.flush();
      return out.toString();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Concurrent readers (one session each, REST-shaped) racing an updating writer.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void concurrentReadersServeConsistentRevisionsDuringMaintenance() throws Exception {
    final Random rnd = new Random(SEED ^ 0x51AB);
    final List<Row> oracle = new ArrayList<>(800);
    final StringBuilder json = new StringBuilder(800 * 48).append('[');
    for (int i = 0; i < 800; i++) {
      final Row row = Row.record((long) (18 + rnd.nextInt(73)), rnd.nextBoolean(),
                                 DEPTS[rnd.nextInt(DEPTS.length)]);
      oracle.add(row);
      if (i > 0) {
        json.append(',');
      }
      json.append(recordJson(row));
    }
    json.append(']');
    query("jn:store('" + CONC_DB + "','conc.jn','" + json + "')");
    query("let $doc := jn:doc('" + CONC_DB + "','conc.jn') "
        + "let $stats := jn:create-projection-index($doc, '/[]', "
        + "('/[]/age', '/[]/active', '/[]/dept'), ('long', 'boolean', 'string')) "
        + "return {\"revision\": sdb:commit($doc)}");

    final Map<Integer, Long> expectedSumByRevision = new ConcurrentHashMap<>();
    final AtomicBoolean running = new AtomicBoolean(true);
    final AtomicReference<Throwable> readerFailure = new AtomicReference<>();
    final int readerCount = 3;
    final ExecutorService readers = Executors.newFixedThreadPool(readerCount);

    try (final Database<JsonResourceSession> writerDatabase = openStressDatabase(CONC_DB);
         final JsonResourceSession writerSession = writerDatabase.beginResourceSession("conc.jn")) {
      expectedSumByRevision.put(writerSession.getMostRecentRevisionNumber(),
                                oracleStats(oracle)[0]);

      final List<Future<Long>> checks = new ArrayList<>(readerCount);
      for (int r = 0; r < readerCount; r++) {
        checks.add(readers.submit(() -> {
          long performed = 0;
          try {
            final Random readerRnd = new Random(Thread.currentThread().threadId());
            while (running.get() && readerFailure.get() == null) {
              // A fresh database + session PER CHECK — the shape every REST request has,
              // and the only way a reader sees revisions committed after its open.
              final Integer[] revisions = expectedSumByRevision.keySet().toArray(new Integer[0]);
              final int revision = revisions[readerRnd.nextInt(revisions.length)];
              try (final Database<JsonResourceSession> database = openStressDatabase(CONC_DB);
                   final JsonResourceSession session = database.beginResourceSession("conc.jn")) {
                final SirixVectorizedExecutor executor =
                    new SirixVectorizedExecutor(session, revision, 2);
                try {
                  final Sequence sum = executor.executeAggregate(null, SOURCE_PATH, "sum", "age");
                  Assertions.assertNotNull(sum, "revision " + revision + " must be answered");
                  Assertions.assertEquals(expectedSumByRevision.get(revision).longValue(),
                                          ((Int64) sum).longValue(),
                                          "isolated sum at revision " + revision);
                } finally {
                  executor.close();
                }
              }
              performed++;
            }
          } catch (final Throwable t) {
            readerFailure.compareAndSet(null, t);
          }
          return performed;
        }));
      }

      for (int commit = 0; commit < 10 && readerFailure.get() == null; commit++) {
        try (final JsonNodeTrx wtx = writerSession.beginNodeTrx()) {
          for (int op = 0; op < 15; op++) {
            final int idx = pickAgeBearingRow(rnd, oracle);
            final long newAge = 18 + rnd.nextInt(73);
            moveToRow(wtx, idx);
            setAgeOnCurrentRecord(wtx, newAge);
            oracle.get(idx).age = newAge;
          }
          wtx.commit();
        }
        expectedSumByRevision.put(writerSession.getMostRecentRevisionNumber(),
                                  oracleStats(oracle)[0]);
      }

      running.set(false);
      long totalChecks = 0;
      for (final Future<Long> check : checks) {
        totalChecks += check.get(60, TimeUnit.SECONDS);
      }
      readers.shutdown();
      Assertions.assertTrue(readers.awaitTermination(30, TimeUnit.SECONDS));
      if (readerFailure.get() != null) {
        Assertions.fail("a concurrent reader observed an inconsistency", readerFailure.get());
      }
      Assertions.assertTrue(totalChecks >= readerCount,
          "every reader must have completed at least one revision check, got " + totalChecks);
    } finally {
      readers.shutdownNow();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Repeated tombstone → same-definition rebuild cycles.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void tombstoneRebuildCyclesKeepServingExactly() throws IOException {
    query("jn:store('" + CYCLE_DB + "','cycle.jn','{"
        + "\"records\":[{\"age\":1},{\"age\":2},{\"age\":3},{\"age\":4},{\"age\":5},"
        + "{\"age\":6},{\"age\":7},{\"age\":8},{\"age\":9},{\"age\":10}],"
        + "\"archive\":[]}')");
    final String createQuery = "let $doc := jn:doc('" + CYCLE_DB + "','cycle.jn') "
        + "let $stats := jn:create-projection-index($doc, '/records/[]', "
        + "('/records/[]/age'), ('long')) "
        + "return {\"revision\": sdb:commit($doc)}";
    query(createQuery);

    final String[] recordsPath = { "records", "[]" };
    long expectedSum = 55;
    for (int cycle = 0; cycle < 4; cycle++) {
      // Served before the unattributable change.
      assertRecordsSumServed(recordsPath, expectedSum, "cycle " + cycle + " pre-move");

      // Move the first record OUT of the record set — unattributable, must tombstone.
      // A fresh database + session per phase: revisions committed through other database
      // instances (the query() helpers) are only visible to sessions opened afterwards.
      try (final Database<JsonResourceSession> database = openStressDatabase(CYCLE_DB);
           final JsonResourceSession session = database.beginResourceSession("cycle.jn")) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          Assertions.assertTrue(wtx.moveToDocumentRoot());
          Assertions.assertTrue(wtx.moveToFirstChild());       // top-level OBJECT
          Assertions.assertTrue(wtx.moveToFirstChild());       // "records" fused array
          Assertions.assertEquals(NodeKind.OBJECT_NAMED_ARRAY, wtx.getKind());
          final long recordsArrayKey = wtx.getNodeKey();
          Assertions.assertTrue(wtx.moveToFirstChild());       // first record
          final long recordKey = wtx.getNodeKey();
          Assertions.assertTrue(wtx.moveTo(recordsArrayKey));
          Assertions.assertTrue(wtx.moveToRightSibling());     // "archive" fused array
          Assertions.assertEquals(NodeKind.OBJECT_NAMED_ARRAY, wtx.getKind());
          wtx.moveSubtreeToFirstChild(recordKey);
          wtx.commit();
        }
        expectedSum -= (cycle + 1);

        // Tombstoned: the projection must DECLINE (controller-mediated open is null), while
        // the executor's generic fallback and the interpreter both stay exact.
        final int afterMove = session.getMostRecentRevisionNumber();
        final ProjectionIndexRegistry.Handle leaked = openProjection(session, afterMove, recordsPath);
        Assertions.assertNull(leaked,
            "cycle " + cycle + ": a moved-out record must invalidate the projection — "
                + describeLeakedHandle(session, afterMove, leaked));
        final SirixVectorizedExecutor fallback = new SirixVectorizedExecutor(session, afterMove, 2);
        try {
          final Sequence sum = fallback.executeAggregate(null, recordsPath, "sum", "age");
          Assertions.assertNotNull(sum, "the generic fallback must still answer");
          Assertions.assertEquals(expectedSum, ((Int64) sum).longValue(),
              "cycle " + cycle + ": post-tombstone answers must come from live data, never "
                  + "from a stale projection snapshot");
        } finally {
          fallback.close();
        }
      }
      test("let $doc := jn:doc('" + CYCLE_DB + "','cycle.jn') "
          + "return sum(for $r in $doc.records[] return $r.age)", String.valueOf(expectedSum));

      // Same-definition re-create must rebuild and serve again.
      query(createQuery);
      assertRecordsSumServed(recordsPath, expectedSum, "cycle " + cycle + " post-rebuild");
    }
  }

  private static void assertRecordsSumServed(final String[] recordsPath, final long expectedSum,
      final String phase) {
    try (final Database<JsonResourceSession> database = openStressDatabase(CYCLE_DB);
         final JsonResourceSession session = database.beginResourceSession("cycle.jn")) {
      final int revision = session.getMostRecentRevisionNumber();
      Assertions.assertNotNull(openProjection(session, revision, recordsPath),
          phase + ": the projection must SERVE (controller-mediated open)");
      final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(session, revision, 2);
      try {
        final Sequence sum = executor.executeAggregate(null, recordsPath, "sum", "age");
        Assertions.assertNotNull(sum, phase + ": the aggregate must be answered");
        Assertions.assertEquals(expectedSum, ((Int64) sum).longValue(), phase + ": served sum");
      } finally {
        executor.close();
      }
    }
  }

  /**
   * Forensics for the CI-only tombstone failure: what the leaked handle contains and what
   * the store says at that revision, so the assertion message localizes the leak (stale
   * pre-move snapshot vs. missing tombstone vs. wrong revision).
   */
  private static String describeLeakedHandle(final JsonResourceSession session, final int revision,
      final ProjectionIndexRegistry.Handle leaked) {
    if (leaked == null) {
      return "";
    }
    final StringBuilder sb = new StringBuilder(256);
    sb.append("revision=").append(revision)
      .append(" mostRecent=").append(session.getMostRecentRevisionNumber())
      .append(" validFrom=").append(leaked.validFromRevision())
      .append(" leaves=").append(leaked.leafPayloads().size());
    int rows = 0;
    for (final byte[] leaf : leaked.leafPayloads()) {
      rows += ProjectionIndexLeafPage.deserialize(leaf).getRowCount();
    }
    sb.append(" rowTotal=").append(rows);
    sb.append(" projectionDefsAtRevision=").append(session.getRtxIndexController(revision)
        .getIndexes().getNrOfIndexDefsWithType(IndexType.PROJECTION));
    return sb.toString();
  }

  /** Controller-mediated committed open — {@code null} when the projection declines. */
  private static ProjectionIndexRegistry.Handle openProjection(final JsonResourceSession session,
      final int revision, final String[] recordsPath) {
    try (final var rtx = session.beginNodeReadOnlyTrx(revision)) {
      return session.getRtxIndexController(revision)
          .openProjectionIndex(rtx.getStorageEngineReader(), recordsPath, new String[] { "age" });
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Shared fixture plumbing.
  // ---------------------------------------------------------------------------------------------

  private void storeInitialRecordSet(final Random rnd, final List<Row> oracle) {
    final StringBuilder json = new StringBuilder(INITIAL_RECORDS * 48).append('[');
    for (int i = 0; i < INITIAL_RECORDS; i++) {
      final Row row = randomRecord(rnd);
      oracle.add(row);
      if (i > 0) {
        json.append(',');
      }
      json.append(recordJson(row));
    }
    json.append(']');
    query("jn:store('" + SOAK_DB + "','" + SOAK_RESOURCE + "','" + json + "')");
  }

  private void createProjection() {
    query("let $doc := jn:doc('" + SOAK_DB + "','" + SOAK_RESOURCE + "') "
        + "let $stats := jn:create-projection-index($doc, '/[]', "
        + "('/[]/age', '/[]/active', '/[]/dept'), ('long', 'boolean', 'string')) "
        + "return {\"revision\": sdb:commit($doc)}");
  }

  /** Sparse-aware random record: ~12% miss age, ~10% miss active, ~15% miss dept. */
  private static Row randomRecord(final Random rnd) {
    final Long age = rnd.nextInt(100) < 88 ? (long) (18 + rnd.nextInt(73)) : null;
    final Boolean active = rnd.nextInt(100) < 90 ? rnd.nextBoolean() : null;
    final String dept = rnd.nextInt(100) < 85 ? DEPTS[rnd.nextInt(DEPTS.length)] : null;
    return Row.record(age, active, dept);
  }

  private static String recordJson(final Row row) {
    final StringBuilder sb = new StringBuilder(48).append('{');
    boolean first = true;
    if (row.age != null) {
      sb.append("\"age\":").append(row.age.longValue());
      first = false;
    }
    if (row.active != null) {
      if (!first) {
        sb.append(',');
      }
      sb.append("\"active\":").append(row.active.booleanValue());
      first = false;
    }
    if (row.dept != null) {
      if (!first) {
        sb.append(',');
      }
      sb.append("\"dept\":\"").append(row.dept).append('"');
    }
    return sb.append('}').toString();
  }

  private static Database<JsonResourceSession> openStressDatabase(final String db) {
    final Path dbPath = Path.of(JsonTestHelper.PATHS.PATH1.getFile().getParent().toString(), db);
    return Databases.openJsonDatabase(dbPath);
  }

  /** Index of a random oracle row that carries an age, or {@code -1} if none exists. */
  private static int pickAgeBearingRow(final Random rnd, final List<Row> oracle) {
    for (int attempt = 0; attempt < 64; attempt++) {
      final int idx = rnd.nextInt(oracle.size());
      if (oracle.get(idx).age != null) {
        return idx;
      }
    }
    for (int idx = 0; idx < oracle.size(); idx++) {
      if (oracle.get(idx).age != null) {
        return idx;
      }
    }
    return -1;
  }

  /** Position the wtx on the top-level ARRAY node. */
  private static void moveToArray(final JsonNodeTrx wtx) {
    Assertions.assertTrue(wtx.moveToDocumentRoot());
    Assertions.assertTrue(wtx.moveToFirstChild());
    Assertions.assertEquals(NodeKind.ARRAY, wtx.getKind());
  }

  /** Position the wtx on array child {@code rowIndex}. */
  private static void moveToRow(final JsonNodeTrx wtx, final int rowIndex) {
    moveToArray(wtx);
    Assertions.assertTrue(wtx.moveToFirstChild());
    for (int i = 0; i < rowIndex; i++) {
      Assertions.assertTrue(wtx.moveToRightSibling(), "row " + rowIndex + " must exist");
    }
  }

  /** From a record OBJECT node, find the fused "age" field and update its value. */
  private static void setAgeOnCurrentRecord(final JsonNodeTrx wtx, final long newAge) {
    Assertions.assertEquals(NodeKind.OBJECT, wtx.getKind(), "age updates target object records");
    Assertions.assertTrue(wtx.moveToFirstChild(), "record must have fields");
    while (!(wtx.getKind() == NodeKind.OBJECT_NAMED_NUMBER
        && "age".equals(wtx.getName().getLocalName()))) {
      Assertions.assertTrue(wtx.moveToRightSibling(), "record must carry an age field");
    }
    wtx.setNumberValue(newAge);
  }
}
