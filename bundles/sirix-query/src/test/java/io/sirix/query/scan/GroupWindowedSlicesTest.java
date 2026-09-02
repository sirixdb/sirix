package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.GroupTableSpill;
import io.sirix.index.projection.HeapHeadroom;
import io.sirix.index.projection.ProjectionColumnScan;
import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The sliced group arms with the column fill budget pinned to one byte: no column can be resident,
 * so the arms must feed their kernels per-sub-chunk windowed slices ({@code windowedSlices}) instead
 * of falling back to the whole-leaf byte kernels — the 100M shape of a fat column.
 *
 * <p>
 * Each query runs windowed FIRST (a resident fill persists in the catalog's store across executors,
 * so the order matters), then resident with the budget restored, then through the interpreter; all
 * three must agree. Engagement is asserted per query — a serve that quietly took another route would
 * make the agreement vacuous — and every fixture is fresh, because the handle promotes whole-leaf
 * payloads after two route arrivals. Strict serving is on: an arm that failed and fell back fails the
 * test. Two seam variants: single-leaf sub-chunks with the hash-range pass budget and the spill
 * threshold pinned low (many fills and releases, passes re-creating the per-worker arrays), and the
 * defaults.
 * </p>
 */
final class GroupWindowedSlicesTest {
  private static final String DB = "group-windowed-db";
  private static final String RES = "records.jn";
  private static final int N = 8_000;
  private static final String DOC = "jn:doc('" + DB + "','" + RES + "')[]";

  /** Shapes whose arm must serve WINDOWED under the one-byte budget. */
  private static final List<String> WINDOWED = List.of(
      // numeric key with a zone-prunable predicate (leaves below the bound decode nothing), top-k by count
      "subsequence(for $h in " + DOC + " where $h.amount ge 2000 let $k := $h.k40 group by $k let $c := count($h) "
          + "order by $c descending return {\"k40\": $k, \"c\": $c, \"sum\": sum($h.amount)}, 1, 12)",
      // numeric key with a grouped COUNT(DISTINCT)
      "subsequence(for $h in " + DOC + " let $k := $h.k40 group by $k let $u := count(distinct-values($h.u)) "
          + "order by $u descending return {\"k40\": $k, \"u\": $u}, 1, 12)",
      // numeric key under an OR predicate tree without leaf evidence (k7 and k40 span every leaf)
      "subsequence(for $h in " + DOC + " where $h.k7 eq 1 or $h.k40 eq 3 let $k := $h.k40 group by $k "
          + "let $c := count($h) order by $c descending return {\"k40\": $k, \"c\": $c}, 1, 12)",
      // OR trees WITH leaf evidence (amount is the document order, so both branches name leaf ranges):
      // the tree's keep mask must drop the middle leaves on every arm — numeric, string and composite keys
      "subsequence(for $h in " + DOC + " where ($h.amount ge 7000 and $h.k7 eq 1) or $h.amount lt 500 "
          + "let $k := $h.k40 group by $k let $c := count($h) order by $c descending "
          + "return {\"k40\": $k, \"c\": $c, \"sum\": sum($h.amount)}, 1, 12)",
      "subsequence(for $h in " + DOC + " where ($h.amount ge 7000 and $h.k7 eq 1) or $h.amount lt 500 "
          + "let $k := $h.s group by $k let $c := count($h) order by $c descending "
          + "return {\"s\": $k, \"c\": $c, \"sum\": sum($h.amount)}, 1, 12)",
      "subsequence(for $h in " + DOC + " where ($h.amount ge 7000 and $h.k7 eq 1) or $h.amount lt 500 "
          + "let $a := $h.k7, $b := $h.k40 group by $a, $b let $c := count($h) order by $c descending "
          + "return {\"k7\": $a, \"k40\": $b, \"c\": $c}, 1, 12)",
      // string key, top-k (winner emission reads each winner's leaf dictionary through a one-leaf access)
      "subsequence(for $h in " + DOC + " let $k := $h.s group by $k let $c := count($h) "
          + "order by $c descending return {\"s\": $k, \"c\": $c, \"sum\": sum($h.amount)}, 1, 12)",
      // string key with a predicate and a grouped COUNT(DISTINCT) of a numeric operand
      "subsequence(for $h in " + DOC + " where $h.amount lt 6000 let $k := $h.s group by $k "
          + "let $u := count(distinct-values($h.u)) order by $u descending return {\"s\": $k, \"u\": $u}, 1, 12)",
      // composite numeric keys with a predicate
      "subsequence(for $h in " + DOC + " where $h.amount ge 2000 let $a := $h.k7, $b := $h.k40 group by $a, $b "
          + "let $c := count($h) order by $c descending return {\"k7\": $a, \"k40\": $b, \"c\": $c, \"sum\": sum($h.amount)}, 1, 12)",
      // composite key with a string component (winner key parts read through the one-leaf access)
      "subsequence(for $h in " + DOC + " let $a := $h.k7, $s := $h.s group by $a, $s let $c := count($h) "
          + "order by $c descending return {\"k7\": $a, \"s\": $s, \"c\": $c}, 1, 12)",
      // composite key with a grouped COUNT(DISTINCT)
      "subsequence(for $h in " + DOC + " let $a := $h.k7, $b := $h.k40 group by $a, $b "
          + "let $u := count(distinct-values($h.u)) order by $u descending return {\"k7\": $a, \"k40\": $b, \"u\": $u}, 1, 12)",
      // packed ISO-minute substring key
      "subsequence(for $h in " + DOC + " let $m := substring($h.t, 1, 16) group by $m let $c := count($h) "
          + "order by $c descending return {\"m\": $m, \"c\": $c}, 1, 12)",
      // group by a CONSTANT (the const-group fold: no key column at all), with a predicate
      "for $h in " + DOC + " where $h.amount ge 2000 let $g := 1 group by $g "
          + "return {\"c\": count($h), \"s\": sum($h.amount), \"mx\": max($h.u), \"mn\": min($h.k40)}",
      // the legacy emission legs (ORDER BY without a LIMIT: no order plan) — numeric and string keys
      "for $h in " + DOC + " where $h.amount ge 1500 let $k := $h.k40 group by $k order by $k "
          + "return {\"k40\": $k, \"c\": count($h), \"s\": sum($h.amount)}",
      "for $h in " + DOC + " let $k := $h.s group by $k order by $k return {\"s\": $k, \"c\": count($h)}",
      // deferred string extrema (pass 2 over the winners' groups, windowed too), with a predicate
      "subsequence(for $h in " + DOC + " where $h.amount ge 1000 let $k := $h.s group by $k let $c := count($h) "
          + "order by $c descending return {\"s\": $k, \"c\": $c, \"w\": min($h.w), \"x\": max($h.w)}, 1, 12)");

  /** Shapes the windowed route deliberately leaves alone; they must still serve and agree. */
  private static final List<String> RESIDENT_ONLY = List.of();

  private Path dbDir;
  private String previousGlobalDictMode;
  private long previousBudget = -1L;
  private long previousGroupBudget;
  private int previousThreshold;
  private int previousSubChunk;

  static Stream<Arguments> shapes() {
    final List<Arguments> out = new ArrayList<>();
    for (final boolean tight : new boolean[] {true, false}) {
      for (final String q : WINDOWED) {
        out.add(Arguments.of(q, true, tight));
      }
      for (final String q : RESIDENT_ONLY) {
        out.add(Arguments.of(q, false, tight));
      }
    }
    return out.stream();
  }

  @BeforeEach
  void setUp() throws Exception {
    // Per-leaf dictionaries for every string column: `w` (8000 distinct values) would otherwise be
    // promoted to a GLOBAL dictionary, and global deferred operands fold in the whole-leaf kernels by
    // design — the windowed pass 2 under test reads per-leaf dictionaries.
    previousGlobalDictMode = System.getProperty("sirix.projection.globalDict");
    System.setProperty("sirix.projection.globalDict", "never");
    SirixVectorizedExecutor.STRICT_SERVING = true;
    dbDir = Files.createTempDirectory("sirix-group-windowed-");
    final StringBuilder sb = new StringBuilder(N * 96);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) {
        sb.append(',');
      }
      // 240 distinct minutes: the packed substring arm keeps every pass within the pinned group budget
      final int minute = (i * 7919) % 240;
      final int day = minute / 1440;
      final int hh = (minute % 1440) / 60;
      final int mm = minute % 60;
      sb.append("{\"id\":").append(i).append(",\"k7\":").append(i % 7).append(",\"k40\":").append(i % 40)
        // w: 8000 distinct values, so every group's string extremum lives in ONE leaf — a pass that
        // overwrote instead of folding across sub-chunks would answer with the wrong leaf's value
        .append(",\"s\":\"s").append(i % 50).append("\",\"w\":\"w").append(String.format("%05d", (i * 7919) % 8000))
        .append("\",\"u\":")
        .append(i % 97).append(",\"amount\":").append(i).append(",\"t\":\"2024-")
        .append(String.format("%02d", 1 + day / 28)).append('-').append(String.format("%02d", 1 + day % 28))
        .append('T').append(String.format("%02d", hh)).append(':').append(String.format("%02d", mm)).append(":00\"}");
    }
    sb.append(']');
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb + "')").evaluate(ctx);
      new Query(chain, """
          let $doc := jn:doc('%s','%s')
          let $stats := jn:create-projection-index($doc, '/[]',
            ('/[]/id', '/[]/k7', '/[]/k40', '/[]/s', '/[]/w', '/[]/u', '/[]/amount', '/[]/t'),
            ('long', 'long', 'long', 'string', 'string', 'long', 'long', 'string'))
          return sdb:commit($doc)
          """.formatted(DB, RES)).evaluate(ctx);
    }
  }

  @AfterEach
  void tearDown() {
    if (previousGlobalDictMode == null) {
      System.clearProperty("sirix.projection.globalDict");
    } else {
      System.setProperty("sirix.projection.globalDict", previousGlobalDictMode);
    }
    if (previousBudget >= 0L) {
      ProjectionColumnStore.setColumnFillBudgetBytesForTesting(previousBudget);
      previousBudget = -1L;
    }
    SirixVectorizedExecutor.STRICT_SERVING = false;
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (dbDir != null) {
      Databases.removeDatabase(dbDir.resolve(DB));
    }
  }

  @ParameterizedTest(name = "[{index}] windowed={1} tightSeams={2}")
  @MethodSource("shapes")
  void windowedSlicesAgreeWithResidentAndInterpreter(final String query, final boolean expectWindowed,
      final boolean tightSeams) throws Exception {
    if (tightSeams) {
      previousGroupBudget = GroupTableSpill.setGroupBudgetForTesting(32L);
      previousThreshold = GroupTableSpill.setFlushGroupsForTesting(8);
      previousSubChunk = GroupTableSpill.setSubChunkLeavesForTesting(1);
    }
    try {
      final long windowedRoutesBefore = SirixVectorizedExecutor.groupWindowedSlicesCount();
      final long windowedAccessBefore = ProjectionColumnStore.windowedLeafAccessCount();
      final long servedBefore = groupServes();
      previousBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(1L);
      final String windowed = run(query, true);
      ProjectionColumnStore.setColumnFillBudgetBytesForTesting(previousBudget);
      previousBudget = -1L;
      assertTrue(groupServes() > servedBefore, "not served by a group arm under the one-byte budget: " + query);
      if (expectWindowed) {
        assertTrue(SirixVectorizedExecutor.groupWindowedSlicesCount() > windowedRoutesBefore,
            "the windowed-slice route never engaged for: " + query);
        assertTrue(ProjectionColumnStore.windowedLeafAccessCount() > windowedAccessBefore,
            "no windowed leaf access was opened for: " + query);
      } else {
        assertEquals(windowedRoutesBefore, SirixVectorizedExecutor.groupWindowedSlicesCount(),
            "a shape the windowed route must leave alone took it: " + query);
      }
      final long servedBeforeResident = groupServes();
      final String resident = run(query, true);
      assertTrue(groupServes() > servedBeforeResident, "not served by a group arm with the budget restored: " + query);
      final String generic = run(query, false);
      assertEquals(generic, windowed, "windowed slices diverge from the interpreter for: " + query);
      assertEquals(resident, windowed, "windowed slices diverge from the resident arm for: " + query);
    } finally {
      if (tightSeams) {
        GroupTableSpill.setGroupBudgetForTesting(previousGroupBudget);
        GroupTableSpill.setFlushGroupsForTesting(previousThreshold);
        GroupTableSpill.setSubChunkLeavesForTesting(previousSubChunk);
      }
    }
  }

  /** The OR-tree WHERE whose both branches name leaf ranges of the document order, per tree-capable arm. */
  static Stream<String> treeArms() {
    final String where = " where ($h.amount ge 7000 and $h.k7 eq 1) or $h.amount lt 500 ";
    return Stream.of(
        // numeric key
        "subsequence(for $h in " + DOC + where + "let $k := $h.k40 group by $k let $c := count($h) "
            + "order by $c descending return {\"k40\": $k, \"c\": $c, \"sum\": sum($h.amount)}, 1, 12)",
        // string key
        "subsequence(for $h in " + DOC + where + "let $k := $h.s group by $k let $c := count($h) "
            + "order by $c descending return {\"s\": $k, \"c\": $c, \"sum\": sum($h.amount)}, 1, 12)",
        // composite key
        "subsequence(for $h in " + DOC + where + "let $a := $h.k7, $b := $h.k40 group by $a, $b let $c := count($h) "
            + "order by $c descending return {\"k7\": $a, \"k40\": $b, \"c\": $c}, 1, 12)",
        // packed substring key
        "subsequence(for $h in " + DOC + where + "let $m := substring($h.t, 1, 16) group by $m let $c := count($h) "
            + "order by $c descending return {\"m\": $m, \"c\": $c}, 1, 12)");
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("treeArms")
  void orTreesPruneLeavesThroughTheKeepMaskOnBothRoutes(final String query) throws Exception {
    // The keep mask used to be conjunction-only: ONE `or` made the whole WHERE a tree and the tree's
    // columns were filled FULL (q40 at 100M fetched all 97,654 leaves of every column for 723 rows'
    // worth of CounterID). Both branches here name leaf ranges of the document order, so the tree's
    // mask must drop the middle leaves — windowed AND resident — and the answer must not move. One
    // fixture per arm: the handle promotes whole-leaf payloads after two sliced route arrivals, and
    // the whole-leaf kernels compute no keep mask.
    final String generic = run(query, false);
    final long prunedBefore = ProjectionColumnScan.treeLeavesPrunedCount();
    final long windowedBefore = SirixVectorizedExecutor.groupWindowedSlicesCount();
    previousBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(1L);
    final String windowed = run(query, true);
    ProjectionColumnStore.setColumnFillBudgetBytesForTesting(previousBudget);
    previousBudget = -1L;
    assertEquals(windowedBefore + 1, SirixVectorizedExecutor.groupWindowedSlicesCount(), "windowed route engaged");
    final long prunedWindowed = ProjectionColumnScan.treeLeavesPrunedCount() - prunedBefore;
    assertTrue(prunedWindowed > 0, "the windowed tree route pruned no leaf");
    final long slicedBefore = SirixVectorizedExecutor.groupAggSlicedServedCount();
    final String resident = run(query, true);
    assertTrue(SirixVectorizedExecutor.groupAggSlicedServedCount() > slicedBefore,
        "the second arrival must still serve sliced (resident), or the resident assertion is vacuous");
    assertTrue(ProjectionColumnScan.treeLeavesPrunedCount() - prunedBefore > prunedWindowed,
        "the resident tree fill pruned no leaf");
    assertEquals(generic, windowed, "windowed tree pruning changed the answer");
    assertEquals(generic, resident, "resident tree pruning changed the answer");
  }

  @Test
  void columnsThatEachFitButNotTogetherGoWindowedOnTheFirstTry() throws Exception {
    // FIT is the COMBINED projected fill. With the budget between one column's fill and both columns'
    // fills, a per-column judgement fills the key column, retains it, and throws the budget door on
    // the aggregate column mid-route — a whole-leaf re-entry (q9 at 100M/8 GB: 172 s cold, 1.5 s once
    // windowed). The first try must go windowed directly: nothing retained, no decline, no failure.
    final String query = "subsequence(for $h in " + DOC + " let $k := $h.k40 group by $k let $c := count($h) "
        + "order by $c descending return {\"k40\": $k, \"c\": $c, \"sum\": sum($h.amount)}, 1, 12)";
    final String generic = run(query, false);
    final ProjectionColumnStore store;
    final long budget;
    try (var db = Databases.openJsonDatabase(dbDir.resolve(DB)); var session = db.beginResourceSession(RES)) {
      final ProjectionIndexRegistry.Handle handle = ProjectionIndexCatalog.lookupCovering(session,
          session.getResourceConfig().getResource().toString(), session.getMostRecentRevisionNumber(),
          new String[] {"[]"}, new String[] {"k40", "amount"});
      assertNotNull(handle, "the projection must be loadable");
      store = handle.columnStoreOrNull();
      assertNotNull(store, "the catalog must build a column store");
      final int k40 = handle.columnOf("k40");
      final int amount = handle.columnOf("amount");
      assertTrue(k40 >= 0 && amount >= 0, "field columns");
      final long a = store.projectedColumnFillBytes(k40);
      final long b = store.projectedColumnFillBytes(amount);
      assertTrue(a > 0L && b > 0L, "projected fills");
      budget = store.retainedFillBytes() + Math.max(a, b) + Math.min(a, b) / 2;
    }
    final long retainedBefore = store.retainedFillBytes();
    final long windowedBefore = SirixVectorizedExecutor.groupWindowedSlicesCount();
    final long declinedBefore = SirixVectorizedExecutor.groupAggregateDeclinedCount();
    final long failedBefore = SirixVectorizedExecutor.groupAggFailedCount();
    final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
    previousBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(budget);
    final String served = run(query, true);
    ProjectionColumnStore.setColumnFillBudgetBytesForTesting(previousBudget);
    previousBudget = -1L;
    assertEquals(generic, served, "windowed slices diverge from the interpreter");
    assertEquals(servedBefore + 1, SirixVectorizedExecutor.groupAggServedCount(), "served by a group arm");
    assertEquals(windowedBefore + 1, SirixVectorizedExecutor.groupWindowedSlicesCount(),
        "the windowed-slice route must engage on the first try");
    assertEquals(declinedBefore, SirixVectorizedExecutor.groupAggregateDeclinedCount(), "no decline");
    assertEquals(failedBefore, SirixVectorizedExecutor.groupAggFailedCount(), "no failure signal");
    assertEquals(retainedBefore, store.retainedFillBytes(),
        "the resident path must not have filled a column first: the fit decision is the combined fill");
  }

  @Test
  void theHeadroomShareGatesRetentionAndTheQueryExitReleasesIt() throws Exception {
    // R1 is OPT-IN since the 100M leg measured what it costs when it decides against a query that is
    // about to re-read the column it just filled (see ProjectionColumnStore's flag). The lever still
    // has to work when a deployment asks for it, so this witness turns it on explicitly and restores
    // the process default afterwards.
    final boolean previousResidency = ProjectionColumnStore.setResidencyHeadroomForTesting(true);
    try {
      theHeadroomShareGatesRetentionAndTheQueryExitReleasesItWithResidencyOn();
    } finally {
      ProjectionColumnStore.setResidencyHeadroomForTesting(previousResidency);
    }
  }

  private void theHeadroomShareGatesRetentionAndTheQueryExitReleasesItWithResidencyOn() throws Exception {
    // R1. The static fill budget is untouched here and is orders of magnitude above this fixture:
    // the ONLY thing deciding residency is the shared heap-headroom share, and the only thing
    // returning bytes is the query scope's exit.
    final String query = "subsequence(for $h in " + DOC + " let $k := $h.k40 group by $k let $c := count($h) "
        + "order by $c descending return {\"k40\": $k, \"c\": $c, \"sum\": sum($h.amount)}, 1, 12)";
    final String generic = run(query, false);
    final ProjectionColumnStore store;
    final long combined;
    try (var db = Databases.openJsonDatabase(dbDir.resolve(DB)); var session = db.beginResourceSession(RES)) {
      final ProjectionIndexRegistry.Handle handle = ProjectionIndexCatalog.lookupCovering(session,
          session.getResourceConfig().getResource().toString(), session.getMostRecentRevisionNumber(),
          new String[] {"[]"}, new String[] {"k40", "amount"});
      assertNotNull(handle, "the projection must be loadable");
      store = handle.columnStoreOrNull();
      assertNotNull(store, "the catalog must build a column store");
      combined = store.projectedColumnFillBytes(handle.columnOf("k40"))
          + store.projectedColumnFillBytes(handle.columnOf("amount"));
      assertTrue(combined > 0L, "projected fills");
    }
    final long previousHeadroom = HeapHeadroom.setHeadroomForTesting(-1L);
    try {
      // (a) Over the share: the arm serves through windowed slices and the store retains nothing.
      shareOf(combined - 1);
      final long retainedBefore = store.retainedFillBytes();
      final long windowedBefore = SirixVectorizedExecutor.groupWindowedSlicesCount();
      assertEquals(generic, run(query, true), "the windowed serve must answer like the interpreter");
      assertTrue(SirixVectorizedExecutor.groupWindowedSlicesCount() > windowedBefore,
          "over the headroom share the arm must take the windowed-slice route");
      assertEquals(retainedBefore, store.retainedFillBytes(), "a fill over the share must retain nothing");

      // (b) Under the share: the same query fills resident, and its exit keeps what still fits.
      shareOf(4L * combined);
      assertEquals(generic, run(query, true));
      final long resident = store.retainedFillBytes();
      assertTrue(resident > retainedBefore, "under the share the arm must retain its fills");

      // (c) The share falls below what is retained: the NEXT query's exit returns the bytes.
      final long releasedBefore = ProjectionColumnStore.residencyReleasedBytes();
      shareOf(resident / 2);
      assertEquals(generic, run(query, true), "an already-resident column still serves and still agrees");
      assertTrue(store.retainedFillBytes() < resident, "the query exit must return bytes");
      assertTrue(store.retainedFillBytes() <= resident / 2, "…down to the share");
      assertTrue(ProjectionColumnStore.residencyReleasedBytes() > releasedBefore, "…and say so");
    } finally {
      HeapHeadroom.setHeadroomForTesting(previousHeadroom);
      ProjectionColumnStore.sampleHeadroomShare();
    }
  }

  /** Pin the shared headroom share to exactly {@code bytes} (a quarter of the headroom on this heap). */
  private static void shareOf(final long bytes) {
    HeapHeadroom.setHeadroomForTesting(Math.max(0L, bytes) * 4L);
    assertEquals(bytes, ProjectionColumnStore.sampleHeadroomShare(),
        "the share must be a quarter of the pinned headroom on this heap");
  }

  /** Serves by any group arm: the keyed arms or the constant-key fold. */
  private static long groupServes() {
    return SirixVectorizedExecutor.groupAggServedCount() + SirixVectorizedExecutor.constGroupAggServedCount();
  }

  private String run(final String query, final boolean vectorized) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = vectorized
            ? SirixCompileChain.createWithJsonStore(store)
            : SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      SirixVectorizedExecutor exec = null;
      try {
        if (vectorized) {
          final var db = Databases.openJsonDatabase(dbDir.resolve(DB));
          final JsonResourceSession session = db.beginResourceSession(RES);
          exec = new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber());
          SequentialPipelineStrategy.setVectorizedExecutor(exec);
        }
        final Sequence result = new Query(chain, query).execute(ctx);
        final StringWriter out = new StringWriter();
        try (PrintWriter pw = new PrintWriter(out)) {
          new StringSerializer(pw).serialize(result);
        }
        return out.toString();
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        if (exec != null) {
          exec.close();
        }
      }
    }
  }
}
